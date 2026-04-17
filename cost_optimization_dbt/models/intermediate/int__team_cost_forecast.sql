-- Per-team cost forecast using monthly linear regression.
-- Requires at least 2 months of data per team for meaningful projection.
WITH monthly_team AS (
    SELECT
        team_name,
        month,
        SUM(total_cost) AS monthly_cost,
        DATEDIFF('month', MIN(month) OVER (PARTITION BY team_name), month) AS month_offset
    FROM {{ ref('int__team_cost_attribution') }}
    GROUP BY team_name, month
),

team_regression AS (
    SELECT
        team_name,
        REGR_SLOPE(monthly_cost, month_offset) AS slope,
        REGR_INTERCEPT(monthly_cost, month_offset) AS intercept,
        STDDEV(monthly_cost) AS stddev,
        MAX(month_offset) AS max_offset,
        COUNT(*) AS data_months,
        MAX(month) AS latest_month
    FROM monthly_team
    GROUP BY team_name
    HAVING COUNT(*) >= 2
),

future_months AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS future_month_num
    FROM TABLE(GENERATOR(ROWCOUNT => 3))
)

SELECT
    tr.team_name,
    DATEADD('month', fm.future_month_num, tr.latest_month)::DATE AS forecast_month,
    fm.future_month_num + tr.max_offset + 1 AS projected_offset,
    GREATEST(0, tr.intercept + tr.slope * (fm.future_month_num + tr.max_offset + 1)) AS predicted_monthly_cost,
    GREATEST(0, tr.intercept + tr.slope * (fm.future_month_num + tr.max_offset + 1) - 1.96 * tr.stddev) AS ci_lower,
    tr.intercept + tr.slope * (fm.future_month_num + tr.max_offset + 1) + 1.96 * tr.stddev AS ci_upper,
    tr.slope AS monthly_trend,
    tr.data_months,
    CURRENT_TIMESTAMP() AS generated_at
FROM team_regression tr
CROSS JOIN future_months fm
