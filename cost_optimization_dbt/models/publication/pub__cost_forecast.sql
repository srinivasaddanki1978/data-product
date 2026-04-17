-- Publication layer for cost forecast: monthly aggregated forecasts + actuals for chart context.
WITH forecast_monthly AS (
    SELECT
        DATE_TRUNC('month', forecast_date)::DATE AS month,
        'FORECAST' AS data_type,
        SUM(predicted_total_cost) AS total_cost,
        SUM(predicted_compute_cost) AS compute_cost,
        SUM(predicted_storage_cost) AS storage_cost,
        SUM(predicted_serverless_cost) AS serverless_cost,
        SUM(ci_lower) AS ci_lower,
        SUM(ci_upper) AS ci_upper
    FROM {{ ref('int__cost_forecast') }}
    GROUP BY 1
),

actuals_monthly AS (
    SELECT
        DATE_TRUNC('month', date)::DATE AS month,
        'ACTUAL' AS data_type,
        SUM(total_cost) AS total_cost,
        SUM(compute_cost) AS compute_cost,
        SUM(storage_cost) AS storage_cost,
        SUM(serverless_cost) AS serverless_cost,
        NULL AS ci_lower,
        NULL AS ci_upper
    FROM {{ ref('int__daily_cost_rollup') }}
    WHERE date >= DATEADD('month', -3, DATE_TRUNC('month', CURRENT_DATE()))
    GROUP BY 1
),

combined AS (
    SELECT * FROM actuals_monthly
    UNION ALL
    SELECT * FROM forecast_monthly
),

annual_projection AS (
    SELECT
        -- YTD actuals
        (SELECT COALESCE(SUM(total_cost), 0)
         FROM {{ ref('int__daily_cost_rollup') }}
         WHERE date >= DATE_TRUNC('year', CURRENT_DATE())
           AND date < CURRENT_DATE()
        ) AS ytd_actual,
        -- Remaining forecast for current year
        (SELECT COALESCE(SUM(predicted_total_cost), 0)
         FROM {{ ref('int__cost_forecast') }}
         WHERE forecast_date >= CURRENT_DATE()
           AND forecast_date < DATEADD('year', 1, DATE_TRUNC('year', CURRENT_DATE()))
        ) AS remaining_forecast
)

SELECT
    c.month,
    c.data_type,
    c.total_cost,
    c.compute_cost,
    c.storage_cost,
    c.serverless_cost,
    c.ci_lower,
    c.ci_upper,
    ap.ytd_actual + ap.remaining_forecast AS projected_annual_spend
FROM combined c
CROSS JOIN annual_projection ap
ORDER BY c.month, c.data_type
