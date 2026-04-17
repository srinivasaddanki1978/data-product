-- Cost Forecasting: linear regression on last 90 days, projecting 90 days forward.
-- Uses Snowflake built-in REGR_SLOPE/REGR_INTERCEPT for each cost category.
WITH historical AS (
    SELECT
        date,
        DATEDIFF('day', MIN(date) OVER (), date) AS day_offset,
        total_cost,
        compute_cost,
        storage_cost,
        serverless_cost
    FROM {{ ref('int__daily_cost_rollup') }}
    WHERE date >= DATEADD('day', -90, CURRENT_DATE())
      AND date < CURRENT_DATE()
),

regression AS (
    SELECT
        REGR_SLOPE(total_cost, day_offset) AS total_slope,
        REGR_INTERCEPT(total_cost, day_offset) AS total_intercept,
        STDDEV(total_cost) AS total_stddev,
        REGR_SLOPE(compute_cost, day_offset) AS compute_slope,
        REGR_INTERCEPT(compute_cost, day_offset) AS compute_intercept,
        STDDEV(compute_cost) AS compute_stddev,
        REGR_SLOPE(storage_cost, day_offset) AS storage_slope,
        REGR_INTERCEPT(storage_cost, day_offset) AS storage_intercept,
        STDDEV(storage_cost) AS storage_stddev,
        REGR_SLOPE(serverless_cost, day_offset) AS serverless_slope,
        REGR_INTERCEPT(serverless_cost, day_offset) AS serverless_intercept,
        STDDEV(serverless_cost) AS serverless_stddev,
        MAX(day_offset) AS max_offset
    FROM historical
),

future_days AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS future_day_num
    FROM TABLE(GENERATOR(ROWCOUNT => 90))
)

SELECT
    DATEADD('day', fd.future_day_num, CURRENT_DATE()) AS forecast_date,
    fd.future_day_num + r.max_offset + 1 AS projected_offset,
    GREATEST(0, r.total_intercept + r.total_slope * (fd.future_day_num + r.max_offset + 1)) AS predicted_total_cost,
    GREATEST(0, r.compute_intercept + r.compute_slope * (fd.future_day_num + r.max_offset + 1)) AS predicted_compute_cost,
    GREATEST(0, r.storage_intercept + r.storage_slope * (fd.future_day_num + r.max_offset + 1)) AS predicted_storage_cost,
    GREATEST(0, r.serverless_intercept + r.serverless_slope * (fd.future_day_num + r.max_offset + 1)) AS predicted_serverless_cost,
    -- 95% confidence interval (1.96 * stddev)
    GREATEST(0, r.total_intercept + r.total_slope * (fd.future_day_num + r.max_offset + 1) - 1.96 * r.total_stddev) AS ci_lower,
    r.total_intercept + r.total_slope * (fd.future_day_num + r.max_offset + 1) + 1.96 * r.total_stddev AS ci_upper,
    r.total_slope AS daily_trend,
    r.total_stddev AS forecast_stddev,
    CURRENT_TIMESTAMP() AS generated_at
FROM future_days fd
CROSS JOIN regression r
