SELECT
    date,
    compute_cost,
    storage_cost,
    serverless_cost,
    total_cost,
    rolling_7d_avg,
    rolling_30d_avg,
    is_anomaly,
    DAYOFWEEK(date) AS day_of_week,
    EXTRACT(HOUR FROM date) AS hour_of_day,
    DATE_TRUNC('week', date)::DATE AS week_start
FROM {{ ref('int__daily_cost_rollup') }}
ORDER BY date
