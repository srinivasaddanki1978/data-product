-- Analyze query volume by hour/day to detect off-peak scheduling opportunities.
WITH hourly_usage AS (
    SELECT
        warehouse_name,
        DAYOFWEEK(start_time) AS day_of_week,
        EXTRACT(HOUR FROM start_time) AS hour,
        COUNT(*) AS query_count,
        SUM(estimated_cost_usd) AS credits_cost
    FROM {{ ref('int__query_cost_attribution') }}
    GROUP BY 1, 2, 3
),

total_per_warehouse AS (
    SELECT
        warehouse_name,
        SUM(credits_cost) AS total_cost
    FROM hourly_usage
    GROUP BY 1
),

classified AS (
    SELECT
        hu.warehouse_name,
        hu.day_of_week,
        hu.hour,
        hu.query_count,
        hu.credits_cost,
        -- Off-peak: weekends or 8pm-6am weekdays
        CASE
            WHEN hu.day_of_week IN (0, 6) THEN TRUE  -- Weekend
            WHEN hu.hour < 6 OR hu.hour >= 20 THEN TRUE  -- Night hours
            ELSE FALSE
        END AS is_off_peak,
        hu.credits_cost AS schedulable_savings_usd
    FROM hourly_usage hu
)

SELECT
    warehouse_name,
    day_of_week,
    hour,
    query_count,
    credits_cost,
    is_off_peak,
    CASE WHEN is_off_peak AND query_count < 5 THEN credits_cost ELSE 0 END AS schedulable_savings_usd
FROM classified
