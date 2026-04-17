-- Alert: Storage growth anomaly — database storage grew >20% week-over-week.
WITH config AS (
    SELECT threshold_value
    FROM {{ ref('alert_configuration') }}
    WHERE alert_id = 'storage_growth_anomaly' AND enabled = TRUE
),

weekly_storage AS (
    SELECT
        database_name,
        usage_date,
        average_database_bytes,
        LAG(average_database_bytes, 7) OVER (
            PARTITION BY database_name ORDER BY usage_date
        ) AS prev_week_bytes
    FROM {{ ref('stg__database_storage_usage_history') }}
),

growth AS (
    SELECT
        database_name,
        usage_date,
        average_database_bytes,
        prev_week_bytes,
        {{ safe_divide(
            '(average_database_bytes - prev_week_bytes)',
            'prev_week_bytes'
        ) }} * 100 AS growth_pct
    FROM weekly_storage
    WHERE prev_week_bytes IS NOT NULL
      AND prev_week_bytes > 0
      AND usage_date >= DATEADD('day', -1, CURRENT_DATE())
)

SELECT
    'storage_growth_anomaly' AS alert_id,
    g.usage_date AS detected_at,
    g.database_name AS resource_key,
    g.growth_pct AS metric_value,
    c.threshold_value,
    OBJECT_CONSTRUCT(
        'database_name', g.database_name,
        'current_bytes', g.average_database_bytes,
        'prev_week_bytes', g.prev_week_bytes,
        'growth_pct', g.growth_pct
    )::VARCHAR AS details_json
FROM growth g
CROSS JOIN config c
WHERE g.growth_pct > c.threshold_value
