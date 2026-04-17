-- Identify hours where queuing occurs, with frequency and impact metrics.
SELECT
    warehouse_name,
    EXTRACT(HOUR FROM interval_start) AS hour_of_day,
    COUNT(*) AS total_intervals,
    SUM(CASE WHEN avg_queued_load > 0 THEN 1 ELSE 0 END) AS intervals_with_queue,
    {{ safe_divide(
        'SUM(CASE WHEN avg_queued_load > 0 THEN 1 ELSE 0 END)::FLOAT',
        'COUNT(*)'
    ) }} * 100 AS queue_frequency_pct,
    AVG(CASE WHEN avg_queued_load > 0 THEN avg_queued_load ELSE NULL END) AS avg_queue_depth
FROM {{ ref('int__warehouse_utilisation') }}
GROUP BY 1, 2
ORDER BY warehouse_name, hour_of_day
