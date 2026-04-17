WITH load AS (
    SELECT
        warehouse_name,
        start_time,
        end_time,
        avg_running,
        avg_queued_load,
        avg_queued_provisioning,
        avg_blocked,
        avg_running + avg_queued_load + avg_queued_provisioning + avg_blocked AS total_load
    FROM {{ ref('stg__warehouse_load_history') }}
)

SELECT
    warehouse_name,
    start_time AS interval_start,
    end_time AS interval_end,
    avg_running,
    avg_queued_load,
    avg_blocked,
    {{ safe_divide('avg_running', 'total_load') }} AS utilisation_pct,
    {{ safe_divide('avg_queued_load', 'total_load') }} AS queue_ratio,
    {{ safe_divide('avg_blocked', 'total_load') }} AS blocked_ratio,
    CASE WHEN avg_running = 0 AND total_load = 0 THEN TRUE ELSE FALSE END AS is_idle
FROM load
