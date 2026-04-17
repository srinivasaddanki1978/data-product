-- Per-warehouse sizing analysis using LIVE metadata from QUERY_HISTORY.
-- The warehouse_size column comes directly from each query's execution context,
-- so we always have the actual size at the time the query ran.
WITH query_stats AS (
    SELECT
        warehouse_name,
        -- Get the most recent warehouse_size for this warehouse
        -- (handles mid-day resizes by using the most common size)
        COALESCE(
            MODE(warehouse_size),
            MAX(warehouse_size)
        ) AS current_size,
        COUNT(*) AS total_queries,
        AVG(execution_time_ms) AS avg_exec_ms,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY execution_time_ms) AS p50_exec_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time_ms) AS p95_exec_ms,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY execution_time_ms) AS p99_exec_ms,
        AVG(queued_overload_time_ms) AS avg_queue_ms,
        SUM(CASE WHEN bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0) * 100 AS spill_rate_pct,
        AVG(bytes_scanned) AS avg_bytes_scanned,
        COUNT(DISTINCT user_name) AS unique_users,
        COUNT(DISTINCT role_name) AS unique_roles
    FROM {{ ref('stg__query_history') }}
    WHERE execution_status = 'SUCCESS'
      AND warehouse_name IS NOT NULL
    GROUP BY 1
),

utilisation_by_hour AS (
    SELECT
        warehouse_name,
        EXTRACT(HOUR FROM interval_start) AS hour_of_day,
        AVG(utilisation_pct) AS hourly_utilisation
    FROM {{ ref('int__warehouse_utilisation') }}
    GROUP BY 1, 2
),

peak_utilisation AS (
    SELECT
        warehouse_name,
        MAX(hourly_utilisation) AS peak_hour_utilisation
    FROM utilisation_by_hour
    GROUP BY 1
)

SELECT
    qs.warehouse_name,
    qs.current_size,
    qs.total_queries,
    qs.unique_users,
    qs.unique_roles,
    qs.avg_exec_ms,
    qs.p50_exec_ms,
    qs.p95_exec_ms,
    qs.p99_exec_ms,
    qs.avg_queue_ms,
    qs.spill_rate_pct,
    qs.avg_bytes_scanned,
    COALESCE(pu.peak_hour_utilisation, 0) AS peak_hour_utilisation
FROM query_stats qs
LEFT JOIN peak_utilisation pu ON qs.warehouse_name = pu.warehouse_name
