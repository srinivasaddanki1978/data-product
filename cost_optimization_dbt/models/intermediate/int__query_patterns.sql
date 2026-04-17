WITH query_groups AS (
    SELECT
        query_parameterized_hash,
        query_type,
        warehouse_name,
        start_time::DATE AS query_date,
        COUNT(*) AS daily_execution_count,
        AVG(execution_time_s) AS avg_execution_time_s,
        SUM(estimated_cost_usd) AS daily_total_cost_usd,
        AVG(estimated_cost_usd) AS avg_cost_per_execution,
        AVG(bytes_scanned) AS avg_bytes_scanned,
        MAX(bytes_spilled_to_remote_storage) AS max_bytes_spilled_remote
    FROM {{ ref('int__query_cost_attribution') }}
    WHERE query_parameterized_hash IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT
    query_parameterized_hash AS query_hash,
    query_type,
    warehouse_name,
    query_date,
    daily_execution_count AS execution_count,
    avg_execution_time_s,
    daily_total_cost_usd AS total_cost_usd,
    avg_cost_per_execution,
    avg_bytes_scanned,
    max_bytes_spilled_remote,
    CASE WHEN daily_execution_count > 5 THEN TRUE ELSE FALSE END AS is_repeated
FROM query_groups
