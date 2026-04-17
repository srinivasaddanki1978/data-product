SELECT
    query_type,
    COUNT(*) AS query_count,
    SUM(estimated_cost_usd) AS total_cost_usd,
    AVG(estimated_cost_usd) AS avg_cost_per_query,
    AVG(execution_time_s) AS avg_duration_s,
    SUM(bytes_scanned) AS total_bytes_scanned,
    AVG(bytes_scanned) AS avg_bytes_scanned,
    {{ safe_divide(
        'SUM(estimated_cost_usd)',
        '(SELECT SUM(estimated_cost_usd) FROM ' ~ ref('int__query_cost_attribution') ~ ')'
    ) }} * 100 AS pct_of_total_cost
FROM {{ ref('int__query_cost_attribution') }}
GROUP BY query_type
ORDER BY total_cost_usd DESC
