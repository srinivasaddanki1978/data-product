WITH user_costs AS (
    SELECT
        user_name,
        DATE_TRUNC('month', start_time)::DATE AS month,
        COUNT(*) AS total_queries,
        SUM(estimated_cost_usd) AS total_cost_usd,
        AVG(estimated_cost_usd) AS avg_cost_per_query,
        SUM(bytes_scanned) AS total_bytes_scanned,
        AVG(execution_time_s) AS avg_execution_time_s
    FROM {{ ref('int__query_cost_attribution') }}
    GROUP BY 1, 2
)

SELECT
    user_name,
    month,
    total_queries,
    total_cost_usd,
    avg_cost_per_query,
    total_bytes_scanned,
    avg_execution_time_s,
    RANK() OVER (PARTITION BY month ORDER BY total_cost_usd DESC) AS cost_rank
FROM user_costs
