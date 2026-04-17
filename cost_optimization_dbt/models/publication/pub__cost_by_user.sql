WITH user_summary AS (
    SELECT
        user_name,
        SUM(total_queries) AS total_queries,
        SUM(total_cost_usd) AS total_cost_usd,
        AVG(avg_cost_per_query) AS avg_cost_per_query,
        SUM(total_bytes_scanned) AS total_bytes_scanned,
        AVG(avg_execution_time_s) AS avg_execution_time_s
    FROM {{ ref('int__user_cost_summary') }}
    GROUP BY 1
),

top_queries AS (
    SELECT
        user_name,
        query_id,
        query_type,
        estimated_cost_usd,
        execution_time_s,
        bytes_scanned,
        ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY estimated_cost_usd DESC) AS cost_rank
    FROM {{ ref('int__query_cost_attribution') }}
)

SELECT
    us.user_name,
    us.total_queries,
    us.total_cost_usd,
    us.avg_cost_per_query,
    us.total_bytes_scanned,
    us.avg_execution_time_s,
    RANK() OVER (ORDER BY us.total_cost_usd DESC) AS overall_cost_rank,
    -- Top 5 most expensive queries as array
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'query_id', tq.query_id,
            'query_type', tq.query_type,
            'cost_usd', tq.estimated_cost_usd,
            'execution_time_s', tq.execution_time_s
        )
    ) WITHIN GROUP (ORDER BY tq.cost_rank) AS top_5_expensive_queries
FROM user_summary us
LEFT JOIN top_queries tq
    ON us.user_name = tq.user_name
    AND tq.cost_rank <= 5
GROUP BY
    us.user_name,
    us.total_queries,
    us.total_cost_usd,
    us.avg_cost_per_query,
    us.total_bytes_scanned,
    us.avg_execution_time_s
