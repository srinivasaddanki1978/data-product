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
        MAX(bytes_spilled_to_remote_storage) AS max_bytes_spilled_remote,
        MAX(user_name) AS sample_user_name,
        MAX(query_id) AS sample_query_id
    FROM {{ ref('int__query_cost_attribution') }}
    WHERE query_parameterized_hash IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

sample_queries AS (
    SELECT
        query_parameterized_hash,
        LEFT(query_text, 500) AS sample_query_text,
        ROW_NUMBER() OVER (PARTITION BY query_parameterized_hash ORDER BY start_time DESC) AS rn
    FROM {{ ref('int__query_cost_attribution') }}
    WHERE query_parameterized_hash IS NOT NULL
      AND query_text IS NOT NULL
)

SELECT
    g.query_parameterized_hash AS query_hash,
    g.query_type,
    g.warehouse_name,
    g.query_date,
    g.daily_execution_count AS execution_count,
    g.avg_execution_time_s,
    g.daily_total_cost_usd AS total_cost_usd,
    g.avg_cost_per_execution,
    g.avg_bytes_scanned,
    g.max_bytes_spilled_remote,
    g.sample_user_name,
    g.sample_query_id,
    sq.sample_query_text,
    CASE WHEN g.daily_execution_count > 5 THEN TRUE ELSE FALSE END AS is_repeated
FROM query_groups g
LEFT JOIN sample_queries sq
    ON g.query_parameterized_hash = sq.query_parameterized_hash
    AND sq.rn = 1
