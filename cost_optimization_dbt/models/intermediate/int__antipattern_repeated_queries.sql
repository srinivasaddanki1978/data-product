-- Anti-pattern: Repeated expensive queries — same hash running >10 times/day with total cost >$5.
SELECT
    COALESCE(sample_query_id, query_hash) AS query_id,
    sample_user_name AS user_name,
    warehouse_name,
    'REPEATED_EXPENSIVE' AS antipattern_type,
    'P2' AS severity,
    total_cost_usd AS estimated_waste_usd,
    'Cache results in a table or leverage result caching (ensure same role/warehouse) — ran ' || execution_count || ' times' AS recommendation,
    COALESCE(sample_query_text, query_hash) AS sample_query_text,
    execution_count,
    avg_execution_time_s,
    avg_cost_per_execution,
    query_date AS end_time
FROM {{ ref('int__query_patterns') }}
WHERE execution_count > 10
  AND total_cost_usd > 5.0
