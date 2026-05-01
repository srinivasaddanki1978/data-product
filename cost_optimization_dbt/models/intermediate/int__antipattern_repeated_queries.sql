-- Anti-pattern: Repeated expensive queries — same hash running >10 times/day with total cost >$5.
SELECT
    COALESCE(sample_query_id, query_hash) AS query_id,
    sample_user_name AS user_name,
    warehouse_name,
    'REPEATED_EXPENSIVE' AS antipattern_type,
    'P2' AS severity,
    total_cost_usd AS estimated_waste_usd,
    'Ran ' || execution_count || ' times (avg ' || ROUND(avg_execution_time_s, 1) || 's, avg cost $' || ROUND(avg_cost_per_execution, 4) || '/run). '
        || 'FIX: (1) Materialize results into a table and read from it instead of re-running. '
        || '(2) Use Snowflake result caching — ensure same role, warehouse, and no DDL changes between runs. '
        || '(3) Schedule the query once via a task and store output in a results table. '
        || '(4) If used by a dashboard, cache at the application layer with a TTL.'
    AS recommendation,
    COALESCE(sample_query_text, query_hash) AS sample_query_text,
    execution_count,
    avg_execution_time_s,
    avg_cost_per_execution,
    query_date AS end_time
FROM {{ ref('int__query_patterns') }}
WHERE execution_count > 10
  AND total_cost_usd > 5.0
