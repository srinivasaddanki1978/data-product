-- Anti-pattern: Long-running queries — queries exceeding 5 minutes execution time.
-- These indicate potential optimization opportunities (missing filters, bad joins, etc.).
SELECT
    q.query_id,
    q.user_name,
    q.warehouse_name,
    'LONG_RUNNING' AS antipattern_type,
    CASE
        WHEN q.total_elapsed_time_ms > 30 * 60000 THEN 'P1'  -- >30 min = critical
        WHEN q.total_elapsed_time_ms > 15 * 60000 THEN 'P2'  -- >15 min = high
        ELSE 'P3'                                              -- >5 min = medium
    END AS severity,
    qc.estimated_cost_usd AS estimated_waste_usd,
    'Ran for ' || ROUND(q.total_elapsed_time_ms / 60000.0, 1) || ' minutes'
        || CASE WHEN q.queued_overload_time_ms > 10000
            THEN ' (incl. ' || ROUND(q.queued_overload_time_ms / 1000.0, 0) || 's queued)'
            ELSE '' END
        || '. Scanned ' || ROUND(q.bytes_scanned / 1048576.0, 1) || ' MB. '
        || 'FIX: (1) Add WHERE filters to reduce data scanned. '
        || '(2) Check for missing clustering keys on large tables. '
        || '(3) Break into smaller queries or use materialized intermediate tables. '
        || '(4) Consider upgrading warehouse size if query is I/O bound.'
    AS recommendation,
    LEFT(q.query_text, 500) AS sample_query_text,
    q.end_time
FROM {{ ref('stg__query_history') }} q
LEFT JOIN {{ ref('int__query_cost_attribution') }} qc USING (query_id)
WHERE q.execution_status = 'SUCCESS'
  AND q.total_elapsed_time_ms > 5 * 60000  -- longer than 5 minutes
  AND q.warehouse_name IS NOT NULL
  AND q.end_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  -- Exclude system/internal queries
  AND q.user_name != 'SYSTEM'
  AND COALESCE(q.database_name, '') != 'SNOWFLAKE'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ORGANIZATION_USAGE%'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ACCOUNT_USAGE%'
  AND LOWER(q.query_text) NOT LIKE 'execute streamlit%'
  AND LOWER(q.query_text) NOT LIKE 'execute dbt%'
  AND LOWER(q.query_text) NOT LIKE 'create or replace%'
  AND LOWER(q.query_text) NOT LIKE 'call%'
