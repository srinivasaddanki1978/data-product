-- Anti-pattern: SELECT * — queries using SELECT * which scan unnecessary columns.
SELECT
    q.query_id,
    q.user_name,
    q.warehouse_name,
    'SELECT_STAR' AS antipattern_type,
    'P3' AS severity,
    qc.estimated_cost_usd AS estimated_waste_usd,
    'Scanned ' || ROUND(q.bytes_scanned / 1048576.0, 1) || ' MB using SELECT *. '
        || 'FIX: (1) Replace SELECT * with only the columns you need. '
        || '(2) This reduces I/O, memory usage, and network transfer. '
        || '(3) In Snowflake columnar storage, fewer columns = proportionally less data scanned.'
    AS recommendation,
    LEFT(q.query_text, 8000) AS sample_query_text,
    q.bytes_scanned,
    q.end_time
FROM {{ ref('stg__query_history') }} q
LEFT JOIN {{ ref('int__query_cost_attribution') }} qc ON q.query_id = qc.query_id
WHERE q.execution_status = 'SUCCESS'
  AND q.warehouse_name IS NOT NULL
  AND REGEXP_LIKE(UPPER(TRIM(q.query_text)), '^SELECT\\s+\\*\\s+FROM.*', 'i')
  AND q.bytes_scanned > 1048576  -- Only flag if scanning > 1MB
  -- Exclude Snowflake system database queries (not user-optimizable)
  AND COALESCE(q.database_name, '') != 'SNOWFLAKE'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ORGANIZATION_USAGE%'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ACCOUNT_USAGE%'
