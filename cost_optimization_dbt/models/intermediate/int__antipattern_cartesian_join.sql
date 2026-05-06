-- Anti-pattern: Cartesian joins — queries producing >10x more rows than scanned.
SELECT
    q.query_id,
    q.user_name,
    q.warehouse_name,
    'CARTESIAN_JOIN' AS antipattern_type,
    'P1' AS severity,
    qc.estimated_cost_usd AS estimated_waste_usd,
    'Produced ' || TO_VARCHAR(q.rows_produced, '999,999,999,999') || ' rows — likely a cartesian/cross join. '
        || 'FIX: (1) Check all JOIN conditions — a missing or incorrect ON clause causes row explosion. '
        || '(2) Ensure every JOIN has a proper equality condition on the correct key columns. '
        || '(3) Use INNER JOIN instead of CROSS JOIN unless intentional. '
        || '(4) Add a WHERE clause to filter before joining to reduce intermediate rows.'
    AS recommendation,
    q.query_text AS sample_query_text,
    q.rows_produced,
    q.bytes_scanned,
    q.end_time
FROM {{ ref('stg__query_history') }} q
LEFT JOIN {{ ref('int__query_cost_attribution') }} qc ON q.query_id = qc.query_id
WHERE q.execution_status = 'SUCCESS'
  AND q.query_type = 'SELECT'
  AND q.rows_produced > 0
  AND q.bytes_scanned > 0
  -- Heuristic: rows produced much larger than bytes scanned suggests cartesian
  AND q.rows_produced > 10 * (q.bytes_scanned / 100)  -- rough row estimate
  AND q.rows_produced > 1000000  -- At least 1M rows to avoid false positives
  -- Exclude Snowflake system database queries (not user-optimizable)
  AND COALESCE(q.database_name, '') != 'SNOWFLAKE'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ORGANIZATION_USAGE%'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ACCOUNT_USAGE%'
