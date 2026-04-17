-- Anti-pattern: SELECT * — queries using SELECT * which scan unnecessary columns.
SELECT
    q.query_id,
    q.user_name,
    q.warehouse_name,
    'SELECT_STAR' AS antipattern_type,
    'P3' AS severity,
    qc.estimated_cost_usd AS estimated_waste_usd,
    'Specify only needed columns to reduce I/O and improve performance' AS recommendation,
    LEFT(q.query_text, 500) AS sample_query_text,
    q.bytes_scanned,
    q.end_time
FROM {{ ref('stg__query_history') }} q
LEFT JOIN {{ ref('int__query_cost_attribution') }} qc ON q.query_id = qc.query_id
WHERE q.execution_status = 'SUCCESS'
  AND q.warehouse_name IS NOT NULL
  AND REGEXP_LIKE(UPPER(TRIM(q.query_text)), '^SELECT\\s+\\*\\s+FROM.*', 'i')
  AND q.bytes_scanned > 1048576  -- Only flag if scanning > 1MB
