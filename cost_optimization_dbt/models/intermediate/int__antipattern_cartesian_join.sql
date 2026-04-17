-- Anti-pattern: Cartesian joins — queries producing >10x more rows than scanned.
SELECT
    q.query_id,
    q.user_name,
    q.warehouse_name,
    'CARTESIAN_JOIN' AS antipattern_type,
    'P1' AS severity,
    qc.estimated_cost_usd AS estimated_waste_usd,
    'Review join conditions — likely missing or incorrect ON clause' AS recommendation,
    LEFT(q.query_text, 500) AS sample_query_text,
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
