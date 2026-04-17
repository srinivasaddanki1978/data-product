-- Anti-pattern: Large sorts without LIMIT — ORDER BY on >100K rows with no LIMIT.
SELECT
    q.query_id,
    q.user_name,
    q.warehouse_name,
    'LARGE_SORT_NO_LIMIT' AS antipattern_type,
    'P3' AS severity,
    qc.estimated_cost_usd AS estimated_waste_usd,
    'Add LIMIT clause or remove ORDER BY if full sorted result is not needed' AS recommendation,
    LEFT(q.query_text, 500) AS sample_query_text,
    q.rows_produced,
    q.end_time
FROM {{ ref('stg__query_history') }} q
LEFT JOIN {{ ref('int__query_cost_attribution') }} qc ON q.query_id = qc.query_id
WHERE q.execution_status = 'SUCCESS'
  AND q.rows_produced > 100000
  AND UPPER(q.query_text) LIKE '%ORDER BY%'
  AND UPPER(q.query_text) NOT LIKE '%LIMIT%'
