-- Anti-pattern: Query spill to storage — queries spilling to local or remote storage.
SELECT
    q.query_id,
    q.user_name,
    q.warehouse_name,
    'SPILL_TO_STORAGE' AS antipattern_type,
    CASE
        WHEN q.bytes_spilled_to_remote_storage > 0 THEN 'P1'
        ELSE 'P2'
    END AS severity,
    qc.estimated_cost_usd AS estimated_waste_usd,
    CASE
        WHEN q.bytes_spilled_to_remote_storage > 0
        THEN 'Increase warehouse size for this query, or optimize to reduce memory usage'
        ELSE 'Query spilling to local storage — consider warehouse resize for heavy workloads'
    END AS recommendation,
    LEFT(q.query_text, 500) AS sample_query_text,
    q.bytes_spilled_to_local_storage,
    q.bytes_spilled_to_remote_storage,
    q.end_time
FROM {{ ref('stg__query_history') }} q
LEFT JOIN {{ ref('int__query_cost_attribution') }} qc ON q.query_id = qc.query_id
WHERE q.execution_status = 'SUCCESS'
  AND (q.bytes_spilled_to_local_storage > 0 OR q.bytes_spilled_to_remote_storage > 0)
