-- Anti-pattern: Full table scans — queries scanning >80% of partitions on large tables.
SELECT
    query_id,
    user_name,
    warehouse_name,
    'FULL_TABLE_SCAN' AS antipattern_type,
    'P2' AS severity,
    estimated_cost_usd AS estimated_waste_usd,
    'Add clustering key or WHERE filter on partition column to reduce scan scope' AS recommendation,
    LEFT(query_text, 500) AS sample_query_text,
    partitions_scanned,
    partitions_total,
    {{ safe_divide('partitions_scanned', 'partitions_total') }} AS scan_ratio,
    bytes_scanned,
    end_time
FROM {{ ref('stg__query_history') }} q
LEFT JOIN {{ ref('int__query_cost_attribution') }} qc USING (query_id)
WHERE q.execution_status = 'SUCCESS'
  AND q.partitions_total > 100
  AND {{ safe_divide('q.partitions_scanned', 'q.partitions_total') }} > 0.8
  AND q.partitions_scanned > 0
