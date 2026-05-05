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
        THEN 'Spilled ' || ROUND(q.bytes_spilled_to_remote_storage / 1073741824.0, 2) || ' GB to REMOTE storage (slow). '
            || 'FIX: (1) Increase warehouse size by one level to add more memory. '
            || '(2) Break query into smaller steps using temp tables. '
            || '(3) Reduce data volume with tighter WHERE filters or pre-aggregation. '
            || '(4) Avoid wide SELECT * — select only needed columns.'
        ELSE 'Spilled ' || ROUND(q.bytes_spilled_to_local_storage / 1073741824.0, 2) || ' GB to LOCAL storage. '
            || 'FIX: (1) Optimize JOINs — ensure join keys are selective. '
            || '(2) Add WHERE filters to reduce intermediate result set size. '
            || '(3) If persistent, increase warehouse size by one level.'
    END AS recommendation,
    LEFT(q.query_text, 8000) AS sample_query_text,
    q.bytes_spilled_to_local_storage,
    q.bytes_spilled_to_remote_storage,
    q.end_time
FROM {{ ref('stg__query_history') }} q
LEFT JOIN {{ ref('int__query_cost_attribution') }} qc ON q.query_id = qc.query_id
WHERE q.execution_status = 'SUCCESS'
  AND (q.bytes_spilled_to_local_storage > 0 OR q.bytes_spilled_to_remote_storage > 0)
  -- Exclude Snowflake system database queries (not user-optimizable)
  AND COALESCE(q.database_name, '') != 'SNOWFLAKE'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ORGANIZATION_USAGE%'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ACCOUNT_USAGE%'
