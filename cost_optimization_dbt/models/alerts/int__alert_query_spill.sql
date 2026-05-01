-- Alert: Heavy query spill — queries spilling >1GB to remote storage.
WITH config AS (
    SELECT threshold_value
    FROM {{ ref('alert_configuration') }}
    WHERE alert_id = 'query_spill_heavy' AND enabled = TRUE
)

SELECT
    'query_spill_heavy' AS alert_id,
    q.end_time AS detected_at,
    q.warehouse_name || '/' || q.query_id AS resource_key,
    q.bytes_spilled_to_remote_storage AS metric_value,
    c.threshold_value,
    OBJECT_CONSTRUCT(
        'query_id', q.query_id,
        'user_name', q.user_name,
        'warehouse_name', q.warehouse_name,
        'bytes_spilled_remote', q.bytes_spilled_to_remote_storage,
        'bytes_spilled_local', q.bytes_spilled_to_local_storage,
        'execution_time_s', q.execution_time_ms / 1000.0
    )::VARCHAR AS details_json
FROM {{ ref('stg__query_history') }} q
CROSS JOIN config c
WHERE q.bytes_spilled_to_remote_storage > c.threshold_value
  AND q.end_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  AND q.query_text IS NOT NULL
  AND LOWER(q.query_text) NOT LIKE 'execute streamlit%'
  AND LOWER(q.query_text) NOT LIKE 'execute dbt%'
  AND LOWER(q.query_text) NOT LIKE 'create or replace%'
  AND LOWER(q.query_text) NOT LIKE 'alter%'
  AND LOWER(q.query_text) NOT LIKE 'grant%'
  AND LOWER(q.query_text) NOT LIKE 'call%'
