-- Alert: Long-running queries — flags queries running longer than threshold (default 5 minutes).
WITH config AS (
    SELECT threshold_value
    FROM {{ ref('alert_configuration') }}
    WHERE alert_id = 'query_long_running' AND enabled = TRUE
)

SELECT
    'query_long_running' AS alert_id,
    q.end_time AS detected_at,
    q.warehouse_name || '/' || q.user_name AS resource_key,
    ROUND(q.total_elapsed_time_ms / 60000.0, 1) AS metric_value,
    c.threshold_value,
    OBJECT_CONSTRUCT(
        'query_id', q.query_id,
        'user_name', q.user_name,
        'warehouse_name', q.warehouse_name,
        'warehouse_size', q.warehouse_size,
        'duration_minutes', ROUND(q.total_elapsed_time_ms / 60000.0, 1),
        'queued_time_s', ROUND(q.queued_overload_time_ms / 1000.0, 1),
        'bytes_scanned_mb', ROUND(q.bytes_scanned / 1048576.0, 1),
        'rows_produced', q.rows_produced,
        'query_type', q.query_type,
        'query_preview', LEFT(q.query_text, 200)
    )::VARCHAR AS details_json
FROM {{ ref('stg__query_history') }} q
CROSS JOIN config c
WHERE q.execution_status = 'SUCCESS'
  AND q.total_elapsed_time_ms > c.threshold_value * 60000  -- threshold is in minutes
  AND q.end_time >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
  -- Exclude system/internal queries
  AND q.user_name != 'SYSTEM'
  AND COALESCE(q.database_name, '') != 'SNOWFLAKE'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ORGANIZATION_USAGE%'
  AND q.query_text NOT ILIKE '%SNOWFLAKE.ACCOUNT_USAGE%'
  AND LOWER(q.query_text) NOT LIKE 'execute streamlit%'
  AND LOWER(q.query_text) NOT LIKE 'create or replace%'
  AND LOWER(q.query_text) NOT LIKE 'call%'
