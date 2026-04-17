-- Alert: Extended warehouse idle — flags warehouses running with 0 queries for >30 minutes.
WITH config AS (
    SELECT threshold_value
    FROM {{ ref('alert_configuration') }}
    WHERE alert_id = 'warehouse_idle_extended' AND enabled = TRUE
)

SELECT
    'warehouse_idle_extended' AS alert_id,
    ip.idle_start AS detected_at,
    ip.warehouse_name AS resource_key,
    ip.idle_duration_minutes AS metric_value,
    c.threshold_value,
    OBJECT_CONSTRUCT(
        'warehouse_name', ip.warehouse_name,
        'idle_start', ip.idle_start,
        'idle_end', ip.idle_end,
        'idle_minutes', ip.idle_duration_minutes,
        'wasted_credits', ip.wasted_credits,
        'wasted_cost_usd', ip.wasted_cost_usd
    )::VARCHAR AS details_json
FROM {{ ref('int__idle_warehouse_periods') }} ip
CROSS JOIN config c
WHERE ip.idle_duration_minutes > c.threshold_value
  AND ip.idle_start >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
