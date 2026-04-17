-- Alert: Repeated expensive query — same hash running >20 times/day costing >$1 each.
WITH config AS (
    SELECT threshold_value
    FROM {{ ref('alert_configuration') }}
    WHERE alert_id = 'repeated_expensive_query' AND enabled = TRUE
)

SELECT
    'repeated_expensive_query' AS alert_id,
    qp.query_date AS detected_at,
    qp.query_hash AS resource_key,
    qp.execution_count AS metric_value,
    c.threshold_value,
    OBJECT_CONSTRUCT(
        'query_hash', qp.query_hash,
        'query_type', qp.query_type,
        'warehouse_name', qp.warehouse_name,
        'daily_executions', qp.execution_count,
        'avg_cost_per_run', qp.avg_cost_per_execution,
        'total_daily_cost', qp.total_cost_usd
    )::VARCHAR AS details_json
FROM {{ ref('int__query_patterns') }} qp
CROSS JOIN config c
WHERE qp.execution_count > c.threshold_value
  AND qp.avg_cost_per_execution > 1.0
  AND qp.query_date >= DATEADD('day', -1, CURRENT_DATE())
