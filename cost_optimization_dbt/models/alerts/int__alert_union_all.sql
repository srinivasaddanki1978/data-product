-- Union of all alert detection models into a standardized schema.
SELECT alert_id, detected_at, resource_key, metric_value, threshold_value, details_json
FROM {{ ref('int__alert_cost_daily_spike') }}

UNION ALL

SELECT alert_id, detected_at, resource_key, metric_value, threshold_value, details_json
FROM {{ ref('int__alert_warehouse_idle') }}

UNION ALL

SELECT alert_id, detected_at, resource_key, metric_value, threshold_value, details_json
FROM {{ ref('int__alert_credit_budget') }}

UNION ALL

SELECT alert_id, detected_at, resource_key, metric_value, threshold_value, details_json
FROM {{ ref('int__alert_query_spill') }}

UNION ALL

SELECT alert_id, detected_at, resource_key, metric_value, threshold_value, details_json
FROM {{ ref('int__alert_storage_growth') }}

UNION ALL

SELECT alert_id, detected_at, resource_key, metric_value, threshold_value, details_json
FROM {{ ref('int__alert_repeated_expensive') }}
