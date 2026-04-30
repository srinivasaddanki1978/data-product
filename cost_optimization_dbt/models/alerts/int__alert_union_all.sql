-- Union of all alert detection models into a standardized schema,
-- with suppression logic for targeted rules and bank holidays.
WITH all_alerts AS (
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
),

suppressions AS (
    SELECT *
    FROM {{ ref('alert_suppressions') }}
),

holidays AS (
    SELECT *
    FROM {{ ref('bank_holidays') }}
),

config AS (
    SELECT alert_id, suppress_on_holidays
    FROM {{ ref('alert_configuration') }}
),

evaluated AS (
    SELECT
        a.alert_id,
        a.detected_at,
        a.resource_key,
        a.metric_value,
        a.threshold_value,
        a.details_json,
        CASE
            WHEN s.alert_id IS NOT NULL THEN TRUE
            WHEN h.holiday_date IS NOT NULL AND COALESCE(c.suppress_on_holidays, FALSE) = TRUE THEN TRUE
            ELSE FALSE
        END AS is_suppressed,
        CASE
            WHEN s.alert_id IS NOT NULL THEN 'Suppression rule: ' || s.suppression_reason
            WHEN h.holiday_date IS NOT NULL AND COALESCE(c.suppress_on_holidays, FALSE) = TRUE THEN 'Bank holiday: ' || h.description
            ELSE NULL
        END AS suppression_reason
    FROM all_alerts a
    LEFT JOIN suppressions s
        ON a.alert_id = s.alert_id
        AND a.resource_key = s.resource_value
        AND a.detected_at::DATE BETWEEN s.start_date AND s.end_date
    LEFT JOIN holidays h
        ON a.detected_at::DATE = h.holiday_date
    LEFT JOIN config c
        ON a.alert_id = c.alert_id
)

SELECT
    alert_id,
    detected_at,
    resource_key,
    metric_value,
    threshold_value,
    details_json,
    is_suppressed,
    suppression_reason
FROM evaluated
WHERE is_suppressed = FALSE
