{{
  config(
    materialized='incremental',
    unique_key='alert_episode_key',
    incremental_strategy='merge',
    schema='PUBLICATION'
  )
}}

-- Full audit trail of all alerts: fired, continuation, resolved, and sent status.
WITH tracker AS (
    SELECT *
    FROM {{ ref('int__alert_state_tracker') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY alert_episode_key ORDER BY detected_at DESC) = 1
),

config AS (
    SELECT *
    FROM {{ ref('alert_configuration') }}
),

payload AS (
    SELECT
        alert_episode_key,
        sent_at,
        api_response_code,
        send_success
    FROM {{ ref('pub__teams_alert_payload') }}
)

SELECT
    t.alert_episode_key,
    t.alert_id,
    c.alert_name,
    c.severity,
    t.detected_at,
    t.resource_key,
    t.metric_value,
    t.threshold_value,
    t.details_json,
    t.is_new_episode,
    t.is_continuation,
    t.episode_number,
    t.evaluated_at,
    c.teams_enabled,
    c.enabled AS alert_enabled,
    c.resolver_team,
    FALSE AS is_suppressed,
    NULL AS suppression_reason,
    p.sent_at AS teams_sent_at,
    p.api_response_code AS teams_response_code,
    p.send_success AS teams_send_success
FROM tracker t
LEFT JOIN config c ON t.alert_id = c.alert_id
LEFT JOIN payload p ON t.alert_episode_key = p.alert_episode_key
