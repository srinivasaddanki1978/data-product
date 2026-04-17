{{
  config(
    materialized='table',
    schema='PUBLICATION'
  )
}}

-- Generates Microsoft Teams Adaptive Card JSON payloads for new alert episodes.
-- Only creates payloads for alerts with teams_enabled=TRUE and enabled=TRUE.
WITH new_episodes AS (
    SELECT *
    FROM {{ ref('int__alert_state_tracker') }}
    WHERE is_new_episode = TRUE
),

alert_config AS (
    SELECT *
    FROM {{ ref('alert_configuration') }}
    WHERE teams_enabled = TRUE
      AND enabled = TRUE
)

SELECT
    ne.alert_episode_key,
    ne.alert_id,
    ne.detected_at,
    ne.resource_key,
    ne.metric_value,
    ne.threshold_value,
    ne.details_json,
    ne.episode_number,
    ac.alert_name,
    ac.description AS alert_description,
    ac.severity,
    ac.teams_channel,
    ac.resolver_team,
    -- Adaptive Card JSON payload
    OBJECT_CONSTRUCT(
        'type', 'message',
        'attachments', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'contentType', 'application/vnd.microsoft.card.adaptive',
                'content', OBJECT_CONSTRUCT(
                    '$schema', 'http://adaptivecards.io/schemas/adaptive-card.json',
                    'type', 'AdaptiveCard',
                    'version', '1.4',
                    'body', ARRAY_CONSTRUCT(
                        OBJECT_CONSTRUCT(
                            'type', 'TextBlock',
                            'size', 'Large',
                            'weight', 'Bolder',
                            'text', ac.severity || ' — ' || ac.alert_name,
                            'color', CASE ac.severity
                                WHEN 'P0' THEN 'Attention'
                                WHEN 'P1' THEN 'Warning'
                                ELSE 'Default'
                            END
                        ),
                        OBJECT_CONSTRUCT(
                            'type', 'TextBlock',
                            'text', ac.description,
                            'wrap', TRUE
                        ),
                        OBJECT_CONSTRUCT(
                            'type', 'FactSet',
                            'facts', ARRAY_CONSTRUCT(
                                OBJECT_CONSTRUCT('title', 'Resource', 'value', ne.resource_key),
                                OBJECT_CONSTRUCT('title', 'Metric Value', 'value', ne.metric_value::VARCHAR),
                                OBJECT_CONSTRUCT('title', 'Threshold', 'value', ne.threshold_value::VARCHAR),
                                OBJECT_CONSTRUCT('title', 'Detected At', 'value', ne.detected_at::VARCHAR),
                                OBJECT_CONSTRUCT('title', 'Episode', 'value', ne.episode_number::VARCHAR),
                                OBJECT_CONSTRUCT('title', 'Resolver Team', 'value', ac.resolver_team)
                            )
                        )
                    )
                )
            )
        )
    )::VARCHAR AS teams_payload_json,
    NULL::TIMESTAMP_NTZ AS sent_at,
    NULL::INT AS api_response_code,
    NULL::BOOLEAN AS send_success
FROM new_episodes ne
INNER JOIN alert_config ac ON ne.alert_id = ac.alert_id
