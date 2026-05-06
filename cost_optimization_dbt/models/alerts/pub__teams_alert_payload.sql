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
                        ),
                        -- Alert-type-specific details
                        OBJECT_CONSTRUCT(
                            'type', 'TextBlock',
                            'text', '**Details**',
                            'wrap', TRUE,
                            'separator', TRUE
                        ),
                        OBJECT_CONSTRUCT(
                            'type', 'FactSet',
                            'facts', CASE ne.alert_id
                                -- Cost spike: show daily cost, baseline, detection method
                                WHEN 'cost_daily_spike' THEN ARRAY_CONSTRUCT(
                                    OBJECT_CONSTRUCT('title', 'Daily Cost', 'value', '$' || COALESCE(ROUND(PARSE_JSON(ne.details_json):daily_cost::FLOAT, 2)::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Rolling 30d Avg', 'value', '$' || COALESCE(ROUND(PARSE_JSON(ne.details_json):rolling_30d_avg::FLOAT, 2)::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Detection Method', 'value', COALESCE(PARSE_JSON(ne.details_json):detection_method::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Z-Score', 'value', COALESCE(ROUND(PARSE_JSON(ne.details_json):z_score::FLOAT, 2)::VARCHAR, 'N/A'))
                                )
                                -- Warehouse idle: show idle duration, wasted cost
                                WHEN 'warehouse_idle_extended' THEN ARRAY_CONSTRUCT(
                                    OBJECT_CONSTRUCT('title', 'Warehouse', 'value', COALESCE(PARSE_JSON(ne.details_json):warehouse_name::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Idle Minutes', 'value', COALESCE(PARSE_JSON(ne.details_json):idle_minutes::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Wasted Credits', 'value', COALESCE(ROUND(PARSE_JSON(ne.details_json):wasted_credits::FLOAT, 2)::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Wasted Cost', 'value', '$' || COALESCE(ROUND(PARSE_JSON(ne.details_json):wasted_cost_usd::FLOAT, 2)::VARCHAR, 'N/A'))
                                )
                                -- Budget alerts: show MTD credits, budget, % used
                                WHEN 'credit_budget_80pct' THEN ARRAY_CONSTRUCT(
                                    OBJECT_CONSTRUCT('title', 'MTD Credits', 'value', COALESCE(ROUND(PARSE_JSON(ne.details_json):mtd_credits::FLOAT, 1)::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Budget Credits', 'value', COALESCE(PARSE_JSON(ne.details_json):budget_credits::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', '% Used', 'value', COALESCE(ROUND(PARSE_JSON(ne.details_json):pct_used::FLOAT, 1)::VARCHAR, 'N/A') || '%')
                                )
                                WHEN 'credit_budget_100pct' THEN ARRAY_CONSTRUCT(
                                    OBJECT_CONSTRUCT('title', 'MTD Credits', 'value', COALESCE(ROUND(PARSE_JSON(ne.details_json):mtd_credits::FLOAT, 1)::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Budget Credits', 'value', COALESCE(PARSE_JSON(ne.details_json):budget_credits::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', '% Used', 'value', COALESCE(ROUND(PARSE_JSON(ne.details_json):pct_used::FLOAT, 1)::VARCHAR, 'N/A') || '%')
                                )
                                -- Query spill: show query info + spill bytes
                                WHEN 'query_spill_heavy' THEN ARRAY_CONSTRUCT(
                                    OBJECT_CONSTRUCT('title', 'Query ID', 'value', COALESCE(PARSE_JSON(ne.details_json):query_id::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'User', 'value', COALESCE(PARSE_JSON(ne.details_json):user_name::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Warehouse', 'value', COALESCE(PARSE_JSON(ne.details_json):warehouse_name::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Spill Remote (GB)', 'value', COALESCE(ROUND(PARSE_JSON(ne.details_json):bytes_spilled_remote::FLOAT / 1073741824, 2)::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Execution Time (s)', 'value', COALESCE(ROUND(PARSE_JSON(ne.details_json):execution_time_s::FLOAT, 1)::VARCHAR, 'N/A'))
                                )
                                -- Repeated expensive: show hash, executions, cost
                                WHEN 'repeated_expensive_query' THEN ARRAY_CONSTRUCT(
                                    OBJECT_CONSTRUCT('title', 'Query Hash', 'value', COALESCE(PARSE_JSON(ne.details_json):query_hash::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Warehouse', 'value', COALESCE(PARSE_JSON(ne.details_json):warehouse_name::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Daily Executions', 'value', COALESCE(PARSE_JSON(ne.details_json):daily_executions::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Avg Cost/Run', 'value', '$' || COALESCE(ROUND(PARSE_JSON(ne.details_json):avg_cost_per_run::FLOAT, 2)::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Total Daily Cost', 'value', '$' || COALESCE(ROUND(PARSE_JSON(ne.details_json):total_daily_cost::FLOAT, 2)::VARCHAR, 'N/A'))
                                )
                                -- Long-running query: show query details + duration
                                WHEN 'query_long_running' THEN ARRAY_CONSTRUCT(
                                    OBJECT_CONSTRUCT('title', 'Query ID', 'value', COALESCE(PARSE_JSON(ne.details_json):query_id::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'User', 'value', COALESCE(PARSE_JSON(ne.details_json):user_name::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Warehouse', 'value', COALESCE(PARSE_JSON(ne.details_json):warehouse_name::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Duration (min)', 'value', COALESCE(PARSE_JSON(ne.details_json):duration_minutes::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Queued Time (s)', 'value', COALESCE(PARSE_JSON(ne.details_json):queued_time_s::VARCHAR, 'N/A')),
                                    OBJECT_CONSTRUCT('title', 'Query Preview', 'value', COALESCE(LEFT(PARSE_JSON(ne.details_json):query_preview::VARCHAR, 200), 'N/A'))
                                )
                                -- Fallback for any other alert type
                                ELSE ARRAY_CONSTRUCT(
                                    OBJECT_CONSTRUCT('title', 'Raw Details', 'value', LEFT(ne.details_json, 300))
                                )
                            END
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
