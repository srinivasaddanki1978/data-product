{{
  config(
    materialized='incremental',
    unique_key='alert_episode_key',
    incremental_strategy='merge'
  )
}}

-- Alert state tracker with episode-based deduplication.
-- New episodes trigger notifications; continuations do not.
WITH current_alerts AS (
    SELECT
        alert_id,
        detected_at,
        resource_key,
        metric_value,
        threshold_value,
        details_json,
        alert_id || '||' || resource_key AS alert_resource_key
    FROM {{ ref('int__alert_union_all') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY alert_id, resource_key ORDER BY detected_at DESC) = 1
),

{% if is_incremental() %}
previous_state AS (
    SELECT
        alert_resource_key,
        MAX(episode_number) AS last_episode,
        MAX(detected_at) AS last_detected_at,
        -- An alert is still "active" if it was detected in the last cycle
        CASE
            WHEN MAX(detected_at) >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
            THEN TRUE ELSE FALSE
        END AS is_still_active
    FROM {{ this }}
    GROUP BY alert_resource_key
),
{% endif %}

enriched AS (
    SELECT
        ca.alert_id,
        ca.detected_at,
        ca.resource_key,
        ca.alert_resource_key,
        ca.metric_value,
        ca.threshold_value,
        ca.details_json,
        {% if is_incremental() %}
        CASE
            WHEN ps.alert_resource_key IS NULL THEN TRUE  -- Brand new alert
            WHEN ps.is_still_active = FALSE THEN TRUE     -- Was resolved, now re-firing
            ELSE FALSE                                      -- Continuation
        END AS is_new_episode,
        CASE
            WHEN ps.alert_resource_key IS NOT NULL AND ps.is_still_active = TRUE
            THEN TRUE ELSE FALSE
        END AS is_continuation,
        COALESCE(ps.last_episode, 0) +
            CASE
                WHEN ps.alert_resource_key IS NULL THEN 1
                WHEN ps.is_still_active = FALSE THEN 1
                ELSE 0
            END AS episode_number
        {% else %}
        TRUE AS is_new_episode,
        FALSE AS is_continuation,
        1 AS episode_number
        {% endif %}
    FROM current_alerts ca
    {% if is_incremental() %}
    LEFT JOIN previous_state ps ON ca.alert_resource_key = ps.alert_resource_key
    {% endif %}
)

SELECT
    alert_id || '_episode_' || episode_number || '_' || resource_key AS alert_episode_key,
    alert_id,
    detected_at,
    resource_key,
    alert_resource_key,
    metric_value,
    threshold_value,
    details_json,
    is_new_episode,
    is_continuation,
    episode_number,
    CURRENT_TIMESTAMP() AS evaluated_at
FROM enriched
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY alert_id || '_episode_' || episode_number || '_' || resource_key
    ORDER BY detected_at DESC
) = 1
