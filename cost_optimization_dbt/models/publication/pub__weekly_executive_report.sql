-- Weekly Executive Report: single-row summary for push-based email delivery.
WITH this_week AS (
    SELECT
        SUM(total_cost) AS this_week_cost,
        SUM(compute_cost) AS this_week_compute,
        SUM(storage_cost) AS this_week_storage,
        SUM(serverless_cost) AS this_week_serverless
    FROM {{ ref('int__daily_cost_rollup') }}
    WHERE date >= DATE_TRUNC('week', CURRENT_DATE())
      AND date < CURRENT_DATE()
),

last_week AS (
    SELECT
        SUM(total_cost) AS last_week_cost
    FROM {{ ref('int__daily_cost_rollup') }}
    WHERE date >= DATEADD('week', -1, DATE_TRUNC('week', CURRENT_DATE()))
      AND date < DATE_TRUNC('week', CURRENT_DATE())
),

top_cost_drivers AS (
    SELECT
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'warehouse_name', warehouse_name,
                'total_cost_usd', total_cost_usd
            )
        ) WITHIN GROUP (ORDER BY total_cost_usd DESC) AS top_warehouses_json
    FROM (
        SELECT warehouse_name, total_cost_usd
        FROM {{ ref('pub__cost_by_warehouse') }}
        ORDER BY total_cost_usd DESC
        LIMIT 3
    )
),

top_savings AS (
    SELECT
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'recommendation_id', recommendation_id,
                'category', category,
                'target_object', target_object,
                'estimated_monthly_savings_usd', estimated_monthly_savings_usd
            )
        ) WITHIN GROUP (ORDER BY estimated_monthly_savings_usd DESC) AS top_savings_json
    FROM (
        SELECT recommendation_id, category, target_object, estimated_monthly_savings_usd
        FROM {{ ref('pub__all_recommendations') }}
        ORDER BY estimated_monthly_savings_usd DESC
        LIMIT 3
    )
),

active_alerts AS (
    SELECT COUNT(*) AS alert_count
    FROM {{ ref('int__alert_state_tracker') }}
    WHERE is_new_episode = TRUE
      AND detected_at >= DATEADD('day', -7, CURRENT_DATE())
),

unrealised_savings AS (
    SELECT COALESCE(SUM(estimated_monthly_savings_usd), 0) AS total_unrealised_savings
    FROM {{ ref('pub__all_recommendations') }}
)

SELECT
    CURRENT_DATE() AS report_date,
    DATE_TRUNC('week', CURRENT_DATE())::DATE AS week_start,
    COALESCE(tw.this_week_cost, 0) AS this_week_cost,
    COALESCE(lw.last_week_cost, 0) AS last_week_cost,
    CASE
        WHEN COALESCE(lw.last_week_cost, 0) > 0
        THEN (COALESCE(tw.this_week_cost, 0) - lw.last_week_cost) / lw.last_week_cost * 100
        ELSE 0
    END AS wow_change_pct,
    COALESCE(tw.this_week_compute, 0) AS this_week_compute,
    COALESCE(tw.this_week_storage, 0) AS this_week_storage,
    COALESCE(tw.this_week_serverless, 0) AS this_week_serverless,
    tcd.top_warehouses_json,
    ts.top_savings_json,
    aa.alert_count AS active_alert_count,
    us.total_unrealised_savings,
    CURRENT_TIMESTAMP() AS generated_at
FROM this_week tw
CROSS JOIN last_week lw
CROSS JOIN top_cost_drivers tcd
CROSS JOIN top_savings ts
CROSS JOIN active_alerts aa
CROSS JOIN unrealised_savings us
