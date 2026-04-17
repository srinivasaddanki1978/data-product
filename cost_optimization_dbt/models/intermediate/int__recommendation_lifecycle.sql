-- Recommendation lifecycle: tracks status and computes ROI for implemented recommendations.
WITH recommendations AS (
    SELECT
        recommendation_id,
        category,
        recommendation_type,
        target_object,
        description,
        current_monthly_cost_usd,
        estimated_monthly_savings_usd,
        effort,
        confidence,
        priority_score,
        action_sql
    FROM {{ ref('pub__all_recommendations') }}
),

actions AS (
    SELECT
        recommendation_id,
        status,
        actioned_by,
        actioned_at,
        implemented_at,
        notes
    FROM {{ ref('recommendation_actions') }}
),

-- For IMPLEMENTED warehouse recs, compute actual savings by comparing
-- the recommendation's estimated cost vs current 30-day cost
recent_warehouse_costs AS (
    SELECT
        warehouse_name,
        SUM(estimated_cost_usd) AS last_30d_cost_usd
    FROM {{ ref('int__warehouse_daily_credits') }}
    WHERE date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY warehouse_name
),

lifecycle AS (
    SELECT
        r.recommendation_id,
        r.category,
        r.recommendation_type,
        r.target_object,
        r.description,
        r.current_monthly_cost_usd,
        r.estimated_monthly_savings_usd,
        r.effort,
        r.confidence,
        r.priority_score,
        r.action_sql,
        COALESCE(a.status, 'OPEN') AS status,
        a.actioned_by,
        a.actioned_at,
        a.implemented_at,
        a.notes,
        -- Compute actual savings for IMPLEMENTED warehouse recommendations
        CASE
            WHEN COALESCE(a.status, 'OPEN') = 'IMPLEMENTED' AND r.category = 'WAREHOUSE'
            THEN GREATEST(0, r.current_monthly_cost_usd - COALESCE(wc.last_30d_cost_usd, r.current_monthly_cost_usd))
            ELSE NULL
        END AS actual_savings_usd,
        CASE
            WHEN COALESCE(a.status, 'OPEN') = 'IMPLEMENTED'
            THEN DATEDIFF('day', a.implemented_at, CURRENT_DATE())
            ELSE NULL
        END AS days_since_implementation
    FROM recommendations r
    LEFT JOIN actions a ON r.recommendation_id = a.recommendation_id
    LEFT JOIN recent_warehouse_costs wc
        ON r.category = 'WAREHOUSE'
        AND r.target_object = wc.warehouse_name
        AND COALESCE(a.status, 'OPEN') = 'IMPLEMENTED'
)

SELECT
    *,
    CASE
        WHEN actual_savings_usd IS NOT NULL AND estimated_monthly_savings_usd > 0
        THEN {{ safe_divide('actual_savings_usd', 'estimated_monthly_savings_usd') }} * 100
        ELSE NULL
    END AS roi_pct
FROM lifecycle
