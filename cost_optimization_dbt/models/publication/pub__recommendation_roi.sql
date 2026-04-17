-- Publication layer for recommendation ROI with funnel aggregation.
WITH detail AS (
    SELECT *
    FROM {{ ref('int__recommendation_lifecycle') }}
),

funnel AS (
    SELECT
        COUNT(*) AS total_recommendations,
        SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_count,
        SUM(CASE WHEN status = 'ACCEPTED' THEN 1 ELSE 0 END) AS accepted_count,
        SUM(CASE WHEN status = 'IMPLEMENTED' THEN 1 ELSE 0 END) AS implemented_count,
        SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected_count,
        SUM(CASE WHEN status = 'DEFERRED' THEN 1 ELSE 0 END) AS deferred_count,
        SUM(estimated_monthly_savings_usd) AS total_estimated_savings,
        SUM(CASE WHEN status = 'IMPLEMENTED' THEN estimated_monthly_savings_usd ELSE 0 END) AS implemented_estimated_savings,
        SUM(COALESCE(actual_savings_usd, 0)) AS total_actual_savings
    FROM detail
)

SELECT
    d.recommendation_id,
    d.category,
    d.recommendation_type,
    d.target_object,
    d.description,
    d.estimated_monthly_savings_usd,
    d.actual_savings_usd,
    d.roi_pct,
    d.status,
    d.actioned_by,
    d.actioned_at,
    d.implemented_at,
    d.days_since_implementation,
    d.notes,
    d.effort,
    d.confidence,
    d.priority_score,
    f.total_recommendations,
    f.open_count,
    f.accepted_count,
    f.implemented_count,
    f.rejected_count,
    f.deferred_count,
    f.total_estimated_savings,
    f.implemented_estimated_savings,
    f.total_actual_savings
FROM detail d
CROSS JOIN funnel f
