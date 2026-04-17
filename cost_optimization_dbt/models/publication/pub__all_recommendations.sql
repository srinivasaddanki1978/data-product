-- Master unified recommendations engine: warehouse + query + storage combined.
-- Ranked by priority_score = (savings x confidence) / effort.
WITH warehouse_recs AS (
    SELECT
        'WAREHOUSE' AS category,
        recommendation_type,
        warehouse_name AS target_object,
        recommendation_type || ': ' || current_state || ' -> ' || recommended_state AS description,
        0 AS current_monthly_cost_usd,
        estimated_monthly_savings_usd,
        effort,
        confidence,
        sql_to_apply AS action_sql
    FROM {{ ref('pub__warehouse_recommendations') }}
),

query_recs AS (
    SELECT
        'QUERY' AS category,
        antipattern_type AS recommendation_type,
        COALESCE(warehouse_name, 'N/A') || '/' || COALESCE(user_name, 'N/A') AS target_object,
        recommendation AS description,
        0 AS current_monthly_cost_usd,
        estimated_waste_usd AS estimated_monthly_savings_usd,
        CASE severity
            WHEN 'P1' THEN 'LOW'
            WHEN 'P2' THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS effort,
        'HIGH' AS confidence,
        '-- Query optimization: ' || LEFT(sample_query_text, 100) AS action_sql
    FROM {{ ref('pub__query_optimization_candidates') }}
    WHERE optimization_rank <= 50
),

storage_recs AS (
    SELECT
        'STORAGE' AS category,
        recommendation_type,
        target_object,
        description,
        current_cost_usd AS current_monthly_cost_usd,
        savings_if_applied_usd AS estimated_monthly_savings_usd,
        effort,
        confidence,
        action_sql
    FROM {{ ref('int__storage_recommendations') }}
),

all_recs AS (
    SELECT * FROM warehouse_recs
    UNION ALL
    SELECT * FROM query_recs
    UNION ALL
    SELECT * FROM storage_recs
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY
            estimated_monthly_savings_usd
                * CASE confidence WHEN 'HIGH' THEN 3 WHEN 'MEDIUM' THEN 2 ELSE 1 END
                / CASE effort WHEN 'LOW' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END
            DESC
        ) AS overall_rank,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY estimated_monthly_savings_usd DESC) AS category_rank,
        estimated_monthly_savings_usd
            * CASE confidence WHEN 'HIGH' THEN 3 WHEN 'MEDIUM' THEN 2 ELSE 1 END
            / CASE effort WHEN 'LOW' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END
        AS priority_score
    FROM all_recs
    WHERE estimated_monthly_savings_usd > 0
)

SELECT
    'REC-' || LPAD(overall_rank::VARCHAR, 4, '0') AS recommendation_id,
    category,
    recommendation_type,
    target_object,
    description,
    current_monthly_cost_usd,
    estimated_monthly_savings_usd,
    effort,
    confidence,
    priority_score,
    action_sql,
    category_rank,
    overall_rank
FROM ranked
ORDER BY overall_rank
