-- Top 100 most impactful query optimization opportunities ranked by waste.
SELECT
    ROW_NUMBER() OVER (ORDER BY estimated_waste_usd DESC) AS optimization_rank,
    query_id,
    user_name,
    warehouse_name,
    antipattern_type,
    severity,
    estimated_waste_usd,
    recommendation,
    LEFT(sample_query_text, 8000) AS sample_query_text,
    end_time
FROM {{ ref('int__antipattern_union_all') }}
WHERE estimated_waste_usd > 0
ORDER BY estimated_waste_usd DESC
LIMIT 100
