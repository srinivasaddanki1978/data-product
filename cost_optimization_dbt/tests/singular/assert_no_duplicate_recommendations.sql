-- Validates no duplicate recommendation IDs in the unified recommendations.
SELECT recommendation_id, COUNT(*) AS cnt
FROM {{ ref('pub__all_recommendations') }}
GROUP BY recommendation_id
HAVING COUNT(*) > 1
