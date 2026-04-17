-- Validates that warehouse efficiency scores are between 0 and 100.
SELECT warehouse_name, efficiency_score
FROM {{ ref('pub__warehouse_efficiency') }}
WHERE efficiency_score < 0 OR efficiency_score > 100
