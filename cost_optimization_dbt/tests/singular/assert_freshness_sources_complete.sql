-- Validates that at least 5 data sources are monitored in the freshness model.
SELECT 1
FROM (
    SELECT COUNT(DISTINCT source_name) AS source_count
    FROM {{ ref('int__data_freshness_monitor') }}
) t
WHERE t.source_count < 5
