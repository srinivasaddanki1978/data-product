-- Validates that the seasonality baseline has at least 30 days of data.
SELECT 1
FROM (
    SELECT COUNT(DISTINCT date) AS day_count
    FROM {{ ref('int__cost_seasonality_baseline') }}
) t
WHERE t.day_count < 30
