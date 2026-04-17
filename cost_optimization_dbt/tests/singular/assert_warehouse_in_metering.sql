-- Validates that warehouses in recommendations exist in metering data.
-- LEFT JOIN: unknown warehouse should not generate a recommendation.
SELECT r.warehouse_name
FROM {{ ref('pub__warehouse_recommendations') }} r
LEFT JOIN (
    SELECT DISTINCT warehouse_name
    FROM {{ ref('stg__warehouse_metering_history') }}
) m ON r.warehouse_name = m.warehouse_name
WHERE m.warehouse_name IS NULL
