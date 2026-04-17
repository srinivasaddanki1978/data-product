-- Validates that all Snowflake editions have credit pricing defined.
-- Fails if any expected edition is missing from the credit_pricing seed.
WITH expected_editions AS (
    SELECT 'STANDARD' AS edition
    UNION ALL SELECT 'ENTERPRISE'
    UNION ALL SELECT 'BUSINESS_CRITICAL'
),

actual_editions AS (
    SELECT DISTINCT edition
    FROM {{ ref('credit_pricing') }}
    WHERE effective_to >= CURRENT_DATE()
)

SELECT e.edition
FROM expected_editions e
LEFT JOIN actual_editions a ON e.edition = a.edition
WHERE a.edition IS NULL
