-- Alert: Credit budget thresholds — flags at 80% and 100% of monthly budget.
WITH config AS (
    SELECT alert_id, threshold_value
    FROM {{ ref('alert_configuration') }}
    WHERE alert_id IN ('credit_budget_80pct', 'credit_budget_100pct')
      AND enabled = TRUE
),

mtd_credits AS (
    SELECT SUM(total_credits) AS mtd_credits
    FROM {{ ref('int__warehouse_daily_credits') }}
    WHERE date >= DATE_TRUNC('month', CURRENT_DATE())
),

budget AS (
    SELECT budget_credits
    FROM {{ ref('monthly_budget') }}
    WHERE budget_month = DATE_TRUNC('month', CURRENT_DATE())
    LIMIT 1
)

SELECT
    c.alert_id,
    CURRENT_TIMESTAMP() AS detected_at,
    'account' AS resource_key,
    (m.mtd_credits / b.budget_credits) * 100 AS metric_value,
    c.threshold_value,
    OBJECT_CONSTRUCT(
        'mtd_credits', m.mtd_credits,
        'budget_credits', b.budget_credits,
        'pct_used', (m.mtd_credits / b.budget_credits) * 100
    )::VARCHAR AS details_json
FROM mtd_credits m
CROSS JOIN budget b
CROSS JOIN config c
WHERE (m.mtd_credits / NULLIF(b.budget_credits, 0)) * 100 >= c.threshold_value
