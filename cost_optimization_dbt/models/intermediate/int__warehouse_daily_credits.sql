WITH metering AS (
    SELECT
        warehouse_name,
        start_time::DATE AS date,
        SUM(credits_used_compute) AS credits_compute,
        SUM(credits_used_cloud_services) AS credits_cloud,
        SUM(credits_used) AS total_credits
    FROM {{ ref('stg__warehouse_metering_history') }}
    GROUP BY 1, 2
),

pricing AS (
    SELECT credit_price_usd
    FROM {{ ref('credit_pricing') }}
    WHERE edition = 'ENTERPRISE'
      AND CURRENT_DATE() BETWEEN effective_from AND effective_to
    LIMIT 1
)

SELECT
    m.warehouse_name,
    m.date,
    m.credits_compute,
    m.credits_cloud,
    m.total_credits,
    m.total_credits * p.credit_price_usd AS estimated_cost_usd
FROM metering m
CROSS JOIN pricing p
