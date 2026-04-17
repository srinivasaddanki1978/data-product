WITH compute_costs AS (
    SELECT
        date,
        SUM(estimated_cost_usd) AS compute_cost
    FROM {{ ref('int__warehouse_daily_credits') }}
    GROUP BY 1
),

storage_costs AS (
    SELECT
        usage_date AS date,
        -- $23/TB/month ≈ $0.767/TB/day
        (storage_bytes + stage_bytes + failsafe_bytes) / POWER(1024, 4) * 0.767 AS storage_cost
    FROM {{ ref('stg__storage_usage') }}
),

serverless_costs AS (
    SELECT
        date,
        SUM(estimated_cost_usd) AS serverless_cost
    FROM {{ ref('int__serverless_credits') }}
    GROUP BY 1
),

combined AS (
    SELECT
        COALESCE(c.date, s.date, sv.date) AS date,
        COALESCE(c.compute_cost, 0) AS compute_cost,
        COALESCE(s.storage_cost, 0) AS storage_cost,
        COALESCE(sv.serverless_cost, 0) AS serverless_cost,
        COALESCE(c.compute_cost, 0) + COALESCE(s.storage_cost, 0) + COALESCE(sv.serverless_cost, 0) AS total_cost
    FROM compute_costs c
    FULL OUTER JOIN storage_costs s ON c.date = s.date
    FULL OUTER JOIN serverless_costs sv ON COALESCE(c.date, s.date) = sv.date
)

SELECT
    date,
    compute_cost,
    storage_cost,
    serverless_cost,
    total_cost,
    AVG(total_cost) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_avg,
    AVG(total_cost) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_avg,
    CASE
        WHEN total_cost > 2 * AVG(total_cost) OVER (ORDER BY date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)
        THEN TRUE
        ELSE FALSE
    END AS is_anomaly
FROM combined
WHERE date IS NOT NULL
