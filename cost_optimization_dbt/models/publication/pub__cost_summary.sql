WITH monthly AS (
    SELECT
        DATE_TRUNC('month', date)::DATE AS month,
        SUM(compute_cost) AS compute_cost,
        SUM(storage_cost) AS storage_cost,
        SUM(serverless_cost) AS serverless_cost,
        SUM(total_cost) AS total_cost
    FROM {{ ref('int__daily_cost_rollup') }}
    GROUP BY 1
),

with_change AS (
    SELECT
        month,
        compute_cost,
        storage_cost,
        serverless_cost,
        total_cost,
        LAG(total_cost) OVER (ORDER BY month) AS prev_month_total,
        {{ safe_divide(
            '(total_cost - LAG(total_cost) OVER (ORDER BY month))',
            'LAG(total_cost) OVER (ORDER BY month)'
        ) }} * 100 AS mom_change_pct
    FROM monthly
)

SELECT
    month,
    compute_cost,
    storage_cost,
    serverless_cost,
    total_cost,
    prev_month_total,
    COALESCE(mom_change_pct, 0) AS mom_change_pct,
    {{ safe_divide('compute_cost', 'total_cost') }} * 100 AS compute_pct,
    {{ safe_divide('storage_cost', 'total_cost') }} * 100 AS storage_pct,
    {{ safe_divide('serverless_cost', 'total_cost') }} * 100 AS serverless_pct
FROM with_change
ORDER BY month DESC
