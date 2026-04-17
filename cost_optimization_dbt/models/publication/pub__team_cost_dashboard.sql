-- Team cost dashboard using LIVE derived team attribution.
-- Teams come from query_tag + role_name patterns, not seed files.
WITH team_monthly AS (
    SELECT
        team_name,
        month,
        SUM(compute_cost) AS monthly_cost,
        SUM(total_queries) AS total_queries,
        SUM(unique_users) AS unique_users,
        LAG(SUM(compute_cost)) OVER (PARTITION BY team_name ORDER BY month) AS prev_month_cost,
        {{ safe_divide(
            '(SUM(compute_cost) - LAG(SUM(compute_cost)) OVER (PARTITION BY team_name ORDER BY month))',
            'LAG(SUM(compute_cost)) OVER (PARTITION BY team_name ORDER BY month)'
        ) }} * 100 AS mom_change_pct
    FROM {{ ref('int__team_cost_attribution') }}
    GROUP BY 1, 2
),

monthly_totals AS (
    SELECT month, SUM(monthly_cost) AS total_cost
    FROM team_monthly
    GROUP BY 1
)

SELECT
    tmo.team_name,
    tmo.month,
    tmo.monthly_cost,
    tmo.total_queries,
    tmo.unique_users,
    {{ safe_divide('tmo.monthly_cost', 'mt.total_cost') }} * 100 AS pct_of_total,
    tmo.prev_month_cost,
    COALESCE(tmo.mom_change_pct, 0) AS mom_change_pct,
    RANK() OVER (PARTITION BY tmo.month ORDER BY tmo.monthly_cost DESC) AS cost_rank
FROM team_monthly tmo
LEFT JOIN monthly_totals mt ON tmo.month = mt.month
ORDER BY tmo.month DESC, tmo.monthly_cost DESC
