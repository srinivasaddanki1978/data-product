-- Team cost attribution using LIVE metadata from query execution.
-- Derives team from THREE sources (priority order):
--   1. query_tag structured field: 'team:{name};...' (set by workload generator)
--   2. role_name pattern matching (ADMIN→Platform, ANALYST→Analytics, etc.)
--   3. Default to 'Unassigned'
-- No seed files needed — everything comes from ACCOUNT_USAGE.
WITH query_costs AS (
    SELECT
        q.warehouse_name,
        q.user_name,
        q.role_name,
        q.query_tag,
        q.query_tag_team,
        q.start_time,
        q.execution_time_s,
        q.estimated_cost_usd,
        q.warehouse_size,
        -- Derive team: prefer query_tag, then role pattern, then warehouse pattern
        COALESCE(
            q.query_tag_team,
            CASE
                WHEN q.role_name ILIKE '%ADMIN%' OR q.role_name = 'ACCOUNTADMIN' THEN 'Platform'
                WHEN q.role_name ILIKE '%ANALYST%' THEN 'Analytics'
                WHEN q.role_name ILIKE '%ENGINEER%' THEN 'Engineering'
                WHEN q.role_name ILIKE '%TRANSFORM%' THEN 'Data Engineering'
                WHEN q.role_name = 'SYSADMIN' THEN 'Platform'
                WHEN q.role_name = 'PUBLIC' THEN 'Unassigned'
                WHEN q.role_name ILIKE '%TENSOR%' THEN 'AI/ML'
                ELSE NULL
            END,
            CASE
                WHEN q.warehouse_name ILIKE '%ANALYTICS%' THEN 'Analytics'
                WHEN q.warehouse_name ILIKE '%ETL%' THEN 'Data Engineering'
                WHEN q.warehouse_name ILIKE '%COST_OPT%' THEN 'Cost Optimization'
                WHEN q.warehouse_name ILIKE '%LEARNING%' THEN 'Training'
                ELSE 'Unassigned'
            END
        ) AS derived_team
    FROM {{ ref('int__query_cost_attribution') }} q
),

attributed AS (
    SELECT
        derived_team AS team_name,
        warehouse_name,
        role_name,
        DATE_TRUNC('month', start_time)::DATE AS month,
        SUM(estimated_cost_usd) AS compute_cost,
        COUNT(*) AS total_queries,
        SUM(execution_time_s) AS total_execution_time_s,
        COUNT(DISTINCT user_name) AS unique_users
    FROM query_costs
    GROUP BY 1, 2, 3, 4
),

monthly_totals AS (
    SELECT month, SUM(compute_cost) AS total_monthly_cost
    FROM attributed
    GROUP BY 1
)

SELECT
    a.team_name,
    a.warehouse_name,
    a.role_name,
    a.month,
    a.compute_cost,
    0 AS storage_cost,
    a.compute_cost AS total_cost,
    a.total_queries,
    a.unique_users,
    {{ safe_divide('a.compute_cost', 'mt.total_monthly_cost') }} * 100 AS pct_of_total
FROM attributed a
LEFT JOIN monthly_totals mt ON a.month = mt.month
