-- Dynamic extraction of warehouse-to-role-to-user mapping from ACCOUNT_USAGE.
-- Replaces static seed file with actual usage data.
WITH query_usage AS (
    SELECT DISTINCT
        warehouse_name,
        role_name,
        user_name
    FROM {{ ref('stg__query_history') }}
    WHERE warehouse_name IS NOT NULL
      AND role_name IS NOT NULL
      AND user_name IS NOT NULL
),

usage_stats AS (
    SELECT
        warehouse_name,
        role_name,
        user_name,
        COUNT(*) AS query_count,
        MIN(start_time) AS first_seen,
        MAX(end_time) AS last_seen
    FROM {{ ref('stg__query_history') }}
    WHERE warehouse_name IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    qu.warehouse_name,
    qu.role_name,
    qu.user_name,
    us.query_count,
    us.first_seen,
    us.last_seen,
    -- Derive team name from role_name pattern
    CASE
        WHEN qu.role_name ILIKE '%ADMIN%' THEN 'Platform'
        WHEN qu.role_name ILIKE '%ANALYST%' THEN 'Analytics'
        WHEN qu.role_name ILIKE '%ENGINEER%' THEN 'Engineering'
        WHEN qu.role_name ILIKE '%TRANSFORM%' THEN 'Data Engineering'
        WHEN qu.role_name ILIKE '%SYSADMIN%' THEN 'Platform'
        WHEN qu.role_name ILIKE '%ACCOUNTADMIN%' THEN 'Platform'
        WHEN qu.role_name = 'PUBLIC' THEN 'Unassigned'
        ELSE 'Unassigned'
    END AS derived_team_name
FROM query_usage qu
LEFT JOIN usage_stats us
    ON qu.warehouse_name = us.warehouse_name
    AND qu.role_name = us.role_name
    AND qu.user_name = us.user_name
