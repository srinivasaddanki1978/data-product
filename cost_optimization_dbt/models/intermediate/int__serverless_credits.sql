WITH pricing AS (
    SELECT credit_price_usd
    FROM {{ ref('credit_pricing') }}
    WHERE edition = 'ENTERPRISE'
      AND CURRENT_DATE() BETWEEN effective_from AND effective_to
    LIMIT 1
),

pipe AS (
    SELECT
        'SNOWPIPE' AS service_type,
        pipe_name AS object_name,
        start_time::DATE AS date,
        credits_used
    FROM {{ ref('stg__pipe_usage_history') }}
),

clustering AS (
    SELECT
        'AUTOMATIC_CLUSTERING' AS service_type,
        database_name || '.' || schema_name || '.' || table_name AS object_name,
        start_time::DATE AS date,
        credits_used
    FROM {{ ref('stg__automatic_clustering_history') }}
),

mv_refresh AS (
    SELECT
        'MATERIALIZED_VIEW' AS service_type,
        database_name || '.' || schema_name || '.' || table_name AS object_name,
        start_time::DATE AS date,
        credits_used
    FROM {{ ref('stg__materialized_view_refresh_history') }}
),

tasks AS (
    SELECT
        'SERVERLESS_TASK' AS service_type,
        database_name || '.' || schema_name || '.' || task_name AS object_name,
        start_time::DATE AS date,
        credits_used
    FROM {{ ref('stg__serverless_task_history') }}
),

search_opt AS (
    SELECT
        'SEARCH_OPTIMIZATION' AS service_type,
        database_name || '.' || schema_name || '.' || table_name AS object_name,
        start_time::DATE AS date,
        credits_used
    FROM {{ ref('stg__search_optimization_history') }}
),

all_serverless AS (
    SELECT * FROM pipe
    UNION ALL SELECT * FROM clustering
    UNION ALL SELECT * FROM mv_refresh
    UNION ALL SELECT * FROM tasks
    UNION ALL SELECT * FROM search_opt
)

SELECT
    s.service_type,
    s.object_name,
    s.date,
    SUM(s.credits_used) AS credits_used,
    SUM(s.credits_used) * p.credit_price_usd AS estimated_cost_usd
FROM all_serverless s
CROSS JOIN pricing p
GROUP BY 1, 2, 3, p.credit_price_usd
