WITH warehouse_credits AS (
    SELECT
        warehouse_name,
        SUM(total_credits) AS total_credits,
        SUM(estimated_cost_usd) AS total_cost_usd,
        SUM(credits_compute) AS compute_credits,
        SUM(credits_cloud) AS cloud_credits,
        MIN(date) AS first_date,
        MAX(date) AS last_date
    FROM {{ ref('int__warehouse_daily_credits') }}
    GROUP BY 1
),

utilisation AS (
    SELECT
        warehouse_name,
        AVG(utilisation_pct) AS avg_utilisation_pct,
        SUM(CASE WHEN is_idle THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS idle_pct,
        AVG(queue_ratio) * 100 AS avg_queue_pct
    FROM {{ ref('int__warehouse_utilisation') }}
    GROUP BY 1
),

query_stats AS (
    SELECT
        warehouse_name,
        COUNT(*) AS total_queries,
        AVG(estimated_cost_usd) AS avg_query_cost_usd,
        MEDIAN(execution_time_s) AS median_execution_time_s
    FROM {{ ref('int__query_cost_attribution') }}
    GROUP BY 1
)

SELECT
    wc.warehouse_name,
    wc.total_credits,
    wc.total_cost_usd,
    wc.compute_credits,
    wc.cloud_credits,
    COALESCE(u.avg_utilisation_pct, 0) AS avg_utilisation_pct,
    COALESCE(u.idle_pct, 0) AS idle_pct,
    COALESCE(u.avg_queue_pct, 0) AS avg_queue_pct,
    COALESCE(qs.total_queries, 0) AS total_queries,
    COALESCE(qs.avg_query_cost_usd, 0) AS avg_query_cost_usd,
    COALESCE(qs.median_execution_time_s, 0) AS median_execution_time_s,
    wc.first_date,
    wc.last_date
FROM warehouse_credits wc
LEFT JOIN utilisation u ON wc.warehouse_name = u.warehouse_name
LEFT JOIN query_stats qs ON wc.warehouse_name = qs.warehouse_name
