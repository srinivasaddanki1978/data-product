WITH utilisation_stats AS (
    SELECT
        warehouse_name,
        AVG(utilisation_pct) AS avg_utilisation,
        SUM(CASE WHEN is_idle THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS idle_ratio,
        AVG(queue_ratio) AS avg_queue_ratio,
        AVG(blocked_ratio) AS avg_blocked_ratio
    FROM {{ ref('int__warehouse_utilisation') }}
    GROUP BY 1
),

spill_stats AS (
    SELECT
        warehouse_name,
        SUM(CASE WHEN bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0) AS spill_ratio
    FROM {{ ref('int__query_cost_attribution') }}
    GROUP BY 1
)

SELECT
    u.warehouse_name,
    COALESCE(u.avg_utilisation, 0) * 100 AS utilisation_pct,
    COALESCE(u.idle_ratio, 0) * 100 AS idle_pct,
    COALESCE(u.avg_queue_ratio, 0) * 100 AS queue_pct,
    COALESCE(u.avg_blocked_ratio, 0) * 100 AS blocked_pct,
    COALESCE(s.spill_ratio, 0) * 100 AS spill_pct,
    -- Efficiency score: 100 = perfect. Deduct for idle, queue, spill.
    GREATEST(0, LEAST(100,
        100
        - (COALESCE(u.idle_ratio, 0) * 40)         -- Up to -40 for idle time
        - (COALESCE(u.avg_queue_ratio, 0) * 30)     -- Up to -30 for queuing
        - (COALESCE(s.spill_ratio, 0) * 20)          -- Up to -20 for spill
        - (COALESCE(u.avg_blocked_ratio, 0) * 10)    -- Up to -10 for blocking
    )) AS efficiency_score,
    CASE
        WHEN COALESCE(u.idle_ratio, 0) > 0.5 THEN 'High idle time — consider reducing auto-suspend timeout'
        WHEN COALESCE(u.avg_queue_ratio, 0) > 0.1 THEN 'Frequent queuing — consider scaling up or multi-cluster'
        WHEN COALESCE(s.spill_ratio, 0) > 0.1 THEN 'Frequent query spills — consider larger warehouse for heavy queries'
        ELSE 'Warehouse is operating efficiently'
    END AS primary_recommendation
FROM utilisation_stats u
LEFT JOIN spill_stats s ON u.warehouse_name = s.warehouse_name
