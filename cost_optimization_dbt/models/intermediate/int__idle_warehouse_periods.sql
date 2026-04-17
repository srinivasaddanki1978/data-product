WITH load_intervals AS (
    SELECT
        warehouse_name,
        interval_start,
        interval_end,
        is_idle,
        -- Detect transitions: mark each change from non-idle to idle
        CASE
            WHEN is_idle AND (LAG(is_idle) OVER (PARTITION BY warehouse_name ORDER BY interval_start) = FALSE
                              OR LAG(is_idle) OVER (PARTITION BY warehouse_name ORDER BY interval_start) IS NULL)
            THEN 1
            ELSE 0
        END AS new_idle_period
    FROM {{ ref('int__warehouse_utilisation') }}
),

idle_groups AS (
    SELECT
        *,
        SUM(new_idle_period) OVER (PARTITION BY warehouse_name ORDER BY interval_start) AS idle_group_id
    FROM load_intervals
    WHERE is_idle = TRUE
),

pricing AS (
    SELECT credit_price_usd
    FROM {{ ref('credit_pricing') }}
    WHERE edition = 'ENTERPRISE'
      AND CURRENT_DATE() BETWEEN effective_from AND effective_to
    LIMIT 1
),

wh_sizes AS (
    SELECT warehouse_size, credits_per_hour
    FROM {{ ref('warehouse_size_credits') }}
)

SELECT
    ig.warehouse_name,
    MIN(ig.interval_start) AS idle_start,
    MAX(ig.interval_end) AS idle_end,
    DATEDIFF('minute', MIN(ig.interval_start), MAX(ig.interval_end)) AS idle_duration_minutes,
    -- Assume X-Small (1 credit/hour) as default
    (DATEDIFF('second', MIN(ig.interval_start), MAX(ig.interval_end)) / 3600.0) * 1 AS wasted_credits,
    (DATEDIFF('second', MIN(ig.interval_start), MAX(ig.interval_end)) / 3600.0) * 1 * p.credit_price_usd AS wasted_cost_usd
FROM idle_groups ig
CROSS JOIN pricing p
GROUP BY ig.warehouse_name, ig.idle_group_id, p.credit_price_usd
HAVING idle_duration_minutes > 0
