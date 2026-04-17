-- Analyze idle gaps to determine optimal auto-suspend setting.
WITH idle_gaps AS (
    SELECT
        warehouse_name,
        idle_duration_minutes,
        wasted_credits,
        wasted_cost_usd
    FROM {{ ref('int__idle_warehouse_periods') }}
),

gap_stats AS (
    SELECT
        warehouse_name,
        COUNT(*) AS idle_period_count,
        AVG(idle_duration_minutes) AS avg_idle_minutes,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY idle_duration_minutes) AS median_idle_minutes,
        SUM(wasted_credits) AS total_idle_credits,
        SUM(wasted_cost_usd) AS total_idle_cost_usd
    FROM idle_gaps
    GROUP BY 1
),

pricing AS (
    SELECT credit_price_usd
    FROM {{ ref('credit_pricing') }}
    WHERE edition = 'ENTERPRISE'
      AND CURRENT_DATE() BETWEEN effective_from AND effective_to
    LIMIT 1
)

SELECT
    gs.warehouse_name,
    60 AS current_auto_suspend_seconds,  -- Default from setup_snowflake_objects.py
    -- Recommend auto-suspend = median idle gap (min 60s)
    GREATEST(60, LEAST(gs.median_idle_minutes * 60, 300))::INT AS recommended_auto_suspend_seconds,
    gs.idle_period_count,
    gs.avg_idle_minutes,
    gs.total_idle_credits AS monthly_idle_credits,
    gs.total_idle_cost_usd AS monthly_idle_cost_usd,
    -- Estimate savings: reduce idle time by difference between current and recommended
    GREATEST(0, gs.total_idle_cost_usd * 0.3) AS potential_savings_usd
FROM gap_stats gs
CROSS JOIN pricing p
