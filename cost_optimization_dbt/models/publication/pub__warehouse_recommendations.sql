-- Warehouse-level recommendations with dollar savings, effort, confidence, and SQL to apply.
WITH sizing_recs AS (
    -- Right-sizing: undersized warehouses (high queue + spill)
    SELECT
        warehouse_name,
        'RESIZE' AS recommendation_type,
        current_size AS current_state,
        CASE
            WHEN avg_queue_ms > 1000 AND spill_rate_pct > 10 THEN
                CASE current_size
                    WHEN 'X-Small' THEN 'Small'
                    WHEN 'Small' THEN 'Medium'
                    WHEN 'Medium' THEN 'Large'
                    ELSE current_size
                END
            WHEN peak_hour_utilisation < 0.2 AND spill_rate_pct < 1 THEN
                CASE current_size
                    WHEN 'Medium' THEN 'Small'
                    WHEN 'Large' THEN 'Medium'
                    WHEN 'X-Large' THEN 'Large'
                    ELSE current_size
                END
            ELSE NULL
        END AS recommended_state,
        CASE
            WHEN peak_hour_utilisation < 0.2 THEN 500.0  -- Estimated monthly savings from downsizing
            ELSE 0
        END AS estimated_monthly_savings_usd,
        'LOW' AS effort,
        CASE
            WHEN avg_queue_ms > 1000 OR spill_rate_pct > 10 THEN 'HIGH'
            WHEN peak_hour_utilisation < 0.2 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS confidence,
        OBJECT_CONSTRUCT(
            'avg_exec_ms', avg_exec_ms,
            'p95_exec_ms', p95_exec_ms,
            'avg_queue_ms', avg_queue_ms,
            'spill_rate_pct', spill_rate_pct,
            'peak_utilisation', peak_hour_utilisation
        )::VARCHAR AS evidence
    FROM {{ ref('int__warehouse_sizing_analysis') }}
    WHERE -- Only recommend if there's a clear signal
        (avg_queue_ms > 1000 AND spill_rate_pct > 10)
        OR peak_hour_utilisation < 0.2
),

suspend_recs AS (
    SELECT
        warehouse_name,
        'AUTO_SUSPEND' AS recommendation_type,
        current_auto_suspend_seconds::VARCHAR AS current_state,
        recommended_auto_suspend_seconds::VARCHAR AS recommended_state,
        potential_savings_usd AS estimated_monthly_savings_usd,
        'LOW' AS effort,
        CASE WHEN idle_period_count > 10 THEN 'HIGH' ELSE 'MEDIUM' END AS confidence,
        OBJECT_CONSTRUCT(
            'idle_period_count', idle_period_count,
            'avg_idle_minutes', avg_idle_minutes,
            'monthly_idle_credits', monthly_idle_credits
        )::VARCHAR AS evidence
    FROM {{ ref('int__warehouse_auto_suspend_analysis') }}
    WHERE recommended_auto_suspend_seconds != current_auto_suspend_seconds
),

schedule_recs AS (
    SELECT
        warehouse_name,
        'SCHEDULE' AS recommendation_type,
        'Always On' AS current_state,
        'Suspend during off-peak' AS recommended_state,
        SUM(schedulable_savings_usd) AS estimated_monthly_savings_usd,
        'MEDIUM' AS effort,
        'MEDIUM' AS confidence,
        OBJECT_CONSTRUCT(
            'off_peak_query_count', SUM(CASE WHEN is_off_peak THEN query_count ELSE 0 END),
            'off_peak_cost', SUM(schedulable_savings_usd)
        )::VARCHAR AS evidence
    FROM {{ ref('int__warehouse_schedule_analysis') }}
    WHERE is_off_peak = TRUE AND query_count < 5
    GROUP BY warehouse_name
    HAVING SUM(schedulable_savings_usd) > 10
),

all_recs AS (
    SELECT * FROM sizing_recs WHERE recommended_state IS NOT NULL
    UNION ALL
    SELECT * FROM suspend_recs
    UNION ALL
    SELECT * FROM schedule_recs
)

SELECT
    warehouse_name,
    recommendation_type,
    current_state,
    recommended_state,
    estimated_monthly_savings_usd,
    effort,
    confidence,
    evidence,
    -- Priority: savings x confidence / effort
    estimated_monthly_savings_usd
        * CASE confidence WHEN 'HIGH' THEN 3 WHEN 'MEDIUM' THEN 2 ELSE 1 END
        / CASE effort WHEN 'LOW' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END
    AS priority_score,
    -- SQL to apply
    CASE recommendation_type
        WHEN 'RESIZE' THEN 'ALTER WAREHOUSE ' || warehouse_name || ' SET WAREHOUSE_SIZE = ''' || recommended_state || ''';'
        WHEN 'AUTO_SUSPEND' THEN 'ALTER WAREHOUSE ' || warehouse_name || ' SET AUTO_SUSPEND = ' || recommended_state || ';'
        WHEN 'SCHEDULE' THEN '-- Create a resource monitor or task to suspend ' || warehouse_name || ' during off-peak hours'
        ELSE '-- Manual review required'
    END AS sql_to_apply
FROM all_recs
ORDER BY priority_score DESC
