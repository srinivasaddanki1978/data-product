-- Detect tables with 0 reads in 90+ days that still have active storage.
SELECT
    database_name,
    schema_name,
    table_name,
    active_bytes,
    active_tb,
    time_travel_tb,
    failsafe_tb,
    total_tb,
    estimated_monthly_cost_usd,
    last_read_date,
    days_since_last_read,
    'DROP or archive table — no reads in ' || days_since_last_read || ' days' AS recommendation,
    estimated_monthly_cost_usd AS savings_if_dropped_usd
FROM {{ ref('int__storage_breakdown') }}
WHERE days_since_last_read > 90
  AND active_bytes > 0
  AND last_read_date IS NOT NULL  -- Only flag tables with known access history
