SELECT
    database_name,
    schema_name,
    table_name,
    table_type,
    is_transient,
    active_bytes,
    time_travel_bytes,
    failsafe_bytes,
    retained_for_clone_bytes,
    active_tb,
    time_travel_tb,
    failsafe_tb,
    total_tb,
    estimated_monthly_cost_usd,
    last_read_date,
    days_since_last_read,
    CASE
        WHEN days_since_last_read > 90 AND active_bytes > 0 THEN TRUE
        ELSE FALSE
    END AS is_unused,
    CASE
        WHEN time_travel_bytes > active_bytes AND active_bytes > 0 THEN TRUE
        ELSE FALSE
    END AS has_tt_waste
FROM {{ ref('int__storage_breakdown') }}
WHERE active_bytes > 0 OR time_travel_bytes > 0 OR failsafe_bytes > 0
ORDER BY total_tb DESC
