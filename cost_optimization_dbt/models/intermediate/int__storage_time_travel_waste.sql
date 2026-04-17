-- Tables where time travel storage exceeds active storage — candidates for retention reduction.
SELECT
    database_name,
    schema_name,
    table_name,
    active_bytes,
    time_travel_bytes,
    active_tb,
    time_travel_tb,
    {{ safe_divide('time_travel_bytes', 'active_bytes') }} AS tt_to_active_ratio,
    -- Savings from reducing TT retention to 1 day (estimate ~70% reduction)
    time_travel_tb * 23.0 * 0.7 AS estimated_savings_usd,
    'Reduce TIME_TRAVEL_RETENTION_IN_DAYS to 1 — TT storage exceeds active data' AS recommendation,
    'ALTER TABLE ' || database_name || '.' || schema_name || '.' || table_name
        || ' SET DATA_RETENTION_TIME_IN_DAYS = 1;' AS action_sql
FROM {{ ref('int__storage_breakdown') }}
WHERE time_travel_bytes > active_bytes
  AND active_bytes > 0
  AND time_travel_bytes > 0
