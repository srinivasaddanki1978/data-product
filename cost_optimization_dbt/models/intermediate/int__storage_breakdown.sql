WITH storage AS (
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
        active_bytes / POWER(1024, 4) AS active_tb,
        time_travel_bytes / POWER(1024, 4) AS time_travel_tb,
        failsafe_bytes / POWER(1024, 4) AS failsafe_tb,
        (active_bytes + time_travel_bytes + failsafe_bytes) / POWER(1024, 4) AS total_tb,
        table_created,
        table_dropped,
        last_altered
    FROM {{ ref('stg__table_storage_metrics') }}
    WHERE table_dropped IS NULL
      AND database_name IS NOT NULL
),

-- Get last read date from access_history
last_access AS (
    SELECT
        f.value:objectName::STRING AS full_table_name,
        MAX(a.query_start_time) AS last_read_date
    FROM {{ ref('stg__access_history') }} a,
    LATERAL FLATTEN(input => a.base_objects_accessed) f
    GROUP BY 1
)

SELECT
    s.database_name,
    s.schema_name,
    s.table_name,
    s.table_type,
    s.is_transient,
    s.active_bytes,
    s.time_travel_bytes,
    s.failsafe_bytes,
    s.retained_for_clone_bytes,
    s.active_tb,
    s.time_travel_tb,
    s.failsafe_tb,
    s.total_tb,
    -- Storage cost: ~$23/TB/month for on-demand
    s.total_tb * 23.0 AS estimated_monthly_cost_usd,
    s.table_created,
    s.last_altered,
    la.last_read_date,
    DATEDIFF('day', COALESCE(la.last_read_date, s.table_created), CURRENT_TIMESTAMP()) AS days_since_last_read
FROM storage s
LEFT JOIN last_access la
    ON la.full_table_name = s.database_name || '.' || s.schema_name || '.' || s.table_name
