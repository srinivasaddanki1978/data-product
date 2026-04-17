WITH source AS (
    SELECT * FROM {{ source('account_usage', 'WAREHOUSE_LOAD_HISTORY') }}
)

SELECT
    warehouse_name,
    start_time::TIMESTAMP_NTZ AS start_time,
    end_time::TIMESTAMP_NTZ AS end_time,
    avg_running,
    avg_queued_load,
    avg_queued_provisioning,
    avg_blocked,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
