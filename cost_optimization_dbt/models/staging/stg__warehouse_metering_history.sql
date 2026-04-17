WITH source AS (
    SELECT * FROM {{ source('account_usage', 'WAREHOUSE_METERING_HISTORY') }}
)

SELECT
    warehouse_name,
    start_time::TIMESTAMP_NTZ AS start_time,
    end_time::TIMESTAMP_NTZ AS end_time,
    credits_used,
    credits_used_compute,
    credits_used_cloud_services,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
