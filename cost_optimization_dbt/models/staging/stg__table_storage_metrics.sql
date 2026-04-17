WITH source AS (
    SELECT * FROM {{ source('account_usage', 'TABLE_STORAGE_METRICS') }}
)

SELECT
    id AS table_id,
    table_name,
    table_schema AS schema_name,
    table_catalog AS database_name,
    table_type,
    is_transient,
    active_bytes,
    time_travel_bytes,
    failsafe_bytes,
    retained_for_clone_bytes,
    table_created::TIMESTAMP_NTZ AS table_created,
    table_dropped::TIMESTAMP_NTZ AS table_dropped,
    table_entered_failsafe::TIMESTAMP_NTZ AS table_entered_failsafe,
    last_altered::TIMESTAMP_NTZ AS last_altered,
    comment,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
