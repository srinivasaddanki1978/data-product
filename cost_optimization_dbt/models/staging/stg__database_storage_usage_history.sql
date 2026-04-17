WITH source AS (
    SELECT * FROM {{ source('account_usage', 'DATABASE_STORAGE_USAGE_HISTORY') }}
)

SELECT
    usage_date,
    database_name,
    average_database_bytes,
    average_failsafe_bytes,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
