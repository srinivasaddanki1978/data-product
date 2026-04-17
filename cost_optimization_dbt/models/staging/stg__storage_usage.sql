WITH source AS (
    SELECT * FROM {{ source('account_usage', 'STORAGE_USAGE') }}
)

SELECT
    usage_date,
    storage_bytes,
    stage_bytes,
    failsafe_bytes,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
