WITH source AS (
    SELECT * FROM {{ source('account_usage', 'PIPE_USAGE_HISTORY') }}
)

SELECT
    pipe_name,
    start_time::TIMESTAMP_NTZ AS start_time,
    end_time::TIMESTAMP_NTZ AS end_time,
    credits_used,
    bytes_inserted,
    files_inserted,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
