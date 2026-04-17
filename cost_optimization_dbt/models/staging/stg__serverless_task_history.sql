WITH source AS (
    SELECT * FROM {{ source('account_usage', 'SERVERLESS_TASK_HISTORY') }}
)

SELECT
    task_name,
    database_name,
    schema_name,
    start_time::TIMESTAMP_NTZ AS start_time,
    end_time::TIMESTAMP_NTZ AS end_time,
    credits_used,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
