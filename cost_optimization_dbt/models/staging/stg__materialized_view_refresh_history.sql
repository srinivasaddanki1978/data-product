WITH source AS (
    SELECT * FROM {{ source('account_usage', 'MATERIALIZED_VIEW_REFRESH_HISTORY') }}
)

SELECT
    table_id,
    table_name,
    schema_id,
    schema_name,
    database_id,
    database_name,
    start_time::TIMESTAMP_NTZ AS start_time,
    end_time::TIMESTAMP_NTZ AS end_time,
    credits_used,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
