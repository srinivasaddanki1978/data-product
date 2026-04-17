WITH source AS (
    SELECT * FROM {{ source('account_usage', 'ACCESS_HISTORY') }}
)

SELECT
    query_id,
    query_start_time::TIMESTAMP_NTZ AS query_start_time,
    user_name,
    direct_objects_accessed,
    base_objects_accessed,
    objects_modified,
    object_modified_by_ddl,
    policies_referenced,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
