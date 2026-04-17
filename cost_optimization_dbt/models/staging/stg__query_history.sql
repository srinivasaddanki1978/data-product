{{
  config(
    materialized='incremental',
    unique_key='query_id',
    incremental_strategy='merge',
    schema='STAGING'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('account_usage', 'QUERY_HISTORY') }}
    {% if is_incremental() %}
    WHERE end_time > (SELECT MAX(end_time) FROM {{ this }})
    {% endif %}
)

SELECT
    query_id,
    query_text,
    query_type,
    query_tag,
    session_id,
    user_name,
    role_name,
    warehouse_name,
    warehouse_size,
    warehouse_type,
    database_name,
    schema_name,
    execution_status,
    error_code,
    error_message,
    start_time::TIMESTAMP_NTZ AS start_time,
    end_time::TIMESTAMP_NTZ AS end_time,
    total_elapsed_time AS total_elapsed_time_ms,
    execution_time AS execution_time_ms,
    queued_provisioning_time AS queued_provisioning_time_ms,
    queued_repair_time AS queued_repair_time_ms,
    queued_overload_time AS queued_overload_time_ms,
    compilation_time AS compilation_time_ms,
    bytes_scanned,
    rows_produced,
    bytes_written,
    bytes_written_to_result,
    bytes_read_from_result,
    partitions_scanned,
    partitions_total,
    bytes_spilled_to_local_storage,
    bytes_spilled_to_remote_storage,
    query_parameterized_hash,
    query_hash,
    credits_used_cloud_services,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
