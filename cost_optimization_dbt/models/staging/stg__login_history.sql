WITH source AS (
    SELECT * FROM {{ source('account_usage', 'LOGIN_HISTORY') }}
)

SELECT
    event_id,
    event_timestamp::TIMESTAMP_NTZ AS event_timestamp,
    event_type,
    user_name,
    client_ip,
    reported_client_type,
    reported_client_version,
    first_authentication_factor,
    second_authentication_factor,
    is_success,
    error_code,
    error_message,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
