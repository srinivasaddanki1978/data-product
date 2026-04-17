WITH source AS (
    SELECT * FROM {{ source('account_usage', 'SESSIONS') }}
)

SELECT
    session_id,
    user_name,
    created_on::TIMESTAMP_NTZ AS created_on,
    authentication_method,
    client_application_id,
    client_application_version,
    client_build_id,
    client_environment,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at
FROM source
