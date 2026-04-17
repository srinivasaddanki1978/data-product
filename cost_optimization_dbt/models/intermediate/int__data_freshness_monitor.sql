-- Data Freshness Monitor: tracks staleness of key ACCOUNT_USAGE source tables.
-- ACCOUNT_USAGE views have up to 45-minute latency; this model makes that visible.
WITH source_freshness AS (
    SELECT
        'query_history' AS source_name,
        'stg__query_history' AS model_name,
        MAX(end_time) AS latest_record_at
    FROM {{ ref('stg__query_history') }}

    UNION ALL

    SELECT
        'warehouse_metering_history' AS source_name,
        'stg__warehouse_metering_history' AS model_name,
        MAX(end_time) AS latest_record_at
    FROM {{ ref('stg__warehouse_metering_history') }}

    UNION ALL

    SELECT
        'storage_usage' AS source_name,
        'stg__storage_usage' AS model_name,
        MAX(usage_date) AS latest_record_at
    FROM {{ ref('stg__storage_usage') }}

    UNION ALL

    SELECT
        'warehouse_load_history' AS source_name,
        'stg__warehouse_load_history' AS model_name,
        MAX(start_time) AS latest_record_at
    FROM {{ ref('stg__warehouse_load_history') }}

    UNION ALL

    SELECT
        'login_history' AS source_name,
        'stg__login_history' AS model_name,
        MAX(event_timestamp) AS latest_record_at
    FROM {{ ref('stg__login_history') }}

    UNION ALL

    SELECT
        'database_storage_usage_history' AS source_name,
        'stg__database_storage_usage_history' AS model_name,
        MAX(usage_date) AS latest_record_at
    FROM {{ ref('stg__database_storage_usage_history') }}
)

SELECT
    source_name,
    model_name,
    latest_record_at,
    DATEDIFF('minute', latest_record_at, CURRENT_TIMESTAMP()) AS staleness_minutes,
    CASE
        WHEN DATEDIFF('minute', latest_record_at, CURRENT_TIMESTAMP()) < 30 THEN 'FRESH'
        WHEN DATEDIFF('minute', latest_record_at, CURRENT_TIMESTAMP()) < 60 THEN 'STALE'
        ELSE 'CRITICAL'
    END AS freshness_status,
    CURRENT_TIMESTAMP() AS checked_at
FROM source_freshness
