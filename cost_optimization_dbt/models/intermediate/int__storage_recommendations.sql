-- Union of all storage optimization findings.
SELECT
    'UNUSED_TABLE' AS recommendation_type,
    database_name || '.' || schema_name || '.' || table_name AS target_object,
    recommendation AS description,
    estimated_monthly_cost_usd AS current_cost_usd,
    savings_if_dropped_usd AS savings_if_applied_usd,
    'MEDIUM' AS effort,
    CASE WHEN days_since_last_read > 180 THEN 'HIGH' ELSE 'MEDIUM' END AS confidence,
    'DROP TABLE ' || database_name || '.' || schema_name || '.' || table_name || ';' AS action_sql
FROM {{ ref('int__storage_unused_tables') }}

UNION ALL

SELECT
    'TIME_TRAVEL_WASTE' AS recommendation_type,
    database_name || '.' || schema_name || '.' || table_name AS target_object,
    recommendation AS description,
    time_travel_tb * 23.0 AS current_cost_usd,
    estimated_savings_usd AS savings_if_applied_usd,
    'LOW' AS effort,
    'HIGH' AS confidence,
    action_sql
FROM {{ ref('int__storage_time_travel_waste') }}

UNION ALL

SELECT
    'TRANSIENT_CANDIDATE' AS recommendation_type,
    database_name || '.' || schema_name || '.' || table_name AS target_object,
    recommendation AS description,
    failsafe_tb * 23.0 AS current_cost_usd,
    estimated_savings_usd AS savings_if_applied_usd,
    'MEDIUM' AS effort,
    'MEDIUM' AS confidence,
    action_sql
FROM {{ ref('int__storage_transient_candidates') }}

UNION ALL

SELECT
    'CLONE_OVERHEAD' AS recommendation_type,
    database_name || '.' || schema_name || '.' || table_name AS target_object,
    recommendation AS description,
    estimated_clone_cost_usd AS current_cost_usd,
    estimated_clone_cost_usd AS savings_if_applied_usd,
    'MEDIUM' AS effort,
    'MEDIUM' AS confidence,
    '-- Review and potentially drop clone: ' || database_name || '.' || schema_name || '.' || table_name AS action_sql
FROM {{ ref('int__storage_clone_overhead') }}
