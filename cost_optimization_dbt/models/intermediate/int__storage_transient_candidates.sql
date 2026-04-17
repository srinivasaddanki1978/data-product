-- Tables that could be TRANSIENT (no fail-safe needed) — staging/temp patterns.
SELECT
    database_name,
    schema_name,
    table_name,
    table_type,
    is_transient,
    failsafe_bytes,
    failsafe_tb,
    failsafe_tb * 23.0 AS estimated_savings_usd,
    'Convert to TRANSIENT table to eliminate fail-safe storage cost' AS recommendation,
    'CREATE OR REPLACE TRANSIENT TABLE ' || database_name || '.' || schema_name || '.' || table_name
        || ' CLONE ' || database_name || '.' || schema_name || '.' || table_name || ';' AS action_sql
FROM {{ ref('int__storage_breakdown') }}
WHERE is_transient = 'NO'
  AND failsafe_bytes > 0
  -- Pattern match: staging, temp, stg, tmp tables are transient candidates
  AND (
    LOWER(schema_name) LIKE '%staging%'
    OR LOWER(schema_name) LIKE '%stg%'
    OR LOWER(schema_name) LIKE '%temp%'
    OR LOWER(schema_name) LIKE '%tmp%'
    OR LOWER(table_name) LIKE '%staging%'
    OR LOWER(table_name) LIKE '%stg%'
    OR LOWER(table_name) LIKE '%temp%'
    OR LOWER(table_name) LIKE '%tmp%'
  )
