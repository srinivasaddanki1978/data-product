-- Union of all anti-pattern detection models into standardized schema.
-- Excludes Streamlit internal execution queries (not user-authored SQL).
WITH all_antipatterns AS (
    SELECT query_id, user_name, warehouse_name, antipattern_type, severity,
           estimated_waste_usd, recommendation, sample_query_text, end_time
    FROM {{ ref('int__antipattern_full_table_scan') }}

    UNION ALL

    SELECT query_id, user_name, warehouse_name, antipattern_type, severity,
           estimated_waste_usd, recommendation, sample_query_text, end_time
    FROM {{ ref('int__antipattern_select_star') }}

    UNION ALL

    SELECT query_id, user_name, warehouse_name, antipattern_type, severity,
           estimated_waste_usd, recommendation, sample_query_text, end_time
    FROM {{ ref('int__antipattern_spill_to_storage') }}

    UNION ALL

    SELECT query_id, user_name, warehouse_name, antipattern_type, severity,
           estimated_waste_usd, recommendation, sample_query_text, end_time
    FROM {{ ref('int__antipattern_repeated_queries') }}

    UNION ALL

    SELECT query_id, user_name, warehouse_name, antipattern_type, severity,
           estimated_waste_usd, recommendation, sample_query_text, end_time
    FROM {{ ref('int__antipattern_cartesian_join') }}

    UNION ALL

    SELECT query_id, user_name, warehouse_name, antipattern_type, severity,
           estimated_waste_usd, recommendation, sample_query_text, end_time
    FROM {{ ref('int__antipattern_large_sort_no_limit') }}

    UNION ALL

    SELECT query_id, user_name, warehouse_name, antipattern_type, severity,
           estimated_waste_usd, recommendation, sample_query_text, end_time
    FROM {{ ref('int__antipattern_long_running') }}
)

SELECT *
FROM all_antipatterns
WHERE sample_query_text IS NOT NULL
  AND TRIM(sample_query_text) != ''
  AND LOWER(sample_query_text) NOT LIKE 'execute streamlit%'
  AND LOWER(sample_query_text) NOT LIKE 'execute dbt project%'
  AND LOWER(sample_query_text) NOT LIKE 'execute dbt%'
  AND LOWER(sample_query_text) NOT LIKE 'create or replace%'
  AND LOWER(sample_query_text) NOT LIKE 'alter%'
  AND LOWER(sample_query_text) NOT LIKE 'grant%'
  AND LOWER(sample_query_text) NOT LIKE 'call%'
