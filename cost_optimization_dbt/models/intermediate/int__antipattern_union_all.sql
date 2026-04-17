-- Union of all anti-pattern detection models into standardized schema.
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
