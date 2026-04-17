-- Detect cloned tables and calculate diverged storage overhead.
SELECT
    database_name,
    schema_name,
    table_name,
    table_type,
    active_bytes,
    retained_for_clone_bytes,
    retained_for_clone_bytes / POWER(1024, 4) AS clone_overhead_tb,
    (retained_for_clone_bytes / POWER(1024, 4)) * 23.0 AS estimated_clone_cost_usd,
    'Review clone — diverged storage adding cost. Consider dropping if no longer needed.' AS recommendation
FROM {{ ref('int__storage_breakdown') }}
WHERE retained_for_clone_bytes > 0
