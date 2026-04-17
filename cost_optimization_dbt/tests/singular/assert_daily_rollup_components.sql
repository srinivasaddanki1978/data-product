-- Validates that daily cost rollup total = compute + storage + serverless.
SELECT
    date,
    total_cost,
    compute_cost + storage_cost + serverless_cost AS component_sum,
    ABS(total_cost - (compute_cost + storage_cost + serverless_cost)) AS diff
FROM {{ ref('int__daily_cost_rollup') }}
WHERE ABS(total_cost - (compute_cost + storage_cost + serverless_cost)) > 0.01
