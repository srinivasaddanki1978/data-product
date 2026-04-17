-- Validates that team-attributed costs sum to approximately the total cost (within 1%).
WITH team_total AS (
    SELECT SUM(compute_cost) AS team_sum
    FROM {{ ref('int__team_cost_attribution') }}
),

actual_total AS (
    SELECT SUM(estimated_cost_usd) AS query_sum
    FROM {{ ref('int__query_cost_attribution') }}
)

SELECT
    team_sum,
    query_sum,
    ABS(team_sum - query_sum) AS diff,
    ABS(team_sum - query_sum) / NULLIF(query_sum, 0) * 100 AS diff_pct
FROM team_total
CROSS JOIN actual_total
WHERE ABS(team_sum - query_sum) / NULLIF(query_sum, 0) > 0.01
  AND query_sum > 0
