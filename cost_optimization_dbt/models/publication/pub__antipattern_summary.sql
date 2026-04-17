-- Summary statistics per anti-pattern type with trends.
WITH current_period AS (
    SELECT
        antipattern_type,
        COUNT(*) AS query_count,
        SUM(COALESCE(estimated_waste_usd, 0)) AS total_estimated_waste,
        AVG(COALESCE(estimated_waste_usd, 0)) AS avg_waste_per_query
    FROM {{ ref('int__antipattern_union_all') }}
    WHERE end_time >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1
),

prev_period AS (
    SELECT
        antipattern_type,
        COUNT(*) AS prev_query_count
    FROM {{ ref('int__antipattern_union_all') }}
    WHERE end_time >= DATEADD('day', -60, CURRENT_DATE())
      AND end_time < DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1
)

SELECT
    cp.antipattern_type,
    cp.query_count,
    cp.total_estimated_waste,
    cp.avg_waste_per_query,
    COALESCE(pp.prev_query_count, 0) AS prev_period_count,
    CASE
        WHEN COALESCE(pp.prev_query_count, 0) = 0 THEN NULL
        ELSE {{ safe_divide(
            '(cp.query_count - pp.prev_query_count)::FLOAT',
            'pp.prev_query_count'
        ) }} * 100
    END AS trend_pct
FROM current_period cp
LEFT JOIN prev_period pp ON cp.antipattern_type = pp.antipattern_type
ORDER BY cp.total_estimated_waste DESC
