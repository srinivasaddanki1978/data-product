-- Validates that no staging model contains timestamps in the future.
-- Checks the most recent end_time in query_history.
SELECT end_time
FROM {{ ref('stg__query_history') }}
WHERE end_time > DATEADD('hour', 1, CURRENT_TIMESTAMP())
LIMIT 1
