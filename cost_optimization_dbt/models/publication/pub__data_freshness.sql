-- Publication layer for data freshness — adds overall freshness status (worst-case).
WITH source_detail AS (
    SELECT
        source_name,
        model_name,
        latest_record_at,
        staleness_minutes,
        freshness_status,
        checked_at
    FROM {{ ref('int__data_freshness_monitor') }}
),

overall AS (
    SELECT
        MAX(staleness_minutes) AS max_staleness_minutes,
        CASE
            WHEN MAX(staleness_minutes) < 30 THEN 'FRESH'
            WHEN MAX(staleness_minutes) < 60 THEN 'STALE'
            ELSE 'CRITICAL'
        END AS overall_freshness_status,
        MIN(latest_record_at) AS oldest_record_at
    FROM source_detail
)

SELECT
    sd.source_name,
    sd.model_name,
    sd.latest_record_at,
    sd.staleness_minutes,
    sd.freshness_status,
    sd.checked_at,
    o.overall_freshness_status,
    o.max_staleness_minutes AS overall_max_staleness_minutes,
    o.oldest_record_at AS overall_oldest_record_at
FROM source_detail sd
CROSS JOIN overall o
