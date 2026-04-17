-- Per-query cost estimation.
-- Derives warehouse size and credits-per-hour from the QUERY_HISTORY itself
-- (warehouse_size column), NOT from seed files. Credit pricing uses seed for
-- edition config, but warehouse sizing is fully dynamic from live metadata.
WITH queries AS (
    SELECT
        query_id,
        user_name,
        role_name,
        warehouse_name,
        warehouse_size,
        query_type,
        query_tag,
        query_text,
        query_parameterized_hash,
        start_time,
        end_time,
        execution_time_ms,
        total_elapsed_time_ms,
        bytes_scanned,
        rows_produced,
        partitions_scanned,
        partitions_total,
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage,
        execution_status
    FROM {{ ref('stg__query_history') }}
    WHERE execution_status = 'SUCCESS'
      AND warehouse_name IS NOT NULL
),

-- Derive credits_per_hour from the warehouse_size column in query_history.
-- This is Snowflake-standard pricing per size — no seed file needed.
wh_credits AS (
    SELECT column1 AS warehouse_size, column2 AS credits_per_hour
    FROM (VALUES
        ('X-Small', 1), ('X-SMALL', 1), ('XSMALL', 1),
        ('Small', 2), ('SMALL', 2),
        ('Medium', 4), ('MEDIUM', 4),
        ('Large', 8), ('LARGE', 8),
        ('X-Large', 16), ('X-LARGE', 16), ('XLARGE', 16),
        ('2X-Large', 32), ('2X-LARGE', 32), ('2XLARGE', 32),
        ('3X-Large', 64), ('3X-LARGE', 64), ('3XLARGE', 64),
        ('4X-Large', 128), ('4X-LARGE', 128), ('4XLARGE', 128),
        ('5X-Large', 256), ('5X-LARGE', 256), ('5XLARGE', 256),
        ('6X-Large', 512), ('6X-LARGE', 512), ('6XLARGE', 512)
    )
),

pricing AS (
    SELECT credit_price_usd
    FROM {{ ref('credit_pricing') }}
    WHERE edition = 'ENTERPRISE'
      AND CURRENT_DATE() BETWEEN effective_from AND effective_to
    LIMIT 1
)

SELECT
    q.query_id,
    q.user_name,
    q.role_name,
    q.warehouse_name,
    q.warehouse_size,
    q.query_type,
    q.query_tag,
    q.query_text,
    q.query_parameterized_hash,
    q.start_time,
    q.end_time,
    q.execution_time_ms,
    q.execution_time_ms / 1000.0 AS execution_time_s,
    q.bytes_scanned,
    q.rows_produced,
    q.partitions_scanned,
    q.partitions_total,
    {{ safe_divide('q.partitions_scanned', 'q.partitions_total') }} AS partitions_scanned_ratio,
    q.bytes_spilled_to_local_storage,
    q.bytes_spilled_to_remote_storage,
    COALESCE(wc.credits_per_hour, 1) AS credits_per_hour,
    (q.execution_time_ms / 1000.0 / 3600.0) * COALESCE(wc.credits_per_hour, 1) AS estimated_credits,
    (q.execution_time_ms / 1000.0 / 3600.0) * COALESCE(wc.credits_per_hour, 1) * p.credit_price_usd AS estimated_cost_usd,
    -- Extract team from query_tag if structured (team:{name};...)
    CASE
        WHEN q.query_tag LIKE 'team:%'
        THEN SPLIT_PART(SPLIT_PART(q.query_tag, 'team:', 2), ';', 1)
        ELSE NULL
    END AS query_tag_team
FROM queries q
LEFT JOIN wh_credits wc ON UPPER(q.warehouse_size) = UPPER(wc.warehouse_size)
CROSS JOIN pricing p
