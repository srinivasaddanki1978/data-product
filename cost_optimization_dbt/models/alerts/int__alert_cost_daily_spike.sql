-- Alert: Daily cost spike — seasonality-aware z-score detection with simple fallback.
-- Uses day-of-week baselines adjusted for trend when sufficient history exists.
-- Falls back to simple 2x multiplier when seasonality data is insufficient.
WITH config AS (
    SELECT
        threshold_value,
        COALESCE(seasonality_sensitivity, 2.0) AS seasonality_sensitivity
    FROM {{ ref('alert_configuration') }}
    WHERE alert_id = 'cost_daily_spike' AND enabled = TRUE
),

daily_costs AS (
    SELECT
        date,
        total_cost,
        rolling_30d_avg
    FROM {{ ref('int__daily_cost_rollup') }}
    WHERE date >= DATEADD('day', -1, CURRENT_DATE())
),

seasonality AS (
    SELECT
        date,
        adjusted_baseline,
        effective_stddev,
        is_month_end
    FROM {{ ref('int__cost_seasonality_baseline') }}
    WHERE date >= DATEADD('day', -1, CURRENT_DATE())
),

-- Seasonality-aware z-score detection (when baseline data exists and stddev > 0)
seasonality_alerts AS (
    SELECT
        'cost_daily_spike' AS alert_id,
        dc.date AS detected_at,
        'account' AS resource_key,
        dc.total_cost AS metric_value,
        s.adjusted_baseline + (c.seasonality_sensitivity * s.effective_stddev) AS threshold_value,
        OBJECT_CONSTRUCT(
            'daily_cost', dc.total_cost,
            'adjusted_baseline', s.adjusted_baseline,
            'effective_stddev', s.effective_stddev,
            'z_score', CASE WHEN s.effective_stddev > 0
                THEN (dc.total_cost - s.adjusted_baseline) / s.effective_stddev
                ELSE NULL END,
            'is_month_end', s.is_month_end,
            'detection_method', 'seasonality_zscore',
            'rolling_30d_avg', dc.rolling_30d_avg
        )::VARCHAR AS details_json
    FROM daily_costs dc
    INNER JOIN seasonality s ON dc.date = s.date
    CROSS JOIN config c
    WHERE s.effective_stddev > 0
      AND s.adjusted_baseline > 0
      AND dc.total_cost > s.adjusted_baseline + (c.seasonality_sensitivity * s.effective_stddev)
),

-- Simple multiplier fallback (when no seasonality data or stddev = 0)
simple_alerts AS (
    SELECT
        'cost_daily_spike' AS alert_id,
        dc.date AS detected_at,
        'account' AS resource_key,
        dc.total_cost AS metric_value,
        dc.rolling_30d_avg * c.threshold_value AS threshold_value,
        OBJECT_CONSTRUCT(
            'daily_cost', dc.total_cost,
            'rolling_30d_avg', dc.rolling_30d_avg,
            'multiplier', {{ safe_divide('dc.total_cost', 'dc.rolling_30d_avg') }},
            'detection_method', 'simple_multiplier'
        )::VARCHAR AS details_json
    FROM daily_costs dc
    CROSS JOIN config c
    LEFT JOIN seasonality s ON dc.date = s.date
    WHERE (s.date IS NULL OR s.effective_stddev = 0 OR s.adjusted_baseline <= 0)
      AND dc.total_cost > dc.rolling_30d_avg * c.threshold_value
      AND dc.rolling_30d_avg > 0
)

SELECT * FROM seasonality_alerts
UNION ALL
SELECT * FROM simple_alerts
