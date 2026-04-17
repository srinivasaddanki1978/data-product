-- Seasonality-aware cost baseline using day-of-week and month-end patterns.
-- Produces per-date baselines with trend adjustment for smarter anomaly detection.
WITH daily_data AS (
    SELECT
        date,
        total_cost,
        DAYOFWEEK(date) AS day_of_week,
        DAY(date) AS day_of_month,
        -- Last 3 business days of month = month-end
        CASE
            WHEN date >= DATEADD('day', -3, LAST_DAY(date))
            THEN TRUE ELSE FALSE
        END AS is_month_end
    FROM {{ ref('int__daily_cost_rollup') }}
    WHERE date >= DATEADD('day', -90, CURRENT_DATE())
      AND date < CURRENT_DATE()
),

-- Day-of-week aggregation
dow_stats AS (
    SELECT
        day_of_week,
        AVG(total_cost) AS dow_avg_cost,
        STDDEV(total_cost) AS dow_stddev
    FROM daily_data
    GROUP BY day_of_week
),

-- Day-of-month aggregation
dom_stats AS (
    SELECT
        day_of_month,
        AVG(total_cost) AS dom_avg_cost,
        STDDEV(total_cost) AS dom_stddev
    FROM daily_data
    GROUP BY day_of_month
),

-- Month-end aggregation
month_end_stats AS (
    SELECT
        AVG(total_cost) AS month_end_avg_cost,
        STDDEV(total_cost) AS month_end_stddev
    FROM daily_data
    WHERE is_month_end = TRUE
),

-- Weekly trend via REGR_SLOPE
weekly_totals AS (
    SELECT
        DATE_TRUNC('week', date)::DATE AS week_start,
        SUM(total_cost) AS weekly_cost,
        DATEDIFF('week', MIN(date) OVER (), DATE_TRUNC('week', date)) AS week_offset
    FROM daily_data
    GROUP BY 1
),

trend AS (
    SELECT
        REGR_SLOPE(weekly_cost, week_offset) AS wow_growth_slope,
        MAX(week_offset) AS max_week_offset
    FROM weekly_totals
)

SELECT
    dd.date,
    dd.total_cost,
    dd.day_of_week,
    dd.day_of_month,
    dd.is_month_end,
    COALESCE(dw.dow_avg_cost, 0) AS dow_avg_cost,
    COALESCE(dw.dow_stddev, 0) AS dow_stddev,
    COALESCE(dm.dom_avg_cost, 0) AS dom_avg_cost,
    COALESCE(dm.dom_stddev, 0) AS dom_stddev,
    COALESCE(me.month_end_avg_cost, 0) AS month_end_avg_cost,
    COALESCE(me.month_end_stddev, 0) AS month_end_stddev,
    COALESCE(t.wow_growth_slope, 0) AS wow_growth_slope,
    -- Trend adjustment: shift DOW avg by linear trend
    COALESCE(dw.dow_avg_cost, 0)
        + COALESCE(t.wow_growth_slope, 0)
          * (DATEDIFF('week', (SELECT MIN(date) FROM daily_data), dd.date) - COALESCE(t.max_week_offset, 0) / 2.0)
          / 7.0
    AS adjusted_baseline,
    -- Use month-end stddev when on month-end days (higher threshold to avoid false positives)
    CASE
        WHEN dd.is_month_end AND COALESCE(me.month_end_stddev, 0) > COALESCE(dw.dow_stddev, 0)
        THEN COALESCE(me.month_end_stddev, 0)
        ELSE COALESCE(dw.dow_stddev, 0)
    END AS effective_stddev
FROM daily_data dd
LEFT JOIN dow_stats dw ON dd.day_of_week = dw.day_of_week
LEFT JOIN dom_stats dm ON dd.day_of_month = dm.day_of_month
CROSS JOIN month_end_stats me
CROSS JOIN trend t
