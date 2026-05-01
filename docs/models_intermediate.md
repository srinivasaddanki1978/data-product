# Intermediate Models

The intermediate layer contains 31 models with all business logic -- cost attribution, utilization analysis, anti-pattern detection, anomaly detection, forecasting, storage optimization, and recommendations. All materialized as **tables** unless noted.

---

## Cost Attribution & Rollup

### int__query_cost_attribution

**File:** `models/intermediate/int__query_cost_attribution.sql`

Central per-query cost estimation model. Derives `credits_per_hour` from `warehouse_size` using an inline VALUES table (handles all size variants: X-SMALL/XSMALL, X-LARGE/XLARGE, etc.). Multiplies by execution time and credit price to get `estimated_cost_usd`. Extracts team name from structured `query_tag` (`team:{name};...` format).

**Key columns:** `query_id`, `user_name`, `role_name`, `warehouse_name`, `warehouse_size`, `execution_time_s`, `credits_per_hour`, `estimated_credits`, `estimated_cost_usd`, `query_tag_team`

**Upstream:** `stg__query_history`, `credit_pricing`

---

### int__warehouse_daily_credits

**File:** `models/intermediate/int__warehouse_daily_credits.sql`

Aggregates hourly metering data to daily warehouse-level credit and cost totals.

**Key columns:** `warehouse_name`, `date`, `credits_compute`, `credits_cloud`, `total_credits`, `estimated_cost_usd`

**Upstream:** `stg__warehouse_metering_history`, `credit_pricing`

---

### int__serverless_credits

**File:** `models/intermediate/int__serverless_credits.sql`

Consolidates credits from all 5 serverless services (Snowpipe, Automatic Clustering, MV Refresh, Serverless Tasks, Search Optimization) into a single standardized table via UNION ALL.

**Key columns:** `service_type`, `object_name`, `date`, `credits_used`, `estimated_cost_usd`

**Upstream:** `stg__pipe_usage_history`, `stg__automatic_clustering_history`, `stg__materialized_view_refresh_history`, `stg__serverless_task_history`, `stg__search_optimization_history`, `credit_pricing`

---

### int__daily_cost_rollup

**File:** `models/intermediate/int__daily_cost_rollup.sql`

Full daily cost picture combining compute, storage ($23/TB/month), and serverless costs via FULL OUTER JOINs. Computes 7-day and 30-day rolling averages. Flags anomalies where daily total exceeds 2x the 30-day rolling average.

**Key columns:** `date`, `compute_cost`, `storage_cost`, `serverless_cost`, `total_cost`, `rolling_7d_avg`, `rolling_30d_avg`, `is_anomaly`

**Upstream:** `int__warehouse_daily_credits`, `stg__storage_usage`, `int__serverless_credits`

---

### int__user_cost_summary

**File:** `models/intermediate/int__user_cost_summary.sql`

Monthly cost rollup per user with within-month cost rank.

**Key columns:** `user_name`, `month`, `total_queries`, `total_cost_usd`, `avg_cost_per_query`, `cost_rank`

**Upstream:** `int__query_cost_attribution`

---

### int__team_cost_attribution

**File:** `models/intermediate/int__team_cost_attribution.sql`

Attributes compute costs to teams using a 3-tier priority:
1. Structured `query_tag` (`team:{name};...`)
2. Role name pattern matching (ADMIN->Platform, ANALYST->Analytics, etc.)
3. Warehouse name pattern (ANALYTICS_WH->Analytics, ETL_WH->Data Engineering, etc.)

Aggregates monthly by team + warehouse + role. Computes `pct_of_total`.

**Key columns:** `team_name`, `warehouse_name`, `role_name`, `month`, `total_cost`, `total_queries`, `pct_of_total`

**Upstream:** `int__query_cost_attribution`

---

## Warehouse Analysis

### int__warehouse_utilisation

**File:** `models/intermediate/int__warehouse_utilisation.sql`

Computes utilization, queue, and blocked ratios for each 5-minute load interval using `safe_divide`. Flags idle intervals where no queries are running.

**Key columns:** `warehouse_name`, `interval_start`, `avg_running`, `utilisation_pct`, `queue_ratio`, `blocked_ratio`, `is_idle`

**Upstream:** `stg__warehouse_load_history`

---

### int__idle_warehouse_periods

**File:** `models/intermediate/int__idle_warehouse_periods.sql`

Groups consecutive idle intervals into episodes using a window-function island pattern. Calculates wasted credits and cost assuming X-Small (1 credit/hour).

**Key columns:** `warehouse_name`, `idle_start`, `idle_end`, `idle_duration_minutes`, `wasted_credits`, `wasted_cost_usd`

**Upstream:** `int__warehouse_utilisation`, `credit_pricing`, `warehouse_size_credits`

---

### int__warehouse_queue_analysis

**File:** `models/intermediate/int__warehouse_queue_analysis.sql`

Aggregates load history by warehouse and hour-of-day to calculate queue frequency and depth -- identifies peak hours needing scale-up.

**Key columns:** `warehouse_name`, `hour_of_day`, `queue_frequency_pct`, `avg_queue_depth`

**Upstream:** `int__warehouse_utilisation`

---

### int__warehouse_auto_suspend_analysis

**File:** `models/intermediate/int__warehouse_auto_suspend_analysis.sql`

Analyzes idle gap durations to recommend optimal auto-suspend settings. Uses median idle duration (clamped 60s-300s). Estimates 30% savings from optimized suspension.

**Key columns:** `warehouse_name`, `current_auto_suspend_seconds`, `recommended_auto_suspend_seconds`, `monthly_idle_cost_usd`, `potential_savings_usd`

**Upstream:** `int__idle_warehouse_periods`, `credit_pricing`

---

### int__warehouse_schedule_analysis

**File:** `models/intermediate/int__warehouse_schedule_analysis.sql`

Analyzes query volume by day-of-week and hour to detect off-peak scheduling opportunities. Off-peak = weekends or weekday hours before 6am / after 8pm.

**Key columns:** `warehouse_name`, `day_of_week`, `hour`, `query_count`, `is_off_peak`, `schedulable_savings_usd`

**Upstream:** `int__query_cost_attribution`

---

### int__warehouse_sizing_analysis

**File:** `models/intermediate/int__warehouse_sizing_analysis.sql`

Per-warehouse sizing metrics: P50/P95/P99 execution times, queue times, spill rates, and peak utilization. Uses `MODE(warehouse_size)` for current dominant size.

**Key columns:** `warehouse_name`, `current_size`, `total_queries`, `p50_exec_ms`, `p95_exec_ms`, `p99_exec_ms`, `spill_rate_pct`, `peak_hour_utilisation`

**Upstream:** `stg__query_history`, `int__warehouse_utilisation`

---

## Query Pattern Analysis

### int__query_patterns

**File:** `models/intermediate/int__query_patterns.sql`

Groups queries by `query_parameterized_hash` + warehouse + date. Flags repeated queries (>5 executions/day).

**Key columns:** `query_hash`, `warehouse_name`, `query_date`, `execution_count`, `total_cost_usd`, `avg_cost_per_execution`, `is_repeated`

**Upstream:** `int__query_cost_attribution`

---

## Anti-Pattern Detection (6 + 1 union)

### int__antipattern_full_table_scan

Detects queries scanning >80% of partitions on tables with >100 total partitions. **Severity: P2.**

**Upstream:** `stg__query_history`, `int__query_cost_attribution`

---

### int__antipattern_select_star

Detects `SELECT *` queries scanning >1MB. **Severity: P3.**

**Upstream:** `stg__query_history`, `int__query_cost_attribution`

---

### int__antipattern_spill_to_storage

Detects queries with non-zero spill bytes. Remote spill = **P1**; local only = **P2**. Recommends warehouse resize.

**Upstream:** `stg__query_history`, `int__query_cost_attribution`

---

### int__antipattern_repeated_queries

Flags query hashes running >10 times/day with total daily cost >$5. **Severity: P2.** Recommends result caching.

**Upstream:** `int__query_patterns`

---

### int__antipattern_cartesian_join

Heuristically detects cartesian joins where `rows_produced > 10 * (bytes_scanned / 100)` and >1M rows. **Severity: P1.**

**Upstream:** `stg__query_history`, `int__query_cost_attribution`

---

### int__antipattern_large_sort_no_limit

Detects `ORDER BY` queries on >100K rows without a `LIMIT` clause. **Severity: P3.**

**Upstream:** `stg__query_history`, `int__query_cost_attribution`

---

### int__antipattern_union_all

UNION ALL of all 6 anti-pattern detection models into a standardized schema. Single source of truth for the publication layer.

**Standardized columns:** `query_id`, `user_name`, `warehouse_name`, `antipattern_type`, `severity`, `estimated_waste_usd`, `recommendation`, `sample_query_text`, `end_time`

---

## Storage Optimization

### int__storage_breakdown

**File:** `models/intermediate/int__storage_breakdown.sql`

Joins table storage metrics with access history (via `LATERAL FLATTEN`) to derive last read date per table. Calculates storage cost at $23/TB/month and `days_since_last_read`.

**Key columns:** `database_name`, `table_name`, `active_bytes`, `time_travel_bytes`, `failsafe_bytes`, `total_tb`, `estimated_monthly_cost_usd`, `last_read_date`, `days_since_last_read`

**Upstream:** `stg__table_storage_metrics`, `stg__access_history`

---

### int__storage_unused_tables

Identifies tables with no reads in 90+ days that still have active storage. Generates DROP recommendation with savings estimate.

**Upstream:** `int__storage_breakdown`

---

### int__storage_time_travel_waste

Flags tables where time-travel storage exceeds active storage. Estimates 70% savings from reducing retention to 1 day. Generates `ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = 1` SQL.

**Upstream:** `int__storage_breakdown`

---

### int__storage_transient_candidates

Identifies non-transient tables in staging/temp schemas that could be converted to TRANSIENT (eliminating fail-safe cost). Generates `CREATE OR REPLACE TRANSIENT TABLE ... CLONE ...` SQL.

**Upstream:** `int__storage_breakdown`

---

### int__storage_clone_overhead

Detects tables with non-zero `retained_for_clone_bytes` and calculates clone overhead cost.

**Upstream:** `int__storage_breakdown`

---

### int__storage_recommendations

UNION of all 4 storage optimization types (UNUSED_TABLE, TIME_TRAVEL_WASTE, TRANSIENT_CANDIDATE, CLONE_OVERHEAD) into a standardized schema with effort, confidence, and `action_sql`.

**Upstream:** `int__storage_unused_tables`, `int__storage_time_travel_waste`, `int__storage_transient_candidates`, `int__storage_clone_overhead`

---

## Forecasting & Seasonality

### int__cost_forecast

**File:** `models/intermediate/int__cost_forecast.sql`

90-day linear regression forecast using Snowflake's `REGR_SLOPE`/`REGR_INTERCEPT` on the last 90 days. Generates future dates via `TABLE(GENERATOR(ROWCOUNT => 90))`. Produces 95% confidence intervals.

**Key columns:** `forecast_date`, `predicted_total_cost`, `predicted_compute_cost`, `ci_lower`, `ci_upper`, `daily_trend`

**Upstream:** `int__daily_cost_rollup`

---

### int__team_cost_forecast

Per-team 3-month forward forecast using monthly linear regression. Requires at least 2 months of data.

**Key columns:** `team_name`, `forecast_month`, `predicted_monthly_cost`, `ci_lower`, `ci_upper`, `monthly_trend`

**Upstream:** `int__team_cost_attribution`

---

### int__cost_seasonality_baseline

Builds a seasonality-aware cost baseline: day-of-week averages, day-of-month stats, month-end stats, and week-over-week trend. Combines into `adjusted_baseline` and `effective_stddev` for z-score anomaly detection.

**Key columns:** `date`, `total_cost`, `dow_avg_cost`, `dow_stddev`, `adjusted_baseline`, `effective_stddev`

**Upstream:** `int__daily_cost_rollup`

---

## Monitoring & Lifecycle

### int__data_freshness_monitor

Tracks staleness of 6 key ACCOUNT_USAGE source tables. Labels each: FRESH (<30 min), STALE (<60 min), or CRITICAL (>=60 min).

**Key columns:** `source_name`, `latest_record_at`, `staleness_minutes`, `freshness_status`

**Upstream:** 6 staging models

---

### int__recommendation_lifecycle

Joins master recommendations to `recommendation_actions` seed to track status. For IMPLEMENTED warehouse recommendations, computes `actual_savings_usd` and `roi_pct`.

**Key columns:** `recommendation_id`, `status`, `estimated_monthly_savings_usd`, `actual_savings_usd`, `roi_pct`, `days_since_implementation`

**Upstream:** `pub__all_recommendations`, `recommendation_actions`, `int__warehouse_daily_credits`
