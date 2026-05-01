# Publication Models

The publication layer contains 16 dashboard-ready models materialized as **tables** (2 are **incremental**). These are the models consumed by the Streamlit dashboard and external reporting tools.

---

## Cost Summaries

### pub__cost_summary

Monthly cost summary with month-over-month change percentage and cost-category breakdown (compute/storage/serverless percentages).

**Key columns:** `month`, `compute_cost`, `storage_cost`, `serverless_cost`, `total_cost`, `mom_change_pct`, `compute_pct`, `storage_pct`, `serverless_pct`

**Upstream:** `int__daily_cost_rollup`

---

### pub__cost_by_warehouse

All-time warehouse cost and efficiency summary combining credits, utilization/idle/queue rates, and query stats (count, avg cost, median execution time).

**Key columns:** `warehouse_name`, `total_credits`, `total_cost_usd`, `avg_utilisation_pct`, `idle_pct`, `avg_queue_pct`, `total_queries`, `median_execution_time_s`

**Upstream:** `int__warehouse_daily_credits`, `int__warehouse_utilisation`, `int__query_cost_attribution`

---

### pub__cost_by_user

Per-user cost summary with overall cost rank and `top_5_expensive_queries` (JSON array with query_id, type, cost, execution time).

**Key columns:** `user_name`, `total_queries`, `total_cost_usd`, `overall_cost_rank`, `top_5_expensive_queries`

**Upstream:** `int__user_cost_summary`, `int__query_cost_attribution`

---

### pub__cost_by_query_type

Cost metrics aggregated by query type (SELECT, INSERT, DML, DDL, etc.) with percentage of total cost.

**Key columns:** `query_type`, `query_count`, `total_cost_usd`, `avg_cost_per_query`, `pct_of_total_cost`

**Upstream:** `int__query_cost_attribution`

---

### pub__cost_trends_daily

Daily cost time series for charting. Pass-through of `int__daily_cost_rollup` with added `day_of_week` and `week_start`.

**Key columns:** `date`, `compute_cost`, `storage_cost`, `serverless_cost`, `total_cost`, `rolling_7d_avg`, `rolling_30d_avg`, `is_anomaly`

**Upstream:** `int__daily_cost_rollup`

---

### pub__cost_forecast

Monthly forecast combined with 3-month actuals for chart context. UNION ALL of FORECAST and ACTUAL rows. Includes projected annual spend.

**Key columns:** `month`, `data_type` (FORECAST/ACTUAL), `total_cost`, `ci_lower`, `ci_upper`, `projected_annual_spend`

**Upstream:** `int__cost_forecast`, `int__daily_cost_rollup`

---

### pub__team_cost_dashboard

Monthly per-team cost dashboard with month-over-month change, percentage of total, and cost rank. Teams derived dynamically from query history.

**Key columns:** `team_name`, `month`, `monthly_cost`, `total_queries`, `pct_of_total`, `mom_change_pct`, `cost_rank`

**Upstream:** `int__team_cost_attribution`

---

## Warehouse & Query Optimization

### pub__warehouse_efficiency

Composite efficiency score (0-100) per warehouse. Deducts points for idle time (up to 40), queuing (30), spill rate (20), and blocking (10). Provides `primary_recommendation` text.

**Key columns:** `warehouse_name`, `utilisation_pct`, `idle_pct`, `efficiency_score`, `primary_recommendation`

**Upstream:** `int__warehouse_utilisation`, `int__query_cost_attribution`

---

### pub__query_optimization_candidates

Top 100 most impactful query optimization opportunities ranked by estimated waste. Only rows with `estimated_waste_usd > 0`.

**Key columns:** `optimization_rank`, `query_id`, `antipattern_type`, `severity`, `estimated_waste_usd`, `recommendation`, `sample_query_text`

**Upstream:** `int__antipattern_union_all`

---

### pub__antipattern_summary

Summary statistics per anti-pattern type for the current 30-day period with trend comparison to the prior 30 days.

**Key columns:** `antipattern_type`, `query_count`, `total_estimated_waste`, `prev_period_count`, `trend_pct`

**Upstream:** `int__antipattern_union_all`

---

## Storage

### pub__storage_analysis

Table-level storage view with `is_unused` flag (>90 days no read) and `has_tt_waste` flag (time-travel > active). Ordered by `total_tb` DESC.

**Key columns:** `database_name`, `table_name`, `active_tb`, `time_travel_tb`, `failsafe_tb`, `total_tb`, `estimated_monthly_cost_usd`, `days_since_last_read`, `is_unused`, `has_tt_waste`

**Upstream:** `int__storage_breakdown`

---

## Recommendations

### pub__warehouse_recommendations

Warehouse-level recommendations combining 3 types: RESIZE, AUTO_SUSPEND, and SCHEDULE. Generates exact `ALTER WAREHOUSE` SQL. Priority scoring: `savings x confidence / effort`.

**Key columns:** `warehouse_name`, `recommendation_type`, `current_state`, `recommended_state`, `estimated_monthly_savings_usd`, `priority_score`, `sql_to_apply`

**Upstream:** `int__warehouse_sizing_analysis`, `int__warehouse_auto_suspend_analysis`, `int__warehouse_schedule_analysis`

---

### pub__all_recommendations

Master unified recommendations engine combining WAREHOUSE, QUERY, and STORAGE recommendations. Assigns `recommendation_id` (REC-0001 format). Priority: `savings x confidence_factor / effort_factor`.

**Key columns:** `recommendation_id`, `category`, `recommendation_type`, `target_object`, `estimated_monthly_savings_usd`, `priority_score`, `action_sql`, `overall_rank`

**Upstream:** `pub__warehouse_recommendations`, `pub__query_optimization_candidates`, `int__storage_recommendations`

---

### pub__recommendation_roi

ROI tracking with funnel aggregation (counts by status, total/implemented/actual savings). Joins lifecycle detail with aggregate metrics.

**Key columns:** `recommendation_id`, `status`, `estimated_monthly_savings_usd`, `actual_savings_usd`, `roi_pct`, `total_recommendations`, `implemented_count`, `total_actual_savings`

**Upstream:** `int__recommendation_lifecycle`

---

## Monitoring & Reporting

### pub__data_freshness

Data freshness publication layer. Adds overall freshness assessment (worst-case across all sources) via CROSS JOIN.

**Key columns:** `source_name`, `staleness_minutes`, `freshness_status`, `overall_freshness_status`

**Upstream:** `int__data_freshness_monitor`

---

### pub__weekly_executive_report

Single-row weekly executive summary for push-based delivery. Current vs prior week costs, top 3 warehouses (JSON), top 3 savings opportunities (JSON), active alert count, and total unrealized savings.

**Key columns:** `report_date`, `this_week_cost`, `last_week_cost`, `wow_change_pct`, `top_warehouses_json`, `top_savings_json`, `active_alert_count`, `total_unrealised_savings`

**Upstream:** `int__daily_cost_rollup`, `pub__cost_by_warehouse`, `pub__all_recommendations`, `int__alert_state_tracker`
