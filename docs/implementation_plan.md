# Snowflake Cost Optimisation Framework — Complete 8-Week Implementation Plan

## Context

**Goal**: Build a production-grade Snowflake cost visibility, optimization, and alerting framework on the Bilvantis Snowflake environment (`chc70950.us-east-1`) as an internal demo/accelerator that can be deployed to any customer (e.g., TR — Thomson Reuters).

**Current State**: Steps 1-2 already completed (Snowflake objects created, CLI connection verified). Repository has only proposal docs — no code yet.

**Environment**: 25 databases, 3 warehouses (all X-Small), 571K+ query history rows, 992 metering records. Snowflake native dbt via `snow dbt deploy/execute`.

**What makes this "excellent"**: Not just dashboards — proactive Teams alerting with toggleable routing, anomaly detection, query anti-pattern engine, prioritized savings recommendations with dollar estimates, and a self-service alert configuration layer.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     SNOWFLAKE.ACCOUNT_USAGE (Source of Truth)                │
│  query_history · warehouse_metering · storage_metrics · access_history ...   │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   STAGING (views)    │  12 models — clean column names,
                    │   1:1 source mirrors │  type casting, rename conventions
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  INTERMEDIATE (tables)│  Cost attribution, utilization,
                    │  Business logic       │  anti-patterns, anomaly detection,
                    │                       │  alert state tracking
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                 │
   ┌──────────▼────────┐ ┌────▼──────────┐ ┌───▼──────────────┐
   │  PUBLICATION       │ │  ALERTS       │ │  RECOMMENDATIONS │
   │  Dashboard-ready   │ │  Teams payload│ │  Prioritized     │
   │  aggregations      │ │  + state mgmt │ │  savings engine   │
   └──────────┬─────────┘ └────┬──────────┘ └───┬──────────────┘
              │                │                 │
   ┌──────────▼────────┐ ┌────▼──────────┐ ┌───▼──────────────┐
   │  STREAMLIT         │ │  TEAMS        │ │  STREAMLIT        │
   │  Dashboard (5 tabs)│ │  Webhook POST │ │  Recommendations  │
   └───────────────────┘ └───────────────┘ └───────────────────┘
```

---

## PHASE 1: COST VISIBILITY & ATTRIBUTION (Weeks 1–4)

### WEEK 1: Foundation + Core Staging Models

**Objective**: dbt project scaffolding + all 14 staging models deployed and tested.

#### 1.1 dbt Project Structure

**Directory**: `C:\Srinivas\project\data-product\cost_optimization_dbt\`

```
cost_optimization_dbt/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── models/
│   ├── sources.yml                           (14 ACCOUNT_USAGE sources)
│   ├── staging/                              (14 view models)
│   │   ├── _stg__models.yml                  (docs + tests for all staging)
│   │   ├── stg__query_history.sql
│   │   ├── stg__warehouse_metering_history.sql
│   │   ├── stg__warehouse_load_history.sql
│   │   ├── stg__table_storage_metrics.sql
│   │   ├── stg__storage_usage.sql
│   │   ├── stg__database_storage_usage_history.sql
│   │   ├── stg__login_history.sql
│   │   ├── stg__access_history.sql
│   │   ├── stg__automatic_clustering_history.sql
│   │   ├── stg__materialized_view_refresh_history.sql
│   │   ├── stg__pipe_usage_history.sql
│   │   ├── stg__serverless_task_history.sql
│   │   └── stg__sessions.sql
│   ├── intermediate/
│   ├── publication/
│   └── alerts/
├── seeds/
│   ├── credit_pricing.csv
│   ├── warehouse_size_credits.csv
│   ├── warehouse_team_mapping.csv
│   └── alert_configuration.csv
├── macros/
│   ├── dbt_overrides/
│   │   └── generate_schema_name.sql
│   └── helpers/
│       └── safe_divide.sql
├── tests/
│   ├── generic/
│   │   └── assert_positive_value.sql
│   └── singular/
│       ├── assert_credit_pricing_complete.sql
│       └── assert_no_future_dates.sql
└── snowflake_objects/
    └── setup_alerts_infrastructure.sql
```

#### 1.2 Configuration Files

**dbt_project.yml**: Model materialization config per layer:
- staging → `view` (lightweight, always fresh)
- intermediate → `table` (persisted business logic)
- publication → `table` (dashboard-ready)
- alerts → `table` (notification state tracking)

**profiles.yml**: Snowflake native dbt connection (account, role, warehouse, database).

**packages.yml**: `dbt-labs/dbt_utils >=1.0.0, <2.0.0`

**Macro — `generate_schema_name.sql`**: Reuse from globalmart — returns custom schema as-is (STAGING, INTERMEDIATE, etc.) instead of dbt's default `PUBLIC_STAGING` concatenation.

**Macro — `safe_divide.sql`**: `NULLIF`-based division macro to prevent divide-by-zero across all cost calculation models.

#### 1.3 Sources Definition (14 ACCOUNT_USAGE views)

**`models/sources.yml`** — declares all sources from `SNOWFLAKE.ACCOUNT_USAGE`:

| Source View | Key Columns | Retention |
|-------------|-------------|-----------|
| `QUERY_HISTORY` | query_id, warehouse_name, user_name, execution_time, bytes_scanned, partitions_scanned/total, bytes_spilled, query_tag, query_parameterized_hash | 365 days |
| `WAREHOUSE_METERING_HISTORY` | warehouse_name, credits_used, credits_used_compute, credits_used_cloud_services, start_time | 365 days |
| `WAREHOUSE_LOAD_HISTORY` | warehouse_name, avg_running, avg_queued_load, avg_blocked | 365 days |
| `TABLE_STORAGE_METRICS` | table_name, active_bytes, time_travel_bytes, failsafe_bytes, table_created, table_dropped, last_altered | Current |
| `STORAGE_USAGE` | storage_bytes, stage_bytes, failsafe_bytes, usage_date | 365 days |
| `DATABASE_STORAGE_USAGE_HISTORY` | database_name, average_database_bytes, average_failsafe_bytes | 365 days |
| `LOGIN_HISTORY` | user_name, client_type, is_success, error_code, first_authentication_factor | 365 days |
| `ACCESS_HISTORY` | query_id, user_name, direct_objects_accessed, base_objects_accessed | 365 days |
| `AUTOMATIC_CLUSTERING_HISTORY` | table_name, credits_used, num_bytes_reclustered | 365 days |
| `MATERIALIZED_VIEW_REFRESH_HISTORY` | table_name, credits_used, num_rows_inserted | 365 days |
| `PIPE_USAGE_HISTORY` | pipe_name, credits_used, bytes_inserted, files_inserted | 365 days |
| `SERVERLESS_TASK_HISTORY` | task_name, credits_used, start_time | 365 days |
| `SESSIONS` | session_id, user_name, client_application_id, created_on | 365 days |
| `SEARCH_OPTIMIZATION_HISTORY` | table_name, credits_used | 365 days |

#### 1.4 Staging Models (14 views)

Each staging model:
- Selects from `{{ source('account_usage', 'VIEW_NAME') }}`
- Renames columns to snake_case convention
- Casts timestamps to `TIMESTAMP_NTZ`
- Adds `_loaded_at` metadata column via `CURRENT_TIMESTAMP()`
- No business logic — pure cleaning

**Special handling for `stg__query_history`** (571K rows):
- Materialized as `incremental` (not view) to avoid re-scanning 571K rows each build
- Incremental strategy: filter on `end_time > (SELECT MAX(end_time) FROM {{ this }})`
- Extracts `query_tag` components if structured (e.g., `team:analytics;product:revenue`)

#### 1.5 Seed Files

**`credit_pricing.csv`** — Snowflake credit price by edition:
```
edition,credit_price_usd,effective_from,effective_to
STANDARD,2.00,2024-01-01,9999-12-31
ENTERPRISE,3.00,2024-01-01,9999-12-31
BUSINESS_CRITICAL,4.00,2024-01-01,9999-12-31
```

**`warehouse_size_credits.csv`** — Credits per hour by warehouse size:
```
warehouse_size,credits_per_hour
XSMALL,1
SMALL,2
MEDIUM,4
LARGE,8
XLARGE,16
2XLARGE,32
3XLARGE,64
4XLARGE,128
5XLARGE,256
6XLARGE,512
```

**`warehouse_team_mapping.csv`** — Warehouse/role to team mapping:
```
warehouse_name,role_name,team_name,cost_center
COMPUTE_WH,,Platform,PLATFORM-001
COST_OPT_WH,,Cost Optimization,COSTOPT-001
```
*(Populated during Week 1 based on actual warehouse/role discovery)*

#### 1.6 Week 1 Tests

| Test | Type | What It Validates |
|------|------|-------------------|
| `unique` on `stg__query_history.query_id` | Generic | No duplicate queries |
| `not_null` on all staging primary keys | Generic | Source data completeness |
| `accepted_values` on `stg__warehouse_metering_history.warehouse_name` | Generic | Only known warehouses |
| `assert_no_future_dates` | Singular | No timestamps beyond current time |
| `assert_credit_pricing_complete` | Singular | All editions have pricing |
| `relationships` staging → seeds | Generic | Warehouse names match seed mappings |

#### 1.7 Week 1 Deliverables
- [ ] dbt project created and deployed to Snowflake
- [ ] 14 staging models running successfully
- [ ] All seeds loaded
- [ ] All Week 1 tests passing
- [ ] `snow dbt execute cost_optimization build` green

---

### WEEK 2: Intermediate + Publication Models (Cost Attribution Engine)

**Objective**: All business logic models + dashboard-ready publication layer.

#### 2.1 Intermediate Models (10 tables)

| Model | Logic | Key Output Columns |
|-------|-------|--------------------|
| **`int__warehouse_daily_credits`** | Aggregate metering by warehouse+day. Join warehouse_size_credits seed for $/credit. Calculate compute vs cloud services split. | warehouse_name, date, credits_compute, credits_cloud, total_credits, estimated_cost_usd |
| **`int__query_cost_attribution`** | Per-query cost = (execution_time_s / 3600) × wh_credits_per_hour × credit_price. Join warehouse size from metering. | query_id, user_name, warehouse_name, role_name, query_type, execution_time_s, estimated_cost_usd, bytes_scanned, partitions_scanned_ratio |
| **`int__warehouse_utilisation`** | From load_history: avg_running / (avg_running + avg_queued + avg_blocked) = utilisation %. Idle periods = avg_running = 0 intervals. | warehouse_name, interval_start, utilisation_pct, is_idle, queue_ratio, blocked_ratio |
| **`int__user_cost_summary`** | Aggregate query_cost_attribution by user+month. Rank users by spend. | user_name, month, total_queries, total_cost_usd, avg_cost_per_query, cost_rank |
| **`int__storage_breakdown`** | From table_storage_metrics: active vs TT vs failsafe bytes per database/schema/table. Convert to TB. Join access_history for last_read_date. | database_name, schema_name, table_name, active_tb, time_travel_tb, failsafe_tb, total_tb, estimated_monthly_cost_usd, last_read_date, days_since_last_read |
| **`int__serverless_credits`** | UNION ALL of pipe, clustering, MV refresh, task, search optimization credits. Categorize by service type. | service_type, object_name, date, credits_used, estimated_cost_usd |
| **`int__idle_warehouse_periods`** | Detect consecutive intervals where warehouse is running but avg_running = 0. Calculate idle duration and wasted credits. | warehouse_name, idle_start, idle_end, idle_duration_minutes, wasted_credits, wasted_cost_usd |
| **`int__query_patterns`** | Classify queries: by type (SELECT/INSERT/CREATE/COPY), by frequency (query_parameterized_hash grouping), by size (bytes_scanned buckets). | query_hash, query_type, execution_count, avg_execution_time, total_cost_usd, is_repeated (>5 runs/day) |
| **`int__daily_cost_rollup`** | Single daily cost table combining compute + storage + serverless. Compute 7-day and 30-day rolling averages for anomaly baseline. | date, compute_cost, storage_cost, serverless_cost, total_cost, rolling_7d_avg, rolling_30d_avg, is_anomaly (>2x 30d avg) |
| **`int__team_cost_attribution`** | Join query costs → warehouse_team_mapping seed. Allocate shared warehouse costs proportionally by query execution time. | team_name, cost_center, month, compute_cost, storage_cost, total_cost, pct_of_total |

#### 2.2 Publication Models (8 tables)

| Model | Purpose | Consumed By |
|-------|---------|-------------|
| **`pub__cost_summary`** | Current month + last 3 months: total spend, compute/storage/serverless split, MoM change % | Executive Summary tab |
| **`pub__cost_by_warehouse`** | Per-warehouse: credits, cost, utilisation %, idle %, queue time, avg query cost | Warehouse Deep Dive tab |
| **`pub__cost_by_user`** | Per-user: total cost, query count, avg cost/query, most expensive queries (top 5 per user) | Team Attribution tab |
| **`pub__cost_by_query_type`** | Per query_type (SELECT/INSERT/COPY/etc.): cost, count, avg duration, total bytes | Trend Analysis tab |
| **`pub__storage_analysis`** | Per-database/table: active/TT/failsafe storage, cost, days since last read, unused flag | Storage Explorer tab |
| **`pub__cost_trends_daily`** | Daily time series: total cost, compute/storage/serverless breakdown, rolling averages, anomaly flags | Trend Analysis tab |
| **`pub__warehouse_efficiency`** | Per-warehouse: efficiency score (0-100), idle %, queue %, spill %, recommendations text | Warehouse Deep Dive tab |
| **`pub__team_cost_dashboard`** | Per-team: monthly cost, trend, top users, top queries, cost rank | Team Attribution tab |

#### 2.3 Week 2 Tests

| Test | Type | What It Validates |
|------|------|-------------------|
| `assert_positive_value` on all cost columns | Generic | No negative costs |
| `unique` on pub model primary keys | Generic | No duplicates in dashboard data |
| `not_null` on cost_summary date columns | Generic | Complete time series |
| Sum of team costs ≈ total cost (within 1%) | Singular | Attribution adds up |
| Warehouse efficiency score between 0-100 | Singular | Score normalization correct |
| Daily cost rollup matches sum of components | Singular | Compute + storage + serverless = total |

#### 2.4 Week 2 Deliverables
- [ ] 10 intermediate models deployed and populated
- [ ] 8 publication models deployed and populated
- [ ] All Week 2 tests passing
- [ ] Spot-check: `SELECT * FROM PUBLICATION.PUB__COST_SUMMARY LIMIT 10` returns real data
- [ ] Spot-check: `SELECT * FROM PUBLICATION.PUB__COST_BY_WAREHOUSE` returns all 3 warehouses

---

### WEEK 3: Streamlit Dashboard (5 Interactive Tabs)

**Objective**: Deploy interactive Streamlit-in-Snowflake dashboard consuming publication models.

#### 3.1 Dashboard Structure

**Directory**: `C:\Srinivas\project\data-product\streamlit_app\`

```
streamlit_app/
├── app.py                              (Main entry — KPI cards + navigation)
├── pages/
│   ├── 1_Executive_Summary.py          (Cost overview, MoM trends)
│   ├── 2_Warehouse_Deep_Dive.py        (Per-warehouse analysis)
│   ├── 3_Team_Attribution.py           (Cost by team/user)
│   ├── 4_Storage_Explorer.py           (Storage by database/table)
│   └── 5_Trend_Analysis.py             (90-day trends, anomaly flags)
├── utils/
│   ├── connection.py                   (Snowflake session helper)
│   ├── queries.py                      (SQL query constants)
│   └── formatters.py                   (Currency, %, byte formatting)
├── snowflake.yml                       (Streamlit-in-Snowflake config)
└── environment.yml                     (Python dependencies)
```

#### 3.2 Page Specifications

**Main App (`app.py`)**:
- 4 KPI cards: Total Spend (MTD), Compute Cost, Storage Cost, Serverless Cost
- MoM % change badges (green/red)
- Donut chart: cost split by category
- Global date range filter (default: last 30 days)

**Page 1: Executive Summary**:
- Line chart: daily total cost (90 days) with 30-day rolling average overlay
- Bar chart: MoM cost comparison (last 6 months)
- Table: top 5 most expensive warehouses
- Table: top 5 most expensive users
- Anomaly markers on chart where cost > 2x baseline

**Page 2: Warehouse Deep Dive**:
- Dropdown: select warehouse (or "All")
- Gauge chart: utilisation % (green >70%, yellow 40-70%, red <40%)
- Stacked bar: credits_compute vs credits_cloud by day
- Table: idle periods with duration and wasted cost
- Bar chart: queue time by hour of day (identify peak contention)
- Efficiency score card (0-100) with breakdown

**Page 3: Team Attribution**:
- Treemap: cost by team (sized by spend)
- Dropdown: select team → drill into users
- Table: user-level cost breakdown with sparkline trends
- Bar chart: cost by query type per team
- Percentage contribution pie chart

**Page 4: Storage Explorer**:
- Stacked bar: active vs TT vs failsafe per database
- Table: largest tables with storage breakdown
- Highlighted rows: tables with 0 reads in 90+ days (unused)
- Table: tables where TT storage > active storage (waste indicator)
- Total storage cost card

**Page 5: Trend Analysis**:
- Multi-line chart: compute/storage/serverless trends (90 days)
- Week-over-week comparison table
- Anomaly detection highlights (flagged days from `pub__cost_trends_daily`)
- Heatmap: cost by day-of-week × hour-of-day

#### 3.3 Deployment Config

**`snowflake.yml`**:
```yaml
definition_version: 2
entities:
  cost_optimization_dashboard:
    type: streamlit
    identifier:
      name: COST_OPTIMIZATION_DASHBOARD
    title: "Cost Optimization Dashboard"
    query_warehouse: COST_OPT_WH
    main_file: app.py
    pages_dir: pages/
    stage: COST_OPTIMIZATION_DB.PUBLIC.STREAMLIT_STAGE
```

**`environment.yml`**:
```yaml
dependencies:
  - plotly
  - pandas
```

#### 3.4 Week 3 Tests

| Test | What It Validates |
|------|-------------------|
| Dashboard loads without errors | App.py renders KPIs |
| Each page loads with default filters | No SQL errors, no empty charts |
| Date filter changes data correctly | Query re-runs with new date range |
| Warehouse dropdown shows all 3 warehouses | Dropdown populated from pub model |
| Numbers match raw SQL spot-checks | KPI cards = direct SQL on PUBLICATION tables |

#### 3.5 Week 3 Deliverables
- [ ] Streamlit app deployed to Snowflake
- [ ] All 5 pages rendering with real data
- [ ] Date range filter working
- [ ] Warehouse/team drill-down working
- [ ] `snow streamlit deploy --connection cost_optimization` successful

---

### WEEK 4: Microsoft Teams Alert Routing + Automation

**Objective**: Metadata-driven alert system with toggleable Teams webhook routing, episode tracking, and scheduled automation.

#### 4.1 Alert Architecture

```
┌───────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ Alert Detection│────▶│ Alert State      │────▶│ Teams Payload       │
│ Models (dbt)   │     │ Tracker (dbt)    │     │ Generator (dbt)     │
│                │     │                  │     │                     │
│ 7 alert types  │     │ Episode tracking │     │ Adaptive Card JSON  │
│ threshold eval │     │ Deduplication    │     │ per new episode     │
└───────────────┘     └──────────────────┘     └─────────┬───────────┘
                                                          │
                                                          ▼
┌───────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ Alert Config   │     │ Snowflake TASK   │────▶│ Stored Procedure    │
│ Seed (CSV)     │     │ (every 15 min)   │     │ POST to Teams       │
│                │     │                  │     │ webhook             │
│ teams_enabled  │     │ Triggers SP      │     │ Updates sent_at     │
│ = TRUE/FALSE   │     │ for unsent rows  │     │ in history table    │
└───────────────┘     └──────────────────┘     └─────────────────────┘
```

#### 4.2 Alert Configuration Seed

**`seeds/alert_configuration.csv`**:
```
alert_id,alert_name,description,severity,teams_enabled,teams_channel,schedule_minutes,threshold_value,threshold_type,resolver_team,enabled
cost_daily_spike,Daily Cost Spike,Daily spend exceeds 2x 30-day rolling average,P1,TRUE,cost-alerts,360,2.0,multiplier,platform-team,TRUE
warehouse_idle_extended,Warehouse Idle,Warehouse running with 0 queries for >30 minutes,P2,TRUE,cost-alerts,15,30,minutes,platform-team,TRUE
credit_budget_80pct,Budget 80% Warning,Monthly credits approaching 80% of budget,P1,TRUE,finance-alerts,60,80,percentage,finance-team,TRUE
credit_budget_100pct,Budget Exceeded,Monthly credits exceeded 100% of budget,P0,TRUE,finance-alerts,60,100,percentage,finance-team,TRUE
query_spill_heavy,Heavy Query Spill,Queries spilling >1GB to remote storage,P2,TRUE,cost-alerts,30,1073741824,bytes,platform-team,TRUE
storage_growth_anomaly,Storage Spike,Database storage grew >20% week-over-week,P3,FALSE,cost-alerts,1440,20,percentage,data-team,TRUE
repeated_expensive_query,Repeated Expensive Query,Same query hash running >20 times/day costing >$1 each,P2,TRUE,cost-alerts,60,20,count,platform-team,TRUE
```

**Toggle mechanism**: Change `teams_enabled` to `FALSE` or `enabled` to `FALSE` → re-seed → alert stops. No code changes needed.

#### 4.3 Alert Detection Models (7 intermediate models)

| Model | Detection Logic | Threshold |
|-------|----------------|-----------|
| **`int__alert_cost_daily_spike`** | Compare today's cost to `rolling_30d_avg` from `int__daily_cost_rollup`. Flag if ratio > threshold. | 2x multiplier |
| **`int__alert_warehouse_idle`** | From `int__idle_warehouse_periods`: current idle periods exceeding threshold minutes. | 30 minutes |
| **`int__alert_credit_budget`** | MTD credits vs monthly budget (from seed). Calculate % used. Flag at 80% and 100%. | 80% / 100% |
| **`int__alert_query_spill`** | From `stg__query_history`: queries with `bytes_spilled_to_remote_storage > threshold` in last interval. | 1GB |
| **`int__alert_storage_growth`** | WoW % change in `database_storage_usage_history`. Flag if growth > threshold %. | 20% |
| **`int__alert_repeated_expensive`** | From `int__query_patterns`: same hash running > N times/day with avg cost > $1. | 20 runs/day |
| **`int__alert_union_all`** | UNION ALL of all 6 alert detection models into standardized schema: `alert_id, detected_at, resource_key, metric_value, threshold_value, details_json`. | — |

#### 4.4 Alert State Tracker

**`int__alert_state_tracker`** (incremental table):
- Joins `int__alert_union_all` with previous state
- Assigns **episode numbers**: new firing = new episode, continuation = same episode
- Tracks: `is_new_episode`, `is_continuation`, `is_resolved`
- Uses `aggregate_episode_key = alert_id || '_episode_' || episode_number` for deduplication
- Only new episodes trigger notifications (prevents alert fatigue)

#### 4.5 Teams Payload Model

**`pub__teams_alert_payload`** (publication table):
- Joins `int__alert_state_tracker` (where `is_new_episode = TRUE`) with `alert_configuration` seed
- Filters: `WHERE alert_config.teams_enabled = TRUE AND alert_config.enabled = TRUE`
- Generates Microsoft Teams **Adaptive Card** JSON payload per alert
- Includes tracking columns: `sent_at`, `api_response_code`, `send_success`

**`pub__alert_history`** (publication table):
- Full audit trail of all alerts (fired, resolved, sent)
- Used by Streamlit alert management page

#### 4.6 Snowflake Infrastructure for Teams Webhook

**Objects created via `snowflake_objects/setup_alerts_infrastructure.sql`**:

```sql
-- 1. Secret to store Teams webhook URL
CREATE SECRET teams_webhook_secret
  TYPE = GENERIC_STRING
  SECRET_STRING = '<teams-incoming-webhook-url>';

-- 2. Network rule allowing egress to Teams
CREATE NETWORK RULE teams_webhook_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('outlook.webhook.office.com:443',
                '*.webhook.office.com:443');

-- 3. External access integration
CREATE EXTERNAL ACCESS INTEGRATION teams_alert_integration
  ALLOWED_NETWORK_RULES = (teams_webhook_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (teams_webhook_secret)
  ENABLED = TRUE;

-- 4. Python stored procedure to POST to Teams
CREATE PROCEDURE send_teams_alerts()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  EXTERNAL_ACCESS_INTEGRATIONS = (teams_alert_integration)
  SECRETS = ('webhook_url' = teams_webhook_secret)
  HANDLER = 'main'
AS $$ ... $$;
-- Logic: SELECT from pub__teams_alert_payload WHERE sent_at IS NULL
--        POST each to webhook, UPDATE sent_at + response_code

-- 5. Snowflake TASK to run every 15 minutes
CREATE TASK send_teams_alerts_task
  WAREHOUSE = COST_OPT_WH
  SCHEDULE = '15 MINUTE'
AS CALL send_teams_alerts();

ALTER TASK send_teams_alerts_task RESUME;  -- Enable
-- ALTER TASK send_teams_alerts_task SUSPEND;  -- Disable (master off switch)
```

#### 4.7 Streamlit Alert Management Page

Add **Page 6: Alert Management** to the Streamlit dashboard:
- Table: all alert configurations with current enabled/teams_enabled status
- Table: recent alert history (last 50 alerts fired)
- Chart: alerts fired over time by severity
- Status badges: alert pipeline health (last run, last sent, failures)

#### 4.8 Automation: Snowflake TASK for dbt Refresh

```sql
-- Refresh dbt models every 6 hours
CREATE TASK refresh_cost_models_task
  WAREHOUSE = COST_OPT_WH
  SCHEDULE = '360 MINUTE'
AS
  -- Trigger a full dbt build via Snowflake native dbt
  -- (Alternative: call a stored procedure that runs the build)
  EXECUTE IMMEDIATE 'SELECT 1';  -- Placeholder for native dbt task trigger

ALTER TASK refresh_cost_models_task RESUME;
```

#### 4.9 Week 4 Tests

| Test | Type | What It Validates |
|------|------|-------------------|
| Alert fires when cost > 2x baseline | Positive | Spike detection works |
| Alert does NOT fire when cost < 2x baseline | Negative | No false positives |
| Alert does NOT re-fire for same episode | Corner | Episode deduplication |
| Alert with `teams_enabled=FALSE` has no payload row | Negative | Toggle mechanism works |
| Alert with `enabled=FALSE` is completely skipped | Negative | Master disable works |
| Teams payload JSON is valid Adaptive Card schema | Positive | Webhook will accept it |
| Stored procedure handles webhook timeout gracefully | Corner | No data loss on failure |
| `sent_at` is populated after successful send | Positive | Audit trail complete |
| Empty alert_union (no alerts firing) produces 0 payload rows | Corner | Handles quiet periods |

#### 4.10 Week 4 Deliverables
- [ ] 7 alert detection models deployed
- [ ] Alert state tracker with episode deduplication
- [ ] Teams payload generator producing valid Adaptive Cards
- [ ] Alert configuration seed loaded and toggleable
- [ ] Snowflake EXTERNAL_ACCESS_INTEGRATION created
- [ ] Stored procedure sending to Teams webhook
- [ ] Snowflake TASK running every 15 minutes
- [ ] Alert Management page in Streamlit
- [ ] All Week 4 tests passing
- [ ] End-to-end demo: trigger a cost spike → Teams message arrives

---

## PHASE 2: QUERY OPTIMISATION & RECOMMENDATIONS (Weeks 5–8)

### WEEK 5: Warehouse Right-Sizing Engine

**Objective**: Detect over/undersized warehouses and generate sizing recommendations with dollar savings.

#### 5.1 New Intermediate Models (4 tables)

| Model | Logic | Key Outputs |
|-------|-------|-------------|
| **`int__warehouse_sizing_analysis`** | Per warehouse: avg query execution time, p50/p95/p99 execution time, avg queue time, spill frequency, utilization patterns by hour. Compare against warehouse size benchmarks from seed. | warehouse_name, current_size, avg_exec_ms, p95_exec_ms, avg_queue_ms, spill_rate_pct, peak_hour_utilisation |
| **`int__warehouse_queue_analysis`** | From load_history: identify hours where avg_queued > 0. Calculate queue frequency, avg queue duration, and impacted query count. | warehouse_name, hour_of_day, queue_frequency, avg_queue_seconds, queries_queued |
| **`int__warehouse_auto_suspend_analysis`** | Detect actual idle gaps between queries. Calculate optimal auto_suspend setting vs current. Estimate credit savings from tighter auto-suspend. | warehouse_name, current_auto_suspend, recommended_auto_suspend, monthly_idle_credits, potential_savings_usd |
| **`int__warehouse_schedule_analysis`** | Analyze query volume by hour/day. Detect if warehouse could use scheduling (suspend during off-hours). Calculate savings. | warehouse_name, day_of_week, hour, query_count, credits_used, is_off_peak, schedulable_savings_usd |

#### 5.2 New Publication Model

**`pub__warehouse_recommendations`**:
| Field | Description |
|-------|-------------|
| warehouse_name | Target warehouse |
| recommendation_type | RESIZE / AUTO_SUSPEND / SCHEDULE / MULTI_CLUSTER |
| current_state | Current size / setting |
| recommended_state | Recommended change |
| estimated_monthly_savings_usd | Dollar savings estimate |
| effort | LOW (config change) / MEDIUM / HIGH |
| confidence | HIGH (clear signal) / MEDIUM / LOW |
| evidence | JSON with supporting metrics |
| priority_score | savings × confidence / effort (auto-ranked) |
| sql_to_apply | Actual ALTER WAREHOUSE statement to apply |

#### 5.3 Streamlit Page

**Page 7: Warehouse Optimizer**:
- Recommendation cards sorted by priority_score
- Per-recommendation: before/after comparison, savings estimate, one-click SQL copy
- Total potential savings banner at top
- Filter by effort level (show me "quick wins" = LOW effort only)

#### 5.4 Week 5 Tests

| Test | What It Validates |
|------|-------------------|
| Warehouse with high queue time gets "scale up" recommendation | Positive |
| Warehouse with low utilisation gets "downsize" recommendation | Positive |
| Warehouse already optimal gets no recommendation | Negative |
| Auto-suspend recommendation never suggests 0 (always-on) | Corner |
| Savings estimate is always >= 0 | Corner |
| SQL_to_apply is syntactically valid | Positive |
| p95 execution time calculated correctly (verify manually) | Positive |

---

### WEEK 6: Query Anti-Pattern Detection Engine

**Objective**: Identify expensive query patterns and provide actionable optimization recommendations.

#### 6.1 New Intermediate Models (7 tables)

| Model | Detection Method | Recommendation |
|-------|-----------------|----------------|
| **`int__antipattern_full_table_scan`** | `partitions_scanned / NULLIF(partitions_total, 0) > 0.8` AND partitions_total > 100 | Add clustering key or WHERE filter on partition column |
| **`int__antipattern_select_star`** | Query text LIKE `SELECT *%` or `SELECT  *%` (regex pattern) | Specify only needed columns to reduce I/O |
| **`int__antipattern_spill_to_storage`** | `bytes_spilled_to_local_storage > 0` OR `bytes_spilled_to_remote_storage > 0` | Increase warehouse size for this query, or optimize query to reduce memory |
| **`int__antipattern_repeated_queries`** | Same `query_parameterized_hash` > 10 times/day with total cost > $5 | Cache results in table, or use result caching (ensure same role/warehouse) |
| **`int__antipattern_cartesian_join`** | `rows_produced > 10 * rows_scanned` in JOIN queries | Review join conditions — likely missing or incorrect ON clause |
| **`int__antipattern_large_sort_no_limit`** | Query has ORDER BY, `rows_produced > 100000`, no LIMIT in query text | Add LIMIT clause or remove ORDER BY if full result not needed |
| **`int__antipattern_union_all`** | UNION ALL of all anti-pattern models into standardized schema: `query_id, user_name, warehouse_name, antipattern_type, severity, estimated_waste_usd, recommendation, sample_query_text` | — |

#### 6.2 New Publication Models

**`pub__query_optimization_candidates`**:
- Top 100 most impactful query optimization opportunities
- Ranked by estimated_waste_usd (highest savings first)
- Grouped by antipattern_type for summary stats
- Includes anonymized/truncated query text (first 500 chars)

**`pub__antipattern_summary`**:
- Aggregate: count of queries per antipattern type
- Total estimated waste per antipattern
- Trend: are antipatterns increasing or decreasing over time?

#### 6.3 Streamlit Page

**Page 8: Query Optimizer**:
- Summary cards: total queries with anti-patterns, total estimated waste
- Bar chart: waste by anti-pattern type
- Table: top optimization candidates with expandable query text
- Trend chart: anti-pattern frequency over last 30 days
- Filter: by anti-pattern type, by user, by warehouse

#### 6.4 Week 6 Tests

| Test | What It Validates |
|------|-------------------|
| Query with 100% partition scan flagged as full_table_scan | Positive |
| Query with 5% partition scan NOT flagged | Negative |
| Query with < 100 total partitions NOT flagged (small table) | Corner — avoid false positive on tiny tables |
| SELECT * detection works with varying whitespace | Corner |
| Spill detection catches both local and remote spill | Positive |
| Repeated query detection groups by parameterized hash (not exact text) | Positive |
| Anti-pattern with 0 estimated waste excluded from top 100 | Corner |
| Query text truncated at 500 chars (no PII leak) | Security |

---

### WEEK 7: Storage Optimization + Recommendations Engine

**Objective**: Identify storage waste and build the unified prioritized recommendations report.

#### 7.1 New Intermediate Models (5 tables)

| Model | Detection Logic |
|-------|----------------|
| **`int__storage_unused_tables`** | Join `table_storage_metrics` with `access_history`. Tables with 0 reads in 90+ days AND active_bytes > 0. Calculate storage cost of keeping them. |
| **`int__storage_time_travel_waste`** | Tables where `time_travel_bytes > active_bytes`. These have more historical data than current data — candidate for reducing retention. Estimate savings from reducing to 1-day retention. |
| **`int__storage_transient_candidates`** | Tables that are dropped and recreated frequently (detect from `table_created` timestamps repeating). These should be TRANSIENT (no fail-safe). Calculate fail-safe cost savings. |
| **`int__storage_clone_overhead`** | Tables created via CLONE (detect from `table_type = 'CLONE'`). Calculate diverged storage (active_bytes that are unique to clone). |
| **`int__storage_recommendations`** | UNION of all storage findings with: recommendation_type, table_name, current_cost, savings_if_applied, effort, action_sql. |

#### 7.2 Unified Recommendations Engine

**`pub__all_recommendations`** (master publication model):
- UNION ALL of:
  - `pub__warehouse_recommendations` (from Week 5)
  - `pub__query_optimization_candidates` (from Week 6, top 50)
  - `int__storage_recommendations` (from Week 7)
- Standardized schema:

| Column | Description |
|--------|-------------|
| recommendation_id | Auto-generated unique ID |
| category | WAREHOUSE / QUERY / STORAGE |
| recommendation_type | RESIZE / ANTI_PATTERN / UNUSED_TABLE / etc. |
| target_object | Warehouse name, query hash, table name |
| description | Human-readable recommendation |
| current_monthly_cost_usd | What it costs now |
| estimated_monthly_savings_usd | What you'd save |
| effort | LOW / MEDIUM / HIGH |
| confidence | HIGH / MEDIUM / LOW |
| priority_score | (savings × confidence_weight) / effort_weight |
| action_sql | SQL to apply the fix (when applicable) |
| category_rank | Rank within category (by savings) |
| overall_rank | Global rank across all categories |

#### 7.3 Streamlit Pages

**Page 9: Storage Optimizer**:
- Total storage cost card, waste identified card
- Treemap: storage by database (colored by waste %)
- Table: unused tables with "days since last read" and cost
- Table: TT waste candidates with savings estimate
- Table: transient table candidates

**Page 10: Recommendations Hub** (the "money page"):
- **Total Savings Banner**: "We identified **$X,XXX/month** in potential savings"
- Pie chart: savings by category (warehouse/query/storage)
- Stacked bar: savings by effort level
- Full sortable table of all recommendations
- Filters: category, effort, minimum savings threshold
- Export to CSV button
- Detail modal: click recommendation → see evidence, SQL, before/after

#### 7.4 Week 7 Tests

| Test | What It Validates |
|------|-------------------|
| Table with 0 reads in 180 days flagged as unused | Positive |
| Table read yesterday NOT flagged as unused | Negative |
| Table with no access_history entry treated as "unknown" not "unused" | Corner — access_history has 365-day retention limit |
| TT waste: table with TT_bytes=0 excluded | Negative |
| Transient candidate: table created once (never recreated) excluded | Negative |
| All recommendations have positive savings | Positive |
| Priority score correctly ranks high-savings + low-effort at top | Positive |
| pub__all_recommendations has no duplicate recommendation_ids | Positive |
| Total savings = sum of individual savings (no double-counting) | Corner |

---

### WEEK 8: Polish, Advanced Features, Final Testing & Documentation

**Objective**: Production-ready quality, comprehensive testing, documentation, and presentation materials.

#### 8.1 Advanced Features

**8.1.1 Incremental Optimization**:
- Convert `stg__query_history` to incremental (if not already)
- Add incremental strategy to `int__query_cost_attribution`
- Ensure `pub__alert_history` is append-only incremental

**8.1.2 dbt Documentation**:
- Add descriptions to ALL models and columns in YAML files
- Generate dbt docs: `snow dbt execute cost_optimization docs generate`
- Deploy dbt docs site (optional: via Streamlit iframe)

**8.1.3 Data Freshness Monitoring**:
- Add `dbt source freshness` checks
- ACCOUNT_USAGE views have up to 45-minute latency — validate this
- Add freshness warning banner to Streamlit dashboard

**8.1.4 Role-Based Access** (optional, if time permits):
- Create `COST_OPT_VIEWER` role with READ-only on PUBLICATION schema
- Create `COST_OPT_ADMIN` role with full access
- Dashboard shows/hides admin features based on role

#### 8.2 Comprehensive Test Suite

##### Positive Tests (Happy Path)
| # | Test | Expected Result |
|---|------|-----------------|
| P1 | Full `dbt build` completes without errors | All models + tests pass |
| P2 | Streamlit dashboard loads all 10 pages | No errors, all charts render |
| P3 | Teams alert fires for genuine cost spike | Adaptive Card arrives in Teams |
| P4 | Teams alert resolves when cost normalizes | Resolution tracked in history |
| P5 | Recommendations sorted by priority_score descending | Highest savings first |
| P6 | Daily cost rollup matches sum of hourly warehouse metering | Totals align within $0.01 |
| P7 | Credit pricing seed correctly applied (3.00 for Enterprise) | Cost = credits × $3.00 |
| P8 | Warehouse team mapping correctly attributes costs | Team totals sum to overall total |
| P9 | Storage analysis shows all 25 databases | No databases missing |
| P10 | Incremental run processes only new data | Row count delta = new rows only |

##### Negative Tests (Error Handling)
| # | Test | Expected Result |
|---|------|-----------------|
| N1 | Warehouse with 0 queries in period | Shows $0 cost, not NULL or error |
| N2 | User with no team mapping | Attributed to "Unassigned" team |
| N3 | Table with 0 bytes storage | Excluded from storage analysis (not divide-by-zero) |
| N4 | Query with NULL execution_time | Excluded from cost calc, not error |
| N5 | Alert configuration with all alerts disabled | Zero payload rows, no Teams messages |
| N6 | Teams webhook URL unreachable | SP logs failure, does NOT crash, retries next cycle |
| N7 | Empty ACCOUNT_USAGE view (e.g., no pipes) | Model produces 0 rows gracefully |
| N8 | Seed file with duplicate warehouse mapping | dbt test catches it (unique test on seed) |

##### Corner Cases
| # | Test | Expected Result |
|---|------|-----------------|
| C1 | First-ever run (no historical data in incremental tables) | Full load succeeds, no "table doesn't exist" error |
| C2 | Exactly 365 days of data (ACCOUNT_USAGE retention boundary) | No off-by-one: 365th day included |
| C3 | Warehouse resized mid-day (XS→M at 2pm) | Cost calculation uses correct credits_per_hour per interval |
| C4 | Query spanning midnight (start 23:50, end 00:10) | Attributed to correct date (end_time date) |
| C5 | Zero credits used but cloud_services > 0 | Cloud services cost still captured |
| C6 | Same query hash run by different users | Attributed separately per user |
| C7 | Division by zero: warehouse with 0 total partitions | safe_divide macro returns NULL, not error |
| C8 | Alert fires, resolves, fires again | New episode created (episode_number increments) |
| C9 | 100+ concurrent alerts in single cycle | Stored procedure handles batch, doesn't timeout |
| C10 | dbt run with no new data (idempotent re-run) | No errors, no duplicate rows, same row counts |
| C11 | Snowflake credit price change mid-month | Handled via effective_from/effective_to in seed |
| C12 | Warehouse_team_mapping has a warehouse not in metering data | LEFT JOIN: cost = 0, team still shows |

#### 8.3 Documentation Deliverables

1. **User Guide** (Streamlit "Help" page or separate doc):
   - How to read each dashboard page
   - How to configure alert routing (edit CSV → re-seed)
   - How to add a new team to warehouse mapping
   - How to apply a recommendation

2. **Technical Guide** (in-repo README or dbt docs):
   - Architecture diagram
   - Data flow diagram
   - Model dependency graph (from dbt docs)
   - How to re-deploy after changes
   - How to add a new alert type
   - How to add a new recommendation type

3. **Runbook** (operational):
   - How to verify pipeline health
   - How to troubleshoot failed Teams alerts
   - How to enable/disable alerting
   - How to check data freshness

#### 8.4 Week 8 Deliverables
- [ ] All models incremental-optimized where applicable
- [ ] Full dbt docs generated
- [ ] Complete test suite (30+ tests) all passing
- [ ] All 10 Streamlit pages polished and working
- [ ] User guide complete
- [ ] Technical documentation complete
- [ ] Final stakeholder walkthrough presentation ready
- [ ] `snow dbt execute cost_optimization build` — full pipeline green
- [ ] End-to-end demo script prepared

---

## Complete File Inventory

### dbt Models (Total: 53 files)

| Layer | Count | Models |
|-------|-------|--------|
| Staging | 14 | stg__query_history, stg__warehouse_metering_history, stg__warehouse_load_history, stg__table_storage_metrics, stg__storage_usage, stg__database_storage_usage_history, stg__login_history, stg__access_history, stg__automatic_clustering_history, stg__materialized_view_refresh_history, stg__pipe_usage_history, stg__serverless_task_history, stg__sessions, stg__search_optimization_history |
| Intermediate (Cost) | 10 | int__warehouse_daily_credits, int__query_cost_attribution, int__warehouse_utilisation, int__user_cost_summary, int__storage_breakdown, int__serverless_credits, int__idle_warehouse_periods, int__query_patterns, int__daily_cost_rollup, int__team_cost_attribution |
| Intermediate (Alerts) | 8 | int__alert_cost_daily_spike, int__alert_warehouse_idle, int__alert_credit_budget, int__alert_query_spill, int__alert_storage_growth, int__alert_repeated_expensive, int__alert_union_all, int__alert_state_tracker |
| Intermediate (Phase 2) | 16 | int__warehouse_sizing_analysis, int__warehouse_queue_analysis, int__warehouse_auto_suspend_analysis, int__warehouse_schedule_analysis, int__antipattern_full_table_scan, int__antipattern_select_star, int__antipattern_spill_to_storage, int__antipattern_repeated_queries, int__antipattern_cartesian_join, int__antipattern_large_sort_no_limit, int__antipattern_union_all, int__storage_unused_tables, int__storage_time_travel_waste, int__storage_transient_candidates, int__storage_clone_overhead, int__storage_recommendations |
| Publication | 13 | pub__cost_summary, pub__cost_by_warehouse, pub__cost_by_user, pub__cost_by_query_type, pub__storage_analysis, pub__cost_trends_daily, pub__warehouse_efficiency, pub__team_cost_dashboard, pub__warehouse_recommendations, pub__query_optimization_candidates, pub__antipattern_summary, pub__teams_alert_payload, pub__alert_history, pub__all_recommendations |

### Seeds (5 files)
credit_pricing.csv, warehouse_size_credits.csv, warehouse_team_mapping.csv, alert_configuration.csv, monthly_budget.csv

### Macros (3 files)
generate_schema_name.sql, safe_divide.sql, generate_alert_detection.sql (optional helper)

### Tests (32+ tests)
10 positive, 8 negative, 12 corner cases, 2+ singular custom tests

### Streamlit (15+ files)
app.py, 10 pages, utils/ (3 files), snowflake.yml, environment.yml

### Snowflake Objects (1 setup script)
setup_alerts_infrastructure.sql (secret, network rule, external access, stored procedure, tasks)

---

## Execution Order (Implementation Sequence)

### Already Completed
- [x] Step 1: Snowflake objects created (database, schemas, warehouse)
- [x] Step 2: Snowflake CLI connection added and verified

### Week 1 Implementation
1. Create dbt project structure (dbt_project.yml, profiles.yml, packages.yml)
2. Create generate_schema_name macro + safe_divide macro
3. Create sources.yml (14 sources)
4. Create all 14 staging models
5. Create all seed CSV files
6. Create staging YAML docs + tests (_stg__models.yml)
7. Deploy: `snow dbt deploy` → `snow dbt execute deps` → `seed` → `run` → `test`

### Week 2 Implementation
8. Create all 10 intermediate cost models
9. Create all 8 initial publication models
10. Create intermediate + publication YAML docs + tests
11. Deploy and validate all models
12. Spot-check publication data against raw ACCOUNT_USAGE queries

### Week 3 Implementation
13. Create Streamlit app structure (app.py, utils/, config)
14. Build pages 1-5 (Executive Summary through Trend Analysis)
15. Deploy Streamlit to Snowflake
16. Validate all pages render with real data

### Week 4 Implementation
17. Create alert_configuration.csv seed + monthly_budget.csv seed
18. Create 7 alert detection intermediate models
19. Create alert state tracker model
20. Create Teams payload publication model + alert history model
21. Create Snowflake alert infrastructure (secret, network rule, integration, SP, task)
22. Add Alert Management page (#6) to Streamlit
23. End-to-end alert test: verify Teams message delivery

### Week 5 Implementation
24. Create 4 warehouse analysis intermediate models
25. Create pub__warehouse_recommendations
26. Add Warehouse Optimizer page (#7) to Streamlit
27. Validate recommendations against manual analysis

### Week 6 Implementation
28. Create 7 anti-pattern detection intermediate models
29. Create pub__query_optimization_candidates + pub__antipattern_summary
30. Add Query Optimizer page (#8) to Streamlit
31. Validate anti-pattern detection against known queries

### Week 7 Implementation
32. Create 5 storage optimization intermediate models
33. Create pub__all_recommendations (unified)
34. Add Storage Optimizer page (#9) + Recommendations Hub page (#10) to Streamlit
35. Validate savings estimates

### Week 8 Implementation
36. Optimize incremental models
37. Generate dbt docs
38. Run complete test suite (positive + negative + corner cases)
39. Polish Streamlit UI (formatting, error handling, loading states)
40. Write user guide + technical docs + runbook
41. Final end-to-end validation
42. Prepare demo script

---

## Verification Checklist (Final State)

- [ ] `snow connection test --connection cost_optimization` → OK
- [ ] `snow dbt execute cost_optimization build` → all models + tests green
- [ ] `SELECT COUNT(*) FROM PUBLICATION.PUB__COST_SUMMARY` → rows > 0
- [ ] `SELECT COUNT(*) FROM PUBLICATION.PUB__ALL_RECOMMENDATIONS` → rows > 0
- [ ] Streamlit dashboard: all 10 pages load without errors
- [ ] Teams alert: trigger test spike → message received in Teams channel
- [ ] Teams alert: disable alert in seed → re-seed → confirm no messages
- [ ] Recommendations: total savings banner shows realistic dollar amount
- [ ] dbt docs: browsable model documentation
- [ ] Full test suite: 32+ tests passing
- [ ] .gitignore: no secrets committed
- [ ] CLAUDE.md: updated with complete project structure
