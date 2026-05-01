# Snowflake Cost Optimisation Framework

A production-grade cost visibility, optimization, and alerting system built on Snowflake. Designed as an internal accelerator deployable to any Snowflake customer.

## What It Does

- **Cost Visibility** — Daily/monthly cost breakdowns by warehouse, user, team, query type, and storage
- **Anti-Pattern Detection** — Automatically identifies 6 types of wasteful query patterns (full table scans, SELECT *, spill to storage, repeated expensive queries, cartesian joins, large sorts without LIMIT)
- **Warehouse Optimization** — Right-sizing recommendations, auto-suspend tuning, and off-peak scheduling with ready-to-run ALTER SQL
- **Storage Optimization** — Finds unused tables (90+ days), time travel waste, transient candidates, and clone overhead
- **Alerting** — 7 configurable alert types with episode-based deduplication, Microsoft Teams delivery, holiday suppression, and targeted suppression rules
- **Forecasting** — 90-day cost projections with confidence intervals, per-team monthly forecasts
- **Recommendations Engine** — Unified priority-scored recommendations across warehouse, query, and storage with ROI tracking
- **Interactive Dashboard** — 10-page Streamlit-in-Snowflake app with drill-down, filtering, and CSV export

## Architecture

```
SNOWFLAKE.ACCOUNT_USAGE (14 views)
        |
   [ Staging ]         15 views — clean column names, type casting
        |
  [ Intermediate ]     31 tables — cost attribution, anti-patterns, forecasts, alerts
        |
  [ Publication ]      16 tables — dashboard-ready aggregations
        |
  +-----+------+
  |            |
Streamlit    Teams Alerts
(10 pages)   (7 alert types, 2 channels)
```

## Repository Structure

```
data-product/
├── cost_optimization_dbt/          # dbt project (72 models, 168 tests, 8 seeds)
│   ├── models/
│   │   ├── staging/                # 15 clean ACCOUNT_USAGE mirrors
│   │   ├── intermediate/           # 31 business logic models
│   │   ├── publication/            # 16 dashboard-ready models
│   │   └── alerts/                 # 10 alert pipeline models
│   ├── seeds/                      # 8 configuration CSVs
│   ├── macros/                     # generate_schema_name, safe_divide, call_send_teams_alerts
│   ├── tests/                      # Custom generic + singular tests
│   └── snowflake_objects/          # Alert infrastructure SQL (procedures, tasks, secrets)
│
├── streamlit_app/                  # Streamlit-in-Snowflake dashboard
│   ├── app.py                      # Main entry: KPIs + cost donut chart
│   └── pages/                      # 10 interactive pages
│
├── docs/                           # Model and seed documentation
│   ├── models_staging.md
│   ├── models_intermediate.md
│   ├── models_publication.md
│   ├── models_alerts.md
│   └── seeds.md
│
└── setup_snowflake_objects.py      # Initial DB/schema/warehouse setup
```

## Prerequisites

- Snowflake account with `ACCOUNTADMIN` role (for `ACCOUNT_USAGE` access)
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (`snow`) installed
- Connection named `cost_optimization` in `~/.snowflake/connections.toml`

## Quick Start

### 1. Set Up Snowflake Objects

```bash
python setup_snowflake_objects.py
```

Creates the `COST_OPTIMIZATION_DB` database with schemas: `STAGING`, `INTERMEDIATE`, `PUBLICATION`, `SEEDS`.

### 2. Deploy and Run dbt

```bash
# Deploy the dbt project to Snowflake
snow dbt deploy cost_optimization

# Install dependencies
snow dbt execute cost_optimization deps

# Load seed data (credit pricing, alert config, bank holidays, etc.)
snow dbt execute cost_optimization seed

# Build all models and run tests
snow dbt execute cost_optimization build
```

### 3. Deploy Alert Infrastructure (Optional)

```bash
# Create procedures, tasks, and webhook secrets
snow sql --connection cost_optimization --enable-templating NONE \
  -f cost_optimization_dbt/snowflake_objects/setup_alerts_infrastructure.sql

# Set up per-channel webhook secrets (update URLs first)
snow sql --connection cost_optimization --enable-templating NONE \
  -f cost_optimization_dbt/snowflake_objects/setup_webhook_secrets.sql
```

### 4. Deploy Streamlit Dashboard

**Option A: Snowflake CLI**

```bash
cd streamlit_app
snow streamlit deploy --connection cost_optimization
```

> **Note**: `snow streamlit deploy` may fail with certain account formats (e.g., `chc70950.us-east-1`). If so, use Option B.

**Option B: deploy_sis.py (recommended)**

Automated deployment script that bypasses `snow streamlit deploy` using direct Snowpark PUT + CREATE STREAMLIT commands.

```bash
cd streamlit_app
python deploy_sis.py
```

Reads connection config from `~/.snowflake/connections.toml`, uploads all files with cache-busting timestamps, and creates the Streamlit app. Requires `snowflake-snowpark-python`.

## dbt Model Layers

| Layer | Models | Materialization | Purpose |
|---|---|---|---|
| **Staging** | 15 | Views (1 incremental) | Clean mirrors of ACCOUNT_USAGE |
| **Intermediate** | 31 | Tables | Cost attribution, anti-patterns, forecasts, storage analysis |
| **Publication** | 16 | Tables (2 incremental) | Dashboard-ready aggregations |
| **Alerts** | 10 | Tables (2 incremental) | Detection, suppression, state tracking, Teams delivery |

See `docs/` for detailed documentation of every model and seed.

## Streamlit Dashboard Pages

| Page | Description |
|---|---|
| Home | KPI cards + cost breakdown donut chart |
| Executive Summary | Weekly report, cost trends, forecast |
| Warehouse Deep Dive | Per-warehouse cost, utilization, efficiency scores |
| Team Attribution | Cost by team with MoM trends |
| Storage Explorer | Table-level storage breakdown with unused/waste flags |
| Trend Analysis | Daily cost time series with anomaly highlighting |
| Alert Management | Active alerts, history, suppression rules |
| Warehouse Optimizer | Resize, auto-suspend, scheduling recommendations |
| Query Optimizer | Anti-pattern detection results with optimization guidance |
| Storage Optimizer | Unused tables, time travel waste, transient candidates |
| Recommendations Hub | Unified priority-ranked recommendations with ROI tracking |

## Alert Types

| Alert | Severity | Teams Channel | Holiday Suppressed |
|---|---|---|---|
| Daily Cost Spike (seasonality-aware) | P1 | cost-alerts | No |
| Warehouse Idle (>30 min) | P2 | cost-alerts | Yes |
| Budget 80% Warning | P1 | finance-alerts | No |
| Budget 100% Exceeded | P0 | finance-alerts | No |
| Heavy Query Spill (>1GB remote) | P2 | cost-alerts | Yes |
| Storage Growth (>20% WoW) | P3 | cost-alerts | Yes |
| Repeated Expensive Query (>20/day) | P2 | cost-alerts | Yes |

Alerts are automatically sent to Microsoft Teams after every `dbt build` via the `on-run-end` hook. P0/P1 critical alerts fire on holidays; P2/P3 are suppressed.

## Configuration

All configuration is managed through seed CSV files — no code changes needed:

| Seed | Purpose |
|---|---|
| `credit_pricing.csv` | Credit price per Snowflake edition |
| `alert_configuration.csv` | Alert thresholds, channels, severity, holiday suppression |
| `monthly_budget.csv` | Monthly credit/USD budgets for budget alerts |
| `alert_suppressions.csv` | Targeted alert suppression rules (maintenance windows) |
| `bank_holidays.csv` | Holiday calendar for alert suppression (India 2025-2026) |
| `recommendation_actions.csv` | Track recommendation lifecycle (OPEN/ACCEPTED/IMPLEMENTED) |

## Key Design Decisions

- **Dynamic team attribution** from `QUERY_HISTORY` (role/tag/warehouse patterns) — no static mapping required
- **Incremental `stg__query_history`** — merge on `query_id` to avoid re-scanning 571K+ rows
- **Episode-based alert deduplication** — prevents alert fatigue by grouping related firings
- **Seasonality-aware anomaly detection** — day-of-week, day-of-month, and month-end baselines with z-score thresholds
- **Safe `on-run-end` hook** — checks if `send_teams_alerts()` procedure exists before calling

## Documentation

Detailed model documentation is in the `docs/` folder:

- [Staging Models](docs/models_staging.md) — 15 source mirrors
- [Intermediate Models](docs/models_intermediate.md) — 31 business logic models
- [Publication Models](docs/models_publication.md) — 16 dashboard-ready models
- [Alert Models](docs/models_alerts.md) — 10 alert pipeline models + infrastructure
- [Seeds](docs/seeds.md) — 8 configuration files
