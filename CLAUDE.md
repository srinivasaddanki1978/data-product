# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains a **Snowflake Cost Optimisation Framework** — a production-grade cost visibility, optimization, and alerting system built on Snowflake, designed as an internal accelerator deployable to any customer (e.g., TR — Thomson Reuters).

## Repository Structure

```
data-product/
├── README.md
├── CLAUDE.md
├── .gitignore
├── setup_snowflake_objects.py              # Snowflake DB/schema/warehouse setup
├── Connect-token-secret.txt                # Auth token (DO NOT COMMIT)
│
├── docs/                                    # Model & seed documentation
│   ├── models_staging.md                    # 15 staging model docs
│   ├── models_intermediate.md               # 31 intermediate model docs
│   ├── models_publication.md                # 16 publication model docs
│   ├── models_alerts.md                     # 10 alert model docs
│   └── seeds.md                             # 8 seed file docs
│
├── cost_optimization_dbt/                   # dbt project (Snowflake native dbt)
│   ├── dbt_project.yml                      # Project config + on-run-end hook
│   ├── profiles.yml                         # Connection config
│   ├── packages.yml                         # dbt_utils dependency
│   ├── models/
│   │   ├── sources.yml                      # 14 ACCOUNT_USAGE source definitions
│   │   ├── staging/                         # 15 view models (clean ACCOUNT_USAGE mirrors)
│   │   ├── intermediate/                    # 31 table models (business logic)
│   │   ├── publication/                     # 16 table models (dashboard-ready)
│   │   └── alerts/                          # 10 models (detection + suppression + state + payload)
│   ├── seeds/                               # 8 CSV reference/config files
│   ├── macros/
│   │   ├── dbt_overrides/                   # generate_schema_name
│   │   └── helpers/                         # safe_divide, call_send_teams_alerts
│   ├── tests/                               # Custom generic + singular tests (168 total)
│   └── snowflake_objects/                   # Snowflake infrastructure SQL
│       ├── setup_alerts_infrastructure.sql  # Procedures, tasks, network rules
│       └── setup_webhook_secrets.sql        # Per-channel Teams webhook secrets
│
└── streamlit_app/                           # Streamlit-in-Snowflake dashboard
    ├── app.py                               # Main entry: KPIs + cost donut
    ├── pages/                               # 10 interactive pages
    │   ├── 1_Executive_Summary.py
    │   ├── 2_Warehouse_Deep_Dive.py
    │   ├── 3_Team_Attribution.py
    │   ├── 4_Storage_Explorer.py
    │   ├── 5_Trend_Analysis.py
    │   ├── 6_Alert_Management.py
    │   ├── 7_Warehouse_Optimizer.py
    │   ├── 8_Query_Optimizer.py
    │   ├── 9_Storage_Optimizer.py
    │   └── 10_Recommendations_Hub.py
    ├── utils/                               # connection, queries, formatters
    ├── snowflake.yml                        # Streamlit-in-Snowflake config
    └── environment.yml                      # Python dependencies
```

## Snowflake Connection

- **Account**: `chc70950.us-east-1`
- **User**: `SRINIVAS`
- **Role**: `ACCOUNTADMIN`
- **Database**: `COST_OPTIMIZATION_DB`
- **Schemas**: `STAGING`, `INTERMEDIATE`, `PUBLICATION`, `SEEDS`
- **Warehouse**: `COST_OPT_WH` (X-Small, auto-suspend 60s)
- **CLI Connection**: `cost_optimization` in `~/.snowflake/connections.toml`
- **Token**: Stored in `Connect-token-secret.txt` (do not commit)

## dbt Commands

```bash
# Deploy and run full pipeline
snow dbt deploy cost_optimization
snow dbt execute cost_optimization deps
snow dbt execute cost_optimization seed
snow dbt execute cost_optimization run
snow dbt execute cost_optimization test

# Full build (seed + run + test)
snow dbt execute cost_optimization build

# Full refresh (needed when adding new columns to incremental models or seeds)
snow dbt execute cost_optimization build -- --full-refresh

# Generate docs
snow dbt execute cost_optimization docs generate
```

## Deploy Alert Infrastructure

```bash
# Deploy procedures, tasks, and network rules
snow sql --connection cost_optimization --enable-templating NONE \
  -f cost_optimization_dbt/snowflake_objects/setup_alerts_infrastructure.sql

# Deploy per-channel webhook secrets (update URLs in file first)
snow sql --connection cost_optimization --enable-templating NONE \
  -f cost_optimization_dbt/snowflake_objects/setup_webhook_secrets.sql
```

Note: Use `--enable-templating NONE` to prevent `snow sql` from interpreting `&` in webhook URLs as template variables.

## Streamlit Deployment

```bash
cd streamlit_app
snow streamlit deploy --connection cost_optimization
```

## Architecture

- **Source**: `SNOWFLAKE.ACCOUNT_USAGE` (14 views, 365-day retention)
- **Staging**: 15 view models — clean column names, type casting, no business logic
- **Intermediate**: 31 table models — cost attribution, utilization, anti-patterns, anomaly detection, forecasting, alert state tracking, warehouse sizing, storage optimization
- **Publication**: 16 table models — dashboard-ready aggregations, executive reports, ROI tracking
- **Alerts**: 10 models — 7 alert types, suppression filtering (rules + bank holidays), episode-based deduplication, Teams Adaptive Card payloads
- **Seeds**: 8 CSV files — credit pricing, alert configuration, budgets, suppressions, bank holidays, recommendation actions
- **Streamlit**: 10-page interactive dashboard with drill-down, filtering, CSV export
- **on-run-end**: Automatic Teams alert delivery after every `dbt run`/`dbt build` via `call_send_teams_alerts` macro

## Alert Pipeline

```
Detection (6 models) → Suppression (union_all) → State Tracker (episode dedup) → Teams Payload → send_teams_alerts()
```

- **Suppression**: `alert_suppressions.csv` for targeted rules, `bank_holidays.csv` for holiday suppression
- **Channels**: `cost-alerts` (P2/P3 operational), `finance-alerts` (P0/P1 budget)
- **Delivery**: Power Automate webhooks, per-channel routing via Snowflake secrets
- **Hook**: `on-run-end` calls `send_teams_alerts()` automatically after successful dbt runs

## Key Design Decisions

- Team/user attribution derived dynamically from `ACCOUNT_USAGE.QUERY_HISTORY` (role→team mapping via `stg__warehouse_role_usage`) rather than static seed files
- `stg__query_history` is incremental (merge on `query_id`) to avoid re-scanning 571K+ rows
- Credit pricing uses ENTERPRISE edition ($3.00/credit) — configurable via `credit_pricing.csv` seed
- Alert state tracker uses episode-based deduplication to prevent alert fatigue
- Alert suppression supports targeted rules (alert_id + resource + date range) and bank holidays (configurable per alert via `suppress_on_holidays`)
- `generate_schema_name` macro overridden so schemas are used as-is (STAGING, not PUBLIC_STAGING)
- `safe_divide` macro prevents divide-by-zero across all cost calculations
- `call_send_teams_alerts` macro checks `INFORMATION_SCHEMA.PROCEDURES` before calling to avoid failures when procedure is not deployed
- Seasonality-aware anomaly detection uses day-of-week, day-of-month, and month-end baselines with z-score thresholds

## Key Domain Context

- **Customer**: TR (Thomson Reuters) — large Snowflake data lake on AWS, 350+ source systems
- **Demo environment**: Bilvantis Snowflake account with 25 databases, 3 warehouses, 571K+ query history rows
- **Tensor**: Internal accelerator name for agentic data platform built on Snowflake Cortex

## Proposal Phases

- **Phase 1 (Weeks 1-4)**: Cost visibility and attribution — staging models, intermediate business logic, publication layer, Streamlit dashboard, Teams alerting with episode tracking
- **Phase 2 (Weeks 5-8)**: Query optimisation and recommendations — warehouse right-sizing, anti-pattern detection (6 types), storage optimization, unified recommendations engine with priority scoring
