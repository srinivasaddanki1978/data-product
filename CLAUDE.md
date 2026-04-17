# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains a **Snowflake Cost Optimisation Framework** — a production-grade cost visibility, optimization, and alerting system built on Snowflake, designed as an internal accelerator deployable to any customer (e.g., TR — Thomson Reuters).

## Repository Structure

```
data-product/
├── CLAUDE.md
├── .gitignore
├── setup_snowflake_objects.py              # Snowflake DB/schema/warehouse setup
├── Connect-token-secret.txt                # Auth token (DO NOT COMMIT)
│
├── docs/                                    # Proposal docs & meeting transcripts
│   ├── snowflake_cost_optimisation_framework.md
│   ├── implementation_plan.md               # Full 8-week implementation plan
│   ├── generate_proposal_docx.py
│   ├── generate_cost_optimisation_docs.py
│   └── *.docx / *.pptx
│
├── cost_optimization_dbt/                   # dbt project (Snowflake native dbt)
│   ├── dbt_project.yml
│   ├── profiles.yml                         # Connection config (gitignored)
│   ├── packages.yml                         # dbt_utils dependency
│   ├── models/
│   │   ├── sources.yml                      # 14 ACCOUNT_USAGE source definitions
│   │   ├── staging/                         # 15 view models (clean ACCOUNT_USAGE mirrors)
│   │   ├── intermediate/                    # 26 table models (business logic)
│   │   ├── publication/                     # 12 table models (dashboard-ready)
│   │   └── alerts/                          # 10 models (detection + state + payload)
│   ├── seeds/                               # 5 CSV reference files
│   ├── macros/                              # generate_schema_name, safe_divide
│   ├── tests/                               # 8 custom tests (generic + singular)
│   └── snowflake_objects/                   # Teams alert infrastructure SQL
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

# Generate docs
snow dbt execute cost_optimization docs generate
```

## Streamlit Deployment

```bash
cd streamlit_app
snow streamlit deploy --connection cost_optimization
```

## Architecture

- **Source**: `SNOWFLAKE.ACCOUNT_USAGE` (14 views, 365-day retention)
- **Staging**: 15 view models — clean column names, type casting, no business logic
- **Intermediate**: 26 table models — cost attribution, utilization, anti-patterns, anomaly detection, alert state tracking, warehouse sizing, storage optimization
- **Publication**: 12 table models — dashboard-ready aggregations
- **Alerts**: Episode-based deduplication, Teams Adaptive Card payloads, scheduled webhook delivery
- **Streamlit**: 10-page interactive dashboard with drill-down, filtering, CSV export

## Key Design Decisions

- Team/user attribution derived dynamically from `ACCOUNT_USAGE.QUERY_HISTORY` (role→team mapping via `stg__warehouse_role_usage`) rather than static seed files
- `stg__query_history` is incremental (merge on `query_id`) to avoid re-scanning 571K+ rows
- Credit pricing uses ENTERPRISE edition ($3.00/credit) — configurable via `credit_pricing.csv` seed
- Alert state tracker uses episode-based deduplication to prevent alert fatigue
- `generate_schema_name` macro overridden so schemas are used as-is (STAGING, not PUBLIC_STAGING)
- `safe_divide` macro prevents divide-by-zero across all cost calculations

## Key Domain Context

- **Customer**: TR (Thomson Reuters) — large Snowflake data lake on AWS, 350+ source systems
- **Demo environment**: Bilvantis Snowflake account with 25 databases, 3 warehouses, 571K+ query history rows
- **Tensor**: Internal accelerator name for agentic data platform built on Snowflake Cortex

## Proposal Phases

- **Phase 1 (Weeks 1–4)**: Cost visibility and attribution — staging models, intermediate business logic, publication layer, Streamlit dashboard (5 tabs), Teams alerting with episode tracking
- **Phase 2 (Weeks 5–8)**: Query optimisation and recommendations — warehouse right-sizing, anti-pattern detection (6 types), storage optimization, unified recommendations engine with priority scoring
