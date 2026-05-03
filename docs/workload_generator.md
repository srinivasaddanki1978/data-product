# Workload Generator — Demo Scenario Reference

## Overview

The workload generator creates intentional Snowflake workloads that trigger every detection mechanism in the cost optimization framework. It produces realistic anti-pattern queries across 10 scenarios, enabling end-to-end validation of the pipeline from raw query execution through to dashboard visualization and Teams alerting.

## Prerequisites

- **TPC-DS share**: The `SNOWFLAKE_SAMPLE_DATA` database must be available (shared by default on all Snowflake accounts). Scenarios query `TPCDS_SF10TCL` tables (INVENTORY, CUSTOMER, PROMOTION, etc.)
- **Demo warehouses**: `ANALYTICS_WH` (Small) and `ETL_WH` (Medium) — created by `setup_demo_environment.py`
- **Default warehouse**: `COMPUTE_WH` (X-Small) — used by most scenarios
- **Python**: `snowflake-snowpark-python` installed

## Quick Start

```bash
# 1. Create demo infrastructure (one-time)
python workload_generator/setup_demo_environment.py

# 2. Run all 10 workload scenarios
python workload_generator/generate_workloads.py --scenario all

# 3. Wait 45 min–3 hours for ACCOUNT_USAGE latency, then rebuild dbt
snow dbt execute cost_optimization build
```

## Scenarios

| # | Scenario | Anti-Pattern Triggered | Warehouse | Query Description |
|---|----------|----------------------|-----------|-------------------|
| 1 | `full_table_scan` | Full table scan | COMPUTE_WH | Scans all partitions on INVENTORY (1.3B rows) with no date filter |
| 2 | `select_star` | SELECT * | COMPUTE_WH | SELECT * on large multi-table joins returning all columns |
| 3 | `spill_to_storage` | Excessive spilling | COMPUTE_WH | Complex window functions (7-day MA, 30-day volatility, z-scores) forcing memory spill on X-Small |
| 4 | `repeated_query` | Repeated expensive query | COMPUTE_WH | Same parameterized query structure run 25 times with different date values |
| 5 | `cartesian_join` | Cartesian join | COMPUTE_WH | PROMOTION x HOUSEHOLD_DEMOGRAPHICS x CUSTOMER_DEMOGRAPHICS with no join conditions |
| 6 | `large_sort_no_limit` | Large sort without LIMIT | COMPUTE_WH | ORDER BY on 60M customer rows without LIMIT |
| 7 | `expensive_join` | High compilation / expensive | COMPUTE_WH | Complex multi-table inventory valuation with CTEs and cumulative window functions |
| 8 | `cost_spike` | Daily cost spike | COMPUTE_WH | 4 heavy queries fired rapidly to trigger cost anomaly detection |
| 9 | `multi_warehouse` | Multi-team attribution | COMPUTE_WH, ANALYTICS_WH, ETL_WH | Same query across 3 warehouses for cost attribution comparison |
| 10 | `idle_warehouse` | Warehouse idle | ANALYTICS_WH | Resume warehouse and let it idle to trigger idle detection |

## Script Reference

### generate_workloads.py

The main scenario runner. Executes tagged queries against Snowflake sample data.

```bash
# Run all scenarios
python workload_generator/generate_workloads.py --scenario all

# Run a single scenario
python workload_generator/generate_workloads.py --scenario spill_to_storage

# List available scenarios
python workload_generator/generate_workloads.py --list
```

**CLI Arguments:**
| Argument | Description | Default |
|----------|-------------|---------|
| `--scenario` | Scenario name or `all` | `all` |
| `--list` | List available scenarios and exit | — |

**Query Tagging**: All queries are tagged with structured metadata: `team:{team};scenario:{scenario};run_id:{uuid};query:{n}`

**Logging**: Results are logged to `COST_OPTIMIZATION_DB.WORKLOADS.WORKLOAD_LOG` with run_id, scenario, warehouse, status, query_id, and completion time.

### demo_runner.py

End-to-end demo orchestrator that combines all steps into a single workflow.

```bash
# Full pipeline
python workload_generator/demo_runner.py --step all

# Individual steps
python workload_generator/demo_runner.py --step setup
python workload_generator/demo_runner.py --step scan
python workload_generator/demo_runner.py --step run
python workload_generator/demo_runner.py --step verify
python workload_generator/demo_runner.py --step check

# Quick demo (3 key scenarios)
python workload_generator/demo_runner.py --step quick-demo
```

**CLI Arguments:**
| Argument | Description | Default |
|----------|-------------|---------|
| `--step` | Step to run: `setup`, `scan`, `run`, `verify`, `check`, `all`, `quick-demo` | `all` |

**Steps:**
1. **setup** — Creates demo warehouses and workload logging schema
2. **scan** — Discovers current environment via `scan_environment.py`
3. **run** — Executes all 10 workload scenarios
4. **verify** — Queries ACCOUNT_USAGE to validate workload execution
5. **check** — Validates publication tables have new data post-dbt-build

### scan_environment.py

Discovers Snowflake account resources and saves metadata as JSON.

```bash
python workload_generator/scan_environment.py
```

**No CLI arguments.** Outputs a console summary and writes `environment_scan.json` with:
- Databases, warehouses, roles, users
- Tables with data (row counts, bytes)
- Query history stats (90-day)
- Warehouse metering (all-time credits)

### setup_demo_environment.py

One-time setup for demo infrastructure.

```bash
python workload_generator/setup_demo_environment.py
```

**No CLI arguments.** Creates:
- `ANALYTICS_WH` (Small, auto-suspend 60s) — simulates analytics team
- `ETL_WH` (Medium, auto-suspend 60s) — simulates data engineering team
- Required schemas (STAGING, INTERMEDIATE, PUBLICATION, SEEDS)
- `WORKLOADS.WORKLOAD_LOG` table for execution logging

## Verification

After running workloads and waiting for ACCOUNT_USAGE latency (45 min–3 hours):

```sql
-- Check tagged queries appeared
SELECT QUERY_TAG, QUERY_TYPE, WAREHOUSE_NAME,
       TOTAL_ELAPSED_TIME/1000 AS seconds,
       BYTES_SCANNED, BYTES_SPILLED_TO_LOCAL_STORAGE
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TAG LIKE '%scenario:%'
ORDER BY START_TIME DESC
LIMIT 20;

-- Check workload log
SELECT * FROM COST_OPTIMIZATION_DB.WORKLOADS.WORKLOAD_LOG
ORDER BY completed_at DESC
LIMIT 20;
```

## Demo Runner Workflow

The recommended demo flow:

1. **Setup** — `python workload_generator/setup_demo_environment.py`
2. **Generate** — `python workload_generator/generate_workloads.py --scenario all`
3. **Wait** — Allow 45 min–3 hours for ACCOUNT_USAGE data to populate
4. **Build** — Run `dbt build` to process new query history through the pipeline
5. **Verify** — Open the Streamlit dashboard and explore anti-pattern detection results, cost attribution, and alert generation
