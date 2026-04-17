# Workload Generation & Live Metadata Plan

## Why This Exists

The original plan relied on **seed files** (static CSVs) to feed reference data and assumed the framework would passively read whatever existed in `ACCOUNT_USAGE`. For client demos, we need to be **active**:

1. **Scan** the Snowflake environment — know what databases, tables, warehouses, roles exist
2. **Generate intentional workloads** — run queries that trigger every anti-pattern, cost spike, and alert
3. **Track in real-time** — show the framework detecting and reporting what just happened
4. **Replace seed assumptions** — derive warehouse sizes, team mappings from live Snowflake metadata

## Environment Discovery (Actual Scan Results)

### Databases (26 total)
| Database | Type | Notable Tables |
|----------|------|---------------|
| TPCDS_10TB | IMPORTED (shared) | STORE_SALES (28.8B rows, 1.17TB), CATALOG_SALES (14.4B, 871GB), WEB_SALES (7.2B, 436GB), CUSTOMER (65M), ITEM (402K), INVENTORY (1.3B) |
| GLOBALMART | Standard | FACT_SALES (6K), RAW_TRANSACTIONS (10K), DIM_PRODUCTS (1.7K) |
| BRAZIL_ECOMMERCE_DB | Standard | GEOLOCATION (1M), ORDERS (99K), ORDER_ITEMS (113K) |
| COST_OPTIMIZATION_DB | Standard | Framework models (staging/intermediate/publication) |
| 22 others | Various | Mostly small/empty demo databases |

### Warehouses (6 total — after demo setup)
| Warehouse | Size | Credits/hr | Team | Purpose |
|-----------|------|------------|------|---------|
| COMPUTE_WH | X-Small | 1 | Platform | General workload (286 credits used historically) |
| COST_OPT_WH | X-Small | 1 | Cost Optimization | Framework dbt builds |
| ANALYTICS_WH | Small | 2 | Analytics | **NEW** — Demo analytics team |
| ETL_WH | Medium | 4 | Data Engineering | **NEW** — Demo ETL team |
| SNOWFLAKE_LEARNING_WH | X-Small | 1 | Training | Learning exercises |
| SYSTEM$STREAMLIT_NOTEBOOK_WH | X-Small | 1 | System | Streamlit runtime |

### Users (12) & Roles (16)
- 12 real users across the account
- 16 roles including ACCOUNTADMIN, ANALYST_ROLE, FINANCE_ROLE, TENSOR_ROLE, etc.
- 587K historical queries across 17 distinct users, 11 warehouses

## Workload Scenarios (10 Types)

### Anti-Pattern Detection Scenarios

| # | Scenario | What It Triggers | Warehouse | Key Queries |
|---|----------|-----------------|-----------|-------------|
| 1 | **Full Table Scan** | `int__antipattern_full_table_scan` | COMPUTE_WH | Scan INVENTORY (1.3B rows), CUSTOMER (65M) — partitions_scanned/total > 0.8 |
| 2 | **SELECT *** | `int__antipattern_select_star` | COMPUTE_WH | `SELECT * FROM CUSTOMER LIMIT 500K`, `SELECT * FROM GEOLOCATION LIMIT 100K` |
| 3 | **Spill to Storage** | `int__antipattern_spill_to_storage` | COMPUTE_WH (XS) | Window functions on INVENTORY — X-Small memory forces spill |
| 4 | **Repeated Queries** | `int__antipattern_repeated_queries` + alert | ANALYTICS_WH | Same INVENTORY+ITEM join run 25 times with different dates |
| 5 | **Cartesian Join** | `int__antipattern_cartesian_join` | COMPUTE_WH | STORE × DATE_DIM cross join, PROMOTION × HOUSEHOLD_DEMOGRAPHICS |
| 6 | **Large Sort No LIMIT** | `int__antipattern_large_sort_no_limit` | COMPUTE_WH | ORDER BY on ITEM (402K rows), GEOLOCATION (1M rows), no LIMIT |

### Cost & Attribution Scenarios

| # | Scenario | What It Triggers | Warehouse | Key Queries |
|---|----------|-----------------|-----------|-------------|
| 7 | **Expensive Multi-Join** | Cost attribution, warehouse comparison | ANALYTICS_WH + ETL_WH | Same INVENTORY×DATE×ITEM join on both warehouses — shows size-based cost difference |
| 8 | **Cost Spike** | `int__alert_cost_daily_spike` | ANALYTICS_WH | 4 expensive queries in burst — daily cost > 2x baseline |
| 9 | **Multi-Warehouse** | Team attribution across 3 WHs | All 3 | Same query on COMPUTE_WH, ANALYTICS_WH, ETL_WH — shows per-team cost |
| 10 | **Idle Warehouse** | `int__alert_warehouse_idle` | ETL_WH | Resume warehouse, run nothing — detects idle period |

### Query Tagging for Attribution
Every workload query uses structured query_tag:
```
query_tag = 'team:{team_name};scenario:{scenario_name};run_id:{uuid};query:{n}'
```
The dbt model `int__query_cost_attribution` extracts `team` from this tag for attribution.

## Live Metadata vs Seed Files

### What Changed
| Data Point | Before (Seed) | After (Live) |
|------------|---------------|--------------|
| Warehouse sizes | `warehouse_size_credits.csv` | Inline VALUES in `int__query_cost_attribution` + `warehouse_size` column from QUERY_HISTORY |
| Team mapping | `warehouse_team_mapping.csv` | Derived from `query_tag` + `role_name` pattern matching in `int__team_cost_attribution` |
| Credit pricing | `credit_pricing.csv` (kept) | Still a seed — this is static Snowflake edition pricing |
| Alert config | `alert_configuration.csv` (kept) | Still a seed — this IS configuration |
| Monthly budget | `monthly_budget.csv` (kept) | Still a seed — this IS configuration |

### Team Derivation Priority
```
1. query_tag → 'team:analytics;...' → team = 'analytics'
2. role_name → 'ANALYST_ROLE' → team = 'Analytics'
3. warehouse_name → 'ETL_WH' → team = 'Data Engineering'
4. Default → 'Unassigned'
```

## Demo Flow

### Quick Demo (5 minutes + 45-minute wait)
```bash
cd workload_generator

# 1. Run 3 key scenarios (full_scan, repeated, multi_warehouse)
python generate_workloads.py --scenario full_scan,repeated,multi_warehouse

# 2. Wait 45 minutes for ACCOUNT_USAGE latency

# 3. Build dbt models
snow dbt execute cost_optimization build

# 4. Open Streamlit dashboard
# → Show anti-patterns in Query Optimizer page
# → Show multi-warehouse attribution in Team Attribution page
# → Show cost breakdown in Executive Summary
```

### Full Demo (15 minutes + 45-minute wait)
```bash
# 1. Run ALL 10 scenarios
python generate_workloads.py --scenario all

# 2. Verify queries appeared
python demo_runner.py --step verify

# 3. After 45 min wait, build and check
snow dbt execute cost_optimization build
python demo_runner.py --step check
```

### Client Presentation Script
1. **"Let me run a query"** → Run `full_scan` scenario
2. **"Now let's see what the framework caught"** → Open Query Optimizer page
3. **"Here's the cost impact"** → Switch to Executive Summary
4. **"And here's the recommendation"** → Switch to Recommendations Hub
5. **"This works across teams"** → Run `multi_warehouse` → Show Team Attribution
6. **"And it alerts automatically"** → Show Alert Management page

## File Structure

```
workload_generator/
├── scan_environment.py       # Discovers databases, warehouses, roles, tables
├── setup_demo_environment.py # Creates ANALYTICS_WH, ETL_WH, WORKLOADS schema
├── generate_workloads.py     # 10 scenarios, structured query tags, logging
├── demo_runner.py            # Orchestrates: setup → scan → run → verify → check
└── environment_scan.json     # Last scan results (auto-generated)
```
