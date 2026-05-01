# User Guide — Snowflake Cost Optimization Dashboard

## 1. Getting Started

The Cost Optimization Dashboard runs as a **Streamlit-in-Snowflake (SiS)** app inside Snowsight.

### Accessing the Dashboard

1. Log in to [Snowsight](https://app.snowflake.com/) with your Snowflake credentials
2. Navigate to **Projects → Streamlit** in the left sidebar
3. Open **COST_OPTIMIZATION_DASHBOARD** under `COST_OPTIMIZATION_DB`
4. The dashboard loads with the Home page showing KPIs and cost breakdown

### Requirements

- Role: `ACCOUNTADMIN` (or a role with `SELECT` on `COST_OPTIMIZATION_DB`)
- Warehouse: `COST_OPT_WH` is used automatically by the app

---

## 2. Dashboard Pages

### Home (app.py)
- **KPI cards**: Total Spend (MTD), Compute Cost, Storage Cost, Serverless Cost with month-over-month change
- **Cost split donut chart**: visual breakdown of compute vs storage vs serverless
- **Data freshness banner**: shows how stale the underlying data is — green (<2 hours), yellow (2–6 hours), red (>6 hours)

### 1. Executive Summary
- Monthly cost trends with breakdowns by category
- Top 5 most expensive warehouses and users
- Use the date range filter to adjust the lookback window

### 2. Warehouse Deep Dive
- Per-warehouse cost, efficiency, and utilization metrics
- Select a warehouse from the dropdown to see daily credit consumption and idle periods
- Idle periods table shows when a warehouse was running with no queries

### 3. Team Attribution
- Cost broken down by team (derived from role → team mapping)
- Per-user cost ranking
- Cost by query type (SELECT, INSERT, MERGE, etc.)

### 4. Storage Explorer
- Storage consumption by database: active, time-travel, and failsafe
- Total storage cost estimates
- Identify databases with excessive time-travel or failsafe storage

### 5. Trend Analysis
- Multi-line chart: compute, storage, serverless, and 7-day rolling average
- Week-over-week comparison table
- Anomaly detection: days where cost exceeded 2x the 30-day baseline
- Day-of-week × hour-of-day heatmap showing cost concentration patterns

### 6. Alert Management
- View active and historical alerts
- Check alert delivery status (sent/pending/failed)
- Review Teams message payloads

### 7. Warehouse Optimizer
- Right-sizing recommendations based on utilization analysis
- Identifies over-provisioned and under-provisioned warehouses
- Shows potential savings from size changes

### 8. Query Optimizer
- Anti-pattern detection across 6 categories: full table scans, excessive spilling, cartesian joins, repeated expensive queries, unused results, high compilation time
- Per-query cost estimates and optimization suggestions

### 9. Storage Optimizer
- Identifies tables with high cloning, time-travel, or failsafe overhead
- Recommends retention policy changes
- Shows potential storage cost savings

### 10. Recommendations Hub
- Unified view of all optimization recommendations with priority scoring
- Each recommendation includes ready-to-run SQL
- ROI tracking for implemented recommendations

---

## 3. Configuring Alerts

Alerts are configured via the seed file `cost_optimization_dbt/seeds/alert_configuration.csv`.

### Columns

| Column | Description |
|--------|-------------|
| `alert_id` | Unique identifier (e.g., `cost_daily_spike`) |
| `alert_name` | Human-readable name |
| `severity` | Priority level: P0 (critical), P1 (high), P2 (medium), P3 (low) |
| `enabled` | `true` / `false` — master switch for this alert |
| `teams_enabled` | `true` / `false` — whether to send to Teams |
| `teams_channel` | Target channel: `cost-alerts` or `finance-alerts` |
| `threshold_value` | Numeric threshold (interpretation depends on `threshold_type`) |
| `threshold_type` | `percentage`, `absolute`, `z_score` |
| `suppress_on_holidays` | `true` / `false` — skip alerting on bank holidays |

### How to Change

1. Edit `cost_optimization_dbt/seeds/alert_configuration.csv`
2. Re-seed: `snow dbt execute cost_optimization seed`
3. Rebuild alerts: `snow dbt execute cost_optimization run`

### Example: Disable a noisy alert

Change the `enabled` column to `false` for that alert's row, then re-seed and rebuild.

---

## 4. Adding Alert Suppressions

To suppress a specific alert for a specific resource during a maintenance window, add a row to `cost_optimization_dbt/seeds/alert_suppressions.csv`.

### Columns

| Column | Description |
|--------|-------------|
| `alert_id` | Which alert to suppress (must match `alert_configuration.alert_id`) |
| `resource_value` | The resource to suppress (e.g., warehouse name `ETL_WH`) |
| `suppression_reason` | Free text explanation |
| `created_by` | Who added the suppression |
| `start_date` | Suppression start date (`YYYY-MM-DD`) |
| `end_date` | Suppression end date (`YYYY-MM-DD`) |

### Example

```csv
alert_id,resource_value,suppression_reason,created_by,start_date,end_date
warehouse_idle_extended,ETL_WH,Planned migration downtime,srinivas,2026-05-10,2026-05-12
```

After editing, re-seed and rebuild:
```bash
snow dbt execute cost_optimization seed
snow dbt execute cost_optimization run
```

---

## 5. Managing Budgets

Budget alerts compare actual spend against targets defined in `cost_optimization_dbt/seeds/monthly_budget.csv`.

### Columns

| Column | Description |
|--------|-------------|
| `budget_month` | Month (`YYYY-MM`) |
| `budget_credits` | Credit budget for the month |
| `budget_usd` | Dollar budget (credits × credit price) |
| `team_name` | Team the budget applies to |

### How to Update

1. Add or modify rows in `monthly_budget.csv` for upcoming months
2. Re-seed: `snow dbt execute cost_optimization seed`
3. Rebuild: `snow dbt execute cost_optimization run`

Budget alerts fire when actual spend exceeds the configured threshold percentage of the budget.

---

## 6. Applying Recommendations

The **Recommendations Hub** (page 10) shows actionable SQL for each optimization recommendation.

### Workflow

1. Open **Recommendations Hub** in the dashboard
2. Review recommendations sorted by priority score (higher = more impactful)
3. Copy the provided SQL statement
4. Open a Snowsight worksheet and paste the SQL
5. Review the SQL carefully before executing
6. After applying, the next dbt build will detect the change and update ROI tracking

### Common Recommendations

- **Warehouse right-sizing**: `ALTER WAREHOUSE ... SET WAREHOUSE_SIZE = ...`
- **Auto-suspend tuning**: `ALTER WAREHOUSE ... SET AUTO_SUSPEND = ...`
- **Storage retention**: `ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = ...`

---

## 7. Adding Bank Holidays

Bank holidays suppress alerts on specified dates. Edit `cost_optimization_dbt/seeds/bank_holidays.csv`.

### Columns

| Column | Description |
|--------|-------------|
| `holiday_date` | Date (`YYYY-MM-DD`) |
| `description` | Holiday name |
| `region` | Region code (e.g., `IN` for India) |

### Example

```csv
holiday_date,description,region
2026-08-15,Independence Day,IN
2026-10-02,Gandhi Jayanti,IN
```

After editing, re-seed and rebuild:
```bash
snow dbt execute cost_optimization seed
snow dbt execute cost_optimization run
```

Only alerts with `suppress_on_holidays = true` in `alert_configuration.csv` will be suppressed on these dates.
