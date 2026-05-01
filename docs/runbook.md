# Operational Runbook — Snowflake Cost Optimization Pipeline

## 1. Pipeline Health Check

### Verify dbt build status

```bash
# Run a full build and check for errors
snow dbt execute cost_optimization build

# Run tests only to verify data quality
snow dbt execute cost_optimization test
```

### Check Refresh Log

```sql
-- Recent refresh events
SELECT * FROM COST_OPTIMIZATION_DB.PUBLICATION.REFRESH_LOG
ORDER BY refresh_at DESC LIMIT 10;
```

### Check model row counts

```sql
-- Quick health check: key tables should have rows
SELECT 'int__query_cost_attribution' AS model, COUNT(*) AS rows FROM COST_OPTIMIZATION_DB.INTERMEDIATE.INT__QUERY_COST_ATTRIBUTION
UNION ALL
SELECT 'pub__cost_summary', COUNT(*) FROM COST_OPTIMIZATION_DB.PUBLICATION.PUB__COST_SUMMARY
UNION ALL
SELECT 'pub__cost_trends_daily', COUNT(*) FROM COST_OPTIMIZATION_DB.PUBLICATION.PUB__COST_TRENDS_DAILY
UNION ALL
SELECT 'pub__cost_by_warehouse', COUNT(*) FROM COST_OPTIMIZATION_DB.PUBLICATION.PUB__COST_BY_WAREHOUSE;
```

---

## 2. Data Freshness

### Dashboard banner

The Home page displays a freshness banner:
- **Green**: Data is <2 hours old — normal
- **Yellow**: Data is 2–6 hours old — ACCOUNT_USAGE latency may be high
- **Red**: Data is >6 hours old — pipeline may need a refresh

### Check freshness directly

```sql
SELECT SOURCE_NAME, MODEL_NAME, LATEST_RECORD_AT, STALENESS_MINUTES, FRESHNESS_STATUS
FROM COST_OPTIMIZATION_DB.PUBLICATION.PUB__DATA_FRESHNESS
ORDER BY STALENESS_MINUTES DESC;
```

### Understanding ACCOUNT_USAGE latency

`SNOWFLAKE.ACCOUNT_USAGE` views have a **45-minute to 3-hour latency**. This is a Snowflake platform limitation — data will never be truly real-time. The dashboard shows the most recent data available.

---

## 3. Troubleshooting Teams Alerts

### Check for unsent alerts

```sql
-- Alerts generated but not yet delivered
SELECT *
FROM COST_OPTIMIZATION_DB.INTERMEDIATE.INT__TEAMS_ALERT_PAYLOAD
WHERE sent_at IS NULL
ORDER BY created_at DESC;
```

### Verify webhook secrets exist

```sql
-- Check that webhook secrets are configured
SHOW SECRETS IN SCHEMA COST_OPTIMIZATION_DB.PUBLIC;
```

Expected secrets: `TEAMS_WEBHOOK_COST_ALERTS`, `TEAMS_WEBHOOK_FINANCE_ALERTS`

### Check the send procedure exists

```sql
SELECT *
FROM COST_OPTIMIZATION_DB.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_NAME = 'SEND_TEAMS_ALERTS';
```

### Re-deploy alert infrastructure

If the procedure or secrets are missing:

```bash
# Deploy procedures, tasks, network rules
snow sql --connection cost_optimization --enable-templating NONE \
  -f cost_optimization_dbt/snowflake_objects/setup_alerts_infrastructure.sql

# Deploy webhook secrets (update URLs in file first)
snow sql --connection cost_optimization --enable-templating NONE \
  -f cost_optimization_dbt/snowflake_objects/setup_webhook_secrets.sql
```

---

## 4. Enable/Disable Alerting

### Enable Teams alert delivery

```sql
ALTER TASK send_teams_alerts_task RESUME;
```

### Disable Teams alert delivery (master off switch)

```sql
ALTER TASK send_teams_alerts_task SUSPEND;
```

Note: The `on-run-end` hook in `dbt_project.yml` also calls `send_teams_alerts()` after every successful `dbt run` / `dbt build`. To fully disable alerting, both the task and the hook should be addressed.

---

## 5. Enable/Disable Refresh Tasks

Three scheduled tasks refresh `stg__query_history` (the only incremental staging model) at 10:30 AM, 1:00 PM, and 4:00 PM IST.

### Enable

```sql
ALTER TASK refresh_cost_models_morning RESUME;
ALTER TASK refresh_cost_models_midday RESUME;
ALTER TASK refresh_cost_models_afternoon RESUME;
```

### Disable

```sql
ALTER TASK refresh_cost_models_morning SUSPEND;
ALTER TASK refresh_cost_models_midday SUSPEND;
ALTER TASK refresh_cost_models_afternoon SUSPEND;
```

### Check task status

```sql
SHOW TASKS IN SCHEMA COST_OPTIMIZATION_DB.PUBLIC;
```

**Cost note**: Each task run spins up `COST_OPT_WH`. In dev/demo environments, keep these suspended and use manual `snow dbt execute cost_optimization build` instead.

---

## 6. Full Refresh

A full refresh is needed when:
- New columns are added to incremental models (e.g., `stg__query_history`)
- Seed CSV column types change
- Schema changes are made to any table model

### Command

```bash
snow dbt execute cost_optimization build -- --full-refresh
```

This drops and recreates all table models from scratch. It takes longer than an incremental build but ensures clean state.

---

## 7. Re-deploying After Code Changes

When dbt models, seeds, or macros are modified:

```bash
# 1. Deploy the updated dbt project to Snowflake
snow dbt deploy cost_optimization

# 2. Install/update dependencies
snow dbt execute cost_optimization deps

# 3. Load seed data
snow dbt execute cost_optimization seed

# 4. Run all models
snow dbt execute cost_optimization run

# 5. Run tests to verify
snow dbt execute cost_optimization test
```

Or use the combined command:

```bash
snow dbt deploy cost_optimization
snow dbt execute cost_optimization build
```

---

## 8. Re-deploying Streamlit

When dashboard code is modified (`streamlit_app/` files):

```bash
cd streamlit_app
python deploy_sis.py
```

Or with an explicit connection:

```bash
python deploy_sis.py --connection cost_optimization
```

The script:
1. Reads auth from `~/.snowflake/connections.toml`
2. Injects cache-busting timestamps into `.py` files
3. Uploads all files to `COST_OPTIMIZATION_DB.PUBLIC.STREAMLIT_STAGE`
4. Runs `CREATE OR REPLACE STREAMLIT` to update the app

**Known issue**: `snow streamlit deploy` may fail with account format `chc70950.us-east-1`. Use `deploy_sis.py` instead.

---

## 9. Common Errors

### Seed column type errors

**Symptom**: `dbt seed` fails with date parsing errors on `alert_suppressions` or `bank_holidays`.

**Fix**: Ensure `dbt_project.yml` has explicit column types:
```yaml
seeds:
  cost_optimization:
    alert_suppressions:
      +column_types:
        start_date: date
        end_date: date
    bank_holidays:
      +column_types:
        holiday_date: date
```

Then run with `--full-refresh`:
```bash
snow dbt execute cost_optimization seed -- --full-refresh
```

### on-run-end hook failure

**Symptom**: `dbt run` succeeds but logs a warning about `send_teams_alerts()` not found.

**Cause**: The `call_send_teams_alerts` macro checks `INFORMATION_SCHEMA.PROCEDURES` before calling. If the procedure isn't deployed, it logs a message and skips gracefully. This is informational, not an error.

**Fix**: Deploy the alert infrastructure if you want Teams alerts:
```bash
snow sql --connection cost_optimization --enable-templating NONE \
  -f cost_optimization_dbt/snowflake_objects/setup_alerts_infrastructure.sql
```

### Snowflake CLI host format error

**Symptom**: `snow streamlit deploy` fails with "connection host was missing or not in the expected format".

**Cause**: The CLI doesn't handle account format `chc70950.us-east-1` correctly for Streamlit deployment.

**Fix**: Use `deploy_sis.py` instead of `snow streamlit deploy`.

### ACCOUNT_USAGE permission denied

**Symptom**: Staging models fail with "insufficient privileges" on `SNOWFLAKE.ACCOUNT_USAGE`.

**Fix**: Ensure the role has the `IMPORTED PRIVILEGES` grant:
```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ACCOUNTADMIN;
```

### dbt docs generate not available

**Symptom**: `snow dbt execute cost_optimization docs generate` fails with "No such command 'docs'".

**Cause**: Snowflake-native dbt (`snow dbt execute`) does not support the `docs` subcommand. This is a platform limitation.

**Workaround**: Model documentation is maintained in the `docs/` directory as markdown files (`models_staging.md`, `models_intermediate.md`, `models_publication.md`, `models_alerts.md`, `seeds.md`).
