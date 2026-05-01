# Alert Models

The alerts layer contains 10 models implementing an episode-based alert pipeline with Microsoft Teams integration, suppression rules, and bank holiday awareness.

## Architecture

```
Detection Models (6)           Suppression           State Tracking        Delivery
+------------------+      +------------------+    +------------------+   +------------------+
| cost_daily_spike |----->|                  |    |                  |   |                  |
| warehouse_idle   |----->| alert_union_all  |--->| alert_state_     |-->| teams_alert_     |
| credit_budget    |----->| (suppression +   |    | tracker          |   | payload          |
| query_spill      |----->|  holiday filter) |    | (episode dedup)  |   | (Adaptive Card)  |
| storage_growth   |----->|                  |    |                  |   |                  |
| repeated_expense |----->|                  |    |                  |   | alert_history    |
+------------------+      +------------------+    +------------------+   | (audit trail)    |
                                                                         +------------------+
```

---

## Detection Models

Each detection model queries upstream data and emits rows matching a standardized schema: `alert_id`, `detected_at`, `resource_key`, `metric_value`, `threshold_value`, `details_json`.

---

### int__alert_cost_daily_spike

**Alert ID:** `cost_daily_spike` | **Severity:** P1

Two-tier spike detection:
1. **Primary:** Seasonality-aware z-score using `int__cost_seasonality_baseline`. Fires when actual cost > `adjusted_baseline + N * effective_stddev` (N = configurable `seasonality_sensitivity`, default 2.0)
2. **Fallback:** Simple 2x rolling-30d-average multiplier when no seasonality data exists

Only evaluates the most recent day.

**Upstream:** `int__daily_cost_rollup`, `int__cost_seasonality_baseline`, `alert_configuration`

---

### int__alert_warehouse_idle

**Alert ID:** `warehouse_idle_extended` | **Severity:** P2

Fires when a warehouse has been idle longer than the configured threshold (default 30 minutes). Only considers idle periods from the last 6 hours.

**Resource key:** warehouse name
**Upstream:** `int__idle_warehouse_periods`, `alert_configuration`

---

### int__alert_credit_budget

**Alert IDs:** `credit_budget_80pct` (P1), `credit_budget_100pct` (P0)

Fires at 80% and 100% of monthly credit budget. Computes MTD credits against current month's budget from the `monthly_budget` seed.

**Resource key:** account
**Upstream:** `int__warehouse_daily_credits`, `monthly_budget`, `alert_configuration`

---

### int__alert_query_spill

**Alert ID:** `query_spill_heavy` | **Severity:** P2

Fires when a query spills more than 1GB to remote storage. Only looks at queries from the last hour.

**Resource key:** warehouse_name/query_id
**Upstream:** `stg__query_history`, `alert_configuration`

---

### int__alert_storage_growth

**Alert ID:** `storage_growth_anomaly` | **Severity:** P3

Fires when a database's storage grew more than 20% week-over-week. Uses 7-row LAG window on `database_storage_usage_history`.

**Resource key:** database name
**Upstream:** `stg__database_storage_usage_history`, `alert_configuration`

---

### int__alert_repeated_expensive

**Alert ID:** `repeated_expensive_query` | **Severity:** P2

Fires when a query hash runs more than 20 times/day and each execution costs >$1. Looks at the last 1 day.

**Resource key:** query hash
**Upstream:** `int__query_patterns`, `alert_configuration`

---

## Suppression & Union

### int__alert_union_all

UNION ALL of all 6 detection models with suppression logic applied:

1. **Targeted suppression:** LEFT JOIN `alert_suppressions` seed on `alert_id` + `resource_key` + date range
2. **Bank holiday suppression:** LEFT JOIN `bank_holidays` seed on `detected_at::DATE`, filtered by `suppress_on_holidays` flag in `alert_configuration`

Rows where `is_suppressed = TRUE` are filtered out so they never reach the state tracker.

**Output columns:** `alert_id`, `detected_at`, `resource_key`, `metric_value`, `threshold_value`, `details_json`, `is_suppressed`, `suppression_reason`

**Upstream:** All 6 detection models + `alert_suppressions` + `bank_holidays` + `alert_configuration`

---

## State Tracking

### int__alert_state_tracker

**Materialization:** incremental (merge on `alert_episode_key`)

Episode-based deduplication engine:
- **New episode:** Alert is brand new OR was previously resolved (not seen in last 6 hours)
- **Continuation:** Alert is still active from a previous cycle
- Episode number increments only on new episodes
- Generates `alert_episode_key` = `{alert_id}_episode_{N}_{resource_key}`

On full refresh, every alert starts as episode 1.

**Key columns:** `alert_episode_key`, `alert_id`, `resource_key`, `is_new_episode`, `is_continuation`, `episode_number`, `evaluated_at`

**Upstream:** `int__alert_union_all`

---

## Delivery

### pub__teams_alert_payload

**Materialization:** table (schema: PUBLICATION)

Generates Microsoft Teams Adaptive Card v1.4 JSON payloads for new alert episodes where `teams_enabled = TRUE`. The card includes severity-colored title, description, and a FactSet with Resource, Metric Value, Threshold, Detected At, Episode number, and Resolver Team.

- P0 = Attention (red), P1 = Warning (orange), others = Default

**Key columns:** `alert_episode_key`, `teams_payload_json`, `teams_channel`, `sent_at` (null until delivered), `api_response_code`, `send_success`

**Upstream:** `int__alert_state_tracker`, `alert_configuration`

**Delivery:** The `send_teams_alerts()` stored procedure reads unsent rows (`sent_at IS NULL`), POSTs each to the correct channel's webhook URL, and updates the send status.

---

### pub__alert_history

**Materialization:** incremental (merge on `alert_episode_key`, schema: PUBLICATION)

Full audit trail combining state tracker metadata, alert configuration, and Teams delivery status. Includes `is_suppressed` and `suppression_reason` columns for audit visibility (always FALSE in practice since suppressed alerts are filtered upstream).

**Key columns:** `alert_episode_key`, `alert_id`, `alert_name`, `severity`, `detected_at`, `resource_key`, `is_new_episode`, `episode_number`, `teams_sent_at`, `teams_send_success`, `is_suppressed`, `suppression_reason`

**Upstream:** `int__alert_state_tracker`, `alert_configuration`, `pub__teams_alert_payload`

---

## Snowflake Infrastructure

The alert pipeline relies on Snowflake objects deployed via `snowflake_objects/setup_alerts_infrastructure.sql` and `snowflake_objects/setup_webhook_secrets.sql`:

| Object | Type | Purpose |
|---|---|---|
| `cost_alerts_webhook_secret` | Secret | Webhook URL for `cost-alerts` channel |
| `finance_alerts_webhook_secret` | Secret | Webhook URL for `finance-alerts` channel |
| `teams_webhook_rule` | Network Rule | Allows egress to Power Automate |
| `teams_alert_integration` | External Access Integration | Binds network rule + secrets |
| `send_teams_alerts()` | Stored Procedure (Python) | POSTs unsent payloads to Teams |
| `send_teams_alerts_task` | Task (15 min) | Scheduled alert delivery |

The `on-run-end` hook in `dbt_project.yml` calls `send_teams_alerts()` automatically after every successful `dbt run` or `dbt build` via the `call_send_teams_alerts` macro.
