# Seed Files

The project uses 8 CSV seed files loaded into the `SEEDS` schema. Seeds are configuration-driven reference data -- not transactional. Run `dbt seed --full-refresh` to reload after any CSV change.

---

## credit_pricing.csv

Configures credit price per Snowflake edition. Models filter on `edition = 'ENTERPRISE'`.

| edition | credit_price_usd | effective_from | effective_to |
|---|---|---|---|
| STANDARD | 2.00 | 2024-01-01 | 9999-12-31 |
| ENTERPRISE | 3.00 | 2024-01-01 | 9999-12-31 |
| BUSINESS_CRITICAL | 4.00 | 2024-01-01 | 9999-12-31 |

**Used by:** `int__query_cost_attribution`, `int__warehouse_daily_credits`, `int__idle_warehouse_periods`, `int__serverless_credits`, `int__daily_cost_rollup`, `int__warehouse_auto_suspend_analysis`

---

## warehouse_size_credits.csv

Reference mapping of warehouse sizes to credits per hour. Note: `int__query_cost_attribution` uses an inline VALUES table instead (to handle naming variants like X-SMALL vs XSMALL), but this seed is used by `int__idle_warehouse_periods`.

| warehouse_size | credits_per_hour |
|---|---|
| XSMALL | 1 |
| SMALL | 2 |
| MEDIUM | 4 |
| LARGE | 8 |
| XLARGE | 16 |
| 2XLARGE - 6XLARGE | 32 - 512 |

---

## monthly_budget.csv

Monthly credit and USD budget used for budget alert thresholds. Covers 2024-01 through 2026-06. All rows currently set to 1,000 credits / $3,000 for the Platform team.

**Columns:** `budget_month`, `budget_credits`, `budget_usd`, `team_name`

**Used by:** `int__alert_credit_budget`

---

## warehouse_team_mapping.csv

Static fallback mapping of warehouse names to teams and cost centers. Not used for primary team attribution (which is dynamic from query history), but available as a reference.

| warehouse_name | team_name | cost_center |
|---|---|---|
| COMPUTE_WH | Platform | PLATFORM-001 |
| COST_OPT_WH | Cost Optimization | COSTOPT-001 |
| ANALYTICS_WH | Analytics | ANALYTICS-001 |
| ETL_WH | Data Engineering | DATAENG-001 |
| SNOWFLAKE_LEARNING_WH | Training | TRAINING-001 |

---

## recommendation_actions.csv

Tracks the lifecycle status of recommendations. Users add rows as recommendations are actioned. Header-only by default.

**Columns:** `recommendation_id`, `status` (OPEN/ACCEPTED/IMPLEMENTED/REJECTED/DEFERRED), `actioned_by`, `actioned_at`, `implemented_at`, `notes`

**Used by:** `int__recommendation_lifecycle`

---

## alert_configuration.csv

Master configuration for all 7 alert rules. Controls enablement, Teams routing, scheduling, thresholds, severity, resolver team, holiday suppression, and seasonality sensitivity.

| alert_id | severity | threshold | teams_channel | suppress_on_holidays |
|---|---|---|---|---|
| cost_daily_spike | P1 | 2.0x multiplier | cost-alerts | FALSE |
| warehouse_idle_extended | P2 | 30 minutes | cost-alerts | TRUE |
| credit_budget_80pct | P1 | 80% | finance-alerts | FALSE |
| credit_budget_100pct | P0 | 100% | finance-alerts | FALSE |
| query_spill_heavy | P2 | 1GB bytes | cost-alerts | TRUE |
| storage_growth_anomaly | P3 | 20% | cost-alerts | TRUE |
| repeated_expensive_query | P2 | 20 count | cost-alerts | TRUE |

- P0/P1 (critical): `suppress_on_holidays = FALSE` -- fire regardless of holidays
- P2/P3 (non-critical): `suppress_on_holidays = TRUE` -- suppressed on bank holidays

**Used by:** All 6 alert detection models, `pub__teams_alert_payload`, `pub__alert_history`, `int__alert_union_all`

---

## alert_suppressions.csv

Targeted alert suppression rules. Each row silences a specific `alert_id` for a specific `resource_value` during a date range. Use for planned maintenance windows or known noisy periods. Header-only by default -- users add rows as needed.

**Columns:** `alert_id`, `resource_value`, `suppression_reason`, `created_by`, `start_date`, `end_date`

**Example usage:**
```csv
warehouse_idle_extended,COMPUTE_WH,Planned maintenance window,platform-team,2026-05-10,2026-05-12
```

**Used by:** `int__alert_union_all`

---

## bank_holidays.csv

Indian public holidays for 2025-2026 (43 entries, region = IN). Used to suppress non-critical alerts on days when reduced activity is expected.

**Columns:** `holiday_date`, `description`, `region`

**Used by:** `int__alert_union_all` (joined on `detected_at::DATE = holiday_date`)
