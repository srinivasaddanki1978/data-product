# Staging Models

The staging layer contains 15 models that provide clean, type-cast mirrors of `SNOWFLAKE.ACCOUNT_USAGE` source tables. All staging models are materialized as **views** except `stg__query_history` which is **incremental**.

No business logic is applied in this layer -- only column renaming, type casting, and adding `_loaded_at` timestamps.

## Sources

All 14 source tables are declared in `models/sources.yml` pointing to `SNOWFLAKE.ACCOUNT_USAGE` with 365-day retention. Freshness checks warn at 24 hours and error at 48 hours.

| Source Table | Description |
|---|---|
| QUERY_HISTORY | One row per query executed |
| WAREHOUSE_METERING_HISTORY | Hourly credit consumption per warehouse |
| WAREHOUSE_LOAD_HISTORY | 5-minute interval warehouse load metrics |
| TABLE_STORAGE_METRICS | Current storage bytes per table |
| STORAGE_USAGE | Daily account-level storage usage |
| DATABASE_STORAGE_USAGE_HISTORY | Daily storage per database |
| LOGIN_HISTORY | User login attempts and authentication |
| ACCESS_HISTORY | Object access audit trail per query |
| AUTOMATIC_CLUSTERING_HISTORY | Credits for automatic clustering |
| MATERIALIZED_VIEW_REFRESH_HISTORY | Credits for MV refreshes |
| PIPE_USAGE_HISTORY | Credits and data volume for Snowpipe |
| SERVERLESS_TASK_HISTORY | Credits for serverless tasks |
| SESSIONS | Session details including client application |
| SEARCH_OPTIMIZATION_HISTORY | Credits for search optimization service |

---

## stg__query_history

**File:** `models/staging/stg__query_history.sql`
**Materialization:** incremental (merge on `query_id`)

Clean mirror of `ACCOUNT_USAGE.QUERY_HISTORY`. Uses incremental merge strategy to avoid re-scanning 571K+ rows -- only loads rows where `end_time > MAX(end_time)` in the existing table.

**Key columns:** `query_id`, `query_text`, `query_type`, `user_name`, `role_name`, `warehouse_name`, `warehouse_size`, `start_time`, `end_time`, `execution_time_ms`, `bytes_scanned`, `rows_produced`, `bytes_spilled_to_local_storage`, `bytes_spilled_to_remote_storage`, `query_parameterized_hash`, `query_tag`

---

## stg__warehouse_metering_history

**File:** `models/staging/stg__warehouse_metering_history.sql`
**Materialization:** view

Hourly warehouse credit consumption.

**Key columns:** `warehouse_name`, `start_time`, `end_time`, `credits_used`, `credits_used_compute`, `credits_used_cloud_services`

---

## stg__warehouse_load_history

**File:** `models/staging/stg__warehouse_load_history.sql`
**Materialization:** view

5-minute warehouse load intervals used for utilization analysis.

**Key columns:** `warehouse_name`, `start_time`, `end_time`, `avg_running`, `avg_queued_load`, `avg_queued_provisioning`, `avg_blocked`

---

## stg__storage_usage

**File:** `models/staging/stg__storage_usage.sql`
**Materialization:** view

Daily account-level storage bytes.

**Key columns:** `usage_date`, `storage_bytes`, `stage_bytes`, `failsafe_bytes`

---

## stg__database_storage_usage_history

**File:** `models/staging/stg__database_storage_usage_history.sql`
**Materialization:** view

Daily storage bytes per database.

**Key columns:** `usage_date`, `database_name`, `average_database_bytes`, `average_failsafe_bytes`

---

## stg__table_storage_metrics

**File:** `models/staging/stg__table_storage_metrics.sql`
**Materialization:** view

Current table-level storage breakdown. Derives `table_type` label from the `is_transient` flag.

**Key columns:** `table_id`, `table_name`, `schema_name`, `database_name`, `table_type`, `active_bytes`, `time_travel_bytes`, `failsafe_bytes`, `retained_for_clone_bytes`

---

## stg__login_history

**File:** `models/staging/stg__login_history.sql`
**Materialization:** view

User login attempts and authentication events.

**Key columns:** `event_id`, `event_timestamp`, `user_name`, `client_ip`, `is_success`, `first_authentication_factor`, `second_authentication_factor`

---

## stg__access_history

**File:** `models/staging/stg__access_history.sql`
**Materialization:** view

Object access audit trail -- used downstream for determining last read dates on tables via `LATERAL FLATTEN` on `base_objects_accessed`.

**Key columns:** `query_id`, `query_start_time`, `user_name`, `direct_objects_accessed`, `base_objects_accessed`, `objects_modified`

---

## stg__sessions

**File:** `models/staging/stg__sessions.sql`
**Materialization:** view

Session-level details for client application tracking.

**Key columns:** `session_id`, `user_name`, `created_on`, `authentication_method`, `client_application_id`, `client_application_version`

---

## stg__automatic_clustering_history

**File:** `models/staging/stg__automatic_clustering_history.sql`
**Materialization:** view

Credits and row/byte counts for automatic clustering operations.

**Key columns:** `table_name`, `database_name`, `start_time`, `end_time`, `credits_used`, `num_bytes_reclustered`, `num_rows_reclustered`

---

## stg__materialized_view_refresh_history

**File:** `models/staging/stg__materialized_view_refresh_history.sql`
**Materialization:** view

Credit consumption for materialized view refresh operations.

**Key columns:** `table_name`, `database_name`, `start_time`, `end_time`, `credits_used`

---

## stg__pipe_usage_history

**File:** `models/staging/stg__pipe_usage_history.sql`
**Materialization:** view

Snowpipe credit and data volume consumption.

**Key columns:** `pipe_name`, `start_time`, `end_time`, `credits_used`, `bytes_inserted`, `files_inserted`

---

## stg__serverless_task_history

**File:** `models/staging/stg__serverless_task_history.sql`
**Materialization:** view

Credit consumption for Snowflake serverless tasks.

**Key columns:** `task_name`, `database_name`, `start_time`, `end_time`, `credits_used`

---

## stg__search_optimization_history

**File:** `models/staging/stg__search_optimization_history.sql`
**Materialization:** view

Credit usage for the search optimization service.

**Key columns:** `table_name`, `database_name`, `start_time`, `end_time`, `credits_used`

---

## stg__warehouse_role_usage

**File:** `models/staging/stg__warehouse_role_usage.sql`
**Materialization:** view

Dynamically extracts distinct warehouse-to-role-to-user mappings from live query history (not from seed files). Derives `derived_team_name` from role name patterns:

| Role Pattern | Team |
|---|---|
| ADMIN | Platform |
| ANALYST | Analytics |
| ENGINEER | Engineering |
| TRANSFORM | Data Engineering |
| SYSADMIN / ACCOUNTADMIN | Platform |
| PUBLIC | Unassigned |

**Key columns:** `warehouse_name`, `role_name`, `user_name`, `query_count`, `first_seen`, `last_seen`, `derived_team_name`

**Upstream:** `stg__query_history`
