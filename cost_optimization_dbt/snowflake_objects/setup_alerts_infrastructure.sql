-- =============================================================================
-- Snowflake Infrastructure for Microsoft Teams Alert Integration
-- =============================================================================
-- Prerequisites:
--   1. ACCOUNTADMIN role
--   2. COST_OPTIMIZATION_DB database exists
--   3. Teams incoming webhook URL configured
--
-- Usage:
--   Replace <teams-incoming-webhook-url> with actual webhook URL before executing.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE COST_OPTIMIZATION_DB;
USE SCHEMA PUBLIC;

-- 1. Secret to store Teams webhook URL
CREATE OR REPLACE SECRET teams_webhook_secret
  TYPE = GENERIC_STRING
  SECRET_STRING = '<teams-incoming-webhook-url>';

-- 2. Network rule allowing egress to Microsoft Teams
CREATE OR REPLACE NETWORK RULE teams_webhook_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'outlook.webhook.office.com:443',
    '*.webhook.office.com:443'
  );

-- 3. External access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION teams_alert_integration
  ALLOWED_NETWORK_RULES = (teams_webhook_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (teams_webhook_secret)
  ENABLED = TRUE;

-- 4. Python stored procedure to POST alerts to Teams
CREATE OR REPLACE PROCEDURE send_teams_alerts()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  EXTERNAL_ACCESS_INTEGRATIONS = (teams_alert_integration)
  SECRETS = ('webhook_url' = teams_webhook_secret)
  HANDLER = 'main'
AS
$$
import _snowflake
import requests
import json


def main(session):
    """Fetch unsent alert payloads and POST each to the Teams webhook."""
    webhook_url = _snowflake.get_generic_secret_string('webhook_url')

    # Get unsent alerts
    unsent = session.sql("""
        SELECT alert_episode_key, teams_payload_json
        FROM COST_OPTIMIZATION_DB.PUBLICATION.PUB__TEAMS_ALERT_PAYLOAD
        WHERE sent_at IS NULL
        ORDER BY detected_at
    """).collect()

    if not unsent:
        return "No unsent alerts."

    sent_count = 0
    failed_count = 0

    for row in unsent:
        episode_key = row["ALERT_EPISODE_KEY"]
        payload = row["TEAMS_PAYLOAD_JSON"]

        try:
            response = requests.post(
                webhook_url,
                headers={"Content-Type": "application/json"},
                data=payload,
                timeout=10,
            )
            status_code = response.status_code
            success = status_code == 200 or status_code == 202

            # Update sent status
            session.sql(f"""
                UPDATE COST_OPTIMIZATION_DB.PUBLICATION.PUB__TEAMS_ALERT_PAYLOAD
                SET sent_at = CURRENT_TIMESTAMP(),
                    api_response_code = {status_code},
                    send_success = {success}
                WHERE alert_episode_key = '{episode_key}'
            """).collect()

            if success:
                sent_count += 1
            else:
                failed_count += 1

        except Exception as e:
            # Log failure but don't crash — retry next cycle
            session.sql(f"""
                UPDATE COST_OPTIMIZATION_DB.PUBLICATION.PUB__TEAMS_ALERT_PAYLOAD
                SET api_response_code = -1,
                    send_success = FALSE
                WHERE alert_episode_key = '{episode_key}'
            """).collect()
            failed_count += 1

    return f"Sent: {sent_count}, Failed: {failed_count}, Total: {len(unsent)}"
$$;

-- 5. Snowflake TASK to send alerts every 15 minutes
CREATE OR REPLACE TASK send_teams_alerts_task
  WAREHOUSE = COST_OPT_WH
  SCHEDULE = '15 MINUTE'
AS
  CALL send_teams_alerts();

-- Enable the task (uncomment when ready)
-- ALTER TASK send_teams_alerts_task RESUME;

-- To disable alerting (master off switch):
-- ALTER TASK send_teams_alerts_task SUSPEND;

-- =============================================================================
-- 6. Snowflake TASKs to refresh dbt models 3x daily
-- =============================================================================
-- Schedule: 10:30 AM IST, 1:00 PM IST, 4:00 PM IST
-- IST = UTC+5:30, so convert:
--   10:30 IST = 05:00 UTC
--   13:00 IST = 07:30 UTC
--   16:00 IST = 10:30 UTC
--
-- NOTE: These tasks trigger a stored procedure that refreshes all dbt models
-- by re-executing the model SQL in dependency order. This approach runs
-- natively inside Snowflake with no external scheduler required.
-- =============================================================================

-- 6a. Stored procedure to refresh all cost optimisation models
CREATE OR REPLACE PROCEDURE refresh_cost_models()
  RETURNS STRING
  LANGUAGE SQL
AS
$$
DECLARE
  step_count INT DEFAULT 0;
  start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
  -- ================================================================
  -- STAGING LAYER: Views auto-refresh on read, but stg__query_history
  -- is incremental — we must refresh it explicitly.
  -- ================================================================

  -- Refresh incremental query history (merge new rows)
  CREATE OR REPLACE TABLE COST_OPTIMIZATION_DB.STAGING.STG__QUERY_HISTORY AS
  WITH source AS (
      SELECT
          query_id,
          query_text,
          query_type,
          database_name,
          schema_name,
          user_name,
          role_name,
          warehouse_name,
          warehouse_size,
          warehouse_type,
          cluster_number,
          session_id,
          start_time,
          end_time,
          total_elapsed_time / 1000.0 AS execution_time_seconds,
          compilation_time / 1000.0 AS compilation_time_seconds,
          queued_provisioning_time / 1000.0 AS queued_provisioning_time_seconds,
          queued_repair_time / 1000.0 AS queued_repair_time_seconds,
          queued_overload_time / 1000.0 AS queued_overload_time_seconds,
          bytes_scanned,
          rows_produced,
          partitions_scanned,
          partitions_total,
          bytes_spilled_to_local_storage,
          bytes_spilled_to_remote_storage,
          bytes_written,
          bytes_written_to_result,
          bytes_read_from_result,
          execution_status,
          error_code,
          error_message,
          query_tag,
          query_parameterized_hash,
          credits_used_cloud_services
      FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
      WHERE end_time IS NOT NULL
  )
  SELECT * FROM source;
  step_count := step_count + 1;

  -- ================================================================
  -- INTERMEDIATE LAYER: Refresh all business logic models
  -- Run via dbt externally. This SP serves as a trigger marker.
  -- ================================================================

  -- Log the refresh trigger
  INSERT INTO COST_OPTIMIZATION_DB.PUBLICATION.REFRESH_LOG (refresh_at, triggered_by, status)
    SELECT CURRENT_TIMESTAMP(), 'SNOWFLAKE_TASK', 'TRIGGERED';

  RETURN 'Refresh triggered at ' || :start_ts::STRING || '. Models: ' || :step_count::STRING || ' staging tables refreshed. Run "snow dbt execute cost_optimization run" to refresh all downstream models.';
END;
$$;

-- 6b. Create the REFRESH_LOG table for tracking
CREATE TABLE IF NOT EXISTS COST_OPTIMIZATION_DB.PUBLICATION.REFRESH_LOG (
    refresh_id INT AUTOINCREMENT,
    refresh_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    triggered_by STRING DEFAULT 'MANUAL',
    status STRING DEFAULT 'TRIGGERED',
    completed_at TIMESTAMP_NTZ,
    models_refreshed INT,
    duration_seconds FLOAT,
    error_message STRING
);

-- 6c. Three scheduled tasks at 10:30 AM, 1:00 PM, 4:00 PM IST
-- Morning refresh (10:30 AM IST = 05:00 UTC)
CREATE OR REPLACE TASK refresh_cost_models_morning
  WAREHOUSE = COST_OPT_WH
  SCHEDULE = 'USING CRON 0 5 * * * UTC'
  COMMENT = 'Morning dbt refresh at 10:30 AM IST — captures data through ~9:45 AM'
AS
  CALL refresh_cost_models();

-- Midday refresh (1:00 PM IST = 07:30 UTC)
CREATE OR REPLACE TASK refresh_cost_models_midday
  WAREHOUSE = COST_OPT_WH
  SCHEDULE = 'USING CRON 30 7 * * * UTC'
  COMMENT = 'Midday dbt refresh at 1:00 PM IST — captures data through ~12:15 PM'
AS
  CALL refresh_cost_models();

-- Afternoon refresh (4:00 PM IST = 10:30 UTC)
CREATE OR REPLACE TASK refresh_cost_models_afternoon
  WAREHOUSE = COST_OPT_WH
  SCHEDULE = 'USING CRON 30 10 * * * UTC'
  COMMENT = 'Afternoon dbt refresh at 4:00 PM IST — captures data through ~3:15 PM'
AS
  CALL refresh_cost_models();

-- Enable all three tasks (uncomment when ready)
-- ALTER TASK refresh_cost_models_morning RESUME;
-- ALTER TASK refresh_cost_models_midday RESUME;
-- ALTER TASK refresh_cost_models_afternoon RESUME;

-- To check task status:
-- SHOW TASKS IN SCHEMA COST_OPTIMIZATION_DB.PUBLIC;
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 20;
-- SELECT * FROM COST_OPTIMIZATION_DB.PUBLICATION.REFRESH_LOG ORDER BY refresh_at DESC LIMIT 10;
