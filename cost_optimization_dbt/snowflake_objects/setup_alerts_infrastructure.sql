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

-- 6. Snowflake TASK to refresh dbt models every 6 hours
CREATE OR REPLACE TASK refresh_cost_models_task
  WAREHOUSE = COST_OPT_WH
  SCHEDULE = '360 MINUTE'
AS
  SELECT 1;  -- Placeholder: replace with native dbt task trigger or SP call

-- ALTER TASK refresh_cost_models_task RESUME;
