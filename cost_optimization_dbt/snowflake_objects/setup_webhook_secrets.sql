USE ROLE ACCOUNTADMIN;
USE DATABASE COST_OPTIMIZATION_DB;
USE SCHEMA PUBLIC;

-- Per-channel webhook secrets for Teams alerting
CREATE OR REPLACE SECRET cost_alerts_webhook_secret
  TYPE = GENERIC_STRING
  SECRET_STRING = 'https://defaultde45a50d90ca4e23b9090fb176ddcf.4f.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/00c82b93ab634cff8b5e580107b8a4f1/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=IIh9DL1DFhcqlZONnJcfnYyKEOL22GKQJD2yW73r9ws';

CREATE OR REPLACE SECRET finance_alerts_webhook_secret
  TYPE = GENERIC_STRING
  SECRET_STRING = 'https://defaultde45a50d90ca4e23b9090fb176ddcf.4f.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/978ce3dbf1884d3abda8f8c5aeeb776f/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=D7rKI2uXQS_JJvgMxd2E8BEqJXFTgI2H-QGlr2iW9JM';

-- Update external access integration to include both secrets
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION teams_alert_integration
  ALLOWED_NETWORK_RULES = (teams_webhook_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (cost_alerts_webhook_secret, finance_alerts_webhook_secret)
  ENABLED = TRUE;

-- Updated procedure: routes alerts to the correct channel webhook
CREATE OR REPLACE PROCEDURE send_teams_alerts()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  EXTERNAL_ACCESS_INTEGRATIONS = (teams_alert_integration)
  SECRETS = ('cost_alerts_url' = cost_alerts_webhook_secret, 'finance_alerts_url' = finance_alerts_webhook_secret)
  HANDLER = 'main'
AS
$$
import _snowflake
import requests
import json


def main(session):
    """Fetch unsent alert payloads and POST each to the correct Teams channel webhook."""
    # Per-channel webhook URLs
    webhook_urls = {
        'cost-alerts': _snowflake.get_generic_secret_string('cost_alerts_url'),
        'finance-alerts': _snowflake.get_generic_secret_string('finance_alerts_url'),
    }

    # Get unsent alerts with their configured channel
    unsent = session.sql("""
        SELECT
            p.alert_episode_key,
            p.teams_payload_json,
            c.teams_channel
        FROM COST_OPTIMIZATION_DB.PUBLICATION.PUB__TEAMS_ALERT_PAYLOAD p
        JOIN COST_OPTIMIZATION_DB.SEEDS.ALERT_CONFIGURATION c
          ON p.alert_id = c.alert_id
        WHERE p.sent_at IS NULL
        ORDER BY p.detected_at
    """).collect()

    if not unsent:
        return "No unsent alerts."

    sent_count = 0
    failed_count = 0

    for row in unsent:
        episode_key = row["ALERT_EPISODE_KEY"]
        payload = row["TEAMS_PAYLOAD_JSON"]
        channel = row["TEAMS_CHANNEL"]

        # Look up webhook URL for this channel; fall back to cost-alerts
        webhook_url = webhook_urls.get(channel, webhook_urls.get('cost-alerts'))

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
