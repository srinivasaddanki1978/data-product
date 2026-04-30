{% macro call_send_teams_alerts() %}
{% if execute %}
{% set check_result = run_query("SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_NAME = 'SEND_TEAMS_ALERTS'") %}
{% if check_result and check_result.rows[0][0] > 0 %}
CALL send_teams_alerts()
{% else %}
SELECT 'send_teams_alerts not deployed yet — skipping' AS status
{% endif %}
{% else %}
SELECT 1
{% endif %}
{% endmacro %}
