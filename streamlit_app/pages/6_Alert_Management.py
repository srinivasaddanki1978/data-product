"""Alert Management — View alert configurations, history, and pipeline health."""

import streamlit as st
import plotly.express as px
from utils.connection import run_query
from utils.formatters import format_number

PUB = "COST_OPTIMIZATION_DB.PUBLICATION"
SEEDS = "COST_OPTIMIZATION_DB.SEEDS"

st.set_page_config(page_title="Alert Management", layout="wide")
st.title("Alert Management")

try:
    # ── Alert Configuration Status ───────────────────────────────────
    st.subheader("Alert Configurations")
    df_config = run_query(f"SELECT * FROM {SEEDS}.ALERT_CONFIGURATION ORDER BY severity")
    if not df_config.empty:
        st.dataframe(df_config, use_container_width=True, hide_index=True)

    # ── Pipeline Health ──────────────────────────────────────────────
    st.subheader("Pipeline Health")
    col1, col2, col3 = st.columns(3)

    df_payload = run_query(f"""
        SELECT
            COUNT(*) AS total_alerts,
            SUM(CASE WHEN send_success = TRUE THEN 1 ELSE 0 END) AS sent_ok,
            SUM(CASE WHEN send_success = FALSE THEN 1 ELSE 0 END) AS sent_fail,
            SUM(CASE WHEN sent_at IS NULL THEN 1 ELSE 0 END) AS pending,
            MAX(sent_at) AS last_sent
        FROM {PUB}.PUB__TEAMS_ALERT_PAYLOAD
    """)

    if not df_payload.empty and df_payload.iloc[0]["TOTAL_ALERTS"] > 0:
        row = df_payload.iloc[0]
        col1.metric("Total Alert Payloads", format_number(row["TOTAL_ALERTS"]))
        col2.metric("Successfully Sent", format_number(row.get("SENT_OK", 0)))
        col3.metric("Pending / Failed",
                     f"{format_number(row.get('PENDING', 0))} / {format_number(row.get('SENT_FAIL', 0))}")
        if row.get("LAST_SENT"):
            st.info(f"Last alert sent: {row['LAST_SENT']}")
    else:
        st.info("No alert payloads generated yet.")

    # ── Seasonality-Aware Detection ──────────────────────────────────
    with st.expander("About Seasonality-Aware Anomaly Detection"):
        st.markdown("""
**How it works:** The Daily Cost Spike alert uses a seasonality-aware z-score
approach instead of a simple 2x multiplier. The system:

1. Computes **day-of-week baselines** from 90 days of history (e.g., Mondays
   typically cost more than weekends)
2. Applies a **linear trend adjustment** so growth doesn't trigger false alarms
3. Uses **higher thresholds on month-end days** (last 3 days of each month)
   where batch processing often causes natural spikes
4. Falls back to the **simple 2x multiplier** when insufficient history exists

An alert fires when the daily cost exceeds the adjusted baseline by more than
2.0 standard deviations (configurable via `seasonality_sensitivity` in
alert_configuration).
        """)

    # ── Recent Alert History ─────────────────────────────────────────
    st.subheader("Recent Alert History (Last 50)")
    df_history = run_query(f"""
        SELECT
            alert_id, alert_name, severity, detected_at, resource_key,
            metric_value, threshold_value, is_new_episode, episode_number,
            teams_sent_at, teams_send_success
        FROM {PUB}.PUB__ALERT_HISTORY
        ORDER BY detected_at DESC
        LIMIT 50
    """)
    if not df_history.empty:
        st.dataframe(df_history, use_container_width=True, hide_index=True)
    else:
        st.info("No alerts in history.")

    # ── Alerts Over Time by Severity ─────────────────────────────────
    st.subheader("Alerts Fired Over Time")
    df_trend = run_query(f"""
        SELECT
            detected_at::DATE AS alert_date,
            severity,
            COUNT(*) AS alert_count
        FROM {PUB}.PUB__ALERT_HISTORY
        WHERE is_new_episode = TRUE
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if not df_trend.empty:
        fig = px.bar(df_trend, x="ALERT_DATE", y="ALERT_COUNT",
                     color="SEVERITY", barmode="stack",
                     title="New Alert Episodes by Severity")
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # ── Data Source Freshness ──────────────────────────────────────────
    st.subheader("Data Source Freshness")
    df_freshness = run_query(f"""
        SELECT SOURCE_NAME, MODEL_NAME, LATEST_RECORD_AT, STALENESS_MINUTES, FRESHNESS_STATUS
        FROM {PUB}.PUB__DATA_FRESHNESS
        ORDER BY STALENESS_MINUTES DESC
    """)
    if not df_freshness.empty:
        st.dataframe(df_freshness, use_container_width=True, hide_index=True)
    else:
        st.info("Data freshness information not available. Run the dbt pipeline to generate freshness data.")

except Exception as e:
    st.error(f"Error loading alert management: {e}")
