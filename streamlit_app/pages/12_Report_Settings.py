"""Report Settings — Preview and configure weekly executive email reports."""

import streamlit as st
from utils.connection import run_query
from utils.formatters import format_currency, format_pct

PUB = "COST_OPTIMIZATION_DB.PUBLICATION"

st.set_page_config(page_title="Report Settings", layout="wide")
st.title("Report Settings")

try:
    # ── Report Preview ─────────────────────────────────────────────
    st.subheader("Weekly Executive Report Preview")
    df = run_query(f"SELECT * FROM {PUB}.PUB__WEEKLY_EXECUTIVE_REPORT")

    if not df.empty:
        row = df.iloc[0]

        # Cost summary
        col1, col2, col3 = st.columns(3)
        col1.metric("This Week Cost", format_currency(row.get("THIS_WEEK_COST", 0)))
        col2.metric("Last Week Cost", format_currency(row.get("LAST_WEEK_COST", 0)))
        wow_change = row.get("WOW_CHANGE_PCT", 0)
        col3.metric("WoW Change", format_pct(wow_change),
                     delta=f"{wow_change:+.1f}%")

        # Cost breakdown
        st.markdown("**Cost Breakdown**")
        bc1, bc2, bc3 = st.columns(3)
        bc1.metric("Compute", format_currency(row.get("THIS_WEEK_COMPUTE", 0)))
        bc2.metric("Storage", format_currency(row.get("THIS_WEEK_STORAGE", 0)))
        bc3.metric("Serverless", format_currency(row.get("THIS_WEEK_SERVERLESS", 0)))

        # Top cost drivers
        st.markdown("**Top Cost Drivers (Warehouses)**")
        top_wh = row.get("TOP_WAREHOUSES_JSON", None)
        if top_wh:
            import json
            try:
                warehouses = json.loads(top_wh) if isinstance(top_wh, str) else top_wh
                for i, wh in enumerate(warehouses[:3], 1):
                    st.text(f"  {i}. {wh.get('warehouse_name', 'N/A')}: "
                            f"${wh.get('total_cost_usd', 0):,.2f}")
            except Exception:
                st.text(f"  {top_wh}")

        # Alert summary
        st.markdown(f"**Active Alerts (Last 7 Days):** {int(row.get('ACTIVE_ALERT_COUNT', 0))}")

        # Savings opportunities
        st.markdown(f"**Unrealised Savings:** {format_currency(row.get('TOTAL_UNREALISED_SAVINGS', 0))}/month")

        # Last generated
        st.caption(f"Report generated at: {row.get('GENERATED_AT', 'N/A')}")

    else:
        st.info("No report data available. Run the dbt pipeline to generate the weekly report.")

    # ── Recipient Configuration ────────────────────────────────────
    st.subheader("Recipient Configuration")
    st.info("""
Recipients are configured in the Snowflake notification integration and stored procedure.
To update recipients:
1. Update `ALLOWED_RECIPIENTS` in the notification integration
2. Update the recipient array in the task definition
    """)

    # ── Manual Trigger ─────────────────────────────────────────────
    st.subheader("Manual Trigger")
    st.markdown("To manually send the report, run the following SQL in Snowflake:")
    st.code("""CALL COST_OPTIMIZATION_DB.PUBLICATION.send_weekly_report(
    ARRAY_CONSTRUCT('user@example.com')
);""", language="sql")

    st.markdown("To enable the automated weekly schedule:")
    st.code("ALTER TASK COST_OPTIMIZATION_DB.PUBLICATION.send_weekly_report_task RESUME;",
            language="sql")

except Exception as e:
    st.error(f"Error loading report settings: {e}")
