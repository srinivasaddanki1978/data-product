"""Warehouse Deep Dive — Per-warehouse analysis with efficiency scores."""

import streamlit as st
import plotly.graph_objects as go
from utils.connection import run_query
from utils.formatters import format_currency, format_pct
from utils.queries import (
    WAREHOUSE_LIST, WAREHOUSE_DETAILS, WAREHOUSE_EFFICIENCY,
    WAREHOUSE_DAILY_CREDITS, IDLE_PERIODS,
)

st.set_page_config(page_title="Warehouse Deep Dive", layout="wide")
st.title("Warehouse Deep Dive")

try:
    # ── Warehouse selector ───────────────────────────────────────────
    df_wh_list = run_query(WAREHOUSE_LIST)
    warehouses = ["All"] + df_wh_list["WAREHOUSE_NAME"].tolist() if not df_wh_list.empty else ["All"]
    selected_wh = st.selectbox("Select Warehouse", warehouses)

    days_back = st.selectbox("Date Range", [30, 60, 90], index=0,
                             format_func=lambda x: f"Last {x} days")

    # ── Warehouse summary ────────────────────────────────────────────
    df_details = run_query(WAREHOUSE_DETAILS)
    df_efficiency = run_query(WAREHOUSE_EFFICIENCY)

    if selected_wh != "All":
        df_details = df_details[df_details["WAREHOUSE_NAME"] == selected_wh]
        df_efficiency = df_efficiency[df_efficiency["WAREHOUSE_NAME"] == selected_wh]

    if not df_efficiency.empty:
        for _, row in df_efficiency.iterrows():
            wh_name = row["WAREHOUSE_NAME"]
            score = row.get("EFFICIENCY_SCORE", 0)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric(f"{wh_name} — Efficiency Score", f"{score:.0f}/100")

            # Gauge chart for utilisation
            util_pct = row.get("UTILISATION_PCT", 0)
            color = "#2ECC71" if util_pct > 70 else "#F39C12" if util_pct > 40 else "#E74C3C"
            col2.metric("Utilisation", format_pct(util_pct), delta=None)
            col3.metric("Idle %", format_pct(row.get("IDLE_PCT", 0)))
            col4.metric("Queue %", format_pct(row.get("QUEUE_PCT", 0)))

            st.info(f"**Recommendation**: {row.get('PRIMARY_RECOMMENDATION', 'N/A')}")
            st.divider()

    # ── Daily credits chart ──────────────────────────────────────────
    df_credits = run_query(WAREHOUSE_DAILY_CREDITS.format(days=days_back))
    if not df_credits.empty:
        if selected_wh != "All":
            df_credits = df_credits[df_credits["WAREHOUSE_NAME"] == selected_wh]

        fig = go.Figure()
        for wh in df_credits["WAREHOUSE_NAME"].unique():
            wh_data = df_credits[df_credits["WAREHOUSE_NAME"] == wh]
            fig.add_trace(go.Bar(
                x=wh_data["DATE"], y=wh_data["CREDITS_COMPUTE"],
                name=f"{wh} (Compute)",
            ))
            fig.add_trace(go.Bar(
                x=wh_data["DATE"], y=wh_data["CREDITS_CLOUD"],
                name=f"{wh} (Cloud)",
            ))
        fig.update_layout(title="Daily Credits: Compute vs Cloud Services",
                          barmode="stack", height=400)
        st.plotly_chart(fig, use_container_width=True)

    # ── Idle periods table ───────────────────────────────────────────
    if selected_wh != "All":
        st.subheader("Idle Periods")
        df_idle = run_query(IDLE_PERIODS.format(warehouse=selected_wh))
        if not df_idle.empty:
            st.dataframe(df_idle, use_container_width=True)
        else:
            st.success("No significant idle periods detected.")

except Exception as e:
    st.error(f"Error loading warehouse analysis: {e}")
