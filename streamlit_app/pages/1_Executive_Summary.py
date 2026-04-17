"""Executive Summary — Cost overview, MoM trends, anomaly markers."""

import streamlit as st
import plotly.graph_objects as go
from utils.connection import run_query
from utils.formatters import format_currency
from utils.queries import COST_TRENDS_DAILY, TOP_WAREHOUSES, TOP_USERS

st.set_page_config(page_title="Executive Summary", layout="wide")
st.title("Executive Summary")

days_back = st.selectbox("Date Range", [30, 60, 90, 180, 365], index=2,
                         format_func=lambda x: f"Last {x} days")

try:
    # ── Daily Cost Trend ─────────────────────────────────────────────
    query = COST_TRENDS_DAILY.format(days=days_back)
    df_trends = run_query(query)

    if not df_trends.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_trends["DATE"], y=df_trends["TOTAL_COST"],
            mode="lines", name="Daily Cost",
            line=dict(color="#636EFA", width=2),
        ))
        fig.add_trace(go.Scatter(
            x=df_trends["DATE"], y=df_trends["ROLLING_30D_AVG"],
            mode="lines", name="30-Day Avg",
            line=dict(color="#FFA15A", width=2, dash="dash"),
        ))

        # Mark anomalies
        anomalies = df_trends[df_trends["IS_ANOMALY"] == True]
        if not anomalies.empty:
            fig.add_trace(go.Scatter(
                x=anomalies["DATE"], y=anomalies["TOTAL_COST"],
                mode="markers", name="Anomaly",
                marker=dict(color="red", size=12, symbol="triangle-up"),
            ))

        fig.update_layout(title="Daily Total Cost", xaxis_title="Date",
                          yaxis_title="Cost (USD)", height=450)
        st.plotly_chart(fig, use_container_width=True)

        # ── MoM Comparison Bar ───────────────────────────────────────
        df_monthly = df_trends.copy()
        df_monthly["MONTH"] = df_monthly["DATE"].astype(str).str[:7]
        monthly_agg = df_monthly.groupby("MONTH")[["TOTAL_COST"]].sum().reset_index()
        monthly_agg = monthly_agg.tail(6)

        fig_bar = go.Figure(data=[go.Bar(
            x=monthly_agg["MONTH"], y=monthly_agg["TOTAL_COST"],
            marker_color="#636EFA",
        )])
        fig_bar.update_layout(title="Monthly Cost Comparison", height=350)
        st.plotly_chart(fig_bar, use_container_width=True)

    # ── Top Warehouses and Users ─────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 5 Warehouses by Cost")
        df_wh = run_query(TOP_WAREHOUSES)
        if not df_wh.empty:
            st.dataframe(df_wh, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Top 5 Users by Cost")
        df_usr = run_query(TOP_USERS)
        if not df_usr.empty:
            st.dataframe(df_usr, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading executive summary: {e}")
