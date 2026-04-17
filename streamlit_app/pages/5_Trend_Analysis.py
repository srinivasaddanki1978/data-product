"""Trend Analysis — 90-day cost trends with anomaly detection."""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from utils.connection import run_query
from utils.formatters import format_currency
from utils.queries import COST_TRENDS_DAILY, ANOMALY_DAYS

st.set_page_config(page_title="Trend Analysis", layout="wide")
st.title("Trend Analysis")

days_back = st.selectbox("Date Range", [30, 60, 90, 180, 365], index=2,
                         format_func=lambda x: f"Last {x} days")

try:
    query = COST_TRENDS_DAILY.format(days=days_back)
    df = run_query(query)

    if not df.empty:
        # ── Multi-line trend chart ───────────────────────────────────
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["DATE"], y=df["COMPUTE_COST"],
                                 mode="lines", name="Compute",
                                 line=dict(color="#636EFA")))
        fig.add_trace(go.Scatter(x=df["DATE"], y=df["STORAGE_COST"],
                                 mode="lines", name="Storage",
                                 line=dict(color="#EF553B")))
        fig.add_trace(go.Scatter(x=df["DATE"], y=df["SERVERLESS_COST"],
                                 mode="lines", name="Serverless",
                                 line=dict(color="#00CC96")))
        fig.add_trace(go.Scatter(x=df["DATE"], y=df["ROLLING_7D_AVG"],
                                 mode="lines", name="7-Day Avg",
                                 line=dict(color="#FFA15A", dash="dot")))
        fig.update_layout(title="Cost Trends by Category", height=450,
                          xaxis_title="Date", yaxis_title="Cost (USD)")
        st.plotly_chart(fig, use_container_width=True)

        # ── Week-over-week comparison ────────────────────────────────
        st.subheader("Week-over-Week Comparison")
        df_weekly = df.copy()
        df_weekly["WEEK"] = df_weekly["WEEK_START"]
        weekly_agg = df_weekly.groupby("WEEK")[["TOTAL_COST", "COMPUTE_COST",
                                                 "STORAGE_COST", "SERVERLESS_COST"]].sum()
        weekly_agg = weekly_agg.reset_index().tail(12)
        st.dataframe(weekly_agg, use_container_width=True, hide_index=True)

        # ── Anomaly detection highlights ─────────────────────────────
        st.subheader("Anomaly Days")
        df_anomalies = run_query(ANOMALY_DAYS)
        if not df_anomalies.empty:
            st.warning(f"{len(df_anomalies)} anomaly days detected (cost > 2x baseline)")
            st.dataframe(
                df_anomalies[["DATE", "TOTAL_COST", "ROLLING_30D_AVG"]],
                use_container_width=True, hide_index=True,
            )
        else:
            st.success("No anomalies detected in the selected period.")

        # ── Heatmap: cost by day of week ─────────────────────────────
        st.subheader("Cost by Day of Week")
        dow_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu",
                     4: "Fri", 5: "Sat", 6: "Sun"}
        df_heatmap = df.copy()
        df_heatmap["DAY_NAME"] = df_heatmap["DAY_OF_WEEK"].map(dow_names)
        dow_agg = df_heatmap.groupby("DAY_NAME")[["TOTAL_COST"]].mean().reset_index()
        fig_bar = px.bar(dow_agg, x="DAY_NAME", y="TOTAL_COST",
                         title="Average Daily Cost by Day of Week",
                         color="TOTAL_COST", color_continuous_scale="Blues")
        fig_bar.update_layout(height=350)
        st.plotly_chart(fig_bar, use_container_width=True)

    else:
        st.warning("No trend data available.")

except Exception as e:
    st.error(f"Error loading trend analysis: {e}")
