"""Cost Optimization Dashboard — Main Entry Point.

Displays KPI cards and cost split donut chart. Navigation to detail pages via sidebar.
"""

import streamlit as st
from utils.connection import run_query
from utils.formatters import format_currency, change_badge
from utils.queries import COST_SUMMARY, DATA_FRESHNESS_SUMMARY

st.set_page_config(page_title="Cost Optimization Dashboard", layout="wide")
st.title("Snowflake Cost Optimization Dashboard")

# ── Data Freshness Banner ────────────────────────────────────────────
try:
    df_fresh = run_query(DATA_FRESHNESS_SUMMARY)
    if not df_fresh.empty:
        row = df_fresh.iloc[0]
        status = row.get("OVERALL_FRESHNESS_STATUS", "UNKNOWN")
        staleness = row.get("OVERALL_MAX_STALENESS_MINUTES", 0)
        oldest = row.get("OVERALL_OLDEST_RECORD_AT", "N/A")
        msg = f"Data as of: {oldest} ({int(staleness)} min ago)"
        if status == "FRESH":
            st.success(msg)
        elif status == "STALE":
            st.warning(msg)
        else:
            st.error(msg)
except Exception:
    pass  # Don't break dashboard if freshness data unavailable

# ── Global date range filter ──────────────────────────────────────────
col_filter1, col_filter2 = st.columns(2)
with col_filter1:
    days_back = st.selectbox("Date Range", [30, 60, 90, 180, 365], index=0,
                             format_func=lambda x: f"Last {x} days")

# ── KPI Cards ────────────────────────────────────────────────────────
try:
    df = run_query(COST_SUMMARY)

    if not df.empty:
        current = df.iloc[0]
        total_spend = current.get("TOTAL_COST", 0)
        compute_cost = current.get("COMPUTE_COST", 0)
        storage_cost = current.get("STORAGE_COST", 0)
        serverless_cost = current.get("SERVERLESS_COST", 0)
        mom_change = current.get("MOM_CHANGE_PCT", 0)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Spend (MTD)", format_currency(total_spend),
                  delta=change_badge(mom_change))
        c2.metric("Compute Cost", format_currency(compute_cost))
        c3.metric("Storage Cost", format_currency(storage_cost))
        c4.metric("Serverless Cost", format_currency(serverless_cost))

        # ── Cost Split Donut Chart ────────────────────────────────────
        import plotly.graph_objects as go

        fig = go.Figure(data=[go.Pie(
            labels=["Compute", "Storage", "Serverless"],
            values=[compute_cost, storage_cost, serverless_cost],
            hole=0.5,
            marker_colors=["#636EFA", "#EF553B", "#00CC96"],
            textinfo="label+percent",
        )])
        fig.update_layout(
            title_text="Cost Split by Category",
            showlegend=True,
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Monthly Summary Table ─────────────────────────────────────
        st.subheader("Monthly Cost Summary")
        st.dataframe(
            df[["MONTH", "TOTAL_COST", "COMPUTE_COST", "STORAGE_COST",
                "SERVERLESS_COST", "MOM_CHANGE_PCT"]].head(6),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.warning("No cost data available. Please run the dbt pipeline first.")
except Exception as e:
    st.error(f"Error loading dashboard data: {e}")
