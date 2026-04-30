"""Query Optimizer — Anti-pattern detection with optimization recommendations."""

import streamlit as st
import plotly.express as px
from utils.connection import run_query
from utils.formatters import format_currency, format_number

PUB = "COST_OPTIMIZATION_DB.PUBLICATION"

st.set_page_config(page_title="Query Optimizer", layout="wide")
st.title("Query Optimizer")

try:
    # ── Summary cards ────────────────────────────────────────────────
    df_summary = run_query(f"SELECT * FROM {PUB}.PUB__ANTIPATTERN_SUMMARY")
    df_candidates = run_query(f"SELECT * FROM {PUB}.PUB__QUERY_OPTIMIZATION_CANDIDATES")

    if not df_summary.empty:
        total_queries = df_summary["QUERY_COUNT"].sum()
        total_waste = df_summary["TOTAL_ESTIMATED_WASTE"].sum()

        c1, c2 = st.columns(2)
        c1.metric("Queries with Anti-Patterns", format_number(total_queries))
        c2.metric("Total Estimated Waste", format_currency(total_waste))

        # ── Waste by anti-pattern type ───────────────────────────────
        fig = px.bar(
            df_summary, x="ANTIPATTERN_TYPE", y="TOTAL_ESTIMATED_WASTE",
            title="Estimated Waste by Anti-Pattern Type",
            color="ANTIPATTERN_TYPE",
            text="QUERY_COUNT",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

        # ── Trend indicator ──────────────────────────────────────────
        st.subheader("30-Day Trend")
        for _, row in df_summary.iterrows():
            trend = row.get("TREND_PCT")
            trend_str = f"{trend:+.1f}%" if trend is not None else "N/A"
            direction = "increasing" if trend and trend > 0 else "decreasing" if trend and trend < 0 else "stable"
            st.write(f"**{row['ANTIPATTERN_TYPE']}**: {format_number(row['QUERY_COUNT'])} queries "
                     f"({format_currency(row['TOTAL_ESTIMATED_WASTE'])} waste) — "
                     f"Trend: {trend_str} ({direction})")

    # ── Filters ──────────────────────────────────────────────────────
    st.subheader("Top Optimization Candidates")
    if not df_candidates.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            type_filter = st.multiselect(
                "Anti-Pattern Type",
                df_candidates["ANTIPATTERN_TYPE"].unique().tolist(),
                default=df_candidates["ANTIPATTERN_TYPE"].unique().tolist(),
            )
        with col2:
            user_filter = st.multiselect(
                "User",
                df_candidates["USER_NAME"].dropna().unique().tolist(),
            )
        with col3:
            wh_filter = st.multiselect(
                "Warehouse",
                df_candidates["WAREHOUSE_NAME"].dropna().unique().tolist(),
            )

        df_filtered = df_candidates[df_candidates["ANTIPATTERN_TYPE"].isin(type_filter)]
        if user_filter:
            df_filtered = df_filtered[df_filtered["USER_NAME"].isin(user_filter)]
        if wh_filter:
            df_filtered = df_filtered[df_filtered["WAREHOUSE_NAME"].isin(wh_filter)]

        st.dataframe(
            df_filtered[["OPTIMIZATION_RANK", "ANTIPATTERN_TYPE", "SEVERITY",
                          "USER_NAME", "WAREHOUSE_NAME", "ESTIMATED_WASTE_USD",
                          "RECOMMENDATION", "SAMPLE_QUERY_TEXT"]],
            use_container_width=True,
        )
    else:
        st.info("No anti-patterns detected. Queries are running efficiently.")

except Exception as e:
    st.error(f"Error loading query optimizer: {e}")
