"""Warehouse Optimizer — Right-sizing recommendations with savings estimates."""

import streamlit as st
import plotly.express as px
from utils.connection import run_query
from utils.formatters import format_currency

PUB = "COST_OPTIMIZATION_DB.PUBLICATION"

st.set_page_config(page_title="Warehouse Optimizer", layout="wide")
st.title("Warehouse Optimizer")

try:
    df = run_query(f"SELECT * FROM {PUB}.PUB__WAREHOUSE_RECOMMENDATIONS ORDER BY PRIORITY_SCORE DESC")

    if not df.empty:
        # ── Total Savings Banner ─────────────────────────────────────
        total_savings = df["ESTIMATED_MONTHLY_SAVINGS_USD"].sum()
        st.success(f"**Total Potential Monthly Savings: {format_currency(total_savings)}**")

        # ── Filter by effort ─────────────────────────────────────────
        effort_filter = st.multiselect(
            "Filter by Effort Level",
            ["LOW", "MEDIUM", "HIGH"],
            default=["LOW", "MEDIUM", "HIGH"],
        )
        df_filtered = df[df["EFFORT"].isin(effort_filter)]

        # ── Recommendation Cards ─────────────────────────────────────
        for _, row in df_filtered.iterrows():
            with st.expander(
                f"**{row['WAREHOUSE_NAME']}** — {row['RECOMMENDATION_TYPE']} "
                f"({format_currency(row['ESTIMATED_MONTHLY_SAVINGS_USD'])}/mo savings) "
                f"[{row['CONFIDENCE']} confidence, {row['EFFORT']} effort]"
            ):
                col1, col2 = st.columns(2)
                col1.write(f"**Current**: {row['CURRENT_STATE']}")
                col2.write(f"**Recommended**: {row['RECOMMENDED_STATE']}")
                st.write(f"**Priority Score**: {row['PRIORITY_SCORE']:.1f}")
                st.code(row["SQL_TO_APPLY"], language="sql")

        # ── Savings by type chart ────────────────────────────────────
        st.subheader("Savings by Recommendation Type")
        savings_by_type = df.groupby("RECOMMENDATION_TYPE")["ESTIMATED_MONTHLY_SAVINGS_USD"].sum().reset_index()
        fig = px.bar(savings_by_type, x="RECOMMENDATION_TYPE",
                     y="ESTIMATED_MONTHLY_SAVINGS_USD",
                     title="Monthly Savings by Recommendation Type",
                     color="RECOMMENDATION_TYPE")
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("No warehouse recommendations at this time. All warehouses appear optimally configured.")

except Exception as e:
    st.error(f"Error loading warehouse optimizer: {e}")
