"""Team Attribution — Cost by team/role/user with drill-down."""

import streamlit as st
import plotly.express as px
from utils.connection import run_query
from utils.formatters import format_currency
from utils.queries import TEAM_COST_DASHBOARD, COST_BY_USER, COST_BY_QUERY_TYPE

st.set_page_config(page_title="Team Attribution", layout="wide")
st.title("Team Attribution")

try:
    # ── Team cost treemap ────────────────────────────────────────────
    df_team = run_query(TEAM_COST_DASHBOARD)

    if not df_team.empty:
        # Get latest month
        latest_month = df_team["MONTH"].max()
        df_latest = df_team[df_team["MONTH"] == latest_month]

        st.subheader(f"Cost by Team — {latest_month}")

        if len(df_latest) > 0:
            fig_tree = px.treemap(
                df_latest,
                path=["TEAM_NAME"],
                values="MONTHLY_COST",
                color="MONTHLY_COST",
                color_continuous_scale="Blues",
                title="Cost Distribution by Team",
            )
            fig_tree.update_layout(height=400)
            st.plotly_chart(fig_tree, use_container_width=True)

        # ── Team selector for drill-down ─────────────────────────────
        teams = df_team["TEAM_NAME"].unique().tolist()
        selected_team = st.selectbox("Select Team for Details", ["All"] + teams)

        if selected_team != "All":
            team_data = df_team[df_team["TEAM_NAME"] == selected_team]
            st.dataframe(team_data, use_container_width=True)

        # ── Percentage contribution pie ──────────────────────────────
        fig_pie = px.pie(
            df_latest, names="TEAM_NAME", values="MONTHLY_COST",
            title="Cost Share by Team",
        )
        fig_pie.update_layout(height=350)
        st.plotly_chart(fig_pie, use_container_width=True)

    # ── User-level cost breakdown ────────────────────────────────────
    st.subheader("User Cost Breakdown")
    df_users = run_query(COST_BY_USER)
    if not df_users.empty:
        st.dataframe(
            df_users[["USER_NAME", "TOTAL_QUERIES", "TOTAL_COST_USD",
                       "AVG_COST_PER_QUERY", "OVERALL_COST_RANK"]].head(20),
            use_container_width=True,
        )

    # ── Cost by query type ───────────────────────────────────────────
    st.subheader("Cost by Query Type")
    df_qt = run_query(COST_BY_QUERY_TYPE)
    if not df_qt.empty:
        fig_qt = px.bar(
            df_qt, x="QUERY_TYPE", y="TOTAL_COST_USD",
            title="Cost by Query Type", color="QUERY_TYPE",
        )
        fig_qt.update_layout(height=350)
        st.plotly_chart(fig_qt, use_container_width=True)

except Exception as e:
    st.error(f"Error loading team attribution: {e}")
