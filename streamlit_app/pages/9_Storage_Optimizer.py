"""Storage Optimizer — Identify storage waste and savings opportunities."""

import streamlit as st
import plotly.express as px
from utils.connection import run_query
from utils.formatters import format_currency, format_number

PUB = "COST_OPTIMIZATION_DB.PUBLICATION"
INT = "COST_OPTIMIZATION_DB.INTERMEDIATE"

st.set_page_config(page_title="Storage Optimizer", layout="wide")
st.title("Storage Optimizer")

try:
    # ── Summary metrics ──────────────────────────────────────────────
    df_storage = run_query(f"SELECT * FROM {PUB}.PUB__STORAGE_ANALYSIS ORDER BY TOTAL_TB DESC")
    df_unused = run_query(f"SELECT * FROM {INT}.INT__STORAGE_UNUSED_TABLES ORDER BY SAVINGS_IF_DROPPED_USD DESC")
    df_tt_waste = run_query(f"SELECT * FROM {INT}.INT__STORAGE_TIME_TRAVEL_WASTE ORDER BY ESTIMATED_SAVINGS_USD DESC")
    df_transient = run_query(f"SELECT * FROM {INT}.INT__STORAGE_TRANSIENT_CANDIDATES ORDER BY ESTIMATED_SAVINGS_USD DESC")

    total_cost = df_storage["ESTIMATED_MONTHLY_COST_USD"].sum() if not df_storage.empty else 0
    waste_total = (
        (df_unused["SAVINGS_IF_DROPPED_USD"].sum() if not df_unused.empty else 0)
        + (df_tt_waste["ESTIMATED_SAVINGS_USD"].sum() if not df_tt_waste.empty else 0)
        + (df_transient["ESTIMATED_SAVINGS_USD"].sum() if not df_transient.empty else 0)
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Storage Cost", format_currency(total_cost), help="Monthly")
    c2.metric("Waste Identified", format_currency(waste_total), help="Potential monthly savings")
    c3.metric("Waste %", f"{(waste_total / total_cost * 100):.1f}%" if total_cost > 0 else "0%")

    # ── Storage treemap by database ──────────────────────────────────
    if not df_storage.empty:
        db_agg = df_storage.groupby("DATABASE_NAME").agg({
            "TOTAL_TB": "sum",
            "ESTIMATED_MONTHLY_COST_USD": "sum"
        }).reset_index()

        fig = px.treemap(
            db_agg, path=["DATABASE_NAME"], values="TOTAL_TB",
            color="ESTIMATED_MONTHLY_COST_USD",
            color_continuous_scale="Reds",
            title="Storage by Database (TB)"
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

    # ── Unused tables ────────────────────────────────────────────────
    if not df_unused.empty:
        st.subheader(f"Unused Tables — {len(df_unused)} found")
        st.dataframe(df_unused, use_container_width=True, hide_index=True)

    # ── TT waste candidates ──────────────────────────────────────────
    if not df_tt_waste.empty:
        st.subheader(f"Time Travel Waste — {len(df_tt_waste)} tables")
        st.dataframe(df_tt_waste, use_container_width=True, hide_index=True)

    # ── Transient candidates ─────────────────────────────────────────
    if not df_transient.empty:
        st.subheader(f"Transient Table Candidates — {len(df_transient)} tables")
        st.dataframe(df_transient, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading storage optimizer: {e}")
