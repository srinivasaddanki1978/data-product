"""Storage Explorer — Storage analysis by database/table with unused detection."""

import streamlit as st
import plotly.express as px
from utils.connection import run_query
from utils.formatters import format_currency, format_bytes
from utils.queries import STORAGE_ANALYSIS, STORAGE_BY_DATABASE

st.set_page_config(page_title="Storage Explorer", layout="wide")
st.title("Storage Explorer")

try:
    # ── Total storage cost card ──────────────────────────────────────
    df_db = run_query(STORAGE_BY_DATABASE)
    df_tables = run_query(STORAGE_ANALYSIS)

    if not df_db.empty:
        total_tb = df_db["TOTAL_TB"].sum()
        total_cost = df_db["TOTAL_COST_USD"].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Storage", f"{total_tb:,.2f} TB")
        c2.metric("Monthly Storage Cost", format_currency(total_cost))
        c3.metric("Databases", len(df_db))

        # ── Stacked bar: storage by database ─────────────────────────
        fig = px.bar(
            df_db, x="DATABASE_NAME",
            y=["ACTIVE_TB", "TIME_TRAVEL_TB", "FAILSAFE_TB"],
            title="Storage by Database (TB)",
            labels={"value": "TB", "variable": "Storage Type"},
            barmode="stack",
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

    # ── Largest tables ───────────────────────────────────────────────
    if not df_tables.empty:
        st.subheader("Largest Tables")
        st.dataframe(
            df_tables[["DATABASE_NAME", "SCHEMA_NAME", "TABLE_NAME",
                        "ACTIVE_TB", "TIME_TRAVEL_TB", "FAILSAFE_TB",
                        "TOTAL_TB", "ESTIMATED_MONTHLY_COST_USD",
                        "DAYS_SINCE_LAST_READ", "IS_UNUSED"]].head(50),
            use_container_width=True, hide_index=True,
        )

        # ── Unused tables (90+ days no reads) ────────────────────────
        df_unused = df_tables[df_tables["IS_UNUSED"] == True]
        if not df_unused.empty:
            st.subheader(f"Unused Tables (90+ days without reads) — {len(df_unused)} found")
            st.dataframe(
                df_unused[["DATABASE_NAME", "SCHEMA_NAME", "TABLE_NAME",
                            "TOTAL_TB", "ESTIMATED_MONTHLY_COST_USD",
                            "DAYS_SINCE_LAST_READ"]],
                use_container_width=True, hide_index=True,
            )
            unused_cost = df_unused["ESTIMATED_MONTHLY_COST_USD"].sum()
            st.warning(f"Total monthly cost of unused tables: {format_currency(unused_cost)}")

        # ── Time travel waste candidates ─────────────────────────────
        df_tt_waste = df_tables[df_tables["HAS_TT_WASTE"] == True]
        if not df_tt_waste.empty:
            st.subheader(f"Time Travel Waste — {len(df_tt_waste)} tables")
            st.caption("Tables where time travel storage exceeds active storage.")
            st.dataframe(
                df_tt_waste[["DATABASE_NAME", "TABLE_NAME",
                              "ACTIVE_TB", "TIME_TRAVEL_TB"]],
                use_container_width=True, hide_index=True,
            )

except Exception as e:
    st.error(f"Error loading storage explorer: {e}")
