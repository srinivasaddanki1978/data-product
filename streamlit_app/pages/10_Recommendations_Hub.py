"""Recommendations Hub — The 'money page': unified savings report across all categories."""

import streamlit as st
import plotly.express as px
from utils.connection import run_query
from utils.formatters import format_currency, format_number, format_pct

PUB = "COST_OPTIMIZATION_DB.PUBLICATION"

st.set_page_config(page_title="Recommendations Hub", layout="wide")
st.title("Recommendations Hub")

try:
    tab_recs, tab_roi = st.tabs(["Recommendations", "ROI Dashboard"])

    # ── Tab 1: Recommendations (existing functionality) ────────────
    with tab_recs:
        df = run_query(f"SELECT * FROM {PUB}.PUB__ALL_RECOMMENDATIONS ORDER BY OVERALL_RANK")

        if not df.empty:
            # ── Total Savings Banner ───────────────────────────────
            total_savings = df["ESTIMATED_MONTHLY_SAVINGS_USD"].sum()
            st.success(f"### We identified **{format_currency(total_savings)}/month** in potential savings")

            # ── Summary metrics ────────────────────────────────────
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Recommendations", format_number(len(df)))
            c2.metric("Warehouse Savings",
                       format_currency(df[df["CATEGORY"] == "WAREHOUSE"]["ESTIMATED_MONTHLY_SAVINGS_USD"].sum()))
            c3.metric("Query Savings",
                       format_currency(df[df["CATEGORY"] == "QUERY"]["ESTIMATED_MONTHLY_SAVINGS_USD"].sum()))
            c4.metric("Storage Savings",
                       format_currency(df[df["CATEGORY"] == "STORAGE"]["ESTIMATED_MONTHLY_SAVINGS_USD"].sum()))

            # ── Savings by category pie ────────────────────────────
            col1, col2 = st.columns(2)
            with col1:
                cat_savings = df.groupby("CATEGORY")["ESTIMATED_MONTHLY_SAVINGS_USD"].sum().reset_index()
                fig_pie = px.pie(cat_savings, names="CATEGORY",
                                 values="ESTIMATED_MONTHLY_SAVINGS_USD",
                                 title="Savings by Category",
                                 color_discrete_sequence=["#636EFA", "#EF553B", "#00CC96"])
                fig_pie.update_layout(height=350)
                st.plotly_chart(fig_pie, use_container_width=True)

            with col2:
                effort_savings = df.groupby("EFFORT")["ESTIMATED_MONTHLY_SAVINGS_USD"].sum().reset_index()
                fig_bar = px.bar(effort_savings, x="EFFORT",
                                 y="ESTIMATED_MONTHLY_SAVINGS_USD",
                                 title="Savings by Effort Level",
                                 color="EFFORT",
                                 color_discrete_map={"LOW": "#2ECC71", "MEDIUM": "#F39C12", "HIGH": "#E74C3C"})
                fig_bar.update_layout(height=350)
                st.plotly_chart(fig_bar, use_container_width=True)

            # ── Filters ────────────────────────────────────────────
            st.subheader("All Recommendations")
            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                cat_filter = st.multiselect("Category",
                                             df["CATEGORY"].unique().tolist(),
                                             default=df["CATEGORY"].unique().tolist())
            with col_f2:
                effort_filter = st.multiselect("Effort",
                                                ["LOW", "MEDIUM", "HIGH"],
                                                default=["LOW", "MEDIUM", "HIGH"])
            with col_f3:
                min_savings = st.number_input("Min Monthly Savings ($)", value=0.0, step=10.0)

            df_filtered = df[
                (df["CATEGORY"].isin(cat_filter))
                & (df["EFFORT"].isin(effort_filter))
                & (df["ESTIMATED_MONTHLY_SAVINGS_USD"] >= min_savings)
            ]

            # Add STATUS column from lifecycle if available
            try:
                df_lifecycle = run_query(f"""
                    SELECT RECOMMENDATION_ID, STATUS
                    FROM {PUB}.PUB__RECOMMENDATION_ROI
                """)
                if not df_lifecycle.empty:
                    df_filtered = df_filtered.merge(
                        df_lifecycle[["RECOMMENDATION_ID", "STATUS"]],
                        on="RECOMMENDATION_ID", how="left"
                    )
                    df_filtered["STATUS"] = df_filtered["STATUS"].fillna("OPEN")
            except Exception:
                pass  # Lifecycle data not yet available

            display_cols = ["RECOMMENDATION_ID", "CATEGORY", "RECOMMENDATION_TYPE",
                            "TARGET_OBJECT", "DESCRIPTION", "ESTIMATED_MONTHLY_SAVINGS_USD",
                            "EFFORT", "CONFIDENCE", "PRIORITY_SCORE", "ACTION_SQL"]
            if "STATUS" in df_filtered.columns:
                display_cols.insert(5, "STATUS")

            st.dataframe(
                df_filtered[display_cols],
                use_container_width=True, hide_index=True,
            )

            # ── CSV Export ─────────────────────────────────────────
            csv = df_filtered.to_csv(index=False)
            st.download_button(
                label="Export to CSV",
                data=csv,
                file_name="cost_optimization_recommendations.csv",
                mime="text/csv",
            )

        else:
            st.info("No recommendations available. Run the full dbt pipeline to generate insights.")

    # ── Tab 2: ROI Dashboard ───────────────────────────────────────
    with tab_roi:
        df_roi = run_query(f"SELECT * FROM {PUB}.PUB__RECOMMENDATION_ROI")

        if not df_roi.empty:
            # KPI row
            row = df_roi.iloc[0]
            total_est = row.get("TOTAL_ESTIMATED_SAVINGS", 0)
            impl_est = row.get("IMPLEMENTED_ESTIMATED_SAVINGS", 0)
            actual = row.get("TOTAL_ACTUAL_SAVINGS", 0)
            roi = (actual / impl_est * 100) if impl_est and impl_est > 0 else 0

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Estimated Savings", format_currency(total_est) + "/mo")
            k2.metric("Implemented Estimated", format_currency(impl_est) + "/mo")
            k3.metric("Actual Realized", format_currency(actual) + "/mo")
            k4.metric("ROI %", format_pct(roi))

            # Conversion funnel
            st.subheader("Recommendation Funnel")
            funnel_data = {
                "Stage": ["Open", "Accepted", "Implemented", "Rejected", "Deferred"],
                "Count": [
                    row.get("OPEN_COUNT", 0),
                    row.get("ACCEPTED_COUNT", 0),
                    row.get("IMPLEMENTED_COUNT", 0),
                    row.get("REJECTED_COUNT", 0),
                    row.get("DEFERRED_COUNT", 0),
                ]
            }
            import pandas as pd
            df_funnel = pd.DataFrame(funnel_data)
            fig_funnel = px.bar(df_funnel, x="Stage", y="Count",
                                color="Stage",
                                color_discrete_map={
                                    "Open": "#3498DB", "Accepted": "#2ECC71",
                                    "Implemented": "#27AE60", "Rejected": "#E74C3C",
                                    "Deferred": "#F39C12"
                                },
                                title="Recommendation Status Distribution")
            fig_funnel.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig_funnel, use_container_width=True)

            # Implemented detail table
            df_impl = df_roi[df_roi["STATUS"] == "IMPLEMENTED"]
            if not df_impl.empty:
                st.subheader("Implemented Recommendations — ROI Detail")
                st.dataframe(
                    df_impl[["RECOMMENDATION_ID", "CATEGORY", "TARGET_OBJECT",
                             "ESTIMATED_MONTHLY_SAVINGS_USD", "ACTUAL_SAVINGS_USD",
                             "ROI_PCT", "DAYS_SINCE_IMPLEMENTATION", "NOTES"]],
                    use_container_width=True, hide_index=True,
                )
            else:
                st.info("No recommendations have been implemented yet. "
                        "Update `recommendation_actions.csv` and re-run `dbt seed` to track progress.")
        else:
            st.info("ROI data not available. Run the dbt pipeline to generate recommendation lifecycle data.")

except Exception as e:
    st.error(f"Error loading recommendations hub: {e}")
