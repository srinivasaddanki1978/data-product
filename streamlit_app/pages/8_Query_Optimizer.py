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
            height=500,
        )
        # ── AI Query Analysis (Cortex) ─────────────────────────────
        st.subheader("AI Query Analysis (Snowflake Cortex)")
        st.caption("Select a query from the list above and get AI-powered optimization suggestions.")

        query_options = {
            f"#{int(r['OPTIMIZATION_RANK'])} — {r['ANTIPATTERN_TYPE']} | {r['USER_NAME']} | {format_currency(r['ESTIMATED_WASTE_USD'])}": idx
            for idx, r in df_filtered.reset_index(drop=True).iterrows()
        }

        if query_options:
            selected_label = st.selectbox("Select a query to analyze", list(query_options.keys()))
            selected_idx = query_options[selected_label]
            selected_row = df_filtered.reset_index(drop=True).iloc[selected_idx]

            with st.expander("View selected query SQL", expanded=False):
                st.code(selected_row["SAMPLE_QUERY_TEXT"], language="sql")

            if st.button("Analyze with Cortex AI"):
                with st.spinner("Analyzing query with Snowflake Cortex..."):
                    query_text = selected_row["SAMPLE_QUERY_TEXT"].replace("'", "''")
                    antipattern = selected_row["ANTIPATTERN_TYPE"]
                    recommendation = selected_row["RECOMMENDATION"].replace("'", "''")

                    cortex_prompt = f"""You are a Snowflake SQL performance expert. Analyze this query that has been flagged with the anti-pattern: {antipattern}.

Current recommendation: {recommendation}

SQL Query:
{query_text}

Provide a specific, actionable optimization plan:
1. What exactly is wrong with this query (be specific to this SQL, not generic)
2. The optimized version of this query (rewritten SQL)
3. What Snowflake features to leverage (clustering, materialized views, result caching, etc.)
4. Expected performance improvement

Keep the response concise and practical."""

                    cortex_prompt_escaped = cortex_prompt.replace("'", "''")

                    cortex_sql = f"""
                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                        'mistral-large2',
                        '{cortex_prompt_escaped}'
                    ) AS ai_suggestion
                    """

                    try:
                        df_ai = run_query(cortex_sql)
                        if not df_ai.empty:
                            ai_response = df_ai.iloc[0]["AI_SUGGESTION"]
                            st.markdown("**AI Optimization Suggestion:**")
                            st.markdown(ai_response)
                        else:
                            st.warning("No response from Cortex. Try again.")
                    except Exception as cortex_err:
                        st.error(f"Cortex analysis failed: {cortex_err}")

    else:
        st.info("No anti-patterns detected. Queries are running efficiently.")

except Exception as e:
    st.error(f"Error loading query optimizer: {e}")
