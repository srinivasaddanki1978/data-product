"""Cost Forecast — Linear trend projections with confidence intervals."""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from utils.connection import run_query
from utils.formatters import format_currency
from utils.queries import PUB, INT

st.set_page_config(page_title="Cost Forecast", layout="wide")
st.title("Cost Forecast")

try:
    # ── Publication forecast (monthly actuals + forecast) ────────────
    df_forecast = run_query(f"SELECT * FROM {PUB}.PUB__COST_FORECAST ORDER BY MONTH")

    if not df_forecast.empty:
        actuals = df_forecast[df_forecast["DATA_TYPE"] == "ACTUAL"]
        forecasts = df_forecast[df_forecast["DATA_TYPE"] == "FORECAST"]

        # ── KPI Row ─────────────────────────────────────────────────
        next_month_cost = forecasts["TOTAL_COST"].iloc[0] if not forecasts.empty else 0
        next_quarter_cost = forecasts["TOTAL_COST"].sum() if not forecasts.empty else 0
        annual_spend = df_forecast["PROJECTED_ANNUAL_SPEND"].iloc[0] if "PROJECTED_ANNUAL_SPEND" in df_forecast.columns else 0
        daily_trend = run_query(f"""
            SELECT daily_trend FROM {INT}.INT__COST_FORECAST LIMIT 1
        """)
        trend_val = daily_trend.iloc[0]["DAILY_TREND"] if not daily_trend.empty else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Next Month Projected", format_currency(next_month_cost))
        c2.metric("Next Quarter Projected", format_currency(next_quarter_cost))
        c3.metric("Projected Annual Spend", format_currency(annual_spend))
        c4.metric("Daily Trend", format_currency(trend_val) + "/day",
                   delta=f"{'Growing' if trend_val > 0 else 'Declining'}")

        # ── Actuals + Forecast Line Chart ───────────────────────────
        st.subheader("Cost Projection")
        fig = go.Figure()

        # Actuals
        if not actuals.empty:
            fig.add_trace(go.Scatter(
                x=actuals["MONTH"], y=actuals["TOTAL_COST"],
                mode="lines+markers", name="Actuals",
                line=dict(color="#636EFA", width=3),
            ))

        # Forecast
        if not forecasts.empty:
            fig.add_trace(go.Scatter(
                x=forecasts["MONTH"], y=forecasts["TOTAL_COST"],
                mode="lines+markers", name="Forecast",
                line=dict(color="#EF553B", width=3, dash="dash"),
            ))

            # Confidence band
            if "CI_UPPER" in forecasts.columns and "CI_LOWER" in forecasts.columns:
                fig.add_trace(go.Scatter(
                    x=forecasts["MONTH"], y=forecasts["CI_UPPER"],
                    mode="lines", name="95% CI Upper",
                    line=dict(width=0), showlegend=False,
                ))
                fig.add_trace(go.Scatter(
                    x=forecasts["MONTH"], y=forecasts["CI_LOWER"],
                    mode="lines", name="95% CI Lower",
                    line=dict(width=0), showlegend=False,
                    fill="tonexty", fillcolor="rgba(239,85,59,0.15)",
                ))

        fig.update_layout(
            title="Monthly Cost: Actuals vs Forecast",
            xaxis_title="Month", yaxis_title="Cost (USD)",
            height=450, hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Forecast by Cost Type (Stacked Bar) ────────────────────
        if not forecasts.empty:
            st.subheader("Forecast by Cost Category")
            fig_bar = go.Figure()
            for col, label, color in [
                ("COMPUTE_COST", "Compute", "#636EFA"),
                ("STORAGE_COST", "Storage", "#EF553B"),
                ("SERVERLESS_COST", "Serverless", "#00CC96"),
            ]:
                if col in forecasts.columns:
                    fig_bar.add_trace(go.Bar(
                        x=forecasts["MONTH"], y=forecasts[col],
                        name=label, marker_color=color,
                    ))
            fig_bar.update_layout(barmode="stack", height=350,
                                   xaxis_title="Month", yaxis_title="Cost (USD)")
            st.plotly_chart(fig_bar, use_container_width=True)

        # ── Team Projections ────────────────────────────────────────
        st.subheader("Team Cost Projections")
        df_team = run_query(f"""
            SELECT TEAM_NAME, FORECAST_MONTH, PREDICTED_MONTHLY_COST,
                   CI_LOWER, CI_UPPER, MONTHLY_TREND, DATA_MONTHS
            FROM {INT}.INT__TEAM_COST_FORECAST
            ORDER BY TEAM_NAME, FORECAST_MONTH
        """)
        if not df_team.empty:
            teams = df_team["TEAM_NAME"].unique().tolist()
            selected_teams = st.multiselect("Filter Teams", teams, default=teams)
            df_team_filtered = df_team[df_team["TEAM_NAME"].isin(selected_teams)]
            st.dataframe(df_team_filtered, use_container_width=True)
        else:
            st.info("Team forecast requires at least 2 months of data per team.")

        # ── CSV Export ──────────────────────────────────────────────
        csv = df_forecast.to_csv(index=False)
        st.download_button(
            label="Export Forecast to CSV",
            data=csv,
            file_name="cost_forecast.csv",
            mime="text/csv",
        )

    else:
        st.info("No forecast data available. Run the dbt pipeline to generate cost projections.")

except Exception as e:
    st.error(f"Error loading cost forecast: {e}")
