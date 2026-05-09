"""
app.py
Author    : R05 - Faith (DevOps & Deployment Engineer)
Purpose   : Streamlit dashboard for the Kenya Rainfall Onset Advisory System.
            Tabs: Overview | County Map | Live Forecast | Historical Trends.
Milestone : M5/M6 - Interactive Dashboard
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import streamlit as st

st.set_page_config(
    page_title="Kenya Rainfall Onset Advisory",
    page_icon="🌧",
    layout="wide",
)

from src.config import KENYA_COUNTIES, COUNTY_NAMES
from src.dashboard.data_loader import (
    load_historical_onset,
    load_streaming_alerts,
    load_live_forecast,
)
from src.ml.realtime_scorer import predict_onset

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("Kenya Rainfall Onset Advisory")
st.sidebar.markdown("SDS 2412 — Lambda Architecture Demo")

county_options = ["All Counties"] + COUNTY_NAMES
selected_county = st.sidebar.selectbox("Select County", county_options)

st.sidebar.markdown("---")
st.sidebar.caption("Auto-refreshes every 30 seconds on Overview tab.")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_overview, tab_map, tab_forecast, tab_history = st.tabs(
    ["Overview", "County Map", "Live Forecast", "Historical Trends"]
)

# ── Overview ─────────────────────────────────────────────────────────────────
with tab_overview:
    st.header("System Overview")

    alerts_df = load_streaming_alerts()
    n_alerts  = len(alerts_df)
    n_watch   = 0
    last_model_update = "N/A"

    # Compute live predictions for all counties
    predictions = {c: predict_onset(c) for c in COUNTY_NAMES}
    n_watch = sum(1 for p in predictions.values() if p["alert_level"] == "WATCH")

    col1, col2, col3 = st.columns(3)
    col1.metric("Active Onset Alerts",  n_alerts)
    col2.metric("Counties at WATCH",    n_watch)
    col3.metric("Last Model Update",    last_model_update)

    st.subheader("Latest Streaming Alerts")
    if alerts_df.empty:
        st.info("No streaming alerts yet. Start the Kafka pipeline to see live data.")
    else:
        st.dataframe(alerts_df, use_container_width=True)

    # Auto-rerun every 30s
    import time
    time.sleep(0)  # placeholder; use st.rerun() in Streamlit >= 1.27
    if st.button("Refresh Alerts"):
        st.rerun()

# ── County Map ───────────────────────────────────────────────────────────────
with tab_map:
    st.header("Kenya County Alert Map")
    try:
        import folium
        from streamlit_folium import st_folium

        m = folium.Map(location=[-0.5, 37.0], zoom_start=6)

        ALERT_COLORS = {
            "LOW":      "green",
            "MODERATE": "orange",
            "HIGH":     "red",
            "WATCH":    "darkred",
        }

        for county, coords in KENYA_COUNTIES.items():
            pred = predict_onset(county)
            color = ALERT_COLORS.get(pred["alert_level"], "blue")
            folium.CircleMarker(
                location=[coords["lat"], coords["lon"]],
                radius=12,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.7,
                popup=folium.Popup(
                    f"<b>{county}</b><br>"
                    f"Alert: {pred['alert_level']}<br>"
                    f"Onset probability: {pred['onset_probability']:.1%}<br>"
                    f"Onset DOY estimate: {pred['onset_doy_estimate']}",
                    max_width=200,
                ),
                tooltip=f"{county}: {pred['alert_level']}",
            ).add_to(m)

        st_folium(m, width=900, height=500)
    except ImportError:
        st.warning("Install folium and streamlit-folium for the map view.")
        # Fallback: table view
        import pandas as pd
        map_data = []
        for county, coords in KENYA_COUNTIES.items():
            pred = predict_onset(county)
            map_data.append({
                "County": county,
                "Latitude": coords["lat"],
                "Longitude": coords["lon"],
                "Alert Level": pred["alert_level"],
                "Onset Probability": f"{pred['onset_probability']:.1%}",
            })
        st.dataframe(pd.DataFrame(map_data), use_container_width=True)

# ── Live Forecast ─────────────────────────────────────────────────────────────
with tab_forecast:
    county = selected_county if selected_county != "All Counties" else COUNTY_NAMES[0]
    st.header(f"Live Forecast: {county}")

    forecast = load_live_forecast(county)
    pred     = predict_onset(county)

    col1, col2 = st.columns([2, 1])
    with col1:
        try:
            import plotly.graph_objects as go
            if forecast["dates"]:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=forecast["dates"],
                    y=forecast["precip"],
                    name="Precipitation (mm)",
                    marker_color="steelblue",
                ))
                fig.update_layout(
                    title=f"7-Day Precipitation Forecast — {county}",
                    xaxis_title="Date",
                    yaxis_title="Precipitation (mm)",
                    height=350,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No forecast data available.")
        except ImportError:
            st.warning("Install plotly for interactive charts.")
            if forecast["dates"]:
                import pandas as pd
                df = pd.DataFrame({
                    "Date": forecast["dates"],
                    "Precip (mm)": forecast["precip"],
                })
                st.bar_chart(df.set_index("Date"))

    with col2:
        st.metric(
            label="Onset Probability",
            value=f"{pred['onset_probability']:.1%}",
        )
        alert_color = {
            "LOW": "green", "MODERATE": "orange",
            "HIGH": "red", "WATCH": "darkred",
        }.get(pred["alert_level"], "grey")
        st.markdown(
            f"**Alert Level:** "
            f"<span style='color:{alert_color};font-size:1.3em'>"
            f"**{pred['alert_level']}**</span>",
            unsafe_allow_html=True,
        )
        st.caption(f"Onset DOY estimate: {pred['onset_doy_estimate']}")

# ── Historical Trends ────────────────────────────────────────────────────────
with tab_history:
    st.header("Historical Onset Trends")
    onset_df = load_historical_onset()

    county_filter = selected_county if selected_county != "All Counties" else None
    county_col    = "station_id" if "station_id" in onset_df.columns else "county"

    if county_filter and county_col in onset_df.columns:
        plot_df = onset_df[onset_df[county_col] == county_filter]
    else:
        plot_df = onset_df

    if "onset_doy" not in plot_df.columns and "onset_date" in plot_df.columns:
        import pandas as pd
        plot_df = plot_df.copy()
        plot_df["onset_doy"] = pd.to_datetime(plot_df["onset_date"]).dt.day_of_year

    try:
        import plotly.express as px
        if not plot_df.empty and "onset_doy" in plot_df.columns:
            fig = px.line(
                plot_df.sort_values("year"),
                x="year", y="onset_doy",
                color=county_col if county_filter is None else None,
                title="Rainfall Onset Day-of-Year Trend",
                labels={"onset_doy": "Onset Day of Year", "year": "Year"},
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

            # Year-on-year anomaly
            mean_doy = plot_df["onset_doy"].mean()
            plot_df = plot_df.copy()
            plot_df["anomaly"] = plot_df["onset_doy"] - mean_doy
            fig2 = px.bar(
                plot_df.sort_values("year"),
                x="year", y="anomaly",
                color=county_col if county_filter is None else None,
                title="Year-on-Year Onset Anomaly (days vs climatological mean)",
                height=300,
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No historical onset data available.")
    except ImportError:
        st.warning("Install plotly for interactive charts.")
        if not plot_df.empty:
            st.dataframe(plot_df.head(50), use_container_width=True)
