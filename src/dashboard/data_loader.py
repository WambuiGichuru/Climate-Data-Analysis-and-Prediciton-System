"""
data_loader.py
Author    : R05 - Faith (DevOps & Deployment Engineer)
Purpose   : Cached data loading functions for the Streamlit dashboard.
            Uses st.cache_data with per-function TTLs to balance freshness
            and performance.
Milestone : M5 - Dashboard Data Layer
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

try:
    import streamlit as st
    _cache = st.cache_data
except ImportError:
    # Fallback: identity decorator when Streamlit is not available
    def _cache(**kwargs):
        def decorator(fn):
            return fn
        return decorator

from src.config import (
    DATA_DIR, LOG_DIR, KENYA_COUNTIES, OPENMETEO_FORECAST_URL,
)


@_cache(ttl=3600)
def load_historical_onset() -> pd.DataFrame:
    """Load historical onset dates. TTL: 1 hour."""
    path = DATA_DIR / "processed" / "historical_onset_dates.parquet"
    if path.exists():
        return pd.read_parquet(path)
    # Synthetic fallback
    from src.ml.feature_engineer import _synthetic_onset
    return _synthetic_onset()


@_cache(ttl=30)
def load_streaming_alerts() -> pd.DataFrame:
    """Load latest streaming alerts from parquet output. TTL: 30 seconds."""
    alerts_dir = LOG_DIR / "streaming_output" / "onset_alerts"
    if alerts_dir.exists():
        files = sorted(alerts_dir.glob("*.parquet"))
        if files:
            # Read last 5 parquet files and concat
            frames = [pd.read_parquet(f) for f in files[-5:]]
            df = pd.concat(frames, ignore_index=True)
            return df.tail(20)
    # Return empty fallback
    return pd.DataFrame(columns=["county", "timestamp", "alert_level",
                                  "onset_probability", "rolling_72hr_precip"])


@_cache(ttl=300)
def load_live_forecast(county: str) -> dict:
    """Fetch 7-day forecast from OpenMeteo for one county. TTL: 5 minutes."""
    coords = KENYA_COUNTIES.get(county, {"lat": -1.29, "lon": 36.82})
    params = {
        "latitude":    coords["lat"],
        "longitude":   coords["lon"],
        "daily":       "precipitation_sum,temperature_2m_max,temperature_2m_min",
        "forecast_days": 7,
        "timezone":    "Africa/Nairobi",
    }
    try:
        resp = requests.get(OPENMETEO_FORECAST_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        daily = data.get("daily", {})
        return {
            "dates":      daily.get("time", []),
            "precip":     daily.get("precipitation_sum", []),
            "tmax":       daily.get("temperature_2m_max", []),
            "tmin":       daily.get("temperature_2m_min", []),
            "county":     county,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception:
        return {"dates": [], "precip": [], "tmax": [], "tmin": [], "county": county}
