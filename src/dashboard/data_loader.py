"""
data_loader.py
Author    : R05 - Faith (DevOps & Deployment Engineer)
Purpose   : Cached data loading functions for the Streamlit dashboard.
            Uses st.cache_data with per-function TTLs to balance freshness
            and performance.
Milestone : M5 - Dashboard Data Layer
"""
from __future__ import annotations

import logging
import os
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Deployed-API integration
# ---------------------------------------------------------------------------
# Setting API_URL to the Cloud Run service URL switches the dashboard
# from "local + OpenMeteo only" to "live GCP data". The API in turn
# pulls from BigQuery (batch), Firestore (speed) and Vertex AI (ML),
# so this single env var unlocks the whole GCP-backed view.
API_URL       = os.environ.get("API_URL", "").rstrip("/")
API_TIMEOUT_S = float(os.environ.get("API_TIMEOUT_S", "5"))


def api_enabled() -> bool:
    """True when an API base URL is configured."""
    return bool(API_URL)


@_cache(ttl=30)
def api_health() -> dict:
    """Probe the deployed API's /health endpoint.

    Returns {"reachable": bool, "status": str, "url": str, "error": str}.
    Used by the Streamlit sidebar to render a connection-status pill so
    the user can immediately see whether GCP-backed data is live.
    """
    if not API_URL:
        return {"reachable": False, "status": "disabled",
                "url": "", "error": "API_URL env var not set"}
    try:
        resp = requests.get(f"{API_URL}/health", timeout=API_TIMEOUT_S)
        resp.raise_for_status()
        body = resp.json()
        return {
            "reachable": True,
            "status":    body.get("status", "unknown"),
            "url":       API_URL,
            "version":   body.get("version", ""),
            "vertex":    bool(body.get("vertex_endpoint", False)),
            "error":     "",
        }
    except Exception as exc:
        return {"reachable": False, "status": "error",
                "url": API_URL, "error": str(exc)}


@_cache(ttl=60)
def load_risk_map_from_api() -> dict:
    """Fetch /api/v1/risk-map from the deployed API.

    Returns the raw GeoJSON-style payload, or {} on failure. Callers
    should treat empty as "fall back to local sources".
    """
    if not API_URL:
        return {}
    try:
        resp = requests.get(f"{API_URL}/api/v1/risk-map", timeout=API_TIMEOUT_S)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.debug("risk-map API call failed: %s", exc)
        return {}


@_cache(ttl=300)
def load_historical_trend_from_api(county: str | None, days: int = 180) -> pd.DataFrame:
    """Fetch BigQuery-backed onset history via /api/v1/historical-trend.

    Returns an empty DataFrame on failure so the caller can fall back
    to the local parquet / synthetic dataset.
    """
    if not API_URL:
        return pd.DataFrame()
    params: dict[str, str | int] = {"days": days}
    if county:
        params["county"] = county
    try:
        resp = requests.get(
            f"{API_URL}/api/v1/historical-trend",
            params=params,
            timeout=API_TIMEOUT_S,
        )
        resp.raise_for_status()
        series = resp.json().get("series", [])
    except Exception as exc:
        logger.debug("historical-trend API call failed: %s", exc)
        return pd.DataFrame()
    if not series:
        return pd.DataFrame()
    df = pd.DataFrame(series)
    df["day"] = pd.to_datetime(df["day"])
    return df


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
    """Load the latest onset alerts. TTL: 30 seconds.

    Source preference:
      1. Deployed API risk-map (Firestore-backed - real GCP data).
      2. Local parquet from src/streaming/spark_consumer.py output.
      3. Empty frame (so the UI can still render).
    """
    payload = load_risk_map_from_api()
    if payload:
        rows = []
        for feat in payload.get("features", []):
            props = feat.get("properties", {})
            cum   = float(props.get("cum_72hr_mm", 0.0) or 0.0)
            risk  = props.get("onset_risk", "LOW")
            if cum > 0 or risk in {"MODERATE", "HIGH"}:
                rows.append({
                    "county":              props.get("county"),
                    "timestamp":           props.get("alert_timestamp")
                                            or payload.get("metadata", {})
                                                       .get("last_updated_utc"),
                    "alert_level":         risk,
                    "onset_probability":   props.get("onset_risk_score", 0.0),
                    "rolling_72hr_precip": cum,
                    "ml_probability":      props.get("ml_probability"),
                })
        if rows:
            return pd.DataFrame(rows).tail(20)

    alerts_dir = LOG_DIR / "streaming_output" / "onset_alerts"
    if alerts_dir.exists():
        files = sorted(alerts_dir.glob("*.parquet"))
        if files:
            # Read last 5 parquet files and concat
            frames = [pd.read_parquet(f) for f in files[-5:]]
            df = pd.concat(frames, ignore_index=True)
            return df.tail(20)
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
