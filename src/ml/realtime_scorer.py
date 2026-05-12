"""
realtime_scorer.py
Purpose : Dashboard-facing onset-probability scorer.

          Tries three signal sources in order of preference:
            1. Deployed FastAPI (API_URL env var) - speed + ML blended
            2. Local XGBoost joblib + live OpenMeteo - degraded but real
            3. Pure rule-based on OpenMeteo precip                    - offline

          Returns a uniform dict so the Streamlit UI never has to
          branch on which source produced the score.

Milestone: M4 / M5 - ML serving glue
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.config import (
    KENYA_COUNTIES,
    ONSET_THRESHOLD_MM,
    OPENMETEO_FORECAST_URL,
)
from src.ml.feature_engineer import build_feature_vector

logger = logging.getLogger(__name__)

# Path to the XGBoost artifact produced by analysis/kenya_xgboost_model.py.
MODEL_PATH = _REPO / "xgb_outputs" / "models" / "kenya_xgboost_v1.joblib"

# Env-controlled API base URL. When set, the dashboard prefers the
# deployed FastAPI (which already does Firestore + Vertex blending).
_API_URL = os.environ.get("API_URL", "").rstrip("/")
_API_TIMEOUT_S = float(os.environ.get("API_TIMEOUT_S", "5"))


# ---------------------------------------------------------------------------
# Source 1 - deployed FastAPI
# ---------------------------------------------------------------------------
def _score_via_api(county: str) -> dict | None:
    """Hit /api/v1/county/{county} on the deployed API.

    Returns None on any failure so callers fall back to local scoring.
    A short timeout is used because Streamlit needs sub-second
    responsiveness for the County Map render.
    """
    if not _API_URL:
        return None
    try:
        resp = requests.get(
            f"{_API_URL}/api/v1/county/{county}", timeout=_API_TIMEOUT_S,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.debug("API county lookup failed for %s: %s", county, exc)
        return None

    prob = data.get("ml_probability")
    if prob is None:
        prob = data.get("onset_risk_score", 0.0)
    alert = data.get("onset_risk", "LOW")
    if alert == "HIGH" and bool(data.get("onset_flag", False)):
        alert = "WATCH"
    return {
        "county":             county,
        "onset_probability":  float(prob or 0.0),
        "alert_level":        alert,
        "onset_doy_estimate": _doy_estimate(),
        "source":             "api",
        "cum_72hr_mm":        float(data.get("cum_72hr_mm", 0.0) or 0.0),
        "as_of_utc":          datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Source 2 - local XGBoost + OpenMeteo
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _load_local_model() -> Any | None:
    """Load the XGBoost joblib once per process. Returns None if absent.

    Why: a Codespaces clone may not have the model yet (it lives in
    xgb_outputs/ which is committed but could be missing on a fresh
    branch). Treating that as a soft failure lets the dashboard still
    boot.
    """
    if not MODEL_PATH.exists():
        logger.info("Local model not found at %s", MODEL_PATH)
        return None
    try:
        import joblib
        model = joblib.load(MODEL_PATH)
        logger.info("Loaded local XGBoost model from %s", MODEL_PATH)
        return model
    except Exception as exc:
        logger.warning("Failed to load local model: %s", exc)
        return None


def _fetch_openmeteo_snapshot(county: str) -> dict[str, float]:
    """Pull the most recent precip and temperature for one county.

    Returns a small dict with cum_72hr_mm, temp_c, temp_lag_6h_c,
    temp_lag_24h_c. Used both for direct rule-based scoring and as
    inputs for the local model.
    """
    coords = KENYA_COUNTIES.get(county)
    if not coords:
        return {}
    params = {
        "latitude":    coords["lat"],
        "longitude":   coords["lon"],
        "hourly":      "precipitation,temperature_2m,surface_pressure",
        "past_days":   3,
        "forecast_days": 1,
        "timezone":    "Africa/Nairobi",
    }
    try:
        resp = requests.get(OPENMETEO_FORECAST_URL, params=params, timeout=8)
        resp.raise_for_status()
        hourly = resp.json().get("hourly", {})
    except Exception as exc:
        logger.debug("OpenMeteo fetch failed for %s: %s", county, exc)
        return {}

    precip   = hourly.get("precipitation", []) or []
    temps    = hourly.get("temperature_2m", []) or []
    pressure = hourly.get("surface_pressure", []) or []

    # Last 72 hours of precipitation (data is hourly).
    cum_72hr = float(sum(p or 0.0 for p in precip[-72:]))

    def _last(lst: list[float], offset: int) -> float | None:
        if len(lst) > offset:
            return lst[-1 - offset]
        return None

    return {
        "cum_72hr_mm":    cum_72hr,
        "temp_c":         _last(temps, 0),
        "temp_lag_6h_c":  _last(temps, 6),
        "temp_lag_24h_c": _last(temps, 24),
        "pressure_hpa":   _last(pressure, 0) or 1013.0,
    }


def _score_via_local_model(county: str) -> dict | None:
    """Score using the on-disk XGBoost model + OpenMeteo snapshot.

    The training script defined target as 'y' (recent precip), so the
    raw model output is a precipitation forecast in the same units as
    the input. We convert it to a 0-1 onset probability by comparing
    against ONSET_THRESHOLD_MM via a soft sigmoid, then blend with the
    speed-layer rule. Returns None if either the model or the network
    snapshot is unavailable.
    """
    model = _load_local_model()
    snap  = _fetch_openmeteo_snapshot(county)
    if model is None or not snap:
        return None

    features = build_feature_vector(
        county,
        recent_precip_mm=snap["cum_72hr_mm"] / 72.0,
        surface_pressure_hpa=snap.get("pressure_hpa", 1013.0),
        temp_c=snap.get("temp_c"),
        temp_lag_6h_c=snap.get("temp_lag_6h_c"),
        temp_lag_24h_c=snap.get("temp_lag_24h_c"),
    )
    try:
        import numpy as np
        raw = float(model.predict(np.array([features]))[0])
    except Exception as exc:
        logger.warning("Local model predict failed for %s: %s", county, exc)
        return None

    # Soft probability from regression output: how far the predicted
    # precip is past half the onset threshold.
    margin = (raw * 72.0 - 0.5 * ONSET_THRESHOLD_MM) / ONSET_THRESHOLD_MM
    prob_model = 1.0 / (1.0 + pow(2.71828, -2.0 * margin))

    rule = 1.0 if snap["cum_72hr_mm"] >= ONSET_THRESHOLD_MM else 0.0
    blended = 0.33 * rule + 0.67 * prob_model
    blended = max(0.0, min(1.0, blended))

    return {
        "county":             county,
        "onset_probability":  blended,
        "alert_level":        _alert_level(blended, rule_triggered=bool(rule)),
        "onset_doy_estimate": _doy_estimate(),
        "source":             "local_model",
        "cum_72hr_mm":        snap["cum_72hr_mm"],
        "as_of_utc":          datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Source 3 - pure rule on OpenMeteo (works offline against cached data)
# ---------------------------------------------------------------------------
def _score_via_rule(county: str) -> dict:
    """Final fallback: cum-72hr rule on whatever OpenMeteo returns.

    Always returns a result so the dashboard can render a complete
    map even when the API and the local model are both unavailable.
    """
    snap = _fetch_openmeteo_snapshot(county)
    cum = snap.get("cum_72hr_mm", 0.0)
    rule_hit = cum >= ONSET_THRESHOLD_MM
    prob = 1.0 if rule_hit else min(0.4, cum / (ONSET_THRESHOLD_MM * 2))
    return {
        "county":             county,
        "onset_probability":  prob,
        "alert_level":        _alert_level(prob, rule_triggered=rule_hit),
        "onset_doy_estimate": _doy_estimate(),
        "source":             "rule",
        "cum_72hr_mm":        cum,
        "as_of_utc":          datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _alert_level(prob: float, rule_triggered: bool) -> str:
    """Map a blended probability to the four-tier dashboard scale.

    WATCH is reserved for cases where the speed-layer rule has already
    fired - it conveys 'onset detected now' rather than 'onset likely',
    which is the distinction the agricultural officers in the M6
    stakeholder review asked for.
    """
    if rule_triggered:
        return "WATCH"
    if prob > 0.7:
        return "HIGH"
    if prob > 0.4:
        return "MODERATE"
    return "LOW"


def _doy_estimate() -> int:
    """Today's day-of-year - the dashboard shows this next to the score."""
    return datetime.now(timezone.utc).timetuple().tm_yday


def predict_onset(county: str) -> dict:
    """Return the best available onset score for a single county.

    Tries API -> local model -> rule fallback. Always returns a dict
    with the same shape so the Streamlit UI doesn't need to branch.
    """
    return (
        _score_via_api(county)
        or _score_via_local_model(county)
        or _score_via_rule(county)
    )
