"""
realtime_scorer.py
Author    : R04 - Eric (EDA & ML Engineer)
Purpose   : Loads trained XGBoost models and exposes predict_onset() for
            real-time inference by the dashboard and API.
            Falls back to mock predictions if models are not yet trained.
Milestone : M4 - Real-time Scoring
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from loguru import logger

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.config import MODEL_DIR, setup_logging

_CLASSIFIER = None
_REGRESSOR  = None


def _load_models() -> tuple[Any, Any]:
    """Load (or reload) persisted models. Returns (classifier, regressor)."""
    global _CLASSIFIER, _REGRESSOR
    if _CLASSIFIER is not None and _REGRESSOR is not None:
        return _CLASSIFIER, _REGRESSOR

    clf_path = MODEL_DIR / "onset_classifier.pkl"
    reg_path = MODEL_DIR / "onset_regressor.pkl"

    if clf_path.exists() and reg_path.exists():
        try:
            import joblib
            _CLASSIFIER = joblib.load(clf_path)
            _REGRESSOR  = joblib.load(reg_path)
            logger.info("Models loaded from %s", MODEL_DIR)
        except Exception as exc:
            logger.warning("Could not load models: %s — using mock predictions.", exc)
    else:
        logger.warning("Model files not found in %s — using mock predictions.", MODEL_DIR)

    return _CLASSIFIER, _REGRESSOR


def _alert_level(probability: float) -> str:
    """Map onset probability to a categorical alert level."""
    if probability < 0.30:
        return "LOW"
    if probability < 0.60:
        return "MODERATE"
    if probability < 0.85:
        return "HIGH"
    return "WATCH"


def _mock_prediction(county: str) -> dict:
    """Deterministic mock prediction keyed by county name hash."""
    import hashlib
    h = int(hashlib.md5(county.encode()).hexdigest(), 16)
    prob = (h % 100) / 100.0
    doy  = 80 + (h % 60)
    return {
        "county":             county,
        "onset_probability":  round(prob, 4),
        "onset_doy_estimate": doy,
        "alert_level":        _alert_level(prob),
        "source":             "mock",
    }


def predict_onset(county: str, forecast_data: dict | None = None) -> dict:
    """
    Predict rainfall onset for a given county.

    Parameters
    ----------
    county        : County name (must be in KENYA_COUNTIES).
    forecast_data : Optional dict with keys matching FEATURE_COLS:
                    mean_precip_30d, precip_anomaly, soil_moisture_proxy,
                    temp_anomaly_tmax.
                    If None, uses zeros (neutral feature vector).

    Returns
    -------
    dict with keys: county, onset_probability, onset_doy_estimate, alert_level, source.
    """
    clf, reg = _load_models()

    if clf is None or reg is None:
        return _mock_prediction(county)

    import numpy as np
    feature_keys = ["mean_precip_30d", "precip_anomaly", "soil_moisture_proxy", "temp_anomaly_tmax"]
    data = forecast_data or {}
    features = np.array([[data.get(k, 0.0) for k in feature_keys]])

    try:
        probability = float(clf.predict_proba(features)[0, 1])
        doy_estimate = int(reg.predict(features)[0])
    except Exception as exc:
        logger.warning("Prediction failed for %s: %s — returning mock.", county, exc)
        return _mock_prediction(county)

    return {
        "county":             county,
        "onset_probability":  round(probability, 4),
        "onset_doy_estimate": doy_estimate,
        "alert_level":        _alert_level(probability),
        "source":             "model",
    }


if __name__ == "__main__":
    setup_logging()
    from src.config import COUNTY_NAMES
    for c in COUNTY_NAMES[:3]:
        result = predict_onset(c)
        print(f"  {c}: {result}")
