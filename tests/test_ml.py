"""
test_ml.py
Author    : R04 - Eric (EDA & ML Engineer)
Purpose   : Unit tests for feature_engineer and realtime_scorer.
            All tests pass without real data files or trained models.
Milestone : M4 - ML tests
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np
import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.ml.feature_engineer import (
    engineer_features,
    _synthetic_monthly,
    _synthetic_onset,
)
from src.ml.realtime_scorer import predict_onset, _alert_level, _mock_prediction


# ---------------------------------------------------------------------------
# feature_engineer tests
# ---------------------------------------------------------------------------

@pytest.fixture
def synthetic_data():
    monthly = _synthetic_monthly()
    onset   = _synthetic_onset()
    return monthly, onset


def test_synthetic_monthly_has_required_columns(synthetic_data):
    monthly, _ = synthetic_data
    required = {"station_id", "year", "month", "monthly_total_precip_mm"}
    assert required.issubset(set(monthly.columns))


def test_synthetic_monthly_row_count(synthetic_data):
    monthly, _ = synthetic_data
    # 10 counties x 24 years x 12 months = 2880 rows
    assert len(monthly) == 10 * 24 * 12


def test_engineer_features_output_columns(synthetic_data):
    monthly, onset = synthetic_data
    features = engineer_features(monthly, onset)
    required = {
        "county", "year", "season",
        "mean_precip_30d", "precip_anomaly", "soil_moisture_proxy",
        "temp_anomaly_tmax", "onset_doy", "onset_occurred",
    }
    assert required.issubset(set(features.columns))


def test_engineer_features_onset_occurred_is_bool(synthetic_data):
    monthly, onset = synthetic_data
    features = engineer_features(monthly, onset)
    assert features["onset_occurred"].dtype == bool or \
           features["onset_occurred"].dtype == np.dtype("bool")


def test_engineer_features_not_empty(synthetic_data):
    monthly, onset = synthetic_data
    features = engineer_features(monthly, onset)
    assert len(features) > 0


def test_engineer_features_seasons_only_mam_ond(synthetic_data):
    monthly, onset = synthetic_data
    features = engineer_features(monthly, onset)
    assert set(features["season"].unique()).issubset({"MAM", "OND"})


# ---------------------------------------------------------------------------
# realtime_scorer tests
# ---------------------------------------------------------------------------

def test_alert_level_low():
    assert _alert_level(0.1) == "LOW"


def test_alert_level_moderate():
    assert _alert_level(0.45) == "MODERATE"


def test_alert_level_high():
    assert _alert_level(0.75) == "HIGH"


def test_alert_level_watch():
    assert _alert_level(0.90) == "WATCH"


def test_mock_prediction_has_required_keys():
    result = _mock_prediction("Nairobi")
    required = {"county", "onset_probability", "onset_doy_estimate", "alert_level", "source"}
    assert required.issubset(set(result.keys()))


def test_mock_prediction_probability_in_range():
    for county in ["Nairobi", "Kisumu", "Garissa", "Embu"]:
        result = _mock_prediction(county)
        assert 0.0 <= result["onset_probability"] <= 1.0


def test_predict_onset_returns_dict_without_models():
    # No models on disk in test env - should return mock
    result = predict_onset("Nairobi")
    assert isinstance(result, dict)
    assert "onset_probability" in result
    assert "alert_level" in result


def test_predict_onset_all_counties():
    from src.config import COUNTY_NAMES
    for county in COUNTY_NAMES:
        result = predict_onset(county)
        assert result["county"] == county
        assert result["alert_level"] in ("LOW", "MODERATE", "HIGH", "WATCH")
