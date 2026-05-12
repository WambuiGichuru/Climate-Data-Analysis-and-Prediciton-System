"""
feature_engineer.py
Purpose : Feature-engineering helpers for the dashboard.  Builds the
          six-element feature vector the XGBoost model trained in
          analysis/kenya_xgboost_model.py expects, and produces a
          synthetic onset history when no real parquet is present.
Milestone: M4 / M5 - ML serving glue
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.config import COUNTY_NAMES, KENYA_COUNTIES

logger = logging.getLogger(__name__)

# Feature order MUST match analysis/kenya_xgboost_model.py:
#   ['y', 'surface_pressure', 'temp_k', 'temp_lag_6h', 'temp_lag_24h', 'county_cat']
FEATURE_NAMES: list[str] = [
    "y",
    "surface_pressure",
    "temp_k",
    "temp_lag_6h",
    "temp_lag_24h",
    "county_cat",
]

# Categorical code MUST match df['county'].astype('category').cat.codes
# from the training script - that uses alphabetical order.
_COUNTY_CAT: dict[str, int] = {
    name: idx for idx, name in enumerate(sorted(KENYA_COUNTIES.keys()))
}


def county_cat(county: str) -> int:
    """Return the integer category code XGBoost was trained on for a county.

    Why: the training script relied on pandas categorical.cat.codes which
    is alphabetical. Hard-coding the same ordering here keeps inference
    consistent without dragging the training dataframe along.
    """
    return _COUNTY_CAT.get(county, 0)


def build_feature_vector(
    county: str,
    recent_precip_mm: float,
    surface_pressure_hpa: float = 1013.0,
    temp_c: float | None = None,
    temp_lag_6h_c: float | None = None,
    temp_lag_24h_c: float | None = None,
) -> list[float]:
    """Assemble a single feature row for the XGBoost regressor.

    Temperatures are accepted in Celsius for convenience and converted
    to Kelvin (the unit used during training). Missing temperature
    values fall back to ~22C (295.15 K), which is the climatological
    annual mean across the Kenyan counties in the training set - a
    neutral prior rather than zero.
    """
    def _to_k(v: float | None, default_k: float = 295.15) -> float:
        if v is None:
            return default_k
        try:
            return float(v) + 273.15
        except (TypeError, ValueError):
            return default_k

    return [
        float(recent_precip_mm or 0.0),
        float(surface_pressure_hpa),
        _to_k(temp_c),
        _to_k(temp_lag_6h_c),
        _to_k(temp_lag_24h_c),
        float(county_cat(county)),
    ]


def _synthetic_onset(years: int = 20, seed: int = 7) -> pd.DataFrame:
    """Generate a deterministic synthetic onset history for offline mode.

    Why: the dashboard's Historical Trends tab must render something
    sensible even before the BigQuery batch table is populated. This
    keeps the chart axes well-defined during early integration tests
    and Codespaces demos that run without GCP credentials.

    Returns a long-format frame with columns: county, year, onset_doy.
    Day-of-year is drawn from a county-specific Gaussian centred on
    the published long rains onset window (DOY 75-120 for most of
    Kenya, slightly later for arid northern counties).
    """
    rng = np.random.default_rng(seed)
    current_year = datetime.now().year
    start_year = current_year - years

    # Rough climatological mean onset DOY per county (long rains).
    mean_doy: dict[str, int] = {
        "Nairobi":  85,
        "Kisumu":   75,
        "Nakuru":   90,
        "Meru":     80,
        "Kitui":    95,
        "Garissa":  110,
        "Machakos": 100,
        "Eldoret":  82,
        "Kakamega": 78,
        "Embu":     88,
    }

    rows: list[dict] = []
    for county in COUNTY_NAMES:
        mu = mean_doy.get(county, 90)
        for year in range(start_year, current_year + 1):
            doy = int(np.clip(rng.normal(mu, 12), 30, 200))
            rows.append({"county": county, "year": year, "onset_doy": doy})
    return pd.DataFrame(rows)
