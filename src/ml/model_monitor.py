"""
model_monitor.py
Author    : R04 - Eric (EDA & ML Engineer)
Purpose   : Population Stability Index (PSI) drift detection.
            Compares current feature distributions to the training baseline.
            PSI > 0.2 logs a CRITICAL warning.
            Designed to run as a weekly Airflow task.
Milestone : M4 - Model Monitoring (M5 operational requirement)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.config import DATA_DIR, MODEL_DIR, setup_logging

FEATURES_PARQUET = DATA_DIR / "features" / "onset_features.parquet"
FEATURE_COLS = [
    "mean_precip_30d",
    "precip_anomaly",
    "soil_moisture_proxy",
    "temp_anomaly_tmax",
]

PSI_CRITICAL_THRESHOLD = 0.2
PSI_WARNING_THRESHOLD  = 0.1
N_BINS = 10


def _psi(expected: np.ndarray, actual: np.ndarray, n_bins: int = N_BINS) -> float:
    """
    Compute Population Stability Index between two distributions.

    PSI = sum((actual_pct - expected_pct) * ln(actual_pct / expected_pct))

    Interpretation:
      PSI < 0.1  : no significant drift
      0.1 - 0.2  : moderate drift, monitor closely
      PSI > 0.2  : significant drift, retrain recommended
    """
    bins = np.percentile(expected, np.linspace(0, 100, n_bins + 1))
    bins = np.unique(bins)
    if len(bins) < 2:
        return 0.0

    exp_counts, _ = np.histogram(expected, bins=bins)
    act_counts, _ = np.histogram(actual,   bins=bins)

    exp_pct = exp_counts / max(exp_counts.sum(), 1)
    act_pct = act_counts / max(act_counts.sum(), 1)

    # Replace zeros to avoid log(0)
    exp_pct = np.where(exp_pct == 0, 1e-6, exp_pct)
    act_pct = np.where(act_pct == 0, 1e-6, act_pct)

    psi = float(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)))
    return psi


def run_drift_check(
    baseline_df: pd.DataFrame | None = None,
    current_df:  pd.DataFrame | None = None,
) -> dict:
    """
    Check feature drift between baseline (training) and current data.

    If DataFrames not provided, loads from the features parquet and splits
    temporally: first 80% = baseline, last 20% = current.

    Returns a dict of {feature: psi_score} and logs warnings.
    """
    if baseline_df is None or current_df is None:
        if not FEATURES_PARQUET.exists():
            logger.warning("Features parquet not found — generating synthetic data.")
            from src.ml.feature_engineer import _synthetic_monthly, _synthetic_onset, engineer_features
            baseline_df = engineer_features(_synthetic_monthly(), _synthetic_onset())
            current_df  = baseline_df.copy()
        else:
            df = pd.read_parquet(FEATURES_PARQUET)
            split = int(len(df) * 0.8)
            baseline_df = df.iloc[:split]
            current_df  = df.iloc[split:]

    results = {}
    for col in FEATURE_COLS:
        if col not in baseline_df.columns or col not in current_df.columns:
            continue
        base_vals = baseline_df[col].dropna().values
        curr_vals = current_df[col].dropna().values
        if len(base_vals) == 0 or len(curr_vals) == 0:
            continue
        psi = _psi(base_vals, curr_vals)
        results[col] = round(psi, 4)

        if psi > PSI_CRITICAL_THRESHOLD:
            logger.critical(
                "DRIFT DETECTED: feature='%s' PSI=%.4f > %.1f — retrain recommended!",
                col, psi, PSI_CRITICAL_THRESHOLD,
            )
        elif psi > PSI_WARNING_THRESHOLD:
            logger.warning(
                "Drift warning: feature='%s' PSI=%.4f (moderate)", col, psi
            )
        else:
            logger.info("Feature '%s': PSI=%.4f (stable)", col, psi)

    return results


if __name__ == "__main__":
    setup_logging("model_monitor.log")
    drift = run_drift_check()
    print("\nDrift Report:")
    for feat, psi in drift.items():
        status = "CRITICAL" if psi > PSI_CRITICAL_THRESHOLD else (
                 "WARNING"  if psi > PSI_WARNING_THRESHOLD  else "OK")
        print(f"  {feat:<30} PSI={psi:.4f}  [{status}]")
