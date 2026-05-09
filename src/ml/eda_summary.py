"""
eda_summary.py
Author    : R04 - Eric (EDA & ML Engineer)
Purpose   : Extracts key EDA findings from notebooks as reusable Python
            functions. Produces summary stats, onset distribution plot,
            and correlation matrix from processed parquets or synthetic data.
Milestone : M4 - EDA Summary Module
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

from src.config import DATA_DIR, DOCS_DIR, setup_logging

FEATURES_PARQUET = DATA_DIR / "features" / "onset_features.parquet"


def _load_or_synthetic() -> pd.DataFrame:
    if FEATURES_PARQUET.exists():
        return pd.read_parquet(FEATURES_PARQUET)
    logger.warning("Features parquet not found — using synthetic data.")
    from src.ml.feature_engineer import _synthetic_monthly, _synthetic_onset, engineer_features
    return engineer_features(_synthetic_monthly(), _synthetic_onset())


def load_and_describe(parquet_path: str | Path | None = None) -> dict:
    """
    Load features parquet and return a summary statistics dict.

    Returns
    -------
    dict with keys: shape, dtypes, describe, onset_rate, missing_pct
    """
    path = Path(parquet_path) if parquet_path else FEATURES_PARQUET
    df = pd.read_parquet(path) if path.exists() else _load_or_synthetic()

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    onset_rate = float(df["onset_occurred"].mean()) if "onset_occurred" in df.columns else None

    return {
        "shape":       df.shape,
        "dtypes":      df.dtypes.astype(str).to_dict(),
        "describe":    df[numeric_cols].describe().to_dict(),
        "onset_rate":  onset_rate,
        "missing_pct": (df.isnull().mean() * 100).to_dict(),
    }


def plot_onset_distribution(out_path: str | Path | None = None) -> Path:
    """
    Plot histogram of onset_doy across all counties and seasons.
    Saves to docs/onset_distribution.png (or out_path).
    Returns the output path.
    """
    out = Path(out_path) if out_path else DOCS_DIR / "onset_distribution.png"
    df = _load_or_synthetic()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        DOCS_DIR.mkdir(parents=True, exist_ok=True)
        fig, ax = plt.subplots(figsize=(10, 5))
        df_onset = df.dropna(subset=["onset_doy"])
        ax.hist(df_onset["onset_doy"], bins=30, color="steelblue", edgecolor="white")
        ax.set_xlabel("Onset Day of Year")
        ax.set_ylabel("Count")
        ax.set_title("Kenya Rainfall Onset Distribution (10 Counties, 2000-2023)")
        fig.tight_layout()
        fig.savefig(out, dpi=120)
        plt.close(fig)
        logger.info("Onset distribution plot saved -> %s", out)
    except ImportError:
        logger.warning("Matplotlib not available — skipping plot.")

    return out


def correlation_matrix(out_path: str | Path | None = None) -> Path:
    """
    Compute and plot a correlation matrix for numeric features.
    Saves to docs/correlation_matrix.png (or out_path).
    Returns the output path.
    """
    out = Path(out_path) if out_path else DOCS_DIR / "correlation_matrix.png"
    df = _load_or_synthetic()
    numeric_cols = [c for c in df.columns
                    if c in ("mean_precip_30d", "precip_anomaly",
                              "soil_moisture_proxy", "temp_anomaly_tmax", "onset_doy")]
    corr = df[numeric_cols].corr()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        DOCS_DIR.mkdir(parents=True, exist_ok=True)
        fig, ax = plt.subplots(figsize=(8, 6))
        im = ax.imshow(corr.values, cmap="RdBu_r", vmin=-1, vmax=1)
        ax.set_xticks(range(len(numeric_cols)))
        ax.set_yticks(range(len(numeric_cols)))
        ax.set_xticklabels(numeric_cols, rotation=45, ha="right")
        ax.set_yticklabels(numeric_cols)
        plt.colorbar(im, ax=ax)
        ax.set_title("Feature Correlation Matrix")
        fig.tight_layout()
        fig.savefig(out, dpi=120)
        plt.close(fig)
        logger.info("Correlation matrix saved -> %s", out)
    except ImportError:
        logger.warning("Matplotlib not available — skipping correlation plot.")

    return out


if __name__ == "__main__":
    setup_logging("eda_summary.log")
    report = load_and_describe()
    print(f"Dataset shape: {report['shape']}")
    print(f"Onset rate:    {report['onset_rate']:.2%}" if report["onset_rate"] else "Onset rate: N/A")
    plot_onset_distribution()
    correlation_matrix()
