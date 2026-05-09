"""
feature_engineer.py
Author    : R04 - Eric (EDA & ML Engineer)
Purpose   : Reads processed monthly aggregates and historical onset dates
            to build ML-ready features per county per year-season.
            Generates synthetic fallback data if processed parquets are absent.
Milestone : M4 - Feature Engineering
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

from src.config import DATA_DIR, COUNTY_NAMES, setup_logging

MONTHLY_PARQUET = DATA_DIR / "processed" / "monthly_aggregates.parquet"
ONSET_PARQUET   = DATA_DIR / "processed" / "historical_onset_dates.parquet"
FEATURES_OUT    = DATA_DIR / "features"  / "onset_features.parquet"


# ---------------------------------------------------------------------------
# Synthetic fallback data
# ---------------------------------------------------------------------------

def _synthetic_monthly() -> pd.DataFrame:
    """Generate realistic synthetic monthly aggregates (2000-2023, 10 counties)."""
    rng = np.random.default_rng(42)
    rows = []
    for county in COUNTY_NAMES:
        for year in range(2000, 2024):
            for month in range(1, 13):
                # Seasonal precipitation pattern: MAM (3-5) and OND (10-12) are wet
                base = 60 if month in (3, 4, 5, 10, 11, 12) else 15
                prcp = max(0, rng.normal(base, 20))
                rows.append({
                    "station_id":           county,
                    "year":                 year,
                    "month":                month,
                    "monthly_total_precip_mm": round(prcp, 2),
                    "monthly_avg_tmax_c":      round(rng.normal(27, 3), 2),
                    "monthly_avg_tmin_c":      round(rng.normal(15, 3), 2),
                    "days_with_rain":           int(rng.integers(2, 20)),
                    "dry_spell_max_days":       int(rng.integers(1, 15)),
                })
    return pd.DataFrame(rows)


def _synthetic_onset() -> pd.DataFrame:
    """Generate synthetic onset dates for MAM and OND seasons."""
    rng = np.random.default_rng(42)
    rows = []
    for county in COUNTY_NAMES:
        for year in range(2000, 2024):
            for season in ("MAM", "OND"):
                base_doy = 75 if season == "MAM" else 285
                doy = int(rng.normal(base_doy, 14))
                doy = max(60 if season == "MAM" else 274, min(doy, 150 if season == "MAM" else 365))
                rows.append({
                    "station_id":          county,
                    "year":                year,
                    "season":              season,
                    "onset_date":          f"{year}-{(doy//30+1):02d}-01",
                    "cum_precip_at_onset": round(abs(rng.normal(25, 5)), 2),
                })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def engineer_features(monthly_df: pd.DataFrame, onset_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build per-county per-year-season feature rows.

    Output columns
    --------------
    county, year, season, mean_precip_30d, precip_anomaly, soil_moisture_proxy,
    temp_anomaly_tmax, onset_doy (regression target), onset_occurred (classification target)
    """
    # Seasonal precipitation: last 3 months before the season
    season_months = {"MAM": [2, 3, 4], "OND": [9, 10, 11]}

    county_col = "station_id" if "station_id" in monthly_df.columns else "county"

    # Compute climatological mean precipitation per county/month
    clim_mean = (monthly_df.groupby([county_col, "month"])["monthly_total_precip_mm"]
                            .mean().rename("clim_mean_precip"))

    rows = []
    for county in monthly_df[county_col].unique():
        cdf = monthly_df[monthly_df[county_col] == county].copy()
        for year in cdf["year"].unique():
            for season, months in season_months.items():
                window = cdf[(cdf["year"] == year) & (cdf["month"].isin(months))]
                if window.empty:
                    continue

                mean_precip = window["monthly_total_precip_mm"].mean()
                clim_vals = [
                    clim_mean.get((county, m), mean_precip) for m in months
                ]
                clim_avg = float(np.mean(clim_vals))
                precip_anomaly = mean_precip - clim_avg

                soil_proxy = (window["monthly_total_precip_mm"].sum() / 5.0)

                tmax_mean = window["monthly_avg_tmax_c"].mean() if "monthly_avg_tmax_c" in window else 27.0
                tmax_clim = 27.0
                temp_anomaly = tmax_mean - tmax_clim

                # Find onset for this county/year/season
                onset_col = "station_id" if "station_id" in onset_df.columns else "county"
                match = onset_df[
                    (onset_df[onset_col] == county) &
                    (onset_df["year"]   == year) &
                    (onset_df["season"] == season)
                ]
                if not match.empty:
                    onset_doy = pd.to_datetime(match.iloc[0]["onset_date"]).day_of_year
                    onset_occurred = True
                else:
                    onset_doy = np.nan
                    onset_occurred = False

                rows.append({
                    "county":              county,
                    "year":                int(year),
                    "season":              season,
                    "mean_precip_30d":     round(float(mean_precip), 2),
                    "precip_anomaly":      round(float(precip_anomaly), 2),
                    "soil_moisture_proxy": round(float(soil_proxy), 2),
                    "temp_anomaly_tmax":   round(float(temp_anomaly), 2),
                    "onset_doy":           float(onset_doy) if not np.isnan(onset_doy) else np.nan,
                    "onset_occurred":      bool(onset_occurred),
                })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging("feature_engineer.log")

    if MONTHLY_PARQUET.exists() and ONSET_PARQUET.exists():
        logger.info("Loading processed parquets ...")
        monthly_df = pd.read_parquet(MONTHLY_PARQUET)
        onset_df   = pd.read_parquet(ONSET_PARQUET)
    else:
        logger.warning("Processed parquets not found — using synthetic fallback data.")
        monthly_df = _synthetic_monthly()
        onset_df   = _synthetic_onset()

    logger.info("Monthly rows: %d | Onset rows: %d", len(monthly_df), len(onset_df))
    features = engineer_features(monthly_df, onset_df)
    logger.info("Feature rows: %d", len(features))

    FEATURES_OUT.parent.mkdir(parents=True, exist_ok=True)
    features.to_parquet(FEATURES_OUT, index=False)
    logger.info("Features saved -> %s", FEATURES_OUT)
    return features


if __name__ == "__main__":
    main()
