"""
openmeteo_historical_ingest.py
Author    : R01 — Dennis (Data Ingestion Engineer)
Purpose   : Fetch daily historical weather data (2000-01-01 to 2023-12-31)
            for all 10 Kenyan counties from the OpenMeteo ERA5-Land archive
            API.  Saves one parquet per county and one combined parquet.
            No Copernicus credentials required — ERA5-Land data is served
            transparently by the OpenMeteo archive endpoint.
Milestone : M1 — Data Ingestion (OpenMeteo Historical / ERA5 proxy)

Usage
-----
    python src/ingest/openmeteo_historical_ingest.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import requests
from loguru import logger

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.config import (
    DATA_DIR, KENYA_COUNTIES, OPENMETEO_ARCHIVE_URL,
    OPENMETEO_HISTORICAL_START, OPENMETEO_HISTORICAL_END,
    setup_logging,
)
from src.ingest.data_validator import generate_report, DataValidationError

RAW_DIR = DATA_DIR / "raw"

_VARIABLES = [
    "precipitation_sum",
    "temperature_2m_max",
    "temperature_2m_min",
    "et0_fao_evapotranspiration",
]

_MAX_RETRY = 3


def _fetch_county(county: str, lat: float, lon: float,
                  start: str, end: str) -> pd.DataFrame:
    """Fetch daily archive data for one county with retry."""
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start,
        "end_date":   end,
        "daily":      ",".join(_VARIABLES),
        "timezone":   "Africa/Nairobi",
    }

    for attempt in range(1, _MAX_RETRY + 1):
        try:
            resp = requests.get(OPENMETEO_ARCHIVE_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            break
        except requests.RequestException as exc:
            wait = 2 ** attempt
            logger.warning("[%s] Attempt %d/%d failed: %s — retry in %ds",
                           county, attempt, _MAX_RETRY, exc, wait)
            if attempt == _MAX_RETRY:
                raise
            time.sleep(wait)

    daily = data.get("daily", {})
    times = daily.get("time", [])
    if not times:
        raise ValueError(f"Empty payload for {county}")

    df = pd.DataFrame({"date": pd.to_datetime(times)})
    for var in _VARIABLES:
        df[var] = daily.get(var, [None] * len(times))
    df.insert(0, "county", county)
    df.insert(1, "lat", lat)
    df.insert(2, "lon", lon)
    return df


def ingest_all() -> None:
    """Fetch all counties and save per-county and combined parquets."""
    setup_logging("openmeteo_historical.log")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    all_frames: list[pd.DataFrame] = []

    for county, coords in KENYA_COUNTIES.items():
        logger.info("Fetching %s (%s → %s) …",
                    county, OPENMETEO_HISTORICAL_START, OPENMETEO_HISTORICAL_END)
        try:
            df = _fetch_county(
                county, coords["lat"], coords["lon"],
                OPENMETEO_HISTORICAL_START, OPENMETEO_HISTORICAL_END,
            )
            # Validate
            try:
                generate_report(df)
            except DataValidationError as exc:
                logger.warning("[%s] Validation warning: %s", county, exc)

            out = RAW_DIR / f"openmeteo_historical_{county.lower()}.parquet"
            df.to_parquet(out, index=False)
            logger.info("  → %d rows → %s", len(df), out.name)
            all_frames.append(df)
        except Exception as exc:
            logger.error("  → FAILED for %s: %s", county, exc)

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        out_all = RAW_DIR / "openmeteo_historical_all.parquet"
        combined.to_parquet(out_all, index=False)
        logger.info("Combined: %d rows → %s", len(combined), out_all.name)
    else:
        logger.error("No data fetched for any county.")


if __name__ == "__main__":
    ingest_all()
