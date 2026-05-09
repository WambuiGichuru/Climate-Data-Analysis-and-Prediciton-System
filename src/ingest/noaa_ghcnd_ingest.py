"""
noaa_ghcnd_ingest.py
Author    : R01 — Dennis (Data Ingestion Engineer)
Purpose   : Download NOAA GHCND daily data for Kenyan weather stations
            using the public NOAA file server (no API key required).
            Parses the fixed-width .dly format, filters for Kenya (KE),
            and saves to data/raw/noaa_ghcnd_kenya.parquet.
Milestone : M1 — Data Ingestion (NOAA GHCND)

Usage
-----
    python src/ingest/noaa_ghcnd_ingest.py           # all Kenya stations
    python src/ingest/noaa_ghcnd_ingest.py --test    # 3 stations only
"""

from __future__ import annotations

import argparse
import io
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from loguru import logger

# ── path bootstrap ───────────────────────────────────────────────────────────
_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.config import (
    DATA_DIR, LOG_DIR, NOAA_STATIONS_URL, NOAA_DATA_URL, setup_logging,
)
from src.ingest.data_validator import generate_report, DataValidationError

# ── constants ────────────────────────────────────────────────────────────────
RAW_DIR   = DATA_DIR / "raw"
OUT_FILE  = RAW_DIR / "noaa_ghcnd_kenya.parquet"
MAX_RETRY = 3

# GHCND .dly fixed-width column specification (from NOAA readme.txt)
_ELEMENTS = ["PRCP", "TMAX", "TMIN", "SNOW", "SNWD"]


def _backoff_get(url: str, retries: int = MAX_RETRY, timeout: int = 30) -> bytes:
    """GET with exponential-backoff retry. Returns raw bytes."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as exc:
            wait = 2 ** attempt
            logger.warning("Attempt %d/%d failed for %s: %s — retrying in %ds",
                           attempt, retries, url, exc, wait)
            if attempt == retries:
                raise
            time.sleep(wait)
    raise RuntimeError("unreachable")


def fetch_kenya_station_ids(test: bool = False) -> list[str]:
    """Download ghcnd-stations.txt, return IDs for Kenya (country code KE)."""
    logger.info("Downloading station list from NOAA …")
    raw = _backoff_get(NOAA_STATIONS_URL).decode("utf-8", errors="replace")

    station_ids: list[str] = []
    for line in raw.splitlines():
        if len(line) < 11:
            continue
        country = line[0:2]   # first two chars are FIPS country code
        if country == "KE":
            station_ids.append(line[0:11].strip())

    logger.info("Found %d Kenya stations.", len(station_ids))
    if test:
        station_ids = station_ids[:3]
        logger.info("--test mode: using first 3 stations only.")
    return station_ids


def parse_dly(raw_bytes: bytes, station_id: str) -> pd.DataFrame:
    """
    Parse a NOAA GHCND .dly file (fixed-width format) into a tidy DataFrame.

    .dly format: each line describes one element for one station/year/month.
    Columns 0-11: station, year (11-14), month (15-16), element (17-21).
    Then 31 repetitions of value(5) + mflag(1) + qflag(1) + sflag(1).

    Returns DataFrame with columns:
        station_id, date, element, value, quality_flag
    """
    records = []
    text = raw_bytes.decode("ascii", errors="replace")
    for line in text.splitlines():
        if len(line) < 269:
            continue
        sid     = line[0:11]
        year    = int(line[11:15])
        month   = int(line[15:17])
        element = line[17:21].strip()
        if element not in _ELEMENTS:
            continue
        for day in range(1, 32):
            offset = 21 + (day - 1) * 8
            val_str  = line[offset:offset + 5]
            qflag    = line[offset + 6:offset + 7].strip()
            try:
                val = int(val_str)
            except ValueError:
                continue
            if val == -9999:
                continue
            try:
                date = pd.Timestamp(year=year, month=month, day=day)
            except ValueError:
                continue
            # PRCP, TMAX, TMIN are stored in tenths of units
            if element in ("PRCP", "TMAX", "TMIN"):
                val = val / 10.0
            records.append({
                "station_id":   sid,
                "date":         date,
                "element":      element,
                "value":        val,
                "quality_flag": qflag if qflag else None,
            })
    return pd.DataFrame(records)


def ingest_all(test: bool = False) -> None:
    """Main ingestion routine."""
    setup_logging("noaa_ghcnd.log")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    station_ids = fetch_kenya_station_ids(test=test)
    if not station_ids:
        logger.error("No Kenya stations found — check network / NOAA URL.")
        sys.exit(1)

    frames: list[pd.DataFrame] = []
    failed: list[str] = []

    for i, sid in enumerate(station_ids, 1):
        url = NOAA_DATA_URL.format(station_id=sid)
        logger.info("[%d/%d] Downloading %s …", i, len(station_ids), sid)
        try:
            raw = _backoff_get(url)
            df  = parse_dly(raw, sid)
            if not df.empty:
                frames.append(df)
                logger.info("  → %d records parsed.", len(df))
            else:
                logger.warning("  → No records parsed for %s.", sid)
        except Exception as exc:
            logger.error("  → FAILED for %s: %s", sid, exc)
            failed.append(sid)

    if not frames:
        logger.error("No data downloaded. Exiting.")
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Total records: %d  |  Failed stations: %d", len(combined), len(failed))

    # Validate
    try:
        report = generate_report(combined)
        logger.info("Validation passed: %s", report)
    except DataValidationError as exc:
        logger.warning("Validation warning: %s", exc)

    combined.to_parquet(OUT_FILE, index=False)
    logger.info("Saved → %s", OUT_FILE)
    if failed:
        logger.warning("Failed stations: %s", failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NOAA GHCND Kenya ingestion")
    parser.add_argument("--test", action="store_true",
                        help="Download only 3 stations for quick testing")
    args = parser.parse_args()
    ingest_all(test=args.test)
