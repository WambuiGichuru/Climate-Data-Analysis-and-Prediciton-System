"""
openmeteo_live_poller.py
Author    : R03 - Alexander Kihoi (Streaming & Real-Time Engineer)
Purpose   : Poll OpenMeteo forecast API for all 10 Kenya counties. Saves raw
            JSON to logs/openmeteo_raw/ and prints a velocity characterisation
            report. This is the M1 prototype that kafka_producer.py extends.
Milestone : M1 - OpenMeteo Live Polling prototype
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from loguru import logger

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.config import KENYA_COUNTIES, OPENMETEO_FORECAST_URL, LOG_DIR, setup_logging

RAW_DIR = LOG_DIR / "openmeteo_raw"

_VARIABLES = (
    "precipitation,soil_moisture_0_to_1cm,"
    "relative_humidity_2m,wind_speed_10m,temperature_2m"
)


def poll_once() -> list[dict]:
    """Fetch current hourly data for all 10 counties. Returns list of records."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    records = []
    for county, coords in KENYA_COUNTIES.items():
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "hourly": _VARIABLES,
            "forecast_days": 1,
            "timezone": "Africa/Nairobi",
        }
        try:
            resp = requests.get(OPENMETEO_FORECAST_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.warning("[%s] fetch failed: %s", county, exc)
            continue

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        out = RAW_DIR / f"{county.lower()}_{ts}.json"
        out.write_text(json.dumps(data, indent=2))
        logger.info("[%s] saved -> %s", county, out.name)

        hourly = data.get("hourly", {})
        times  = hourly.get("time", [])
        if times:
            idx = len(times) - 1
            records.append({
                "county":        county,
                "timestamp":     times[idx],
                "lat":           coords["lat"],
                "lon":           coords["lon"],
                "precipitation": hourly.get("precipitation", [0.0])[idx] or 0.0,
                "temperature":   hourly.get("temperature_2m", [None])[idx],
                "soil_moisture": hourly.get("soil_moisture_0_to_1cm", [None])[idx],
                "humidity":      hourly.get("relative_humidity_2m", [None])[idx],
                "wind_speed":    hourly.get("wind_speed_10m", [None])[idx],
            })
    return records


def velocity_report(records: list[dict]) -> None:
    print("\n=== OpenMeteo Velocity Characterisation ===")
    print(f"  Counties polled  : {len(records)}/10")
    precips = [r["precipitation"] for r in records if r["precipitation"] is not None]
    if precips:
        print(f"  Precip range (mm): {min(precips):.2f} - {max(precips):.2f}")
    print(f"  Timestamp        : {datetime.now(timezone.utc).isoformat()}")
    print("===========================================\n")


if __name__ == "__main__":
    setup_logging("openmeteo_poller.log")
    logger.info("Starting OpenMeteo live poller ...")
    records = poll_once()
    velocity_report(records)
    logger.info("Poll complete: %d records", len(records))
