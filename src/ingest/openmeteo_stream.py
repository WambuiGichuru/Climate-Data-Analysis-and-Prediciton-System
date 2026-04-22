"""
openmeteo_stream.py
R03 – Streaming & Real-Time Engineer (Alexander Kihoi)
Milestone 1: OpenMeteo API ingestion prototype

Fetches hourly forecast data for 10 Kenya counties relevant to the
Rainfall Onset Advisory system and saves each county's data as a
timestamped JSON file, simulating what will later flow into Pub/Sub.

Data source: Open-Meteo Forecast API (no API key required)
Docs: https://open-meteo.com/en/docs
"""

import requests
import json
import os
from datetime import datetime

# ── County coordinates (lat/lon for county HQ or centroid) ──────────────────
KENYA_COUNTIES = {
    "Nairobi":      {"lat": -1.2921,  "lon": 36.8219},
    "Kisumu":       {"lat": -0.0917,  "lon": 34.7679},
    "Nakuru":       {"lat": -0.3031,  "lon": 36.0800},
    "Meru":         {"lat":  0.0467,  "lon": 37.6491},
    "Kitui":        {"lat": -1.3667,  "lon": 38.0167},
    "Garissa":      {"lat": -0.4532,  "lon": 39.6461},   # Tana River basin
    "Machakos":     {"lat": -1.5177,  "lon": 37.2634},
    "Eldoret":      {"lat":  0.5143,  "lon": 35.2698},   # Uasin Gishu
    "Kakamega":     {"lat":  0.2827,  "lon": 34.7519},
    "Embu":         {"lat": -0.5300,  "lon": 37.4500},
}

# ── OpenMeteo variables we care about for onset detection ───────────────────
HOURLY_VARS = [
    "precipitation",          # mm/hr  – core onset signal
    "soil_moisture_0_to_1cm", # m³/m³  – antecedent moisture
    "relative_humidity_2m",   # %      – atmospheric moisture
    "wind_speed_10m",         # km/h   – synoptic pattern indicator
    "temperature_2m",         # °C     – context variable
]

# ── Output directory ─────────────────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../../logs/openmeteo_raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_county_forecast(county_name: str, lat: float, lon: float) -> dict | None:
    """
    Call the OpenMeteo Forecast API for a single county.
    Returns the parsed JSON response or None on failure.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":        lat,
        "longitude":       lon,
        "hourly":          ",".join(HOURLY_VARS),
        "forecast_days":   7,          # 7-day lookahead for onset detection
        "timezone":        "Africa/Nairobi",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Attach metadata for traceability
        data["_meta"] = {
            "county":       county_name,
            "fetched_at":   datetime.utcnow().isoformat() + "Z",
            "source":       "OpenMeteo Forecast API v1",
            "role":         "R03 – Streaming & Real-Time Engineer",
            "milestone":    "M1 – Data Foundations & System Architecture",
        }
        return data

    except requests.exceptions.Timeout:
        print(f"  [TIMEOUT]  {county_name} – request took too long")
    except requests.exceptions.HTTPError as e:
        print(f"  [HTTP ERR] {county_name} – {e}")
    except requests.exceptions.RequestException as e:
        print(f"  [NET ERR]  {county_name} – {e}")

    return None


def compute_onset_signals(hourly: dict) -> dict:
    """
    Basic onset signal computation from raw hourly data.

    Onset heuristic (simplified for M1):
      - 3-day cumulative precipitation >= 20 mm
      - No dry spell (0 mm day) in the 3 days that follow
    Returns a dict with computed signals for the first available 72-hour window.
    """
    precip = hourly.get("precipitation", [])
    times  = hourly.get("time", [])

    if len(precip) < 72:
        return {"onset_signal": "insufficient_data"}

    # Sum first 72 hours (3 days)
    cum_72hr = sum(precip[:72])

    # Check for dry spell in hours 72–168 (next 4 days)
    dry_spell = any(p == 0.0 for p in precip[72:168])

    onset_flag = cum_72hr >= 20.0 and not dry_spell

    return {
        "cum_precipitation_72hr_mm": round(cum_72hr, 2),
        "dry_spell_detected":        dry_spell,
        "onset_flag":                onset_flag,
        "onset_window_start":        times[0]  if times else None,
        "onset_window_end":          times[71] if len(times) > 71 else None,
    }


def save_to_json(county_name: str, data: dict) -> str:
    """Save county forecast to a timestamped JSON file in logs/openmeteo_raw/"""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename  = f"{county_name.lower()}_{timestamp}.json"
    filepath  = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    return filepath


def print_summary(county_name: str, signals: dict) -> None:
    """Print a human-readable summary to terminal."""
    flag   = "🟢 ONSET LIKELY" if signals.get("onset_flag") else "🔴 No onset"
    cum    = signals.get("cum_precipitation_72hr_mm", "N/A")
    dry    = signals.get("dry_spell_detected", "N/A")

    print(f"\n  County        : {county_name}")
    print(f"  72-hr rainfall: {cum} mm")
    print(f"  Dry spell next: {dry}")
    print(f"  Signal        : {flag}")


def main():
    print("=" * 60)
    print("  OpenMeteo Ingestion Prototype — R03 Streaming Engineer")
    print(f"  Run time (UTC): {datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    results = []

    for county, coords in KENYA_COUNTIES.items():
        print(f"\n[FETCH] {county} ({coords['lat']}, {coords['lon']}) ...")

        data = fetch_county_forecast(county, coords["lat"], coords["lon"])
        if data is None:
            print(f"  [SKIP] {county} – no data returned")
            continue

        # Compute onset signals
        signals = compute_onset_signals(data.get("hourly", {}))
        data["_onset_signals"] = signals

        # Save raw + signals to JSON
        filepath = save_to_json(county, data)
        print(f"  [SAVED] {filepath}")

        # Print summary
        print_summary(county, signals)

        results.append({
            "county":  county,
            "signals": signals,
        })

    # ── Velocity characterisation for M1 report ─────────────────────────────
    print("\n" + "=" * 60)
    print("  STREAMING DATA CHARACTERISATION (M1 – Velocity)")
    print("=" * 60)
    print(f"  Counties polled    : {len(results)}")
    print(f"  Variables per call : {len(HOURLY_VARS)}")
    print(f"  Hours per variable : 168  (7-day × 24hr)")
    print(f"  Records this run   : {len(results) * len(HOURLY_VARS) * 168:,}")
    print(f"  Update frequency   : Hourly (in production via Pub/Sub)")
    print(f"  Daily record volume: {len(KENYA_COUNTIES) * len(HOURLY_VARS) * 168 * 24:,} rows/day (47 counties at scale)")
    print("=" * 60)
    print("\n✅ M1 prototype complete. Check logs/openmeteo_raw/ for raw JSON files.")


if __name__ == "__main__":
    main()