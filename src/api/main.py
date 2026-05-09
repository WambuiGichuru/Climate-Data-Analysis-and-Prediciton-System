"""
main.py
Author    : R05 - Faith (DevOps & Deployment Engineer)
Purpose   : FastAPI REST service exposing forecasts, onset predictions,
            county info and live alerts. Loads ML models at startup.
            Returns mock predictions if models are not yet trained.
Milestone : M5 - REST API
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from src.config import KENYA_COUNTIES, COUNTY_NAMES, OPENMETEO_FORECAST_URL, setup_logging
from src.api.schemas import (
    HealthResponse, CountyInfo, ForecastResponse, ForecastDay,
    OnsetPrediction, AlertRecord,
)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Kenya Rainfall Onset Advisory API",
    version="1.0.0",
    description="Lambda Architecture — SDS 2412",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_models_loaded = False


@app.on_event("startup")
async def _startup():
    global _models_loaded
    setup_logging("api.log")
    logger.info("FastAPI startup: loading ML models ...")
    try:
        from src.ml.realtime_scorer import _load_models
        clf, reg = _load_models()
        _models_loaded = clf is not None
        if _models_loaded:
            logger.info("Models loaded successfully.")
        else:
            logger.warning("Models not found — mock predictions will be used.")
    except Exception as exc:
        logger.warning("Model loading failed: %s — mock mode active.", exc)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="ok",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/counties", response_model=list[CountyInfo])
async def list_counties():
    return [
        CountyInfo(name=name, lat=coords["lat"], lon=coords["lon"])
        for name, coords in KENYA_COUNTIES.items()
    ]


@app.get("/forecast/{county}", response_model=ForecastResponse)
async def get_forecast(county: str):
    if county not in KENYA_COUNTIES:
        raise HTTPException(
            status_code=404,
            detail=f"County '{county}' not found. Valid: {COUNTY_NAMES}",
        )
    coords = KENYA_COUNTIES[county]
    params = {
        "latitude":    coords["lat"],
        "longitude":   coords["lon"],
        "daily":       "precipitation_sum,temperature_2m_max,temperature_2m_min",
        "forecast_days": 7,
        "timezone":    "Africa/Nairobi",
    }
    try:
        resp = requests.get(OPENMETEO_FORECAST_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"OpenMeteo API error: {exc}")

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    precips = daily.get("precipitation_sum", [None] * len(dates))
    tmaxs   = daily.get("temperature_2m_max", [None] * len(dates))
    tmins   = daily.get("temperature_2m_min", [None] * len(dates))

    forecast_days = [
        ForecastDay(
            date=dates[i],
            precipitation_mm=precips[i],
            temp_max_c=tmaxs[i],
            temp_min_c=tmins[i],
        )
        for i in range(len(dates))
    ]
    return ForecastResponse(county=county, forecast=forecast_days)


@app.get("/onset/{county}", response_model=OnsetPrediction)
async def get_onset_prediction(county: str):
    if county not in KENYA_COUNTIES:
        raise HTTPException(
            status_code=404,
            detail=f"County '{county}' not found. Valid: {COUNTY_NAMES}",
        )
    from src.ml.realtime_scorer import predict_onset
    result = predict_onset(county)
    return OnsetPrediction(**result)


@app.get("/alerts/live", response_model=list[AlertRecord])
async def get_live_alerts():
    from src.config import LOG_DIR
    alerts_dir = LOG_DIR / "streaming_output" / "onset_alerts"
    records = []
    if alerts_dir.exists():
        import pandas as pd
        files = sorted(alerts_dir.glob("*.parquet"))
        if files:
            frames = [pd.read_parquet(f) for f in files[-3:]]
            df = pd.concat(frames, ignore_index=True).tail(20)
            records = df.to_dict(orient="records")
    return [AlertRecord(**r) for r in records]
