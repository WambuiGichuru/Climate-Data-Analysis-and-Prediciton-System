"""
schemas.py
Author    : R05 - Faith (DevOps & Deployment Engineer)
Purpose   : Pydantic v2 request/response models for the FastAPI service.
Milestone : M5 - API Schemas
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = "ok"
    timestamp: str


class CountyInfo(BaseModel):
    name: str
    lat: float
    lon: float


class ForecastDay(BaseModel):
    date: str
    precipitation_mm: Optional[float]
    temp_max_c: Optional[float]
    temp_min_c: Optional[float]


class ForecastResponse(BaseModel):
    county: str
    forecast: list[ForecastDay]
    source: str = "openmeteo"


class OnsetPrediction(BaseModel):
    county: str
    onset_probability: float = Field(ge=0.0, le=1.0)
    onset_doy_estimate: int
    alert_level: str
    source: str


class AlertRecord(BaseModel):
    county: str
    timestamp: Optional[str]
    alert_level: Optional[str]
    onset_probability: Optional[float]
    rolling_72hr_precip: Optional[float]
