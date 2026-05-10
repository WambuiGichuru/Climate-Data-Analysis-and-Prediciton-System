"""
config.py
Author  : SDS 2412 — Kenya Rainfall Onset Advisory Dashboard (shared foundation)
Purpose : Central constants and configuration for the full Lambda-Architecture
          pipeline.  Every other module imports from here.
Milestone: M0 (shared) — referenced by all milestones M1-M6.
"""

from pathlib import Path
import sys

# ---------------------------------------------------------------------------
# Root paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR  = REPO_ROOT / "data"
LOG_DIR   = REPO_ROOT / "logs"
MODEL_DIR = REPO_ROOT / "models"
DOCS_DIR  = REPO_ROOT / "docs"

for _d in [DATA_DIR / "raw", DATA_DIR / "processed", DATA_DIR / "features",
           LOG_DIR, MODEL_DIR, DOCS_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Kenya county coordinates  (WGS-84)
# ---------------------------------------------------------------------------
KENYA_COUNTIES: dict[str, dict[str, float]] = {
    "Nairobi":  {"lat": -1.2921, "lon": 36.8219},
    "Kisumu":   {"lat": -0.0917, "lon": 34.7679},
    "Nakuru":   {"lat": -0.3031, "lon": 36.0800},
    "Meru":     {"lat":  0.0467, "lon": 37.6491},
    "Kitui":    {"lat": -1.3667, "lon": 38.0167},
    "Garissa":  {"lat": -0.4532, "lon": 39.6461},
    "Machakos": {"lat": -1.5177, "lon": 37.2634},
    "Eldoret":  {"lat":  0.5143, "lon": 35.2698},
    "Kakamega": {"lat":  0.2827, "lon": 34.7519},
    "Embu":     {"lat": -0.5300, "lon": 37.4500},
}
COUNTY_NAMES: list[str] = list(KENYA_COUNTIES.keys())

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BROKER       = "localhost:9092"
KAFKA_RAW_TOPIC    = "raw-weather-stream"
KAFKA_ALERTS_TOPIC = "onset-alerts"

# ---------------------------------------------------------------------------
# Onset detection thresholds
# ---------------------------------------------------------------------------
ONSET_THRESHOLD_MM  = 20.0
ONSET_WINDOW_HOURS  = 72

# ---------------------------------------------------------------------------
# OpenMeteo API
# ---------------------------------------------------------------------------
OPENMETEO_FORECAST_URL        = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_ARCHIVE_URL         = "https://archive-api.open-meteo.com/v1/archive"
OPENMETEO_HISTORICAL_START    = "2000-01-01"
OPENMETEO_HISTORICAL_END      = "2023-12-31"

# ---------------------------------------------------------------------------
# NOAA GHCND
# ---------------------------------------------------------------------------
NOAA_STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
NOAA_DATA_URL     = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{station_id}.dly"

# ---------------------------------------------------------------------------
# Logging (loguru)
# ---------------------------------------------------------------------------

def setup_logging(log_file: str = "app.log", level: str = "INFO") -> None:
    """Configure loguru. Call once per entry-point script."""
    try:
        from loguru import logger
        logger.remove()
        logger.add(sys.stderr, level=level,
                   format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                          "<level>{level: <8}</level> | "
                          "<cyan>{name}</cyan>:<cyan>{line}</cyan> — {message}")
        logger.add(LOG_DIR / log_file, level=level, rotation="10 MB",
                   retention="30 days", encoding="utf-8",
                   format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
                          "{name}:{line} — {message}")
    except ImportError:
        import logging as _logging
        _logging.basicConfig(
            level=getattr(_logging, level.upper(), _logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        )
