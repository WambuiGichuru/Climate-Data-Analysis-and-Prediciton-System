"""
kafka_producer.py
Author    : R03 — Alexander Kihoi (Streaming & Real-Time Engineer)
Milestone : M2 — Streaming Infrastructure
Purpose   : Extends the M1 OpenMeteo live poller to publish hourly weather
            events as JSON messages to the Kafka topic 'raw-weather-stream'.
            Polls the OpenMeteo Forecast API once per POLL_INTERVAL seconds,
            emitting one message per county per cycle.

            Message schema (JSON):
              {
                "county":        str,   # e.g. "Nairobi"
                "timestamp":     str,   # ISO-8601 UTC, e.g. "2025-04-01T14:00"
                "lat":           float, # county latitude
                "lon":           float, # county longitude
                "precipitation": float, # mm/hr
                "temperature":   float, # °C
                "soil_moisture": float, # m³/m³
                "humidity":      float, # % relative humidity
                "wind_speed":    float  # m/s
              }

Requires:
    kafka-python >= 2.0.2  (pip install kafka-python)
    Docker Kafka on localhost:9092  (docker-compose up -d from repo root)

Run:
    python src/streaming/kafka_producer.py
"""

import json
import signal
import time
import sys
from pathlib import Path
from datetime import datetime, timezone

import requests
from loguru import logger

# ── Lazy import kafka so the module can be imported without Kafka installed ───
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError as _kafka_err:
    raise ImportError(
        "kafka-python is not installed.\n"
        "Fix: pip install 'kafka-python>=2.0.2'"
    ) from _kafka_err

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.config import (
    KENYA_COUNTIES, KAFKA_BROKER, KAFKA_RAW_TOPIC,
    OPENMETEO_FORECAST_URL, LOG_DIR, setup_logging,
)

# ── Kafka / polling config ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = KAFKA_BROKER
KAFKA_TOPIC     = KAFKA_RAW_TOPIC
POLL_INTERVAL   = 60   # seconds between full county sweeps
MAX_RETRIES     = 5    # connection attempts before giving up
RETRY_DELAY     = 10   # seconds between retries

OPENMETEO_URL   = OPENMETEO_FORECAST_URL

_SHUTDOWN = False

def _handle_sigint(signum, frame):
    global _SHUTDOWN
    _SHUTDOWN = True
    logger.info("Shutdown signal received — will exit after current cycle.")

signal.signal(signal.SIGINT,  _handle_sigint)
signal.signal(signal.SIGTERM, _handle_sigint)


# ── OpenMeteo fetch ────────────────────────────────────────────────────────────

def fetch_current_weather(county: str, lat: float, lon: float) -> dict | None:
    """
    Fetch latest hourly observation from OpenMeteo for one county.

    Returns a dict matching the full Kafka message schema, or None on error.
    Retries up to 3x with exponential backoff on API failures.
    """
    params = {
        "latitude":    lat,
        "longitude":   lon,
        "hourly":      ("precipitation,temperature_2m,soil_moisture_0_to_1cm,"
                        "relative_humidity_2m,wind_speed_10m"),
        "forecast_days": 1,
        "timezone":    "Africa/Nairobi",
    }
    for attempt in range(1, 4):
        try:
            resp = requests.get(OPENMETEO_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            break
        except requests.RequestException as exc:
            wait = 2 ** attempt
            logger.warning("HTTP error fetching %s (attempt %d/3): %s — retry in %ds",
                           county, attempt, exc, wait)
            if attempt == 3:
                return None
            time.sleep(wait)

    hourly = data.get("hourly", {})
    times  = hourly.get("time", [])
    if not times:
        logger.warning("Empty hourly payload for %s — skipping.", county)
        return None

    idx = len(times) - 1

    def _safe(key, default=None):
        vals = hourly.get(key, [])
        return round(float(vals[idx]), 4) if idx < len(vals) and vals[idx] is not None else default

    return {
        "county":        county,
        "timestamp":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lat":           lat,
        "lon":           lon,
        "precipitation": _safe("precipitation", 0.0),
        "temperature":   _safe("temperature_2m"),
        "soil_moisture": _safe("soil_moisture_0_to_1cm"),
        "humidity":      _safe("relative_humidity_2m"),
        "wind_speed":    _safe("wind_speed_10m"),
    }


# ── Kafka producer lifecycle ───────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    """
    Connect to Kafka with exponential-backoff retry.
    Raises ConnectionError if all retries are exhausted.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                # Serialise every message value to UTF-8 JSON bytes
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",    # wait for leader + all in-sync replicas
                retries=3,
                linger_ms=5,   # micro-batch window to improve throughput
            )
            logger.info("Connected to Kafka at %s (attempt %d).", KAFKA_BOOTSTRAP, attempt)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available (attempt %d/%d). Retrying in %ds …",
                attempt, MAX_RETRIES, RETRY_DELAY,
            )
            time.sleep(RETRY_DELAY)

    raise ConnectionError(
        f"Cannot reach Kafka at {KAFKA_BOOTSTRAP} after {MAX_RETRIES} attempts.\n"
        "Is Docker running?  Try: docker-compose up -d"
    )


def poll_and_publish(producer: KafkaProducer) -> int:
    """
    Fetch weather data for all 10 counties and publish each as a Kafka message.
    Returns the count of successfully delivered messages.
    """
    sent = 0
    for county, coords in KENYA_COUNTIES.items():
        msg = fetch_current_weather(county, coords["lat"], coords["lon"])
        if msg is None:
            continue  # already logged inside fetch_current_weather

        future = producer.send(KAFKA_TOPIC, value=msg)
        try:
            meta = future.get(timeout=10)
            logger.info(
                "SENT  county=%-10s  precip=%5.2f mm  → partition=%d  offset=%d",
                county, msg.get("precipitation", 0.0), meta.partition, meta.offset,
            )
            sent += 1
        except Exception as exc:
            logger.error("Publish failed for %s: %s", county, exc)

    producer.flush()   # ensure all buffered messages are delivered
    return sent


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    setup_logging("streaming.log")
    logger.info("=" * 60)
    logger.info("Kafka Weather Producer — R03 M2/M3")
    logger.info("Topic  : %s", KAFKA_TOPIC)
    logger.info("Broker : %s", KAFKA_BOOTSTRAP)
    logger.info("Interval: %ds per county sweep", POLL_INTERVAL)
    logger.info("=" * 60)

    producer = create_producer()
    cycle = 0

    try:
        while not _SHUTDOWN:
            cycle += 1
            logger.info("Poll cycle %d  (%s UTC)",
                        cycle, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))
            sent = poll_and_publish(producer)
            logger.info(
                "Cycle %d done — %d/%d messages sent. Sleeping %ds ...",
                cycle, sent, len(KENYA_COUNTIES), POLL_INTERVAL,
            )
            time.sleep(POLL_INTERVAL)
    finally:
        producer.close()
        logger.info("KafkaProducer closed cleanly.")


if __name__ == "__main__":
    main()
