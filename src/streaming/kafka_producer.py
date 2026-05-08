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
                "timestamp":     str,   # ISO-8601, e.g. "2025-04-01T14:00"
                "precipitation": float, # mm/hr
                "temperature":   float, # °C
                "soil_moisture": float  # m³/m³
              }

Requires:
    kafka-python >= 2.0.2  (pip install kafka-python)
    Docker Kafka on localhost:9092  (docker-compose up -d from repo root)

Run:
    python src/streaming/kafka_producer.py
"""

import json
import time
import logging
import sys
from pathlib import Path
from datetime import datetime

import requests

# ── Lazy import kafka so the module can be imported without Kafka installed ───
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError as _kafka_err:
    raise ImportError(
        "kafka-python is not installed.\n"
        "Fix: pip install 'kafka-python>=2.0.2'"
    ) from _kafka_err

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "streaming.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("kafka_producer")

# ── Kafka / polling config ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "raw-weather-stream"
POLL_INTERVAL   = 60   # seconds between full county sweeps
MAX_RETRIES     = 5    # connection attempts before giving up
RETRY_DELAY     = 10   # seconds between retries

# ── Kenya county coordinates (mirrors M1 openmeteo_stream.py) ─────────────────
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

OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"


# ── OpenMeteo fetch ────────────────────────────────────────────────────────────

def fetch_current_weather(county: str, lat: float, lon: float) -> dict | None:
    """
    Fetch the latest available hourly observation from OpenMeteo for one county.

    Returns a dict matching the Kafka message schema, or None on any error.
    Field mapping:
      precipitation  ← hourly.precipitation       (mm/hr)
      temperature    ← hourly.temperature_2m       (°C)
      soil_moisture  ← hourly.soil_moisture_0_to_1cm (m³/m³)
    """
    params = {
        "latitude":    lat,
        "longitude":   lon,
        "hourly":      "precipitation,temperature_2m,soil_moisture_0_to_1cm",
        "forecast_days": 1,
        "timezone":    "Africa/Nairobi",
    }
    try:
        resp = requests.get(OPENMETEO_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        logger.warning("HTTP error fetching %s: %s", county, exc)
        return None

    hourly = data.get("hourly", {})
    times  = hourly.get("time", [])
    precip = hourly.get("precipitation", [])
    temps  = hourly.get("temperature_2m", [])
    soil   = hourly.get("soil_moisture_0_to_1cm", [])

    if not times:
        logger.warning("Empty hourly payload for %s — skipping.", county)
        return None

    # Use the most recent available hour index
    idx = len(times) - 1
    return {
        "county":        county,
        "timestamp":     times[idx],                                  # ISO string
        "precipitation": round(float(precip[idx]), 2) if idx < len(precip) else 0.0,
        "temperature":   round(float(temps[idx]),  1) if idx < len(temps)  else None,
        "soil_moisture": round(float(soil[idx]),   4) if idx < len(soil)   else None,
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
    logger.info("=" * 60)
    logger.info("Kafka Weather Producer — R03 M2")
    logger.info("Topic  : %s", KAFKA_TOPIC)
    logger.info("Broker : %s", KAFKA_BOOTSTRAP)
    logger.info("Interval: %ds per county sweep", POLL_INTERVAL)
    logger.info("=" * 60)

    producer = create_producer()
    cycle = 0

    try:
        while True:
            cycle += 1
            logger.info("─── Poll cycle %d  (%s UTC) ───",
                        cycle, datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"))
            sent = poll_and_publish(producer)
            logger.info(
                "Cycle %d done — %d/%d messages sent. Sleeping %ds …",
                cycle, sent, len(KENYA_COUNTIES), POLL_INTERVAL,
            )
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user (KeyboardInterrupt).")
    finally:
        producer.close()
        logger.info("KafkaProducer closed cleanly.")


if __name__ == "__main__":
    main()
