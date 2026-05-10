"""
topic_setup.py
Author    : R03 - Alexander Kihoi (Streaming & Real-Time Engineer)
Purpose   : Creates Kafka topics programmatically on first run.
            Retries if Kafka is still starting up (polls 10x with 5s sleep).
Milestone : M3 - Streaming Infrastructure
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from loguru import logger
from src.config import (
    KAFKA_BROKER, KAFKA_RAW_TOPIC, KAFKA_ALERTS_TOPIC, setup_logging,
)

TOPICS = {
    KAFKA_RAW_TOPIC: {
        "num_partitions":     10,
        "replication_factor": 1,
        "config":             {"retention.ms": str(7 * 24 * 3600 * 1000)},
    },
    KAFKA_ALERTS_TOPIC: {
        "num_partitions":     1,
        "replication_factor": 1,
        "config":             {"retention.ms": str(30 * 24 * 3600 * 1000)},
    },
}

MAX_RETRIES = 10
RETRY_SLEEP = 5


def create_topics() -> None:
    """Create Kafka topics; retry if broker is not yet ready."""
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
    except ImportError as exc:
        raise ImportError(
            "kafka-python not installed: pip install kafka-python>=2.0.2"
        ) from exc

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER, client_id="topic_setup"
            )
            break
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not ready (attempt %d/%d) - waiting %ds ...",
                attempt, MAX_RETRIES, RETRY_SLEEP,
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_SLEEP)

    existing = set(admin.list_topics())
    to_create = [
        NewTopic(
            name=name,
            num_partitions=cfg["num_partitions"],
            replication_factor=cfg["replication_factor"],
            topic_configs=cfg.get("config", {}),
        )
        for name, cfg in TOPICS.items()
        if name not in existing
    ]

    if not to_create:
        logger.info("All topics already exist: %s", list(TOPICS))
        admin.close()
        return

    try:
        admin.create_topics(to_create, validate_only=False)
        for t in to_create:
            logger.info(
                "Created topic: %s (%d partitions)", t.name, t.num_partitions
            )
    except TopicAlreadyExistsError:
        logger.info("Topics already exist (race condition - OK).")
    finally:
        admin.close()


if __name__ == "__main__":
    setup_logging("topic_setup.log")
    create_topics()
    logger.info("Topic setup complete.")
