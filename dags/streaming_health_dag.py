"""
streaming_health_dag.py
Author    : R05 - Faith (DevOps & Deployment Engineer)
Purpose   : Airflow DAG that checks Kafka consumer group lag every 15 minutes.
            Logs CRITICAL if lag > 1000 messages.
Milestone : M5 - Stream Monitoring
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

DEFAULT_ARGS = {
    "owner":            "r05-faith",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

LAG_CRITICAL_THRESHOLD = 1000


def _check_consumer_lag(**kwargs) -> None:
    """
    Check consumer group lag on raw-weather-stream.
    Logs CRITICAL if lag > 1000.
    """
    from loguru import logger

    try:
        from kafka.admin import KafkaAdminClient
        from kafka import KafkaConsumer, TopicPartition
        from src.config import KAFKA_BROKER, KAFKA_RAW_TOPIC
    except ImportError:
        logger.warning("kafka-python not installed — skipping lag check.")
        return

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            group_id="spark-streaming-consumer",
            enable_auto_commit=False,
        )
        partitions = consumer.partitions_for_topic(KAFKA_RAW_TOPIC)
        if not partitions:
            logger.warning("No partitions found for topic: %s", KAFKA_RAW_TOPIC)
            consumer.close()
            return

        tps = [TopicPartition(KAFKA_RAW_TOPIC, p) for p in partitions]
        consumer.assign(tps)
        consumer.seek_to_end(*tps)
        end_offsets = {tp: consumer.position(tp) for tp in tps}
        committed = {tp: (consumer.committed(tp) or 0) for tp in tps}

        total_lag = sum(end_offsets[tp] - committed[tp] for tp in tps)
        consumer.close()

        if total_lag > LAG_CRITICAL_THRESHOLD:
            logger.critical(
                "CONSUMER LAG CRITICAL: %d messages behind on %s",
                total_lag, KAFKA_RAW_TOPIC,
            )
        else:
            logger.info("Consumer lag: %d (healthy)", total_lag)

        kwargs["ti"].xcom_push(key="consumer_lag", value=total_lag)

    except Exception as exc:
        logger.warning("Lag check failed: %s", exc)


with DAG(
    dag_id="streaming_health_check",
    default_args=DEFAULT_ARGS,
    description="Check Kafka consumer group lag every 15 minutes",
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["streaming", "monitoring", "sds2412"],
) as dag:

    check_lag = PythonOperator(
        task_id="check_consumer_lag",
        python_callable=_check_consumer_lag,
    )
