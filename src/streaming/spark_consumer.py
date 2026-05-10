"""
spark_consumer.py
Author    : R03 — Alexander Kihoi (Streaming & Real-Time Engineer)
Milestone : M3 — Streaming & Real-Time Systems
Purpose   : PySpark Structured Streaming consumer for the Rainfall Onset
            Advisory system.

            Reads JSON weather events from Kafka topic 'raw-weather-stream',
            applies:
              • 3-hour  tumbling window  → cumulative rainfall per county
              • 24-hour sliding window  (1-hr slide) → rolling rainfall view
              • 72-hour tumbling window  → onset detection via foreachBatch

            Onset condition (Kenyan meteorological standard):
              72-hr cumulative precipitation ≥ 20 mm  AND
              no zero-precipitation hour within the observation window
              (proxy for the 48-hour follow-up dry-spell check, because
               a live stream cannot look ahead).

            Onset alerts are written back to Kafka topic 'onset-alerts'.
            All windowed aggregates are persisted to logs/streaming_output/
            for the M6 batch–speed layer merge.

            Count-Min Sketch (count_min_sketch.py) is updated every
            micro-batch via foreachBatch, tracking event frequency per
            county across the lifetime of the stream.

Schema evolution:  If a Kafka message is missing county or timestamp,
            the record is silently dropped (logged at WARNING level) rather
            than crashing the job.  Extra/unknown fields are ignored.

Run (preferred — spark-submit downloads Kafka connector automatically):
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        src/streaming/spark_consumer.py

Run (alternative — SparkSession sets packages, requires Maven in PATH):
    python src/streaming/spark_consumer.py

Target latency: < 5 seconds end-to-end (micro-batch trigger = 10 s).
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Make the streaming package importable when run via spark-submit ──────────
_REPO_ROOT = Path(__file__).resolve().parents[2]
_STREAMING_DIR = Path(__file__).resolve().parent
for _p in [str(_REPO_ROOT), str(_STREAMING_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── Logging (must be configured before PySpark imports to avoid Py4J noise) ──
LOG_DIR = _REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "streaming.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("spark_consumer")

# ── PySpark imports ────────────────────────────────────────────────────────────
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, from_json, to_json, struct,
        window,
        sum      as spark_sum,
        avg      as spark_avg,
        min      as spark_min,
        count    as spark_count,
        when, current_timestamp, lit,
    )
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, TimestampType,
    )
except ImportError as exc:
    logger.error("PySpark not found: %s\nInstall with: pip install pyspark>=3.5.0", exc)
    sys.exit(1)

# ── Count-Min Sketch (same package, driver-side singleton) ────────────────────
from count_min_sketch import CountMinSketch

# ── Output / checkpoint directories ──────────────────────────────────────────
STREAMING_OUTPUT = LOG_DIR / "streaming_output"
CHECKPOINT_BASE  = LOG_DIR / "checkpoints"

for _sub in ["3hr_tumbling", "24hr_sliding", "onset_alerts"]:
    (STREAMING_OUTPUT / _sub).mkdir(parents=True, exist_ok=True)
for _sub in ["3hr_tumbling", "24hr_sliding", "onset"]:
    (CHECKPOINT_BASE / _sub).mkdir(parents=True, exist_ok=True)

# ── Kafka config ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC     = "raw-weather-stream"
ALERT_TOPIC     = "onset-alerts"

# ── Firestore (speed-layer sink read by the dashboard API) ───────────────────
FIRESTORE_COLLECTION = "live_forecast"
FIRESTORE_TTL_DAYS   = 7

# ── Onset threshold ───────────────────────────────────────────────────────────
ONSET_THRESHOLD_MM = 20.0

# ── County names (used by CMS top_k display) ─────────────────────────────────
KENYA_COUNTIES = [
    "Nairobi", "Kisumu", "Nakuru", "Meru", "Kitui",
    "Garissa", "Machakos", "Eldoret", "Kakamega", "Embu",
]

# ── Driver-side Count-Min Sketch singleton ────────────────────────────────────
# Instantiated once on the driver; updated inside foreachBatch callbacks
# which also run on the driver (not on executors).
_cms = CountMinSketch(width=1000, depth=5, seed=42)

# ── Message schema ─────────────────────────────────────────────────────────────
# from_json() returns null for any field that is absent or wrong type,
# so schema evolution (added / removed fields) never crashes the job —
# we filter nulls out explicitly after parsing.
MESSAGE_SCHEMA = StructType([
    StructField("county",        StringType(), nullable=True),
    StructField("timestamp",     StringType(), nullable=True),  # ISO-8601 string
    StructField("precipitation", DoubleType(), nullable=True),
    StructField("temperature",   DoubleType(), nullable=True),
    StructField("soil_moisture", DoubleType(), nullable=True),
])


# ── SparkSession ──────────────────────────────────────────────────────────────

def build_spark_session() -> SparkSession:
    """
    Create (or retrieve) a SparkSession configured for Kafka Structured Streaming.

    spark.jars.packages instructs PySpark to download the Kafka SQL connector
    from Maven Central on first run (~30 s) and cache it for subsequent runs.
    Setting shuffle partitions to 4 keeps local-dev overhead low.
    """
    return (
        SparkSession.builder
        .appName("ClimateOnsetDetector_R03_M3")
        .config(
            "spark.jars.packages",
            # Connector version must match the Spark major.minor (3.5)
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        # Reduce default 200 shuffle partitions for local single-node use
        .config("spark.sql.shuffle.partitions", "4")
        # Reduce watermark delay tolerance for faster local demos
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )


# ── Stream parsing ────────────────────────────────────────────────────────────

def parse_stream(raw_df: DataFrame) -> DataFrame:
    """
    Decode raw Kafka bytes → clean, typed DataFrame.

    Pipeline:
      1. Cast Kafka `value` column (bytes) → UTF-8 string.
      2. Parse the JSON string using MESSAGE_SCHEMA.
         - Extra fields in the message are silently ignored.
         - Missing or type-mismatched fields become null.
      3. Drop rows where county OR timestamp is null.
         This is the schema-evolution guard: a producer that removes a
         critical field produces messages that are quietly discarded here
         with a driver-side WARNING log — the job keeps running.
      4. Cast the ISO-8601 timestamp string → TimestampType (event_time)
         for use in window() expressions.  Rows with unparseable timestamps
         are also dropped.

    Returns a streaming DataFrame with columns:
      county, timestamp (str), precipitation, temperature, soil_moisture,
      event_time (Timestamp)
    """
    # Step 1: Kafka value bytes → string
    value_str = raw_df.selectExpr("CAST(value AS STRING) AS json_str")

    # Step 2: JSON string → typed struct, then flatten
    parsed = value_str.select(
        from_json(col("json_str"), MESSAGE_SCHEMA).alias("d")
    ).select("d.*")

    # Step 3: drop schema-invalid records (schema evolution guard)
    valid = parsed.filter(
        col("county").isNotNull() & col("timestamp").isNotNull()
    )
    # Log a driver-side warning whenever the filter removes rows.
    # We cannot count() a streaming DF, so we log once at startup.
    logger.info(
        "parse_stream: records missing county/timestamp will be dropped "
        "(schema evolution guard active)."
    )

    # Step 4: timestamp string → TimestampType for windowing
    event_df = valid.withColumn(
        "event_time", col("timestamp").cast(TimestampType())
    ).filter(col("event_time").isNotNull())

    return event_df


# ── 3-hour tumbling window ────────────────────────────────────────────────────

def stream_3hr_tumbling(
    event_df: DataFrame,
    output_path: str,
    checkpoint_path: str,
):
    """
    3-hour non-overlapping (tumbling) window: cumulative rainfall per county.

    Window semantics:
      Tumbling window = fixed duration, no overlap.
      Each event belongs to exactly ONE window.
      Windows: [00:00–03:00), [03:00–06:00), [06:00–09:00), …

    Use case: short-term accumulation alarms — e.g. "did more than X mm
    fall in the last 3-hour block?"  Suitable for flash-flood early warning.

    Watermark: 30-minute late-data tolerance.  Events arriving up to
    30 minutes after their window closes are still included; later events
    are dropped to bound state size.
    """
    windowed = (
        event_df
        .withWatermark("event_time", "30 minutes")
        .groupBy(
            # "3 hours" = tumbling (no slide argument)
            window(col("event_time"), "3 hours"),
            col("county"),
        )
        .agg(
            spark_sum("precipitation").alias("cum_precipitation_mm"),
            spark_avg("temperature").alias("avg_temperature_c"),
            spark_avg("soil_moisture").alias("avg_soil_moisture"),
            spark_count("*").alias("event_count"),
        )
        # Flatten the nested window struct into flat columns for JSON output
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
        .drop("window")
    )

    query = (
        windowed.writeStream
        # "append" emits a row only after the window is finalised (past watermark)
        .outputMode("append")
        .format("json")
        .option("path",              output_path)
        .option("checkpointLocation", checkpoint_path)
        # Process new data every 10 seconds; contributes to < 5 s latency goal
        .trigger(processingTime="10 seconds")
        .queryName("q_3hr_tumbling")
        .start()
    )
    logger.info("Stream started: 3-hr tumbling  →  %s", output_path)
    return query


# ── 24-hour sliding window ────────────────────────────────────────────────────

def stream_24hr_sliding(
    event_df: DataFrame,
    output_path: str,
    checkpoint_path: str,
):
    """
    24-hour sliding window (slide = 1 hour): rolling rainfall view per county.

    Window semantics:
      Sliding window = fixed duration with a step smaller than the duration.
      Each event can belong to up to (24h / 1h) = 24 overlapping windows.
      Windows: [00:00–24:00), [01:00–25:00), [02:00–26:00), …

    Difference from tumbling:
      Tumbling windows are disjoint — an event in the 03:00–06:00 block
      counts only once.  Sliding windows overlap — the same event counts
      in 24 consecutive windows, producing a continuously updated
      "last 24 hours" rolling total that is re-evaluated every hour.

    Use case: daily rainfall advisory — farmers need a rolling 24-hour
    total, not just the current 3-hour block.

    Watermark: 2 hours (allows for delayed API responses / network lag).
    """
    windowed = (
        event_df
        .withWatermark("event_time", "2 hours")
        .groupBy(
            # "24 hours" window duration, "1 hour" slide interval
            window(col("event_time"), "24 hours", "1 hour"),
            col("county"),
        )
        .agg(
            spark_sum("precipitation").alias("cum_precipitation_mm"),
            spark_avg("temperature").alias("avg_temperature_c"),
            spark_count("*").alias("event_count"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
        .drop("window")
    )

    query = (
        windowed.writeStream
        .outputMode("append")
        .format("json")
        .option("path",              output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .queryName("q_24hr_sliding")
        .start()
    )
    logger.info("Stream started: 24-hr sliding   →  %s", output_path)
    return query


# ── Firestore sink helpers ───────────────────────────────────────────────────

_FIRESTORE_CLIENT = None


def _get_firestore_client(firestore_module):
    """Lazy-build a single Firestore client per driver process.

    A SparkSession outlives many micro-batches; constructing a fresh
    client on every batch would leak gRPC channels. Caching one
    module-level client keeps connection overhead constant.
    """
    global _FIRESTORE_CLIENT
    if _FIRESTORE_CLIENT is None:
        _FIRESTORE_CLIENT = firestore_module.Client()
    return _FIRESTORE_CLIENT


def _publish_alerts_to_firestore(alerts_df: DataFrame) -> None:
    """Mirror onset alerts into Firestore collection 'live_forecast'.

    Why a parallel sink alongside Kafka:
      The dashboard API reads from Firestore — it is the speed-layer
      view served to end users. Kafka remains the durable, replayable
      event stream consumed by other systems.

    TTL:
      Each document carries 'expires_at = now + 7 days'. A Firestore
      TTL policy (configured manually per
      infrastructure/gcp/setup_firestore_ttl.md) deletes documents
      whose expires_at has passed, capping per-county history at one
      week and bounding storage cost.

    Doc id:
      Uses the county name as the document id so the most recent
      alert per county overwrites the previous one — the dashboard
      always sees the freshest value without needing a query.

    Failure semantics:
      Firestore errors are caught and logged. A Firestore outage
      must not kill the Spark streaming job — Kafka has already
      received the alert in this same batch, so the canonical event
      is preserved.
    """
    try:
        from google.cloud import firestore   # optional dep — lazy import
    except ImportError:
        logger.warning(
            "google-cloud-firestore not installed; skipping Firestore sink. "
            "Install with: pip install google-cloud-firestore"
        )
        return

    try:
        client     = _get_firestore_client(firestore)
        collection = client.collection(FIRESTORE_COLLECTION)
        now        = datetime.now(timezone.utc)
        expires_at = now + timedelta(days=FIRESTORE_TTL_DAYS)

        # alerts_df has at most ~10 rows (one per county) → toPandas safe
        pdf = alerts_df.select(
            "county", "alert_timestamp", "cum_72hr_mm", "onset_flag"
        ).toPandas()

        for _, row in pdf.iterrows():
            doc = {
                "county":          str(row["county"]),
                "alert_timestamp": row["alert_timestamp"],
                "cum_72hr_mm":     float(row["cum_72hr_mm"]),
                "onset_flag":      bool(row["onset_flag"]),
                "expires_at":      expires_at,
                "ingested_at":     now,
            }
            collection.document(str(row["county"])).set(doc)

        logger.info(
            "Firestore sink: wrote %d doc(s) to '%s' (expires_at=%s).",
            len(pdf), FIRESTORE_COLLECTION, expires_at.isoformat(),
        )
    except Exception as exc:
        logger.warning("Firestore sink failed (Kafka was unaffected): %s", exc)


# ── 72-hour onset detection (foreachBatch) ────────────────────────────────────

def _make_onset_processor(spark: SparkSession):
    """
    Factory that returns a foreachBatch callback for onset detection.

    Why foreachBatch instead of a pure streaming sink?
      1. We need to write to *two* sinks (Kafka + disk) from one aggregation.
      2. The onset flag involves conditional logic (if/else) that is easier
         to express in Python than in a streaming DSL.
      3. The CMS update is a Python object mutation — it cannot run on
         executors; foreachBatch runs on the driver, where _cms lives.

    Onset condition implemented here:
      onset_flag = (cum_72hr_mm >= 20.0) AND (min_precipitation > 0.0)

      The second condition is a proxy for the meteorological requirement
      "no zero-rainfall hour in the 48 hours following the 72-hour window".
      Because the stream cannot look ahead, we use min_precipitation > 0
      within the current 72-hour window as evidence that rainfall was
      sustained throughout (no dry hours detected).  This is explicitly
      documented as a streaming-proxy approximation in the project spec.
    """

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Called by Structured Streaming after each micro-batch completes."""

        if batch_df.isEmpty():
            logger.debug("Onset batch %d: empty — no work.", batch_id)
            return

        row_count = batch_df.count()
        logger.info("Onset batch %d: %d county-window rows.", batch_id, row_count)

        # ── Apply onset flag ──────────────────────────────────────────────
        flagged = batch_df.withColumn(
            "onset_flag",
            when(
                (col("cum_72hr_mm") >= ONSET_THRESHOLD_MM) &
                (col("min_precipitation") > 0.0),   # no dry-hour proxy
                True,
            ).otherwise(False),
        ).withColumn(
            "alert_timestamp", current_timestamp(),
        )

        # ── Publish onset alerts to Kafka ─────────────────────────────────
        alerts = flagged.filter(col("onset_flag") == True)
        if not alerts.isEmpty():
            alert_count = alerts.count()
            logger.info(
                "Batch %d: %d ONSET ALERT(S) detected — publishing to '%s'.",
                batch_id, alert_count, ALERT_TOPIC,
            )
            (
                alerts
                .select(
                    # Kafka requires a 'value' column (bytes / string)
                    to_json(struct(
                        col("county"),
                        col("alert_timestamp"),
                        col("cum_72hr_mm"),
                        col("onset_flag"),
                    )).alias("value")
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", ALERT_TOPIC)
                .save()
            )

            # Parallel sink: mirror the same alerts into Firestore so the
            # dashboard API can read them. See _publish_alerts_to_firestore
            # for the full rationale (TTL, failure semantics, doc id).
            _publish_alerts_to_firestore(alerts)
        else:
            logger.info("Batch %d: no onset conditions met.", batch_id)

        # ── Persist 72-hr aggregates to disk (for M6 batch-speed merge) ──
        # Writes all rows (onset_flag True and False) so the batch layer
        # can perform its own offline analysis on the full history.
        (
            flagged.write
            .mode("append")
            .json(str(STREAMING_OUTPUT / "onset_alerts"))
        )

        # ── Update Count-Min Sketch with county event counts ──────────────
        # The 72-hr aggregate has at most 10 rows (one per county), so
        # toPandas() is safe — it's a tiny DataFrame on the driver.
        try:
            pdf = batch_df.select("county", "event_count").toPandas()
            for _, row in pdf.iterrows():
                _cms.update(str(row["county"]), int(row["event_count"]))

            # Report top counties by CMS-estimated stream frequency
            top5 = _cms.top_k(KENYA_COUNTIES, k=5)
            logger.info(
                "CMS top-5 (batch %d, total events tracked=%d): %s",
                batch_id,
                _cms.total_count,
                ", ".join(f"{c}={f}" for c, f in top5),
            )
        except Exception as exc:
            logger.warning("CMS update failed in batch %d: %s", batch_id, exc)

    return process_batch


def stream_onset_detection(
    event_df: DataFrame,
    checkpoint_path: str,
    spark: SparkSession,
):
    """
    72-hour tumbling window per county → foreachBatch onset detection.

    Aggregates per window:
      cum_72hr_mm       : total precipitation (mm) — onset threshold check
      min_precipitation : minimum hourly value — zero = dry hour detected
      event_count       : number of hourly records received in this window

    The foreachBatch callback (built by _make_onset_processor) then:
      a) Applies the onset flag condition.
      b) Publishes alerts to Kafka 'onset-alerts' topic.
      c) Writes all aggregates to disk for M6.
      d) Updates the driver-side Count-Min Sketch.
    """
    windowed_72hr = (
        event_df
        .withWatermark("event_time", "2 hours")
        .groupBy(
            window(col("event_time"), "72 hours"),   # 3-day tumbling window
            col("county"),
        )
        .agg(
            spark_sum("precipitation").alias("cum_72hr_mm"),
            spark_min("precipitation").alias("min_precipitation"),
            spark_count("*").alias("event_count"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
        .drop("window")
    )

    query = (
        windowed_72hr.writeStream
        .outputMode("append")
        .foreachBatch(_make_onset_processor(spark))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .queryName("q_onset_72hr")
        .start()
    )
    logger.info("Stream started: 72-hr onset detection (foreachBatch).")
    return query


# ── Main entry point ──────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("Climate Onset Detector — R03 M3 — Startup")
    logger.info("  Kafka broker  : %s", KAFKA_BOOTSTRAP)
    logger.info("  Input topic   : %s", INPUT_TOPIC)
    logger.info("  Alert topic   : %s", ALERT_TOPIC)
    logger.info("  Onset threshold: %.1f mm / 72 hr", ONSET_THRESHOLD_MM)
    logger.info("  Output dir    : %s", STREAMING_OUTPUT)
    logger.info("=" * 60)

    # Build SparkSession (downloads Kafka connector on first run)
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")   # suppress Spark's verbose INFO logs

    # ── Read raw Kafka stream ─────────────────────────────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        # "latest" — only process messages that arrive after this job starts.
        # Change to "earliest" to replay historical messages for backfill.
        .option("startingOffsets", "latest")
        # Don't fail if the topic is deleted/recreated during dev/testing
        .option("failOnDataLoss", "false")
        .load()
    )
    logger.info("Kafka readStream connected to topic '%s'.", INPUT_TOPIC)

    # ── Parse JSON → typed DataFrame ──────────────────────────────────────
    event_df = parse_stream(raw_df)

    # ── Launch three concurrent streaming queries ──────────────────────────
    q1 = stream_3hr_tumbling(
        event_df,
        output_path=str(STREAMING_OUTPUT / "3hr_tumbling"),
        checkpoint_path=str(CHECKPOINT_BASE / "3hr_tumbling"),
    )

    q2 = stream_24hr_sliding(
        event_df,
        output_path=str(STREAMING_OUTPUT / "24hr_sliding"),
        checkpoint_path=str(CHECKPOINT_BASE / "24hr_sliding"),
    )

    q3 = stream_onset_detection(
        event_df,
        checkpoint_path=str(CHECKPOINT_BASE / "onset"),
        spark=spark,
    )

    logger.info("All 3 streaming queries active.  Press Ctrl-C to stop.")

    try:
        # Block until any query fails or is manually stopped
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received — stopping all queries.")
        for q in [q1, q2, q3]:
            try:
                q.stop()
            except Exception as exc:
                logger.warning("Error stopping query '%s': %s", q.name, exc)
    finally:
        spark.stop()
        logger.info("SparkSession closed.  CMS state at shutdown: %s", _cms)


if __name__ == "__main__":
    main()
