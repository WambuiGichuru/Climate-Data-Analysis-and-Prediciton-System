# Streaming Architecture — M3 Technical Document

**Author**: R03 — Alexander Kihoi (Streaming & Real-Time Engineer)  
**Milestone**: M3 — Streaming & Real-Time Systems  
**Project**: Climate Data Analysis & Prediction System (SDS 2412)  
**Topic**: Rainfall Onset Advisory for Kenyan Farmers (10 Counties)

---

## a. Event-Driven Architecture Diagram (ASCII)

```
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                    LAMBDA ARCHITECTURE — SPEED LAYER                    │
  └─────────────────────────────────────────────────────────────────────────┘

  OpenMeteo API
  (10 Kenya counties,
   hourly forecast)
        │
        │ HTTP GET /v1/forecast
        ▼
  ┌───────────────────┐
  │  kafka_producer   │  ← M2: polls API every 60 s, publishes JSON
  │  (Python process) │
  └─────────┬─────────┘
            │  JSON messages
            │  topic: raw-weather-stream
            ▼
  ┌───────────────────────────────────┐
  │           Apache Kafka            │
  │  Broker: localhost:9092           │
  │  ┌────────────────────────────┐   │
  │  │  raw-weather-stream        │   │  ← partitioned by county
  │  │  (input topic)             │   │
  │  └────────────────────────────┘   │
  │  ┌────────────────────────────┐   │
  │  │  onset-alerts              │   │  ← written by spark_consumer
  │  │  (output topic)            │   │
  │  └────────────────────────────┘   │
  └───────────────┬───────────────────┘
                  │  readStream (Kafka SQL connector)
                  ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                    spark_consumer.py                                  │
  │               (PySpark Structured Streaming)                          │
  │                                                                       │
  │   parse_stream()                                                      │
  │   ┌────────────────────────────────────────────────────────────────┐  │
  │   │  from_json(MESSAGE_SCHEMA)  → filter nulls → cast Timestamp   │  │
  │   └───────────────────────────┬────────────────────────────────────┘  │
  │                               │  clean event_df                       │
  │            ┌──────────────────┼──────────────────┐                    │
  │            ▼                  ▼                  ▼                    │
  │   ┌────────────────┐  ┌──────────────────┐  ┌──────────────────┐     │
  │   │  3-hr tumbling │  │  24-hr sliding   │  │  72-hr tumbling  │     │
  │   │  window        │  │  window (1h step)│  │  + foreachBatch  │     │
  │   │  (append mode) │  │  (append mode)   │  │  onset detection │     │
  │   └───────┬────────┘  └────────┬─────────┘  └────────┬─────────┘     │
  │           │                   │                      │                │
  │           ▼                   ▼                      │                │
  │   logs/streaming_output/  logs/streaming_output/     │                │
  │   3hr_tumbling/ (JSON)    24hr_sliding/ (JSON)       │                │
  │                                                      ├──► onset-alerts│
  │                                                      │    (Kafka)     │
  │                                                      │                │
  │                                                      ├──► onset_alerts│
  │                                                      │    / (JSON)    │
  │                                                      │                │
  │                                                      └──► CountMin-   │
  │                                                           Sketch      │
  │                                                           (driver)    │
  └───────────────────────────────────────────────────────────────────────┘
                  │                              │
                  ▼                              ▼
         logs/streaming_output/          Kafka onset-alerts
         (speed layer views)             (for downstream consumers)
                  │
                  ▼
         M6: Batch–Speed Merge
         (Lambda serving layer)
```

---

## b. Kafka Topic Schema

### Input Topic: `raw-weather-stream`

| Field | Type | Description | Example |
|---|---|---|---|
| `county` | `string` | Kenyan county name | `"Nairobi"` |
| `timestamp` | `string` | ISO-8601 hourly timestamp (Nairobi TZ) | `"2025-04-01T14:00"` |
| `precipitation` | `float` | Hourly rainfall (mm/hr) | `3.72` |
| `temperature` | `float` | Air temperature at 2 m (°C) | `21.5` |
| `soil_moisture` | `float` | Volumetric soil moisture 0–1 cm (m³/m³) | `0.312` |

**Example message (raw Kafka value bytes, UTF-8 JSON):**
```json
{
  "county": "Kisumu",
  "timestamp": "2025-04-01T08:00",
  "precipitation": 5.14,
  "temperature": 23.8,
  "soil_moisture": 0.287
}
```

**Partitioning**: Not explicitly keyed by the producer (`key=null`); Kafka assigns round-robin partition. For production, keying by county would co-locate all county events in one partition.

**Retention**: Default Kafka retention (7 days) — sufficient for the 72-hour onset window plus operational margin.

---

### Output Topic: `onset-alerts`

| Field | Type | Description | Example |
|---|---|---|---|
| `county` | `string` | County where onset was detected | `"Kisumu"` |
| `alert_timestamp` | `string` | Wall-clock time the alert was generated | `"2025-04-04T09:05:00"` |
| `cum_72hr_mm` | `float` | 72-hour cumulative precipitation (mm) | `23.41` |
| `onset_flag` | `boolean` | Always `true` (alerts are only emitted for onset) | `true` |

**Example alert message:**
```json
{
  "county": "Kisumu",
  "alert_timestamp": "2025-04-04T09:05:00",
  "cum_72hr_mm": 23.41,
  "onset_flag": true
}
```

---

## c. Window Definitions

### 3-Hour Tumbling Window

```
Time →   0h    3h    6h    9h   12h   15h
         |--W1--|--W2--|--W3--|--W4--|--W5--|

Each event belongs to EXACTLY ONE window.
Windows are non-overlapping and contiguous.
```

**PySpark definition:**
```python
window(col("event_time"), "3 hours")           # no slide argument = tumbling
```

**Use case:** Short-term rainfall accumulation for flash-flood early warning. Answers: "How much rain fell in this 3-hour block?"

**Watermark:** 30 minutes — late events accepted up to 30 min after window closes.

---

### 24-Hour Sliding Window (1-hour slide)

```
Time →   0h    1h    2h    3h    4h ...
         |--------W1 (24h)--------|
              |--------W2 (24h)--------|
                   |--------W3 (24h)--------|

Each event belongs to UP TO 24 overlapping windows (24h ÷ 1h).
```

**PySpark definition:**
```python
window(col("event_time"), "24 hours", "1 hour")   # slide = 1 hour
```

**Difference from tumbling:** A tumbling window resets at each boundary — an event in the 03:00–06:00 block contributes only to that block. A sliding window overlaps — the same event contributes to 24 consecutive windows, producing a continuously updated "last 24 hours" total that is re-evaluated every hour.

**Use case:** Daily rainfall advisory. Answers: "How much rain has fallen in the last 24 hours as of this moment?" — updated every hour as new data arrives.

**Watermark:** 2 hours — accommodates API polling delays and network lag.

---

### 72-Hour Tumbling Window (Onset Detection)

```
Time →   0h          72h         144h
         |-----W1-----|-----W2-----|

3-day blocks. Emitted only after the window closes (append mode).
```

**PySpark definition:**
```python
window(col("event_time"), "72 hours")             # tumbling, onset detection
```

**Watermark:** 2 hours.

---

## d. Onset Detection Algorithm

**Definition (Kenyan Meteorological Department standard):**  
Rainfall onset is declared when 72-hour cumulative precipitation ≥ 20 mm with no dry spell (zero-rainfall hour) in the following 48 hours.

### Pseudocode

```
ONSET_THRESHOLD = 20.0  # mm

function detect_onset(county_precips: list[float]) -> list[OnsetEvent]:
    """Sliding 72-hour window over hourly precipitation values."""

    prefix_sum  ← cumulative sum array (length n+1)
    prefix_zeros ← cumulative zero-hour count array (length n+1)

    for start in 0 .. (n - 72):
        end = start + 72
        cum_72hr = prefix_sum[end] - prefix_sum[start]    # O(1)
        zeros    = prefix_zeros[end] - prefix_zeros[start] # O(1)

        # Proxy: no dry hours within the observation window
        # (cannot look 48h ahead in a live stream)
        onset_flag = (cum_72hr >= ONSET_THRESHOLD) AND (zeros == 0)

        if onset_flag:
            emit OnsetEvent(county, cum_72hr, window_start=start, window_end=end)

    return alerts
```

### Streaming Proxy for the 48-Hour Follow-Up Check

The meteorological definition requires checking that no dry spell occurs in the **48 hours after** the 72-hour window. A live stream cannot look ahead. The proxy used here is:

> `min_precipitation > 0` within the current 72-hour window  
> ≡ No zero-rainfall hour was observed during the evaluation period

This is a conservative approximation: it confirms sustained rainfall within the known window, which is correlated with (though not identical to) absence of a dry spell afterward. The approximation is documented explicitly and its limitation is acknowledged in the M3 report.

### Spark Implementation

```python
# 72-hour windowed aggregation
windowed_72hr = (
    event_df
    .withWatermark("event_time", "2 hours")
    .groupBy(window(col("event_time"), "72 hours"), col("county"))
    .agg(
        spark_sum("precipitation").alias("cum_72hr_mm"),
        spark_min("precipitation").alias("min_precipitation"),   # dry-hour proxy
        spark_count("*").alias("event_count"),
    )
)

# Onset flag in foreachBatch
onset_flag = (cum_72hr_mm >= 20.0) AND (min_precipitation > 0.0)
```

---

## e. Count-Min Sketch Integration

### What It Is

A **Count-Min Sketch** (Cormode & Muthukrishnan, 2005) is a probabilistic, sub-linear-space data structure for estimating item frequencies in a stream. It uses a 2-D integer table `T[depth][width]` and `depth` independent hash functions.

### Error Guarantees

| Parameter | Value | Meaning |
|---|---|---|
| `width` | 1000 | Columns per hash row |
| `depth` | 5 | Independent hash functions |
| `seed` | 42 | RNG seed for reproducibility |
| ε (error factor) | e / 1000 ≈ 0.0027 | Max overcount = ε × N |
| δ (failure prob) | e⁻⁵ ≈ 0.007 | Probability bound is exceeded |

For N = 100,000 total events: max overcount per query ≤ **272 events** (0.27% of N). Failure probability: **< 0.7%**.

### Time Complexity

| Operation | Complexity |
|---|---|
| `update(county, count)` | O(depth) = **O(1)** |
| `query(county)` | O(depth) = **O(1)** |
| Space | O(width × depth) = O(5,000) integers |

Because `depth` is a fixed constant (5), both operations are truly constant-time regardless of stream length.

### Integration Point

```python
# In spark_consumer.py — foreachBatch callback (runs on Spark driver):

def process_batch(batch_df, batch_id):
    # toPandas() is safe: at most 10 rows (one per county)
    pdf = batch_df.select("county", "event_count").toPandas()
    for _, row in pdf.iterrows():
        _cms.update(row["county"], int(row["event_count"]))

    # O(1) per county — report top-5 by estimated frequency
    top5 = _cms.top_k(KENYA_COUNTIES, k=5)
    logger.info("CMS top-5: %s", top5)
```

The sketch lives as a **driver-side singleton** (`_cms` in `spark_consumer.py`). Because `foreachBatch` callbacks execute on the driver (not on distributed executors), the CMS state is consistent across all micro-batches without serialization overhead.

### Visualisation (after hypothetical 10,000 events)

```
County      Est. Frequency   True Rank
─────────── ─────────────── ──────────
Nairobi           1,024          1
Kisumu              987          2
Nakuru              956          3
Meru                891          4
Kitui               834          5
```

---

## f. Fault Tolerance Strategy

### Kafka Offset Replay

Structured Streaming tracks the last-consumed Kafka offset in the **checkpoint directory** (`logs/checkpoints/`). If `spark_consumer.py` crashes:

1. On restart, Spark reads the checkpoint to determine the last committed offset.
2. It re-requests all un-committed messages from Kafka (within the retention window).
3. Processing resumes exactly where it left off — **at-least-once delivery**.

```
logs/checkpoints/
├── 3hr_tumbling/         # offsets for the tumbling window query
├── 24hr_sliding/         # offsets for the sliding window query
└── onset/                # offsets for the onset detection query
```

**Key Kafka setting:** `failOnDataLoss=false` — allows the job to continue if Kafka deletes old offsets (e.g., after topic recreation during development). In production, set `failOnDataLoss=true` and increase Kafka retention.

### Structured Streaming Checkpointing

Each `writeStream` query has its own `checkpointLocation`. Spark stores:
- **Offsets file**: which Kafka offsets have been read.
- **Commits file**: which micro-batches have been successfully written to the sink.
- **State store**: windowed aggregation state (county × window buckets).

This enables recovery after driver restarts without data loss or duplicates (exactly-once for file sinks; at-least-once for Kafka sinks unless idempotent producers are enabled).

### Schema Evolution Handling

```python
# parse_stream() in spark_consumer.py:

# 1. from_json returns null for missing/wrong-type fields — no crash
parsed = value_str.select(from_json(col("json_str"), MESSAGE_SCHEMA).alias("d"))

# 2. Explicit null filter drops bad records and logs a warning
valid = parsed.filter(col("county").isNotNull() & col("timestamp").isNotNull())
```

| Scenario | Behaviour |
|---|---|
| New field added to producer | Ignored (not in schema) — no crash |
| Required field removed | Record dropped, WARNING logged |
| Field type changed (e.g., precip as string) | Cast to null → record dropped |
| Malformed JSON | Entire message → null struct → dropped |

---

## g. Latency Benchmark Results

> Run `python src/streaming/latency_benchmark.py` to populate this table.  
> Results are also saved to `logs/latency_benchmark.csv`.

| Scale | Throughput (msg/s) | Avg Latency (ms) | Peak / P99 Latency (ms) |
|---|---|---|---|
| 100 | 29,851 | 0.0327 | 0.1017 |
| 1,000 | 40,284 | 0.0243 | 0.0950 |
| 10,000 | 37,423 | 0.0263 | 0.0813 |
| 100,000 | 37,224 | 0.0264 | 0.0468 |

*Measured on Windows 11 / Python 3.13.  Results saved to `logs/latency_benchmark.csv`.*

**Methodology:**
- **Serialization time**: wall-clock time to `json.dumps()` each message individually.
- **Onset processing time**: time to deserialise + run the 72-hour sliding-window onset logic (prefix-sum optimised, O(n) per county).
- **End-to-end latency**: serialization + proportional share of onset processing.
- **Throughput**: N / total_seconds.
- **Peak latency**: 99th percentile (P99) of per-message end-to-end times.
- **No Kafka broker or Spark cluster required** — fully offline synthetic benchmark.

**Latency target**: < 5 seconds end-to-end (micro-batch trigger = 10 seconds). The benchmark demonstrates that pure Python processing at all four scales is well within this budget; the dominant latency contribution in a real deployment is Spark's micro-batch scheduling overhead (~2–4 s), not message processing.

---

## h. Batch vs. Streaming Comparison

| Dimension | Batch Layer (R01/R02) | Speed Layer (R03 — this milestone) |
|---|---|---|
| **Technology** | Apache Spark (batch), HDFS | PySpark Structured Streaming, Kafka |
| **Data source** | ERA5 historical NetCDF / GHCND CSVs | OpenMeteo live API (hourly) |
| **Latency** | Hours to days (scheduled jobs) | < 5 seconds (micro-batch) |
| **Throughput** | Tens of GB/run | ~10 messages/minute (10 counties × 1/hr) |
| **Window type** | Batch aggregation over full history | Tumbling (3h, 72h) + Sliding (24h) |
| **Onset detection** | Exact: full 72h + 48h lookahead | Approximate: 72h window, no-dry-hour proxy |
| **State management** | Stateless per-run | Stateful (Spark state store + Kafka offsets) |
| **Fault tolerance** | Idempotent re-runs from HDFS | Kafka offset replay + checkpointing |
| **Output** | Parquet files in HDFS | JSON in logs/streaming_output/ + Kafka alerts |
| **Role in Lambda** | Batch view (high accuracy, high latency) | Speed view (approximate, low latency) |
| **M6 merge** | Provides ground-truth historical onset dates | Provides real-time onset signals for correction |

**Lambda Architecture principle**: The batch layer produces high-accuracy results over the full historical dataset; the speed layer provides low-latency approximate results for recent data. In M6, the serving layer merges both: when the batch layer catches up to a time window, it supersedes the speed layer's approximation for that window.

---

*Document generated for SDS 2412 — Milestone M3, feature/r03-openmeteo branch.*  
*Last updated: 2026-05-09*
