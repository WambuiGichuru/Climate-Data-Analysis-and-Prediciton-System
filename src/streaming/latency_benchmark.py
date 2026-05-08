"""
latency_benchmark.py
Author    : R03 — Alexander Kihoi (Streaming & Real-Time Engineer)
Milestone : M3 — Streaming & Real-Time Systems
Purpose   : Standalone end-to-end latency and throughput benchmark for the
            streaming pipeline.

            Does NOT require a running Kafka broker or Spark cluster.
            Generates synthetic weather messages at four scales, times
            serialization and onset-detection processing, and saves
            results to logs/latency_benchmark.csv.

            Provides the empirical evidence for the latency and throughput
            evaluation required by M3.

Benchmark methodology:
  For each scale N ∈ {100, 1_000, 10_000, 100_000}:
    (a) Serialization   — JSON-encode each message individually, recording
                          per-message wall-clock time with time.perf_counter().
    (b) Onset detection — Deserialize all messages, then run the 72-hour
                          sliding-window onset logic (same algorithm as
                          spark_consumer.py) over the county data.
    (c) End-to-end      — Sum of (a) + (b) total time.
    (d) Throughput      — N / total_seconds  (messages per second).
    (e) Avg latency     — mean per-message time (ms).
    (f) Peak latency    — 99th-percentile per-message time (ms).

Run:
    python src/streaming/latency_benchmark.py
"""

import csv
import json
import logging
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# ── Logging ────────────────────────────────────────────────────────────────────
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
logger = logging.getLogger("latency_benchmark")

# ── Constants ──────────────────────────────────────────────────────────────────
KENYA_COUNTIES: list[str] = [
    "Nairobi", "Kisumu", "Nakuru", "Meru", "Kitui",
    "Garissa", "Machakos", "Eldoret", "Kakamega", "Embu",
]

# Scales to benchmark — matches the four datapoints required by M3.
BENCHMARK_SCALES: list[int] = [100, 1_000, 10_000, 100_000]

# Onset threshold: mirrors spark_consumer.ONSET_THRESHOLD_MM
ONSET_THRESHOLD_MM: float = 20.0

# Output CSV path
OUTPUT_CSV = LOG_DIR / "latency_benchmark.csv"

# Fixed RNG seed for reproducible synthetic data
_RNG = random.Random(42)

# Base timestamp for synthetic event sequence
_BASE_TIME = datetime(2025, 4, 1, 0, 0, 0)


# ── Synthetic message generation ───────────────────────────────────────────────

def _generate_message(county: str, hour_offset: int) -> dict:
    """
    Build one synthetic weather message matching the Kafka producer schema:
      { county, timestamp, precipitation, temperature, soil_moisture }

    Precipitation uses a log-normal distribution (mu=0.5, sigma=1.2) to
    mimic the right-skewed shape of real rainfall data — most hours have
    light rain, with occasional heavy events.  Values are clipped to ≥ 0.
    """
    ts     = _BASE_TIME + timedelta(hours=hour_offset)
    precip = max(0.0, round(_RNG.lognormvariate(0.5, 1.2), 2))
    return {
        "county":        county,
        "timestamp":     ts.isoformat() + "Z",
        "precipitation": precip,
        "temperature":   round(_RNG.uniform(15.0, 32.0), 1),
        "soil_moisture": round(_RNG.uniform(0.05, 0.45), 3),
    }


def generate_batch(n: int) -> list[dict]:
    """
    Generate exactly `n` synthetic messages distributed round-robin across
    the 10 Kenyan counties, with sequenced hourly timestamps.
    """
    return [
        _generate_message(KENYA_COUNTIES[i % len(KENYA_COUNTIES)], hour_offset=i)
        for i in range(n)
    ]


# ── Onset detection logic (mirrors spark_consumer.py, pure Python) ────────────

def check_onset(messages: list[dict]) -> list[dict]:
    """
    Run the 72-hour sliding-window onset detection algorithm in pure Python.

    This function mirrors the logic in spark_consumer.py's 72-hour tumbling
    window + foreachBatch, allowing it to be benchmarked without Spark.

    Algorithm:
      1. Group messages by county.
      2. For each county, build a list of hourly precipitation values.
      3. Slide a 72-element window over the list.
      4. For each window position:
           cum_72hr  = sum of precipitation in the window
           has_dry   = any hour in the window has precipitation == 0.0
           onset_flag = (cum_72hr >= 20.0) AND (NOT has_dry)
      5. Collect all windows where onset_flag is True.

    Optimisation: prefix sums reduce the per-window sum from O(72) to O(1),
    and a prefix zero-count reduces the dry-hour check from O(72) to O(1).
    Total complexity: O(n) per county (one pass for prefix arrays,
    one pass for the sliding window).
    """
    # Step 1: group precipitation values by county
    county_precip: dict[str, list[float]] = {}
    for msg in messages:
        county = msg.get("county", "UNKNOWN")
        precip = float(msg.get("precipitation") or 0.0)
        county_precip.setdefault(county, []).append(precip)

    alerts: list[dict] = []

    for county, precips in county_precip.items():
        n = len(precips)
        if n < 72:
            # Not enough data to form a 72-hour window — skip this county.
            continue

        # Step 2: Build prefix arrays for O(1) window queries
        #   prefix_sum[i]  = sum(precips[0 .. i-1])
        #   prefix_zeros[i] = count of zeros in precips[0 .. i-1]
        prefix_sum   = [0.0] * (n + 1)
        prefix_zeros = [0]   * (n + 1)
        for i, p in enumerate(precips):
            prefix_sum[i + 1]   = prefix_sum[i] + p
            prefix_zeros[i + 1] = prefix_zeros[i] + (1 if p == 0.0 else 0)

        # Step 3: Slide the 72-hour window
        for start in range(n - 71):   # inclusive start positions
            end = start + 72

            # O(1) range sum and zero count using prefix arrays
            cum_72hr       = prefix_sum[end]   - prefix_sum[start]
            zeros_in_window = prefix_zeros[end] - prefix_zeros[start]

            onset_flag = (cum_72hr >= ONSET_THRESHOLD_MM) and (zeros_in_window == 0)

            if onset_flag:
                alerts.append({
                    "county":       county,
                    "cum_72hr_mm":  round(cum_72hr, 2),
                    "onset_flag":   True,
                    "window_start": start,
                    "window_end":   end - 1,
                })

    return alerts


# ── Benchmark runner ──────────────────────────────────────────────────────────

def benchmark_scale(n: int) -> dict:
    """
    Run the full serialization + onset benchmark for a batch of `n` messages.

    Returns a result dict with keys:
      scale, throughput_mps, avg_latency_ms, peak_latency_ms,
      serialize_total_ms, onset_total_ms, alerts_detected
    """
    logger.info("  Scale = %d …", n)

    # Generate synthetic data (generation time is not counted in the benchmark)
    messages = generate_batch(n)

    # ── (a) Serialization benchmark ───────────────────────────────────────
    # Simulate the Kafka producer serialising each message to JSON bytes
    # before sending.  We time each message individually so we can compute
    # per-message statistics (avg, 99th percentile).
    per_msg_times_s: list[float] = []
    serialized: list[str] = []

    t_ser_start = time.perf_counter()
    for msg in messages:
        t0 = time.perf_counter()
        payload = json.dumps(msg)
        serialized.append(payload)
        per_msg_times_s.append(time.perf_counter() - t0)
    serialize_total_s = time.perf_counter() - t_ser_start

    # ── (b) Onset-detection benchmark ─────────────────────────────────────
    # Simulate the Spark consumer: deserialise JSON → run onset logic.
    # This is what happens inside foreachBatch on each micro-batch.
    t_onset_start = time.perf_counter()
    deserialized  = [json.loads(s) for s in serialized]
    alerts        = check_onset(deserialized)
    onset_total_s = time.perf_counter() - t_onset_start

    # ── (c) End-to-end statistics ─────────────────────────────────────────
    # Each message's end-to-end latency = its serialization time
    # + its proportional share of the onset processing time.
    onset_per_msg_s   = onset_total_s / n
    latencies_ms: list[float] = [
        (t_ser + onset_per_msg_s) * 1000.0
        for t_ser in per_msg_times_s
    ]

    e2e_total_s     = serialize_total_s + onset_total_s
    throughput_mps  = n / e2e_total_s if e2e_total_s > 0 else float("inf")
    avg_latency_ms  = sum(latencies_ms) / len(latencies_ms)

    # 99th-percentile peak latency
    sorted_lat      = sorted(latencies_ms)
    p99_idx         = min(int(0.99 * len(sorted_lat)), len(sorted_lat) - 1)
    peak_latency_ms = sorted_lat[p99_idx]

    result = {
        "scale":             n,
        "throughput_mps":    round(throughput_mps, 1),
        "avg_latency_ms":    round(avg_latency_ms, 4),
        "peak_latency_ms":   round(peak_latency_ms, 4),
        "serialize_total_ms": round(serialize_total_s * 1000, 2),
        "onset_total_ms":    round(onset_total_s * 1000, 2),
        "alerts_detected":   len(alerts),
    }

    logger.info(
        "    -> Throughput: %.0f msg/s  |  Avg: %.3f ms  |  P99: %.3f ms  "
        "|  Alerts: %d",
        throughput_mps, avg_latency_ms, peak_latency_ms, len(alerts),
    )
    return result


def run_all_benchmarks() -> list[dict]:
    """Execute benchmarks for all four scales and return the results list."""
    logger.info("Running latency benchmarks at %d scales ...", len(BENCHMARK_SCALES))
    results = []
    for scale in BENCHMARK_SCALES:
        results.append(benchmark_scale(scale))
    return results


# ── Output ─────────────────────────────────────────────────────────────────────

def save_csv(results: list[dict], path: Path) -> None:
    """Write benchmark results to a CSV file."""
    fieldnames = [
        "scale", "throughput_mps", "avg_latency_ms", "peak_latency_ms",
        "serialize_total_ms", "onset_total_ms", "alerts_detected",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    logger.info("Results saved -> %s", path)


def print_table(results: list[dict]) -> None:
    """Print a formatted results table to stdout."""
    # Column widths
    COL = {"scale": 10, "tput": 22, "avg": 20, "peak": 20}
    header = (
        f"{'Scale':>{COL['scale']}}  "
        f"{'Throughput (msg/s)':>{COL['tput']}}  "
        f"{'Avg Latency (ms)':>{COL['avg']}}  "
        f"{'Peak Latency (ms)':>{COL['peak']}}"
    )
    sep = "-" * len(header)

    print()
    print(sep)
    print("  LATENCY BENCHMARK - R03 M3 - Climate Onset Detector")
    print(f"  Run: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(sep)
    print(header)
    print(sep)
    for r in results:
        print(
            f"{r['scale']:>{COL['scale']},}  "
            f"{r['throughput_mps']:>{COL['tput']},.1f}  "
            f"{r['avg_latency_ms']:>{COL['avg']}.4f}  "
            f"{r['peak_latency_ms']:>{COL['peak']}.4f}"
        )
    print(sep)
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("Latency Benchmark - R03 M3 - Starting")
    logger.info("Scales: %s", BENCHMARK_SCALES)
    logger.info("Output: %s", OUTPUT_CSV)
    logger.info("=" * 60)

    results = run_all_benchmarks()
    print_table(results)
    save_csv(results, OUTPUT_CSV)

    logger.info("Benchmark complete.")


if __name__ == "__main__":
    main()
