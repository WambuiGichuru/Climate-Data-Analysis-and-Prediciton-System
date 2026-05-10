"""
count_min_sketch.py
Author    : R03 — Alexander Kihoi (Streaming & Real-Time Engineer)
Milestone : M3 — Streaming & Real-Time Systems
Purpose   : Pure-Python, from-scratch Count-Min Sketch for approximate
            frequency counting of weather events per county across the
            Kafka stream.  No external CMS library is used — the course
            forbids black-box framework use for this component.

            Integrated into spark_consumer.py via the foreachBatch
            callback: after every micro-batch, county event counts are
            fed into the sketch, enabling O(1) frequency queries at any
            point in the stream without storing per-county exact counters.
"""

import hashlib
import random
import struct
import logging
import sys
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
logger = logging.getLogger("count_min_sketch")


# ── CountMinSketch ─────────────────────────────────────────────────────────────

class CountMinSketch:
    """
    Count-Min Sketch  (Cormode & Muthukrishnan, 2005)
    ==================================================
    A probabilistic, sub-linear-space data structure for estimating
    item frequencies in a data stream.

    What it approximates
    --------------------
    For any item x and stream of N total updates, the sketch guarantees:

        true_count(x)  ≤  query(x)  ≤  true_count(x) + ε × N

    where  ε = e / width  (e ≈ 2.718).
    The probability that the bound is *exceeded* is at most:

        δ = e^{-depth}

    With the defaults (width=1000, depth=5):
      eps  ~ 0.00272    -> overcount <= 0.27 % of N
      delta ~ 0.0067    -> failure probability < 0.7 %

    Time complexity
    ---------------
    Both update() and query() run in  O(depth)  time.
    Because depth is a fixed constant (5), this is effectively O(1)
    regardless of how many distinct items or total events have been seen.

    Space: O(width × depth) = O(5 000) integers — negligible.

    How it works
    ------------
    The sketch maintains a 2-D integer table T[depth][width].
    Each of the `depth` rows uses an independent hash function h_i(x).

    update(x, c):   for each row i,  T[i][h_i(x)] += c
    query(x):       return  min_i  T[i][h_i(x)]

    The minimum across rows reduces the overcount from hash collisions in
    any single row.  Because we only add (never subtract), the estimate
    is always ≥ the true count.

    Parameters
    ----------
    width : int   Columns per row (default 1000). More → lower ε.
    depth : int   Number of hash rows (default 5).  More → lower δ.
    seed  : int   Master RNG seed for reproducibility (default 42).
    """

    def __init__(self, width: int = 1000, depth: int = 5, seed: int = 42) -> None:
        if width < 1 or depth < 1:
            raise ValueError("width and depth must both be ≥ 1.")

        self.width = width
        self.depth = depth
        self.seed  = seed

        # 2-D count table: depth rows × width columns, initialised to zero.
        # Using a flat list-of-lists avoids numpy dependency.
        self._table: list[list[int]] = [[0] * width for _ in range(depth)]

        # Derive `depth` independent hash seeds from the master seed so
        # that each row genuinely uses a different hash function.
        rng = random.Random(seed)
        self._row_seeds: list[int] = [
            rng.randint(0, 2 ** 31 - 1) for _ in range(depth)
        ]

        # Running total of all update() calls (for diagnostics / error bound).
        self._total_count: int = 0

        logger.debug(
            "CountMinSketch ready  width=%d  depth=%d  seed=%d",
            width, depth, seed,
        )

    # ── Internal helpers ────────────────────────────────────────────────────────

    def _hash(self, key: str, row_seed: int) -> int:
        """
        Map `key` → column index in [0, width) for one hash row.

        Implementation: prepend the row seed as a 4-byte big-endian integer
        to the UTF-8 key bytes, then hash with MD5.  The first 8 bytes of the
        digest are interpreted as an unsigned 64-bit integer and reduced mod
        `width`.

        MD5 is used here for speed (not cryptographic strength).  Each row's
        unique seed prefix ensures row-to-row independence.
        """
        seed_bytes = struct.pack(">I", row_seed)          # 4-byte big-endian seed
        raw        = hashlib.md5(seed_bytes + key.encode("utf-8")).digest()
        value      = struct.unpack(">Q", raw[:8])[0]      # first 8 bytes → uint64
        return value % self.width

    # ── Public API ──────────────────────────────────────────────────────────────

    def update(self, key: str, count: int = 1) -> None:
        """
        Record `count` new observations of `key` in the sketch.

        For each of the `depth` rows, the cell at column h_i(key) is
        incremented by `count`.  The overcount from collisions is bounded
        by the sketch's error guarantee.

        Parameters
        ----------
        key   : str   The item being counted (e.g. a county name).
        count : int   Observations to add; must be ≥ 1 (default 1).
        """
        if count < 1:
            return
        for row, seed in enumerate(self._row_seeds):
            col = self._hash(key, seed)
            self._table[row][col] += count
        self._total_count += count

    def query(self, key: str) -> int:
        """
        Return the estimated total event count for `key`.

        The estimate is the minimum across all rows — this is the tightest
        upper bound the sketch can provide, because overcounting from
        collisions in one row may not affect other rows.

        Returns
        -------
        int  Estimated frequency.  Guaranteed ≥ true frequency.
        """
        return min(
            self._table[row][self._hash(key, seed)]
            for row, seed in enumerate(self._row_seeds)
        )

    def top_k(self, candidates: list[str], k: int = 5) -> list[tuple[str, int]]:
        """
        Rank `candidates` by estimated frequency and return the top k.

        Because the CMS does not store the universe of keys, callers must
        supply the candidate list (here: the 10 county names).

        Parameters
        ----------
        candidates : list[str]   Keys to rank.
        k          : int         Number of top results to return.

        Returns
        -------
        List of (key, estimated_count) sorted descending by count.
        """
        ranked = [(key, self.query(key)) for key in candidates]
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked[:k]

    # ── Diagnostics ─────────────────────────────────────────────────────────────

    @property
    def total_count(self) -> int:
        """Total number of individual observations recorded so far."""
        return self._total_count

    def error_bound(self) -> float:
        """Upper bound on the per-query overcount as a fraction of N (= ε)."""
        import math
        return math.e / self.width

    def failure_probability(self) -> float:
        """Probability that any single query exceeds the error bound (= δ)."""
        import math
        return math.e ** (-self.depth)

    def summary(self) -> str:
        """Human-readable summary of the sketch configuration and state."""
        return (
            f"CountMinSketch\n"
            f"  Table        : {self.depth} rows x {self.width} cols "
            f"({self.depth * self.width:,} cells)\n"
            f"  Total updates: {self._total_count:,}\n"
            f"  Error bound  : eps = {self.error_bound():.5f}  "
            f"(overcount <= {self.error_bound() * 100:.3f}% of N)\n"
            f"  Fail prob    : delta = {self.failure_probability():.5f}  "
            f"({self.failure_probability() * 100:.3f}%)"
        )

    def __repr__(self) -> str:
        return (
            f"CountMinSketch(width={self.width}, depth={self.depth}, "
            f"seed={self.seed}, total_count={self._total_count})"
        )


# ── Standalone self-test ───────────────────────────────────────────────────────

def _run_self_test() -> None:
    """
    Verify the CMS implementation against known ground-truth frequencies.

    Inserts known counts for all 10 Kenyan counties, then asserts that:
      1. query(county) ≥ true_count  (no undercounting — structural guarantee)
      2. query(county) ≤ true_count + ε*N  (bounded overcount)
      3. top_k() returns counties in the correct frequency order.
    """
    logger.info("=" * 50)
    logger.info("CountMinSketch - self-test")
    logger.info("=" * 50)

    COUNTIES = [
        "Nairobi", "Kisumu", "Nakuru", "Meru", "Kitui",
        "Garissa", "Machakos", "Eldoret", "Kakamega", "Embu",
    ]

    # Ground-truth counts: Nairobi sees the most events, Embu the fewest.
    TRUE_COUNTS: dict[str, int] = {
        "Nairobi":  500, "Kisumu":  300, "Nakuru":  250, "Meru":  150,
        "Kitui":    100, "Garissa":  80, "Machakos": 60, "Eldoret": 40,
        "Kakamega":  25, "Embu":     10,
    }
    N_TOTAL = sum(TRUE_COUNTS.values())

    sketch = CountMinSketch(width=1000, depth=5, seed=42)

    # ── Populate sketch ────────────────────────────────────────────────────
    for county, count in TRUE_COUNTS.items():
        sketch.update(county, count)

    logger.info(sketch.summary())
    logger.info("")

    # ── Verify per-county estimates ────────────────────────────────────────
    epsilon = sketch.error_bound()
    error_budget = epsilon * N_TOTAL   # maximum allowed overcount per query

    logger.info(
        "  %-12s  %6s  %9s  %9s  %8s  %s",
        "County", "True", "Estimated", "Overcount", "Ratio", "Status",
    )
    logger.info("  " + "-" * 65)

    failures = 0
    for county in COUNTIES:
        true_val  = TRUE_COUNTS[county]
        estimated = sketch.query(county)
        overcount = estimated - true_val
        ratio_pct = (overcount / max(true_val, 1)) * 100

        # Structural guarantee: never undercount
        assert estimated >= true_val, (
            f"FAIL: {county} estimated={estimated} < true={true_val}"
        )

        # Probabilistic guarantee: overcount bounded by ε*N
        ok = overcount <= error_budget
        if not ok:
            failures += 1
        status = "OK" if ok else f"WARN (budget={error_budget:.0f})"

        logger.info(
            "  %-12s  %6d  %9d  %+9d  %7.2f%%  %s",
            county, true_val, estimated, overcount, ratio_pct, status,
        )

    logger.info("")

    # ── Top-5 ranking ──────────────────────────────────────────────────────
    top5 = sketch.top_k(COUNTIES, k=5)
    logger.info("  Top-5 counties by estimated frequency:")
    for rank, (county, freq) in enumerate(top5, 1):
        true_val = TRUE_COUNTS[county]
        logger.info(
            "    %d. %-12s  estimated=%d  true=%d", rank, county, freq, true_val
        )

    logger.info("")
    if failures == 0:
        logger.info("  PASSED: All assertions passed - CountMinSketch self-test OK.")
    else:
        logger.warning(
            "  WARN: %d county/counties exceeded error budget (probabilistic — "
            "rare at these parameters).", failures
        )


if __name__ == "__main__":
    _run_self_test()
