import time
import logging
import pandas as pd
import numpy as np
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/scalability_benchmark.log"),
        logging.StreamHandler()
    ]
)

log = logging.getLogger(__name__)
Path("logs").mkdir(exist_ok=True)

# Data sizes to test
SIZES = [10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000]

def make_synthetic_ghcnd(n: int) -> pd.DataFrame:
    """Build a synthetic GHCN-like DataFrame with n rows."""
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "STATION": rng.choice(["USW00094728", "USW00023174", "USC00045721"], n),
        "DATE": pd.date_range("1960-01-01", periods=n, freq="s"),
        "TMAX": rng.normal(25, 10, n).round(1),
        "TMIN": rng.normal(15, 8, n).round(1),
        "PRCP": np.abs(rng.normal(0, 5, n)).round(1)
    })

def time_operation(fn, *args, repeats=3) -> float:
    """Run fn(*args) `repeats` times, return minimum elapsed seconds."""
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn(*args)
        times.append(time.perf_counter() - t0)
    return min(times)

def naive_filter(df: pd.DataFrame, station: str) -> pd.DataFrame:
    """O(n) — scans every row."""
    return df[df["STATION"] == station]

def indexed_filter(df: pd.DataFrame, station: str) -> pd.DataFrame:
    """O(log n) after index build — binary search on sorted index."""
    return df.set_index("STATION").loc[station]

def run_benchmark() -> pd.DataFrame:
    results = []
    log.info("Starting scalability benchmark...")

    for n in SIZES:
        log.info(f"Generating synthetic data: n={n}.")
        df = make_synthetic_ghcnd(n)

        t_naive = time_operation(naive_filter, df, "USW00094728")
        t_indexed = time_operation(indexed_filter, df, "USW00094728")

        speedup = t_naive / max(t_indexed, 1e-9)

        result = {
            "n": n,
            "naive_ms": round(t_naive * 1000, 3),
            "indexed_ms": round(t_indexed * 1000, 3),
            "speedup_x": round(speedup, 1)
        }
        results.append(result)
        log.info(
            f"n={n:>9,} | naive={result['naive_ms']:>8.3f}ms | "
            f"indexed={result['indexed_ms']:>8.3f}ms | speedup={speedup:.1f}x"
        )

    df_results = pd.DataFrame(results)
    df_results.to_csv("logs/scalability_benchmark.csv", index=False)
    log.info("Results saved to logs/scalability_benchmark.csv")
    return df_results

if __name__ == "__main__":
    results = run_benchmark()
    print("\n" + "="*65)
    print(f"{'n':>12} | {'naive (ms)':>12} | {'indexed (ms)':>12} | {'speedup (x)':>10}")
    print("-"*65)
    for _, r in results.iterrows():
        print(f"{int(r.n):>12,} | {r.naive_ms:>12.3f} | {r.indexed_ms:>12.3f} | {r.speedup_x:>10.1f}")
    print("="*65)
    print("Copy this table into Section 7 of the Milestone 1 report.")
