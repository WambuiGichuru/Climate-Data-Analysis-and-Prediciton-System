"""
data_partitioner.py
Author    : R02 - Ashley (Batch Processing Engineer)
Purpose   : Prints partition statistics for processed parquets and recommends
            an optimal partition count based on dataset size.
Milestone : M2 - Spark Batch Processing
"""
from __future__ import annotations
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from loguru import logger
from src.config import DATA_DIR, setup_logging
from src.batch.spark_session import get_spark_session

TARGETS = {
    "monthly_aggregates":     DATA_DIR / "processed" / "monthly_aggregates.parquet",
    "historical_onset_dates": DATA_DIR / "processed" / "historical_onset_dates.parquet",
}


def report_partitions(spark, label, path):
    if not path.exists():
        logger.warning("%s: not found at %s", label, path)
        print(f"  {label}: NOT FOUND at {path}")
        return
    df = spark.read.parquet(str(path))
    total  = df.count()
    n_part = df.rdd.getNumPartitions()
    recommended = max(1, total // 100_000)
    print(f"
Dataset : {label}")
    print(f"  Path              : {path}")
    print(f"  Total rows        : {total:,}")
    print(f"  Current partitions: {n_part}")
    print(f"  Recommended (100K rows/partition): {recommended}")
    logger.info("%s - rows=%d partitions=%d recommended=%d",
                label, total, n_part, recommended)


def main():
    setup_logging("partitioner.log")
    spark = get_spark_session("DataPartitioner")
    print("
Partition Statistics Report")
    print("=" * 50)
    for label, path in TARGETS.items():
        report_partitions(spark, label, path)
    print()


if __name__ == "__main__":
    main()
