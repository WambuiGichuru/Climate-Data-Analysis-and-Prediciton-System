"""
scalability_analysis.py
Author    : R02 - Ashley (Batch Processing Engineer)
Purpose   : Measures aggregation wall-clock time at 5 scales (1K to 1M rows).
            Saves text table to docs/scalability_analysis.txt.
            This is the Big-O / M2 scalability evidence.
Milestone : M2 - Scalability Analysis
"""
from __future__ import annotations
import sys, time, random, datetime
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from loguru import logger
from src.config import DOCS_DIR, setup_logging
from src.batch.spark_session import get_spark_session

SCALES = [1_000, 10_000, 100_000, 500_000, 1_000_000]
OUT_FILE = DOCS_DIR / "scalability_analysis.txt"


def _make_data(spark, n):
    import pandas as pd
    rng = random.Random(42)
    stations = [f"KE{str(i).zfill(8)}" for i in range(1, 11)]
    start = datetime.date(2000, 1, 1)
    date_range = (datetime.date(2023, 12, 31) - start).days
    rows = [{
        "station_id": rng.choice(stations),
        "date": str(start + datetime.timedelta(days=rng.randint(0, date_range))),
        "element": rng.choice(["PRCP", "TMAX", "TMIN"]),
        "value": round(abs(rng.gauss(3, 4)), 2),
        "quality_flag": None,
    } for _ in range(n)]
    pdf = pd.DataFrame(rows)
    pdf["date"] = pd.to_datetime(pdf["date"])
    return spark.createDataFrame(pdf)


def _run_agg(spark, df):
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col
    df2 = df.withColumn("year", F.year("date")).withColumn("month", F.month("date"))
    return (df2.filter(col("element") == "PRCP")
               .groupBy("station_id", "year", "month")
               .agg(F.sum("value"))
               .count())


def main():
    setup_logging("scalability.log")
    spark = get_spark_session("ScalabilityAnalysis")
    header = f"{'Scale':>12}  {'Time(s)':>8}  {'Rows/s':>14}  Notes"
    sep    = "-" * 60
    lines  = [header, sep]
    print()
    print("SCALABILITY ANALYSIS - Spark Precipitation Aggregation")
    print(sep)
    print(header)
    print(sep)
    for n in SCALES:
        logger.info("Running scale: %d", n)
        df = _make_data(spark, n)
        t0 = time.time()
        cnt = _run_agg(spark, df)
        elapsed = time.time() - t0
        rps = n / elapsed if elapsed > 0 else 0
        line = f"{n:>12,}  {elapsed:>8.2f}  {rps:>14,.0f}  output_groups={cnt}"
        lines.append(line)
        print(line)
    print(sep)
    lines.append(sep)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w") as fh:
        fh.write("
".join(lines) + "
")
    logger.info("Scalability results saved -> %s", OUT_FILE)


if __name__ == "__main__":
    main()
