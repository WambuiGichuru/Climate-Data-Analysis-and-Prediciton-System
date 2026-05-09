"""
noaa_spark_processor.py
Author    : R02 - Ashley (Batch Processing Engineer)
Purpose   : PySpark batch job: monthly aggregates + WMO onset dates.
            Generates 50000 synthetic rows when NOAA parquet is absent.
Milestone : M2 - Spark Batch Processing
"""
from __future__ import annotations
import sys, time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from loguru import logger
from src.config import DATA_DIR, setup_logging
from src.batch.spark_session import get_spark_session

RAW_NOAA    = DATA_DIR / "raw"       / "noaa_ghcnd_kenya.parquet"
OUT_MONTHLY = DATA_DIR / "processed" / "monthly_aggregates.parquet"
OUT_ONSET   = DATA_DIR / "processed" / "historical_onset_dates.parquet"


def _generate_synthetic(spark, n=50_000):
    import random, datetime, pandas as pd
    rng = random.Random(42)
    stations = [f"KE{str(i).zfill(8)}" for i in range(1, 11)]
    elements = ["PRCP", "TMAX", "TMIN"]
    start = datetime.date(2000, 1, 1)
    date_range = (datetime.date(2023, 12, 31) - start).days
    rows = []
    for _ in range(n):
        sid = rng.choice(stations)
        d   = start + datetime.timedelta(days=rng.randint(0, date_range))
        el  = rng.choice(elements)
        if el == "PRCP":
            val = round(abs(rng.gauss(2, 5)), 2)
        elif el == "TMAX":
            val = round(rng.gauss(28, 4), 2)
        else:
            val = round(rng.gauss(16, 3), 2)
        rows.append({
            "station_id": sid, "date": str(d),
            "element": el, "value": val, "quality_flag": None,
        })
    pdf = pd.DataFrame(rows)
    pdf["date"] = pd.to_datetime(pdf["date"])
    return spark.createDataFrame(pdf)


def compute_monthly_aggregates(spark, df):
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col, when
    df = df.withColumn("year", F.year("date")).withColumn("month", F.month("date"))
    prcp = df.filter(col("element") == "PRCP")
    tmax = df.filter(col("element") == "TMAX")
    tmin = df.filter(col("element") == "TMIN")
    mp = prcp.groupBy("station_id", "year", "month").agg(
        F.sum("value").alias("monthly_total_precip_mm"),
        F.sum(when(col("value") > 1, 1).otherwise(0)).alias("days_with_rain"),
    )
    mt = tmax.groupBy("station_id", "year", "month").agg(
        F.avg("value").alias("monthly_avg_tmax_c"))
    mn = tmin.groupBy("station_id", "year", "month").agg(
        F.avg("value").alias("monthly_avg_tmin_c"))
    monthly = (mp.join(mt, ["station_id","year","month"], "left")
                 .join(mn, ["station_id","year","month"], "left"))
    return monthly.withColumn("dry_spell_max_days",
                               (30 - col("days_with_rain")).cast("int"))


def compute_onset_dates_pandas(pdf):
    import pandas as pd
    seasons = {"MAM": [3, 4, 5], "OND": [10, 11, 12]}
    results = []
    for station_id, sdf in pdf.groupby("station_id"):
        sdf = sdf.sort_values("date").copy()
        sdf["date"] = pd.to_datetime(sdf["date"])
        for year in sdf["date"].dt.year.unique():
            for season, months in seasons.items():
                s = sdf[
                    (sdf["date"].dt.year == year) &
                    (sdf["date"].dt.month.isin(months))
                ].copy()
                if len(s) < 10:
                    continue
                s = s.set_index("date").sort_index()
                precip = s["value"]
                for i in range(len(precip) - 9):
                    window = precip.iloc[i:i + 10]
                    if window.sum() >= 20.0:
                        onset_date = window.index[0]
                        follow = precip[onset_date:].iloc[1:31]
                        dry_run = max_dry = 0
                        for v in follow:
                            if v <= 0:
                                dry_run += 1
                                max_dry = max(max_dry, dry_run)
                            else:
                                dry_run = 0
                        if max_dry <= 9:
                            results.append({
                                "station_id": station_id,
                                "year": int(year),
                                "season": season,
                                "onset_date": str(onset_date.date()),
                                "cum_precip_at_onset": round(float(window.sum()), 2),
                            })
                            break
    return pd.DataFrame(results)


def main():
    setup_logging("spark_processor.log")
    t0 = time.time()
    spark = get_spark_session("NoaaBatchProcessor")
    logger.info("SparkSession ready.")
    if RAW_NOAA.exists():
        logger.info("Loading NOAA data from %s", RAW_NOAA)
        df = spark.read.parquet(str(RAW_NOAA))
    else:
        logger.warning("NOAA parquet missing - generating 50000 synthetic rows.")
        df = _generate_synthetic(spark, 50_000)
    logger.info("Input records: %d", df.count())
    monthly = compute_monthly_aggregates(spark, df)
    logger.info("Monthly aggregates: %d rows", monthly.count())
    monthly.write.mode("overwrite").partitionBy("year").parquet(str(OUT_MONTHLY))
    logger.info("Saved monthly -> %s", OUT_MONTHLY)
    prcp_pdf = (df.filter(df["element"] == "PRCP")
                  .select("station_id", "date", "value")
                  .toPandas())
    onset_pdf = compute_onset_dates_pandas(prcp_pdf)
    logger.info("Onset records: %d", len(onset_pdf))
    if not onset_pdf.empty:
        spark.createDataFrame(onset_pdf).write.mode("overwrite")              .partitionBy("year").parquet(str(OUT_ONSET))
        logger.info("Saved onset -> %s", OUT_ONSET)
    logger.info("Job complete in %.1f s", time.time() - t0)


if __name__ == "__main__":
    main()
