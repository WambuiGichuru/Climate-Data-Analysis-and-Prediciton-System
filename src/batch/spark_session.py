"""
spark_session.py
Author    : R02 — Ashley (Batch Processing Engineer)
Purpose   : Singleton SparkSession factory for the Climate Onset batch
            pipeline.  Reads SPARK_MASTER from the environment (defaults
            to local[*]) and sets log level WARN to suppress verbose output.
Milestone : M2 — Spark Batch Processing
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

_spark_session_singleton = None


def get_spark_session(app_name: str = "ClimateOnsetSystem"):
    """
    Return a configured SparkSession, creating it on first call.

    The session is cached so multiple callers in the same process share
    one SparkContext (Spark does not allow more than one per JVM).
    """
    global _spark_session_singleton
    if _spark_session_singleton is not None:
        return _spark_session_singleton

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise ImportError(
            "PySpark not installed. Run: pip install pyspark>=3.5.0"
        ) from exc

    master = os.getenv("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    _spark_session_singleton = spark
    return spark
