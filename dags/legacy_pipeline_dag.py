"""
climate_batch_dag.py
Author    : R05 - Faith (DevOps & Deployment Engineer)
Purpose   : Airflow DAG for the nightly batch pipeline:
            NOAA ingest -> OpenMeteo ingest -> Spark processing ->
            Feature engineering -> Model drift monitoring.
Milestone : M5 - Workflow Orchestration
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner":            "r05-faith",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _validate_or_ingest_noaa(**kwargs):
    """Check yesterday's NOAA download exists; if not, run --test ingest."""
    import subprocess
    from src.config import DATA_DIR
    noaa_path = DATA_DIR / "raw" / "noaa_ghcnd_kenya.parquet"
    if noaa_path.exists():
        print(f"NOAA parquet exists: {noaa_path}")
    else:
        print("NOAA parquet missing — running test ingest ...")
        result = subprocess.run(
            [sys.executable, str(_REPO / "src" / "ingest" / "noaa_ghcnd_ingest.py"), "--test"],
            capture_output=True, text=True,
        )
        print(result.stdout[-2000:])
        if result.returncode != 0:
            raise RuntimeError(f"NOAA ingest failed: {result.stderr[-1000:]}")


def _run_openmeteo_historical(**kwargs):
    """Fetch/refresh OpenMeteo historical data for all counties."""
    import subprocess
    result = subprocess.run(
        [sys.executable, str(_REPO / "src" / "ingest" / "openmeteo_historical_ingest.py")],
        capture_output=True, text=True,
    )
    print(result.stdout[-2000:])
    if result.returncode != 0:
        raise RuntimeError(f"OpenMeteo ingest failed: {result.stderr[-1000:]}")


def _run_feature_engineering(**kwargs):
    """Run feature_engineer.py to produce ML-ready features."""
    from src.ml.feature_engineer import main as fe_main
    fe_main()


def _run_model_monitor(**kwargs):
    """Run drift detection check."""
    from src.ml.model_monitor import run_drift_check
    results = run_drift_check()
    print("Drift results:", results)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="climate_batch_pipeline",
    default_args=DEFAULT_ARGS,
    description="Nightly batch: ingest -> Spark -> features -> drift check",
    schedule_interval="0 2 * * *",   # 2am UTC nightly
    catchup=False,
    tags=["climate", "batch", "sds2412"],
) as dag:

    t1 = PythonOperator(
        task_id="validate_or_ingest_noaa",
        python_callable=_validate_or_ingest_noaa,
    )

    t2 = PythonOperator(
        task_id="ingest_openmeteo_historical",
        python_callable=_run_openmeteo_historical,
    )

    t3 = BashOperator(
        task_id="spark_batch_processing",
        bash_command=(
            f"cd {_REPO} && "
            f"{sys.executable} src/batch/noaa_spark_processor.py"
        ),
    )

    t4 = PythonOperator(
        task_id="feature_engineering",
        python_callable=_run_feature_engineering,
    )

    t5 = PythonOperator(
        task_id="model_drift_monitor",
        python_callable=_run_model_monitor,
    )

    t1 >> t2 >> t3 >> t4 >> t5
