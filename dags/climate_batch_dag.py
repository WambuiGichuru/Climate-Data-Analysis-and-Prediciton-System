"""
climate_batch_dag.py
Author    : R05 - Faith Gichuru (DevOps, Deployment & Reporting Lead)
Milestone : M2 - Distributed Processing & Workflow Orchestration
Purpose   : Daily Airflow DAG for the Kenya County-Level Rainfall Onset
            Advisory Dashboard. Drives the batch leg of the Lambda
            architecture: ingest ERA5 reanalysis, convert to Parquet,
            run the Spark feature/aggregation pipeline, refresh the
            OpenMeteo live snapshot, then validate outputs and notify.

Tasks:
    validate_environment - confirm env vars and dependencies before any I/O
    download_era5        - pull ERA5 NetCDF from Copernicus CDS
    convert_to_parquet   - convert NetCDF -> partitioned Parquet on GCS
    run_spark_pipeline   - submit Dataproc Serverless Spark batch job
    poll_openmeteo       - one-shot OpenMeteo poll (parallel with ERA5 path)
    validate_outputs     - confirm BigQuery row counts and GCS landing
    notify_complete      - log final pipeline summary

Schedule: 04:00 Africa/Nairobi every day. Catchup disabled.

Run-time configuration is read from environment variables so the DAG
ships with no hardcoded project, bucket, or region values:
    REPO_ROOT, GCP_PROJECT_ID, GCS_BUCKET, BQ_DATASET, GCP_REGION
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Run-time configuration (env-driven; sane defaults match project README)
# ---------------------------------------------------------------------------
REPO_ROOT      = os.environ.get("REPO_ROOT", "/opt/airflow/repo")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "climate-prediction-system")
GCS_BUCKET     = os.environ.get("GCS_BUCKET", "climate-prediction-system-data")
BQ_DATASET     = os.environ.get("BQ_DATASET", "kenya_onset")
GCP_REGION     = os.environ.get("GCP_REGION", "us-central1")

REQUIRED_ENV_VARS = ("GCP_PROJECT_ID", "GCS_BUCKET")


# ---------------------------------------------------------------------------
# Python callables
# ---------------------------------------------------------------------------
def validate_environment_fn(**_context) -> None:
    """Fail fast if required env vars are unset.

    Why: every downstream task assumes GCP credentials and bucket names
    are present. Catching this here prevents partial pipeline runs that
    waste Dataproc minutes and leave half-written outputs on GCS.
    """
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )
    log.info(
        "Environment OK | project=%s bucket=%s dataset=%s region=%s",
        GCP_PROJECT_ID, GCS_BUCKET, BQ_DATASET, GCP_REGION,
    )


def notify_complete_fn(**context) -> None:
    """Log a one-line summary at end of run.

    Replaces email/Slack hooks with a structured log line that downstream
    monitoring (Cloud Logging) can alert on. Uses Airflow context to
    record the logical date so the message is reproducible across runs.
    """
    logical_date = context.get("logical_date") or context.get("execution_date")
    log.info(
        "Pipeline complete | dag=kenya_onset_pipeline logical_date=%s",
        logical_date,
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "r05-faith",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="kenya_onset_pipeline",
    description="Kenya rainfall onset daily batch pipeline (M2)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 4 * * *",          # 04:00 Africa/Nairobi (UTC+3)
    catchup=False,
    max_active_runs=1,
    tags=["kenya", "onset", "climate", "batch", "r05"],
) as dag:

    validate_environment = PythonOperator(
        task_id="validate_environment",
        python_callable=validate_environment_fn,
    )

    download_era5 = BashOperator(
        task_id="download_era5",
        bash_command=(
            f"cd {REPO_ROOT} && "
            f"python -m src.ingest.era5_downloader "
            f"--bucket {GCS_BUCKET}"
        ),
    )

    convert_to_parquet = BashOperator(
        task_id="convert_to_parquet",
        bash_command=(
            f"cd {REPO_ROOT} && "
            f"python -m src.processing.era5_to_parquet "
            f"--bucket {GCS_BUCKET}"
        ),
    )

    run_spark_pipeline = BashOperator(
        task_id="run_spark_pipeline",
        bash_command=f"bash {REPO_ROOT}/infrastructure/gcp/submit_spark_job.sh",
    )

    poll_openmeteo = BashOperator(
        task_id="poll_openmeteo",
        bash_command=(
            f"cd {REPO_ROOT} && "
            f"python -m src.ingest.openmeteo_live_poller"
        ),
    )

    validate_outputs = BashOperator(
        task_id="validate_outputs",
        bash_command=f"bash {REPO_ROOT}/infrastructure/gcp/verify_bigquery.sh",
    )

    notify_complete = PythonOperator(
        task_id="notify_complete",
        python_callable=notify_complete_fn,
    )

    # -----------------------------------------------------------------------
    # Dependencies
    #
    # validate_environment fans out to two parallel branches:
    #   * batch leg : download_era5 -> convert_to_parquet -> run_spark_pipeline
    #   * speed leg : poll_openmeteo
    # Both branches must complete before validate_outputs runs.
    # notify_complete is the final sink.
    # -----------------------------------------------------------------------
    validate_environment >> [download_era5, poll_openmeteo]
    download_era5 >> convert_to_parquet >> run_spark_pipeline
    [run_spark_pipeline, poll_openmeteo] >> validate_outputs >> notify_complete
