#!/usr/bin/env bash
# submit_spark_job.sh
# Author    : R05 - Faith Gichuru
# Milestone : M2
# Purpose   : Stage src/processing/spark_pipeline.py to GCS and submit it
#             as a Dataproc Serverless PySpark batch. Used by the Airflow
#             run_spark_pipeline task.
#
# Required env vars:
#   GCP_PROJECT_ID  - target GCP project (e.g. climate-prediction-system)
#   GCS_BUCKET      - bucket WITHOUT gs:// prefix (e.g. climate-prediction-system-data)
#
# Optional env vars:
#   GCP_REGION      - Dataproc region (default: us-central1)
#   SPARK_SCRIPT    - local path to the pyspark entry-point
#                     (default: src/processing/spark_pipeline.py)
#   BATCH_PREFIX    - GCS path under the bucket for staging
#                     (default: code/spark)

set -euo pipefail

# ---------------------------------------------------------------------------
# Resolve config
# ---------------------------------------------------------------------------
: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
: "${GCS_BUCKET:?GCS_BUCKET must be set}"

GCP_REGION="${GCP_REGION:-us-central1}"
SPARK_SCRIPT="${SPARK_SCRIPT:-src/processing/spark_pipeline.py}"
BATCH_PREFIX="${BATCH_PREFIX:-code/spark}"

BATCH_ID="kenya-onset-$(date -u +%Y%m%dt%H%M%S)"
SCRIPT_NAME="$(basename "${SPARK_SCRIPT}")"
GCS_SCRIPT_URI="gs://${GCS_BUCKET}/${BATCH_PREFIX}/${SCRIPT_NAME}"

echo "[submit_spark_job] project=${GCP_PROJECT_ID} region=${GCP_REGION}"
echo "[submit_spark_job] local script=${SPARK_SCRIPT}"
echo "[submit_spark_job] gcs target =${GCS_SCRIPT_URI}"
echo "[submit_spark_job] batch id   =${BATCH_ID}"

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
if [[ ! -f "${SPARK_SCRIPT}" ]]; then
  echo "[submit_spark_job] ERROR: ${SPARK_SCRIPT} not found" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 1 - upload the pyspark script to GCS
# ---------------------------------------------------------------------------
echo "[submit_spark_job] step 1/2: uploading script to GCS"
gsutil cp "${SPARK_SCRIPT}" "${GCS_SCRIPT_URI}"

# ---------------------------------------------------------------------------
# Step 2 - submit Dataproc Serverless batch
# ---------------------------------------------------------------------------
echo "[submit_spark_job] step 2/2: submitting Dataproc batch"
gcloud dataproc batches submit pyspark "${GCS_SCRIPT_URI}" \
  --batch="${BATCH_ID}" \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --version=2.2 \
  --properties="spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4" \
  -- \
  --project-id="${GCP_PROJECT_ID}" \
  --bucket="${GCS_BUCKET}"

echo "[submit_spark_job] DONE batch=${BATCH_ID}"
