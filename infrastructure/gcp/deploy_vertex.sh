#!/usr/bin/env bash
# deploy_vertex.sh
# Author    : R05 - Faith Gichuru
# Milestone : M4
# Purpose   : Register the Kenya XGBoost model in Vertex AI Model
#             Registry, create an online Endpoint, deploy the model
#             onto it, and print the resulting ENDPOINT_ID. The
#             dashboard API (src/serving/api.py) reads VERTEX_ENDPOINT_ID
#             from the environment to enable the ML layer.
#
# Required env vars:
#   GCP_PROJECT_ID  - GCP project (e.g. sds2412-kenya-onset)
#   GCS_BUCKET      - bucket without gs:// (e.g. sds2412-kenya-onset-data)
#
# Optional env vars:
#   GCP_REGION         - default us-central1
#   MODEL_DISPLAY_NAME - default xgboost-onset-v1
#   ENDPOINT_NAME      - default kenya-onset-endpoint
#   GCS_MODEL_FILE     - existing single-file artifact path on GCS
#                        (default: gs://${GCS_BUCKET}/ml/models/xgboost_onset_v1.joblib)
#   GCS_MODEL_DIR      - directory Vertex AI will read (default:
#                        gs://${GCS_BUCKET}/ml/models/xgboost_onset_v1)
#   PREDICTION_IMAGE   - prebuilt container (default: sklearn 1.3 CPU,
#                        which loads .joblib via joblib.load)
#   MACHINE_TYPE       - default n1-standard-2
#   MIN_REPLICAS       - default 1
#   MAX_REPLICAS       - default 3

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
: "${GCS_BUCKET:?GCS_BUCKET must be set}"

GCP_REGION="${GCP_REGION:-us-central1}"
MODEL_DISPLAY_NAME="${MODEL_DISPLAY_NAME:-xgboost-onset-v1}"
ENDPOINT_NAME="${ENDPOINT_NAME:-kenya-onset-endpoint}"
GCS_MODEL_FILE="${GCS_MODEL_FILE:-gs://${GCS_BUCKET}/ml/models/xgboost_onset_v1.joblib}"
GCS_MODEL_DIR="${GCS_MODEL_DIR:-gs://${GCS_BUCKET}/ml/models/xgboost_onset_v1}"
PREDICTION_IMAGE="${PREDICTION_IMAGE:-us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest}"
MACHINE_TYPE="${MACHINE_TYPE:-n1-standard-2}"
MIN_REPLICAS="${MIN_REPLICAS:-1}"
MAX_REPLICAS="${MAX_REPLICAS:-3}"

echo "[deploy_vertex] project        = ${GCP_PROJECT_ID}"
echo "[deploy_vertex] region         = ${GCP_REGION}"
echo "[deploy_vertex] model file (in)= ${GCS_MODEL_FILE}"
echo "[deploy_vertex] model dir (out)= ${GCS_MODEL_DIR}"
echo "[deploy_vertex] image          = ${PREDICTION_IMAGE}"
echo "[deploy_vertex] machine        = ${MACHINE_TYPE} (replicas ${MIN_REPLICAS}-${MAX_REPLICAS})"

# ---------------------------------------------------------------------------
# Step 1 - stage the joblib into a directory layout Vertex AI accepts
# ---------------------------------------------------------------------------
# The sklearn prebuilt container expects exactly one file named
# `model.joblib` inside the artifact directory. The R04 artifact is
# stored as a single file at GCS_MODEL_FILE, so we copy it into the
# expected layout. This step is idempotent.
echo "[deploy_vertex] step 1/4: staging model.joblib into artifact directory"
if ! gsutil -q stat "${GCS_MODEL_FILE}"; then
  echo "[deploy_vertex] ERROR: source model not found at ${GCS_MODEL_FILE}" >&2
  exit 1
fi
gsutil cp "${GCS_MODEL_FILE}" "${GCS_MODEL_DIR}/model.joblib"

# ---------------------------------------------------------------------------
# Step 2 - upload to Vertex AI Model Registry
# ---------------------------------------------------------------------------
echo "[deploy_vertex] step 2/4: registering model in Model Registry"
gcloud ai models upload \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --display-name="${MODEL_DISPLAY_NAME}" \
  --artifact-uri="${GCS_MODEL_DIR}" \
  --container-image-uri="${PREDICTION_IMAGE}" \
  --description="Kenya county-level rainfall onset XGBoost regressor (R04)"

MODEL_ID="$(
  gcloud ai models list \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --filter="displayName=${MODEL_DISPLAY_NAME}" \
    --format='value(MODEL_ID)' \
    --sort-by=~createTime \
  | head -n 1
)"
if [[ -z "${MODEL_ID}" ]]; then
  echo "[deploy_vertex] ERROR: could not resolve MODEL_ID after upload" >&2
  exit 1
fi
echo "[deploy_vertex]   MODEL_ID=${MODEL_ID}"

# ---------------------------------------------------------------------------
# Step 3 - create the endpoint (idempotent: reuse if already present)
# ---------------------------------------------------------------------------
echo "[deploy_vertex] step 3/4: ensuring endpoint exists"
ENDPOINT_ID="$(
  gcloud ai endpoints list \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --filter="displayName=${ENDPOINT_NAME}" \
    --format='value(ENDPOINT_ID)' \
  | head -n 1
)"
if [[ -z "${ENDPOINT_ID}" ]]; then
  echo "[deploy_vertex]   creating endpoint ${ENDPOINT_NAME}"
  gcloud ai endpoints create \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --display-name="${ENDPOINT_NAME}"
  ENDPOINT_ID="$(
    gcloud ai endpoints list \
      --project="${GCP_PROJECT_ID}" \
      --region="${GCP_REGION}" \
      --filter="displayName=${ENDPOINT_NAME}" \
      --format='value(ENDPOINT_ID)' \
    | head -n 1
  )"
else
  echo "[deploy_vertex]   reusing endpoint ${ENDPOINT_NAME} (${ENDPOINT_ID})"
fi

# ---------------------------------------------------------------------------
# Step 4 - deploy the model onto the endpoint
# ---------------------------------------------------------------------------
echo "[deploy_vertex] step 4/4: deploying model to endpoint"
gcloud ai endpoints deploy-model "${ENDPOINT_ID}" \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --model="${MODEL_ID}" \
  --display-name="${MODEL_DISPLAY_NAME}-deploy" \
  --machine-type="${MACHINE_TYPE}" \
  --min-replica-count="${MIN_REPLICAS}" \
  --max-replica-count="${MAX_REPLICAS}" \
  --traffic-split=0=100

echo ""
echo "[deploy_vertex] DONE"
echo "[deploy_vertex]   MODEL_ID    = ${MODEL_ID}"
echo "[deploy_vertex]   ENDPOINT_ID = ${ENDPOINT_ID}"
echo ""
echo "Next: export VERTEX_ENDPOINT_ID='${ENDPOINT_ID}' and pass it to the"
echo "Cloud Run API service so src/serving/api.py can call the model."
