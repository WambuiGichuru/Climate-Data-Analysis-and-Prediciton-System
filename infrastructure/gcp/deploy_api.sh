#!/usr/bin/env bash
# deploy_api.sh
# Author    : R05 - Faith Gichuru
# Milestone : M5
# Purpose   : Build the dashboard API image with Cloud Build, push it to
#             Artifact Registry, and deploy it to Cloud Run as a public
#             HTTP service. Idempotent: re-running just rolls a new
#             revision onto the same service.
#
# Required env vars:
#   GCP_PROJECT_ID  - GCP project (e.g. sds2412-kenya-onset)
#   GCS_BUCKET      - bucket without gs:// (passed through to the API
#                     so historical-trend etc. can resolve resources)
#
# Optional env vars:
#   GCP_REGION         - default us-central1
#   AR_REPO            - Artifact Registry repo (default: kenya-onset)
#   IMAGE_NAME         - image short name (default: kenya-onset-api)
#   SERVICE_NAME       - Cloud Run service name (default: kenya-onset-api)
#   IMAGE_TAG          - tag for this build (default: timestamp)
#   VERTEX_ENDPOINT_ID - Vertex AI endpoint id (optional; api.py disables
#                        the ML layer cleanly when unset)
#   BQ_DATASET         - default kenya_onset

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
: "${GCS_BUCKET:?GCS_BUCKET must be set}"

GCP_REGION="${GCP_REGION:-us-central1}"
AR_REPO="${AR_REPO:-kenya-onset}"
IMAGE_NAME="${IMAGE_NAME:-kenya-onset-api}"
SERVICE_NAME="${SERVICE_NAME:-kenya-onset-api}"
IMAGE_TAG="${IMAGE_TAG:-$(date -u +%Y%m%dt%H%M%S)}"
VERTEX_ENDPOINT_ID="${VERTEX_ENDPOINT_ID:-}"
BQ_DATASET="${BQ_DATASET:-kenya_onset}"

IMAGE_URI="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${AR_REPO}/${IMAGE_NAME}:${IMAGE_TAG}"
DOCKERFILE="infrastructure/docker/Dockerfile.api"

echo "[deploy_api] project = ${GCP_PROJECT_ID}"
echo "[deploy_api] region  = ${GCP_REGION}"
echo "[deploy_api] image   = ${IMAGE_URI}"
echo "[deploy_api] service = ${SERVICE_NAME}"
echo "[deploy_api] vertex  = ${VERTEX_ENDPOINT_ID:-<unset, ML layer disabled>}"

if [[ ! -f "${DOCKERFILE}" ]]; then
  echo "[deploy_api] ERROR: ${DOCKERFILE} not found" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 1 - ensure Artifact Registry repo exists
# ---------------------------------------------------------------------------
echo "[deploy_api] step 1/3: ensuring Artifact Registry repo exists"
if ! gcloud artifacts repositories describe "${AR_REPO}" \
        --project="${GCP_PROJECT_ID}" \
        --location="${GCP_REGION}" >/dev/null 2>&1; then
  gcloud artifacts repositories create "${AR_REPO}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --repository-format=docker \
    --description="Kenya rainfall onset images"
fi

# ---------------------------------------------------------------------------
# Step 2 - build and push with Cloud Build
# ---------------------------------------------------------------------------
echo "[deploy_api] step 2/3: building image with Cloud Build"
gcloud builds submit \
  --project="${GCP_PROJECT_ID}" \
  --tag="${IMAGE_URI}" \
  --file="${DOCKERFILE}" \
  .

# ---------------------------------------------------------------------------
# Step 3 - deploy to Cloud Run (creates the service if missing)
# ---------------------------------------------------------------------------
echo "[deploy_api] step 3/3: deploying to Cloud Run"

# Build the env-var string up front so empty vars (Vertex endpoint when
# the ML layer isn't deployed yet) don't leak as literal empty strings.
ENV_VARS="GCP_PROJECT_ID=${GCP_PROJECT_ID},GCS_BUCKET=${GCS_BUCKET},BQ_DATASET=${BQ_DATASET},GCP_REGION=${GCP_REGION}"
if [[ -n "${VERTEX_ENDPOINT_ID}" ]]; then
  ENV_VARS="${ENV_VARS},VERTEX_ENDPOINT_ID=${VERTEX_ENDPOINT_ID}"
fi

gcloud run deploy "${SERVICE_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --image="${IMAGE_URI}" \
  --platform=managed \
  --allow-unauthenticated \
  --port=8080 \
  --memory=512Mi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=10 \
  --set-env-vars="${ENV_VARS}"

SERVICE_URL="$(
  gcloud run services describe "${SERVICE_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --format='value(status.url)'
)"

echo ""
echo "[deploy_api] DONE"
echo "[deploy_api]   image   = ${IMAGE_URI}"
echo "[deploy_api]   service = ${SERVICE_NAME}"
echo "[deploy_api]   URL     = ${SERVICE_URL}"
