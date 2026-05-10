#!/usr/bin/env bash
# deploy_poller.sh
# Author    : R05 - Faith Gichuru
# Milestone : M3
# Purpose   : Build the OpenMeteo poller image, push it to Artifact
#             Registry, and create/update the Cloud Run Job that runs
#             it. Cloud Scheduler (setup_scheduler.sh) drives the
#             trigger cadence; this script is idempotent so re-running
#             it just updates the Job to the latest image.
#
# Required env vars:
#   GCP_PROJECT_ID  - GCP project (e.g. sds2412-kenya-onset)
#   GCS_BUCKET      - data bucket without gs:// (e.g. sds2412-kenya-onset-data)
#
# Optional env vars:
#   GCP_REGION      - default us-central1
#   AR_REPO         - Artifact Registry repo name (default: kenya-onset)
#   IMAGE_NAME      - image short name (default: openmeteo-poller)
#   JOB_NAME        - Cloud Run Job name (default: openmeteo-poller)
#   IMAGE_TAG       - tag for this build (default: timestamp)

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
: "${GCS_BUCKET:?GCS_BUCKET must be set}"

GCP_REGION="${GCP_REGION:-us-central1}"
AR_REPO="${AR_REPO:-kenya-onset}"
IMAGE_NAME="${IMAGE_NAME:-openmeteo-poller}"
JOB_NAME="${JOB_NAME:-openmeteo-poller}"
IMAGE_TAG="${IMAGE_TAG:-$(date -u +%Y%m%dt%H%M%S)}"

IMAGE_URI="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${AR_REPO}/${IMAGE_NAME}:${IMAGE_TAG}"
DOCKERFILE="infrastructure/docker/Dockerfile.poller"

echo "[deploy_poller] project=${GCP_PROJECT_ID} region=${GCP_REGION}"
echo "[deploy_poller] image  =${IMAGE_URI}"
echo "[deploy_poller] job    =${JOB_NAME}"
echo "[deploy_poller] dockerfile=${DOCKERFILE}"

if [[ ! -f "${DOCKERFILE}" ]]; then
  echo "[deploy_poller] ERROR: ${DOCKERFILE} not found" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 1 - ensure Artifact Registry repo exists (idempotent)
# ---------------------------------------------------------------------------
echo "[deploy_poller] step 1/4: ensuring Artifact Registry repo exists"
if ! gcloud artifacts repositories describe "${AR_REPO}" \
        --project="${GCP_PROJECT_ID}" \
        --location="${GCP_REGION}" >/dev/null 2>&1; then
  echo "[deploy_poller]   creating repo ${AR_REPO}"
  gcloud artifacts repositories create "${AR_REPO}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --repository-format=docker \
    --description="Kenya rainfall onset images"
else
  echo "[deploy_poller]   repo ${AR_REPO} already exists"
fi

# ---------------------------------------------------------------------------
# Step 2 - build the image with Cloud Build (no local Docker daemon needed)
# ---------------------------------------------------------------------------
echo "[deploy_poller] step 2/4: building image with Cloud Build"
gcloud builds submit \
  --project="${GCP_PROJECT_ID}" \
  --tag="${IMAGE_URI}" \
  --file="${DOCKERFILE}" \
  .

# ---------------------------------------------------------------------------
# Step 3 - create or update the Cloud Run Job
# ---------------------------------------------------------------------------
echo "[deploy_poller] step 3/4: deploying Cloud Run Job ${JOB_NAME}"
ENV_VARS="GCP_PROJECT_ID=${GCP_PROJECT_ID},GCS_BUCKET=${GCS_BUCKET}"

if gcloud run jobs describe "${JOB_NAME}" \
      --project="${GCP_PROJECT_ID}" \
      --region="${GCP_REGION}" >/dev/null 2>&1; then
  echo "[deploy_poller]   job exists - updating to new image"
  gcloud run jobs update "${JOB_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --image="${IMAGE_URI}" \
    --set-env-vars="${ENV_VARS}" \
    --max-retries=2 \
    --task-timeout=300s
else
  echo "[deploy_poller]   job does not exist - creating"
  gcloud run jobs create "${JOB_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --image="${IMAGE_URI}" \
    --set-env-vars="${ENV_VARS}" \
    --max-retries=2 \
    --task-timeout=300s
fi

# ---------------------------------------------------------------------------
# Step 4 - print the Job URI for setup_scheduler.sh
# ---------------------------------------------------------------------------
echo "[deploy_poller] step 4/4: resolving Job URI for Cloud Scheduler"
JOB_URI="https://${GCP_REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${GCP_PROJECT_ID}/jobs/${JOB_NAME}:run"
echo "[deploy_poller] DONE"
echo "[deploy_poller]   image  = ${IMAGE_URI}"
echo "[deploy_poller]   job    = ${JOB_NAME}"
echo "[deploy_poller]   uri    = ${JOB_URI}"
echo ""
echo "Next: export JOB_URI='${JOB_URI}' && bash infrastructure/gcp/setup_scheduler.sh"
