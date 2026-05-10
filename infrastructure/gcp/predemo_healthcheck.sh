#!/usr/bin/env bash
# predemo_healthcheck.sh
# Author    : R05 - Faith Gichuru
# Milestone : M6
# Purpose   : End-to-end smoke test before a live demo. Checks that all
#             five system components are reachable and have data:
#               1. Cloud Run API   (kenya-onset-api /health)
#               2. GCS processed/  (Parquet outputs of the Spark pipeline)
#               3. BigQuery        (kenya_onset.historical_onset rows > 0)
#               4. Vertex AI       (endpoint is DEPLOYED)
#               5. Cloud Run Job   (openmeteo-poller present)
#
# Each check prints PASS or FAIL with a UTC timestamp. Exit code is 0
# if every check passed, 1 otherwise.
#
# Required env vars:
#   API_URL         - Cloud Run service URL (https://kenya-onset-api-...)
#   GCP_PROJECT_ID  - GCP project
#   GCS_BUCKET      - bucket without gs://
#
# Optional env vars:
#   GCP_REGION      - default us-central1
#   BQ_DATASET      - default kenya_onset
#   BQ_TABLE        - default historical_onset
#   POLLER_JOB      - default openmeteo-poller
#   ENDPOINT_NAME   - default kenya-onset-endpoint

set -uo pipefail   # not -e: we want to keep running through failures

: "${API_URL:?API_URL must be set (e.g. https://kenya-onset-api-xxxxx.a.run.app)}"
: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
: "${GCS_BUCKET:?GCS_BUCKET must be set}"

GCP_REGION="${GCP_REGION:-us-central1}"
BQ_DATASET="${BQ_DATASET:-kenya_onset}"
BQ_TABLE="${BQ_TABLE:-historical_onset}"
POLLER_JOB="${POLLER_JOB:-openmeteo-poller}"
ENDPOINT_NAME="${ENDPOINT_NAME:-kenya-onset-endpoint}"

FAILED=0

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
pass() { echo "[$(ts)] PASS  $1"; }
fail() { echo "[$(ts)] FAIL  $1"; FAILED=1; }

echo "============================================================"
echo " Kenya Onset pre-demo healthcheck"
echo "   project = ${GCP_PROJECT_ID}"
echo "   region  = ${GCP_REGION}"
echo "   api     = ${API_URL}"
echo "============================================================"

# ---------------------------------------------------------------------------
# 1. Cloud Run API /health
# ---------------------------------------------------------------------------
echo "--- 1. Cloud Run API /health ---"
HEALTH_BODY="$(curl -fsS --max-time 10 "${API_URL}/health" 2>/dev/null || true)"
if echo "${HEALTH_BODY}" | grep -q '"status"[[:space:]]*:[[:space:]]*"healthy"'; then
  pass "/health returned status=healthy"
else
  fail "/health did not return healthy (got: ${HEALTH_BODY:-<no response>})"
fi

# ---------------------------------------------------------------------------
# 2. GCS processed/ has objects
# ---------------------------------------------------------------------------
echo "--- 2. GCS processed/ has objects ---"
GCS_LISTING="$(gsutil ls "gs://${GCS_BUCKET}/processed/" 2>/dev/null || true)"
if [[ -n "${GCS_LISTING}" ]]; then
  COUNT="$(echo "${GCS_LISTING}" | wc -l | tr -d ' ')"
  pass "gs://${GCS_BUCKET}/processed/ has ${COUNT} entries"
else
  fail "gs://${GCS_BUCKET}/processed/ is empty or unreadable"
fi

# ---------------------------------------------------------------------------
# 3. BigQuery historical_onset row count > 0
# ---------------------------------------------------------------------------
echo "--- 3. BigQuery ${BQ_DATASET}.${BQ_TABLE} row count ---"
BQ_COUNT="$(
  bq query \
    --project_id="${GCP_PROJECT_ID}" \
    --use_legacy_sql=false \
    --format=csv \
    --quiet \
    "SELECT COUNT(*) AS n FROM \`${GCP_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`" \
    2>/dev/null \
  | tail -n 1
)"
if [[ "${BQ_COUNT}" =~ ^[0-9]+$ ]] && (( BQ_COUNT > 0 )); then
  pass "${BQ_TABLE} has ${BQ_COUNT} rows"
else
  fail "${BQ_TABLE} row count failed or zero (got: '${BQ_COUNT:-<empty>}')"
fi

# ---------------------------------------------------------------------------
# 4. Vertex AI endpoint is DEPLOYED
# ---------------------------------------------------------------------------
echo "--- 4. Vertex AI endpoint state ---"
VERTEX_LISTING="$(
  gcloud ai endpoints list \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --filter="displayName=${ENDPOINT_NAME}" \
    --format='value(deployedModels.id)' \
    2>/dev/null \
  | head -n 1
)"
if [[ -n "${VERTEX_LISTING}" ]]; then
  pass "endpoint '${ENDPOINT_NAME}' has at least one deployed model"
else
  fail "endpoint '${ENDPOINT_NAME}' has no deployed models (or is missing)"
fi

# ---------------------------------------------------------------------------
# 5. Cloud Run Job openmeteo-poller exists
# ---------------------------------------------------------------------------
echo "--- 5. Cloud Run Job '${POLLER_JOB}' exists ---"
JOB_NAME_FOUND="$(
  gcloud run jobs list \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --filter="metadata.name=${POLLER_JOB}" \
    --format='value(metadata.name)' \
    2>/dev/null \
  | head -n 1
)"
if [[ "${JOB_NAME_FOUND}" == "${POLLER_JOB}" ]]; then
  pass "Cloud Run Job '${POLLER_JOB}' is registered"
else
  fail "Cloud Run Job '${POLLER_JOB}' not found"
fi

echo "============================================================"
if (( FAILED == 0 )); then
  echo "[$(ts)] OVERALL: PASS — system is demo-ready"
  exit 0
else
  echo "[$(ts)] OVERALL: FAIL — fix the items above before demoing" >&2
  exit 1
fi
