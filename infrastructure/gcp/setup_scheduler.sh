#!/usr/bin/env bash
# setup_scheduler.sh
# Author    : R05 - Faith Gichuru
# Milestone : M3
# Purpose   : Create (or update) the Cloud Scheduler job that triggers
#             the openmeteo-poller Cloud Run Job once an hour, on the
#             hour, in Africa/Nairobi local time.
#
#             Uses an OAuth-token authenticated HTTP target pointing at
#             the Cloud Run Jobs `:run` admin API. The default Compute
#             Engine service account needs the `run.invoker` role on the
#             Job for the trigger to succeed (granted in step 1).
#
# Required env vars:
#   GCP_PROJECT_ID  - GCP project (e.g. sds2412-kenya-onset)
#
# Optional env vars:
#   GCP_REGION         - default us-central1
#   JOB_NAME           - Cloud Run Job name (default: openmeteo-poller)
#   SCHEDULER_NAME     - Scheduler job name (default: openmeteo-hourly-trigger)
#   SCHEDULE_CRON      - cron expression (default: "0 * * * *" = top of every hour)
#   SCHEDULE_TZ        - timezone (default: Africa/Nairobi)
#   INVOKER_SA         - service account used by Cloud Scheduler
#                        (default: <project-number>-compute@developer.gserviceaccount.com)

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"

GCP_REGION="${GCP_REGION:-us-central1}"
JOB_NAME="${JOB_NAME:-openmeteo-poller}"
SCHEDULER_NAME="${SCHEDULER_NAME:-openmeteo-hourly-trigger}"
SCHEDULE_CRON="${SCHEDULE_CRON:-0 * * * *}"
SCHEDULE_TZ="${SCHEDULE_TZ:-Africa/Nairobi}"

PROJECT_NUMBER="$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')"
INVOKER_SA="${INVOKER_SA:-${PROJECT_NUMBER}-compute@developer.gserviceaccount.com}"

JOB_URI="https://${GCP_REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${GCP_PROJECT_ID}/jobs/${JOB_NAME}:run"

echo "[setup_scheduler] project   = ${GCP_PROJECT_ID}"
echo "[setup_scheduler] region    = ${GCP_REGION}"
echo "[setup_scheduler] scheduler = ${SCHEDULER_NAME}"
echo "[setup_scheduler] cron      = '${SCHEDULE_CRON}' tz=${SCHEDULE_TZ}"
echo "[setup_scheduler] target    = ${JOB_URI}"
echo "[setup_scheduler] invoker   = ${INVOKER_SA}"

# ---------------------------------------------------------------------------
# Step 1 - grant run.invoker on the Job to the scheduler service account
# ---------------------------------------------------------------------------
echo "[setup_scheduler] step 1/2: granting roles/run.invoker on ${JOB_NAME}"
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --member="serviceAccount:${INVOKER_SA}" \
  --role="roles/run.invoker" \
  --quiet >/dev/null

# ---------------------------------------------------------------------------
# Step 2 - create or update the Cloud Scheduler job
# ---------------------------------------------------------------------------
echo "[setup_scheduler] step 2/2: creating/updating Cloud Scheduler job"
COMMON_ARGS=(
  --project="${GCP_PROJECT_ID}"
  --location="${GCP_REGION}"
  --schedule="${SCHEDULE_CRON}"
  --time-zone="${SCHEDULE_TZ}"
  --uri="${JOB_URI}"
  --http-method=POST
  --oauth-service-account-email="${INVOKER_SA}"
  --description="Hourly trigger for ${JOB_NAME} (Africa/Nairobi)"
)

if gcloud scheduler jobs describe "${SCHEDULER_NAME}" \
      --project="${GCP_PROJECT_ID}" \
      --location="${GCP_REGION}" >/dev/null 2>&1; then
  echo "[setup_scheduler]   scheduler exists - updating"
  gcloud scheduler jobs update http "${SCHEDULER_NAME}" "${COMMON_ARGS[@]}"
else
  echo "[setup_scheduler]   scheduler does not exist - creating"
  gcloud scheduler jobs create http "${SCHEDULER_NAME}" "${COMMON_ARGS[@]}"
fi

echo "[setup_scheduler] DONE"
echo "[setup_scheduler]   next run will fire on the next ${SCHEDULE_TZ} hour boundary"
