#!/usr/bin/env bash
# setup_monitoring.sh
# Author    : R05 - Faith Gichuru
# Milestone : M4
# Purpose   : Create a Cloud Monitoring alerting policy that fires when
#             the Vertex AI online-prediction p95 latency exceeds 500 ms
#             for 5 consecutive minutes. Notifications are emailed to
#             ALERT_EMAIL.
#
# Required env vars:
#   GCP_PROJECT_ID  - GCP project (e.g. sds2412-kenya-onset)
#
# Optional env vars:
#   GCP_REGION         - default us-central1 (only used in display)
#   ALERT_EMAIL        - placeholder default below; user must override
#                        with their own address before running in CI.
#   POLICY_NAME        - default: "Vertex AI prediction p95 latency"
#   THRESHOLD_MS       - default 500
#   DURATION_SECONDS   - default 300 (5 minutes)

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"

GCP_REGION="${GCP_REGION:-us-central1}"
ALERT_EMAIL="${ALERT_EMAIL:-REPLACE_ME@example.com}"
POLICY_NAME="${POLICY_NAME:-Vertex AI prediction p95 latency}"
THRESHOLD_MS="${THRESHOLD_MS:-500}"
DURATION_SECONDS="${DURATION_SECONDS:-300}"

echo "[setup_monitoring] project    = ${GCP_PROJECT_ID}"
echo "[setup_monitoring] region     = ${GCP_REGION}"
echo "[setup_monitoring] policy     = ${POLICY_NAME}"
echo "[setup_monitoring] threshold  = ${THRESHOLD_MS} ms p95 over ${DURATION_SECONDS}s"
echo "[setup_monitoring] alert email= ${ALERT_EMAIL}"

if [[ "${ALERT_EMAIL}" == "REPLACE_ME@example.com" ]]; then
  echo "[setup_monitoring] WARNING: ALERT_EMAIL is the placeholder value." >&2
  echo "[setup_monitoring]          Set ALERT_EMAIL=your@addr before running." >&2
fi

# ---------------------------------------------------------------------------
# Step 1 - ensure an email notification channel exists
# ---------------------------------------------------------------------------
echo "[setup_monitoring] step 1/2: ensuring email notification channel"
CHANNEL_ID="$(
  gcloud beta monitoring channels list \
    --project="${GCP_PROJECT_ID}" \
    --filter="type=email AND labels.email_address=${ALERT_EMAIL}" \
    --format='value(name)' \
  | head -n 1
)"

if [[ -z "${CHANNEL_ID}" ]]; then
  echo "[setup_monitoring]   creating channel for ${ALERT_EMAIL}"
  CHANNEL_ID="$(
    gcloud beta monitoring channels create \
      --project="${GCP_PROJECT_ID}" \
      --display-name="Vertex AI alerts (${ALERT_EMAIL})" \
      --type=email \
      --channel-labels="email_address=${ALERT_EMAIL}" \
      --format='value(name)'
  )"
else
  echo "[setup_monitoring]   reusing existing channel"
fi
echo "[setup_monitoring]   CHANNEL=${CHANNEL_ID}"

# ---------------------------------------------------------------------------
# Step 2 - create the alerting policy from a JSON definition
# ---------------------------------------------------------------------------
# The Vertex AI online prediction latency metric is a distribution; we
# align it with ALIGN_PERCENTILE_95 to express "p95 over the last 60 s",
# then trigger when that p95 stays above the threshold for the full
# DURATION_SECONDS window.
echo "[setup_monitoring] step 2/2: creating alerting policy"
POLICY_FILE="$(mktemp -t vertex_policy.XXXXXX.json)"
trap 'rm -f "${POLICY_FILE}"' EXIT

cat > "${POLICY_FILE}" <<EOF
{
  "displayName": "${POLICY_NAME}",
  "combiner": "OR",
  "conditions": [
    {
      "displayName": "Prediction latency p95 > ${THRESHOLD_MS}ms",
      "conditionThreshold": {
        "filter": "metric.type=\"aiplatform.googleapis.com/prediction/online/prediction_latencies\" AND resource.type=\"aiplatform.googleapis.com/Endpoint\"",
        "aggregations": [
          {
            "alignmentPeriod": "60s",
            "perSeriesAligner": "ALIGN_PERCENTILE_95",
            "crossSeriesReducer": "REDUCE_MAX",
            "groupByFields": ["resource.label.endpoint_id"]
          }
        ],
        "comparison": "COMPARISON_GT",
        "thresholdValue": ${THRESHOLD_MS},
        "duration": "${DURATION_SECONDS}s",
        "trigger": { "count": 1 }
      }
    }
  ],
  "notificationChannels": ["${CHANNEL_ID}"],
  "documentation": {
    "content": "Vertex AI endpoint p95 prediction latency exceeded ${THRESHOLD_MS}ms for ${DURATION_SECONDS}s. Investigate the Cloud Run dashboard API and the deployed model — autoscaling may be lagging behind a traffic spike.",
    "mimeType": "text/markdown"
  }
}
EOF

# Deduplicate: if a policy with this display name already exists, replace it
# rather than create a duplicate.
EXISTING_POLICY="$(
  gcloud alpha monitoring policies list \
    --project="${GCP_PROJECT_ID}" \
    --filter="displayName=\"${POLICY_NAME}\"" \
    --format='value(name)' \
  | head -n 1
)"
if [[ -n "${EXISTING_POLICY}" ]]; then
  echo "[setup_monitoring]   policy already exists - deleting before recreate"
  gcloud alpha monitoring policies delete "${EXISTING_POLICY}" \
    --project="${GCP_PROJECT_ID}" --quiet
fi

gcloud alpha monitoring policies create \
  --project="${GCP_PROJECT_ID}" \
  --policy-from-file="${POLICY_FILE}"

echo "[setup_monitoring] DONE"
