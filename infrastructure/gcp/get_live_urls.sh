#!/usr/bin/env bash
# get_live_urls.sh
# Milestone : M6
# Purpose   : Discover and print the real URLs of the deployed system
#             so the README and dashboard mockup can be updated with
#             working links (no more placeholders).
#
#             Queries gcloud for:
#               * Cloud Run service URL  (API)
#               * Firebase Hosting site URL  (dashboard)
#               * Cloud Run Job presence    (openmeteo-poller)
#               * BigQuery dataset access    (kenya_onset.historical_onset)
#
# Required env vars:
#   GCP_PROJECT_ID  - GCP project (default: climate-prediction-system)
#
# Optional env vars:
#   GCP_REGION      - Cloud Run region (default: us-central1)
#   SERVICE_NAME    - Cloud Run service (default: kenya-onset-api)
#   BQ_DATASET      - BigQuery dataset (default: kenya_onset)
set -euo pipefail

GCP_PROJECT_ID="${GCP_PROJECT_ID:-climate-prediction-system}"
GCP_REGION="${GCP_REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-kenya-onset-api}"
BQ_DATASET="${BQ_DATASET:-kenya_onset}"

echo "[get_live_urls] project = ${GCP_PROJECT_ID}"
echo "[get_live_urls] region  = ${GCP_REGION}"
echo

# ---------------------------------------------------------------------------
# 1. Cloud Run API
# ---------------------------------------------------------------------------
echo "[get_live_urls] resolving Cloud Run service URL ..."
API_URL="$(
    gcloud run services describe "${SERVICE_NAME}" \
        --project="${GCP_PROJECT_ID}" \
        --region="${GCP_REGION}" \
        --format='value(status.url)' 2>/dev/null || true
)"

# ---------------------------------------------------------------------------
# 2. Firebase Hosting
# ---------------------------------------------------------------------------
echo "[get_live_urls] resolving Firebase Hosting site URL ..."
FIREBASE_URL=""
if command -v firebase >/dev/null 2>&1; then
    # The first site listed for the project is typically the default
    # one created by `firebase init hosting`. Falls through to the
    # standard *.web.app convention when the CLI is unavailable.
    FIREBASE_URL="$(
        firebase hosting:sites:list \
            --project "${GCP_PROJECT_ID}" \
            --json 2>/dev/null \
        | python -c "import json,sys; d=json.load(sys.stdin); print(d['result'][0]['defaultUrl'] if d.get('result') else '')" \
            2>/dev/null || true
    )"
fi
if [[ -z "${FIREBASE_URL}" ]]; then
    FIREBASE_URL="https://${GCP_PROJECT_ID}.web.app"
fi

# ---------------------------------------------------------------------------
# 3. Cloud Run Jobs - poller present?
# ---------------------------------------------------------------------------
echo "[get_live_urls] checking openmeteo-poller Cloud Run Job ..."
POLLER_STATUS="missing"
if gcloud run jobs describe openmeteo-poller \
        --project="${GCP_PROJECT_ID}" \
        --region="${GCP_REGION}" >/dev/null 2>&1; then
    POLLER_STATUS="present"
fi

# ---------------------------------------------------------------------------
# 4. BigQuery dataset row count - is real data flowing?
# ---------------------------------------------------------------------------
echo "[get_live_urls] checking BigQuery historical_onset row count ..."
BQ_ROWS="unknown"
if command -v bq >/dev/null 2>&1; then
    BQ_ROWS="$(
        bq query --project_id="${GCP_PROJECT_ID}" --nouse_legacy_sql --format=csv \
            "SELECT COUNT(*) AS n FROM \`${GCP_PROJECT_ID}.${BQ_DATASET}.historical_onset\`" \
            2>/dev/null | tail -n1 || echo "unknown"
    )"
fi

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
echo
echo "================================================================"
echo "  Live System URLs"
echo "================================================================"
printf "  %-12s : %s\n" "Dashboard"   "${FIREBASE_URL}/dashboard_mockup.html"
printf "  %-12s : %s\n" "API"         "${API_URL:-<not deployed yet>}"
printf "  %-12s : %s\n" "API Docs"    "${API_URL:+${API_URL}/docs}"
printf "  %-12s : %s\n" "API Health"  "${API_URL:+${API_URL}/health}"
printf "  %-12s : %s\n" "Poller Job"  "${POLLER_STATUS}"
printf "  %-12s : %s rows\n" "BQ table"   "${BQ_ROWS}"
echo "================================================================"
echo
echo "  Paste-ready Markdown for README.md (replace the Live System table):"
echo
echo "  | Dashboard | ${FIREBASE_URL}/dashboard_mockup.html |"
echo "  | API       | ${API_URL:-not deployed} |"
echo "  | API Docs  | ${API_URL:+${API_URL}/docs} |"
echo
echo "  Paste-ready env for the Streamlit dashboard:"
echo
echo "      export API_URL=${API_URL:-https://YOUR-CLOUD-RUN-URL}"
echo
