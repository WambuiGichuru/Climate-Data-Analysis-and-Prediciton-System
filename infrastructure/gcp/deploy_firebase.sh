#!/usr/bin/env bash
# deploy_firebase.sh
# Author    : R05 - Faith Gichuru
# Milestone : M6
# Purpose   : Publish docs/ to Firebase Hosting and print the live URL.
#             docs/dashboard_mockup.html embeds an M6 <script> overlay
#             that fetches the Cloud Run API once Firebase serves it.
#
# Required env vars:
#   GCP_PROJECT_ID  - GCP / Firebase project (e.g. climate-prediction-system)
#
# Optional env vars:
#   FIREBASE_SITE   - hosting site id (default: same as GCP_PROJECT_ID)

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
FIREBASE_SITE="${FIREBASE_SITE:-${GCP_PROJECT_ID}}"

echo "[deploy_firebase] project = ${GCP_PROJECT_ID}"
echo "[deploy_firebase] site    = ${FIREBASE_SITE}"

# ---------------------------------------------------------------------------
# Step 1 - install firebase-tools if not already on PATH
# ---------------------------------------------------------------------------
echo "[deploy_firebase] step 1/3: ensuring firebase CLI is installed"
if ! command -v firebase >/dev/null 2>&1; then
  if ! command -v npm >/dev/null 2>&1; then
    echo "[deploy_firebase] ERROR: npm is not installed; install Node.js first." >&2
    exit 1
  fi
  echo "[deploy_firebase]   installing firebase-tools globally via npm"
  npm install -g firebase-tools
else
  echo "[deploy_firebase]   firebase-tools already present ($(firebase --version))"
fi

# ---------------------------------------------------------------------------
# Step 2 - sanity-check the project root has firebase.json + docs/
# ---------------------------------------------------------------------------
echo "[deploy_firebase] step 2/3: validating project layout"
if [[ ! -f firebase.json ]]; then
  echo "[deploy_firebase] ERROR: firebase.json not found in CWD" >&2
  exit 1
fi
if [[ ! -d docs ]]; then
  echo "[deploy_firebase] ERROR: docs/ not found in CWD" >&2
  exit 1
fi
if [[ ! -f docs/dashboard_mockup.html ]]; then
  echo "[deploy_firebase] WARNING: docs/dashboard_mockup.html missing — site will deploy without dashboard" >&2
fi

# ---------------------------------------------------------------------------
# Step 3a - optionally substitute the real Cloud Run URL into the HTML.
# Uses an in-place edit guarded by a backup so the working tree is
# restored even if the deploy fails. No-op when API_URL is unset.
# ---------------------------------------------------------------------------
HTML="docs/dashboard_mockup.html"
HTML_BAK=""
if [[ -n "${API_URL:-}" && -f "${HTML}" ]]; then
  echo "[deploy_firebase] step 3a: injecting API_URL=${API_URL} into ${HTML}"
  HTML_BAK="$(mktemp)"
  cp "${HTML}" "${HTML_BAK}"
  # The | delimiter avoids escaping the slashes in the URL.
  sed -i.tmp "s|https://kenya-onset-api-REPLACE_ME.a.run.app|${API_URL}|g" "${HTML}"
  rm -f "${HTML}.tmp"
fi

restore_html() {
  if [[ -n "${HTML_BAK}" && -f "${HTML_BAK}" ]]; then
    mv "${HTML_BAK}" "${HTML}"
    echo "[deploy_firebase] restored ${HTML} from backup"
  fi
}
trap restore_html EXIT

# ---------------------------------------------------------------------------
# Step 3 - deploy hosting only
# ---------------------------------------------------------------------------
echo "[deploy_firebase] step 3/3: running 'firebase deploy --only hosting'"
firebase deploy \
  --only hosting \
  --project "${GCP_PROJECT_ID}" \
  --non-interactive

# Resolve the canonical URL Firebase prints. The default site URL pattern
# is ${SITE}.web.app for Firebase Hosting.
SITE_URL="https://${FIREBASE_SITE}.web.app"

echo ""
echo "[deploy_firebase] DONE"
echo "[deploy_firebase]   site URL   = ${SITE_URL}"
echo "[deploy_firebase]   dashboard  = ${SITE_URL}/dashboard_mockup.html"
echo ""
if [[ -z "${API_URL:-}" ]]; then
  echo "[deploy_firebase] note: API_URL env var was not set — the dashboard"
  echo "was deployed with the placeholder URL. Set API_URL=... and re-run"
  echo "to publish a version that talks to your live Cloud Run service."
fi
