#!/usr/bin/env bash
# deploy_firebase.sh
# Author    : R05 - Faith Gichuru
# Milestone : M6
# Purpose   : Publish docs/ to Firebase Hosting and print the live URL.
#             docs/dashboard_mockup.html embeds an M6 <script> overlay
#             that fetches the Cloud Run API once Firebase serves it.
#
# Required env vars:
#   GCP_PROJECT_ID  - GCP / Firebase project (e.g. sds2412-kenya-onset)
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
echo "Remember to update docs/dashboard_mockup.html — replace the API_URL"
echo "placeholder with the Cloud Run service URL printed by deploy_api.sh."
