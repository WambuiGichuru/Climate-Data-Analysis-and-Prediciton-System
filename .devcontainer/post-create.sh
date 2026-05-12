#!/usr/bin/env bash
# post-create.sh
# Milestone : M6
# Purpose   : Bootstrap the Codespaces environment after the container
#             is created. Installs Python deps, pulls the docker-compose
#             images, and prints next-step instructions for the user.
set -euo pipefail

echo "[post-create] installing python deps from requirements.txt"
pip install --upgrade pip
pip install --no-cache-dir -r requirements.txt

echo "[post-create] installing gcloud SDK (if missing)"
if ! command -v gcloud >/dev/null 2>&1; then
    # Add the Google Cloud SDK apt repo then install the CLI.
    sudo apt-get update
    sudo apt-get install -y apt-transport-https ca-certificates gnupg curl
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] \
https://packages.cloud.google.com/apt cloud-sdk main" \
        | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
    sudo apt-get update
    sudo apt-get install -y google-cloud-cli
fi
gcloud --version || true

echo "[post-create] preparing data directories"
mkdir -p data/raw data/processed data/features logs models

echo "[post-create] pre-pulling docker-compose images (best-effort)"
docker compose pull --ignore-pull-failures || true

cat <<'EOF'

================================================================
  Kenya Onset Advisory - Codespaces ready

  Next steps (run these in the Codespaces terminal):

  1) Authenticate to GCP so the API can read BigQuery / Firestore /
     Vertex AI from inside the container:

         gcloud auth login
         gcloud auth application-default login
         gcloud config set project sds2412-kenya-onset

  2) Start the full stack (Kafka + FastAPI + Streamlit):

         docker compose up --build

     Streamlit auto-forwards on port 8501 and will open in a tab.

  3) Or run the dashboard standalone against a deployed Cloud Run
     API (replace the URL with the one printed by deploy_api.sh):

         API_URL=https://kenya-onset-api-xxx.a.run.app \
             docker compose up streamlit

  4) Once deployed, fetch the real Cloud Run + Firebase URLs:

         bash infrastructure/gcp/get_live_urls.sh

================================================================
EOF
