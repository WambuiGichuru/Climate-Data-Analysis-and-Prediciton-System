#!/usr/bin/env bash
# post-create.sh
# Milestone : M6
# Purpose   : Bootstrap the Codespaces environment after the container
#             is created. Installs dashboard-only Python deps, gcloud
#             CLI, and prints next-step instructions.
#
# Intentionally NOT `set -e`: every step is wrapped so a single failure
# (e.g. apt mirror flake, GCP CLI repo signing change) does not block
# the codespace from opening - the user can re-run any step manually.
set -uo pipefail

step() {
    echo
    echo "============================================================"
    echo "[post-create] $*"
    echo "============================================================"
}

# ---------------------------------------------------------------------------
# 1. Dashboard-only Python deps.
#
#    The full requirements.txt pulls in apache-airflow / pyspark /
#    prophet / shap / cdsapi which take 5-10 minutes and frequently
#    need build tools or Airflow constraint files - not worth waiting
#    for if all the user wants is to run the Streamlit dashboard.
#    Install the minimal set here; the user can `pip install -r
#    requirements.txt` later if they need the rest.
# ---------------------------------------------------------------------------
step "installing dashboard-only python deps (slim)"
pip install --upgrade pip || true
pip install --no-cache-dir \
    "streamlit>=1.31.0" \
    "streamlit-folium>=0.20.0" \
    "folium>=0.15.0" \
    "plotly>=5.18.0" \
    "pandas>=2.1.0" \
    "numpy>=1.26.0" \
    "pyarrow>=14.0.0" \
    "requests>=2.31.0" \
    "joblib>=1.3.0" \
    "xgboost>=2.0.0" \
    "scikit-learn>=1.4.0" \
    "fastapi>=0.110.0" \
    "uvicorn>=0.27.0" \
    "google-cloud-firestore>=2.16.0" \
    "google-cloud-bigquery>=3.21.0" \
    "google-cloud-aiplatform>=1.49.0" \
    "python-dotenv>=1.0.0" \
    "loguru>=0.7.0" \
    || echo "[post-create] WARNING: some pip installs failed - see above"

# ---------------------------------------------------------------------------
# 2. gcloud SDK
# ---------------------------------------------------------------------------
step "installing gcloud SDK (if missing)"
if ! command -v gcloud >/dev/null 2>&1; then
    sudo apt-get update -y || true
    sudo apt-get install -y apt-transport-https ca-certificates gnupg curl || true
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg || true
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] \
https://packages.cloud.google.com/apt cloud-sdk main" \
        | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list >/dev/null
    sudo apt-get update -y || true
    sudo apt-get install -y google-cloud-cli || \
        echo "[post-create] WARNING: gcloud install failed - rerun this script later"
fi
gcloud --version || echo "[post-create] gcloud not on PATH yet"

# ---------------------------------------------------------------------------
# 3. Repo scaffolding
# ---------------------------------------------------------------------------
step "preparing data directories"
mkdir -p data/raw data/processed data/features logs models

cat <<'EOF'

================================================================
  Kenya Onset Advisory - Codespaces ready

  Next steps (run these in the Codespaces terminal):

  1) Authenticate to GCP so the API can read BigQuery / Firestore /
     Vertex AI from inside the container:

         gcloud auth login
         gcloud auth application-default login
         gcloud config set project climate-prediction-system

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
