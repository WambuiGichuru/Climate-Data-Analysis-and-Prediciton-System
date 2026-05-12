#!/usr/bin/env bash
# entrypoint_streamlit.sh
# Milestone : M6
# Purpose   : Container entrypoint for the Streamlit dashboard.
#             Honours $PORT (Cloud Run / Codespaces injects this) and
#             forwards it to `streamlit run`. Address 0.0.0.0 so the
#             dashboard is reachable from outside the container.
set -euo pipefail
exec streamlit run src/dashboard/app.py \
    --server.port "${PORT:-8501}" \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false
