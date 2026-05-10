#!/usr/bin/env bash
# entrypoint.sh
# Author    : R05 - Faith Gichuru
# Milestone : M5
# Purpose   : Container entrypoint for the Cloud Run API service.
#             Cloud Run injects $PORT (and may change it between
#             revisions); we forward it to uvicorn.
set -euo pipefail
exec uvicorn src.serving.api:app --host 0.0.0.0 --port "${PORT:-8080}"
