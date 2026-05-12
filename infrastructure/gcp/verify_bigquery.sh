#!/usr/bin/env bash
# verify_bigquery.sh
# Author    : R05 - Faith Gichuru
# Milestone : M2
# Purpose   : Sanity-check that the two BigQuery tables produced by the
#             Spark pipeline contain rows. Used by the Airflow
#             validate_outputs task and by the M6 pre-demo healthcheck.
#
# Required env vars:
#   GCP_PROJECT_ID - target GCP project (e.g. climate-prediction-system)
#
# Optional env vars:
#   BQ_DATASET     - dataset name (default: kenya_onset)
#   MIN_ROWS       - minimum row count to pass (default: 1)
#
# Exit code: 0 if every table has >= MIN_ROWS, 1 otherwise.

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
BQ_DATASET="${BQ_DATASET:-kenya_onset}"
MIN_ROWS="${MIN_ROWS:-1}"

TABLES=(
  "historical_onset"
  "monthly_aggregates"
)

echo "[verify_bigquery] project=${GCP_PROJECT_ID} dataset=${BQ_DATASET} min_rows=${MIN_ROWS}"

overall_status=0

for table in "${TABLES[@]}"; do
  fq="${GCP_PROJECT_ID}.${BQ_DATASET}.${table}"
  echo "[verify_bigquery] checking ${fq}"

  # bq query --format=csv prints header then value; tail -n1 grabs the count
  count="$(
    bq query \
      --project_id="${GCP_PROJECT_ID}" \
      --use_legacy_sql=false \
      --format=csv \
      --quiet \
      "SELECT COUNT(*) AS n FROM \`${fq}\`" \
    | tail -n 1
  )"

  if [[ -z "${count}" || ! "${count}" =~ ^[0-9]+$ ]]; then
    echo "[verify_bigquery] FAIL ${fq} - could not read row count (got: '${count}')"
    overall_status=1
    continue
  fi

  if (( count >= MIN_ROWS )); then
    echo "[verify_bigquery] PASS ${fq} rows=${count}"
  else
    echo "[verify_bigquery] FAIL ${fq} rows=${count} < ${MIN_ROWS}"
    overall_status=1
  fi
done

if (( overall_status == 0 )); then
  echo "[verify_bigquery] OVERALL: PASS"
else
  echo "[verify_bigquery] OVERALL: FAIL" >&2
fi

exit "${overall_status}"
