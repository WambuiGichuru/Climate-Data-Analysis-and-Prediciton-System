# Cloud Monitoring dashboard — manual setup

The M5 deliverable requires a single Cloud Monitoring dashboard that
shows the health of the five GCP components the project depends on.
Cloud Monitoring's dashboard JSON schema is verbose and brittle, so
this document is the canonical procedure rather than a script.

> Audience: anyone with `Monitoring Editor` on
> `climate-prediction-system` (or whatever `GCP_PROJECT_ID` is set to).

---

## 1. Create the dashboard

1. Open <https://console.cloud.google.com/monitoring/dashboards>.
2. Click **+ Create Dashboard** → name it **`Kenya Onset Pipeline`**.
3. Set the time range selector to **Last 1 hour** (top right).

You'll add 5 charts, in the order below. For each chart click
**+ Add chart** → **Line** unless noted otherwise.

---

## 2. Chart 1 — Cloud Run latency (kenya-onset-api)

- **Title**: `API request latency (p50/p95/p99)`
- **Metric**: `run.googleapis.com/request_latencies`
  (resource type *Cloud Run Revision*).
- **Filter**: `service_name = kenya-onset-api`.
- **Group by**: `service_name`.
- **Aggregator**: `none`. Add three series with the same metric:
  - aligner `ALIGN_PERCENTILE_50`
  - aligner `ALIGN_PERCENTILE_95`
  - aligner `ALIGN_PERCENTILE_99`
- Y-axis units: ms.

This is the chart that will alarm if anything in the request path
(Firestore, Vertex, BigQuery) starts dragging.

---

## 3. Chart 2 — Firestore reads

- **Title**: `Firestore document reads`
- **Metric**: `firestore.googleapis.com/document/read_count`
  (resource type *Firestore Database*).
- **Group by**: `database_id`.
- Aligner `ALIGN_RATE`, reducer `REDUCE_SUM`.
- Y-axis units: ops/s.

The dashboard API reads `live_forecast` on every request to
`/risk-map`, so this chart should track API traffic closely. A flat
line while API traffic is non-zero means the speed-layer client
is failing silently — see the API's `Firestore unavailable` warning.

---

## 4. Chart 3 — BigQuery query count

- **Title**: `BigQuery queries (historical-trend)`
- **Metric**: `bigquery.googleapis.com/query/count`
  (resource type *BigQuery Project*).
- **Filter**: `project_id = climate-prediction-system`.
- Aligner `ALIGN_RATE`, reducer `REDUCE_SUM`.
- Y-axis units: queries/s.

Useful for spotting runaway query loops (`/api/v1/historical-trend`
hitting a hot retry path) and for billing-cost visibility.

---

## 5. Chart 4 — Vertex AI prediction latency

- **Title**: `Vertex AI prediction p95 latency`
- **Metric**:
  `aiplatform.googleapis.com/prediction/online/prediction_latencies`
  (resource type *Vertex AI Endpoint*).
- **Group by**: `endpoint_id`.
- Aligner `ALIGN_PERCENTILE_95`, reducer `REDUCE_MAX`,
  alignment period `60s`.
- Y-axis units: ms.
- **Threshold line**: 500 ms (matches the alerting policy created
  by `infrastructure/gcp/setup_monitoring.sh`).

When this line crosses the threshold the alerting policy will
already be firing — the chart is for visual correlation with
dependent services.

---

## 6. Chart 5 — Cloud Run Jobs completion (openmeteo-poller)

- Switch the chart type to **Stacked bar**.
- **Title**: `OpenMeteo poller — job runs (success/failure)`
- **Metric**: `run.googleapis.com/job/completed_task_attempt_count`
  (resource type *Cloud Run Job*).
- **Filter**: `job_name = openmeteo-poller`.
- **Group by**: `result`. Two series will appear:
  `succeeded` (green) and `failed` (red).
- Aligner `ALIGN_DELTA`, reducer `REDUCE_SUM`,
  alignment period `1h`.
- Y-axis units: count.

Confirms Cloud Scheduler is firing every hour and the Job is
completing. Sustained `failed` bars mean the poller is broken even
though Scheduler's own success metric stays green.

---

## 7. Save and pin

1. Click **Save** in the top-right of the dashboard editor.
2. Pin the dashboard to your console home: **⋮ → Add to favourites**.
3. Capture a screenshot for the M5 / M6 report appendix.

---

## Appendix — exporting JSON for backup

Once the dashboard renders correctly, you can keep a copy in the repo:

```bash
gcloud monitoring dashboards list \
  --project="${GCP_PROJECT_ID}" \
  --filter='displayName="Kenya Onset Pipeline"' \
  --format='value(name)'

gcloud monitoring dashboards describe <RESOURCE_NAME> \
  --project="${GCP_PROJECT_ID}" \
  --format=json \
  > infrastructure/gcp/dashboard_kenya_onset.json
```

Restoring on a fresh project:

```bash
gcloud monitoring dashboards create \
  --project="${GCP_PROJECT_ID}" \
  --config-from-file=infrastructure/gcp/dashboard_kenya_onset.json
```

This is optional — the manual procedure above is the canonical M5
deliverable; the JSON export is just a convenience.
