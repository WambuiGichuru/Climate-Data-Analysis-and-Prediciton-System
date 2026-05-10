# CLAUDE.md

## Session Rules — Read First, Every Time

- You are a coding assistant helping a student developer complete a university project.
- NEVER commit directly to main. All work goes to branch: feature/r05-setup
- NEVER self-identify in commit messages, comments, or code. No "AI-generated", 
  no "Claude", no "LLM". Write code and comments as a student would.
- Add the following to .gitignore if not already there:
    .claude/
    .claudeignore
    CLAUDE.md
    .cursor/
    .copilot/
    *.ai-cache
- Do NOT overwrite any existing file that has real content. 
  Read the file first. If it has working code, only ADD to it — never replace.
- After completing each task, stage and push to feature/r05-setup only.
  Then create a Pull Request to main with a plain student-style description.
- Commit message style: "feat: add Cloud Run deployment config" 
  NOT "feat: Claude generated Cloud Run config"

---

## Project Overview

Kenya County-Level Rainfall Onset Advisory Dashboard
Course: SDS2412 — Analysis of Large Datasets, Group Two
GCP Project: sds2412-kenya-onset
Bucket: gs://sds2412-kenya-onset-data
BigQuery dataset: kenya_onset
Region: us-central1
GitHub: github.com/WambuiGichuru/Climate-Data-Analysis-and-Prediciton-System

---

## What Each Role Has Completed — Do Not Touch These

### R01 — Dennis (Data & Infrastructure Lead) — DONE
- ERA5 ingestion pipeline: src/ingest/era5_downloader.py
- NetCDF to Parquet conversion: src/processing/era5_to_parquet.py
- Parquet files live on GCS: gs://sds2412-kenya-onset-data/processed/
- Ingestion logs in logs/

### R02 — Ashley (Distributed Processing Engineer) — DONE
- PySpark pipeline: src/processing/spark_pipeline.py
- Scalability benchmark: src/processing/scalability_analysis.py
- BigQuery tables loaded: kenya_onset.historical_onset, kenya_onset.monthly_aggregates
- Benchmark results in logs/scalability_benchmark.csv

### R04 — Eric (ML & Analytics Engineer) — DONE
- XGBoost model trained and serialised
- Model artifact: gs://sds2412-kenya-onset-data/ml/models/xgboost_onset_v1.joblib
- SHAP analysis complete
- EDA figures in analysis/figures/

### R03 — Alex (Streaming & Real-Time Engineer) — PARTIALLY DONE
- Kafka + Zookeeper in docker-compose.yml — DONE
- Kafka producer: src/ingest/openmeteo_poller.py — DONE
- Spark Structured Streaming consumer: src/streaming/spark_consumer.py — STUB, needs completing
- Bloom filter and Count-Min Sketch — NOT DONE, add to spark_consumer.py
- Firestore sink (onset-alerts → Firestore) — NOT DONE

---

## Your Role — R05 Faith (DevOps, Deployment & Reporting Lead)

Work sequentially through the milestones below. 
Complete each fully before moving to the next.
Read every existing file before touching it.

---

## MILESTONE 2 — What R05 Still Owes

### Tasks
1. Verify the Airflow DAG in dags/kenya_onset_pipeline_dag.py is complete.
   - Must have tasks: validate_environment, download_era5, convert_to_parquet,
     run_spark_pipeline, poll_openmeteo, validate_outputs, notify_complete
   - Dependencies must be wired correctly
   - If any task is missing or the dependency chain is broken, fix it
   - Do not rewrite tasks that already exist — only add what is missing

2. Create infrastructure/gcp/submit_spark_job.sh
   - Script that runs: gcloud dataproc batches submit pyspark
   - Copies spark_pipeline.py to GCS before submitting
   - Parameterised with PROJECT_ID and GCS_BUCKET from environment

3. Create infrastructure/gcp/verify_bigquery.sh
   - Runs bq query to confirm row counts in both BQ tables
   - Prints pass/fail result clearly

4. Update .gitignore to include all AI tool directories and files

---

## MILESTONE 3 — Streaming & Real-Time

### Your tasks (R05)
1. Create infrastructure/docker/Dockerfile.poller
   - Based on Dockerfile.api but CMD runs src/ingest/openmeteo_poller.py
   - Must be a slim image — no unnecessary packages

2. Create infrastructure/gcp/deploy_poller.sh
   - Builds the poller image and pushes to Artifact Registry
   - Creates the Cloud Run Job: openmeteo-poller
   - Sets env vars: GCP_PROJECT_ID, GCS_BUCKET from environment
   - Parameterised — no hardcoded values

3. Create infrastructure/gcp/setup_scheduler.sh
   - Creates Cloud Scheduler job: openmeteo-hourly-trigger
   - Schedule: every 60 minutes
   - Timezone: Africa/Nairobi
   - Targets the Cloud Run Job URI

4. Create infrastructure/gcp/setup_firestore_ttl.md
   - Step-by-step instructions for setting the TTL policy manually
     (Firestore TTL cannot be set via gcloud CLI — must be console or Admin SDK)
   - Field: expires_at, duration: 7 days

5. Update dags/kenya_onset_pipeline_dag.py
   - Add task: poll_openmeteo_production
   - This task triggers the Cloud Run Job via gcloud CLI
   - Wire it: validate_environment >> poll_openmeteo_production (parallel with download_era5)

### Alex's remaining tasks (R03) — complete these if his stubs are empty
Read src/streaming/spark_consumer.py first.
If it is a stub (less than 50 lines of real logic), complete it:
- Read from Kafka topic raw-weather-stream using Spark Structured Streaming readStream
- Apply 72-hour sliding window cumulative rainfall per county
- Implement Bloom filter for county deduplication (use pyspark.ml or a simple bitarray)
- Implement Count-Min Sketch for rainfall event frequency estimation
- Write onset events to Kafka topic onset-alerts
- Write onset events to Firestore collection live_forecast
- Each function must have a docstring explaining what it does and why

---

## MILESTONE 4 — Machine Learning Deployment

### Your tasks (R05)
1. Create infrastructure/gcp/deploy_vertex.sh
   - Registers the model from GCS in Vertex AI Model Registry
   - Creates a Vertex AI Endpoint
   - Deploys the model to the endpoint
   - Machine type: n1-standard-2, min replicas: 1, max: 3
   - Prints the ENDPOINT_ID at the end — needed for api.py

2. Update src/serving/api.py
   - Read the file first. Find the get_risk_map() function.
   - The function currently returns mock or Firestore-only data
   - Add a call to the Vertex AI Endpoint for each county
   - Add ml_probability field to each county's GeoJSON properties
   - Add error handling: if Vertex AI call fails, return onset_risk without ml_probability
     rather than crashing the entire endpoint
   - Do not rewrite functions that already work — only extend get_risk_map()

3. Create infrastructure/gcp/setup_monitoring.sh
   - Creates a Cloud Monitoring alerting policy via gcloud
   - Alert: Vertex AI prediction latency p95 > 500ms for 5 minutes
   - Notification: email (placeholder — user fills in email)

---

## MILESTONE 5 — System Optimisation & Deployment

### Your tasks (R05)
1. Verify infrastructure/docker/Dockerfile.api exists and is correct
   - If incomplete, complete it: multi-stage build, non-root user, PORT env var
   - Do not rewrite if it already works

2. Create infrastructure/gcp/deploy_api.sh
   - Builds Dockerfile.api and pushes to Artifact Registry
   - Deploys to Cloud Run:
     --allow-unauthenticated
     --min-instances=0 --max-instances=10
     --memory=512Mi --cpu=1
     --set-env-vars from environment
   - Prints the Service URL at the end

3. Update src/serving/api.py
   - Add Cache-Control: public, max-age=900 header to /risk-map response
   - Use JSONResponse with headers parameter
   - Do not touch any other endpoint

4. Create infrastructure/docker/entrypoint.sh
   - Simple: exec uvicorn src.serving.api:app --host 0.0.0.0 --port $PORT
   - Make it executable

5. Create locustfile.py in project root
   - HttpUser with two tasks: GET /api/v1/risk-map (weight 3), 
     GET /api/v1/county/Nairobi (weight 1)
   - wait_time between(1, 3)
   - Comments explaining what each task tests

6. Create infrastructure/gcp/setup_monitoring_dashboard.md
   - Step-by-step instructions for creating the Cloud Monitoring dashboard
   - 5 charts: Cloud Run latency, Firestore reads, BigQuery query count, 
     Vertex AI latency, Cloud Run Jobs completion
   - Cannot be scripted — must be done in console

---

## MILESTONE 6 — Integrated System & Capstone

### Your tasks (R05)
1. Update docs/dashboard_mockup.html
   - Read the existing file first — it has a static layout already
   - Add a <script> block at the bottom (do not change the HTML structure)
   - Fetch /api/v1/risk-map from the live API URL (use a const API_URL variable 
     at the top of the script so it is easy to change)
   - Update the county risk display with real data from the fetch response
   - Update the data freshness indicator with metadata.last_updated_utc
   - Keep the mockup functional as a static file too (graceful fallback if API is unreachable)

2. Create infrastructure/gcp/deploy_firebase.sh
   - Installs firebase-tools if not present
   - Runs firebase deploy --only hosting
   - Prints the live URL at the end

3. Create infrastructure/gcp/predemo_healthcheck.sh
   - Checks all 5 system components:
     1. curl $API_URL/health — expects {"status":"healthy"}
     2. gsutil ls gs://$GCS_BUCKET/processed/ — expects non-empty
     3. bq query row count on historical_onset — expects > 0
     4. gcloud ai endpoints list — expects endpoint in DEPLOYED state
     5. gcloud run jobs list — expects openmeteo-poller present
   - Prints PASS or FAIL for each with a timestamp
   - Exits with code 1 if any check fails

4. Create a firebase.json in project root
   - Hosting config: public directory = docs, ignore node_modules and .git
   - Single page: false (it is a static multi-file site)

5. Update README.md
   - Add section: ## Live System
   - Placeholder lines for: Dashboard URL, API URL, API Docs URL
   - Add section: ## Running the Project with placeholders for each milestone's run command
   - Do not remove anything already in the README

---

## After Each Milestone — Git Workflow

After completing all tasks for a milestone:
1. git add -A
2. git commit -m "feat(r05): complete M[X] deployment infrastructure"
   Replace [X] with the milestone number.
   Keep the message plain and student-like.
3. git push origin feature/r05-setup
4. Open a Pull Request to main on GitHub with title:
   "R05 — Milestone [X] infrastructure and deployment"
   Body: list what was added in plain language.

---

## Style Rules for All Code Written

- Docstrings on every function: what it does, why it exists, what it returns
- No print statements — use the logging module
- No hardcoded project IDs, bucket names, or credentials — read from environment variables
- Shell scripts must start with set -euo pipefail
- All scripts must print what they are doing before each major step
- Python files must have a module-level docstring explaining the file's purpose
- Keep the same code style as the existing files in the repo