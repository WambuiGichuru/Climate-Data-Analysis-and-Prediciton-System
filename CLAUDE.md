# CLAUDE.md — Project Context for Claude Code

## Project
Kenya County-Level Rainfall Onset Advisory Dashboard
Course: SDS2412, Group Two, GCP project: sds2412-kenya-onset

## What is done
- R01 Dennis: ERA5 ingestion pipeline, GCS upload, Parquet files live
- R02 Ashley: PySpark pipeline complete, BigQuery tables loaded, scalability benchmark done
- R04 Eric: XGBoost model trained, serialised at gs://bucket/ml/models/xgboost_onset_v1.joblib, SHAP done

## What is NOT done — active work
### R03 Alex (Streaming)
- Spark Structured Streaming consumer (src/streaming/spark_consumer.py — stub only)
- Bloom filter for county deduplication
- Count-Min Sketch for rainfall frequency
- Firestore sink writing onset-alerts

### R05 Faith (DevOps — YOU)
- M3: Cloud Run Job for openmeteo_poller.py, Cloud Scheduler hourly trigger, Firestore TTL policy
- M5: Containerise FastAPI → Cloud Run deployment, Cache-Control on /risk-map, 
        Cloud Monitoring dashboard (5 charts), Locust load test (100 users, p95 < 300ms)
- M6: Firebase Hosting deploy of docs/dashboard_mockup.html, 
        update HTML to call live Cloud Run API, final technical report, GitHub tag v1.0-m6

## Key files
- src/serving/api.py — FastAPI app (stub, needs Cloud Run deploy)
- src/ingest/openmeteo_poller.py — needs Cloud Run Job wrapper
- infrastructure/docker/Dockerfile.api — multi-stage build ready
- dags/kenya_onset_pipeline_dag.py — Airflow DAG (needs streaming task added)
- docs/dashboard_mockup.html — frontend (needs fetch() calls to live API)
- config/settings.py — all 47 counties, GCP config, onset definition

## GCP
- Project: sds2412-kenya-onset
- Bucket: gs://sds2412-kenya-onset-data
- BigQuery dataset: kenya_onset
- Region: us-central1

## Onset definition
>=20mm over 3 consecutive days, no dry spell >=7 days in next 10 days