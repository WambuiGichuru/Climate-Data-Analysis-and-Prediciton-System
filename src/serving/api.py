"""
api.py
Author    : R05 - Faith Gichuru (DevOps, Deployment & Reporting Lead)
Milestone : M4 - ML serving / dashboard API (extended in M5 + M6)
Purpose   : FastAPI service that powers the Kenya County-Level Rainfall
            Onset Advisory Dashboard. Combines three data layers
            (the same Lambda-Architecture mix the project uses end-to-end):

              * batch     - BigQuery historical_onset / monthly_aggregates
              * speed     - Firestore live_forecast collection (rule-based,
                            written by src/streaming/spark_consumer.py)
              * ML        - Vertex AI online endpoint (XGBoost regressor,
                            trained in analysis/kenya_xgboost_model.py)

Endpoints:
    GET /health                     - liveness probe for Cloud Run
    GET /api/v1/risk-map            - GeoJSON FeatureCollection, all counties
    GET /api/v1/county/{county}     - per-county detail + ml_probability
    GET /api/v1/historical-trend    - daily onset trend from BigQuery

All upstream calls (Firestore, Vertex AI, BigQuery) degrade gracefully:
the API returns whichever layers responded. Vertex AI failures in
particular omit ml_probability rather than 5xx-ing the request.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Make project root importable when launched via uvicorn from /app
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from src.config import KENYA_COUNTIES

logger = logging.getLogger("api")
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

# ---------------------------------------------------------------------------
# Run-time configuration (all from env so the image is portable)
# ---------------------------------------------------------------------------
GCP_PROJECT_ID     = os.environ.get("GCP_PROJECT_ID", "sds2412-kenya-onset")
GCP_REGION         = os.environ.get("GCP_REGION", "us-central1")
BQ_DATASET         = os.environ.get("BQ_DATASET", "kenya_onset")
VERTEX_ENDPOINT_ID = os.environ.get("VERTEX_ENDPOINT_ID", "")
MODEL_VERSION      = os.environ.get("MODEL_VERSION", "xgboost_onset_v1")
FIRESTORE_COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "live_forecast")

# Onset thresholds — kept in sync with src/streaming/spark_consumer.py.
ONSET_THRESHOLD_MM = 20.0


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Kenya Rainfall Onset Advisory API",
    description="Dashboard API serving batch + speed + ML risk views.",
    version="0.4.0",
)


# ---------------------------------------------------------------------------
# Lazy upstream clients (constructed on first use, cached at module level)
# ---------------------------------------------------------------------------
_FIRESTORE_CLIENT: Any | None = None
_BQ_CLIENT: Any | None = None
_VERTEX_ENDPOINT: Any | None = None


def _get_firestore_client() -> Any | None:
    """Return a cached Firestore client, or None if the SDK is unavailable.

    Why: local dev / tests may run without google-cloud-firestore installed.
    Returning None lets callers degrade gracefully instead of crashing.
    """
    global _FIRESTORE_CLIENT
    if _FIRESTORE_CLIENT is not None:
        return _FIRESTORE_CLIENT
    try:
        from google.cloud import firestore
        _FIRESTORE_CLIENT = firestore.Client(project=GCP_PROJECT_ID)
        return _FIRESTORE_CLIENT
    except Exception as exc:
        logger.warning("Firestore unavailable: %s", exc)
        return None


def _get_bq_client() -> Any | None:
    """Return a cached BigQuery client, or None if unavailable."""
    global _BQ_CLIENT
    if _BQ_CLIENT is not None:
        return _BQ_CLIENT
    try:
        from google.cloud import bigquery
        _BQ_CLIENT = bigquery.Client(project=GCP_PROJECT_ID)
        return _BQ_CLIENT
    except Exception as exc:
        logger.warning("BigQuery unavailable: %s", exc)
        return None


def _get_vertex_endpoint() -> Any | None:
    """Return a cached Vertex AI Endpoint object, or None if unavailable.

    Returns None when:
      * the aiplatform SDK is not installed (local dev),
      * VERTEX_ENDPOINT_ID env var is empty (deploy_vertex.sh hasn't run yet),
      * SDK init fails for any reason.
    Callers must handle None and proceed without ml_probability.
    """
    global _VERTEX_ENDPOINT
    if _VERTEX_ENDPOINT is not None:
        return _VERTEX_ENDPOINT
    if not VERTEX_ENDPOINT_ID:
        logger.info("VERTEX_ENDPOINT_ID not set; ML layer disabled.")
        return None
    try:
        from google.cloud import aiplatform
        aiplatform.init(project=GCP_PROJECT_ID, location=GCP_REGION)
        _VERTEX_ENDPOINT = aiplatform.Endpoint(VERTEX_ENDPOINT_ID)
        return _VERTEX_ENDPOINT
    except Exception as exc:
        logger.warning("Vertex AI unavailable: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Speed-layer helpers (Firestore live_forecast)
# ---------------------------------------------------------------------------
def _read_live_forecast() -> dict[str, dict]:
    """Pull the latest alert per county from Firestore live_forecast.

    The streaming consumer writes one document per county, keyed by the
    county name, with cum_72hr_mm / onset_flag / alert_timestamp /
    expires_at. Returns {} on any failure so the API stays up even if
    Firestore is offline or the collection is empty.
    """
    client = _get_firestore_client()
    if client is None:
        return {}
    try:
        docs = client.collection(FIRESTORE_COLLECTION).stream()
        return {d.id: d.to_dict() for d in docs}
    except Exception as exc:
        logger.warning("Firestore read failed: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# ML layer (Vertex AI XGBoost)
# ---------------------------------------------------------------------------
# Feature order MUST match analysis/kenya_xgboost_model.py:
#   ['y', 'surface_pressure', 'temp_k', 'temp_lag_6h', 'temp_lag_24h', 'county_cat']
# y is the recent precipitation observation; county_cat is the integer
# code from pandas categorical encoding (alphabetical by county name).
_COUNTY_CAT_INDEX = {name: idx for idx, name in enumerate(sorted(KENYA_COUNTIES.keys()))}


def _build_feature_vector(county: str, fc: dict) -> list[float]:
    """Build a single XGBoost feature vector from a Firestore alert doc.

    Falls back to neutral values when fields are missing — this keeps
    the prediction call shape-correct so the ML layer can still produce
    a baseline score for counties with no live data.
    """
    cum = float(fc.get("cum_72hr_mm", 0.0) or 0.0)
    return [
        cum,                                          # y (recent precipitation)
        float(fc.get("surface_pressure", 1013.0)),    # surface_pressure (hPa)
        float(fc.get("temp_k", 295.0)),               # temp_k (kelvin, ~22C)
        float(fc.get("temp_lag_6h", 295.0)),          # temp_lag_6h
        float(fc.get("temp_lag_24h", 295.0)),         # temp_lag_24h
        float(_COUNTY_CAT_INDEX.get(county, 0)),      # county_cat
    ]


def _normalise_prediction(raw: Any) -> float | None:
    """Coerce a Vertex AI prediction value into a [0, 1] probability.

    The XGBRegressor trained in analysis/kenya_xgboost_model.py returns
    a continuous score — the adaptive risk engine in
    milestone_3_to_6scripts/adaptive_risk_engine.py treats this as a
    probability-like input weighted at 40%. We clamp to [0, 1] here so
    the dashboard's colour scale is well-defined.
    """
    try:
        if isinstance(raw, list):
            raw = raw[0] if raw else None
        if raw is None:
            return None
        v = float(raw)
        if v != v:           # NaN
            return None
        return max(0.0, min(1.0, v))
    except (TypeError, ValueError):
        return None


def _call_vertex_for_counties(features: dict[str, list[float]]) -> dict[str, float]:
    """Score every county in one Vertex AI batch and return county -> probability.

    Returns {} on any failure (SDK missing, endpoint unset, network
    error, malformed response). Callers must treat ml_probability as
    optional in the response — that is the contract per CLAUDE.md M4.
    """
    endpoint = _get_vertex_endpoint()
    if endpoint is None or not features:
        return {}

    counties = list(features.keys())
    instances = [features[c] for c in counties]

    try:
        response = endpoint.predict(instances=instances)
        preds = getattr(response, "predictions", None) or []
    except Exception as exc:
        logger.warning("Vertex AI predict() failed: %s", exc)
        return {}

    out: dict[str, float] = {}
    for county, raw in zip(counties, preds):
        prob = _normalise_prediction(raw)
        if prob is not None:
            out[county] = prob
    return out


# ---------------------------------------------------------------------------
# Risk classification (mirrors adaptive_risk_engine.py thresholds)
# ---------------------------------------------------------------------------
def _risk_level(score: float) -> str:
    """Map a 0-1 risk score to LOW / MODERATE / HIGH bands."""
    if score > 0.7:
        return "HIGH"
    if score > 0.4:
        return "MODERATE"
    return "LOW"


def _county_risk_score(cum_mm: float, ml_prob: float | None) -> float:
    """Blend the speed-layer rule and the ML probability.

    Mirrors adaptive_risk_engine.calculate_nairobi_risk weighting where
    Prophet is unavailable: rule-based gets 0.33, ML gets 0.67. When
    ML is unavailable the rule signal stands alone (binary 0 / 1).
    """
    rule = 1.0 if cum_mm >= ONSET_THRESHOLD_MM else 0.0
    if ml_prob is None:
        return rule
    return (0.33 * rule) + (0.67 * ml_prob)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health() -> dict:
    """Liveness probe consumed by Cloud Run, predemo healthcheck, and locust."""
    return {
        "status":         "healthy",
        "service":        "kenya-onset-api",
        "version":        app.version,
        "time_utc":       datetime.now(timezone.utc).isoformat(),
        "vertex_endpoint": bool(VERTEX_ENDPOINT_ID),
    }


@app.get("/api/v1/risk-map")
def get_risk_map() -> JSONResponse:
    """Return a GeoJSON FeatureCollection of all counties with risk fields.

    Each feature carries:
      * county, lat/lon
      * cum_72hr_mm, alert_timestamp (from Firestore — speed layer)
      * onset_risk_score, onset_risk    (blended)
      * ml_probability                  (only when Vertex AI responded)

    If Vertex AI is unreachable, ml_probability is omitted from every
    feature but the endpoint still returns 200 with the speed-layer
    view — see CLAUDE.md M4 contract.

    Cache-Control: public, max-age=900 (15 minutes) is set so Cloud
    CDN / browser caches absorb dashboard refresh traffic — see M5.
    """
    live = _read_live_forecast()

    # Build feature vectors for every county before calling Vertex,
    # so a single batch predict covers the whole map.
    feature_vectors = {
        county: _build_feature_vector(county, live.get(county, {}))
        for county in KENYA_COUNTIES
    }
    ml_probs = _call_vertex_for_counties(feature_vectors)

    features = []
    for county, coords in KENYA_COUNTIES.items():
        fc = live.get(county, {})
        cum = float(fc.get("cum_72hr_mm", 0.0) or 0.0)
        ml_prob = ml_probs.get(county)
        score = _county_risk_score(cum, ml_prob)

        properties: dict[str, Any] = {
            "county":            county,
            "cum_72hr_mm":       cum,
            "onset_risk_score":  round(score, 4),
            "onset_risk":        _risk_level(score),
            "alert_timestamp":   _iso(fc.get("alert_timestamp")),
        }
        if ml_prob is not None:
            properties["ml_probability"] = round(ml_prob, 4)

        features.append({
            "type": "Feature",
            "geometry": {
                "type":        "Point",
                "coordinates": [coords["lon"], coords["lat"]],
            },
            "properties": properties,
        })

    payload = {
        "type":     "FeatureCollection",
        "metadata": {
            "last_updated_utc": datetime.now(timezone.utc).isoformat(),
            "model_version":    MODEL_VERSION,
            "ml_layer_active":  bool(ml_probs),
            "speed_layer_docs": len(live),
            "feature_count":    len(features),
        },
        "features": features,
    }
    return JSONResponse(
        content=payload,
        headers={"Cache-Control": "public, max-age=900"},
    )


@app.get("/api/v1/county/{county_name}")
def get_county_detail(county_name: str) -> dict:
    """Return the speed + ML view for a single county.

    Raises 404 if the county is unknown. Vertex AI errors leave
    ml_probability null; everything else still returns.
    """
    if county_name not in KENYA_COUNTIES:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown county '{county_name}'. "
                   f"Known: {sorted(KENYA_COUNTIES.keys())}",
        )

    coords = KENYA_COUNTIES[county_name]
    fc     = _read_live_forecast().get(county_name, {})
    cum    = float(fc.get("cum_72hr_mm", 0.0) or 0.0)

    ml_probs = _call_vertex_for_counties(
        {county_name: _build_feature_vector(county_name, fc)}
    )
    ml_prob = ml_probs.get(county_name)
    score   = _county_risk_score(cum, ml_prob)

    return {
        "county":           county_name,
        "lat":              coords["lat"],
        "lon":              coords["lon"],
        "cum_72hr_mm":      cum,
        "onset_flag":       bool(fc.get("onset_flag", False)),
        "onset_risk_score": round(score, 4),
        "onset_risk":       _risk_level(score),
        "ml_probability":   round(ml_prob, 4) if ml_prob is not None else None,
        "alert_timestamp":  _iso(fc.get("alert_timestamp")),
        "model_version":    MODEL_VERSION,
        "last_updated_utc": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/v1/historical-trend")
def get_historical_trend(
    county: str | None = Query(default=None, description="Filter to one county"),
    days:   int        = Query(default=90, ge=1, le=365),
) -> dict:
    """Return a daily onset trend series from BigQuery historical_onset.

    Falls back to an empty series when BigQuery is unavailable so the
    dashboard can render a placeholder chart.
    """
    if county is not None and county not in KENYA_COUNTIES:
        raise HTTPException(
            status_code=404, detail=f"Unknown county '{county}'."
        )

    client = _get_bq_client()
    if client is None:
        return _empty_trend(county, days, reason="bigquery_unavailable")

    where_clauses = [f"DATE(observation_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"]
    params: dict[str, Any] = {}
    if county is not None:
        where_clauses.append("county = @county")
        params["county"] = county

    query = f"""
        SELECT
          DATE(observation_date)        AS day,
          county,
          AVG(cum_72hr_mm)              AS avg_cum_72hr_mm,
          SUM(CAST(onset_flag AS INT64)) AS onset_count
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.historical_onset`
        WHERE {" AND ".join(where_clauses)}
        GROUP BY day, county
        ORDER BY day
    """

    try:
        from google.cloud import bigquery
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(k, "STRING", v)
                for k, v in params.items()
            ]
        )
        rows = list(client.query(query, job_config=job_config).result())
    except Exception as exc:
        logger.warning("BigQuery historical-trend query failed: %s", exc)
        return _empty_trend(county, days, reason="bigquery_query_failed")

    series = [
        {
            "day":             r["day"].isoformat(),
            "county":          r["county"],
            "avg_cum_72hr_mm": float(r["avg_cum_72hr_mm"] or 0.0),
            "onset_count":     int(r["onset_count"] or 0),
        }
        for r in rows
    ]

    return {
        "county":   county,
        "days":     days,
        "rows":     len(series),
        "series":   series,
        "metadata": {
            "source":           f"{GCP_PROJECT_ID}.{BQ_DATASET}.historical_onset",
            "last_updated_utc": datetime.now(timezone.utc).isoformat(),
        },
    }


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _iso(value: Any) -> str | None:
    """Best-effort ISO-8601 stringify for Firestore timestamp fields."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _empty_trend(county: str | None, days: int, reason: str) -> dict:
    """Build a degraded but well-formed historical-trend response."""
    return {
        "county":   county,
        "days":     days,
        "rows":     0,
        "series":   [],
        "metadata": {
            "source":           f"{GCP_PROJECT_ID}.{BQ_DATASET}.historical_onset",
            "degraded_reason":  reason,
            "last_updated_utc": datetime.now(timezone.utc).isoformat(),
        },
    }
