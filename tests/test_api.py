"""
test_api.py
Author    : R05 - Faith (DevOps & Deployment Engineer)
Purpose   : FastAPI TestClient tests for /health, /counties, /onset/{county}.
            Mock the ML model so tests pass without model files.
Milestone : M5 - API tests
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from src.api.main import app
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

def test_health_returns_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "timestamp" in data


# ---------------------------------------------------------------------------
# /counties
# ---------------------------------------------------------------------------

def test_counties_returns_list(client):
    resp = client.get("/counties")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 10


def test_counties_have_required_fields(client):
    resp = client.get("/counties")
    for county in resp.json():
        assert "name" in county
        assert "lat"  in county
        assert "lon"  in county


def test_counties_includes_nairobi(client):
    resp = client.get("/counties")
    names = [c["name"] for c in resp.json()]
    assert "Nairobi" in names


# ---------------------------------------------------------------------------
# /onset/{county}
# ---------------------------------------------------------------------------

def test_onset_nairobi(client):
    resp = client.get("/onset/Nairobi")
    assert resp.status_code == 200
    data = resp.json()
    assert data["county"] == "Nairobi"
    assert 0.0 <= data["onset_probability"] <= 1.0
    assert data["alert_level"] in ("LOW", "MODERATE", "HIGH", "WATCH")


def test_onset_unknown_county_returns_404(client):
    resp = client.get("/onset/UnknownCounty")
    assert resp.status_code == 404


def test_onset_all_valid_counties(client):
    from src.config import COUNTY_NAMES
    for county in COUNTY_NAMES:
        resp = client.get(f"/onset/{county}")
        assert resp.status_code == 200, f"Expected 200 for {county}, got {resp.status_code}"


# ---------------------------------------------------------------------------
# /alerts/live
# ---------------------------------------------------------------------------

def test_alerts_live_returns_list(client):
    resp = client.get("/alerts/live")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
