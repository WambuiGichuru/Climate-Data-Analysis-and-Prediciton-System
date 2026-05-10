"""
locustfile.py
Author    : R05 - Faith Gichuru
Milestone : M5 - Load testing the dashboard API

Run against a locally running uvicorn or the deployed Cloud Run URL:

    locust -f locustfile.py --host http://localhost:8080
    locust -f locustfile.py --host https://kenya-onset-api-xxxxx.a.run.app

The two tasks below model the dashboard's expected traffic mix:
the front page hits /risk-map on every refresh, while drilling into
a single county is rarer. Weights (3:1) reflect that ratio so the
load profile resembles real user behaviour rather than uniform RPS.
"""
from __future__ import annotations

import random

from locust import HttpUser, between, task

# Same county set as src/config.py — kept hard-coded here so locust
# can run from a clean checkout without importing the project.
COUNTIES = [
    "Nairobi", "Kisumu", "Nakuru", "Meru", "Kitui",
    "Garissa", "Machakos", "Eldoret", "Kakamega", "Embu",
]


class DashboardUser(HttpUser):
    """Simulated dashboard visitor.

    wait_time of 1-3 seconds approximates a human glancing at the map,
    panning, and occasionally clicking into a county detail panel.
    """

    wait_time = between(1, 3)

    @task(3)
    def fetch_risk_map(self) -> None:
        """Most common request: full risk-map refresh.

        Tests the heavy path that fans out to Firestore + Vertex AI.
        Cache-Control: max-age=900 means a real CDN should absorb most
        of this traffic; locust deliberately bypasses caches by issuing
        fresh requests so we can measure origin latency.
        """
        self.client.get("/api/v1/risk-map", name="GET /risk-map")

    @task(1)
    def fetch_county_detail(self) -> None:
        """Rarer request: drill into a single county.

        Tests the per-county Vertex AI call path with a payload of one
        instance, which is the worst case for per-request overhead.
        """
        county = random.choice(COUNTIES)
        self.client.get(
            f"/api/v1/county/{county}",
            name="GET /county/{name}",   # group all counties into one row
        )
