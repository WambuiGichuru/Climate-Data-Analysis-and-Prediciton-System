"""
test_batch.py
Author    : R02 - Ashley (Batch Processing Engineer)
Purpose   : Unit tests for WMO onset date detection logic using synthetic
            pandas DataFrames. No Spark cluster required.
Milestone : M2 - Spark Batch Processing tests
"""
from __future__ import annotations
import sys, datetime
from pathlib import Path

import pandas as pd
import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.batch.noaa_spark_processor import compute_onset_dates_pandas


def _make_pdf(station_id, start_date, daily_vals):
    rows = []
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    for i, v in enumerate(daily_vals):
        rows.append({
            "station_id": station_id,
            "date": str(start + datetime.timedelta(days=i)),
            "value": v,
        })
    pdf = pd.DataFrame(rows)
    pdf["date"] = pd.to_datetime(pdf["date"])
    return pdf


@pytest.fixture
def rainy_mam_pdf():
    # 3mm/day for first 10 days -> 30mm cumulative, onset in MAM 2020
    vals = [3.0] * 10 + [0.5] * 80
    return _make_pdf("KE01", "2020-03-01", vals)


@pytest.fixture
def dry_pdf():
    # All zeros - no onset should be detected
    vals = [0.0] * 90
    return _make_pdf("KE02", "2020-03-01", vals)


def test_onset_detected_in_mam(rainy_mam_pdf):
    result = compute_onset_dates_pandas(rainy_mam_pdf)
    assert not result.empty, "Expected onset to be detected for 3mm/day rain"
    assert result.iloc[0]["season"] == "MAM"


def test_no_onset_for_dry_station(dry_pdf):
    result = compute_onset_dates_pandas(dry_pdf)
    assert result.empty, "No onset should occur for a station with zero rainfall"


def test_onset_columns_present(rainy_mam_pdf):
    result = compute_onset_dates_pandas(rainy_mam_pdf)
    if not result.empty:
        required = {"station_id", "year", "season", "onset_date", "cum_precip_at_onset"}
        assert required.issubset(set(result.columns))


def test_cum_precip_meets_threshold(rainy_mam_pdf):
    result = compute_onset_dates_pandas(rainy_mam_pdf)
    if not result.empty:
        assert (result["cum_precip_at_onset"] >= 20.0).all()


def test_combined_stations(rainy_mam_pdf, dry_pdf):
    combined = pd.concat([rainy_mam_pdf, dry_pdf], ignore_index=True)
    result = compute_onset_dates_pandas(combined)
    # Only KE01 should have an onset
    if not result.empty:
        assert "KE01" in result["station_id"].values
        assert "KE02" not in result["station_id"].values
