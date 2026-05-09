"""
test_ingest.py
Author    : R01 — Dennis (Data Ingestion Engineer)
Purpose   : Unit tests for data_validator.py using synthetic DataFrames.
            All tests run without network access — HTTP calls are mocked.
Milestone : M1 — Data Ingestion tests
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.ingest.data_validator import (
    check_completeness,
    check_value_ranges,
    generate_report,
    DataValidationError,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def clean_df() -> pd.DataFrame:
    """10 years of daily data with no gaps and valid values."""
    dates = pd.date_range("2010-01-01", "2019-12-31", freq="D")
    return pd.DataFrame({
        "date":           dates,
        "precipitation_sum": [2.5] * len(dates),
        "temperature_2m_max": [28.0] * len(dates),
        "temperature_2m_min": [15.0] * len(dates),
    })


@pytest.fixture
def gap_df() -> pd.DataFrame:
    """DataFrame with a 60-day gap in the middle."""
    part1 = pd.date_range("2010-01-01", "2010-06-01", freq="D")
    part2 = pd.date_range("2010-08-01", "2010-12-31", freq="D")
    dates = list(part1) + list(part2)
    return pd.DataFrame({
        "date":              dates,
        "precipitation_sum": [1.0] * len(dates),
    })


@pytest.fixture
def bad_range_df() -> pd.DataFrame:
    """DataFrame with out-of-range precipitation and temperature."""
    dates = pd.date_range("2010-01-01", periods=5, freq="D")
    return pd.DataFrame({
        "date":              dates,
        "precipitation_sum": [-5.0, 600.0, 2.0, 3.0, 1.0],
        "temperature_2m_max": [50.0, 28.0, 28.0, 28.0, 28.0],
    })


@pytest.fixture
def high_null_df() -> pd.DataFrame:
    """DataFrame where > 20% of precipitation values are null."""
    dates = pd.date_range("2010-01-01", periods=10, freq="D")
    vals  = [None, None, None, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0]
    return pd.DataFrame({"date": dates, "precipitation_sum": vals})


# ---------------------------------------------------------------------------
# check_completeness
# ---------------------------------------------------------------------------

def test_no_gaps_in_clean_data(clean_df):
    gaps = check_completeness(clean_df, date_col="date", gap_days=30)
    assert gaps == [], f"Expected no gaps, got {gaps}"


def test_detects_60_day_gap(gap_df):
    gaps = check_completeness(gap_df, date_col="date", gap_days=30)
    assert len(gaps) == 1
    assert gaps[0]["gap_days"] > 30


def test_no_date_column_returns_empty():
    df = pd.DataFrame({"value": [1, 2, 3]})
    assert check_completeness(df) == []


# ---------------------------------------------------------------------------
# check_value_ranges
# ---------------------------------------------------------------------------

def test_clean_df_no_warnings(clean_df):
    warnings = check_value_ranges(clean_df)
    assert warnings == []


def test_bad_range_returns_warnings(bad_range_df):
    warnings = check_value_ranges(bad_range_df)
    assert len(warnings) >= 1


def test_negative_precip_flagged():
    df = pd.DataFrame({"date": ["2010-01-01"], "precipitation_sum": [-1.0]})
    warnings = check_value_ranges(df)
    assert any("precipitation" in w or "precip" in w.lower() for w in warnings)


# ---------------------------------------------------------------------------
# generate_report
# ---------------------------------------------------------------------------

def test_generate_report_clean(clean_df):
    report = generate_report(clean_df)
    assert report["rows"] == len(clean_df)
    assert report["date_range"]["min"] == "2010-01-01"
    assert report["date_range"]["max"] == "2019-12-31"
    assert report["gaps"] == []
    assert report["range_warnings"] == []


def test_generate_report_raises_on_high_nulls(high_null_df):
    with pytest.raises(DataValidationError):
        generate_report(high_null_df)


def test_generate_report_with_gaps(gap_df):
    report = generate_report(gap_df)
    assert len(report["gaps"]) >= 1
