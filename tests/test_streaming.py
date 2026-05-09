"""
test_streaming.py
Author    : R03 - Alexander Kihoi (Streaming & Real-Time Engineer)
Purpose   : Unit tests for CountMinSketch and onset flag logic.
            No Kafka or Spark required - all tests are standalone.
Milestone : M3 - Streaming tests
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.streaming.count_min_sketch import CountMinSketch


# ---------------------------------------------------------------------------
# Count-Min Sketch tests
# ---------------------------------------------------------------------------

def test_cms_no_undercount():
    cms = CountMinSketch(width=500, depth=5, seed=42)
    true_counts = {"Nairobi": 100, "Kisumu": 50, "Nakuru": 25}
    for key, cnt in true_counts.items():
        cms.update(key, cnt)
    for key, true_val in true_counts.items():
        assert cms.query(key) >= true_val, (
            f"Undercount for {key}: got {cms.query(key)} < {true_val}"
        )


def test_cms_update_and_query_single_key():
    cms = CountMinSketch()
    cms.update("Garissa", 42)
    assert cms.query("Garissa") >= 42


def test_cms_zero_or_positive_for_unseen_key():
    cms = CountMinSketch()
    cms.update("Nairobi", 1)
    result = cms.query("UnseenCounty")
    assert result >= 0, "Query result must be non-negative"


def test_cms_top_k_order():
    cms = CountMinSketch(width=1000, depth=5, seed=42)
    counties = [
        "Nairobi", "Kisumu", "Nakuru", "Meru", "Kitui",
        "Garissa", "Machakos", "Eldoret", "Kakamega", "Embu",
    ]
    counts = [500, 300, 250, 150, 100, 80, 60, 40, 25, 10]
    for county, cnt in zip(counties, counts):
        cms.update(county, cnt)
    top5 = cms.top_k(counties, k=5)
    assert len(top5) == 5
    assert top5[0][0] == "Nairobi"


def test_cms_total_count():
    cms = CountMinSketch()
    cms.update("Nairobi", 10)
    cms.update("Kisumu",  5)
    assert cms.total_count == 15


def test_cms_error_bound_is_positive():
    cms = CountMinSketch(width=1000, depth=5)
    assert cms.error_bound() > 0


def test_cms_repr_contains_width_depth():
    cms = CountMinSketch(width=200, depth=3)
    r = repr(cms)
    assert "200" in r and "3" in r


def test_cms_update_increments_total():
    cms = CountMinSketch()
    cms.update("Embu", 7)
    assert cms.total_count == 7
    cms.update("Embu", 3)
    assert cms.total_count == 10


# ---------------------------------------------------------------------------
# Onset flag logic tests (mirrors spark_consumer.py logic)
# ---------------------------------------------------------------------------

def _onset_flag(precip_72hr: float, zero_precip_hours_in_48: int) -> bool:
    """
    Onset detection: rolling_72hr_precip >= 20.0 AND no zero-precip hour in
    last 48 hrs. Mirrors the condition in spark_consumer.py.
    """
    from src.config import ONSET_THRESHOLD_MM
    return precip_72hr >= ONSET_THRESHOLD_MM and zero_precip_hours_in_48 == 0


def test_onset_flag_triggered():
    assert _onset_flag(25.0, 0) is True


def test_onset_flag_not_triggered_low_rain():
    assert _onset_flag(10.0, 0) is False


def test_onset_flag_not_triggered_dry_spell():
    assert _onset_flag(30.0, 5) is False


def test_onset_flag_exact_threshold():
    assert _onset_flag(20.0, 0) is True


def test_onset_flag_just_below_threshold():
    assert _onset_flag(19.99, 0) is False
