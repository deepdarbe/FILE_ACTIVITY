"""Tests for capacity / quota forecasting (issue #113).

Pure-Python OLS regression — these tests pin the contract documented in
``src/reports/forecast.py``:

* n<3 returns ``predicted == last_value`` (no extrapolation)
* alarm date when growth crosses the threshold
* no alarm when slope <= 0
* alarm == today when current size already exceeds threshold
* high R² for clean linear data, low R² for random data
* /api/forecast/{id} smoke test on a TestClient-built mini app
"""

from __future__ import annotations

import math
import os
import random
import sys
from datetime import datetime, timedelta

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.reports.forecast import ForecastResult, forecast_growth  # noqa: E402


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _series_linear(n: int, start_bytes: int, daily_bytes: int,
                    start_dt: datetime | None = None,
                    interval_days: int = 1) -> list:
    """Build a perfectly-linear scan history."""
    if start_dt is None:
        start_dt = datetime(2025, 1, 1, 12, 0, 0)
    return [
        {
            "started_at": (start_dt + timedelta(days=i * interval_days)).isoformat(),
            "total_size_bytes": start_bytes + daily_bytes * i,
        }
        for i in range(n)
    ]


def _series_random(n: int, baseline: int, jitter: int, seed: int = 42) -> list:
    rng = random.Random(seed)
    start_dt = datetime(2025, 1, 1, 12, 0, 0)
    return [
        {
            "started_at": (start_dt + timedelta(days=i)).isoformat(),
            "total_size_bytes": baseline + rng.randint(-jitter, jitter),
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Issue #113 required tests
# ---------------------------------------------------------------------------


def test_forecast_with_3_points_returns_real_prediction():
    """Three perfectly-linear points: predicted == intercept + slope*horizon_x.

    Series: 100, 200, 300 GiB on consecutive days.
    Slope = 100 GiB/day, intercept = 100 GiB at x=0.
    Horizon 30 days from last (x=2): predicted = 100 + 100*32 = 3300 GiB.
    """
    GiB = 1024 ** 3
    rows = _series_linear(3, start_bytes=100 * GiB, daily_bytes=100 * GiB)
    r = forecast_growth(rows, horizon_days=30, source_id=7)

    assert isinstance(r, ForecastResult)
    assert r.samples_used == 3
    # Slope ≈ 100 GiB/day.
    assert math.isclose(r.slope_bytes_per_day, 100 * GiB, rel_tol=1e-9)
    # Predicted size at horizon (last x = 2, plus 30 days = 32):
    expected = 100 * GiB + 100 * GiB * 32
    assert math.isclose(r.predicted_bytes, expected, rel_tol=1e-9)
    # Perfect line -> r2 == 1.0 and residual_std == 0 -> CI collapses.
    assert math.isclose(r.r_squared, 1.0, rel_tol=1e-9)
    assert math.isclose(r.ci_low_bytes, expected, rel_tol=1e-9)
    assert math.isclose(r.ci_high_bytes, expected, rel_tol=1e-9)
    # History is materialised for the chart.
    assert len(r.history) == 3
    assert r.source_id == 7


def test_forecast_with_2_points_returns_last_value():
    """Two points fit a line trivially — pretending that's a real trend
    leads to disastrous extrapolation. The contract is: hold the last value."""
    rows = _series_linear(2, start_bytes=10_000, daily_bytes=5_000)
    r = forecast_growth(rows, horizon_days=180)

    assert r.samples_used == 2
    last_value = rows[-1]["total_size_bytes"]
    assert r.predicted_bytes == float(last_value)
    assert r.ci_low_bytes == float(last_value)
    assert r.ci_high_bytes == float(last_value)
    assert r.r_squared == 0.0
    assert r.capacity_alarm_at is None
    # History still includes the two points so the chart can render them.
    assert len(r.history) == 2


def test_forecast_capacity_alarm_when_growth_crosses_threshold():
    """Linear growth of 10 GiB/day from 100 GiB; threshold 200 GiB →
    alarm hits ~10 days from start."""
    GiB = 1024 ** 3
    rows = _series_linear(5, start_bytes=100 * GiB, daily_bytes=10 * GiB)
    threshold = 200 * GiB

    r = forecast_growth(rows, horizon_days=30, capacity_threshold_bytes=threshold)
    assert r.capacity_alarm_at is not None
    # The 5 rows span days 0..4. Threshold of 200 GiB is hit at day 10
    # (100 + 10*10 = 200) which is in the future relative to last (x=4).
    alarm_date = datetime.fromisoformat(r.capacity_alarm_at)
    first_dt = datetime(2025, 1, 1, 12, 0, 0)
    days_offset = (alarm_date - first_dt.replace(hour=0, minute=0, second=0)).days
    # Allow ±1 day for date-floor rounding.
    assert 9 <= days_offset <= 11, (
        f"alarm should be ~10 days from start, got {days_offset} ({alarm_date})"
    )


def test_forecast_no_alarm_when_growth_decreases():
    """Negative slope (storage shrinking, e.g. cleanup running) should
    NEVER raise an alarm — the projection only goes down."""
    rows = _series_linear(6, start_bytes=500_000_000, daily_bytes=-10_000_000)
    r = forecast_growth(rows, horizon_days=180,
                        capacity_threshold_bytes=1_000_000_000)
    assert r.slope_bytes_per_day < 0
    assert r.capacity_alarm_at is None


def test_forecast_no_alarm_when_threshold_already_exceeded():
    """If the latest scan is already over the threshold, alarm is today
    (operators want this surfaced immediately, not buried in the future)."""
    rows = _series_linear(3, start_bytes=1_500_000_000, daily_bytes=1_000_000)
    r = forecast_growth(rows, horizon_days=30,
                        capacity_threshold_bytes=1_000_000_000)
    assert r.capacity_alarm_at == datetime.utcnow().date().isoformat()


def test_forecast_r_squared_high_for_linear_data():
    rows = _series_linear(20, start_bytes=10 ** 9, daily_bytes=5 * 10 ** 7)
    r = forecast_growth(rows, horizon_days=90)
    assert r.samples_used == 20
    assert r.r_squared > 0.999, f"perfectly linear should give r2 ~ 1.0, got {r.r_squared}"


def test_forecast_r_squared_low_for_random_data():
    rows = _series_random(40, baseline=10 ** 9, jitter=10 ** 8, seed=1)
    r = forecast_growth(rows, horizon_days=90)
    assert r.samples_used == 40
    # Random noise around a flat baseline — slope ≈ 0, fit explains very little.
    assert r.r_squared < 0.20, (
        f"random data should give low r2, got {r.r_squared}"
    )


# ---------------------------------------------------------------------------
# Manual fixture: 30-point linear series, prediction within 5%
# ---------------------------------------------------------------------------


def test_forecast_30_point_linear_within_5_percent():
    """Issue #113 'manual fixture' check: 30-point linear series, the
    predicted value must be within 5% of the analytic expected value."""
    GiB = 1024 ** 3
    start = 50 * GiB
    daily = 2 * GiB
    rows = _series_linear(30, start_bytes=start, daily_bytes=daily)
    horizon = 60
    r = forecast_growth(rows, horizon_days=horizon)
    # last x = 29, horizon_x = 89  -> expected = start + daily * 89
    expected = start + daily * (29 + horizon)
    err = abs(r.predicted_bytes - expected) / expected
    assert err < 0.05, f"prediction off by {err*100:.2f}% (expected {expected}, got {r.predicted_bytes})"


# ---------------------------------------------------------------------------
# API smoke test
# ---------------------------------------------------------------------------


def _build_mini_app(scan_history_by_source):
    """A minimal FastAPI app that mirrors the dashboard's
    /api/forecast/{source_id} contract — used to smoke-test the JSON shape
    without standing up the full create_app() machinery."""
    app = FastAPI()

    @app.get("/api/forecast/{source_id}")
    async def forecast(source_id: int, horizon_days: int = 180,
                       threshold_bytes: int | None = None):
        rows = scan_history_by_source.get(source_id)
        if rows is None:
            raise HTTPException(404, "source bulunamadi")
        result = forecast_growth(
            rows, horizon_days=horizon_days,
            capacity_threshold_bytes=threshold_bytes,
            source_id=source_id,
        )
        return result.to_dict()

    return app


def test_api_forecast_returns_expected_shape():
    GiB = 1024 ** 3
    rows = _series_linear(15, start_bytes=10 * GiB, daily_bytes=int(0.5 * GiB))
    app = _build_mini_app({1: rows, 2: []})
    client = TestClient(app)

    r = client.get("/api/forecast/1?horizon_days=90")
    assert r.status_code == 200
    body = r.json()
    # Required fields
    for key in (
        "source_id", "horizon_days", "predicted_bytes", "ci_low_bytes",
        "ci_high_bytes", "samples_used", "r_squared", "capacity_alarm_at",
        "history",
    ):
        assert key in body, f"missing {key}"
    assert body["source_id"] == 1
    assert body["horizon_days"] == 90
    assert body["samples_used"] == 15
    assert isinstance(body["history"], list)
    assert len(body["history"]) == 15
    for pt in body["history"]:
        assert "ts" in pt and "bytes" in pt
    # alarm not requested -> always None
    assert body["capacity_alarm_at"] is None

    # Empty source -> n=0 result
    r = client.get("/api/forecast/2?horizon_days=30")
    assert r.status_code == 200
    body = r.json()
    assert body["samples_used"] == 0
    assert body["history"] == []


def test_api_forecast_with_threshold_query_param():
    """Threshold passed via query string should drive the alarm field."""
    GiB = 1024 ** 3
    rows = _series_linear(10, start_bytes=200 * GiB, daily_bytes=20 * GiB)
    app = _build_mini_app({5: rows})
    client = TestClient(app)

    threshold = 500 * GiB
    r = client.get(f"/api/forecast/5?horizon_days=60&threshold_bytes={threshold}")
    assert r.status_code == 200
    body = r.json()
    assert body["capacity_alarm_at"] is not None  # 200 -> 380 by day 9, 500 around day 15


# ---------------------------------------------------------------------------
# Defensive parsing
# ---------------------------------------------------------------------------


def test_forecast_handles_total_size_alias():
    """The DB column is ``total_size`` (not ``total_size_bytes``). The
    helper must accept either."""
    rows = []
    base = datetime(2025, 1, 1)
    for i in range(5):
        rows.append({
            "started_at": (base + timedelta(days=i)).isoformat(),
            "total_size": 1_000 + 100 * i,
        })
    r = forecast_growth(rows, horizon_days=10)
    assert r.samples_used == 5
    assert r.slope_bytes_per_day == pytest.approx(100.0, rel=1e-9)


def test_forecast_handles_descending_input():
    """Caller may pass DESC-ordered rows — internally we re-sort."""
    rows = _series_linear(5, start_bytes=1_000_000, daily_bytes=10_000)
    rows_desc = list(reversed(rows))
    r_asc = forecast_growth(rows, horizon_days=10)
    r_desc = forecast_growth(rows_desc, horizon_days=10)
    assert math.isclose(r_asc.predicted_bytes, r_desc.predicted_bytes, rel_tol=1e-9)
    assert math.isclose(r_asc.slope_bytes_per_day, r_desc.slope_bytes_per_day,
                        rel_tol=1e-9)


def test_forecast_handles_sqlite_string_format():
    """SQLite stores started_at as 'YYYY-MM-DD HH:MM:SS' (space, no T)."""
    rows = [
        {"started_at": "2025-01-01 12:00:00", "total_size_bytes": 100},
        {"started_at": "2025-01-02 12:00:00", "total_size_bytes": 200},
        {"started_at": "2025-01-03 12:00:00", "total_size_bytes": 300},
        {"started_at": "2025-01-04 12:00:00", "total_size_bytes": 400},
    ]
    r = forecast_growth(rows, horizon_days=5)
    assert r.samples_used == 4
    assert r.slope_bytes_per_day == pytest.approx(100.0, rel=1e-9)
