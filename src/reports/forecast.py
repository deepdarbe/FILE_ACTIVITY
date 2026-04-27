"""Quota / capacity forecasting via linear regression on ``scan_runs`` history
(issue #113).

Given a list of ``scan_runs`` rows for a single source ordered ascending by
``started_at`` (each carries ``total_size_bytes``), this module computes a
plain ordinary-least-squares (OLS) linear regression in pure Python (stdlib
only — no numpy / pandas), projects forward by ``horizon_days`` and returns
a ``ForecastResult`` dataclass with:

* predicted size at the horizon (intercept + slope * horizon_x)
* a 95% confidence interval, derived from the residual standard error
  multiplied by the conservative z-score 1.96 (we use 1.96 instead of a
  full t-table — for n=3 the CI underestimates the true uncertainty, but
  capacity-planning callers care more about *direction* than exact widths,
  and 1.96 is the same constant used by ``/api/system/health`` ops widgets)
* ``capacity_alarm_at``: ISO date when the projection line crosses
  ``capacity_threshold_bytes`` (or None if it never does within +10 yr)
* ``r_squared``: a quick goodness-of-fit hint for the dashboard

Edge cases:

* ``n < 3``: do NOT extrapolate. Return ``predicted == ci_low == ci_high ==
  last_value`` with ``r_squared = 0`` and ``capacity_alarm_at = None``.
  Two points fit a line with zero residuals — pretending that's a real
  trend leads to wildly wrong projections.
* All-equal y: slope = 0, predicted = last_value, alarm only if last_value
  >= threshold (in which case alarm = today).
* Negative slope (storage shrinking): never raises an alarm.
"""

from __future__ import annotations

import logging
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("file_activity.reports.forecast")

# 1.96 ≈ 97.5th-percentile of the standard normal — a conservative stand-in
# for the t-distribution at large n. For n in [3, 30] this *understates* the
# true CI; we accept that since the report is read-only and the alarm logic
# uses the point estimate, not the bound.
_Z_95 = 1.96
# Cap the alarm search at 10 years out — anything further is noise.
_MAX_ALARM_DAYS = 365 * 10


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class ForecastResult:
    """Capacity forecast for a single source.

    The frontend renders ``history`` directly as the time-series, then draws
    a projection cone using ``ci_low_bytes`` / ``ci_high_bytes`` at the
    horizon. ``capacity_alarm_at`` drives the KPI banner.
    """

    source_id: int
    horizon_days: int
    predicted_bytes: float
    ci_low_bytes: float
    ci_high_bytes: float
    samples_used: int
    r_squared: float
    capacity_alarm_at: Optional[str]  # ISO date or None
    history: List[Dict[str, Any]] = field(default_factory=list)
    # Diagnostic fields — useful for debugging but not load-bearing for the UI.
    slope_bytes_per_day: float = 0.0
    intercept_bytes: float = 0.0
    last_bytes: float = 0.0
    last_ts: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Normalise floats — JSON does not handle NaN/Infinity.
        for k in (
            "predicted_bytes", "ci_low_bytes", "ci_high_bytes", "r_squared",
            "slope_bytes_per_day", "intercept_bytes", "last_bytes",
        ):
            v = d.get(k)
            if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                d[k] = 0.0
        return d


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def forecast_growth(
    scan_history: List[Dict[str, Any]],
    horizon_days: int,
    capacity_threshold_bytes: Optional[int] = None,
    *,
    source_id: int = 0,
) -> ForecastResult:
    """Linear-regression forecast over ``scan_history``.

    ``scan_history`` is a list of dicts with at least ``started_at`` (ISO
    timestamp string OR datetime) and ``total_size_bytes`` (int / float).
    Caller is responsible for sorting ascending — but we re-sort defensively
    because the chargeback-style helpers usually return DESC.

    Time axis: days since the first sample (float, fractional days
    preserved). The predicted point is at ``last_x + horizon_days``.
    """
    horizon_days = max(1, int(horizon_days or 0))

    points = _normalise_history(scan_history)
    n = len(points)

    if n == 0:
        # Nothing to project from. Return an empty-shape result.
        return ForecastResult(
            source_id=int(source_id),
            horizon_days=horizon_days,
            predicted_bytes=0.0,
            ci_low_bytes=0.0,
            ci_high_bytes=0.0,
            samples_used=0,
            r_squared=0.0,
            capacity_alarm_at=None,
            history=[],
        )

    # Always materialise the history payload first — even when n<3 we still
    # want the chart to draw the points.
    history_payload = [
        {
            "ts": _iso(p["dt"]),
            "bytes": int(p["bytes"]),
        }
        for p in points
    ]
    last = points[-1]
    last_bytes = float(last["bytes"])

    if n < 3:
        # Not enough samples for a meaningful regression. Hold steady at
        # the last value; let the UI surface "yetersiz veri" rather than
        # rendering a ramp from two points.
        alarm_iso = None
        if (
            capacity_threshold_bytes is not None
            and last_bytes >= float(capacity_threshold_bytes)
        ):
            # Already exceeding the threshold — surface that immediately.
            alarm_iso = _today_iso()
        return ForecastResult(
            source_id=int(source_id),
            horizon_days=horizon_days,
            predicted_bytes=last_bytes,
            ci_low_bytes=last_bytes,
            ci_high_bytes=last_bytes,
            samples_used=n,
            r_squared=0.0,
            capacity_alarm_at=alarm_iso,
            history=history_payload,
            slope_bytes_per_day=0.0,
            intercept_bytes=last_bytes,
            last_bytes=last_bytes,
            last_ts=history_payload[-1]["ts"] if history_payload else None,
        )

    # ── OLS regression on (x = days since first sample, y = bytes) ──
    t0 = points[0]["dt"]
    xs = [_days_between(t0, p["dt"]) for p in points]
    ys = [float(p["bytes"]) for p in points]

    slope, intercept, r2, residual_std = _ols(xs, ys)

    last_x = xs[-1]
    horizon_x = last_x + float(horizon_days)
    predicted = intercept + slope * horizon_x

    # 95% CI via residual std error * z-score. This is a *conservative*
    # surrogate for a proper prediction interval — it ignores the leverage
    # term (1 + 1/n + (x* - mean_x)^2/Sxx). For stable historical series
    # the leverage term is small at the right edge of the data; the
    # operational use case (capacity alarm) only needs order-of-magnitude.
    half_width = _Z_95 * residual_std
    ci_low = max(0.0, predicted - half_width)
    ci_high = predicted + half_width

    # ── capacity_alarm_at ─────────────────────────────────────────────
    alarm_iso = _solve_alarm_date(
        slope=slope,
        intercept=intercept,
        last_x=last_x,
        threshold=capacity_threshold_bytes,
        last_bytes=last_bytes,
        t0=t0,
    )

    return ForecastResult(
        source_id=int(source_id),
        horizon_days=horizon_days,
        predicted_bytes=predicted,
        ci_low_bytes=ci_low,
        ci_high_bytes=ci_high,
        samples_used=n,
        r_squared=r2,
        capacity_alarm_at=alarm_iso,
        history=history_payload,
        slope_bytes_per_day=slope,
        intercept_bytes=intercept,
        last_bytes=last_bytes,
        last_ts=history_payload[-1]["ts"] if history_payload else None,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_history(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Coerce + sort + de-duplicate raw scan_runs rows.

    Accepts either ``total_size_bytes`` (preferred field name from the
    issue) or ``total_size`` (the actual column name on ``scan_runs``).
    """
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        ts = (
            r.get("started_at")
            or r.get("ts")
            or r.get("date")
            or r.get("completed_at")
        )
        size = (
            r.get("total_size_bytes")
            if r.get("total_size_bytes") is not None
            else r.get("total_size")
        )
        if ts is None or size is None:
            continue
        try:
            dt = _to_datetime(ts)
        except Exception:
            continue
        try:
            b = float(size)
        except (TypeError, ValueError):
            continue
        if b < 0:
            continue
        out.append({"dt": dt, "bytes": b})
    # Sort ascending by timestamp (stable). Drop rows where two scans
    # share the same timestamp by keeping the larger value (defensive —
    # a re-run of the same scan should not zero-slope the regression).
    out.sort(key=lambda x: x["dt"])
    if not out:
        return out
    deduped: List[Dict[str, Any]] = [out[0]]
    for row in out[1:]:
        if row["dt"] == deduped[-1]["dt"]:
            if row["bytes"] > deduped[-1]["bytes"]:
                deduped[-1] = row
        else:
            deduped.append(row)
    return deduped


def _ols(xs: List[float], ys: List[float]) -> tuple:
    """Plain ordinary least squares — returns (slope, intercept, r2, residual_std).

    Uses the textbook formulation with a single pass over the data:
        slope = Sxy / Sxx
        intercept = mean_y - slope * mean_x
        r2 = 1 - SSres / SStot
        residual_std = sqrt(SSres / (n - 2))

    All calls here have n >= 3 (the n<2 branch is handled earlier).
    """
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    sxx = 0.0
    sxy = 0.0
    syy = 0.0
    for x, y in zip(xs, ys):
        dx = x - mean_x
        dy = y - mean_y
        sxx += dx * dx
        sxy += dx * dy
        syy += dy * dy
    if sxx == 0:
        # All x's identical — degenerate; treat as flat series.
        return 0.0, mean_y, 0.0, 0.0
    slope = sxy / sxx
    intercept = mean_y - slope * mean_x

    # Residuals
    ss_res = 0.0
    for x, y in zip(xs, ys):
        y_pred = intercept + slope * x
        diff = y - y_pred
        ss_res += diff * diff
    if syy == 0:
        # All y's identical — perfect fit on a flat line. r2 is conventionally
        # undefined here; report 1.0 (model explains everything trivially).
        r2 = 1.0
    else:
        r2 = 1.0 - (ss_res / syy)
        # Numerical noise can push this slightly outside [0,1].
        if r2 < 0:
            r2 = 0.0
        elif r2 > 1:
            r2 = 1.0
    if n > 2:
        residual_std = math.sqrt(ss_res / (n - 2))
    else:
        residual_std = 0.0
    return slope, intercept, r2, residual_std


def _solve_alarm_date(
    *,
    slope: float,
    intercept: float,
    last_x: float,
    threshold: Optional[float],
    last_bytes: float,
    t0: datetime,
) -> Optional[str]:
    """Solve ``intercept + slope * t = threshold`` for ``t`` (in days since
    t0), then map back to an ISO date.

    Returns None when:
      * threshold is None or non-positive
      * slope <= 0 AND last value below threshold (storage flat / shrinking)
      * the crossing is more than ``_MAX_ALARM_DAYS`` in the future

    Returns today's ISO date when the threshold is already exceeded —
    callers should surface this as an immediate alert.
    """
    if threshold is None:
        return None
    try:
        thr = float(threshold)
    except (TypeError, ValueError):
        return None
    if thr <= 0:
        return None

    # Already at or above threshold — alarm is today.
    if last_bytes >= thr:
        return _today_iso()

    if slope <= 0:
        # Flat or decreasing series — the projection never reaches the
        # threshold within reasonable time.
        return None

    # intercept + slope * t_cross = thr  ->  t_cross = (thr - intercept) / slope
    t_cross = (thr - intercept) / slope
    days_from_last = t_cross - last_x
    if days_from_last <= 0:
        # The fitted line *already* says we should be over the threshold —
        # surface it as today rather than confusing callers with a past date.
        return _today_iso()
    if days_from_last > _MAX_ALARM_DAYS:
        return None
    alarm_dt = (t0 + timedelta(days=t_cross)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return alarm_dt.date().isoformat()


def _to_datetime(ts: Any) -> datetime:
    """Coerce ``ts`` (datetime / ISO string / SQLite ``YYYY-MM-DD HH:MM:SS``
    string) to a naive datetime. We strip timezone info — all maths here
    is delta-based, so the absolute reference frame does not matter."""
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=None)
    if not isinstance(ts, str):
        raise TypeError(f"unsupported timestamp type: {type(ts)!r}")
    s = ts.strip()
    if not s:
        raise ValueError("empty timestamp")
    # Try ISO 8601 first (handles 'T' separator + timezone suffix).
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        pass
    # SQLite default format: 'YYYY-MM-DD HH:MM:SS'
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"unparseable timestamp: {ts!r}")


def _days_between(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds() / 86400.0


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def _today_iso() -> str:
    return datetime.utcnow().date().isoformat()
