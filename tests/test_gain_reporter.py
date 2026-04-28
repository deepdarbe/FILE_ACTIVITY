"""Tests for ``src/storage/gain_reporter.py`` (issue #83).

Covers the generic "kazanim raporu" helper that captures before /
after / delta snapshots around any write operation. Duplicate cleaner
is the first user; the unit tests here exercise the helper in
isolation with a freshly-built SQLite fixture.

Coverage targets:
  * delta math: positive when corpus shrinks
  * save() returns the inserted row id
  * save() round-trips before/after/delta JSON cleanly
  * save() persists source_id + audit_event_id + quarantine_path
  * list_reports filters by operation, source_id, paginates
  * count_reports matches the same filters
  * get_report returns None when missing
  * compute_delta tolerates non-numeric / asymmetric keys
  * concurrent saves of different operations don't collide
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.storage.gain_reporter import GainReporter  # noqa: E402


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────


def _make_db(tmp_path: Path) -> Database:
    """Fresh SQLite DB + a single source + a completed scan_run row so
    snapshots have a scan_id to anchor on. The corpus stays empty
    intentionally — these tests exercise the reporter, not the
    storage-level metric queries."""
    db = Database({"path": str(tmp_path / "gr.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (id, name, unc_path) VALUES (1, ?, ?)",
            ("src1", str(tmp_path / "share")),
        )
        cur.execute(
            "INSERT INTO scan_runs (id, source_id, status) "
            "VALUES (1, 1, 'completed')"
        )
    return db


# ──────────────────────────────────────────────
# Delta math
# ──────────────────────────────────────────────


def test_delta_math_positive_on_shrink():
    """before - after; positive numbers mean we shed N units."""
    before = {"total_files": 100, "duplicate_waste_gb": 1.5,
              "total_size_bytes": 1024}
    after = {"total_files": 90, "duplicate_waste_gb": 1.1,
             "total_size_bytes": 512}
    delta = GainReporter.compute_delta(before, after)
    assert delta["total_files"] == 10
    assert delta["duplicate_waste_gb"] == pytest.approx(0.4, rel=1e-3)
    assert delta["total_size_bytes"] == 512


def test_delta_math_ignores_non_numeric_and_asymmetric():
    """Strings, bools, asymmetric keys are silently dropped — never raise."""
    before = {"label": "before", "n": 10, "extra": 5}
    after = {"label": "after", "n": 7, "flag": True}
    delta = GainReporter.compute_delta(before, after)
    assert delta == {"n": 3}


# ──────────────────────────────────────────────
# save() round-trip + JSON
# ──────────────────────────────────────────────


def test_save_returns_row_id_and_round_trips_json(tmp_path):
    db = _make_db(tmp_path)
    reporter = GainReporter(db, {})
    before = {"total_files": 100, "duplicate_waste_gb": 1.5}
    after = {"total_files": 90, "duplicate_waste_gb": 1.1}
    rid = reporter.save(
        operation="duplicate_quarantine",
        before=before, after=after,
        scan_id=1, source_id=1,
    )
    assert isinstance(rid, int) and rid > 0

    persisted = reporter.get_report(rid)
    assert persisted is not None
    assert persisted["operation"] == "duplicate_quarantine"
    assert persisted["before"]["total_files"] == 100
    assert persisted["after"]["total_files"] == 90
    # delta computed automatically
    assert persisted["delta"]["total_files"] == 10
    # source_id / scan_id denormalised
    assert persisted["source_id"] == 1
    assert persisted["scan_id"] == 1


def test_save_persists_audit_and_quarantine_path(tmp_path):
    db = _make_db(tmp_path)
    reporter = GainReporter(db, {})
    rid = reporter.save(
        operation="duplicate_quarantine",
        before={"total_files": 5}, after={"total_files": 4},
        source_id=1, scan_id=1,
        audit_event_id=42,
        quarantine_path="data/quarantine/20260428/abc",
    )
    persisted = reporter.get_report(rid)
    assert persisted["audit_event_id"] == 42
    assert persisted["quarantine_path"] == "data/quarantine/20260428/abc"


def test_save_rejects_empty_operation(tmp_path):
    db = _make_db(tmp_path)
    reporter = GainReporter(db, {})
    with pytest.raises(ValueError):
        reporter.save(operation="", before={}, after={})
    with pytest.raises(ValueError):
        reporter.save(operation="   ", before={}, after={})


# ──────────────────────────────────────────────
# list / count / get / pagination
# ──────────────────────────────────────────────


def test_list_reports_filters_and_paginates(tmp_path):
    db = _make_db(tmp_path)
    reporter = GainReporter(db, {})
    # Seed three different operation labels, sources.
    ids = []
    for i, (op, sid) in enumerate([
        ("duplicate_quarantine", 1),
        ("archive_run", 1),
        ("duplicate_quarantine", 2),
        ("duplicate_quarantine", 1),
    ]):
        ids.append(reporter.save(
            operation=op, before={"i": i}, after={"i": i + 1},
            source_id=sid, scan_id=1,
        ))

    # Filter by operation.
    only_dup = reporter.list_reports(operation="duplicate_quarantine")
    assert all(r["operation"] == "duplicate_quarantine" for r in only_dup)
    assert len(only_dup) == 3

    # Filter by source_id.
    src1 = reporter.list_reports(source_id=1)
    assert all(r["source_id"] == 1 for r in src1)
    assert len(src1) == 3

    # Combine filters.
    combo = reporter.list_reports(
        operation="duplicate_quarantine", source_id=1,
    )
    assert len(combo) == 2

    # Pagination — page 1 + page 2 with limit=1 covers exactly two rows.
    p1 = reporter.list_reports(operation="duplicate_quarantine",
                                source_id=1, limit=1, page=1)
    p2 = reporter.list_reports(operation="duplicate_quarantine",
                                source_id=1, limit=1, page=2)
    assert len(p1) == 1 and len(p2) == 1
    assert p1[0]["id"] != p2[0]["id"]


def test_count_reports_matches_filters(tmp_path):
    db = _make_db(tmp_path)
    reporter = GainReporter(db, {})
    for op in ("duplicate_quarantine", "archive_run",
               "duplicate_quarantine"):
        reporter.save(operation=op, before={}, after={}, source_id=1)
    assert reporter.count_reports() == 3
    assert reporter.count_reports(operation="duplicate_quarantine") == 2
    assert reporter.count_reports(operation="archive_run") == 1
    assert reporter.count_reports(source_id=999) == 0


def test_get_report_returns_none_when_missing(tmp_path):
    db = _make_db(tmp_path)
    reporter = GainReporter(db, {})
    assert reporter.get_report(99999) is None


# ──────────────────────────────────────────────
# Concurrent / mixed-op saves don't collide
# ──────────────────────────────────────────────


def test_concurrent_saves_different_operations(tmp_path):
    """Two saves of different operations write independent rows; both
    are visible via list_reports + get_report."""
    db = _make_db(tmp_path)
    reporter = GainReporter(db, {})
    rid_a = reporter.save(
        operation="duplicate_quarantine",
        before={"total_files": 100}, after={"total_files": 90},
        source_id=1, scan_id=1,
    )
    rid_b = reporter.save(
        operation="archive_run",
        before={"total_files": 90}, after={"total_files": 80},
        source_id=1, scan_id=1,
    )
    assert rid_a != rid_b
    assert reporter.get_report(rid_a)["operation"] == "duplicate_quarantine"
    assert reporter.get_report(rid_b)["operation"] == "archive_run"
    rows = reporter.list_reports(limit=10)
    ops = {r["operation"] for r in rows}
    assert "duplicate_quarantine" in ops and "archive_run" in ops


def test_capture_before_and_after_are_dicts(tmp_path):
    """Even with empty scope, snapshots return a dict (with a stub) so
    the UI never has to handle a None payload."""
    db = _make_db(tmp_path)
    reporter = GainReporter(db, {})
    before = reporter.capture_before({"source_id": 1})
    after = reporter.capture_after({"source_id": 1})
    assert isinstance(before, dict) and isinstance(after, dict)
    assert before.get("phase") == "before"
    assert after.get("phase") == "after"
    # Scope echoed back.
    assert before.get("source_id") == 1


def test_safe_loads_handles_already_decoded(tmp_path):
    """Round-trips work even if the underlying row already carries
    a dict instead of a JSON string (defensive — happens in tests
    that mock the row factory)."""
    db = _make_db(tmp_path)
    reporter = GainReporter(db, {})
    # Reserve attribute access — _safe_loads is a static method.
    fn = GainReporter._safe_loads
    assert fn(None) == {}
    assert fn("") == {}
    assert fn({"a": 1}) == {"a": 1}
    assert fn(json.dumps({"x": 2})) == {"x": 2}
    # Malformed JSON is captured under _raw — never raises.
    res = fn("not-json")
    assert "_raw" in res
