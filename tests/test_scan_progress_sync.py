"""Issue #137 — Sources page card + DOSYA KPI sync with the ops banner
during the MFT collection phase.

The bug: while the scanner is in the early ``MFT okuma`` phase, the DB
``scan_runs`` row + the in-memory ``progress["file_count"]`` are still
zero (the scanner hasn't iterated MFT records into batches yet). But the
ops banner already shows ``MFT okuma: 50,648 kayit``, so users see two
mutually-contradictory numbers in the same screen.

The fix exposes the live ``processed`` counter from the operations
registry on ``GET /api/scan/progress/{source_id}`` as ``live_count``,
and the frontend prefers it over ``file_count`` whenever it's larger.

Tests cover:

* Endpoint surfaces ``live_count`` when an active op is registered for
  ``source_id`` with ``metadata['processed']`` populated.
* Endpoint returns ``live_count=None`` when no active op matches.
* :meth:`OperationsRegistry.find_active_op_by_metadata` matches the
  first op (by ``started_at``) whose metadata is a superset of the
  filter dict.
* When the scanner has progressed past MFT collection (live_count
  absent / smaller), ``file_count`` from the in-memory progress dict
  remains the authoritative value.
"""

from __future__ import annotations

import os
import sys
import time

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.dashboard.api import create_app  # noqa: E402
from src.storage.database import Database  # noqa: E402
from src.storage.operations_tracker import OperationsRegistry  # noqa: E402


# ─────────────────────────────────────────────────────────────────────
# Stubs (reused from test_system_status_endpoint to keep boot tiny)
# ─────────────────────────────────────────────────────────────────────


class _StubADLookup:
    available = False

    def lookup(self, name, force_refresh=False):
        return {"username": name, "email": None, "display_name": None,
                "found": False, "source": "live"}

    def health(self):
        return {"available": False, "configured": False}


class _StubEmailNotifier:
    available = False

    def send(self, *a, **kw):
        return False

    def health(self):
        return {"available": False, "configured": False}



_BASE_CONFIG = {
    # Issue #158 C-1: disable auth for TestClient runs.
    "dashboard": {"auth": {"enabled": False}},
    "security": {
        "ransomware": {"enabled": False},
        "orphan_sid": {"enabled": False},
    },
    "analytics": {},
    "backup": {"enabled": False, "dir": "/tmp/_no_backups",
               "keep_last_n": 1, "keep_weekly": 0},
    "integrations": {"syslog": {"enabled": False}},
}


@pytest.fixture
def app_client(tmp_path):
    db = Database({"path": str(tmp_path / "scan_progress.db")})
    db.connect()
    app = create_app(
        db, _BASE_CONFIG,
        ad_lookup=_StubADLookup(),
        email_notifier=_StubEmailNotifier(),
    )
    return app, TestClient(app)


# ─────────────────────────────────────────────────────────────────────
# Registry helper unit test
# ─────────────────────────────────────────────────────────────────────


def test_find_active_op_by_metadata_returns_first_match():
    reg = OperationsRegistry()
    a = reg.start("scan", "Tarama A", metadata={"source_id": 1})
    time.sleep(0.01)
    b = reg.start("scan", "Tarama B", metadata={"source_id": 2})
    time.sleep(0.01)
    c = reg.start("analysis", "Analiz", metadata={"source_id": 1})

    # Filter on a single key — match the OLDEST op (oldest-first sort).
    hit = reg.find_active_op_by_metadata(source_id=1)
    assert hit is not None
    assert hit.op_id == a

    # Multi-key filter — must require ALL keys to match.
    hit2 = reg.find_active_op_by_metadata(source_id=1)
    assert hit2.op_id == a  # still the oldest matching op

    # Non-matching value → None.
    miss = reg.find_active_op_by_metadata(source_id=999)
    assert miss is None

    # Empty filter returns the oldest active op (matches list_active order).
    hit3 = reg.find_active_op_by_metadata()
    assert hit3 is not None
    assert hit3.op_id == a

    # After finishing the matching op, the next oldest takes over.
    reg.finish(a)
    hit4 = reg.find_active_op_by_metadata(source_id=1)
    assert hit4 is not None
    assert hit4.op_id == c

    # Cleanup.
    reg.finish(b)
    reg.finish(c)


def test_progress_stores_processed_in_metadata():
    """``progress(processed=...)`` must surface under ``metadata['processed']``
    so :meth:`find_active_op_by_metadata` callers can read it back without
    parsing the free-form label.
    """
    reg = OperationsRegistry()
    op = reg.start("scan", "Tarama", metadata={"source_id": 7})
    reg.progress(op, label="MFT okuma: 50,648 kayit", processed=50648)
    [snap] = reg.list_active()
    assert snap.metadata.get("processed") == 50648
    assert snap.metadata.get("source_id") == 7  # original key preserved
    # Negative / bad values clamp safely — don't poison the dict.
    reg.progress(op, processed=-5)
    [snap2] = reg.list_active()
    assert snap2.metadata["processed"] == 0
    reg.progress(op, processed="not a number")
    # Bad value silently ignored — last good value stays.
    [snap3] = reg.list_active()
    assert snap3.metadata["processed"] == 0


# ─────────────────────────────────────────────────────────────────────
# Endpoint tests
# ─────────────────────────────────────────────────────────────────────


def test_progress_endpoint_returns_live_count_when_op_active(app_client):
    app, client = app_client
    source_id = 42
    op_id = app.state.operations.start(
        "scan", "Tarama: \\\\fs01\\dept",
        metadata={"source_id": source_id},
    )
    app.state.operations.progress(
        op_id,
        label="MFT okuma: 50,648 kayit",
        processed=50648,
    )
    try:
        r = client.get(f"/api/scan/progress/{source_id}")
        assert r.status_code == 200
        body = r.json()
        assert body.get("live_count") == 50648
    finally:
        app.state.operations.finish(op_id)


def test_progress_endpoint_live_count_null_when_no_op(app_client):
    _app, client = app_client
    r = client.get("/api/scan/progress/12345")
    assert r.status_code == 200
    body = r.json()
    assert "live_count" in body
    assert body["live_count"] is None


def test_progress_endpoint_falls_back_to_db_file_count(app_client):
    """When no op is active, the existing ``file_count`` from
    :func:`get_scan_progress` continues to be the source of truth.
    The new ``live_count`` field is null and the frontend uses
    ``file_count`` for display.
    """
    from src.scanner import file_scanner as fs

    app, client = app_client
    source_id = 99

    # Seed the in-memory scan progress dict with a non-zero file_count to
    # simulate a scanner that's past the MFT collection phase.
    fs._scan_progress[source_id] = {
        "source_id": source_id,
        "source_name": "test",
        "status": "scanning",
        "file_count": 1234,
        "total_size": 0,
        "total_size_formatted": "0 B",
        "errors": 0,
        "current_dir": "",
        "started_at": "2026-04-28 00:00:00",
        "elapsed": "0s",
        "files_per_second": 0,
    }
    try:
        r = client.get(f"/api/scan/progress/{source_id}")
        assert r.status_code == 200
        body = r.json()
        # No active op for this source → live_count is null, file_count
        # comes through unchanged.
        assert body.get("live_count") is None
        assert body.get("file_count") == 1234
        assert body.get("status") == "scanning"
        assert body.get("finished") is False
    finally:
        fs._scan_progress.pop(source_id, None)


def test_progress_endpoint_idle_includes_live_count_field(app_client):
    """The idle response shape MUST also carry the new field — the
    frontend reads ``data.live_count`` unconditionally.
    """
    _app, client = app_client
    r = client.get("/api/scan/progress/77")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "idle"
    assert body["finished"] is False
    assert "live_count" in body
    assert body["live_count"] is None


def test_progress_endpoint_tolerates_missing_registry(app_client):
    """If ``app.state.operations`` is None (e.g. registry init failed at
    boot), the endpoint must still respond cleanly with
    ``live_count=None`` rather than 500.
    """
    app, client = app_client
    saved = app.state.operations
    app.state.operations = None
    try:
        r = client.get("/api/scan/progress/1")
        assert r.status_code == 200
        body = r.json()
        assert body["live_count"] is None
    finally:
        app.state.operations = saved
