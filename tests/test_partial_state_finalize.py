"""Tests for #347 — completed scan must not report a stale partial scan_state.

On burculogo a completed scan (status='completed') whose rolling partial
snapshot was frozen at scan_state='enrich' (process restarted mid-enrichment)
pinned loadFrequency/treemap/sizes in the partial 0-count view, hiding the
completed scan's real data. _load_partial_summary_v2_for_source now overrides
scan_state to 'completed' when scan_runs.status='completed'.

fastapi-gated (TestClient); the fix lives in an api.py closure.
"""

from __future__ import annotations

import importlib.util
import json

import pytest

from src.storage.database import Database

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
requires_fastapi = pytest.mark.skipif(
    not HAS_FASTAPI, reason="fastapi not installed in this environment"
)


@pytest.fixture
def db(tmp_path):
    d = Database({
        "path": str(tmp_path / "ps.db"),
        "retention": {"auto_cleanup_on_startup": False},
    })
    d.connect()
    with d.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e', 'x')")
    yield d
    d.close()


def _seed_scan(db, *, status, partial_scan_state):
    sid = db.create_scan_run(1)
    partial = {
        "schema_version": 2, "scan_state": partial_scan_state,
        "progress": {"files_so_far": 100, "rate_per_sec": 5.0},
    }
    with db.get_cursor() as cur:
        cur.execute(
            "UPDATE scan_runs SET status=?, partial_summary_json=?, "
            "partial_updated_at=datetime('now') WHERE id=?",
            (status, json.dumps(partial), sid))
    return sid


@pytest.fixture
def client(db):
    from fastapi.testclient import TestClient
    from src.dashboard.api import create_app
    app = create_app(db, {"dashboard": {"auth": {"enabled": False}}})
    return TestClient(app)


@requires_fastapi
def test_completed_scan_overrides_stale_enrich_state(db, client):
    _seed_scan(db, status="completed", partial_scan_state="enrich")
    r = client.get("/api/sources/1/partial-summary")
    assert r.status_code == 200, r.text
    assert r.json()["scan_state"] == "completed"


@requires_fastapi
def test_running_scan_state_is_preserved(db, client):
    _seed_scan(db, status="running", partial_scan_state="enrich")
    r = client.get("/api/sources/1/partial-summary")
    assert r.status_code == 200
    assert r.json()["scan_state"] == "enrich"   # live scan → keep partial


@requires_fastapi
def test_completed_state_passthrough(db, client):
    _seed_scan(db, status="completed", partial_scan_state="completed")
    r = client.get("/api/sources/1/partial-summary")
    assert r.json()["scan_state"] == "completed"
