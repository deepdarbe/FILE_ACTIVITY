"""Smoke for ``GET /api/system/status`` (issue #125).

The endpoint is the contract between the operations registry
(``OperationsRegistry``) and the dashboard's "su an ne oluyor" banner.

Guarantees we exercise here:

* Always 200; empty list when nothing is active.
* Reflects real registry state (not a stale cache).
* Tolerates a missing registry — e.g. if ``app.state.operations`` was
  somehow never set, the endpoint must still respond cleanly rather
  than 500.
"""

from __future__ import annotations

import os
import sys

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.dashboard.api import create_app  # noqa: E402
from src.storage.database import Database  # noqa: E402


# Reuse the smoke test stubs to keep the boot footprint tiny.
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


class _StubAnalytics:
    available = False

    def health(self):
        return {"available": False, "configured": False}

    def close(self):
        pass


_BASE_CONFIG = {
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
    db = Database({"path": str(tmp_path / "ops.db")})
    db.connect()
    app = create_app(
        db, _BASE_CONFIG,
        analytics=_StubAnalytics(),
        ad_lookup=_StubADLookup(),
        email_notifier=_StubEmailNotifier(),
    )
    return app, TestClient(app)


def test_status_empty_when_idle(app_client):
    _app, client = app_client
    r = client.get("/api/system/status")
    assert r.status_code == 200
    body = r.json()
    assert body == {"operations": []}


def test_status_reflects_active_op(app_client):
    app, client = app_client
    op_id = app.state.operations.start(
        "scan", "Tarama: \\\\fs01\\dept",
        metadata={"source_id": 1},
    )
    app.state.operations.progress(op_id, pct=45, eta_seconds=1800)
    try:
        r = client.get("/api/system/status")
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body["operations"], list)
        assert len(body["operations"]) == 1
        op = body["operations"][0]
        assert op["type"] == "scan"
        assert op["label"] == "Tarama: \\\\fs01\\dept"
        assert op["progress_pct"] == 45
        assert op["eta_seconds"] == 1800
        assert op["metadata"] == {"source_id": 1}
        assert isinstance(op["started_at"], (int, float))
    finally:
        app.state.operations.finish(op_id)

    r2 = client.get("/api/system/status")
    assert r2.json() == {"operations": []}


def test_status_tolerates_missing_registry(app_client):
    app, client = app_client
    saved = app.state.operations
    app.state.operations = None
    try:
        r = client.get("/api/system/status")
        assert r.status_code == 200
        assert r.json() == {"operations": []}
    finally:
        app.state.operations = saved
