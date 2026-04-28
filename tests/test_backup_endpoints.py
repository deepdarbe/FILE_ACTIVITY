"""TestClient smoke for the /api/system/backups/* endpoints (issue #81 —
System pages, this subagent's slice).

We re-implement the handler bodies inline — exactly as ``test_dashboard_api``
and ``test_dashboard_integrations_pages`` do — so the tests cover the wire
behaviour without booting the full ``create_app`` factory and its database
+ analytics + AD dependency chain. The handlers exercise the real
``BackupManager`` against a tmp_path SQLite DB.

Coverage:
  * GET /api/system/backups        — empty list + populated list
  * POST .../snapshot              — confirm-gate refusal (HTTP 400)
  * POST .../snapshot              — confirmed snapshot writes manifest row
  * POST .../restore/{id}          — confirm-gate refusal (HTTP 400)
  * Disabled feature flag returns rows=[] without 5xx
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.backup_manager import BackupManager  # noqa: E402


def _seed_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
        conn.executemany("INSERT INTO t (payload) VALUES (?)",
                         [(f"row-{i}",) for i in range(20)])
        conn.commit()
    finally:
        conn.close()


def _build_backups_app(tmp_path: Path, enabled: bool = True) -> FastAPI:
    """Build a FastAPI app exposing the same endpoint bodies as ``api.py``.

    Each test gets a fresh tmp_path-backed BackupManager so the handler
    runs against real disk state and we exercise the manifest + VACUUM
    INTO code paths end-to-end.
    """
    db_path = tmp_path / "live.db"
    _seed_db(db_path)
    cfg = {
        "database": {"path": str(db_path)},
        "backup": {
            "enabled": enabled,
            "dir": str(tmp_path / "backups"),
            "keep_last_n": 10,
            "keep_weekly": 4,
        },
    }
    mgr = BackupManager(str(db_path), cfg)

    app = FastAPI()
    app.state.backup_manager = mgr

    @app.get("/api/system/backups")
    async def list_backups():
        rows = [m.to_dict() for m in mgr.list_snapshots()]
        return {
            "enabled": bool(mgr.enabled),
            "configured": True,
            "backup_dir": mgr.backup_dir,
            "keep_last_n": mgr.keep_last_n,
            "keep_weekly": mgr.keep_weekly,
            "rows": rows,
        }

    @app.post("/api/system/backups/snapshot")
    async def create_snapshot(body: dict):
        body = body or {}
        if not bool(body.get("confirm", False)):
            raise HTTPException(400, "confirm: true required")
        if not mgr.enabled:
            raise HTTPException(400, "backup feature disabled")
        reason = (body.get("reason") or "manual").strip() or "manual"
        meta = mgr.snapshot(reason=reason)
        return {"ok": True, **meta.to_dict()}

    @app.post("/api/system/backups/restore/{snapshot_id}")
    async def restore_snapshot(snapshot_id: str, body: dict):
        body = body or {}
        if not bool(body.get("confirm", False)):
            raise HTTPException(400, "confirm: true required")
        # Audit M-3: mirror PURGE / QUARANTINE — confirm alone is not
        # enough; the literal token must match.
        if body.get("safety_token", "") != "RESTORE":
            raise HTTPException(
                400, "safety_token must equal 'RESTORE'"
            )
        if not mgr.enabled:
            raise HTTPException(400, "backup feature disabled")
        try:
            mgr.restore(snapshot_id)
        except KeyError:
            raise HTTPException(404, f"unknown: {snapshot_id}")
        except RuntimeError as e:
            raise HTTPException(409, str(e))
        return {"ok": True, "restored": snapshot_id}

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_list_backups_empty(tmp_path: Path):
    """Fresh manager + no snapshots → 200 with rows=[] and metadata."""
    client = TestClient(_build_backups_app(tmp_path))
    resp = client.get("/api/system/backups")

    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert body["configured"] is True
    assert body["rows"] == []
    assert body["keep_last_n"] == 10
    assert body["keep_weekly"] == 4
    # backup_dir must round-trip the configured path so the UI can show it
    assert "backups" in body["backup_dir"]


def test_list_backups_disabled_flag_still_returns_200(tmp_path: Path):
    """If config disables backups, the page banner needs the ``enabled``
    flag — but the GET must NOT 5xx. That would break page navigation."""
    client = TestClient(_build_backups_app(tmp_path, enabled=False))
    resp = client.get("/api/system/backups")

    assert resp.status_code == 200
    assert resp.json()["enabled"] is False


def test_snapshot_requires_confirm(tmp_path: Path):
    """Without ``confirm: true`` the endpoint must refuse — this is the
    same defence the MCP write tools have."""
    client = TestClient(_build_backups_app(tmp_path))
    resp = client.post("/api/system/backups/snapshot",
                       json={"reason": "test"})

    assert resp.status_code == 400
    assert "confirm" in resp.json()["detail"].lower()


def test_snapshot_confirmed_creates_manifest_row(tmp_path: Path):
    """Smoke: confirm=true → meta row + the file actually exists on disk."""
    app = _build_backups_app(tmp_path)
    client = TestClient(app)
    resp = client.post("/api/system/backups/snapshot",
                       json={"reason": "manual-test", "confirm": True})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["reason"] == "manual-test"
    assert body["size_bytes"] > 0
    assert os.path.exists(body["path"])

    # GET must now reflect the row.
    list_resp = client.get("/api/system/backups")
    assert list_resp.status_code == 200
    rows = list_resp.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["id"] == body["id"]
    assert rows[0]["sha256"] == body["sha256"]


def test_restore_requires_confirm(tmp_path: Path):
    client = TestClient(_build_backups_app(tmp_path))
    resp = client.post("/api/system/backups/restore/20990101_000000",
                       json={})

    assert resp.status_code == 400
    assert "confirm" in resp.json()["detail"].lower()


def test_restore_requires_safety_token(tmp_path: Path):
    """Audit M-3: confirm: true without safety_token is now rejected."""
    client = TestClient(_build_backups_app(tmp_path))
    resp = client.post("/api/system/backups/restore/20990101_000000",
                       json={"confirm": True})

    assert resp.status_code == 400
    assert "safety_token" in resp.json()["detail"].lower()


def test_restore_unknown_id_returns_404(tmp_path: Path):
    client = TestClient(_build_backups_app(tmp_path))
    resp = client.post(
        "/api/system/backups/restore/20990101_000000",
        json={"confirm": True, "safety_token": "RESTORE"},
    )

    assert resp.status_code == 404
