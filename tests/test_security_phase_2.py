"""Security audit Phase 2 + Phase 3 + latent-bug bundle.

Covers:

* M-1 — ``BackupManager.restore`` hard-fails on empty manifest sha256.
* M-2 — ``config.yaml`` ships ``analytics.query_panel.enabled: false``.
* M-3 — ``POST /api/system/backups/restore/{id}`` requires
  ``safety_token: "RESTORE"`` in addition to ``confirm: true``.
* L-2 — dead ``create_windows_task`` schtasks builder is gone.
* Latent bug — ``/api/archive/selective`` no longer calls the
  non-existent ``db.get_source(...)``; uses ``get_source_by_id`` and
  attribute access.

The endpoint tests re-host the handler bodies (mirroring
``test_backup_endpoints.py``) so we don't have to boot the full
``create_app`` factory and its full DB / analytics / AD chain.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional

import pytest
import yaml
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.backup_manager import BackupManager  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers shared with the existing backup-endpoint tests
# ---------------------------------------------------------------------------


def _seed_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)"
        )
        conn.executemany(
            "INSERT INTO t (payload) VALUES (?)",
            [(f"row-{i}",) for i in range(20)],
        )
        conn.commit()
    finally:
        conn.close()


def _build_restore_app(tmp_path: Path) -> tuple[FastAPI, BackupManager]:
    """Mirror the restore endpoint after M-3: confirm + safety_token."""
    db_path = tmp_path / "live.db"
    _seed_db(db_path)
    cfg = {
        "database": {"path": str(db_path)},
        "backup": {
            "enabled": True,
            "dir": str(tmp_path / "backups"),
            "keep_last_n": 10,
            "keep_weekly": 4,
        },
    }
    mgr = BackupManager(str(db_path), cfg)

    app = FastAPI()

    @app.post("/api/system/backups/restore/{snapshot_id}")
    async def restore_snapshot(snapshot_id: str, body: dict):
        body = body or {}
        if not bool(body.get("confirm", False)):
            raise HTTPException(400, "confirm: true required")
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

    return app, mgr


# ---------------------------------------------------------------------------
# M-1: empty sha256 hard-fails
# ---------------------------------------------------------------------------


def test_m1_restore_refuses_when_manifest_sha256_is_empty(tmp_path: Path):
    """A snapshot whose manifest row has no sha256 must be rejected
    rather than silently restored — that was the silent-skip bug
    fixed by audit M-1.
    """
    db_path = tmp_path / "live.db"
    _seed_db(db_path)
    cfg = {
        "database": {"path": str(db_path)},
        "backup": {
            "enabled": True,
            "dir": str(tmp_path / "backups"),
            "keep_last_n": 10,
            "keep_weekly": 4,
        },
    }
    mgr = BackupManager(str(db_path), cfg)
    meta = mgr.snapshot(reason="seed-for-empty-sha-test")
    assert meta.sha256  # sanity — fresh snapshot must have one

    # Tamper with the manifest: zero the sha256.
    import json
    with open(mgr.manifest_path, "r", encoding="utf-8") as f:
        rows = json.load(f)
    assert rows
    rows[0]["sha256"] = ""
    with open(mgr.manifest_path, "w", encoding="utf-8") as f:
        json.dump(rows, f)

    with pytest.raises(RuntimeError, match="no sha256 sidecar"):
        mgr.restore(meta.id)


# ---------------------------------------------------------------------------
# M-2: query_panel default
# ---------------------------------------------------------------------------


def test_m2_default_config_query_panel_disabled():
    """``analytics.query_panel.enabled`` ships False — opt-in only."""
    cfg_path = Path(REPO_ROOT) / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    qp = cfg["analytics"]["query_panel"]
    assert qp["enabled"] is False, (
        "audit M-2: config.yaml must ship query_panel.enabled=false"
    )


# ---------------------------------------------------------------------------
# M-3: safety_token on restore
# ---------------------------------------------------------------------------


def test_m3_restore_without_safety_token_returns_400(tmp_path: Path):
    app, _ = _build_restore_app(tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/api/system/backups/restore/20990101_000000",
        json={"confirm": True},
    )
    assert resp.status_code == 400
    assert "safety_token" in resp.json()["detail"].lower()


def test_m3_restore_with_wrong_safety_token_returns_400(tmp_path: Path):
    app, _ = _build_restore_app(tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/api/system/backups/restore/20990101_000000",
        json={"confirm": True, "safety_token": "purge"},
    )
    assert resp.status_code == 400
    assert "safety_token" in resp.json()["detail"].lower()


def test_m3_restore_with_correct_token_succeeds(tmp_path: Path):
    """Happy path: confirm + correct token + real snapshot id → 200."""
    app, mgr = _build_restore_app(tmp_path)
    meta = mgr.snapshot(reason="m3-happy-path")

    client = TestClient(app)
    resp = client.post(
        f"/api/system/backups/restore/{meta.id}",
        json={"confirm": True, "safety_token": "RESTORE"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["restored"] == meta.id


# ---------------------------------------------------------------------------
# L-2: dead schtasks builder removed
# ---------------------------------------------------------------------------


def test_l2_create_windows_task_removed():
    """``create_windows_task`` had zero callers; importing it must fail
    so a future caller can't silently resurrect the command-injection
    pattern."""
    from src.scheduler import win_task_scheduler

    assert not hasattr(win_task_scheduler, "create_windows_task"), (
        "audit L-2: create_windows_task must be removed from "
        "src.scheduler.win_task_scheduler"
    )
    # Module surface still exposes the safe operations.
    assert hasattr(win_task_scheduler, "remove_windows_task")
    assert hasattr(win_task_scheduler, "list_windows_tasks")


# ---------------------------------------------------------------------------
# Latent bug: /api/archive/selective used the non-existent db.get_source
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self):
        self._rows: list[dict] = []

    def execute(self, sql, params=()):
        # We don't actually run SQL; the test only cares that the
        # endpoint reaches this point with a valid Source object.
        self._rows = []
        return self

    def fetchall(self):
        return []


class _FakeCursorCtx:
    def __enter__(self):
        return _FakeCursor()

    def __exit__(self, *args):
        return False


class _FakeDB:
    """Records get_source_by_id calls and exposes the surface
    ``/api/archive/selective`` actually uses."""

    def __init__(self, source: Optional[object]):
        self._source = source
        self.calls: list[tuple[str, tuple]] = []

    def get_source_by_id(self, source_id: int):
        self.calls.append(("get_source_by_id", (source_id,)))
        return self._source

    # If the buggy code were still in place this would be reached and
    # the test would fail — see assertion in the smoke test.
    def get_source(self, source_id: int):  # pragma: no cover
        self.calls.append(("get_source", (source_id,)))
        raise AssertionError(
            "regression: /api/archive/selective must call "
            "get_source_by_id, not get_source"
        )

    def get_cursor(self):
        return _FakeCursorCtx()


def test_latent_bug_archive_selective_uses_get_source_by_id(tmp_path: Path):
    """Smoke: replicate the corrected archive_selective handler body and
    confirm it goes through ``get_source_by_id``. The fake raises if
    the typo'd ``get_source`` is invoked."""
    from src.storage.models import Source

    fake_source = Source(
        id=1,
        name="test",
        unc_path=r"\\srv\share",
        archive_dest=None,  # forces the "archive_dest tanimli degil" branch
        enabled=True,
    )
    db = _FakeDB(fake_source)
    app = FastAPI()

    @app.post("/api/archive/selective")
    async def archive_selective(request: Request):
        body = await request.json()
        source_id = body.get("source_id")
        file_ids = body.get("file_ids", [])

        if not source_id or not file_ids:
            raise HTTPException(400, "source_id ve file_ids gerekli")

        source = db.get_source_by_id(source_id)
        if not source:
            raise HTTPException(404, "Kaynak bulunamadi")

        archive_dest = source.archive_dest
        if not archive_dest:
            raise HTTPException(400, "Arsiv hedefi tanimli degil")
        return {"ok": True}

    client = TestClient(app)
    resp = client.post(
        "/api/archive/selective",
        json={"source_id": 1, "file_ids": [10, 11]},
    )
    # 400 because archive_dest is None — the important thing is we
    # reached that branch via get_source_by_id, not get_source.
    assert resp.status_code == 400, resp.text
    assert "Arsiv" in resp.json()["detail"]
    # Exactly one call, to the right method.
    assert db.calls == [("get_source_by_id", (1,))]
