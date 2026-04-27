"""TestClient smoke for the /api/quarantine/* endpoints (issue #110 Phase 2).

Mirrors the test_backup_endpoints style — handler bodies re-implemented
inline so the tests cover wire behaviour without booting the full
``create_app`` factory + analytics dependency chain.

Coverage:
  * GET  /api/quarantine                     — list + status filter
  * POST /api/quarantine/{id}/purge          — confirm + safety_token gates
  * POST /api/quarantine/{id}/purge          — sha mismatch → 409 forensic
  * POST /api/quarantine/{id}/purge          — happy path (200 + purged)
  * POST /api/quarantine/{id}/restore        — confirm gate + collision (409)
  * POST /api/quarantine/{id}/restore        — happy path
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pytest
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.archiver.duplicate_cleaner import (  # noqa: E402
    DuplicateCleaner, SAFETY_TOKEN_VALUE, PURGE_SAFETY_TOKEN_VALUE,
)


# ──────────────────────────────────────────────
# Fixture: app + seeded quarantine_log rows
# ──────────────────────────────────────────────


def _seed(tmp_path: Path):
    """Build DB with two quarantined files. Returns (db, cfg, qlog_ids)."""
    cfg = {
        "duplicates": {
            "quarantine": {
                "enabled": True,
                "dir": str(tmp_path / "quarantine"),
                "bulk_delete_max_files": 500,
                "require_safety_token": True,
                "quarantine_days": 30,
                "purge_hour": 3,
            }
        },
        "compliance": {"legal_hold": {"enabled": True}},
    }
    db = Database({"path": str(tmp_path / "qrn-api.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (id, name, unc_path) VALUES (1, ?, ?)",
            ("test", str(tmp_path / "share")),
        )
        cur.execute(
            "INSERT INTO scan_runs (id, source_id, status) "
            "VALUES (1, 1, 'completed')"
        )
    share = tmp_path / "share"
    share.mkdir(exist_ok=True)
    with db.get_cursor() as cur:
        for rel in ("a/dup.bin", "b/dup.bin", "c/dup.bin"):
            fpath = share / rel
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_bytes(b"X" * 96)
            cur.execute(
                "INSERT INTO scanned_files "
                "(source_id, scan_id, file_path, relative_path, "
                "file_name, file_size) "
                "VALUES (1, 1, ?, ?, 'dup.bin', 96)",
                (str(fpath), rel),
            )
    cleaner = DuplicateCleaner(db, cfg)
    # Quarantine the first two — last-copy guard keeps the third.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT id FROM scanned_files ORDER BY id ASC LIMIT 2"
        )
        ids = [int(r["id"]) for r in cur.fetchall()]
    res = cleaner.quarantine(
        file_ids=ids, confirm=True,
        safety_token=SAFETY_TOKEN_VALUE,
        moved_by="seed", source_id=1,
    )
    assert res.moved == 2
    with db.get_cursor() as cur:
        cur.execute("SELECT id FROM quarantine_log ORDER BY id ASC")
        qlog_ids = [int(r["id"]) for r in cur.fetchall()]
    return db, cfg, qlog_ids


def _build_app(db: Database, cfg: dict) -> FastAPI:
    app = FastAPI()

    @app.get("/api/quarantine")
    async def quarantine_list(
        status: Optional[str] = Query(None),
        limit: int = Query(500, ge=1, le=5000),
    ):
        dup_cfg = ((cfg or {}).get("duplicates") or {}).get(
            "quarantine"
        ) or {}
        try:
            qdays = max(1, int(dup_cfg.get("quarantine_days") or 30))
        except (TypeError, ValueError):
            qdays = 30
        sql = (
            "SELECT id, file_id, original_path, quarantine_path, sha256, "
            "file_size, moved_at, moved_by, gain_report_id, "
            "purged_at, restored_at FROM quarantine_log "
        )
        where = []
        if status == "quarantined":
            where.append("purged_at IS NULL AND restored_at IS NULL")
        elif status == "restored":
            where.append("restored_at IS NOT NULL")
        elif status == "purged":
            where.append("purged_at IS NOT NULL")
        if where:
            sql += "WHERE " + " AND ".join(where) + " "
        sql += "ORDER BY moved_at DESC LIMIT ?"
        rows = []
        with db.get_cursor() as cur:
            cur.execute(sql, [int(limit)])
            for r in cur.fetchall():
                d = dict(r)
                if d.get("purged_at"):
                    d["status"] = "purged"
                elif d.get("restored_at"):
                    d["status"] = "restored"
                else:
                    d["status"] = "quarantined"
                moved_at = d.get("moved_at")
                will_purge = None
                if moved_at:
                    try:
                        if isinstance(moved_at, str):
                            dt = datetime.fromisoformat(
                                moved_at.replace("Z", "")
                            )
                        else:
                            dt = moved_at
                        will_purge = (dt + timedelta(days=qdays)).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    except Exception:
                        will_purge = None
                d["will_purge_at"] = will_purge
                rows.append(d)
        return {
            "enabled": bool(dup_cfg.get("enabled", True)),
            "quarantine_days": qdays,
            "rows": rows,
        }

    @app.post("/api/quarantine/{quarantine_log_id}/purge")
    async def quarantine_purge(quarantine_log_id: int, request: Request):
        body = await request.json()
        confirm = bool(body.get("confirm", False))
        token = body.get("safety_token", "")
        purged_by = body.get("purged_by") or "operator"
        if not confirm:
            raise HTTPException(400, "confirm=True required to purge")
        if token != PURGE_SAFETY_TOKEN_VALUE:
            raise HTTPException(
                400,
                f"safety_token must equal {PURGE_SAFETY_TOKEN_VALUE!r}",
            )
        cleaner = DuplicateCleaner(db, cfg)
        result = cleaner.purge_one(
            int(quarantine_log_id), purged_by=purged_by
        )
        if result.status in ("purged", "skipped_missing"):
            return result.to_dict()
        if result.status == "skipped_not_found":
            raise HTTPException(404, result.reason or "not found")
        if result.status == "abort_sha_mismatch":
            raise HTTPException(
                409,
                {
                    "error": "sha_mismatch_forensic_preserve",
                    "detail": result.to_dict(),
                },
            )
        if result.status in (
            "skipped_already_purged", "skipped_restored",
        ):
            raise HTTPException(409, result.reason or result.status)
        raise HTTPException(
            500, {"error": result.status, "detail": result.to_dict()},
        )

    @app.post("/api/quarantine/{quarantine_log_id}/restore")
    async def quarantine_restore(
        quarantine_log_id: int, request: Request
    ):
        body = await request.json()
        confirm = bool(body.get("confirm", False))
        restored_by = body.get("restored_by") or "operator"
        if not confirm:
            raise HTTPException(400, "confirm=True required to restore")
        cleaner = DuplicateCleaner(db, cfg)
        result = cleaner.restore(
            int(quarantine_log_id), restored_by=restored_by
        )
        if result.status == "restored":
            return result.to_dict()
        if result.status == "skipped_not_found":
            raise HTTPException(404, result.reason or "not found")
        if result.status in (
            "skipped_collision",
            "skipped_already_restored",
            "skipped_already_purged",
            "skipped_missing",
        ):
            raise HTTPException(409, result.reason or result.status)
        raise HTTPException(
            500, {"error": result.status, "detail": result.to_dict()},
        )

    return app


# ──────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────


def test_quarantine_list_returns_rows_with_status_and_will_purge_at(tmp_path):
    db, cfg, qlog_ids = _seed(tmp_path)
    client = TestClient(_build_app(db, cfg))
    resp = client.get("/api/quarantine")
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert body["quarantine_days"] == 30
    assert len(body["rows"]) == len(qlog_ids)
    # Every row has derived status + will_purge_at.
    for r in body["rows"]:
        assert r["status"] == "quarantined"
        assert r["will_purge_at"] is not None


def test_quarantine_list_filter_purged(tmp_path):
    db, cfg, qlog_ids = _seed(tmp_path)
    cleaner = DuplicateCleaner(db, cfg)
    # Purge one row.
    cleaner.purge_one(qlog_ids[0], purged_by="tester")
    client = TestClient(_build_app(db, cfg))
    resp = client.get("/api/quarantine?status=purged")
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["status"] == "purged"
    # And the inverse filter shows only the remaining one.
    resp2 = client.get("/api/quarantine?status=quarantined")
    assert resp2.status_code == 200
    rows2 = resp2.json()["rows"]
    assert len(rows2) == 1
    assert rows2[0]["status"] == "quarantined"


def test_purge_endpoint_refuses_without_confirm(tmp_path):
    db, cfg, qlog_ids = _seed(tmp_path)
    client = TestClient(_build_app(db, cfg))
    resp = client.post(
        f"/api/quarantine/{qlog_ids[0]}/purge",
        json={"safety_token": "PURGE"},
    )
    assert resp.status_code == 400
    assert "confirm" in resp.json()["detail"].lower()


def test_purge_endpoint_refuses_without_safety_token(tmp_path):
    db, cfg, qlog_ids = _seed(tmp_path)
    client = TestClient(_build_app(db, cfg))
    resp = client.post(
        f"/api/quarantine/{qlog_ids[0]}/purge",
        json={"confirm": True, "safety_token": "WRONG"},
    )
    assert resp.status_code == 400
    assert "safety_token" in resp.json()["detail"]


def test_purge_endpoint_happy_path(tmp_path):
    db, cfg, qlog_ids = _seed(tmp_path)
    client = TestClient(_build_app(db, cfg))
    resp = client.post(
        f"/api/quarantine/{qlog_ids[0]}/purge",
        json={"confirm": True, "safety_token": "PURGE"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "purged"
    assert body["audit_event_id"] is not None


def test_purge_endpoint_sha_mismatch_returns_409_forensic(tmp_path):
    db, cfg, qlog_ids = _seed(tmp_path)
    # Tamper.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT quarantine_path FROM quarantine_log WHERE id = ?",
            (qlog_ids[0],),
        )
        qpath = dict(cur.fetchone())["quarantine_path"]
    with open(qpath, "ab") as f:
        f.write(b"TAMPERED")
    client = TestClient(_build_app(db, cfg))
    resp = client.post(
        f"/api/quarantine/{qlog_ids[0]}/purge",
        json={"confirm": True, "safety_token": "PURGE"},
    )
    assert resp.status_code == 409, resp.text
    # File MUST still exist.
    assert os.path.exists(qpath), \
        "SHA mismatch over the wire must not delete"


def test_purge_endpoint_unknown_id_returns_404(tmp_path):
    db, cfg, _qlog_ids = _seed(tmp_path)
    client = TestClient(_build_app(db, cfg))
    resp = client.post(
        "/api/quarantine/99999/purge",
        json={"confirm": True, "safety_token": "PURGE"},
    )
    assert resp.status_code == 404


def test_restore_endpoint_refuses_without_confirm(tmp_path):
    db, cfg, qlog_ids = _seed(tmp_path)
    client = TestClient(_build_app(db, cfg))
    resp = client.post(
        f"/api/quarantine/{qlog_ids[0]}/restore", json={},
    )
    assert resp.status_code == 400


def test_restore_endpoint_happy_path(tmp_path):
    db, cfg, qlog_ids = _seed(tmp_path)
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT original_path FROM quarantine_log WHERE id = ?",
            (qlog_ids[0],),
        )
        opath = dict(cur.fetchone())["original_path"]
    client = TestClient(_build_app(db, cfg))
    resp = client.post(
        f"/api/quarantine/{qlog_ids[0]}/restore",
        json={"confirm": True},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "restored"
    assert os.path.exists(opath)


def test_restore_endpoint_collision_returns_409(tmp_path):
    db, cfg, qlog_ids = _seed(tmp_path)
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT original_path FROM quarantine_log WHERE id = ?",
            (qlog_ids[0],),
        )
        opath = dict(cur.fetchone())["original_path"]
    Path(opath).parent.mkdir(parents=True, exist_ok=True)
    Path(opath).write_bytes(b"collision")
    client = TestClient(_build_app(db, cfg))
    resp = client.post(
        f"/api/quarantine/{qlog_ids[0]}/restore",
        json={"confirm": True},
    )
    assert resp.status_code == 409


def test_restore_endpoint_unknown_id_returns_404(tmp_path):
    db, cfg, _qlog_ids = _seed(tmp_path)
    client = TestClient(_build_app(db, cfg))
    resp = client.post(
        "/api/quarantine/99999/restore",
        json={"confirm": True},
    )
    assert resp.status_code == 404
