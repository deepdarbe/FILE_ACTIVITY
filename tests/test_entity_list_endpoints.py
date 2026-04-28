"""Tests for issue #80: entity-list bulk-action endpoints.

Covers the two new POST endpoints introduced for the reusable
``entity-list.js`` component:

  * ``POST /api/archive/bulk-from-list`` — list-driven bulk archive with
    a dry-run preview mode and a confirm gate (mirrors #158 C-2).
  * ``POST /api/explorer/open`` — Windows-only Explorer launcher with a
    source-roots traversal guard.

Plus a regression check that the existing
``/api/reports/mit-naming/{id}/files`` shape is unchanged.
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


# ---------------------------------------------------------------------------
# Stubbed sidecar dependencies (same shape as test_archive_confirm_gate.py).
# ---------------------------------------------------------------------------


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

    def close(self):  # pragma: no cover - defensive
        pass


def _base_config() -> dict:
    return {
        "dashboard": {"auth": {"enabled": False}},
        "archiving": {
            "verify_checksum": False,
            "dry_run": True,
            "cleanup_empty_dirs": False,
        },
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
def client(tmp_path, monkeypatch):
    """Boot create_app() with a tiny seeded SQLite + an
    ArchiveEngine.archive_files stub that records kwargs."""
    db_path = tmp_path / "naming.db"
    db = Database({"path": str(db_path)})
    db.connect()

    # Seed: one source rooted at tmp_path/share with archive_dest, one
    # completed scan, and two violation files inside the share.
    share = tmp_path / "share"
    share.mkdir()
    (share / "Bad File.txt").write_text("hi")  # R1: contains space
    (share / "Bad File 2.txt").write_text("hi2")  # R1: contains space
    archive_dest = str(tmp_path / "archive")
    os.makedirs(archive_dest, exist_ok=True)

    file_a = str(share / "Bad File.txt")
    file_b = str(share / "Bad File 2.txt")

    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path, archive_dest) "
            "VALUES ('s1', ?, ?)",
            (str(share), archive_dest),
        )
        cur.execute(
            "INSERT INTO scan_runs (source_id, status, completed_at) "
            "VALUES (1, 'completed', '2026-01-01 00:00:00')"
        )
        cur.execute(
            "INSERT INTO scanned_files "
            "(source_id, scan_id, file_path, relative_path, file_name, "
            " file_size, last_access_time) "
            "VALUES (1, 1, ?, 'Bad File.txt', 'Bad File.txt', 100, "
            "        '2020-01-01 00:00:00')",
            (file_a,),
        )
        cur.execute(
            "INSERT INTO scanned_files "
            "(source_id, scan_id, file_path, relative_path, file_name, "
            " file_size, last_access_time) "
            "VALUES (1, 1, ?, 'Bad File 2.txt', 'Bad File 2.txt', 200, "
            "        '2020-01-01 00:00:00')",
            (file_b,),
        )

    archive_calls: list[dict] = []

    def fake_archive_files(self, files, archive_dest_, source_unc,
                           source_id, archived_by="manual",
                           dry_run=None, trigger_type="manual",
                           trigger_detail=None):
        archive_calls.append({
            "files_count": len(files),
            "archive_dest": archive_dest_,
            "source_id": source_id,
            "archived_by": archived_by,
            "dry_run": dry_run,
            "trigger_type": trigger_type,
            "trigger_detail": trigger_detail,
        })
        return {
            "archived": len(files),
            "failed": 0,
            "total_size": sum(int(f.get("file_size") or 0) for f in files),
            "total_size_formatted": "300 B",
            "errors": [],
            "dry_run": bool(dry_run),
        }

    from src.archiver import archive_engine as _ae
    monkeypatch.setattr(_ae.ArchiveEngine, "archive_files", fake_archive_files)

    cfg = _base_config()
    app = create_app(
        db,
        cfg,
        analytics=_StubAnalytics(),
        ad_lookup=_StubADLookup(),
        email_notifier=_StubEmailNotifier(),
    )
    tc = TestClient(app)
    tc.archive_calls = archive_calls
    tc.db = db
    tc.share_root = str(share)
    tc.file_a = file_a
    tc.file_b = file_b
    return tc


def _audit_event_count(db, event_type=None):
    with db.get_read_cursor() as cur:
        if event_type is not None:
            cur.execute(
                "SELECT COUNT(*) AS c FROM file_audit_events "
                "WHERE event_type=?",
                (event_type,),
            )
        else:
            cur.execute("SELECT COUNT(*) AS c FROM file_audit_events")
        return int(cur.fetchone()["c"])


# ---------------------------------------------------------------------------
# /api/archive/bulk-from-list
# ---------------------------------------------------------------------------


def test_bulk_from_list_dry_run_returns_preview(client):
    """dry_run=true => 200, preview body, no engine invocation."""
    resp = client.post("/api/archive/bulk-from-list", json={
        "file_paths": [client.file_a, client.file_b],
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["dry_run"] is True
    assert body["preview"] is True
    assert body["matched"] == 2
    assert body["total_size"] == 300  # 100 + 200
    assert "total_size_formatted" in body
    assert len(body["sample"]) == 2
    # Engine MUST NOT be called for a preview.
    assert not client.archive_calls


def test_bulk_from_list_real_without_confirm_blocked(client):
    """dry_run=false without confirm=true is rejected (issue #158 C-2)."""
    resp = client.post("/api/archive/bulk-from-list", json={
        "file_paths": [client.file_a],
        "dry_run": False,
    })
    assert resp.status_code == 400
    assert "confirm=true" in resp.text
    assert not client.archive_calls


def test_bulk_from_list_real_with_confirm_invokes_engine(client):
    resp = client.post("/api/archive/bulk-from-list", json={
        "file_paths": [client.file_a],
        "dry_run": False,
        "confirm": True,
    })
    assert resp.status_code == 200, resp.text
    assert len(client.archive_calls) == 1
    call = client.archive_calls[-1]
    assert call["dry_run"] is False
    assert call["files_count"] == 1
    assert call["trigger_detail"] == "bulk-from-list"


def test_bulk_from_list_audit_event_written(client):
    """Every bulk-from-list call (dry-run or real) writes an audit event."""
    before = _audit_event_count(client.db)
    resp = client.post("/api/archive/bulk-from-list", json={
        "file_paths": [client.file_a],
        "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    after = _audit_event_count(client.db)
    assert after == before + 1
    # Real-run path also logs.
    resp2 = client.post("/api/archive/bulk-from-list", json={
        "file_paths": [client.file_a],
        "dry_run": False,
        "confirm": True,
    })
    assert resp2.status_code == 200, resp2.text
    assert _audit_event_count(client.db) == after + 1


def test_bulk_from_list_rejects_empty_file_paths(client):
    resp = client.post("/api/archive/bulk-from-list", json={
        "file_paths": [],
        "dry_run": True,
    })
    assert resp.status_code == 400
    assert "file_paths" in resp.text


def test_bulk_from_list_unknown_paths_404(client):
    resp = client.post("/api/archive/bulk-from-list", json={
        "file_paths": ["/no/such/file"],
        "dry_run": True,
    })
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /api/explorer/open
# ---------------------------------------------------------------------------


def test_explorer_open_non_windows_refused(client, monkeypatch):
    """On Linux/Mac the endpoint must return 400 with a clear message."""
    if os.name == "nt":
        pytest.skip("non-Windows guard test")
    resp = client.post("/api/explorer/open", json={
        "paths": [client.share_root],
    })
    assert resp.status_code == 400
    assert "Windows" in resp.text


def test_explorer_open_rejects_path_traversal(client, monkeypatch):
    """A path outside any source root must be rejected.

    We force os.name='nt' via a stub so the Windows-only branch is reachable
    on the CI (Linux) runner. subprocess.Popen is patched to a no-op so we
    don't actually try to launch explorer.exe.
    """
    monkeypatch.setattr(os, "name", "nt")
    import subprocess as _sp
    popen_calls: list[list[str]] = []

    class _FakePopen:
        def __init__(self, argv, **kw):
            popen_calls.append(list(argv))

    monkeypatch.setattr(_sp, "Popen", _FakePopen)

    # /etc/passwd (or any non-source path) is outside the allowed root.
    bad = "/etc/passwd" if os.path.exists("/etc/passwd") else os.path.expanduser("~")
    resp = client.post("/api/explorer/open", json={"paths": [bad]})
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["ok"] is False
    assert any(r["reason"] == "outside_source_roots" for r in body["rejected"])
    # Importantly, Popen was never called for the rejected path.
    assert popen_calls == []


def test_explorer_open_audit_event_written(client, monkeypatch):
    """Every explorer/open call writes a single audit event regardless of
    whether any path actually opened."""
    monkeypatch.setattr(os, "name", "nt")
    import subprocess as _sp
    monkeypatch.setattr(
        _sp, "Popen", lambda argv, **kw: None,
    )

    before = _audit_event_count(client.db, "explorer_open")
    # Even a fully-rejected request increments the audit counter.
    resp = client.post("/api/explorer/open", json={"paths": ["/etc/passwd"]})
    assert resp.status_code in (200, 400)
    after = _audit_event_count(client.db, "explorer_open")
    assert after == before + 1


# ---------------------------------------------------------------------------
# /api/reports/mit-naming/{id}/files — shape regression
# ---------------------------------------------------------------------------


def test_mit_naming_files_shape_unchanged(client):
    """Existing endpoint must keep returning {code,total,page,page_size,
    total_pages,files:[...]} so the entity-list page does not break."""
    resp = client.get("/api/reports/mit-naming/1/files?code=R1&page=1&page_size=50")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    for key in ("code", "total", "page", "page_size", "total_pages", "files"):
        assert key in body, "missing key: " + key
    assert body["code"] == "R1"
    # Both seeded files match R1 (contain a space).
    assert body["total"] == 2
    assert isinstance(body["files"], list)
    if body["files"]:
        f = body["files"][0]
        # Fields the entity-list mit-naming page consumes.
        for key in ("file_path", "file_name", "file_size", "owner",
                    "last_modify_time", "file_size_formatted", "directory"):
            assert key in f, "missing field in row: " + key
