"""Tests for issue #158 C-2: archive confirm gate.

Coverage:
  * /api/archive/run: dry-run without confirm -> ok.
  * /api/archive/run: real run without confirm -> 400 (refused).
  * /api/archive/run: real run with confirm=true -> ok (engine called
    with dry_run=False).
  * /api/archive/selective: dry-run without confirm -> ok.
  * /api/archive/selective: real run without confirm -> 400.
  * /api/archive/selective: real run with confirm=true -> ok.

The test patches ``ArchiveEngine.archive_files`` so we don't actually
move files; we just assert the kwargs the endpoint passes.
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
# Stubbed dependencies — same pattern as test_dashboard_smoke.py.
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


def _base_config(*, dry_run_default: bool = True) -> dict:
    return {
        "dashboard": {"auth": {"enabled": False}},
        "archiving": {
            "verify_checksum": False,
            "dry_run": dry_run_default,
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
    """Boot a real ``create_app`` with a tiny seeded SQLite + an
    ArchiveEngine.archive_files stub that records kwargs."""
    db_path = tmp_path / "archive.db"
    db = Database({"path": str(db_path)})
    db.connect()

    # Seed: one source with archive_dest, one completed scan, one file.
    archive_dest = str(tmp_path / "archive")
    os.makedirs(archive_dest, exist_ok=True)
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path, archive_dest) "
            "VALUES ('s1', '/share', ?)",
            (archive_dest,),
        )
        cur.execute(
            "INSERT INTO scan_runs (source_id, status, completed_at) "
            "VALUES (1, 'completed', '2026-01-01 00:00:00')"
        )
        cur.execute(
            "INSERT INTO scanned_files "
            "(source_id, scan_id, file_path, relative_path, file_name, "
            " file_size, last_access_time) "
            "VALUES (1, 1, '/share/a.txt', 'a.txt', 'a.txt', 100, "
            "        '2020-01-01 00:00:00')"
        )

    # Capture every archive_files invocation so tests can assert the
    # endpoint's confirm/dry_run plumbing.
    calls: list[dict] = []

    def fake_archive_files(self, files, archive_dest_, source_unc,
                           source_id, archived_by="manual",
                           dry_run=None, trigger_type="manual",
                           trigger_detail=None):
        calls.append({
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
            "total_size": 100,
            "total_size_formatted": "100 B",
            "errors": [],
            "dry_run": bool(dry_run),
        }

    from src.archiver import archive_engine as _ae
    monkeypatch.setattr(_ae.ArchiveEngine, "archive_files", fake_archive_files)

    # Also patch the policy engine to return our seeded file regardless
    # of its access age — saves us mocking out the freshness logic.
    from src.archiver import archive_policy as _ap

    def fake_get_files_by_days(self, source_id, scan_id, days):
        return [{
            "id": 1, "source_id": 1, "scan_id": 1,
            "file_path": "/share/a.txt", "file_name": "a.txt",
            "file_size": 100, "relative_path": "a.txt",
        }]

    monkeypatch.setattr(
        _ap.ArchivePolicyEngine, "get_files_by_days", fake_get_files_by_days,
    )

    # The /api/archive/selective endpoint calls a legacy
    # ``db.get_source(source_id)`` method that exists nowhere in
    # ``src/storage/database.py`` — it predates this PR and is tracked
    # separately. For the confirm-gate tests we shim it onto our
    # Database instance so the happy path can run end-to-end.
    def _get_source_shim(source_id):
        src_obj = db.get_source_by_id(source_id)
        if not src_obj:
            return None
        return {
            "id": src_obj.id, "name": src_obj.name,
            "unc_path": src_obj.unc_path,
            "archive_dest": src_obj.archive_dest,
        }

    db.get_source = _get_source_shim  # type: ignore[attr-defined]

    cfg = _base_config(dry_run_default=True)
    app = create_app(
        db,
        cfg,
        analytics=_StubAnalytics(),
        ad_lookup=_StubADLookup(),
        email_notifier=_StubEmailNotifier(),
    )
    tc = TestClient(app)
    tc.archive_calls = calls  # piggy-back the call recorder onto the client
    return tc


# ---------------------------------------------------------------------------
# /api/archive/run
# ---------------------------------------------------------------------------


def test_run_dry_run_without_confirm_ok(client):
    resp = client.post("/api/archive/run", json={
        "source_id": 1, "days": 30, "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    assert client.archive_calls, "engine should have been invoked"
    assert client.archive_calls[-1]["dry_run"] is True


def test_run_real_without_confirm_blocked(client):
    resp = client.post("/api/archive/run", json={
        "source_id": 1, "days": 30, "dry_run": False,
    })
    assert resp.status_code == 400
    assert "confirm=true" in resp.text
    assert not client.archive_calls, (
        "engine must NOT be called when confirm gate refuses"
    )


def test_run_real_with_confirm_ok(client):
    resp = client.post("/api/archive/run", json={
        "source_id": 1, "days": 30, "dry_run": False, "confirm": True,
    })
    assert resp.status_code == 200, resp.text
    assert client.archive_calls[-1]["dry_run"] is False


def test_run_default_dry_run_from_config(client):
    """No explicit dry_run -> falls back to config (true) -> ok."""
    resp = client.post("/api/archive/run", json={
        "source_id": 1, "days": 30,
    })
    assert resp.status_code == 200, resp.text
    assert client.archive_calls[-1]["dry_run"] is True


# ---------------------------------------------------------------------------
# /api/archive/selective
# ---------------------------------------------------------------------------


def test_selective_dry_run_without_confirm_ok(client):
    resp = client.post("/api/archive/selective", json={
        "source_id": 1, "file_ids": [1], "dry_run": True,
    })
    assert resp.status_code == 200, resp.text
    assert client.archive_calls[-1]["dry_run"] is True


def test_selective_real_without_confirm_blocked(client):
    resp = client.post("/api/archive/selective", json={
        "source_id": 1, "file_ids": [1], "dry_run": False,
    })
    assert resp.status_code == 400
    assert "confirm=true" in resp.text
    assert not client.archive_calls


def test_selective_real_with_confirm_ok(client):
    resp = client.post("/api/archive/selective", json={
        "source_id": 1, "file_ids": [1],
        "dry_run": False, "confirm": True,
    })
    assert resp.status_code == 200, resp.text
    assert client.archive_calls[-1]["dry_run"] is False
