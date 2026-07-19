"""Bulk background archive job (#archive-all): worker + endpoints.

The worker archives the FULL set of files matching an AI insight in batches.
These tests pin the data-safety guardrails an adversarial review required for a
1.7M-file / 11.3 TB run — especially that the loop **terminates** (keyset cursor,
not a NOT-EXISTS-only advance) and that a dry-run is a true no-op.

Worker tests call ``run_archive_job`` directly (no thread) and need no fastapi,
so they run everywhere. Endpoint tests are fastapi-gated (Docker CI).
"""

from __future__ import annotations

import importlib.util
import os

import pytest

from src.archiver.archive_job_worker import run_archive_job
from src.archiver.insight_queries import insight_where
from src.storage.database import Database

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
requires_fastapi = pytest.mark.skipif(
    not HAS_FASTAPI, reason="fastapi not installed in this environment")

# last_access_time old enough to match stale_3year (>1095 days).
_OLD = "2016-01-01 00:00:00"
_RECENT = "2026-07-01 00:00:00"


def _mk_db(tmp_path):
    db = Database({"path": str(tmp_path / "aj.db"),
                   "retention": {"auto_cleanup_on_startup": False}})
    db.connect()
    return db


def _seed(db, tmp_path, n=5, make_files=False):
    """Source (with archive_dest) + one completed scan + n stale files (+ 1
    recent non-match). When make_files, create the real source files on disk so
    a real (non-dry-run) archive has something to move."""
    src_dir = tmp_path / "src"
    dest_dir = tmp_path / "arsiv"
    src_dir.mkdir(exist_ok=True)
    dest_dir.mkdir(exist_ok=True)
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path, archive_dest) "
                    "VALUES('e', ?, ?)", (str(src_dir), str(dest_dir)))
        cur.execute("INSERT INTO scan_runs(source_id, status) VALUES(1,'completed')")
        for i in range(1, n + 1):
            rel = f"f{i}.txt"
            fp = str(src_dir / rel)
            if make_files:
                (src_dir / rel).write_text(f"content-{i}\n")
            cur.execute(
                "INSERT INTO scanned_files(source_id,scan_id,file_path,"
                "relative_path,file_name,extension,file_size,last_access_time) "
                "VALUES(1,1,?,?,?,'txt',?,?)", (fp, rel, rel, 10 + i, _OLD))
        # a recent file that must NOT match stale_3year
        cur.execute(
            "INSERT INTO scanned_files(source_id,scan_id,file_path,relative_path,"
            "file_name,extension,file_size,last_access_time) "
            "VALUES(1,1,?,?,?,'txt',5,?)",
            (str(src_dir / "recent.txt"), "recent.txt", "recent.txt", _RECENT))
    return str(dest_dir)


def _cfg(dry_run=True, batch_size=2, allow=False):
    return {
        "archiving": {
            "dry_run": dry_run, "verify_checksum": True,
            "cleanup_empty_dirs": False, "allow_bulk_daemon": allow,
            "background_jobs": {"batch_size": batch_size,
                                "sleep_between_batches_seconds": 0,
                                "resume_on_startup": True},
        },
        "backup": {"enabled": False},
    }


def _job(db, dest, insight="stale_3year", dry_run=True):
    wf, ex = insight_where(insight)
    matched, size = db.count_insight_unarchived(1, 1, wf, ex)
    jid = db.create_archive_job(1, insight, 1, dest, dry_run, matched, size)
    return jid, matched


# ── worker (no fastapi) ────────────────────────────────────────────────

def test_dry_run_terminates_and_moves_nothing(tmp_path):
    """The anti-infinite-loop regression + dry-run no-op."""
    db = _mk_db(tmp_path)
    dest = _seed(db, tmp_path, n=5)
    jid, matched = _job(db, dest, dry_run=True)
    run_archive_job(db, _cfg(dry_run=True, batch_size=2), jid)  # must RETURN
    job = db.get_archive_job(jid)
    assert job["status"] == "completed"
    assert job["archived"] == matched == 5
    with db.get_read_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM archived_files")
        assert cur.fetchone()["n"] == 0          # dry-run moved nothing
    db.close()


def test_batch_count_matches_keyset(tmp_path, monkeypatch):
    db = _mk_db(tmp_path)
    dest = _seed(db, tmp_path, n=5)
    calls = {"n": 0}
    real = Database.fetch_insight_batch

    def spy(self, *a, **k):
        calls["n"] += 1
        return real(self, *a, **k)
    monkeypatch.setattr(Database, "fetch_insight_batch", spy)
    jid, _ = _job(db, dest, dry_run=True)
    run_archive_job(db, _cfg(batch_size=2), jid)
    # 5 files / 2 per batch => 3 full fetches + 1 empty fetch that ends the loop
    assert calls["n"] == 4
    db.close()


def test_real_run_moves_files_then_double_run_noop(tmp_path):
    db = _mk_db(tmp_path)
    dest = _seed(db, tmp_path, n=4, make_files=True)
    jid, matched = _job(db, dest, dry_run=False)
    run_archive_job(db, _cfg(dry_run=False, batch_size=2, allow=True), jid)
    job = db.get_archive_job(jid)
    assert job["status"] == "completed", job["error"]
    assert job["archived"] == matched == 4
    # sources gone, dest populated, archived_files recorded
    assert not (tmp_path / "src" / "f1.txt").exists()
    assert (tmp_path / "arsiv" / "f1.txt").exists()
    with db.get_read_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM archived_files")
        assert cur.fetchone()["n"] == 4
    # a second job over the same set archives 0 (NOT EXISTS skip)
    jid2, matched2 = _job(db, dest, dry_run=False)
    assert matched2 == 0            # count already excludes archived
    db.close()


def test_single_snapshot_for_whole_job(tmp_path, monkeypatch):
    db = _mk_db(tmp_path)
    dest = _seed(db, tmp_path, n=5, make_files=True)
    from src.archiver.archive_engine import ArchiveEngine
    snaps = {"n": 0}
    monkeypatch.setattr(ArchiveEngine, "_maybe_pre_apply_snapshot",
                        lambda self, reason="": snaps.__setitem__("n", snaps["n"] + 1))
    jid, _ = _job(db, dest, dry_run=False)
    run_archive_job(db, _cfg(dry_run=False, batch_size=2, allow=True), jid)
    assert db.get_archive_job(jid)["status"] == "completed"
    assert snaps["n"] == 1          # exactly one snapshot over ~3 batches
    db.close()


def test_stall_guard_stops_on_zero_progress_batch(tmp_path, monkeypatch):
    """A real non-empty batch that archives nothing must FAIL the job, not loop."""
    db = _mk_db(tmp_path)
    dest = _seed(db, tmp_path, n=5, make_files=True)
    from src.archiver.archive_engine import ArchiveEngine
    monkeypatch.setattr(ArchiveEngine, "_maybe_pre_apply_snapshot",
                        lambda self, reason="": None)
    monkeypatch.setattr(
        ArchiveEngine, "archive_files",
        lambda self, files, *a, **k: {
            "archived": 0, "failed": len(files), "total_size": 0, "errors": []})
    jid, _ = _job(db, dest, dry_run=False)
    run_archive_job(db, _cfg(dry_run=False, batch_size=2, allow=True), jid)
    job = db.get_archive_job(jid)
    assert job["status"] == "failed"
    assert "ilerlemedi" in (job["error"] or "")
    db.close()


def test_disk_full_at_start_fails_before_moving(tmp_path, monkeypatch):
    db = _mk_db(tmp_path)
    dest = _seed(db, tmp_path, n=5, make_files=True)
    import src.archiver.archive_job_worker as w

    class _DU:
        free = 1
    monkeypatch.setattr(w.shutil, "disk_usage", lambda p: _DU())
    jid, _ = _job(db, dest, dry_run=False)
    run_archive_job(db, _cfg(dry_run=False, batch_size=2, allow=True), jid)
    job = db.get_archive_job(jid)
    assert job["status"] == "failed"
    assert "yer yok" in (job["error"] or "")
    assert job["archived"] == 0
    db.close()


def test_cancel_between_batches(tmp_path):
    db = _mk_db(tmp_path)
    dest = _seed(db, tmp_path, n=6)
    jid, matched = _job(db, dest, dry_run=True)
    db.request_archive_job_cancel(jid)     # cancel before the loop even starts
    run_archive_job(db, _cfg(batch_size=2), jid)
    job = db.get_archive_job(jid)
    assert job["status"] == "cancelled"
    assert job["archived"] < matched
    db.close()


def test_resume_skips_already_archived(tmp_path):
    """A resumed real job completes only the remainder (NOT EXISTS skip)."""
    db = _mk_db(tmp_path)
    dest = _seed(db, tmp_path, n=4, make_files=True)
    # pretend f1/f2 were already archived in a prior (interrupted) run
    with db.get_cursor() as cur:
        for rel in ("f1.txt", "f2.txt"):
            cur.execute(
                "INSERT INTO archived_files(source_id, original_path, "
                "archive_path, file_name, relative_path, extension, file_size) "
                "VALUES(1,?,?,?,?, 'txt', 10)",
                (str(tmp_path / "src" / rel), str(tmp_path / "arsiv" / rel),
                 rel, rel))
    wf, ex = insight_where("stale_3year")
    matched, size = db.count_insight_unarchived(1, 1, wf, ex)
    assert matched == 2                    # 4 - 2 already archived
    jid = db.create_archive_job(1, "stale_3year", 1, dest, False, matched, size)
    run_archive_job(db, _cfg(dry_run=False, batch_size=5, allow=True), jid)
    job = db.get_archive_job(jid)
    assert job["status"] == "completed"
    assert job["archived"] == 2
    db.close()


def test_second_active_job_rejected_at_db(tmp_path):
    import sqlite3
    db = _mk_db(tmp_path)
    dest = _seed(db, tmp_path, n=3)
    db.create_archive_job(1, "stale_3year", 1, dest, True, 3, 30)
    with pytest.raises(sqlite3.IntegrityError):
        db.create_archive_job(1, "stale_1year", 1, dest, True, 3, 30)
    db.close()


# ── endpoints (fastapi-gated) ──────────────────────────────────────────

def _client(db):
    from fastapi.testclient import TestClient
    from src.dashboard.api import create_app
    return TestClient(create_app(
        db, {"dashboard": {"auth": {"enabled": False}},
             "archiving": {"dry_run": True, "verify_checksum": True,
                           "allow_bulk_daemon": False,
                           "background_jobs": {"batch_size": 2,
                                               "sleep_between_batches_seconds": 0}}}))


@requires_fastapi
def test_endpoint_dry_run_job_completes_and_moves_nothing(tmp_path):
    import time
    db = _mk_db(tmp_path)
    _seed(db, tmp_path, n=5)
    c = _client(db)
    r = c.post("/api/archive/by-insight/all",
               json={"type": "stale_3year", "source_id": 1})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dry_run"] is True
    assert body["feature_disabled_reason"] == "archiving.dry_run=true"
    jid = body["job_id"]
    deadline = time.time() + 8
    while time.time() < deadline:
        j = c.get(f"/api/archive/jobs/{jid}").json()
        if j["status"] in ("completed", "failed", "cancelled"):
            break
        time.sleep(0.1)
    assert j["status"] == "completed"
    assert j["archived"] == body["total_matched"] == 5
    with db.get_read_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM archived_files")
        assert cur.fetchone()["n"] == 0
    db.close()


@requires_fastapi
def test_endpoint_second_job_is_409(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, tmp_path, n=5)
    # pre-create an active job so the endpoint's guard fires deterministically
    db.create_archive_job(1, "stale_3year", 1, str(tmp_path / "arsiv"), True, 5, 50)
    c = _client(db)
    r = c.post("/api/archive/by-insight/all",
               json={"type": "stale_1year", "source_id": 1})
    assert r.status_code == 409
    db.close()


@requires_fastapi
def test_endpoint_unsupported_type_400(tmp_path):
    db = _mk_db(tmp_path)
    _seed(db, tmp_path, n=3)
    c = _client(db)
    r = c.post("/api/archive/by-insight/all",
               json={"type": "duplicates", "source_id": 1})
    assert r.status_code == 400
    db.close()
