"""Tests for issue #110 Phase 2: hard-delete + restore for the duplicate
quarantine flow.

SAFETY-CRITICAL coverage:
  * purge_one with valid sidecar removes the file + stamps purged_at
  * purge_one ABORTS on SHA-256 mismatch (forensic preserve, never delete)
  * purge_one writes an audit event on success / abort / skip-missing
  * purge_expired only fires for rows older than quarantine_days
  * purge_expired tolerates a row whose file was already deleted manually
  * restore moves the file back to original_path and stamps restored_at
  * restore refuses on collision (original_path already exists)
  * restore refuses if the row was already restored
  * restore refuses if the row was already purged

Stdlib only. tmp_path-rooted fixtures so every run is hermetic on Linux.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.archiver.duplicate_cleaner import (  # noqa: E402
    DuplicateCleaner,
    PurgeResult,
    RestoreResult,
    SAFETY_TOKEN_VALUE,
    PURGE_SAFETY_TOKEN_VALUE,
)


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────


def _config(tmp_path: Path, **overrides) -> dict:
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
    if overrides:
        cfg["duplicates"]["quarantine"].update(overrides)
    return cfg


def _make_db(tmp_path: Path) -> Database:
    db = Database({"path": str(tmp_path / "phase2.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (id, name, unc_path) VALUES (1, ?, ?)",
            ("test_src", str(tmp_path / "share")),
        )
        cur.execute(
            "INSERT INTO scan_runs (id, source_id, status) "
            "VALUES (1, 1, 'completed')"
        )
    return db


def _seed_files(db: Database, tmp_path: Path, paths: list[str],
                size: int = 64, name: str = "dup.bin") -> dict:
    share = tmp_path / "share"
    share.mkdir(parents=True, exist_ok=True)
    id_map: dict = {}
    with db.get_cursor() as cur:
        for rel in paths:
            fpath = share / rel
            fpath.parent.mkdir(parents=True, exist_ok=True)
            data = (name + ":" + rel).encode()
            if len(data) < size:
                data = data + b"\0" * (size - len(data))
            else:
                data = data[:size]
            fpath.write_bytes(data)
            cur.execute(
                "INSERT INTO scanned_files "
                "(source_id, scan_id, file_path, relative_path, "
                "file_name, extension, file_size) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (1, 1, str(fpath), rel, name,
                 os.path.splitext(name)[1].lstrip(".") or None, size),
            )
            id_map[str(fpath)] = cur.lastrowid
    return id_map


def _make_cleaner(tmp_path: Path, **overrides):
    db = _make_db(tmp_path)
    cfg = _config(tmp_path, **overrides)
    return db, DuplicateCleaner(db, cfg), cfg


def _quarantine_two(tmp_path: Path):
    """Helper: seed 3 dupes, quarantine 2 of them (last-copy guard keeps
    one). Returns (db, cleaner, quarantine_log_ids).
    """
    db, cleaner, _cfg = _make_cleaner(tmp_path)
    ids = _seed_files(
        db, tmp_path, ["a/dup.bin", "b/dup.bin", "c/dup.bin"],
        size=80, name="dup.bin",
    )
    sorted_ids = sorted(ids.values())
    res = cleaner.quarantine(
        file_ids=sorted_ids[:2],
        confirm=True,
        safety_token=SAFETY_TOKEN_VALUE,
        moved_by="tester",
        source_id=1,
    )
    assert res.moved == 2, res
    qlog_ids: list[int] = []
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT id FROM quarantine_log ORDER BY id ASC"
        )
        qlog_ids = [int(r["id"]) for r in cur.fetchall()]
    return db, cleaner, qlog_ids


# ──────────────────────────────────────────────
# purge_one
# ──────────────────────────────────────────────


def test_purge_one_with_valid_sidecar_succeeds(tmp_path):
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    target_id = qlog_ids[0]
    # Sanity: file + sidecars exist before purge.
    with db.get_cursor() as cur:
        cur.execute("SELECT * FROM quarantine_log WHERE id = ?", (target_id,))
        row = dict(cur.fetchone())
    qpath = row["quarantine_path"]
    assert os.path.exists(qpath)
    assert os.path.exists(qpath + ".sha256")
    assert os.path.exists(qpath + ".manifest.json")

    result = cleaner.purge_one(target_id, purged_by="tester")
    assert isinstance(result, PurgeResult)
    assert result.status == "purged", result
    assert not os.path.exists(qpath)
    assert not os.path.exists(qpath + ".sha256")
    assert not os.path.exists(qpath + ".manifest.json")

    # purged_at stamped.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT purged_at, restored_at FROM quarantine_log WHERE id = ?",
            (target_id,),
        )
        r = dict(cur.fetchone())
    assert r["purged_at"] is not None
    assert r["restored_at"] is None


def test_purge_one_aborts_on_sha_mismatch(tmp_path):
    """SHA-256 mismatch = forensic preserve. NEVER delete the file."""
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    target_id = qlog_ids[0]
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT quarantine_path FROM quarantine_log WHERE id = ?",
            (target_id,),
        )
        qpath = dict(cur.fetchone())["quarantine_path"]

    # Corrupt the on-disk file (sidecar still has the original digest).
    with open(qpath, "ab") as f:
        f.write(b"TAMPERED")

    result = cleaner.purge_one(target_id, purged_by="tester")
    assert result.status == "abort_sha_mismatch", result
    # Critical assertion: the file is STILL THERE.
    assert os.path.exists(qpath), \
        "SHA mismatch must NOT delete — forensic preserve"
    # purged_at must NOT be stamped.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT purged_at FROM quarantine_log WHERE id = ?",
            (target_id,),
        )
        assert dict(cur.fetchone())["purged_at"] is None
    # Audit event written for the SOC.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM file_audit_events "
            "WHERE event_type = 'duplicate_quarantine_purge_sha_mismatch'"
        )
        assert dict(cur.fetchone())["c"] >= 1


def test_purge_one_writes_audit_event(tmp_path):
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    result = cleaner.purge_one(qlog_ids[0], purged_by="tester")
    assert result.status == "purged"
    assert result.audit_event_id is not None
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM file_audit_events "
            "WHERE event_type = 'duplicate_quarantine_purged'"
        )
        assert dict(cur.fetchone())["c"] >= 1


def test_purge_one_skips_already_purged(tmp_path):
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    first = cleaner.purge_one(qlog_ids[0], purged_by="tester")
    assert first.status == "purged"
    second = cleaner.purge_one(qlog_ids[0], purged_by="tester")
    assert second.status == "skipped_already_purged"


def test_purge_one_skips_restored(tmp_path):
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    # Restore first.
    res = cleaner.restore(qlog_ids[0], restored_by="tester")
    assert res.status == "restored", res
    # Then attempt to purge — must refuse.
    p = cleaner.purge_one(qlog_ids[0], purged_by="tester")
    assert p.status == "skipped_restored"


def test_purge_one_not_found(tmp_path):
    _db, cleaner, _qlog_ids = _quarantine_two(tmp_path)
    result = cleaner.purge_one(99999, purged_by="tester")
    assert result.status == "skipped_not_found"


# ──────────────────────────────────────────────
# purge_expired
# ──────────────────────────────────────────────


def test_purge_expired_skips_recent(tmp_path):
    """Recent rows (within quarantine_days) are NOT purged."""
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    # The two rows just got moved_at=NOW. Calling purge_expired with the
    # default 30-day cutoff must skip them entirely.
    results = cleaner.purge_expired()
    assert results == [], (
        "fresh rows must not be purged by purge_expired"
    )
    # All rows still un-stamped.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM quarantine_log "
            "WHERE purged_at IS NULL"
        )
        assert dict(cur.fetchone())["c"] == len(qlog_ids)


def test_purge_expired_handles_missing_file(tmp_path):
    """A row whose file was deleted out-of-band must not abort the batch."""
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    # Force the rows to be ancient.
    old = (datetime.now() - timedelta(days=999)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    with db.get_cursor() as cur:
        cur.execute("UPDATE quarantine_log SET moved_at = ?", (old,))
    # Yank the file for the FIRST row out from under the cleaner.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT id, quarantine_path FROM quarantine_log "
            "ORDER BY id ASC"
        )
        rows = [dict(r) for r in cur.fetchall()]
    first_path = rows[0]["quarantine_path"]
    os.remove(first_path)
    # Also nuke its sidecars to simulate a complete manual cleanup.
    for s in (".sha256", ".manifest.json"):
        try:
            os.remove(first_path + s)
        except FileNotFoundError:
            pass

    results = cleaner.purge_expired(purged_by="scheduler")
    assert len(results) == len(qlog_ids), \
        "every candidate row must produce a result entry"
    statuses = sorted(r.status for r in results)
    # First file is missing; second is purged successfully.
    assert "skipped_missing" in statuses
    assert "purged" in statuses

    # Both rows have purged_at stamped (missing-on-disk also stamps so
    # we don't keep retrying every night).
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM quarantine_log "
            "WHERE purged_at IS NOT NULL"
        )
        assert dict(cur.fetchone())["c"] == len(qlog_ids)


def test_purge_expired_uses_now_argument(tmp_path):
    """now= override lets us walk the cutoff forward in tests."""
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    # Default now() — nothing eligible.
    assert cleaner.purge_expired() == []
    # Walk now forward by 365 days; quarantine_days=30 → everything is old.
    future = datetime.now() + timedelta(days=365)
    results = cleaner.purge_expired(now=future)
    assert len(results) == len(qlog_ids)
    assert all(r.status == "purged" for r in results)


# ──────────────────────────────────────────────
# restore
# ──────────────────────────────────────────────


def test_restore_succeeds_for_quarantined_file(tmp_path):
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    target_id = qlog_ids[0]
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT original_path, quarantine_path "
            "FROM quarantine_log WHERE id = ?",
            (target_id,),
        )
        r = dict(cur.fetchone())
    opath = r["original_path"]
    qpath = r["quarantine_path"]
    # Sanity: original is gone, quarantine has the file.
    assert not os.path.exists(opath)
    assert os.path.exists(qpath)

    result = cleaner.restore(target_id, restored_by="tester")
    assert isinstance(result, RestoreResult)
    assert result.status == "restored", result
    assert os.path.exists(opath), "file must be back at original_path"
    assert not os.path.exists(qpath), "quarantine_path must be empty"
    # restored_at stamped, audit emitted.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT restored_at, purged_at FROM quarantine_log WHERE id = ?",
            (target_id,),
        )
        row = dict(cur.fetchone())
    assert row["restored_at"] is not None
    assert row["purged_at"] is None
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM file_audit_events "
            "WHERE event_type = 'duplicate_quarantine_restored'"
        )
        assert dict(cur.fetchone())["c"] >= 1


def test_restore_refuses_on_collision(tmp_path):
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    target_id = qlog_ids[0]
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT original_path, quarantine_path "
            "FROM quarantine_log WHERE id = ?",
            (target_id,),
        )
        r = dict(cur.fetchone())
    opath = r["original_path"]
    qpath = r["quarantine_path"]
    # Plant a collision at original_path.
    Path(opath).parent.mkdir(parents=True, exist_ok=True)
    Path(opath).write_bytes(b"someone-else-put-this-here")

    result = cleaner.restore(target_id, restored_by="tester")
    assert result.status == "skipped_collision", result
    # Quarantine file untouched.
    assert os.path.exists(qpath)
    # Collision file untouched.
    assert Path(opath).read_bytes() == b"someone-else-put-this-here"
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT restored_at FROM quarantine_log WHERE id = ?",
            (target_id,),
        )
        assert dict(cur.fetchone())["restored_at"] is None


def test_restore_refuses_already_restored(tmp_path):
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    target_id = qlog_ids[0]
    first = cleaner.restore(target_id, restored_by="tester")
    assert first.status == "restored"
    second = cleaner.restore(target_id, restored_by="tester")
    assert second.status == "skipped_already_restored", second


def test_restore_refuses_already_purged(tmp_path):
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    target_id = qlog_ids[0]
    purged = cleaner.purge_one(target_id, purged_by="tester")
    assert purged.status == "purged"
    result = cleaner.restore(target_id, restored_by="tester")
    assert result.status == "skipped_already_purged", result


def test_restore_not_found(tmp_path):
    _db, cleaner, _qlog_ids = _quarantine_two(tmp_path)
    result = cleaner.restore(99999, restored_by="tester")
    assert result.status == "skipped_not_found"


# ──────────────────────────────────────────────
# Round-trip: quarantine → restore → re-quarantine OK
# ──────────────────────────────────────────────


def test_round_trip_quarantine_restore_keeps_db_consistent(tmp_path):
    db, cleaner, qlog_ids = _quarantine_two(tmp_path)
    target_id = qlog_ids[0]
    # Restore it.
    assert cleaner.restore(target_id).status == "restored"
    # restored_at IS NOT NULL.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT purged_at, restored_at FROM quarantine_log WHERE id = ?",
            (target_id,),
        )
        row = dict(cur.fetchone())
    assert row["restored_at"] is not None
    assert row["purged_at"] is None
    # Subsequent purge_expired (with future now) skips this row.
    future = datetime.now() + timedelta(days=365)
    results = cleaner.purge_expired(now=future)
    statuses = [r.status for r in results]
    # Only the OTHER row should be purged; the restored one is filtered.
    assert "purged" in statuses
    assert all(
        r.quarantine_log_id != target_id for r in results
    ), "restored row must not appear in purge_expired"
