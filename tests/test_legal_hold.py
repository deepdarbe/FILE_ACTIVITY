"""Tests for issue #59: legal hold registry.

Coverage (8 tests, all Linux-runnable):
  * add_hold returns ID, persists row, and writes audit event.
  * is_held matches fnmatch glob (``/share/finance/*``).
  * is_held returns None when no active hold matches.
  * release_hold stamps released_at and marks the hold inactive.
  * release_hold returns False on already-released or unknown id.
  * list_active filters released holds out.
  * count_held_paths joins legal_holds + scanned_files correctly.
  * archive_files integration: held file is skipped + audited and the
    summary's ``skipped_held`` counter is incremented.
"""

from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.compliance.legal_hold import LegalHoldRegistry  # noqa: E402
from src.archiver.archive_engine import ArchiveEngine  # noqa: E402


# ── Fixtures ───────────────────────────────────────────────


def _make_db(tmp_path) -> Database:
    db = Database({"path": str(tmp_path / "lh.db")})
    db.connect()
    # Seed a source so audit events with source_id=1 satisfy the FK.
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (id, name, unc_path) VALUES (1, ?, ?)",
            ("test_src", "/share"),
        )
    return db


def _make_registry(tmp_path) -> tuple[Database, LegalHoldRegistry]:
    db = _make_db(tmp_path)
    reg = LegalHoldRegistry(db, {})
    return db, reg


# ── Mutations ──────────────────────────────────────────────


def test_add_hold_returns_id_persists_row_and_audits(tmp_path):
    db, reg = _make_registry(tmp_path)
    hold_id = reg.add_hold(
        pattern="/share/finance/*",
        reason="SEC investigation 2026-Q2",
        case_ref="SEC-2026-001",
        created_by="alice",
    )
    assert isinstance(hold_id, int) and hold_id > 0

    with db.get_cursor() as cur:
        cur.execute("SELECT * FROM legal_holds WHERE id = ?", (hold_id,))
        row = cur.fetchone()
    assert row is not None
    assert row["path_pattern"] == "/share/finance/*"
    assert row["reason"] == "SEC investigation 2026-Q2"
    assert row["case_reference"] == "SEC-2026-001"
    assert row["created_by"] == "alice"
    assert row["released_at"] is None

    # Audit event recorded.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT * FROM file_audit_events "
            "WHERE event_type = 'legal_hold_added' "
            "ORDER BY id DESC LIMIT 1"
        )
        ev = cur.fetchone()
    assert ev is not None
    assert ev["username"] == "alice"
    assert ev["file_path"] == "/share/finance/*"
    assert "Legal hold" in (ev["details"] or "")


# ── is_held ────────────────────────────────────────────────


def test_is_held_matches_fnmatch_pattern(tmp_path):
    _db, reg = _make_registry(tmp_path)
    reg.add_hold("/share/finance/*", "audit", "C1", "alice")
    held = reg.is_held("/share/finance/x.txt")
    assert held is not None
    assert held["path_pattern"] == "/share/finance/*"
    # And recursive globs work too.
    reg.add_hold("/legal/**/*.eml", "preserve", None, "alice")
    # fnmatch treats ** the same as *, so a single-segment match still hits.
    assert reg.is_held("/legal/case42/note.eml") is not None


def test_is_held_returns_none_when_no_active_hold(tmp_path):
    _db, reg = _make_registry(tmp_path)
    assert reg.is_held("/share/finance/x.txt") is None
    # Unrelated hold doesn't match.
    reg.add_hold("/other/*", "noop", None, "alice")
    assert reg.is_held("/share/finance/x.txt") is None


# ── release_hold ───────────────────────────────────────────


def test_release_hold_marks_inactive_and_stamps_metadata(tmp_path):
    db, reg = _make_registry(tmp_path)
    hold_id = reg.add_hold("/share/finance/*", "audit", "C1", "alice")
    assert reg.is_held("/share/finance/x.txt") is not None

    ok = reg.release_hold(hold_id, released_by="bob")
    assert ok is True
    assert reg.is_held("/share/finance/x.txt") is None

    with db.get_cursor() as cur:
        cur.execute("SELECT released_at, released_by FROM legal_holds WHERE id = ?",
                    (hold_id,))
        row = cur.fetchone()
    assert row["released_at"] is not None
    assert row["released_by"] == "bob"

    # Release event audited.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT * FROM file_audit_events "
            "WHERE event_type = 'legal_hold_released' "
            "ORDER BY id DESC LIMIT 1"
        )
        ev = cur.fetchone()
    assert ev is not None and ev["username"] == "bob"


def test_release_hold_returns_false_on_unknown_or_already_released(tmp_path):
    _db, reg = _make_registry(tmp_path)
    assert reg.release_hold(9999, released_by="bob") is False
    hold_id = reg.add_hold("/x/*", "r", None, "alice")
    assert reg.release_hold(hold_id, released_by="bob") is True
    # Idempotent — second release is a no-op.
    assert reg.release_hold(hold_id, released_by="bob") is False


# ── list / count ───────────────────────────────────────────


def test_list_active_filters_released(tmp_path):
    _db, reg = _make_registry(tmp_path)
    a = reg.add_hold("/a/*", "ra", None, "alice")
    b = reg.add_hold("/b/*", "rb", None, "alice")
    c = reg.add_hold("/c/*", "rc", None, "alice")
    reg.release_hold(b, released_by="bob")

    actives = reg.list_active()
    ids = sorted(h["id"] for h in actives)
    assert ids == sorted([a, c])

    # History includes the released one.
    hist = reg.list_history()
    all_ids = sorted(h["id"] for h in hist["holds"])
    assert all_ids == sorted([a, b, c])
    assert hist["total"] == 3


def test_count_held_paths_joins_legal_holds_and_scanned_files(tmp_path):
    db, reg = _make_registry(tmp_path)
    # Seed scanned_files: 3 finance, 2 hr, 1 unrelated.
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO scan_runs (id, source_id) VALUES (1, 1)")
        rows = [
            (1, 1, "/share/finance/a.txt", "finance/a.txt", "a.txt", "txt", 10),
            (1, 1, "/share/finance/b.txt", "finance/b.txt", "b.txt", "txt", 20),
            (1, 1, "/share/finance/sub/c.txt", "finance/sub/c.txt", "c.txt", "txt", 30),
            (1, 1, "/share/hr/x.doc", "hr/x.doc", "x.doc", "doc", 5),
            (1, 1, "/share/hr/y.doc", "hr/y.doc", "y.doc", "doc", 6),
            (1, 1, "/share/marketing/z.png", "marketing/z.png", "z.png", "png", 7),
        ]
        for r in rows:
            cur.execute(
                "INSERT INTO scanned_files "
                "(scan_id, source_id, file_path, relative_path, file_name, "
                "extension, file_size) VALUES (?, ?, ?, ?, ?, ?, ?)",
                r,
            )

    # No holds → 0.
    assert reg.count_held_paths() == 0

    reg.add_hold("/share/finance/*", "audit", "C1", "alice")
    # SQLite GLOB '*' is greedy across slashes (unlike shell fnmatch),
    # so all three files under /share/finance/ match — including the
    # one nested in sub/. count_held_paths uses GLOB for cheap counting,
    # which is exactly what operators want for a sidebar badge ("how
    # many scanned files would be blocked"). 3 expected.
    assert reg.count_held_paths() == 3

    reg.add_hold("/share/hr/*", "audit", "C1", "alice")
    # Now 3 + 2 = 5, with no overlap.
    assert reg.count_held_paths() == 5

    # Releasing one drops the count.
    actives = reg.list_active()
    finance_id = next(h["id"] for h in actives if "finance" in h["path_pattern"])
    reg.release_hold(finance_id, released_by="bob")
    assert reg.count_held_paths() == 2

    # source_id filter scopes correctly.
    assert reg.count_held_paths(source_id=1) == 2
    assert reg.count_held_paths(source_id=999) == 0


# ── ArchiveEngine integration ──────────────────────────────


def test_archive_files_skips_held_and_audits(tmp_path):
    db = _make_db(tmp_path)
    reg = LegalHoldRegistry(db, {})

    # Real on-disk files so archive_files can do its copy/checksum/delete.
    src_root = tmp_path / "src"
    dst_root = tmp_path / "dst"
    (src_root / "finance").mkdir(parents=True)
    (src_root / "marketing").mkdir(parents=True)
    held_file = src_root / "finance" / "secret.txt"
    free_file = src_root / "marketing" / "ok.txt"
    held_file.write_text("HELD")
    free_file.write_text("FREE")

    # Hold matches the finance file only.
    reg.add_hold(str(src_root / "finance" / "*"), "audit", "C1", "alice")

    engine = ArchiveEngine(db, {"archiving": {"verify_checksum": False,
                                              "cleanup_empty_dirs": False}})
    files = [
        {
            "file_path": str(held_file),
            "relative_path": "finance/secret.txt",
            "file_name": "secret.txt",
            "extension": "txt",
            "file_size": held_file.stat().st_size,
        },
        {
            "file_path": str(free_file),
            "relative_path": "marketing/ok.txt",
            "file_name": "ok.txt",
            "extension": "txt",
            "file_size": free_file.stat().st_size,
        },
    ]

    summary = engine.archive_files(
        files=files,
        archive_dest=str(dst_root),
        source_unc=str(src_root),
        source_id=1,
        archived_by="test",
    )

    assert summary["skipped_held"] == 1
    assert summary["archived"] == 1
    # Held file remained on disk; free file was moved.
    assert held_file.exists(), "held file must NOT be removed by archiver"
    assert not free_file.exists(), "free file should have been moved to dst"
    assert (dst_root / "marketing" / "ok.txt").exists()

    # Skip event audited.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT * FROM file_audit_events "
            "WHERE event_type = 'archive_skipped_legal_hold'"
        )
        skip_events = cur.fetchall()
    assert len(skip_events) == 1
    assert skip_events[0]["file_path"] == str(held_file)
    assert "Hold #" in (skip_events[0]["details"] or "")
