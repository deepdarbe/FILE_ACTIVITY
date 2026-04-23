"""Tests for issue #58: GDPR retention engine.

Linux-runnable. Exercises CRUD, dry-run safety, fnmatch semantics, the
archive-engine handshake (mocked) and the attestation report.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.compliance.retention import RetentionEngine  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


class _StubArchiveEngine:
    """Minimal duck-typed stand-in for ArchiveEngine — records calls."""

    def __init__(self):
        self.calls: list[dict] = []

    def archive_files(self, files, archive_dest, operation_id=None,
                      source_id=None):
        self.calls.append({
            "files": list(files),
            "archive_dest": archive_dest,
            "operation_id": operation_id,
            "source_id": source_id,
        })
        return {"archived": len(files)}


@pytest.fixture
def engine(tmp_path):
    db_path = tmp_path / "ret.db"
    db = Database({"path": str(db_path)})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path) VALUES ('s', '/x')"
        )
        cur.execute("INSERT INTO scan_runs (source_id) VALUES (1)")
    cfg = {"compliance": {"retention": {"enabled": True}}}
    archiver = _StubArchiveEngine()
    return RetentionEngine(db, cfg, archive_engine=archiver), db, archiver


def _seed_file(db, file_path, modify_offset_days):
    """Insert a scanned_files row with last_modify_time relative to now."""
    mtime = datetime.now() - timedelta(days=modify_offset_days)
    fname = os.path.basename(file_path)
    ext = fname.rpartition(".")[2] if "." in fname else ""
    with db.get_cursor() as cur:
        cur.execute(
            """INSERT INTO scanned_files
               (source_id, scan_id, file_path, relative_path, file_name,
                extension, file_size, last_modify_time, owner)
               VALUES (1, 1, ?, ?, ?, ?, 100,
                       ?, 'alice')""",
            (file_path, fname, fname, ext,
             mtime.strftime("%Y-%m-%d %H:%M:%S")),
        )


# ──────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────


def test_crud_add_list_remove_policies(engine):
    e, _db, _ = engine
    pid = e.add_policy("logs_30d", "*.log", 30, "delete")
    assert pid > 0
    pid2 = e.add_policy("invoices_5y", "*/invoices/*.pdf", 1825, "archive")
    assert pid2 > pid

    rows = e.list_policies()
    names = [r["name"] for r in rows]
    assert "logs_30d" in names
    assert "invoices_5y" in names
    # Sorted by name ASC.
    assert names == sorted(names)

    assert e.remove_policy("logs_30d") is True
    assert e.remove_policy("logs_30d") is False  # already gone
    remaining = [r["name"] for r in e.list_policies()]
    assert "logs_30d" not in remaining
    assert "invoices_5y" in remaining


def test_add_policy_validates_action_and_days(engine):
    e, _db, _ = engine
    with pytest.raises(ValueError):
        e.add_policy("bad_action", "*.log", 30, "shred")
    with pytest.raises(ValueError):
        e.add_policy("bad_days", "*.log", 0, "delete")


def test_apply_dry_run_never_touches_disk(engine, tmp_path):
    e, db, archiver = engine
    f = tmp_path / "old.log"
    f.write_text("data", encoding="utf-8")
    _seed_file(db, str(f), modify_offset_days=120)
    # A file too new — must NOT match.
    f2 = tmp_path / "new.log"
    f2.write_text("data", encoding="utf-8")
    _seed_file(db, str(f2), modify_offset_days=5)

    e.add_policy("logs_30d", "*.log", 30, "delete")
    result = e.apply("logs_30d", dry_run=True)

    # Counts: one match, both files still on disk.
    assert result["matched"] == 1
    assert result["processed"] == 1
    assert result["dry_run"] is True
    assert f.exists() is True
    assert f2.exists() is True
    # Archiver must NOT have been called for a delete-action policy.
    assert archiver.calls == []


def test_apply_archive_action_invokes_archive_engine(engine, tmp_path):
    e, db, archiver = engine
    f = tmp_path / "old.pdf"
    f.write_text("data", encoding="utf-8")
    _seed_file(db, str(f), modify_offset_days=2000)

    e.add_policy("invoices_5y", "*.pdf", 1825, "archive")
    result = e.apply("invoices_5y", dry_run=False)
    assert result["matched"] == 1
    assert result["processed"] == 1
    assert result["dry_run"] is False
    assert len(archiver.calls) == 1
    archived_paths = [
        r["file_path"] for r in archiver.calls[0]["files"]
    ]
    assert str(f) in archived_paths


def test_pattern_match_uses_fnmatch(engine, tmp_path):
    e, db, _ = engine
    log_file = tmp_path / "app.log"
    log_file.write_text("x", encoding="utf-8")
    pdf_file = tmp_path / "report.pdf"
    pdf_file.write_text("x", encoding="utf-8")
    _seed_file(db, str(log_file), modify_offset_days=120)
    _seed_file(db, str(pdf_file), modify_offset_days=120)

    e.add_policy("only_logs", "*.log", 30, "delete")
    result = e.apply("only_logs", dry_run=True)
    assert result["matched"] == 1

    # Direct helper check too.
    assert e._matches_pattern("/srv/share/foo.log", "*.log") is True
    assert e._matches_pattern("/srv/share/foo.pdf", "*.log") is False
    # Empty pattern matches everything (operator's blanket retention).
    assert e._matches_pattern("/anything", "") is True


def test_attestation_report_returns_expected_structure(engine, tmp_path):
    e, db, _ = engine
    f = tmp_path / "old.log"
    f.write_text("x", encoding="utf-8")
    _seed_file(db, str(f), modify_offset_days=120)

    e.add_policy("logs_30d", "*.log", 30, "delete")
    e.apply("logs_30d", dry_run=True)

    report = e.attestation_report(since_days=30)
    assert "since_days" in report
    assert report["since_days"] == 30
    assert "generated_at" in report
    assert "totals" in report
    assert report["totals"]["delete"] >= 1
    assert "by_policy" in report
    assert any(
        r["policy"] == "logs_30d" and r["action"] == "delete"
        for r in report["by_policy"]
    )
    assert "events" in report
    assert any(ev["policy"] == "logs_30d" for ev in report["events"])
