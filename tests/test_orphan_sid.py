"""Tests for issue #56: Orphaned-SID report + bulk reassignment.

Linux-runnable. The Windows-only path (``reassign_owner`` with
``dry_run=False``) is covered by asserting that it raises
``NotImplementedError`` on non-Windows hosts; everything else exercises
the DB-backed report against synthetic ``scanned_files`` rows in a tmp
SQLite, plus a stubbed ``ad_lookup`` that simulates orphaned vs.
resolved owners.
"""

from __future__ import annotations

import csv
import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.security.orphan_sid import OrphanSidAnalyzer  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Test doubles
# ──────────────────────────────────────────────────────────────────────


class StubADLookup:
    """Records every lookup call. Anything in ``orphans`` returns
    ``found=False`` (= orphaned); everything else returns ``found=True``.
    """

    def __init__(self, orphans):
        self.orphans = set(orphans)
        self.calls = []

    def lookup(self, name, force_refresh=False):  # noqa: D401 - stub
        self.calls.append(name)
        if name in self.orphans:
            return {
                "username": name,
                "email": None,
                "display_name": None,
                "found": False,
                "source": "live",
            }
        return {
            "username": name,
            "email": f"{name}@example.com",
            "display_name": name.upper(),
            "found": True,
            "source": "live",
        }


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def db_with_files(tmp_path):
    """Returns ``(db, source_id, scan_id)`` with three owners pre-seeded:

    - ``DOMAIN\\alice``  : 3 files (orphan)
    - ``DOMAIN\\bob``    : 2 files (resolved)
    - ``S-1-5-21-DELETED``: 4 files (orphan)
    """
    db = Database({"path": str(tmp_path / "orphan.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources (name, unc_path) VALUES ('s1', '/share')")
        source_id = cur.lastrowid
        cur.execute("INSERT INTO scan_runs (source_id) VALUES (?)", (source_id,))
        scan_id = cur.lastrowid

        rows = []
        for i in range(3):
            rows.append((source_id, scan_id,
                         f"/share/alice/file{i}.txt", f"alice/file{i}.txt",
                         f"file{i}.txt", "txt", 100 + i, "DOMAIN\\alice"))
        for i in range(2):
            rows.append((source_id, scan_id,
                         f"/share/bob/file{i}.txt", f"bob/file{i}.txt",
                         f"file{i}.txt", "txt", 200 + i, "DOMAIN\\bob"))
        for i in range(4):
            rows.append((source_id, scan_id,
                         f"/share/legacy/file{i}.txt", f"legacy/file{i}.txt",
                         f"file{i}.txt", "txt", 300 + i, "S-1-5-21-DELETED"))
        cur.executemany(
            """INSERT INTO scanned_files
               (source_id, scan_id, file_path, relative_path, file_name,
                extension, file_size, owner)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
    return db, source_id, scan_id


def _make_analyzer(db, ad_lookup, **overrides):
    cfg = {
        "security": {
            "orphan_sid": {
                "enabled": True,
                "cache_ttl_minutes": 1440,
                "max_unique_sids": 1000,
                **overrides,
            }
        }
    }
    return OrphanSidAnalyzer(db, cfg, ad_lookup=ad_lookup)


# ──────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────


def test_is_supported_false_on_linux(db_with_files):
    db, _src, _scan = db_with_files
    a = _make_analyzer(db, StubADLookup(orphans=[]))
    if sys.platform == "win32":
        pytest.skip("Linux-only assertion")
    assert a.is_supported() is False


def test_detect_orphans_finds_expected_sids(db_with_files):
    """Two of the three owners are orphaned; alice's 3 + legacy's 4
    files should add up to 7 orphan files.
    """
    db, _src, scan_id = db_with_files
    ad = StubADLookup(orphans=["DOMAIN\\alice", "S-1-5-21-DELETED"])
    a = _make_analyzer(db, ad)

    out = a.detect_orphans(scan_id)
    assert out["scan_id"] == scan_id
    assert out["total_files"] == 9
    assert out["total_orphan_files"] == 7

    sids = {row["sid"] for row in out["orphan_sids"]}
    assert sids == {"DOMAIN\\alice", "S-1-5-21-DELETED"}

    by_sid = {row["sid"]: row for row in out["orphan_sids"]}
    assert by_sid["DOMAIN\\alice"]["file_count"] == 3
    assert by_sid["S-1-5-21-DELETED"]["file_count"] == 4
    # sample_paths capped at 5; legacy has 4 so we get 4.
    assert len(by_sid["S-1-5-21-DELETED"]["sample_paths"]) == 4
    assert len(by_sid["DOMAIN\\alice"]["sample_paths"]) == 3
    # bob (resolved) must NOT appear.
    assert "DOMAIN\\bob" not in sids


def test_cache_persists_across_calls(db_with_files):
    """Second detect_orphans must use the cache and not re-query AD."""
    db, _src, scan_id = db_with_files
    ad = StubADLookup(orphans=["DOMAIN\\alice", "S-1-5-21-DELETED"])
    a = _make_analyzer(db, ad)

    a.detect_orphans(scan_id)
    first_call_count = len(ad.calls)
    assert first_call_count == 3  # alice + bob + legacy SID

    a.detect_orphans(scan_id)
    # No additional AD calls — every owner is in the cache and fresh.
    assert len(ad.calls) == first_call_count

    # Cache rows present.
    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM orphan_sid_cache")
        assert cur.fetchone()["c"] == 3
        cur.execute("SELECT resolved FROM orphan_sid_cache "
                    "WHERE sid = 'DOMAIN\\alice'")
        assert cur.fetchone()["resolved"] == 0
        cur.execute("SELECT resolved FROM orphan_sid_cache "
                    "WHERE sid = 'DOMAIN\\bob'")
        assert cur.fetchone()["resolved"] == 1


def test_get_orphan_files_paginates(db_with_files):
    db, source_id, _scan = db_with_files
    a = _make_analyzer(db, StubADLookup(orphans=[]))

    page1 = a.get_orphan_files(source_id, "S-1-5-21-DELETED",
                               page=1, page_size=2)
    assert page1["total"] == 4
    assert page1["page"] == 1
    assert len(page1["files"]) == 2

    page2 = a.get_orphan_files(source_id, "S-1-5-21-DELETED",
                               page=2, page_size=2)
    assert len(page2["files"]) == 2
    assert {f["file_path"] for f in page2["files"]} & \
           {f["file_path"] for f in page1["files"]} == set()

    page3 = a.get_orphan_files(source_id, "S-1-5-21-DELETED",
                               page=3, page_size=2)
    assert page3["files"] == []


def test_export_csv_writes_headers_and_rows(db_with_files, tmp_path):
    db, source_id, scan_id = db_with_files
    ad = StubADLookup(orphans=["DOMAIN\\alice", "S-1-5-21-DELETED"])
    a = _make_analyzer(db, ad)

    out_path = tmp_path / "orphan.csv"
    rows_written = a.export_csv(source_id, scan_id, str(out_path))
    assert rows_written == 7  # 3 alice + 4 legacy

    with open(out_path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        rows = list(reader)
    assert rows[0] == ["path", "owner_sid", "file_size",
                        "last_modify_time", "owner_resolved"]
    assert len(rows) - 1 == 7
    # bob's files (resolved owner) must NOT be in the export.
    paths = [r[0] for r in rows[1:]]
    assert not any("/bob/" in p for p in paths)
    # All rows record owner_resolved=false.
    assert all(r[4] == "false" for r in rows[1:])


def test_reassign_owner_raises_on_linux(db_with_files):
    db, source_id, _scan = db_with_files
    a = _make_analyzer(db, StubADLookup(orphans=[]))
    if sys.platform == "win32":
        pytest.skip("Linux-only assertion")
    with pytest.raises(NotImplementedError):
        a.reassign_owner(source_id, "DOMAIN\\alice",
                          new_owner="DOMAIN\\manager", dry_run=False)


# ──────────────────────────────────────────────────────────────────────
# Bonus coverage (defensive paths) — kept tight so we stay near the
# 6-test target while still exercising dry_run, validation, and the
# is_supported() docstring contract.
# ──────────────────────────────────────────────────────────────────────


def test_reassign_owner_dry_run_is_default_and_safe(db_with_files):
    """Dry run must work cross-platform and never call into pywin32."""
    db, source_id, _scan = db_with_files
    a = _make_analyzer(db, StubADLookup(orphans=[]))
    out = a.reassign_owner(source_id, "DOMAIN\\alice",
                            new_owner="DOMAIN\\manager")
    assert out["dry_run"] is True
    assert out["scanned"] == 3
    assert out["changed"] == 0
    assert out["errors"] == 0


def test_reassign_owner_rejects_blank_new_owner(db_with_files):
    db, source_id, _scan = db_with_files
    a = _make_analyzer(db, StubADLookup(orphans=[]))
    with pytest.raises(ValueError):
        a.reassign_owner(source_id, "DOMAIN\\alice", new_owner="   ")
