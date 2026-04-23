"""Tests for issue #49: NTFS ACL / effective-permissions analyzer.

Designed to run on Linux. Anything that requires a real Windows ACL
read is gated behind ``sys.platform == 'win32'``; the rest exercises the
DB-backed queries against synthetic ``file_acl_snapshots`` rows in a tmp
SQLite, plus pure-Python helpers (``_mask_to_name``, ``is_supported``).
"""

from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.security.acl_analyzer import AclAnalyzer  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def analyzer(tmp_path):
    db_path = tmp_path / "acl.db"
    db = Database({"path": str(db_path)})
    db.connect()
    # Seed scan_runs (1, 2) so the file_acl_snapshots FK to scan_runs is
    # satisfied. file_acl_snapshots.scan_id REFERENCES scan_runs(id).
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources (name, unc_path) VALUES ('t', '/t')")
        for _ in range(2):
            cur.execute("INSERT INTO scan_runs (source_id) VALUES (1)")
    cfg = {
        "security": {
            "acl_analyzer": {
                "enabled": True,
                "snapshot_during_scan": False,
                "sprawl_threshold_mask": 0x001301BF,
            }
        }
    }
    return AclAnalyzer(db, cfg), db


def _insert_snapshot(db, **kwargs):
    """Insert one synthetic file_acl_snapshots row with sane defaults."""
    row = {
        "scan_id": 1,
        "file_path": "/share/foo.txt",
        "trustee_sid": "S-1-1-0",  # Everyone
        "trustee_name": "Everyone",
        "permissions_mask": 0x001F01FF,  # FullControl
        "permission_name": "FullControl",
        "is_inherited": 0,
        "ace_type": "ALLOW",
    }
    row.update(kwargs)
    with db.get_cursor() as cur:
        cur.execute(
            """INSERT INTO file_acl_snapshots
               (scan_id, file_path, trustee_sid, trustee_name,
                permissions_mask, permission_name, is_inherited, ace_type)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                row["scan_id"], row["file_path"], row["trustee_sid"],
                row["trustee_name"], row["permissions_mask"],
                row["permission_name"], row["is_inherited"], row["ace_type"],
            ),
        )


# ──────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────


def test_is_supported_false_on_linux(analyzer):
    """On non-Windows hosts the analyzer must self-identify as unsupported."""
    a, _db = analyzer
    if sys.platform == "win32":
        pytest.skip("Linux-only assertion")
    assert a.is_supported() is False


def test_mask_to_name_resolves_known_bundles(analyzer):
    a, _ = analyzer
    assert a._mask_to_name(0x001F01FF) == "FullControl"
    assert a._mask_to_name(0x001301BF) == "Modify"
    assert a._mask_to_name(0x001200A9) == "Read+Execute"
    assert a._mask_to_name(0x00120089) == "Read"
    assert a._mask_to_name(0x00120116) == "Write"


def test_mask_to_name_falls_back_to_custom(analyzer):
    a, _ = analyzer
    out = a._mask_to_name(0xCAFEBABE)
    assert out.startswith("Custom (0x")
    # Hex must round-trip in upper-case 8 chars.
    assert "CAFEBABE" in out


def test_mask_to_name_handles_garbage(analyzer):
    a, _ = analyzer
    assert a._mask_to_name("not a mask").startswith("Custom (")


def test_get_effective_acl_raises_on_linux(analyzer, tmp_path):
    a, _ = analyzer
    if sys.platform == "win32":
        pytest.skip("Linux-only assertion: would actually read an ACL on Windows")
    with pytest.raises(NotImplementedError):
        a.get_effective_acl(str(tmp_path))


def test_snapshot_source_raises_on_linux(analyzer):
    a, _ = analyzer
    if sys.platform == "win32":
        pytest.skip("Linux-only assertion")
    with pytest.raises(NotImplementedError):
        a.snapshot_source(source_id=1, scan_id=1)


def test_detect_sprawl_groups_by_trustee(analyzer):
    """Sprawl picks up the Everyone-grants-Modify+ pattern."""
    a, db = analyzer
    # Everyone with FullControl on three files (severity well above Modify).
    for path in ("/share/a", "/share/b", "/share/c"):
        _insert_snapshot(db, file_path=path, trustee_sid="S-1-1-0",
                         trustee_name="Everyone",
                         permissions_mask=0x001F01FF,
                         permission_name="FullControl")
    # Domain Users with Modify on two files.
    for path in ("/share/x", "/share/y"):
        _insert_snapshot(db, file_path=path,
                         trustee_sid="S-1-5-21-DU",
                         trustee_name="DOMAIN\\Domain Users",
                         permissions_mask=0x001301BF,
                         permission_name="Modify")
    # One Read-only ACE — must NOT show up at threshold=Modify.
    _insert_snapshot(db, file_path="/share/readonly", trustee_sid="S-1-1-0",
                     permissions_mask=0x00120089, permission_name="Read")
    # One DENY entry — DENY doesn't grant access, must be excluded.
    _insert_snapshot(db, file_path="/share/denied", trustee_sid="S-1-1-0",
                     permissions_mask=0x001F01FF,
                     permission_name="FullControl",
                     ace_type="DENY")

    out = a.detect_sprawl()
    assert isinstance(out, list)
    # Two distinct trustees above threshold.
    sids = {row["trustee_sid"] for row in out}
    assert "S-1-1-0" in sids
    assert "S-1-5-21-DU" in sids

    by_sid = {row["trustee_sid"]: row for row in out}
    # Everyone has 3 distinct ALLOW paths above threshold.
    assert by_sid["S-1-1-0"]["file_count"] == 3
    # Domain Users has 2.
    assert by_sid["S-1-5-21-DU"]["file_count"] == 2
    # Order is by file_count DESC.
    assert out[0]["trustee_sid"] == "S-1-1-0"


def test_detect_sprawl_filters_by_scan_id(analyzer):
    a, db = analyzer
    _insert_snapshot(db, scan_id=1, file_path="/share/keep",
                     trustee_sid="S-1-1-0", trustee_name="Everyone")
    _insert_snapshot(db, scan_id=2, file_path="/share/skip",
                     trustee_sid="S-1-1-0", trustee_name="Everyone")
    out = a.detect_sprawl(scan_id=1)
    assert len(out) == 1
    assert out[0]["file_count"] == 1


def test_detect_sprawl_respects_threshold(analyzer):
    """Bumping the threshold above FullControl removes everything."""
    a, db = analyzer
    _insert_snapshot(db, file_path="/share/foo")
    out = a.detect_sprawl(severity_threshold=0x7FFFFFFF)
    assert out == []


def test_find_paths_for_trustee_returns_only_matching_sid(analyzer):
    a, db = analyzer
    _insert_snapshot(db, file_path="/a", trustee_sid="S-1-1-0",
                     trustee_name="Everyone")
    _insert_snapshot(db, file_path="/b", trustee_sid="S-1-1-0",
                     trustee_name="Everyone",
                     permissions_mask=0x001301BF, permission_name="Modify")
    _insert_snapshot(db, file_path="/other", trustee_sid="S-1-5-32-544",
                     trustee_name="Administrators")

    out = a.find_paths_for_trustee("S-1-1-0")
    paths = sorted(r["file_path"] for r in out)
    assert paths == ["/a", "/b"]
    # Permission_name carried through.
    perms = {r["file_path"]: r["permission_name"] for r in out}
    assert perms["/a"] == "FullControl"
    assert perms["/b"] == "Modify"


def test_find_paths_for_trustee_skips_deny_aces(analyzer):
    a, db = analyzer
    _insert_snapshot(db, file_path="/blocked", trustee_sid="S-1-1-0",
                     ace_type="DENY")
    out = a.find_paths_for_trustee("S-1-1-0")
    assert out == []


def test_find_paths_for_trustee_dedupes_per_path(analyzer):
    """Re-snapshotting the same path must not double-count."""
    a, db = analyzer
    # Two snapshots of the same path/trustee — only the most recent counts.
    _insert_snapshot(db, scan_id=1, file_path="/p", trustee_sid="S-1-1-0",
                     permissions_mask=0x00120089, permission_name="Read")
    _insert_snapshot(db, scan_id=2, file_path="/p", trustee_sid="S-1-1-0",
                     permissions_mask=0x001F01FF, permission_name="FullControl")
    out = a.find_paths_for_trustee("S-1-1-0")
    assert len(out) == 1
    # Latest mask wins.
    assert out[0]["permission_name"] == "FullControl"


def test_find_paths_for_trustee_limit_caps_results(analyzer):
    a, db = analyzer
    for i in range(20):
        _insert_snapshot(db, file_path=f"/f{i}", trustee_sid="S-1-1-0")
    out = a.find_paths_for_trustee("S-1-1-0", limit=5)
    assert len(out) == 5


def test_table_and_indexes_exist(analyzer):
    """Schema migration ran — table + indexes are present."""
    _, db = analyzer
    with db.get_cursor() as cur:
        cur.execute("SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name='file_acl_snapshots'")
        assert cur.fetchone() is not None
        cur.execute("SELECT name FROM sqlite_master "
                    "WHERE type='index' AND name='idx_acl_trustee'")
        assert cur.fetchone() is not None


def test_config_defaults_when_block_missing(tmp_path):
    """Constructor must tolerate a config dict with no 'security' key."""
    db_path = tmp_path / "acl.db"
    db = Database({"path": str(db_path)})
    db.connect()
    a = AclAnalyzer(db, {})
    assert a.enabled is True
    assert a.snapshot_during_scan is False
    assert a.sprawl_threshold_mask == 0x001301BF
