"""#308 consistency: report endpoints honor owner_scope (loop #7).

The naming-compliance, AI-insight, and top-creators report endpoints were
missed by the original #308 per-user-scoping sweep — a viewer role could
enumerate every owner's files (or, via top-creators, every owner's identity
and file count). These tests pin the DB-layer scoping that top-creators now
relies on; the file-list endpoints filter their cached/returned lists in
memory via ``get_scope_username`` (covered in test_user_scope.py).
"""
import pytest

from src.storage.database import Database


@pytest.fixture()
def seeded(tmp_path):
    d = Database({"path": str(tmp_path / "creators.db")})
    d.connect()
    with d.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path, archive_dest) VALUES('s','/s','/a')")
        sid = cur.lastrowid
        cur.execute("INSERT INTO scan_runs(source_id, status) VALUES(?, 'completed')", (sid,))
        scan_id = cur.lastrowid
        rows = [
            (sid, scan_id, '/s/a1', 'a1', 'a1', 'txt', 100, 'CORP\\alice'),
            (sid, scan_id, '/s/a2', 'a2', 'a2', 'txt', 200, 'CORP\\alice'),
            (sid, scan_id, '/s/b1', 'b1', 'b1', 'txt', 300, 'CORP\\bob'),
            (sid, scan_id, '/s/b2', 'b2', 'b2', 'txt', 400, 'CORP\\bob'),
            (sid, scan_id, '/s/b3', 'b3', 'b3', 'txt', 500, 'CORP\\bob'),
        ]
        cur.executemany(
            "INSERT INTO scanned_files(source_id, scan_id, file_path, relative_path, "
            "file_name, extension, file_size, owner) VALUES (?,?,?,?,?,?,?,?)", rows)
    return d, sid, scan_id


def test_top_creators_unscoped_returns_all_owners(seeded):
    d, sid, scan_id = seeded
    res = d.get_top_file_creators(sid, scan_id=scan_id)
    owners = {c["owner"] for c in res}
    assert owners == {'CORP\\alice', 'CORP\\bob'}
    # bob (3 files) ranks above alice (2 files)
    assert res[0]["owner"] == 'CORP\\bob'


def test_top_creators_scoped_to_viewer(seeded):
    d, sid, scan_id = seeded
    res = d.get_top_file_creators(
        sid, scan_id=scan_id,
        owner_scope=('AND owner LIKE ?', ['%alice%']),
    )
    assert len(res) == 1
    assert res[0]["owner"] == 'CORP\\alice'
    assert res[0]["file_count"] == 2
    # percentage is relative to the SCOPED total (2 files), so alice is 100% —
    # the global file total (5) is never leaked to the viewer.
    assert res[0]["percentage"] == 100.0


def test_top_creators_scope_survives_limit_truncation(seeded):
    """A viewer outside the global top-N still sees their own row.

    alice is the *smaller* creator; with limit=1 the unscoped query would
    return only bob. The scoped query must still surface alice — proving the
    scope is applied in SQL (pre-LIMIT), not by post-filtering the top-N.
    """
    d, sid, scan_id = seeded
    unscoped = d.get_top_file_creators(sid, scan_id=scan_id, limit=1)
    assert unscoped[0]["owner"] == 'CORP\\bob'  # alice absent from top-1

    scoped = d.get_top_file_creators(
        sid, scan_id=scan_id, limit=1,
        owner_scope=('AND owner LIKE ?', ['%alice%']),
    )
    assert len(scoped) == 1
    assert scoped[0]["owner"] == 'CORP\\alice'


def test_top_creators_scope_uses_bound_param_not_fstring(seeded):
    """A malicious 'owner' value can't break out — it's a bound LIKE param."""
    d, sid, scan_id = seeded
    res = d.get_top_file_creators(
        sid, scan_id=scan_id,
        owner_scope=('AND owner LIKE ?', ["%'; DROP TABLE scanned_files;--%"]),
    )
    assert res == []                    # no match, and no injection
    # table still intact
    assert len(d.get_top_file_creators(sid, scan_id=scan_id)) == 2
