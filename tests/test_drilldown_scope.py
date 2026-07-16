"""#308 consistency: get_files_by_size_range honors owner_scope (loop #5).

The size drilldown (GET /api/drilldown/size + its XLSX export) was missed by the
original #308 per-user-scoping sweep — its siblings frequency/type/owner were
scoped but size was not, so a viewer role could enumerate every owner's files by
size. These tests pin the DB-layer scoping the endpoints now rely on.
"""
import pytest

from src.storage.database import Database


@pytest.fixture()
def seeded(tmp_path):
    d = Database({"path": str(tmp_path / "scope.db")})
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
        ]
        cur.executemany(
            "INSERT INTO scanned_files(source_id, scan_id, file_path, relative_path, "
            "file_name, extension, file_size, owner) VALUES (?,?,?,?,?,?,?,?)", rows)
    return d, sid, scan_id


def test_size_range_unscoped_returns_all(seeded):
    d, sid, scan_id = seeded
    res = d.get_files_by_size_range(sid, scan_id, 0, None, 100, 0)
    assert res["total"] == 3


def test_size_range_scoped_to_owner(seeded):
    d, sid, scan_id = seeded
    res = d.get_files_by_size_range(
        sid, scan_id, 0, None, 100, 0,
        owner_scope=('AND owner LIKE ?', ['%alice%']),
    )
    assert res["total"] == 2
    assert all('alice' in (f["owner"] or '') for f in res["files"])


def test_size_range_scope_uses_bound_param_not_fstring(seeded):
    """A malicious 'owner' value can't break out — it's a bound LIKE param."""
    d, sid, scan_id = seeded
    res = d.get_files_by_size_range(
        sid, scan_id, 0, None, 100, 0,
        owner_scope=('AND owner LIKE ?', ["%'; DROP TABLE scanned_files;--%"]),
    )
    assert res["total"] == 0            # no match, and no injection
    # table still intact
    assert d.get_files_by_size_range(sid, scan_id, 0, None, 100, 0)["total"] == 3
