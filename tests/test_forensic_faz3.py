"""Tests for #340 Faz 3 — unified deletion-forensics feed.

``Database.get_file_deletion_events`` merges the two deletion data paths that
Faz 1/2 populate — USN (``file_audit_events``, event_type='delete') and the
Security-log collector (``user_access_logs``, access_type='delete') — into one
paginated, newest-first feed so a deletion caught by only one collector still
surfaces on the "Dosya Silme Olaylari" page.

The DB-level tests use only sqlite (stdlib) and run everywhere. The endpoint
test is fastapi-gated (TestClient), same convention as the other suites.
"""

from __future__ import annotations

import importlib.util

import pytest

from src.storage.database import Database

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
requires_fastapi = pytest.mark.skipif(
    not HAS_FASTAPI, reason="fastapi not installed in this environment"
)


@pytest.fixture
def db(tmp_path):
    d = Database({
        "path": str(tmp_path / "f3.db"),
        "retention": {"auto_cleanup_on_startup": False},
    })
    d.connect()
    with d.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e', 'x')")  # id=1
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('f', 'y')")  # id=2
    yield d
    d.close()


def _fae(db, *, user, path, etype="delete", src=1, age_days=0,
         detected_by="watcher"):
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO file_audit_events"
            "(source_id, event_time, event_type, username, file_path,"
            " file_name, detected_by) "
            "VALUES(?, datetime('now','localtime', ?), ?, ?, ?, ?, ?)",
            (src, f"-{age_days} days", etype, user, path,
             path.rsplit("\\", 1)[-1], detected_by))


def _ual(db, *, user, path, atype="delete", eid=4660, ip=None, src=1,
         age_days=0, size=0):
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO user_access_logs"
            "(source_id, username, domain, file_path, file_name, access_type,"
            " access_time, client_ip, file_size, event_id) "
            "VALUES(?, ?, 'ITWISE', ?, ?, ?, datetime('now','localtime', ?),"
            " ?, ?, ?)",
            (src, user, path, path.rsplit("\\", 1)[-1], atype,
             f"-{age_days} days", ip, size, eid))


def test_union_merges_both_paths_and_filters_noise(db):
    # USN deletes
    _fae(db, user="alice", path=r"E:\ortak\a.xlsx")
    _fae(db, user="bob", path=r"E:\ortak\b.docx")
    _fae(db, user="alice", path=r"E:\ortak\keep.txt", etype="modify")  # not a delete
    # EventLog: the authoritative 4660 delete carries the IP…
    _ual(db, user="carol", path=r"\\fs\share\c.pdf", eid=4660, ip="10.0.0.5")
    # …but the 4656 handle-open and 4663 read must NOT count (multiplicity).
    _ual(db, user="carol", path=r"\\fs\share\c.pdf", eid=4656)
    _ual(db, user="dave", path=r"\\fs\share\d.txt", atype="read", eid=4663)

    res = db.get_file_deletion_events(days=30, mass_delete_threshold=99)

    assert res["total"] == 3, res  # 2 USN + 1 EventLog(4660)
    uids = [e["uid"] for e in res["events"]]
    assert len(uids) == len(set(uids)) == 3  # collision-free across tables
    assert all(u.startswith(("fae:", "ual:")) for u in uids)

    el = [e for e in res["events"] if e["detected_by"] == "EventLog"]
    assert len(el) == 1
    assert el[0]["client_ip"] == "10.0.0.5"
    assert el[0]["event_id"] == 4660

    s = res["summary"]
    assert s["by_source"] == {"USN": 2, "EventLog": 1}
    assert s["with_client_ip"] == 1
    assert s["distinct_users"] == 3


def test_4656_without_4660_yields_nothing(db):
    # A lone 4656 (handle requested with DELETE) is not a completed deletion.
    _ual(db, user="mallory", path=r"\\fs\share\x", eid=4656)
    res = db.get_file_deletion_events(days=30)
    assert res["total"] == 0


def test_events_outside_window_excluded(db):
    _fae(db, user="alice", path=r"E:\old.txt", age_days=100)
    _fae(db, user="alice", path=r"E:\new.txt", age_days=1)
    res = db.get_file_deletion_events(days=30)
    assert res["total"] == 1
    assert res["events"][0]["file_path"] == r"E:\new.txt"


def test_mass_delete_severity_and_filter(db):
    for i in range(3):
        _fae(db, user="attacker", path=rf"E:\ortak\f{i}.dat")
    _fae(db, user="normal", path=r"E:\ortak\one.dat")

    res = db.get_file_deletion_events(days=30, mass_delete_threshold=3)
    sev = {e["file_path"]: e["severity"] for e in res["events"]}
    assert sev[r"E:\ortak\one.dat"] == "normal"
    assert all(v == "high" for k, v in sev.items() if "attacker" not in k
               and k != r"E:\ortak\one.dat")
    assert res["summary"]["mass_delete_users"] == ["attacker"]

    only_high = db.get_file_deletion_events(
        days=30, severity="high", mass_delete_threshold=3)
    assert only_high["total"] == 3
    assert {e["username"] for e in only_high["events"]} == {"attacker"}

    # No mass-delete user at a higher threshold → high filter is empty.
    none_high = db.get_file_deletion_events(
        days=30, severity="high", mass_delete_threshold=99)
    assert none_high["total"] == 0
    assert none_high["events"] == []


def test_null_username_mass_deleter_does_not_crash(db):
    # USN delete rows carry a NULL username (file_watcher _record_audit
    # ('delete', dpath) → owner=None). Without COALESCE, a NULL-username burst
    # alongside a named burst makes mass_set={None,'alice'} and the summary's
    # sorted(mass_set) raises TypeError → HTTP 500 on the whole page.
    for i in range(4):
        _fae(db, user=None, path=rf"E:\ortak\n{i}.dat")      # NULL-username burst
    for i in range(3):
        _fae(db, user="alice", path=rf"E:\ortak\a{i}.dat")   # named burst too

    res = db.get_file_deletion_events(days=30, mass_delete_threshold=3)
    assert set(res["summary"]["mass_delete_users"]) == {"(bilinmiyor)", "alice"}

    # severity='high' must still surface the NULL-username burst (previously
    # dropped by `username IN (NULL)` three-valued logic).
    high = db.get_file_deletion_events(days=30, severity="high",
                                       mass_delete_threshold=3)
    assert {e["username"] for e in high["events"]} == {"(bilinmiyor)", "alice"}
    assert high["total"] == 7


def test_cross_source_deletion_not_double_counted_for_massdelete(db):
    # Same user deletes 12 files, each caught by BOTH collectors (USN +
    # EventLog 4660) on one SMB-shared volume → 24 union rows but only 12 real
    # deletions. Mass-delete must use MAX(per-source)=12, not the union sum 24,
    # so a threshold-20 alarm does NOT fire at 12 real deletions (#340 review).
    for i in range(12):
        p = rf"E:\ortak\dup{i}.dat"
        _fae(db, user="carol", path=p)
        _ual(db, user="carol", path=p, eid=4660, ip="10.0.0.9")

    res = db.get_file_deletion_events(days=30, mass_delete_threshold=20)
    assert res["summary"]["mass_delete_users"] == []          # not a false positive
    assert all(e["severity"] == "normal" for e in res["events"])
    # Every detection is still listed — nothing hidden from the forensic feed.
    assert res["total"] == 24
    assert res["summary"]["by_source"] == {"USN": 12, "EventLog": 12}


def test_source_and_username_filters(db):
    _fae(db, user="alice", path=r"E:\s1.txt", src=1)
    _fae(db, user="bob", path=r"E:\s2.txt", src=2)
    _ual(db, user="alice", path=r"\\fs\s1b", src=1, eid=4660)

    by_src2 = db.get_file_deletion_events(days=30, source_id=2)
    assert by_src2["total"] == 1
    assert by_src2["events"][0]["username"] == "bob"

    by_user = db.get_file_deletion_events(days=30, username="alice")
    assert by_user["total"] == 2  # one USN + one EventLog, both alice
    assert {e["username"] for e in by_user["events"]} == {"alice"}


@requires_fastapi
def test_endpoint_shape(db):
    from fastapi.testclient import TestClient
    from src.dashboard.api import create_app

    _fae(db, user="alice", path=r"E:\ortak\a.xlsx")
    _ual(db, user="carol", path=r"\\fs\share\c.pdf", eid=4660, ip="10.0.0.5")

    app = create_app(db, {"dashboard": {"auth": {"enabled": False}},
                          "user_activity": {"anomaly": {"high_delete_count": 3}}})
    client = TestClient(app)
    r = client.get("/api/forensic/file-deletions?days=30")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 2
    assert body["summary"]["by_source"] == {"USN": 1, "EventLog": 1}
    assert {e["uid"].split(":")[0] for e in body["events"]} == {"fae", "ual"}
