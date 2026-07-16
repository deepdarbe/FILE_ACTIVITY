"""GET /api/reports/duplicates/{id} resolves the scan like the rest of the
dashboard — include_running=True (loop #9).

The duplicates *page* called get_duplicate_groups without a scan_id, so it fell
to that method's completed-only internal lookup, while the duplicates *export*
(and overview / mit-naming / insights) all use get_latest_scan_id(
include_running=True). Result: when the latest scan was still 'running' (or was
interrupted before being marked 'completed'), the page rendered empty even
though the running scan — the one the rest of the UI shows — had duplicate data.

This pins the endpoint on the running scan.
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


class _StubAnalytics:
    available = False

    def health(self):
        return {"available": False, "configured": False}

    def close(self):
        pass


_BASE_CONFIG = {
    "dashboard": {"auth": {"enabled": False}},
    "security": {"ransomware": {"enabled": False}, "orphan_sid": {"enabled": False}},
    "analytics": {},
    "backup": {"enabled": False, "dir": "/tmp/_no_backups", "keep_last_n": 1, "keep_weekly": 0},
    "integrations": {"syslog": {"enabled": False}},
}


@pytest.fixture
def client_with_running_dupes(tmp_path):
    db = Database({"path": str(tmp_path / "dupes.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('s','/s')")
        sid = cur.lastrowid
        # A RUNNING scan (never marked completed) — the case the bug missed.
        cur.execute("INSERT INTO scan_runs(source_id, status) VALUES(?, 'running')", (sid,))
        scan_id = cur.lastrowid
        rows = [
            # two identical name+size pairs => one duplicate group, waste = 1*1000
            (sid, scan_id, '/s/dup1', 'dup1', 'report.docx', 'docx', 1000, 'CORP\\a'),
            (sid, scan_id, '/s/dup2', 'dup2', 'report.docx', 'docx', 1000, 'CORP\\b'),
            (sid, scan_id, '/s/uniq', 'uniq', 'solo.txt', 'txt', 42, 'CORP\\a'),
        ]
        cur.executemany(
            "INSERT INTO scanned_files(source_id, scan_id, file_path, relative_path, "
            "file_name, extension, file_size, owner) VALUES (?,?,?,?,?,?,?,?)", rows)
    app = create_app(db, _BASE_CONFIG, analytics=_StubAnalytics())
    return sid, TestClient(app)


def test_duplicate_report_uses_running_scan(client_with_running_dupes):
    sid, client = client_with_running_dupes
    r = client.get(f"/api/reports/duplicates/{sid}")
    assert r.status_code == 200
    body = r.json()
    # The running scan's duplicate group must surface — not an empty page.
    assert body["total_groups"] == 1
    assert len(body["groups"]) == 1
    g = body["groups"][0]
    assert g["file_name"] == 'report.docx'
    assert g["count"] == 2
    assert g["waste_size"] == 1000
    assert body["total_waste_size"] == 1000


def test_duplicate_report_empty_when_no_scan(tmp_path):
    """No scan at all → clean empty envelope, not a 500."""
    db = Database({"path": str(tmp_path / "none.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('s','/s')")
        sid = cur.lastrowid
    app = create_app(db, _BASE_CONFIG, analytics=_StubAnalytics())
    client = TestClient(app)
    r = client.get(f"/api/reports/duplicates/{sid}")
    assert r.status_code == 200
    body = r.json()
    assert body["total_groups"] == 0
    assert body["groups"] == []
    assert body["total_waste_size_formatted"]  # formatted key present, no KeyError
