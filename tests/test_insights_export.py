"""#362 — the AI-insights drilldown (JSON url /insights/{sid}/files) had no
matching export endpoint, so its XLS/CSV buttons errored "Drilldown URL
bulunamadi". This pins the new GET /api/insights/{sid}/files/export.xlsx.

CSV path only (no openpyxl needed); fastapi-gated (runs in Docker CI).
"""

from __future__ import annotations

import importlib.util

import pytest

from src.storage.database import Database

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
requires_fastapi = pytest.mark.skipif(
    not HAS_FASTAPI, reason="fastapi not installed in this environment")


@pytest.fixture
def seeded(tmp_path):
    db = Database({"path": str(tmp_path / "ins.db"),
                   "retention": {"auto_cleanup_on_startup": False}})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e', 'x')")  # id 1
    sid = db.create_scan_run(1)
    with db.get_cursor() as cur:
        cur.execute("UPDATE scan_runs SET status='completed' WHERE id=?", (sid,))
        # A stale (>1yr un-accessed) file — matches insight_type=stale_1year.
        cur.execute(
            "INSERT INTO scanned_files(source_id, scan_id, file_path,"
            " relative_path, file_name, extension, file_size, last_access_time,"
            " last_modify_time, owner) "
            "VALUES(1, ?, ?, ?, ?, 'txt', 1024, datetime('now','-800 days'),"
            " datetime('now','-800 days'), 'BURCU\\grafik')",
            (sid, r"E:\old\rapor.txt", r"old\rapor.txt", "rapor.txt"))
    yield db
    db.close()


@requires_fastapi
def test_insight_csv_export_streams_matching_rows(seeded):
    from fastapi.testclient import TestClient

    from src.dashboard.api import create_app
    client = TestClient(create_app(seeded, {"dashboard": {"auth": {"enabled": False}}}))

    r = client.get("/api/insights/1/files/export.xlsx"
                   "?insight_type=stale_1year&format=csv")
    assert r.status_code == 200, r.text
    assert "text/csv" in r.headers["content-type"]
    body = r.text
    assert "rapor.txt" in body
    assert r"E:\old\rapor.txt" in body


@requires_fastapi
def test_insight_export_unknown_type_is_400(seeded):
    from fastapi.testclient import TestClient

    from src.dashboard.api import create_app
    client = TestClient(create_app(seeded, {"dashboard": {"auth": {"enabled": False}}}))
    r = client.get("/api/insights/1/files/export.xlsx?insight_type=bogus&format=csv")
    assert r.status_code == 400
