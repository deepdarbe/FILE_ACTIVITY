"""Tests for the incremental partial summary (issue #139).

The partial summary aggregates ``scanned_files`` rows belonging to a
running scan so the dashboard can render rolling KPIs instead of
all-zeros while a 5M-row MFT walk is in progress.

Coverage
--------

* :func:`test_compute_returns_correct_aggregates` — known fixture of 1000
  rows, assert totals + per-extension top-10 ordering.
* :func:`test_compute_uses_read_cursor` — concurrent writer thread keeps
  inserting while we compute; the partial summary still completes.
* :func:`test_endpoint_returns_partial_when_running` — running scan +
  saved partial_summary_json => /api/overview reports
  ``is_partial=true``.
* :func:`test_endpoint_falls_back_to_summary_json_when_complete` —
  completed scan => /api/overview reports ``is_partial=false``.
* :func:`test_endpoint_returns_no_data_when_no_partial_yet` — running
  scan + no partial yet => preserves the original
  ``scan_in_progress`` placeholder.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from typing import List, Tuple

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.analyzer.partial_summary import compute_partial_summary  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _seed_source_and_scan(database, status: str = "running") -> Tuple[int, int]:
    with database.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
            ("partial_src", "/tmp/partial"),
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, ?)",
            (source_id, status),
        )
        scan_id = cur.lastrowid
    return source_id, scan_id


def _bulk_insert_fixture(database, source_id: int, scan_id: int,
                         rows: List[dict]) -> None:
    with database.get_cursor() as cur:
        cur.executemany(
            """INSERT INTO scanned_files
               (source_id, scan_id, file_path, relative_path, file_name,
                extension, file_size, creation_time, last_access_time,
                last_modify_time, owner, attributes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(source_id, scan_id, r["file_path"], r["relative_path"],
              r["file_name"], r.get("extension"), r["file_size"],
              r.get("creation_time"), r.get("last_access_time"),
              r.get("last_modify_time"), r.get("owner"),
              r.get("attributes", 0)) for r in rows],
        )


def _make_row(i: int, ext: str, size: int, atime: str | None = None,
              owner: str = "alice") -> dict:
    return {
        "file_path": f"C:/data/{i}.{ext or 'bin'}",
        "relative_path": f"{i}.{ext or 'bin'}",
        "file_name": f"{i}.{ext or 'bin'}",
        "extension": ext,
        "file_size": size,
        "creation_time": "2024-01-01",
        "last_access_time": atime,
        "last_modify_time": atime or "2024-01-01",
        "owner": owner,
    }


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "partial.db"
    cfg = {"path": str(db_path)}
    database = Database(cfg)
    database.connect()
    yield database
    database.close()


# ---------------------------------------------------------------------------
# Direct compute tests
# ---------------------------------------------------------------------------


def test_compute_returns_correct_aggregates(db):
    """1000-row fixture: assert totals, top-extensions, size buckets."""
    source_id, scan_id = _seed_source_and_scan(db)

    rows: List[dict] = []
    # 600 small pdfs (5 KiB each) ~ 3 MiB, owners alice/bob round-robin.
    for i in range(600):
        rows.append(_make_row(i, "pdf", 5_000,
                              owner="alice" if i % 2 == 0 else "bob"))
    # 300 medium docx (200 KiB each)
    for i in range(600, 900):
        rows.append(_make_row(i, "docx", 200_000, owner="carol"))
    # 100 tiny csv (500 bytes each)
    for i in range(900, 1000):
        rows.append(_make_row(i, "csv", 500, owner="dave"))
    _bulk_insert_fixture(db, source_id, scan_id, rows)

    t0 = time.perf_counter()
    summary = compute_partial_summary(db, scan_id)
    elapsed = time.perf_counter() - t0

    assert summary["is_partial"] is True
    assert summary["total_files"] == 1000
    assert summary["total_size"] == (
        600 * 5_000 + 300 * 200_000 + 100 * 500
    )
    # 4 distinct owners
    assert summary["unique_owners"] == 4
    # top extensions: pdf (600) > docx (300) > csv (100)
    exts = [(e["ext"], e["count"]) for e in summary["top_extensions"]]
    assert exts[0] == ("pdf", 600)
    assert exts[1] == ("docx", 300)
    assert exts[2] == ("csv", 100)
    # Size bucket scaffold present
    assert "tiny" in summary["size_buckets"]
    assert "small" in summary["size_buckets"]
    # Age bucket scaffold present
    assert {"30d", "90d", "180d", "365d"}.issubset(summary["age_buckets"].keys())
    # Compute envelope marks elapsed time
    assert summary["compute_elapsed_ms"] >= 0
    # 1000 rows must compute in well under 5 sec; warn if anywhere close.
    assert elapsed < 2.0, f"compute took {elapsed:.2f}s on 1000 rows"


def test_compute_uses_read_cursor(db):
    """Concurrent writer thread inserts while we compute partial; partial
    must still return a valid (non-error) result. This smoke-tests the
    contract that ``get_read_cursor`` does not contend with the writer.
    """
    source_id, scan_id = _seed_source_and_scan(db)
    seed = [_make_row(i, "pdf", 4096) for i in range(200)]
    _bulk_insert_fixture(db, source_id, scan_id, seed)

    stop_event = threading.Event()

    def writer():
        i = 200
        while not stop_event.is_set():
            batch = [_make_row(i + k, "log", 1024) for k in range(50)]
            try:
                _bulk_insert_fixture(db, source_id, scan_id, batch)
            except Exception:
                # SQLite busy is acceptable - we're not testing the
                # writer's throughput, only that the reader doesn't
                # deadlock against it.
                pass
            i += 50
            time.sleep(0.005)

    th = threading.Thread(target=writer, daemon=True)
    th.start()
    try:
        # Run the compute several times while the writer hammers.
        last = None
        for _ in range(5):
            last = compute_partial_summary(db, scan_id)
            assert last["is_partial"] is True
            assert last["total_files"] >= 200
        assert "error" not in last, (
            f"compute_partial_summary returned error: {last.get('error')}"
        )
    finally:
        stop_event.set()
        th.join(timeout=2)


# ---------------------------------------------------------------------------
# Endpoint integration tests
# ---------------------------------------------------------------------------


def _make_app(database):
    """Spin up the FastAPI app + a TestClient. Imported lazily so the
    rest of the suite still runs in environments without ``httpx``.
    """
    from fastapi.testclient import TestClient  # noqa: WPS433
    from src.dashboard.api import create_app
    cfg = {"dashboard": {"host": "127.0.0.1", "port": 0}}
    app = create_app(database, cfg)
    return TestClient(app)


def test_endpoint_returns_partial_when_running(db):
    source_id, scan_id = _seed_source_and_scan(db, status="running")
    rows = [_make_row(i, "pdf", 1024) for i in range(50)]
    _bulk_insert_fixture(db, source_id, scan_id, rows)

    # Compute + persist a partial snapshot.
    payload = compute_partial_summary(db, scan_id)
    db.save_scan_partial_summary(scan_id, payload)

    client = _make_app(db)
    resp = client.get(f"/api/overview/{source_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data.get("is_partial") is True
    assert data.get("has_data") is True
    assert data.get("scan_in_progress") is True
    assert data.get("total_files") == 50
    assert "partial_updated_at" in data


def test_endpoint_falls_back_to_summary_json_when_complete(db):
    source_id, scan_id = _seed_source_and_scan(db, status="completed")
    rows = [_make_row(i, "pdf", 1024) for i in range(40)]
    _bulk_insert_fixture(db, source_id, scan_id, rows)
    # Mark scan completed_at so get_latest_scan_id picks it up.
    with db.get_cursor() as cur:
        cur.execute(
            "UPDATE scan_runs SET completed_at=datetime('now','localtime') "
            "WHERE id=?", (scan_id,),
        )
    # Compute the *full* summary so the completed-scan path has a payload.
    db.compute_scan_summary(scan_id)

    client = _make_app(db)
    resp = client.get(f"/api/overview/{source_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data.get("has_data") is True
    assert data.get("is_partial") is False
    assert data.get("total_files") == 40


def test_endpoint_returns_no_data_when_no_partial_yet(db):
    """Running scan, no scanned_files rows, no partial snapshot. The
    endpoint must preserve the existing ``scan_in_progress`` placeholder
    rather than synthesising an empty partial.
    """
    source_id, _scan_id = _seed_source_and_scan(db, status="running")

    client = _make_app(db)
    resp = client.get(f"/api/overview/{source_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data.get("has_data") is False
    assert data.get("scan_in_progress") is True
    # Original placeholder reason key preserved.
    assert "reason" in data
