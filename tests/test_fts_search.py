"""Tests for the embedded FTS5 (trigram) file search.

Covers the database layer (``rebuild_fts`` / ``search_files``) and the
``GET /api/files/search`` endpoint.

Acceptance criteria from the agent instructions:

* A substring query finds the right rows (path / name / owner).
* A non-matching query returns ``[]``.
* ``scan_id`` scoping works.
* A too-short (< 3 char trigram) query returns empty cleanly.
* The HTTP endpoint follows PaginationParams (page / page_size / items).
"""

from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from fastapi.testclient import TestClient  # noqa: E402

from src.dashboard.api import create_app  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ---------------------------------------------------------------------------
# Stubs for create_app
# ---------------------------------------------------------------------------


class _StubAnalytics:
    available = False


def _make_config() -> dict:
    return {
        "dashboard": {"host": "127.0.0.1", "port": 8085, "auth": {"enabled": False}},
        "archiving": {"enabled": False, "dry_run": True},
        "audit": {"chain_enabled": False},
        "database": {},
    }


# A small, deterministic corpus exercising path / name / owner matches and
# two distinct scan_ids for the scoping test.
#  (source_id, scan_id, file_path, relative_path, file_name, extension,
#   file_size, owner)
_ROWS = [
    (1, 1, r"E:\Finans\rapor_2024.xlsx", r"Finans\rapor_2024.xlsx",
     "rapor_2024.xlsx", "xlsx", 4096, r"CORP\ahmet"),
    (1, 1, r"E:\HR\maaslar.docx", r"HR\maaslar.docx",
     "maaslar.docx", "docx", 2048, r"CORP\zeynep"),
    (1, 1, r"E:\Finans\butce_taslak.pdf", r"Finans\butce_taslak.pdf",
     "butce_taslak.pdf", "pdf", 8192, r"CORP\ahmet"),
    (1, 2, r"E:\Arsiv\eski_rapor.xlsx", r"Arsiv\eski_rapor.xlsx",
     "eski_rapor.xlsx", "xlsx", 1024, r"CORP\mehmet"),
]


def _seed(database, source_id):
    with database.get_cursor() as cur:
        # scanned_files.scan_id FK-references scan_runs(id); seed the two
        # scan runs (ids 1 and 2) first so the inserts satisfy the FK.
        # Distinct started_at so get_latest_scan_id() resolves scan 2 as
        # "latest" deterministically (same-timestamp inserts would tie).
        for ts in ("2026-01-01 10:00:00", "2026-01-02 10:00:00"):
            cur.execute(
                "INSERT INTO scan_runs(source_id, status, started_at) "
                "VALUES(?, 'completed', ?)",
                (source_id, ts),
            )
        for r in _ROWS:
            cur.execute(
                "INSERT INTO scanned_files("
                "source_id, scan_id, file_path, relative_path, file_name, "
                "extension, file_size, owner) VALUES (?,?,?,?,?,?,?,?)",
                (source_id,) + r[1:],
            )


@pytest.fixture
def db(tmp_path):
    """On-disk SQLite with one source + seeded scanned_files + built FTS."""
    db_path = tmp_path / "fts_test.db"
    database = Database({"path": str(db_path)})
    database.connect()
    with database.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
            ("share1", r"\\fs\share1"),
        )
        source_id = cur.lastrowid
    _seed(database, source_id)
    database.rebuild_fts()
    yield database, source_id
    database.close()


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------


def test_substring_matches_filename(db):
    """A name fragment finds the matching files (trigram substring)."""
    database, _ = db
    res = database.search_files("rapor")
    names = {f["file_name"] for f in res["files"]}
    assert "rapor_2024.xlsx" in names
    assert "eski_rapor.xlsx" in names
    assert res["total"] == 2
    # Non-matching file is absent.
    assert "maaslar.docx" not in names


def test_substring_matches_partial_token(db):
    """Trigram matches a 4-char fragment inside a longer token."""
    database, _ = db
    res = database.search_files("maas")
    assert [f["file_name"] for f in res["files"]] == ["maaslar.docx"]


def test_substring_matches_owner(db):
    """Owner substring search returns every file owned by that principal."""
    database, _ = db
    res = database.search_files("ahmet")
    assert res["total"] == 2
    assert all("ahmet" in f["owner"] for f in res["files"])


def test_substring_matches_path_component(db):
    """A path-fragment search hits files anywhere under that folder."""
    database, _ = db
    res = database.search_files("Finans")
    names = {f["file_name"] for f in res["files"]}
    assert names == {"rapor_2024.xlsx", "butce_taslak.pdf"}


def test_non_match_returns_empty(db):
    """A query matching nothing returns total=0 and an empty list."""
    database, _ = db
    res = database.search_files("zzzznotpresent")
    assert res["total"] == 0
    assert res["files"] == []


def test_glob_wildcard_is_stripped(db):
    """Glob-style input ('*.xlsx') normalizes to a substring search so it
    matches real filenames (the '*' is a literal FTS5 char otherwise)."""
    database, _ = db
    star = database.search_files("*.xlsx")
    plain = database.search_files(".xlsx")
    assert star["total"] == plain["total"]
    assert star["total"] >= 1  # seeded .xlsx files


def test_fts_has_data_true_after_rebuild(db):
    """fts_has_data() reports True once the index has been built (fixture)."""
    database, _ = db
    assert database.fts_has_data() is True


def test_scan_id_scoping(db):
    """scan_id filter restricts results to that scan only."""
    database, _ = db
    # "rapor" matches in both scans; scoping to scan 2 keeps only the
    # archived one.
    res_all = database.search_files("rapor")
    assert res_all["total"] == 2
    res_scan2 = database.search_files("rapor", scan_id=2)
    assert res_scan2["total"] == 1
    assert res_scan2["files"][0]["file_name"] == "eski_rapor.xlsx"


def test_short_query_returns_empty(db):
    """A query shorter than the trigram minimum returns empty, not error."""
    database, _ = db
    assert database.search_files("ra")["files"] == []
    assert database.search_files("")["total"] == 0


def test_rebuild_is_idempotent(db):
    """rebuild_fts can run repeatedly without changing results."""
    database, _ = db
    database.rebuild_fts()
    database.rebuild_fts(scan_id=1)
    res = database.search_files("rapor")
    assert res["total"] == 2


def test_pagination_offset(db):
    """limit/offset paginate the ordered (file_size DESC) result set."""
    database, _ = db
    page1 = database.search_files("xlsx", limit=1, offset=0)
    page2 = database.search_files("xlsx", limit=1, offset=1)
    assert len(page1["files"]) == 1
    assert len(page2["files"]) == 1
    # Ordered by file_size DESC: rapor_2024 (4096) before eski_rapor (1024).
    assert page1["files"][0]["file_name"] == "rapor_2024.xlsx"
    assert page2["files"][0]["file_name"] == "eski_rapor.xlsx"


# ---------------------------------------------------------------------------
# HTTP endpoint
# ---------------------------------------------------------------------------


@pytest.fixture
def client(db):
    database, source_id = db
    app = create_app(
        db=database,
        config=_make_config(),
        analytics=_StubAnalytics(),
        ad_lookup=None,
        email_notifier=None,
    )
    return TestClient(app), source_id


def test_endpoint_returns_pagination_envelope(client):
    tc, _ = client
    resp = tc.get("/api/files/search", params={"q": "rapor"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # PaginationParams envelope shape.
    assert body["page"] == 1
    assert body["page_size"] == 100
    assert body["total"] == 2
    assert len(body["items"]) == 2
    names = {f["file_name"] for f in body["items"]}
    assert names == {"rapor_2024.xlsx", "eski_rapor.xlsx"}


def test_endpoint_scoped_by_source(client):
    tc, source_id = client
    resp = tc.get(
        "/api/files/search",
        params={"q": "rapor", "source_id": source_id},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # source_id resolves to the latest scan (scan 2) → only eski_rapor.
    assert body["total"] == 1
    assert body["items"][0]["file_name"] == "eski_rapor.xlsx"


def test_endpoint_non_match_empty(client):
    tc, _ = client
    resp = tc.get("/api/files/search", params={"q": "zzzznotpresent"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 0
    assert body["items"] == []


def test_endpoint_unknown_source_404(client):
    tc, _ = client
    resp = tc.get(
        "/api/files/search",
        params={"q": "rapor", "source_id": 999999},
    )
    assert resp.status_code == 404
