"""Tests for issue #82, Bug 2: ai_insights.get_insight_files() should
loudly reject unknown insight_type values instead of silently returning
an empty list (which makes the "Incele" button look broken).

Covers:
* Every key in `VALID_INSIGHT_TYPES` returns data when matching rows
  exist.
* An unknown `insight_type` raises `ValueError` with a helpful message
  listing the valid types.
* The `/api/insights/{source_id}/files` endpoint translates that
  `ValueError` into HTTP 400 with the same detail string.

These tests deliberately avoid spinning up the full `create_app(...)`
factory — instead, we mount a minimal FastAPI app that mirrors just the
endpoint under test. Same approach `test_dashboard_api.py` uses for the
Bug 1 endpoint.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytest
from fastapi import FastAPI, HTTPException, Query
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.analyzer.ai_insights import (  # noqa: E402
    VALID_INSIGHT_TYPES,
    get_insight_files,
)
from src.storage.database import Database  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def seeded_db(tmp_path):
    """Database with a single source + scan and a handful of files
    chosen so that every `VALID_INSIGHT_TYPES` query matches at least
    one row (so we can assert non-empty results across the board).
    """
    db_path = tmp_path / "insights.db"
    db = Database({"path": str(db_path)})
    db.connect()

    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path) VALUES ('s', '/x')"
        )
        cur.execute("INSERT INTO scan_runs (source_id) VALUES (1)")

    now = datetime.now()
    very_old = (now - timedelta(days=1500)).strftime("%Y-%m-%d %H:%M:%S")
    old_1y = (now - timedelta(days=400)).strftime("%Y-%m-%d %H:%M:%S")
    recent = now.strftime("%Y-%m-%d %H:%M:%S")

    rows = [
        # very_old: matches stale_1year, stale_3year, stale_180, all_files
        ("/a/old.bin", "old.bin", "bin", 500, very_old),
        # >100MB: large_files, all_files
        ("/a/big.iso", "big.iso", "iso", 200 * 1024 * 1024, recent),
        # >1GB: large_files, very_large, all_files
        ("/a/huge.iso", "huge.iso", "iso", 2 * 1024 * 1024 * 1024, recent),
        # tmp ext: temp_files, all_files
        ("/a/scratch.tmp", "scratch.tmp", "tmp", 1024, recent),
        # empty: empty_files, all_files
        ("/a/empty.txt", "empty.txt", "txt", 0, recent),
        # 180-day stale (not 1y): stale_180, all_files
        ("/a/midstale.doc", "midstale.doc", "doc", 1234, old_1y),
        # Duplicate pair (same name+size, >1MB) for "duplicates" query
        ("/a/dup.dat", "dup.dat", "dat", 5 * 1024 * 1024, recent),
        ("/b/dup.dat", "dup.dat", "dat", 5 * 1024 * 1024, recent),
    ]
    with db.get_cursor() as cur:
        for path, name, ext, size, atime in rows:
            cur.execute(
                """INSERT INTO scanned_files
                   (source_id, scan_id, file_path, relative_path, file_name,
                    extension, file_size, last_access_time, last_modify_time,
                    owner)
                   VALUES (1, 1, ?, ?, ?, ?, ?, ?, ?, 'alice')""",
                (path, name, name, ext, size, atime, atime),
            )
    return db


# ──────────────────────────────────────────────────────────────────────
# Module-level invariants
# ──────────────────────────────────────────────────────────────────────


def test_valid_insight_types_is_non_empty_frozenset():
    """The public set of valid types must be discoverable and immutable."""
    assert isinstance(VALID_INSIGHT_TYPES, frozenset)
    assert len(VALID_INSIGHT_TYPES) > 0
    # The historic core types we care about must remain supported so we
    # don't accidentally break the existing dashboard buttons.
    for must_have in (
        "stale_1year", "stale_3year", "stale_180", "large_files",
        "very_large", "temp_files", "duplicates", "empty_files",
        "all_files",
    ):
        assert must_have in VALID_INSIGHT_TYPES


# ──────────────────────────────────────────────────────────────────────
# get_insight_files: happy path
# ──────────────────────────────────────────────────────────────────────


def test_get_insight_files_known_types_return_data(seeded_db):
    """Every documented insight_type returns a list (not raise) and the
    seed data is chosen so every type matches at least one row."""
    for itype in sorted(VALID_INSIGHT_TYPES):
        files = get_insight_files(seeded_db, scan_id=1, insight_type=itype)
        assert isinstance(files, list), f"{itype} did not return a list"
        assert files, (
            f"{itype} returned an empty list — seed data should match "
            f"every supported insight_type"
        )
        # Sanity: rows are dicts with expected columns.
        assert "file_path" in files[0]
        assert "file_size" in files[0]


# ──────────────────────────────────────────────────────────────────────
# get_insight_files: unknown type
# ──────────────────────────────────────────────────────────────────────


def test_get_insight_files_unknown_type_raises(seeded_db):
    """Unknown insight_type must raise ValueError, NOT silently return []
    (issue #82, Bug 2). The message should mention the bad value and the
    valid set so callers / users can self-correct."""
    with pytest.raises(ValueError) as excinfo:
        get_insight_files(seeded_db, scan_id=1, insight_type="bogus_type")

    msg = str(excinfo.value)
    assert "bogus_type" in msg
    assert "Valid" in msg
    # Spot-check that the message advertises at least one real type.
    assert "all_files" in msg


def test_get_insight_files_empty_string_raises(seeded_db):
    """Empty string is also not a valid type."""
    with pytest.raises(ValueError):
        get_insight_files(seeded_db, scan_id=1, insight_type="")


# ──────────────────────────────────────────────────────────────────────
# Endpoint: /api/insights/{source_id}/files
# ──────────────────────────────────────────────────────────────────────


def _build_endpoint_app(db: Database) -> FastAPI:
    """Mirror the real endpoint body so we can exercise the
    ValueError-to-HTTP-400 translation through TestClient. Mirrors the
    pattern already used in test_dashboard_api.py for Bug 1."""
    from src.utils.size_formatter import format_size

    app = FastAPI()

    @app.get("/api/insights/{source_id}/files")
    async def insight_files(
        source_id: int,
        insight_type: str = "stale_1year",
        page: int = Query(1, ge=1, le=10000),
        page_size: int = Query(100, ge=1, le=500),
    ):
        scan_id = db.get_latest_scan_id(source_id, include_running=True)
        if not scan_id:
            raise HTTPException(404, "Tarama bulunamadi")
        try:
            files = get_insight_files(db, scan_id, insight_type)
        except ValueError as e:
            raise HTTPException(400, str(e))
        total = len(files)
        offset = (page - 1) * page_size
        page_files = files[offset:offset + page_size]
        for f in page_files:
            f["file_size_formatted"] = format_size(f.get("file_size", 0))
            sep = "\\" if "\\" in f["file_path"] else "/"
            f["directory"] = f["file_path"].rsplit(sep, 1)[0]
        return {
            "insight_type": insight_type,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
            "files": page_files,
        }

    return app


def test_endpoint_unknown_insight_type_returns_400(seeded_db):
    """Hitting the endpoint with `?insight_type=bogus` should yield a
    400 (not 200 + empty list, not 500). The detail string should carry
    the same explanation get_insight_files raises so the frontend can
    show it directly in the modal."""
    client = TestClient(_build_endpoint_app(seeded_db))
    r = client.get("/api/insights/1/files", params={"insight_type": "bogus"})
    assert r.status_code == 400, r.text
    body = r.json()
    assert "bogus" in body["detail"]
    assert "Valid" in body["detail"]


def test_endpoint_known_insight_type_returns_200(seeded_db):
    """Sanity check: a known type still works through the endpoint."""
    client = TestClient(_build_endpoint_app(seeded_db))
    r = client.get(
        "/api/insights/1/files", params={"insight_type": "all_files"}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["insight_type"] == "all_files"
    assert body["total"] >= 1
    assert len(body["files"]) == body["total"] or body["total"] > body["page_size"]
