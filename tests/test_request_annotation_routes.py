"""Regression: two POST routes forgot to annotate their Starlette request param.

`async def archive_by_insight(request):` / `async def bulk_restore(request):` —
without `: Request`, FastAPI treats `request` as a required QUERY parameter, so
the POST returns 422 ("field required" for query `request`) and the JSON body is
never parsed. In the UI this surfaced as the AI-insight "Uygula" button throwing
"Insight arsivleme hatasi: [object Object]" (the 422 detail array). Fix: annotate
`request: Request` (Request is imported module-level; dozens of sibling handlers
already use it).

These tests pin that the routes parse their JSON body (reach the in-body 400s)
instead of 422-ing at the query-param layer.

fastapi-gated (TestClient); runs in the Docker CI image.
"""

from __future__ import annotations

import importlib.util

import pytest

from src.storage.database import Database

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
requires_fastapi = pytest.mark.skipif(
    not HAS_FASTAPI, reason="fastapi not installed in this environment")


@pytest.fixture
def db(tmp_path):
    d = Database({"path": str(tmp_path / "ra.db"),
                  "retention": {"auto_cleanup_on_startup": False}})
    d.connect()
    with d.get_cursor() as cur:
        # source WITHOUT archive_dest → archive_by_insight reaches its in-body
        # "Arsiv hedefi tanimli degil" 400 once the body actually parses.
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e', '/x')")
        cur.execute("INSERT INTO scan_runs(source_id, status) VALUES(1, 'completed')")
    yield d
    d.close()


def _client(db):
    from fastapi.testclient import TestClient

    from src.dashboard.api import create_app
    return TestClient(create_app(db, {"dashboard": {"auth": {"enabled": False}}}))


@requires_fastapi
def test_archive_by_insight_parses_body_not_422(db):
    r = _client(db).post("/api/archive/by-insight",
                         json={"type": "stale_1year", "source_id": 1, "confirm": False})
    # Before the fix: 422 (FastAPI wanted a query param `request`).
    assert r.status_code != 422, r.text
    # After the fix: the body parses and we hit the archive_dest guard.
    assert r.status_code == 400
    assert "Arsiv hedefi" in r.text


@requires_fastapi
def test_bulk_restore_parses_body_not_422(db):
    r = _client(db).post("/api/restore/bulk", json={"confirm": False})
    assert r.status_code != 422, r.text
    # Empty archive_ids → the in-body 400 fires (proves the JSON body parsed).
    assert r.status_code == 400
    assert "archive_ids" in r.text


@requires_fastapi
def test_missing_type_is_a_clean_400_not_422(db):
    # A parsed-but-incomplete body must reach the handler's own validation.
    r = _client(db).post("/api/archive/by-insight", json={"source_id": 1})
    assert r.status_code == 400
    assert "type" in r.text
