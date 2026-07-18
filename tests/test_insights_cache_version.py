"""#370: /api/insights version-gates its scan_id-keyed cache.

burculogo's scan completed BEFORE #365 (distinct stale_1year/stale_3year
insight_type) shipped, so the cached insights lacked insight_type. Because the
cache is keyed on scan_id and never otherwise invalidated, the fix never reached
the box — the "3+ Yillik" insight kept opening the "1 Yildan Eski" list. A
schema_version gate recomputes a stale cache transparently.

fastapi-gated (TestClient); runs in the Docker CI image.
"""

from __future__ import annotations

import importlib.util

import pytest

from src.analyzer.ai_insights import INSIGHTS_SCHEMA_VERSION
from src.storage.database import Database

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
requires_fastapi = pytest.mark.skipif(
    not HAS_FASTAPI, reason="fastapi not installed in this environment")


@pytest.fixture
def db(tmp_path):
    d = Database({"path": str(tmp_path / "ic.db"),
                  "retention": {"auto_cleanup_on_startup": False}})
    d.connect()
    with d.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('s', '/x')")
        cur.execute("INSERT INTO scan_runs(source_id, status) VALUES(1, 'completed')")
    yield d
    d.close()


def _client(db):
    from fastapi.testclient import TestClient

    from src.dashboard.api import create_app
    return TestClient(create_app(db, {"dashboard": {"auth": {"enabled": False}}}))


@requires_fastapi
def test_stale_schema_cache_is_recomputed(db):
    # Pre-#365 cache: no schema_version, a sentinel insight that must NOT survive.
    db.save_scan_insights(1, {"insights": [{"category": "stale",
                                            "title": "OLD-SENTINEL"}], "score": 0})
    body = _client(db).get("/api/insights/1").json()
    assert body.get("schema_version") == INSIGHTS_SCHEMA_VERSION
    assert "OLD-SENTINEL" not in [i.get("title") for i in body.get("insights", [])]


@requires_fastapi
def test_current_schema_cache_is_served(db):
    db.save_scan_insights(1, {
        "insights": [{"category": "stale", "title": "FRESH",
                      "insight_type": "stale_3year"}],
        "score": 7, "schema_version": INSIGHTS_SCHEMA_VERSION})
    body = _client(db).get("/api/insights/1").json()
    assert body.get("from_cache") is True
    assert body["insights"][0]["title"] == "FRESH"
