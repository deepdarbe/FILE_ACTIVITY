"""#340 Faz 5 — /api/audit/verify surfaces ``chain_enabled`` so the forensic
page's delil-zinciri card can show the config gate (Rule 8) when the
tamper-evident chain is off vs simply empty.

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
    d = Database({"path": str(tmp_path / "f5.db"),
                  "retention": {"auto_cleanup_on_startup": False}})
    d.connect()
    yield d
    d.close()


def _client(db, config):
    from fastapi.testclient import TestClient

    from src.dashboard.api import create_app
    return TestClient(create_app(db, config))


@requires_fastapi
def test_verify_reports_chain_disabled_by_default(db):
    body = _client(db, {"dashboard": {"auth": {"enabled": False}}}).get(
        "/api/audit/verify").json()
    assert body["chain_enabled"] is False           # default off
    assert body["verified"] is True and body["total"] == 0   # empty chain


@requires_fastapi
def test_verify_reports_chain_enabled(db):
    body = _client(db, {"dashboard": {"auth": {"enabled": False}},
                        "audit": {"chain_enabled": True}}).get(
        "/api/audit/verify").json()
    assert body["chain_enabled"] is True
