"""Tests for #339 — source partial update (archive_dest editable post-create).

Customer bug: a source added WITHOUT an archive destination could never get
one afterwards — no update path existed anywhere in the stack (no PUT
endpoint, no Database.update_source, no edit UI), so every archive feature
400'd with 'Arsiv hedefi tanimli degil' forever.

DB-level tests run everywhere (plain sqlite). The HTTP-level tests are
fastapi-gated (repo convention) and disable the Wave 10 auth middleware via
config so TestClient calls don't 401.
"""

from __future__ import annotations

import importlib.util
import sqlite3

import pytest

from src.storage.database import Database

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
requires_fastapi = pytest.mark.skipif(
    not HAS_FASTAPI, reason="fastapi not installed in this environment"
)


@pytest.fixture
def db(tmp_path):
    d = Database({
        "path": str(tmp_path / "s.db"),
        "retention": {"auto_cleanup_on_startup": False},
    })
    d.connect()
    with d.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path, archive_dest) "
            "VALUES('ortak', '\\\\fs\\ortak', NULL)")
        cur.execute(
            "INSERT INTO sources(name, unc_path, archive_dest) "
            "VALUES('ik', '\\\\fs\\ik', 'D:\\Archive\\ik')")
    yield d
    d.close()


def test_set_archive_dest_after_create(db):
    assert db.get_source_by_id(1).archive_dest is None
    assert db.update_source(1, {"archive_dest": "D:\\Archive\\ortak"}) is True
    assert db.get_source_by_id(1).archive_dest == "D:\\Archive\\ortak"


def test_clear_archive_dest(db):
    assert db.update_source(2, {"archive_dest": None}) is True
    assert db.get_source_by_id(2).archive_dest is None


def test_unc_path_not_updatable(db):
    """unc_path must be ignored — archive/restore mapping is anchored to it."""
    assert db.update_source(1, {"unc_path": "\\\\evil\\path"}) is False
    assert db.get_source_by_id(1).unc_path == "\\\\fs\\ortak"


def test_enabled_none_is_skipped(db):
    """Explicit enabled=None must NOT silently disable the source."""
    assert db.update_source(1, {"enabled": None}) is False
    assert db.get_source_by_id(1).enabled is True


def test_name_unique_conflict_raises(db):
    with pytest.raises(sqlite3.IntegrityError):
        db.update_source(1, {"name": "ik"})


def test_unknown_source_returns_false(db):
    assert db.update_source(999, {"archive_dest": "x"}) is False


# ---------------------------------------------------------------------------
# HTTP-level (fastapi-gated)
# ---------------------------------------------------------------------------


@pytest.fixture
def client(db):
    from fastapi.testclient import TestClient
    from src.dashboard.api import create_app

    app = create_app(db, {"dashboard": {"auth": {"enabled": False}}})
    return TestClient(app)


@requires_fastapi
def test_put_sets_archive_dest_and_audits(db, client):
    r = client.put("/api/sources/1", json={"archive_dest": "D:\\Arsiv"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True and body["updated"] is True
    assert db.get_source_by_id(1).archive_dest == "D:\\Arsiv"
    with db.get_read_cursor() as cur:
        n = cur.execute(
            "SELECT COUNT(*) AS c FROM file_audit_events "
            "WHERE event_type='source_updated'").fetchone()["c"]
    assert n == 1


@requires_fastapi
def test_put_empty_string_clears_dest(db, client):
    r = client.put("/api/sources/2", json={"archive_dest": "   "})
    assert r.status_code == 200, r.text
    assert db.get_source_by_id(2).archive_dest is None


@requires_fastapi
def test_put_404_unknown_source(client):
    assert client.put("/api/sources/999", json={"archive_dest": "x"}).status_code == 404


@requires_fastapi
def test_put_409_duplicate_name(client):
    assert client.put("/api/sources/1", json={"name": "ik"}).status_code == 409


@requires_fastapi
def test_put_400_empty_body(db, client):
    r = client.put("/api/sources/1", json={})
    assert r.status_code == 400
    # No audit row for a rejected update.
    with db.get_read_cursor() as cur:
        n = cur.execute(
            "SELECT COUNT(*) AS c FROM file_audit_events "
            "WHERE event_type='source_updated'").fetchone()["c"]
    assert n == 0
