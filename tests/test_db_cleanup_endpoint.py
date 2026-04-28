"""Tests for /api/db/cleanup endpoint — all three call forms.

Covers acceptance criteria from issue #133 / agent instructions:

* ``POST /api/db/cleanup?keep_last=0`` → 200, deletes all scans.
* ``POST /api/db/cleanup?keep_last_n_scans=3`` → 200, keeps last 3.
* ``POST /api/db/cleanup`` with body ``{"keep_last_n_scans": 5, "confirm": true}`` → 200.
* Body without ``confirm: true`` → 400.
* Audit event ``db_cleanup`` is written on success.
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
# Minimal stubs required by create_app
# ---------------------------------------------------------------------------


class _StubAnalytics:
    available = False


class _StubADLookup:
    pass


class _StubEmailNotifier:
    pass


def _make_config() -> dict:
    return {
        "dashboard": {"host": "127.0.0.1", "port": 8085, "auth": {"enabled": False}},
        "archiving": {"enabled": False, "dry_run": True},
        "audit": {"chain_enabled": False},
        "database": {},
    }


# ---------------------------------------------------------------------------
# Fixture: real SQLite DB + one source + seeded scan_runs
# ---------------------------------------------------------------------------


@pytest.fixture
def seeded_db(tmp_path):
    """On-disk SQLite with one source and several completed scan_runs."""
    db_path = tmp_path / "cleanup_test.db"
    cfg = {"path": str(db_path)}
    database = Database(cfg)
    database.connect()
    with database.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
            ("share1", "\\\\fs\\share1"),
        )
        source_id = cur.lastrowid
    yield database, source_id
    database.close()


@pytest.fixture
def client(seeded_db, tmp_path):
    """Full create_app TestClient backed by the seeded DB."""
    database, _ = seeded_db
    app = create_app(
        db=database,
        config=_make_config(),
        analytics=_StubAnalytics(),
        ad_lookup=_StubADLookup(),
        email_notifier=_StubEmailNotifier(),
    )
    return TestClient(app), database


def _seed_runs(database, source_id, n: int) -> None:
    with database.get_cursor() as cur:
        for _ in range(n):
            cur.execute(
                "INSERT INTO scan_runs(source_id, status) VALUES(?, 'completed')",
                (source_id,),
            )


def _count_runs(database) -> int:
    with database.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM scan_runs")
        return cur.fetchone()["cnt"]


# ---------------------------------------------------------------------------
# Form 1: ?keep_last=N  (legacy query param)
# ---------------------------------------------------------------------------


def test_keep_last_0_deletes_all(client, seeded_db):
    """`?keep_last=0` must return 200 and delete every scan_run."""
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 4)
    assert _count_runs(database) == 4

    resp = tc.post("/api/db/cleanup?keep_last=0")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "deleted_runs" in body
    assert body["deleted_runs"] >= 4
    assert _count_runs(database) == 0


def test_keep_last_2_leaves_two_runs(client, seeded_db):
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 6)

    resp = tc.post("/api/db/cleanup?keep_last=2")
    assert resp.status_code == 200, resp.text
    assert _count_runs(database) == 2


def test_keep_last_no_param_defaults_to_5(client, seeded_db):
    """No-param POST keeps the legacy default of 5."""
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 8)

    resp = tc.post("/api/db/cleanup")
    assert resp.status_code == 200, resp.text
    assert _count_runs(database) == 5


# ---------------------------------------------------------------------------
# Form 2: ?keep_last_n_scans=N  (alias query param)
# ---------------------------------------------------------------------------


def test_keep_last_n_scans_alias_returns_200(client, seeded_db):
    """`?keep_last_n_scans=3` must be accepted with 200."""
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 7)

    resp = tc.post("/api/db/cleanup?keep_last_n_scans=3")
    assert resp.status_code == 200, resp.text
    assert _count_runs(database) == 3


def test_keep_last_n_scans_0_deletes_all(client, seeded_db):
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 3)

    resp = tc.post("/api/db/cleanup?keep_last_n_scans=0")
    assert resp.status_code == 200, resp.text
    assert _count_runs(database) == 0


# ---------------------------------------------------------------------------
# Form 3: JSON body {"keep_last_n_scans": N, "confirm": true}
# ---------------------------------------------------------------------------


def test_body_with_confirm_true_returns_200(client, seeded_db):
    """`{keep_last_n_scans: 5, confirm: true}` must return 200."""
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 8)

    resp = tc.post(
        "/api/db/cleanup",
        json={"keep_last_n_scans": 5, "confirm": True},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "deleted_runs" in body
    assert _count_runs(database) == 5


def test_body_keep_0_with_confirm_deletes_all(client, seeded_db):
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 4)

    resp = tc.post(
        "/api/db/cleanup",
        json={"keep_last_n_scans": 0, "confirm": True},
    )
    assert resp.status_code == 200, resp.text
    assert _count_runs(database) == 0


def test_body_without_confirm_returns_400(client, seeded_db):
    """Body without `confirm: true` must be rejected with HTTP 400."""
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 3)

    resp = tc.post(
        "/api/db/cleanup",
        json={"keep_last_n_scans": 2},
    )
    assert resp.status_code == 400, resp.text
    assert "confirm" in resp.json()["detail"].lower()
    # Cleanup must NOT have run.
    assert _count_runs(database) == 3


def test_body_with_confirm_false_returns_400(client, seeded_db):
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 3)

    resp = tc.post(
        "/api/db/cleanup",
        json={"keep_last_n_scans": 1, "confirm": False},
    )
    assert resp.status_code == 400, resp.text
    assert _count_runs(database) == 3


# ---------------------------------------------------------------------------
# Audit event written on successful cleanup
# ---------------------------------------------------------------------------


def test_audit_event_written_on_cleanup(client, seeded_db):
    """A `db_cleanup` audit event must be inserted after a successful run."""
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 3)

    resp = tc.post("/api/db/cleanup?keep_last=1")
    assert resp.status_code == 200, resp.text

    with database.get_cursor() as cur:
        cur.execute(
            "SELECT * FROM file_audit_events WHERE event_type='db_cleanup' ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
    assert row is not None, "No db_cleanup audit event found"
    assert row["event_type"] == "db_cleanup"
    assert "keep_last_n=1" in (row["details"] or "")


def test_audit_event_written_for_body_form(client, seeded_db):
    """Audit event is also written when cleanup is triggered via JSON body."""
    tc, database = client
    _, source_id = seeded_db
    _seed_runs(database, source_id, 4)

    resp = tc.post(
        "/api/db/cleanup",
        json={"keep_last_n_scans": 2, "confirm": True},
    )
    assert resp.status_code == 200, resp.text

    with database.get_cursor() as cur:
        cur.execute(
            "SELECT * FROM file_audit_events WHERE event_type='db_cleanup' ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
    assert row is not None, "No db_cleanup audit event found"
    assert "keep_last_n=2" in (row["details"] or "")
