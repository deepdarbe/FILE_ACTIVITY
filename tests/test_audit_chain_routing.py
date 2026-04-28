"""H-3 (issue #158): public-wrapper routing tests.

The public ``insert_audit_event`` / ``insert_audit_event_simple`` must
auto-route to the hash-chained variant (issue #38) when
``audit.chain_enabled`` is true, and fall back to the raw INSERT
otherwise. Existing call-sites in scanner / archiver / dashboard etc.
keep working unchanged in both modes.
"""

from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402


def _make_db(tmp_path, chain_enabled: bool) -> Database:
    db = Database({"path": str(tmp_path / "test.db")})
    db.connect()
    db.set_audit_chain_enabled(chain_enabled)
    # FK on file_audit_events.source_id -> sources(id); seed one row.
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (id, name, unc_path) VALUES (1, ?, ?)",
            ("test_src", "//srv/share"),
        )
    return db


def _chain_count(db: Database) -> int:
    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM audit_log_chain")
        return int(cur.fetchone()["c"])


def _events_count(db: Database) -> int:
    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM file_audit_events")
        return int(cur.fetchone()["c"])


# ── chain_enabled=true: public wrapper goes through chained path ──


def test_insert_audit_event_routes_to_chained_when_enabled(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)

    event_id = db.insert_audit_event(
        source_id=1,
        event_time="2026-04-28 12:00:00",
        event_type="modify",
        username="alice",
        file_path="/share/a.txt",
        file_name="a.txt",
        details=None,
        detected_by="watcher",
    )

    assert event_id is not None
    assert _events_count(db) == 1
    # The smoking gun: chain row exists.
    assert _chain_count(db) == 1
    result = db.verify_audit_chain()
    assert result["verified"] is True
    assert result["total"] == 1


def test_insert_audit_event_simple_routes_to_chained_when_enabled(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)

    event_id = db.insert_audit_event_simple(
        source_id=1,
        event_type="archive_skipped_legal_hold",
        username="system",
        file_path="/share/b.txt",
        details="Hold #7: investigation",
        detected_by="archive",
    )

    assert event_id is not None
    assert _events_count(db) == 1
    assert _chain_count(db) == 1


# ── chain_enabled=false: public wrapper goes through unchained path ──


def test_insert_audit_event_routes_to_unchained_when_disabled(tmp_path):
    db = _make_db(tmp_path, chain_enabled=False)

    event_id = db.insert_audit_event(
        source_id=1,
        event_time="2026-04-28 12:00:00",
        event_type="modify",
        username="bob",
        file_path="/share/c.txt",
        file_name="c.txt",
        details=None,
        detected_by="watcher",
    )

    assert event_id is not None
    assert _events_count(db) == 1
    # No chain row written when flag is off — preserves zero-overhead default.
    assert _chain_count(db) == 0


def test_insert_audit_event_simple_routes_to_unchained_when_disabled(tmp_path):
    db = _make_db(tmp_path, chain_enabled=False)

    event_id = db.insert_audit_event_simple(
        source_id=1,
        event_type="sql_query",
        username="admin",
        file_path="/share/d.txt",
        details="SELECT 1",
        detected_by="dashboard",
    )

    assert event_id is not None
    assert _events_count(db) == 1
    assert _chain_count(db) == 0


# ── existing call-site shapes keep working unchanged ──


def test_existing_callsite_shape_file_watcher(tmp_path):
    """``file_watcher.py`` calls ``insert_audit_event_chained(event)`` with
    the dict shape ``{source_id, event_time, event_type, username,
    file_path, file_name, detected_by}``. Sanity check that flips of
    chain_enabled never break that contract."""
    for enabled in (False, True):
        db = _make_db(tmp_path / f"watch_{enabled}", chain_enabled=enabled)
        event = {
            "source_id": 1,
            "event_time": "2026-04-28 12:00:00",
            "event_type": "create",
            "username": "alice",
            "file_path": "/share/x.txt",
            "file_name": "x.txt",
            "detected_by": "watcher",
        }
        eid = db.insert_audit_event_chained(event)
        assert eid is not None
        assert _events_count(db) == 1
        assert _chain_count(db) == (1 if enabled else 0)


def test_existing_callsite_shape_dashboard_simple(tmp_path):
    """``dashboard/api.py`` calls ``insert_audit_event_simple(...)`` with
    keyword args. Routing must preserve that signature in both modes."""
    for enabled in (False, True):
        db = _make_db(tmp_path / f"dash_{enabled}", chain_enabled=enabled)
        eid = db.insert_audit_event_simple(
            source_id=None,
            event_type="scan_cancelled",
            username="admin",
            file_path="/share/scan42.txt",
            details="scan_id=42;partial_files=10;forced=False",
            detected_by="dashboard",
        )
        assert eid is not None
        assert _events_count(db) == 1
        assert _chain_count(db) == (1 if enabled else 0)
