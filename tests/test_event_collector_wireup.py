"""Tests for #340 Faz 1 — EventCollector wire-up.

The collector existed since the plan was written but was wired NOWHERE
(no CLI, no scheduler, no config). Faz 1 connects it: collect-events CLI,
a config-gated interval scheduler job, an idempotency guard
(idx_ual_dedup + INSERT OR IGNORE) and the pywintypes time normalization.

No pywin32 / fastapi needed — parsers take plain values, the scheduler is
exercised object-level, and the CLI is inspected via click's registry.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime

import pytest

from src.storage.database import Database
from src.user_activity.event_collector import EventCollector, _parse_access_mask

# apscheduler/click chains are runtime deps present in CI's Docker image;
# gate the scheduler/CLI tests where they're absent (same convention as
# the fastapi-gated suites).
HAS_APSCHEDULER = importlib.util.find_spec("apscheduler") is not None
requires_apscheduler = pytest.mark.skipif(
    not HAS_APSCHEDULER, reason="apscheduler not installed in this environment"
)


@pytest.fixture
def db(tmp_path):
    d = Database({
        "path": str(tmp_path / "ec.db"),
        "retention": {"auto_cleanup_on_startup": False},
    })
    d.connect()
    yield d
    d.close()


def test_parse_access_mask_delete():
    assert _parse_access_mask("0x10000") == "delete"
    assert _parse_access_mask("0x1") == "read"
    assert _parse_access_mask("junk") == "unknown"


def test_build_record_normalizes_datetime(db):
    """pywintypes TimeGenerated (a datetime subclass) must be stored as the
    schema's TEXT format, local wall-clock."""
    c = EventCollector(db, {})
    rec = c._build_record(
        "mehmet", "ITWISE", r"E:\ortak\rapor.xlsx", "0x10000",
        datetime(2026, 7, 16, 13, 26, 17), "10.0.0.5", 4660)
    assert rec["access_time"] == "2026-07-16 13:26:17"
    assert rec["access_type"] == "delete"
    assert rec["file_name"] == "rapor.xlsx"


def test_build_record_filters(db):
    c = EventCollector(db, {})
    assert c._build_record("PC01$", "D", r"E:\x.txt", "0x1",
                           datetime.now(), None, 4663) is None  # machine acct
    assert c._build_record("SYSTEM", "NT", r"E:\x.txt", "0x1",
                           datetime.now(), None, 4663) is None  # excluded
    assert c._build_record("u", "D", "E:\\dir\\", "0x1",
                           datetime.now(), None, 4663) is None  # directory
    assert c._build_record("u", "D", r"E:\a.tmp", "0x1",
                           datetime.now(), None, 4663) is None  # excluded ext


def test_bulk_insert_is_idempotent(db):
    """#340: overlapping lookback windows re-insert the same events —
    idx_ual_dedup + INSERT OR IGNORE must collapse them to one row set."""
    rows = [{
        "source_id": None, "username": "mehmet", "domain": "ITWISE",
        "file_path": r"E:\ortak\a.txt", "file_name": "a.txt",
        "extension": "txt", "access_type": "delete",
        "access_time": "2026-07-16 13:00:00", "client_ip": "10.0.0.5",
        "file_size": 0, "event_id": 4660,
    }, {
        "source_id": None, "username": "mehmet", "domain": "ITWISE",
        "file_path": r"E:\ortak\b.txt", "file_name": "b.txt",
        "extension": "txt", "access_type": "delete",
        "access_time": "2026-07-16 13:00:01", "client_ip": None,
        "file_size": 0, "event_id": 4660,
    }]
    db.bulk_insert_access_logs(rows)
    db.bulk_insert_access_logs(rows)   # overlapping second window
    with db.get_read_cursor() as cur:
        n = cur.execute("SELECT COUNT(*) AS c FROM user_access_logs").fetchone()["c"]
    assert n == 2


def _mk_scheduler(db, config):
    from src.scheduler.task_scheduler import TaskScheduler
    return TaskScheduler(db, config)


@requires_apscheduler
def test_scheduler_job_gated_on_config(db):
    """collect_events registers only when user_activity.enabled=true."""
    s_off = _mk_scheduler(db, {"user_activity": {"enabled": False}})
    s_off._register_event_collect_job()
    assert s_off.scheduler.get_job("collect_events") is None

    s_on = _mk_scheduler(db, {"user_activity": {"enabled": True,
                                                "collect_interval_minutes": 15}})
    s_on._register_event_collect_job()
    job = s_on.scheduler.get_job("collect_events")
    assert job is not None
    assert job.name == "collect_events:security_log"


@requires_apscheduler
def test_run_collect_events_skips_when_disabled(db):
    s = _mk_scheduler(db, {"user_activity": {"enabled": False}})
    res = s._run_collect_events()
    assert res == {"status": "skipped", "message": "user_activity.enabled=false"}


def test_cli_command_registered():
    """main.py exposes collect-events.

    Deliberately a SOURCE-TEXT pin, not an import: main.py replaces
    sys.stdout at import time (utf-8 TextIOWrapper, main.py:10-11),
    which destroys pytest's capture streams and cascades
    'I/O operation on closed file' into every later test's
    setup/teardown (observed in Docker CI).
    """
    import pathlib
    src = pathlib.Path(__file__).resolve().parent.parent / "main.py"
    text = src.read_text(encoding="utf-8")
    assert '@cli.command("collect-events")' in text
    assert "EventCollector(app.db, app.config)" in text
