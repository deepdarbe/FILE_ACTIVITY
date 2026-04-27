"""Tests for the analyzer cache (issue #123).

Goals
-----

Issue #123 reported that ``/api/reports/{frequency,types,sizes}/{id}``
re-ran the analyzers on every poll, hammering the DB on 2.5M-row scans.
The fix is a two-tier cache (in-memory LRU + DB-persisted) keyed on
``(analyzer_name, scan_id)``.

These tests cover:

* ``test_first_call_computes_and_caches`` - cold call invokes ``compute``
  and persists to both tiers.
* ``test_second_call_hits_memory`` - warm call short-circuits via the
  in-memory LRU; ``compute`` is NOT invoked again.
* ``test_db_cache_survives_lru_eviction`` - clearing the LRU forces a
  DB read; ``compute`` still not invoked.
* ``test_different_scan_ids_independent`` - cache slots are per-scan_id;
  a new scan does not return stale data.
* ``test_startup_cleanup_purges_orphan_cache_rows`` - rows for vanished
  scan_ids get cleaned up by ``cleanup_old_scans``.

Also includes a smoke test against the FastAPI handler logic:
warm-call response time < 50ms vs cold-call must do real work.
"""

from __future__ import annotations

import os
import sys
import time

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.analyzer import cache as analyzer_cache  # noqa: E402
from src.storage.database import Database  # noqa: E402


@pytest.fixture
def db(tmp_path):
    """Bare DB with a source + completed scan_run, no scanned_files needed.

    The cache layer treats the analyzer ``compute`` callable as opaque,
    so we don't need realistic data — a counter callable suffices.
    """
    db_path = tmp_path / "cache.db"
    cfg = {"path": str(db_path)}
    database = Database(cfg)
    database.connect()
    with database.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path) VALUES(?, ?)",
            ("src1", "/tmp/src1"),
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, 'completed')",
            (source_id,),
        )
        scan_id = cur.lastrowid

    # Tests run sequentially in this module but the LRU is module-global;
    # reset between cases so coverage doesn't bleed across tests.
    analyzer_cache.clear_memory_cache()
    yield database, source_id, scan_id
    database.close()


def _counting_compute():
    """Return a callable that records invocation count + a deterministic
    payload. The cache must invoke it at most once per cold path."""
    calls = {"n": 0}

    def _compute():
        calls["n"] += 1
        return {
            "frequency": [
                {"days": 30, "label": "0-30", "file_count": 100, "total_size": 1024},
                {"days": 90, "label": "31-90", "file_count": 50, "total_size": 512},
            ],
            "scan_id": 1,
            "call_n": calls["n"],
        }

    return _compute, calls


# ---------------------------------------------------------------------------


def test_first_call_computes_and_caches(db):
    database, _, scan_id = db
    compute, calls = _counting_compute()

    envelope = analyzer_cache.get_or_compute(database, "frequency", scan_id, compute)

    assert calls["n"] == 1
    assert envelope["cache"]["hit"] is False
    assert envelope["cache"]["source"] is None
    assert envelope["results"]["call_n"] == 1
    # Must have hit the DB tier as well
    with database.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM analyzer_cache WHERE scan_id=? AND analyzer_name=?",
            (scan_id, "frequency"),
        )
        assert cur.fetchone()["c"] == 1


def test_second_call_hits_memory(db):
    database, _, scan_id = db
    compute, calls = _counting_compute()

    analyzer_cache.get_or_compute(database, "frequency", scan_id, compute)
    envelope = analyzer_cache.get_or_compute(database, "frequency", scan_id, compute)

    assert calls["n"] == 1, "compute should NOT be re-invoked on warm call"
    assert envelope["cache"]["hit"] is True
    assert envelope["cache"]["source"] == "memory"
    assert envelope["results"]["call_n"] == 1


def test_db_cache_survives_lru_eviction(db):
    database, _, scan_id = db
    compute, calls = _counting_compute()

    # Cold call - both tiers populated
    analyzer_cache.get_or_compute(database, "types", scan_id, compute)
    assert calls["n"] == 1

    # Wipe the in-memory tier (simulates process restart for the LRU only)
    analyzer_cache.clear_memory_cache()
    assert analyzer_cache.lru_size() == 0

    envelope = analyzer_cache.get_or_compute(database, "types", scan_id, compute)

    # compute must not run - DB tier covered the gap
    assert calls["n"] == 1
    assert envelope["cache"]["hit"] is True
    assert envelope["cache"]["source"] == "db"
    assert envelope["results"]["call_n"] == 1
    # And the DB hit should have hydrated the LRU
    assert analyzer_cache.lru_size() == 1


def test_different_scan_ids_independent(db):
    database, source_id, scan_id_a = db

    # Add a second completed scan for the same source - new scan_id
    with database.get_cursor() as cur:
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, 'completed')",
            (source_id,),
        )
        scan_id_b = cur.lastrowid
    assert scan_id_b != scan_id_a

    compute_a, calls_a = _counting_compute()
    compute_b, calls_b = _counting_compute()

    env_a = analyzer_cache.get_or_compute(database, "sizes", scan_id_a, compute_a)
    env_b = analyzer_cache.get_or_compute(database, "sizes", scan_id_b, compute_b)

    # Each scan_id triggers its own compute
    assert calls_a["n"] == 1
    assert calls_b["n"] == 1
    assert env_a["cache"]["hit"] is False
    assert env_b["cache"]["hit"] is False

    # Warm calls hit memory independently
    env_a2 = analyzer_cache.get_or_compute(database, "sizes", scan_id_a, compute_a)
    env_b2 = analyzer_cache.get_or_compute(database, "sizes", scan_id_b, compute_b)
    assert calls_a["n"] == 1
    assert calls_b["n"] == 1
    assert env_a2["cache"]["source"] == "memory"
    assert env_b2["cache"]["source"] == "memory"


def test_startup_cleanup_purges_orphan_cache_rows(db):
    database, source_id, scan_id = db
    compute, _ = _counting_compute()

    # Populate cache for the live scan
    analyzer_cache.get_or_compute(database, "frequency", scan_id, compute)

    # Insert an orphan row referring to a scan_id that no longer exists.
    orphan_scan_id = scan_id + 999
    with database.get_cursor() as cur:
        cur.execute(
            "INSERT INTO analyzer_cache(scan_id, analyzer_name, result_json) "
            "VALUES(?, 'frequency', '{}')",
            (orphan_scan_id,),
        )

    # cleanup_old_scans purges orphan_cache rows alongside scanned_files orphans
    result = database.cleanup_old_scans(keep_last_n=10)
    assert "deleted_cache" in result
    assert result["deleted_cache"] >= 1

    with database.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM analyzer_cache WHERE scan_id=?",
            (orphan_scan_id,),
        )
        assert cur.fetchone()["c"] == 0
        # Live row untouched
        cur.execute(
            "SELECT COUNT(*) AS c FROM analyzer_cache WHERE scan_id=?",
            (scan_id,),
        )
        assert cur.fetchone()["c"] == 1


def test_smoke_warm_call_under_50ms(db):
    """Cold call may be slow (compute does real work); warm call must
    short-circuit. Issue #123 acceptance criterion.
    """
    database, _, scan_id = db
    slow_calls = {"n": 0}

    def slow_compute():
        slow_calls["n"] += 1
        time.sleep(0.05)  # simulate the multi-second prod query
        return {"big": list(range(100))}

    # Cold - includes the simulated slow work
    analyzer_cache.get_or_compute(database, "frequency", scan_id, slow_compute)
    assert slow_calls["n"] == 1

    # Warm - must NOT call slow_compute again, and must be fast
    t0 = time.perf_counter()
    envelope = analyzer_cache.get_or_compute(database, "frequency", scan_id, slow_compute)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    assert slow_calls["n"] == 1
    assert envelope["cache"]["hit"] is True
    assert elapsed_ms < 50, f"warm call took {elapsed_ms:.1f}ms (>= 50ms budget)"
