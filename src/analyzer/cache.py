"""Two-tier cache for analyzer reports (issue #123).

Background
----------

The frequency / type / size analyzer endpoints recompute the same expensive
SQL aggregations every time they are called. With a 2.5M-row scan, each
call is slow, and the dashboard was hitting them every 1-2 minutes via a
scan-progress side-effect (see ``index.html`` near the
``pollScanProgress`` handler). Result: constant CPU + DB load.

Insight: the analyzer output is **deterministic given a scan_id**. Once a
scan is finished, recomputing is pointless. Even mid-scan, recomputing
every poll is overkill.

Cache layers
------------

1. **In-memory LRU** — process-local, keyed on ``(analyzer_name, scan_id)``.
   Survives only until the process restarts but covers the >99% case
   (same dashboard session, same scan).
2. **DB-persisted** — survives restart. New ``analyzer_cache`` table
   keyed on ``(scan_id, analyzer_name)``.

Lookup order:

1. memory LRU hit -> return ``cache.source = "memory"``
2. DB hit         -> hydrate LRU, return ``cache.source = "db"``
3. miss           -> compute fresh, persist to both, return
   ``cache.hit = False``

Invalidation
------------

None needed. Cache is keyed on ``scan_id`` which is immutable. A new scan
gets a new ``scan_id`` and therefore a fresh cache slot. Old ``scan_id``
entries become harmless dead rows; ``cleanup_old_scans`` purges them at
startup along with the orphan ``scanned_files`` rows.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from functools import lru_cache
from typing import Any, Callable, Optional

logger = logging.getLogger("file_activity.analyzer.cache")


# ---------------------------------------------------------------------------
# In-memory LRU
# ---------------------------------------------------------------------------
#
# Stored values are JSON-encoded strings rather than dicts so that:
#   (a) ``functools.lru_cache`` doesn't keep references to mutable objects
#       that callers might accidentally mutate after caching.
#   (b) Memory and DB layers store the same canonical wire format -
#       round-tripping between them is a no-op rather than a re-serialise.
#
# 128 entries x 3 analyzers = 384 max in-memory rows. Each row is at most
# a few KB of JSON. Trivial RAM cost.


@lru_cache(maxsize=128)
def _memo(analyzer_name: str, scan_id: int) -> Optional[str]:
    """Sentinel slot. Real population happens via :func:`_memo_set`.

    ``functools.lru_cache`` doesn't expose a public setter, but we can fake
    one by calling ``_memo`` to allocate the cell and then patching the
    cache dict directly. We avoid that hack and instead use a manual dict
    with size-bounded eviction below.
    """
    return None  # pragma: no cover - never invoked directly


# Replace the lru_cache-based memo with an explicit OrderedDict-backed LRU
# so we can actually set values. The lru_cache import above stays as a
# documented reference for the design constraint (issue #123 spec).

from collections import OrderedDict  # noqa: E402

_LRU_MAXSIZE = 128 * 3  # 128 per analyzer x 3 analyzers
_lru: OrderedDict[tuple[str, int], tuple[str, float]] = OrderedDict()


def _lru_get(analyzer_name: str, scan_id: int) -> Optional[tuple[str, float]]:
    """Return (json_str, computed_at_epoch) or None."""
    key = (analyzer_name, scan_id)
    if key in _lru:
        # Move to end - most recently used
        _lru.move_to_end(key)
        return _lru[key]
    return None


def _lru_set(analyzer_name: str, scan_id: int, value: str, computed_at: float) -> None:
    key = (analyzer_name, scan_id)
    _lru[key] = (value, computed_at)
    _lru.move_to_end(key)
    while len(_lru) > _LRU_MAXSIZE:
        _lru.popitem(last=False)


def clear_memory_cache() -> None:
    """Test helper. Drops the in-memory LRU - DB layer is untouched."""
    _lru.clear()


def lru_size() -> int:
    """For introspection / tests."""
    return len(_lru)


# ---------------------------------------------------------------------------
# DB schema management
# ---------------------------------------------------------------------------


def ensure_table(db) -> None:
    """Create the ``analyzer_cache`` table if it doesn't exist.

    Called from ``Database._create_tables`` so every fresh DB has it; safe
    to call repeatedly (idempotent ``CREATE TABLE IF NOT EXISTS``).
    """
    with db.get_cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS analyzer_cache (
                scan_id        INTEGER NOT NULL,
                analyzer_name  TEXT NOT NULL,
                result_json    TEXT NOT NULL,
                computed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (scan_id, analyzer_name)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_analyzer_cache_scan "
            "ON analyzer_cache(scan_id)"
        )


def purge_orphan_rows(db) -> int:
    """Delete cache rows for scan_ids that no longer exist.

    Called at startup cleanup time alongside the existing scanned_files
    orphan purge. Returns number of rows deleted.
    """
    try:
        with db.get_cursor() as cur:
            cur.execute(
                "DELETE FROM analyzer_cache "
                "WHERE scan_id NOT IN (SELECT id FROM scan_runs)"
            )
            return cur.rowcount or 0
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("analyzer_cache orphan purge failed: %s", e)
        return 0


# ---------------------------------------------------------------------------
# Lookup / store helpers
# ---------------------------------------------------------------------------


def _db_get(db, analyzer_name: str, scan_id: int) -> Optional[tuple[str, float]]:
    try:
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT result_json, computed_at FROM analyzer_cache "
                "WHERE scan_id = ? AND analyzer_name = ?",
                (scan_id, analyzer_name),
            )
            row = cur.fetchone()
            if not row:
                return None
            computed_at = row["computed_at"]
            ts = _parse_timestamp(computed_at)
            return row["result_json"], ts
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("analyzer_cache DB read failed: %s", e)
        return None


def _db_set(db, analyzer_name: str, scan_id: int, value: str) -> None:
    try:
        with db.get_cursor() as cur:
            cur.execute(
                "INSERT OR REPLACE INTO analyzer_cache "
                "(scan_id, analyzer_name, result_json, computed_at) "
                "VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                (scan_id, analyzer_name, value),
            )
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("analyzer_cache DB write failed: %s", e)


def _parse_timestamp(ts: Any) -> float:
    """Best-effort parse of ``CURRENT_TIMESTAMP`` text into epoch seconds.

    SQLite stores ``CURRENT_TIMESTAMP`` as ISO-8601 text in UTC. We only
    need this for the ``age_seconds`` field in the cache envelope, so
    fall back to ``time.time()`` on parse failure rather than crashing.
    """
    if isinstance(ts, (int, float)):
        return float(ts)
    if isinstance(ts, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(ts, fmt).timestamp()
            except ValueError:
                continue
    return time.time()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def get_or_compute(
    db,
    analyzer_name: str,
    scan_id: int,
    compute: Callable[[], dict],
) -> dict:
    """Return analyzer result wrapped in a cache envelope.

    Response shape:

        {
            "results": <whatever ``compute()`` returned>,
            "cache": {"hit": bool, "source": "memory"|"db"|None,
                      "age_seconds": int}
        }

    ``compute()`` is only invoked on full cache miss.
    """
    now = time.time()

    hit = _lru_get(analyzer_name, scan_id)
    if hit is not None:
        value_json, computed_at = hit
        result = json.loads(value_json)
        return {
            "results": result,
            "cache": {
                "hit": True,
                "source": "memory",
                "age_seconds": int(max(0, now - computed_at)),
            },
        }

    db_hit = _db_get(db, analyzer_name, scan_id)
    if db_hit is not None:
        value_json, computed_at = db_hit
        # Hydrate the in-memory tier so subsequent calls are O(1).
        _lru_set(analyzer_name, scan_id, value_json, computed_at)
        result = json.loads(value_json)
        return {
            "results": result,
            "cache": {
                "hit": True,
                "source": "db",
                "age_seconds": int(max(0, now - computed_at)),
            },
        }

    # Full miss - compute, persist both tiers.
    result = compute()
    try:
        value_json = json.dumps(result, default=str)
    except (TypeError, ValueError) as e:  # pragma: no cover - defensive
        logger.warning(
            "analyzer_cache: result for %s/%d not JSON-serialisable: %s",
            analyzer_name, scan_id, e,
        )
        # Still return the result; just don't cache it.
        return {
            "results": result,
            "cache": {"hit": False, "source": None, "age_seconds": 0},
        }

    _lru_set(analyzer_name, scan_id, value_json, now)
    _db_set(db, analyzer_name, scan_id, value_json)
    return {
        "results": result,
        "cache": {"hit": False, "source": None, "age_seconds": 0},
    }
