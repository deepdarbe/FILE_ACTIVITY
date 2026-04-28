"""Incremental partial summary for in-flight scans (issue #139).

Background
----------

Before this module, the Overview / Reports / Insights pages showed *all
zeros* during the long MFT enumeration + insert phases of a fresh scan
because they only ever consulted ``scan_runs.summary_json`` which is
written exactly once at scan completion (``compute_scan_summary``).

A multi-million-row NTFS share takes 10-30 minutes; users staring at an
empty dashboard were filing duplicate "is it broken?" tickets.

Solution
--------

While the scanner is running, periodically aggregate the
``scanned_files`` rows that have *already* been written for the active
``scan_id`` and persist a compact JSON blob to
``scan_runs.partial_summary_json``. The relevant dashboard endpoints
fall back to this blob when their normal completed-scan cache is empty
and a scan is in progress. The frontend renders a banner explaining
that the numbers are still moving.

Constraints
-----------

* **Never block the writer.** The aggregate runs through
  :py:meth:`Database.get_read_cursor` which opens an independent
  ``mode=ro`` SQLite handle (issue #134). Even a 30-second compute
  cannot starve the bulk-INSERT loop.
* **Cheap.** Single ``GROUP BY`` per aggregate, ``LIMIT 10`` for
  per-extension, no ``ORDER BY file_size`` over the full table.
  Indexed by ``(source_id, scan_id, extension)`` and
  ``(source_id, scan_id)`` so even on 50M rows the SQL optimiser stays
  on the composite indices.
* **Throttled.** The scanner only invokes us every 10 minutes OR every
  100,000 records, whichever comes first. If a single compute exceeds
  30 seconds we double the throttle to 20 minutes (the orchestrator
  takes care of that — see ``FileScanner.scan_source``).

Output shape (matches the issue spec):

    {
        "total_files": int,
        "total_size": int,
        "unique_owners": int,
        "top_extensions": [
            {"ext": "pdf", "count": 1234, "size": 567890},
            ...                                            # top 10 by count
        ],
        "size_buckets": {"tiny": int, "small": int, ...},  # config buckets
        "age_buckets":  {"30d": int, "90d": int, "180d": int, "365d": int},
        "is_partial": True,
        "computed_at": "<iso8601>",
    }
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict

logger = logging.getLogger("file_activity.analyzer.partial_summary")


# ---------------------------------------------------------------------------
# Defaults — kept in sync with ``Database._get_size_buckets_config``.
# ---------------------------------------------------------------------------

_DEFAULT_SIZE_BUCKETS: Dict[str, int] = {
    "tiny": 102_400,        # < 100 KiB
    "small": 1_048_576,     # 100 KiB - 1 MiB
    "medium": 104_857_600,  # 1 MiB - 100 MiB
    "large": 1_073_741_824, # 100 MiB - 1 GiB
}


def _resolve_size_buckets(db) -> Dict[str, int]:
    """Pull the ``analysis.size_buckets`` config dict via the DB helper.

    Falls back to the hard-coded default if the helper raises or
    returns something unexpected; the calling endpoint will still get a
    populated ``size_buckets`` map and a non-zero compute.
    """
    try:
        if hasattr(db, "_get_size_buckets_config"):
            cfg = db._get_size_buckets_config()
            if isinstance(cfg, dict) and cfg:
                return cfg
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("size_buckets resolve failed, using defaults: %s", e)
    return _DEFAULT_SIZE_BUCKETS


def _today_minus(days: int) -> str:
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")


def compute_partial_summary(db, scan_id: int) -> Dict[str, Any]:
    """Cheap aggregate over ``scanned_files`` rows written so far for ``scan_id``.

    Uses :py:meth:`Database.get_read_cursor` so it never contends with
    the scanner's writer lock.

    Returns the dict shape documented at module-top. On a transient
    SQLite error (e.g. database was VACUUMing) returns a minimal
    ``{"is_partial": True, "computed_at": ..., "error": "..."}`` so the
    caller can persist *something* and not retry-spam.
    """
    started = datetime.now()
    iso = started.isoformat(timespec="seconds")
    out: Dict[str, Any] = {
        "total_files": 0,
        "total_size": 0,
        "unique_owners": 0,
        "top_extensions": [],
        "size_buckets": {},
        "age_buckets": {"30d": 0, "90d": 0, "180d": 0, "365d": 0},
        "is_partial": True,
        "computed_at": iso,
    }

    size_buckets_cfg = _resolve_size_buckets(db)
    # Ensure the response always has the four canonical bucket keys
    # initialised to zero — endpoints render them as cards even when
    # we have no rows for that bucket yet.
    for label in size_buckets_cfg:
        out["size_buckets"].setdefault(label, 0)
    out["size_buckets"].setdefault("huge", 0)

    try:
        with db.get_read_cursor() as cur:
            # 1) Totals + unique owners — single row.
            row = cur.execute(
                "SELECT COUNT(*) AS c, "
                "       COALESCE(SUM(file_size), 0) AS s, "
                "       COUNT(DISTINCT owner) AS o "
                "FROM scanned_files WHERE scan_id=?",
                (scan_id,),
            ).fetchone()
            if row:
                out["total_files"] = int(row.get("c") or 0)
                out["total_size"] = int(row.get("s") or 0)
                out["unique_owners"] = int(row.get("o") or 0)

            # Short-circuit: no rows yet → return zeros + bucket scaffold.
            if out["total_files"] == 0:
                _stamp_elapsed(out, started)
                return out

            # 2) Top 10 extensions by count.
            ext_rows = cur.execute(
                "SELECT extension, COUNT(*) AS c, "
                "       COALESCE(SUM(file_size), 0) AS s "
                "FROM scanned_files WHERE scan_id=? "
                "GROUP BY extension ORDER BY c DESC LIMIT 10",
                (scan_id,),
            ).fetchall()
            out["top_extensions"] = [
                {
                    "ext": (r.get("extension") or "(uzantisiz)"),
                    "count": int(r.get("c") or 0),
                    "size": int(r.get("s") or 0),
                }
                for r in ext_rows
            ]

            # 3) Size buckets — one CASE-WHEN GROUP BY.
            sb_sorted = sorted(size_buckets_cfg.items(), key=lambda kv: kv[1])
            size_bucket_defs = []
            prev_max = 0
            for label, threshold in sb_sorted:
                size_bucket_defs.append((label, prev_max, threshold))
                prev_max = threshold
            # Open-ended top bucket; matches compute_scan_summary semantics.
            size_bucket_defs.append(("huge", prev_max, None))

            size_case_parts = []
            size_params: list = []
            for label, bmin, bmax in size_bucket_defs:
                if bmax is None:
                    size_case_parts.append("WHEN file_size >= ? THEN ?")
                    size_params.extend([bmin, label])
                else:
                    size_case_parts.append(
                        "WHEN file_size >= ? AND file_size < ? THEN ?"
                    )
                    size_params.extend([bmin, bmax, label])
            size_case_sql = " ".join(size_case_parts)
            size_rows = cur.execute(
                f"""
                SELECT bucket, COUNT(*) c FROM (
                    SELECT CASE {size_case_sql} ELSE NULL END AS bucket
                    FROM scanned_files WHERE scan_id=?
                )
                WHERE bucket IS NOT NULL
                GROUP BY bucket
                """,
                (*size_params, scan_id),
            ).fetchall()
            for r in size_rows:
                out["size_buckets"][r["bucket"]] = int(r["c"] or 0)

            # 4) Age buckets — 30/90/180/365 days since today, fall back to
            #    last_modify_time when last_access_time is NULL (NTFS
            #    NtfsDisableLastAccessUpdate trampoline). Counts files
            #    OLDER than the cutoff (i.e. 30d = "not touched in last 30 days").
            cutoffs = {
                "30d": _today_minus(30),
                "90d": _today_minus(90),
                "180d": _today_minus(180),
                "365d": _today_minus(365),
            }
            age_rows = cur.execute(
                """
                SELECT
                  SUM(CASE WHEN ts IS NOT NULL AND ts <= ? THEN 1 ELSE 0 END) AS d30,
                  SUM(CASE WHEN ts IS NOT NULL AND ts <= ? THEN 1 ELSE 0 END) AS d90,
                  SUM(CASE WHEN ts IS NOT NULL AND ts <= ? THEN 1 ELSE 0 END) AS d180,
                  SUM(CASE WHEN ts IS NOT NULL AND ts <= ? THEN 1 ELSE 0 END) AS d365
                FROM (
                    SELECT COALESCE(last_access_time, last_modify_time) AS ts
                    FROM scanned_files WHERE scan_id=?
                )
                """,
                (
                    cutoffs["30d"], cutoffs["90d"],
                    cutoffs["180d"], cutoffs["365d"],
                    scan_id,
                ),
            ).fetchone()
            if age_rows:
                out["age_buckets"] = {
                    "30d": int(age_rows.get("d30") or 0),
                    "90d": int(age_rows.get("d90") or 0),
                    "180d": int(age_rows.get("d180") or 0),
                    "365d": int(age_rows.get("d365") or 0),
                }
    except sqlite3.OperationalError as e:
        # Read-only handle can momentarily 5-busy during a checkpoint.
        # Don't crash the scanner thread; return what we have so far.
        logger.warning(
            "compute_partial_summary read failed scan=%s: %s", scan_id, e,
        )
        out["error"] = str(e)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning(
            "compute_partial_summary unexpected scan=%s: %s", scan_id, e,
        )
        out["error"] = str(e)

    _stamp_elapsed(out, started)
    return out


def _stamp_elapsed(out: Dict[str, Any], started: datetime) -> None:
    elapsed_ms = int((datetime.now() - started).total_seconds() * 1000)
    out["compute_elapsed_ms"] = elapsed_ms
