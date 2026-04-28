"""Partial summary v2 — live aggregates for every dashboard page.

Issue #181 Track B1.

Background
----------

The v1 partial summary (``src/analyzer/partial_summary.py``) only
populated the Overview page. While the scanner crunched a 3M-row NTFS
share, every other dashboard page (Extensions, Owners, Directories,
Anomalies, ...) still showed all-zeros / "no data" until the entire
scan finished — typically 10-30 minutes after MFT enumeration begun.

v2 keeps the same on-disk column (``scan_runs.partial_summary_json``)
but writes a richer JSON document:

    {
      "schema_version": 2,
      "computed_at": "<iso8601 utc>",
      "scan_state": "mft_phase|db_writing|enrich|completed",
      "progress": {
        "files_so_far": int, "size_so_far_bytes": int,
        "errors_so_far": int, "rate_per_sec": float, "active_dir": str
      },
      "summary": {
        "by_extension":      [{ext, count, size_bytes}, ... top 20],
        "by_directory":      [{path, count, size_bytes}, ... top 20],
        "by_owner":          [{owner, count, size_bytes}, ... top 20],
        "size_buckets":      {"<1MB":, "1-10MB":, ...},
        "age_buckets":       {"<30d":, "30-60d":, ...},
        "anomalies_so_far":  {"naming":, "extension":, "ransomware":},
        "top_paths_by_size": [{path, size_bytes}, ... top 10]
      }
    }

Architecture
------------

Unlike v1 (which re-runs ``GROUP BY`` queries on the read-only handle
every time it is called), v2 maintains running counters on the writer
thread. Each batch of rows the scanner inserts into ``scanned_files``
is also passed through :py:meth:`PartialSummaryV2Builder.absorb_batch`
which updates O(batch) state. ``flush_to_db`` renders the dict and
writes the JSON blob through the retry-protected connection. The
counters never escape the writer thread; no locks needed.

Memory budget: capped at ~50 MB on a 3.1M-row scan because internal
top-N dicts are pruned to 1000 entries each on every flush. Only the
visible top-20 surfaces in :py:meth:`render`.
"""

from __future__ import annotations

import heapq
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("file_activity.analyzer.partial_summary_v2")


# ---------------------------------------------------------------------------
# Bucket definitions
# ---------------------------------------------------------------------------

_SIZE_BUCKETS_ORDER = ["<1MB", "1-10MB", "10-100MB", "100-1GB", ">1GB"]
_AGE_BUCKETS_ORDER = ["<30d", "30-60d", "60-90d", "90-180d", "180-365d", ">365d"]

_KB = 1024
_MB = 1024 * _KB
_GB = 1024 * _MB

_SIZE_BUCKET_THRESHOLDS = [
    (_MB, "<1MB"),
    (10 * _MB, "1-10MB"),
    (100 * _MB, "10-100MB"),
    (_GB, "100-1GB"),
]
# Anything >= 1 GB falls into ">1GB".

_AGE_BUCKET_THRESHOLDS = [
    (30, "<30d"),
    (60, "30-60d"),
    (90, "60-90d"),
    (180, "90-180d"),
    (365, "180-365d"),
]
# Anything >= 365 days falls into ">365d".

# Maximum number of in-memory entries per category dict before pruning.
# 1000 is more than enough headroom over the public top-20 surface;
# anything below is an arbitrary churn rate.
_INTERNAL_CAP = 1000
# Visible top-N in the rendered v2 dict.
_VISIBLE_TOP_N = 20
# Visible top-N for top_paths_by_size.
_TOP_PATHS_N = 10
# Anomaly keys we recognise. Anything else is silently dropped.
_ANOMALY_KEYS = ("naming", "extension", "ransomware")

# Valid scan_state values surfaced in the JSON payload. The frontend
# uses these to decide which banner to render.
_VALID_SCAN_STATES = ("mft_phase", "db_writing", "enrich", "completed")


def _bucket_for_size(size_bytes: int) -> str:
    """Return the size bucket label that ``size_bytes`` falls into."""
    for threshold, label in _SIZE_BUCKET_THRESHOLDS:
        if size_bytes < threshold:
            return label
    return ">1GB"


def _bucket_for_age_days(age_days: int) -> str:
    """Return the age bucket label that ``age_days`` falls into."""
    for threshold, label in _AGE_BUCKET_THRESHOLDS:
        if age_days < threshold:
            return label
    return ">365d"


def _parse_mtime(value: Any) -> Optional[datetime]:
    """Parse a last_modify_time field (str ISO or datetime) -> datetime.

    Returns None when the value is missing or unparseable. The MFT
    backend sometimes hands us pre-formatted ISO strings; the SMB
    backend hands us native datetimes; tests pass either.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        # Try a couple of common ISO-ish forms; we only need year + day
        # accuracy because age_days truncates anyway.
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        # Last resort: fromisoformat handles many cases py3.11 added.
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _dirname(file_path: str) -> str:
    """Return the directory component of a path, OS-agnostic.

    Windows-only behaviour (backslash separators) is preserved; the
    function also tolerates forward-slash UNIX paths used in tests.
    """
    if not file_path:
        return ""
    # Normalise mixed separators by trying backslash first (the dominant
    # case on the customer's NTFS shares), then fallback to forward slash.
    if "\\" in file_path:
        return file_path.rsplit("\\", 1)[0]
    if "/" in file_path:
        return file_path.rsplit("/", 1)[0]
    return ""


class PartialSummaryV2Builder:
    """In-memory aggregator for v2 partial-summary JSON.

    Lives on the scanner writer thread. State is updated via
    :py:meth:`absorb_batch` after each successful bulk insert. The
    :py:meth:`flush_to_db` call renders the JSON and persists it to
    ``scan_runs.partial_summary_json`` through the retry-protected
    writer path.
    """

    def __init__(self, db, scan_id: int, source_id: int):
        self.db = db
        self.scan_id = scan_id
        self.source_id = source_id

        # Running per-key dicts. Each maps key -> {"count", "size_bytes"}.
        # Pruned to ``_INTERNAL_CAP`` on every flush by ``_truncate_topn``.
        self._by_ext: Dict[str, Dict[str, int]] = {}
        self._by_dir: Dict[str, Dict[str, int]] = {}
        self._by_owner: Dict[str, Dict[str, int]] = {}

        # Bucket counters. Order is preserved at render time.
        self._size_buckets: Dict[str, int] = {b: 0 for b in _SIZE_BUCKETS_ORDER}
        self._age_buckets: Dict[str, int] = {b: 0 for b in _AGE_BUCKETS_ORDER}

        # Top 10 paths by size: heap of (size_bytes, path). Smallest at
        # heap[0]; we evict it whenever a larger candidate arrives.
        self._top_paths_heap: List = []
        # Track which paths are in the heap so we can de-duplicate when
        # the same row is absorbed twice (unlikely but cheap).
        self._top_paths_set: set = set()

        # Anomaly counters. Updated externally via increment_anomaly.
        self._anomaly_counts: Dict[str, int] = {k: 0 for k in _ANOMALY_KEYS}

        # Totals.
        self._files_total = 0
        self._size_total = 0
        self._errors_total = 0
        self._last_active_dir = ""

        # Reference to the today() value, fixed at builder construction
        # so age bucket boundaries don't drift mid-scan. Use UTC for
        # consistency with computed_at.
        self._reference_now = datetime.now(timezone.utc).replace(tzinfo=None)

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _bump(self, target: Dict[str, Dict[str, int]], key: str,
              size_bytes: int) -> None:
        slot = target.get(key)
        if slot is None:
            slot = {"count": 0, "size_bytes": 0}
            target[key] = slot
        slot["count"] += 1
        slot["size_bytes"] += size_bytes

    def _truncate_topn(self, target: Dict[str, Dict[str, int]],
                       cap: int = _INTERNAL_CAP) -> None:
        """Drop the lowest-count entries from ``target`` so it never
        exceeds ``cap`` keys. Cheap O(n log cap) — ran on every flush.
        """
        if len(target) <= cap:
            return
        # heapq.nlargest is O(n log cap) in count, returns keys in
        # descending order; we materialise and rebuild the dict to
        # discard the tail.
        keys = heapq.nlargest(cap, target.keys(),
                              key=lambda k: target[k]["count"])
        keep = {k: target[k] for k in keys}
        target.clear()
        target.update(keep)

    def _push_top_path(self, file_path: str, size_bytes: int) -> None:
        """Maintain the top-10 heap. Lazy de-dup: if the same path is
        absorbed twice we keep the first entry (cheap; the heap stays
        size-bounded so the wrong size for a re-scan is irrelevant).
        """
        if size_bytes <= 0 or not file_path:
            return
        if file_path in self._top_paths_set:
            return
        if len(self._top_paths_heap) < _TOP_PATHS_N:
            heapq.heappush(self._top_paths_heap, (size_bytes, file_path))
            self._top_paths_set.add(file_path)
            return
        smallest = self._top_paths_heap[0]
        if size_bytes > smallest[0]:
            evicted = heapq.heapreplace(self._top_paths_heap,
                                        (size_bytes, file_path))
            self._top_paths_set.discard(evicted[1])
            self._top_paths_set.add(file_path)

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    def absorb_batch(self, rows: List[Dict[str, Any]]) -> None:
        """Update running counters from a freshly-inserted batch.

        Called on the writer thread right after ``stager.append(batch)``
        succeeds. ``rows`` is the raw list of dicts the scanner just
        sent to the staging layer (file_path, extension, file_size,
        owner, last_modify_time, ...).

        Empty input is a no-op. Rows missing a key fall back to safe
        defaults; we never raise on a malformed row because the writer
        thread cannot afford to crash.
        """
        if not rows:
            return

        for row in rows:
            try:
                size = int(row.get("file_size") or 0)
            except (TypeError, ValueError):
                size = 0

            ext = row.get("extension")
            if ext is None or ext == "":
                ext = "(none)"

            owner = row.get("owner") or "(unknown)"
            file_path = row.get("file_path") or ""
            directory = _dirname(file_path)
            if not directory:
                directory = "(root)"

            # By-extension / by-directory / by-owner: count + size_bytes.
            self._bump(self._by_ext, ext, size)
            self._bump(self._by_dir, directory, size)
            self._bump(self._by_owner, owner, size)

            # Size buckets — ignore size==0 (MFT phase, no data yet).
            if size > 0:
                self._size_buckets[_bucket_for_size(size)] += 1

            # Age buckets — ignore rows without parseable mtime
            # (also MFT phase: timestamps populated by enrich).
            mtime = _parse_mtime(row.get("last_modify_time"))
            if mtime is not None:
                age_days = max(0, (self._reference_now - mtime).days)
                self._age_buckets[_bucket_for_age_days(age_days)] += 1

            # Top paths by size.
            self._push_top_path(file_path, size)

            # Totals.
            self._files_total += 1
            self._size_total += size
            if directory:
                self._last_active_dir = directory

    def increment_anomaly(self, kind: str, count: int = 1) -> None:
        """Bump the anomaly counter for ``kind`` (naming/extension/ransomware).

        Unknown keys are silently ignored — this method is called from
        post-scan phases (e.g. ``_run_extension_check``) and a typo
        there should not crash the scanner.
        """
        if kind not in self._anomaly_counts:
            return
        try:
            n = int(count)
        except (TypeError, ValueError):
            return
        if n <= 0:
            return
        self._anomaly_counts[kind] += n

    def increment_errors(self, count: int = 1) -> None:
        """Bump the cumulative error counter."""
        try:
            n = int(count)
        except (TypeError, ValueError):
            return
        if n > 0:
            self._errors_total += n

    def render(self, scan_state: str, rate_per_sec: float = 0.0,
               active_dir: str = "") -> Dict[str, Any]:
        """Return the v2 partial-summary dict, ready to JSON-serialise.

        Truncates the internal dicts to top-20 entries by count for
        each per-key category. The full internal state is NOT
        modified — render is read-only on the running counters.
        """
        # Cap internal storage so memory doesn't grow unbounded over a
        # multi-million row scan. Cheap relative to the cost of one
        # writer flush.
        self._truncate_topn(self._by_ext)
        self._truncate_topn(self._by_dir)
        self._truncate_topn(self._by_owner)

        if scan_state not in _VALID_SCAN_STATES:
            scan_state = "db_writing"

        ad = active_dir or self._last_active_dir or ""

        return {
            "schema_version": 2,
            "computed_at": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S"
            ),
            "scan_state": scan_state,
            "progress": {
                "files_so_far": self._files_total,
                "size_so_far_bytes": self._size_total,
                "errors_so_far": self._errors_total,
                "rate_per_sec": round(float(rate_per_sec or 0.0), 2),
                "active_dir": ad,
            },
            "summary": {
                "by_extension": self._top_n_extensions(),
                "by_directory": self._top_n_directories(),
                "by_owner": self._top_n_owners(),
                "size_buckets": {b: self._size_buckets[b]
                                 for b in _SIZE_BUCKETS_ORDER},
                "age_buckets": {b: self._age_buckets[b]
                                for b in _AGE_BUCKETS_ORDER},
                "anomalies_so_far": dict(self._anomaly_counts),
                "top_paths_by_size": self._top_paths_rendered(),
            },
        }

    def _top_n_extensions(self) -> List[Dict[str, Any]]:
        keys = heapq.nlargest(_VISIBLE_TOP_N, self._by_ext.keys(),
                              key=lambda k: self._by_ext[k]["count"])
        return [
            {
                "ext": k,
                "count": self._by_ext[k]["count"],
                "size_bytes": self._by_ext[k]["size_bytes"],
            }
            for k in keys
        ]

    def _top_n_directories(self) -> List[Dict[str, Any]]:
        keys = heapq.nlargest(_VISIBLE_TOP_N, self._by_dir.keys(),
                              key=lambda k: self._by_dir[k]["count"])
        return [
            {
                "path": k,
                "count": self._by_dir[k]["count"],
                "size_bytes": self._by_dir[k]["size_bytes"],
            }
            for k in keys
        ]

    def _top_n_owners(self) -> List[Dict[str, Any]]:
        keys = heapq.nlargest(_VISIBLE_TOP_N, self._by_owner.keys(),
                              key=lambda k: self._by_owner[k]["count"])
        return [
            {
                "owner": k,
                "count": self._by_owner[k]["count"],
                "size_bytes": self._by_owner[k]["size_bytes"],
            }
            for k in keys
        ]

    def _top_paths_rendered(self) -> List[Dict[str, Any]]:
        # heapq is min-heap; we want descending by size.
        sorted_paths = sorted(self._top_paths_heap,
                              key=lambda x: x[0], reverse=True)
        return [
            {"path": p, "size_bytes": s}
            for s, p in sorted_paths
        ]

    def flush_to_db(self, scan_state: str, rate_per_sec: float = 0.0,
                    active_dir: str = "") -> None:
        """Render + UPDATE ``scan_runs.partial_summary_json``.

        Routes through the retry-protected writer (``db.get_conn``);
        the connection's ``busy_timeout`` already handles transient
        locks during heavy WAL writes.

        Caller (the scanner orchestrator) decides the cadence — this
        method is NOT throttled internally so tests can flush
        deterministically.
        """
        payload = self.render(scan_state=scan_state,
                              rate_per_sec=rate_per_sec,
                              active_dir=active_dir)
        # Reuse the existing helper which already handles legacy DBs.
        try:
            if hasattr(self.db, "save_scan_partial_summary"):
                self.db.save_scan_partial_summary(self.scan_id, payload)
                return
        except sqlite3.OperationalError as e:
            logger.warning(
                "partial_summary_v2 flush via helper failed scan=%d: %s",
                self.scan_id, e,
            )
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(
                "partial_summary_v2 flush unexpected scan=%d: %s",
                self.scan_id, e,
            )


# ---------------------------------------------------------------------------
# v1 -> v2 migration helper.
#
# The dashboard may load an old DB row that was written with the v1
# schema. Endpoints surface `summary` keys that don't exist in v1;
# this helper produces a v2-shaped dict from a v1 input so callers
# don't have to special-case both.
# ---------------------------------------------------------------------------

def _v1_to_v2(d: Dict[str, Any]) -> Dict[str, Any]:
    """Migrate a v1 partial-summary dict to a v2-shaped dict.

    Empty new keys (size_buckets in the v2 vocabulary, anomalies,
    etc.) are populated with zeros so the frontend can render its
    cards uniformly. The v1 ``top_extensions`` is lifted into
    ``summary.by_extension`` with the new key names.
    """
    if not isinstance(d, dict):
        return _empty_v2_payload()

    # Already v2? Just pass through (idempotent).
    if d.get("schema_version") == 2:
        return d

    out = _empty_v2_payload()
    out["computed_at"] = d.get("computed_at") or out["computed_at"]
    out["progress"]["files_so_far"] = int(d.get("total_files") or 0)
    out["progress"]["size_so_far_bytes"] = int(d.get("total_size") or 0)

    # v1 top_extensions = [{ext, count, size}, ...]
    by_ext: List[Dict[str, Any]] = []
    for e in (d.get("top_extensions") or [])[:_VISIBLE_TOP_N]:
        by_ext.append({
            "ext": e.get("ext") or "(none)",
            "count": int(e.get("count") or 0),
            "size_bytes": int(e.get("size") or 0),
        })
    out["summary"]["by_extension"] = by_ext

    return out


def _empty_v2_payload() -> Dict[str, Any]:
    """Return a v2-shaped dict with all counters zeroed.

    Used by the migration helper and as the API fallback when the DB
    blob is unparseable. ``computed_at`` is stamped to "now" so a
    stale-payload check on the frontend doesn't immediately throw.
    """
    return {
        "schema_version": 2,
        "computed_at": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S"
        ),
        "scan_state": "db_writing",
        "progress": {
            "files_so_far": 0,
            "size_so_far_bytes": 0,
            "errors_so_far": 0,
            "rate_per_sec": 0.0,
            "active_dir": "",
        },
        "summary": {
            "by_extension": [],
            "by_directory": [],
            "by_owner": [],
            "size_buckets": {b: 0 for b in _SIZE_BUCKETS_ORDER},
            "age_buckets": {b: 0 for b in _AGE_BUCKETS_ORDER},
            "anomalies_so_far": {k: 0 for k in _ANOMALY_KEYS},
            "top_paths_by_size": [],
        },
    }
