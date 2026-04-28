"""In-memory tracker for active background operations (issue #125).

Issue #125 — "Su an ne oluyor" durum banner'i.

Backend module that maintains a process-local registry of currently
running operations (scans, analyses, archive runs, purges, PII sweeps,
snapshots). The dashboard frontend polls
``GET /api/system/status`` every 5 seconds and surfaces an at-a-glance
banner so users always know whether the app is busy — even on slow
loads where the page itself is still waiting on data.

Design decisions
----------------

* **In-memory only** — operations are ephemeral. Nothing about a "scan
  is in progress *right now*" deserves a DB row; on restart the
  registry resets to empty, which is exactly the desired behaviour.
* **Process-local singleton** — stored on ``app.state.operations`` in
  ``create_app``. Cross-process / cross-host visibility is out of scope.
* **Thread-safe** — scans run on threads; the tracker uses a single
  ``RLock`` to serialise all mutations and reads.
* **Defensive callsites** — the ``start``/``progress``/``finish`` API
  is designed so an outage of the tracker (None registry, attribute
  errors) MUST NOT break the wrapped operation. Callers wrap the
  tracker calls in try/except and continue on failure.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Optional


@dataclass
class OperationStatus:
    """A single in-flight operation as exposed by the registry.

    Attributes
    ----------
    type:
        One of: ``"scan"``, ``"analysis"``, ``"archive"``, ``"purge"``,
        ``"pii"``, ``"snapshot"``. New types may be added freely;
        callers never branch on this value other than for icons.
    label:
        Short Turkish-language human label, e.g. ``"Tarama: \\\\fs01\\dept"``.
    started_at:
        Unix timestamp (``time.time()``) at which ``start()`` was called.
    progress_pct:
        Optional integer 0..100. ``None`` => indeterminate.
    eta_seconds:
        Optional remaining seconds estimate. ``None`` => unknown.
    metadata:
        Free-form dict for callsite-specific context (source_id,
        scan_id, file counts, ...). Surfaced verbatim in the API.
    """

    type: str
    label: str
    started_at: float
    progress_pct: Optional[int] = None
    eta_seconds: Optional[int] = None
    metadata: dict = field(default_factory=dict)

    # Internal — assigned by the registry, not part of the public dataclass
    # contract documented above. Hidden from API by ``to_public_dict``.
    op_id: str = ""

    def to_public_dict(self) -> dict:
        """Return the dict shape the ``/api/system/status`` endpoint emits.

        Includes ``op_id`` so the frontend can deduplicate updates across
        polls. Drops the dataclass-internal field name unchanged.
        """
        d = asdict(self)
        return d


class OperationsRegistry:
    """Process-local in-memory registry for active operations.

    Singleton on ``app.state.operations``. Construction is cheap; the
    only state is a dict + lock.

    Thread-safety
    -------------

    Every mutation and read goes through ``self._lock`` (an ``RLock``).
    Returned snapshots from :meth:`list_active` are detached copies so
    the caller cannot accidentally mutate registry state from outside.

    Failure mode
    ------------

    The class is intentionally lenient:

    * :meth:`progress` and :meth:`finish` accept unknown ``op_id`` values
      silently (they just no-op). This means a callsite that lost its
      handle — or whose ``start`` race-failed — never raises a follow-up
      exception that could mask the real work.
    * Bad input types (None, wrong shapes) coerce to safe defaults
      rather than raising.
    """

    def __init__(self) -> None:
        self._ops: dict[str, OperationStatus] = {}
        self._lock = threading.RLock()

    def start(
        self,
        op_type: str,
        label: str,
        metadata: Optional[dict] = None,
    ) -> str:
        """Register a new running operation; return its ``op_id``.

        ``op_id`` is a short uuid4 hex string. Caller stores it locally
        and passes it back to :meth:`progress` / :meth:`finish`.
        """
        op_id = uuid.uuid4().hex[:12]
        op = OperationStatus(
            type=str(op_type or "unknown"),
            label=str(label or "")[:200],
            started_at=time.time(),
            progress_pct=None,
            eta_seconds=None,
            metadata=dict(metadata or {}),
            op_id=op_id,
        )
        with self._lock:
            self._ops[op_id] = op
        return op_id

    def progress(
        self,
        op_id: str,
        pct: Optional[int] = None,
        eta_seconds: Optional[int] = None,
        label: Optional[str] = None,
        processed: Optional[int] = None,
        **extra_metadata,
    ) -> None:
        """Update progress fields for an existing op. Silent no-op if
        ``op_id`` is not registered (e.g. race after :meth:`finish`).

        Issue #137 — ``processed`` (and any ``extra_metadata`` kwargs) are
        merged into ``op.metadata`` so callers can surface live counters
        (e.g. MFT records collected) without parsing them out of the
        free-form label string. ``processed`` is stored under
        ``metadata['processed']`` for the canonical "live row count"
        consumed by ``/api/scan/progress/{source_id}``.
        """
        if not op_id:
            return
        with self._lock:
            op = self._ops.get(op_id)
            if op is None:
                return
            if pct is not None:
                try:
                    op.progress_pct = max(0, min(100, int(pct)))
                except (TypeError, ValueError):
                    pass
            if eta_seconds is not None:
                try:
                    op.eta_seconds = max(0, int(eta_seconds))
                except (TypeError, ValueError):
                    pass
            if label:
                op.label = str(label)[:200]
            if processed is not None:
                try:
                    op.metadata["processed"] = max(0, int(processed))
                except (TypeError, ValueError):
                    pass
            if extra_metadata:
                # Merge any other structured fields callsites want to
                # surface. Bad keys / values aren't filtered here — the
                # registry treats metadata as opaque, free-form context.
                try:
                    op.metadata.update(extra_metadata)
                except (TypeError, AttributeError):  # pragma: no cover
                    pass

    def finish(self, op_id: str, success: bool = True) -> None:
        """Remove an op from the active registry. Silent if unknown.

        ``success`` is accepted for forward compatibility (and so call
        sites can always ``finish(op, success=False)`` without the
        signature drifting), but the registry only tracks *active*
        operations — there is no completed-history bucket.
        """
        if not op_id:
            return
        with self._lock:
            self._ops.pop(op_id, None)
        # ``success`` intentionally unused — kept in signature to keep
        # callsite uniformity and leave room to grow later.
        del success

    def list_active(self) -> list[OperationStatus]:
        """Return a list of currently running operations.

        The returned list is a fresh shallow copy; the caller may sort
        / filter / serialise without holding the lock.
        """
        with self._lock:
            # Sort oldest-first so the banner shows the longest-running
            # job at the top — matches user expectation ("what's been
            # spinning").
            return sorted(
                self._ops.values(), key=lambda o: o.started_at,
            )

    def find_active_op_by_metadata(
        self, **filters,
    ) -> Optional[OperationStatus]:
        """Issue #137 — return the first active op whose ``metadata``
        contains all key/value pairs in ``filters``.

        Used by ``/api/scan/progress/{source_id}`` to look up the
        in-flight scan op for a given ``source_id`` and pull its
        ``metadata['processed']`` counter so the Sources page card and
        DOSYA KPI can stay in sync with the ops banner during the MFT
        collection phase (when the DB row count is still 0).

        Returns ``None`` if nothing matches. Empty ``filters`` returns
        the oldest active op, matching :meth:`list_active` order.
        """
        with self._lock:
            ops_sorted = sorted(
                self._ops.values(), key=lambda o: o.started_at,
            )
        for op in ops_sorted:
            md = op.metadata or {}
            if all(md.get(k) == v for k, v in filters.items()):
                return op
        return None
