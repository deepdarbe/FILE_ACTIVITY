"""Reusable building blocks for dashboard endpoints.

See ``docs/standards/endpoint-conventions.md`` for the rules these
helpers codify. EPIC #225 R-1 — foundation; R-2 migrates existing
endpoints to use the helpers.

The two helpers exposed:
  * ``cached_report_endpoint(...)`` — wraps the analyzer_cache lookup,
    operation tracking, and envelope unwrap into one call. Replaces
    ~15 lines of boilerplate per report endpoint. Rule 1 of the
    standard.
  * ``PaginationParams`` — FastAPI ``Depends()`` parameter object that
    standardises ``page`` / ``page_size`` across the dashboard.
    Replaces the ``(page, limit)`` / ``(page, page_size)`` /
    ``(offset, limit)`` drift documented in the 2026-05-22 audit.
    Rule 2 of the standard.

Both are intentionally lightweight — no class hierarchy, no decorator
magic. Each helper is a single function/class call so the call site
reads as plainly as the hand-rolled code it replaces.
"""
from __future__ import annotations

from typing import Any, Callable, ContextManager

from fastapi import Query


def cached_report_endpoint(
    db: Any,
    *,
    scan_id: int,
    report_name: str,
    compute_fn: Callable[[], dict],
    track_op: Callable[..., ContextManager],
    track_op_label: str,
    track_op_metadata: dict | None = None,
    attach_envelope_fn: Callable[[dict], dict],
    custom_key_suffix: str = "",
) -> dict:
    """Canonical wrapper for any report endpoint that iterates >100k rows.

    Replaces this pattern (~15 lines per endpoint, the shape of
    ``report_frequency`` / ``report_types`` / ``report_sizes`` /
    ``mit_naming_report`` / ``report_full`` after PR #224/#227/#228):

        with _track_op("analysis", f"X: {src.name}", metadata={...}):
            envelope = analyzer_cache.get_or_compute(
                db, "X", scan_id, lambda: gen.generate_X_report(src.id),
            )
            return _attach_cache_envelope(envelope)

    With this::

        return cached_report_endpoint(
            db,
            scan_id=scan_id,
            report_name="X",
            compute_fn=lambda: gen.generate_X_report(src.id),
            track_op=_track_op,
            track_op_label=f"X: {src.name}",
            track_op_metadata={"source_id": src.id},
            attach_envelope_fn=_attach_cache_envelope,
        )

    The two function-typed parameters (``track_op``, ``attach_envelope_fn``)
    are passed in rather than imported because both live as closures
    inside ``create_app(...)`` — they need access to ``app.state.operations``
    and similar. Passing them keeps the helper free of FastAPI-app
    coupling and easy to unit test.

    The ``custom_key_suffix`` is appended to the cache key with a colon
    separator. Use it when a single endpoint serves multiple cache
    slots — e.g. mit_naming_files keys by ``(scan_id, code)`` via
    ``custom_key_suffix=code``.
    """
    from src.analyzer import cache as analyzer_cache

    cache_key = report_name
    if custom_key_suffix:
        cache_key = f"{report_name}:{custom_key_suffix}"

    with track_op("analysis", track_op_label, metadata=track_op_metadata):
        envelope = analyzer_cache.get_or_compute(
            db, cache_key, scan_id, compute_fn,
        )
    return attach_envelope_fn(envelope)


class PaginationParams:
    """Canonical pagination params for FastAPI ``Depends()``.

    Use::

        @app.get("/api/whatever")
        def whatever(p: PaginationParams = Depends()):
            rows = some_query(...)
            return p.response(total=len(rows), items=p.slice(rows))

    Always emits ``page`` + ``page_size`` (not ``limit``); the response
    helper carries ``total`` + ``total_pages`` so the frontend has
    everything for pager rendering without a second round-trip.

    Caps:
      * ``page`` 1..10000  — sanity bound, above 10k page is a misuse.
      * ``page_size`` 1..500 — keeps response payloads small.

    Use ``slice(items)`` when the caller has the full result list in
    memory (cheap in-memory pagination off a cached list — see
    ``mit_naming_files``). For DB-side LIMIT/OFFSET, just use
    ``p.offset`` + ``p.page_size`` in the SQL.
    """

    def __init__(
        self,
        page: int = Query(1, ge=1, le=10000),
        page_size: int = Query(100, ge=1, le=500),
    ):
        self.page = page
        self.page_size = page_size
        self.offset = (page - 1) * page_size

    def slice(self, items: list) -> list:
        """Return the items belonging to this page from a full list."""
        return items[self.offset:self.offset + self.page_size]

    def response(self, total: int, items: list) -> dict:
        """Standard pagination envelope.

        ``items`` should already be the page slice (use ``self.slice``
        or pass the result of a LIMIT/OFFSET query directly).
        """
        return {
            "page": self.page,
            "page_size": self.page_size,
            "total": total,
            "total_pages": max(1, -(-total // self.page_size)),
            "items": items,
        }
