"""Unit tests for src/dashboard/_endpoint_helpers.py.

EPIC #225 R-1 — the helpers stand on their own; tests don't need a
running FastAPI app. ``cached_report_endpoint`` is exercised with a
fake db + a fake analyzer_cache (monkey-patched) + a no-op track_op
context manager. ``PaginationParams`` is just a value object — direct
construction.
"""
from __future__ import annotations

import contextlib

import pytest

from src.dashboard._endpoint_helpers import (
    PaginationParams,
    cached_report_endpoint,
)


# ---------------------------------------------------------------------------
# cached_report_endpoint
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _noop_track_op(op_type, label, metadata=None):
    yield


def _passthrough_envelope(envelope):
    """Mirror of dashboard.api._attach_cache_envelope behaviour."""
    results = envelope.get("results", {})
    if not isinstance(results, dict):
        return {"results": results, "cache": envelope.get("cache", {})}
    out = dict(results)
    out["cache"] = envelope.get("cache", {})
    return out


def _patch_cache(monkeypatch, captured_keys: list[tuple[str, int]],
                 result: dict):
    """Replace analyzer_cache.get_or_compute with a recorder."""
    from src.analyzer import cache as analyzer_cache

    def fake(db, name, scan_id, compute):
        captured_keys.append((name, scan_id))
        return {"results": compute() if not result else result,
                "cache": {"hit": False, "source": None, "age_seconds": 0}}

    monkeypatch.setattr(analyzer_cache, "get_or_compute", fake)


def test_cached_report_endpoint_basic(monkeypatch):
    keys = []
    _patch_cache(monkeypatch, keys, {})
    result = cached_report_endpoint(
        db=object(),
        scan_id=42,
        report_name="X",
        compute_fn=lambda: {"a": 1},
        track_op=_noop_track_op,
        track_op_label="X bench",
        attach_envelope_fn=_passthrough_envelope,
    )
    assert result == {"a": 1, "cache": {"hit": False, "source": None,
                                         "age_seconds": 0}}
    assert keys == [("X", 42)]


def test_cached_report_endpoint_with_custom_key_suffix(monkeypatch):
    keys = []
    _patch_cache(monkeypatch, keys, {})
    cached_report_endpoint(
        db=object(),
        scan_id=7,
        report_name="mit_naming_files",
        compute_fn=lambda: {"items": []},
        track_op=_noop_track_op,
        track_op_label="mit_naming_files R2",
        attach_envelope_fn=_passthrough_envelope,
        custom_key_suffix="R2",
    )
    assert keys == [("mit_naming_files:R2", 7)]


def test_cached_report_endpoint_passes_metadata(monkeypatch):
    keys = []
    _patch_cache(monkeypatch, keys, {})
    captured_metadata = []

    @contextlib.contextmanager
    def track(op_type, label, metadata=None):
        captured_metadata.append(metadata)
        yield

    cached_report_endpoint(
        db=object(),
        scan_id=1,
        report_name="X",
        compute_fn=lambda: {},
        track_op=track,
        track_op_label="X",
        track_op_metadata={"source_id": 9},
        attach_envelope_fn=_passthrough_envelope,
    )
    assert captured_metadata == [{"source_id": 9}]


# ---------------------------------------------------------------------------
# PaginationParams
# ---------------------------------------------------------------------------


def test_pagination_offset_basic():
    p = PaginationParams(page=3, page_size=50)
    assert p.offset == 100
    assert p.page == 3
    assert p.page_size == 50


def test_pagination_slice():
    p = PaginationParams(page=2, page_size=3)
    items = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert p.slice(items) == [4, 5, 6]


def test_pagination_response_shape():
    p = PaginationParams(page=1, page_size=10)
    resp = p.response(total=23, items=list(range(10)))
    assert resp == {
        "page": 1,
        "page_size": 10,
        "total": 23,
        "total_pages": 3,
        "items": list(range(10)),
    }


def test_pagination_response_total_pages_rounds_up():
    # 23 / 10 → 3 pages (last page partial)
    p = PaginationParams(page=1, page_size=10)
    assert p.response(total=23, items=[]).get("total_pages") == 3
    # 30 / 10 → 3 pages (exact)
    assert p.response(total=30, items=[]).get("total_pages") == 3
    # 0 total still emits 1 page (so the frontend shows "Sayfa 1 / 1")
    assert p.response(total=0, items=[]).get("total_pages") == 1
