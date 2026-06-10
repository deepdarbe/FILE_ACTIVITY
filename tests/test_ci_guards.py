"""Unit tests for scripts/ci_guards.py.

The CI guards are themselves a regression surface: a future "fix" that
weakens a regex would silently let the original bug class back through.
These tests pin the checks to their intended behaviour by exercising
them on known-bad fixtures.

Only D-CHAIN is tested here — it's the check added in the 2026-05-22
migration and the one most directly tied to a recurring prod-bug
class (#200 / #201 / #202 null-deref via chained innerHTML). The other
checks (LOADERS, HTML-BUDGET, SVC-PARITY, YAML) were inherited and
are exercised indirectly via the CI run on every PR.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import scripts.ci_guards as g


@pytest.fixture
def fake_index_html(tmp_path, monkeypatch):
    """Redirect INDEX_HTML at scripts.ci_guards to a tmp file we control."""
    html = tmp_path / "index.html"
    monkeypatch.setattr(g, "INDEX_HTML", html)
    return html


def test_d_chain_passes_on_safe_helper(fake_index_html):
    fake_index_html.write_text(
        """
        <html><body><script>
        _setHtmlSafe('foo', '<div>hi</div>');
        _setHtmlSafe('bar', `<span>${x}</span>`);
        </script></body></html>
        """
    )
    assert g.check_innerhtml_direct_chain() is True


def test_d_chain_fails_on_getelementbyid_chain(fake_index_html):
    fake_index_html.write_text(
        """
        <html><body><script>
        document.getElementById('foo').innerHTML = '<div>danger</div>';
        </script></body></html>
        """
    )
    assert g.check_innerhtml_direct_chain() is False


def test_d_chain_fails_on_queryselector_chain(fake_index_html):
    fake_index_html.write_text(
        """
        <html><body><script>
        document.querySelector('.bar').innerHTML = '<div>danger</div>';
        </script></body></html>
        """
    )
    assert g.check_innerhtml_direct_chain() is False


def test_d_chain_ignores_stored_reference(fake_index_html):
    """`const el = ...; el.innerHTML = ...` is *also* a write but is not
    the null-deref pattern we're guarding against — the stored-ref form
    forces the author to either null-check or accept the risk locally.
    HTML-BUDGET catches it via the global count; D-CHAIN does not.
    """
    fake_index_html.write_text(
        """
        <html><body><script>
        const el = document.getElementById('foo');
        if (el) el.innerHTML = '<div>safe</div>';
        </script></body></html>
        """
    )
    assert g.check_innerhtml_direct_chain() is True


def test_d_chain_runs_against_real_index_html():
    """Sanity: the live index.html in master must pass D-CHAIN.

    This is the regression bait — if a future PR re-introduces the
    chained pattern, this test fails locally before CI even runs.
    """
    assert g.check_innerhtml_direct_chain() is True


# ---------------------------------------------------------------------------
# R-CACHE — direct analyzer_cache.get_or_compute outside helper
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_api_py(tmp_path, monkeypatch):
    """Redirect _API_PY at scripts.ci_guards to a tmp file we control."""
    f = tmp_path / "api.py"
    monkeypatch.setattr(g, "_API_PY", f)
    return f


def test_r_cache_passes_when_using_helper(fake_api_py):
    fake_api_py.write_text(
        "def safe_endpoint():\n"
        "    return cached_report_endpoint(db, scan_id=1, report_name='X',\n"
        "        compute_fn=lambda: {}, track_op=t, track_op_label='X',\n"
        "        attach_envelope_fn=a)\n"
    )
    assert g.check_r_cache() is True


def test_r_cache_fails_on_direct_call(fake_api_py):
    fake_api_py.write_text(
        "def bad_endpoint():\n"
        "    return analyzer_cache.get_or_compute(db, 'X', 1, lambda: {})\n"
    )
    assert g.check_r_cache() is False


def test_r_cache_allowlist_works(fake_api_py, monkeypatch):
    monkeypatch.setattr(g, "R_CACHE_ALLOWLIST", {"legit_endpoint"})
    fake_api_py.write_text(
        "def legit_endpoint():\n"
        "    return analyzer_cache.get_or_compute(db, 'X', 1, lambda: {})\n"
    )
    assert g.check_r_cache() is True


def test_r_cache_doesnt_flag_nested_defs(fake_api_py):
    """A function whose nested helper calls get_or_compute should NOT
    fail R-CACHE — only the function that contains the call in its OWN
    body. This is what makes the rule survive create_app(...) which is
    a giant outer function containing many endpoints."""
    fake_api_py.write_text(
        "def create_app():\n"
        "    def good_endpoint():\n"
        "        return cached_report_endpoint(...)  # safe\n"
        "    def bad_endpoint():\n"
        "        return analyzer_cache.get_or_compute(db, 'X', 1, lambda: {})\n"
    )
    # create_app itself should NOT be flagged. bad_endpoint should be.
    result = g.check_r_cache()
    assert result is False


def test_r_cache_runs_against_real_api_py():
    """Live api.py must pass R-CACHE with the documented allowlist."""
    assert g.check_r_cache() is True


# ---------------------------------------------------------------------------
# A-AWAIT — async def must use await
# ---------------------------------------------------------------------------


def test_a_await_passes_with_await(fake_api_py):
    fake_api_py.write_text(
        "async def good_endpoint(request):\n"
        "    body = await request.json()\n"
        "    return body\n"
    )
    assert g.check_a_await() is True


def test_a_await_fails_without_await(fake_api_py):
    fake_api_py.write_text(
        "async def bad_endpoint():\n"
        "    return {'hello': 'world'}\n"
    )
    assert g.check_a_await() is False


def test_a_await_passes_plain_def(fake_api_py):
    """Plain def (no async) is the desired shape — should always pass."""
    fake_api_py.write_text(
        "def fine_endpoint():\n"
        "    return {'hello': 'world'}\n"
    )
    assert g.check_a_await() is True


def test_a_await_handles_async_with(fake_api_py):
    fake_api_py.write_text(
        "async def good_endpoint():\n"
        "    async with thing() as t:\n"
        "        return t\n"
    )
    assert g.check_a_await() is True


def test_a_await_runs_against_real_api_py():
    """Live api.py must pass A-AWAIT after PR #215."""
    assert g.check_a_await() is True


# ---------------------------------------------------------------------------
# C-CURSOR — read endpoints get_read_cursor, writes get_cursor
# ---------------------------------------------------------------------------


def test_c_cursor_passes_get_using_read_cursor(fake_api_py):
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def my_endpoint():\n"
        "    with db.get_read_cursor() as cur:\n"
        "        return cur.fetchone()\n"
    )
    assert g.check_c_cursor() is True


def test_c_cursor_fails_get_using_writer_pool(fake_api_py):
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def my_endpoint():\n"
        "    with db.get_cursor() as cur:\n"
        "        return cur.fetchone()\n"
    )
    assert g.check_c_cursor() is False


def test_c_cursor_get_writer_allowlist_works(fake_api_py, monkeypatch):
    monkeypatch.setattr(
        g, "C_CURSOR_GET_WRITER_ALLOWLIST", {"legit_get"},
    )
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def legit_get():\n"
        "    with db.get_cursor() as cur:\n"
        "        return cur.fetchone()\n"
    )
    assert g.check_c_cursor() is True


def test_c_cursor_passes_write_using_writer_pool(fake_api_py):
    fake_api_py.write_text(
        "@app.post('/api/x')\n"
        "def my_post():\n"
        "    with db.get_cursor() as cur:\n"
        "        cur.execute('INSERT ...')\n"
    )
    assert g.check_c_cursor() is True


def test_c_cursor_fails_write_using_only_reader(fake_api_py):
    fake_api_py.write_text(
        "@app.post('/api/x')\n"
        "def bad_post():\n"
        "    with db.get_read_cursor() as cur:\n"
        "        return cur.fetchone()\n"
    )
    assert g.check_c_cursor() is False


def test_c_cursor_write_using_both_pools_passes(fake_api_py):
    """Write handler that reads (read pool) then writes (writer pool)
    is fine — the writer pool call is what makes the SQL writable."""
    fake_api_py.write_text(
        "@app.post('/api/x')\n"
        "def mixed_post():\n"
        "    with db.get_read_cursor() as cur:\n"
        "        existing = cur.fetchone()\n"
        "    with db.get_cursor() as cur:\n"
        "        cur.execute('INSERT ...')\n"
    )
    assert g.check_c_cursor() is True


def test_c_cursor_ignores_non_route_functions(fake_api_py):
    fake_api_py.write_text(
        "def helper_not_a_route():\n"
        "    with db.get_cursor() as cur:\n"
        "        return cur.fetchone()\n"
    )
    assert g.check_c_cursor() is True


def test_c_cursor_runs_against_real_api_py():
    """Live api.py must pass C-CURSOR with the documented allowlists."""
    assert g.check_c_cursor() is True


# ---------------------------------------------------------------------------
# P-PAGE — paginated endpoints use PaginationParams (Depends)
# ---------------------------------------------------------------------------


def test_p_page_passes_with_pagination_params(fake_api_py):
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def good_endpoint(p: PaginationParams = Depends()):\n"
        "    return {}\n"
    )
    assert g.check_p_page() is True


def test_p_page_fails_with_hand_rolled(fake_api_py):
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def bad_endpoint(page: int = 1, limit: int = 50):\n"
        "    return {}\n"
    )
    assert g.check_p_page() is False


def test_p_page_fails_with_page_size_drift(fake_api_py):
    """page+page_size is *closer* to canonical but still hand-rolled."""
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def drifted(page: int = 1, page_size: int = 50):\n"
        "    return {}\n"
    )
    assert g.check_p_page() is False


def test_p_page_fails_with_offset_limit(fake_api_py):
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def offset_style(offset: int = 0, limit: int = 50):\n"
        "    return {}\n"
    )
    assert g.check_p_page() is False


def test_p_page_passes_when_allowlisted(fake_api_py, monkeypatch):
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def grandfathered(page: int = 1, limit: int = 50):\n"
        "    return {}\n"
    )
    monkeypatch.setattr(g, "P_PAGE_ALLOWLIST", {"grandfathered"})
    assert g.check_p_page() is True


def test_p_page_ignores_non_paginated_handlers(fake_api_py):
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def no_pagination(source_id: int):\n"
        "    return {}\n"
    )
    assert g.check_p_page() is True


def test_p_page_ignores_non_route_functions(fake_api_py):
    """Helper functions with page/limit args are not endpoints."""
    fake_api_py.write_text(
        "def helper(page: int, limit: int):\n"
        "    return page * limit\n"
    )
    assert g.check_p_page() is True


def test_p_page_fails_partial_migration(fake_api_py):
    """Helper added but legacy page/limit args left behind — FastAPI would
    bind BOTH from the query string, recreating the Rule-2 drift. Flag.
    (2026-06-04 review repro R4.)"""
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def partial(p: PaginationParams = Depends(), page: int = 1, limit: int = 50):\n"
        "    return {}\n"
    )
    assert g.check_p_page() is False


def test_p_page_passes_annotated_pagination_params(fake_api_py):
    """PEP 593 Annotated[PaginationParams, Depends()] is the canonical
    FastAPI form — Subscript slice is a Tuple, which the original
    implementation missed (2026-06-04 review repro R3)."""
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def modern(page: Annotated[PaginationParams, Depends()]):\n"
        "    return {}\n"
    )
    assert g.check_p_page() is True


def test_p_page_passes_optional_pagination_params(fake_api_py):
    """Optional[PaginationParams] (Subscript -> Name) keeps working."""
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def opt(limit: Optional[PaginationParams] = Depends()):\n"
        "    return {}\n"
    )
    assert g.check_p_page() is True


def test_p_page_runs_against_real_api_py():
    """Live api.py must pass P-PAGE with the documented allowlists."""
    assert g.check_p_page() is True


# ---------------------------------------------------------------------------
# A-AUDIT — mutating route handlers emit an audit event (Rule 4)
# ---------------------------------------------------------------------------


def test_a_audit_passes_when_audit_called(fake_api_py):
    fake_api_py.write_text(
        "@app.post('/api/x')\n"
        "def good_post():\n"
        "    db.insert_audit_event_simple('x', {})\n"
        "    return {}\n"
    )
    assert g.check_a_audit() is True


def test_a_audit_fails_when_audit_missing(fake_api_py):
    fake_api_py.write_text(
        "@app.post('/api/x')\n"
        "def bad_post():\n"
        "    return {}\n"
    )
    assert g.check_a_audit() is False


def test_a_audit_fails_for_delete_without_audit(fake_api_py):
    fake_api_py.write_text(
        "@app.delete('/api/x')\n"
        "def bad_delete():\n"
        "    return {}\n"
    )
    assert g.check_a_audit() is False


def test_a_audit_fails_for_patch_without_audit(fake_api_py):
    fake_api_py.write_text(
        "@app.patch('/api/x')\n"
        "def bad_patch():\n"
        "    return {}\n"
    )
    assert g.check_a_audit() is False


def test_a_audit_ignores_get_handlers(fake_api_py):
    """GET handlers are not mutating; no audit expected."""
    fake_api_py.write_text(
        "@app.get('/api/x')\n"
        "def read_only():\n"
        "    return {}\n"
    )
    assert g.check_a_audit() is True


def test_a_audit_passes_when_allowlisted(fake_api_py, monkeypatch):
    fake_api_py.write_text(
        "@app.post('/api/x')\n"
        "def grandfathered():\n"
        "    return {}\n"
    )
    monkeypatch.setattr(g, "A_AUDIT_ALLOWLIST", {"grandfathered"})
    assert g.check_a_audit() is True


def test_a_audit_ignores_non_route_functions(fake_api_py):
    """Helper functions that happen to lack an audit call don't count."""
    fake_api_py.write_text(
        "def helper():\n"
        "    return 1\n"
    )
    assert g.check_a_audit() is True


def test_a_audit_fails_audit_only_in_dead_nested_def(fake_api_py):
    """An audit call inside a nested helper proves nothing — the helper
    may never be invoked. Own-body semantics match A-AWAIT/C-CURSOR.
    (2026-06-04 review repro R2.)"""
    fake_api_py.write_text(
        "@app.post('/api/x')\n"
        "def evil():\n"
        "    def _unused():\n"
        "        db.insert_audit_event_simple('x', {})\n"
        "    return {}\n"
    )
    assert g.check_a_audit() is False


def test_a_audit_passes_audit_inside_branch(fake_api_py):
    """Branches (try/if/with) are part of the own body — they count."""
    fake_api_py.write_text(
        "@app.post('/api/x')\n"
        "def ok():\n"
        "    try:\n"
        "        db.insert_audit_event_simple('x', {})\n"
        "    except Exception:\n"
        "        pass\n"
        "    return {}\n"
    )
    assert g.check_a_audit() is True


def test_a_audit_runs_against_real_api_py():
    """Live api.py must pass A-AUDIT with the documented allowlists."""
    assert g.check_a_audit() is True


# ---------------------------------------------------------------------------
# S-SHAPE — no raw summary_json access in api.py (Rule 3)
# ---------------------------------------------------------------------------


def test_s_shape_passes_on_canonical_access(fake_api_py):
    """Reading via db.get_scan_summary then indexing is fine — that path
    already routed through normalize_summary."""
    fake_api_py.write_text(
        "def read_summary():\n"
        "    summary = db.get_scan_summary(scan_id)\n"
        "    return summary.get('age_buckets')\n"
    )
    assert g.check_s_shape() is True


def test_s_shape_fails_on_row_subscript(fake_api_py):
    fake_api_py.write_text(
        "def bad_read(row):\n"
        "    return row['summary_json']\n"
    )
    assert g.check_s_shape() is False


def test_s_shape_fails_on_row_get(fake_api_py):
    fake_api_py.write_text(
        "def bad_read(row):\n"
        "    return row.get('summary_json')\n"
    )
    assert g.check_s_shape() is False


def test_s_shape_fails_on_json_loads(fake_api_py):
    fake_api_py.write_text(
        "import json\n"
        "def bad_read(row):\n"
        "    return json.loads(row['summary_json'])\n"
    )
    assert g.check_s_shape() is False


def test_s_shape_fails_on_partial_summary_json(fake_api_py):
    fake_api_py.write_text(
        "import json\n"
        "def bad_read(row):\n"
        "    return json.loads(row['partial_summary_json'])\n"
    )
    assert g.check_s_shape() is False


def test_s_shape_respects_noqa_marker(fake_api_py):
    """The per-line override marker exempts a documented exception."""
    fake_api_py.write_text(
        "def needed_raw(row):\n"
        "    return row['summary_json']  # noqa: S-SHAPE — backup tooling needs the raw blob\n"
    )
    assert g.check_s_shape() is True


def test_s_shape_ignores_comment_lines(fake_api_py):
    """Commented-out example code shouldn't trip the guard."""
    fake_api_py.write_text(
        "# Old code: summary = json.loads(row['summary_json'])\n"
        "# Use db.get_scan_summary(scan_id) instead.\n"
    )
    assert g.check_s_shape() is True


def test_s_shape_fails_on_partial_get(fake_api_py):
    """Regression for the on-ship bypass: row.get('partial_summary_json')
    was not covered by the original pattern set even though api.py used
    exactly this shape at 2998/3065. (2026-06-04 review repro R1.)"""
    fake_api_py.write_text(
        "def bad_read(row):\n"
        "    return row.get('partial_summary_json')\n"
    )
    assert g.check_s_shape() is False


def test_s_shape_noqa_case_insensitive(fake_api_py):
    """flake8/ruff-style lower-case noqa works too."""
    fake_api_py.write_text(
        "def needed_raw(row):\n"
        "    return row['summary_json']  # noqa: s-shape - documented\n"
    )
    assert g.check_s_shape() is True


def test_s_shape_noqa_in_string_does_not_silence(fake_api_py):
    """A noqa marker inside a string literal (no # comment) must NOT
    exempt the line."""
    fake_api_py.write_text(
        "def sneaky(row):\n"
        "    return str(row['summary_json']) + 'noqa: S-SHAPE'\n"
    )
    assert g.check_s_shape() is False


def test_s_shape_runs_against_real_api_py():
    """Live api.py must pass S-SHAPE — R-3/R-4 cleanup confirmed."""
    assert g.check_s_shape() is True
