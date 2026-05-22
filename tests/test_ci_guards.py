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
