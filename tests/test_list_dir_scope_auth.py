"""Regression tests for issue #278 (M1): folder-picker scope gate.

`list_dir` / `open_folder` previously let *any* unauthenticated localhost
caller (riding the ``allow_unauth_localhost`` bypass) enumerate the whole
server filesystem (drive letters, ``C:\\Users`` …). The fix scopes a
tokenless localhost caller to the configured source roots + their parent
chain, while an *authenticated* admin (valid bearer token) keeps the full
picker.

These tests drive the SAME functions the real route handlers use
(``list_dir_impl`` / ``open_folder_impl`` + ``DashboardAuth.has_valid_token``
+ ``_normalize_source_roots``) through a minimal FastAPI app that mirrors
``create_app``'s wiring — the established pattern in ``test_dashboard_auth.py``
and ``test_list_dir_endpoint.py``.

Coverage:
  * Unauth localhost, path OUTSIDE source roots -> 403 (the hole is closed).
  * Unauth localhost, path INSIDE a source root -> 200 (picker still works).
  * Unauth localhost, path that is an ANCESTOR of a source root -> 200
    (can navigate down to the source).
  * Authenticated (valid bearer) localhost, OUT-of-scope path -> 200
    (admin keeps the full picker).
  * Empty path (logical roots) -> 200 even unauth (navigation entry point).
  * First-run (no configured sources) + unauth -> every concrete path 403.
  * open-folder mirrors the same gate (unauth out-of-scope -> 403).
  * ``has_valid_token`` ignores the localhost bypass (unit-level).
"""

from __future__ import annotations

import importlib.util
import os
import tempfile
from typing import Optional

import pytest

# fastapi may be absent in some CI images. The ``has_valid_token`` unit test
# (and the pure-helper scope tests) do NOT need fastapi, so we guard only the
# HTTP-level tests rather than skipping the whole module.
# ``src.dashboard.api`` imports fastapi at module top, so importing the scope
# helpers from it requires fastapi. ``DashboardAuth`` lives in
# ``src.security.dashboard_auth`` which has NO fastapi dependency, so the pure
# ``has_valid_token`` unit test below runs even where fastapi is absent.
HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
requires_fastapi = pytest.mark.skipif(
    not HAS_FASTAPI, reason="fastapi not installed in this environment"
)

from src.security.dashboard_auth import DashboardAuth  # noqa: E402

if HAS_FASTAPI:
    from fastapi import FastAPI, Request  # noqa: E402
    from fastapi.responses import JSONResponse  # noqa: E402
    from src.dashboard.api import (  # noqa: E402
        _normalize_source_roots,
        _path_within_source_scope,
        list_dir_impl,
        open_folder_impl,
    )


class _FakeSource:
    """Stand-in for storage.models.Source — only ``unc_path`` is read."""

    def __init__(self, unc_path: str) -> None:
        self.unc_path = unc_path


def _build_app(
    source_roots: list[str],
    cfg: dict,
    *,
    force_client_host: Optional[str] = None,
):
    """Minimal app mirroring create_app's #278 wiring for the two routes.

    Registers the auth middleware FIRST (so it runs INNERMOST relative to
    the client-host override registered SECOND — Starlette runs middleware
    LIFO, last-registered outermost), exactly like test_dashboard_auth.py.
    """
    app = FastAPI()
    app.state.dashboard_auth = DashboardAuth(cfg)

    def _picker_authed(request: Request) -> bool:
        gate = app.state.dashboard_auth
        if gate is None or not getattr(gate, "enabled", True):
            return True
        return bool(gate.has_valid_token(request))

    def _picker_allowed_roots() -> list[str]:
        return _normalize_source_roots(
            (s.unc_path or "") for s in [_FakeSource(r) for r in source_roots]
        )

    @app.middleware("http")
    async def auth_mw(request: Request, call_next):
        gate = app.state.dashboard_auth
        if gate.check(request):
            return await call_next(request)
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)

    if force_client_host is not None:
        @app.middleware("http")
        async def _override_client(request: Request, call_next):
            request.scope["client"] = (force_client_host, 0)
            return await call_next(request)

    from src.security.dashboard_auth import resolve_effective_client_host

    @app.get("/api/system/list-dir")
    def list_dir(request: Request, path: str = "", show_hidden: bool = False):
        client_host = resolve_effective_client_host(request)
        return list_dir_impl(
            path,
            client_host,
            show_hidden=show_hidden,
            authed=_picker_authed(request),
            allowed_roots=_picker_allowed_roots(),
        )

    @app.post("/api/system/open-folder")
    async def open_folder(request: Request):
        body = await request.json()
        client_host = resolve_effective_client_host(request)
        return open_folder_impl(
            body,
            client_host,
            popen=lambda *a, **k: None,  # never actually spawn explorer
            authed=_picker_authed(request),
            allowed_roots=_picker_allowed_roots(),
        )

    return app


def _client(app):
    from fastapi.testclient import TestClient

    return TestClient(app)


@pytest.fixture
def source_tree():
    """A temp 'source root' with a child dir + an out-of-scope sibling dir."""
    with tempfile.TemporaryDirectory() as base:
        base = os.path.realpath(base)
        root = os.path.join(base, "share_root")
        child = os.path.join(root, "sub")
        outside = os.path.join(base, "secret_elsewhere")
        os.makedirs(child)
        os.makedirs(outside)
        yield {"base": base, "root": root, "child": child, "outside": outside}


def _default_cfg() -> dict:
    # enabled=true, allow_unauth_localhost=true (the shipped default).
    return {"dashboard": {"auth": {"enabled": True}}}


# ---------------------------------------------------------------------------
# The core regression: unauth localhost can no longer enumerate out of scope.
# ---------------------------------------------------------------------------


@requires_fastapi
def test_unauth_localhost_out_of_scope_refused(monkeypatch, source_tree):
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app(
        [source_tree["root"]], _default_cfg(), force_client_host="127.0.0.1"
    )
    client = _client(app)
    resp = client.get(
        "/api/system/list-dir", params={"path": source_tree["outside"]}
    )
    assert resp.status_code == 403, resp.text


@requires_fastapi
def test_unauth_localhost_inside_source_allowed(monkeypatch, source_tree):
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app(
        [source_tree["root"]], _default_cfg(), force_client_host="127.0.0.1"
    )
    client = _client(app)
    # The configured root itself …
    assert (
        client.get(
            "/api/system/list-dir", params={"path": source_tree["root"]}
        ).status_code
        == 200
    )
    # … and a child inside it.
    resp = client.get(
        "/api/system/list-dir", params={"path": source_tree["child"]}
    )
    assert resp.status_code == 200, resp.text


@requires_fastapi
def test_unauth_localhost_ancestor_of_source_allowed(monkeypatch, source_tree):
    """The picker must let you navigate DOWN to a configured source, so an
    ancestor of a source root is in scope."""
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app(
        [source_tree["root"]], _default_cfg(), force_client_host="127.0.0.1"
    )
    client = _client(app)
    resp = client.get(
        "/api/system/list-dir", params={"path": source_tree["base"]}
    )
    assert resp.status_code == 200, resp.text


@requires_fastapi
def test_authenticated_localhost_out_of_scope_allowed(monkeypatch, source_tree):
    """A valid bearer token => full picker, even from localhost and even for
    a path outside the configured source roots."""
    monkeypatch.setenv("FILEACTIVITY_DASHBOARD_TOKEN", "s3cret")
    app = _build_app(
        [source_tree["root"]], _default_cfg(), force_client_host="127.0.0.1"
    )
    client = _client(app)
    resp = client.get(
        "/api/system/list-dir",
        params={"path": source_tree["outside"]},
        headers={"Authorization": "Bearer s3cret"},
    )
    assert resp.status_code == 200, resp.text


@requires_fastapi
def test_unauth_empty_path_returns_roots(monkeypatch, source_tree):
    """Logical roots stay available to the unauth picker (entry point)."""
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app(
        [source_tree["root"]], _default_cfg(), force_client_host="127.0.0.1"
    )
    client = _client(app)
    resp = client.get("/api/system/list-dir", params={"path": ""})
    assert resp.status_code == 200, resp.text
    assert resp.json()["parent"] is None
    assert isinstance(resp.json()["entries"], list)


@requires_fastapi
def test_first_run_no_sources_unauth_concrete_path_refused(
    monkeypatch, source_tree
):
    """No configured sources + unauth => every concrete path is refused
    (deny-all scope). The operator types the UNC path or sets the token."""
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app([], _default_cfg(), force_client_host="127.0.0.1")
    client = _client(app)
    resp = client.get(
        "/api/system/list-dir", params={"path": source_tree["root"]}
    )
    assert resp.status_code == 403, resp.text
    # …but an authed admin still gets it.
    monkeypatch.setenv("FILEACTIVITY_DASHBOARD_TOKEN", "tok")
    app2 = _build_app([], _default_cfg(), force_client_host="127.0.0.1")
    c2 = _client(app2)
    assert (
        c2.get(
            "/api/system/list-dir",
            params={"path": source_tree["root"]},
            headers={"Authorization": "Bearer tok"},
        ).status_code
        == 200
    )


@requires_fastapi
def test_remote_caller_blocked_by_middleware(monkeypatch, source_tree):
    """Sanity: a remote caller without a token is 401'd by the middleware
    before the route even runs (unchanged pre-#278 behaviour)."""
    monkeypatch.setenv("FILEACTIVITY_DASHBOARD_TOKEN", "s3cret")
    app = _build_app(
        [source_tree["root"]], _default_cfg(), force_client_host="10.0.0.5"
    )
    client = _client(app)
    resp = client.get(
        "/api/system/list-dir", params={"path": source_tree["root"]}
    )
    assert resp.status_code == 401, resp.text


# ---------------------------------------------------------------------------
# open-folder mirrors the same gate.
# ---------------------------------------------------------------------------


@requires_fastapi
def test_open_folder_unauth_out_of_scope_refused(monkeypatch, source_tree):
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app(
        [source_tree["root"]], _default_cfg(), force_client_host="127.0.0.1"
    )
    client = _client(app)
    resp = client.post(
        "/api/system/open-folder", json={"path": source_tree["outside"]}
    )
    assert resp.status_code == 403, resp.text


@requires_fastapi
def test_open_folder_unauth_in_scope_ok(monkeypatch, source_tree):
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app(
        [source_tree["root"]], _default_cfg(), force_client_host="127.0.0.1"
    )
    client = _client(app)
    resp = client.post(
        "/api/system/open-folder", json={"path": source_tree["child"]}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json().get("mode") == "native"


@requires_fastapi
def test_open_folder_authed_out_of_scope_ok(monkeypatch, source_tree):
    monkeypatch.setenv("FILEACTIVITY_DASHBOARD_TOKEN", "s3cret")
    app = _build_app(
        [source_tree["root"]], _default_cfg(), force_client_host="127.0.0.1"
    )
    client = _client(app)
    resp = client.post(
        "/api/system/open-folder",
        json={"path": source_tree["outside"]},
        headers={"Authorization": "Bearer s3cret"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json().get("mode") == "native"


# ---------------------------------------------------------------------------
# Unit-level: has_valid_token ignores the localhost bypass.
# ---------------------------------------------------------------------------


class _Req:
    def __init__(self, host: str, headers: dict) -> None:
        self.client = type("C", (), {"host": host})()
        self.headers = headers


def test_has_valid_token_ignores_localhost_bypass(monkeypatch):
    monkeypatch.setenv("FILEACTIVITY_DASHBOARD_TOKEN", "s3cret")
    gate = DashboardAuth({"dashboard": {"auth": {"enabled": True}}})
    # check() bypasses on localhost (no token needed) …
    assert gate.check(_Req("127.0.0.1", {})) is True
    # … but has_valid_token() demands the credential regardless of host.
    assert gate.has_valid_token(_Req("127.0.0.1", {})) is False
    assert (
        gate.has_valid_token(
            _Req("127.0.0.1", {"Authorization": "Bearer s3cret"})
        )
        is True
    )
    assert (
        gate.has_valid_token(
            _Req("127.0.0.1", {"Authorization": "Bearer wrong"})
        )
        is False
    )


# ---------------------------------------------------------------------------
# Unit-level: the scope predicate itself (no fastapi needed).
# ---------------------------------------------------------------------------


@requires_fastapi
def test_path_within_source_scope_predicate():
    sep = os.sep
    root = sep.join(["", "srv", "share"])  # /srv/share (posix) style
    roots = [root]
    # Inside the root and the root itself -> in scope.
    assert _path_within_source_scope(root, roots) is True
    assert _path_within_source_scope(root + sep + "sub", roots) is True
    # Ancestor of the root -> in scope (navigate down to it).
    assert _path_within_source_scope(sep + "srv", roots) is True
    # Unrelated sibling / system path -> out of scope.
    assert _path_within_source_scope(sep + "srv" + sep + "other", roots) is False
    assert _path_within_source_scope(sep + "etc", roots) is False
    # A path that merely shares a string prefix but not a path boundary must
    # NOT match (e.g. /srv/share-secret vs /srv/share).
    assert _path_within_source_scope(root + "-secret", roots) is False
    # Empty allow-list -> nothing is in scope (first-run deny-all).
    assert _path_within_source_scope(root, []) is False


@requires_fastapi
def test_normalize_source_roots_drops_blanks():
    out = _normalize_source_roots(["", None, "  ", os.getcwd()])
    assert out == [os.path.realpath(os.getcwd()).rstrip("\\/")]
