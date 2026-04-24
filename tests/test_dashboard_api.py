"""Dashboard API tests (issue #82, Bug 1).

Covers the dual-behaviour `/api/system/open-folder` endpoint:

* Local client -> spawns Windows Explorer via subprocess.Popen and returns
  ``{"success": True, "mode": "native"}``.
* Remote client -> does NOT touch subprocess; returns HTTP 200 with
  ``{"success": False, "mode": "remote_client"}`` so the frontend can copy
  the path to the user's clipboard instead of opening a window on the
  (invisible) server.
* Missing path -> HTTP 404, preserving the previous behaviour.

Rather than spin up the full `create_app(...)` factory (which wants a real
Database, AnalyticsEngine, etc.), these tests exercise the module-level
`open_folder_impl` helper directly AND mount a minimal FastAPI app that
registers the exact endpoint handler to keep end-to-end coverage for the
remote-vs-local branching via the `TestClient` wire path.
"""

from __future__ import annotations

import os
import tempfile

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from src.dashboard.api import open_folder_impl


# ---------------------------------------------------------------------------
# Minimal app mirroring the real endpoint (same 3-line body as in api.py).
# This lets us drive the endpoint through TestClient so the `request.client`
# plumbing is real, and we can override the client host via a middleware to
# simulate a remote caller without needing a real TCP socket.
# ---------------------------------------------------------------------------


def _build_app(force_client_host: str | None = None) -> FastAPI:
    app = FastAPI()

    if force_client_host is not None:
        # Starlette lets us rewrite the ASGI scope's "client" tuple before the
        # request is dispatched; that is exactly what `request.client.host`
        # reads from. This avoids needing a real remote socket to test the
        # "remote client" branch.
        @app.middleware("http")
        async def _override_client(request: Request, call_next):
            request.scope["client"] = (force_client_host, 0)
            return await call_next(request)

    @app.post("/api/system/open-folder")
    async def open_folder(request: Request):
        body = await request.json()
        client_host = request.client.host if request.client else ""
        return open_folder_impl(body, client_host)

    return app


@pytest.fixture
def tmp_folder():
    with tempfile.TemporaryDirectory() as d:
        # realpath so tests see the same value the endpoint will return.
        yield os.path.realpath(d)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_open_folder_localhost_calls_subprocess(monkeypatch, tmp_folder):
    """Local client -> subprocess.Popen is invoked, response mode=native."""
    calls: list[tuple] = []

    def fake_popen(argv, shell=False):
        calls.append((tuple(argv), shell))

        class _P:  # minimal Popen stand-in
            pid = 1234

        return _P()

    import subprocess

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    # Starlette's TestClient defaults `request.client.host` to the literal
    # string "testclient", so we explicitly force 127.0.0.1 to model a user
    # browsing the dashboard from the same host as the server process.
    app = _build_app(force_client_host="127.0.0.1")
    client = TestClient(app)
    resp = client.post("/api/system/open-folder", json={"path": tmp_folder})

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["success"] is True
    assert data["mode"] == "native"
    assert data["path"] == tmp_folder

    assert len(calls) == 1, f"expected exactly one Popen call, got {calls}"
    argv, shell = calls[0]
    assert argv == ("explorer", tmp_folder)
    assert shell is False


def test_open_folder_remote_client_skips_subprocess(monkeypatch, tmp_folder):
    """Remote client -> subprocess is NOT invoked, response mode=remote_client."""
    calls: list[tuple] = []

    def fake_popen(argv, shell=False):  # pragma: no cover - should never run
        calls.append((tuple(argv), shell))

    import subprocess

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    app = _build_app(force_client_host="10.0.0.5")
    client = TestClient(app)
    resp = client.post("/api/system/open-folder", json={"path": tmp_folder})

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["success"] is False
    assert data["mode"] == "remote_client"
    assert data["path"] == tmp_folder
    assert "hint" in data and data["hint"]

    assert calls == [], (
        "subprocess.Popen MUST NOT be called for a remote client; "
        f"got {calls}"
    )


def test_open_folder_404_on_missing_path(monkeypatch):
    """Missing path -> HTTP 404, regardless of client locality."""
    def fake_popen(argv, shell=False):  # pragma: no cover - should never run
        raise AssertionError("Popen must not be called for a missing path")

    import subprocess

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    missing = os.path.join(
        tempfile.gettempdir(), "file_activity_does_not_exist_82_bug1"
    )
    # Make sure it really doesn't exist.
    assert not os.path.exists(missing)

    app = _build_app()
    client = TestClient(app)
    resp = client.post("/api/system/open-folder", json={"path": missing})

    assert resp.status_code == 404, resp.text


def test_open_folder_impl_direct_remote_client(tmp_folder):
    """Unit-level sanity check bypassing HTTP entirely."""
    called: list[tuple] = []

    def fake_popen(argv, shell=False):  # pragma: no cover
        called.append((tuple(argv), shell))

    data = open_folder_impl(
        {"path": tmp_folder}, client_host="192.168.1.50", popen=fake_popen
    )
    assert data["success"] is False
    assert data["mode"] == "remote_client"
    assert called == []


def test_open_folder_impl_rejects_empty_path():
    """Empty / missing path key -> HTTPException 400 (preserved behaviour)."""
    with pytest.raises(HTTPException) as exc:
        open_folder_impl({}, client_host="127.0.0.1")
    assert exc.value.status_code == 400
