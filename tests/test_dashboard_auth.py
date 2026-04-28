"""Tests for issue #158 C-1: bearer-token dashboard auth.

Coverage:
  * Localhost (127.0.0.1) without token -> 200 (allow_unauth_localhost).
  * Remote without token -> 401.
  * Remote with correct bearer token -> 200.
  * Remote with wrong bearer token -> 401.
  * enabled=false -> all calls 200 (explicit opt-out, backwards compat).
  * Static files always accessible (whitelist), even unauth + remote.
  * allow_unauth_localhost=false also gates localhost.
  * Empty token + empty Authorization header still rejected (anti-trick).
"""

from __future__ import annotations

import os
from typing import Optional

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from src.security.dashboard_auth import DashboardAuth


def _build_app(
    config: dict,
    *,
    force_client_host: Optional[str] = None,
) -> FastAPI:
    """Build a minimal FastAPI app that mirrors create_app's middleware
    wiring without dragging in the entire dashboard.

    Note on middleware order: Starlette runs ``@app.middleware`` handlers
    in LIFO order (the LAST decorated runs OUTERMOST / first). To make
    the client-host override visible to the auth check we must register
    the auth middleware FIRST and the override SECOND.
    """
    app = FastAPI()
    app.state.dashboard_auth = DashboardAuth(config)

    @app.middleware("http")
    async def auth_mw(request: Request, call_next):
        path = request.url.path or ""
        if path.startswith("/static/") or path == "/favicon.ico":
            return await call_next(request)
        gate = app.state.dashboard_auth
        if gate.check(request):
            return await call_next(request)
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)

    if force_client_host is not None:
        @app.middleware("http")
        async def _override_client(request: Request, call_next):
            request.scope["client"] = (force_client_host, 0)
            return await call_next(request)

    @app.get("/api/ping")
    async def ping():
        return {"ok": True}

    @app.get("/static/app.js")
    async def static_asset():
        return {"asset": "ok"}

    return app


# ---------------------------------------------------------------------------
# Defaults: enabled=true, allow_unauth_localhost=true.
# ---------------------------------------------------------------------------


def _default_cfg() -> dict:
    return {"dashboard": {"auth": {"enabled": True}}}


def test_localhost_without_token_passes(monkeypatch):
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app(_default_cfg(), force_client_host="127.0.0.1")
    client = TestClient(app)
    resp = client.get("/api/ping")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_remote_without_token_blocked(monkeypatch):
    monkeypatch.setenv("FILEACTIVITY_DASHBOARD_TOKEN", "s3cret")
    app = _build_app(_default_cfg(), force_client_host="10.0.0.5")
    client = TestClient(app)
    resp = client.get("/api/ping")
    assert resp.status_code == 401
    assert resp.json() == {"detail": "Unauthorized"}


def test_remote_with_correct_bearer_passes(monkeypatch):
    monkeypatch.setenv("FILEACTIVITY_DASHBOARD_TOKEN", "s3cret")
    app = _build_app(_default_cfg(), force_client_host="10.0.0.5")
    client = TestClient(app)
    resp = client.get(
        "/api/ping", headers={"Authorization": "Bearer s3cret"}
    )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_remote_with_wrong_bearer_blocked(monkeypatch):
    monkeypatch.setenv("FILEACTIVITY_DASHBOARD_TOKEN", "s3cret")
    app = _build_app(_default_cfg(), force_client_host="10.0.0.5")
    client = TestClient(app)
    resp = client.get(
        "/api/ping", headers={"Authorization": "Bearer wrong"}
    )
    assert resp.status_code == 401


def test_remote_with_empty_bearer_blocked_when_token_unset(monkeypatch):
    """Anti-trick: server has no token configured; an attacker sending
    `Authorization: Bearer ` should NOT match the empty string."""
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app(_default_cfg(), force_client_host="10.0.0.5")
    client = TestClient(app)
    resp = client.get(
        "/api/ping", headers={"Authorization": "Bearer "}
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# enabled=false -> behaves exactly like the pre-1.9 unauth dashboard.
# ---------------------------------------------------------------------------


def test_disabled_lets_all_calls_through(monkeypatch):
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    cfg = {"dashboard": {"auth": {"enabled": False}}}
    app = _build_app(cfg, force_client_host="10.0.0.5")
    client = TestClient(app)
    assert client.get("/api/ping").status_code == 200
    assert client.get(
        "/api/ping", headers={"Authorization": "Bearer anything"}
    ).status_code == 200


# ---------------------------------------------------------------------------
# Static files / favicon are never gated.
# ---------------------------------------------------------------------------


def test_static_files_pass_unauth_remote(monkeypatch):
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    app = _build_app(_default_cfg(), force_client_host="10.0.0.5")
    client = TestClient(app)
    resp = client.get("/static/app.js")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# allow_unauth_localhost=false also gates localhost.
# ---------------------------------------------------------------------------


def test_localhost_gated_when_bypass_disabled(monkeypatch):
    monkeypatch.setenv("FILEACTIVITY_DASHBOARD_TOKEN", "s3cret")
    cfg = {
        "dashboard": {
            "auth": {
                "enabled": True,
                "allow_unauth_localhost": False,
            }
        }
    }
    app = _build_app(cfg, force_client_host="127.0.0.1")
    client = TestClient(app)
    # No header -> 401 even from localhost.
    assert client.get("/api/ping").status_code == 401
    # Correct bearer still works.
    resp = client.get(
        "/api/ping", headers={"Authorization": "Bearer s3cret"}
    )
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Custom token_env name is honoured.
# ---------------------------------------------------------------------------


def test_custom_token_env_name(monkeypatch):
    monkeypatch.delenv("FILEACTIVITY_DASHBOARD_TOKEN", raising=False)
    monkeypatch.setenv("MY_DASH_TOKEN", "custom-tok")
    cfg = {
        "dashboard": {
            "auth": {
                "enabled": True,
                "token_env": "MY_DASH_TOKEN",
            }
        }
    }
    app = _build_app(cfg, force_client_host="10.0.0.5")
    client = TestClient(app)
    resp = client.get(
        "/api/ping", headers={"Authorization": "Bearer custom-tok"}
    )
    assert resp.status_code == 200
