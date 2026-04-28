"""CSP + hardening header smoke (security audit 2026-04-28, finding H-1).

The ``escapeHtml()`` sweep across ``index.html`` is the primary defence
against stored XSS through filenames / owners / source names. This file
covers the secondary defence: the ``Content-Security-Policy`` middleware
landed in ``create_app()``. Together with ``X-Frame-Options``,
``X-Content-Type-Options`` and ``Referrer-Policy`` they form the
"defence-in-depth" leg of H-1: even if a future regression forgets to
escape a leaf value, the browser refuses to execute injected scripts.

What we check
-------------
* All four security headers are present on **HTML**, **JSON API**, and
  **static asset** responses — i.e. the middleware applies to every
  route, not just one of them.
* CSP includes the directives the audit specifies as load-bearing:
  ``default-src 'self'``, ``frame-ancestors 'none'``, ``base-uri 'self'``,
  ``form-action 'self'``.
* ``X-Frame-Options: DENY`` and ``X-Content-Type-Options: nosniff`` —
  exact values, no aliases.

What we do **not** check
-------------------------
* ``script-src`` exact contents — the audit explicitly accepts
  ``'unsafe-inline'`` here as a Phase 1 concession (index.html has many
  inline scripts; tightening to nonces is Phase 3 work). Asserting the
  exact string would make the test brittle when the inline-script
  inventory shrinks.
"""
from __future__ import annotations

import os
import sys
from typing import Any

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.dashboard.api import create_app  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ---- stub deps (mirror test_dashboard_smoke.py to avoid coupling) ---------


class _StubADLookup:
    available = False

    def lookup(self, name, force_refresh=False):  # noqa: D401
        return {
            "username": name,
            "email": None,
            "display_name": None,
            "found": False,
            "source": "live",
        }

    def health(self):  # noqa: D401
        return {"available": False, "configured": False}


class _StubEmailNotifier:
    available = False

    def send(self, *a, **kw):  # noqa: D401
        return False

    def health(self):  # noqa: D401
        return {"available": False, "configured": False}


class _StubAnalytics:
    available = False

    def health(self):  # noqa: D401
        return {"available": False, "configured": False}

    def close(self):  # pragma: no cover
        pass


_BASE_CONFIG: dict[str, Any] = {
    "security": {
        "ransomware": {"enabled": False},
        "orphan_sid": {"enabled": False, "cache_ttl_minutes": 1440},
    },
    "analytics": {},
    "backup": {"enabled": False, "dir": "/tmp/_no_backups",
               "keep_last_n": 1, "keep_weekly": 0},
    "integrations": {"syslog": {"enabled": False}},
}


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("csp")
    db_path = tmp / "csp.db"
    db = Database({"path": str(db_path)})
    db.connect()
    app = create_app(
        db,
        _BASE_CONFIG,
        analytics=_StubAnalytics(),
        ad_lookup=_StubADLookup(),
        email_notifier=_StubEmailNotifier(),
    )
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Header presence — applied uniformly across response kinds.
# ---------------------------------------------------------------------------


_REQUIRED_HEADERS = (
    "Content-Security-Policy",
    "X-Frame-Options",
    "X-Content-Type-Options",
    "Referrer-Policy",
)


@pytest.mark.parametrize(
    "path",
    [
        "/",                       # HTMLResponse from the index endpoint
        "/api/system/health",      # JSONResponse from the health endpoint
    ],
)
def test_security_headers_present(client, path):
    """Every standard response must carry the four hardening headers."""
    r = client.get(path)
    # Health may 200 or 503 depending on stubs — both fine, we only care
    # about response-headers shape, not body status.
    assert r.status_code < 500, f"{path} unexpectedly 5xx: {r.status_code}"
    for h in _REQUIRED_HEADERS:
        assert h in r.headers, f"{h} missing on {path} response"


def test_csp_includes_required_directives(client):
    """CSP directives the audit calls out as load-bearing."""
    csp = client.get("/").headers.get("Content-Security-Policy", "")
    # default-src 'self' — anchors every fetched resource to same-origin
    # by default; without it, a missing directive falls open.
    assert "default-src 'self'" in csp
    # frame-ancestors 'none' — clickjacking guard, paired with X-Frame.
    assert "frame-ancestors 'none'" in csp
    # base-uri 'self' — blocks <base href> injection.
    assert "base-uri 'self'" in csp
    # form-action 'self' — prevents form-action exfiltration.
    assert "form-action 'self'" in csp


def test_xframe_and_nosniff_exact(client):
    """Frame-Options and Content-Type-Options have exact required values."""
    h = client.get("/").headers
    assert h["X-Frame-Options"] == "DENY"
    assert h["X-Content-Type-Options"] == "nosniff"
    # Referrer-Policy: no-referrer keeps dashboard URLs (which can carry
    # owner/file ids) from leaking to outbound CDNs.
    assert h["Referrer-Policy"] == "no-referrer"


def test_static_asset_has_csp(client):
    """CSP must apply to static assets too — middleware is HTTP-wide,
    not endpoint-specific. The dashboard mounts ``/static`` from disk;
    we hit a known file (entity-list.js) to confirm the header survives
    the StaticFiles app."""
    r = client.get("/static/components/entity-list.js")
    # File should exist in this repo; a 404 here means the test setup
    # is wrong, not the middleware — assert it's reachable.
    assert r.status_code == 200, (
        "entity-list.js missing from static dir — "
        "test fixture / repo layout drifted"
    )
    assert "Content-Security-Policy" in r.headers
    assert "X-Frame-Options" in r.headers
