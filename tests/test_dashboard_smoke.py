"""End-to-end smoke for every API endpoint reachable from ``index.html``.

Bug 4 of issue #82 — companion to ``test_button_audit.py``. The audit
catches client-side regressions (orphan onclick handlers, typos); this
file catches *server-side* divergence — endpoints that disappear, get
renamed, or start 500'ing on the parameter shape the frontend sends.

That latter category is exactly how the "Konuma Git" bug shipped to
production before PR #85: the frontend kept calling an old endpoint
shape after a backend refactor, the response was a 500, and only manual
QA caught it. With this test in CI, any ``index.html`` path that fails
to round-trip through ``TestClient`` against the real ``create_app(...)``
fails the build instead.

Approach
--------

1. Statically scan ``index.html`` for every distinct ``/api/...`` literal
   the frontend can hit — ``api('/x')``, ``api(`/x`)``, ``fetch('/api/x')``,
   ``fetchAndDownload('/api/x')``, etc.
2. Normalise template-string interpolations (``${...}``) and querystrings
   into a clean path with placeholder values.
3. Map each frontend path onto a real FastAPI route via the ``app.routes``
   list (matching path-parameter shapes like ``/sources/{source_id}``).
4. Drive the route through ``TestClient(create_app(...))``.

Tolerated outcomes
------------------

* 2xx — happy path.
* 4xx (400, 401, 403, 404, 409, 422, 501) — the endpoint *exists* and is
  rejecting our placeholder input on policy / validation / auth grounds.
  Still a healthy signal that the wire shape lines up.

Failures
--------

* 5xx — the endpoint blew up. Either a contract mismatch (frontend calls
  with shape backend doesn't expect) or a regression in the handler.
* No matching route — frontend references a path that doesn't exist on
  the backend.

Skips
-----

A small set of endpoints need real state to return anything useful (the
audit-chain verifier, AD lookups against a stubbed lookup, etc.). They
appear in ``_SKIP_PATHS`` with a comment explaining why.
"""

from __future__ import annotations

import os
import re
import sys
from typing import Any

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.dashboard.api import create_app  # noqa: E402
from src.storage.database import Database  # noqa: E402

INDEX_HTML = os.path.join(
    REPO_ROOT, "src", "dashboard", "static", "index.html"
)


# ---------------------------------------------------------------------------
# Stubbed dependencies — match the pattern in test_dashboard_security_pages.py
# ---------------------------------------------------------------------------


class _StubADLookup:
    available = False

    def lookup(self, name, force_refresh=False):  # noqa: D401 - stub
        return {
            "username": name,
            "email": None,
            "display_name": None,
            "found": False,
            "source": "live",
        }

    def health(self):  # noqa: D401 - stub
        return {"available": False, "configured": False}


class _StubEmailNotifier:
    available = False

    def send(self, *a, **kw):  # noqa: D401 - stub
        return False

    def health(self):  # noqa: D401 - stub
        return {"available": False, "configured": False}


_BASE_CONFIG: dict[str, Any] = {
    # Issue #158 C-1: smoke test drives endpoints via TestClient,
    # whose ``client.host`` ("testclient") isn't on the localhost
    # bypass list. Disable auth here; auth itself is covered by
    # ``tests/test_dashboard_auth.py``.
    "dashboard": {"auth": {"enabled": False}},
    "security": {
        "ransomware": {
            "enabled": True,
            "rename_velocity_threshold": 50,
            "rename_velocity_window": 60,
            "deletion_velocity_threshold": 100,
            "deletion_velocity_window": 60,
            "risky_new_extensions": ["encrypted"],
            "canary_file_names": ["_canary.txt"],
            "auto_kill_session": False,
            "notification_email": "",
        },
        "orphan_sid": {"enabled": True, "cache_ttl_minutes": 1440},
    },
    "analytics": {},
    "backup": {"enabled": False, "dir": "/tmp/_no_backups", "keep_last_n": 1,
                "keep_weekly": 0},
    "integrations": {"syslog": {"enabled": False}},
}


# ---------------------------------------------------------------------------
# Endpoint extraction
# ---------------------------------------------------------------------------


# ``api('/x')`` and ``api(`/x`)`` — the dashboard helper that prefixes the
# path with ``/api`` before calling fetch().
_API_HELPER_RE = re.compile(r"""api\(\s*['"`](/[^'"`]+?)['"`]""")
# Direct ``/api/...`` literals in fetch() / fetchAndDownload() / href etc.
_API_LITERAL_RE = re.compile(r"""['"`](/api/[^'"`\s${}]+)['"`]""")
# ``endpoint: '/api/...'`` — appears in a couple of dispatch tables.
_ENDPOINT_KEY_RE = re.compile(r"""endpoint\s*:\s*['"`](/api/[^'"`]+?)['"`]""")


def _extract_paths(html: str) -> set[str]:
    """Return the set of distinct ``/api/...`` paths the frontend can hit.

    Template-string interpolations (``${...}``) and querystrings get
    normalised away — what we want is the *route shape*, not the call
    site's exact URL.
    """
    raw: set[str] = set()
    for m in _API_HELPER_RE.finditer(html):
        raw.add("/api" + m.group(1))
    for m in _API_LITERAL_RE.finditer(html):
        raw.add(m.group(1))
    for m in _ENDPOINT_KEY_RE.finditer(html):
        raw.add(m.group(1))

    cleaned: set[str] = set()
    for p in raw:
        # Drop everything from the first ``?`` (querystring) — we'll add
        # query params back per-route as needed.
        p = p.split("?", 1)[0]
        # Replace ``${...}`` interpolations with a placeholder token that
        # preserves the segment boundary. ``api(`/users/${u}/detail`)``
        # must round-trip to ``/api/users/<x>/detail`` — collapsing the
        # interpolation to "" would yield ``/api/users//detail`` which
        # then collapses to ``/api/users/detail`` and doesn't match the
        # ``/api/users/{username}/detail`` route.
        p = re.sub(r"\$\{[^}]*\}", "__VAR__", p)
        # Collapse accidental double-slash, strip trailing slash.
        p = re.sub(r"/+", "/", p).rstrip("/")
        if p.startswith("/api"):
            cleaned.add(p)
    return cleaned


# ---------------------------------------------------------------------------
# Route matching
# ---------------------------------------------------------------------------


# Per-segment placeholder values keyed by the route's parameter name. Any
# parameter name not listed falls back to ``"1"``.
_PLACEHOLDER_VALUES = {
    "source_id": "1",
    "scan_id": "1",
    "snapshot_id": "20990101_000000",
    "policy_id": "1",
    "task_id": "1",
    "alert_id": "1",
    "anomaly_id": "1",
    "op_id": "1",
    "username": "test",
    "sid": "S-1-5-21-1",
    "name": "test",
    "policy_name": "test",
    "hold_id": "1",
    "job_id": "test",
}


def _route_matches(route_path: str, frontend_path: str) -> bool:
    """Return True if ``frontend_path`` matches the FastAPI ``route_path``
    pattern (with ``{x}`` placeholders).

    Both inputs may contain wildcards: the route uses ``{name}`` segments,
    and a frontend path may have come from a template-string call site
    where ``${expr}`` was normalised to the literal ``__VAR__`` token.
    Either is treated as "any non-slash run".
    """
    if route_path == frontend_path:
        return True
    pat = re.escape(route_path)
    pat = re.sub(r"\\\{[^}]+\\\}", r"[^/]+", pat)

    # Allow ``__VAR__`` on the frontend side to match any single segment
    # of the route. We don't replace it in the route_path regex; instead
    # we substitute it into the candidate before matching.
    candidate = frontend_path
    # If candidate has __VAR__, expand to a regex on the candidate side
    # and do a route-shape comparison the other way around.
    if "__VAR__" in candidate:
        candidate_regex = re.escape(candidate).replace("__VAR__", "[^/]+")
        # Both sides have wildcards → strip route-side params to a token
        # then check the literal-by-literal match.
        route_with_token = re.sub(r"\{[^}]+\}", "__VAR__", route_path)
        return route_with_token == candidate
    return re.fullmatch(pat, candidate) is not None


def _materialise(route_path: str) -> str:
    """Substitute placeholder values into ``{...}`` segments of a route."""

    def repl(m: re.Match[str]) -> str:
        return _PLACEHOLDER_VALUES.get(m.group(1), "1")

    return re.sub(r"\{([^}]+)\}", repl, route_path)


def _fixed_query(route_path: str) -> str:
    """Some endpoints require query params for sensible behaviour. Route
    only — keyed by the un-materialised path. Empty string means no
    query string."""
    table = {
        "/api/db/cleanup": "?keep_last=10",
        "/api/security/ransomware/alerts": "?since_minutes=1440",
        "/api/security/ransomware/alerts/acknowledge-all":
            "?by_user=test&since_minutes=1440",
        "/api/security/ransomware/alerts/export.xlsx":
            "?since_minutes=1440",
        "/api/security/acl/sprawl": "?severity_threshold=1",
        "/api/security/acl/sprawl/export.xlsx": "?severity_threshold=1",
        "/api/security/acl/trustee/{sid}/paths/export.xlsx": "?limit=10",
        "/api/archive/search": "?q=test",
        "/api/export/start": "?report_type=duplicates&source_id=1",
        "/api/users/{username}/activity": "",
        "/api/audit/events": "",
        "/api/watcher/status": "",
    }
    return table.get(route_path, "")


# Endpoints we deliberately don't drive through TestClient. Each one needs
# either real state, an external service, or has destructive side effects
# we don't want to exercise in a smoke test. The route still gets matched
# (so a path disappearing is still caught by ``test_every_frontend_path_has_a_route``)
# — we just don't execute it.
_SKIP_PATHS = {
    # POST endpoint flips a global update flag — destructive on a shared
    # filesystem if anything goes wrong.
    "/api/system/update",
    # Audit verify walks the full chain; on a freshly-initialised db it's
    # cheap, but it'll log noisy "no events" warnings — not useful here.
    "/api/audit/verify",
    # Open-folder spawns a subprocess on Windows — covered by
    # test_dashboard_api.py with proper monkeypatching.
    "/api/system/open-folder",
    # Bulk restore + restore-by-operation need a populated archive
    # operations table; covered by their own dedicated tests.
    "/api/restore/bulk",
}

# Status codes treated as "endpoint exists, just rejected our input".
_OK_STATUSES = {
    200, 201, 202, 204,
    400, 401, 403, 404, 405, 409, 415, 422, 501,
}


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def html_paths() -> list[str]:
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    return sorted(_extract_paths(html))


@pytest.fixture(scope="module")
def app_and_routes(tmp_path_factory):
    """Boot a real ``create_app`` + a tiny seeded SQLite, return the app
    and the list of registered ``APIRoute`` instances.

    Module-scoped so the smoke test pays the (~100ms) startup cost once.
    """
    tmp = tmp_path_factory.mktemp("smoke")
    db_path = tmp / "smoke.db"
    db = Database({"path": str(db_path)})
    db.connect()

    # Seed one source + one completed scan so source-keyed endpoints have
    # something to operate on. Mirrors the seed in
    # test_dashboard_security_pages.py — keep it small.
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path) VALUES ('s1', '/share')"
        )
        cur.execute(
            "INSERT INTO scan_runs (source_id, status) VALUES (1, 'completed')"
        )

    app = create_app(
        db,
        _BASE_CONFIG,
        ad_lookup=_StubADLookup(),
        email_notifier=_StubEmailNotifier(),
    )
    routes = [r for r in app.routes if isinstance(r, APIRoute)]
    return app, routes


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_extracted_paths_nonempty(html_paths):
    """Sanity: the regex extractor must find the API surface or the smoke
    test below silently passes for the wrong reason."""
    assert len(html_paths) >= 30, (
        f"only extracted {len(html_paths)} /api/ paths from index.html — "
        f"the regexes probably regressed"
    )


def test_every_frontend_path_has_a_route(html_paths, app_and_routes):
    """Every ``/api/...`` literal in the frontend must map to a registered
    FastAPI route. A frontend reference with no backend match is the kind
    of bug that broke 'Konuma Git' (PR #85)."""
    _, routes = app_and_routes

    unmatched: list[str] = []
    for fp in html_paths:
        if any(_route_matches(r.path, fp) for r in routes):
            continue
        # Some frontend literals are partial — e.g. ``/api/db/cleanup``
        # used as ``api('/db/cleanup?keep_last=' + N)`` — re-check after
        # treating any remaining segment as a possible match prefix.
        if any(r.path.startswith(fp) and r.path[len(fp):].startswith("/")
               for r in routes):
            continue
        unmatched.append(fp)

    assert not unmatched, (
        f"frontend references {len(unmatched)} /api paths with no "
        f"matching backend route: {unmatched}"
    )


def test_no_endpoint_returns_500(app_and_routes, html_paths):
    """The big one: drive every endpoint reachable from index.html and
    fail the build if any of them 500s on placeholder input.

    This doesn't validate response *content* — just that the wire path
    works and the handler doesn't blow up. Content-shape contracts
    belong in the dedicated test files for each feature.
    """
    app, routes = app_and_routes
    client = TestClient(app, raise_server_exceptions=False)

    # Build the candidate set: route patterns matched by at least one
    # frontend path (so we don't smoke-test internal-only routes), then
    # subtract the explicit skip list.
    candidate_routes: list[APIRoute] = []
    for r in routes:
        if r.path in _SKIP_PATHS:
            continue
        if not r.path.startswith("/api/"):
            continue
        if any(_route_matches(r.path, fp) for fp in html_paths):
            candidate_routes.append(r)

    assert len(candidate_routes) >= 30, (
        f"only {len(candidate_routes)} routes match frontend paths — "
        f"check the route-matching regex"
    )

    failures: list[str] = []
    json_failures: list[str] = []

    for r in candidate_routes:
        url = _materialise(r.path) + _fixed_query(r.path)
        for method in r.methods:
            if method == "HEAD":
                continue
            try:
                if method == "GET":
                    resp = client.get(url)
                elif method == "DELETE":
                    resp = client.delete(url)
                elif method == "POST":
                    resp = client.post(url, json={})
                elif method == "PUT":
                    resp = client.put(url, json={})
                else:  # pragma: no cover - other verbs not used by dashboard
                    continue
            except Exception as e:
                failures.append(f"{method} {url} raised {type(e).__name__}: {e}")
                continue

            if resp.status_code >= 500 and resp.status_code not in _OK_STATUSES:
                failures.append(
                    f"{method} {url} -> {resp.status_code} "
                    f"(body: {resp.text[:200]!r})"
                )
                continue
            if resp.status_code not in _OK_STATUSES:
                failures.append(
                    f"{method} {url} -> unexpected status {resp.status_code}"
                )
                continue

            # If the endpoint claims JSON, it must parse as JSON.
            ctype = resp.headers.get("content-type", "")
            if ctype.startswith("application/json") and resp.content:
                try:
                    resp.json()
                except ValueError as e:
                    json_failures.append(
                        f"{method} {url} -> JSON parse error: {e}"
                    )

    assert not failures, "smoke failures:\n  " + "\n  ".join(failures)
    assert not json_failures, (
        "JSON parse failures:\n  " + "\n  ".join(json_failures)
    )


def test_index_html_served(app_and_routes):
    """The bare ``GET /`` must serve the dashboard HTML. If this 500s,
    every other test in this file is meaningless."""
    app, _ = app_and_routes
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert "<html" in resp.text.lower() or "FILE ACTIVITY" in resp.text
