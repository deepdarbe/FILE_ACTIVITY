"""Tests for the /api/system/list-dir folder-browser endpoint.

Issue #82 (Bug 4) / Issue #105 — backs the in-dashboard folder picker
modal. Mirrors the test pattern used for /api/system/open-folder
(``test_dashboard_api.py``): unit-tests the pure ``list_dir_impl`` helper
AND drives the FastAPI endpoint through ``TestClient`` with a middleware
that rewrites the ASGI scope's client tuple so we can simulate a remote
caller without a real socket.

Coverage:

* Localhost client lists entries with the documented shape.
* Remote client gets HTTP 403 with an explanatory message (per PR #85's
  localhost-only lesson).
* Non-existent path -> HTTP 404.
* Entry list capped at ``LIST_DIR_MAX_ENTRIES`` (5000) -> ``truncated`` flag.
* Hidden / dotfile entries skipped by default; ``show_hidden=true`` returns
  them.
"""

from __future__ import annotations

import os
import tempfile

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from src.dashboard.api import LIST_DIR_MAX_ENTRIES, list_dir_impl


def _build_app(force_client_host: str | None = None) -> FastAPI:
    """Minimal FastAPI app exposing only /api/system/list-dir.

    The middleware below rewrites ``request.scope["client"]`` before the
    handler runs, which is what ``request.client.host`` reads. Lets us
    assert the localhost-only branch end-to-end without a real socket.
    """
    app = FastAPI()

    if force_client_host is not None:
        @app.middleware("http")
        async def _override_client(request: Request, call_next):
            request.scope["client"] = (force_client_host, 0)
            return await call_next(request)

    @app.get("/api/system/list-dir")
    async def list_dir(
        request: Request, path: str = "", show_hidden: bool = False
    ):
        client_host = request.client.host if request.client else ""
        return list_dir_impl(path, client_host, show_hidden=show_hidden)

    return app


@pytest.fixture
def tmp_folder_with_entries():
    """Temp dir with two files + two subdirs + one dotfile + one dotdir."""
    with tempfile.TemporaryDirectory() as d:
        d = os.path.realpath(d)
        os.makedirs(os.path.join(d, "alpha_dir"))
        os.makedirs(os.path.join(d, "beta_dir"))
        os.makedirs(os.path.join(d, ".hidden_dir"))
        with open(os.path.join(d, "file_a.txt"), "w") as f:
            f.write("hello")
        with open(os.path.join(d, "file_b.txt"), "w") as f:
            f.write("world!")
        with open(os.path.join(d, ".hidden_file"), "w") as f:
            f.write("x")
        yield d


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_list_dir_localhost_lists_entries(tmp_folder_with_entries):
    """Localhost call returns 200 with the documented response shape."""
    app = _build_app(force_client_host="127.0.0.1")
    client = TestClient(app)
    resp = client.get(
        "/api/system/list-dir", params={"path": tmp_folder_with_entries}
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["path"] == tmp_folder_with_entries
    assert "parent" in data
    assert isinstance(data["entries"], list)

    # Dirs sort before files (case-insensitive). Hidden entries excluded
    # by default. 4 visible entries: alpha_dir, beta_dir, file_a.txt, file_b.txt.
    names = [e["name"] for e in data["entries"]]
    assert names == ["alpha_dir", "beta_dir", "file_a.txt", "file_b.txt"]
    types = [e["type"] for e in data["entries"]]
    assert types == ["dir", "dir", "file", "file"]
    # Files carry size + mtime.
    file_a = next(e for e in data["entries"] if e["name"] == "file_a.txt")
    assert file_a["size"] == 5  # len("hello")
    assert isinstance(file_a["mtime"], (int, float))


def test_list_dir_remote_returns_403(tmp_folder_with_entries):
    """A remote client must NOT be allowed to walk the server filesystem."""
    app = _build_app(force_client_host="10.0.0.5")
    client = TestClient(app)
    resp = client.get(
        "/api/system/list-dir", params={"path": tmp_folder_with_entries}
    )
    assert resp.status_code == 403, resp.text
    body = resp.json()
    # Explanatory message for the operator (the JS surfaces this verbatim).
    assert "sunucudan" in body.get("detail", "").lower() or \
           "manuel" in body.get("detail", "").lower()


def test_list_dir_nonexistent_returns_404():
    """Non-existent path -> 404, not 500 / not a leaked stack trace."""
    missing = os.path.join(
        tempfile.gettempdir(), "file_activity_does_not_exist_82_bug4"
    )
    assert not os.path.exists(missing)

    app = _build_app(force_client_host="127.0.0.1")
    client = TestClient(app)
    resp = client.get("/api/system/list-dir", params={"path": missing})
    assert resp.status_code == 404, resp.text


def test_list_dir_caps_entries_at_5000(tmp_path):
    """A directory with more than 5000 entries is truncated.

    Use a small ``max_entries`` against the ``list_dir_impl`` helper rather
    than create 5001 files on disk — the cap parameter is what we want to
    verify, not the literal value 5000.
    """
    # Create 12 entries.
    for i in range(12):
        (tmp_path / f"file_{i:02d}.txt").write_text("x")

    out = list_dir_impl(
        str(tmp_path),
        client_host="127.0.0.1",
        show_hidden=False,
        max_entries=10,
    )
    assert len(out["entries"]) == 10
    assert out.get("truncated") is True
    assert out.get("max_entries") == 10

    # And the production cap is the documented 5000.
    assert LIST_DIR_MAX_ENTRIES == 5000


def test_list_dir_skips_hidden_by_default(tmp_folder_with_entries):
    """Dotfiles / dotdirs are excluded unless ``show_hidden=true``."""
    app = _build_app(force_client_host="127.0.0.1")
    client = TestClient(app)

    # Default: hidden entries excluded.
    resp = client.get(
        "/api/system/list-dir",
        params={"path": tmp_folder_with_entries},
    )
    names = [e["name"] for e in resp.json()["entries"]]
    assert ".hidden_dir" not in names
    assert ".hidden_file" not in names

    # Opt-in: hidden entries returned.
    resp2 = client.get(
        "/api/system/list-dir",
        params={"path": tmp_folder_with_entries, "show_hidden": "true"},
    )
    names2 = [e["name"] for e in resp2.json()["entries"]]
    assert ".hidden_dir" in names2
    assert ".hidden_file" in names2


def test_list_dir_empty_path_returns_logical_roots():
    """Empty path -> logical roots (drives on Windows, '/' on POSIX).

    Exercises the platform-conditional branch of ``list_dir_impl`` with a
    real syscall: at least one root must exist on every supported OS, so
    the response must be non-empty and ``parent`` must be ``None`` so the
    UI hides the up-one-level affordance.
    """
    out = list_dir_impl("", client_host="127.0.0.1")
    assert out["path"] == ""
    assert out["parent"] is None
    assert isinstance(out["entries"], list)
    assert len(out["entries"]) >= 1
    assert all(e["type"] == "dir" for e in out["entries"])
