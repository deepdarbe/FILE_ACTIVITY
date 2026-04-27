"""Smoke-import tests for the Streamlit playground (issue #75).

Streamlit + plotly + pandas are *optional* dependencies (see
``requirements-playground.txt``). On CI runners that don't install
them this entire test file is skipped via ``importorskip`` rather
than failing — the playground is opt-in and shouldn't gate merges
of unrelated changes.

When the deps are present we still don't want top-level Streamlit
calls firing during ``import``; each page guards its body with
``if STREAMLIT_RUN_PLAYGROUND_TESTS_ONLY != "1":``. We set that env
var here so the modules import cleanly without rendering anything.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
pytest.importorskip("plotly")
pytest.importorskip("pandas")


_PAGES = (
    "src.playground.pages.01_cold_data",
    "src.playground.pages.02_duplicate_walker",
    "src.playground.pages.03_audit_timeline",
    "src.playground.pages.04_retention_what_if",
    "src.playground.pages.05_pii_pivot",
)


@pytest.fixture(autouse=True)
def _silence_streamlit_render(monkeypatch):
    """Prevent the top-level ``_render()`` call inside each page
    module from firing at import time."""
    monkeypatch.setenv("STREAMLIT_RUN_PLAYGROUND_TESTS_ONLY", "1")
    # Make sure the env var is visible to importlib's per-module exec.
    yield


def test_package_marker_imports() -> None:
    """``src.playground`` and helpers import without side-effects."""
    importlib.import_module("src.playground")
    importlib.import_module("src.playground.auth")
    importlib.import_module("src.playground.data_access")


def test_app_imports() -> None:
    """The entry-point module imports without rendering."""
    # ``import_module`` re-uses the cache, so force a fresh import to
    # actually run the module body under the env-var guard.
    name = "src.playground.app"
    if name in list(__import__("sys").modules):
        del __import__("sys").modules[name]
    importlib.import_module(name)


@pytest.mark.parametrize("module_name", _PAGES)
def test_page_imports(module_name: str) -> None:
    """Every page module under ``src/playground/pages/`` parses and
    imports cleanly. Top-level rendering is suppressed by the
    env-var fixture above."""
    sys = __import__("sys")
    if module_name in sys.modules:
        del sys.modules[module_name]
    mod = importlib.import_module(module_name)
    # Each page exposes an internal ``_render`` callable for
    # Streamlit to invoke when the script is actually run.
    assert callable(getattr(mod, "_render", None)), (
        f"{module_name} should expose a _render() function"
    )


def test_data_access_assert_select_only_rejects_writes() -> None:
    """The assert_select_only guard is a key part of the read-only
    contract and must reject every write verb, even nested."""
    from src.playground.data_access import assert_select_only

    # Allowed
    assert_select_only("SELECT 1")
    assert_select_only("WITH cte AS (SELECT 1) SELECT * FROM cte")
    assert_select_only("PRAGMA table_info('scanned_files')")

    # Rejected
    for sql in (
        "INSERT INTO sources VALUES (1)",
        "UPDATE sources SET name = 'x'",
        "DELETE FROM sources",
        "DROP TABLE sources",
        "ALTER TABLE sources ADD COLUMN bad TEXT",
        "SELECT * FROM scanned_files; DELETE FROM sources",
    ):
        with pytest.raises(ValueError):
            assert_select_only(sql)


def test_pages_directory_layout() -> None:
    """Sanity check: pages live where Streamlit's multi-page router
    expects them so ``streamlit run app.py`` discovers them."""
    here = Path(__file__).resolve().parent.parent
    pages_dir = here / "src" / "playground" / "pages"
    assert pages_dir.is_dir()
    expected = {
        "01_cold_data.py",
        "02_duplicate_walker.py",
        "03_audit_timeline.py",
        "04_retention_what_if.py",
        "05_pii_pivot.py",
    }
    found = {p.name for p in pages_dir.glob("*.py")}
    missing = expected - found
    assert not missing, f"Eksik playground sayfasi: {missing}"


def test_env_token_helper() -> None:
    """``env_token`` returns the env var, stripped, or empty string."""
    from src.playground.data_access import env_token

    os.environ.pop("FILEACTIVITY_PLAYGROUND_TOKEN", None)
    assert env_token() == ""
    os.environ["FILEACTIVITY_PLAYGROUND_TOKEN"] = "  abc123  "
    try:
        assert env_token() == "abc123"
    finally:
        os.environ.pop("FILEACTIVITY_PLAYGROUND_TOKEN", None)
