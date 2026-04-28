"""Sanity tests for the Docker test image (issue #91, Phase 1+3).

These tests don't exercise application code — they're a tripwire that
fails loud if the test runner image (``docker/Dockerfile.test``) drifts
out from under us. Concrete failures they catch:

* Required runtime dep missing from ``requirements.txt`` *and* the image
  (e.g. ``duckdb`` or ``pyarrow`` accidentally dropped during a refactor).
* Optional accel dep silently uninstallable on the slim base — surfaces
  as a *skip* in CI output, not a hard failure, so we still merge but
  the operator sees the regression in the PR check log.
* Python interpreter accidentally bumped off 3.11 — the rest of the
  toolchain (PyInstaller spec, deploy scripts) still pins 3.11, so a
  silent bump would split prod and CI environments.

Runs unchanged on the host-runner pytest path too. The file is named
``test_docker_smoke.py`` because the *image* is what's under test, not
because it must run inside Docker — host runs are equally valid and
catch dep drift on developer workstations.
"""

from __future__ import annotations

import importlib
import sys

import pytest


# ----------------------------------------------------------------------
# Required runtime deps — listed in requirements.txt, must always import.
# A failure here means the image (or the host venv) is missing a package
# the application core actually needs at runtime, so we hard-fail.
# ----------------------------------------------------------------------
REQUIRED_PACKAGES = [
    "sqlite3",   # stdlib — sanity check that the python build isn't broken
    "duckdb",    # analytics engine
    "pyarrow",   # parquet staging for fast bulk ingest
    "yaml",      # config loader (PyYAML)
    "click",     # CLI entrypoint
    "fastapi",   # dashboard
    "uvicorn",   # ASGI server
]


# ----------------------------------------------------------------------
# Optional packages — gated by feature flags / pytest.importorskip in the
# real test suite. We only verify they're *importable* if installed; if
# they're absent (e.g. hyperscan on a glibc that PyPI doesn't ship a
# wheel for) we record a skip so the CI log shows the gap without
# turning the build red.
# ----------------------------------------------------------------------
OPTIONAL_PACKAGES = [
    "watchdog",      # event-driven scanner (issue #14)
    "magic",         # python-magic — wrong-extension detection (issue #144)
    "imagehash",     # perceptual image hashing (issue #144 phase 2)
    "hyperscan",     # PII regex engine (issue #64) — Linux x86_64 only
    "ldap3",         # AD lookup
    "openpyxl",      # xlsx reports
    "reportlab",     # pdf reports
    "apscheduler",   # scheduling
]


@pytest.mark.parametrize("name", REQUIRED_PACKAGES)
def test_required_package_importable(name: str) -> None:
    """Required runtime deps must import cleanly."""
    importlib.import_module(name)


@pytest.mark.parametrize("name", OPTIONAL_PACKAGES)
def test_optional_package_importable_or_skipped(name: str) -> None:
    """Optional deps either import or get skipped — never error."""
    try:
        importlib.import_module(name)
    except ImportError as exc:
        pytest.skip(f"optional dep {name!r} not installed: {exc}")


def test_python_version_is_3_11() -> None:
    """Stay on 3.11 to match the production deploy + PyInstaller spec.

    Bumping the Python minor version requires a coordinated update to
    ``deploy/setup-source.ps1`` (which auto-installs Python on Windows)
    and ``file_activity.spec`` (which bakes the interpreter into the EXE
    release). Splitting CI off from those would mean prod ships against
    one runtime and the test suite validates against another — which is
    exactly the failure mode this image is supposed to *prevent*.
    """
    assert sys.version_info[:2] == (3, 11), (
        f"expected Python 3.11.x in the test image, got "
        f"{sys.version_info.major}.{sys.version_info.minor}"
    )


def test_pytest_importable() -> None:
    """The runner itself must be importable — guards against a broken
    requirements-dev.txt install layer in the Dockerfile."""
    import pytest as _pytest  # noqa: F401  -- import is the assertion
