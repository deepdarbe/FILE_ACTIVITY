"""End-to-end integration test — corpus → scan → DB → dashboard.

Closes debt item D5 in ``docs/architecture/audit-2026-04-28.md`` (issue #194).
The local pytest suite has historically only exercised individual modules
against synthetic 10–100-file fixtures; the gap between that and the
customer's real workload (millions of files on NTFS) is the entire risk
surface. This test stitches the full pipeline together against the
deterministic synthetic corpus from ``tests/fixtures/generate_corpus``
so any regression in scanner row-count, schema, or dashboard wiring shows
up here instead of being discovered in production.

Scope: the *quick* corpus (~1 000 files, sparse, < 5 s build) — fast
enough to run on every PR. A non-quick variant could later be wired
behind an ``INTEGRATION_FULL=1`` env switch if we want the full 10 K
shape in nightly CI; for now the quick corpus has every interesting
bucket (duplicates, large/very-large, stale, hidden, temp, naming
violations, PII) so structural regressions surface either way.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.dashboard.api import create_app  # noqa: E402
from src.scanner.file_scanner import FileScanner  # noqa: E402
from src.storage.database import Database  # noqa: E402
from tests.fixtures.generate_corpus import generate_corpus  # noqa: E402
from tests.fixtures.manifest import CorpusManifest  # noqa: E402


# ---------------------------------------------------------------------------
# Stub dependencies (mirrors test_dashboard_smoke.py — keep them local so
# this file stays a self-contained reproduction of the e2e contract).
# ---------------------------------------------------------------------------


class _StubADLookup:
    available = False

    def lookup(self, name, force_refresh=False):
        return {
            "username": name,
            "email": None,
            "display_name": None,
            "found": False,
            "source": "live",
        }

    def health(self):
        return {"available": False, "configured": False}


class _StubEmailNotifier:
    available = False

    def send(self, *a, **kw):
        return False

    def health(self):
        return {"available": False, "configured": False}


_DASHBOARD_CONFIG: dict = {
    "dashboard": {"auth": {"enabled": False}},
    "security": {
        "ransomware": {"enabled": False},
        "orphan_sid": {"enabled": False, "cache_ttl_minutes": 1440},
    },
    "analytics": {},
    "backup": {
        "enabled": False,
        "dir": "/tmp/_no_backups",
        "keep_last_n": 1,
        "keep_weekly": 0,
    },
    "integrations": {"syslog": {"enabled": False}},
}


# Scanner config kept flat (no ``"scanner"`` wrapper) so the same dict
# also carries the ``"reports"`` subtree below — ``FileScanner.__init__``
# would otherwise hide it from ``ReportExporter`` and the auto-report
# would land in ``./reports`` relative to cwd, polluting the repo.


def _scanner_config(reports_dir: Path) -> dict:
    return {
        "batch_size": 500,
        # Generator emits .hidden / .system equivalents we *want* counted
        # so the row-count assertion against the manifest is exact.
        "skip_hidden": False,
        "skip_system": False,
        # The corpus generator drops one ``_owners.json`` sidecar per
        # directory for AD-owner roundtripping; the manifest does not
        # count them, so exclude them from the scan.
        "exclude_patterns": ["_owners.json"],
        "read_owner": False,
        # Redirect the post-scan auto-report away from ``./reports``.
        "reports": {"output_dir": str(reports_dir)},
    }


# ---------------------------------------------------------------------------
# Module-scoped fixture: build corpus once, scan once, share across asserts.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def scanned_corpus(tmp_path_factory):
    """Build the quick corpus, scan it, return a bundle for assertions."""
    corpus_dir = tmp_path_factory.mktemp("e2e_corpus")
    manifest: CorpusManifest = generate_corpus(corpus_dir, quick=True)

    db_dir = tmp_path_factory.mktemp("e2e_db")
    db_path = db_dir / "fa.db"
    db = Database({"path": str(db_path)})
    db.connect()

    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path) VALUES (?, ?)",
            ("e2e_source", str(corpus_dir)),
        )
        source_id = cur.lastrowid

    reports_dir = tmp_path_factory.mktemp("e2e_reports")
    scanner = FileScanner(db, _scanner_config(reports_dir))
    result = scanner.scan_source(source_id, "e2e_source", str(corpus_dir))

    # ``scan_source`` leaves the WAL with un-checkpointed frames; opening
    # a fresh ``mode=ro`` connection (what ``get_read_cursor`` does) on
    # that state surfaces as ``disk I/O error`` on Linux+SQLite when the
    # writer is still alive in another thread. Production avoids this
    # because the dashboard reads come *after* the scheduler-driven
    # checkpoint pass; in this in-process test we do it eagerly.
    with db.get_cursor() as cur:
        cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    yield {
        "manifest": manifest,
        "db": db,
        "result": result,
        "source_id": source_id,
        "corpus_dir": corpus_dir,
    }

    db.close()


def _row_val(row, key: str):
    """``Database`` rows come back as dicts; tolerate tuples for safety."""
    if isinstance(row, dict):
        return row[key]
    return row[0]


# ---------------------------------------------------------------------------
# Scanner-side assertions.
# ---------------------------------------------------------------------------


def test_scan_completes_cleanly(scanned_corpus):
    """The scanner must report ``completed`` with zero errors against
    a deterministic corpus rooted under a writable tmp dir."""
    result = scanned_corpus["result"]
    assert result["status"] == "completed", result
    assert result["errors"] == 0, (
        f"scan reported {result['errors']} errors on a deterministic "
        f"corpus — see scanner logs"
    )
    assert result["total_size"] > 0


def test_scanner_row_count_matches_manifest(scanned_corpus):
    """The scanner's ``total_files`` must equal the manifest's design-time
    file count exactly (modulo the ``_owners.json`` sidecars, excluded
    via scanner config). Drift here means either the generator regressed
    or the scanner started skipping rows we expect to see."""
    result = scanned_corpus["result"]
    manifest = scanned_corpus["manifest"]
    assert result["total_files"] == manifest.total_files, (
        f"scanner total_files={result['total_files']}, "
        f"manifest.total_files={manifest.total_files}"
    )


# ---------------------------------------------------------------------------
# DB-side assertions — the rows the scanner *says* it wrote actually
# landed in scanned_files with sane values.
# ---------------------------------------------------------------------------


def test_db_row_count_matches_manifest(scanned_corpus):
    db = scanned_corpus["db"]
    src_id = scanned_corpus["source_id"]
    manifest = scanned_corpus["manifest"]
    with db.get_read_cursor() as cur:
        row = cur.execute(
            "SELECT COUNT(*) AS n FROM scanned_files WHERE source_id = ?",
            (src_id,),
        ).fetchone()
    assert _row_val(row, "n") == manifest.total_files


def test_db_large_file_buckets(scanned_corpus):
    """Large (>=100 MB) and very-large (>=1 GB) buckets must match the
    manifest. The generator writes 101 MB sparse files for the ``large``
    bucket and 1 GB+1 byte sparse files for ``very_large``; both count
    as ``>=100 MB`` so the >=100 MB query returns the union."""
    db = scanned_corpus["db"]
    src_id = scanned_corpus["source_id"]
    manifest = scanned_corpus["manifest"]
    with db.get_read_cursor() as cur:
        ge_100mb = cur.execute(
            "SELECT COUNT(*) AS n FROM scanned_files "
            "WHERE source_id = ? AND file_size >= ?",
            (src_id, 100 * 1024 * 1024),
        ).fetchone()
        ge_1gb = cur.execute(
            "SELECT COUNT(*) AS n FROM scanned_files "
            "WHERE source_id = ? AND file_size >= ?",
            (src_id, 1024 * 1024 * 1024),
        ).fetchone()

    expected_ge_100mb = (
        manifest.expected_large_files + manifest.expected_very_large
    )
    assert _row_val(ge_100mb, "n") == expected_ge_100mb
    assert _row_val(ge_1gb, "n") == manifest.expected_very_large


def test_db_zero_byte_bucket(scanned_corpus):
    db = scanned_corpus["db"]
    src_id = scanned_corpus["source_id"]
    manifest = scanned_corpus["manifest"]
    with db.get_read_cursor() as cur:
        row = cur.execute(
            "SELECT COUNT(*) AS n FROM scanned_files "
            "WHERE source_id = ? AND file_size = 0",
            (src_id,),
        ).fetchone()
    # The empty bucket emits exactly ``expected_empty_files`` zero-byte
    # files. Naming-violation / temp-file emitters write tiny non-empty
    # text bodies, so zero-byte is a clean signal.
    assert _row_val(row, "n") == manifest.expected_empty_files


# ---------------------------------------------------------------------------
# Dashboard-side assertions — the read endpoints don't 500 against a
# real (small) populated DB. We deliberately don't over-fit to response
# shape: structural regressions in shape are caught by the dashboard
# smoke test; here we only assert "endpoint exists and returns 200 with
# *some* data referencing this source".
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dashboard_client(scanned_corpus):
    app = create_app(
        scanned_corpus["db"],
        _DASHBOARD_CONFIG,
        ad_lookup=_StubADLookup(),
        email_notifier=_StubEmailNotifier(),
    )
    return TestClient(app, raise_server_exceptions=False)


def test_dashboard_sources_lists_e2e_source(dashboard_client, scanned_corpus):
    resp = dashboard_client.get("/api/sources")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # ``/api/sources`` returns either a list or a {"sources": [...]} envelope
    # depending on version; accept either.
    sources = body if isinstance(body, list) else body.get("sources", [])
    assert any(
        s.get("id") == scanned_corpus["source_id"]
        or s.get("name") == "e2e_source"
        for s in sources
    ), f"e2e source missing from /api/sources response: {body!r}"


def test_dashboard_overview_returns_200(dashboard_client, scanned_corpus):
    """``/api/overview/{id}`` is the canonical dashboard read path. After
    a completed scan it must respond 200, regardless of whether the
    pre-computed summary has been written or the endpoint falls through
    to the partial/no-data shape — any of those is a healthy "endpoint
    works" signal."""
    src_id = scanned_corpus["source_id"]
    resp = dashboard_client.get(f"/api/overview/{src_id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert isinstance(body, dict), body
