"""Tests for issue #144 Phase 1 — wrong-extension detection.

Linux-runnable. Uses an in-process libmagic stub so tests don't depend
on the system ``libmagic`` package being installed; one test verifies
the graceful no-op path when ``python-magic`` is unavailable.
"""

from __future__ import annotations

import os
import sys
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.analyzer.extension_check import (  # noqa: E402
    ExtensionAnomaly, ExtensionChecker,
)
from src.dashboard.api import create_app  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Test doubles
# ──────────────────────────────────────────────────────────────────────


class _FakeMagic:
    """Stand-in for ``magic.Magic(mime=True)`` returning preset answers."""

    def __init__(self, mime_for: dict[str, str]):
        self.mime_for = mime_for

    def from_file(self, path: str) -> str:
        # Allow lookups by basename or full path
        if path in self.mime_for:
            return self.mime_for[path]
        base = os.path.basename(path)
        return self.mime_for.get(base, "application/octet-stream")


def _checker_with(mime_for: dict[str, str]) -> ExtensionChecker:
    """Build an ExtensionChecker pre-wired with a fake magic backend."""
    c = ExtensionChecker()
    c._magic = _FakeMagic(mime_for)
    c._magic_unavailable = False
    return c


# ──────────────────────────────────────────────────────────────────────
# ExtensionChecker — happy path / mismatches
# ──────────────────────────────────────────────────────────────────────


def test_pdf_with_pdf_magic_no_anomaly(tmp_path):
    """A real PDF (declared .pdf, magic application/pdf) -> no anomaly."""
    p = tmp_path / "real.pdf"
    p.write_bytes(b"%PDF-1.4\n...")
    c = _checker_with({str(p): "application/pdf"})
    assert c.check_file(str(p)) is None


def test_pdf_with_zip_magic_high_severity(tmp_path):
    """A zip masquerading as .pdf -> high severity (top-tier doc)."""
    p = tmp_path / "rapor.pdf"
    p.write_bytes(b"PK\x03\x04...")
    c = _checker_with({str(p): "application/zip"})
    hit = c.check_file(str(p))
    assert isinstance(hit, ExtensionAnomaly)
    assert hit.declared_ext == "pdf"
    assert hit.detected_mime == "application/zip"
    assert hit.detected_ext == "zip"
    assert hit.severity == "high"


def test_image_with_executable_magic_critical(tmp_path):
    """A Windows PE disguised as .png -> critical severity."""
    p = tmp_path / "logo.png"
    p.write_bytes(b"MZ\x90\x00")
    c = _checker_with({str(p): "application/x-dosexec"})
    hit = c.check_file(str(p))
    assert hit is not None
    assert hit.severity == "critical"
    assert hit.detected_ext == "exe"


def test_pdf_with_executable_magic_critical(tmp_path):
    """The canonical ransomware-payload pattern -> critical."""
    p = tmp_path / "invoice.pdf"
    p.write_bytes(b"MZ\x90\x00")
    c = _checker_with({str(p): "application/x-msdownload"})
    hit = c.check_file(str(p))
    assert hit is not None
    assert hit.severity == "critical"


def test_unknown_extension_returns_none(tmp_path):
    """Extensions we don't have a baseline for are skipped."""
    p = tmp_path / "weird.xyz"
    p.write_bytes(b"data")
    c = _checker_with({str(p): "application/zip"})
    assert c.check_file(str(p)) is None


def test_no_extension_returns_none(tmp_path):
    p = tmp_path / "Makefile"
    p.write_bytes(b"all:\n\techo hi")
    c = _checker_with({str(p): "text/plain"})
    assert c.check_file(str(p)) is None


def test_docx_zip_mime_no_anomaly(tmp_path):
    """docx files report as application/zip in some libmagic builds — must
    NOT raise a finding."""
    p = tmp_path / "report.docx"
    p.write_bytes(b"PK\x03\x04...")
    c = _checker_with({str(p): "application/zip"})
    assert c.check_file(str(p)) is None


def test_libmagic_missing_returns_none(tmp_path):
    """If python-magic isn't installed, check_file returns None for
    everything (no exception, no log spam beyond the one-time warning).
    """
    p = tmp_path / "rapor.pdf"
    p.write_bytes(b"PK\x03\x04...")

    # Force the lazy import path to fail.
    real_import = __import__

    def _fake_import(name, *args, **kwargs):
        if name == "magic":
            raise ImportError("simulated python-magic missing")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=_fake_import):
        c = ExtensionChecker()

    assert c.available is False
    assert c.check_file(str(p)) is None
    # check_files also returns []
    assert c.check_files([str(p)]) == []


def test_check_files_filters_none_hits(tmp_path):
    p1 = tmp_path / "ok.pdf"
    p1.write_bytes(b"%PDF-1.4")
    p2 = tmp_path / "evil.pdf"
    p2.write_bytes(b"MZ")
    c = _checker_with({
        str(p1): "application/pdf",
        str(p2): "application/x-dosexec",
    })
    out = c.check_files([str(p1), str(p2)])
    assert len(out) == 1
    assert out[0].file_path == str(p2)
    assert out[0].severity == "critical"


def test_text_family_low_severity(tmp_path):
    """A .json detected as text/plain — low severity, not actionable."""
    p = tmp_path / "data.json"
    p.write_text("not actually json")
    c = _checker_with({str(p): "text/plain"})
    hit = c.check_file(str(p))
    # text/plain is in expected for json, so no anomaly at all.
    assert hit is None


def test_jpg_with_zip_magic_medium_severity(tmp_path):
    """A jpg whose magic is application/zip — medium (image, not top-tier doc)."""
    p = tmp_path / "img.jpg"
    p.write_bytes(b"PK\x03\x04")
    c = _checker_with({str(p): "application/zip"})
    hit = c.check_file(str(p))
    assert hit is not None
    assert hit.severity == "medium"


# ──────────────────────────────────────────────────────────────────────
# Database integration — idempotent schema + insert helpers
# ──────────────────────────────────────────────────────────────────────


def test_extension_anomalies_table_exists(tmp_path):
    db = Database({"path": str(tmp_path / "ext.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='extension_anomalies'"
        )
        assert cur.fetchone() is not None


def test_insert_and_list_extension_anomalies(tmp_path):
    db = Database({"path": str(tmp_path / "ext.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources (name, unc_path) VALUES ('s', '/x')")
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs (source_id, status) VALUES (?, 'completed')",
            (source_id,),
        )
        scan_id = cur.lastrowid

    anomalies = [
        ExtensionAnomaly(
            file_path="/share/a.pdf", declared_ext="pdf",
            detected_mime="application/zip", detected_ext="zip",
            severity="high",
        ),
        ExtensionAnomaly(
            file_path="/share/b.png", declared_ext="png",
            detected_mime="application/x-dosexec", detected_ext="exe",
            severity="critical",
        ),
    ]
    n = db.insert_extension_anomalies(scan_id, anomalies)
    assert n == 2

    rows = db.list_extension_anomalies(scan_id=scan_id, limit=10)
    assert len(rows) == 2
    # Critical ordered first by severity sort
    assert rows[0]["severity"] == "critical"
    assert rows[1]["severity"] == "high"

    # Severity filter
    only_crit = db.list_extension_anomalies(
        scan_id=scan_id, severity="critical",
    )
    assert len(only_crit) == 1
    assert only_crit[0]["file_path"] == "/share/b.png"

    assert db.count_extension_anomalies(scan_id=scan_id) == 2
    assert db.count_extension_anomalies(scan_id=scan_id, severity="high") == 1


# ──────────────────────────────────────────────────────────────────────
# API smoke tests
# ──────────────────────────────────────────────────────────────────────


_BASE_CONFIG = {
    "dashboard": {"auth": {"enabled": False}},
    "scanner": {"detect_wrong_extensions": True},
    "security": {
        "ransomware": {"enabled": False},
        "orphan_sid": {"enabled": False},
    },
    "analytics": {},
}



@pytest.fixture
def api_client(tmp_path):
    db = Database({"path": str(tmp_path / "api.db")})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources (name, unc_path) VALUES ('s1', '/x')")
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs (source_id, status) VALUES (?, 'completed')",
            (source_id,),
        )
        scan_id = cur.lastrowid

    db.insert_extension_anomalies(scan_id, [
        ExtensionAnomaly("/share/a.pdf", "pdf", "application/zip", "zip", "high"),
        ExtensionAnomaly("/share/b.png", "png", "application/x-dosexec", "exe", "critical"),
        ExtensionAnomaly("/share/c.docx", "docx", "application/x-msdownload", "exe", "critical"),
    ])

    app = create_app(
        db,
        _BASE_CONFIG,
    )
    return TestClient(app), source_id, scan_id


def test_api_list_extension_anomalies(api_client):
    client, source_id, scan_id = api_client
    r = client.get(
        f"/api/security/extension-anomalies?scan_id={scan_id}"
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 3
    assert body["by_severity"]["critical"] == 2
    assert body["by_severity"]["high"] == 1
    # Critical rows ordered first
    assert body["items"][0]["severity"] == "critical"


def test_api_list_extension_anomalies_filter(api_client):
    client, _src, scan_id = api_client
    r = client.get(
        f"/api/security/extension-anomalies?scan_id={scan_id}&severity=high"
    )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["severity"] == "high"


def test_api_list_extension_anomalies_bad_severity(api_client):
    client, _src, scan_id = api_client
    r = client.get(
        f"/api/security/extension-anomalies?scan_id={scan_id}&severity=bogus"
    )
    assert r.status_code == 400


def test_api_export_xlsx(api_client):
    client, source_id, _scan = api_client
    r = client.get(
        f"/api/security/extension-anomalies/{source_id}/export.xlsx"
    )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    # XLSX is a ZIP under the hood
    assert r.content[:2] == b"PK"


def test_api_feature_flags_includes_extension_anomalies(api_client):
    client, _src, _scan = api_client
    r = client.get("/api/security/feature-flags")
    assert r.status_code == 200
    body = r.json()
    assert "extension_anomalies" in body
    assert body["extension_anomalies"]["enabled"] is True


def test_api_source_id_resolves_to_latest_scan(api_client):
    client, source_id, _scan = api_client
    r = client.get(
        f"/api/security/extension-anomalies?source_id={source_id}"
    )
    assert r.status_code == 200
    body = r.json()
    # Should have resolved to the latest scan and returned all 3 rows.
    assert body["total"] == 3
