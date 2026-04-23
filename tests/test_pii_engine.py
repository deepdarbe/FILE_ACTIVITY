"""Tests for issue #58: GDPR PII detection engine.

Linux-runnable. The engine has no platform-specific code, so every
test exercises real on-disk scanning of synthetic files in tmp_path
plus the SQLite-backed persistence layer.
"""

from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.compliance.pii_engine import PiiEngine  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def engine_db(tmp_path):
    """A connected DB + an engine + a seeded source/scan."""
    db_path = tmp_path / "pii.db"
    db = Database({"path": str(db_path)})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path) VALUES ('src', '/share')"
        )
        cur.execute("INSERT INTO scan_runs (source_id) VALUES (1)")
    cfg = {"compliance": {"pii": {"enabled": True}}}
    return PiiEngine(db, cfg), db


def _seed_file(db, source_id, scan_id, file_path):
    """Insert one scanned_files row pointing at file_path."""
    fname = os.path.basename(file_path)
    ext = fname.rpartition(".")[2] if "." in fname else ""
    with db.get_cursor() as cur:
        cur.execute(
            """INSERT INTO scanned_files
               (source_id, scan_id, file_path, relative_path,
                file_name, extension, file_size, last_modify_time, owner)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (source_id, scan_id, file_path, fname, fname,
             ext, os.path.getsize(file_path) if os.path.exists(file_path) else 0,
             "2024-01-01 12:00:00", "alice"),
        )


# ──────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────


def test_scan_file_detects_email_iban_tckn(engine_db, tmp_path):
    engine, _ = engine_db
    # The default iban_tr regex shipped in the spec matches the
    # TR + 2-digit-checksum + 5x4-digit groups + 2-digit tail layout
    # (22 digit groups), so we use exactly that shape in the fixture.
    p = tmp_path / "doc.txt"
    p.write_text(
        "Contact alice@example.com or bob@example.com, also "
        "carol@example.com.\nIBAN: TR33 0006 1005 1978 6457 26\n"
        "TCKN: 12345678901\n",
        encoding="utf-8",
    )
    out = engine.scan_file(str(p))
    assert out["scanned_bytes"] > 0
    hits = out["hits"]
    assert "email" in hits
    assert len(hits["email"]) == 3
    assert "iban_tr" in hits
    assert len(hits["iban_tr"]) >= 1
    assert "tckn" in hits
    assert len(hits["tckn"]) >= 1


def test_scan_file_skips_binary_extension(engine_db, tmp_path):
    """A file with a binary extension should never be opened."""
    engine, _ = engine_db
    p = tmp_path / "evil.exe"
    # Write something that *would* match if we scanned it.
    p.write_bytes(b"alice@example.com")
    out = engine.scan_file(str(p))
    assert out["hits"] == {}
    assert out["scanned_bytes"] == 0


def test_redaction_does_not_expose_full_email(engine_db, tmp_path):
    engine, _ = engine_db
    p = tmp_path / "leak.txt"
    p.write_text("john@example.com", encoding="utf-8")
    out = engine.scan_file(str(p))
    assert "email" in out["hits"]
    snippet = out["hits"]["email"][0]
    # Local-part middle is masked; domain preserved for operator context.
    assert "***" in snippet
    assert "john" not in snippet
    assert snippet.endswith("@example.com")


def test_custom_patterns_from_config_picked_up(tmp_path):
    db_path = tmp_path / "pii.db"
    db = Database({"path": str(db_path)})
    db.connect()
    cfg = {
        "compliance": {
            "pii": {
                "enabled": True,
                "patterns": {
                    "internal_id": r"\bEMP-\d{4}\b",
                },
            }
        }
    }
    engine = PiiEngine(db, cfg)
    assert "internal_id" in engine.patterns
    p = tmp_path / "hr.txt"
    p.write_text("Employee EMP-1234 reviewed by EMP-5678.", encoding="utf-8")
    out = engine.scan_file(str(p))
    assert "internal_id" in out["hits"]
    assert len(out["hits"]["internal_id"]) == 2
    # Default pattern still present.
    assert "email" in engine.patterns


def test_scan_source_persists_findings(engine_db, tmp_path):
    engine, db = engine_db
    p = tmp_path / "leak.txt"
    p.write_text("Reach me at alice@example.com.", encoding="utf-8")
    _seed_file(db, source_id=1, scan_id=1, file_path=str(p))

    result = engine.scan_source(source_id=1)
    assert result["scan_id"] == 1
    assert result["scanned"] == 1
    assert result["hits_total"] >= 1

    with db.get_cursor() as cur:
        cur.execute("SELECT pattern_name, hit_count, sample_snippet "
                    "FROM pii_findings WHERE file_path = ?", (str(p),))
        rows = [dict(r) for r in cur.fetchall()]
    assert any(r["pattern_name"] == "email" for r in rows)
    # Persisted snippet must be the redacted form.
    snippet = next(r for r in rows if r["pattern_name"] == "email")["sample_snippet"]
    assert "alice" not in snippet
    assert "***" in snippet


def test_scan_source_idempotent_skips_already_scanned(engine_db, tmp_path):
    engine, db = engine_db
    p = tmp_path / "leak.txt"
    p.write_text("alice@example.com", encoding="utf-8")
    _seed_file(db, source_id=1, scan_id=1, file_path=str(p))

    first = engine.scan_source(source_id=1)
    assert first["scanned"] == 1
    assert first["skipped"] == 0

    # Second call: same source, no new scanned_files — must skip.
    second = engine.scan_source(source_id=1)
    assert second["scanned"] == 0
    assert second["skipped"] == 1
    assert second["hits_total"] == 0

    # overwrite_existing=True forces a rescan.
    third = engine.scan_source(source_id=1, overwrite_existing=True)
    assert third["scanned"] == 1


def test_find_for_subject_returns_only_files_containing_term(engine_db, tmp_path):
    engine, db = engine_db
    p1 = tmp_path / "alice.txt"
    p1.write_text("alice@example.com", encoding="utf-8")
    p2 = tmp_path / "bob.txt"
    p2.write_text("bob@example.com", encoding="utf-8")
    _seed_file(db, 1, 1, str(p1))
    _seed_file(db, 1, 1, str(p2))

    engine.scan_source(source_id=1)
    results = engine.find_for_subject("alice")
    paths = [r["file_path"] for r in results]
    # alice.txt must match (file path contains "alice"); bob.txt must not.
    assert str(p1) in paths
    assert str(p2) not in paths
    # Subject row carries owner + last_modify_time from scanned_files.
    alice_row = next(r for r in results if r["file_path"] == str(p1))
    assert alice_row["owner"] == "alice"
    assert alice_row["last_modify_time"] == "2024-01-01 12:00:00"


def test_export_subject_csv_writes_rows(engine_db, tmp_path):
    engine, db = engine_db
    p = tmp_path / "alice.txt"
    p.write_text("alice@example.com", encoding="utf-8")
    _seed_file(db, 1, 1, str(p))
    engine.scan_source(source_id=1)

    out = tmp_path / "export.csv"
    n = engine.export_subject_csv("alice", str(out))
    assert n >= 1
    text = out.read_text(encoding="utf-8")
    assert "file_path" in text  # header
    assert "alice.txt" in text
    # Raw email never appears.
    assert "alice@example.com" not in text
