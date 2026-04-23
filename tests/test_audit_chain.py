"""Tests for issue #38: tamper-evident hash-chained audit log + WORM export.

Coverage:
  * 5 chained inserts -> chain valid, genesis prev = 64*'0'.
  * Tamper a middle row -> verify reports broken_at = that seq.
  * Verify can resume from a non-1 since_seq.
  * AuditExporter writes JSONL with header + per-row lines, sha256 matches.
  * Default behaviour (chain_enabled=false) is unchanged: no chain rows.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.storage.audit_export import AuditExporter  # noqa: E402


def _make_db(tmp_path, chain_enabled: bool = True) -> Database:
    db = Database({"path": str(tmp_path / "test.db")})
    db.connect()
    db.set_audit_chain_enabled(chain_enabled)
    # file_audit_events.source_id has a FK to sources(id); seed one row
    # so chained inserts (source_id=1) don't fail at the FK constraint.
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (id, name, unc_path) VALUES (1, ?, ?)",
            ("test_src", "//srv/share"),
        )
    return db


def _insert_event(db: Database, n: int):
    return db.insert_audit_event_chained({
        "source_id": 1,
        "event_type": "modify",
        "username": f"user{n}",
        "file_path": f"/share/file_{n}.txt",
        "file_name": f"file_{n}.txt",
        "details": f"event #{n}",
        "detected_by": "test",
    })


# ── Chain integrity ────────────────────────────────────────────


def test_chain_five_events_verified(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)
    ids = [_insert_event(db, i) for i in range(5)]
    assert all(i is not None for i in ids), "every insert returns event id"

    result = db.verify_audit_chain()
    assert result == {
        "verified": True, "total": 5,
        "broken_at": None, "broken_reason": None,
    }


def test_genesis_uses_zero_hash(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)
    _insert_event(db, 0)
    with db.get_cursor() as cur:
        cur.execute("SELECT seq, prev_hash FROM audit_log_chain ORDER BY seq ASC LIMIT 1")
        row = cur.fetchone()
    assert row["seq"] == 1
    assert row["prev_hash"] == "0" * 64


def test_tamper_breaks_chain(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(5):
        _insert_event(db, i)

    # Tamper: rewrite event_type on the chain's middle row (seq=3 -> event_id=3).
    with db.get_cursor() as cur:
        cur.execute("SELECT event_id FROM audit_log_chain WHERE seq = 3")
        target_event_id = cur.fetchone()["event_id"]
        cur.execute(
            "UPDATE file_audit_events SET event_type = 'TAMPERED' WHERE id = ?",
            (target_event_id,),
        )

    result = db.verify_audit_chain()
    assert result["verified"] is False
    assert result["total"] == 5
    # Tampering at event seq=3 will be caught at seq=3 (its own row's
    # recomputed hash diverges) — subsequent rows are also broken but we
    # report the first.
    assert result["broken_at"] == 3
    assert result["broken_reason"] is not None


def test_chain_disabled_writes_no_chain_rows(tmp_path):
    db = _make_db(tmp_path, chain_enabled=False)
    for i in range(3):
        _insert_event(db, i)
    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM file_audit_events")
        assert cur.fetchone()["c"] == 3
        cur.execute("SELECT COUNT(*) AS c FROM audit_log_chain")
        assert cur.fetchone()["c"] == 0
    # Verify on empty chain is vacuously true.
    assert db.verify_audit_chain()["verified"] is True


def test_verify_partial_range(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(5):
        _insert_event(db, i)
    # Tamper seq=2; verifying [3..5] should still pass since the broken
    # row is excluded from the walk and the seed prev_hash is taken from
    # seq=2's stored row_hash (which itself remains internally consistent).
    with db.get_cursor() as cur:
        cur.execute("SELECT event_id FROM audit_log_chain WHERE seq = 2")
        ev = cur.fetchone()["event_id"]
        cur.execute(
            "UPDATE file_audit_events SET username = 'attacker' WHERE id = ?",
            (ev,),
        )
    full = db.verify_audit_chain()
    assert full["verified"] is False
    assert full["broken_at"] == 2

    partial = db.verify_audit_chain(start_seq=3)
    assert partial["verified"] is True
    assert partial["total"] == 3


# ── WORM export ────────────────────────────────────────────────


def test_export_jsonl_count_and_sha256(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(5):
        _insert_event(db, i)

    out_dir = tmp_path / "export"
    exporter = AuditExporter(db, {"audit": {"worm_export_dir": str(out_dir)}})
    result = exporter.export_range(start_date=None, end_date=None,
                                    output_dir=str(out_dir))

    assert result["row_count"] == 5
    assert result["signed"] is False  # no key configured
    assert os.path.exists(result["file"])

    # Recompute sha256 and compare
    h = hashlib.sha256()
    with open(result["file"], "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    assert h.hexdigest() == result["sha256"]

    # File layout: 1 header + 5 event lines = 6 total
    with open(result["file"], "r", encoding="utf-8") as f:
        lines = [ln.rstrip("\n") for ln in f if ln.strip()]
    assert len(lines) == 6
    header = json.loads(lines[0])
    assert header["__meta__"] == "audit_chain_export_v1"
    assert header["row_count"] == 5
    # Each subsequent line should be a chain+event dict
    for ln in lines[1:]:
        obj = json.loads(ln)
        assert "seq" in obj and "event_id" in obj and "row_hash" in obj


def test_export_signing_skips_when_cryptography_absent_or_no_key(tmp_path):
    """Calling export with no signing_key_path must never crash."""
    db = _make_db(tmp_path, chain_enabled=True)
    _insert_event(db, 0)
    out_dir = tmp_path / "export2"
    exporter = AuditExporter(db, {"audit": {
        "worm_export_dir": str(out_dir),
        "signing_key_path": "",
    }})
    result = exporter.export_range(None, None, output_dir=str(out_dir))
    assert result["signed"] is False
    assert os.path.exists(result["file"])


def test_export_missing_key_path_logs_and_skips(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)
    _insert_event(db, 0)
    out_dir = tmp_path / "export3"
    exporter = AuditExporter(db, {"audit": {
        "worm_export_dir": str(out_dir),
        "signing_key_path": str(tmp_path / "does-not-exist.pem"),
    }})
    result = exporter.export_range(None, None, output_dir=str(out_dir))
    assert result["signed"] is False  # absent key is a soft-fail
