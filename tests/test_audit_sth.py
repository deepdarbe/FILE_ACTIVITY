"""Tests for Signed Tree Head (STH) export — Trillian/Rekor pattern.

Coverage:
  * compute_sth recomputes the SAME root the live Database chain produces
    (the duplicated _row_hash / _canonical_event_json never drift).
  * genkey -> sign -> verify round-trips; wrong key / tampered signature
    fail closed (verify returns False, never raises).
  * Tamper detection: flip one audit row -> recomputed root differs -> the
    old signed STH no longer matches the chain.
  * Canonical-JSON determinism (stable bytes across calls + key order).
  * Empty chain is a valid, signable checkpoint (tree_size=0, genesis root).

The ``cryptography`` package is an OPTIONAL dep in this repo; every test
that signs/verifies is guarded by ``importorskip`` so CI skips gracefully
when it is absent. The pure-chain tests (compute_sth, canonical_json) need
no crypto and always run.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.storage import audit_sth  # noqa: E402


# ── fixtures / helpers ─────────────────────────────────────────


def _make_db(tmp_path, chain_enabled: bool = True) -> Database:
    db = Database({"path": str(tmp_path / "test.db")})
    db.connect()
    db.set_audit_chain_enabled(chain_enabled)
    # file_audit_events.source_id FKs sources(id); seed one so chained
    # inserts (source_id=1) don't trip the FK constraint.
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


# ── compute_sth vs the live chain ──────────────────────────────


def test_compute_sth_matches_live_chain_tip(tmp_path):
    """The recomputed root must equal the chain's stored tip row_hash.

    This pins audit_sth's duplicated _row_hash / _canonical_event_json to
    Database's — if either drifts, the STH would commit to a root the live
    verifier never produces, and this test fails.
    """
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(5):
        _insert_event(db, i)

    sth = audit_sth.compute_sth(db)
    assert sth["tree_size"] == 5
    assert sth["algo"] == "sha256"
    assert sth["version"] == audit_sth.STH_VERSION

    # The stored row_hash at the highest seq IS the hash-linked tip.
    with db.get_cursor() as cur:
        cur.execute("SELECT row_hash FROM audit_log_chain ORDER BY seq DESC LIMIT 1")
        stored_tip = cur.fetchone()["row_hash"]
    assert sth["root_hash"] == stored_tip


def test_compute_sth_empty_chain_is_genesis(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)  # no events inserted
    sth = audit_sth.compute_sth(db)
    assert sth["tree_size"] == 0
    assert sth["root_hash"] == "0" * 64


def test_compute_sth_up_to_seq_pins_history(tmp_path):
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(5):
        _insert_event(db, i)
    # Root at seq<=3 must equal the stored row_hash of seq=3.
    sth3 = audit_sth.compute_sth(db, up_to_seq=3)
    assert sth3["tree_size"] == 3
    with db.get_cursor() as cur:
        cur.execute("SELECT row_hash FROM audit_log_chain WHERE seq = 3")
        tip3 = cur.fetchone()["row_hash"]
    assert sth3["root_hash"] == tip3


# ── canonical JSON determinism ─────────────────────────────────


def test_canonical_json_is_deterministic_and_sorted():
    a = {"b": 1, "a": 2, "c": [3, 2, 1]}
    b = {"c": [3, 2, 1], "a": 2, "b": 1}  # same content, different insert order
    assert audit_sth.canonical_json(a) == audit_sth.canonical_json(b)
    # sorted keys, no whitespace
    assert audit_sth.canonical_json(a) == '{"a":2,"b":1,"c":[3,2,1]}'


def test_signing_payload_excludes_signature_fields():
    """The payload must NOT include signature/public_key (added post-sign)."""
    sth = {
        "version": 1, "algo": "sha256", "tree_size": 3,
        "root_hash": "ab" * 32, "timestamp": "2026-06-12T00:00:00+00:00",
        "signature": "deadbeef", "public_key": "cafef00d",
    }
    payload = audit_sth._sth_signing_payload(sth)
    assert b"signature" not in payload
    assert b"public_key" not in payload
    assert b"root_hash" in payload and b"tree_size" in payload


# ── sign / verify round-trip (needs cryptography) ──────────────


def test_genkey_sign_verify_roundtrip(tmp_path):
    pytest.importorskip("cryptography")
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(4):
        _insert_event(db, i)

    priv_pem, pub_hex = audit_sth.generate_keypair()
    key_path = tmp_path / "sth.pem"
    key_path.write_bytes(priv_pem)

    sth = audit_sth.compute_sth(db)
    signed = audit_sth.sign_sth(sth, str(key_path))

    # signed STH carries the core fields + signature + public_key
    assert signed["root_hash"] == sth["root_hash"]
    assert signed["tree_size"] == sth["tree_size"]
    assert signed["public_key"] == pub_hex
    assert isinstance(signed["signature"], str) and signed["signature"]

    # verify against the published public key -> True
    assert audit_sth.verify_sth(signed, pub_hex) is True
    # verify against the embedded public key -> also True
    assert audit_sth.verify_sth(signed, signed["public_key"]) is True


def test_verify_fails_wrong_key(tmp_path):
    pytest.importorskip("cryptography")
    db = _make_db(tmp_path, chain_enabled=True)
    _insert_event(db, 0)

    priv_pem, _pub_hex = audit_sth.generate_keypair()
    key_path = tmp_path / "sth.pem"
    key_path.write_bytes(priv_pem)
    signed = audit_sth.sign_sth(audit_sth.compute_sth(db), str(key_path))

    # A different keypair's public key must NOT verify the signature.
    _other_priv, other_pub = audit_sth.generate_keypair()
    assert audit_sth.verify_sth(signed, other_pub) is False


def test_verify_fails_on_tampered_sth_field(tmp_path):
    pytest.importorskip("cryptography")
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(3):
        _insert_event(db, i)
    priv_pem, pub_hex = audit_sth.generate_keypair()
    key_path = tmp_path / "sth.pem"
    key_path.write_bytes(priv_pem)
    signed = audit_sth.sign_sth(audit_sth.compute_sth(db), str(key_path))

    # Flip the root_hash in the signed STH -> signature no longer matches.
    forged = dict(signed)
    forged["root_hash"] = "00" + signed["root_hash"][2:]
    assert audit_sth.verify_sth(forged, pub_hex) is False

    # Bumping tree_size likewise breaks it.
    forged2 = dict(signed)
    forged2["tree_size"] = signed["tree_size"] + 1
    assert audit_sth.verify_sth(forged2, pub_hex) is False


def test_verify_fails_closed_on_malformed_input(tmp_path):
    pytest.importorskip("cryptography")
    _priv, pub_hex = audit_sth.generate_keypair()
    # Missing signature key, bad hex, wrong types -> False, never raises.
    assert audit_sth.verify_sth({"root_hash": "x"}, pub_hex) is False
    assert audit_sth.verify_sth({"signature": "nothex!!"}, pub_hex) is False
    assert audit_sth.verify_sth({"signature": "abcd"}, "not-a-valid-pubkey") is False


# ── end-to-end tamper detection ────────────────────────────────


def test_tamper_audit_row_breaks_old_sth(tmp_path):
    """Flip one audit row AFTER signing -> old STH no longer matches chain.

    This is the whole point of STH: an auditor holding the old signed
    checkpoint recomputes the root from the (now tampered) chain and sees
    it diverge. The signature on the OLD STH is still valid (we issued it),
    but its committed root != the recomputed root.
    """
    pytest.importorskip("cryptography")
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(5):
        _insert_event(db, i)

    priv_pem, pub_hex = audit_sth.generate_keypair()
    key_path = tmp_path / "sth.pem"
    key_path.write_bytes(priv_pem)

    # Issue + persist a signed checkpoint over the pristine chain.
    signed = audit_sth.sign_sth(audit_sth.compute_sth(db), str(key_path))
    old_root = signed["root_hash"]
    assert audit_sth.verify_sth(signed, pub_hex) is True  # signature good

    # Tamper a middle event row (seq=3) directly in the DB.
    with db.get_cursor() as cur:
        cur.execute("SELECT event_id FROM audit_log_chain WHERE seq = 3")
        ev = cur.fetchone()["event_id"]
        cur.execute(
            "UPDATE file_audit_events SET event_type = 'TAMPERED' WHERE id = ?",
            (ev,),
        )

    # Recompute the root from the tampered chain.
    recomputed = audit_sth.compute_sth(db)
    assert recomputed["tree_size"] == signed["tree_size"]
    # The recomputed root diverges from the signed one -> tamper detected.
    assert recomputed["root_hash"] != old_root

    # The OLD STH's signature is still valid (we really signed it) ...
    assert audit_sth.verify_sth(signed, pub_hex) is True
    # ... but it no longer commits to the current chain state. That mismatch
    # IS the detection an auditor acts on.
    assert recomputed["root_hash"] != signed["root_hash"]


def test_emit_signed_sth_convenience(tmp_path):
    pytest.importorskip("cryptography")
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(2):
        _insert_event(db, i)
    priv_pem, pub_hex = audit_sth.generate_keypair()
    key_path = tmp_path / "sth.pem"
    key_path.write_bytes(priv_pem)

    signed = audit_sth.emit_signed_sth(db, str(key_path))
    assert signed["tree_size"] == 2
    assert audit_sth.verify_sth(signed, pub_hex) is True
    # round-trips through JSON unchanged (the file format the CLI writes)
    reloaded = json.loads(json.dumps(signed, sort_keys=True))
    assert audit_sth.verify_sth(reloaded, pub_hex) is True


def test_compute_sth_raises_on_missing_event(tmp_path):
    """A chain row whose event vanished is refused, not silently signed."""
    db = _make_db(tmp_path, chain_enabled=True)
    for i in range(3):
        _insert_event(db, i)
    # Delete the underlying event for seq=2's chain row.
    with db.get_cursor() as cur:
        cur.execute("SELECT event_id FROM audit_log_chain WHERE seq = 2")
        ev = cur.fetchone()["event_id"]
        cur.execute("DELETE FROM file_audit_events WHERE id = ?", (ev,))
    with pytest.raises(ValueError):
        audit_sth.compute_sth(db)
