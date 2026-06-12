"""Signed Tree Head (STH) export for the tamper-evident audit chain.

Competitive-research punch-list #1 (2026-06-12): the Trillian / Rekor /
Certificate-Transparency pattern. Today the audit chain (issue #38) is
hash-linked — each ``audit_log_chain`` row folds the previous row's
``row_hash`` into its own, so the row at the highest ``seq`` is a
cryptographic commitment to the entire history (the same role a Merkle
tree head plays for a CT log). It is *server-computed*, though, so an
external auditor must currently trust the running system.

This module closes that gap. We publish a tiny **Signed Tree Head**:

    {tree_size, root_hash, timestamp, algo, version, signature, public_key}

Given the WORM dump (``src/storage/audit_export.py``) + the STH + our
published public key, an auditor verifies — *without trusting us* — that
the chain has not been rewritten:

  1. import the WORM JSONL into a fresh DB and ``verify_audit_chain``
     (proves the hash-links are internally consistent), then
  2. ``compute_sth`` on that DB and check ``root_hash`` + ``tree_size``
     equal the signed STH, then
  3. ``verify_sth`` against the published public key (proves WE issued
     this checkpoint and nobody substituted a forged root).

Any retroactive UPDATE/DELETE on ``file_audit_events`` changes the
recomputed root, so an old signed STH stops matching — tamper-evident
end to end.

Design choice — hash-linked tip, not a binary Merkle tree
---------------------------------------------------------
The existing chain is a hash *list* (linked list of SHA-256 commitments),
not a binary Merkle *tree*. We deliberately keep ``root_hash`` = the
recomputed tip hash of that list rather than reshaping the data into a
binary tree:

  * It matches the chain's actual on-disk semantics (no second, divergent
    notion of "the root" that could drift from ``verify_audit_chain``).
  * The security property we need (any past edit invalidates the
    checkpoint) holds for a hash-linked tip exactly as it does for a
    Merkle root.
  * It is auditable with the stdlib alone — recompute the fold, compare.

The tradeoff a binary tree would buy — O(log n) inclusion/consistency
proofs for a *single* entry without shipping the whole log — is not
needed here: the auditor already has the full WORM dump. If per-entry
proofs become a requirement, ``root_hash`` can be upgraded to an RFC-6962
Merkle root behind the same STH envelope (bump ``version``).

Crucially this module READS the chain; it never rewrites it. The fold it
recomputes uses the *exact* construction in
``Database._row_hash`` / ``Database._canonical_event_json`` so a tampered
event row yields a different root — the verification is meaningful, not a
rubber stamp.

Dependency: ``cryptography`` (Ed25519). It is an optional dep in this repo
(see requirements.txt); importing it lazily keeps the rest of the module
usable for chain-only callers, and the test-suite skips signing tests when
the package is absent.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, Tuple

# STH envelope version. Bump when the canonical-JSON shape or the
# root-hash construction changes (e.g. a future RFC-6962 Merkle upgrade).
STH_VERSION = 1
STH_ALGO = "sha256"
_GENESIS_HASH = "0" * 64


# ---------------------------------------------------------------------------
# Canonical JSON — the auditor must reproduce these bytes exactly.
# ---------------------------------------------------------------------------
def canonical_json(obj: dict) -> str:
    """Deterministic JSON: sorted keys, no whitespace, UTF-8.

    This is the byte string we sign and verify over. It must be stable
    across Python versions and platforms, so we pin ``sort_keys=True`` +
    the compact separators and forbid non-ASCII surprises via
    ``ensure_ascii=True`` (the default). ``default=str`` keeps any stray
    datetime/Decimal deterministic without forcing callers to pre-coerce.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _sth_signing_payload(sth: dict) -> bytes:
    """Canonical bytes of the *unsigned* STH fields only.

    Signing must cover exactly the fields an auditor reconstructs from the
    chain — never the signature/public_key themselves (those are added
    after signing). We therefore project to the stable field set rather
    than signing whatever dict was passed in.
    """
    core = {
        "version": sth["version"],
        "algo": sth["algo"],
        "tree_size": sth["tree_size"],
        "root_hash": sth["root_hash"],
        "timestamp": sth["timestamp"],
    }
    return canonical_json(core).encode("utf-8")


# ---------------------------------------------------------------------------
# compute_sth — recompute the rolling root from the chain in seq order.
# ---------------------------------------------------------------------------
def _row_hash(seq: int, event_id: int, prev_hash: str, canonical_event: str) -> str:
    """Re-implementation of ``Database._row_hash`` (kept byte-identical).

    Pinned by tests against the live ``Database`` so the two never drift;
    duplicated (not imported) so an auditor can run STH verification from
    this module alone without constructing a ``Database``.
    """
    payload = f"{seq}|{event_id}|{prev_hash}|{canonical_event}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _canonical_event_json(event_row: dict) -> str:
    """Re-implementation of ``Database._canonical_event_json``.

    Same construction (sorted keys, compact separators, ``default=str``);
    kept local for the auditor-standalone reason above.
    """
    return json.dumps(event_row, sort_keys=True, separators=(",", ":"), default=str)


def compute_sth(db, *, up_to_seq: Optional[int] = None) -> dict:
    """Walk ``audit_log_chain`` in ``seq`` order and recompute the root.

    Reads (read-only) the chain rows and their joined ``file_audit_events``
    rows, recomputes the rolling hash fold exactly as the writer did, and
    returns an *unsigned* STH dict.

    The recomputation is the point: we fold the *current* event rows, so if
    any row was tampered after the fact the recomputed ``root_hash`` differs
    from what an old signed STH committed to. We do NOT simply read the
    stored ``row_hash`` of the tip.

    Args:
        db: a connected ``Database`` (uses ``get_read_cursor`` — never writes).
        up_to_seq: if given, fold only rows with ``seq <= up_to_seq`` (lets an
            auditor pin a historical checkpoint). ``None`` = whole chain.

    Returns:
        {
            "version":   STH_VERSION,
            "algo":      "sha256",
            "tree_size": <number of chain rows folded>,
            "root_hash": <hex sha256 tip of the recomputed fold>,
            "timestamp": <UTC ISO-8601, when this STH was computed>,
        }

    An empty chain returns ``tree_size=0`` and ``root_hash = 64*'0'`` (the
    genesis hash), which is a valid, signable checkpoint meaning "no events".
    """
    with db.get_read_cursor() as cur:
        if up_to_seq is not None:
            cur.execute(
                "SELECT c.seq AS seq, c.event_id AS event_id "
                "FROM audit_log_chain c WHERE c.seq <= ? ORDER BY c.seq ASC",
                (int(up_to_seq),),
            )
        else:
            cur.execute(
                "SELECT c.seq AS seq, c.event_id AS event_id "
                "FROM audit_log_chain c ORDER BY c.seq ASC"
            )
        chain_rows = [dict(r) for r in cur.fetchall()]

        prev_hash = _GENESIS_HASH
        tree_size = 0
        for chain in chain_rows:
            seq = chain["seq"]
            event_id = chain["event_id"]
            cur.execute(
                "SELECT * FROM file_audit_events WHERE id = ?", (event_id,)
            )
            ev = cur.fetchone()
            if ev is None:
                # A chain row whose event vanished is itself tamper evidence.
                # Surface it loudly rather than silently producing a root that
                # happens to match a forged STH.
                raise ValueError(
                    f"audit chain references missing event_id {event_id} "
                    f"at seq {seq}; chain is incomplete, refusing to sign"
                )
            canonical = _canonical_event_json(dict(ev))
            prev_hash = _row_hash(seq, event_id, prev_hash, canonical)
            tree_size += 1

    return {
        "version": STH_VERSION,
        "algo": STH_ALGO,
        "tree_size": tree_size,
        "root_hash": prev_hash,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# Keypair generation — one-time helper.
# ---------------------------------------------------------------------------
def generate_keypair() -> Tuple[bytes, str]:
    """Generate an Ed25519 keypair.

    Returns ``(private_pem_bytes, public_key_hex)``. The PEM is what
    ``sign_sth`` loads; the hex public key is what you publish out-of-band
    and hand to ``verify_sth``. The private PEM is unencrypted — store it
    with filesystem permissions, NEVER commit it.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    priv = Ed25519PrivateKey.generate()
    priv_pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_hex = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    ).hex()
    return priv_pem, pub_hex


def _load_private_key(private_key_pem_path: str):
    """Load an Ed25519 private key from a PEM (or raw 32-byte) file.

    Mirrors ``AuditExporter._sign_file`` key handling so a key that signs
    WORM exports also signs STHs.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    with open(private_key_pem_path, "rb") as f:
        key_bytes = f.read()
    try:
        priv = serialization.load_pem_private_key(key_bytes, password=None)
    except Exception:
        if len(key_bytes) == 32:
            priv = Ed25519PrivateKey.from_private_bytes(key_bytes)
        else:
            raise
    if not isinstance(priv, Ed25519PrivateKey):
        raise ValueError("signing key is not an Ed25519 private key")
    return priv


# ---------------------------------------------------------------------------
# sign_sth / verify_sth — the publish + audit sides.
# ---------------------------------------------------------------------------
def sign_sth(sth: dict, private_key_pem_path: str) -> dict:
    """Sign an STH with Ed25519 over its canonical JSON.

    The signature covers the canonical bytes of the *core* STH fields
    (version, algo, tree_size, root_hash, timestamp) — see
    ``_sth_signing_payload`` — so adding the signature/public_key fields
    afterwards does not invalidate it.

    Returns a NEW dict = the STH core fields + ``signature`` (hex) and
    ``public_key`` (hex, raw Ed25519). The public key is embedded for
    convenience, but an auditor should verify against the public key they
    obtained out-of-band, not the one in the file.
    """
    from cryptography.hazmat.primitives import serialization

    priv = _load_private_key(private_key_pem_path)
    payload = _sth_signing_payload(sth)
    signature = priv.sign(payload)
    pub_hex = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    ).hex()

    out = {
        "version": sth["version"],
        "algo": sth["algo"],
        "tree_size": sth["tree_size"],
        "root_hash": sth["root_hash"],
        "timestamp": sth["timestamp"],
        "signature": signature.hex(),
        "public_key": pub_hex,
    }
    return out


def verify_sth(sth_signed: dict, public_key_hex: str) -> bool:
    """Verify a signed STH against a raw-hex Ed25519 public key.

    The auditor side. Reconstructs the exact signing payload from the STH's
    core fields and checks the Ed25519 signature. Returns ``True`` iff the
    signature is valid for ``public_key_hex``; ``False`` on any signature
    mismatch or malformed input. Never raises for a bad signature — a
    tampered STH must come back ``False``, not blow up.

    Note this checks only *who signed* the checkpoint. To prove the chain
    itself is untampered the auditor also recomputes ``compute_sth`` on the
    WORM dump and compares ``root_hash`` + ``tree_size`` to this STH.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    from cryptography.exceptions import InvalidSignature

    try:
        pub = Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key_hex))
        signature = bytes.fromhex(sth_signed["signature"])
    except (ValueError, KeyError, TypeError):
        return False

    payload = _sth_signing_payload(sth_signed)
    try:
        pub.verify(signature, payload)
        return True
    except InvalidSignature:
        return False
    except Exception:
        # Defensive: any backend-level failure is a non-verification, not a
        # crash. The auditor must be able to trust a False here.
        return False


# ---------------------------------------------------------------------------
# Convenience: compute + sign in one call (used by the CLI --emit path).
# ---------------------------------------------------------------------------
def emit_signed_sth(db, private_key_pem_path: str, *,
                    up_to_seq: Optional[int] = None) -> dict:
    """Compute the current STH and sign it. Returns the signed STH dict."""
    sth = compute_sth(db, up_to_seq=up_to_seq)
    return sign_sth(sth, private_key_pem_path)
