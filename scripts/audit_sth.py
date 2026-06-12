#!/usr/bin/env python3
"""Signed Tree Head (STH) CLI for the tamper-evident audit chain.

The operator/auditor front-end for ``src/storage/audit_sth.py`` — the
Trillian / Rekor / Certificate-Transparency checkpoint pattern. Lets you:

  * generate a one-time Ed25519 signing keypair,
  * emit a signed STH ``{tree_size, root_hash, timestamp, algo, version,
    signature, public_key}`` over the current chain, and
  * verify a signed STH (auditor side) against a published public key,
    optionally re-checking the root against the live chain.

Strictly read-only against the database: the STH is computed via
``db.get_read_cursor()`` and signing happens offline. This is a CLI/offline
operation by design — it adds NO dashboard endpoint, so the audit-event /
cursor endpoint conventions don't apply to it.

Usage (operator box):
    fa.cmd sth --genkey                     # one-time: writes a private key
    fa.cmd sth --emit                       # write data/audit/sth-<ts>.json

Standalone:
    python scripts/audit_sth.py --config config.yaml --genkey
    python scripts/audit_sth.py --config config.yaml --emit
    python scripts/audit_sth.py --verify data/audit/sth-XXXX.json <pubkey_hex>
    python scripts/audit_sth.py --verify sth.json <pubkey_hex> --config config.yaml --check-chain

``--verify`` does NOT need the running system unless ``--check-chain`` is
passed; the pure signature check works from the STH file + public key alone.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Optional

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.storage.audit_sth import (  # noqa: E402
    compute_sth,
    emit_signed_sth,
    generate_keypair,
    verify_sth,
)

# Safe default key path when audit.sth.private_key_path is unset. Lives
# beside the data dir, never inside the repo, so it is never committed.
_DEFAULT_KEY_PATH = "data/audit/sth_signing_key.pem"
_DEFAULT_STH_DIR = "data/audit"


def _sth_config(config: dict) -> dict:
    """Return the ``audit.sth`` sub-dict (or {} if absent)."""
    audit = (config.get("audit") or {}) if isinstance(config, dict) else {}
    return audit.get("sth") or {}


def _resolve_key_path(config: dict, override: Optional[str]) -> str:
    if override:
        return override
    return _sth_config(config).get("private_key_path") or _DEFAULT_KEY_PATH


def _resolve_out_dir(config: dict, override: Optional[str], db_path: str) -> str:
    if override:
        return override
    configured = _sth_config(config).get("output_dir")
    if configured:
        return configured
    # Default next to the data dir, mirroring collect_diag's data-relative out.
    data_dir = os.path.dirname(os.path.abspath(db_path))
    return os.path.join(data_dir, "audit")


def _build_db_and_config(config_path: str):
    """Construct config + connected Database the same way main.py does."""
    from src.utils.config_loader import load_config
    from src.storage.database import Database

    config = load_config(config_path)
    db_conf = config.get("database", {}) or {}
    db_conf["_config_path"] = config_path
    db = Database(db_conf)
    db.connect()
    return db, config


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------
def cmd_genkey(args) -> int:
    """Generate an Ed25519 keypair; write the private PEM, print the pubkey."""
    config = {}
    if args.config and os.path.exists(args.config):
        try:
            from src.utils.config_loader import load_config
            config = load_config(args.config)
        except Exception:
            config = {}
    key_path = _resolve_key_path(config, args.key)

    if os.path.exists(key_path) and not args.force:
        print(f"[ERROR] Key already exists: {key_path}\n"
              f"        Refusing to overwrite (use --force to replace, but "
              f"rotating a signing key invalidates every prior STH's trust "
              f"anchor — publish the new public key out-of-band first).",
              file=sys.stderr)
        return 1

    priv_pem, pub_hex = generate_keypair()
    os.makedirs(os.path.dirname(os.path.abspath(key_path)) or ".", exist_ok=True)
    # 0600 the private key on POSIX; Windows ignores mode but inherits ACLs.
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(priv_pem)
    except Exception:
        os.close(fd)
        raise
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass

    print(f"[OK] Private signing key written: {key_path}  (mode 0600)")
    print(f"[OK] Public key (publish this OUT-OF-BAND, e.g. on your website / "
          f"in your compliance docs):")
    print(f"     {pub_hex}")
    print("[WARN] NEVER commit the private key. Back it up securely; losing it "
          "means you can no longer issue verifiable checkpoints.")
    return 0


def cmd_emit(args) -> int:
    """Compute + sign an STH over the current chain; write it to a file."""
    db, config = _build_db_and_config(args.config)
    db_conf = config.get("database", {}) or {}
    db_path = os.path.abspath(db_conf.get("path", "data/file_activity.db"))
    key_path = _resolve_key_path(config, args.key)

    if not os.path.exists(key_path):
        try:
            db.close()
        except Exception:
            pass
        print(f"[ERROR] Signing key not found: {key_path}\n"
              f"        Run `--genkey` first (one-time), or point "
              f"audit.sth.private_key_path / --key at an existing Ed25519 key.",
              file=sys.stderr)
        return 1

    try:
        signed = emit_signed_sth(db, key_path, up_to_seq=args.up_to_seq)
    finally:
        try:
            db.close()
        except Exception:
            pass

    out_dir = _resolve_out_dir(config, args.out, db_path)
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(out_dir, f"sth-{stamp}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(signed, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"[OK] Signed Tree Head written: {out_path}")
    print(f"     tree_size = {signed['tree_size']}")
    print(f"     root_hash = {signed['root_hash']}")
    print(f"     timestamp = {signed['timestamp']}")
    print(f"     public_key = {signed['public_key']}")
    print("[INFO] An auditor verifies with:")
    print(f"       python scripts/audit_sth.py --verify {out_path} "
          f"{signed['public_key']}")
    return 0


def cmd_verify(args) -> int:
    """Verify a signed STH file against a public key (auditor side)."""
    sth_path = args.verify[0]
    pub_hex = args.verify[1]
    try:
        with open(sth_path, "r", encoding="utf-8") as f:
            sth_signed = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[ERROR] Could not read STH file {sth_path}: {exc}",
              file=sys.stderr)
        return 2

    ok = verify_sth(sth_signed, pub_hex)
    if not ok:
        print(f"[FAIL] Signature INVALID for {sth_path} under the given public "
              f"key. The STH was not signed by this key, or it was modified.",
              file=sys.stderr)
        return 1
    print(f"[OK] Signature VALID — this checkpoint was issued by the holder of "
          f"the given public key.")
    print(f"     tree_size = {sth_signed.get('tree_size')}")
    print(f"     root_hash = {sth_signed.get('root_hash')}")
    print(f"     timestamp = {sth_signed.get('timestamp')}")

    # Optional: recompute the root from the live chain and compare. This is
    # the second half of the audit — it proves the chain still matches the
    # signed root (no retroactive edits since the STH was issued).
    if args.check_chain:
        db, _ = _build_db_and_config(args.config)
        try:
            recomputed = compute_sth(
                db,
                up_to_seq=(sth_signed.get("tree_size")
                           if sth_signed.get("tree_size") else None),
            )
        finally:
            try:
                db.close()
            except Exception:
                pass
        size_match = recomputed["tree_size"] == sth_signed.get("tree_size")
        root_match = recomputed["root_hash"] == sth_signed.get("root_hash")
        if size_match and root_match:
            print(f"[OK] Chain MATCHES the signed root — no tampering detected "
                  f"up to tree_size {recomputed['tree_size']}.")
        else:
            print(f"[FAIL] Chain does NOT match the signed STH:\n"
                  f"        signed   tree_size={sth_signed.get('tree_size')} "
                  f"root={sth_signed.get('root_hash')}\n"
                  f"        current  tree_size={recomputed['tree_size']} "
                  f"root={recomputed['root_hash']}\n"
                  f"        The chain was rewritten after this STH was issued.",
                  file=sys.stderr)
            return 1
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Signed Tree Head (STH) tool for the audit chain.")
    parser.add_argument("--config", "-c", default="config.yaml",
                        help="Path to config.yaml")
    parser.add_argument("--key", default=None,
                        help="Private key path (default: "
                             "audit.sth.private_key_path or "
                             f"{_DEFAULT_KEY_PATH})")
    parser.add_argument("--out", default=None,
                        help="Output dir for --emit (default: <data>/audit)")
    parser.add_argument("--up-to-seq", type=int, default=None,
                        help="Pin the checkpoint to chain rows with seq <= N "
                             "(default: whole chain)")
    parser.add_argument("--force", action="store_true",
                        help="--genkey: overwrite an existing key (dangerous)")
    parser.add_argument("--check-chain", action="store_true",
                        help="--verify: also recompute the root from the live "
                             "chain and compare (needs --config + DB)")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--genkey", action="store_true",
                      help="Generate a one-time Ed25519 signing keypair")
    mode.add_argument("--emit", action="store_true",
                      help="Compute + sign an STH and write it to a file")
    mode.add_argument("--verify", nargs=2, metavar=("STH_JSON", "PUBKEY_HEX"),
                      help="Verify a signed STH file against a public key")

    args = parser.parse_args(argv)

    try:
        if args.genkey:
            return cmd_genkey(args)
        if args.emit:
            return cmd_emit(args)
        if args.verify:
            return cmd_verify(args)
    except ImportError:
        print("[ERROR] The `cryptography` package is required for STH "
              "signing/verification. Install it: pip install cryptography>=41.0",
              file=sys.stderr)
        return 2
    return 2  # unreachable: argparse requires one mode


if __name__ == "__main__":
    sys.exit(main())
