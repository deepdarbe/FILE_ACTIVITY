"""WORM-storable audit log export (issue #38).

Writes the hash-chained ``audit_log_chain`` joined with ``file_audit_events``
to a JSONL file suitable for write-once-read-many storage (S3 Object Lock,
Azure Blob immutable, ZFS snapshot, optical media). Each line is one event
+ chain metadata so a tampered DB row is detectable from the file alone by
re-running ``Database.verify_audit_chain`` on a freshly-imported copy.

Optional Ed25519 signing via ``cryptography`` (not a hard dep). When the
package is missing or no key path is configured, signing is silently
skipped — caller sees ``signed=False`` in the result.

References: NIST SP 800-92, OWASP Logging Cheat Sheet,
HHS HIPAA Security Rule §164.312(b) (audit controls).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger("file_activity.audit_export")


class AuditExporter:
    """Export hash-chained audit log slices to JSONL for WORM storage."""

    def __init__(self, db, config: Optional[dict] = None):
        self.db = db
        # Accept either the full app config or just the audit sub-dict.
        cfg = config or {}
        self.audit_cfg = cfg.get("audit", cfg) or {}
        self.output_dir = self.audit_cfg.get("worm_export_dir",
                                              "data/audit_export")
        self.signing_key_path = self.audit_cfg.get("signing_key_path", "") or ""

    # ── helpers ──────────────────────────────────────────────

    @staticmethod
    def _sha256_file(path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    def _sign_file(self, file_path: str) -> bool:
        """Sign file's SHA-256 with Ed25519, write detached <file>.sig.

        Returns True if a signature was written. Logs+returns False on any
        soft failure (missing dep, missing key, parse error). Hard errors
        are also caught and logged — never raised, since signing is optional.
        """
        if not self.signing_key_path:
            return False
        if not os.path.exists(self.signing_key_path):
            logger.warning("Signing skipped: key file missing: %s",
                           self.signing_key_path)
            return False
        try:
            from cryptography.hazmat.primitives import serialization  # type: ignore
            from cryptography.hazmat.primitives.asymmetric.ed25519 import (
                Ed25519PrivateKey,  # type: ignore
            )
        except Exception:
            logger.warning(
                "Signing requested but `cryptography` package not installed; "
                "skipping. Install cryptography>=41.0 to enable WORM signing."
            )
            return False
        try:
            with open(self.signing_key_path, "rb") as f:
                key_bytes = f.read()
            try:
                priv = serialization.load_pem_private_key(key_bytes, password=None)
            except Exception:
                # Try raw 32-byte private key
                if len(key_bytes) == 32:
                    priv = Ed25519PrivateKey.from_private_bytes(key_bytes)
                else:
                    raise
            if not isinstance(priv, Ed25519PrivateKey):
                logger.warning("Signing key is not Ed25519; skipping signature")
                return False
            digest = self._sha256_file(file_path).encode("ascii")
            signature = priv.sign(digest)
            sig_path = file_path + ".sig"
            with open(sig_path, "wb") as f:
                f.write(signature)
            return True
        except Exception as e:
            logger.warning("Ed25519 signing failed for %s: %s", file_path, e)
            return False

    # ── public API ────────────────────────────────────────────

    def export_range(self, start_date: Optional[str], end_date: Optional[str],
                     output_dir: Optional[str] = None) -> dict:
        """Export chain+events in [start_date, end_date] to a JSONL file.

        Dates are SQL strings (``YYYY-MM-DD`` or ``YYYY-MM-DD HH:MM:SS``).
        Either bound may be None for unbounded.

        Returns:
            {file, sha256, row_count, signed, start_date, end_date}
        """
        out_dir = output_dir or self.output_dir
        os.makedirs(out_dir, exist_ok=True)

        rows = self.db.get_audit_chain_for_export(start_date, end_date)

        # Filename — fall back to "all" tags when bound missing.
        def _tag(d):
            if not d:
                return "all"
            # take YYYY-MM-DD prefix
            return d[:10]

        fname = f"audit-{_tag(start_date)}-to-{_tag(end_date)}.jsonl"
        file_path = os.path.join(out_dir, fname)

        with open(file_path, "w", encoding="utf-8") as f:
            # Header line carries export metadata; verifiers should skip it
            # (json object with __meta__ key) before reconstructing the chain.
            header = {
                "__meta__": "audit_chain_export_v1",
                "exported_at": datetime.now().isoformat(timespec="seconds"),
                "start_date": start_date,
                "end_date": end_date,
                "row_count": len(rows),
            }
            f.write(json.dumps(header, sort_keys=True, default=str) + "\n")
            for r in rows:
                f.write(json.dumps(r, sort_keys=True, default=str) + "\n")

        sha256 = self._sha256_file(file_path)
        signed = self._sign_file(file_path)

        return {
            "file": file_path,
            "sha256": sha256,
            "row_count": len(rows),
            "signed": signed,
            "start_date": start_date,
            "end_date": end_date,
        }

    def export_since_last(self, output_dir: Optional[str] = None) -> dict:
        """Export every chain row produced since the most recent export file.

        Tracks last export by reading the highest end_date in the export
        directory's filenames. Falls back to "all" if no prior exports exist.
        """
        out_dir = output_dir or self.output_dir
        os.makedirs(out_dir, exist_ok=True)

        last_end: Optional[str] = None
        for name in os.listdir(out_dir):
            # Filename format: audit-<start>-to-<end>.jsonl
            if not (name.startswith("audit-") and name.endswith(".jsonl")):
                continue
            try:
                middle = name[len("audit-"):-len(".jsonl")]
                # split on "-to-" once
                if "-to-" not in middle:
                    continue
                _, end_part = middle.split("-to-", 1)
                if end_part == "all":
                    continue
                if last_end is None or end_part > last_end:
                    last_end = end_part
            except Exception:
                continue

        end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return self.export_range(last_end, end_date, output_dir=out_dir)
