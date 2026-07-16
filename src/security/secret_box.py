"""Envelope encryption for secrets at rest (#318).

Uses Fernet (AES-128-CBC + HMAC-SHA256) from ``cryptography`` — already a
declared dependency (#281). The key is derived from, in order of preference:

  1. ``FILEACTIVITY_TOTP_KEY``      (dedicated, strongest — lives OUTSIDE the DB)
  2. ``FILEACTIVITY_SESSION_SECRET`` (the operator's JWT secret env override)
  3. a caller-supplied fallback     (typically the persisted JWT secret)

Real at-rest protection needs the key OUTSIDE the database (options 1–2); the
fallback still turns the stored TOTP seed into Fernet ciphertext, so a casual DB
copy / backup on a share no longer leaks live second factors.

Degrades gracefully: if ``cryptography`` is unavailable, or no key material is
resolvable, ``encrypt``/``decrypt`` become identity pass-throughs (secrets are
stored as before — plaintext — with a warning), so callers never break.

Ciphertext is tagged with an ``enc:`` prefix so a value written before this
feature (legacy plaintext) is recognised and returned untouched — seamless,
per-row lazy migration on the next re-enrollment.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import os

logger = logging.getLogger(__name__)

try:
    from cryptography.fernet import Fernet
    _HAVE_CRYPTO = True
except Exception:  # pragma: no cover - cryptography missing/broken
    _HAVE_CRYPTO = False

_PREFIX = "enc:"


class SecretBox:
    """Encrypt/decrypt short secrets (e.g. TOTP seeds) for at-rest storage."""

    def __init__(self, key_material: str | None = None):
        material = (
            os.environ.get("FILEACTIVITY_TOTP_KEY")
            or os.environ.get("FILEACTIVITY_SESSION_SECRET")
            or key_material
        )
        self._fernet = None
        if not _HAVE_CRYPTO:
            logger.warning("cryptography unavailable — secrets stored in plaintext")
            return
        if not material:
            logger.warning("no key material — secrets stored in plaintext")
            return
        try:
            digest = hashlib.sha256(material.encode("utf-8")).digest()
            self._fernet = Fernet(base64.urlsafe_b64encode(digest))
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("SecretBox init failed, storing plaintext: %s", e)

    @property
    def active(self) -> bool:
        """True when encryption is actually in effect."""
        return self._fernet is not None

    def encrypt(self, plaintext: str) -> str:
        if not self._fernet or plaintext is None:
            return plaintext
        return _PREFIX + self._fernet.encrypt(plaintext.encode("utf-8")).decode("ascii")

    def decrypt(self, stored: str) -> str:
        # Legacy plaintext (pre-#318) is unprefixed → return untouched.
        if not self._fernet or not stored or not stored.startswith(_PREFIX):
            return stored
        try:
            return self._fernet.decrypt(stored[len(_PREFIX):].encode("ascii")).decode("utf-8")
        except Exception:
            # Wrong key / corruption — return raw rather than crash a login flow.
            logger.error("SecretBox decrypt failed (wrong key?) — value unusable")
            return stored
