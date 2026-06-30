"""TOTP/MFA management for per-user second-factor (Wave 10 #311).

Implements RFC 6238 Time-based One-Time Password enrollment and verification.
Depends on pyotp (optional — degrades gracefully when not installed).
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

try:
    import pyotp
    _HAVE_PYOTP = True
except ImportError:
    _HAVE_PYOTP = False
    logger.warning("pyotp not installed — TOTP/MFA will be unavailable")


class TOTPManager:
    """Manages TOTP secrets and verification for per-user MFA enrollment."""

    def __init__(self, db):
        """db: Database instance (must expose get_cursor / get_read_cursor)."""
        self.db = db
        self._ensure_table()

    def _ensure_table(self):
        """Create user_totp_secrets table if it does not exist."""
        with self.db.get_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_totp_secrets (
                    username   TEXT PRIMARY KEY,
                    secret     TEXT NOT NULL,
                    enabled    INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)

    # ------------------------------------------------------------------
    # Read helpers — use get_read_cursor (Rule 6)
    # ------------------------------------------------------------------

    def is_enabled(self, username: str) -> bool:
        """Return True if TOTP is enabled for *username*."""
        with self.db.get_read_cursor() as cur:
            cur.execute(
                "SELECT enabled FROM user_totp_secrets WHERE username=?",
                (username,),
            )
            row = cur.fetchone()
        return bool(row and row[0])

    # ------------------------------------------------------------------
    # Write helpers — use get_cursor (Rule 6)
    # ------------------------------------------------------------------

    def generate_setup(self, username: str, issuer: str = "FileActivity") -> dict:
        """Generate a new TOTP secret and provisioning URI for QR code enrollment.

        Stores the secret as *pending* (enabled=0) until the user verifies
        a live code via :meth:`verify_and_enable`.

        Returns:
            ``{'secret': ..., 'uri': ...}`` on success.
            ``{'error': 'pyotp not installed'}`` when pyotp is unavailable.
        """
        if not _HAVE_PYOTP:
            return {"error": "pyotp not installed"}

        secret = pyotp.random_base32()
        totp = pyotp.TOTP(secret)
        uri = totp.provisioning_uri(name=username, issuer_name=issuer)

        # Upsert: allow re-enrollment (resets any existing pending secret)
        with self.db.get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_totp_secrets (username, secret, enabled)
                VALUES (?, ?, 0)
                ON CONFLICT(username) DO UPDATE
                    SET secret=excluded.secret, enabled=0
                """,
                (username, secret),
            )

        logger.info("TOTP setup generated for user %s", username)
        return {"secret": secret, "uri": uri}

    def verify_and_enable(self, username: str, code: str) -> bool:
        """Verify *code* against the pending secret and enable TOTP on success.

        Returns True if the code was correct and TOTP is now active.
        Returns False if pyotp is unavailable, no pending secret exists,
        or the code is wrong.
        """
        if not _HAVE_PYOTP:
            return False

        with self.db.get_read_cursor() as cur:
            cur.execute(
                "SELECT secret FROM user_totp_secrets WHERE username=?",
                (username,),
            )
            row = cur.fetchone()

        if not row:
            logger.warning("TOTP verify_and_enable: no secret for user %s", username)
            return False

        totp = pyotp.TOTP(row[0])
        if not totp.verify(code, valid_window=1):
            logger.info("TOTP verify_and_enable: wrong code for user %s", username)
            return False

        with self.db.get_cursor() as cur:
            cur.execute(
                "UPDATE user_totp_secrets SET enabled=1 WHERE username=?",
                (username,),
            )

        logger.info("TOTP enabled for user %s", username)
        return True

    def disable(self, username: str) -> bool:
        """Disable TOTP for *username* (does not delete the secret row).

        Returns True if the row existed and was updated; False otherwise.
        """
        with self.db.get_cursor() as cur:
            cur.execute(
                "UPDATE user_totp_secrets SET enabled=0 WHERE username=?",
                (username,),
            )
            changed = cur.rowcount if hasattr(cur, "rowcount") else 1
        result = bool(changed)
        logger.info("TOTP disabled for user %s (found=%s)", username, result)
        return result

    def verify_code(self, username: str, code: str) -> bool:
        """Verify a TOTP *code* for a user that already has TOTP enabled.

        Pass-through rules (returns True without verification):
        - pyotp is not installed
        - no row exists for *username*
        - TOTP is disabled (``enabled=0``) for *username*

        Returns False only when TOTP is enabled AND the code is wrong.
        """
        if not _HAVE_PYOTP:
            # Cannot check — let through; operator should install pyotp
            return True

        with self.db.get_read_cursor() as cur:
            cur.execute(
                "SELECT secret, enabled FROM user_totp_secrets WHERE username=?",
                (username,),
            )
            row = cur.fetchone()

        if not row or not row[1]:
            # Not enrolled — pass through (opt-in model)
            return True

        totp = pyotp.TOTP(row[0])
        return totp.verify(code, valid_window=1)
