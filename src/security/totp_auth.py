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

try:
    import segno  # pure-python, zero-dependency QR generator
    _HAVE_SEGNO = True
except ImportError:
    _HAVE_SEGNO = False


def _render_qr_svg(uri: str) -> str | None:
    """Render *uri* as an inline SVG string, entirely on-box.

    Returns None if segno is not installed (the UI then falls back to
    manual secret entry). We deliberately do NOT call any external QR
    service: the otpauth:// URI embeds the shared TOTP secret, so sending
    it off-box would leak the second factor.
    """
    if not _HAVE_SEGNO:
        return None
    try:
        import io
        buf = io.StringIO()
        segno.make(uri, error="m").save(buf, kind="svg", scale=5, border=2)
        return buf.getvalue()
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("TOTP QR render failed: %s", e)
        return None


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
        # NOTE: the production Database sets row_factory=dict_factory on both
        # pools, so rows are dicts — always index by column name, never row[0].
        return bool(row and row["enabled"])

    # ------------------------------------------------------------------
    # Write helpers — use get_cursor (Rule 6)
    # ------------------------------------------------------------------

    def generate_setup(self, username: str, issuer: str = "FileActivity") -> dict:
        """Generate a new TOTP secret and provisioning URI for QR code enrollment.

        Stores the secret as *pending* (enabled=0) until the user verifies
        a live code via :meth:`verify_and_enable`.

        Returns:
            ``{'secret': ..., 'uri': ...}`` on success.
            ``{'error': ...}`` when pyotp is unavailable or TOTP is already
            enabled for this user (re-enrollment must go through disable first,
            which requires the current code — otherwise setup would be a
            code-free way to disarm active MFA).
        """
        if not _HAVE_PYOTP:
            return {"error": "pyotp not installed"}

        # SECURITY: never let a bare setup call silently disarm an active
        # second factor. If TOTP is already enabled, the caller must disable
        # it first (that path requires a valid current code). Only a *pending*
        # (enabled=0) or absent enrollment may be (re)generated here.
        if self.is_enabled(username):
            return {"error": "TOTP already enabled — disable it first (requires current code)"}

        secret = pyotp.random_base32()
        totp = pyotp.TOTP(secret)
        uri = totp.provisioning_uri(name=username, issuer_name=issuer)

        # Upsert: replace only a pending/absent enrollment (enabled stays 0).
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
        # QR is rendered on-box (segno) so the secret never leaves the server.
        # qr_svg is None when segno is absent → UI shows the secret for manual entry.
        return {"secret": secret, "uri": uri, "qr_svg": _render_qr_svg(uri)}

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

        totp = pyotp.TOTP(row["secret"])
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
            changed = cur.rowcount
        result = changed > 0
        logger.info("TOTP disabled for user %s (found=%s)", username, result)
        return result

    def verify_code(self, username: str, code: str) -> bool:
        """Verify a TOTP *code* for a user that already has TOTP enabled.

        Semantics:
        - No row / ``enabled=0`` → pass through (returns True): TOTP is opt-in,
          so an un-enrolled user is not gated by a second factor.
        - Enrolled (``enabled=1``) but pyotp unavailable → fail CLOSED
          (returns False): we cannot verify, so we must not accept any code.
        - Enrolled and pyotp available → returns the real verification result.
        """
        with self.db.get_read_cursor() as cur:
            cur.execute(
                "SELECT secret, enabled FROM user_totp_secrets WHERE username=?",
                (username,),
            )
            row = cur.fetchone()

        if not row or not row["enabled"]:
            # Not enrolled — pass through (opt-in model)
            return True

        if not _HAVE_PYOTP:
            # Enrolled but cannot verify — fail closed, never accept blindly.
            logger.error(
                "TOTP enabled for %s but pyotp unavailable — denying login", username
            )
            return False

        totp = pyotp.TOTP(row["secret"])
        return totp.verify(code, valid_window=1)
