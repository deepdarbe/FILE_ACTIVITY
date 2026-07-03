"""JWT session management for per-user portal access (Wave 10, #307).

Issues short-lived access tokens (8h) and long-lived refresh tokens (24h).
Secret key sourced from FILEACTIVITY_SESSION_SECRET env var or auto-generated
(stored in SQLite so it survives restarts).
"""
import logging
import os
import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_ACCESS_TOKEN_HOURS = 8
_REFRESH_TOKEN_HOURS = 24


class SessionManager:
    """Issues and verifies JWT tokens for authenticated users."""

    def __init__(self, db, config: dict):
        """db: Database instance; config: full app config dict."""
        self._db = db
        self._auth_cfg = config.get('dashboard', {}).get('auth', {})
        self._admin_groups = self._auth_cfg.get('admin_groups', [])
        self._manager_groups = self._auth_cfg.get('manager_groups', [])
        self._ensure_session_table()
        self.secret = self._load_or_create_secret()

    def _ensure_session_table(self):
        """Create session_config table if it doesn't exist."""
        with self._db.get_cursor() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS session_config "
                "(key TEXT PRIMARY KEY, value TEXT)"
            )

    def _load_or_create_secret(self) -> str:
        """Load secret from env / DB, or generate + persist a new one."""
        env_secret = os.environ.get('FILEACTIVITY_SESSION_SECRET')
        if env_secret:
            # A short HS256 key is offline-crackable → forged admin tokens.
            # Refuse to start rather than sign with a weak secret (RFC 7518 §3.2
            # recommends a key at least as long as the HMAC output: 32 bytes).
            if len(env_secret) < 32:
                raise RuntimeError(
                    "FILEACTIVITY_SESSION_SECRET must be at least 32 characters "
                    f"(got {len(env_secret)}); generate one with "
                    "`python -c \"import secrets; print(secrets.token_hex(32))\"`"
                )
            logger.debug("Using FILEACTIVITY_SESSION_SECRET from environment")
            return env_secret

        with self._db.get_cursor() as conn:
            row = conn.execute(
                "SELECT value FROM session_config WHERE key = 'jwt_secret'"
            ).fetchone()
            if row:
                # dict_factory rows — index by column name, never row[0].
                # row[0] raised KeyError → hard startup crash on every restart
                # once a jwt_secret row existed and no env override was set.
                return row["value"]

            new_secret = secrets.token_hex(32)
            conn.execute(
                "INSERT INTO session_config (key, value) VALUES ('jwt_secret', ?)",
                (new_secret,),
            )
            logger.info("Generated new JWT session secret and persisted to DB")
            return new_secret

    def _determine_role(self, groups: list) -> str:
        """Map AD group membership to a role string."""
        group_set = set(groups)
        if group_set & set(self._admin_groups):
            return 'admin'
        if group_set & set(self._manager_groups):
            return 'manager'
        return 'viewer'

    def issue_tokens(self, user_info: dict) -> dict:
        """Issue access + refresh tokens for an authenticated user."""
        try:
            import jwt
        except ImportError:
            raise RuntimeError("PyJWT not installed — cannot issue tokens")

        now = datetime.now(tz=timezone.utc)
        username = user_info['username']
        display_name = user_info.get('display_name', username)
        email = user_info.get('email', '')
        groups = user_info.get('groups', [])
        role = self._determine_role(groups)

        access_payload = {
            'sub': username,
            'name': display_name,
            'email': email,
            'role': role,
            'type': 'access',
            'exp': now + timedelta(hours=_ACCESS_TOKEN_HOURS),
            'iat': now,
        }
        refresh_payload = {
            'sub': username,
            'type': 'refresh',
            'exp': now + timedelta(hours=_REFRESH_TOKEN_HOURS),
            'iat': now,
        }

        access_token = jwt.encode(access_payload, self.secret, algorithm='HS256')
        refresh_token = jwt.encode(refresh_payload, self.secret, algorithm='HS256')

        return {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'expires_in': _ACCESS_TOKEN_HOURS * 3600,
        }

    def verify_access_token(self, token: str) -> Optional[dict]:
        """Decode and validate an access token. Returns payload or None."""
        try:
            import jwt
        except ImportError:
            return None

        try:
            payload = jwt.decode(token, self.secret, algorithms=['HS256'])
            if payload.get('type') != 'access':
                return None
            return payload
        except Exception:
            return None

    def refresh_access_token(self, refresh_token: str, user_info: dict) -> Optional[dict]:
        """Verify refresh token and issue a new access token dict."""
        try:
            import jwt
        except ImportError:
            return None

        try:
            payload = jwt.decode(refresh_token, self.secret, algorithms=['HS256'])
            if payload.get('type') != 'refresh':
                return None
        except Exception:
            return None

        tokens = self.issue_tokens(user_info)
        # Return only access token on refresh (client keeps existing refresh token)
        return {
            'access_token': tokens['access_token'],
            'expires_in': tokens['expires_in'],
        }
