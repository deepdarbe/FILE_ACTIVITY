"""Tests for Wave 10 #307: LDAP auth + JWT session management.

Note on PyJWT import: this test environment has a broken system-level
``cryptography`` package (missing ``_cffi_backend`` Rust extension). PyJWT's
module-level init tries to import ``cryptography`` even for pure-HMAC use.
We stub out the broken paths at the top of the module so HS256 encode/decode
(which need NO crypto library) work correctly in the test runner.
"""
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Environment fix: stub broken cryptography package for test runner.
# Only applied when the installed cryptography package is non-functional.
# ---------------------------------------------------------------------------

def _stub_broken_cryptography():
    """Replace the broken cryptography stubs so PyJWT's module-level import
    doesn't panic. Has no effect if cryptography is already importable."""
    _CRYPTO_MODS = [
        'cryptography',
        'cryptography.hazmat',
        'cryptography.hazmat._oid',
        'cryptography.hazmat.bindings',
        'cryptography.hazmat.bindings._rust',
        'cryptography.hazmat.primitives',
        'cryptography.hazmat.primitives.asymmetric',
        'cryptography.hazmat.primitives.asymmetric.ec',
        'cryptography.hazmat.primitives.asymmetric.rsa',
        'cryptography.hazmat.primitives.asymmetric.ed25519',
        'cryptography.hazmat.primitives.asymmetric.ed448',
        'cryptography.hazmat.primitives.asymmetric.x448',
        'cryptography.hazmat.primitives.asymmetric.x25519',
        'cryptography.hazmat.primitives.serialization',
        'cryptography.hazmat.primitives.hashes',
        'cryptography.hazmat.backends',
        'cryptography.exceptions',
        'cryptography.x509',
    ]
    # Quick probe
    try:
        from cryptography.hazmat._oid import ObjectIdentifier  # noqa: F401
        return  # cryptography is fine — no stub needed
    except Exception:
        pass
    # cryptography is broken — stub everything out
    for mod_name in _CRYPTO_MODS:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()


_stub_broken_cryptography()


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _dict_factory(cursor, row):
    """Mirror production Database.dict_factory so tests see dict rows (not
    sqlite3.Row, which would mask row[0]-vs-row['col'] bugs)."""
    return {col[0]: row[i] for i, col in enumerate(cursor.description)}


class _FakeDB:
    """Minimal Database stub for SessionManager tests."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = _dict_factory

    def get_cursor(self):
        return _FakeCursor(self._conn)

    def get_read_cursor(self):
        return _FakeCursor(self._conn)


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._cur = None

    def __enter__(self):
        self._cur = self._conn.cursor()
        return self._cur

    def __exit__(self, *args):
        self._conn.commit()
        self._cur.close()


@pytest.fixture()
def tmp_db():
    with tempfile.TemporaryDirectory() as d:
        yield _FakeDB(str(Path(d) / "test.db"))


@pytest.fixture()
def session_cfg():
    return {
        'dashboard': {
            'auth': {
                'admin_groups': ['FileActivity-Admins', 'Domain Admins'],
                'manager_groups': ['FileActivity-Managers'],
            }
        }
    }


@pytest.fixture()
def session_manager(tmp_db, session_cfg):
    from src.security.session import SessionManager
    return SessionManager(tmp_db, session_cfg)


# ---------------------------------------------------------------------------
# SessionManager tests
# ---------------------------------------------------------------------------

class TestSessionManagerIssueTokens:
    def test_returns_expected_keys(self, session_manager):
        user = {'username': 'alice', 'display_name': 'Alice', 'email': 'alice@corp', 'groups': []}
        result = session_manager.issue_tokens(user)
        assert set(result.keys()) == {'access_token', 'refresh_token', 'expires_in'}
        assert result['expires_in'] == 8 * 3600

    def test_access_token_has_correct_claims(self, session_manager):
        import jwt
        user = {'username': 'bob', 'display_name': 'Bob', 'email': 'bob@corp', 'groups': ['FileActivity-Admins']}
        tokens = session_manager.issue_tokens(user)
        payload = jwt.decode(tokens['access_token'], session_manager.secret, algorithms=['HS256'])
        assert payload['sub'] == 'bob'
        assert payload['name'] == 'Bob'
        assert payload['role'] == 'admin'
        assert payload['type'] == 'access'

    def test_viewer_role_for_no_groups(self, session_manager):
        import jwt
        user = {'username': 'carol', 'display_name': 'Carol', 'email': '', 'groups': []}
        tokens = session_manager.issue_tokens(user)
        payload = jwt.decode(tokens['access_token'], session_manager.secret, algorithms=['HS256'])
        assert payload['role'] == 'viewer'

    def test_manager_role(self, session_manager):
        import jwt
        user = {'username': 'dave', 'display_name': 'Dave', 'email': '', 'groups': ['FileActivity-Managers']}
        tokens = session_manager.issue_tokens(user)
        payload = jwt.decode(tokens['access_token'], session_manager.secret, algorithms=['HS256'])
        assert payload['role'] == 'manager'


class TestSessionManagerVerify:
    def test_valid_token_returns_payload(self, session_manager):
        user = {'username': 'eve', 'display_name': 'Eve', 'email': '', 'groups': []}
        tokens = session_manager.issue_tokens(user)
        payload = session_manager.verify_access_token(tokens['access_token'])
        assert payload is not None
        assert payload['sub'] == 'eve'

    def test_wrong_signature_returns_none(self, session_manager):
        import jwt
        payload = {'sub': 'hacker', 'type': 'access', 'exp': int(time.time()) + 3600}
        bad_token = jwt.encode(payload, 'wrong-secret', algorithm='HS256')
        assert session_manager.verify_access_token(bad_token) is None

    def test_refresh_token_rejected_by_verify_access(self, session_manager):
        user = {'username': 'frank', 'display_name': 'Frank', 'email': '', 'groups': []}
        tokens = session_manager.issue_tokens(user)
        # refresh token should NOT pass verify_access_token
        assert session_manager.verify_access_token(tokens['refresh_token']) is None

    def test_expired_token_returns_none(self, session_manager):
        import jwt
        import time as _time
        payload = {
            'sub': 'ghost',
            'type': 'access',
            'exp': int(_time.time()) - 1,  # already expired
        }
        expired_token = jwt.encode(payload, session_manager.secret, algorithm='HS256')
        assert session_manager.verify_access_token(expired_token) is None


class TestSessionManagerSecret:
    def test_secret_persisted_across_instances(self, tmp_db, session_cfg):
        from src.security.session import SessionManager
        sm1 = SessionManager(tmp_db, session_cfg)
        sm2 = SessionManager(tmp_db, session_cfg)
        assert sm1.secret == sm2.secret

    def test_env_var_overrides_db_secret(self, tmp_db, session_cfg, monkeypatch):
        strong = 'x' * 40  # >= 32 chars (see length guard below)
        monkeypatch.setenv('FILEACTIVITY_SESSION_SECRET', strong)
        from src.security.session import SessionManager
        sm = SessionManager(tmp_db, session_cfg)
        assert sm.secret == strong

    def test_short_env_secret_rejected(self, tmp_db, session_cfg, monkeypatch):
        """A weak (<32 char) env secret must hard-fail startup, not sign tokens."""
        monkeypatch.setenv('FILEACTIVITY_SESSION_SECRET', 'too-short')
        from src.security.session import SessionManager
        with pytest.raises(RuntimeError, match="at least 32"):
            SessionManager(tmp_db, session_cfg)

    def test_secret_survives_restart_dict_rows(self, tmp_db, session_cfg, monkeypatch):
        """Regression: the persisted-secret read used row[0], which is a KeyError
        under the production dict row factory → hard crash on every restart."""
        monkeypatch.delenv('FILEACTIVITY_SESSION_SECRET', raising=False)
        from src.security.session import SessionManager
        sm1 = SessionManager(tmp_db, session_cfg)   # generates + persists
        sm2 = SessionManager(tmp_db, session_cfg)   # must READ it back, not crash
        assert sm1.secret == sm2.secret


# ---------------------------------------------------------------------------
# LDAPAuthenticator tests
# ---------------------------------------------------------------------------

class TestLDAPAuthenticatorDisabled:
    def test_disabled_returns_none(self):
        from src.security.ldap_auth import LDAPAuthenticator
        auth = LDAPAuthenticator({'active_directory': {'enabled': False}})
        result = auth.authenticate('alice', 'password')
        assert result is None

    def test_no_config_returns_none(self):
        from src.security.ldap_auth import LDAPAuthenticator
        auth = LDAPAuthenticator({})
        result = auth.authenticate('alice', 'password')
        assert result is None


class TestLDAPAuthenticatorEscape:
    def test_escape_special_chars(self):
        from src.security.ldap_auth import LDAPAuthenticator
        auth = LDAPAuthenticator({})
        assert auth._escape_ldap('alice*(evil)') == r'alice\2a\28evil\29'
        assert auth._escape_ldap('back\\slash') == r'back\5cslash'
        assert auth._escape_ldap('null\x00char') == r'null\00char'

    def test_escape_clean_string_unchanged(self):
        from src.security.ldap_auth import LDAPAuthenticator
        auth = LDAPAuthenticator({})
        assert auth._escape_ldap('normaluser123') == 'normaluser123'


class TestLDAPAuthenticatorNormalize:
    def test_strip_domain_prefix(self):
        from src.security.ldap_auth import LDAPAuthenticator
        auth = LDAPAuthenticator({})
        assert auth._normalize_username('CORP\\alice') == 'alice'

    def test_strip_upn_suffix(self):
        from src.security.ldap_auth import LDAPAuthenticator
        auth = LDAPAuthenticator({})
        assert auth._normalize_username('alice@corp.local') == 'alice'

    def test_plain_username_unchanged(self):
        from src.security.ldap_auth import LDAPAuthenticator
        auth = LDAPAuthenticator({})
        assert auth._normalize_username('alice') == 'alice'


class TestRoleDetermination:
    def test_admin_group_gives_admin_role(self, session_manager):
        user = {'username': 'x', 'display_name': 'X', 'email': '', 'groups': ['Domain Admins']}
        tokens = session_manager.issue_tokens(user)
        payload = session_manager.verify_access_token(tokens['access_token'])
        assert payload['role'] == 'admin'

    def test_manager_group_gives_manager_role(self, session_manager):
        user = {'username': 'y', 'display_name': 'Y', 'email': '', 'groups': ['FileActivity-Managers']}
        tokens = session_manager.issue_tokens(user)
        payload = session_manager.verify_access_token(tokens['access_token'])
        assert payload['role'] == 'manager'

    def test_no_group_gives_viewer_role(self, session_manager):
        user = {'username': 'z', 'display_name': 'Z', 'email': '', 'groups': []}
        tokens = session_manager.issue_tokens(user)
        payload = session_manager.verify_access_token(tokens['access_token'])
        assert payload['role'] == 'viewer'
