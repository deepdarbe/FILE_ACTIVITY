"""Tests for Wave 10 #311: TOTP/MFA enrollment and verification.

Requires pyotp to be installed. Tests are skipped if pyotp is absent.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

class _FakeDB:
    """Minimal Database stub that wraps a real SQLite connection."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row

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
def totp_mgr(tmp_db):
    pytest.importorskip("pyotp")
    from src.security.totp_auth import TOTPManager
    return TOTPManager(tmp_db)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestTOTPManagerTableCreation:
    def test_table_created_on_init(self, tmp_db):
        """TOTPManager creates user_totp_secrets table on construction."""
        pytest.importorskip("pyotp")
        from src.security.totp_auth import TOTPManager
        TOTPManager(tmp_db)
        with tmp_db.get_read_cursor() as cur:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='user_totp_secrets'"
            )
            row = cur.fetchone()
        assert row is not None, "user_totp_secrets table should exist after TOTPManager init"


class TestTOTPManagerGenerateSetup:
    def test_generate_setup_returns_secret_and_uri(self, totp_mgr):
        """generate_setup returns a dict with 'secret' and 'uri' keys."""
        result = totp_mgr.generate_setup("alice")
        assert "secret" in result, "should return secret"
        assert "uri" in result, "should return uri"
        assert "error" not in result

    def test_generate_setup_uri_contains_username(self, totp_mgr):
        """The provisioning URI includes the username."""
        result = totp_mgr.generate_setup("bob")
        assert "bob" in result["uri"]

    def test_generate_setup_uri_contains_issuer(self, totp_mgr):
        """The provisioning URI includes the issuer name."""
        result = totp_mgr.generate_setup("carol", issuer="MyApp")
        assert "MyApp" in result["uri"]

    def test_generate_setup_stores_pending_secret(self, totp_mgr, tmp_db):
        """generate_setup stores secret with enabled=0 (pending confirmation)."""
        totp_mgr.generate_setup("dave")
        with tmp_db.get_read_cursor() as cur:
            cur.execute(
                "SELECT secret, enabled FROM user_totp_secrets WHERE username=?",
                ("dave",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["enabled"] == 0, "should be stored as pending (enabled=0)"
        assert row["secret"], "secret should be non-empty"


class TestTOTPManagerVerifyAndEnable:
    def test_correct_code_enables_totp(self, totp_mgr):
        """verify_and_enable returns True and enables TOTP on a correct code."""
        import pyotp
        setup = totp_mgr.generate_setup("eve")
        code = pyotp.TOTP(setup["secret"]).now()
        result = totp_mgr.verify_and_enable("eve", code)
        assert result is True
        assert totp_mgr.is_enabled("eve") is True

    def test_wrong_code_does_not_enable(self, totp_mgr):
        """verify_and_enable returns False for an incorrect code."""
        totp_mgr.generate_setup("frank")
        result = totp_mgr.verify_and_enable("frank", "000000")
        assert result is False
        assert totp_mgr.is_enabled("frank") is False

    def test_no_pending_secret_returns_false(self, totp_mgr):
        """verify_and_enable returns False for a user with no secret row."""
        result = totp_mgr.verify_and_enable("ghost", "123456")
        assert result is False


class TestTOTPManagerVerifyCode:
    def test_enabled_user_correct_code_passes(self, totp_mgr):
        """verify_code returns True when TOTP is enabled and code is correct."""
        import pyotp
        setup = totp_mgr.generate_setup("grace")
        code = pyotp.TOTP(setup["secret"]).now()
        totp_mgr.verify_and_enable("grace", code)
        # Get a fresh code for the verification check
        fresh_code = pyotp.TOTP(setup["secret"]).now()
        assert totp_mgr.verify_code("grace", fresh_code) is True

    def test_enabled_user_wrong_code_fails(self, totp_mgr):
        """verify_code returns False when TOTP is enabled and code is wrong."""
        import pyotp
        setup = totp_mgr.generate_setup("henry")
        code = pyotp.TOTP(setup["secret"]).now()
        totp_mgr.verify_and_enable("henry", code)
        assert totp_mgr.verify_code("henry", "000000") is False

    def test_totp_not_enabled_passes_through(self, totp_mgr):
        """verify_code returns True (pass-through) when TOTP is not enabled."""
        # User has no row at all
        assert totp_mgr.verify_code("ida", "000000") is True

    def test_totp_pending_not_enabled_passes_through(self, totp_mgr):
        """verify_code passes through when secret exists but enabled=0."""
        totp_mgr.generate_setup("jake")  # creates enabled=0 row
        # Even a wrong code should pass through (TOTP not confirmed yet)
        assert totp_mgr.verify_code("jake", "000000") is True


class TestTOTPManagerIsEnabled:
    def test_is_enabled_false_before_enable(self, totp_mgr):
        """is_enabled returns False before verify_and_enable."""
        totp_mgr.generate_setup("kim")
        assert totp_mgr.is_enabled("kim") is False

    def test_is_enabled_true_after_enable(self, totp_mgr):
        """is_enabled returns True after successful verify_and_enable."""
        import pyotp
        setup = totp_mgr.generate_setup("lee")
        code = pyotp.TOTP(setup["secret"]).now()
        totp_mgr.verify_and_enable("lee", code)
        assert totp_mgr.is_enabled("lee") is True

    def test_is_enabled_false_for_unknown_user(self, totp_mgr):
        """is_enabled returns False for a user with no row."""
        assert totp_mgr.is_enabled("nobody") is False


class TestTOTPManagerDisable:
    def test_disable_clears_enabled(self, totp_mgr):
        """disable() sets enabled=0 for a previously-enabled user."""
        import pyotp
        setup = totp_mgr.generate_setup("max")
        code = pyotp.TOTP(setup["secret"]).now()
        totp_mgr.verify_and_enable("max", code)
        assert totp_mgr.is_enabled("max") is True
        totp_mgr.disable("max")
        assert totp_mgr.is_enabled("max") is False

    def test_disable_pass_through_after_disable(self, totp_mgr):
        """After disable, verify_code passes through (TOTP no longer enforced)."""
        import pyotp
        setup = totp_mgr.generate_setup("nina")
        code = pyotp.TOTP(setup["secret"]).now()
        totp_mgr.verify_and_enable("nina", code)
        totp_mgr.disable("nina")
        # Any code (or wrong code) should pass through after disabling
        assert totp_mgr.verify_code("nina", "000000") is True


class TestTOTPManagerNoPyotp:
    """Tests that TOTPManager degrades gracefully when pyotp is not installed."""

    def test_generate_setup_returns_error_without_pyotp(self, totp_mgr):
        """generate_setup returns {'error': ...} when pyotp is absent."""
        with patch("src.security.totp_auth._HAVE_PYOTP", False):
            result = totp_mgr.generate_setup("oscar")
        assert "error" in result

    def test_verify_code_passes_through_without_pyotp(self, totp_mgr):
        """verify_code returns True (pass-through) when pyotp is absent."""
        with patch("src.security.totp_auth._HAVE_PYOTP", False):
            result = totp_mgr.verify_code("oscar", "000000")
        assert result is True

    def test_verify_and_enable_returns_false_without_pyotp(self, totp_mgr):
        """verify_and_enable returns False when pyotp is absent."""
        with patch("src.security.totp_auth._HAVE_PYOTP", False):
            result = totp_mgr.verify_and_enable("oscar", "000000")
        assert result is False
