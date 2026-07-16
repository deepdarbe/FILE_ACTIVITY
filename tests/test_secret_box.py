"""Tests for SecretBox — at-rest envelope encryption (#318)."""
import pytest

from src.security.secret_box import SecretBox, _HAVE_CRYPTO

requires_crypto = pytest.mark.skipif(
    not _HAVE_CRYPTO, reason="cryptography not available in this environment"
)


@requires_crypto
def test_round_trip():
    box = SecretBox("a-strong-key-material-abcdef-0123456789")
    assert box.active is True
    ct = box.encrypt("JBSWY3DPEHPK3PXP")
    assert ct != "JBSWY3DPEHPK3PXP"       # actually encrypted
    assert ct.startswith("enc:")           # tagged
    assert box.decrypt(ct) == "JBSWY3DPEHPK3PXP"


@requires_crypto
def test_legacy_plaintext_passthrough():
    """A value without the enc: prefix (pre-#318) is returned untouched."""
    box = SecretBox("some-key-material-value-987654321000")
    assert box.decrypt("JBSWY3DPEHPK3PXP") == "JBSWY3DPEHPK3PXP"


@requires_crypto
def test_same_material_interoperates():
    """A second box with the same key can read the first's ciphertext."""
    ct = SecretBox("shared-material-xxxxxxxxxxxxxxxxxxxx").encrypt("hello")
    assert SecretBox("shared-material-xxxxxxxxxxxxxxxxxxxx").decrypt(ct) == "hello"


@requires_crypto
def test_wrong_key_does_not_crash():
    ct = SecretBox("key-one-aaaaaaaaaaaaaaaaaaaaaaaaaaaa").encrypt("secret")
    # A box with a different key can't decrypt → returns the raw stored value
    # (logged) rather than raising, so a login flow degrades instead of 500-ing.
    out = SecretBox("key-two-bbbbbbbbbbbbbbbbbbbbbbbbbbbb").decrypt(ct)
    assert out == ct


def test_no_key_material_is_passthrough(monkeypatch):
    """No env key + no fallback => inactive, identity pass-through (plaintext)."""
    monkeypatch.delenv("FILEACTIVITY_TOTP_KEY", raising=False)
    monkeypatch.delenv("FILEACTIVITY_SESSION_SECRET", raising=False)
    box = SecretBox(None)
    assert box.active is False
    assert box.encrypt("x") == "x"
    assert box.decrypt("x") == "x"
