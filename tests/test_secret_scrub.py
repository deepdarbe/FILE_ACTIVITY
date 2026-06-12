"""Tests for src/utils/secret_scrub.py — value-level secret scrub (issue #279).

The scrub masks high-signal secret SHAPES regardless of the key a value sits
under, composing with (not replacing) the existing key-name redaction in
collect_diag.py and error_reporter.py. These tests pin both halves of the
contract: it masks real secrets, and it is conservative enough NOT to mangle
ordinary config (lowercase-hex digests, ids, paths, plain URLs).
"""

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.utils.secret_scrub import REDACTED, scrub_secret_values  # noqa: E402


# ---------------------------------------------------------------------------
# non-string / empty inputs pass through untouched
# ---------------------------------------------------------------------------
def test_non_string_inputs_unchanged():
    assert scrub_secret_values(None) is None
    assert scrub_secret_values(42) == 42
    assert scrub_secret_values(True) is True
    assert scrub_secret_values("") == ""
    assert scrub_secret_values(["x"]) == ["x"]  # lists are not strings -> as-is


# ---------------------------------------------------------------------------
# high-signal secret shapes are masked, surrounding text preserved
# ---------------------------------------------------------------------------
def test_github_pat_masked_keep_surrounding():
    text = "see token ghp_0123456789abcdefABCDEF0123456789abcdef now"
    out = scrub_secret_values(text)
    assert "ghp_0123456789abcdefABCDEF0123456789abcdef" not in out
    assert REDACTED in out
    # surrounding words survive — we mask the match, not the whole value
    assert out.startswith("see token ")
    assert out.endswith(" now")


def test_github_oauth_and_server_tokens_masked():
    for tok in (
        "gho_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ab",
        "ghs_ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210zz",
        "ghr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11",
    ):
        assert scrub_secret_values(tok) == REDACTED


def test_slack_token_masked():
    out = scrub_secret_values("hook xoxb-12345678901-abcdEFGHijkl done")
    assert "xoxb-12345678901-abcdEFGHijkl" not in out
    assert REDACTED in out


def test_aws_access_key_id_masked():
    out = scrub_secret_values("creds AKIAIOSFODNN7EXAMPLE here")
    assert "AKIAIOSFODNN7EXAMPLE" not in out
    assert REDACTED in out


def test_pem_private_key_block_masked():
    pem = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF\n"
        "xQk1Kk3x...truncated...\n"
        "-----END RSA PRIVATE KEY-----"
    )
    text = f"key: {pem}\ntrailer"
    out = scrub_secret_values(text)
    assert "BEGIN RSA PRIVATE KEY" not in out
    assert "MIIEpAIBAAKCAQEA" not in out
    assert REDACTED in out
    assert out.endswith("trailer")


def test_pem_ec_key_block_masked():
    pem = (
        "-----BEGIN EC PRIVATE KEY-----\n"
        "MHcCAQEEIL... base64 body ...\n"
        "-----END EC PRIVATE KEY-----"
    )
    assert scrub_secret_values(pem) == REDACTED


def test_url_credentials_masked_keep_framing():
    out = scrub_secret_values(
        "repo: https://alice:ghp_supersecretpat12345@github.com/o/r.git"
    )
    # the user:pass run is gone, but the URL framing stays readable
    assert "alice" not in out
    assert "ghp_supersecretpat12345" not in out
    assert "https://" in out
    assert "@github.com/o/r.git" in out
    assert REDACTED in out


def test_generic_base64_key_material_masked():
    # mixed case + digits, > 40 chars => looks like key material
    blob = "dGhpc0lzQVZlcnlMb25nQmFzZTY0RW5jb2RlZFNlY3JldEtleTEyMzQ1Ng=="
    out = scrub_secret_values(f"opaque = {blob}")
    assert blob not in out
    assert REDACTED in out


# ---------------------------------------------------------------------------
# false-positive guard: ordinary config must NOT be over-masked
# ---------------------------------------------------------------------------
def test_lowercase_hex_sha256_not_masked():
    # A real SHA-256 digest (64 lowercase hex chars) is common in config /
    # logs and must survive — no uppercase => excluded by the charset rule.
    digest = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    assert scrub_secret_values(f"sha256={digest}") == f"sha256={digest}"


def test_md5_and_short_hashes_not_masked():
    assert scrub_secret_values("5d41402abc4b2a76b9719d911017c592") == (
        "5d41402abc4b2a76b9719d911017c592"
    )


def test_uuid_and_numeric_id_not_masked():
    uuid = "550e8400-e29b-41d4-a716-446655440000"
    assert scrub_secret_values(uuid) == uuid
    assert scrub_secret_values("1234567890123456789012345") == (
        "1234567890123456789012345"
    )


def test_clean_url_and_paths_not_masked():
    url = "https://github.com/deepdarbe/file_activity"
    assert scrub_secret_values(url) == url
    path = r"\\fileserver\Finans\rapor.xlsx"
    assert scrub_secret_values(path) == path
    assert scrub_secret_values("host: mail.local port 25") == (
        "host: mail.local port 25"
    )


# ---------------------------------------------------------------------------
# idempotency — re-running over scrubbed output is a no-op
# ---------------------------------------------------------------------------
def test_idempotent():
    samples = [
        "token ghp_0123456789abcdefABCDEF0123456789abcdef end",
        "https://u:p@host/x",
        "-----BEGIN RSA PRIVATE KEY-----\nzz\n-----END RSA PRIVATE KEY-----",
        "AKIAIOSFODNN7EXAMPLE",
        "blob dGhpc0lzQVZlcnlMb25nQmFzZTY0RW5jb2RlZFNlY3JldEtleTEyMzQ1Ng==",
    ]
    for s in samples:
        once = scrub_secret_values(s)
        twice = scrub_secret_values(once)
        assert once == twice, s
        assert REDACTED in once
