"""Tests for ``src.telemetry.error_reporter`` (issue #118 Phase 1).

Covers the five essentials: disabled-by-default short-circuit, secret
sanitization, UNC path redaction, fingerprint stability and rolling-
window rate limiting. ``urllib.request.urlopen`` is patched so no test
ever touches the network.
"""

from __future__ import annotations

import io
import json
from unittest.mock import patch, MagicMock

import pytest

from src.telemetry.error_reporter import ErrorReporter


def _enabled_config() -> dict:
    return {
        "telemetry": {
            "enabled": True,
            "github": {
                "repo": "owner/repo",
                "token_env": "FAKE_TELEMETRY_TOKEN",
                "label": "auto-report",
            },
            "rate_limit": {"max_per_hour": 10},
            "privacy": {"redact_paths": True},
        }
    }


def _raise_exc(make_exc):
    """Run *make_exc* under a try/except so the returned exception
    carries a real traceback (mirrors the shape FastAPI hands us)."""
    try:
        make_exc()
    except Exception as e:  # noqa: BLE001
        return e
    raise AssertionError("make_exc() did not raise")


def _fake_response(payload: dict):
    raw = json.dumps(payload).encode("utf-8")
    cm = MagicMock()
    cm.__enter__.return_value = io.BytesIO(raw)
    cm.__exit__.return_value = False
    return cm


# --------------------------------------------------------------------- 1
def test_disabled_means_no_capture(monkeypatch):
    monkeypatch.setenv("FAKE_TELEMETRY_TOKEN", "x")
    config = {"telemetry": {"enabled": False}}
    reporter = ErrorReporter(config, "1.0.0")
    exc = _raise_exc(lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    with patch("urllib.request.urlopen") as mock_open:
        result = reporter.capture(exc, {"path": "/x", "method": "GET"})

    assert result is None
    assert mock_open.call_count == 0


# --------------------------------------------------------------------- 2
def test_secrets_stripped_from_context(monkeypatch):
    monkeypatch.setenv("FAKE_TELEMETRY_TOKEN", "tkn")
    reporter = ErrorReporter(_enabled_config(), "1.0.0")
    exc = _raise_exc(lambda: (_ for _ in ()).throw(ValueError("nope")))

    captured = {}

    def fake_urlopen(req, timeout=None):
        captured["body"] = req.data.decode("utf-8")
        return _fake_response({"number": 42})

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        issue_no = reporter.capture(
            exc,
            {
                "path": "/x",
                "password": "hunter2",
                "api_key": "sk-abc",
                "nested": {"signing_key": "topsecret", "ok": "fine"},
            },
        )

    assert issue_no == 42
    body_payload = json.loads(captured["body"])
    body = body_payload["body"]
    assert "***REDACTED***" in body
    assert "hunter2" not in body
    assert "sk-abc" not in body
    assert "topsecret" not in body
    assert "fine" in body  # non-secret values must survive


# --------------------------------------------------------------------- 3
def test_unc_path_redacted(monkeypatch):
    monkeypatch.setenv("FAKE_TELEMETRY_TOKEN", "tkn")
    reporter = ErrorReporter(_enabled_config(), "1.0.0")

    redacted = reporter._redact_paths(r"\\fs01\share\foo")
    assert redacted == r"\\<redacted>\<redacted>\foo"

    home = reporter._redact_paths(r"C:\Users\jdoe\Desktop\x.txt")
    assert home == r"C:\Users\<redacted>\Desktop\x.txt"


# --------------------------------------------------------------------- 4
def test_fingerprint_stable_across_calls(monkeypatch):
    monkeypatch.setenv("FAKE_TELEMETRY_TOKEN", "tkn")
    reporter = ErrorReporter(_enabled_config(), "1.0.0")

    def boom():
        raise RuntimeError("repeat")

    exc1 = _raise_exc(boom)
    exc2 = _raise_exc(boom)
    fp1 = reporter._build_report(exc1, {}).fingerprint
    fp2 = reporter._build_report(exc2, {}).fingerprint
    assert fp1 == fp2

    # Sanity: a different exception type yields a different fingerprint.
    exc3 = _raise_exc(lambda: (_ for _ in ()).throw(ValueError("other")))
    fp3 = reporter._build_report(exc3, {}).fingerprint
    assert fp3 != fp1


# --------------------------------------------------------------------- 5
def test_rate_limit_blocks_after_n(monkeypatch):
    monkeypatch.setenv("FAKE_TELEMETRY_TOKEN", "tkn")
    reporter = ErrorReporter(_enabled_config(), "1.0.0")

    counter = {"n": 0}

    def fake_urlopen(req, timeout=None):
        counter["n"] += 1
        # Distinct issue numbers so each call dedupes uniquely.
        return _fake_response({"number": 1000 + counter["n"]})

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        # 10 distinct exceptions (different lineno → different fp) so
        # we exercise the new-issue path rather than the comment path.
        results = []
        for i in range(11):
            try:
                exec(f"raise RuntimeError('x{i}')", {"__name__": f"m{i}"})
            except RuntimeError as e:
                results.append(reporter.capture(e, {"i": i}))

    accepted = [r for r in results if r is not None]
    rejected = [r for r in results if r is None]
    assert len(accepted) == 10
    assert len(rejected) == 1
    # And the throttled call must not have hit the network.
    assert counter["n"] == 10


# --------------------------------------------------------------------- 6
def test_value_level_secret_scrubbed_under_offlist_key(monkeypatch):
    """#279: a PAT / URL-creds under a key NOT matched by SECRET_KEY_PATTERN
    is masked by VALUE shape before the report leaves the box."""
    monkeypatch.setenv("FAKE_TELEMETRY_TOKEN", "tkn")
    reporter = ErrorReporter(_enabled_config(), "1.0.0")
    exc = _raise_exc(lambda: (_ for _ in ()).throw(ValueError("nope")))

    captured = {}

    def fake_urlopen(req, timeout=None):
        captured["body"] = req.data.decode("utf-8")
        return _fake_response({"number": 99})

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        reporter.capture(
            exc,
            {
                # 'notes' / 'repo_url' are NOT in SECRET_KEY_PATTERN, so the
                # key-name pass leaves them — the value scrub must catch them.
                "notes": "deploy PAT ghp_0123456789abcdefABCDEF0123456789abcdef",
                "repo_url": "https://ci:supersecretpass123@git.example.com/x",
            },
        )

    body = json.loads(captured["body"])["body"]
    assert "ghp_0123456789abcdefABCDEF0123456789abcdef" not in body
    assert "supersecretpass123" not in body
    assert "***REDACTED***" in body


# --------------------------------------------------------------------- 7
def test_value_level_scrub_not_gated_by_redact_paths(monkeypatch):
    """The secret-value scrub must run even when redact_paths is off — a
    secret is not a path, so the path-privacy flag must not gate it."""
    monkeypatch.setenv("FAKE_TELEMETRY_TOKEN", "tkn")
    config = {
        "telemetry": {
            "enabled": True,
            "github": {"repo": "o/r", "token_env": "FAKE_TELEMETRY_TOKEN"},
            "privacy": {"redact_paths": False},  # paths NOT redacted
        }
    }
    reporter = ErrorReporter(config, "1.0.0")
    exc = _raise_exc(lambda: (_ for _ in ()).throw(ValueError("x")))

    captured = {}

    def fake_urlopen(req, timeout=None):
        captured["body"] = req.data.decode("utf-8")
        return _fake_response({"number": 7})

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        reporter.capture(
            exc, {"misc": "tok gho_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ab"}
        )

    body = json.loads(captured["body"])["body"]
    assert "gho_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ab" not in body
    assert "***REDACTED***" in body
