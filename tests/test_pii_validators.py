"""Tests for the PII checksum/format post-filter (validators.is_plausible).

Behaviour tests need the optional ``python-stdnum`` + ``phonenumbers`` deps
and skip without them; the no-op + unknown-pattern tests run regardless so
the deps-absent contract (keep every hit) is always covered.
"""
from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.compliance.pii import validators              # noqa: E402
from src.compliance.pii.validators import is_plausible  # noqa: E402

try:
    import stdnum            # noqa: F401
    import phonenumbers      # noqa: F401
    _HAVE_DEPS = True
except ImportError:
    _HAVE_DEPS = False

requires_deps = pytest.mark.skipif(
    not _HAVE_DEPS, reason="needs python-stdnum + phonenumbers"
)


def _valid_tckn() -> str:
    """Construct a checksum-valid TC kimlik no from a fixed 9-digit seed."""
    d = [7, 1, 2, 3, 4, 5, 6, 7, 8]
    d.append(((d[0] + d[2] + d[4] + d[6] + d[8]) * 7
              - (d[1] + d[3] + d[5] + d[7])) % 10)
    d.append(sum(d) % 10)
    return "".join(map(str, d))


# ── deps-absent: every value stays plausible (no-op) ─────────────────
def test_noop_when_deps_missing(monkeypatch):
    monkeypatch.setattr(validators, "_loaded", True)
    monkeypatch.setattr(validators, "_luhn", None)
    monkeypatch.setattr(validators, "_iban", None)
    monkeypatch.setattr(validators, "_tckimlik", None)
    monkeypatch.setattr(validators, "_phonenumbers", None)
    # Even an obviously-invalid card is kept when validators are unavailable.
    assert is_plausible("credit_card", "1234567890123456") is True
    assert is_plausible("phone_tr", "1234") is True


# ── unknown patterns + empty values are always kept ──────────────────
def test_unknown_and_empty_always_plausible():
    assert is_plausible("email", "x@y.com") is True
    assert is_plausible("some_custom_pattern", "anything") is True
    assert is_plausible("credit_card", "") is True


# ── behaviour (needs the optional deps) ──────────────────────────────
@requires_deps
def test_credit_card_luhn():
    assert is_plausible("credit_card", "4111 1111 1111 1111") is True   # valid Luhn
    assert is_plausible("credit_card", "1234 5678 9012 3456") is False  # fails Luhn


@requires_deps
def test_iban_mod97():
    assert is_plausible("iban_tr", "TR33 0006 1005 1978 6457 8413 26") is True
    assert is_plausible("iban_tr", "TR00 0000 0000 0000 0000 0000 00") is False


@requires_deps
def test_tckn_checksum():
    assert is_plausible("tckn", _valid_tckn()) is True
    assert is_plausible("tckn", "12345678901") is False   # fails checksum


@requires_deps
def test_phone_tr():
    assert is_plausible("phone_tr", "+90 532 123 45 67") is True
    assert is_plausible("phone_tr", "1234") is False
