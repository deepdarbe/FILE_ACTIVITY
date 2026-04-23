"""Tests for issue #64: pluggable PII regex backends.

Verifies the stdlib-re backend and the optional Hyperscan backend
produce the same set of ``(pattern_name, redacted_snippet)`` tuples
on a fixture corpus, so on-disk ``pii_findings`` rows stay
backend-agnostic. Hyperscan-specific tests are skipped automatically
when the ``hyperscan`` package isn't importable.
"""

from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.compliance._pii_backends import (  # noqa: E402
    HyperscanBackend,
    ReBackend,
    hyperscan_available,
    make_backend,
)
from src.compliance.pii_engine import PiiEngine  # noqa: E402
from src.storage.database import Database  # noqa: E402


# A small but deliberately varied corpus that exercises every default
# pattern the engine ships with. Each entry is (label, text).
FIXTURE_CORPUS = [
    ("emails",
     "Reach me at alice@example.com or bob@example.com — also "
     "carol+inbox@some-corp.co for invoices."),
    ("iban",
     "IBAN: TR33 0006 1005 1978 6457 26\n"
     "Spaceless: TR330006100519786457260000 (too long, won't match)\n"
     "Second IBAN TR55 1234 5678 9012 3456 78 in body."),
    ("tckn", "Citizen 12345678901 and another 98765432101."),
    ("phone_tr",
     "Cep: 0532 123 45 67 ve sabit hat 0212 555 11 22 (sabit yok); "
     "ayrica +90 533 444 33 22."),
    ("credit_card",
     "Card: 4111 1111 1111 1111 (Visa test); also 5500-0000-0000-0004."),
    ("multiline",
     "first line\n"
     "alice@example.com second line\n"
     "TR33 0006 1005 1978 6457 26\n"
     "12345678901\n"
     "tail"),
    ("multibyte",
     # Hyperscan without HS_FLAG_UCP treats \w / \d / \s as ASCII-only
     # — matching the documented #74 default. We therefore exercise
     # the codepath that matters in production: an ASCII-only PII
     # token (jane@example.com, an 11-digit TCKN) embedded in a body
     # of multibyte UTF-8 prose. Both backends must agree.
     "Türkiye'den müşteri portalı: jane@example.com\n"
     "Trailing TCKN 12345678901."),
    ("nothing", "lorem ipsum dolor sit amet, no PII here at all"),
]

PATTERNS = dict(PiiEngine.DEFAULT_PATTERNS)


def _scan_to_set(backend, text: str) -> set[tuple[str, str]]:
    """Run a backend over ``text`` and return ``{(name, redacted)}``.

    We compare *redacted* snippets — that's what hits the DB, and it
    masks any per-byte differences in how the backend slices a multi-
    byte boundary while still proving the engine surfaces the same
    matches.
    """
    out: set[tuple[str, str]] = set()
    for name, raw in backend.scan(text):
        out.add((name, PiiEngine._redact(raw)))
    return out


# ──────────────────────────────────────────────────────────────────────
# Cross-backend equivalence
# ──────────────────────────────────────────────────────────────────────


def test_re_backend_finds_expected_patterns():
    backend = ReBackend.compile(PATTERNS)
    all_hits: set[tuple[str, str]] = set()
    for _label, text in FIXTURE_CORPUS:
        all_hits |= _scan_to_set(backend, text)
    found_patterns = {n for n, _ in all_hits}
    # Every default pattern should have at least one hit somewhere in
    # the corpus.
    assert {"email", "iban_tr", "tckn", "credit_card"} <= found_patterns


@pytest.mark.skipif(
    not hyperscan_available(),
    reason="hyperscan package not installed",
)
def test_hyperscan_backend_matches_re_backend():
    re_backend = ReBackend.compile(PATTERNS)
    hs_backend = HyperscanBackend.compile(PATTERNS)
    for label, text in FIXTURE_CORPUS:
        re_hits = _scan_to_set(re_backend, text)
        hs_hits = _scan_to_set(hs_backend, text)
        assert hs_hits == re_hits, (
            f"backend mismatch on fixture {label!r}: "
            f"re-only={re_hits - hs_hits}, hs-only={hs_hits - re_hits}"
        )


@pytest.mark.skipif(
    not hyperscan_available(),
    reason="hyperscan package not installed",
)
def test_default_patterns_compile_natively():
    """Issue #74 regression guard.

    Every default PII pattern is ASCII-only, so under a correctly
    configured Hyperscan backend they must all compile into the native
    multi-pattern database — never fall through to the per-pattern
    stdlib ``re`` route. A non-empty ``_fallback`` here means the
    operator-visible benchmark would silently report ~1x speedup
    (root cause of #74: ``HS_FLAG_UCP`` blowing ``\\w`` up to the full
    Unicode word class so Hyperscan refuses with "Pattern is too
    large"). Linux x86_64 with the ``hyperscan`` wheel installed is
    the supported acceleration target, so this assertion must hold.
    """
    backend = make_backend("hyperscan", PATTERNS)
    assert isinstance(backend, HyperscanBackend), backend
    # No pattern should have been demoted to the per-pattern fallback.
    assert backend._fallback == {}, (
        "default patterns leaked into stdlib re fallback: "
        f"{sorted(backend._fallback)}"
    )
    # Every default pattern name must appear in the native id->name map.
    native_names = set(backend._ids.values())
    assert set(PATTERNS) <= native_names, (
        f"missing from native HS db: {set(PATTERNS) - native_names}"
    )


@pytest.mark.skipif(
    not hyperscan_available(),
    reason="hyperscan package not installed",
)
def test_hyperscan_handles_multibyte_without_crashing():
    """A match that lives entirely in ASCII should still come back as
    plain str even when the surrounding text has Turkish characters."""
    hs = HyperscanBackend.compile(PATTERNS)
    text = "Türkiye müşteri: jane@example.com bitti"
    hits = list(hs.scan(text))
    assert any(name == "email" and "jane@example.com" in raw
                for name, raw in hits), hits


# ──────────────────────────────────────────────────────────────────────
# Factory selection
# ──────────────────────────────────────────────────────────────────────


def test_make_backend_re_returns_re():
    b = make_backend("re", PATTERNS)
    assert b.name == "re"
    assert isinstance(b, ReBackend)


def test_make_backend_auto_returns_best_available():
    b = make_backend("auto", PATTERNS)
    if hyperscan_available():
        assert b.name == "hyperscan"
    else:
        assert b.name == "re"


def test_make_backend_hyperscan_falls_back_when_unavailable(monkeypatch, caplog):
    """When hyperscan is requested explicitly but the package isn't
    importable, the project convention (matches pyarrow / ldap3 /
    cryptography) is to log a warning and fall back to stdlib re —
    never crash a scan because an optional accel dep is missing.
    """
    import src.compliance._pii_backends as backends_mod
    monkeypatch.setattr(backends_mod, "hyperscan_available", lambda: False)
    with caplog.at_level("WARNING",
                          logger="file_activity.compliance.pii_backends"):
        b = backends_mod.make_backend("hyperscan", PATTERNS)
    assert b.name == "re"
    # A clear warning must have been emitted so operators see the
    # downgrade in their logs.
    assert any("hyperscan" in r.message.lower() for r in caplog.records)


def test_make_backend_unknown_pref_warns_and_defaults():
    import src.compliance._pii_backends as backends_mod
    b = backends_mod.make_backend("totally-bogus", PATTERNS)
    # Falls through to "auto" semantics.
    if hyperscan_available():
        assert b.name == "hyperscan"
    else:
        assert b.name == "re"


# ──────────────────────────────────────────────────────────────────────
# PiiEngine integration: same on-disk findings either way
# ──────────────────────────────────────────────────────────────────────


def _new_engine(tmp_path, engine_pref: str):
    db_path = tmp_path / f"pii_{engine_pref}.db"
    db = Database({"path": str(db_path)})
    db.connect()
    cfg = {"compliance": {"pii": {
        "enabled": True,
        "engine": engine_pref,
    }}}
    return PiiEngine(db, cfg), db


def test_pii_engine_uses_configured_backend(tmp_path):
    engine_re, _ = _new_engine(tmp_path, "re")
    assert engine_re.engine_name == "re"

    if hyperscan_available():
        engine_auto, _ = _new_engine(tmp_path, "auto")
        assert engine_auto.engine_name == "hyperscan"


def test_pii_engine_findings_identical_across_backends(tmp_path):
    """Same fixture file → same redacted hits regardless of backend.
    Guarantees the on-disk ``pii_findings`` schema stays stable.
    """
    p = tmp_path / "leak.txt"
    p.write_text(
        "alice@example.com bob@example.com\n"
        "TR33 0006 1005 1978 6457 26\n"
        "12345678901\n",
        encoding="utf-8",
    )

    engine_re, _ = _new_engine(tmp_path, "re")
    re_result = engine_re.scan_file(str(p))

    if hyperscan_available():
        engine_hs, _ = _new_engine(tmp_path, "hyperscan")
        hs_result = engine_hs.scan_file(str(p))
        # Compare per-pattern hit *sets* — order is unspecified across
        # backends, but the contents must match.
        re_keys = set(re_result["hits"].keys())
        hs_keys = set(hs_result["hits"].keys())
        assert re_keys == hs_keys
        for name in re_keys:
            assert set(re_result["hits"][name]) == set(hs_result["hits"][name]), (
                f"backend mismatch on pattern {name!r}: "
                f"re={re_result['hits'][name]} hs={hs_result['hits'][name]}"
            )
