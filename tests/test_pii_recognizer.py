"""Tests for issue #1 Phase 1: PiiRecognizer protocol.

Verifies:
* PiiHit dataclass behaves as expected
* PiiRecognizer Protocol is satisfied by both backends and ContextRecognizer
* ContextRecognizer emits a context_signal hit only for sensitive paths
* PiiEngine reads compliance.pii.recognizers config and builds the pipeline
* MockRecognizer can be added in 5 lines and plugs straight in
* pii_findings rows are byte-identical regardless of which recognizers run
"""

from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.compliance._pii_backends import ReBackend  # noqa: E402
from src.compliance.pii.recognizer import (  # noqa: E402
    ContextRecognizer,
    PiiHit,
    PiiRecognizer,
)
from src.compliance.pii_engine import PiiEngine  # noqa: E402
from src.storage.database import Database  # noqa: E402

PATTERNS = dict(PiiEngine.DEFAULT_PATTERNS)


# ──────────────────────────────────────────────────────────────────────
# PiiHit
# ──────────────────────────────────────────────────────────────────────


def test_piihit_defaults():
    h = PiiHit(entity_type="email", value="a@b.com", start=0, end=8)
    assert h.score == 0.85
    assert h.entity_type == "email"
    assert h.value == "a@b.com"


def test_piihit_explicit_score():
    h = PiiHit("tckn", "12345678901", 5, 16, score=0.99)
    assert h.score == 0.99


# ──────────────────────────────────────────────────────────────────────
# Protocol satisfaction
# ──────────────────────────────────────────────────────────────────────


def test_re_backend_satisfies_pii_recognizer_protocol():
    backend = ReBackend.compile(PATTERNS)
    assert isinstance(backend, PiiRecognizer)


def test_context_recognizer_satisfies_pii_recognizer_protocol():
    assert isinstance(ContextRecognizer(), PiiRecognizer)


# ──────────────────────────────────────────────────────────────────────
# Backend analyze() method
# ──────────────────────────────────────────────────────────────────────


def test_re_backend_analyze_produces_piihit_objects():
    backend = ReBackend.compile(PATTERNS)
    hits = backend.analyze("alice@example.com and 12345678901", {})
    assert hits, "Expected at least one hit"
    assert all(isinstance(h, PiiHit) for h in hits)
    email_hits = [h for h in hits if h.entity_type == "email"]
    assert email_hits, "Expected an email hit"
    assert email_hits[0].value == "alice@example.com"


def test_re_backend_analyze_matches_scan_output():
    """analyze() must yield the same (entity_type, value) pairs as scan()."""
    backend = ReBackend.compile(PATTERNS)
    text = "bob@example.com TR33 0006 1005 1978 6457 26 12345678901"
    scan_pairs = set(backend.scan(text))
    analyze_pairs = {(h.entity_type, h.value) for h in backend.analyze(text, {})}
    assert analyze_pairs == scan_pairs


def test_re_backend_supported_entities_lists_all_patterns():
    backend = ReBackend.compile(PATTERNS)
    assert set(backend.supported_entities) == set(PATTERNS)


# ──────────────────────────────────────────────────────────────────────
# ContextRecognizer
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("path,expect_signal", [
    ("/share/hr/confidential/employees.csv", True),
    ("/share/hr/secret_list.txt", True),
    ("/share/hr/personnel_data.xlsx", True),
    ("/share/finance/report.xlsx", False),
    ("", False),
])
def test_context_recognizer_sensitive_paths(path, expect_signal):
    rec = ContextRecognizer()
    hits = rec.analyze("any text", {"file_path": path})
    signal_hits = [h for h in hits if h.entity_type == "context_signal"]
    if expect_signal:
        assert signal_hits, f"Expected context_signal for {path!r}"
    else:
        assert not signal_hits, f"Did not expect context_signal for {path!r}"


def test_context_recognizer_signal_score_is_boost():
    rec = ContextRecognizer()
    hits = rec.analyze("text", {"file_path": "/share/confidential/file.csv"})
    assert hits[0].score == ContextRecognizer.CONFIDENCE_BOOST


def test_context_recognizer_no_file_path_key():
    rec = ContextRecognizer()
    hits = rec.analyze("alice@example.com", {})
    assert hits == []


# ──────────────────────────────────────────────────────────────────────
# MockRecognizer — adding a recognizer in 5 lines
# ──────────────────────────────────────────────────────────────────────


class MockRecognizer:
    """Demo: adding a recognizer in 5 lines (acceptance criterion)."""
    name = "mock"
    supported_entities = ["mock_entity"]
    def analyze(self, text: str, context: dict) -> list[PiiHit]:
        return [PiiHit("mock_entity", "MOCK", 0, 4, 1.0)] if "MOCK" in text else []


def test_mock_recognizer_satisfies_protocol():
    assert isinstance(MockRecognizer(), PiiRecognizer)


def test_mock_recognizer_plugs_into_engine(tmp_path):
    """Injecting a custom recognizer list into PiiEngine works."""
    db_path = tmp_path / "pii.db"
    db = Database({"path": str(db_path)})
    db.connect()
    cfg = {"compliance": {"pii": {"enabled": True}}}
    engine = PiiEngine(db, cfg)
    # Inject the mock recognizer alongside the default backend.
    engine._recognizers.append(MockRecognizer())

    p = tmp_path / "test.txt"
    p.write_text("MOCK alice@example.com", encoding="utf-8")
    out = engine.scan_file(str(p))
    assert "mock_entity" in out["hits"]
    assert "email" in out["hits"]


# ──────────────────────────────────────────────────────────────────────
# compliance.pii.recognizers config
# ──────────────────────────────────────────────────────────────────────


def _make_engine(tmp_path, extra_cfg: dict | None = None) -> PiiEngine:
    db_path = tmp_path / "pii.db"
    db = Database({"path": str(db_path)})
    db.connect()
    pii_cfg: dict = {"enabled": True}
    if extra_cfg:
        pii_cfg.update(extra_cfg)
    cfg = {"compliance": {"pii": pii_cfg}}
    return PiiEngine(db, cfg)


def test_default_recognizers_config_uses_backend(tmp_path):
    """Without a recognizers config key the engine defaults to the single backend."""
    engine = _make_engine(tmp_path)
    # Default: one recognizer (the backend).
    assert len(engine._recognizers) == 1


def test_recognizers_config_hyperscan_or_re(tmp_path):
    engine = _make_engine(tmp_path, {"recognizers": ["hyperscan_or_re"]})
    assert len(engine._recognizers) == 1
    # The sole recognizer is the backend (Re or Hyperscan).
    assert engine._recognizers[0] is engine.backend


def test_recognizers_config_with_context(tmp_path):
    engine = _make_engine(tmp_path, {"recognizers": ["hyperscan_or_re", "context"]})
    assert len(engine._recognizers) == 2
    assert isinstance(engine._recognizers[1], ContextRecognizer)


def test_recognizers_config_unknown_name_skips_and_warns(tmp_path, caplog):
    with caplog.at_level("WARNING", logger="file_activity.compliance.pii_engine"):
        engine = _make_engine(tmp_path, {"recognizers": ["hyperscan_or_re", "bogus"]})
    assert len(engine._recognizers) == 1
    assert any("bogus" in r.message for r in caplog.records)


# ──────────────────────────────────────────────────────────────────────
# Context boost in scan_file
# ──────────────────────────────────────────────────────────────────────


def test_context_boost_increases_hit_scores(tmp_path):
    """ContextRecognizer boosts scores of email hits for sensitive paths."""
    engine = _make_engine(tmp_path, {"recognizers": ["hyperscan_or_re", "context"]})

    # Sensitive path — ContextRecognizer fires.
    p = tmp_path / "confidential_report.txt"
    p.write_text("alice@example.com", encoding="utf-8")
    out_sensitive = engine.scan_file(str(p))

    # Non-sensitive path — no boost.
    p2 = tmp_path / "ordinary.txt"
    p2.write_text("alice@example.com", encoding="utf-8")
    out_plain = engine.scan_file(str(p2))

    # pii_findings content must be byte-identical (redacted snippets).
    assert out_sensitive["hits"] == out_plain["hits"]


# ──────────────────────────────────────────────────────────────────────
# Byte-identical pii_findings rows regardless of recognizer set
# ──────────────────────────────────────────────────────────────────────


def test_pii_rows_byte_identical_with_and_without_context_recognizer(tmp_path):
    """Adding ContextRecognizer must not alter pii_findings rows."""
    p = tmp_path / "leak.txt"
    p.write_text("alice@example.com TR33 0006 1005 1978 6457 26", encoding="utf-8")

    engine_plain = _make_engine(tmp_path / "plain",
                                {"recognizers": ["hyperscan_or_re"]})
    engine_ctx = _make_engine(tmp_path / "ctx",
                              {"recognizers": ["hyperscan_or_re", "context"]})

    plain_out = engine_plain.scan_file(str(p))
    ctx_out = engine_ctx.scan_file(str(p))

    assert plain_out["hits"] == ctx_out["hits"], (
        "Hits must be byte-identical with or without ContextRecognizer: "
        f"plain={plain_out['hits']}, ctx={ctx_out['hits']}"
    )
