"""Pluggable PII recognizer protocol (issue #1, Phase 1).

This module defines the thin abstraction that every PII detector must
implement so that ``PiiEngine`` can drive an arbitrary pipeline of
recognizers — Hyperscan patterns, stdlib-re patterns, future NER
models, Presidio built-ins, etc.

Architecture (Presidio-inspired)
---------------------------------
::

    class PiiRecognizer(Protocol):
        name: str
        supported_entities: list[str]   # e.g. ['email', 'iban_tr', ...]
        def analyze(self, text: str, context: dict) -> list[PiiHit]: ...

``PiiEngine.scan_file()`` calls every registered recognizer in order,
merges their ``PiiHit`` results and applies the existing redaction +
persistence logic unchanged — so ``pii_findings`` rows are
byte-identical regardless of which recognizers are active.

Shipped recognizers
-------------------
* :class:`ContextRecognizer` — Phase-1 demo.  Boosts the confidence
  score of all other recognizers' hits when the file path looks
  sensitive (contains ``confidential``, ``secret`` or ``personnel``).
  It produces no standalone ``pii_findings`` rows.

Phase-2 recognizers (separate issues)
--------------------------------------
* ``TurkishNERRecognizer`` — savasy/bert-base-turkish-ner-cased
* ``PresidioBuiltinRecognizer`` — credit-card Luhn, IBAN checksum
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


# ──────────────────────────────────────────────────────────────────────
# Value object
# ──────────────────────────────────────────────────────────────────────


@dataclass
class PiiHit:
    """A single PII detection result.

    Attributes
    ----------
    entity_type:
        Pattern / entity name, e.g. ``"email"``, ``"iban_tr"``.
    value:
        Raw (un-redacted) matched string.
    start:
        Character offset of the match start in the scanned text.
        May be 0 when the recognizer does not track positions.
    end:
        Character offset of the match end (exclusive).
        May be 0 when the recognizer does not track positions.
    score:
        Confidence in the range [0.0, 1.0].  Used by future NER passes
        (Phase 2) to decide whether to run a second recognizer; not
        persisted to ``pii_findings`` in Phase 1.
    """

    entity_type: str
    value: str
    start: int
    end: int
    score: float = 0.85


# ──────────────────────────────────────────────────────────────────────
# Protocol
# ──────────────────────────────────────────────────────────────────────


@runtime_checkable
class PiiRecognizer(Protocol):
    """Interface every PII recognizer must satisfy.

    Implementors are free to use any detection strategy — regex,
    NER, checksums, etc.  The engine drives them through a unified
    ``analyze`` call and merges their outputs.

    Attributes
    ----------
    name:
        Short machine-readable identifier, e.g. ``"re"``,
        ``"hyperscan"``, ``"context"``.
    supported_entities:
        Entity types this recognizer can detect.  An empty list means
        the recognizer acts as a pure post-processor (e.g.
        :class:`ContextRecognizer` that only adjusts scores).
    """

    name: str
    supported_entities: list[str]

    def analyze(self, text: str, context: dict) -> list[PiiHit]:
        """Scan *text* and return all detected hits.

        Parameters
        ----------
        text:
            Decoded file content (UTF-8 / Latin-1).
        context:
            Ambient metadata available to the recognizer.
            Guaranteed keys supplied by ``PiiEngine``:

            * ``"file_path"`` — absolute path of the file being scanned.

            Recognizers must treat unknown keys as optional.
        """
        ...  # pragma: no cover


# ──────────────────────────────────────────────────────────────────────
# Shipped recognizers
# ──────────────────────────────────────────────────────────────────────


class ContextRecognizer:
    """Confidence booster based on file-path heuristics (Phase 1 demo).

    When the file path contains a sensitive keyword
    (``confidential``, ``secret`` or ``personnel``) this recognizer
    signals a confidence boost so that future Phase-2 recognizers
    (e.g. NER) can prioritise these files for a second pass.

    It produces **no standalone** ``pii_findings`` rows — hits with
    ``entity_type == "context_signal"`` are intentionally filtered out
    by ``PiiEngine`` before DB persistence, keeping the on-disk schema
    byte-identical to what the pattern-only pipeline produces.

    Adding a new recognizer to the engine is as simple as::

        class MockRecognizer:
            name = "mock"
            supported_entities = ["mock_entity"]
            def analyze(self, text, context):
                return [PiiHit("mock_entity", "MOCK", 0, 4, 1.0)]

    — that is five lines of code, satisfying the acceptance criterion.
    """

    name = "context"

    @property
    def supported_entities(self) -> list[str]:
        """Post-processor only; produces no standalone entity hits."""
        return []

    # Sensitive path keywords that warrant a confidence boost.
    _SENSITIVE_PATH_RE: re.Pattern[str] = re.compile(
        r"confidential|secret|personnel",
        re.IGNORECASE,
    )

    #: Extra confidence added to every hit when the path is sensitive.
    CONFIDENCE_BOOST: float = 0.10

    def analyze(self, text: str, context: dict) -> list[PiiHit]:
        """Return a ``context_signal`` hit when the file path is sensitive.

        The hit's *score* encodes the confidence boost so that
        ``PiiEngine`` can apply it to all other recognizers' hits for
        this file.  The hit is never persisted to ``pii_findings``.
        """
        file_path: str = context.get("file_path") or ""
        if self._SENSITIVE_PATH_RE.search(file_path):
            return [
                PiiHit(
                    entity_type="context_signal",
                    value=file_path,
                    start=0,
                    end=len(file_path),
                    score=self.CONFIDENCE_BOOST,
                )
            ]
        return []


__all__ = [
    "PiiHit",
    "PiiRecognizer",
    "ContextRecognizer",
]
