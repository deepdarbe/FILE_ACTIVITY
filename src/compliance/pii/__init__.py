"""PII recognizer protocol package (issue #1 Phase 1).

Public surface::

    from src.compliance.pii.recognizer import (
        PiiHit,
        PiiRecognizer,
        ContextRecognizer,
    )
"""

from __future__ import annotations

from src.compliance.pii.recognizer import (  # noqa: F401
    ContextRecognizer,
    PiiHit,
    PiiRecognizer,
)

__all__ = ["PiiHit", "PiiRecognizer", "ContextRecognizer"]
