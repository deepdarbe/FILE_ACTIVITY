"""Optional checksum / format validators for PII regex hits.

The patterns in ``PiiEngine.DEFAULT_PATTERNS`` are deliberately broad — e.g.
``tckn`` matches *any* 11-digit run and ``credit_card`` any 16-digit run.
That maximises recall but also surfaces false positives (order numbers,
random IDs, phone-looking strings). These validators run the appropriate
checksum / format check on each raw match so ``PiiEngine`` can drop the ones
that *positively* fail — cutting the dominant false-positive classes without
losing recall on real PII.

Pure-Python and optional:
  * ``python-stdnum`` — Luhn (credit card), ISO 7064 mod-97 (IBAN),
    TC kimlik no algorithm.
  * ``phonenumbers`` — Google's libphonenumber port.

When neither is installed every value is treated as plausible, so behaviour
is byte-identical to the pre-validator pipeline. Install via
``pip install -r requirements-accel.txt``.

The recognizer protocol docstring already anticipated this as the
"PresidioBuiltinRecognizer — credit-card Luhn, IBAN checksum" follow-up;
this keeps it a thin post-filter rather than a new backend.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("file_activity.compliance.pii.validators")

_loaded = False
_luhn = None
_iban = None
_tckimlik = None
_phonenumbers = None


def _ensure_loaded() -> None:
    """Import the optional deps once. Missing deps -> validators no-op."""
    global _loaded, _luhn, _iban, _tckimlik, _phonenumbers
    if _loaded:
        return
    _loaded = True
    try:
        from stdnum import iban as iban_mod
        from stdnum import luhn as luhn_mod
        from stdnum.tr import tckimlik as tckimlik_mod
        _luhn, _iban, _tckimlik = luhn_mod, iban_mod, tckimlik_mod
    except ImportError:
        logger.info(
            "python-stdnum not installed; credit_card / iban / tckn checksum "
            "validation disabled (regex hits kept as-is). Install via "
            "'pip install -r requirements-accel.txt'."
        )
    try:
        import phonenumbers as phonenumbers_mod
        _phonenumbers = phonenumbers_mod
    except ImportError:
        logger.info(
            "phonenumbers not installed; phone validation disabled "
            "(regex hits kept as-is)."
        )


def _digits(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit())


def is_plausible(pattern_name: str, value: str) -> bool:
    """Return ``False`` only when a validator *positively rejects* ``value``.

    Unknown patterns (``email``, operator-defined patterns) and missing
    optional deps both return ``True`` — we never drop a hit we cannot
    check. A validator that raises is also treated as plausible (fail-open),
    so a library quirk can never silence a genuine finding.
    """
    _ensure_loaded()
    name = (pattern_name or "").lower()
    val = (value or "").strip()
    if not val:
        return True
    try:
        if "credit" in name:
            return _luhn.is_valid(_digits(val)) if _luhn is not None else True
        if "iban" in name:
            return (
                _iban.is_valid(val.replace(" ", ""))
                if _iban is not None else True
            )
        if name == "tckn" or "tckimlik" in name or "tc_kimlik" in name:
            return (
                _tckimlik.is_valid(_digits(val))
                if _tckimlik is not None else True
            )
        if "phone" in name:
            if _phonenumbers is None:
                return True
            try:
                num = _phonenumbers.parse(val, "TR")
                return _phonenumbers.is_valid_number(num)
            except Exception:
                # Looked like a phone to the regex but isn't parseable/valid.
                return False
    except Exception as e:  # never let a validator bug drop a real hit
        logger.debug("PII validator error (%s=%r): %s", name, val, e)
        return True
    return True


__all__ = ["is_plausible"]
