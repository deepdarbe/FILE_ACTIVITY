"""Pluggable regex backends for the PII engine (issue #64).

The PII engine is the hot loop of the compliance subsystem: every text
file in every scanned source is fed through a fixed set of regular
expressions. On AVX2/AVX-512 hosts Intel Hyperscan can run that fixed
set 10-100x faster than CPython's stdlib ``re`` because it compiles all
patterns into a single multi-pattern automaton and scans bytes in a
single pass.

This module exposes a thin ``PatternBackend`` protocol with two
concrete implementations:

* :class:`ReBackend` — wraps the existing stdlib path. Always available.
* :class:`HyperscanBackend` — uses ``python-hyperscan``. Optional.

The engine must keep producing byte-identical ``pii_findings`` rows
regardless of which backend is selected. To that end both backends
yield the same ``(pattern_name, raw_match)`` tuples; the caller's
existing ``_redact`` step is unchanged. ``re.findall`` semantics
(non-overlapping leftmost matches) are preserved on the Hyperscan
side by collapsing Hyperscan's multiple match endpoints down to one
greedy non-overlapping match per starting position per pattern.

Hyperscan operates exclusively on bytes — text input is encoded to
UTF-8, the matched byte spans are sliced back out and decoded with
``errors="replace"`` so a match that lands across a multi-byte boundary
still produces a sensible (but never crashing) Python string.
"""

from __future__ import annotations

import logging
import re
from typing import Callable, Iterator, Optional, Protocol

logger = logging.getLogger("file_activity.compliance.pii_backends")


# Sentinel: the package version surfaced by /api/compliance/pii/backend.
# Lazily resolved so tests don't pay an import cost when hyperscan is
# absent.
def hyperscan_version() -> Optional[str]:
    try:
        import hyperscan as hs  # type: ignore
    except Exception:
        return None
    return getattr(hs, "__version__", None)


def hyperscan_available() -> bool:
    try:
        import hyperscan  # noqa: F401  type: ignore
        return True
    except Exception:
        return False


class PatternBackend(Protocol):
    """Common interface every regex backend must implement.

    A backend is constructed once per :class:`PiiEngine` instance with
    the full ``{name: regex}`` mapping pre-validated by the caller, and
    then called once per file via :meth:`scan`.
    """

    name: str

    @classmethod
    def compile(cls, patterns: dict[str, str]) -> "PatternBackend":
        ...

    def scan(self, text: str) -> Iterator[tuple[str, str]]:
        """Yield ``(pattern_name, raw_match)`` for every hit in ``text``.

        Order is unspecified; the caller groups by ``pattern_name`` and
        applies its own redaction policy. Implementations must produce
        the same *set* of ``(pattern_name, raw_match)`` tuples as
        :class:`ReBackend` so on-disk findings stay backend-agnostic.
        """
        ...


# ──────────────────────────────────────────────────────────────────────
# Stdlib `re` backend (default, always available)
# ──────────────────────────────────────────────────────────────────────


class ReBackend:
    """Stdlib ``re`` backend — the original PiiEngine code path."""

    name = "re"

    def __init__(self, compiled: dict[str, "re.Pattern[str]"]):
        self._compiled = compiled

    @classmethod
    def compile(cls, patterns: dict[str, str]) -> "ReBackend":
        compiled: dict[str, re.Pattern[str]] = {}
        for name, regex in patterns.items():
            try:
                compiled[name] = re.compile(regex, re.IGNORECASE)
            except re.error as e:
                logger.warning("PII pattern %s ignored (bad regex): %s", name, e)
        return cls(compiled)

    @property
    def patterns(self) -> dict[str, "re.Pattern[str]"]:
        # Exposed so PiiEngine.patterns keeps its existing shape for any
        # downstream code (and tests) that introspect compiled patterns.
        return self._compiled

    def scan(self, text: str) -> Iterator[tuple[str, str]]:
        for name, regex in self._compiled.items():
            for m in regex.findall(text):
                if isinstance(m, tuple):
                    # Grouped patterns (e.g. ``(a)(b)``) come back as
                    # tuples from ``findall``; rejoin to produce the
                    # full match string.
                    yield name, "".join(m)
                else:
                    yield name, m


# ──────────────────────────────────────────────────────────────────────
# Hyperscan backend (optional, accelerated)
# ──────────────────────────────────────────────────────────────────────


class HyperscanBackend:
    """Hyperscan-accelerated backend.

    All patterns compile into a single ``hs.Database`` with
    ``HS_FLAG_SOM_LEFTMOST`` so we get the leftmost start of every
    match (Hyperscan otherwise reports only end positions). After a
    scan we collapse Hyperscan's multiple-match-per-start behaviour
    down to one greedy non-overlapping match per pattern, matching
    ``re.findall`` semantics.

    Bad regexes (anything Hyperscan refuses to compile, e.g.
    backreferences) are dropped with a warning and re-routed to a
    per-pattern stdlib ``re`` fallback inside the same backend, so a
    single unsupported pattern never disables acceleration for the
    rest.
    """

    name = "hyperscan"

    def __init__(self, db, ids_to_names: dict[int, str],
                 fallback: dict[str, "re.Pattern[str]"]):
        # Imported lazily so the module is import-safe when hyperscan
        # isn't installed.
        import hyperscan as hs  # type: ignore

        self._hs = hs
        self._db = db
        self._ids = ids_to_names
        self._fallback = fallback
        # Hyperscan scratch is per-thread; we recreate per-scan to
        # stay safe under the dashboard's executor pool.

    # Tokens that genuinely require Unicode-aware matching. ``HS_FLAG_UCP``
    # changes the meaning of ``\w \d \s \b`` to span the full Unicode
    # property tables; combined with the alternation / quantifiers in
    # typical PII regexes Hyperscan rejects the result with "Pattern is
    # too large" or "\b unsupported in UCP mode" (root cause of #74).
    #
    # The 5 default PII patterns (email, IBAN_TR, phone_TR, TCKN,
    # credit_card) are ASCII-only by design — TR IBANs are literally
    # ``TR`` + 24 ASCII digits, TCKN is 11 ASCII digits, etc. They must
    # not opt into UCP.
    #
    # We therefore restrict UCP opt-in to patterns whose source contains
    # something that *only* makes sense under Unicode mode:
    #
    #   * a non-ASCII literal byte (e.g. an operator pattern matching
    #     ``ş`` or ``Ğ`` directly), or
    #   * a Unicode property escape ``\p{...}`` / ``\P{...}``.
    #
    # ``\w`` etc. on their own do *not* trigger UCP — Python ``re`` is
    # already Unicode-aware for those by default but Hyperscan's
    # ASCII-flavoured ``\w`` is a close-enough match for the operator's
    # regex intent and crucially keeps acceleration on. Operators who
    # genuinely need ``\w`` to match ``ş`` should embed an explicit
    # non-ASCII literal or ``\p{L}`` in their pattern (or run the stdlib
    # backend).
    _UCP_TRIGGER_RE = re.compile(
        r"\\p\{"                  # \p{...} — Unicode property escape
        r"|\\P\{"                 # \P{...}
        r"|[^\x00-\x7f]"          # any non-ASCII literal in the source
    )

    @classmethod
    def _wants_ucp(cls, regex: str) -> bool:
        """Return True if ``regex`` references a Unicode-sensitive token.

        Default PII patterns are ASCII-only so this is False for all of
        them. Operator-supplied patterns that drop a literal non-ASCII
        character or use ``\\p{...}`` opt into UCP and pay the
        (occasionally fatal) compile-time cost knowingly.
        """
        return bool(cls._UCP_TRIGGER_RE.search(regex))

    @classmethod
    def _flags_for(cls, regex: str):
        import hyperscan as hs  # type: ignore

        flags = hs.HS_FLAG_CASELESS | hs.HS_FLAG_SOM_LEFTMOST | hs.HS_FLAG_UTF8
        if cls._wants_ucp(regex):
            flags |= hs.HS_FLAG_UCP
        return flags

    @classmethod
    def compile(cls, patterns: dict[str, str]) -> "HyperscanBackend":
        import hyperscan as hs  # type: ignore

        compilable: list[tuple[str, str, int]] = []
        fallback: dict[str, re.Pattern[str]] = {}
        ids_to_names: dict[int, str] = {}

        for idx, (name, regex) in enumerate(patterns.items()):
            try:
                # Sanity-check that stdlib accepts it too — we want
                # consistent behaviour across backends if hyperscan
                # later rejects.
                re.compile(regex, re.IGNORECASE)
            except re.error as e:
                logger.warning("PII pattern %s ignored (bad regex): %s", name, e)
                continue
            compilable.append((name, regex, idx))

        if not compilable:
            return cls(None, {}, {})

        expressions = [r.encode("utf-8") for _, r, _ in compilable]
        ids = [i for _, _, i in compilable]
        flags = [cls._flags_for(r) for _, r, _ in compilable]
        for name, _, idx in compilable:
            ids_to_names[idx] = name

        db = hs.Database()
        try:
            db.compile(
                expressions=expressions,
                ids=ids,
                elements=len(expressions),
                flags=flags,
            )
        except hs.error as e:  # pragma: no cover - depends on bad regex
            # Rare: hyperscan rejected the whole batch. Drop offenders
            # one at a time and retry; anything still failing falls
            # back to per-pattern stdlib re. We surface the per-pattern
            # downgrade at WARNING so operators can spot a silently
            # de-accelerated production install (issue #74).
            logger.warning("Hyperscan multi-compile failed (%s); "
                           "retrying per-pattern", e)
            ok_exprs: list[bytes] = []
            ok_ids: list[int] = []
            ok_flags: list[int] = []
            for name, regex, idx in compilable:
                pattern_flags = cls._flags_for(regex)
                try:
                    probe = hs.Database()
                    probe.compile(
                        expressions=[regex.encode("utf-8")],
                        ids=[idx],
                        elements=1,
                        flags=[pattern_flags],
                    )
                    ok_exprs.append(regex.encode("utf-8"))
                    ok_ids.append(idx)
                    ok_flags.append(pattern_flags)
                except hs.error as pe:
                    logger.warning(
                        "PII pattern %r could not compile under Hyperscan "
                        "(%s); falling back to stdlib re for this pattern. "
                        "Acceleration disabled for it — investigate the "
                        "regex if this is unexpected.",
                        name, pe,
                    )
                    fallback[name] = re.compile(regex, re.IGNORECASE)
                    ids_to_names.pop(idx, None)
            if ok_exprs:
                db = hs.Database()
                db.compile(expressions=ok_exprs, ids=ok_ids,
                           elements=len(ok_exprs), flags=ok_flags)
            else:
                db = None

        return cls(db, ids_to_names, fallback)

    @property
    def patterns(self) -> dict[str, "re.Pattern[str]"]:
        # PiiEngine and tests may introspect ``engine.patterns`` to know
        # which names compiled. We synthesise compiled re objects so
        # that view stays consistent across backends.
        result: dict[str, re.Pattern[str]] = {}
        for name in self._ids.values():
            result[name] = re.compile("", re.IGNORECASE)
        result.update(self._fallback)
        return result

    def scan(self, text: str) -> Iterator[tuple[str, str]]:
        # Hyperscan is byte-oriented. Encode once, scan, slice the
        # match span back out of the same buffer.
        if not text:
            return
        buf = text.encode("utf-8", errors="replace")

        # (id, start, end) — collected in callback order.
        raw: list[tuple[int, int, int]] = []

        def _on_match(match_id: int, frm: int, to: int,
                       flags: int, ctx) -> None:
            raw.append((int(match_id), int(frm), int(to)))

        if self._db is not None and raw is not None:
            try:
                self._db.scan(buf, match_event_handler=_on_match)
            except self._hs.error as e:  # pragma: no cover - defensive
                logger.debug("Hyperscan scan failed, fallback for this "
                             "input: %s", e)

        # Collapse Hyperscan's multi-endpoint behaviour to ``re.findall``
        # semantics: per pattern_id, sort by (start asc, end desc) then
        # greedily emit non-overlapping leftmost matches.
        per_pattern: dict[int, list[tuple[int, int]]] = {}
        for pid, frm, to in raw:
            per_pattern.setdefault(pid, []).append((frm, to))

        for pid, spans in per_pattern.items():
            spans.sort(key=lambda s: (s[0], -s[1]))
            cursor = -1
            for frm, to in spans:
                if frm < cursor:
                    continue
                if to <= frm:
                    continue
                snippet = buf[frm:to].decode("utf-8", errors="replace")
                yield self._ids[pid], snippet
                cursor = to

        # Per-pattern stdlib fallback for anything Hyperscan refused.
        for name, regex in self._fallback.items():
            for m in regex.findall(text):
                if isinstance(m, tuple):
                    yield name, "".join(m)
                else:
                    yield name, m


# ──────────────────────────────────────────────────────────────────────
# Factory
# ──────────────────────────────────────────────────────────────────────


def make_backend(prefer: str, patterns: dict[str, str]) -> PatternBackend:
    """Construct the best available backend.

    ``prefer`` accepts:

    * ``"auto"`` — use Hyperscan when importable, else stdlib ``re``.
    * ``"hyperscan"`` — force Hyperscan; if unavailable log a warning
      and silently fall back to stdlib ``re`` (matches every other
      optional backend in this project: pyarrow, ldap3, cryptography,
      etc — none of them crash a scan when missing).
    * ``"re"`` — force stdlib ``re``.

    Anything else logs a warning and behaves like ``"auto"``.
    """
    pref = (prefer or "auto").strip().lower()

    if pref == "re":
        return ReBackend.compile(patterns)

    want_hyperscan = pref in ("hyperscan", "auto")
    if not want_hyperscan:
        logger.warning("Unknown pii.engine=%r; defaulting to auto", prefer)
        want_hyperscan = True

    if want_hyperscan and hyperscan_available():
        try:
            return HyperscanBackend.compile(patterns)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("Hyperscan init failed (%s); falling back to re", e)
            return ReBackend.compile(patterns)

    if pref == "hyperscan":
        # Explicitly requested but unavailable — warn loudly.
        logger.warning(
            "compliance.pii.engine=hyperscan but the 'hyperscan' Python "
            "package is not importable; falling back to stdlib re. "
            "Install with: pip install -r requirements-accel.txt"
        )

    return ReBackend.compile(patterns)


__all__ = [
    "PatternBackend",
    "ReBackend",
    "HyperscanBackend",
    "make_backend",
    "hyperscan_available",
    "hyperscan_version",
]
