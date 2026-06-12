"""Value-level secret scrubbing (issue #279 — hardening M2).

Both the diagnostics bundle (``scripts/collect_diag.py``) and the auto
error-reporter (``src/telemetry/error_reporter.py``) historically masked a
value **only when its KEY name** looked sensitive (``password``, ``token``,
``secret``, ...). A secret stored under an off-list key — a credential pasted
into a free-text ``notes:`` field, a PAT embedded in a URL value, creds under
a custom key — sailed straight through and was uploaded in clear when the
operator ran ``fa.cmd diag --upload`` or the error-reporter fired.

This module adds the missing half: a **value-level regex scrub** that masks
the high-signal secret *shapes* regardless of the key they sit under. It is
composed on top of (not a replacement for) the existing key-name masking and
path scrubbing in each module.

Design contract:
    * Mask the MATCH, not the whole value, so a config/log line stays
      readable (``repo: https://***REDACTED***@github.com/o/r`` rather than
      the whole line vanishing).
    * Conservative on generic blobs: the catch-all base64/key-material
      pattern requires a length floor AND mixed character classes, so a
      lowercase-hex SHA/MD5 digest or an all-digit id in normal config is
      NOT mangled. False positives erode trust in the bundle.
    * Idempotent + safe on non-str / None: ``scrub_secret_values`` returns
      the input unchanged when it is not a non-empty ``str``, and re-running
      it over already-scrubbed text is a no-op (the ``***REDACTED***``
      sentinel contains no character the patterns match).
    * stdlib only — imported from a ``scripts/`` standalone tool, so it must
      carry no third-party dependency.
"""

from __future__ import annotations

import re
from typing import Any

# The replacement sentinel — identical to the one both call sites already use
# for key-name redaction, so the output is visually uniform.
REDACTED = "***REDACTED***"


# Each entry is (compiled_pattern, replacement). ``replacement`` may reference
# capture groups so we can keep the structural framing of a value (e.g. the
# ``://`` and ``@`` around URL credentials) while masking only the secret.
#
# Ordering note: the specific high-signal shapes run BEFORE the generic
# base64/hex catch-all. The catch-all's character class excludes ``*``, so a
# value already collapsed to ``***REDACTED***`` by an earlier pattern is never
# re-matched — keeping the whole pass idempotent.
_SECRET_VALUE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    # PEM private-key blocks — the entire armored block, any key type
    # (RSA / EC / OPENSSH / DSA / generic "PRIVATE KEY"). DOTALL so the
    # base64 body across newlines is consumed; non-greedy to stop at the
    # first matching END line.
    (
        re.compile(
            r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"
            r".*?"
            r"-----END [A-Z0-9 ]*PRIVATE KEY-----",
            re.DOTALL,
        ),
        REDACTED,
    ),
    # GitHub tokens: ghp_ (PAT), gho_ (OAuth), ghu_ (user-to-server),
    # ghs_ (server-to-server), ghr_ (refresh). 36+ char body.
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b"), REDACTED),
    # Slack tokens: xoxb / xoxa / xoxp / xoxr / xoxs prefixes.
    (re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"), REDACTED),
    # AWS access key id: AKIA + 16 upper-alnum.
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), REDACTED),
    # Credentials embedded in a URL authority: scheme://user:pass@host .
    # Keep the ``://`` and ``@`` framing; mask only the ``user:pass`` run.
    # The user/pass character class excludes ``/ : @ whitespace`` so the
    # match is tightly bounded to a single authority component.
    (
        re.compile(r"(://)[^/\s:@]+:[^/\s:@]+(@)"),
        r"\1" + REDACTED + r"\2",
    ),
    # Generic base64 / key-material blob — the conservative catch-all.
    # Requires a 40-char floor AND that the run contains at least one
    # lowercase, one uppercase AND one digit (lookaheads). That charset
    # mix deliberately EXCLUDES:
    #   * lowercase-hex digests (SHA-256/SHA-1/MD5 — no uppercase),
    #   * all-decimal ids (no letters),
    #   * UUIDs / paths (too short once dashes/slashes bound the run),
    # so legitimate config values are not over-masked while real base64
    # secrets (which mix case + digits, often with +//=) are caught.
    (
        re.compile(
            r"(?<![A-Za-z0-9+/=_-])"            # left boundary
            r"(?=[A-Za-z0-9+/=_-]{40,})"        # >= 40 chars in the class
            r"(?=[A-Za-z0-9+/=_-]*[a-z])"       # has a lowercase
            r"(?=[A-Za-z0-9+/=_-]*[A-Z])"       # has an uppercase
            r"(?=[A-Za-z0-9+/=_-]*[0-9])"       # has a digit
            r"[A-Za-z0-9+/=_-]{40,}"
            r"(?![A-Za-z0-9+/=_-])"             # right boundary
        ),
        REDACTED,
    ),
)


def scrub_secret_values(text: Any) -> Any:
    """Mask high-signal secret *shapes* anywhere inside *text*.

    Returns *text* unchanged when it is not a non-empty ``str`` (so the
    helper is safe to drop into a recursive walk over arbitrary config /
    context values). Masks only the matched substring, not the whole value,
    and is idempotent: re-running it over its own output is a no-op.
    """
    if not isinstance(text, str) or not text:
        return text
    for pattern, replacement in _SECRET_VALUE_PATTERNS:
        text = pattern.sub(replacement, text)
    return text
