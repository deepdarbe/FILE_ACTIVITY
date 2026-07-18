"""Regression guard for the inline-handler stored-XSS class (CodeQL
js/incomplete-sanitization #1/#2/#3/#315 + siblings found in the same pass).

The dashboard used to build inline ``on*`` handlers like::

    onchange="ddToggleRow('${escapeHtml(fp).replace(/'/g, "\\'")}')"

That escaping is DEAD: escapeHtml already turned ``'`` into ``&#39;``, so the
trailing ``.replace(/'/g, ...)`` matches nothing; when the string is assigned via
innerHTML the HTML parser decodes ``&#39;`` back to ``'`` BEFORE the on* body is
compiled as JS, re-opening the JS string. A file/owner/username containing a
quote (e.g. a planted ``x'-alert(1)-'.txt`` on a monitored share) then executes
in the operator's session.

Fix: carry the value in an escaped ``data-*`` attribute (safe there — an
attribute value is not re-parsed as JS) and read it back via ``this.dataset.*``.

These guards fail if the broken idiom — or an attacker-data function called with a
single-quoted interpolated arg — comes back. Pure text checks; no deps.
"""

from __future__ import annotations

import re
from pathlib import Path

INDEX = Path(__file__).resolve().parents[1] / "src" / "dashboard" / "static" / "index.html"


def _html() -> str:
    return INDEX.read_text(encoding="utf-8")


def test_no_dead_html_escape_then_js_string_escape():
    """No `escapeHtml(...)/_cbEsc(...).replace(/'/g, ...)` idiom anywhere."""
    html = _html()
    dead = re.findall(r"(?:escapeHtml|_cbEsc)\([^)]*\)\.replace\(/'/g", html)
    assert not dead, (
        f"dead HTML-escape-then-JS-string-escape idiom is back "
        f"({len(dead)} site(s)); use a data-* attribute + this.dataset instead")


def test_attacker_data_handlers_use_dataset_not_inline_string():
    """The handlers that carry filesystem/event-log-derived strings must not take
    a single-quoted interpolated arg (`fn('${...}')`) — they read this.dataset.*."""
    html = _html()
    funcs = ("openFolder", "ddToggleRow", "loadUserDetail", "loadUserEfficiency",
             "cbAssignUnmapped", "cbRemoveOwner", "openChargebackOwnerModal",
             "drilldownOwner")
    pat = re.compile(
        r"on(?:click|change)=\"[^\"]*(?:" + "|".join(funcs) + r")\('\$\{")
    bad = pat.findall(html)
    assert not bad, (
        f"attacker-data handler(s) still pass a single-quoted interpolated arg "
        f"({len(bad)}); route the value through an escaped data-* attribute")


def test_archive_owner_helper_present():
    """The owner-table Arsivle button routes through the dataset-reading helper."""
    html = _html()
    assert "function archiveOwnerDrilldown(" in html
    assert "archiveOwnerDrilldown(this)" in html
