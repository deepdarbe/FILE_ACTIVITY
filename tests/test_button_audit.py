"""Static audit of every onclick handler in ``index.html`` (Issue #82, Bug 4).

This is the regression-prevention infrastructure for the button bugs that
were fixed individually in PRs #85, #95 and #96. Instead of waiting for
the next "Konuma Git click does nothing" bug report, we run a static
analyzer over the dashboard HTML on every test run and assert:

* every onclick references a real top-level ``function X(...)`` definition
  (no orphan handlers that would raise ``ReferenceError`` on click);
* no obvious typos like ``onlcick=`` slipped past code review;
* the total handler count never silently drops — a sentinel that catches
  a refactor accidentally deleting wired-up buttons.

The test calls ``scripts.audit_buttons.audit_file`` directly (no subprocess)
so collection stays well under 1 second.

Note on the *existing* orphan baseline: when this audit was first added
seven onclicks pointed at functions that genuinely don't exist in the
file. They are placeholders for unbuilt features (legal-hold modal,
retention attestation modal, PII subject export modal, etc.) and not
something this PR is fixing. We capture them as a known-baseline allow
list so the test passes today, and we'll knock them off one-by-one as
each feature ships. *Anything* beyond the baseline trips the test and
fails CI.
"""

from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.audit_buttons import audit_file  # noqa: E402


# ---------------------------------------------------------------------------
# Baseline / sentinels
# ---------------------------------------------------------------------------

# Bumping below the current count means somebody deleted wired-up buttons —
# usually accidentally (refactor / merge conflict). Bumping it above is fine
# (just update the constant). We intentionally use ``>=`` not ``==`` so a
# minor PR that adds a button doesn't have to touch this file.
MIN_HANDLERS = 150

# Pre-existing orphan onclicks at the time this audit was added. Each
# entry is a known-broken handler the test tolerates so we don't block
# the audit infrastructure on fixing the underlying features. New orphans
# (anything not in this set) MUST fail the test.
KNOWN_ORPHAN_BASELINE: frozenset[str] = frozenset({
    # loadFolderBrowser is the only remaining placeholder — separate scope
    # (generic folder picker for source / archive destination dialogs).
    "loadFolderBrowser",
})


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def report():
    """Run the audit once per module — it's pure I/O + regex, ~10ms."""
    return audit_file()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_audit_runs_and_returns_expected_shape(report):
    """The audit should always produce a JSON-serialisable dict with the
    keys the report page + downstream tests rely on."""
    for key in (
        "handlers_count",
        "unique_functions_count",
        "function_definitions_count",
        "missing_functions",
        "duplicate_definitions",
        "typos",
        "button_audit",
    ):
        assert key in report, f"audit report missing key: {key}"
    assert isinstance(report["button_audit"], list)
    assert isinstance(report["missing_functions"], list)


def test_handler_count_sentinel(report):
    """Catch refactors that accidentally remove half the buttons."""
    assert report["handlers_count"] >= MIN_HANDLERS, (
        f"handler count dropped from baseline {MIN_HANDLERS} "
        f"to {report['handlers_count']} — did a refactor delete buttons?"
    )


def test_no_onclick_typos(report):
    """A typo'd attribute name (``onlcick=``) silently drops the handler in
    every browser; the original button regression in PR #85 was rooted in
    exactly this category of mistake. Forbid the obvious permutations."""
    assert report["typos"] == [], (
        f"onclick-attribute typos detected: {report['typos']}"
    )


def test_no_new_orphan_onclicks(report):
    """Every onclick must call a function that exists in the same file —
    except for the documented baseline of pre-existing orphans."""
    missing = set(report["missing_functions"])
    new_orphans = sorted(missing - KNOWN_ORPHAN_BASELINE)

    assert not new_orphans, (
        f"NEW orphan onclick handlers detected (no matching function "
        f"definition in index.html): {new_orphans}. Either (a) define the "
        f"function, (b) fix the spelling, or (c) — only if the feature is "
        f"still being built — add the name to KNOWN_ORPHAN_BASELINE in "
        f"this file with a tracking comment."
    )


def test_baseline_orphans_still_orphan_or_documented(report):
    """If the baseline shrinks (somebody finally implemented one of the
    placeholder modals), nudge the developer to remove the entry from
    ``KNOWN_ORPHAN_BASELINE`` so we keep the allow-list tight.

    This is a soft check — only fails if the baseline contains entries
    that are no longer orphan, AND we want to ensure those entries get
    removed promptly so a real regression in those names later doesn't
    silently pass the audit.
    """
    missing = set(report["missing_functions"])
    stale = sorted(KNOWN_ORPHAN_BASELINE - missing)
    assert not stale, (
        f"KNOWN_ORPHAN_BASELINE contains entries that are no longer "
        f"orphan: {stale}. Please remove them from the baseline so the "
        f"audit treats any future regression as a real failure."
    )


def test_no_duplicate_function_definitions(report):
    """``function showPage(...) {}`` defined twice means the second copy
    silently overrides the first; that broke a sources-page action in
    early development. Audit this on every test run."""
    assert report["duplicate_definitions"] == [], (
        f"duplicate top-level function definitions: "
        f"{report['duplicate_definitions']}"
    )


def test_inventory_entries_have_required_fields(report):
    """Every audit row must carry the four fields the JSON consumer
    (and the optional report HTML) expects."""
    for entry in report["button_audit"]:
        assert "line" in entry and isinstance(entry["line"], int)
        assert "function" in entry  # may be None for non-call payloads
        assert "args" in entry
        assert "raw" in entry
