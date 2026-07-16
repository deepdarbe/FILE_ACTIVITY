"""Pin the #351 pre-update backup prompt contract in the generated update.cmd.

setup-source.ps1 writes an update.cmd here-string on the box. Before #351 that
script ALWAYS ran the 2-3 GB SQLite snapshot, making the operator wait on every
update. #351 puts an E/H prompt in front of it with a deliberately SAFE default:
only an explicit "H" skips the backup; Enter, the 30s timeout, a missing
choice.exe (errorlevel 9009), and any unexpected error all fall through to the
backup path.

This is a SOURCE-TEXT pin (no PowerShell interpreter in CI): the here-string is
matched in deploy/setup-source.ps1. It guards two ways of getting it wrong that
were reasoned through and empirically tested when the change landed:

  1. Using `if errorlevel 2` instead of `if "%errorlevel%"=="2"` — the former
     also matches 9009 (choice.exe absent) and would SKIP the backup, the unsafe
     direction. The exact-equality form skips only on a real "H".
  2. Dropping the snapshot line entirely, or flipping the default to H.
"""

from __future__ import annotations

import pathlib

SETUP = pathlib.Path(__file__).resolve().parent.parent / "deploy" / "setup-source.ps1"


def _text() -> str:
    return SETUP.read_text(encoding="utf-8")


def test_prompt_present_with_safe_default():
    """choice prompt exists and defaults to E (backup) on timeout."""
    t = _text()
    assert "choice /C EH /N /T 30 /D E" in t, "E/H prompt with /D E safe default missing"


def test_skip_uses_exact_equality_not_errorlevel_ge():
    """Only an explicit 'H' (errorlevel exactly 2) skips the backup.

    `if errorlevel 2` would also catch 9009 (choice.exe missing) and skip the
    backup — the unsafe direction. The exact-equality guard is the contract.
    """
    t = _text()
    assert 'if "%errorlevel%"=="2" goto fa_skipbackup' in t, "exact-equality skip guard missing"
    # the unsafe form must NOT be how the skip is gated
    assert "if errorlevel 2 goto fa_skipbackup" not in t, "unsafe `if errorlevel 2` skip guard present"


def test_snapshot_line_preserved():
    """The backup itself must still be invoked on the E path."""
    t = _text()
    assert 'backup_manager snapshot --reason "update" --skip-if-recent-minutes 30' in t


def test_skip_and_resume_labels_exist():
    """Both branch targets exist so the flow rejoins the update either way."""
    t = _text()
    assert ":fa_skipbackup" in t
    assert ":fa_doupdate" in t
    # the update itself (irm install.ps1) runs after the join, regardless of
    # choice. Anchor on the here-string's own interpolated URL — the literal
    # "deploy/install.ps1" also appears in the file's .SYNOPSIS header.
    invoke = "$RepoName/$Branch/deploy/install.ps1"
    assert invoke in t
    assert t.index(":fa_doupdate") < t.index(invoke)
