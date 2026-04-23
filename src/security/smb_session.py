"""PowerShell wrapper to enumerate and close SMB sessions for a user.

Used by the ransomware detector when ``security.ransomware.auto_kill_session``
is enabled and a critical alert fires. The actual destructive operation is
opt-in (defaults to ``dry_run=True``); the dry-run path uses ``-WhatIf``
instead of ``-Force`` so the operator can verify behaviour against a live
file server before flipping the flag.

The module is import-safe on non-Windows platforms — ``kill_user_session``
returns ``{"error": "windows_only", "killed": 0}`` immediately without
attempting to spawn ``powershell``. This keeps unit tests / CI green on
Linux/macOS.
"""

from __future__ import annotations

import logging
import platform
import re
import shutil
import subprocess
from typing import List

logger = logging.getLogger("file_activity.security.smb_session")


# A defensive whitelist for user names — Windows accepts DOMAIN\\user or
# user@domain, but we never want to interpolate arbitrary characters into a
# PowerShell snippet. The detector only ever passes usernames that came in
# through file events, so a strict pattern is fine.
_USERNAME_RE = re.compile(r"^[A-Za-z0-9._\-\\@$]{1,128}$")


def _is_windows() -> bool:
    return platform.system().lower() == "windows"


def _build_ps_script(username: str, dry_run: bool) -> str:
    """Build the PowerShell command body.

    On dry-run we prepend a ``Write-Host "DRY RUN"`` marker so the caller can
    grep stdout, and we use ``-WhatIf`` instead of ``-Force`` so PowerShell
    only reports what it *would* close.
    """
    safe_user = username.replace("'", "''")
    close_args = "-WhatIf" if dry_run else "-Force"
    prefix = 'Write-Host "DRY RUN"; ' if dry_run else ""
    return (
        f"{prefix}"
        f"Get-SmbSession | Where-Object {{$_.ClientUserName -eq '{safe_user}'}} | "
        f"ForEach-Object {{ Close-SmbSession -SessionId $_.SessionId {close_args}; "
        f"Write-Output $_.SessionId }}"
    )


def _parse_session_ids(stdout: str) -> List[str]:
    ids: List[str] = []
    for raw in (stdout or "").splitlines():
        line = raw.strip()
        if not line or line.upper().startswith("DRY RUN"):
            continue
        # Skip WhatIf preamble lines like
        #   "What if: Performing the operation 'Close-SmbSession' ..."
        if line.lower().startswith("what if"):
            continue
        ids.append(line)
    return ids


def kill_user_session(username: str, dry_run: bool = True,
                      timeout_seconds: int = 30) -> dict:
    """Kill all SMB sessions belonging to ``username``.

    Args:
        username: Windows user identifier (DOMAIN\\user or user@domain).
        dry_run: When True (the safe default), runs Close-SmbSession with
            ``-WhatIf`` instead of ``-Force`` and prepends a ``DRY RUN``
            stdout marker.
        timeout_seconds: Hard timeout on the powershell process.

    Returns:
        dict with keys ``killed_session_ids`` (list of strings),
        ``stdout``, ``stderr``, ``dry_run``. On non-Windows platforms returns
        ``{"error": "windows_only", "killed": 0, "dry_run": dry_run}``.
    """
    if not _is_windows():
        return {"error": "windows_only", "killed": 0, "dry_run": dry_run}

    if not username or not _USERNAME_RE.match(username):
        return {
            "error": "invalid_username",
            "killed": 0,
            "killed_session_ids": [],
            "stdout": "",
            "stderr": "",
            "dry_run": dry_run,
        }

    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if not powershell:
        return {
            "error": "powershell_not_found",
            "killed": 0,
            "killed_session_ids": [],
            "stdout": "",
            "stderr": "",
            "dry_run": dry_run,
        }

    script = _build_ps_script(username, dry_run)
    try:
        proc = subprocess.run(
            [powershell, "-NoProfile", "-Command", script],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        logger.warning("SMB kill timed out for %s after %ds", username, timeout_seconds)
        return {
            "error": "timeout",
            "killed": 0,
            "killed_session_ids": [],
            "stdout": e.stdout or "",
            "stderr": e.stderr or "",
            "dry_run": dry_run,
        }
    except OSError as e:
        logger.warning("SMB kill spawn failed for %s: %s", username, e)
        return {
            "error": f"spawn_failed: {e}",
            "killed": 0,
            "killed_session_ids": [],
            "stdout": "",
            "stderr": "",
            "dry_run": dry_run,
        }

    ids = _parse_session_ids(proc.stdout or "")
    return {
        "killed_session_ids": ids,
        "killed": len(ids),
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
        "dry_run": dry_run,
        "returncode": proc.returncode,
    }
