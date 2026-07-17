"""Check whether a deleted file is still recoverable from a Volume Shadow Copy.

#340 Faz 4. A deletion audit row already answers kim / ne / ne zaman / nereden;
this adds the fifth forensic question — **kurtarilabilir mi?** — by asking
whether a previous version of the file still lives in a VSS snapshot.

Design (mirrors :mod:`frn_resolver`)
------------------------------------
* **Windows-only, POSIX-import-safe.** Every ``subprocess`` / ``os`` call is
  lazy (inside methods) so the module imports cleanly on Linux for unit tests.
* **Best-effort with a hard floor.** Any failure (no VSS, no admin, non-Windows,
  timeout) returns ``recoverable=None`` ("bilinmiyor"); it never raises into the
  request. Only when shadows for the file's volume ARE enumerable but the file is
  absent from all of them do we assert ``recoverable=False``.
* **Locale-independent.** ``vssadmin``'s output is localized (a TR-locale server
  prints Turkish field labels), so we do NOT parse it. We query WMI
  ``Win32_ShadowCopy`` joined to ``Win32_Volume`` via PowerShell and read
  structured JSON — field names and the resolved drive letter are locale-neutral.

Requires elevation (shadow enumeration is admin-gated). Under a non-elevated
dashboard the shadow list comes back empty and every check is reported as
"bilinmiyor", never as a false "not recoverable".
"""

from __future__ import annotations

import logging
import re
from typing import List, Optional

logger = logging.getLogger("file_activity.scanner.vss_checker")

# One PowerShell pass: map each volume's DeviceID(GUID) -> DriveLetter, then emit
# every shadow copy with its device path + resolved drive + creation time. JSON
# out is locale-neutral. $(...) subexpression keeps it valid on Windows PS 5.1.
_PS_LIST_SHADOWS = (
    "$ErrorActionPreference='SilentlyContinue';"
    "$vols=@{};"
    "Get-CimInstance Win32_Volume | ForEach-Object "
    "{ if($_.DriveLetter){ $vols[$_.DeviceID]=$_.DriveLetter } };"
    "Get-CimInstance Win32_ShadowCopy | ForEach-Object "
    "{ [pscustomobject]@{ device=$_.DeviceObject; drive=$vols[$_.VolumeName];"
    " created=$(if($_.InstallDate){$_.InstallDate.ToString('yyyy-MM-dd HH:mm:ss')}) } }"
    " | ConvertTo-Json -Compress"
)


class VssChecker:
    """Best-effort VSS recoverability probe. Cheap to construct; enumerates
    shadows once (cached per instance) on first :meth:`find_recoverable`."""

    def __init__(self):
        self._shadows_cache: Optional[List[dict]] = None

    # ── parsing (pure, unit-tested) ───────────────────────────────────────
    @staticmethod
    def parse_json(text: Optional[str]) -> List[dict]:
        """Parse the PowerShell JSON into a list of shadow dicts.

        ``ConvertTo-Json`` emits a bare object (not a 1-element array) for a
        single shadow, and nothing for zero — both normalise to a list here.
        """
        import json
        text = (text or "").strip()
        if not text:
            return []
        try:
            data = json.loads(text)
        except (ValueError, TypeError):
            return []
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            return []
        return [d for d in data if isinstance(d, dict)]

    # ── shadow enumeration (Windows, best-effort) ─────────────────────────
    def list_shadows(self) -> List[dict]:
        if self._shadows_cache is not None:
            return self._shadows_cache
        self._shadows_cache = self.parse_json(self._run_ps(_PS_LIST_SHADOWS))
        return self._shadows_cache

    def _run_ps(self, script: str) -> str:
        import subprocess
        try:
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
                capture_output=True, text=True, timeout=25,
            )
            return proc.stdout or ""
        except Exception as e:  # non-Windows, timeout, missing powershell…
            logger.debug("vss powershell probe failed: %s", e)
            return ""

    # ── public API ────────────────────────────────────────────────────────
    def find_recoverable(self, file_path: Optional[str],
                         shadows: Optional[List[dict]] = None) -> dict:
        """Is ``file_path`` present in any shadow copy of its volume?

        Returns ``{recoverable, shadow_path, shadow_created}``:
        * ``recoverable=True``  — a shadow contains the file (with its path/time).
        * ``recoverable=False`` — the volume HAS shadows but none contain it.
        * ``recoverable=None``  — unknown: not a drive-letter path, no shadows
          enumerable (no VSS / no admin / non-Windows). Never a false negative.
        """
        result = {"recoverable": None, "shadow_path": None,
                  "shadow_created": None}
        # Only local drive-letter paths are checkable (UNC/E:\ only). ``E:\x``.
        if not file_path or len(file_path) < 3 or file_path[1] != ":":
            return result
        drive = file_path[:2].upper()      # 'E:'
        rel = file_path[3:].lstrip("\\")   # 'dir\file'
        if not rel:
            return result
        # ``rel`` becomes a filesystem lookup below — reject traversal so it
        # cannot escape the shadow-copy root (defence in depth; the endpoint
        # already resolves the path from the DB, not the request).
        if any(seg in ("..", ".") for seg in re.split(r"[\\/]+", rel)):
            return result

        try:
            shadows = shadows if shadows is not None else self.list_shadows()
        except Exception as e:
            logger.debug("vss list_shadows failed: %s", e)
            return result

        relevant = [s for s in (shadows or [])
                    if s.get("device") and (s.get("drive") or "").upper() == drive]
        if not relevant:
            return result  # no shadows on this volume (or couldn't enumerate)

        import os
        result["recoverable"] = False
        for s in relevant:
            candidate = s["device"].rstrip("\\") + "\\" + rel
            try:
                exists = os.path.exists(candidate)
            except Exception:
                exists = False
            if exists:
                result["recoverable"] = True
                result["shadow_path"] = candidate
                result["shadow_created"] = s.get("created")
                break
        return result
