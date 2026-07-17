"""Tests for #340 Faz 4 — VssChecker (VSS recoverability probe).

Pure logic only: parse_json (the PowerShell JSON shapes) and find_recoverable
with an injected shadow list + a monkeypatched os.path.exists. The actual
WMI/PowerShell subprocess is Windows-admin-only and validated on-box, not here.
"""

from __future__ import annotations

import os

from src.scanner.vss_checker import VssChecker

_DEV = r"\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy7"


def test_parse_json_array():
    import json
    txt = json.dumps([{"device": _DEV, "drive": "E:",
                       "created": "2026-07-17 03:00:00"}])
    out = VssChecker.parse_json(txt)
    assert len(out) == 1 and out[0]["drive"] == "E:"


def test_parse_json_single_object_and_junk():
    # ConvertTo-Json emits a bare object (not a 1-element array) for one shadow.
    assert len(VssChecker.parse_json('{"device":"X","drive":"E:"}')) == 1
    assert VssChecker.parse_json("") == []
    assert VssChecker.parse_json(None) == []
    assert VssChecker.parse_json("not json") == []


def test_find_recoverable_hit(monkeypatch):
    shadows = [{"device": _DEV, "drive": "E:",
                "created": "2026-07-17 03:00:00"}]
    hit = _DEV + r"\ortak\a.xlsx"
    monkeypatch.setattr(os.path, "exists", lambda p: p == hit)
    r = VssChecker().find_recoverable(r"E:\ortak\a.xlsx", shadows=shadows)
    assert r["recoverable"] is True
    assert r["shadow_path"] == hit
    assert r["shadow_created"] == "2026-07-17 03:00:00"


def test_find_recoverable_miss(monkeypatch):
    # A shadow exists on E: but the file is not inside it → definitively False.
    shadows = [{"device": _DEV, "drive": "E:"}]
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    r = VssChecker().find_recoverable(r"E:\ortak\gone.xlsx", shadows=shadows)
    assert r["recoverable"] is False


def test_no_shadow_on_volume_is_unknown():
    # Shadows exist, but none for E: → unknown (not a false "not recoverable").
    shadows = [{"device": _DEV, "drive": "C:"}]
    r = VssChecker().find_recoverable(r"E:\ortak\a.xlsx", shadows=shadows)
    assert r["recoverable"] is None


def test_unc_and_bad_paths_are_unknown():
    vc = VssChecker()
    assert vc.find_recoverable(r"\\fs\share\x", shadows=[])["recoverable"] is None
    assert vc.find_recoverable("", shadows=[])["recoverable"] is None
    assert vc.find_recoverable("E:", shadows=[])["recoverable"] is None       # no rel
    assert vc.find_recoverable("E:\\", shadows=[])["recoverable"] is None     # rel empty


def test_drive_match_is_case_insensitive(monkeypatch):
    shadows = [{"device": _DEV, "drive": "e:"}]
    monkeypatch.setattr(os.path, "exists", lambda p: True)
    r = VssChecker().find_recoverable(r"E:\x.txt", shadows=shadows)
    assert r["recoverable"] is True
