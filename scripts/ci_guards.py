"""CI guards for FILE_ACTIVITY (issue #194 stabilization week / Wave 5).

A single script bundling the lightweight, deterministic checks that should
have caught today's hotfix regressions before they shipped:

  * D-YAML  — duplicate-key detection on config.yaml. PyYAML's default
    loader silently overrides earlier keys; the live ``compliance:``
    duplicate that dropped PII + retention config slipped through
    `.github/workflows/ci.yml` because `yaml.safe_load` reported success
    on a half-loaded document.
  * S-YAML  — schema assertion: the documented top-level keys exist
    under ``compliance`` (pii, retention, legal_hold, standards).
  * LOADERS — every key referenced in the dashboard's ``loaders = {...}``
    dict has a matching function/const declaration. PR #197's
    ``loadPii is not defined`` regression.
  * HTML-BUDGET — the count of raw ``innerHTML =`` writes in
    index.html must not rise above the checked-in baseline. Forces a
    reviewer-visible decision before adding to the XSS / null-crash
    surface (PR #200 / #202 class).
  * D-CHAIN  — zero tolerance for the specific ``document.getElementById
    (...).innerHTML =`` / ``document.querySelector(...).innerHTML =``
    pattern. This is the *exact* shape that produced the #200 / #201 /
    #202 null-deref regressions (chained, no element-existence check).
    Every instance must use ``_setHtmlSafe(id, html)`` which logs and
    no-ops when the element is missing. Baseline 0 after the 2026-05-22
    migration; any new occurrence fails CI.
  * SVC-PARITY — every deploy script that touches the Windows service
    by name must use the same service name. ``update.bat`` and
    ``auto-update.ps1`` drifted to ``FileActivityService`` while
    ``setup-source.ps1`` and ``install_service.ps1`` use
    ``FileActivity`` — silent no-op when update.bat tries to stop the
    service.

Each check prints a GitHub Actions ``::error::`` annotation on failure
and exits the script with a non-zero status. ``--check NAME`` lets the
workflow run a single check at a time; with no flag every check runs.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_YAML = REPO_ROOT / "config.yaml"
INDEX_HTML = REPO_ROOT / "src" / "dashboard" / "static" / "index.html"

# innerHTML write count threshold. Tightened 2026-05-22 after the
# direct-chain migration (18 ``document.getElementById(...).innerHTML =``
# call-sites moved to ``_setHtmlSafe``). The previous 180 was the Wave 3
# baseline; current count is 131. Threshold set above current with a
# small headroom so a single PR adding a handful of writes is reviewed,
# not silently merged. Raise this with reviewer sign-off if a legitimate
# new write site is needed.
INNERHTML_BUDGET = 140

# Windows service name used by the FileActivity service. Set in
# install_service.ps1 / setup-source.ps1; the older update.bat and
# auto-update.ps1 references must agree exactly.
SERVICE_NAME = "FileActivity"

# Files that name the Windows service in a service-management context
# (Stop-Service, Start-Service, sc query, net stop/start).
SVC_PARITY_FILES = [
    "deploy/setup-source.ps1",
    "deploy/install_service.ps1",
    "deploy/uninstall_service.ps1",
    "deploy/auto-update.ps1",
    "deploy/update.bat",
    "deploy/install_tray.ps1",
]


def _err(check: str, msg: str) -> None:
    """Emit a GitHub Actions error annotation."""
    print(f"::error title=ci_guards/{check}::{msg}", file=sys.stderr)


def _ok(check: str, msg: str) -> None:
    print(f"[OK] {check}: {msg}")


# ---------------------------------------------------------------------------
# D-YAML — duplicate-key detection
# ---------------------------------------------------------------------------


def _yaml_no_duplicates_loader():
    """Return a yaml.SafeLoader subclass that raises on duplicate keys.

    PyYAML's default ``construct_mapping`` does not check for duplicates
    — the later key silently overrides. We override
    ``construct_mapping`` to track seen keys per mapping node and raise
    ``yaml.constructor.ConstructorError`` on the first duplicate.
    """
    import yaml

    class _NoDupLoader(yaml.SafeLoader):
        pass

    def _construct_mapping(loader, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            if key in mapping:
                raise yaml.constructor.ConstructorError(
                    None, None,
                    f"duplicate key {key!r} (line {key_node.start_mark.line + 1})",
                    key_node.start_mark,
                )
            mapping[key] = loader.construct_object(value_node, deep=deep)
        return mapping

    _NoDupLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        _construct_mapping,
    )
    return _NoDupLoader


def check_yaml_duplicates() -> bool:
    try:
        import yaml
    except Exception as e:
        _err("D-YAML", f"PyYAML unavailable: {e}")
        return False
    if not CONFIG_YAML.exists():
        _err("D-YAML", f"{CONFIG_YAML} not found")
        return False
    loader_cls = _yaml_no_duplicates_loader()
    with open(CONFIG_YAML, "r", encoding="utf-8") as f:
        try:
            yaml.load(f, Loader=loader_cls)
        except yaml.constructor.ConstructorError as e:
            _err("D-YAML", f"config.yaml: {e}")
            return False
        except yaml.YAMLError as e:
            _err("D-YAML", f"config.yaml parse failure: {e}")
            return False
    _ok("D-YAML", "config.yaml has no duplicate keys")
    return True


# ---------------------------------------------------------------------------
# S-YAML — compliance schema assertion
# ---------------------------------------------------------------------------


COMPLIANCE_REQUIRED_KEYS = ("pii", "retention", "legal_hold", "standards")


def check_yaml_schema() -> bool:
    try:
        import yaml
    except Exception as e:
        _err("S-YAML", f"PyYAML unavailable: {e}")
        return False
    with open(CONFIG_YAML, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        _err("S-YAML", "config.yaml root is not a mapping")
        return False
    compliance = cfg.get("compliance")
    if not isinstance(compliance, dict):
        _err("S-YAML", "config.yaml has no top-level 'compliance' mapping")
        return False
    missing = [k for k in COMPLIANCE_REQUIRED_KEYS if k not in compliance]
    if missing:
        _err(
            "S-YAML",
            "compliance section missing required children: "
            + ", ".join(missing),
        )
        return False
    _ok("S-YAML", "compliance has all required children")
    return True


# ---------------------------------------------------------------------------
# LOADERS — every loaders = {...} value has a matching declaration
# ---------------------------------------------------------------------------


def check_loaders_consistency() -> bool:
    if not INDEX_HTML.exists():
        _err("LOADERS", f"{INDEX_HTML} not found")
        return False
    html = INDEX_HTML.read_text(encoding="utf-8")
    # Find: const loaders = { key: ident, ... } — single-line per the
    # current shape. Regex matches the dict body lazily up to the
    # closing }.
    m = re.search(
        r"const\s+loaders\s*=\s*\{(?P<body>[^}]*)\}",
        html,
    )
    if not m:
        _err("LOADERS", "could not find `const loaders = { ... }`")
        return False
    body = m.group("body")
    # Each entry is `key: identifier` (key may be quoted). Extract the
    # identifier after the colon.
    pairs = re.findall(
        r"(?:'[^']+'|\"[^\"]+\"|\w+)\s*:\s*([A-Za-z_$][A-Za-z0-9_$]*)",
        body,
    )
    if not pairs:
        _err("LOADERS", "loaders dict appears empty")
        return False
    missing: list[str] = []
    for ident in pairs:
        decl = re.search(
            rf"\b(?:async\s+)?function\s+{re.escape(ident)}\s*\("
            rf"|\bconst\s+{re.escape(ident)}\s*="
            rf"|\blet\s+{re.escape(ident)}\s*="
            rf"|\bvar\s+{re.escape(ident)}\s*=",
            html,
        )
        if not decl:
            missing.append(ident)
    if missing:
        _err(
            "LOADERS",
            "loaders reference undeclared function(s): " + ", ".join(missing),
        )
        return False
    _ok("LOADERS", f"all {len(pairs)} loaders declarations resolve")
    return True


# ---------------------------------------------------------------------------
# HTML-BUDGET — innerHTML write-site budget
# ---------------------------------------------------------------------------


def check_innerhtml_budget() -> bool:
    if not INDEX_HTML.exists():
        _err("HTML-BUDGET", f"{INDEX_HTML} not found")
        return False
    html = INDEX_HTML.read_text(encoding="utf-8")
    # Count assignments to .innerHTML or [innerHTML] across the whole
    # file. Both stored-ref and direct-chain patterns hit this.
    count = len(re.findall(r"\.innerHTML\s*=", html))
    if count > INNERHTML_BUDGET:
        _err(
            "HTML-BUDGET",
            f"index.html has {count} innerHTML writes (budget={INNERHTML_BUDGET}). "
            "Migrate new writes to _setHtmlSafe / textContent, "
            "or raise INNERHTML_BUDGET in scripts/ci_guards.py with reviewer "
            "sign-off.",
        )
        return False
    _ok("HTML-BUDGET", f"{count}/{INNERHTML_BUDGET} innerHTML writes")
    return True


# ---------------------------------------------------------------------------
# D-CHAIN — direct getElementById/querySelector innerHTML chain pattern
# ---------------------------------------------------------------------------


# Matches `document.getElementById('foo').innerHTML =` and
# `document.querySelector('.bar').innerHTML =` — the chained shape
# that null-derefs when the element is missing. PR #200 / #201 / #202
# class. Baseline 0 after 2026-05-22 migration.
_DIRECT_CHAIN_PATTERN = re.compile(
    r"document\.(?:getElementById|querySelector)\([^)]+\)\.innerHTML\s*=",
)


def check_innerhtml_direct_chain() -> bool:
    if not INDEX_HTML.exists():
        _err("D-CHAIN", f"{INDEX_HTML} not found")
        return False
    html = INDEX_HTML.read_text(encoding="utf-8")
    offenders: list[tuple[int, str]] = []
    for m in _DIRECT_CHAIN_PATTERN.finditer(html):
        line_no = html.count("\n", 0, m.start()) + 1
        offenders.append((line_no, m.group(0)))
    if offenders:
        for line_no, snippet in offenders:
            _err(
                "D-CHAIN",
                f"index.html:{line_no} uses chained innerHTML "
                f"({snippet[:80]!r}). Replace with "
                f"_setHtmlSafe('id', html) — it null-checks the element "
                "and matches the established codebase pattern (#200/#202).",
            )
        return False
    _ok("D-CHAIN", "no chained document.getElementById(...).innerHTML = writes")
    return True


# ---------------------------------------------------------------------------
# SVC-PARITY — service-name agreement across deploy/*
# ---------------------------------------------------------------------------


# Match Windows service-management call sites where the service NAME
# argument follows immediately. We're looking for the literal
# 'FileActivityService' as a misnamed reference; anything matching
# SERVICE_NAME-prefixed identifiers (FileActivityDashboard, etc.) is fine.
_BAD_SERVICE_PATTERN = re.compile(
    r"\b(?:Stop-Service|Start-Service|Get-Service|net\s+(?:stop|start)|sc\s+query"
    r"|nssm\s+(?:stop|start|install|remove))\b[^\n]*?"
    r"\"?FileActivityService\b\"?",
    re.IGNORECASE,
)


def check_service_name_parity() -> bool:
    offenders: list[tuple[str, int, str]] = []
    for rel in SVC_PARITY_FILES:
        path = REPO_ROOT / rel
        if not path.exists():
            continue
        for i, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if _BAD_SERVICE_PATTERN.search(line):
                offenders.append((rel, i, line.strip()))
    if offenders:
        for rel, ln, line in offenders:
            _err(
                "SVC-PARITY",
                f"{rel}:{ln} uses 'FileActivityService' but the installed service is "
                f"named '{SERVICE_NAME}' (install_service.ps1). This is a silent "
                f"no-op at runtime: {line!r}",
            )
        return False
    _ok("SVC-PARITY", f"service name '{SERVICE_NAME}' consistent across deploy/*")
    return True


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


CHECKS = {
    "yaml-dup": check_yaml_duplicates,
    "yaml-schema": check_yaml_schema,
    "loaders": check_loaders_consistency,
    "html-budget": check_innerhtml_budget,
    "html-chain": check_innerhtml_direct_chain,
    "svc-parity": check_service_name_parity,
}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--check",
        choices=list(CHECKS.keys()) + ["all"],
        default="all",
        help="Run a single check or all (default).",
    )
    args = ap.parse_args(argv)
    if args.check == "all":
        results = [fn() for fn in CHECKS.values()]
    else:
        results = [CHECKS[args.check]()]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
