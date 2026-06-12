#!/usr/bin/env python3
"""Diagnostics bundle collector.

One command that gathers what a remote agent needs to triage a customer
report — version, environment (incl. Windows domain-join, which gates owner
SID resolution), sanitized config, log tail, and read-only DB health
(per-source scan state, owner-resolution ratio, WAL size) — into a single
Markdown report, optionally zipped and optionally posted to a GitHub issue.

Strictly read-only against the database: every query goes through
``db.get_read_cursor()``; this tool never writes a row.

Usage (operator box):
    fa.cmd diag                       # write a zip under data/diagnostics/
    fa.cmd diag --upload              # also open a GitHub issue (needs token)

Standalone:
    python scripts/collect_diag.py --config config/config.yaml [--upload]

The same ``collect()`` powers the dashboard "Tanilama Paketi" download button
(``GET /api/system/diag-bundle``).
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import socket
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Value-level secret scrub shared with the auto error-reporter (issue #279).
# ``collect_diag`` is a stdlib-only standalone script under scripts/, but the
# repo root is on sys.path above, so the src import resolves both when run via
# fa.cmd and from the dashboard endpoint.
from src.utils.secret_scrub import scrub_secret_values  # noqa: E402

# --------------------------------------------------------------------------
# Redaction — secrets are ALWAYS masked, regardless of --no-redact (which only
# governs whether sample owner names / full paths are included).
#
# Two layers, composed: (1) key-name masking — if the KEY looks sensitive the
# whole value is dropped; (2) value-level scrub — for every surviving STRING
# value, mask high-signal secret SHAPES (PAT / PEM / URL-creds / ...) no matter
# the key name, via the shared ``scrub_secret_values`` helper. Layer 2 closes
# issue #279: a credential under an off-list key (free-text notes, a token in a
# URL value) no longer reaches an uploaded GitHub issue in clear.
# --------------------------------------------------------------------------
_SENSITIVE_KEY_HINTS = (
    "password", "passwd", "secret", "token", "api_key", "apikey",
    "client_secret", "private_key", "privatekey", "credential", "smtp_pass",
    "bind_password", "webhook_url", "connection_string", "dsn",
)
_REDACTED = "***REDACTED***"


def _redact_config(obj: Any) -> Any:
    """Recursively mask secrets: by key name AND by value shape (#279)."""
    if isinstance(obj, dict):
        out = {}
        for key, value in obj.items():
            if any(hint in str(key).lower() for hint in _SENSITIVE_KEY_HINTS):
                out[key] = _REDACTED
            else:
                out[key] = _redact_config(value)
        return out
    if isinstance(obj, list):
        return [_redact_config(v) for v in obj]
    if isinstance(obj, str):
        # Off-list key, but the value itself may carry a secret shape.
        return scrub_secret_values(obj)
    return obj


# Path scrubbing for the GitHub-upload path — mirrors the patterns the
# existing telemetry reporter uses (src/telemetry/error_reporter.py) so the
# operator's ``telemetry.privacy.redact_paths`` setting governs both.
_UNC_PATTERN = re.compile(r"\\\\([^\\]+)\\([^\\]+)")
_HOME_PATTERN = re.compile(r"(C:\\Users\\)([^\\]+)", re.I)


def _scrub_paths(text: str) -> str:
    if not isinstance(text, str) or not text:
        return text
    text = _UNC_PATTERN.sub(r"\\\\<redacted>\\<redacted>", text)
    text = _HOME_PATTERN.sub(r"\1<redacted>", text)
    return text


def resolve_github(config: dict, repo_arg: Optional[str] = None,
                   token_arg: Optional[str] = None) -> tuple:
    """Resolve (repo, token, label, scrub_paths) from the shared
    ``telemetry.github`` config — the same surface the auto error-reporter
    uses, so there is exactly one repo/token to configure on the box.

    ``--upload`` is an explicit operator action, so it does NOT require
    ``telemetry.enabled: true`` (that flag only gates automatic capture).
    """
    tele = (config.get("telemetry") or {}) if isinstance(config, dict) else {}
    gh = tele.get("github") or {}
    repo = repo_arg or gh.get("repo")
    token_env = gh.get("token_env", "FILEACTIVITY_TELEMETRY_TOKEN")
    token = (token_arg or os.environ.get(token_env)
             or os.environ.get("FA_GITHUB_TOKEN")
             or os.environ.get("GITHUB_TOKEN"))
    label = gh.get("label", "diagnostics")
    scrub = bool((tele.get("privacy") or {}).get("redact_paths", False))
    return repo, token, label, scrub


def _read_version(base_dir: str) -> str:
    """Mirror main.py: VERSION file sits next to the entrypoint."""
    for candidate in (os.path.join(base_dir, "VERSION"),
                      os.path.join(_REPO_ROOT, "VERSION")):
        try:
            with open(candidate, "r", encoding="utf-8") as handle:
                text = handle.read().strip()
            if text:
                return text
        except OSError:
            continue
    return "unknown"


def _domain_info() -> dict:
    """Best-effort host / domain-join facts.

    Owner SIDs that belong to a domain account only resolve via
    ``LookupAccountSid`` when the box can reach the domain — so a machine
    that is *not* domain-joined (or a service running as a local account)
    is the usual reason owners stay "(Bilinmiyor)" after a rescan. Surfacing
    this here turns a multi-round log chase into a one-glance answer.
    """
    info: dict[str, Any] = {
        "hostname": socket.gethostname(),
        "fqdn": socket.getfqdn(),
        "platform": platform.platform(),
        "is_windows": os.name == "nt",
        "process_user": os.environ.get("USERNAME") or os.environ.get("USER"),
    }
    user_domain = os.environ.get("USERDOMAIN")
    dns_domain = os.environ.get("USERDNSDOMAIN")
    computer = os.environ.get("COMPUTERNAME")
    info["user_domain"] = user_domain
    info["dns_domain"] = dns_domain
    # Heuristic: a DNS domain that differs from the machine name means the
    # logon session is domain-backed. Absence is the red flag for owner resolve.
    if dns_domain:
        info["domain_joined"] = True
    elif user_domain and computer and user_domain.upper() != computer.upper():
        info["domain_joined"] = True
    elif info["is_windows"]:
        info["domain_joined"] = False
    else:
        info["domain_joined"] = None  # not applicable off Windows
    return info


def _tail(path: str, n_lines: int) -> tuple[str, dict]:
    """Return the last ``n_lines`` of a text file plus small metadata.

    Reads a bounded window from the end so a multi-GB log never loads whole.
    """
    meta: dict[str, Any] = {"path": path, "exists": False}
    if not path or not os.path.exists(path):
        return "", meta
    meta["exists"] = True
    size = os.path.getsize(path)
    meta["size_bytes"] = size
    window = min(size, max(n_lines * 400, 65536))
    try:
        with open(path, "rb") as handle:
            if size > window:
                handle.seek(size - window)
            chunk = handle.read()
    except OSError as exc:
        meta["error"] = str(exc)
        return "", meta
    text = chunk.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if size > window and lines:
        lines = lines[1:]  # drop the partial first line
    tail = lines[-n_lines:]
    meta["returned_lines"] = len(tail)
    return "\n".join(tail), meta


def _file_size(path: str) -> Optional[int]:
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def _fmt_size(num: Optional[int]) -> str:
    if num is None:
        return "n/a"
    value = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} TB"


# --------------------------------------------------------------------------
# Database section — strictly read-only, every block guarded so a missing
# table or odd customer state still yields a useful bundle.
# --------------------------------------------------------------------------
def _collect_database(db, db_path: str, redact: bool) -> dict:
    out: dict[str, Any] = {"errors": []}

    out["db_size_bytes"] = _file_size(db_path)
    out["wal_size_bytes"] = _file_size(db_path + "-wal")
    out["shm_size_bytes"] = _file_size(db_path + "-shm")

    # Per-source scan state + owner-resolution ratio.
    sources_report: list[dict] = []
    try:
        with db.get_read_cursor() as cur:
            cur.execute(
                "SELECT id, name, unc_path, enabled FROM sources ORDER BY id"
            )
            sources = [dict(r) for r in cur.fetchall()]
        for src in sources:
            entry: dict[str, Any] = {
                "id": src["id"],
                "name": src["name"],
                "enabled": bool(src["enabled"]),
            }
            if not redact:
                entry["unc_path"] = src["unc_path"]
            try:
                with db.get_read_cursor() as cur:
                    cur.execute(
                        "SELECT id, status, current_phase, started_at, "
                        "completed_at, total_files, errors FROM scan_runs "
                        "WHERE source_id=? ORDER BY started_at DESC LIMIT 1",
                        (src["id"],),
                    )
                    scan = cur.fetchone()
                if scan:
                    scan = dict(scan)
                    entry["latest_scan"] = {
                        "id": scan["id"],
                        "status": scan["status"],
                        "phase": scan.get("current_phase"),
                        "started_at": scan["started_at"],
                        "completed_at": scan["completed_at"],
                        "total_files": scan["total_files"],
                        "errors": scan["errors"],
                    }
                    # Owner-resolution ratio for that scan (single pass).
                    with db.get_read_cursor() as cur:
                        cur.execute(
                            "SELECT COUNT(*) AS total, "
                            "SUM(CASE WHEN owner IS NULL OR owner='' "
                            "THEN 1 ELSE 0 END) AS unresolved "
                            "FROM scanned_files WHERE scan_id=?",
                            (scan["id"],),
                        )
                        row = dict(cur.fetchone())
                    total = row["total"] or 0
                    unresolved = row["unresolved"] or 0
                    entry["owner_resolution"] = {
                        "rows": total,
                        "unresolved": unresolved,
                        "resolved": total - unresolved,
                        "unresolved_pct": round(100.0 * unresolved / total, 1)
                        if total else None,
                    }
                    if not redact and (total - unresolved) > 0:
                        with db.get_read_cursor() as cur:
                            cur.execute(
                                "SELECT owner, COUNT(*) AS c FROM scanned_files "
                                "WHERE scan_id=? AND owner IS NOT NULL "
                                "AND owner<>'' GROUP BY owner "
                                "ORDER BY c DESC LIMIT 10",
                                (scan["id"],),
                            )
                            entry["owner_resolution"]["top_owners"] = [
                                dict(r) for r in cur.fetchall()
                            ]
                else:
                    entry["latest_scan"] = None
            except Exception as exc:  # noqa: BLE001 - diag must not abort
                entry["scan_error"] = str(exc)
            sources_report.append(entry)
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"sources: {exc}")
    out["sources"] = sources_report

    # PII findings count (the page that hung in #8/#9 triage).
    try:
        with db.get_read_cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM pii_findings")
            out["pii_findings_count"] = dict(cur.fetchone())["c"]
    except Exception as exc:  # noqa: BLE001
        out["pii_findings_count"] = None
        out["errors"].append(f"pii_findings: {exc}")

    # Lightweight table inventory.
    try:
        with db.get_read_cursor() as cur:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            out["tables"] = [r["name"] for r in cur.fetchall()]
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"tables: {exc}")

    return out


def collect(db, config: dict, *, log_lines: int = 200, redact: bool = True,
            base_dir: Optional[str] = None) -> dict:
    """Gather the full diagnostics snapshot. Read-only; never writes."""
    base_dir = base_dir or _REPO_ROOT
    general = (config.get("general") or {}) if isinstance(config, dict) else {}
    db_conf = (config.get("database") or {}) if isinstance(config, dict) else {}
    db_path = os.path.abspath(db_conf.get("path", "data/file_activity.db"))
    log_file = os.path.abspath(general.get("log_file", "logs/file_activity.log"))

    log_text, log_meta = _tail(log_file, log_lines)

    scanner_cfg = (config.get("scanner") or {}) if isinstance(config, dict) else {}
    parquet_cfg = scanner_cfg.get("parquet_staging") or {}
    audit_cfg = (config.get("audit") or {}) if isinstance(config, dict) else {}
    key_flags = {
        "scanner.read_owner": scanner_cfg.get("read_owner", "(absent -> code default False)"),
        "scanner.parquet_staging.enabled": parquet_cfg.get(
            "enabled", "(absent -> code default True)"
        ),
        "audit.chain_enabled": audit_cfg.get("chain_enabled", False),
        "database.path": db_path,
    }

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "version": _read_version(base_dir),
            "redacted": redact,
            "tool": "collect_diag",
        },
        "environment": _domain_info(),
        "key_flags": key_flags,
        "config": _redact_config(config) if isinstance(config, dict) else {},
        "log": {**log_meta, "tail": log_text},
        "database": _collect_database(db, db_path, redact),
    }


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------
def render_markdown(diag: dict) -> str:
    meta = diag.get("meta", {})
    env = diag.get("environment", {})
    flags = diag.get("key_flags", {})
    dbd = diag.get("database", {})
    lines: list[str] = []
    add = lines.append

    add(f"# FILE ACTIVITY — Diagnostics bundle")
    add("")
    add(f"- Generated: `{meta.get('generated_at')}`")
    add(f"- Version: `{meta.get('version')}`")
    add(f"- Redacted: `{meta.get('redacted')}`")
    add("")

    add("## Environment")
    add("")
    add(f"- Host: `{env.get('hostname')}` (fqdn `{env.get('fqdn')}`)")
    add(f"- Platform: `{env.get('platform')}`")
    add(f"- Process user: `{env.get('process_user')}` / domain `{env.get('user_domain')}`")
    joined = env.get("domain_joined")
    joined_str = {True: "YES", False: "NO (!)", None: "n/a"}.get(joined, str(joined))
    add(f"- **Domain-joined: {joined_str}**  (owner SID resolution needs domain reach)")
    add("")

    add("## Key config flags")
    add("")
    for key, value in flags.items():
        add(f"- `{key}` = `{value}`")
    add("")

    add("## Database")
    add("")
    add(f"- DB size: {_fmt_size(dbd.get('db_size_bytes'))}")
    wal = dbd.get("wal_size_bytes")
    wal_note = "  ← **>5 GB sustained = WAL bloat, investigate readers**" if (wal or 0) > 5 * 1024**3 else ""
    add(f"- WAL size: {_fmt_size(wal)}{wal_note}")
    add(f"- PII findings: {dbd.get('pii_findings_count')}")
    add("")
    add("### Sources")
    add("")
    for src in dbd.get("sources", []):
        add(f"#### {src.get('name')} (id {src.get('id')}, enabled={src.get('enabled')})")
        scan = src.get("latest_scan")
        if not scan:
            add("- No scan runs.")
            add("")
            continue
        add(f"- Latest scan: id `{scan['id']}`, status `{scan['status']}`, "
            f"phase `{scan.get('phase')}`")
        add(f"- Started `{scan['started_at']}` / completed `{scan['completed_at']}`")
        add(f"- total_files `{scan['total_files']}`, errors `{scan['errors']}`")
        owner = src.get("owner_resolution")
        if owner:
            add(f"- **Owner resolution: {owner['resolved']}/{owner['rows']} resolved, "
                f"{owner['unresolved']} unresolved ({owner['unresolved_pct']}%)**")
            tops = owner.get("top_owners")
            if tops:
                pretty = ", ".join(f"{t['owner']} ({t['c']})" for t in tops[:5])
                add(f"- Top owners: {pretty}")
        if src.get("scan_error"):
            add(f"- scan_error: `{src['scan_error']}`")
        add("")

    if dbd.get("errors"):
        add("### DB collection errors")
        add("")
        for err in dbd["errors"]:
            add(f"- `{err}`")
        add("")

    add("## Config (sanitized)")
    add("")
    add("```yaml")
    add(_dump_yaml(diag.get("config", {})))
    add("```")
    add("")

    log = diag.get("log", {})
    add(f"## Log tail (`{log.get('path')}`, {log.get('returned_lines', 0)} lines)")
    add("")
    add("```")
    add(log.get("tail", "") or "(log not found)")
    add("```")
    return "\n".join(lines)


def _dump_yaml(obj: Any) -> str:
    try:
        import yaml  # type: ignore
        return yaml.safe_dump(obj, allow_unicode=True, sort_keys=False).strip()
    except Exception:  # noqa: BLE001 - fall back to JSON if pyyaml missing
        return json.dumps(obj, indent=2, ensure_ascii=False, default=str)


# --------------------------------------------------------------------------
# Bundle + upload
# --------------------------------------------------------------------------
def build_bundle_bytes(diag: dict, markdown: str) -> bytes:
    """Build the diagnostics zip in memory (used by the dashboard endpoint)."""
    import io

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("report.md", markdown)
        zf.writestr("diag.json",
                    json.dumps(diag, indent=2, ensure_ascii=False, default=str))
        log_tail = (diag.get("log") or {}).get("tail", "")
        if log_tail:
            zf.writestr("log_tail.txt", log_tail)
    return buffer.getvalue()


def build_bundle(diag: dict, markdown: str, out_dir: str) -> Path:
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    bundle = Path(out_dir) / f"diag-{stamp}.zip"
    bundle.write_bytes(build_bundle_bytes(diag, markdown))
    return bundle


def upload_to_github(markdown: str, repo: str, token: str, *,
                     title: Optional[str] = None,
                     label: str = "diagnostics") -> str:
    """POST the report as a GitHub issue. Returns the issue URL.

    Uses only the standard library so the operator box needs no extra deps.
    """
    import urllib.request

    if not repo or not token:
        raise ValueError("github repo and token are required for --upload")
    title = title or f"Diagnostics {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body = markdown
    if len(body) > 60000:  # GitHub issue body cap is ~65k
        body = body[:60000] + "\n\n*(truncated — full bundle in the zip)*"
    payload = json.dumps({
        "title": title,
        "body": body,
        "labels": [label] if label else [],
    }).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/issues",
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "file-activity-diag",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as resp:  # noqa: S310 - fixed host
        data = json.loads(resp.read().decode("utf-8"))
    return data.get("html_url", "")


# --------------------------------------------------------------------------
# Standalone entry point
# --------------------------------------------------------------------------
def _build_db_and_config(config_path: str):
    """Construct config + connected Database the same way main.py does."""
    from src.utils.config_loader import load_config
    from src.storage.database import Database

    config = load_config(config_path)
    db_conf = config.get("database", {}) or {}
    db_conf["_config_path"] = config_path
    db = Database(db_conf)
    db.connect()
    return db, config


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Collect a diagnostics bundle.")
    parser.add_argument("--config", "-c", default="config.yaml",
                        help="Path to config.yaml")
    parser.add_argument("--out", default=None,
                        help="Output dir for the zip (default: <data>/diagnostics)")
    parser.add_argument("--lines", type=int, default=200,
                        help="Log tail line count (default 200)")
    parser.add_argument("--no-redact", action="store_true",
                        help="Include sample owner names / source UNC paths")
    parser.add_argument("--upload", action="store_true",
                        help="Also open a GitHub issue with the report")
    parser.add_argument("--repo", default=None,
                        help="owner/name (default: telemetry.github.repo)")
    parser.add_argument("--token", default=None,
                        help="GitHub token (default: $FILEACTIVITY_TELEMETRY_TOKEN)")
    args = parser.parse_args(argv)

    db, config = _build_db_and_config(args.config)
    try:
        diag = collect(db, config, log_lines=args.lines, redact=not args.no_redact)
    finally:
        try:
            db.close()
        except Exception:  # noqa: BLE001
            pass

    markdown = render_markdown(diag)
    db_conf = config.get("database", {}) or {}
    data_dir = os.path.dirname(os.path.abspath(db_conf.get("path", "data/file_activity.db")))
    out_dir = args.out or os.path.join(data_dir, "diagnostics")
    bundle = build_bundle(diag, markdown, out_dir)
    print(f"[OK] Diagnostics bundle written: {bundle}")

    if args.upload:
        repo, token, label, scrub = resolve_github(config, args.repo, args.token)
        if not repo or not token:
            print("[WARN] --upload skipped: set telemetry.github.repo + "
                  "$FILEACTIVITY_TELEMETRY_TOKEN (or --repo/--token)",
                  file=sys.stderr)
            return 0
        # Path scrub is operator-gated (redact_paths); the secret-value scrub
        # is ALWAYS applied on the upload path (#279) — it also catches a token
        # that surfaced in the log tail, which the per-config scrub never sees.
        body = _scrub_paths(markdown) if scrub else markdown
        body = scrub_secret_values(body)
        try:
            url = upload_to_github(body, repo, token, label=label)
            print(f"[OK] Posted GitHub issue: {url}")
        except Exception as exc:  # noqa: BLE001
            print(f"[ERROR] GitHub upload failed: {exc}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
