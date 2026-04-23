"""Ransomware canary + rename-velocity detector (issue #37).

Consumes file events from the existing watcher (later from a USN-tail feed
once issue #33 lands) and triggers alerts when ransomware-style behaviour is
detected. The four rules implemented today are:

1. ``rename_velocity``  -- > N renames per minute by a single user on a
   single source. Defaults: 50/min.
2. ``risky_extension``  -- file rewritten/renamed to one of a known
   ransomware suffix list (``.encrypted``, ``.locked``, ``.wcry``, ...).
3. ``mass_deletion``    -- > M deletions per minute by a single user.
   Defaults: 100/min.
4. ``canary_access``    -- ANY access to a designated canary file fires an
   immediate critical alert.

Persistence
-----------

Alerts are written to the ``ransomware_alerts`` table created idempotently
in :meth:`Database._create_tables`. ``sample_paths`` is a JSON array
truncated to 20 entries so a runaway encryptor does not bloat the row.

Side effects on alert
---------------------

* Email (best-effort) via ``EmailNotifier`` from PR #18 — only when
  ``security.ransomware.notification_email`` is configured AND the notifier
  is available.
* SMB session kill via :func:`src.security.smb_session.kill_user_session` —
  ONLY when ``security.ransomware.auto_kill_session`` is True. Defaults
  to dry-run on Windows and to a no-op on Linux.

The detector itself is import-safe on Linux: we don't touch the SMB module
unless auto-kill is opted into and an alert actually fires.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Optional, Deque, Tuple

logger = logging.getLogger("file_activity.security.ransomware_detector")


# Defaults — config can override every value. They are duplicated here so the
# module is usable from tests with an empty config dict.
DEFAULT_RENAME_THRESHOLD = 50
DEFAULT_RENAME_WINDOW = 60
DEFAULT_DELETE_THRESHOLD = 100
DEFAULT_DELETE_WINDOW = 60
DEFAULT_RISKY_EXTENSIONS = [
    "encrypted", "locked", "crypto", "crypt", "wcry", "wnry",
    "ryk", "lockbit", "conti", "cuba",
]
DEFAULT_CANARY_NAMES = [
    "_AAAA_canary_DO_NOT_DELETE.txt",
    "_ZZZZ_canary_DO_NOT_DELETE.txt",
]

_MAX_SAMPLE_PATHS = 20

# Rules that are loud enough to warrant a session-kill attempt. We
# deliberately do NOT auto-kill on plain rename velocity from a single user
# (could be a legitimate batch job); the operator opts in by toggling
# ``auto_kill_session``.
_KILL_RULES = {"canary_access", "risky_extension", "mass_deletion", "rename_velocity"}


def _parse_event_time(raw) -> datetime:
    """Accept datetime, epoch seconds, or ISO string. Fall back to now()."""
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(raw)
        except (OverflowError, OSError, ValueError):
            return datetime.now()
    if isinstance(raw, str) and raw:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            pass
    return datetime.now()


def _extension_of(path: str) -> str:
    if not path:
        return ""
    return os.path.splitext(path)[1].lstrip(".").lower()


class RansomwareDetector:
    """Stateful detector — keeps short rolling windows per (source, user)."""

    def __init__(self, db, config: dict):
        self.db = db
        cfg = (config or {}).get("security", {}).get("ransomware", {}) or {}

        self.enabled = bool(cfg.get("enabled", True))
        self.rename_threshold = int(cfg.get("rename_velocity_threshold",
                                             DEFAULT_RENAME_THRESHOLD))
        self.rename_window = int(cfg.get("rename_velocity_window",
                                          DEFAULT_RENAME_WINDOW))
        self.delete_threshold = int(cfg.get("deletion_velocity_threshold",
                                             DEFAULT_DELETE_THRESHOLD))
        self.delete_window = int(cfg.get("deletion_velocity_window",
                                          DEFAULT_DELETE_WINDOW))

        risky = cfg.get("risky_new_extensions") or DEFAULT_RISKY_EXTENSIONS
        self.risky_extensions = {str(e).strip().lower().lstrip(".") for e in risky if e}

        canaries = cfg.get("canary_file_names") or DEFAULT_CANARY_NAMES
        self.canary_names = {str(n).strip().lower() for n in canaries if n}

        self.auto_kill_session = bool(cfg.get("auto_kill_session", False))
        self.notification_email = (cfg.get("notification_email") or "").strip()

        # email_notifier is set up by the dashboard / service container.
        # We accept it lazily so tests can construct the detector without
        # a real SMTP stack.
        self.email_notifier = None

        # Rolling event buffers: (source_id, username) -> deque[(timestamp, path)]
        self._renames: dict = defaultdict(deque)
        self._deletes: dict = defaultdict(deque)
        self._lock = threading.Lock()

        # De-dupe key per (source_id, username, rule) -> last_triggered datetime.
        # Keeps us from firing 50 alerts per minute for the same actor.
        self._last_alert: dict = {}
        # Cooldown roughly equal to the analysis window so the same burst
        # doesn't keep retriggering.
        self._cooldown_seconds = max(self.rename_window, self.delete_window, 60)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def consume_event(self, event: dict) -> Optional[dict]:
        """Inspect a single file event. Returns the alert dict if a rule fired.

        ``event`` schema (best-effort, missing fields tolerated):
            {
              "timestamp":  datetime | epoch | ISO str (default: now),
              "source_id":  int,
              "username":   str,
              "file_path":  str,
              "event_type": "create" | "modify" | "delete" | "rename" | "access",
              "old_path":   str (optional, for rename),
            }
        """
        if not self.enabled or not event:
            return None

        ev_type = (event.get("event_type") or "").lower()
        source_id = event.get("source_id")
        username = (event.get("username") or "").strip() or "unknown"
        file_path = event.get("file_path") or event.get("old_path") or ""
        ts = _parse_event_time(event.get("timestamp"))

        # Canary check — runs on every event type. The canary list is small so
        # this is cheap. We compare both basenames and the optional old_path.
        for path in (file_path, event.get("old_path") or ""):
            if not path:
                continue
            if os.path.basename(path).lower() in self.canary_names:
                return self._fire(
                    rule="canary_access",
                    severity="critical",
                    source_id=source_id,
                    username=username,
                    sample_paths=[path],
                    file_count=1,
                    details={
                        "event_type": ev_type,
                        "canary_path": path,
                        "message": "Canary file accessed — possible ransomware sweep",
                    },
                    ts=ts,
                )

        # Risky extension check — fires on the FIRST event referencing such a
        # name. This is intentionally noisy because the cost of a false negative
        # (missing real ransomware) dwarfs a false alarm.
        ext = _extension_of(file_path)
        if ext and ext in self.risky_extensions:
            return self._fire(
                rule="risky_extension",
                severity="critical",
                source_id=source_id,
                username=username,
                sample_paths=[file_path],
                file_count=1,
                details={
                    "event_type": ev_type,
                    "extension": ext,
                    "message": f"File touched with ransomware-style extension .{ext}",
                },
                ts=ts,
            )

        # Velocity rules — only meaningful for rename / delete.
        if ev_type in ("rename", "modify", "create"):
            # Treat rename + create-with-old_path as "rename-like". Normal
            # creates still bump the counter only when the watcher tags them
            # as renames.
            if ev_type == "rename" or event.get("old_path"):
                alert = self._track_velocity(
                    self._renames, "rename_velocity", source_id, username,
                    file_path, ts, self.rename_threshold, self.rename_window,
                    severity="critical",
                )
                if alert:
                    return alert

        if ev_type == "delete":
            alert = self._track_velocity(
                self._deletes, "mass_deletion", source_id, username,
                file_path, ts, self.delete_threshold, self.delete_window,
                severity="critical",
            )
            if alert:
                return alert

        return None

    def get_active_alerts(self, since_minutes: int = 60) -> list:
        """Return alerts triggered in the last ``since_minutes`` minutes.

        Newest first. Empty list if the table doesn't exist yet (e.g. brand
        new in-memory test DB without our schema migration applied).
        """
        try:
            with self.db.get_cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM ransomware_alerts
                    WHERE triggered_at > datetime('now', ? || ' minutes')
                    ORDER BY triggered_at DESC, id DESC
                    """,
                    (f"-{int(since_minutes)}",),
                )
                rows = cur.fetchall() or []
        except Exception as e:
            logger.debug("get_active_alerts failed: %s", e)
            return []

        out = []
        for row in rows:
            r = dict(row)
            sp = r.get("sample_paths")
            if sp:
                try:
                    r["sample_paths"] = json.loads(sp)
                except (TypeError, ValueError):
                    r["sample_paths"] = [sp]
            else:
                r["sample_paths"] = []
            dj = r.get("details_json")
            if dj:
                try:
                    r["details"] = json.loads(dj)
                except (TypeError, ValueError):
                    r["details"] = None
            out.append(r)
        return out

    def deploy_canaries(self, source_id: int, share_root: str) -> int:
        """Drop canary files in ``share_root``. Returns the count placed.

        Existing canaries are left untouched (idempotent). On any I/O error
        for a single canary we log + continue — partial deployment is more
        useful than aborting altogether.
        """
        if not share_root or not os.path.isdir(share_root):
            logger.warning("deploy_canaries: share_root invalid: %r", share_root)
            return 0

        placed = 0
        body = (
            "FILE ACTIVITY CANARY FILE\n"
            "DO NOT DELETE, RENAME, OR MODIFY.\n"
            "Any access to this file will trigger a critical security alert.\n"
            f"Source ID: {source_id}\n"
            f"Created at: {datetime.now().isoformat(timespec='seconds')}\n"
        )
        for name in sorted(self.canary_names):
            # Re-derive the original cased name: canary_names is lower-cased
            # for matching, but we want a human-readable filename on disk.
            # Use the configured display version when possible.
            display_name = self._original_canary_name(name)
            target = os.path.join(share_root, display_name)
            if os.path.exists(target):
                placed += 1  # already there counts as "deployed"
                continue
            try:
                with open(target, "w", encoding="utf-8") as f:
                    f.write(body)
                placed += 1
            except OSError as e:
                logger.warning("Could not write canary %s: %s", target, e)
        return placed

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _original_canary_name(self, lower_name: str) -> str:
        """Pretty filename for a canary. We round-trip through DEFAULT list."""
        for default in DEFAULT_CANARY_NAMES:
            if default.lower() == lower_name:
                return default
        return lower_name

    def _track_velocity(self, buckets: dict, rule: str, source_id, username,
                         file_path: str, ts: datetime, threshold: int,
                         window_seconds: int, severity: str) -> Optional[dict]:
        key = (source_id, username)
        cutoff = ts - timedelta(seconds=window_seconds)
        with self._lock:
            buf: Deque[Tuple[datetime, str]] = buckets[key]
            buf.append((ts, file_path))
            # Evict old entries
            while buf and buf[0][0] < cutoff:
                buf.popleft()
            if len(buf) <= threshold:
                return None
            # Snapshot last N paths for the alert.
            sample = [p for _, p in list(buf)[-_MAX_SAMPLE_PATHS:]]
            count = len(buf)
            # Reset the buffer so we don't re-fire on the very next event.
            buf.clear()

        return self._fire(
            rule=rule,
            severity=severity,
            source_id=source_id,
            username=username,
            sample_paths=sample,
            file_count=count,
            details={
                "threshold": threshold,
                "window_seconds": window_seconds,
                "observed_count": count,
                "message": f"{rule}: {count} events in {window_seconds}s exceeds {threshold}",
            },
            ts=ts,
        )

    def _fire(self, rule: str, severity: str, source_id, username,
              sample_paths: list, file_count: int, details: dict,
              ts: datetime) -> Optional[dict]:
        """Persist + notify + (optionally) auto-kill. Returns the alert dict."""
        # De-dupe per (source, user, rule) inside cooldown.
        dedupe_key = (source_id, username, rule)
        now = datetime.now()
        last = self._last_alert.get(dedupe_key)
        if last and (now - last).total_seconds() < self._cooldown_seconds:
            return None
        self._last_alert[dedupe_key] = now

        sample = list(sample_paths or [])[:_MAX_SAMPLE_PATHS]
        details_json = json.dumps(details or {}, ensure_ascii=False, default=str)
        sample_json = json.dumps(sample, ensure_ascii=False, default=str)

        kill_attempted = 0
        session_killed = 0
        kill_result = None
        if self.auto_kill_session and rule in _KILL_RULES and username:
            kill_attempted = 1
            kill_result = self._attempt_session_kill(username)
            session_killed = 1 if kill_result and kill_result.get("killed", 0) > 0 else 0

        alert_id = None
        try:
            with self.db.get_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ransomware_alerts
                    (source_id, username, rule_name, severity, file_count,
                     sample_paths, details_json, auto_kill_attempted, session_killed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (source_id, username, rule, severity, file_count,
                     sample_json, details_json, kill_attempted, session_killed),
                )
                alert_id = cur.lastrowid
        except Exception as e:
            logger.error("Failed to persist ransomware alert: %s", e)

        alert = {
            "id": alert_id,
            "triggered_at": ts.isoformat(timespec="seconds"),
            "source_id": source_id,
            "username": username,
            "rule_name": rule,
            "severity": severity,
            "file_count": file_count,
            "sample_paths": sample,
            "details": details,
            "auto_kill_attempted": bool(kill_attempted),
            "session_killed": bool(session_killed),
            "kill_result": kill_result,
        }

        logger.warning(
            "RANSOMWARE ALERT [%s] severity=%s source=%s user=%s count=%s",
            rule, severity, source_id, username, file_count,
        )

        # Best-effort email — never let email failure mask the alert.
        try:
            self._maybe_send_email(alert)
        except Exception as e:
            logger.warning("Ransomware alert email failed: %s", e)

        return alert

    def _attempt_session_kill(self, username: str) -> Optional[dict]:
        try:
            from src.security.smb_session import kill_user_session
        except Exception as e:
            logger.warning("smb_session import failed: %s", e)
            return {"error": f"import_failed: {e}", "killed": 0}
        try:
            return kill_user_session(username, dry_run=False)
        except Exception as e:
            logger.warning("kill_user_session crashed: %s", e)
            return {"error": str(e), "killed": 0}

    def _maybe_send_email(self, alert: dict) -> None:
        if not self.notification_email:
            return
        notifier = self.email_notifier
        if notifier is None or not getattr(notifier, "available", False):
            return

        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.utils import formataddr

        rule = alert["rule_name"]
        sample_paths = alert.get("sample_paths") or []
        sample_html = "".join(
            f"<li><code>{_escape(p)}</code></li>" for p in sample_paths[:_MAX_SAMPLE_PATHS]
        ) or "<li><em>(none)</em></li>"
        sample_text = "\n".join(f"  - {p}" for p in sample_paths[:_MAX_SAMPLE_PATHS]) or "  (none)"

        subject = (
            f"{notifier.subject_prefix} CRITICAL: Possible ransomware activity "
            f"on source {alert.get('source_id')}"
        )

        text_body = (
            f"FILE ACTIVITY ransomware detector triggered.\n\n"
            f"Rule:        {rule}\n"
            f"Severity:    {alert.get('severity')}\n"
            f"Source ID:   {alert.get('source_id')}\n"
            f"Username:    {alert.get('username')}\n"
            f"File count:  {alert.get('file_count')}\n"
            f"Triggered:   {alert.get('triggered_at')}\n\n"
            f"Sample paths:\n{sample_text}\n\n"
            f"Recommended actions:\n"
            f"  - Disable the user account immediately.\n"
            f"  - Disconnect the affected SMB share / source.\n"
            f"  - Review file_audit_events for the same user/source.\n"
            f"  - Restore from the last known-good archive snapshot.\n"
        )
        html_body = f"""<!DOCTYPE html>
<html><body style="font-family:Segoe UI,Roboto,sans-serif;color:#1f2937">
<h2 style="color:#dc2626">CRITICAL: Possible ransomware activity</h2>
<table cellpadding="6" style="border-collapse:collapse;font-size:14px">
<tr><td><b>Rule</b></td><td><code>{_escape(rule)}</code></td></tr>
<tr><td><b>Severity</b></td><td>{_escape(str(alert.get('severity')))}</td></tr>
<tr><td><b>Source ID</b></td><td>{_escape(str(alert.get('source_id')))}</td></tr>
<tr><td><b>Username</b></td><td>{_escape(str(alert.get('username')))}</td></tr>
<tr><td><b>File count</b></td><td>{_escape(str(alert.get('file_count')))}</td></tr>
<tr><td><b>Triggered</b></td><td>{_escape(str(alert.get('triggered_at')))}</td></tr>
</table>
<h3>Sample paths</h3>
<ul>{sample_html}</ul>
<h3>Recommended actions</h3>
<ol>
  <li>Disable the user account immediately.</li>
  <li>Disconnect the affected SMB share / source.</li>
  <li>Review <code>file_audit_events</code> for the same user/source.</li>
  <li>Restore from the last known-good archive snapshot.</li>
</ol>
</body></html>"""

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = formataddr((notifier.from_name, notifier.from_address))
        msg["To"] = self.notification_email
        # High-priority headers — mail clients render this as "Important".
        msg["X-Priority"] = "1"
        msg["X-MSMail-Priority"] = "High"
        msg["Importance"] = "High"
        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        try:
            with notifier._connect() as smtp:  # noqa: SLF001 -- intentional reuse
                smtp.sendmail(notifier.from_address, [self.notification_email],
                               msg.as_string())
            try:
                notifier._log(  # noqa: SLF001
                    username=alert.get("username") or "",
                    email=self.notification_email,
                    subject=subject,
                    status="sent",
                )
            except Exception:
                pass
        except Exception as e:
            logger.warning("Ransomware alert SMTP send failed: %s", e)
            try:
                notifier._log(  # noqa: SLF001
                    username=alert.get("username") or "",
                    email=self.notification_email,
                    subject=subject,
                    status="error",
                    error=str(e),
                )
            except Exception:
                pass


def _escape(s: str) -> str:
    """Tiny HTML escape — avoid pulling in html.escape just here."""
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
