"""Auto error reporting to GitHub Issues (issue #118 — Phase 1).

In-process, blocking sender. Default ``enabled: false`` — zero
behaviour change unless an operator opts in via ``config.yaml`` and
provides a token via the configured environment variable.

Out of scope this round (deferred to Phase 2):
    * Logging-handler subclass for ERROR-level records
    * Manual review mode
    * ``/api/system/reports/test`` endpoint
    * Background flush worker (we send synchronously with a 10s
      ``urlopen`` timeout)

stdlib only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import traceback
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

SECRET_KEY_PATTERN = re.compile(
    r"(password|token|api[_-]?key|secret|signing[_-]?key)", re.I
)
UNC_PATTERN = re.compile(r"\\\\([^\\]+)\\([^\\]+)")
HOME_PATTERN = re.compile(r"(C:\\Users\\)([^\\]+)", re.I)

_REDACTED = "***REDACTED***"
_HTTP_TIMEOUT = 10  # seconds


@dataclass
class ErrorReport:
    fingerprint: str
    title: str
    body: str
    occurred_at: str


class ErrorReporter:
    """Captures unhandled exceptions and files them as GitHub Issues.

    Safe by default: when ``telemetry.enabled`` is false (the shipped
    default) every call to :meth:`capture` short-circuits and returns
    ``None`` *before* building the report or touching the network.
    """

    def __init__(self, config: dict, version: str):
        self.config = config or {}
        self.version = version
        cfg = (config or {}).get("telemetry") or {}
        self.enabled = bool(cfg.get("enabled", False))
        gh = cfg.get("github") or {}
        self.repo = gh.get("repo", "")
        token_env = gh.get("token_env", "FILEACTIVITY_TELEMETRY_TOKEN")
        self.token = os.environ.get(token_env, "")
        self.label = gh.get("label", "auto-report")
        self.privacy = cfg.get("privacy") or {}
        self.rate_limit = cfg.get("rate_limit") or {}
        self.max_per_hour = int(self.rate_limit.get("max_per_hour", 10))
        # Unbounded deque on purpose: ``_is_rate_limited`` evicts entries
        # older than 1 hour itself. Using ``maxlen=max_per_hour`` would
        # silently drop the oldest entry once the limit is hit, defeating
        # the throttle.
        self._sent_timestamps: deque = deque()
        self._dedupe: dict[str, int] = {}  # fingerprint -> issue_number
        self._log = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def capture(
        self, exc: BaseException, context: Optional[dict] = None
    ) -> Optional[int]:
        """Capture *exc* and file (or comment on) a GitHub Issue.

        Returns the issue number on success, ``None`` if telemetry is
        disabled, mis-configured, rate-limited, or the HTTP call failed.
        Never raises — telemetry must not break the host process.
        """
        if not self.enabled or not self.token or not self.repo:
            return None
        if self._is_rate_limited():
            return None
        try:
            report = self._build_report(exc, context or {})
        except Exception as e:  # pragma: no cover - defensive only
            self._log.warning("error report build failed: %s", e)
            return None
        existing = self._dedupe.get(report.fingerprint)
        if existing is not None:
            try:
                issue_no = self._comment_existing(existing, report)
                # Comments count toward the rolling rate limit too —
                # otherwise a hot dedupe key could spam the API.
                self._sent_timestamps.append(time.time())
                return issue_no
            except Exception as e:
                self._log.warning("error report comment failed: %s", e)
                return None
        try:
            issue_no = self._post_new_issue(report)
            self._dedupe[report.fingerprint] = issue_no
            self._sent_timestamps.append(time.time())
            return issue_no
        except Exception as e:
            self._log.warning("error report send failed: %s", e)
            return None

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------
    def _is_rate_limited(self) -> bool:
        now = time.time()
        cutoff = now - 3600
        # Drop entries outside the rolling 1-hour window.
        while self._sent_timestamps and self._sent_timestamps[0] < cutoff:
            self._sent_timestamps.popleft()
        return len(self._sent_timestamps) >= self.max_per_hour

    # ------------------------------------------------------------------
    # Report assembly
    # ------------------------------------------------------------------
    def _build_report(self, exc: BaseException, context: dict) -> ErrorReport:
        exc_type = type(exc).__name__
        tb_text = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        sanitized_tb = self._sanitize_text(tb_text)
        sanitized_ctx = self._sanitize_dict(context)
        fingerprint = self._fingerprint(exc, exc_type)
        occurred_at = datetime.now(timezone.utc).isoformat()
        title = f"[auto] {exc_type}: {self._sanitize_text(str(exc))[:120]}"
        body_lines = [
            f"**Occurred at:** {occurred_at}",
            f"**Version:** {self.version}",
            f"**Fingerprint:** `{fingerprint}`",
            "",
            "## Context",
            "```json",
            json.dumps(sanitized_ctx, indent=2, default=str),
            "```",
            "",
            "## Traceback",
            "```",
            sanitized_tb.rstrip(),
            "```",
        ]
        return ErrorReport(
            fingerprint=fingerprint,
            title=title,
            body="\n".join(body_lines),
            occurred_at=occurred_at,
        )

    def _fingerprint(self, exc: BaseException, exc_type: str) -> str:
        frames = traceback.extract_tb(exc.__traceback__)[:5]
        parts = [exc_type, self.version]
        for f in frames:
            parts.append(f"{os.path.basename(f.filename or '')}:{f.lineno or 0}")
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Sanitization
    # ------------------------------------------------------------------
    def _sanitize_dict(self, d: dict) -> dict:
        return self._redact_secrets(d)

    def _redact_secrets(self, value: Any) -> Any:
        if isinstance(value, dict):
            out = {}
            for k, v in value.items():
                if isinstance(k, str) and SECRET_KEY_PATTERN.search(k):
                    out[k] = _REDACTED
                else:
                    out[k] = self._redact_secrets(v)
            return out
        if isinstance(value, list):
            return [self._redact_secrets(v) for v in value]
        if isinstance(value, tuple):
            return tuple(self._redact_secrets(v) for v in value)
        if isinstance(value, str):
            return self._redact_paths(value)
        return value

    def _sanitize_text(self, text: str) -> str:
        return self._redact_paths(text)

    def _redact_paths(self, text: str) -> str:
        if not isinstance(text, str) or not text:
            return text
        if not bool(self.privacy.get("redact_paths", False)):
            return text
        text = UNC_PATTERN.sub(r"\\\\<redacted>\\<redacted>", text)
        text = HOME_PATTERN.sub(r"\1<redacted>", text)
        return text

    # ------------------------------------------------------------------
    # GitHub API
    # ------------------------------------------------------------------
    def _post_new_issue(self, report: ErrorReport) -> int:
        url = f"https://api.github.com/repos/{self.repo}/issues"
        payload = {
            "title": report.title,
            "body": report.body,
            "labels": [self.label] if self.label else [],
        }
        data = self._http_post(url, payload)
        return int(data.get("number"))

    def _comment_existing(self, issue_no: int, report: ErrorReport) -> int:
        url = (
            f"https://api.github.com/repos/{self.repo}"
            f"/issues/{issue_no}/comments"
        )
        body = (
            f"Re-occurrence at {report.occurred_at} "
            f"(version {self.version}, fp `{report.fingerprint}`)\n\n"
            f"{report.body}"
        )
        self._http_post(url, {"body": body})
        return issue_no

    def _http_post(self, url: str, payload: dict) -> dict:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "file-activity-error-reporter",
            },
        )
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
            raw = resp.read()
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return {}
