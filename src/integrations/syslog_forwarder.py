"""Syslog/CEF forwarder for SIEM integration (issue #50).

Pushes FILE ACTIVITY security-relevant events (ransomware alerts, audit chain
breaks, archive/scan failures) to a downstream syslog collector or SIEM
(Splunk, Elastic, Sentinel, QRadar, ...) using either RFC 5424 or CEF
formatting over UDP, TCP, or TCP+TLS.

Design notes
------------

* The forwarder runs a single background daemon thread reading from a
  bounded ``queue.Queue``. Producers (the detector, the database verifier)
  call :meth:`emit` which is non-blocking — when the queue is full the
  oldest event is dropped (drop-oldest backpressure) so a slow / dead SIEM
  collector cannot stall scanning or detection.
* TCP transports auto-reconnect with capped exponential backoff
  (1, 2, 4, ..., 60 seconds). UDP is fire-and-forget.
* Stdlib only — ``socket``, ``ssl``, ``queue``, ``threading``. No new
  third-party dependencies.
* Cross-platform safe: nothing in this module depends on Windows or
  Linux-only primitives.
"""

from __future__ import annotations

import logging
import queue
import socket
import ssl
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger("file_activity.integrations.syslog")


# RFC 5424 syslog severities
_SEVERITIES = {
    "emergency": 0,
    "alert": 1,
    "critical": 2,
    "error": 3,
    "warning": 4,
    "notice": 5,
    "info": 6,
    "debug": 7,
}

# RFC 5424 facility codes (subset — covers the common ones).
_FACILITIES = {
    "kern": 0,
    "user": 1,
    "mail": 2,
    "daemon": 3,
    "auth": 4,
    "syslog": 5,
    "lpr": 6,
    "news": 7,
    "uucp": 8,
    "cron": 9,
    "authpriv": 10,
    "ftp": 11,
    "local0": 16,
    "local1": 17,
    "local2": 18,
    "local3": 19,
    "local4": 20,
    "local5": 21,
    "local6": 22,
    "local7": 23,
}

# CEF severity is 0..10. Map our textual severities into that range so the
# SIEM can drive its own routing/alerting from the integer.
_CEF_SEVERITY = {
    "emergency": 10,
    "alert": 9,
    "critical": 10,
    "error": 7,
    "warning": 5,
    "notice": 4,
    "info": 3,
    "debug": 1,
}

_DEFAULT_QUEUE_MAX = 10000
_RECONNECT_BACKOFF_MAX = 60.0
_VENDOR = "deepdarbe"
_PRODUCT = "FILE ACTIVITY"
_PRODUCT_VERSION = "1.0"


def _cef_escape(value: Any) -> str:
    """Escape CEF extension values per ArcSight CEF spec."""
    s = "" if value is None else str(value)
    # Order matters: backslash first so we don't double-escape what we add.
    return (
        s.replace("\\", "\\\\")
        .replace("=", "\\=")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _cef_header_escape(value: Any) -> str:
    """Escape CEF header fields (pipe-delimited)."""
    s = "" if value is None else str(value)
    return s.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def _structured_data_escape(value: Any) -> str:
    """Escape RFC 5424 SD-PARAM values."""
    s = "" if value is None else str(value)
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("]", "\\]")


class SyslogForwarder:
    """Background syslog/CEF forwarder. Drop-oldest backpressure.

    The constructor reads ``config["integrations"]["syslog"]``. Missing /
    ``enabled: false`` config produces a no-op forwarder (``available``
    is False, every :meth:`emit` returns False).
    """

    def __init__(self, config: dict):
        cfg = ((config or {}).get("integrations", {}) or {}).get("syslog", {}) or {}

        self.enabled = bool(cfg.get("enabled", False))
        self.host = (cfg.get("host") or "").strip()
        self.port = int(cfg.get("port") or 514)
        self.transport = (cfg.get("transport") or "udp").strip().lower()
        self.fmt = (cfg.get("format") or "rfc5424").strip().lower()
        self.facility_name = (cfg.get("facility") or "local0").strip().lower()
        self.facility = _FACILITIES.get(self.facility_name, 16)
        self.tls_ca_path = (cfg.get("tls_ca_path") or "").strip() or None
        self.queue_max = int(cfg.get("queue_max") or _DEFAULT_QUEUE_MAX)
        self.hostname = (cfg.get("hostname_override") or "").strip() or socket.gethostname()

        # The forwarder is "available" only when explicitly enabled AND
        # configured with a destination. Producers gate on this so they
        # never enqueue events that have nowhere to go.
        self._configured = self.enabled and bool(self.host)

        # Bounded queue: drop-oldest semantics implemented in emit().
        self._queue: "queue.Queue[bytes]" = queue.Queue(maxsize=max(1, self.queue_max))
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Health counters — exposed via health() for the dashboard.
        self._dropped_count = 0
        self._sent_count = 0
        self._last_emit_at: Optional[str] = None
        self._last_error: Optional[str] = None

        # TCP connection state (None for UDP).
        self._sock: Optional[socket.socket] = None
        self._reconnect_delay = 1.0

        self._worker: Optional[threading.Thread] = None
        if self._configured:
            self._worker = threading.Thread(
                target=self._run, name="syslog-forwarder", daemon=True,
            )
            self._worker.start()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """True when the forwarder will accept events."""
        return self._configured and not self._stop_event.is_set()

    def emit(self, severity: str, event_class: str, payload: dict) -> bool:
        """Queue a single event. Non-blocking.

        Returns False when the forwarder is disabled, when ``severity`` is
        unknown, or when the queue is full and the oldest item could not be
        dropped fast enough (extremely unlikely). Returns True when the
        event has been queued for the worker thread.
        """
        if not self.available:
            return False
        sev = (severity or "info").lower()
        if sev not in _SEVERITIES:
            logger.debug("syslog emit: unknown severity %r — using info", severity)
            sev = "info"

        try:
            line = self._format(sev, event_class, payload or {})
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("syslog emit: format failed: %s", e)
            self._last_error = f"format_failed: {e}"
            return False

        try:
            self._queue.put_nowait(line)
        except queue.Full:
            # Drop-oldest. Pop one and retry once. We must not block.
            try:
                self._queue.get_nowait()
                with self._lock:
                    self._dropped_count += 1
                logger.warning(
                    "syslog queue full (max=%s) — dropped oldest event",
                    self.queue_max,
                )
            except queue.Empty:  # pragma: no cover - race
                pass
            try:
                self._queue.put_nowait(line)
            except queue.Full:  # pragma: no cover - race
                with self._lock:
                    self._dropped_count += 1
                return False

        self._last_emit_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return True

    def emit_ransomware_alert(self, alert: dict) -> bool:
        """Convenience helper called from RansomwareDetector."""
        if not alert:
            return False
        severity = (alert.get("severity") or "critical").lower()
        payload = {
            "rule": alert.get("rule_name") or alert.get("rule") or "",
            "src_user": alert.get("username") or "",
            "source_id": alert.get("source_id"),
            "file_count": alert.get("file_count"),
            "alert_id": alert.get("id"),
            "triggered_at": alert.get("triggered_at"),
            "auto_kill_attempted": int(bool(alert.get("auto_kill_attempted"))),
            "session_killed": int(bool(alert.get("session_killed"))),
            "msg": (alert.get("details") or {}).get("message")
            or "Ransomware alert triggered",
        }
        sample = alert.get("sample_paths") or []
        if sample:
            payload["sample_path"] = sample[0]
        return self.emit(severity, "ransomware_alert", payload)

    def emit_audit_break(self, broken_seq: int, reason: str) -> bool:
        return self.emit(
            "critical",
            "audit_chain_break",
            {
                "broken_seq": broken_seq,
                "reason": reason or "",
                "msg": "Audit chain integrity verification FAILED",
            },
        )

    def emit_archive_failure(self, op_id: int, error: str) -> bool:
        return self.emit(
            "error",
            "archive_failure",
            {
                "op_id": op_id,
                "error": error or "",
                "msg": "Archive operation failed",
            },
        )

    def emit_scan_failure(self, scan_id: int, error: str) -> bool:
        return self.emit(
            "error",
            "scan_failure",
            {
                "scan_id": scan_id,
                "error": error or "",
                "msg": "Scan operation failed",
            },
        )

    def health(self) -> dict:
        with self._lock:
            return {
                "available": self.available,
                "configured": self._configured,
                "transport": self.transport,
                "format": self.fmt,
                "host": self.host,
                "port": self.port,
                "queue_depth": self._queue.qsize(),
                "queue_max": self.queue_max,
                "dropped_count": self._dropped_count,
                "sent_count": self._sent_count,
                "last_emit_at": self._last_emit_at,
                "last_error": self._last_error,
            }

    def stop(self) -> None:
        """Signal the worker to drain and close the socket."""
        self._stop_event.set()
        # Wake the worker if it's blocked on queue.get().
        try:
            self._queue.put_nowait(b"")
        except queue.Full:
            pass
        if self._worker is not None:
            self._worker.join(timeout=2.0)
        self._close_sock()

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                line = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if not line:
                # Sentinel from stop() — loop and exit.
                continue
            try:
                self._send(line)
                with self._lock:
                    self._sent_count += 1
            except Exception as e:
                self._last_error = str(e)
                logger.warning("syslog send failed: %s", e)
                # For TCP we'll reconnect on the next iteration. UDP errors
                # are usually transient — drop the line and continue.

    def _send(self, line: bytes) -> None:
        if self.transport == "udp":
            self._send_udp(line)
        elif self.transport in ("tcp", "tcp+tls"):
            self._send_tcp(line)
        else:
            raise ValueError(f"unsupported transport: {self.transport!r}")

    def _send_udp(self, line: bytes) -> None:
        if self._sock is None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.sendto(line, (self.host, self.port))

    def _send_tcp(self, line: bytes) -> None:
        # Lazily (re)connect with capped exponential backoff.
        if self._sock is None:
            self._connect_tcp()
        if self._sock is None:
            # _connect_tcp aborted (stop requested or shutdown mid-retry).
            raise OSError("syslog connect aborted")
        # RFC 6587 octet-counting: send LF-delimited frames. Most SIEMs
        # accept either; LF-delimited is the simpler / more compatible
        # default.
        if not line.endswith(b"\n"):
            line = line + b"\n"
        try:
            self._sock.sendall(line)
        except (OSError, ssl.SSLError) as e:
            self._close_sock()
            raise e

    def _connect_tcp(self) -> None:
        delay = self._reconnect_delay
        while not self._stop_event.is_set():
            try:
                raw = socket.create_connection((self.host, self.port), timeout=10)
                if self.transport == "tcp+tls":
                    ctx = ssl.create_default_context(cafile=self.tls_ca_path)
                    self._sock = ctx.wrap_socket(raw, server_hostname=self.host)
                else:
                    self._sock = raw
                # Reset backoff on success.
                self._reconnect_delay = 1.0
                return
            except (OSError, ssl.SSLError) as e:
                self._last_error = f"connect_failed: {e}"
                logger.warning(
                    "syslog connect to %s:%s failed: %s — retry in %.1fs",
                    self.host, self.port, e, delay,
                )
                # Sleep in small slices so stop() takes effect promptly.
                end = time.monotonic() + delay
                while time.monotonic() < end:
                    if self._stop_event.is_set():
                        return
                    time.sleep(min(0.25, end - time.monotonic()))
                delay = min(delay * 2, _RECONNECT_BACKOFF_MAX)
                self._reconnect_delay = delay

    def _close_sock(self) -> None:
        s = self._sock
        self._sock = None
        if s is not None:
            try:
                s.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Formatters
    # ------------------------------------------------------------------

    def _format(self, severity: str, event_class: str, payload: dict) -> bytes:
        if self.fmt == "cef":
            return self._format_cef(severity, event_class, payload)
        return self._format_rfc5424(severity, event_class, payload)

    def _format_rfc5424(self, severity: str, event_class: str,
                         payload: dict) -> bytes:
        sev_code = _SEVERITIES[severity]
        priority = self.facility * 8 + sev_code
        # ISO 8601 timestamp with timezone (RFC 5424 §6.2.3).
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        procid = "-"
        msgid = event_class or "-"

        # Structured data block carries the typed payload so SIEMs that
        # parse RFC 5424 SD-ELEMENTs can index every field. The custom
        # IANA-like SD-ID uses our enterprise label.
        sd_params = []
        msg_text = ""
        for k, v in (payload or {}).items():
            if k == "msg":
                msg_text = "" if v is None else str(v)
                continue
            sd_params.append(f'{k}="{_structured_data_escape(v)}"')
        sd = "[file_activity@32473 " + " ".join(sd_params) + "]" if sd_params else "-"

        line = (
            f"<{priority}>1 {ts} {self.hostname} FILE_ACTIVITY {procid} "
            f"{msgid} {sd} {msg_text}"
        )
        return line.encode("utf-8", errors="replace")

    def _format_cef(self, severity: str, event_class: str,
                     payload: dict) -> bytes:
        sev_code = _CEF_SEVERITY.get(severity, 5)
        # Title falls back to the event class so the SIEM always has
        # something human-readable in the "name" column.
        title = (payload or {}).get("msg") or event_class

        # Syslog header (RFC 3164 style) prepended so collectors that
        # expect a syslog priority on every line accept it. We use the
        # configured facility plus the mapped RFC 5424 severity.
        rfc_sev = _SEVERITIES[severity]
        priority = self.facility * 8 + rfc_sev
        ts = datetime.now().strftime("%b %d %H:%M:%S")

        ext_parts = []
        for k, v in (payload or {}).items():
            if k == "msg":
                continue
            ext_parts.append(f"{k}={_cef_escape(v)}")
        extension = " ".join(ext_parts)

        cef = (
            f"CEF:0|{_VENDOR}|{_cef_header_escape(_PRODUCT)}|{_PRODUCT_VERSION}|"
            f"{_cef_header_escape(event_class)}|{_cef_header_escape(title)}|"
            f"{sev_code}|{extension}"
        )
        line = f"<{priority}>{ts} {self.hostname} {cef}"
        return line.encode("utf-8", errors="replace")
