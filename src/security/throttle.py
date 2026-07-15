"""Shared in-process failed-attempt throttle with temporary lockout.

Used by both the TOTP code-verify path (``src/security/totp_auth.py``) and the
login gate (``/api/auth/login``) to blunt online brute force / password
spraying. After ``max_attempts`` failures within ``window_s`` for a key, the key
is locked for ``lockout_s``; a success clears its counter.

Single-process only (the dashboard is one process; anyio dispatches sync
endpoints to worker threads, hence the lock). Not a distributed limiter. Uses
``time.monotonic()`` so wall-clock changes cannot shorten a lockout.
"""
from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)


class AttemptThrottle:
    """Per-key failed-attempt limiter with a temporary lockout window."""

    def __init__(self, max_attempts: int = 5, window_s: int = 300,
                 lockout_s: int = 900, name: str = "throttle"):
        self._max = max_attempts
        self._window = window_s
        self._lockout = lockout_s
        self._name = name
        self._lock = threading.Lock()
        self._fails: dict[str, list[float]] = {}
        self._locked: dict[str, float] = {}

    def check(self, key: str) -> tuple[bool, int]:
        """Return ``(allowed, retry_after_seconds)`` for *key*."""
        now = time.monotonic()
        with self._lock:
            unlock = self._locked.get(key)
            if unlock is not None:
                if now < unlock:
                    return False, int(unlock - now) + 1
                # Lockout expired — clear and allow a fresh window.
                self._locked.pop(key, None)
                self._fails.pop(key, None)
            return True, 0

    def record_failure(self, key: str) -> None:
        now = time.monotonic()
        with self._lock:
            times = [t for t in self._fails.get(key, []) if now - t < self._window]
            times.append(now)
            self._fails[key] = times
            if len(times) >= self._max:
                self._locked[key] = now + self._lockout
                self._fails.pop(key, None)
                logger.warning("%s: locked key after %d failures", self._name, self._max)

    def record_success(self, key: str) -> None:
        with self._lock:
            self._fails.pop(key, None)
            self._locked.pop(key, None)
