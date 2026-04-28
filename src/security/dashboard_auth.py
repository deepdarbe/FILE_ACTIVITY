"""Dashboard bearer-token authentication (issue #158, finding C-1).

The FastAPI dashboard previously bound to ``0.0.0.0:8085`` with no
authentication, no CSRF, no CORS — every state-mutating endpoint
(archive, restore, quarantine, retention apply, snapshot restore, AD
config write, …) was reachable from anyone on the LAN. On a customer
production-test deployment this is the dominant finding in the security
audit (`docs/architecture/security-audit-2026-04-28.md`).

This module contributes the *authentication* half of the C-1 fix:

* Bearer-token comparison via :func:`hmac.compare_digest` so wrong
  guesses don't leak via timing.
* ``allow_unauth_localhost`` defaults true so existing dev/local
  workflows ("hit http://localhost:8085 from a browser on the same
  host") continue to work without configuration. Set to ``false`` to
  require the token even from localhost.
* Token is sourced from an environment variable (default
  ``FILEACTIVITY_DASHBOARD_TOKEN``); we deliberately do **not** read it
  from ``config.yaml`` — keeping it out of the file means a leaked
  config does not carry the credential.
* ``enabled`` defaults *true* — the safer default. Existing operators
  whose dashboards live behind a reverse-proxy / VPN and prefer the
  pre-1.9 unauth behaviour can opt out by flipping
  ``dashboard.auth.enabled: false`` (and getting the matching CRITICAL
  log line + the ``--bind 0.0.0.0`` refusal in :mod:`main`).

The middleware that calls :meth:`DashboardAuth.check` is registered in
:func:`src.dashboard.api.create_app`; the static-file whitelist is
applied there too so the login UI / CSS / JS load before the user has a
token.
"""

from __future__ import annotations

import hmac
import logging
import os
from typing import Any

logger = logging.getLogger("file_activity.security.dashboard_auth")


_LOCAL_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class DashboardAuth:
    """Per-process bearer-token gate for the FastAPI dashboard.

    Construction is cheap and side-effect-free; the env var is read
    once at startup so rotating the token requires a process restart.
    Tests can construct one directly with a synthetic config dict.
    """

    def __init__(self, config: Any) -> None:
        cfg_dash = (config.get("dashboard") if isinstance(config, dict) else None) or {}
        cfg = (cfg_dash.get("auth") if isinstance(cfg_dash, dict) else None) or {}

        # Default ON — see module docstring rationale.
        self.enabled: bool = bool(cfg.get("enabled", True))
        self.token_env: str = str(
            cfg.get("token_env") or "FILEACTIVITY_DASHBOARD_TOKEN"
        )
        self.token: str = os.environ.get(self.token_env, "") or ""
        self.allow_unauth_localhost: bool = bool(
            cfg.get("allow_unauth_localhost", True)
        )

        if self.enabled and not self.token:
            # Token missing means *every* remote request will 401.
            # Localhost may still pass when allow_unauth_localhost=true,
            # which is fine for a single-host install. Loud warning so
            # the operator notices before customers do.
            logger.warning(
                "DashboardAuth: enabled=true but %s is unset — remote "
                "callers will get 401 until the env var is exported "
                "(localhost%s gated)",
                self.token_env,
                "" if self.allow_unauth_localhost else " also",
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check(self, request: Any) -> bool:
        """Return True iff the request is authorized to proceed.

        ``request`` is a Starlette/FastAPI ``Request``-like object —
        anything with ``.client.host`` and ``.headers.get`` works, so
        unit tests can pass a tiny stand-in.
        """
        if not self.enabled:
            return True

        client_host = ""
        client = getattr(request, "client", None)
        if client is not None:
            client_host = getattr(client, "host", "") or ""

        if self.allow_unauth_localhost and client_host in _LOCAL_HOSTS:
            return True

        headers = getattr(request, "headers", None)
        if headers is None:
            return False
        try:
            auth_header = headers.get("Authorization", "") or ""
        except Exception:
            return False

        if not auth_header.startswith("Bearer "):
            return False
        if not self.token:
            # No server-side token configured — refuse. Without this
            # check an attacker could send `Authorization: Bearer ` and
            # match the empty server token via compare_digest.
            return False

        provided = auth_header[len("Bearer "):]
        try:
            return hmac.compare_digest(provided, self.token)
        except Exception:
            return False
