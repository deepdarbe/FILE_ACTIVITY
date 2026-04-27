"""Caller identity resolution for the approval framework (issue #112).

Approvals need to know *who* is requesting / approving / executing each
high-impact operation so we can refuse self-approval (B != A) and write
the calling user into the audit trail. Authentication is intentionally
out of scope for this PR — the dashboard ships without an in-app login,
relying on either:

* a Windows host where ``os.getlogin()`` reflects the operator,
* a reverse-proxy injecting an authenticated header
  (e.g. ``X-Forwarded-User`` from Authentik / nginx auth_request /
  IIS Windows Auth), or
* a transitional "client_supplied" mode where the body carries
  ``username`` (UNSAFE — the user is whatever the request claims).

The mode is selected by ``approvals.identity_source`` in config.yaml.
``client_supplied`` is documented as unsafe and emits a startup warning
so operators are nudged to harden the deployment before production use.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger("file_activity.security.identity")


_UNKNOWN = "unknown"


def _config_section(config: Any) -> dict:
    """Return ``approvals`` sub-dict regardless of whether the caller
    passed the full app config or the section directly. Empty dict on
    anything malformed."""
    if not isinstance(config, dict):
        return {}
    if "approvals" in config and isinstance(config["approvals"], dict):
        return config["approvals"]
    return config


def _from_windows() -> Optional[str]:
    try:
        # ``os.getlogin`` is the canonical answer on Windows; on POSIX
        # it can raise OSError when stdin isn't a TTY (e.g. systemd).
        u = os.getlogin()
        if u:
            return u
    except OSError:
        pass
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("os.getlogin failed: %s", e)
    # Fall back to environment variables.
    for env in ("USERNAME", "USER", "LOGNAME"):
        v = os.environ.get(env)
        if v:
            return v
    return None


def _from_header(request: Any, header_name: str) -> Optional[str]:
    if request is None or not header_name:
        return None
    headers = getattr(request, "headers", None)
    if headers is None:
        return None
    try:
        v = headers.get(header_name)
    except Exception:
        return None
    if not v:
        # Try lowercase / case variants — Starlette is case-insensitive
        # but a plain dict (used by tests) might not be.
        try:
            v = headers.get(header_name.lower())
        except Exception:
            v = None
    return v.strip() if isinstance(v, str) and v.strip() else None


def _from_body(body: Any) -> Optional[str]:
    if not isinstance(body, dict):
        return None
    for key in ("username", "user", "requested_by", "approved_by",
                "rejected_by"):
        v = body.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def resolve_user(
    request: Any = None,
    config: Any = None,
    body: Any = None,
) -> str:
    """Resolve the calling user's identity.

    Strategies (selected by ``approvals.identity_source``):

    * ``windows``: ``os.getlogin()``, then ``USERNAME``/``USER`` env.
    * ``header``: read configured header (default ``X-Forwarded-User``).
    * ``client_supplied``: read ``body['username']``. UNSAFE.

    Returns ``"unknown"`` if the chosen strategy fails. The caller is
    expected to treat ``"unknown"`` as a refusal-to-self-approve guard
    rather than a security feature.
    """
    cfg = _config_section(config)
    source = (cfg.get("identity_source") or "client_supplied").strip().lower()
    header_name = cfg.get("identity_header") or "X-Forwarded-User"

    if source == "windows":
        u = _from_windows()
        if u:
            return u
        # Soft fall-through: header → body → unknown so a misconfigured
        # Linux test bench can still surface *some* user instead of
        # silently bricking every approval.
        u = _from_header(request, header_name) or _from_body(body)
        return u or _UNKNOWN

    if source == "header":
        u = _from_header(request, header_name)
        if u:
            return u
        # On miss we *do not* fall back to body — that would defeat the
        # point of trusting only the proxy. Log + return unknown.
        logger.debug(
            "identity header %r missing; returning 'unknown'", header_name
        )
        return _UNKNOWN

    # Default: client_supplied. Body wins, then header, then env.
    u = _from_body(body) or _from_header(request, header_name) or _from_windows()
    return u or _UNKNOWN


def warn_if_unsafe(config: Any) -> None:
    """Emit a startup warning when ``identity_source`` is unsafe.

    Called once at app boot (see :func:`create_app`). Idempotent.
    """
    cfg = _config_section(config)
    if not bool(cfg.get("enabled", False)):
        return
    source = (cfg.get("identity_source") or "client_supplied").strip().lower()
    if source == "client_supplied":
        logger.warning(
            "approvals.identity_source='client_supplied' is UNSAFE for "
            "production: any caller can claim any username. Switch to "
            "'header' (with an auth proxy) or 'windows' (on Windows hosts) "
            "before relying on the two-person rule."
        )
