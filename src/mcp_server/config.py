"""Configuration for the file-activity MCP server.

The server is a thin REST client; the only configuration it needs is the
base URL of the running FILE_ACTIVITY dashboard plus an optional bearer
token for production deployments where the dashboard is reverse-proxied
behind an auth gateway.

Resolution order (highest priority first):
1. Explicit constructor argument.
2. Environment variables (``FILEACTIVITY_BASE_URL``, ``FILEACTIVITY_API_KEY``,
   ``FILEACTIVITY_TIMEOUT``).
3. ``config.yaml`` ``dashboard.host`` / ``dashboard.port`` (relative to repo
   root or ``FILEACTIVITY_CONFIG`` if set).
4. Hard-coded fallback ``http://127.0.0.1:8085``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 8085
_DEFAULT_TIMEOUT = 30.0


def _read_dashboard_from_yaml(path: Path) -> tuple[Optional[str], Optional[int]]:
    """Best-effort parse of dashboard.host/port from config.yaml.

    Uses PyYAML if available; otherwise returns (None, None) — the caller
    will fall back to the hard-coded defaults. We never raise: a malformed
    or missing config file is not a reason to crash the MCP server.
    """
    if not path.exists():
        return None, None
    try:
        import yaml  # type: ignore

        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except Exception:
        return None, None

    dash = (data or {}).get("dashboard") or {}
    host = dash.get("host") if isinstance(dash, dict) else None
    port = dash.get("port") if isinstance(dash, dict) else None
    # 0.0.0.0 is the bind address; for client URL we map to localhost.
    if host in (None, "", "0.0.0.0"):
        host = _DEFAULT_HOST
    return host, int(port) if port else None


def _locate_config_yaml() -> Path:
    """Find config.yaml: $FILEACTIVITY_CONFIG > repo-root > cwd."""
    explicit = os.environ.get("FILEACTIVITY_CONFIG")
    if explicit:
        return Path(explicit)
    here = Path(__file__).resolve()
    # repo_root/src/mcp_server/config.py -> repo_root
    return here.parent.parent.parent / "config.yaml"


@dataclass(frozen=True)
class Settings:
    """Resolved settings for the MCP HTTP client."""

    base_url: str
    api_key: Optional[str]
    timeout: float

    @classmethod
    def load(
        cls,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> "Settings":
        # 1. explicit args
        url = base_url
        key = api_key
        tmo = timeout

        # 2. env vars
        if url is None:
            url = os.environ.get("FILEACTIVITY_BASE_URL")
        if key is None:
            key = os.environ.get("FILEACTIVITY_API_KEY") or None
        if tmo is None:
            env_t = os.environ.get("FILEACTIVITY_TIMEOUT")
            if env_t:
                try:
                    tmo = float(env_t)
                except ValueError:
                    tmo = None

        # 3. config.yaml
        if url is None:
            host, port = _read_dashboard_from_yaml(_locate_config_yaml())
            host = host or _DEFAULT_HOST
            port = port or _DEFAULT_PORT
            url = f"http://{host}:{port}"

        if tmo is None:
            tmo = _DEFAULT_TIMEOUT

        # Normalise: strip trailing slash to mirror the PowerShell module.
        url = url.rstrip("/")
        return cls(base_url=url, api_key=key, timeout=tmo)

    def headers(self) -> dict[str, str]:
        h = {"Accept": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h


__all__ = ["Settings"]
