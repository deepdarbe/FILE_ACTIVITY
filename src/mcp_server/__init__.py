"""file-activity-mcp — Model Context Protocol server for FILE_ACTIVITY.

Thin wrapper that exposes the FILE_ACTIVITY FastAPI dashboard (REST) to MCP
clients (Claude Code, Claude Desktop, ...). All tools call the local REST
API via httpx; no business logic lives here. See ``docs/mcp_server.md``.
"""

from __future__ import annotations

from .server import build_server, main

__all__ = ["build_server", "main"]
