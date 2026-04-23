"""file-activity-mcp server entry point.

Wires the tools defined in :mod:`src.mcp_server.tools` into the MCP
protocol via the official Python SDK. Default transport is stdio (what
``claude mcp add file-activity -- python -m src.mcp_server`` expects);
SSE/HTTP can be enabled by setting ``FILEACTIVITY_MCP_TRANSPORT=sse``
and is documented in ``docs/mcp_server.md``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

import httpx

from .config import Settings
from .tools import TOOL_INDEX, TOOLS, dispatch

logger = logging.getLogger("file_activity_mcp")


# ---------------------------------------------------------------------------
# MCP wiring
# ---------------------------------------------------------------------------


def build_server(settings: Optional[Settings] = None):
    """Construct the MCP ``Server`` and register all tool handlers.

    The ``mcp`` package is imported lazily so the rest of the project
    (and the eval suite, which can mock the SDK) keeps working in
    environments where the SDK is not installed.
    """
    from mcp.server import Server  # noqa: WPS433 (lazy import is intentional)
    from mcp import types as mcp_types  # noqa: WPS433

    settings = settings or Settings.load()

    # One client per server lifetime — connection pooling matters when
    # the LLM fans out 5+ tool calls in parallel.
    state: dict[str, Any] = {"client": None, "settings": settings}

    @asynccontextmanager
    async def _lifespan(_server) -> AsyncIterator[dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            state["client"] = client
            try:
                yield state
            finally:
                state["client"] = None

    server: Any = Server("file-activity", lifespan=_lifespan)

    @server.list_tools()
    async def _list_tools() -> list[Any]:
        return [
            mcp_types.Tool(
                name=t.name,
                description=t.description,
                inputSchema=t.json_schema(),
            )
            for t in TOOLS
        ]

    @server.call_tool()
    async def _call_tool(name: str, arguments: Optional[dict[str, Any]]) -> list[Any]:
        client = state["client"]
        if client is None:
            # Lifespan hasn't started yet — defensive guard for tests
            # that bypass the runner.
            client = httpx.AsyncClient()
            state["client"] = client
        try:
            result = await dispatch(name, arguments, client, state["settings"])
        except KeyError as e:
            return [mcp_types.TextContent(type="text", text=f"error: {e}")]
        except ValueError as e:
            # Pydantic validation failure — surface as a tool-side error
            # so the LLM can fix its arguments and retry.
            return [mcp_types.TextContent(type="text", text=f"validation error: {e}")]
        except Exception as e:  # pragma: no cover - generic safety net
            logger.exception("Tool %s failed", name)
            return [mcp_types.TextContent(type="text", text=f"error: {e}")]

        text = json.dumps(result, indent=2, default=str, ensure_ascii=False)
        return [mcp_types.TextContent(type="text", text=text)]

    return server


# ---------------------------------------------------------------------------
# Transport bootstrap
# ---------------------------------------------------------------------------


async def _run_stdio() -> None:
    from mcp.server.stdio import stdio_server

    server = build_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


async def _run_sse(host: str, port: int) -> None:  # pragma: no cover - optional
    # Optional SSE transport for HTTPS reverse-proxy deployments. Kept
    # behind an env flag so the default ``python -m src.mcp_server``
    # path stays the dead-simple stdio one.
    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.routing import Mount, Route
    import uvicorn

    server = build_server()
    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
            await server.run(
                streams[0],
                streams[1],
                server.create_initialization_options(),
            )

    app = Starlette(routes=[
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ])
    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    await uvicorn.Server(config).serve()


def main() -> None:
    """Entry point for ``python -m src.mcp_server`` and the launcher script."""
    logging.basicConfig(
        level=os.environ.get("FILEACTIVITY_MCP_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    transport = os.environ.get("FILEACTIVITY_MCP_TRANSPORT", "stdio").lower()
    if transport == "sse":  # pragma: no cover - exercised via integration scripts
        host = os.environ.get("FILEACTIVITY_MCP_HOST", "127.0.0.1")
        port = int(os.environ.get("FILEACTIVITY_MCP_PORT", "8765"))
        asyncio.run(_run_sse(host, port))
    else:
        asyncio.run(_run_stdio())


__all__ = ["build_server", "main"]
