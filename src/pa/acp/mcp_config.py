"""ACP MCP server configuration."""

from __future__ import annotations

import shutil

from acp.schema import McpServerStdio

from pa.config import Settings


def pa_mcp_servers(settings: Settings) -> list[McpServerStdio]:
    """Stdio MCP bridge so ACP agents get PA tools in-session."""
    command = shutil.which("pa") or "pa"
    return [
        McpServerStdio(
            name="pa",
            command=command,
            args=["mcp"],
            env=[],
        )
    ]
