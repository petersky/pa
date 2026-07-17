"""ACP MCP server configuration."""

from __future__ import annotations

import os
import shutil

from acp.schema import EnvVariable, McpServerStdio

from pa.config import Settings


def pa_mcp_servers(settings: Settings) -> list[McpServerStdio]:
    """Stdio MCP bridge so ACP agents get PA tools in-session."""
    command = shutil.which("pa") or "pa"
    browser_env = [
        EnvVariable(name=name, value=value)
        for name in (
            "PA_BROWSER_CDP_URL",
            "PA_BROWSER_TARGET_ID",
            "PA_BROWSER_ATTACHMENT_ID",
            "PA_BROWSER_SESSION_ID",
        )
        if (value := os.environ.get(name))
    ]
    return [
        McpServerStdio(
            name="pa",
            command=command,
            args=["mcp"],
            env=browser_env,
        )
    ]
