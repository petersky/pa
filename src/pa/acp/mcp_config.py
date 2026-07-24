"""ACP MCP server configuration."""

from __future__ import annotations

import os
import sys

from acp.schema import EnvVariable, McpServerStdio

from pa.auth.users import UserDirectory
from pa.config import Settings


def _local_api_url(settings: Settings) -> str:
    """Return the owning server's process-local API target."""
    return f"http://127.0.0.1:{settings.port}"


def pa_mcp_servers(settings: Settings) -> list[McpServerStdio]:
    """Stdio MCP bridge so ACP agents get PA tools in-session."""
    # The ACP provider may have a different cwd, PATH, or inherited PA_* set.
    # Pin both the bridge executable and its owner API target to this server.
    # The server creates and forwards the CLI bearer token so the MCP child
    # never needs to create auth state. Mutations go through PA_LOCAL_API_URL.
    cli_token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    owner_env = {
        "PA_DATA_DIR": str(settings.data_dir),
        "PA_LOCAL_API_URL": _local_api_url(settings),
        "PA_LOCAL_API_TOKEN": cli_token,
        "PA_INSTANCE_ID": settings.instance_id,
    }
    browser_env = {
        name: os.environ[name]
        for name in (
            "PA_BROWSER_CDP_URL",
            "PA_BROWSER_TARGET_ID",
            "PA_BROWSER_ATTACHMENT_ID",
            "PA_BROWSER_SESSION_ID",
        )
        if os.environ.get(name)
    }
    forwarded_env = [
        EnvVariable(name=name, value=value)
        for name, value in {**owner_env, **browser_env}.items()
    ]
    return [
        McpServerStdio(
            name="pa",
            command=sys.executable,
            args=["-m", "pa", "mcp"],
            env=forwarded_env,
        )
    ]
