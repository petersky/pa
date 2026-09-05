import os
from collections.abc import Callable
from typing import Any

from pa import __version__
from pa.acp.environment import (
    ASSIGNED_SERVICE_DISPATCH_ENV,
    ASSIGNED_SERVICE_MODE_ENV,
    ASSIGNED_SERVICE_SESSION_ENV,
)
from pa.core.kernel import Kernel

mcp = None

ASSIGNED_SERVICE_TOOL_ALLOWLIST = frozenset(
    {
        "get_assigned_dispatch",
        "get_assigned_goal",
        "propose_assigned_goal_action",
        "record_assigned_goal_evidence",
        "audit_assigned_goal",
        "report_assigned_dispatch_progress",
        "preview_agent_restart_handoff",
        "edit_agent_restart_handoff",
        "request_agent_restart_handoff",
    }
)


class ToolAllowlistProxy:
    """Expose only explicitly named tools while modules register normally."""

    def __init__(self, delegate: Any, allowed: frozenset[str]) -> None:
        self._delegate = delegate
        self._allowed = allowed

    def tool(self, *args, **kwargs) -> Callable:
        register = self._delegate.tool(*args, **kwargs)

        def allowlisted(fn: Callable) -> Callable:
            if fn.__name__ in self._allowed:
                return register(fn)
            return fn

        return allowlisted

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def assigned_service_mcp_mode() -> bool:
    mode = os.environ.get(ASSIGNED_SERVICE_MODE_ENV, "").strip()
    dispatch_id = os.environ.get(ASSIGNED_SERVICE_DISPATCH_ENV, "").strip()
    session_id = os.environ.get(ASSIGNED_SERVICE_SESSION_ENV, "").strip()
    if mode == "1":
        # Registration must never fall back to the broad surface because a
        # restricted descriptor is malformed or partially stripped.
        if not dispatch_id or not session_id:
            raise RuntimeError("assigned MCP session binding is incomplete")
        return True
    if dispatch_id or session_id:
        raise RuntimeError("assigned MCP session binding requires assigned mode")
    return False


def _get_mcp():
    global mcp
    if mcp is None:
        from mcp.server.mcpserver import MCPServer

        mcp = MCPServer("pa", version=__version__)
        kernel = Kernel.boot()
        registration_target = (
            ToolAllowlistProxy(mcp, ASSIGNED_SERVICE_TOOL_ALLOWLIST)
            if assigned_service_mcp_mode()
            else mcp
        )
        kernel.register_mcp(registration_target)
    return mcp


def run_stdio() -> None:
    _get_mcp().run(transport="stdio")
