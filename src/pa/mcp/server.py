import os
from collections.abc import Callable
from typing import Any

from pa import __version__
from pa.acp.environment import (
    ASSIGNED_SERVICE_DISPATCH_ENV,
    ASSIGNED_SERVICE_MODE_ENV,
    ASSIGNED_SERVICE_SESSION_ENV,
)

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

# Restart tools route according to the authenticated bridge mode. The remaining
# assigned tools require Goal governance and cannot serve ordinary card workers.
ASSIGNED_SERVICE_ONLY_TOOLS = ASSIGNED_SERVICE_TOOL_ALLOWLIST - {
    "preview_agent_restart_handoff",
    "edit_agent_restart_handoff",
    "request_agent_restart_handoff",
}


class ToolAllowlistProxy:
    """Expose only explicitly named tools while modules register normally."""

    def __init__(
        self, delegate: Any, allowed: frozenset[str] | None,
        *, excluded: frozenset[str] = frozenset(),
    ) -> None:
        self._delegate = delegate
        self._allowed = allowed
        self._excluded = excluded

    def tool(self, *args, **kwargs) -> Callable:
        register = self._delegate.tool(*args, **kwargs)

        def allowlisted(fn: Callable) -> Callable:
            if (
                (self._allowed is None or fn.__name__ in self._allowed)
                and fn.__name__ not in self._excluded
            ):
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
    if mode or dispatch_id or session_id:
        raise RuntimeError("assigned MCP session binding requires assigned mode")
    return False


def _get_mcp():
    global mcp
    if mcp is None:
        from mcp.server.mcpserver import MCPServer

        # Validate restriction before registering anything or caching a server.
        assigned = assigned_service_mcp_mode()
        from pa.mcp.context import registration_context
        from pa.core.registry import ModuleRegistry
        from pa.core.mcp_registration import UniqueToolRegistrationProxy

        candidate = MCPServer("pa", version=__version__)
        ctx = registration_context()
        registry = ModuleRegistry(ctx, registration_only=True)
        registration_target = (
            ToolAllowlistProxy(candidate, ASSIGNED_SERVICE_TOOL_ALLOWLIST)
            if assigned
            else ToolAllowlistProxy(candidate, None, excluded=ASSIGNED_SERVICE_ONLY_TOOLS)
        )
        guarded = UniqueToolRegistrationProxy(registration_target)
        from importlib import import_module

        for name in (
            "backups", "fleet", "sync", "notifications", "projects",
            "pr_supervisor", "items", "goals", "intake", "limbic", "instance",
            "collaboration", "agent_chat", "telemetry", "browser", "agent_providers",
        ):
            import_module(f"pa.mcp.tools.{name}").register_mcp(guarded, ctx)
        registry.load_entrypoints()
        for entry in registry.modules:
            entry.module.register_mcp(guarded, ctx)
        mcp = candidate
    return mcp


def run_stdio() -> None:
    _get_mcp().run(transport="stdio")
