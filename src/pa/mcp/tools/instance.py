"""instance tools: authenticated owner API proxies."""

from __future__ import annotations

from typing import Any
from pa.core.context import AppContext
from pa.fleet.capacity import DEFAULT_DISPATCH_QUEUE_CAPACITY


def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    settings = ctx.settings

    @mcp.tool()
    def instance_info() -> dict:
        """Return information about this PA instance."""
        return request_local_pa(settings, "GET", "/api/instance")

    @mcp.tool()
    def get_dispatch_capacity() -> dict:
        """Return configured and effective fleet execution capacity."""
        return request_local_pa(settings, "GET", "/api/config")

    @mcp.tool()
    def set_dispatch_capacity(
        dispatch_capacity: int,
        provider_capacities: dict[str, int] | None = None,
        queue_capacity: int = DEFAULT_DISPATCH_QUEUE_CAPACITY,
        provider_queue_capacities: dict[str, int] | None = None,
    ) -> dict:
        """Validate and immediately apply fleet execution capacity."""
        return request_local_pa(
            settings,
            "PATCH",
            "/api/config/capacity",
            json={
                "dispatch_capacity": dispatch_capacity,
                "dispatch_provider_capacities": provider_capacities or {},
                "dispatch_queue_capacity": queue_capacity,
                "dispatch_provider_queue_capacities": (
                    provider_queue_capacities or {}
                ),
                "interface": "mcp",
            },
        )

    @mcp.tool()
    def list_auxiliary_mcp_servers() -> dict:
        """List this instance's redacted auxiliary MCP definitions and readiness."""
        return request_local_pa(settings, "GET", "/api/mcp-servers")

    @mcp.tool()
    def import_auxiliary_mcp_servers(document: dict[str, Any]) -> dict:
        """Validate common mcpServers JSON without persisting secret values."""
        return request_local_pa(
            settings,
            "POST",
            "/api/mcp-servers/import",
            json={"document": document},
        )

    @mcp.tool()
    def save_auxiliary_mcp_servers(
        servers: list[dict[str, Any]],
        expected_revision: str,
        idempotency_key: str,
    ) -> dict:
        """Replace this instance's auxiliary MCP collection idempotently."""
        return request_local_pa(
            settings,
            "PUT",
            "/api/mcp-servers",
            json={
                "servers": servers,
                "expected_revision": expected_revision,
                "idempotency_key": idempotency_key,
            },
        )

    @mcp.tool()
    def probe_auxiliary_mcp_server(name: str) -> dict:
        """Start and handshake one local auxiliary MCP definition."""
        return request_local_pa(
            settings,
            "POST",
            f"/api/mcp-servers/{name}/probe",
        )

    @mcp.tool()
    def configuration_schema(target: str = "local") -> dict:
        """List every supported setting and its shared surface metadata."""
        return request_local_pa(
            settings,
            "GET",
            "/api/configuration/schema",
            params={"target": target},
        )

    @mcp.tool()
    def configuration_list(target: str = "local") -> dict:
        """Read configured/effective values, precedence, and applicability."""
        return request_local_pa(
            settings,
            "GET",
            "/api/configuration",
            params={"target": target},
        )

    @mcp.tool()
    def configuration_validate(
        changes: dict[str, Any],
        clear: list[str] | None = None,
        target: str = "local",
    ) -> dict:
        """Validate a multi-setting patch without writing it."""
        return request_local_pa(
            settings,
            "POST",
            "/api/configuration/validate",
            json={"changes": changes, "clear": clear or [], "target": target},
        )

    @mcp.tool()
    def configuration_diff(
        changes: dict[str, Any],
        clear: list[str] | None = None,
        target: str = "local",
    ) -> dict:
        """Return a secret-safe diff for a staged configuration patch."""
        return request_local_pa(
            settings,
            "POST",
            "/api/configuration/diff",
            json={"changes": changes, "clear": clear or [], "target": target},
        )

    @mcp.tool()
    def configuration_update(
        changes: dict[str, Any],
        expected_revision: str,
        idempotency_key: str,
        clear: list[str] | None = None,
        target: str = "local",
    ) -> dict:
        """Atomically apply an idempotent, audited configuration patch."""
        return request_local_pa(
            settings,
            "PATCH",
            "/api/configuration",
            json={
                "changes": changes,
                "clear": clear or [],
                "expected_revision": expected_revision,
                "idempotency_key": idempotency_key,
                "interface": "mcp",
                "target": target,
            },
        )

    @mcp.tool()
    def configuration_audit(target: str = "local", limit: int = 100) -> dict:
        """List secret-safe configuration change audit events."""
        return request_local_pa(
            settings,
            "GET",
            "/api/configuration/audit",
            params={"target": target, "limit": limit},
        )

    @mcp.tool()
    async def repository_inspect(path: str) -> dict:
        """Inspect and persist this instance's current Git repository state."""
        runtime = ctx.require_service("async_runtime")
        return await runtime.run_blocking(
            "mcp.repository_inspect_http",
            request_local_pa,
            settings,
            "POST",
            "/api/repositories/inspect",
            params={"path": path},
        )

    @mcp.tool()
    async def repository_snapshots() -> list[dict]:
        """List non-authoritative repository observations by instance."""
        runtime = ctx.require_service("async_runtime")
        return await runtime.run_blocking(
            "mcp.repository_snapshots_http",
            request_local_pa,
            settings,
            "GET",
            "/api/repositories",
        )

    @mcp.tool()
    async def workspace_leases(card_id: str | None = None) -> dict:
        """List this instance's durable worktree leases and lifecycle metrics."""
        runtime = ctx.require_service("async_runtime")
        return await runtime.run_blocking(
            "mcp.workspace_leases_http",
            request_local_pa,
            settings,
            "GET",
            "/api/workspaces",
            params={"card_id": card_id},
        )

    @mcp.tool()
    async def workspace_reconcile(collect: bool = True) -> dict:
        """Reconcile terminal local leases and safely collect eligible worktrees."""
        runtime = ctx.require_service("async_runtime")
        return await runtime.run_blocking(
            "mcp.workspace_reconcile_http",
            request_local_pa,
            settings,
            "POST",
            "/api/workspaces/reconcile",
            json={"collect": collect},
            timeout_seconds=120.0,
            timeout=300.0,
        )
