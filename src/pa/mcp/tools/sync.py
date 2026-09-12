"""sync tools: authenticated owner API proxies."""

from __future__ import annotations

import asyncio
from typing import Any, Callable
from pa.core.context import AppContext


async def _offload(
    ctx: AppContext,
    operation: str,
    call: Callable[..., Any],
    /,
    *args: Any,
    timeout: float = 30.0,
    **kwargs: Any,
) -> Any:
    runtime = ctx.services.get("async_runtime")
    if runtime:
        return await runtime.run_blocking(
            operation, call, *args, timeout=timeout, **kwargs
        )
    # Unit/embedded contexts created without a Kernel keep compatibility. Every
    # real ASGI/MCP Kernel installs the bounded runtime before modules load.
    return await asyncio.to_thread(call, *args, **kwargs)

def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    async def sync_status(realm: str = "default") -> dict:
        """Check durable/projection sync consistency through the PA server."""
        return await _offload(
            ctx,
            "mcp.sync_status_http",
            request_local_pa,
            ctx.settings,
            "GET",
            "/api/sync/status",
            params={"realm": realm},
        )

    @mcp.tool()
    async def sync_reconcile(
        idempotency_key: str, realm: str = "default"
    ) -> dict:
        """Repair a stale local projection from its durable event-log head."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return await _offload(
            ctx,
            "mcp.sync_reconcile_http",
            request_local_pa,
            ctx.settings,
            "POST",
            "/api/sync/reconcile",
            json={"realm_id": realm},
            headers={"Idempotency-Key": key},
        )

    @mcp.tool()
    async def dag_index_maintenance(
        idempotency_key: str,
        action: str = "verify",
        realm: str = "default",
    ) -> dict:
        """Verify or rebuild the derived DAG index / object catalog."""
        if action not in {
            "verify",
            "rebuild",
            "cancel",
            "catalog_rebuild",
            "catalog_cancel",
        }:
            raise ValueError(
                "action must be verify, rebuild, cancel, catalog_rebuild, "
                "or catalog_cancel"
            )
        return await _offload(
            ctx,
            "mcp.dag_index_maintenance_http",
            request_local_pa,
            ctx.settings,
            "POST",
            "/api/sync/index/maintenance",
            json={"realm_id": realm, "action": action},
            headers={"Idempotency-Key": idempotency_key},
        )

    @mcp.tool()
    async def resolve_sync_conflicts(
        remote_head: str,
        resolutions: list[dict],
        idempotency_key: str,
        realm: str = "default",
    ) -> dict:
        """Resolve divergent histories with an explicit auditable merge.

        Each resolution is {entity: card|project, id, action, fields}. Use
        update for field conflicts; delete/archive or a full upsert for a
        delete-vs-edit conflict. Include every field reported as conflicting.
        """
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return await _offload(
            ctx,
            "mcp.sync_resolve_http",
            request_local_pa,
            ctx.settings,
            "POST",
            "/api/sync/conflicts/resolve",
            json={
                "realm_id": realm,
                "remote_head": remote_head,
                "resolutions": resolutions,
            },
            headers={"Idempotency-Key": key},
        )
