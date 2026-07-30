from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field, field_validator

from pa.auth.middleware import get_principal_id, require_user
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.domain.instance_config import update_instance_config
from pa.fleet.capacity import (
    DEFAULT_DISPATCH_CAPACITY,
    MAX_DISPATCH_CAPACITY,
    DispatchCapacity,
    effective_capacity,
)
from pa.instance.agent_session import AgentStartupNotReady
from pa.instance.quiesce import QuiesceProgress
from pa.modules.agent_lifecycle import startup_recovery_error
from pa.repository.state import RepositorySnapshotInput

router = APIRouter()

_quiesce_task: asyncio.Task[Any] | None = None
_quiesce_progress: QuiesceProgress | None = None


class QuiesceRequest(BaseModel):
    reason: str = "restart"
    timeout: float = Field(default=300.0, ge=1.0, le=3600.0)
    wait: bool = False


class RepositoryReconcileRequest(BaseModel):
    snapshots: list[RepositorySnapshotInput] = Field(default_factory=list)


class WorkspaceReconcileRequest(BaseModel):
    collect: bool = True


class CapacityConfigUpdate(BaseModel):
    dispatch_capacity: int = Field(
        default=DEFAULT_DISPATCH_CAPACITY, ge=1, le=MAX_DISPATCH_CAPACITY
    )
    dispatch_provider_capacities: dict[str, DispatchCapacity] = Field(
        default_factory=dict
    )

    @field_validator("dispatch_provider_capacities")
    @classmethod
    def validate_provider_capacities(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for provider, limit in value.items():
            key = provider.strip().lower()
            if not key:
                raise ValueError("provider capacity names cannot be empty")
            if isinstance(limit, bool) or not 1 <= limit <= MAX_DISPATCH_CAPACITY:
                raise ValueError(
                    f"capacity for provider {provider!r} must be between 1 and "
                    f"{MAX_DISPATCH_CAPACITY}"
                )
            normalized[key] = limit
        return normalized


def _unreachable_repository_instances(ctx: AppContext) -> set[str]:
    fleet = ctx.services.get("fleet_registry")
    local_id = ctx.settings.instance_id
    return {
        item.instance_id
        for item in (fleet.list_instances() if fleet else [])
        if not item.healthy and item.instance_id != local_id
    }


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request) -> dict:
    """Report API readiness only after owner-scoped runtime services exist."""
    ctx = request.app.state.ctx
    required = {
        "instance_agent",
        "async_runtime",
        "event_log",
        "fleet_registry",
    }
    missing = sorted(required - ctx.services.keys())
    if missing:
        raise HTTPException(
            status_code=503,
            detail={"status": "starting", "missing_services": missing},
        )
    # FastAPI's public OpenAPI view is stable across router implementations.
    # Newer FastAPI releases may keep included routers in ``app.routes`` as
    # internal objects without a ``path`` attribute.
    paths = set(request.app.openapi().get("paths", {}))
    required_paths = {
        "/api/cards",
        "/api/items",
        "/api/projects",
        "/api/sync/status",
    }
    missing_routes = sorted(required_paths - paths)
    if missing_routes:
        raise HTTPException(
            status_code=503,
            detail={"status": "starting", "missing_routes": missing_routes},
        )
    return {
        "status": "ready",
        "instance_id": ctx.settings.instance_id,
        "routes": ["cards", "items", "projects", "sync"],
    }


@router.get("/runtime")
async def async_runtime_status(request: Request) -> dict:
    """Cheap telemetry that remains independent of disk/network health."""
    runtime = request.app.state.ctx.require_service("async_runtime")
    snapshot = runtime.snapshot()
    from pa.acp.sandbox_health import sandbox_health_registry

    snapshot["sandbox_health"] = sandbox_health_registry.snapshot()
    snapshot["lifecycle"] = dict(
        request.app.state.ctx.services.get("agent_lifecycle") or {}
    )
    agent = request.app.state.ctx.services.get("instance_agent")
    runtimes = agent.list_runtimes() if agent else []
    snapshot["pa_mcp"] = [
        {
            "session_id": item.session_id,
            **(
                item.connection.pa_mcp_health
                if item.connection
                else {"state": "disconnected"}
            ),
        }
        for item in runtimes
    ]
    snapshot["queues"] = {
        "agent_transcript_batches": sum(
            item._transcript_queue.qsize() for item in runtimes
        ),
        "agent_transcript_buffered_events": sum(
            len(item._transcript_buffer) for item in runtimes
        ),
        "agent_prompts": sum(len(item._queue) for item in runtimes),
        "dispatch_active": len(
            request.app.state.ctx.services.get("dispatch_worker")._active
        )
        if request.app.state.ctx.services.get("dispatch_worker")
        else 0,
    }
    dispatch_worker = request.app.state.ctx.services.get("dispatch_worker")
    if dispatch_worker:
        snapshot["dispatch_worker"] = dispatch_worker.snapshot()
    progress_service = request.app.state.ctx.services.get("progress_service")
    if progress_service:
        snapshot["progress_backpressure"] = progress_service.snapshot()
    provider_gate = request.app.state.ctx.services.get("provider_action_gate")
    if provider_gate:
        snapshot["queues"]["provider_actions"] = provider_gate.snapshot()
    from pa.core.sse_observability import sse_connections

    snapshot["sse_connections"] = sse_connections.snapshot()
    return snapshot


@router.get("/status")
def instance_status(request: Request) -> dict:
    kernel = request.app.state.kernel
    from pa.status.info import build_status_snapshot

    return build_status_snapshot(
        request.app.state.ctx,
        module_count=len(kernel.registry.modules),
    )


@router.get("/instance")
def instance_info(request: Request) -> dict:
    registry = request.app.state.ctx.require_service("peer_registry")
    log = request.app.state.ctx.services.get("event_log")
    sync_heads = {}
    if log:
        for ref in log.list_refs():
            sync_heads[ref.realm_id] = ref.head_hash
    info = registry.self_info
    info.sync_head = sync_heads
    return info.model_dump()


@router.get("/peers")
async def list_peers(request: Request) -> list[dict]:
    registry = request.app.state.ctx.require_service("peer_registry")
    peers = await registry.discover_peers()
    return [p.model_dump() for p in peers]


@router.get("/repositories")
def repository_snapshots(request: Request) -> list[dict]:
    ctx = request.app.state.ctx
    service = ctx.require_service("repository_state")
    return [
        item.model_dump(mode="json")
        for item in service.list(
            unreachable_instances=_unreachable_repository_instances(ctx)
        )
    ]


@router.get("/workspaces")
def workspace_leases(request: Request, card_id: str | None = None) -> dict:
    """Expose durable local provisioning state and counters for diagnosis."""
    agent = request.app.state.ctx.require_service("instance_agent")
    manager = agent.workspace_manager
    return {
        "leases": [
            lease.model_dump(mode="json") for lease in manager.list(card_id=card_id)
        ],
        "metrics": manager.metrics(),
    }


@router.post("/workspaces/reconcile")
async def reconcile_workspace_leases(
    request: Request, body: WorkspaceReconcileRequest
) -> dict:
    """Reconcile terminal cards/sessions into local lease state and collect safely."""
    agent = request.app.state.ctx.require_service("instance_agent")
    manager = agent.workspace_manager
    runtime = request.app.state.ctx.require_service("async_runtime")

    def reconcile() -> dict:
        before = manager.list()
        reconciliation = manager.reconcile_terminal_state()
        active_session_ids = {
            item.session_id
            for item in agent.list_runtimes()
            if not getattr(item, "_closed", False)
        }
        collection = (
            manager.collect_garbage(active_session_ids=active_session_ids)
            if body.collect
            else None
        )
        after = manager.list()
        return {
            "reconciliation": reconciliation,
            "collection": collection,
            "before": _workspace_state_counts(before),
            "after": _workspace_state_counts(after),
            "metrics": manager.metrics(),
        }

    return await runtime.run_blocking("workspace.reconcile", reconcile, timeout=300.0)


def _workspace_state_counts(leases) -> dict[str, int]:
    counts: dict[str, int] = {}
    for lease in leases:
        counts[lease.state] = counts.get(lease.state, 0) + 1
    return counts


@router.post("/repositories/inspect")
async def inspect_repository(request: Request, path: str = Query(...)) -> dict:
    from pathlib import Path

    service = request.app.state.ctx.require_service("repository_state")
    runtime = request.app.state.ctx.require_service("async_runtime")
    result = await service.refresh_async(Path(path), runtime)
    return result.model_dump(mode="json")


@router.post("/repositories/reconcile")
def reconcile_repository_snapshots(
    request: Request, body: RepositoryReconcileRequest
) -> list[dict]:
    from pa.repository.state import RepositorySnapshot

    ctx = request.app.state.ctx
    service = ctx.require_service("repository_state")
    snapshots = [
        RepositorySnapshot.model_validate(value.model_dump())
        for value in body.snapshots
    ]
    return [
        item.model_dump(mode="json")
        for item in service.reconcile(
            snapshots,
            unreachable_instances=_unreachable_repository_instances(ctx),
        )
    ]


@router.get("/sessions")
def list_sessions(request: Request) -> list[dict]:
    sessions = request.app.state.ctx.store.list_sessions()
    return [s.model_dump(mode="json") for s in sessions]


@router.get("/agent/status")
def agent_status(request: Request) -> dict:
    agent = request.app.state.ctx.services.get("instance_agent")
    if not agent:
        return {
            "connected": False,
            "prompting": False,
            "active_sessions": 0,
            "queued_prompts": 0,
            "quiescing": False,
            "message": "Agent not started",
        }
    progress = agent.progress()
    result = progress.model_dump(mode="json")
    lifecycle = request.app.state.ctx.services.get("session_lifecycle")
    result["session_lifecycle"] = {
        "metrics": dict(lifecycle.metrics) if lifecycle else {},
    }
    return result


@router.get("/agent/quiesce")
def agent_quiesce_status() -> dict:
    global _quiesce_progress
    if _quiesce_progress is None:
        return QuiesceProgress(
            phase="idle",
            message="No quiesce in progress",
            done=True,
        ).model_dump(mode="json")
    return _quiesce_progress.model_dump(mode="json")


@router.post("/agent/quiesce")
async def agent_quiesce(request: Request, body: QuiesceRequest) -> dict:
    global _quiesce_task, _quiesce_progress
    agent = request.app.state.ctx.require_service("instance_agent")

    if _quiesce_task and not _quiesce_task.done():
        return (_quiesce_progress or agent.progress()).model_dump(mode="json")

    _quiesce_progress = agent.progress()
    _quiesce_progress.phase = "starting"
    _quiesce_progress.message = "Starting ACP quiesce…"

    async def _on_progress(progress: QuiesceProgress) -> None:
        global _quiesce_progress
        _quiesce_progress = progress

    async def _run() -> None:
        global _quiesce_progress
        try:
            snapshot = await agent.quiesce(
                reason=body.reason,
                timeout=body.timeout,
                on_progress=_on_progress,
            )
            _quiesce_progress = QuiesceProgress(
                phase="done",
                connected=False,
                prompting=False,
                active_sessions=snapshot.active_count,
                queued_prompts=snapshot.queued_count,
                message=(
                    f"Quiesced {snapshot.active_count} ACP session"
                    f"{'' if snapshot.active_count == 1 else 's'}"
                    f", {snapshot.queued_count} queued prompt"
                    f"{'' if snapshot.queued_count == 1 else 's'}"
                ),
                done=True,
                snapshot=snapshot.model_dump(mode="json"),
            )
        except Exception as exc:
            progress = agent.progress()
            _quiesce_progress = QuiesceProgress(
                phase="error",
                connected=agent.connected,
                prompting=agent.prompting,
                active_sessions=progress.active_sessions,
                queued_prompts=progress.queued_prompts,
                message=str(exc),
                done=True,
                error=str(exc),
            )

    if body.wait:
        await _run()
        return (_quiesce_progress or agent.progress()).model_dump(mode="json")

    _quiesce_task = asyncio.create_task(_run())
    return (_quiesce_progress or agent.progress()).model_dump(mode="json")


@router.post("/agent/prompt")
async def agent_prompt(request: Request, body: dict) -> dict:
    message = body.get("message", "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message required")
    card_id = body.get("card_id") or body.get("item_id")
    project_id = body.get("project_id")
    principal_id = get_principal_id(request)
    if request.state.instance_authenticated and body.get("principal_id"):
        principal_id = body.get("principal_id", principal_id)
    target_instance_id = body.get("target_instance_id")
    realm_id = body.get("realm_id")

    agent = request.app.state.ctx.require_service("instance_agent")
    if agent.quiescing and not target_instance_id:
        stop_reason = await agent.prompt(
            message,
            item_id=card_id,
            principal_id=principal_id,
            project_id=project_id,
        )
        return {"stop_reason": stop_reason, "queued": stop_reason == "queued"}
    if not agent.connected and not target_instance_id:
        raise HTTPException(status_code=503, detail="Instance agent not connected")

    router_svc = request.app.state.ctx.services.get("execution_router")
    if router_svc:
        stop_reason = await router_svc.prompt(
            message,
            principal_id=principal_id,
            card_id=card_id,
            project_id=project_id,
            realm_id=realm_id,
            target_instance_id=target_instance_id,
            local_agent=agent,
        )
    else:
        stop_reason = await agent.prompt(
            message,
            item_id=card_id,
            principal_id=principal_id,
            project_id=project_id,
        )
    return {"stop_reason": stop_reason}


@router.post("/agent/reconnect")
async def agent_reconnect(request: Request) -> dict:
    agent = request.app.state.ctx.require_service("instance_agent")
    gated = startup_recovery_error(agent)
    if gated:
        raise gated
    try:
        connected = await agent.reconnect()
    except AgentStartupNotReady:
        # Recovery can begin between the readiness check and manager admission.
        raise startup_recovery_error(agent) or HTTPException(
            status_code=503,
            detail={
                "code": "agent_recovery_in_progress",
                "message": "PA is restoring durable agent sessions. Try again shortly.",
                "recoverable": True,
                "retry_after_ms": 250,
                "history_url": "/api/agent/history",
            },
            headers={"Retry-After": "1"},
        ) from None
    return {
        "connected": connected,
        "error": agent.last_error,
    }


@router.get("/config")
def get_config(request: Request) -> dict:
    settings = request.app.state.ctx.settings
    return {
        "instance_id": settings.instance_id,
        "instance_name": settings.instance_name,
        "fleet_id": settings.fleet_id,
        "subscribed_realms": settings.subscribed_realms,
        "zone": settings.zone,
        "capabilities": settings.capabilities,
        "dispatch_capacity": settings.dispatch_capacity,
        "dispatch_provider_capacities": settings.dispatch_provider_capacities,
        "effective_dispatch_capacity": effective_capacity(
            configured=settings.dispatch_capacity,
            provider_capacities=settings.dispatch_provider_capacities,
            capabilities=settings.capabilities,
        ).model_dump(mode="json"),
        "relay_enabled": settings.relay_enabled,
        "host": settings.host,
        "port": settings.port,
        "agent_enabled": settings.agent_enabled,
        "peers": settings.peers,
        "debug": settings.debug,
    }


@router.patch("/config/capacity")
def update_capacity_config(request: Request, body: CapacityConfigUpdate) -> dict:
    """Validate, persist, and immediately advertise execution capacity."""

    require_user(request)
    ctx = request.app.state.ctx
    updated = update_instance_config(
        ctx.settings.data_dir,
        dispatch_capacity=body.dispatch_capacity,
        dispatch_provider_capacities=body.dispatch_provider_capacities,
    )
    ctx.settings.dispatch_capacity = updated.dispatch_capacity
    ctx.settings.dispatch_provider_capacities = dict(
        updated.dispatch_provider_capacities
    )
    fleet = ctx.services.get("fleet_registry")
    if fleet:
        from pa.fleet.join import owner_public_url

        fleet.register_self(
            ctx.settings.instance_id,
            ctx.settings.instance_name,
            owner_public_url(ctx.settings),
            zone=ctx.settings.zone,
            capabilities=list(ctx.settings.capabilities),
            dispatch_capacity=ctx.settings.dispatch_capacity,
            dispatch_provider_capacities=dict(
                ctx.settings.dispatch_provider_capacities
            ),
            relay_enabled=ctx.settings.relay_enabled,
            actor=f"user:{get_principal_id(request)}",
        )
    from pa.fleet.overview import cache_for

    cache_for(ctx.settings.data_dir).invalidate(ctx.settings.instance_id, "activity")
    effective = effective_capacity(
        configured=ctx.settings.dispatch_capacity,
        provider_capacities=ctx.settings.dispatch_provider_capacities,
        capabilities=ctx.settings.capabilities,
    )
    return {
        "ok": True,
        "dispatch_capacity": ctx.settings.dispatch_capacity,
        "dispatch_provider_capacities": ctx.settings.dispatch_provider_capacities,
        "effective_dispatch_capacity": effective.model_dump(mode="json"),
        "takes_effect": "immediately for new placement admissions",
    }


class InstanceModule(Module):
    @property
    def name(self) -> str:
        return "instance"

    @property
    def version(self) -> str:
        return "0.2.0"

    @property
    def description(self) -> str:
        return "Instance identity, health, peers, and agent session API"

    def on_load(self, ctx: AppContext) -> None:
        from pa.repository.state import RepositoryStateService

        ctx.register_service(
            "repository_state",
            RepositoryStateService(ctx.settings.data_dir, ctx.settings.instance_id),
        )

    def api_routers(self):
        return [("/api", router, ["instance"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
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
        ) -> dict:
            """Validate and immediately apply fleet execution capacity."""
            return request_local_pa(
                settings,
                "PATCH",
                "/api/config/capacity",
                json={
                    "dispatch_capacity": dispatch_capacity,
                    "dispatch_provider_capacities": provider_capacities or {},
                },
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
                timeout=300.0,
            )
