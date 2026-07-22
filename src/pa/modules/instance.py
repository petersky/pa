from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from pa.auth.middleware import get_principal_id
from pa.config import get_settings
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.instance.quiesce import QuiesceProgress

router = APIRouter()

_quiesce_task: asyncio.Task[Any] | None = None
_quiesce_progress: QuiesceProgress | None = None


class QuiesceRequest(BaseModel):
    reason: str = "restart"
    timeout: float = Field(default=300.0, ge=1.0, le=3600.0)
    wait: bool = False


class RepositoryReconcileRequest(BaseModel):
    snapshots: list[dict] = Field(default_factory=list)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
    service = request.app.state.ctx.require_service("repository_state")
    fleet = request.app.state.ctx.services.get("fleet_registry")
    unreachable = {
        item.instance_id
        for item in (fleet.list_instances() if fleet else [])
        if not item.healthy
    }
    return [
        item.model_dump(mode="json")
        for item in service.list(unreachable_instances=unreachable)
    ]


@router.post("/repositories/inspect")
def inspect_repository(request: Request, path: str = Query(...)) -> dict:
    from pathlib import Path

    service = request.app.state.ctx.require_service("repository_state")
    return service.refresh(Path(path)).model_dump(mode="json")


@router.post("/repositories/reconcile")
def reconcile_repository_snapshots(
    request: Request, body: RepositoryReconcileRequest
) -> list[dict]:
    from pa.repository.state import RepositorySnapshot

    service = request.app.state.ctx.require_service("repository_state")
    snapshots = [RepositorySnapshot.model_validate(value) for value in body.snapshots]
    return [item.model_dump(mode="json") for item in service.reconcile(snapshots)]


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
    return progress.model_dump(mode="json")


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
        from pa.agent.context import augment_message_with_context

        settings = request.app.state.ctx.settings
        realm = realm_id or settings.primary_realm
        message = augment_message_with_context(
            request.app.state.ctx.store,
            message,
            card_id=card_id,
            project_id=project_id,
            realm_id=realm,
        )
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
    connected = await agent.reconnect()
    return {
        "connected": connected,
        "error": agent.last_error,
    }


@router.get("/config")
def get_config() -> dict:
    settings = get_settings()
    return {
        "instance_id": settings.instance_id,
        "instance_name": settings.instance_name,
        "fleet_id": settings.fleet_id,
        "subscribed_realms": settings.subscribed_realms,
        "zone": settings.zone,
        "capabilities": settings.capabilities,
        "relay_enabled": settings.relay_enabled,
        "host": settings.host,
        "port": settings.port,
        "agent_enabled": settings.agent_enabled,
        "peers": settings.peers,
        "debug": settings.debug,
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
        settings = ctx.settings

        @mcp.tool()
        def instance_info() -> dict:
            """Return information about this PA instance."""
            return {
                "id": settings.instance_id,
                "name": settings.instance_name,
                "fleet_id": settings.fleet_id,
                "subscribed_realms": settings.subscribed_realms,
                "zone": settings.zone,
                "capabilities": settings.capabilities,
                "host": settings.host,
                "port": settings.port,
                "peers": settings.peers,
            }

        @mcp.tool()
        def repository_inspect(path: str) -> dict:
            """Inspect and persist this instance's current Git repository state."""
            from pathlib import Path

            service = ctx.require_service("repository_state")
            return service.refresh(Path(path)).model_dump(mode="json")

        @mcp.tool()
        def repository_snapshots() -> list[dict]:
            """List non-authoritative repository observations by instance."""
            service = ctx.require_service("repository_state")
            fleet = ctx.services.get("fleet_registry")
            unreachable = {
                item.instance_id
                for item in (fleet.list_instances() if fleet else [])
                if not item.healthy
            }
            return [
                item.model_dump(mode="json")
                for item in service.list(unreachable_instances=unreachable)
            ]
