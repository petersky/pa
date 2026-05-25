from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from pa.config import get_settings
from pa.core.contracts import Module
from pa.core.context import AppContext

router = APIRouter()


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/instance")
def instance_info(request: Request) -> dict:
    registry = request.app.state.ctx.require_service("peer_registry")
    return registry.self_info.model_dump()


@router.get("/peers")
async def list_peers(request: Request) -> list[dict]:
    registry = request.app.state.ctx.require_service("peer_registry")
    peers = await registry.discover_peers()
    return [p.model_dump() for p in peers]


@router.get("/sessions")
def list_sessions(request: Request) -> list[dict]:
    sessions = request.app.state.ctx.store.list_sessions()
    return [s.model_dump(mode="json") for s in sessions]


@router.post("/agent/prompt")
async def agent_prompt(request: Request, body: dict) -> dict:
    message = body.get("message", "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message required")
    item_id = body.get("item_id")
    agent = request.app.state.ctx.require_service("instance_agent")
    if not agent.connected:
        raise HTTPException(status_code=503, detail="Instance agent not connected")
    stop_reason = await agent.prompt(message, item_id=item_id)
    return {"stop_reason": stop_reason}


@router.get("/config")
def get_config() -> dict:
    settings = get_settings()
    return {
        "instance_id": settings.instance_id,
        "instance_name": settings.instance_name,
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
        return "0.0.1"

    @property
    def description(self) -> str:
        return "Instance identity, health, peers, and agent session API"

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
                "host": settings.host,
                "port": settings.port,
                "peers": settings.peers,
            }
