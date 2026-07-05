from __future__ import annotations

import base64

from fastapi import APIRouter, HTTPException, Request

from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.domain.store import get_store
from pa.fleet.membership import MembershipStore
from pa.sync.compaction import SyncMetrics
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventLog
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore

router = APIRouter()


def _check_realm_access(request: Request, realm_id: str) -> None:
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    if not membership.has_role(realm_id, "local"):
        raise HTTPException(status_code=403, detail="No access to realm")


@router.get("/sync/refs")
def sync_refs(request: Request, realm: str | None = None) -> list[dict]:
    log: EventLog = request.app.state.ctx.require_service("event_log")
    refs = log.list_refs()
    if realm:
        refs = [r for r in refs if r.realm_id == realm]
    return [r.model_dump() for r in refs]


@router.post("/sync/have")
def sync_have(request: Request, body: dict) -> dict:
    realm_id = body.get("realm_id", "default")
    _check_realm_access(request, realm_id)
    store: ObjectStore = request.app.state.ctx.require_service("object_store")
    remote_hashes = set(body.get("hashes", []))
    local = set(store.list_hashes())
    missing = list(local - remote_hashes)
    return {"missing": missing}


@router.post("/sync/get")
def sync_get(request: Request, body: dict) -> dict:
    store: ObjectStore = request.app.state.ctx.require_service("object_store")
    hashes = body.get("hashes", [])
    objects = {}
    for h in hashes:
        data = store.get(h)
        if data:
            objects[h] = base64.b64encode(data).decode()
    return {"objects": objects}


@router.post("/sync/push")
def sync_push(request: Request, body: dict) -> dict:
    realm_id = body.get("realm_id", "default")
    _check_realm_access(request, realm_id)
    head_hash = body.get("head_hash", "")
    objects_b64 = body.get("objects", {})
    engine: SyncEngine = request.app.state.ctx.require_service("sync_engine")
    log: EventLog = request.app.state.ctx.require_service("event_log")
    store = get_store()

    imported = engine.ingest_objects(objects_b64)
    metrics: SyncMetrics = request.app.state.ctx.require_service("sync_metrics")
    metrics.record_pull(len(imported))

    if head_hash:
        log.advance_ref(realm_id, head_hash)
        store.rebuild_from_log(realm_id)

    return {"imported": len(imported), "head": head_hash}


@router.post("/sync/relay")
async def sync_relay(request: Request, body: dict) -> dict:
    settings = request.app.state.ctx.settings
    if not settings.relay_enabled:
        raise HTTPException(status_code=403, detail="Relay not enabled on this instance")
    target_url = body.get("target_url", "")
    if not target_url:
        raise HTTPException(status_code=400, detail="target_url required")
    import httpx

    headers = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{target_url.rstrip('/')}/api/sync/push",
            json={
                "realm_id": body.get("realm_id", "default"),
                "head_hash": body.get("head_hash", ""),
                "objects": body.get("objects", {}),
            },
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()


@router.get("/sync/conflicts")
async def sync_conflicts(request: Request, realm: str | None = None) -> dict:
    """Report divergent heads across peers (for conflict UI)."""
    import httpx

    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    log: EventLog = request.app.state.ctx.require_service("event_log")
    local_head = log.get_head(realm_id)
    peer_heads: dict[str, str | None] = {settings.instance_id: local_head}
    registry = request.app.state.ctx.require_service("peer_registry")
    headers = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    async with httpx.AsyncClient(timeout=5.0) as client:
        for peer_url in settings.peers:
            try:
                resp = await client.get(
                    f"{peer_url.rstrip('/')}/api/sync/refs?realm={realm_id}",
                    headers=headers,
                )
                resp.raise_for_status()
                for ref in resp.json():
                    if ref.get("realm_id") == realm_id:
                        peer_heads[ref.get("instance_id", peer_url)] = ref.get("head_hash")
            except httpx.HTTPError:
                peer_heads[peer_url] = None
    unique_heads = {h for h in peer_heads.values() if h}
    return {
        "realm_id": realm_id,
        "diverged": len(unique_heads) > 1,
        "heads": peer_heads,
    }


@router.get("/sync/status")
async def sync_status(request: Request, realm: str | None = None) -> dict:
    engine: SyncEngine = request.app.state.ctx.require_service("sync_engine")
    metrics: SyncMetrics = request.app.state.ctx.require_service("sync_metrics")
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    status = engine.status(realm_id)
    status["metrics"] = metrics.snapshot()
    return status


class SyncModule(Module):
    @property
    def name(self) -> str:
        return "sync"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "P2P sync protocol for realm-scoped card state"

    def on_load(self, ctx: AppContext) -> None:
        settings = ctx.settings
        obj_store = ObjectStore(settings.objects_dir)
        event_log = EventLog(obj_store, settings.data_dir, settings.instance_id)
        ctx.register_service("object_store", obj_store)
        ctx.register_service("event_log", event_log)
        ctx.register_service("sync_metrics", SyncMetrics(settings.data_dir))

    async def on_startup(self, app, ctx: AppContext) -> None:
        from pa.sync.engine import SyncEngine

        settings = ctx.settings
        obj_store = ctx.require_service("object_store")
        event_log = ctx.require_service("event_log")
        membership = ctx.require_service("membership")
        peer_table = ctx.require_service("peer_table")
        engine = SyncEngine(settings, obj_store, event_log, peer_table, membership)
        ctx.register_service("sync_engine", engine)

        original_append = event_log.append_event

        def append_with_sync(event, on_commit=None):
            def combined(commit):
                if on_commit:
                    on_commit(commit)
                import asyncio
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(engine.notify_commit(commit.realm_id))
                except RuntimeError:
                    pass
            return original_append(event, on_commit=combined)

        event_log.append_event = append_with_sync  # type: ignore[method-assign]

        for realm in settings.subscribed_realms:
            await engine.anti_entropy(realm)

    def api_routers(self):
        return [("/api", router, ["sync"])]
