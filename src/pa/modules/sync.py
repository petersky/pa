from __future__ import annotations

import base64

from fastapi import APIRouter, HTTPException, Request

from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.domain.store import get_store
from pa.domain.models import Card, CardEvent, EventType, Project
from pa.fleet.membership import MembershipStore
from pa.sync.compaction import SyncMetrics
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventLog
from pa.sync.event_log import StaleSyncHeadError
from pa.sync.infrastructure import get_event_log, get_object_store
from pa.sync.object_store import ObjectStore

router = APIRouter()


def _membership_principal(request: Request) -> str:
    principal_id = get_principal_id(request)
    if principal_id.startswith("user:"):
        return principal_id[5:]
    return principal_id


def _check_realm_access(request: Request, realm_id: str) -> None:
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    principal_id = _membership_principal(request)
    if not membership.has_role(realm_id, principal_id):
        raise HTTPException(status_code=403, detail="No access to realm")


def _ensure_projection_at_head(store, log: EventLog, realm_id: str, head: str) -> None:
    """Ensure conflict resolution reads entity state for its exact local head."""
    if store.get_projection_head(realm_id) != head:
        if not log.get_commit(head):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "missing_head_object",
                    "realm_id": realm_id,
                    "head": head,
                },
            )
        store.rebuild_from_log(realm_id)
    actual_head = log.get_head(realm_id)
    projection_head = store.get_projection_head(realm_id)
    if actual_head != head or projection_head != head:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "stale_sync_head",
                "message": "The local head changed while preparing conflict resolution; retry",
                "expected_head": head,
                "actual_head": actual_head,
                "projection_head": projection_head,
            },
        )


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
        local_head = log.get_head(realm_id)
        try:
            if local_head and local_head != head_hash:
                if log.is_ancestor(local_head, head_hash):
                    log.advance_ref(realm_id, head_hash, expected_head=local_head)
                    store.rebuild_from_log(realm_id)
                elif log.is_ancestor(head_hash, local_head):
                    head_hash = local_head
                else:
                    compatible, health = log.compatible_histories(local_head, head_hash)
                    if not compatible:
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "code": "sync_conflict",
                                "message": "Diverged histories modify incompatible fields; operator resolution required",
                                "realm_id": realm_id,
                                "local_head": local_head,
                                "remote_head": head_hash,
                                **health,
                            },
                        )
                    merge = log.merge_heads(
                        realm_id,
                        local_head,
                        head_hash,
                        "sync:auto",
                        expected_head=local_head,
                    )
                    head_hash = merge.hash
                    store.rebuild_from_log(realm_id)
            elif local_head != head_hash:
                log.advance_ref(realm_id, head_hash, expected_head=local_head)
                store.rebuild_from_log(realm_id)
        except StaleSyncHeadError as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "stale_sync_head",
                    "message": "The local head changed during sync; retry against the new head",
                    "realm_id": realm_id,
                    "expected_head": exc.expected,
                    "actual_head": exc.actual,
                },
            ) from exc

    return {"imported": len(imported), "head": head_hash}


@router.post("/sync/relay")
async def sync_relay(request: Request, body: dict) -> dict:
    settings = request.app.state.ctx.settings
    if not settings.relay_enabled:
        raise HTTPException(
            status_code=403, detail="Relay not enabled on this instance"
        )
    target_url = body.get("target_url", "")
    if not target_url:
        raise HTTPException(status_code=400, detail="target_url required")
    from urllib.parse import urlparse

    parsed = urlparse(target_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(status_code=400, detail="Invalid target_url")
    host = parsed.hostname or ""
    if host in ("127.0.0.1", "localhost", "::1") or host.startswith("169.254."):
        raise HTTPException(
            status_code=403, detail="Relay to local/metadata hosts is not allowed"
        )
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
                        peer_heads[ref.get("instance_id", peer_url)] = ref.get(
                            "head_hash"
                        )
            except httpx.HTTPError:
                peer_heads[peer_url] = None
    unique_heads = {h for h in peer_heads.values() if h}
    return {
        "realm_id": realm_id,
        "diverged": len(unique_heads) > 1,
        "heads": peer_heads,
    }


@router.post("/sync/conflicts/resolve")
async def resolve_sync_conflicts(request: Request, body: dict) -> dict:
    """Resolve divergent fields by recording an auditable merge commit."""
    realm_id = body.get("realm_id") or request.app.state.ctx.settings.primary_realm
    remote_head = body.get("remote_head", "")
    resolutions = body.get("resolutions") or []
    _check_realm_access(request, realm_id)
    log: EventLog = request.app.state.ctx.require_service("event_log")
    local_head = log.get_head(realm_id)
    remote_commit = log.get_commit(remote_head) if remote_head else None
    if not local_head or not remote_commit or remote_commit.realm_id != realm_id:
        raise HTTPException(status_code=400, detail="valid remote_head required")
    store = get_store()
    with store.mutation():
        _ensure_projection_at_head(store, log, realm_id, local_head)
    compatible, health = log.compatible_histories(local_head, remote_head)
    if compatible:
        raise HTTPException(
            status_code=409, detail="histories do not require manual resolution"
        )

    supplied: dict[tuple[str, str], dict] = {}
    for item in resolutions:
        entity = item.get("entity")
        entity_id = item.get("id")
        if entity not in {"card", "project"} or not entity_id:
            raise HTTPException(
                status_code=400, detail="each resolution needs entity and id"
            )
        valid_actions = (
            {"update", "delete", "upsert"}
            if entity == "card"
            else {"update", "archive", "upsert"}
        )
        if item.get("action", "update") not in valid_actions:
            raise HTTPException(
                status_code=400,
                detail=f"invalid {entity} resolution action",
            )
        supplied[(entity, entity_id)] = item
    missing = []
    for conflict in health["conflicts"]:
        entity, entity_id = conflict["entity"]
        item = supplied.get((entity, entity_id))
        field = conflict["field"]
        if not item or (
            field != "__terminal__" and field not in item.get("fields", {})
        ):
            missing.append({"entity": entity, "id": entity_id, "field": field})
        elif field == "__terminal__" and item.get("action") not in {
            "delete" if entity == "card" else "archive",
            "upsert",
        }:
            missing.append({"entity": entity, "id": entity_id, "field": field})
    if missing:
        raise HTTPException(
            status_code=400,
            detail={"code": "incomplete_resolution", "missing": missing},
        )

    principal = get_principal_id(request)
    instance = request.app.state.ctx.settings.instance_id
    events: list[CardEvent] = []
    for (entity, entity_id), item in supplied.items():
        action = item.get("action", "update")
        fields = dict(item.get("fields") or {})
        if entity == "card":
            current = store.get_card(entity_id, realm_id=realm_id)
            if action == "update" and not current:
                raise HTTPException(
                    status_code=400,
                    detail=f"card {entity_id} requires an upsert resolution",
                )
            if action in {"update", "upsert"}:
                candidate = (
                    current.model_dump(mode="json") if current else {"id": entity_id}
                )
                try:
                    validated = Card.model_validate(
                        {**candidate, **fields, "id": entity_id, "realm_id": realm_id}
                    )
                except ValueError as exc:
                    raise HTTPException(
                        status_code=400,
                        detail=f"invalid card resolution for {entity_id}: {exc}",
                    ) from exc
                normalized = validated.model_dump(mode="json")
                fields = (
                    normalized
                    if action == "upsert"
                    else {key: normalized[key] for key in fields if key in normalized}
                )
            event_type = {
                "delete": EventType.CARD_DELETED,
                "upsert": EventType.CARD_CREATED,
            }.get(action, EventType.CARD_UPDATED)
            events.append(
                CardEvent(
                    type=event_type,
                    realm_id=realm_id,
                    card_id=entity_id,
                    author_principal=principal,
                    author_instance=instance,
                    payload={"id": entity_id, **fields}
                    if action == "upsert"
                    else fields,
                )
            )
        else:
            current = store.get_project(entity_id, realm_id=realm_id)
            if action == "update" and not current:
                raise HTTPException(
                    status_code=400,
                    detail=f"project {entity_id} requires an upsert resolution",
                )
            if action in {"update", "upsert"}:
                candidate = (
                    current.model_dump(mode="json") if current else {"id": entity_id}
                )
                try:
                    validated = Project.model_validate(
                        {**candidate, **fields, "id": entity_id, "realm_id": realm_id}
                    )
                except ValueError as exc:
                    raise HTTPException(
                        status_code=400,
                        detail=f"invalid project resolution for {entity_id}: {exc}",
                    ) from exc
                normalized = validated.model_dump(mode="json")
                fields = (
                    normalized
                    if action == "upsert"
                    else {key: normalized[key] for key in fields if key in normalized}
                )
            event_type = {
                "archive": EventType.PROJECT_ARCHIVED,
                "upsert": EventType.PROJECT_CREATED,
            }.get(action, EventType.PROJECT_UPDATED)
            events.append(
                CardEvent(
                    type=event_type,
                    realm_id=realm_id,
                    project_id=entity_id,
                    author_principal=principal,
                    author_instance=instance,
                    payload={"id": entity_id, **fields}
                    if action == "upsert"
                    else fields,
                )
            )
    with store.mutation():
        try:
            merge = log.resolve_heads(
                realm_id, local_head, remote_head, events, principal
            )
        except StaleSyncHeadError as exc:
            raise HTTPException(
                status_code=409,
                detail={"code": "stale_sync_head", "actual_head": exc.actual},
            ) from exc
        store.rebuild_from_log(realm_id)
    engine: SyncEngine = request.app.state.ctx.require_service("sync_engine")
    await engine.notify_commit(realm_id)
    return {"realm_id": realm_id, "head": merge.hash, "resolved": len(events)}


@router.get("/sync/status")
async def sync_status(request: Request, realm: str | None = None) -> dict:
    engine: SyncEngine = request.app.state.ctx.require_service("sync_engine")
    metrics: SyncMetrics = request.app.state.ctx.require_service("sync_metrics")
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    status = engine.status(realm_id)
    log: EventLog = request.app.state.ctx.require_service("event_log")
    store = get_store()
    durable_head = log.get_head(realm_id)
    projection_head = store.get_projection_head(realm_id)
    status["head"] = durable_head
    status["projection_head"] = projection_head
    status["consistent"] = durable_head == projection_head
    status["writer"] = "server"
    status["metrics"] = metrics.snapshot()
    return status


@router.post("/sync/reconcile")
def sync_reconcile(request: Request, body: dict) -> dict:
    """Reload durable refs and repair a stale SQLite projection safely."""
    realm_id = body.get("realm_id") or request.app.state.ctx.settings.primary_realm
    _check_realm_access(request, realm_id)
    log: EventLog = request.app.state.ctx.require_service("event_log")
    store = get_store()
    log.reload_refs()
    durable_head = log.get_head(realm_id)
    projection_head = store.get_projection_head(realm_id)
    rebuilt = False
    if durable_head and projection_head != durable_head:
        if not log.get_commit(durable_head):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "missing_head_object",
                    "realm_id": realm_id,
                    "head": durable_head,
                },
            )
        store.rebuild_from_log(realm_id)
        rebuilt = True
    return {
        "realm_id": realm_id,
        "head": durable_head,
        "projection_head": store.get_projection_head(realm_id),
        "rebuilt": rebuilt,
        "consistent": durable_head == store.get_projection_head(realm_id),
    }


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
        obj_store = get_object_store(settings)
        event_log = get_event_log(settings)
        ctx.register_service("object_store", obj_store)
        ctx.register_service("event_log", event_log)
        ctx.register_service("sync_metrics", SyncMetrics(settings.data_dir))

    async def on_startup(self, app, ctx: AppContext) -> None:
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

        store = get_store()
        for realm in settings.subscribed_realms:
            durable_head = event_log.get_head(realm)
            if durable_head and store.get_projection_head(realm) != durable_head:
                if event_log.get_commit(durable_head):
                    store.rebuild_from_log(realm)
            advanced = await engine.anti_entropy(realm)
            if advanced:
                store.rebuild_from_log(realm)

    def api_routers(self):
        return [("/api", router, ["sync"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def sync_status(realm: str = "default") -> dict:
            """Check durable/projection sync consistency through the PA server."""
            return request_local_pa(
                ctx.settings, "GET", "/api/sync/status", params={"realm": realm}
            )

        @mcp.tool()
        def sync_reconcile(realm: str = "default") -> dict:
            """Repair a stale local projection from its durable event-log head."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/sync/reconcile",
                json={"realm_id": realm},
            )

        @mcp.tool()
        def resolve_sync_conflicts(
            remote_head: str,
            resolutions: list[dict],
            realm: str = "default",
        ) -> dict:
            """Resolve divergent histories with an explicit auditable merge.

            Each resolution is {entity: card|project, id, action, fields}. Use
            update for field conflicts; delete/archive or a full upsert for a
            delete-vs-edit conflict. Include every field reported as conflicting.
            """
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/sync/conflicts/resolve",
                json={
                    "realm_id": realm,
                    "remote_head": remote_head,
                    "resolutions": resolutions,
                },
            )
