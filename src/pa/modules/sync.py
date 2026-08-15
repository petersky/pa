from __future__ import annotations

import asyncio
import base64
from datetime import UTC, datetime
from typing import Annotated, Any, Callable

from fastapi import APIRouter, Header, HTTPException, Request, Response

from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.domain.models import Card, CardEvent, EventType, Project
from pa.domain.store import get_store
from pa.fleet.membership import MembershipStore
from pa.intake.models import IdentityBinding, IntakeEnvelope
from pa.intake.projection import get_envelope_payload, get_identity_payload_by_id
from pa.modules.items import _begin_operation, _replay_operation
from pa.sync.compaction import SyncMetrics
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventLog, StaleSyncHeadError
from pa.sync.infrastructure import get_event_log, get_object_store
from pa.sync.object_store import ObjectStore

router = APIRouter()


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


def _replace_convergence_head(value, previous_head: str | None):
    if isinstance(value, dict):
        return {
            key: (
                {"$pa_commit_hash": True}
                if key == "head" and item == previous_head
                else _replace_convergence_head(item, previous_head)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _replace_convergence_head(item, previous_head) for item in value
        ]
    return value


def _hydrate_commit_hash(value, commit_hash: str):
    if isinstance(value, dict):
        if value == {"$pa_commit_hash": True}:
            return commit_hash
        return {
            key: _hydrate_commit_hash(item, commit_hash)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_hydrate_commit_hash(item, commit_hash) for item in value]
    return value


async def _finalize_conflict_operation(
    ctx: AppContext,
    store,
    log: EventLog,
    *,
    realm_id: str,
    key: str,
    resolution_head: str,
    operation_event: CardEvent,
) -> dict:
    """Persist and replicate an exact, self-reconstructing operation outcome."""
    engine: SyncEngine = ctx.require_service("sync_engine")
    convergence = await engine.converge_realm(realm_id)
    result_template = {
        "realm_id": realm_id,
        "head": {"$pa_commit_hash": True},
        "resolution_head": resolution_head,
        "resolved": int((operation_event.operation_result or {}).get("resolved", 0)),
        "convergence": _replace_convergence_head(
            convergence, convergence.get("head")
        ),
    }
    outcome_event = operation_event.model_copy(
        update={
            "payload": {
                "operation_outcome": True,
                "resolution_head": resolution_head,
            },
            "operation_result": result_template,
            "operation_result_complete": True,
            "timestamp": datetime.now(UTC),
        }
    )

    def persist_outcome():
        with store.mutation():
            return store.commit_event(outcome_event)

    outcome_commit = await _offload(
        ctx, "sync.resolve_outcome", persist_outcome, timeout=120.0
    )
    result = _hydrate_commit_hash(result_template, outcome_commit.hash)
    store.complete_operation(key, result)
    # Replicate the receipt-bearing outcome commit. The commit callback also
    # schedules convergence, so a process loss at this boundary remains safe.
    await engine.converge_realm(realm_id)
    return result


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


def _apply_sync_push_local(
    ctx: AppContext,
    realm_id: str,
    head_hash: str,
    objects_b64: dict[str, str],
) -> tuple[int, str, bool]:
    """Apply a full object/ref/projection transaction in one worker."""
    engine: SyncEngine = ctx.require_service("sync_engine")
    log: EventLog = ctx.require_service("event_log")
    store = ctx.store
    imported = engine.ingest_objects(objects_b64)
    metrics: SyncMetrics = ctx.require_service("sync_metrics")
    metrics.record_pull(len(imported))

    head_changed = False
    if head_hash:
        if not log.get_commit(head_hash):
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "missing_head_object",
                    "realm_id": realm_id,
                    "head": head_hash,
                },
            )
        with store.mutation():
            local_head = log.get_head(realm_id)
            try:
                if local_head and local_head != head_hash:
                    if log.is_ancestor(local_head, head_hash):
                        log.advance_ref(realm_id, head_hash, expected_head=local_head)
                        store.rebuild_from_log(realm_id)
                        head_changed = True
                    elif log.is_ancestor(head_hash, local_head):
                        head_hash = local_head
                    else:
                        compatible, health = log.compatible_histories(
                            local_head, head_hash
                        )
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
                            automatic_resolutions=health.get(
                                "automatic_resolutions", []
                            ),
                        )
                        head_hash = merge.hash
                        store.rebuild_from_log(realm_id)
                        head_changed = True
                elif local_head != head_hash:
                    log.advance_ref(realm_id, head_hash, expected_head=local_head)
                    store.rebuild_from_log(realm_id)
                    head_changed = True
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
    return len(imported), head_hash, head_changed


def _sync_status_local(ctx: AppContext, realm_id: str) -> dict:
    engine: SyncEngine = ctx.require_service("sync_engine")
    metrics: SyncMetrics = ctx.require_service("sync_metrics")
    status = engine.status(realm_id)
    log: EventLog = ctx.require_service("event_log")
    durable_head = log.get_head(realm_id)
    projection_head = ctx.store.get_projection_head(realm_id)
    status.update(
        head=durable_head,
        projection_head=projection_head,
        consistent=durable_head == projection_head,
        writer="server",
        metrics=metrics.snapshot(),
    )
    return status


@router.get("/sync/refs")
def sync_refs(request: Request, realm: str | None = None) -> list[dict]:
    log: EventLog = request.app.state.ctx.require_service("event_log")
    refs = log.list_refs()
    if realm:
        refs = [r for r in refs if r.realm_id == realm]
    store = request.app.state.ctx.store
    result = []
    for ref in refs:
        item = ref.model_dump()
        projection_head = store.get_projection_head(ref.realm_id)
        item["projection_head"] = projection_head
        item["consistent"] = projection_head == ref.head_hash
        result.append(item)
    return result


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
async def sync_push(request: Request, body: dict) -> dict:
    realm_id = body.get("realm_id", "default")
    _check_realm_access(request, realm_id)
    head_hash = body.get("head_hash", "")
    objects_b64 = body.get("objects", {})
    if not isinstance(objects_b64, dict):
        raise HTTPException(status_code=400, detail="objects must be an object")
    ctx: AppContext = request.app.state.ctx
    imported, head_hash, head_changed = await _offload(
        ctx,
        "sync.push_transaction",
        _apply_sync_push_local,
        ctx,
        realm_id,
        head_hash,
        objects_b64,
        timeout=120.0,
    )

    if head_changed:
        engine: SyncEngine = ctx.require_service("sync_engine")
        await engine.notify_commit(realm_id)
    return {"imported": imported, "head": head_hash}


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
    headers = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    headers["Content-Type"] = "application/json"
    engine: SyncEngine = request.app.state.ctx.require_service("sync_engine")
    resp = await engine._request(
        "POST",
        f"{target_url.rstrip('/')}/api/sync/push",
        payload={
            "realm_id": body.get("realm_id", "default"),
            "head_hash": body.get("head_hash", ""),
            "objects": body.get("objects", {}),
        },
        headers=headers,
    )
    resp.raise_for_status()
    data = await engine._response_json(resp)
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="Relay target returned invalid JSON")
    return data


@router.get("/sync/conflicts")
async def sync_conflicts(request: Request, realm: str | None = None) -> dict:
    """Run a fresh anti-entropy pass and return actionable conflict details."""
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    _check_realm_access(request, realm_id)
    engine: SyncEngine = request.app.state.ctx.require_service("sync_engine")
    state = await engine.converge_realm(realm_id)
    return {**state, "diverged": state["phase"] in {"conflict", "retrying"}}


@router.post("/sync/converge")
async def start_sync_convergence(request: Request, body: dict) -> dict:
    realm_id = body.get("realm_id") or request.app.state.ctx.settings.primary_realm
    _check_realm_access(request, realm_id)
    engine: SyncEngine = request.app.state.ctx.require_service("sync_engine")
    task = engine.request_convergence(realm_id)
    if body.get("wait"):
        return await task
    return engine.convergence_status(realm_id)


@router.get("/sync/convergence")
def get_sync_convergence(request: Request, realm: str | None = None) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    _check_realm_access(request, realm_id)
    engine: SyncEngine = request.app.state.ctx.require_service("sync_engine")
    return engine.convergence_status(realm_id)


@router.get("/sync/audit")
def sync_audit(request: Request, realm: str | None = None) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    _check_realm_access(request, realm_id)
    log: EventLog = request.app.state.ctx.require_service("event_log")
    return {"realm_id": realm_id, "entries": log.merge_audit(realm_id)}


@router.post("/sync/conflicts/resolve")
async def resolve_sync_conflicts(
    request: Request, body: dict, response: Response,
    _idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=300),
    ],
) -> dict:
    """Resolve divergent fields by recording an auditable merge commit."""
    realm_id = body.get("realm_id") or request.app.state.ctx.settings.primary_realm
    remote_head = body.get("remote_head", "")
    resolutions = body.get("resolutions") or []
    if not isinstance(resolutions, list) or len(resolutions) > 1000:
        raise HTTPException(
            status_code=413, detail="resolutions must contain at most 1000 entries"
        )
    _check_realm_access(request, realm_id)
    store = get_store()
    key, fingerprint, replay = _replay_operation(
        request,
        operation="sync.resolve_conflicts",
        realm_id=realm_id,
        payload={"remote_head": remote_head, "resolutions": resolutions},
        store=store,
    )
    response.headers["X-PA-Operation-ID"] = key
    if replay is not None:
        response.headers["X-PA-Operation-Replayed"] = "true"
        return replay

    ctx: AppContext = request.app.state.ctx
    log: EventLog = ctx.require_service("event_log")
    durable_operation = await _offload(
        ctx,
        "sync.resolve_operation_lookup",
        log.find_operation_event,
        realm_id,
        key,
    )
    if durable_operation and not durable_operation[2].operation_result_complete:
        _key, _fingerprint, claimed_replay = _begin_operation(
            request,
            operation="sync.resolve_conflicts",
            realm_id=realm_id,
            payload={"remote_head": remote_head, "resolutions": resolutions},
            store=store,
        )
        if claimed_replay is not None:
            response.headers["X-PA-Operation-Replayed"] = "true"
            return claimed_replay
        result = await _finalize_conflict_operation(
            ctx,
            store,
            log,
            realm_id=realm_id,
            key=key,
            resolution_head=durable_operation[0],
            operation_event=durable_operation[2],
        )
        response.headers["X-PA-Operation-Replayed"] = "true"
        return result

    local_head, remote_commit = await _offload(
        ctx,
        "sync.resolve_heads_read",
        lambda: (
            log.get_head(realm_id),
            log.get_commit(remote_head) if remote_head else None,
        ),
    )
    if not local_head or not remote_commit or remote_commit.realm_id != realm_id:
        raise HTTPException(status_code=400, detail="valid remote_head required")

    def prepare_resolution() -> tuple[bool, dict]:
        with store.mutation():
            _ensure_projection_at_head(store, log, realm_id, local_head)
        return log.compatible_histories(local_head, remote_head)

    compatible, health = await _offload(
        ctx,
        "sync.resolve_prepare", prepare_resolution, timeout=60.0
    )
    if compatible:
        raise HTTPException(
            status_code=409, detail="histories do not require manual resolution"
        )

    supplied: dict[tuple[str, str], dict] = {}
    for item in resolutions:
        entity = item.get("entity")
        entity_id = item.get("id")
        if (
            entity not in {"card", "project", "intake", "channel_identity"}
            or not entity_id
        ):
            raise HTTPException(
                status_code=400, detail="each resolution needs entity and id"
            )
        if entity == "card":
            valid_actions = {"update", "delete", "upsert"}
        elif entity == "project":
            valid_actions = {"update", "archive", "upsert"}
        else:
            valid_actions = {"update", "upsert"}
        if item.get("action", "update") not in valid_actions:
            raise HTTPException(
                status_code=400,
                detail=f"invalid {entity} resolution action",
            )
        supplied[(entity, entity_id)] = item
    missing = []
    for conflict in health["conflicts"]:
        entity = conflict["entity"]
        entity_id = conflict["id"]
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
            current = await _offload(
                ctx,
                "sqlite.card_read",
                store.get_card,
                entity_id,
                realm_id=realm_id,
            )
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
                "upsert": EventType.CARD_UPSERTED,
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
                    source_operation="sync.resolve_conflict",
                    causal_parent=local_head,
                    causal_card_version=(
                        current.updated_at.isoformat() if current else None
                    ),
                    field_intent=sorted(fields),
                )
            )
        elif entity == "project":
            current = await _offload(
                ctx,
                "sqlite.project_read",
                store.get_project,
                entity_id,
                realm_id=realm_id,
            )
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
        else:
            current_payload = (
                get_envelope_payload(store, entity_id)
                if entity == "intake"
                else get_identity_payload_by_id(store, entity_id)
            )
            if action == "update" and not current_payload:
                raise HTTPException(
                    status_code=400,
                    detail=f"{entity} {entity_id} requires an upsert resolution",
                )
            model = IntakeEnvelope if entity == "intake" else IdentityBinding
            candidate = current_payload or {"id": entity_id, "realm_id": realm_id}
            try:
                validated = model.model_validate(
                    {**candidate, **fields, "id": entity_id}
                )
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"invalid {entity} resolution for {entity_id}: {exc}",
                ) from exc
            normalized = validated.model_dump(mode="json")
            payload = (
                normalized
                if action == "upsert"
                else {key: normalized[key] for key in fields if key in normalized}
            )
            payload.update(
                id=entity_id,
                version=max(
                    int(candidate.get("version") or 0) + 1,
                    int(normalized["version"]),
                ),
            )
            events.append(
                CardEvent(
                    type=(
                        EventType.INTAKE_ENVELOPE_UPSERTED
                        if entity == "intake"
                        else EventType.CHANNEL_IDENTITY_UPSERTED
                    ),
                    realm_id=realm_id,
                    project_id=(
                        normalized.get("project_id") if entity == "intake" else None
                    ),
                    author_principal=principal,
                    author_instance=instance,
                    payload=payload,
                )
            )

    key, fingerprint, replay = _begin_operation(
        request,
        operation="sync.resolve_conflicts",
        realm_id=realm_id,
        payload={"remote_head": remote_head, "resolutions": resolutions},
        store=store,
    )
    if response is not None:
        response.headers["X-PA-Operation-ID"] = key
    if replay is not None:
        if response is not None:
            response.headers["X-PA-Operation-Replayed"] = "true"
        return replay

    def commit_resolution():
        try:
            with store.mutation():
                merge = log.resolve_heads(
                    realm_id,
                    local_head,
                    remote_head,
                    events,
                    principal,
                    idempotency_key=key,
                    request_fingerprint=fingerprint,
                )
                store.rebuild_from_log(realm_id)
            return merge
        except StaleSyncHeadError as exc:
            store.fail_operation(key, "stale_sync_head")
            raise HTTPException(
                status_code=409,
                detail={"code": "stale_sync_head", "actual_head": exc.actual},
            ) from exc

    merge = await _offload(
        ctx,
        "sync.resolve_commit", commit_resolution, timeout=120.0
    )
    durable_operation = await _offload(
        ctx,
        "sync.resolve_operation_lookup",
        log.find_operation_event,
        realm_id,
        key,
    )
    if not durable_operation:
        raise RuntimeError("resolution commit is missing its operation receipt")
    return await _finalize_conflict_operation(
        ctx,
        store,
        log,
        realm_id=realm_id,
        key=key,
        resolution_head=merge.hash,
        operation_event=durable_operation[2],
    )


@router.get("/sync/status")
async def sync_status(request: Request, realm: str | None = None) -> dict:
    ctx: AppContext = request.app.state.ctx
    realm_id = realm or ctx.settings.primary_realm
    _check_realm_access(request, realm_id)
    return await _offload(
        ctx,
        "sync.status", _sync_status_local, ctx, realm_id, timeout=15.0
    )


@router.post("/sync/reconcile")
def sync_reconcile(
    request: Request,
    response: Response,
    body: dict,
    _idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=300),
    ],
) -> dict:
    """Reload durable refs and repair a stale SQLite projection safely."""
    realm_id = body.get("realm_id") or request.app.state.ctx.settings.primary_realm
    _check_realm_access(request, realm_id)
    key, _fingerprint, replay = _begin_operation(
        request,
        operation="sync.reconcile",
        realm_id=realm_id,
        payload={"realm_id": realm_id},
    )
    response.headers["X-PA-Operation-ID"] = key
    if replay is not None:
        response.headers["X-PA-Operation-Replayed"] = "true"
        return replay
    log: EventLog = request.app.state.ctx.require_service("event_log")
    store = get_store()
    try:
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
        result = {
            "realm_id": realm_id,
            "head": durable_head,
            "projection_head": store.get_projection_head(realm_id),
            "rebuilt": rebuilt,
            "consistent": durable_head == store.get_projection_head(realm_id),
        }
        store.complete_operation(key, result)
        return result
    except Exception as exc:
        store.fail_operation(key, type(exc).__name__)
        raise


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
        from pa.core.live_updates import LiveUpdateBroker

        settings = ctx.settings
        obj_store = get_object_store(settings)
        event_log = get_event_log(settings)
        ctx.register_service("object_store", obj_store)
        ctx.register_service("event_log", event_log)
        ctx.register_service("sync_metrics", SyncMetrics(settings.data_dir))
        ctx.register_service("live_updates", LiveUpdateBroker())

    async def on_startup(self, app, ctx: AppContext) -> None:
        settings = ctx.settings
        obj_store = ctx.require_service("object_store")
        event_log = ctx.require_service("event_log")
        membership = ctx.require_service("membership")
        peer_table = ctx.require_service("peer_table")
        engine = SyncEngine(
            settings,
            obj_store,
            event_log,
            peer_table,
            membership,
            ctx.services.get("fleet_registry"),
            ctx.require_service("async_runtime"),
        )
        ctx.register_service("sync_engine", engine)
        live_updates = ctx.require_service("live_updates")
        live_updates.start()

        original_append = event_log.append_event

        def append_with_sync(event, on_commit=None):
            def combined(commit):
                if on_commit:
                    on_commit(commit)
                live_updates.publish(
                    commit.realm_id,
                    {
                        "type": "cards_changed",
                        "realm_id": commit.realm_id,
                        "head": commit.hash,
                        "source": "local",
                    },
                )
                import asyncio

                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(engine.notify_commit(commit.realm_id))
                except RuntimeError:
                    pass

            return original_append(event, on_commit=combined)

        event_log.append_event = append_with_sync  # type: ignore[method-assign]

        store = ctx.store
        def rebuild_projection(realm_id: str) -> None:
            with store.mutation():
                store.rebuild_from_log(realm_id)
            live_updates.publish(
                realm_id,
                {
                    "type": "cards_changed",
                    "realm_id": realm_id,
                    "head": event_log.get_head(realm_id),
                    "source": "sync",
                },
            )

        engine.on_head_advanced(rebuild_projection)
        runtime = ctx.require_service("async_runtime")

        def repair_local_projections() -> None:
            for realm in settings.subscribed_realms:
                durable_head = event_log.get_head(realm)
                if durable_head and store.get_projection_head(realm) != durable_head:
                    if event_log.get_commit(durable_head):
                        store.rebuild_from_log(realm)

        # Local durability is restored before admission. Peer/DNS/network work is
        # explicitly backgrounded so health and status endpoints become live.
        await runtime.run_blocking(
            "sync.startup_reconcile", repair_local_projections, timeout=120.0
        )
        ctx.register_service("sync_startup_repaired", True)
        engine.start()
        for realm in settings.subscribed_realms:
            engine.request_convergence(realm)

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        engine = ctx.services.get("sync_engine")
        if engine:
            await engine.close()

    def api_routers(self):
        return [("/api", router, ["sync"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
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
