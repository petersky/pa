"""Fleet management, realms, membership, and remote install APIs."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import UTC, datetime
from typing import Any, AsyncIterator
from urllib.parse import quote
from uuid import uuid4

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

from pa.agent.context import augment_message_with_context
from pa.auth.middleware import get_principal_id, require_user
from pa.config import get_settings
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.io import atomic_write_json
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import (
    CardEvent,
    CardLane,
    CardUpdate,
    FleetInstance,
    KnowledgeEntry,
    RealmRole,
    EventType,
)
from pa.execution.dispatch import CompletionOutbox, DispatchRecord, DispatchStore
from pa.fleet.join import (
    apply_reachability_settings,
    ensure_sync_token,
    owner_public_url,
    readiness_issues,
    readiness_warnings,
    register_joiner_on_owner,
    remove_peer_url,
    unwire_instance_peers,
)
from pa.fleet.membership import MembershipStore
from pa.fleet.registry import FleetRegistry
from pa.fleet.remote_install import (
    RemoteInstallRequest,
    get_job_store,
    start_install_job_background,
)
from pa.fleet.update import (
    FleetUpdateJobStore,
    FleetUpdateRequest,
    TERMINAL_PHASES,
    recover_update_jobs,
    start_update_job,
)
from pa.network.peer_table import PeerTable

logger = logging.getLogger(__name__)

FLEET_HEALTH_TIMEOUT = 3.0
FLEET_DETAIL_TIMEOUT = 5.0
FLEET_AGGREGATE_TIMEOUT = 9.0

router = APIRouter()
ui_router = APIRouter()
_peer_update_task: asyncio.Task[Any] | None = None
_peer_update_task_operation_id: str | None = None


def _peer_operation_path(settings, operation_id: str):
    return settings.data_dir / "fleet_peer_updates" / f"{operation_id}.json"


def _read_peer_operation(settings, operation_id: str) -> dict | None:
    path = _peer_operation_path(settings, operation_id)
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else None
    except OSError, ValueError:
        return None


def _write_peer_operation(settings, operation_id: str, payload: dict) -> None:
    path = _peer_operation_path(settings, operation_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, {"operation_id": operation_id, **payload})


def _peer_has_exact_release(settings, channel: str, release) -> bool:
    """Check durable install provenance before treating a no-op as failure."""
    from pa.install.metadata import load_install_metadata
    from pa.update.channels import compare_versions
    from pa.update.registry import ReleaseTrack, normalize_track

    metadata = load_install_metadata(settings.data_dir)
    if normalize_track(channel) == ReleaseTrack.DEV:
        expected_revision = release.revision or release.tag
        return bool(
            metadata
            and normalize_track(metadata.channel) == ReleaseTrack.DEV
            and expected_revision
            and metadata.source_revision == expected_revision
        )
    versions = [__import__("pa").__version__]
    if metadata:
        versions.append(metadata.version)
    for version in versions:
        try:
            if compare_versions(version, release.version) == 0:
                return True
        except ValueError:
            continue
    return False


class RemoteAgentStartBody(BaseModel):
    """Start a standalone or card-linked session on a fleet instance."""

    card_id: str | None = None
    project_id: str | None = None
    title: str | None = None
    message: str = ""
    provider: str | None = None
    model_id: str | None = None
    mode_id: str | None = None
    effort: str | None = None
    cwd: str | None = None
    config: dict[str, str | bool] = Field(default_factory=dict)


class DispatchMaterializeBody(BaseModel):
    dispatch_id: str
    mutation_id: str
    card: dict[str, Any]
    card_version: str
    realm_id: str
    authority_instance_id: str
    authority_url: str
    target_instance_id: str
    session_id: str | None = None


class DispatchCompletionBody(BaseModel):
    mutation_id: str
    card_id: str
    realm_id: str
    card_version: str
    source_instance_id: str
    session_id: str | None = None
    result: dict[str, Any] = Field(default_factory=dict)


def _dispatch_store(request: Request) -> DispatchStore:
    service = request.app.state.ctx.services.get("dispatch_store")
    if service:
        return service
    service = DispatchStore(request.app.state.ctx.settings.data_dir)
    request.app.state.ctx.register_service("dispatch_store", service)
    return service


@router.post("/fleet/dispatch/materialize")
def materialize_dispatch(request: Request, body: DispatchMaterializeBody) -> dict:
    """Make an exact authoritative card version resolvable before session creation."""
    settings = request.app.state.ctx.settings
    if body.target_instance_id != settings.instance_id:
        raise HTTPException(
            status_code=409,
            detail={"code": "wrong_target", "expected": settings.instance_id},
        )
    store = request.app.state.ctx.store
    incoming = body.card
    card_id = str(incoming.get("id") or "")
    if not card_id:
        raise HTTPException(status_code=400, detail="card.id required")
    existing = store.get_card(card_id, realm_id=body.realm_id)
    if existing and existing.updated_at.isoformat() != body.card_version:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "stale_target_card",
                "card_id": card_id,
                "target_version": existing.updated_at.isoformat(),
                "authority_version": body.card_version,
            },
        )
    if not existing:
        event = CardEvent(
            type=EventType.CARD_CREATED,
            realm_id=body.realm_id,
            card_id=card_id,
            author_principal="fleet:dispatch",
            author_instance=body.authority_instance_id,
            payload=incoming,
        )
        log = request.app.state.ctx.require_service("event_log")
        log.append_event(event)
        store.apply_event(event)
    record = DispatchRecord(
        dispatch_id=body.dispatch_id,
        mutation_id=body.mutation_id,
        card_id=card_id,
        realm_id=body.realm_id,
        card_version=body.card_version,
        authority_instance_id=body.authority_instance_id,
        authority_url=body.authority_url,
        target_instance_id=body.target_instance_id,
        session_id=body.session_id,
    )
    try:
        _dispatch_store(request).put(record)
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "idempotency_conflict", "message": str(exc)},
        ) from exc
    return {
        "dispatch_id": body.dispatch_id,
        "card_id": card_id,
        "card_version": body.card_version,
        "resolvable": True,
    }


@router.post("/fleet/dispatch/{dispatch_id}/complete")
def complete_dispatch(
    request: Request, dispatch_id: str, body: DispatchCompletionBody
) -> dict:
    """Apply an authenticated, version-aware completion exactly once at origin."""
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if not record:
        raise HTTPException(
            status_code=409, detail={"code": "unknown_dispatch", "recoverable": True}
        )
    if (
        record.mutation_id != body.mutation_id
        or request.headers.get("idempotency-key") != body.mutation_id
    ):
        raise HTTPException(status_code=409, detail={"code": "idempotency_conflict"})
    if record.state == "acknowledged":
        return {"dispatch_id": dispatch_id, "acknowledged": True, "duplicate": True}
    card = request.app.state.ctx.store.get_card(body.card_id, realm_id=body.realm_id)
    if not card:
        raise HTTPException(
            status_code=409,
            detail={"code": "authority_card_missing", "recoverable": True},
        )
    expected_dispatch_transition = (
        card.lane == CardLane.ACTIVE
        and card.preferred_instance == body.source_instance_id
    )
    if (
        card.updated_at.isoformat() != body.card_version
        and card.lane != CardLane.DONE
        and not expected_dispatch_transition
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "authority_version_conflict",
                "expected": body.card_version,
                "actual": card.updated_at.isoformat(),
            },
        )
    if card.lane != CardLane.DONE:
        request.app.state.ctx.store.update_card(
            card.id,
            CardUpdate(lane=CardLane.DONE),
            realm_id=body.realm_id,
            principal_id="fleet:completion",
            instance_id=request.app.state.ctx.settings.instance_id,
        )
    record.state = "acknowledged"
    record.session_id = body.session_id
    record.completion_payload = body.result
    record.acknowledged_at = datetime.now(UTC)
    ledger.put(record)
    return {"dispatch_id": dispatch_id, "acknowledged": True, "duplicate": False}


def _fleet_context(request: Request) -> dict:
    """Build Fleet page context from local state only (no peer probes).

    Live health and ACP provider status are loaded asynchronously via
    ``GET /api/fleet/health`` so the page shell stays fast.
    """
    ctx = request.app.state.ctx
    settings = ctx.settings
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    membership: MembershipStore = ctx.require_service("membership")
    peer_table: PeerTable = ctx.require_service("peer_table")
    warnings = readiness_warnings(settings)
    issues = readiness_issues(settings)
    primary_realm = (
        settings.primary_realm
        if hasattr(settings, "primary_realm")
        else (
            settings.subscribed_realms[0] if settings.subscribed_realms else "personal"
        )
    )
    return {
        "fleet_instances": fleet.list_instances(),
        "local_version": __import__("pa").__version__,
        "realms": membership.list_realms(),
        "memberships": membership.list_memberships(),
        "peer_routes": peer_table.all_routes(),
        "settings": settings,
        "fleet_id": settings.fleet_id,
        "zone": settings.zone,
        "owner_url": owner_public_url(settings),
        "readiness_warnings": warnings,
        "readiness_issues": issues,
        "has_sync_token": bool(settings.sync_token),
        "primary_realm": primary_realm,
        "cards": ctx.store.list_cards(realm_id=primary_realm),
        "projects": ctx.store.list_projects(realm_id=primary_realm),
    }


@router.get("/fleet/readiness")
def fleet_readiness(request: Request) -> dict:
    require_user(request)
    settings = request.app.state.ctx.settings
    return {
        "owner_url": owner_public_url(settings),
        "instance_url": settings.instance_url,
        "has_sync_token": bool(settings.sync_token),
        "host": settings.host,
        "warnings": readiness_warnings(settings),
        "issues": readiness_issues(settings),
        "subscribed_realms": list(settings.subscribed_realms),
        "peers": list(settings.peers),
    }


@router.post("/fleet/readiness")
async def fleet_update_readiness(
    request: Request,
    body: dict,
    background_tasks: BackgroundTasks,
) -> dict:
    """Update advertised URL and/or bind host from the Fleet UI."""
    require_user(request)
    settings = request.app.state.ctx.settings
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")

    kwargs: dict = {}
    if "instance_url" in body:
        kwargs["instance_url"] = body.get("instance_url") or ""
    if "host" in body:
        kwargs["host"] = body.get("host")
    if body.get("bind_all"):
        kwargs["host"] = "0.0.0.0"

    if not kwargs:
        raise HTTPException(status_code=400, detail="Provide instance_url and/or host")

    try:
        result = apply_reachability_settings(settings, **kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    fleet.register_self(
        settings.instance_id,
        settings.instance_name,
        owner_public_url(settings),
        zone=settings.zone,
        capabilities=list(settings.capabilities),
        relay_enabled=settings.relay_enabled,
    )

    restart_started = False
    if result["restart_required"]:

        def _restart() -> None:
            try:
                from pa.cli import service as svc

                svc.restart(settings)
            except Exception:
                pass

        background_tasks.add_task(_restart)
        restart_started = True

    return {
        "ok": True,
        "restart_required": result["restart_required"],
        "restart_started": restart_started,
        "service_refreshed": result["service_refreshed"],
        "instance_url": result["instance_url"],
        "host": result["host"],
        "owner_url": result["owner_url"],
        "warnings": result["warnings"],
        "issues": result["issues"],
        "has_sync_token": bool(settings.sync_token),
    }


@router.post("/fleet/ensure-sync-token")
def fleet_ensure_sync_token(request: Request) -> dict:
    require_user(request)
    settings = request.app.state.ctx.settings
    token = ensure_sync_token(settings)
    return {"ok": True, "has_sync_token": bool(token)}


@router.get("/fleet/instances")
def list_fleet_instances(request: Request) -> list[dict]:
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    return [i.model_dump(mode="json") for i in fleet.list_instances()]


@router.post("/fleet/join")
async def fleet_join(request: Request, body: dict) -> dict:
    token = body.get("token", "")
    joiner_id = body.get("instance_id", "")
    name = body.get("name", "remote")
    url = body.get("url", "")
    zone = body.get("zone", "default")
    capabilities = body.get("capabilities", [])
    if not token or not joiner_id:
        raise HTTPException(status_code=400, detail="token and instance_id required")

    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    join = fleet.consume_join_token(token)
    if not join:
        raise HTTPException(status_code=400, detail="Invalid or expired join token")

    settings = get_settings()
    # Prefer live app settings when available (keeps in-memory peers/sync_token).
    if hasattr(request.app.state, "ctx"):
        settings = request.app.state.ctx.settings
    owner_url = owner_public_url(settings)
    peer_table: PeerTable = request.app.state.ctx.require_service("peer_table")
    realms = list(settings.subscribed_realms)

    inst, sync_token = register_joiner_on_owner(
        fleet,
        peer_table,
        settings,
        joiner_id=joiner_id,
        name=name,
        url=url or owner_url,
        zone=zone,
        capabilities=capabilities,
        realms=realms,
    )
    owner_inst = fleet.get_instance(settings.instance_id)
    return {
        "fleet_id": join.fleet_id,
        "owner_url": owner_url,
        "owner_instance": owner_inst.model_dump(mode="json") if owner_inst else None,
        "subscribed_realms": realms,
        "sync_token": sync_token,
        "peers": [owner_url],
        "instance": inst.model_dump(mode="json"),
    }


@router.post("/fleet/join-token")
def create_join_token(request: Request) -> dict:
    require_user(request)
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    settings = request.app.state.ctx.settings
    ensure_sync_token(settings)
    principal = get_principal_id(request)
    join = fleet.create_join_token(created_by=principal)
    owner = owner_public_url(settings)
    return {
        "token": join.token,
        "expires_at": join.expires_at.isoformat(),
        "fleet_id": join.fleet_id,
        "owner_url": owner,
        "join_command": (
            f"PA_FLEET_OWNER_URL={owner} pa fleet join {join.token} "
            f"--url http://<remote-host>:8080 --name <remote-name>"
        ),
    }


@router.post("/fleet/register-remote")
async def register_remote(request: Request, body: dict) -> dict:
    require_user(request)
    settings = request.app.state.ctx.settings
    peer_table: PeerTable = request.app.state.ctx.require_service("peer_table")
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")

    if "instance_id" not in body or not body.get("instance_id"):
        body = {**body, "instance_id": str(uuid4())}
    inst = FleetInstance.model_validate(body)
    if inst.url.lower().startswith(("javascript:", "data:", "vbscript:")):
        raise HTTPException(status_code=400, detail="Invalid instance URL scheme")

    registered, sync_token = register_joiner_on_owner(
        fleet,
        peer_table,
        settings,
        joiner_id=inst.instance_id,
        name=inst.name,
        url=inst.url,
        zone=inst.zone,
        capabilities=inst.capabilities,
        realms=list(settings.subscribed_realms),
    )
    data = registered.model_dump(mode="json")
    data["sync_token_set"] = bool(sync_token)
    return data


@router.delete("/fleet/instances/{instance_id}")
def remove_instance(request: Request, instance_id: str) -> dict:
    require_user(request)
    settings = request.app.state.ctx.settings
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    peer_table: PeerTable = request.app.state.ctx.require_service("peer_table")
    if instance_id == settings.instance_id:
        raise HTTPException(status_code=400, detail="Cannot remove the local instance")
    inst = fleet.get_instance(instance_id)
    if not inst:
        raise HTTPException(status_code=404, detail="Instance not found")
    unwire_instance_peers(peer_table, instance_id=instance_id, url=inst.url)
    remove_peer_url(settings, inst.url)
    fleet.remove_instance(instance_id)
    return {"removed": instance_id}


@router.get("/fleet/health")
async def fleet_health(request: Request, instance_id: str | None = None) -> list[dict]:
    """Return bounded, independent health dimensions for every fleet instance."""
    require_user(request)
    ctx = request.app.state.ctx
    settings = ctx.settings
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    instances = list(fleet.list_instances())
    if instance_id:
        instances = [inst for inst in instances if inst.instance_id == instance_id]
        if not instances:
            raise HTTPException(status_code=404, detail="Fleet instance not found")
    if not instances:
        return []

    headers: dict[str, str] = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"

    async with httpx.AsyncClient(timeout=FLEET_DETAIL_TIMEOUT) as client:

        async def remote_get(url: str, *, timeout: float, headers=None):
            return await asyncio.wait_for(
                client.get(url, headers=headers, timeout=timeout), timeout=timeout
            )

        async def dimension(coro, parser, *, timeout: float) -> tuple[str, Any]:
            try:
                response = await asyncio.wait_for(coro, timeout=timeout)
                if response.status_code != 200:
                    return "error", None
                return "up", parser(response.json())
            except TimeoutError, asyncio.TimeoutError:
                return "timeout", None
            except httpx.HTTPError, ValueError, TypeError, AttributeError:
                return "error", None

        def dict_payload(value: Any) -> dict:
            if not isinstance(value, dict):
                raise TypeError("expected an object response")
            return value

        async def check_one(inst: FleetInstance) -> dict:
            is_local = inst.instance_id == settings.instance_id
            health_state = "up" if is_local else "down"
            providers: list = []
            current_version = None
            available_version = None
            upgrade_available = False
            update_channel = None
            providers_state = status_state = update_state = "error"
            base = inst.url.rstrip("/")
            if not is_local:
                try:
                    resp = await remote_get(
                        f"{base}/api/health", timeout=FLEET_HEALTH_TIMEOUT
                    )
                    health_state = "up" if resp.status_code == 200 else "down"
                except TimeoutError, asyncio.TimeoutError:
                    health_state = "timeout"
                except httpx.HTTPError:
                    health_state = "down"
            if health_state == "up":
                if is_local:
                    from pa.acp.providers.resolve import list_provider_summaries
                    from pa.release.version import read_version

                    try:
                        current_version = read_version()
                    except OSError, RuntimeError, ValueError:
                        current_version = None
                        status_state = "error"
                    update_channel = settings.release_track
                    if current_version:
                        status_state = "up"

                    async def local_providers() -> tuple[str, list]:
                        try:
                            value = await asyncio.wait_for(
                                asyncio.to_thread(
                                    list_provider_summaries, settings.data_dir
                                ),
                                timeout=FLEET_DETAIL_TIMEOUT,
                            )
                            return "up", value
                        except TimeoutError, asyncio.TimeoutError:
                            return "timeout", []
                        except Exception:
                            return "error", []

                    async def local_update() -> tuple[str, Any]:
                        # Update discovery may use the network, even for local.
                        from pa.update.runner import check_update

                        try:
                            value = await asyncio.wait_for(
                                asyncio.to_thread(check_update, settings),
                                timeout=FLEET_DETAIL_TIMEOUT,
                            )
                            return "up", value
                        except TimeoutError, asyncio.TimeoutError:
                            return "timeout", None
                        except Exception:
                            return "error", None

                    provider_result, update_result = await asyncio.gather(
                        local_providers(), local_update()
                    )
                    providers_state, providers = provider_result
                    update_state, update = update_result
                    if update:
                        available_version = update.latest
                        upgrade_available = update.upgrade_available
                else:
                    (
                        provider_result,
                        status_result,
                        update_result,
                    ) = await asyncio.gather(
                        dimension(
                            remote_get(
                                f"{base}/api/agent/providers",
                                headers=headers,
                                timeout=FLEET_DETAIL_TIMEOUT,
                            ),
                            lambda value: value if isinstance(value, list) else [],
                            timeout=FLEET_DETAIL_TIMEOUT,
                        ),
                        dimension(
                            remote_get(
                                f"{base}/api/status",
                                headers=headers,
                                timeout=FLEET_DETAIL_TIMEOUT,
                            ),
                            dict_payload,
                            timeout=FLEET_DETAIL_TIMEOUT,
                        ),
                        dimension(
                            remote_get(
                                f"{base}/api/fleet/peer-update-check",
                                headers=headers,
                                timeout=FLEET_DETAIL_TIMEOUT,
                            ),
                            dict_payload,
                            timeout=FLEET_DETAIL_TIMEOUT,
                        ),
                    )
                    providers_state, provider_data = provider_result
                    status_state, status_data = status_result
                    update_state, update_data = update_result
                    providers = provider_data or []
                    if status_data:
                        current_version = status_data.get("version")
                        update_channel = status_data.get("release_track")
                    if update_data:
                        available_version = update_data.get("available_version")
                        upgrade_available = bool(update_data.get("upgrade_available"))
                        update_channel = update_data.get("channel") or update_channel
            else:
                providers_state = status_state = update_state = "down"
            data = inst.model_dump(mode="json")
            data["healthy"] = health_state == "up"
            data["state"] = health_state
            data["providers_state"] = providers_state
            data["status_state"] = status_state
            data["update_state"] = update_state
            data["providers"] = providers
            data["current_version"] = current_version
            data["available_version"] = available_version
            data["upgrade_available"] = upgrade_available
            data["update_channel"] = update_channel
            return data

        tasks = [asyncio.create_task(check_one(inst)) for inst in instances]
        done, pending = await asyncio.wait(tasks, timeout=FLEET_AGGREGATE_TIMEOUT)
        completed: dict[asyncio.Task, dict] = {}
        failed: set[asyncio.Task] = set()
        for task in done:
            if task.cancelled():
                continue
            try:
                completed[task] = task.result()
            except Exception:
                # A rendering contract is more useful than allowing one unusual
                # probe/parser failure to discard every peer's completed result.
                failed.add(task)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        results = []
        for inst, task in zip(instances, tasks, strict=True):
            if task in completed:
                results.append(completed[task])
                continue
            data = inst.model_dump(mode="json")
            terminal_state = "error" if task in failed else "timeout"
            data.update(
                {
                    "healthy": False,
                    "state": terminal_state,
                    "providers": [],
                    "providers_state": terminal_state,
                    "status_state": terminal_state,
                    "update_state": terminal_state,
                    "current_version": None,
                    "available_version": None,
                    "upgrade_available": False,
                    "update_channel": None,
                }
            )
            results.append(data)

    now = datetime.now(UTC)
    for inst, live in zip(instances, results, strict=True):
        inst.healthy = bool(live.get("healthy"))
        if inst.healthy:
            inst.last_seen = now
        fleet.upsert_instance(inst)
        live["last_seen"] = inst.last_seen.isoformat() if inst.last_seen else None
    return list(results)


@router.post("/fleet/install-remote")
async def install_remote(request: Request, body: dict) -> dict:
    require_user(request)
    settings = request.app.state.ctx.settings
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    store = get_job_store(settings)

    host = (body.get("host") or "").strip()
    user = (body.get("user") or "").strip()
    instance_name = (body.get("instance_name") or body.get("name") or "").strip()
    instance_url = (body.get("instance_url") or body.get("url") or "").strip()
    if not host or not user or not instance_name or not instance_url:
        raise HTTPException(
            status_code=400,
            detail="host, user, instance_name, and instance_url are required",
        )
    if not settings.instance_url and not settings.host:
        raise HTTPException(
            status_code=400, detail="Owner instance_url is not configured"
        )

    warnings = readiness_warnings(settings)
    # Allow install even with warnings, but surface them.
    req = RemoteInstallRequest(
        host=host,
        user=user,
        port=int(body.get("port") or 22),
        identity_file=(body.get("identity_file") or "").strip(),
        password=body.get("password") or "",
        passphrase=body.get("passphrase") or "",
        instance_name=instance_name,
        instance_url=instance_url,
        channel=(body.get("channel") or settings.release_track or "release").strip(),
        realm=(body.get("realm") or "").strip(),
        join_only=bool(body.get("join_only")),
    )
    # Clear secrets from body reference — they live only on the request object.
    body.pop("password", None)
    body.pop("passphrase", None)

    ensure_sync_token(settings)
    job = start_install_job_background(settings, fleet, store, req)
    return {**job.to_public_dict(), "readiness_warnings": warnings}


@router.get("/fleet/install-remote/{job_id}")
def install_remote_status(request: Request, job_id: str) -> dict:
    require_user(request)
    store = get_job_store(request.app.state.ctx.settings)
    job = store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Install job not found")
    return job.to_public_dict()


@router.get("/fleet/install-remote/{job_id}/events")
async def install_remote_events(request: Request, job_id: str):
    require_user(request)
    store = get_job_store(request.app.state.ctx.settings)
    job = store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Install job not found")

    async def event_stream():
        last_len = 0
        for _ in range(600):
            current = store.get(job_id)
            if not current:
                yield "event: error\ndata: missing\n\n"
                return
            if len(current.log_lines) > last_len:
                for line in current.log_lines[last_len:]:
                    yield f"data: {line}\n\n"
                last_len = len(current.log_lines)
            yield f"event: status\ndata: {current.status.value}\n\n"
            if current.status.value in ("succeeded", "failed"):
                if current.error:
                    yield f"event: error\ndata: {current.error}\n\n"
                yield f"event: done\ndata: {current.status.value}\n\n"
                return
            await asyncio.sleep(0.5)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/realms")
def list_realms(request: Request) -> list[dict]:
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    return [r.model_dump() for r in membership.list_realms()]


@router.post("/realms")
def create_realm(request: Request, body: dict) -> dict:
    require_user(request)
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    realm_id = body.get("id", "")
    if not realm_id:
        raise HTTPException(status_code=400, detail="realm id required")
    realm = membership.ensure_realm(realm_id, body.get("name", ""))
    principal = get_principal_id(request)
    uid = principal[5:] if principal.startswith("user:") else "local"
    membership.ensure_owner_membership(
        realm_id, uid, fleet_id=request.app.state.ctx.settings.fleet_id
    )
    return realm.model_dump()


@router.post("/realms/invite")
def realm_invite(request: Request, body: dict) -> dict:
    require_user(request)
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    realm_id = body.get("realm_id", request.app.state.ctx.settings.primary_realm)
    role = RealmRole(body.get("role", "editor"))
    invite = membership.create_invite(
        realm_id, role, created_by=get_principal_id(request)
    )
    return {
        "token": invite.token,
        "realm_id": invite.realm_id,
        "role": invite.role.value,
        "expires_at": invite.expires_at.isoformat() if invite.expires_at else None,
    }


@router.post("/realms/accept-invite")
def accept_invite(request: Request, body: dict) -> dict:
    require_user(request)
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    token = body.get("token", "")
    principal = get_principal_id(request)
    uid = principal[5:] if principal.startswith("user:") else "local"
    m = membership.accept_invite(
        token, uid, fleet_id=request.app.state.ctx.settings.fleet_id
    )
    if not m:
        raise HTTPException(status_code=400, detail="Invalid invite")
    return m.model_dump(mode="json")


def _fleet_instance_or_404(request: Request, instance_id: str):
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    for inst in fleet.list_instances():
        if inst.instance_id == instance_id:
            return inst
    raise HTTPException(status_code=404, detail="Fleet instance not found")


def _peer_headers(request: Request) -> dict[str, str]:
    settings = request.app.state.ctx.settings
    headers = {"Accept": "application/json"}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    return headers


def _require_instance(request: Request) -> None:
    if not getattr(request.state, "instance_authenticated", False):
        raise HTTPException(
            status_code=401, detail="Fleet instance authentication required"
        )


@router.post("/fleet/peer-update")
async def peer_update(request: Request, body: dict) -> dict:
    """Authenticated peer-side install trigger; the controller owns durable state."""
    global _peer_update_task, _peer_update_task_operation_id
    _require_instance(request)
    settings = request.app.state.ctx.settings
    channel = (body.get("channel") or settings.release_track or "release").strip()
    target_version = (body.get("target_version") or "").strip() or None
    target_identity = (body.get("target_identity") or "").strip() or None
    operation_id = (body.get("operation_id") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9-]{1,80}", operation_id):
        raise HTTPException(status_code=400, detail="A valid operation_id is required")
    if not target_version:
        raise HTTPException(
            status_code=400,
            detail="target_version is required for a fleet peer update",
        )

    existing = _read_peer_operation(settings, operation_id)
    if existing:
        if existing.get("target_version") != target_version or (
            target_identity and existing.get("target_identity") != target_identity
        ):
            raise HTTPException(
                status_code=409,
                detail="Operation id already belongs to a different update target",
            )
        if _peer_update_task and not _peer_update_task.done():
            if _peer_update_task_operation_id == operation_id:
                return {"accepted": True, **existing}
            raise HTTPException(
                status_code=409, detail="A fleet update is already running on this peer"
            )
        if existing.get("status") not in {"installing", "installed"}:
            return {"accepted": True, **existing}

    from pa.update.channels import resolve_release
    from pa.update.runner import apply_update

    try:
        release = await asyncio.to_thread(
            resolve_release,
            channel,
            target_version,
            repo=settings.update_repo,
            revision=target_identity,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if existing:
        if existing.get("target_version") != release.version or existing.get(
            "target_identity"
        ) != (release.revision or release.tag or release.version):
            raise HTTPException(
                status_code=409,
                detail="Operation id already belongs to a different update target",
            )
    if _peer_update_task and not _peer_update_task.done():
        raise HTTPException(
            status_code=409, detail="A fleet update is already running on this peer"
        )

    operation = existing or {
        "status": "installing",
        "target_version": release.version,
        "target_identity": release.revision or release.tag or release.version,
        "channel": channel,
        "error": None,
    }
    if not existing:
        _write_peer_operation(settings, operation_id, operation)

    async def _install_and_restart() -> None:
        await asyncio.sleep(0.25)
        try:
            exact_target = _peer_has_exact_release(settings, channel, release)
            if not exact_target:
                result = await asyncio.to_thread(
                    apply_update,
                    settings,
                    channel_name=channel,
                    restart=False,
                    release=release,
                )
                exact_target = result.upgrade_available or _peer_has_exact_release(
                    settings, channel, release
                )
                installed_version = result.current
            else:
                installed_version = release.version
            if not exact_target:
                raise RuntimeError(
                    "Installer completed without reaching the requested PA target"
                )
            installed_operation = {
                **operation,
                "status": "installed",
                "installed_version": installed_version,
                "message": "Installation complete; preparing a host-managed restart",
            }
            _write_peer_operation(settings, operation_id, installed_operation)
            restarting_operation = {
                **installed_operation,
                "status": "restarting",
                "restart_stage": "requested",
                "message": "Handing restart control to the host service manager",
            }
            _write_peer_operation(settings, operation_id, restarting_operation)
            from pa.cli import service as svc
            from pa.instance.quiesce import request_skip_quiesce

            request_skip_quiesce(settings.data_dir)

            def restart_progress(message: str) -> None:
                current = (
                    _read_peer_operation(settings, operation_id) or restarting_operation
                )
                _write_peer_operation(
                    settings,
                    operation_id,
                    {**current, "status": "restarting", "message": message},
                )

            await asyncio.to_thread(
                svc.request_restart,
                settings,
                progress=restart_progress,
            )
        except Exception as exc:
            current = _read_peer_operation(settings, operation_id) or operation
            restart_was_requested = current.get("status") == "restarting"
            _write_peer_operation(
                settings,
                operation_id,
                {
                    **current,
                    "status": "restart_failed" if restart_was_requested else "failed",
                    "error": str(exc),
                    "message": (
                        "The restart request reported an error; the controller will "
                        "verify peer health"
                        if restart_was_requested
                        else "Installation failed"
                    ),
                },
            )
            return

    _peer_update_task = asyncio.create_task(_install_and_restart())
    _peer_update_task_operation_id = operation_id
    return {
        "accepted": True,
        "current_version": __import__("pa").__version__,
        "target_version": release.version,
        "target_identity": release.revision or release.tag or release.version,
        "channel": channel,
        "operation_id": operation_id,
        "status": "installing",
    }


@router.get("/fleet/peer-update/{operation_id}")
def peer_update_status(request: Request, operation_id: str) -> dict:
    _require_instance(request)
    operation = _read_peer_operation(request.app.state.ctx.settings, operation_id)
    if not operation:
        raise HTTPException(status_code=404, detail="Peer update operation not found")
    return operation


@router.get("/fleet/peer-update-check")
async def peer_update_check(request: Request, channel: str | None = None) -> dict:
    _require_instance(request)
    settings = request.app.state.ctx.settings
    from pa.update.runner import check_update

    result = await asyncio.to_thread(check_update, settings, channel_name=channel)
    return {
        "current_version": result.current,
        "available_version": result.latest,
        "upgrade_available": result.upgrade_available,
        "channel": channel or settings.release_track,
        "target_identity": (
            result.release.revision
            if result.release and result.release.revision
            else (result.release.tag if result.release else None)
        ),
    }


def _update_store(request: Request) -> FleetUpdateJobStore:
    return request.app.state.ctx.require_service("fleet_update_job_store")


@router.get("/fleet/instances/{instance_id}/update-check")
async def fleet_instance_update_check(
    request: Request, instance_id: str, channel: str | None = None
) -> dict:
    """Resolve availability for the exact peer and track the operator selected."""
    require_user(request)
    settings = request.app.state.ctx.settings
    if not settings.sync_token:
        raise HTTPException(
            status_code=409, detail="Configure a fleet sync token before checking peers"
        )
    inst = _fleet_instance_or_404(request, instance_id)
    selected = (channel or settings.release_track or "release").strip()
    headers = _peer_headers(request)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            status_resp, update_resp = await asyncio.gather(
                client.get(f"{inst.url.rstrip('/')}/api/status", headers=headers),
                client.get(
                    f"{inst.url.rstrip('/')}/api/fleet/peer-update-check",
                    headers=headers,
                    params={"channel": selected},
                ),
            )
        status_resp.raise_for_status()
        update_resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"Could not check peer update availability: {exc}"
        ) from exc
    status_data = status_resp.json()
    update_data = update_resp.json()
    return {
        "instance_id": inst.instance_id,
        "current_version": status_data.get("version"),
        "available_version": update_data.get("available_version"),
        "upgrade_available": bool(update_data.get("upgrade_available")),
        "channel": update_data.get("channel") or selected,
        "target_identity": update_data.get("target_identity"),
    }


@router.post("/fleet/instances/{instance_id}/update", status_code=202)
async def update_fleet_instance(
    request: Request,
    instance_id: str,
    body: FleetUpdateRequest,
) -> dict:
    require_user(request)
    settings = request.app.state.ctx.settings
    if not settings.sync_token:
        raise HTTPException(
            status_code=409, detail="Configure a fleet sync token before updating peers"
        )
    inst = _fleet_instance_or_404(request, instance_id)
    store = _update_store(request)
    try:
        job = store.create(inst, body, settings.release_track)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "An update is already active for this instance",
                "job_id": str(exc),
            },
        ) from exc
    start_update_job(settings, store, job)
    return job.public_dict()


@router.get("/fleet/instances/{instance_id}/update")
def list_fleet_instance_updates(request: Request, instance_id: str) -> list[dict]:
    require_user(request)
    _fleet_instance_or_404(request, instance_id)
    return [
        job.public_dict()
        for job in _update_store(request).list()
        if job.instance_id == instance_id
    ]


def _update_job_or_404(request: Request, instance_id: str, job_id: str):
    job = _update_store(request).get(job_id)
    if not job or job.instance_id != instance_id:
        raise HTTPException(status_code=404, detail="Fleet update job not found")
    return job


@router.get("/fleet/instances/{instance_id}/update/{job_id}")
def fleet_instance_update_status(
    request: Request, instance_id: str, job_id: str
) -> dict:
    require_user(request)
    return _update_job_or_404(request, instance_id, job_id).public_dict()


@router.get("/fleet/instances/{instance_id}/update/{job_id}/events")
async def fleet_instance_update_events(request: Request, instance_id: str, job_id: str):
    require_user(request)
    _update_job_or_404(request, instance_id, job_id)
    store = _update_store(request)
    cursor_value = request.query_params.get("after") or request.headers.get(
        "last-event-id", "0"
    )
    try:
        initial_cursor = max(0, int(cursor_value))
    except TypeError, ValueError:
        raise HTTPException(
            status_code=400, detail="Invalid update event cursor"
        ) from None

    async def stream():
        cursor = initial_cursor
        while True:
            job = store.get(job_id)
            if not job:
                yield 'event: error\ndata: {"message":"job missing"}\n\n'
                return
            for event in store.events_after(job, cursor):
                seq = int(event["seq"])
                yield f"id: {seq}\nevent: phase\ndata: {json.dumps(event)}\n\n"
                cursor = seq
            yield f"event: status\ndata: {json.dumps(job.public_dict())}\n\n"
            if job.phase in TERMINAL_PHASES:
                yield f"event: done\ndata: {json.dumps(job.public_dict())}\n\n"
                return
            await asyncio.sleep(0.5)

    return StreamingResponse(stream(), media_type="text/event-stream")


def _agent_path(path: str) -> str:
    parts = path.strip("/").split("/")
    if not path.strip("/") or any(part in {"", ".", ".."} for part in parts):
        raise HTTPException(status_code=400, detail="Invalid agent proxy path")
    return "/".join(quote(part, safe="-._~") for part in parts)


async def _peer_agent_json(
    request: Request,
    instance_id: str,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    timeout: float = 120.0,
) -> dict | list:
    inst = _fleet_instance_or_404(request, instance_id)
    url = f"{inst.url.rstrip('/')}/api/agent/{_agent_path(path)}"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(
                method,
                url,
                headers=_peer_headers(request),
                json=body,
            )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Peer unreachable: {exc}") from exc
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail")
        except ValueError, AttributeError:
            detail = resp.text[:500]
        raise HTTPException(
            status_code=resp.status_code, detail=detail or "Peer request failed"
        )
    return resp.json()


async def _peer_dispatch_json(
    request: Request, instance_id: str, body: dict[str, Any]
) -> dict:
    inst = _fleet_instance_or_404(request, instance_id)
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{inst.url.rstrip('/')}/api/fleet/dispatch/materialize",
                headers={
                    **_peer_headers(request),
                    "Idempotency-Key": str(body["mutation_id"]),
                },
                json=body,
            )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "target_unavailable",
                "message": str(exc),
                "recoverable": True,
            },
        ) from exc
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail")
        except ValueError, AttributeError:
            detail = resp.text[:500]
        raise HTTPException(status_code=resp.status_code, detail=detail)
    return resp.json()


async def _assert_dispatch_sync_health(request: Request, realm_id: str) -> None:
    """Never choose an arbitrary realm head for remote work."""
    settings = request.app.state.ctx.settings
    if not settings.peers:
        return
    log = request.app.state.ctx.require_service("event_log")
    heads: dict[str, str | None] = {settings.instance_id: log.get_head(realm_id)}
    headers = _peer_headers(request)
    async with httpx.AsyncClient(timeout=5.0) as client:
        for peer_url in settings.peers:
            try:
                response = await client.get(
                    f"{peer_url.rstrip('/')}/api/sync/refs",
                    params={"realm": realm_id},
                    headers=headers,
                )
                response.raise_for_status()
                refs = response.json()
                heads[peer_url] = next(
                    (r.get("head_hash") for r in refs if r.get("realm_id") == realm_id),
                    None,
                )
            except httpx.HTTPError, ValueError:
                heads[peer_url] = None
    known = {head for head in heads.values() if head}
    unavailable = sorted(peer for peer, head in heads.items() if head is None)
    if unavailable:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "sync_unavailable",
                "message": "Dispatch blocked because not every configured peer reported its realm head",
                "realm_id": realm_id,
                "heads": heads,
                "unavailable": unavailable,
                "recoverable": True,
            },
        )
    if len(known) > 1:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "sync_conflict",
                "message": "Dispatch blocked until realm heads converge",
                "realm_id": realm_id,
                "heads": heads,
            },
        )


def _project_working_directory(
    project,
    *,
    instance_id: str,
    instance_name: str,
) -> str | None:
    if not project:
        return None
    tool_config = project.tool_config or {}
    paths_by_instance = tool_config.get("repo_paths_by_instance") or {}
    mapped_path = paths_by_instance.get(instance_id) or paths_by_instance.get(
        instance_name
    )
    if mapped_path:
        return str(mapped_path)

    development_instance = tool_config.get("development_instance")
    if development_instance not in {instance_id, instance_name}:
        return None
    for repo in project.repos or []:
        path = (
            repo.get("path") if isinstance(repo, dict) else getattr(repo, "path", None)
        )
        if path:
            return str(path)
    return None


@router.post("/fleet/instances/{instance_id}/agent/start")
async def start_remote_agent_work(
    request: Request,
    instance_id: str,
    body: RemoteAgentStartBody,
) -> dict:
    """Create a remote session, optionally dispatching a local card into it."""
    require_user(request)
    ctx = request.app.state.ctx
    settings = ctx.settings
    store = ctx.store
    realm_id = settings.primary_realm
    card = store.get_card(body.card_id, realm_id=realm_id) if body.card_id else None
    if body.card_id and not card:
        raise HTTPException(status_code=404, detail="Card not found")
    if card:
        await _assert_dispatch_sync_health(request, realm_id)
    project_id = body.project_id or (card.project_id if card else None)
    project = store.get_project(project_id, realm_id=realm_id) if project_id else None
    inst = _fleet_instance_or_404(request, instance_id)

    dispatch_body = None
    authority_record = None
    if card:
        dispatch_id = str(uuid4())
        mutation_id = str(uuid4())
        authority_url = settings.instance_url
        if not authority_url or authority_url.startswith(
            ("http://127.", "http://localhost")
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "authority_unroutable",
                    "message": "Configure a fleet-reachable instance_url before remote card dispatch",
                },
            )
        dispatch_body = {
            "dispatch_id": dispatch_id,
            "mutation_id": mutation_id,
            "card": card.model_dump(mode="json"),
            "card_version": card.updated_at.isoformat(),
            "realm_id": realm_id,
            "authority_instance_id": settings.instance_id,
            "authority_url": authority_url,
            "target_instance_id": instance_id,
        }
        authority_record = DispatchRecord(
            dispatch_id=dispatch_id,
            mutation_id=mutation_id,
            card_id=card.id,
            realm_id=realm_id,
            card_version=card.updated_at.isoformat(),
            authority_instance_id=settings.instance_id,
            authority_url=authority_url,
            target_instance_id=instance_id,
            state="dispatching",
        )
        _dispatch_store(request).put(authority_record)
        # This is deliberately before session creation: a missing/stale target
        # blocks actionably and can never leave an orphan agent session.
        await _peer_dispatch_json(request, instance_id, dispatch_body)

    normalized_cwd = (
        store.project_working_directory(project_id, instance_id) if project_id else None
    )
    if not isinstance(normalized_cwd, str):
        normalized_cwd = None

    session_body: dict[str, Any] = {
        "label": f"card:{card.id}" if card else None,
        "title": body.title or (card.title if card else "Remote agent session"),
        "cwd": body.cwd or normalized_cwd,
        "card_id": card.id if card else None,
        "project_id": project_id,
        "provider": body.provider,
        "model_id": body.model_id,
        "mode_id": body.mode_id,
        "effort": body.effort,
        "config": body.config,
        "surface": "execution",
        "dispatch_id": authority_record.dispatch_id if authority_record else None,
    }
    session_body = {
        key: value for key, value in session_body.items() if value not in (None, "")
    }
    snapshot = await _peer_agent_json(
        request,
        instance_id,
        "POST",
        "sessions",
        body=session_body,
    )
    if not isinstance(snapshot, dict):
        raise HTTPException(status_code=502, detail="Peer returned an invalid session")
    session = snapshot.get("session") or snapshot
    session_id = session.get("id") if isinstance(session, dict) else None
    if not session_id:
        raise HTTPException(status_code=502, detail="Peer did not return a session id")
    if authority_record:
        authority_record.session_id = session_id
        authority_record.state = "dispatched"
        _dispatch_store(request).put(authority_record)

    updated_card = None
    if card:
        # Publish ACTIVE before the non-blocking prompt. Completion may arrive as
        # soon as that call is accepted and must never be overwritten afterward.
        updated_card = store.update_card(
            card.id,
            CardUpdate(lane=CardLane.ACTIVE, preferred_instance=instance_id),
            realm_id=realm_id,
            principal_id=get_principal_id(request),
            instance_id=settings.instance_id,
        )

    message = body.message.strip()
    if card and not message:
        message = "Work on this card autonomously. Report progress, blockers, and the final result."
    prompt_result = None
    prompt_error = None
    if message:
        if card or project_id:
            message = augment_message_with_context(
                store,
                message,
                card_id=card.id if card else None,
                project_id=project_id,
                realm_id=realm_id,
            )
        try:
            prompt_result = await _peer_agent_json(
                request,
                instance_id,
                "POST",
                f"sessions/{session_id}/prompt",
                body={
                    "message": message,
                    "card_id": card.id if card else None,
                    "project_id": project_id,
                },
            )
        except HTTPException as exc:
            # The remote session already exists. Preserve its identity and audit trail so
            # the operator can open it and retry instead of losing track of an orphan.
            prompt_error = str(exc.detail)

    store.add_knowledge(
        KnowledgeEntry(
            session_id=session_id,
            item_id=card.id if card else None,
            card_id=card.id if card else None,
            summary=(
                f"Dispatched {card.title!r} to {inst.name} in session {session_id}."
                if card
                else f"Started remote session {session_id} on {inst.name}."
            )
            + (f" Initial prompt failed: {prompt_error}" if prompt_error else ""),
            source="remote_dispatch",
            tags=[
                "remote-operations",
                f"instance:{instance_id}",
                *(["prompt-error"] if prompt_error else []),
            ],
        )
    )
    return {
        "instance": inst.model_dump(mode="json"),
        "session": snapshot,
        "prompt": prompt_result,
        "prompt_error": prompt_error,
        "card": updated_card.model_dump(mode="json") if updated_card else None,
        "dispatch": authority_record.model_dump(mode="json")
        if authority_record
        else None,
    }


@router.api_route(
    "/fleet/instances/{instance_id}/agent/{agent_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
)
async def fleet_agent_proxy(
    request: Request,
    instance_id: str,
    agent_path: str,
) -> Response:
    """Relay the authenticated agent REST/SSE surface through the local PA origin."""
    require_user(request)
    inst = _fleet_instance_or_404(request, instance_id)
    proxied_path = _agent_path(agent_path)
    target = f"{inst.url.rstrip('/')}/api/agent/{proxied_path}"
    headers = _peer_headers(request)
    for name in ("accept", "content-type", "last-event-id"):
        value = request.headers.get(name)
        if value:
            headers[name] = value
    # Session event streams are intentionally unbounded; every other proxied
    # response must retain a finite read timeout so a stalled peer cannot pin a
    # request forever while the controller buffers its body.
    read_timeout = None if proxied_path.endswith("/events") else 120.0
    client = httpx.AsyncClient(timeout=httpx.Timeout(120.0, read=read_timeout))
    try:
        upstream_request = client.build_request(
            request.method,
            target,
            params=list(request.query_params.multi_items()),
            headers=headers,
            content=await request.body(),
        )
        upstream = await client.send(upstream_request, stream=True)
    except httpx.HTTPError as exc:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Peer unreachable: {exc}") from exc

    response_headers = {
        name: value
        for name, value in upstream.headers.items()
        if name.lower() in {"content-type", "cache-control", "content-disposition"}
    }
    content_type = upstream.headers.get("content-type", "")
    if content_type.startswith("text/event-stream"):

        async def relay() -> AsyncIterator[bytes]:
            try:
                try:
                    async for chunk in upstream.aiter_raw():
                        yield chunk
                except httpx.RemoteProtocolError:
                    # A peer restart can end an unbounded SSE response without a
                    # terminating HTTP chunk. At this point response headers have
                    # already been sent, so the only correct behavior is EOF.
                    logger.info(
                        "Peer %s closed agent event stream during restart",
                        instance_id,
                    )
            finally:
                await upstream.aclose()
                await client.aclose()

        return StreamingResponse(
            relay(),
            status_code=upstream.status_code,
            headers=response_headers,
        )

    try:
        content = await upstream.aread()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"Peer response failed: {exc}"
        ) from exc
    finally:
        await upstream.aclose()
        await client.aclose()
    return Response(
        content=content,
        status_code=upstream.status_code,
        headers=response_headers,
    )


def _proxy_agent_providers(
    request: Request,
    instance_id: str,
    method: str,
    suffix: str,
    body: dict | None = None,
) -> dict | list:
    require_user(request)
    inst = _fleet_instance_or_404(request, instance_id)
    settings = request.app.state.ctx.settings
    headers: dict[str, str] = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    url = f"{inst.url.rstrip('/')}/api/agent/providers{suffix}"
    try:
        with httpx.Client(timeout=120.0) as client:
            resp = client.request(method, url, headers=headers, json=body)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Peer unreachable: {exc}") from exc
    if resp.status_code >= 400:
        try:
            payload = resp.json()
            detail = (
                payload.get("detail", payload) if isinstance(payload, dict) else payload
            )
        except ValueError:
            detail = resp.text[:500]
        raise HTTPException(status_code=resp.status_code, detail=detail)
    return resp.json()


@router.get("/fleet/instances/{instance_id}/agent-providers")
def fleet_agent_providers(request: Request, instance_id: str):
    return _proxy_agent_providers(request, instance_id, "GET", "")


@router.get("/fleet/instances/{instance_id}/agent-providers/{provider_id}")
def fleet_agent_provider(request: Request, instance_id: str, provider_id: str):
    return _proxy_agent_providers(request, instance_id, "GET", f"/{provider_id}")


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/install")
def fleet_agent_provider_install(request: Request, instance_id: str, provider_id: str):
    return _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/install"
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/update")
def fleet_agent_provider_update(request: Request, instance_id: str, provider_id: str):
    return _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/update"
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/configure")
async def fleet_agent_provider_configure(
    request: Request, instance_id: str, provider_id: str, body: dict
):
    return _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/configure", body=body
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/probe")
def fleet_agent_provider_probe(request: Request, instance_id: str, provider_id: str):
    return _proxy_agent_providers(request, instance_id, "POST", f"/{provider_id}/probe")


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/login-jobs")
async def fleet_agent_provider_login_start(
    request: Request, instance_id: str, provider_id: str, body: dict
):
    return _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/login-jobs", body=body
    )


@router.post(
    "/fleet/instances/{instance_id}/agent-providers/{provider_id}/codex-cli/install"
)
def fleet_agent_provider_codex_cli_install(
    request: Request, instance_id: str, provider_id: str
):
    return _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/codex-cli/install"
    )


@router.get(
    "/fleet/instances/{instance_id}/agent-providers/{provider_id}/login-jobs/{job_id}"
)
def fleet_agent_provider_login_status(
    request: Request, instance_id: str, provider_id: str, job_id: str
):
    return _proxy_agent_providers(
        request, instance_id, "GET", f"/{provider_id}/login-jobs/{job_id}"
    )


@router.get(
    "/fleet/instances/{instance_id}/agent-providers/{provider_id}/login-jobs/{job_id}/events"
)
def fleet_agent_provider_login_events(
    request: Request, instance_id: str, provider_id: str, job_id: str, after: int = 0
):
    return _proxy_agent_providers(
        request,
        instance_id,
        "GET",
        f"/{provider_id}/login-jobs/{job_id}/events?after={after}",
    )


@router.post(
    "/fleet/instances/{instance_id}/agent-providers/{provider_id}/login-jobs/{job_id}/cancel"
)
def fleet_agent_provider_login_cancel(
    request: Request, instance_id: str, provider_id: str, job_id: str
):
    return _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/login-jobs/{job_id}/cancel"
    )


@ui_router.get("/fleet")
def fleet_page(request: Request):
    from pa.modules.ui_shell import render_page

    page = request.app.state.ctx.require_service("pages").get_by_path("/fleet")
    if not page:
        raise HTTPException(status_code=404)
    return render_page(request, page)


class FleetModule(Module):
    @property
    def name(self) -> str:
        return "fleet"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Fleet management, realms, and membership"

    def on_load(self, ctx: AppContext) -> None:
        settings = ctx.settings
        from pa.sync.infrastructure import get_membership_store, get_peer_table

        fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
        self_url = owner_public_url(settings)
        fleet.register_self(
            settings.instance_id,
            settings.instance_name,
            self_url,
            zone=settings.zone,
            capabilities=settings.capabilities,
            relay_enabled=settings.relay_enabled,
        )
        ctx.register_service("fleet_registry", fleet)
        membership = get_membership_store(settings)
        for realm in settings.subscribed_realms:
            membership.ensure_realm(realm)
            membership.ensure_owner_membership(
                realm, "local", fleet_id=settings.fleet_id
            )
        ctx.register_service("membership", membership)
        peer_table = get_peer_table(settings)
        for realm in settings.subscribed_realms:
            peer_table.sync_from_settings_peers(realm, settings.peers, settings.zone)
        ctx.register_service("peer_table", peer_table)
        ctx.register_service("fleet_job_store", get_job_store(settings))
        ctx.register_service(
            "fleet_update_job_store", FleetUpdateJobStore(settings.data_dir)
        )
        ctx.register_service("dispatch_store", DispatchStore(settings.data_dir))

        pages: PageRegistry = ctx.require_service("pages")
        pages.register(
            PageDefinition(
                id="fleet",
                path="/fleet",
                label="Fleet",
                icon="fleet",
                template="pages/fleet.html",
                nav_order=50,
                context_builder=_fleet_context,
            )
        )

    async def on_startup(self, app, ctx: AppContext) -> None:
        recover_update_jobs(
            ctx.settings,
            ctx.require_service("fleet_registry"),
            ctx.require_service("fleet_update_job_store"),
        )
        outbox = CompletionOutbox(
            ctx.require_service("dispatch_store"), ctx.settings.sync_token
        )
        outbox.start()
        ctx.register_service("completion_outbox", outbox)
        agent = ctx.require_service("instance_agent")
        agent.completion_handler = outbox.queue

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        outbox = ctx.services.get("completion_outbox")
        if outbox:
            await outbox.close(timeout=5.0)

    def api_routers(self):
        return [("/api", router, ["fleet"])]

    def ui_routers(self):
        return [ui_router]
