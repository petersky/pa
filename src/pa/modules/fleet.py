"""Fleet management, realms, membership, and remote install APIs."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import re
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, AsyncIterator, Literal
from urllib.parse import quote
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from pa.acp.configuration import SessionConfigurationRequest
from pa.attachments import (
    CHUNK_BYTES,
    AttachmentError,
    AttachmentStore,
    manifest_digest,
)
from pa.auth.middleware import get_principal_id, require_user
from pa.core.async_runtime import AsyncRuntime
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.io import atomic_write_json
from pa.core.logging import redact_log_text
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import (
    CardAttachment,
    CardCreate,
    CardEvent,
    CardKind,
    CardLane,
    CardUpdate,
    EventType,
    FleetInstance,
    KnowledgeEntry,
    RealmRole,
)
from pa.execution.dispatch import (
    CapacityAdmission,
    CompletionOutbox,
    ConcurrentCardDispatch,
    DispatchCapacityExhausted,
    DispatchIdempotencyConflict,
    DispatchRecord,
    DispatchStore,
    DispatchWorker,
)
from pa.execution.disposition import decide_card_disposition
from pa.execution.profiles import (
    ExecutionContract,
    MaterializationPlan,
    resolve_materialization_plan,
)
from pa.execution.post_turn import (
    EvidenceReferenceV1,
    FollowupActionName,
    FollowupActionStatus,
    PostTurnEvaluationV1,
    PostTurnEvaluator,
    TurnEndSnapshotV1,
    action_catalog,
    mark_record_only_actions,
)
from pa.execution.progress import (
    PROGRESS_SCHEMA_VERSION,
    SUPPORTED_PROGRESS_VERSIONS,
    CompletionReportV1,
    DispatchProgressEventV1,
    DispatchProgressHeartbeatV1,
    ExplicitProgressCheckpointV1,
    ProgressKind,
    ProgressService,
    sanitize_completion_report,
    sanitize_text,
)
from pa.execution.reconciliation import CompletionReconciler
from pa.fleet.control_plane import build_control_plane_status
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
from pa.fleet.overview import DIMENSIONS, build_overview, cache_for, probe_dimension
from pa.fleet.placement import (
    PlacementCandidate,
    PlacementError,
    PlacementPolicy,
    PlacementRequest,
    PlacementService,
    RoundRobinCursorStore,
)
from pa.fleet.policy import (
    BUILTIN_GROUPS,
    WORKLOAD_PROFILES,
    DispatchIntent,
    FleetPolicyService,
    GroupLifecycle,
    InstanceGroupCreate,
    InstanceGroupUpdate,
    InstanceParticipationPolicy,
    InstanceParticipationPolicyUpdate,
    ParticipationMode,
    PlacementDefault,
)
from pa.fleet.registry import FleetRegistry, reconcile_snapshots, semantic_snapshot
from pa.fleet.remote_install import (
    RemoteInstallRequest,
    get_job_store,
    run_install_job,
)
from pa.fleet.update import (
    TERMINAL_PHASES,
    FleetUpdateJobStore,
    FleetUpdateRequest,
    prepare_update_job_recovery,
    start_update_job,
)
from pa.fleet.workshop import build_workshop_snapshot
from pa.network.peer_table import PeerTable

logger = logging.getLogger(__name__)

FLEET_HEALTH_TIMEOUT = 3.0
FLEET_DETAIL_TIMEOUT = 5.0
FLEET_AGGREGATE_TIMEOUT = 9.0
SESSION_ROUTE_TIMEOUT = 3.0

router = APIRouter()
ui_router = APIRouter()
_peer_update_task: asyncio.Task[Any] | None = None
_peer_update_task_operation_id: str | None = None


@router.get("/fleet/control-plane/status")
def control_plane_status(request: Request) -> dict[str, Any]:
    """Expose honest compatibility state without treating static URLs as election."""
    service = request.app.state.ctx.services.get("pr_supervisor")
    health = service.authority_health() if service is not None else None
    return build_control_plane_status(
        request.app.state.ctx.settings,
        pr_supervisor_health=health,
    )


async def _offload_ctx(
    ctx: AppContext,
    operation: str,
    call,
    *args,
    timeout: float | None = None,
    **kwargs,
):
    runtime = ctx.services.get("async_runtime")
    if isinstance(runtime, AsyncRuntime):
        return await runtime.run_blocking(
            operation, call, *args, timeout=timeout, **kwargs
        )
    return await asyncio.to_thread(call, *args, **kwargs)


async def _offload_request(
    request: Request, operation: str, call, *args, timeout=None, **kwargs
):
    return await _offload_ctx(
        request.app.state.ctx,
        operation,
        call,
        *args,
        timeout=timeout,
        **kwargs,
    )


async def _response_json(request: Request, response: httpx.Response) -> Any:
    runtime = request.app.state.ctx.services.get("async_runtime")
    if isinstance(runtime, AsyncRuntime):
        return await runtime.run_blocking(
            "fleet.response_json", response.json, timeout=3.0
        )
    return response.json()


async def _fleet_http(request: Request, operation: str, awaitable, *, timeout: float):
    runtime = request.app.state.ctx.services.get("async_runtime")
    if isinstance(runtime, AsyncRuntime):
        return await runtime.observe(operation, awaitable, timeout=timeout)
    async with asyncio.timeout(timeout):
        return await awaitable


@asynccontextmanager
async def _borrow_fleet_client(request: Request, *, timeout: float):
    services = getattr(request.app.state.ctx, "services", None)
    client = services.get("fleet_http_client") if isinstance(services, dict) else None
    if client is not None:
        yield client
        return
    async with httpx.AsyncClient(timeout=timeout) as owned:
        yield owned


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

    authority_instance_id: str | None = None
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
    idempotency_key: str | None = None
    resume_session_id: str | None = None
    allow_concurrent: bool = False
    capacity_override: bool = False
    capacity_override_reason: str | None = Field(default=None, max_length=500)
    participation_override: bool = False
    participation_override_reason: str | None = Field(default=None, max_length=500)
    execution_contract: dict[str, Any] | None = None


class FleetDispatchBody(RemoteAgentStartBody):
    """Authority-side dispatch target or placement policy."""

    target_instance_id: str | None = None
    placement_policy: PlacementPolicy | None = None
    group_id: str | None = None
    required_capabilities: list[str] = Field(default_factory=list)


class PlacementDefaultBody(BaseModel):
    realm_id: str | None = None
    project_id: str | None = None
    workload_profile: str | None = None
    group_id: str


class PlacementMigrationBody(BaseModel):
    realm_id: str | None = None
    apply: bool = False


class DispatchControlBody(BaseModel):
    """Idempotency context for a durable dispatch lifecycle mutation."""

    idempotency_key: str | None = None


class DispatchFollowupBody(BaseModel):
    """Idempotent prompt for the session durably linked to a dispatch."""

    message: str = Field(min_length=1, max_length=200_000)
    action: Literal["append", "prepend", "interrupt"] = "append"
    idempotency_key: str


class FollowupActionExecutionBody(BaseModel):
    evaluation_id: str
    action_id: str
    expected_authority_version: str | None = None
    approve: bool = False
    idempotency_key: str


class DispatchMaterializeBody(BaseModel):
    dispatch_id: str
    mutation_id: str
    card: dict[str, Any] | None = None
    card_version: str | None = None
    realm_id: str
    project_id: str | None = None
    principal_id: str = "user:local"
    provenance_version: int = 1
    authority_instance_id: str
    authority_instance_name: str | None = None
    authority_url: str
    target_instance_id: str
    session_id: str | None = None
    progress_versions: list[int] = Field(default_factory=list, max_length=10)
    attachment_manifest: list[CardAttachment] = Field(default_factory=list)
    attachment_digest: str | None = None
    materialization_plan: dict[str, Any] | None = None


class AttachmentFinalizeBody(BaseModel):
    realm_id: str
    card_id: str
    size: int


class DispatchCompletionBody(BaseModel):
    mutation_id: str
    card_id: str | None = None
    realm_id: str
    card_version: str | None = None
    source_instance_id: str
    session_id: str | None = None
    result: dict[str, Any] = Field(default_factory=dict)
    disposition: Any = None
    final_report: CompletionReportV1 | None = None


class DispatchTurnEndBody(BaseModel):
    mutation_id: str
    source_instance_id: str
    session_id: str
    turn_id: str
    result: dict[str, Any] = Field(default_factory=dict)
    final_report: CompletionReportV1 | None = None


def _canonical_dispatch_uuid(value: str | None, field: str) -> str:
    """Reject display/storage slugs at the durable dispatch boundary."""
    try:
        parsed = str(UUID(value or ""))
    except (ValueError, AttributeError) as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "malformed_provenance_id",
                "field": field,
                "value": value,
                "message": f"{field} must be a full canonical UUID",
            },
        ) from exc
    if parsed != value:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "noncanonical_provenance_id",
                "field": field,
                "value": value,
                "message": f"{field} must use canonical lowercase UUID form",
            },
        )
    return parsed


def _dispatch_store(request: Request) -> DispatchStore:
    service = request.app.state.ctx.services.get("dispatch_store")
    if isinstance(service, DispatchStore):
        return service
    service = DispatchStore(request.app.state.ctx.settings.data_dir)
    request.app.state.ctx.register_service("dispatch_store", service)
    return service


def _policy_service(request: Request) -> FleetPolicyService:
    service = request.app.state.ctx.services.get("fleet_policy")
    if isinstance(service, FleetPolicyService):
        return service
    service = FleetPolicyService(request.app.state.ctx.store)
    request.app.state.ctx.register_service("fleet_policy", service)
    return service


def _require_policy_admin(request: Request, permission: str):
    user = require_user(request)
    if getattr(user, "role", None) != "admin":
        raise HTTPException(
            status_code=403,
            detail={
                "code": "fleet_policy_permission_denied",
                "permission": permission,
                "message": f"Administrator permission {permission!r} is required.",
            },
        )
    return user


def _group_public(
    request: Request, group, *, include_membership: bool = False
) -> dict[str, Any]:
    payload = group.model_dump(mode="json")
    if not include_membership:
        return payload
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    instances = {item.instance_id: item for item in fleet.list_instances()}
    payload["included_instances"] = [
        {
            "instance_id": instance_id,
            "name": instances[instance_id].name if instance_id in instances else None,
            "present": instance_id in instances,
        }
        for instance_id in group.included_instance_ids
    ]
    payload["excluded_instances"] = [
        {
            "instance_id": instance_id,
            "name": instances[instance_id].name if instance_id in instances else None,
            "present": instance_id in instances,
        }
        for instance_id in group.excluded_instance_ids
    ]
    return payload


@router.get("/fleet/instance-groups")
def list_instance_groups(
    request: Request,
    realm: str | None = None,
    include_archived: bool = False,
) -> list[dict[str, Any]]:
    require_user(request)
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    return [
        _group_public(request, group, include_membership=True)
        for group in _policy_service(request).list_groups(
            realm_id, include_archived=include_archived
        )
    ]


@router.post("/fleet/instance-groups", status_code=201)
def create_instance_group(
    request: Request, body: InstanceGroupCreate
) -> dict[str, Any]:
    _require_policy_admin(request, "fleet.groups.edit")
    settings = request.app.state.ctx.settings
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    _policy_service(request).migrate(
        realm_id=body.realm_id,
        instances=list(fleet.list_instances()),
        actor=get_principal_id(request),
        author_instance=settings.instance_id,
        apply=True,
    )
    try:
        group = request.app.state.ctx.store.create_instance_group(
            body,
            principal_id=get_principal_id(request),
            instance_id=settings.instance_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _group_public(request, group, include_membership=True)


def _custom_group_or_404(request: Request, group_id: str, realm_id: str):
    if group_id in BUILTIN_GROUPS:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "system_group_immutable",
                "message": "Built-in group semantics are immutable.",
            },
        )
    group = request.app.state.ctx.store.get_instance_group(group_id, realm_id)
    if not group:
        raise HTTPException(status_code=404, detail="Instance group not found")
    return group


@router.get("/fleet/instance-groups/{group_id}")
def get_instance_group(
    request: Request, group_id: str, realm: str | None = None
) -> dict[str, Any]:
    require_user(request)
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    group = _policy_service(request).get_group(realm_id, group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Instance group not found")
    return _group_public(request, group, include_membership=True)


@router.patch("/fleet/instance-groups/{group_id}")
def update_instance_group(
    request: Request,
    group_id: str,
    body: InstanceGroupUpdate,
    realm: str | None = None,
) -> dict[str, Any]:
    _require_policy_admin(request, "fleet.groups.edit")
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    _custom_group_or_404(request, group_id, realm_id)
    try:
        group = request.app.state.ctx.store.update_instance_group(
            group_id,
            body,
            realm_id=realm_id,
            principal_id=get_principal_id(request),
            instance_id=settings.instance_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _group_public(request, group, include_membership=True)


@router.post("/fleet/instance-groups/{group_id}/archive")
def archive_instance_group(
    request: Request, group_id: str, realm: str | None = None
) -> dict[str, Any]:
    return update_instance_group(
        request,
        group_id,
        InstanceGroupUpdate(lifecycle_state=GroupLifecycle.ARCHIVED),
        realm,
    )


@router.delete("/fleet/instance-groups/{group_id}", status_code=204)
def delete_instance_group(
    request: Request, group_id: str, realm: str | None = None
) -> Response:
    _require_policy_admin(request, "fleet.groups.delete")
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    _custom_group_or_404(request, group_id, realm_id)
    request.app.state.ctx.store.delete_instance_group(
        group_id,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    return Response(status_code=204)


def _change_group_membership(
    request: Request,
    group_id: str,
    instance_id: str,
    *,
    excluded: bool,
    remove: bool,
    realm_id: str,
) -> dict[str, Any]:
    _require_policy_admin(request, "fleet.groups.membership.edit")
    group = _custom_group_or_404(request, group_id, realm_id)
    included = set(group.included_instance_ids)
    exclusions = set(group.excluded_instance_ids)
    target = exclusions if excluded else included
    target.discard(instance_id) if remove else target.add(instance_id)
    body = InstanceGroupUpdate(
        included_instance_ids=sorted(included),
        excluded_instance_ids=sorted(exclusions),
        expected_version=group.version,
    )
    updated = request.app.state.ctx.store.update_instance_group(
        group_id,
        body,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    return _group_public(request, updated, include_membership=True)


@router.put("/fleet/instance-groups/{group_id}/members/{instance_id}")
def add_instance_group_member(
    request: Request, group_id: str, instance_id: str, realm: str | None = None
) -> dict[str, Any]:
    return _change_group_membership(
        request,
        group_id,
        instance_id,
        excluded=False,
        remove=False,
        realm_id=realm or request.app.state.ctx.settings.primary_realm,
    )


@router.delete("/fleet/instance-groups/{group_id}/members/{instance_id}")
def remove_instance_group_member(
    request: Request, group_id: str, instance_id: str, realm: str | None = None
) -> dict[str, Any]:
    return _change_group_membership(
        request,
        group_id,
        instance_id,
        excluded=False,
        remove=True,
        realm_id=realm or request.app.state.ctx.settings.primary_realm,
    )


@router.put("/fleet/instance-groups/{group_id}/exclusions/{instance_id}")
def add_instance_group_exclusion(
    request: Request, group_id: str, instance_id: str, realm: str | None = None
) -> dict[str, Any]:
    return _change_group_membership(
        request,
        group_id,
        instance_id,
        excluded=True,
        remove=False,
        realm_id=realm or request.app.state.ctx.settings.primary_realm,
    )


@router.delete("/fleet/instance-groups/{group_id}/exclusions/{instance_id}")
def remove_instance_group_exclusion(
    request: Request, group_id: str, instance_id: str, realm: str | None = None
) -> dict[str, Any]:
    return _change_group_membership(
        request,
        group_id,
        instance_id,
        excluded=True,
        remove=True,
        realm_id=realm or request.app.state.ctx.settings.primary_realm,
    )


@router.get("/fleet/instances/{instance_id}/participation-policy")
def get_instance_participation_policy(
    request: Request, instance_id: str, realm: str | None = None
) -> dict[str, Any]:
    require_user(request)
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    policy, explicit = _policy_service(request).effective_policy(
        realm_id, instance_id
    )
    return {
        **policy.model_dump(mode="json"),
        "explicit": explicit,
        "summary": policy.summary(),
    }


def _policy_change_enables(
    old: InstanceParticipationPolicy, new: InstanceParticipationPolicy
) -> bool:
    profiles = set(WORKLOAD_PROFILES)
    old_allowed = set(old.allowed_profiles) or profiles
    new_allowed = set(new.allowed_profiles) or profiles
    old_effective = old_allowed - set(old.denied_profiles) - set(
        old.hard_denied_profiles
    )
    new_effective = new_allowed - set(new.denied_profiles) - set(
        new.hard_denied_profiles
    )
    return bool(
        (new_effective - old_effective)
        or (not old.automatic_dispatch and new.automatic_dispatch)
        or (not old.manual_dispatch and new.manual_dispatch)
        or (old.maintenance and not new.maintenance)
        or (old.quiescing and not new.quiescing)
    )


@router.put("/fleet/instances/{instance_id}/participation-policy")
def update_instance_participation_policy(
    request: Request,
    instance_id: str,
    body: InstanceParticipationPolicyUpdate,
    realm: str | None = None,
) -> dict[str, Any]:
    _require_policy_admin(request, "fleet.instance_participation.edit")
    ctx = request.app.state.ctx
    realm_id = realm or ctx.settings.primary_realm
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    if not any(
        item.instance_id == instance_id for item in fleet.list_instances()
    ) and not ctx.store.get_instance_participation_policy(instance_id, realm_id):
        raise HTTPException(status_code=404, detail="Fleet instance not found")
    if not ctx.store.list_instance_participation_policies(realm_id):
        _policy_service(request).migrate(
            realm_id=realm_id,
            instances=list(fleet.list_instances()),
            actor=get_principal_id(request),
            author_instance=ctx.settings.instance_id,
            apply=True,
        )
    current, _explicit = _policy_service(request).effective_policy(
        realm_id, instance_id
    )
    if body.expected_version is not None and body.expected_version != current.version:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "participation_policy_version_conflict",
                "expected_version": body.expected_version,
                "actual_version": current.version,
            },
        )
    updates = body.model_dump(
        mode="python",
        exclude_unset=True,
        exclude={
            "expected_version",
            "confirm_enable",
            "confirmation_reason",
        },
    )
    if body.participation_mode is not None:
        if body.automatic_dispatch is None:
            updates["automatic_dispatch"] = (
                body.participation_mode == ParticipationMode.AUTOMATIC
            )
        if body.manual_dispatch is None:
            updates["manual_dispatch"] = (
                body.participation_mode != ParticipationMode.DISABLED
            )
    updated = current.model_copy(deep=True)
    for key, value in updates.items():
        if value is not None:
            setattr(updated, key, value)
    # A fleet authority can add a self-protective limit but cannot remove one
    # already advertised and synchronized by the instance.
    updated.hard_denied_profiles = sorted(
        set(updated.hard_denied_profiles) | set(current.hard_denied_profiles)
    )
    updated.source = "operator"
    for profile, limit in current.hard_max_concurrent_by_profile.items():
        proposed = updated.hard_max_concurrent_by_profile.get(profile)
        updated.hard_max_concurrent_by_profile[profile] = (
            limit if proposed is None else min(limit, proposed)
        )
    updated = InstanceParticipationPolicy.model_validate(
        updated.model_dump(mode="python")
    )
    enabling = _policy_change_enables(current, updated)
    if enabling and (
        not body.confirm_enable or not (body.confirmation_reason or "").strip()
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "participation_enable_confirmation_required",
                "message": "Enabling previously denied work requires explicit confirmation and an audit reason.",
            },
        )
    if enabling:
        updated.enablement_confirmation_reason = body.confirmation_reason.strip()
        updated.reason = (
            updated.reason
            or f"Enabled with confirmation: {body.confirmation_reason.strip()}"
        )
    saved = ctx.store.set_instance_participation_policy(
        updated,
        principal_id=get_principal_id(request),
        instance_id=ctx.settings.instance_id,
    )
    return {
        **saved.model_dump(mode="json"),
        "explicit": True,
        "summary": saved.summary(),
    }


@router.get("/fleet/placement-defaults")
def list_placement_defaults(
    request: Request, realm: str | None = None
) -> list[dict[str, Any]]:
    require_user(request)
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    return [
        item.model_dump(mode="json") | {"scope_key": item.scope_key}
        for item in request.app.state.ctx.store.list_placement_defaults(realm_id)
    ]


@router.put("/fleet/placement-defaults")
def set_placement_default(
    request: Request, body: PlacementDefaultBody
) -> dict[str, Any]:
    _require_policy_admin(request, "fleet.placement_defaults.edit")
    ctx = request.app.state.ctx
    realm_id = body.realm_id or ctx.settings.primary_realm
    group = _policy_service(request).get_group(realm_id, body.group_id)
    if not group or group.lifecycle_state != GroupLifecycle.ACTIVE:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "configured_group_unavailable",
                "message": "A default can reference only an active group.",
            },
        )
    if body.project_id and not ctx.store.get_project(body.project_id, realm_id):
        raise HTTPException(status_code=404, detail="Project not found")
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    _policy_service(request).migrate(
        realm_id=realm_id,
        instances=list(fleet.list_instances()),
        actor=get_principal_id(request),
        author_instance=ctx.settings.instance_id,
        apply=True,
    )
    saved = ctx.store.set_placement_default(
        PlacementDefault(
            realm_id=realm_id,
            project_id=body.project_id,
            workload_profile=body.workload_profile,
            group_id=body.group_id,
        ),
        principal_id=get_principal_id(request),
        instance_id=ctx.settings.instance_id,
    )
    return saved.model_dump(mode="json") | {"scope_key": saved.scope_key}


@router.delete("/fleet/placement-defaults", status_code=204)
def delete_placement_default(
    request: Request,
    realm: str | None = None,
    project_id: str | None = None,
    workload_profile: str | None = None,
) -> Response:
    _require_policy_admin(request, "fleet.placement_defaults.edit")
    ctx = request.app.state.ctx
    ctx.store.delete_placement_default(
        realm_id=realm or ctx.settings.primary_realm,
        project_id=project_id,
        workload_profile=workload_profile,
        principal_id=get_principal_id(request),
        instance_id=ctx.settings.instance_id,
    )
    return Response(status_code=204)


@router.post("/fleet/participation-migration")
def migrate_instance_participation(
    request: Request, body: PlacementMigrationBody
) -> dict[str, Any]:
    _require_policy_admin(request, "fleet.instance_participation.migrate")
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    return _policy_service(request).migrate(
        realm_id=body.realm_id or ctx.settings.primary_realm,
        instances=list(fleet.list_instances()),
        actor=get_principal_id(request),
        author_instance=ctx.settings.instance_id,
        apply=body.apply,
    )


@router.get("/fleet/policy-audit")
def fleet_policy_audit(
    request: Request,
    realm: str | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    require_user(request)
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    events = [
        item.model_dump(mode="json")
        for item in request.app.state.ctx.store.list_fleet_policy_audit(
            realm_id,
            entity_type=entity_type,
            entity_id=entity_id,
            limit=min(max(limit, 1), 1000),
        )
    ]
    if entity_type in {None, "placement_decision"}:
        events.extend(
            {
                "id": record.dispatch_id,
                "realm_id": record.realm_id,
                "entity_type": "placement_decision",
                "entity_id": record.dispatch_id,
                "action": "placement_resolved",
                "actor": record.principal_id,
                "payload": record.placement_decision or {},
                "created_at": (
                    record.placement_resolved_at or record.created_at
                ).isoformat(),
            }
            for record in _dispatch_store(request).list(limit=limit)
            if record.realm_id == realm_id
        )
    return sorted(events, key=lambda item: item["created_at"], reverse=True)[:limit]


@router.post("/fleet/dispatch/materialize")
def materialize_dispatch(request: Request, body: DispatchMaterializeBody) -> dict:
    """Make an exact authoritative card version resolvable before session creation."""
    _require_instance(request)
    settings = request.app.state.ctx.settings
    if body.provenance_version != 1:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "unsupported_provenance_version",
                "provenance_version": body.provenance_version,
                "message": "Remote dispatch requires canonical provenance version 1",
            },
        )
    if body.provenance_version == 1:
        identifiers = {
            "dispatch_id": body.dispatch_id,
            "mutation_id": body.mutation_id,
            "authority_instance_id": body.authority_instance_id,
            "target_instance_id": body.target_instance_id,
        }
        card_id = str((body.card or {}).get("id") or "") or None
        if card_id:
            identifiers["card_id"] = card_id
        if body.project_id:
            identifiers["project_id"] = body.project_id
        if body.session_id:
            identifiers["session_id"] = body.session_id
        for field, value in identifiers.items():
            _canonical_dispatch_uuid(value, field)
        caller = request.headers.get("X-PA-Origin-Instance-ID", "").strip()
        _canonical_dispatch_uuid(caller, "authenticated_origin_instance_id")
        if caller != body.authority_instance_id:
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "dispatch_authority_mismatch",
                    "authenticated_origin_instance_id": caller,
                    "authority_instance_id": body.authority_instance_id,
                    "recoverable": False,
                },
            )
        if body.card and body.card.get("project_id") != body.project_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "dispatch_card_project_mismatch",
                    "card_project_id": body.card.get("project_id"),
                    "project_id": body.project_id,
                    "recoverable": False,
                },
            )
    if body.target_instance_id != settings.instance_id:
        raise HTTPException(
            status_code=409,
            detail={"code": "wrong_target", "expected": settings.instance_id},
        )
    if body.materialization_plan is not None:
        try:
            bound_plan = MaterializationPlan.model_validate(body.materialization_plan)
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail={"code": "invalid_materialization_plan", "message": str(exc)},
            ) from exc
        if bound_plan.target_instance_id != settings.instance_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "stale_materialization_plan",
                    "message": "The materialization target changed after preflight.",
                    "recoverable": True,
                },
            )
        if bound_plan.profile == "repository":
            unavailable = [
                item["repository_id"]
                for item in bound_plan.repositories
                if request.app.state.ctx.store.get_repository(
                    item["repository_id"], body.realm_id
                )
                is None
            ]
            if unavailable:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "repository_context_required",
                        "repositories": unavailable,
                        "message": "Required repositories are unavailable on the target; no session was started.",
                        "recoverable": True,
                    },
                )
    if body.attachment_digest is not None and body.attachment_digest != manifest_digest(
        body.attachment_manifest
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "attachment_manifest_mismatch", "recoverable": False},
        )
    for item in body.attachment_manifest:
        if item.realm_id != body.realm_id or item.card_id != str(
            (body.card or {}).get("id") or ""
        ):
            raise HTTPException(
                status_code=403,
                detail={"code": "attachment_scope_mismatch", "recoverable": False},
            )
    attachment_store = AttachmentStore(request.app.state.ctx.settings.data_dir)
    attachment_store.authorize_transfer(
        body.dispatch_id,
        body.realm_id,
        str((body.card or {}).get("id") or ""),
        body.attachment_manifest,
    )
    missing = [
        {
            "sha256": item.sha256,
            "size": item.size,
            "offset": attachment_store.partial_size(body.dispatch_id, item.sha256),
        }
        for item in body.attachment_manifest
        if not attachment_store.has_verified_blob(item.sha256, item.size)
    ]
    if missing:
        return {
            "dispatch_id": body.dispatch_id,
            "resolvable": False,
            "missing": missing,
            "attachment_digest": body.attachment_digest,
        }
    try:
        attachment_evidence = attachment_store.materialize(
            body.dispatch_id, body.attachment_manifest
        )
    except AttachmentError as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc

    ledger = _dispatch_store(request)
    progress_protocol_version = next(
        (
            version
            for version in sorted(body.progress_versions, reverse=True)
            if version in SUPPORTED_PROGRESS_VERSIONS
        ),
        None,
    )
    recorded = ledger.get(body.dispatch_id)
    if recorded:
        if recorded.mutation_id != body.mutation_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "idempotency_conflict",
                    "message": "dispatch id is already in use",
                },
            )
        return {
            "dispatch_id": body.dispatch_id,
            "card_id": recorded.card_id,
            "card_version": recorded.card_version,
            "resolvable": True,
            "duplicate": True,
            "session_id": recorded.session_id,
            "progress_protocol_version": recorded.progress_protocol_version,
            "attachment_evidence": recorded.attachment_evidence,
            "materialization_plan": recorded.materialization_plan,
        }

    store = request.app.state.ctx.store
    incoming = body.card
    card_id = str((incoming or {}).get("id") or "") or None
    if incoming and not card_id:
        raise HTTPException(status_code=400, detail="card.id required")
    existing = store.get_card(card_id, realm_id=body.realm_id) if card_id else None
    if existing and existing.updated_at.isoformat() != body.card_version:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "target_sync_conflict",
                "card_id": card_id,
                "target_version": existing.updated_at.isoformat(),
                "authority_version": body.card_version,
                "recoverable": True,
            },
        )
    if incoming and not existing:
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
        project_id=body.project_id,
        principal_id=body.principal_id,
        realm_id=body.realm_id,
        card_version=body.card_version,
        authority_instance_id=body.authority_instance_id,
        authority_instance_name=body.authority_instance_name,
        authority_url=body.authority_url,
        target_instance_id=body.target_instance_id,
        session_id=body.session_id,
        resume_requested=bool(body.session_id),
        resume_session_id=body.session_id,
        state="materializing",
        progress_protocol_version=progress_protocol_version,
        attachment_evidence=attachment_evidence,
        materialization_plan=body.materialization_plan,
        request_payload={
            "provenance_version": body.provenance_version,
            "progress_versions": list(body.progress_versions),
        },
    )
    try:
        ledger.put(record)
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
        "duplicate": False,
        "progress_protocol_version": progress_protocol_version,
        "attachment_evidence": attachment_evidence,
    }


@router.get(
    "/fleet/attachments/{card_id}/{attachment_id}",
    response_class=FileResponse,
    response_model=None,
)
def fetch_fleet_attachment(
    request: Request, card_id: str, attachment_id: str, realm_id: str
) -> FileResponse:
    _require_instance(request)
    caller = request.headers.get("X-PA-Origin-Instance-ID", "").strip()
    _fleet_instance_or_404(request, caller)
    card = request.app.state.ctx.store.get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail={"code": "card_not_found"})
    attachment = next(
        (item for item in card.attachments if item.attachment_id == attachment_id), None
    )
    if (
        not attachment
        or attachment.realm_id != realm_id
        or attachment.card_id != card_id
    ):
        raise HTTPException(status_code=404, detail={"code": "attachment_not_found"})
    blobs = AttachmentStore(request.app.state.ctx.settings.data_dir)
    if not blobs.has_verified_blob(attachment.sha256, attachment.size):
        raise HTTPException(
            status_code=409,
            detail={"code": "attachment_blob_missing", "recoverable": True},
        )
    return FileResponse(
        blobs.blob_path(attachment.sha256),
        media_type="application/octet-stream",
        headers={
            "X-PA-Attachment-SHA256": attachment.sha256,
            "X-PA-Attachment-Size": str(attachment.size),
            "Content-Disposition": "attachment",
            "X-Content-Type-Options": "nosniff",
            "Accept-Ranges": "bytes",
        },
    )


@router.put("/fleet/dispatch/{dispatch_id}/attachments/{sha256}")
async def transfer_dispatch_attachment(
    request: Request,
    dispatch_id: str,
    sha256: str,
    realm_id: str,
    card_id: str,
    size: int,
    offset: int,
) -> dict:
    _require_instance(request)
    blobs = AttachmentStore(request.app.state.ctx.settings.data_dir)
    if not blobs.authorized_attachment(dispatch_id, realm_id, card_id, sha256, size):
        raise HTTPException(
            status_code=403, detail={"code": "attachment_transfer_unauthorized"}
        )
    data = await request.body()
    if len(data) > CHUNK_BYTES:
        raise HTTPException(status_code=413, detail={"code": "chunk_too_large"})
    try:
        received = blobs.append_chunk(
            dispatch_id, sha256, offset=offset, data=data, total_size=size
        )
    except AttachmentError as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    return {"received": received, "complete": received == size}


@router.post("/fleet/dispatch/{dispatch_id}/attachments/{sha256}/finalize")
def finalize_dispatch_attachment(
    request: Request, dispatch_id: str, sha256: str, body: AttachmentFinalizeBody
) -> dict:
    _require_instance(request)
    blobs = AttachmentStore(request.app.state.ctx.settings.data_dir)
    if not blobs.authorized_attachment(
        dispatch_id, body.realm_id, body.card_id, sha256, body.size
    ):
        raise HTTPException(
            status_code=403, detail={"code": "attachment_transfer_unauthorized"}
        )
    try:
        blobs.finalize_partial(dispatch_id, sha256, body.size)
    except AttachmentError as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    return {"verified": True, "sha256": sha256, "size": body.size}


def _model_json(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return dict(value) if isinstance(value, dict) else {}


def _record_post_turn_evaluation(
    request: Request,
    ledger: DispatchStore,
    record: DispatchRecord,
    *,
    card: Any,
    watches: list[Any],
    result_override: dict[str, Any] | None = None,
    turn_id_override: str | None = None,
) -> None:
    """Persist the neutral snapshot before running the read-only evaluator."""
    result = dict(
        result_override if result_override is not None else record.completion_payload or {}
    )
    report = record.final_report
    latest = record.latest_progress
    turn_id = str(
        turn_id_override
        or result.get("queued_prompt_id")
        or result.get("prompt_id")
        or f"{record.dispatch_id}:turn:{len(record.turn_end_snapshots) + 1}"
    )
    if any(item.turn_id == turn_id for item in record.turn_end_snapshots):
        return
    evidence: list[EvidenceReferenceV1] = []
    if report:
        for kind, reference in (
            ("branch", report.branch),
            ("commit", report.commit_sha),
            ("pull_request", report.pr_url),
            ("merge_commit", report.merge_commit_sha),
        ):
            if reference:
                evidence.append(
                    EvidenceReferenceV1(
                        kind=kind,
                        reference=str(reference),
                        observed_at=report.created_at,
                        provenance="sanitized completion report",
                    )
                )
        evidence.extend(
            EvidenceReferenceV1(
                kind="ci",
                reference=item,
                observed_at=report.created_at,
                provenance="linked PR supervisor snapshot",
            )
            for item in report.ci_evidence
        )
        evidence.extend(
            EvidenceReferenceV1(
                kind="review",
                reference=item,
                observed_at=report.created_at,
                provenance="linked PR supervisor snapshot",
            )
            for item in report.review_evidence
        )
    failures = []
    if record.last_error:
        failures.append(
            {
                "kind": record.error_code or "dispatch_error",
                "message": record.last_error,
                "recoverable": record.recoverable,
            }
        )
    if record.reconciliation_last_dependency_error:
        failures.append(
            {
                "kind": "reconciliation_dependency",
                "message": record.reconciliation_last_dependency_error,
                "recoverable": record.reconciliation_recoverable,
            }
        )
    operator_requests = (
        [latest.operator_input] if latest and latest.operator_input else []
    )
    current_card = _model_json(card)
    current_lane = str(current_card.get("lane") or "") or None
    authority_version = (
        str(current_card.get("updated_at") or "") or record.card_version
    )
    deliverables = report.model_dump(mode="json") if report else {}
    if report:
        deliverables["changed_files"] = (
            latest.changed_file_count if latest else None
        )
    snapshot = TurnEndSnapshotV1(
        turn_id=turn_id,
        turn_sequence=len(record.turn_end_snapshots) + 1,
        dispatch_id=record.dispatch_id,
        session_id=record.session_id,
        card_id=record.card_id or "",
        project_id=record.project_id,
        authority_instance_id=record.authority_instance_id,
        authority_version=authority_version,
        originating_instance_id=record.target_instance_id,
        stop_reason=result.get("stop_reason"),
        provider_status=result.get("provider_status"),
        session_status=result.get("session_status") or "idle",
        card_lane_before=(
            current_lane if result_override is not None else record.card_lane_before
        ),
        card_lane_after=(
            current_lane if result_override is not None else record.card_lane_after
        ),
        dispatch_state=record.state,
        completion_delivery={
            "classification": record.completion_delivery_class,
            "received_at": (
                record.completion_received_at.isoformat()
                if record.completion_received_at
                else None
            ),
            "acknowledged_at": (
                record.acknowledged_at.isoformat()
                if record.acknowledged_at
                else None
            ),
            "attempts": record.attempts,
        },
        disposition=record.card_disposition_payload,
        disposition_status=record.card_disposition_status,
        disposition_parse_error=result.get("card_disposition_error"),
        final_outcome_text=(
            str(result.get("final_outcome_text") or "")
            or (report.outcome if report else "Agent turn ended.")
        ),
        deliverables=deliverables,
        validations=[
            item.model_dump(mode="json") for item in (report.validations if report else [])
        ],
        blockers=list(report.blockers if report else []),
        failures=failures,
        operator_input_requests=operator_requests,
        queued_prompts=[
            {
                "idempotency_key": key,
                "prompt_id": value.get("response", {}).get("prompt_id"),
                "accepted": value.get("response", {}).get("accepted"),
            }
            for key, value in list(record.followup_operations.items())[-40:]
        ],
        followup_state={
            "turns": record.followup_turns[-20:],
            "automatic_turn_budget": (
                request.app.state.ctx.settings.post_turn_max_automatic_followups
            ),
        },
        evidence=evidence,
        provenance={
            "authority_instance_id": record.authority_instance_id,
            "originating_instance_id": record.target_instance_id,
            "dispatch_id": record.dispatch_id,
            "session_id": record.session_id,
            "provider": record.request_payload.get("provider"),
            "model": record.request_payload.get("model_id"),
            "mode": record.request_payload.get("mode_id"),
            "captured_by": "pa.authority",
        },
    )
    record.turn_end_snapshots.append(snapshot)
    record.turn_end_snapshots = record.turn_end_snapshots[-20:]
    # The snapshot is the durable evaluation boundary. Do not combine this write
    # with the later recommendation write.
    ledger.put(record)

    evaluator = PostTurnEvaluator()
    project = (
        request.app.state.ctx.store.get_project(
            record.project_id, realm_id=record.realm_id
        )
        if record.project_id
        else None
    )
    card_context = current_card or {
        "id": record.card_id,
        "lane": record.card_lane_after or record.card_lane_before,
        "updated_at": authority_version,
    }
    if record.card_lane_after:
        card_context["lane"] = record.card_lane_after
    context = evaluator.build_context(
        snapshot,
        card=card_context,
        project=_model_json(project) or None,
        execution_contract=record.request_payload.get("execution_contract"),
        dispatch_history=[event.model_dump(mode="json") for event in record.events],
        prior_evaluations=[
            item.model_dump(mode="json") for item in record.post_turn_evaluations
        ],
        watches=[_model_json(watch) for watch in watches],
        fleet_capabilities=list(request.app.state.ctx.settings.capabilities),
    )
    record.post_turn_context_digests[snapshot.snapshot_id] = context.digest
    record.post_turn_context_digests = dict(
        list(record.post_turn_context_digests.items())[-20:]
    )
    ledger.put(record)
    evaluation = evaluator.evaluate(context)
    evaluation = evaluator.validate_result(
        evaluation,
        expected_context_digest=context.digest,
        expected_authority_version=authority_version,
    )
    mark_record_only_actions(evaluation)
    record.post_turn_evaluations.append(evaluation)
    record.post_turn_evaluations = record.post_turn_evaluations[-20:]
    ledger.put(record)


@router.post("/fleet/dispatch/{dispatch_id}/complete")
def complete_dispatch(
    request: Request, dispatch_id: str, body: DispatchCompletionBody
) -> dict:
    """Acknowledge immutable completion, then reconcile mutable card intent."""
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
    if (
        body.source_instance_id != record.target_instance_id
        or body.card_id != record.card_id
        or body.card_version != record.card_version
        or body.realm_id != record.realm_id
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "completion_dispatch_mismatch", "recoverable": False},
        )
    if record.session_id and body.session_id != record.session_id:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "completion_session_mismatch",
                "expected": record.session_id,
                "actual": body.session_id,
                "recoverable": False,
            },
        )

    envelope = body.model_dump(mode="json")
    if record.acknowledged_at:
        if record.completion_envelope and record.completion_envelope != envelope:
            raise HTTPException(
                status_code=409,
                detail={"code": "completion_payload_conflict", "recoverable": False},
            )
        return _completion_ack(record, duplicate=True)

    # This write is intentionally before every mutable card read or update.
    record.session_id = body.session_id
    record.completion_payload = body.result
    record.completion_envelope = envelope
    record.completion_received_at = datetime.now(UTC)
    record.acknowledged_at = record.completion_received_at
    record.completion_delivery_class = "acknowledged"
    record.card_disposition_payload = (
        body.disposition if isinstance(body.disposition, dict) else None
    )
    record.reconciliation_state = "pending" if body.card_id else "not_applicable"
    record.reconciliation_reason = "Immutable agent-turn completion acknowledged."
    record.reconciliation_updated_at = record.completion_received_at
    ledger.transition(
        record,
        "completed",
        "Agent turn ended and was durably acknowledged; card outcome is separate.",
        detail={
            "agent_turn_ended": True,
            "reconciliation": record.reconciliation_state,
        },
    )

    card = (
        request.app.state.ctx.store.get_card(body.card_id, realm_id=body.realm_id)
        if body.card_id
        else None
    )
    if body.card_id and not card:
        record.card_disposition_status = "not_applicable"
        record.card_disposition_reason = (
            "The authoritative card was deleted or is unavailable."
        )
        record.reconciliation_state = "not_applicable"
        record.reconciliation_condition = "card_missing"
        record.reconciliation_recoverable = False
        record.reconciliation_updated_at = datetime.now(UTC)
        record.final_report = body.final_report or ledger.build_final_report(
            dispatch_id, body.result
        )
        ledger.put(record)
        _record_post_turn_evaluation(
            request, ledger, record, card=None, watches=[]
        )
        return _completion_ack(record, duplicate=False)

    watches = []
    supervisor_store = request.app.state.ctx.services.get("pr_supervisor_store")
    if card and supervisor_store:
        watches = supervisor_store.list_watches(
            realm_id=body.realm_id, card_id=card.id, include_retired=True
        )
    decision = (
        decide_card_disposition(
            body.disposition, current_lane=card.lane, watches=watches
        )
        if card
        else None
    )
    requested_lane = decision.applied_lane if decision else None
    base_lane_value = (record.card_snapshot or {}).get("lane")
    base_lane = (
        CardLane(base_lane_value)
        if base_lane_value in {lane.value for lane in CardLane}
        else None
    )
    expected_dispatch_transition = bool(card) and (
        card.lane == CardLane.ACTIVE
        and card.preferred_instance == body.source_instance_id
    )
    record.card_lane_before = card.lane.value if card else None
    record.reconciliation_current_card = (
        {
            "lane": card.lane.value,
            "updated_at": card.updated_at.isoformat(),
            "preferred_instance": card.preferred_instance,
        }
        if card
        else None
    )

    if not decision or decision.requested_lane is None:
        outcome = "not_applicable"
        reason = decision.reason if decision else "No card disposition applies."
    elif card.lane == requested_lane:
        outcome = "already_satisfied"
        reason = f"The card is already in requested lane {requested_lane.value}."
    elif card.lane == CardLane.DONE:
        outcome = "operator_state_preserved"
        reason = "The newer authoritative Done state was preserved."
    elif (
        base_lane is not None
        and card.lane != base_lane
        and not expected_dispatch_transition
    ):
        outcome = "conflict_requires_resolution"
        reason = (
            f"Current lane {card.lane.value} conflicts with dispatch-base lane "
            f"{base_lane.value}; requested {requested_lane.value} was not applied."
        )
    else:
        request.app.state.ctx.store.update_card(
            card.id,
            CardUpdate(lane=requested_lane),
            realm_id=body.realm_id,
            principal_id="fleet:card-disposition",
            instance_id=request.app.state.ctx.settings.instance_id,
        )
        outcome = "applied"
        reason = decision.reason

    record.card_disposition_status = decision.status if decision else "not_applicable"
    record.card_disposition_reason = reason
    record.card_lane_after = (
        requested_lane.value if outcome == "applied" else card.lane.value
    )
    record.reconciliation_state = outcome
    record.reconciliation_condition = (
        "operator_resolution" if outcome == "conflict_requires_resolution" else None
    )
    record.reconciliation_recoverable = outcome == "conflict_requires_resolution"
    record.reconciliation_updated_at = datetime.now(UTC)

    report = body.final_report or ledger.build_final_report(dispatch_id, body.result)
    if report:
        linked_watch = next(
            (
                watch
                for watch in watches
                if not decision
                or not decision.watch_id
                or watch.id == decision.watch_id
            ),
            watches[0] if watches else None,
        )
        watch_state = dict(linked_watch.state or {}) if linked_watch else {}
        report = report.model_copy(
            update={
                "pr_url": linked_watch.pr_url if linked_watch else report.pr_url,
                "pr_number": linked_watch.pr_number
                if linked_watch
                else report.pr_number,
                "commit_sha": (
                    str(watch_state.get("head_sha") or linked_watch.head_sha or "")
                    or report.commit_sha
                )
                if linked_watch
                else report.commit_sha,
                "ci_evidence": [
                    sanitize_text(
                        f"{item.get('name')}: {item.get('conclusion') or item.get('status') or 'unknown'}",
                        limit=240,
                    )
                    for item in list(watch_state.get("checks") or [])[:40]
                    if isinstance(item, dict)
                ],
                "review_evidence": [
                    sanitize_text(
                        f"{item.get('path') or 'review'}: {'resolved' if item.get('resolved') else 'open'}",
                        limit=240,
                    )
                    for item in list(watch_state.get("review_threads") or [])[:40]
                    if isinstance(item, dict)
                ],
                "merge_commit_sha": str(watch_state.get("merge_commit_sha") or "")
                or None,
                "card_disposition": body.disposition
                if isinstance(body.disposition, dict)
                else report.card_disposition,
                "resulting_lane": record.card_lane_after,
            }
        )
    record.final_report = sanitize_completion_report(report) if report else None
    ledger.put(record)
    _record_post_turn_evaluation(
        request, ledger, record, card=card, watches=watches
    )
    return _completion_ack(record, duplicate=False)


@router.post("/fleet/dispatch/{dispatch_id}/turn-end")
def complete_followup_turn(
    request: Request, dispatch_id: str, body: DispatchTurnEndBody
) -> dict[str, Any]:
    """Capture a later turn without reopening or replaying dispatch completion."""
    _require_instance(request)
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    expected_key = f"{record.mutation_id}:turn:{body.turn_id}"
    if (
        body.mutation_id != record.mutation_id
        or body.source_instance_id != record.target_instance_id
        or body.session_id != record.session_id
        or request.headers.get("idempotency-key") != expected_key
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "followup_turn_provenance_mismatch"},
        )
    existing = next(
        (item for item in record.turn_end_snapshots if item.turn_id == body.turn_id),
        None,
    )
    if existing:
        return {
            "acknowledged": True,
            "duplicate": True,
            "snapshot_id": existing.snapshot_id,
            "dispatch_state": record.state,
        }
    if not record.acknowledged_at:
        raise HTTPException(
            status_code=409,
            detail={"code": "dispatch_completion_not_acknowledged"},
        )
    card = (
        request.app.state.ctx.store.get_card(
            record.card_id, realm_id=record.realm_id
        )
        if record.card_id
        else None
    )
    watches = []
    supervisor_store = request.app.state.ctx.services.get("pr_supervisor_store")
    if card and supervisor_store:
        watches = supervisor_store.list_watches(
            realm_id=record.realm_id, card_id=card.id, include_retired=True
        )
    if body.final_report:
        record.final_report = sanitize_completion_report(body.final_report)
    linked = next(
        (
            item
            for item in reversed(record.followup_turns)
            if str(item.get("prompt_id") or item.get("idempotency_key"))
            == body.turn_id
        ),
        None,
    )
    if linked:
        linked.update(
            {
                "state": "ended",
                "stop_reason": body.result.get("stop_reason"),
                "ended_at": datetime.now(UTC).isoformat(),
                "delivery_state": "acknowledged",
            }
        )
    else:
        record.followup_turns.append(
            {
                "prompt_id": body.turn_id,
                "state": "ended",
                "stop_reason": body.result.get("stop_reason"),
                "ended_at": datetime.now(UTC).isoformat(),
                "delivery_state": "acknowledged",
                "session_id": body.session_id,
            }
        )
    ledger.transition(
        record,
        record.state,
        "Follow-up agent turn ended; immutable dispatch completion retained.",
        detail={
            "turn_id": body.turn_id,
            "dispatch_state_retained": record.state,
        },
    )
    _record_post_turn_evaluation(
        request,
        ledger,
        record,
        card=card,
        watches=watches,
        result_override=body.result,
        turn_id_override=body.turn_id,
    )
    return {
        "acknowledged": True,
        "duplicate": False,
        "snapshot_id": record.turn_end_snapshots[-1].snapshot_id,
        "evaluation": record.post_turn_evaluations[-1].model_dump(mode="json"),
        "dispatch_state": record.state,
    }


def _completion_ack(record: DispatchRecord, *, duplicate: bool) -> dict[str, Any]:
    return {
        "dispatch_id": record.dispatch_id,
        "acknowledged": True,
        "duplicate": duplicate,
        "acknowledged_at": record.acknowledged_at.isoformat()
        if record.acknowledged_at
        else None,
        "agent_turn": {"ended": True, "completed": True},
        "evaluation": (
            record.post_turn_evaluations[-1].model_dump(mode="json")
            if record.post_turn_evaluations
            else None
        ),
        "card_disposition": {
            "status": record.card_disposition_status,
            "lane_before": record.card_lane_before,
            "lane_after": record.card_lane_after,
            "reason": record.card_disposition_reason,
        },
        "reconciliation": {
            "state": record.reconciliation_state,
            "condition": record.reconciliation_condition,
        },
    }


@router.get("/fleet/progress/capabilities")
def progress_capabilities(request: Request) -> dict[str, Any]:
    require_user(request)
    return {
        "schema": "pa.dispatch-progress",
        "versions": SUPPORTED_PROGRESS_VERSIONS,
        "checkpoint_limit": 200,
        "heartbeat_separate": True,
        "raw_tool_output": False,
    }


@router.post("/fleet/dispatch/{dispatch_id}/progress")
def ingest_dispatch_progress(
    request: Request,
    dispatch_id: str,
    body: DispatchProgressEventV1 | DispatchProgressHeartbeatV1,
):
    """Accept an authenticated target checkpoint exactly once at the authority."""
    _require_instance(request)
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if not record:
        raise HTTPException(
            status_code=409,
            detail={"code": "unknown_dispatch", "recoverable": True},
        )
    caller = request.headers.get("X-PA-Origin-Instance-ID", "").strip()
    if (
        caller != body.originating_instance_id
        or caller != record.target_instance_id
        or request.headers.get("idempotency-key") != body.idempotency_key
    ):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "progress_provenance_mismatch",
                "recoverable": False,
            },
        )
    try:
        result = (
            ledger.ingest_heartbeat(body, delivered=True)
            if body.kind == ProgressKind.HEARTBEAT
            else ledger.ingest_progress(body, delivered=True)
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "progress_conflict",
                "message": str(exc)[:1000],
                "recoverable": False,
            },
        ) from exc
    status_code = 208 if result.status == "duplicate" else 200
    return JSONResponse(result.model_dump(mode="json"), status_code=status_code)


@router.post("/fleet/dispatch-jobs/{dispatch_id}/checkpoint")
async def report_dispatch_checkpoint(
    request: Request,
    dispatch_id: str,
    body: ExplicitProgressCheckpointV1,
) -> dict[str, Any]:
    """Emit an allowlisted explicit checkpoint for a locally linked dispatch."""
    record = _dispatch_store(request).get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    _require_dispatch_access(request, record)
    service = request.app.state.ctx.services.get("progress_service")
    if not service:
        raise HTTPException(
            status_code=503,
            detail={"code": "progress_reporting_unavailable", "recoverable": True},
        )
    if record.progress_protocol_version != PROGRESS_SCHEMA_VERSION:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "progress_protocol_not_negotiated",
                "supported_versions": SUPPORTED_PROGRESS_VERSIONS,
                "recoverable": False,
            },
        )
    try:
        result = await service.explicit(dispatch_id, body)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return result.model_dump(mode="json")


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
    instances = list(fleet.list_instances())
    try:
        policy_service: FleetPolicyService = ctx.require_service("fleet_policy")
    except (KeyError, RuntimeError):
        # Keep direct context/unit-test construction compatible with modules
        # that predate the registered policy service.
        policy_service = FleetPolicyService(ctx.store)
    participation = {}
    for instance in instances:
        policy, explicit = policy_service.effective_policy(
            primary_realm, instance.instance_id
        )
        participation[instance.instance_id] = {
            **policy.model_dump(mode="json"),
            "explicit": explicit,
            "summary": policy.summary(),
        }
    canonical_ids = {
        item.instance_id for item in instances if item.lifecycle_state == "active"
    }
    canonical_urls = {
        item.url.rstrip("/") for item in instances if item.lifecycle_state == "active"
    }
    routes = [
        route
        for route in peer_table.all_routes()
        if (
            route.target_instance_id in canonical_ids
            or route.target_url.rstrip("/") in canonical_urls
        )
    ]
    return {
        "fleet_instances": instances,
        "fleet_overview": build_overview(ctx, instances, routes),
        "local_version": __import__("pa").__version__,
        "realms": membership.list_realms(),
        "memberships": membership.list_memberships(),
        "peer_routes": routes,
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
        "instance_groups": policy_service.list_groups(
            primary_realm, include_archived=True
        ),
        "participation_policies": participation,
        "placement_defaults": ctx.store.list_placement_defaults(primary_realm),
        "fleet_policy_audit": ctx.store.list_fleet_policy_audit(
            primary_realm, limit=50
        ),
    }


def _workshop_context(request: Request) -> dict:
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    peer_table: PeerTable = ctx.require_service("peer_table")
    instances = list(fleet.list_instances())
    overview = build_overview(ctx, instances, list(peer_table.all_routes()))
    return {
        "workshop": build_workshop_snapshot(ctx, overview),
        "primary_realm": ctx.settings.primary_realm,
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
        result = await _offload_request(
            request,
            "filesystem.fleet_reachability_write",
            apply_reachability_settings,
            settings,
            **kwargs,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    await _offload_request(
        request,
        "filesystem.fleet_registry_write",
        fleet.register_self,
        settings.instance_id,
        settings.instance_name,
        owner_public_url(settings),
        zone=settings.zone,
        capabilities=list(settings.capabilities),
        dispatch_capacity=settings.dispatch_capacity,
        dispatch_provider_capacities=dict(settings.dispatch_provider_capacities),
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


def _signed_membership(ctx: AppContext) -> dict[str, Any]:
    if not ctx.settings.sync_token:
        raise HTTPException(
            status_code=503,
            detail="Fleet authentication must be configured before membership exchange",
        )
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    snapshot = fleet.snapshot()
    envelope: dict[str, Any] = {
        "issuer_instance_id": ctx.settings.instance_id,
        "membership": snapshot,
    }
    payload = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
    envelope["signature"] = hmac.new(
        ctx.settings.sync_token.encode(), payload, hashlib.sha256
    ).hexdigest()
    return envelope


def _verify_membership_envelope(
    settings,
    envelope: dict[str, Any],
    *,
    expected_issuer: str = "",
    expected_endpoint: str = "",
) -> dict[str, Any]:
    signature = str(envelope.get("signature", ""))
    unsigned = {
        "issuer_instance_id": envelope.get("issuer_instance_id", ""),
        "membership": envelope.get("membership", {}),
    }
    payload = json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    expected = hmac.new(
        settings.sync_token.encode(), payload, hashlib.sha256
    ).hexdigest()
    if not settings.sync_token or not hmac.compare_digest(signature, expected):
        raise ValueError("membership signature is invalid")
    membership = unsigned["membership"]
    issuer = str(unsigned["issuer_instance_id"])
    members = {
        item.get("instance_id"): item
        for item in membership.get("instances", [])
        if isinstance(item, dict)
    }
    if expected_issuer and issuer != expected_issuer:
        raise ValueError("signed membership issuer does not match authenticated origin")
    if (
        issuer not in members
        or members[issuer].get("lifecycle_state", "active") != "active"
    ):
        raise ValueError("membership issuer is not an active canonical member")
    if expected_endpoint:
        endpoints = {
            str(value).rstrip("/")
            for value in members[issuer].get("endpoints", [])
            if value
        }
        primary = str(members[issuer].get("url", "")).rstrip("/")
        if primary:
            endpoints.add(primary)
        if expected_endpoint.rstrip("/") not in endpoints:
            raise ValueError(
                "signed membership issuer identity does not match the reached endpoint"
            )
    return membership


@router.get("/fleet/membership")
def fleet_membership(request: Request) -> dict[str, Any]:
    """Return the authenticated, versioned canonical membership projection."""
    _require_instance(request)
    return _signed_membership(request.app.state.ctx)


@router.post("/fleet/membership/apply")
def apply_fleet_membership(request: Request, body: dict) -> dict[str, Any]:
    """Install a signed canonical projection and derive routes from it."""
    _require_instance(request)
    ctx = request.app.state.ctx
    try:
        snapshot = _verify_membership_envelope(
            ctx.settings,
            body,
            expected_issuer=request.headers.get("X-PA-Origin-Instance-ID", ""),
        )
        result = ctx.require_service("fleet_registry").apply_snapshot(
            snapshot,
            actor=f"instance:{body.get('issuer_instance_id', '')}",
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    result["routes"] = ctx.require_service("peer_table").reconcile_membership(
        ctx.require_service("fleet_registry").list_instances(),
        realms=list(ctx.settings.subscribed_realms),
        local_instance_id=ctx.settings.instance_id,
    )
    return result


@router.get("/fleet/membership/audit")
def fleet_membership_audit(request: Request, limit: int = 100) -> dict[str, Any]:
    require_user(request)
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    return {
        "fleet_id": fleet.fleet_id,
        "generation": fleet.generation,
        "events": fleet.audit_events(limit=limit),
    }


@router.post("/fleet/membership/reconcile")
async def reconcile_fleet_membership(request: Request) -> dict[str, Any]:
    """Audit local sources and repair from an unambiguous authenticated peer roster."""
    require_user(request)
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    peer_table: PeerTable = ctx.require_service("peer_table")
    before = fleet.snapshot()
    candidates = {
        route.target_url.rstrip("/")
        for route in peer_table.all_routes()
        if route.target_url
    }
    candidates.update(
        member.url.rstrip("/")
        for member in fleet.list_instances()
        if member.instance_id != ctx.settings.instance_id and member.url
    )
    headers = _peer_headers(request)
    reachable: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    async with _borrow_fleet_client(request, timeout=FLEET_DETAIL_TIMEOUT) as client:
        for url in sorted(candidates):
            try:
                response = await client.get(
                    f"{url}/api/fleet/membership",
                    headers=headers,
                    timeout=FLEET_DETAIL_TIMEOUT,
                )
                response.raise_for_status()
                envelope = response.json()
                snapshot = _verify_membership_envelope(
                    ctx.settings,
                    envelope,
                    expected_endpoint=url,
                )
                reachable.append(
                    {"url": url, "envelope": envelope, "snapshot": snapshot}
                )
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                failures.append({"url": url, "error": str(exc)[:300]})
    selected = before
    if reachable:
        try:
            selected = reconcile_snapshots(
                [before, *(item["snapshot"] for item in reachable)]
            )
            fleet.apply_snapshot(
                selected, actor=f"user:{get_principal_id(request)}", require_newer=False
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
    route_result = peer_table.reconcile_membership(
        fleet.list_instances(),
        realms=list(ctx.settings.subscribed_realms),
        local_instance_id=ctx.settings.instance_id,
    )
    after = fleet.snapshot()
    rollout = await _rollout_membership(request)
    return {
        "status": "repaired"
        if semantic_snapshot(before) != semantic_snapshot(after)
        else "converged",
        "before_generation": before["generation"],
        "after_generation": after["generation"],
        "members_before": len(before["instances"]),
        "members_after": len(fleet.list_instances()),
        "authenticated_peers": len(reachable),
        "unreachable_or_incompatible": failures,
        "routes": route_result,
        "rollout": rollout,
        "unresolved_conflicts": [],
    }


def _overview_instance(request: Request, instance_id: str) -> FleetInstance:
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    inst = fleet.get_instance(instance_id)
    if inst:
        return inst
    if instance_id == ctx.settings.instance_id:
        return FleetInstance(
            instance_id=ctx.settings.instance_id,
            name=ctx.settings.instance_name,
            url=owner_public_url(ctx.settings),
            zone=ctx.settings.zone,
            capabilities=list(ctx.settings.capabilities),
            dispatch_capacity=ctx.settings.dispatch_capacity,
            dispatch_provider_capacities=dict(
                ctx.settings.dispatch_provider_capacities
            ),
            healthy=True,
        )
    raise HTTPException(status_code=404, detail="Fleet instance not found")


@router.get("/fleet/overview")
def fleet_overview(request: Request) -> dict:
    """Return cached-first normalized state used by both overview renderings."""
    require_user(request)
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    peer_table: PeerTable = ctx.require_service("peer_table")
    instances = list(fleet.list_instances())
    canonical_ids = {
        item.instance_id for item in instances if item.lifecycle_state == "active"
    }
    canonical_urls = {
        item.url.rstrip("/") for item in instances if item.lifecycle_state == "active"
    }
    routes = [
        route
        for route in peer_table.all_routes()
        if (
            route.target_instance_id in canonical_ids
            or route.target_url.rstrip("/") in canonical_urls
        )
    ]
    return build_overview(ctx, instances, routes)


@router.get("/fleet/workshop")
def fleet_workshop(request: Request) -> dict:
    """Return one canonical, presentation-ready Workshop snapshot."""
    require_user(request)
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    peer_table: PeerTable = ctx.require_service("peer_table")
    instances = list(fleet.list_instances())
    overview = build_overview(ctx, instances, list(peer_table.all_routes()))
    return build_workshop_snapshot(ctx, overview)


@router.get("/fleet/overview/local")
async def fleet_overview_local(request: Request, dimension: str) -> dict:
    """Expose one bounded local dimension to another authenticated fleet peer."""
    if dimension not in DIMENSIONS:
        raise HTTPException(status_code=422, detail="Unknown fleet overview dimension")
    ctx = request.app.state.ctx
    inst = _overview_instance(request, ctx.settings.instance_id)
    value = await probe_dimension(ctx, inst, dimension)
    return {"instance_id": inst.instance_id, "dimension": dimension, **value}


@router.get("/fleet/overview/dimension")
async def fleet_overview_dimension(
    request: Request,
    instance_id: str,
    dimension: str,
    generation: int = 0,
    retry: bool = False,
) -> Response:
    """Probe exactly one field with a strict deadline and observable timing."""
    require_user(request)
    if dimension not in DIMENSIONS:
        raise HTTPException(status_code=422, detail="Unknown fleet overview dimension")
    ctx = request.app.state.ctx
    inst = _overview_instance(request, instance_id)
    value = await probe_dimension(ctx, inst, dimension, force=retry)
    duration = value.get("duration_ms") or 0
    return JSONResponse(
        {
            "instance_id": instance_id,
            "dimension": dimension,
            "generation": generation,
            "snapshot_version": cache_for(ctx.settings.data_dir).revision,
            **value,
        },
        headers={
            "Server-Timing": f'fleet-{dimension};dur={duration};desc="{inst.name}"',
            "X-Fleet-Generation": str(generation),
        },
    )


@router.post("/fleet/join")
async def fleet_join(request: Request, body: dict) -> dict:
    token = body.get("token", "")
    joiner_id = body.get("instance_id", "")
    name = body.get("name", "remote")
    url = body.get("url", "")
    zone = body.get("zone", "default")
    capabilities = body.get("capabilities", [])
    dispatch_capacity = body.get("dispatch_capacity")
    dispatch_provider_capacities = body.get("dispatch_provider_capacities", {})
    if not token or not joiner_id:
        raise HTTPException(status_code=400, detail="token and instance_id required")

    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    join = await _offload_request(
        request, "filesystem.fleet_join_consume", fleet.consume_join_token, token
    )
    if not join:
        raise HTTPException(status_code=400, detail="Invalid or expired join token")

    settings = request.app.state.ctx.settings
    owner_url = owner_public_url(settings)
    peer_table: PeerTable = request.app.state.ctx.require_service("peer_table")
    realms = list(settings.subscribed_realms)
    existing_members = list(fleet.list_instances())

    try:
        inst, sync_token = await _offload_request(
            request,
            "filesystem.fleet_join_register",
            register_joiner_on_owner,
            fleet,
            peer_table,
            settings,
            joiner_id=joiner_id,
            name=name,
            url=url or owner_url,
            zone=zone,
            capabilities=capabilities,
            dispatch_capacity=dispatch_capacity,
            dispatch_provider_capacities=dispatch_provider_capacities,
            realms=realms,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    owner_inst = await _offload_request(
        request,
        "filesystem.fleet_registry_read",
        fleet.get_instance,
        settings.instance_id,
    )
    membership = _signed_membership(request.app.state.ctx)
    rollout = await _rollout_membership(request, members=existing_members)
    return {
        "fleet_id": join.fleet_id,
        "owner_url": owner_url,
        "owner_instance": owner_inst.model_dump(mode="json") if owner_inst else None,
        "subscribed_realms": realms,
        "sync_token": sync_token,
        "peers": [owner_url],
        "instance": inst.model_dump(mode="json"),
        "membership": membership["membership"],
        "membership_schema_version": FleetRegistry.SCHEMA_VERSION,
        "rollout": rollout,
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

    try:
        registered, sync_token = await _offload_request(
            request,
            "filesystem.fleet_join_register",
            register_joiner_on_owner,
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
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    data = registered.model_dump(mode="json")
    data["sync_token_set"] = bool(sync_token)
    data["membership_generation"] = fleet.generation
    data["rollout"] = await _rollout_membership(request)
    return data


@router.patch("/fleet/instances/{instance_id}")
async def update_instance(request: Request, instance_id: str, body: dict) -> dict:
    """Update canonical metadata while preserving stable identity."""
    require_user(request)
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    current = fleet.get_instance(instance_id)
    if not current:
        raise HTTPException(status_code=404, detail="Instance not found")
    allowed = {
        "name",
        "url",
        "endpoints",
        "zone",
        "capabilities",
        "dispatch_capacity",
        "dispatch_provider_capacities",
        "relay_enabled",
        "lifecycle_state",
        "credential_fingerprint",
    }
    unknown = set(body) - allowed
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported membership fields: {sorted(unknown)}",
        )
    data = current.model_dump()
    data.update(body)
    if data.get("lifecycle_state") not in {"active", "disabled"}:
        raise HTTPException(
            status_code=422, detail="lifecycle_state must be active or disabled"
        )
    try:
        updated = fleet.upsert_instance(
            FleetInstance.model_validate(data),
            actor=f"user:{get_principal_id(request)}",
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    ctx.require_service("peer_table").reconcile_membership(
        fleet.list_instances(),
        realms=list(ctx.settings.subscribed_realms),
        local_instance_id=ctx.settings.instance_id,
    )
    return {
        "instance": updated.model_dump(mode="json"),
        "generation": fleet.generation,
        "rollout": await _rollout_membership(request),
    }


@router.delete("/fleet/instances/{instance_id}")
async def remove_instance(request: Request, instance_id: str) -> dict:
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
    fleet.remove_instance(instance_id, actor=f"user:{get_principal_id(request)}")
    return {
        "removed": instance_id,
        "generation": fleet.generation,
        "rollout": await _rollout_membership(request),
    }


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

    async with _borrow_fleet_client(request, timeout=FLEET_DETAIL_TIMEOUT) as client:

        async def remote_get(url: str, *, timeout: float, headers=None):
            # httpx retains the strict transport deadline. This small wrapper
            # allowance prevents a ready response from being classified as a
            # network timeout solely because a loaded loop resumed it late.
            scheduling_grace = min(0.05, max(0.015, timeout * 0.1))
            return await _fleet_http(
                request,
                "http.fleet_health",
                client.get(url, headers=headers, timeout=timeout),
                timeout=timeout + scheduling_grace,
            )

        async def dimension(coro, parser) -> tuple[str, Any]:
            try:
                response = await coro
                if response.status_code != 200:
                    return "error", None
                return "up", parser(await _response_json(request, response))
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
                    from pa.acp.providers.resolve import (
                        list_provider_summaries_bounded,
                    )
                    from pa.release.version import read_version

                    try:
                        current_version = await _offload_request(
                            request, "filesystem.release_version_read", read_version
                        )
                    except OSError, RuntimeError, ValueError:
                        current_version = None
                        status_state = "error"
                    update_channel = settings.release_track
                    if current_version:
                        status_state = "up"

                    async def local_providers() -> tuple[str, list]:
                        try:
                            services = request.app.state.ctx.services
                            value = await list_provider_summaries_bounded(
                                settings.data_dir,
                                manager=services.get("instance_agent")
                                if isinstance(services, dict)
                                else None,
                                async_runtime=services.get("async_runtime")
                                if isinstance(services, dict)
                                and isinstance(
                                    services.get("async_runtime"), AsyncRuntime
                                )
                                else None,
                                timeout=max(0.1, FLEET_DETAIL_TIMEOUT - 0.1),
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
                            value = await _offload_request(
                                request,
                                "fleet.update_check",
                                check_update,
                                settings,
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
                        ),
                        dimension(
                            remote_get(
                                f"{base}/api/status",
                                headers=headers,
                                timeout=FLEET_DETAIL_TIMEOUT,
                            ),
                            dict_payload,
                        ),
                        dimension(
                            remote_get(
                                f"{base}/api/fleet/peer-update-check",
                                headers=headers,
                                timeout=FLEET_DETAIL_TIMEOUT,
                            ),
                            dict_payload,
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
        done, pending = await asyncio.wait(
            tasks, timeout=FLEET_AGGREGATE_TIMEOUT + 0.05
        )
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
        await _offload_request(
            request,
            "filesystem.fleet_registry_write",
            fleet.upsert_instance,
            inst,
        )
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

    await _offload_request(
        request, "filesystem.fleet_sync_token", ensure_sync_token, settings
    )
    job = await _offload_request(request, "fleet.install_job_create", store.create, req)
    asyncio.create_task(
        run_install_job(
            settings,
            fleet,
            store,
            job,
            req,
            async_runtime=request.app.state.ctx.require_service("async_runtime"),
            http_client=request.app.state.ctx.services.get("fleet_http_client"),
        ),
        name=f"pa-fleet-install-{job.job_id}",
    )
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
    from pa.server.shutdown import is_shutting_down, wait_for_shutdown

    require_user(request)
    store = get_job_store(request.app.state.ctx.settings)
    job = store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Install job not found")

    async def event_stream():
        last_len = 0
        for _ in range(600):
            if is_shutting_down() or await request.is_disconnected():
                return
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
            if await wait_for_shutdown(0.5):
                return

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
    headers = {
        "Accept": "application/json",
        "X-PA-Origin-Instance-ID": settings.instance_id,
    }
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    return headers


async def _rollout_membership(
    request: Request,
    *,
    members: list[FleetInstance] | None = None,
) -> list[dict[str, str]]:
    """Push the current generation and retain explicit pending diagnostics."""
    ctx = request.app.state.ctx
    roster = (
        members
        if members is not None
        else ctx.require_service("fleet_registry").list_instances()
    )
    envelope = _signed_membership(ctx)
    results: list[dict[str, str]] = []
    async with _borrow_fleet_client(request, timeout=FLEET_DETAIL_TIMEOUT) as client:
        for member in roster:
            if member.instance_id == ctx.settings.instance_id or not member.url:
                continue
            try:
                response = await client.post(
                    f"{member.url.rstrip('/')}/api/fleet/membership/apply",
                    json=envelope,
                    headers=_peer_headers(request),
                    timeout=FLEET_DETAIL_TIMEOUT,
                )
                response.raise_for_status()
                results.append(
                    {"instance_id": member.instance_id, "status": "converged"}
                )
            except httpx.HTTPError as exc:
                results.append(
                    {
                        "instance_id": member.instance_id,
                        "status": "pending",
                        "detail": str(exc)[:200],
                    }
                )
    return results


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

    existing = await _offload_request(
        request,
        "filesystem.fleet_peer_update_read",
        _read_peer_operation,
        settings,
        operation_id,
    )
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
        release = await _offload_request(
            request,
            "fleet.release_resolve",
            resolve_release,
            channel,
            target_version,
            repo=settings.update_repo,
            revision=target_identity,
            timeout=60.0,
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
        "restart_state": None,
        "restart_diagnostic": None,
    }
    if not existing:
        await _offload_request(
            request,
            "filesystem.fleet_peer_update_write",
            _write_peer_operation,
            settings,
            operation_id,
            operation,
        )

    async def _install_and_restart() -> None:
        from pa.cli import service as svc
        from pa.instance.quiesce import request_skip_quiesce

        await asyncio.sleep(0.25)
        try:
            exact_target = await _offload_request(
                request,
                "filesystem.fleet_release_verify",
                _peer_has_exact_release,
                settings,
                channel,
                release,
            )
            if not exact_target:
                result = await _offload_request(
                    request,
                    "fleet.update_apply",
                    apply_update,
                    settings,
                    channel_name=channel,
                    restart=False,
                    release=release,
                    timeout=900.0,
                )
                exact_target = result.upgrade_available or await _offload_request(
                    request,
                    "filesystem.fleet_release_verify",
                    _peer_has_exact_release,
                    settings,
                    channel,
                    release,
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
            await _offload_request(
                request,
                "filesystem.fleet_peer_update_write",
                _write_peer_operation,
                settings,
                operation_id,
                installed_operation,
            )
            restarting_operation = {
                **installed_operation,
                "status": "restart_requested",
                "restart_state": "restart_requested",
                "message": "Restart requested; waiting for peer health.",
            }
            await _offload_request(
                request,
                "filesystem.fleet_peer_update_write",
                _write_peer_operation,
                settings,
                operation_id,
                restarting_operation,
            )
            await _offload_request(
                request,
                "filesystem.quiesce_marker_write",
                request_skip_quiesce,
                settings.data_dir,
            )

            def restart_progress(message: str) -> None:
                current = (
                    _read_peer_operation(settings, operation_id) or restarting_operation
                )
                _write_peer_operation(
                    settings,
                    operation_id,
                    {**current, "message": redact_log_text(message)},
                )

            diagnostic = await _offload_request(
                request,
                "lifecycle.service_restart",
                svc.request_restart,
                settings,
                progress=restart_progress,
                operation_id=operation_id,
                timeout=120.0,
            )
            diagnostic_payload = diagnostic.public_dict()
            response_lost = diagnostic.state == "restart_response_lost"
            current = (
                await _offload_request(
                    request,
                    "filesystem.fleet_peer_update_read",
                    _read_peer_operation,
                    settings,
                    operation_id,
                )
                or restarting_operation
            )
            await _offload_request(
                request,
                "filesystem.fleet_peer_update_write",
                _write_peer_operation,
                settings,
                operation_id,
                {
                    **current,
                    "status": (
                        "verification_required"
                        if response_lost
                        else "restart_requested"
                    ),
                    "restart_state": diagnostic.state,
                    "restart_diagnostic": diagnostic_payload,
                    "message": "Restart requested; waiting for peer health.",
                },
            )
        except Exception as exc:
            current = (
                await _offload_request(
                    request,
                    "filesystem.fleet_peer_update_read",
                    _read_peer_operation,
                    settings,
                    operation_id,
                )
                or operation
            )
            restart_was_requested = current.get("status") in {
                "restart_requested",
                "verification_required",
            }
            rejected = isinstance(exc, svc.RestartRejectedError)
            diagnostic = getattr(exc, "diagnostic", None)
            diagnostic_payload = (
                diagnostic.public_dict()
                if diagnostic is not None
                else {
                    "state": (
                        "restart_response_lost"
                        if restart_was_requested
                        else "install_failed"
                    ),
                    "backend": "unknown",
                    "command": [],
                    "started_at": None,
                    "completed_at": datetime.now(UTC).isoformat(),
                    "duration_ms": None,
                    "exit_code": None,
                    "signal": None,
                    "stdout": "",
                    "stderr": redact_log_text(exc),
                }
            )
            status = "failed"
            message = "Installation failed"
            if restart_was_requested:
                status = "restart_rejected" if rejected else "verification_required"
                message = (
                    "Restart command was rejected by the host service manager."
                    if rejected
                    else "Restart requested; waiting for peer health."
                )
            await _offload_request(
                request,
                "filesystem.fleet_peer_update_write",
                _write_peer_operation,
                settings,
                operation_id,
                {
                    **current,
                    "status": status,
                    "restart_state": diagnostic_payload["state"],
                    "restart_diagnostic": diagnostic_payload,
                    "error": redact_log_text(exc),
                    "message": message,
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

    result = await _offload_request(
        request,
        "fleet.update_check",
        check_update,
        settings,
        channel_name=channel,
        timeout=60.0,
    )
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
        async with _borrow_fleet_client(request, timeout=10.0) as client:
            status_resp, update_resp = await asyncio.gather(
                _fleet_http(
                    request,
                    "http.fleet_update_check",
                    client.get(
                        f"{inst.url.rstrip('/')}/api/status",
                        headers=headers,
                        timeout=10.0,
                    ),
                    timeout=10.0,
                ),
                _fleet_http(
                    request,
                    "http.fleet_update_check",
                    client.get(
                        f"{inst.url.rstrip('/')}/api/fleet/peer-update-check",
                        headers=headers,
                        params={"channel": selected},
                        timeout=10.0,
                    ),
                    timeout=10.0,
                ),
            )
        status_resp.raise_for_status()
        update_resp.raise_for_status()
    except (httpx.HTTPError, TimeoutError) as exc:
        raise HTTPException(
            status_code=502, detail=f"Could not check peer update availability: {exc}"
        ) from exc
    status_data, update_data = await asyncio.gather(
        _response_json(request, status_resp),
        _response_json(request, update_resp),
    )
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
        job = await _offload_request(
            request,
            "fleet.update_job_create",
            store.create,
            inst,
            body,
            settings.release_track,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "An update is already active for this instance",
                "job_id": str(exc),
            },
        ) from exc
    start_update_job(
        settings,
        store,
        job,
        async_runtime=request.app.state.ctx.require_service("async_runtime"),
        http_client=request.app.state.ctx.require_service("fleet_http_client"),
    )
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
    from pa.server.shutdown import is_shutting_down, wait_for_shutdown

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
            if is_shutting_down() or await request.is_disconnected():
                return
            job = store.get(job_id)
            if not job:
                yield 'event: error\ndata: {"message":"job missing"}\n\n'
                return
            for event in store.events_after(job, cursor):
                seq = int(event["seq"])
                encoded = await _offload_request(
                    request, "fleet.sse_json", json.dumps, event
                )
                yield f"id: {seq}\nevent: phase\ndata: {encoded}\n\n"
                cursor = seq
            public = job.public_dict()
            encoded = await _offload_request(
                request, "fleet.sse_json", json.dumps, public
            )
            yield f"event: status\ndata: {encoded}\n\n"
            if job.phase in TERMINAL_PHASES:
                yield f"event: done\ndata: {encoded}\n\n"
                return
            if await wait_for_shutdown(0.5):
                return

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
    params: dict[str, Any] | None = None,
    timeout: float = 120.0,
) -> dict | list:
    inst = _fleet_instance_or_404(request, instance_id)
    url = f"{inst.url.rstrip('/')}/api/agent/{_agent_path(path)}"
    client = request.app.state.ctx.services.get("fleet_http_client")
    owns_client = client is None
    client = client or httpx.AsyncClient(timeout=timeout)
    request_kwargs: dict[str, Any] = {
        "headers": _peer_headers(request),
        "json": body,
        "timeout": timeout,
    }
    if params is not None:
        request_kwargs["params"] = params
    try:
        resp = await _fleet_http(
            request,
            "http.fleet_agent",
            client.request(
                method,
                url,
                **request_kwargs,
            ),
            timeout=timeout,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Peer unreachable: {exc}") from exc
    finally:
        if owns_client:
            await client.aclose()
    if resp.status_code >= 400:
        try:
            decoded = await _response_json(request, resp)
            detail = decoded.get("detail")
        except ValueError, AttributeError:
            detail = resp.text[:500]
        raise HTTPException(
            status_code=resp.status_code, detail=detail or "Peer request failed"
        )
    return await _response_json(request, resp)


async def _peer_dispatch_json(
    request: Request, instance_id: str, body: dict[str, Any]
) -> dict:
    inst = _fleet_instance_or_404(request, instance_id)
    client = request.app.state.ctx.services.get("fleet_http_client")
    owns_client = client is None
    client = client or httpx.AsyncClient(timeout=15.0)
    try:
        resp = await _fleet_http(
            request,
            "http.fleet_dispatch",
            client.post(
                f"{inst.url.rstrip('/')}/api/fleet/dispatch/materialize",
                headers={
                    **_peer_headers(request),
                    "Idempotency-Key": str(body["mutation_id"]),
                },
                json=body,
                timeout=15.0,
            ),
            timeout=15.0,
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
    finally:
        if owns_client:
            await client.aclose()
    if resp.status_code >= 400:
        try:
            decoded = await _response_json(request, resp)
            detail = decoded.get("detail")
        except ValueError, AttributeError:
            detail = resp.text[:500]
        raise HTTPException(status_code=resp.status_code, detail=detail)
    return await _response_json(request, resp)


async def _peer_authority_json(
    request: Request,
    authority_instance_id: str,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    inst = _fleet_instance_or_404(request, authority_instance_id)
    client = request.app.state.ctx.services.get("fleet_http_client")
    owns_client = client is None
    client = client or httpx.AsyncClient(timeout=timeout)
    headers = _peer_headers(request)
    key = (body or {}).get("idempotency_key")
    if key:
        headers["Idempotency-Key"] = str(key)
    try:
        response = await _fleet_http(
            request,
            "http.fleet_authority",
            client.request(
                method,
                f"{inst.url.rstrip('/')}/api/fleet/{path.lstrip('/')}",
                headers=headers,
                json=body,
                timeout=timeout,
            ),
            timeout=timeout,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "authority_unavailable",
                "message": str(exc),
                "recoverable": True,
            },
        ) from exc
    finally:
        if owns_client:
            await client.aclose()
    if response.status_code >= 400:
        try:
            decoded = await _response_json(request, response)
            detail = decoded.get("detail")
        except ValueError, AttributeError:
            detail = response.text[:500]
        raise HTTPException(status_code=response.status_code, detail=detail)
    return await _response_json(request, response)


async def _transfer_missing_attachments(
    request: Request,
    instance_id: str,
    dispatch_id: str,
    card_id: str,
    realm_id: str,
    manifest: list[CardAttachment],
    missing: list[dict[str, Any]],
) -> None:
    inst = _fleet_instance_or_404(request, instance_id)
    source = AttachmentStore(request.app.state.ctx.settings.data_dir)
    by_hash = {item.sha256: item for item in manifest}
    async with _borrow_fleet_client(request, timeout=120.0) as client:
        for need in missing:
            item = by_hash.get(str(need.get("sha256") or ""))
            if not item or not source.has_verified_blob(item.sha256, item.size):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "authority_attachment_missing",
                        "sha256": need.get("sha256"),
                        "message": "The authority does not have the required verified blob.",
                        "recoverable": True,
                    },
                )
            offset = int(need.get("offset") or 0)
            if offset > item.size:
                raise HTTPException(
                    status_code=409,
                    detail={"code": "invalid_resume_offset", "recoverable": True},
                )
            with source.blob_path(item.sha256).open("rb") as handle:
                handle.seek(offset)
                while offset < item.size:
                    chunk = await _offload_request(
                        request, "attachments.chunk_read", handle.read, CHUNK_BYTES
                    )
                    if not chunk:
                        break
                    response = await _fleet_http(
                        request,
                        "http.attachment_chunk",
                        client.put(
                            f"{inst.url.rstrip('/')}/api/fleet/dispatch/{dispatch_id}/attachments/{item.sha256}",
                            headers={
                                **_peer_headers(request),
                                "Content-Type": "application/octet-stream",
                            },
                            params={
                                "realm_id": realm_id,
                                "card_id": card_id,
                                "size": item.size,
                                "offset": offset,
                            },
                            content=chunk,
                            timeout=120.0,
                        ),
                        timeout=120.0,
                    )
                    if response.status_code >= 400:
                        try:
                            detail = (await _response_json(request, response)).get(
                                "detail"
                            )
                        except ValueError, AttributeError:
                            detail = response.text[:500]
                        raise HTTPException(
                            status_code=response.status_code, detail=detail
                        )
                    offset += len(chunk)
            response = await _fleet_http(
                request,
                "http.attachment_finalize",
                client.post(
                    f"{inst.url.rstrip('/')}/api/fleet/dispatch/{dispatch_id}/attachments/{item.sha256}/finalize",
                    headers=_peer_headers(request),
                    json={"realm_id": realm_id, "card_id": card_id, "size": item.size},
                    timeout=120.0,
                ),
                timeout=120.0,
            )
            if response.status_code >= 400:
                try:
                    detail = (await _response_json(request, response)).get("detail")
                except ValueError, AttributeError:
                    detail = response.text[:500]
                raise HTTPException(status_code=response.status_code, detail=detail)


async def _assert_dispatch_sync_health(
    request: Request,
    realm_id: str,
    target_instance_id: str | None = None,
) -> dict[str, Any] | None:
    """Validate authority state and the selected target without fleet-wide liveness."""
    settings = request.app.state.ctx.settings
    if target_instance_id:
        ctx = request.app.state.ctx
        log = ctx.require_service("event_log")
        durable_head = await _offload_request(
            request, "filesystem.sync_head_read", log.get_head, realm_id
        )
        projection_head = await _offload_request(
            request,
            "sqlite.projection_head_read",
            ctx.store.get_projection_head,
            realm_id,
        )
        if durable_head != projection_head:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "authority_projection_stale",
                    "message": "Dispatch blocked until the authority projection matches its durable realm head.",
                    "realm_id": realm_id,
                    "durable_head": durable_head,
                    "projection_head": projection_head,
                    "recoverable": True,
                    "recovery_url": f"/fleet?section=sync&realm={quote(realm_id)}",
                },
            )
        target = _fleet_instance_or_404(request, target_instance_id)
        target_url = target.url.rstrip("/")
        peer_urls = list(
            dict.fromkeys([target_url, *(url.rstrip("/") for url in settings.peers)])
        )
        headers = _peer_headers(request)

        async def read_peer_head(
            client: httpx.AsyncClient, peer_url: str
        ) -> dict[str, Any]:
            try:
                response = await _fleet_http(
                    request,
                    "http.fleet_sync_head",
                    client.get(
                        f"{peer_url.rstrip('/')}/api/sync/refs",
                        params={"realm": realm_id},
                        headers=headers,
                        timeout=5.0,
                    ),
                    timeout=5.0,
                )
                response.raise_for_status()
                refs = await _response_json(request, response)
                head = next(
                    (
                        item.get("head_hash")
                        for item in refs
                        if item.get("realm_id") == realm_id
                    ),
                    None,
                )
                return {
                    "url": peer_url,
                    "status": "reachable" if head else "missing_head",
                    "head": head,
                }
            except (httpx.HTTPError, TimeoutError) as exc:
                return {
                    "url": peer_url,
                    "status": "unavailable",
                    "head": None,
                    "error": str(exc),
                }
            except ValueError as exc:
                return {
                    "url": peer_url,
                    "status": "invalid_response",
                    "head": None,
                    "error": str(exc),
                }

        async with _borrow_fleet_client(request, timeout=5.0) as client:
            observations = await asyncio.gather(
                *(read_peer_head(client, url) for url in peer_urls)
            )
        target_observation = next(
            item for item in observations if item["url"] == target_url
        )
        if target_observation["status"] != "reachable":
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "target_unavailable",
                    "message": "The selected execution target did not report an authenticated realm head.",
                    "realm_id": realm_id,
                    "target_instance_id": target_instance_id,
                    "target": target_observation,
                    "recoverable": True,
                },
            )
        if target_observation["head"] != durable_head:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "target_sync_conflict",
                    "message": "The selected target does not share the authority realm head.",
                    "realm_id": realm_id,
                    "authority_head": durable_head,
                    "target_head": target_observation["head"],
                    "target_instance_id": target_instance_id,
                    "recoverable": True,
                    "recovery_url": f"/fleet?section=sync&realm={quote(realm_id)}",
                },
            )
        degraded = [
            item
            for item in observations
            if item["url"] != target_url
            and (item["status"] != "reachable" or item.get("head") != durable_head)
        ]
        return {
            "code": "unrelated_peers_degraded" if degraded else "scoped_sync_healthy",
            "realm_id": realm_id,
            "authority_head": durable_head,
            "projection_head": projection_head,
            "target_instance_id": target_instance_id,
            "target_head": target_observation["head"],
            "degraded_peers": degraded,
            "safe_scoped_dispatch": True,
            "checked_at": datetime.now(UTC).isoformat(),
        }

    if not settings.peers:
        return
    engine = request.app.state.ctx.services.get("sync_engine")
    if engine:
        state = await engine.converge_realm(realm_id)
        if state.get("phase") == "converged":
            return
        instances = state.get("instances", [])
        heads = {
            item.get("name") or item.get("instance_id"): item.get("head")
            for item in instances
        }
        unavailable = [
            item.get("name") or item.get("instance_id")
            for item in instances
            if item.get("status")
            in {"unavailable", "invalid_response", "error", "missing_head"}
        ]
        code = "sync_unavailable" if unavailable else "sync_conflict"
        raise HTTPException(
            status_code=409,
            detail={
                "code": code,
                "message": (
                    "Dispatch blocked because one or more fleet instances are unavailable"
                    if unavailable
                    else "Dispatch blocked until realm heads converge"
                ),
                "realm_id": realm_id,
                "heads": heads,
                "unavailable": unavailable,
                "conflicts": state.get("conflicts", []),
                "recoverable": True,
                "recovery_url": f"/fleet?section=sync&realm={quote(realm_id)}",
                "retry_after_convergence": True,
            },
        )
    log = request.app.state.ctx.require_service("event_log")
    local_head = await _offload_request(
        request, "filesystem.sync_head_read", log.get_head, realm_id
    )
    heads: dict[str, str | None] = {settings.instance_id: local_head}
    headers = _peer_headers(request)

    async def read_peer_head(client: httpx.AsyncClient, peer_url: str):
        try:
            response = await _fleet_http(
                request,
                "http.fleet_sync_head",
                client.get(
                    f"{peer_url.rstrip('/')}/api/sync/refs",
                    params={"realm": realm_id},
                    headers=headers,
                    timeout=5.0,
                ),
                timeout=5.0,
            )
            response.raise_for_status()
            refs = await _response_json(request, response)
            head = next(
                (r.get("head_hash") for r in refs if r.get("realm_id") == realm_id),
                None,
            )
            return peer_url, head
        except httpx.HTTPError, TimeoutError, ValueError:
            return peer_url, None

    async with _borrow_fleet_client(request, timeout=5.0) as client:
        results = await asyncio.gather(
            *(read_peer_head(client, peer_url) for peer_url in settings.peers)
        )
    heads.update(results)
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
                "recovery_url": f"/fleet?section=sync&realm={quote(realm_id)}",
                "retry_after_convergence": True,
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
                "recoverable": True,
                "recovery_url": f"/fleet?section=sync&realm={quote(realm_id)}",
                "retry_after_convergence": True,
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


def _dispatch_request(app) -> Request:
    return Request({"type": "http", "app": app, "headers": []})


async def _dispatch_cancelled(
    ctx: AppContext, ledger: DispatchStore, record: DispatchRecord
) -> bool:
    def check_and_transition() -> bool:
        current = ledger.get(record.dispatch_id)
        if not current or not current.cancel_requested:
            return False
        ledger.transition(
            current,
            "cancelled",
            "Dispatch cancelled before prompt acceptance.",
        )
        return True

    return await _offload_ctx(ctx, "dispatch.cancel_check", check_and_transition)


async def _process_remote_dispatch(app, record: DispatchRecord) -> None:
    """Advance one persisted dispatch through independently auditable stages."""
    request = _dispatch_request(app)
    ctx = app.state.ctx
    settings = ctx.settings
    ledger: DispatchStore = ctx.require_service("dispatch_store")
    store = ctx.store

    if await _dispatch_cancelled(ctx, ledger, record):
        return
    await _offload_ctx(
        ctx,
        "dispatch.record_write",
        ledger.transition,
        record,
        "checking_sync",
        "Checking realm convergence.",
    )
    card = None
    if record.card_id:
        sync_evidence = await _assert_dispatch_sync_health(
            request, record.realm_id, record.target_instance_id
        )
        if isinstance(sync_evidence, dict):
            record.sync_evidence = sync_evidence
        card = await _offload_ctx(
            ctx,
            "sqlite.card_read",
            store.get_card,
            record.card_id,
            realm_id=record.realm_id,
        )
        if not card:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "card_changed_during_convergence",
                    "message": "The card disappeared while realm sync converged; review and retry.",
                    "recoverable": True,
                    "recovery_url": f"/fleet?section=sync&realm={quote(record.realm_id)}",
                },
            )
        record.card_version = card.updated_at.isoformat()
        record.card_snapshot = card.model_dump(mode="json")
        record.project_id = record.project_id or card.project_id
        await _offload_ctx(ctx, "dispatch.record_write", ledger.put, record)
    if await _dispatch_cancelled(ctx, ledger, record):
        return

    await _offload_ctx(
        ctx,
        "dispatch.record_write",
        ledger.transition,
        record,
        "materializing",
        "Materializing the exact dispatch context on the target.",
    )
    materialize_payload = {
        "dispatch_id": record.dispatch_id,
        "mutation_id": record.mutation_id,
        "card": record.card_snapshot,
        "card_version": record.card_version,
        "realm_id": record.realm_id,
        "project_id": record.project_id,
        "principal_id": record.principal_id,
        "provenance_version": 1,
        "authority_instance_id": record.authority_instance_id,
        "authority_instance_name": record.authority_instance_name,
        "authority_url": record.authority_url,
        "target_instance_id": record.target_instance_id,
        "session_id": record.resume_session_id if record.resume_requested else None,
        "progress_versions": SUPPORTED_PROGRESS_VERSIONS,
        "attachment_manifest": [
            item.model_dump(mode="json") for item in (card.attachments if card else [])
        ],
        "attachment_digest": manifest_digest(card.attachments if card else []),
        "materialization_plan": record.materialization_plan,
    }
    manifest = [
        CardAttachment.model_validate(item)
        for item in materialize_payload["attachment_manifest"]
    ]
    materialized = await _peer_dispatch_json(
        request, record.target_instance_id, materialize_payload
    )
    if not materialized.get("resolvable"):
        await _transfer_missing_attachments(
            request,
            record.target_instance_id,
            record.dispatch_id,
            record.card_id or "",
            record.realm_id,
            manifest,
            list(materialized.get("missing") or []),
        )
        materialized = await _peer_dispatch_json(
            request, record.target_instance_id, materialize_payload
        )
    if record.card_id and (
        materialized.get("dispatch_id") != record.dispatch_id
        or materialized.get("card_id") != record.card_id
        or materialized.get("card_version") != record.card_version
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "target_materialization_rejected",
                "message": "The target did not acknowledge the exact authoritative dispatch snapshot.",
                "expected": {
                    "dispatch_id": record.dispatch_id,
                    "card_id": record.card_id,
                    "card_version": record.card_version,
                },
                "acknowledged": {
                    "dispatch_id": materialized.get("dispatch_id"),
                    "card_id": materialized.get("card_id"),
                    "card_version": materialized.get("card_version"),
                },
                "recoverable": True,
            },
        )
    if not materialized.get("resolvable") or (
        manifest and not (materialized.get("attachment_evidence") or {}).get("verified")
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "attachment_materialization_unverified",
                "message": "Required attachments did not verify; the agent was not started.",
                "recoverable": True,
            },
        )
    record.attachment_evidence = materialized.get("attachment_evidence") or {
        "digest": manifest_digest(manifest),
        "attachments": [],
        "verified": True,
    }
    negotiated_progress = materialized.get("progress_protocol_version")
    record.progress_protocol_version = (
        int(negotiated_progress)
        if negotiated_progress in SUPPORTED_PROGRESS_VERSIONS
        else None
    )
    await _offload_ctx(ctx, "dispatch.record_write", ledger.put, record)
    if await _dispatch_cancelled(ctx, ledger, record):
        return

    payload = dict(record.request_payload)
    await _offload_ctx(
        ctx,
        "dispatch.record_write",
        ledger.transition,
        record,
        "starting_session",
        "Allocating the remote execution session.",
    )
    session_body: dict[str, Any] = {
        # Every fresh dispatch gets an identity that cannot collide with an old
        # card label. Retried requests remain idempotent through dispatch_id.
        "label": (
            f"card:{record.card_id}:dispatch:{record.dispatch_id}"
            if record.card_id
            else f"dispatch:{record.dispatch_id}"
        ),
        "title": payload.get("title")
        or (card.title if card else "Remote agent session"),
        "cwd": payload.get("cwd") if not record.project_id else None,
        "card_id": record.card_id,
        "project_id": record.project_id,
        "provider": payload.get("provider"),
        "model_id": payload.get("model_id"),
        "mode_id": payload.get("mode_id"),
        "effort": payload.get("effort"),
        "config": payload.get("config") or {},
        "surface": "execution",
        "dispatch_id": record.dispatch_id,
        "resume": record.resume_requested,
        "resume_session_id": record.resume_session_id,
    }
    session_body = {
        key: value
        for key, value in session_body.items()
        if value not in (None, "", False)
    }
    snapshot = await _peer_agent_json(
        request,
        record.target_instance_id,
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
    requested_configuration = SessionConfigurationRequest.from_values(
        model_id=payload.get("model_id"),
        mode_id=payload.get("mode_id"),
        reasoning=payload.get("effort"),
        config=payload.get("config") or {},
    )
    confirmed_configuration: dict[str, Any] = {}
    if not requested_configuration.empty:
        configuration = snapshot.get("configuration")
        if not isinstance(configuration, dict):
            session_config = (
                session.get("config_json") if isinstance(session, dict) else {}
            )
            configuration = dict((session_config or {}).get("configuration") or {})
        confirmed_configuration = dict(configuration)
        effective = dict(configuration.get("effective") or {})
        mismatches: list[str] = []
        if configuration.get("state") != "ready":
            mismatches.append(f"state={configuration.get('state')!r}")
        if (
            requested_configuration.model_id
            and effective.get("model_id") != requested_configuration.model_id
        ):
            mismatches.append(
                f"model={effective.get('model_id')!r} (requested "
                f"{requested_configuration.model_id!r})"
            )
        if (
            requested_configuration.mode_id
            and effective.get("mode_id") != requested_configuration.mode_id
        ):
            mismatches.append(
                f"mode={effective.get('mode_id')!r} (requested "
                f"{requested_configuration.mode_id!r})"
            )
        if (
            requested_configuration.reasoning
            and effective.get("reasoning") != requested_configuration.reasoning
        ):
            mismatches.append(
                f"reasoning={effective.get('reasoning')!r} (requested "
                f"{requested_configuration.reasoning!r})"
            )
        effective_config = dict(effective.get("config") or {})
        for config_id, value in requested_configuration.config.items():
            if effective_config.get(config_id) != value:
                mismatches.append(
                    f"config {config_id}={effective_config.get(config_id)!r} "
                    f"(requested {value!r})"
                )
        if mismatches:
            raise HTTPException(
                status_code=502,
                detail={
                    "code": "remote_configuration_unconfirmed",
                    "message": "The remote agent did not confirm the requested session configuration; no prompt was delivered.",
                    "mismatches": mismatches,
                    "recoverable": True,
                },
            )
    if record.resume_requested and session_id != record.resume_session_id:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "resume_session_mismatch",
                "message": "The target did not resume the explicitly requested session.",
                "expected": record.resume_session_id,
                "actual": session_id,
                "recoverable": False,
            },
        )
    record.session_id = session_id
    await _offload_ctx(ctx, "dispatch.record_write", ledger.put, record)
    if await _dispatch_cancelled(ctx, ledger, record):
        return

    message = str(payload.get("message") or "").strip()
    if card and not message:
        from pa.prompts import PROMPTS

        message = PROMPTS.render(
            "dispatch.remote.default", provider=payload.get("provider") or "default"
        ).text
    prompt_result: dict[str, Any] | None = None
    if message:
        await _offload_ctx(
            ctx,
            "dispatch.record_write",
            ledger.transition,
            record,
            "delivering_prompt",
            "Delivering and awaiting durable prompt acceptance.",
        )
        delivered = await _peer_agent_json(
            request,
            record.target_instance_id,
            "POST",
            f"sessions/{session_id}/prompt",
            body={
                "message": message,
                "card_id": record.card_id,
                "project_id": record.project_id,
                "dispatch_id": record.dispatch_id,
            },
        )
        prompt_result = delivered if isinstance(delivered, dict) else None
        if not (
            prompt_result
            and prompt_result.get("accepted") is True
            and prompt_result.get("session_id") == session_id
            and prompt_result.get("dispatch_id") == record.dispatch_id
            and prompt_result.get("accepted_event")
            in {"queue_enqueued", "user_message"}
        ):
            raise HTTPException(
                status_code=502,
                detail={
                    "code": "prompt_ack_missing",
                    "message": "The target did not prove that the prompt was durably queued in the intended session.",
                    "recoverable": True,
                    "session_id": session_id,
                },
            )
        record.prompt_acknowledged_at = datetime.now(UTC)
        record.prompt_ack = prompt_result
        await _offload_ctx(ctx, "dispatch.record_write", ledger.put, record)
    elif card:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "prompt_missing",
                "message": "A card dispatch requires a non-empty initial prompt.",
                "recoverable": True,
            },
        )

    if card:
        current_card = await _offload_ctx(
            ctx,
            "sqlite.card_read",
            store.get_card,
            card.id,
            realm_id=record.realm_id,
        )
        if current_card and current_card.lane not in {CardLane.ACTIVE, CardLane.DONE}:
            await _offload_ctx(
                ctx,
                "sqlite.card_write",
                store.update_card,
                card.id,
                CardUpdate(
                    lane=CardLane.ACTIVE,
                    preferred_instance=record.target_instance_id,
                ),
                realm_id=record.realm_id,
                principal_id=record.principal_id,
                instance_id=settings.instance_id,
            )
    if not record.knowledge_recorded_at:
        await _offload_ctx(
            ctx,
            "sqlite.knowledge_write",
            store.add_knowledge,
            KnowledgeEntry(
                session_id=session_id,
                item_id=record.card_id,
                card_id=record.card_id,
                summary=(
                    f"Dispatched {card.title!r} to {record.target_instance_name or record.target_instance_id} in session {session_id}."
                    if card
                    else f"Started remote session {session_id} on {record.target_instance_name or record.target_instance_id}."
                ),
                source="remote_dispatch",
                tags=["remote-operations", f"instance:{record.target_instance_id}"],
            ),
        )
        record.knowledge_recorded_at = datetime.now(UTC)
        await _offload_ctx(ctx, "dispatch.record_write", ledger.put, record)
    if record.state != "completed":
        await _offload_ctx(
            ctx,
            "dispatch.record_write",
            ledger.transition,
            record,
            "running",
            "Prompt accepted by the intended remote session."
            if message
            else "Remote session started.",
            detail={
                "session_id": session_id,
                "prompt": prompt_result or {},
                "configuration": confirmed_configuration,
            },
        )


async def _placement_candidates(
    request: Request, instances: list[FleetInstance]
) -> list[PlacementCandidate]:
    ctx = request.app.state.ctx

    async def inspect(inst: FleetInstance) -> PlacementCandidate:
        reachability, activity, providers, repositories = await asyncio.gather(
            *(
                probe_dimension(ctx, inst, dimension, force=True)
                for dimension in (
                    "reachability",
                    "activity",
                    "providers",
                    "repositories",
                )
            )
        )
        return PlacementCandidate(
            instance_id=inst.instance_id,
            name=inst.name,
            zone=inst.zone,
            lifecycle_state=inst.lifecycle_state,
            local=inst.instance_id == ctx.settings.instance_id,
            capabilities=list(inst.capabilities),
            dispatch_capacity=inst.dispatch_capacity,
            dispatch_provider_capacities=dict(inst.dispatch_provider_capacities),
            reachability=reachability,
            activity=activity,
            providers=providers,
            repositories=repositories,
            authorized=True,
        )

    return list(await asyncio.gather(*(inspect(inst) for inst in instances)))


def _placement_materialization_plan(
    request: Request,
    body: FleetDispatchBody,
    *,
    card,
    project,
    project_id: str | None,
    target_instance_id: str,
):
    store = request.app.state.ctx.store
    project_repositories = (
        list(
            store.list_project_repositories(
                project_id,
                realm_id=(
                    card.realm_id
                    if card
                    else request.app.state.ctx.settings.primary_realm
                ),
            )
        )
        if project_id
        else []
    )
    requested_contract = (
        ExecutionContract.model_validate(body.execution_contract)
        if body.execution_contract
        else None
    )
    explicit_ids = [
        item.repository_id
        for item in (
            requested_contract.requirements.repositories if requested_contract else []
        )
    ]
    explicit_repositories = []
    realm_id = card.realm_id if card else request.app.state.ctx.settings.primary_realm
    for repository_id in explicit_ids:
        repository = store.get_repository(repository_id, realm_id)
        if repository:
            explicit_repositories.append(repository)
    plan = resolve_materialization_plan(
        requested=requested_contract,
        card=card,
        project=project,
        project_repositories=project_repositories,
        explicit_repositories=explicit_repositories,
        target_instance_id=target_instance_id,
    )
    repository_ids = [item.repository_id for item in plan.requirements.repositories]
    return plan, repository_ids


async def _resolve_policy_placement(
    request: Request,
    body: FleetDispatchBody,
    *,
    card,
    project,
    project_id: str | None,
) -> tuple[Any, Any]:
    ctx = request.app.state.ctx
    realm_id = card.realm_id if card else ctx.settings.primary_realm
    plan, repository_ids = _placement_materialization_plan(
        request,
        body,
        card=card,
        project=project,
        project_id=project_id,
        target_instance_id=body.target_instance_id or "placement-preview",
    )
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    instances = list(fleet.list_instances())
    candidates = await _placement_candidates(request, instances)
    policies = _policy_service(request)
    requested_group_id = body.group_id
    if body.target_instance_id and requested_group_id:
        raise PlacementError(
            "invalid_placement_group",
            "A named/manual dispatch cannot also specify a worker group.",
            recoverable=False,
        )
    try:
        group = policies.resolve_group(
            realm_id=realm_id,
            project_id=project_id,
            workload_profile=plan.profile.value,
            requested_group_id=(
                "all-active" if body.target_instance_id else requested_group_id
            ),
            candidates=candidates,
            local_instance_id=ctx.settings.instance_id,
        )
    except ValueError as exc:
        code, _separator, message = str(exc).partition(":")
        _group, _separator, message = message.partition(":")
        raise PlacementError(
            code or "configured_group_unavailable",
            message or str(exc),
            recoverable=True,
        ) from exc

    explicit_policies = ctx.store.list_instance_participation_policies(realm_id)
    policy_enforcement_active = bool(
        explicit_policies
        or ctx.store.list_instance_groups(realm_id, include_archived=True)
        or ctx.store.list_placement_defaults(realm_id)
    )
    for candidate in candidates:
        policy, explicit = policies.effective_policy(
            realm_id, candidate.instance_id
        )
        candidate.participation_policy = policy
        candidate.participation_policy_explicit = explicit
        candidate.group_membership = group.membership.get(
            candidate.instance_id, "not_in_requested_group"
        )
        candidate.group_id = group.resolved_group_id
        activity = (
            candidate.activity.get("value")
            if isinstance(candidate.activity, dict)
            else {}
        ) or {}
        schema_version = activity.get("participation_policy_schema_version")
        try:
            candidate.participation_policy_supported = (
                schema_version is None or int(schema_version) >= 1
            )
        except (TypeError, ValueError):
            candidate.participation_policy_supported = False
        candidate.self_protection = dict(
            activity.get("self_protective_participation") or {}
        )

    required_capabilities = sorted(
        set(body.required_capabilities)
        | set(card.preferred_capabilities if card else [])
        | set(plan.requirements.required_capabilities)
    )
    placement: PlacementService = ctx.require_service("placement_service")
    decision = await _offload_request(
        request,
        "fleet.placement_resolve",
        placement.resolve,
        PlacementRequest(
            realm_id=realm_id,
            fleet_id=ctx.settings.fleet_id,
            policy=body.placement_policy,
            instance_id=body.target_instance_id,
            card_id=body.card_id,
            provider=body.provider,
            model_id=body.model_id,
            required_capabilities=required_capabilities,
            repository_ids=repository_ids,
            workload_profile=plan.profile.value,
            project_id=project_id,
            dispatch_intent=(
                DispatchIntent.PRIVILEGED_OVERRIDE
                if body.participation_override
                else (
                    DispatchIntent.MANUAL
                    if body.target_instance_id
                    else DispatchIntent.AUTOMATIC
                )
            ),
            requested_group_id=body.group_id,
            resolved_group_id=group.resolved_group_id,
            resolved_group_name=group.resolved_group_name,
            group_version=group.group_version,
            default_source=(
                "privileged_named_override"
                if body.participation_override
                else (
                    "named_manual_dispatch"
                    if body.target_instance_id
                    else group.default_source
                )
            ),
            permitted_placement_policies=group.permitted_placement_policies,
            principal_id=get_principal_id(request),
            participation_override_reason=body.participation_override_reason,
            policy_enforcement_active=policy_enforcement_active,
            workspace_eligible=not (
                plan.missing_dependencies or plan.stale_dependencies
            ),
            workspace_reason=plan.summary,
            allow_concurrent=body.allow_concurrent,
            capacity_override=body.capacity_override,
        ),
        candidates,
    )
    decision.eligible_candidates = [
        {
            **item,
            "group_version": group.group_version,
        }
        for item in decision.eligible_candidates
    ]
    decision.rejected_candidates = [
        {
            **item,
            "group_version": group.group_version,
        }
        for item in decision.rejected_candidates
    ]
    return decision, plan


@router.post("/fleet/placement/preview")
async def preview_fleet_placement(
    request: Request, body: FleetDispatchBody
) -> dict[str, Any]:
    user = require_user(request)
    _validate_participation_override(user, body)
    ctx = request.app.state.ctx
    realm_id = ctx.settings.primary_realm
    card = (
        await _offload_request(
            request,
            "sqlite.card_read",
            ctx.store.get_card,
            body.card_id,
            realm_id=realm_id,
        )
        if body.card_id
        else None
    )
    if body.card_id and not card:
        raise HTTPException(status_code=404, detail="Card not found")
    project_id = body.project_id or (card.project_id if card else None)
    project = (
        await _offload_request(
            request,
            "sqlite.project_read",
            ctx.store.get_project,
            project_id,
            realm_id=realm_id,
        )
        if project_id
        else None
    )
    if project_id and not project:
        raise HTTPException(status_code=404, detail="Project not found")
    try:
        decision, plan = await _resolve_policy_placement(
            request,
            body,
            card=card,
            project=project,
            project_id=project_id,
        )
    except PlacementError as exc:
        raise _placement_http_error(exc) from exc
    return {
        "schema_version": 1,
        "previewed_at": datetime.now(UTC).isoformat(),
        "decision": decision.model_dump(mode="json"),
        "materialization_plan": plan.model_dump(mode="json"),
        "selected_instance_id": decision.chosen_instance_id,
        "selection_semantics": decision.tie_breaking_reason,
    }


@router.get("/fleet/instance-groups/{group_id}/preview")
async def preview_instance_group(
    request: Request,
    group_id: str,
    workload_profile: str = "research",
    project_id: str | None = None,
    policy: PlacementPolicy = PlacementPolicy.BEST_MATCH,
) -> dict[str, Any]:
    body = FleetDispatchBody(
        placement_policy=policy,
        group_id=group_id,
        project_id=project_id,
        execution_contract={
            "version": 1,
            "profile": workload_profile,
            "confirmed": True,
            "requirements": {},
        },
    )
    return await preview_fleet_placement(request, body)


def _capacity_admission_from_decision(
    decision: dict[str, Any] | None,
    *,
    provider: str | None,
    override: bool,
    override_reason: str | None,
) -> CapacityAdmission | None:
    if not decision:
        return None
    chosen_id = decision.get("chosen_instance_id")
    detail = next(
        (
            item
            for item in decision.get("eligible_candidates") or []
            if item.get("instance_id") == chosen_id
        ),
        None,
    )
    if not detail:
        return None
    capacity = detail.get("capacity_detail") or {}
    observed_at = (detail.get("freshness") or {}).get("activity")
    return CapacityAdmission(
        limit=int(detail.get("capacity") or capacity.get("limit")),
        source=str(capacity.get("source") or "unknown"),
        provider=provider.strip().lower() if provider else None,
        provider_specific=capacity.get("provider_limit") is not None,
        observed_active=int(detail.get("active") or 0),
        observed_queued=int(detail.get("queued") or 0),
        observed_reservations=int(detail.get("reserved") or 0),
        observed_at=observed_at or datetime.now(UTC),
        consumer_links=list(detail.get("consumer_links") or []),
        override=override,
        override_reason=override_reason,
    )


def _placement_http_error(exc: PlacementError) -> HTTPException:
    status = 404 if exc.code == "instance_not_found" else 409
    return HTTPException(
        status_code=status,
        detail={
            "code": exc.code,
            "message": exc.message,
            "recoverable": exc.recoverable,
            "rejected_candidates": exc.rejected_candidates,
            "recovery_url": "/fleet?section=overview",
        },
    )


def _validate_participation_override(user, body: RemoteAgentStartBody) -> None:
    if not body.participation_override:
        return
    if getattr(user, "role", None) != "admin":
        raise HTTPException(
            status_code=403,
            detail={
                "code": "participation_override_forbidden",
                "message": "Only an administrator may perform a participation-policy override.",
                "recoverable": False,
            },
        )
    if not (body.participation_override_reason or "").strip():
        raise HTTPException(
            status_code=422,
            detail={
                "code": "participation_override_reason_required",
                "message": "Record an operator reason for the privileged override.",
                "recoverable": True,
            },
        )
    if isinstance(body, FleetDispatchBody) and not body.target_instance_id:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "participation_override_requires_named_target",
                "message": "A privileged participation override requires a concrete named instance.",
                "recoverable": False,
            },
        )


@router.post("/fleet/dispatch", status_code=202)
async def dispatch_fleet_work(request: Request, body: FleetDispatchBody) -> dict:
    """Resolve a concrete or policy target, then durably admit exactly once."""
    user = require_user(request)
    _validate_participation_override(user, body)
    ctx = request.app.state.ctx
    settings = ctx.settings
    if body.capacity_override:
        if getattr(user, "role", None) != "admin":
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "capacity_override_forbidden",
                    "message": "Only an administrator may override fleet capacity.",
                    "recoverable": False,
                },
            )
        if not (body.capacity_override_reason or "").strip():
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "capacity_override_reason_required",
                    "message": "Record an operator reason for the capacity override.",
                    "recoverable": True,
                },
            )
    selected_authority = body.authority_instance_id or settings.instance_id
    if selected_authority != settings.instance_id:
        if getattr(request.state, "instance_authenticated", False) is True:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "wrong_authority",
                    "message": "The selected authority did not receive the routed request.",
                },
            )
        forwarded = body.model_dump(mode="json")
        forwarded["authority_instance_id"] = selected_authority
        return await _peer_authority_json(
            request, selected_authority, "POST", "dispatch", body=forwarded
        )
    if bool(body.target_instance_id) == bool(body.placement_policy):
        raise _placement_http_error(
            PlacementError(
                "invalid_placement_target",
                "Choose exactly one named/local instance or placement policy.",
                recoverable=False,
            )
        )

    header_key = request.headers.get("idempotency-key")
    if not isinstance(header_key, str):
        header_key = None
    idempotency_key = (header_key or body.idempotency_key or str(uuid4())).strip()
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key cannot be empty")
    placement_payload = body.model_dump(
        mode="json", exclude={"authority_instance_id", "idempotency_key"}
    )
    placement_fingerprint = hashlib.sha256(
        json.dumps(placement_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    ledger = _dispatch_store(request)
    existing = await _offload_request(
        request,
        "dispatch.idempotency_read",
        ledger.by_authority_idempotency,
        settings.instance_id,
        idempotency_key,
    )
    if existing:
        if existing.placement_request_fingerprint != placement_fingerprint:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "idempotency_conflict",
                    "message": "This idempotency key was already used for a different fleet dispatch request.",
                    "dispatch_id": existing.dispatch_id,
                },
            )
        return {
            "accepted": True,
            "duplicate": True,
            "dispatch_id": existing.dispatch_id,
            "job_id": existing.dispatch_id,
            "dispatch": existing.public_dict(),
        }

    store = ctx.store
    realm_id = settings.primary_realm
    card = (
        await _offload_request(
            request,
            "sqlite.card_read",
            store.get_card,
            body.card_id,
            realm_id=realm_id,
        )
        if body.card_id
        else None
    )
    if body.card_id and not card:
        raise HTTPException(status_code=404, detail="Card not found")
    project_id = body.project_id or (card.project_id if card else None)
    project = (
        await _offload_request(
            request,
            "sqlite.project_read",
            store.get_project,
            project_id,
            realm_id=realm_id,
        )
        if project_id
        else None
    )
    if project_id and not project:
        raise HTTPException(status_code=404, detail="Project not found")
    principal_id = get_principal_id(request)
    if project and project.memberships:
        authorized = any(
            membership.principal_id == principal_id
            for membership in project.memberships
        )
        if not authorized and getattr(user, "role", None) != "admin":
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "insufficient_authorization",
                    "message": "This principal is not authorized to dispatch work for the linked project.",
                    "recoverable": False,
                },
            )

    try:
        decision, _plan = await _resolve_policy_placement(
            request,
            body,
            card=card,
            project=project,
            project_id=project_id,
        )
    except PlacementError as exc:
        logger.warning(
            "fleet placement rejected code=%s candidates=%s",
            exc.code,
            exc.rejected_candidates,
        )
        raise _placement_http_error(exc) from exc

    start_payload = body.model_dump(
        mode="json",
        exclude={
            "target_instance_id",
            "placement_policy",
            "group_id",
            "required_capabilities",
        },
    )
    start_payload["authority_instance_id"] = settings.instance_id
    start_payload["idempotency_key"] = idempotency_key
    return await _admit_remote_agent_work(
        request,
        decision.chosen_instance_id,
        RemoteAgentStartBody.model_validate(start_payload),
        placement_decision=decision.model_dump(mode="json"),
        placement_request_fingerprint=placement_fingerprint,
        idempotency_scope="authority",
    )


@router.post("/fleet/instances/{instance_id}/agent/start", status_code=202)
async def start_remote_agent_work(
    request: Request,
    instance_id: str,
    body: RemoteAgentStartBody,
) -> dict:
    """Apply named placement checks, then durably admit remote work."""
    user = require_user(request)
    _validate_participation_override(user, body)
    if body.capacity_override:
        if getattr(user, "role", None) != "admin":
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "capacity_override_forbidden",
                    "message": "Only an administrator may override fleet capacity.",
                    "recoverable": False,
                },
            )
        if not (body.capacity_override_reason or "").strip():
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "capacity_override_reason_required",
                    "message": "Record an operator reason for the capacity override.",
                    "recoverable": True,
                },
            )
    ctx = request.app.state.ctx
    selected_authority = body.authority_instance_id or ctx.settings.instance_id
    if (
        selected_authority != ctx.settings.instance_id
        or "placement_service" not in ctx.services
    ):
        # Forwarding must happen at the selected authority. The service fallback
        # preserves direct internal callers that intentionally construct a
        # minimal context; every booted HTTP/MCP surface registers placement.
        return await _admit_remote_agent_work(request, instance_id, body)
    realm_id = ctx.settings.primary_realm
    card = (
        await _offload_request(
            request,
            "sqlite.card_read",
            ctx.store.get_card,
            body.card_id,
            realm_id=realm_id,
        )
        if body.card_id
        else None
    )
    if body.card_id and not card:
        raise HTTPException(status_code=404, detail="Card not found")
    project_id = body.project_id or (card.project_id if card else None)
    project = (
        await _offload_request(
            request,
            "sqlite.project_read",
            ctx.store.get_project,
            project_id,
            realm_id=realm_id,
        )
        if project_id
        else None
    )
    placement_body = FleetDispatchBody(
        **body.model_dump(mode="python"),
        target_instance_id=instance_id,
    )
    try:
        decision, _plan = await _resolve_policy_placement(
            request,
            placement_body,
            card=card,
            project=project,
            project_id=project_id,
        )
    except PlacementError as exc:
        logger.warning(
            "fleet named placement rejected target=%s code=%s candidates=%s",
            instance_id,
            exc.code,
            exc.rejected_candidates,
        )
        raise _placement_http_error(exc) from exc
    return await _admit_remote_agent_work(
        request,
        instance_id,
        body,
        placement_decision=decision.model_dump(mode="json"),
    )


async def _admit_remote_agent_work(
    request: Request,
    instance_id: str,
    body: RemoteAgentStartBody,
    *,
    placement_decision: dict[str, Any] | None = None,
    placement_request_fingerprint: str | None = None,
    idempotency_scope: str = "target",
) -> dict:
    require_user(request)
    ctx = request.app.state.ctx
    settings = ctx.settings
    selected_authority = body.authority_instance_id or settings.instance_id
    if selected_authority != settings.instance_id:
        if getattr(request.state, "instance_authenticated", False) is True:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "wrong_authority",
                    "message": "The selected authority did not receive the routed request.",
                },
            )
        forwarded = body.model_dump(mode="json")
        forwarded["authority_instance_id"] = selected_authority
        return await _peer_authority_json(
            request,
            selected_authority,
            "POST",
            f"instances/{instance_id}/agent/start",
            body=forwarded,
        )
    store = ctx.store
    realm_id = settings.primary_realm
    card = (
        await _offload_request(
            request,
            "sqlite.card_read",
            store.get_card,
            body.card_id,
            realm_id=realm_id,
        )
        if body.card_id
        else None
    )
    if body.card_id and not card:
        raise HTTPException(status_code=404, detail="Card not found")
    project_id = body.project_id or (card.project_id if card else None)
    project = (
        await _offload_request(
            request,
            "sqlite.project_read",
            store.get_project,
            project_id,
            realm_id=realm_id,
        )
        if project_id
        else None
    )
    if project_id and not project:
        raise HTTPException(status_code=404, detail="Project not found")
    inst = _fleet_instance_or_404(request, instance_id)
    authority_url = settings.instance_url
    if instance_id != settings.instance_id and (
        not authority_url
        or authority_url.startswith(("http://127.", "http://localhost"))
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "authority_unroutable",
                "message": "Configure a fleet-reachable instance_url before remote dispatch.",
                "recoverable": True,
            },
        )
    payload = body.model_dump(
        mode="json",
        exclude={
            "authority_instance_id",
            "idempotency_key",
            "resume_session_id",
            "allow_concurrent",
        },
    )
    payload["project_id"] = project_id
    fingerprint = (
        placement_request_fingerprint
        or hashlib.sha256(
            json.dumps(
                {"target_instance_id": instance_id, "payload": payload},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
    )
    header_key = request.headers.get("idempotency-key")
    if not isinstance(header_key, str):
        header_key = None
    idempotency_key = (header_key or body.idempotency_key or str(uuid4())).strip()
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key cannot be empty")
    ledger = _dispatch_store(request)
    existing_lookup = (
        ledger.by_authority_idempotency
        if idempotency_scope == "authority"
        else ledger.by_idempotency
    )
    existing_key = (
        settings.instance_id if idempotency_scope == "authority" else instance_id
    )
    existing = await _offload_request(
        request,
        "dispatch.idempotency_read",
        existing_lookup,
        existing_key,
        idempotency_key,
    )
    if existing:
        if existing.request_fingerprint != fingerprint:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "idempotency_conflict",
                    "message": "This idempotency key was already used for different remote work.",
                    "dispatch_id": existing.dispatch_id,
                },
            )
        return {
            "accepted": True,
            "duplicate": True,
            "dispatch_id": existing.dispatch_id,
            "job_id": existing.dispatch_id,
            "dispatch": existing.public_dict(),
        }

    project_repositories = (
        list(store.list_project_repositories(project_id, realm_id=realm_id))
        if project_id
        else []
    )
    requested_contract = (
        ExecutionContract.model_validate(body.execution_contract)
        if body.execution_contract
        else None
    )
    explicit_ids = [
        item.repository_id
        for item in (
            requested_contract.requirements.repositories if requested_contract else []
        )
    ]
    explicit_repositories = []
    for repository_id in explicit_ids:
        repository = store.get_repository(repository_id, realm_id)
        if repository:
            explicit_repositories.append(repository)
    plan = resolve_materialization_plan(
        requested=requested_contract,
        card=card,
        project=project,
        project_repositories=project_repositories,
        explicit_repositories=explicit_repositories,
        target_instance_id=instance_id,
    )
    if not plan.admissible:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "materialization_preflight_required",
                "message": plan.summary,
                "plan": plan.model_dump(mode="json"),
                "recoverable": True,
            },
        )

    record = DispatchRecord(
        mutation_id=str(uuid4()),
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint,
        placement_request_fingerprint=placement_request_fingerprint,
        card_id=card.id if card else None,
        project_id=project_id,
        realm_id=realm_id,
        card_version=card.updated_at.isoformat() if card else None,
        card_snapshot=card.model_dump(mode="json") if card else None,
        materialization_plan=plan.model_dump(mode="json"),
        request_payload=payload,
        principal_id=(
            f"instance:{request.headers.get('X-PA-Origin-Instance-ID', 'fleet')}"
            if getattr(request.state, "instance_authenticated", False) is True
            else get_principal_id(request)
        ),
        authority_instance_id=settings.instance_id,
        authority_instance_name=settings.instance_name,
        authority_url=authority_url,
        target_instance_id=instance_id,
        target_instance_name=inst.name,
        placement_policy=str(
            (placement_decision or {}).get("policy") or "named_instance"
        ),
        placement_decision=placement_decision
        or {
            "policy": "named_instance",
            "chosen_instance_id": instance_id,
            "chosen_instance_name": inst.name,
            "tie_breaking_reason": "The concrete API target was requested directly.",
        },
        placement_resolved_at=datetime.now(UTC),
        allow_concurrent=body.allow_concurrent,
        resume_requested=bool(body.resume_session_id),
        resume_session_id=body.resume_session_id,
    )
    try:
        record, duplicate = await _offload_request(
            request,
            "dispatch.record_write",
            ledger.admit,
            record,
            idempotency_scope=idempotency_scope,
            capacity=_capacity_admission_from_decision(
                placement_decision,
                provider=body.provider,
                override=body.capacity_override,
                override_reason=body.capacity_override_reason,
            ),
        )
    except DispatchIdempotencyConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "idempotency_conflict",
                "message": "This idempotency key was already used for different remote work.",
                "dispatch_id": exc.existing.dispatch_id,
            },
        ) from exc
    except ConcurrentCardDispatch as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "card_dispatch_in_progress",
                "message": "This card already has an active durable dispatch. Open it or explicitly allow concurrent dispatch.",
                "dispatch_id": exc.existing.dispatch_id,
                "state": exc.existing.state,
                "recoverable": True,
            },
        ) from exc
    except DispatchCapacityExhausted as exc:
        logger.warning(
            "fleet capacity admission rejected target=%s provider=%s detail=%s",
            instance_id,
            body.provider,
            exc.detail,
        )
        raise HTTPException(status_code=409, detail=exc.detail) from exc
    if duplicate:
        return {
            "accepted": True,
            "duplicate": True,
            "dispatch_id": record.dispatch_id,
            "job_id": record.dispatch_id,
            "dispatch": record.public_dict(),
        }
    worker = ctx.services.get("dispatch_worker")
    if worker:
        worker.wake()
    return {
        "accepted": True,
        "duplicate": False,
        "dispatch_id": record.dispatch_id,
        "job_id": record.dispatch_id,
        "dispatch": record.public_dict(),
    }


@router.get("/fleet/dispatch-jobs")
def list_dispatches(
    request: Request,
    target_instance_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    require_user(request)
    limit = max(1, min(limit, 500))
    return [
        record.public_dict()
        for record in _dispatch_store(request).list(
            target_instance_id=target_instance_id, limit=limit
        )
    ]


@router.get("/fleet/dispatch-jobs/{dispatch_id}")
def get_dispatch(request: Request, dispatch_id: str) -> dict[str, Any]:
    require_user(request)
    record = _dispatch_store(request).get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    return record.public_dict()


@router.get("/fleet/post-turn/action-catalog")
def get_post_turn_action_catalog(request: Request) -> dict[str, Any]:
    require_user(request)
    catalog = action_catalog()
    settings = request.app.state.ctx.settings
    catalog["budgets"] = {
        "maximum_evaluator_attempts": settings.post_turn_evaluator_max_attempts,
        "maximum_automatic_followup_turns": (
            settings.post_turn_max_automatic_followups
        ),
        "evaluation_timeout_seconds": (
            settings.post_turn_evaluation_timeout_seconds
        ),
        "retry_seconds": settings.post_turn_retry_seconds,
        "escalation_threshold": settings.post_turn_escalation_threshold,
    }
    return catalog


@router.get("/fleet/dispatch-jobs/{dispatch_id}/turn-end")
def get_dispatch_turn_end(request: Request, dispatch_id: str) -> dict[str, Any]:
    require_user(request)
    record = _dispatch_store(request).get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    settings = request.app.state.ctx.settings
    return {
        "dispatch_id": dispatch_id,
        "snapshots": [
            item.model_dump(mode="json") for item in record.turn_end_snapshots
        ],
        "evaluations": [
            item.model_dump(mode="json") for item in record.post_turn_evaluations
        ],
        "lifecycle_diagnostics": record.public_dict()["lifecycle_diagnostics"],
        "budgets": {
            "maximum_evaluator_attempts": settings.post_turn_evaluator_max_attempts,
            "maximum_automatic_followup_turns": (
                settings.post_turn_max_automatic_followups
            ),
            "evaluation_timeout_seconds": (
                settings.post_turn_evaluation_timeout_seconds
            ),
            "retry_seconds": settings.post_turn_retry_seconds,
            "escalation_threshold": settings.post_turn_escalation_threshold,
        },
    }


@router.post("/fleet/dispatch-jobs/{dispatch_id}/evaluations")
def submit_post_turn_evaluation(
    request: Request,
    dispatch_id: str,
    body: PostTurnEvaluationV1,
) -> dict[str, Any]:
    """Validate and record a read-only evaluator result without executing writes."""
    _require_instance(request)
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if not record or not record.turn_end_snapshots:
        raise HTTPException(
            status_code=409,
            detail={"code": "turn_end_snapshot_required", "recoverable": True},
        )
    snapshot = record.turn_end_snapshots[-1]
    expected_digest = record.post_turn_context_digests.get(snapshot.snapshot_id)
    if not expected_digest:
        raise HTTPException(
            status_code=409,
            detail={"code": "evaluation_context_missing", "recoverable": True},
        )
    try:
        validated = PostTurnEvaluator.validate_result(
            body,
            expected_context_digest=expected_digest,
            expected_authority_version=snapshot.authority_version,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "stale_evaluation", "message": str(exc)},
        ) from exc
    if any(
        item.evaluation_id == validated.evaluation_id
        for item in record.post_turn_evaluations
    ):
        return {"accepted": True, "duplicate": True}
    mark_record_only_actions(validated)
    record.post_turn_evaluations.append(validated)
    ledger.put(record)
    return {"accepted": True, "duplicate": False}


@router.post(
    "/fleet/dispatch-jobs/{dispatch_id}/actions/{action_id}", status_code=202
)
async def execute_post_turn_action(
    request: Request,
    dispatch_id: str,
    action_id: str,
    body: FollowupActionExecutionBody,
) -> dict[str, Any]:
    """Validate, authorize, and execute one catalog action idempotently."""
    require_user(request)
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    evaluation = next(
        (
            item
            for item in record.post_turn_evaluations
            if item.evaluation_id == body.evaluation_id
        ),
        None,
    )
    if not evaluation:
        raise HTTPException(status_code=404, detail="Evaluation not found")
    action = next(
        (item for item in evaluation.recommended_actions if item.action_id == action_id),
        None,
    )
    if not action:
        raise HTTPException(status_code=404, detail="Follow-up action not found")
    if body.expected_authority_version != evaluation.observed_authority_version:
        raise HTTPException(
            status_code=409,
            detail={"code": "stale_authority_version", "recoverable": True},
        )
    prior = next(
        (
            item
            for item in action.audit
            if item.get("idempotency_key") == body.idempotency_key
        ),
        None,
    )
    if prior:
        return {
            "accepted": True,
            "duplicate": True,
            "status": action.status.value,
            "result": prior.get("result"),
        }
    if action.status in {
        FollowupActionStatus.REJECTED,
        FollowupActionStatus.SUPERSEDED,
    }:
        raise HTTPException(
            status_code=409,
            detail={"code": "action_not_executable", "status": action.status.value},
        )
    if action.human_approval_required and not body.approve:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "operator_approval_required",
                "action": action.name.value,
            },
        )
    action.status = (
        FollowupActionStatus.APPROVED
        if action.human_approval_required
        else action.status
    )
    result: Any = None
    try:
        if action.name in {
            FollowupActionName.NO_ACTION,
            FollowupActionName.RECORD_TURN_OUTCOME,
        }:
            result = {"recorded": True}
        elif action.name == FollowupActionName.MOVE_CARD:
            card = request.app.state.ctx.store.get_card(
                record.card_id, realm_id=record.realm_id
            )
            expected = action.parameters["expected_card_version"]
            if not card or card.updated_at.isoformat() != expected:
                raise ValueError("card authority version changed before move_card")
            updated = request.app.state.ctx.store.update_card(
                card.id,
                CardUpdate(lane=CardLane(action.parameters["lane"])),
                realm_id=record.realm_id,
                principal_id=get_principal_id(request),
                instance_id=request.app.state.ctx.settings.instance_id,
            )
            result = {"card": _model_json(updated)}
        elif action.name == FollowupActionName.PROMPT_SAME_SESSION:
            result = await prompt_dispatch_session(
                request,
                dispatch_id,
                DispatchFollowupBody(
                    message=action.parameters["prompt"],
                    idempotency_key=body.idempotency_key,
                ),
            )
        elif action.name == FollowupActionName.RETRY_DISPATCH:
            result = await _retry_dispatch_api(
                request,
                dispatch_id,
                DispatchControlBody(idempotency_key=body.idempotency_key),
            )
        elif action.name == FollowupActionName.REDISPATCH_CARD:
            provider = action.parameters.get("provider")
            result = await start_remote_agent_work(
                request,
                action.parameters["instance_id"],
                RemoteAgentStartBody(
                    authority_instance_id=record.authority_instance_id,
                    card_id=record.card_id,
                    project_id=record.project_id,
                    provider=None if provider == "default" else provider,
                    mode_id=action.parameters.get("mode"),
                    message=action.parameters["reason"],
                    idempotency_key=body.idempotency_key,
                ),
            )
        elif action.name in {
            FollowupActionName.CREATE_FOLLOWUP_CARD,
            FollowupActionName.RECORD_BUG_OR_FAILURE,
        }:
            dedupe_tag = f"pa-followup:{action.parameters['deduplication_key']}"
            existing = next(
                (
                    card
                    for card in request.app.state.ctx.store.list_cards(
                        realm_id=record.realm_id
                    )
                    if dedupe_tag in card.tags
                ),
                None,
            )
            created = existing or request.app.state.ctx.store.create_card(
                CardCreate(
                    realm_id=record.realm_id,
                    kind=(
                        CardKind.CONCERN
                        if action.name == FollowupActionName.RECORD_BUG_OR_FAILURE
                        else CardKind.TASK
                    ),
                    title=action.parameters["title"],
                    body=action.parameters["body"],
                    lane=CardLane.INBOX,
                    parent_id=action.parameters["parent_card_id"],
                    project_id=action.parameters["project_id"],
                    tags=[dedupe_tag],
                ),
                principal_id=get_principal_id(request),
                instance_id=request.app.state.ctx.settings.instance_id,
            )
            result = {"card": _model_json(created), "duplicate": existing is not None}
        else:
            # Wait, input, refresh, and escalation are intentional record-only
            # state. A separate condition scheduler may later supersede them.
            result = {"recorded": True, "condition": action.parameters}
        action.status = FollowupActionStatus.EXECUTED
        action.executed_at = datetime.now(UTC)
        action.status_reason = "Validated and deterministically executed by PA."
    except Exception as exc:
        action.status = FollowupActionStatus.FAILED
        action.status_reason = sanitize_text(exc, limit=1_000)
        action.audit.append(
            {
                "event": "failed",
                "at": datetime.now(UTC).isoformat(),
                "idempotency_key": body.idempotency_key,
                "error": action.status_reason,
            }
        )
        ledger.put(record)
        raise HTTPException(
            status_code=409,
            detail={
                "code": "followup_action_failed",
                "message": action.status_reason,
            },
        ) from exc
    action.audit.append(
        {
            "event": "executed",
            "at": action.executed_at.isoformat() if action.executed_at else None,
            "executor": "pa.post-turn",
            "idempotency_key": body.idempotency_key,
            "result": result,
        }
    )
    ledger.put(record)
    return {
        "accepted": True,
        "duplicate": False,
        "status": action.status.value,
        "result": result,
    }


@router.post("/fleet/dispatch-jobs/{dispatch_id}/repair-terminal")
def repair_terminal_dispatch(
    request: Request,
    dispatch_id: str,
    body: DispatchControlBody | None = None,
) -> dict[str, Any]:
    """Audited normalization for acknowledged legacy target records."""
    require_user(request)
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    key = _dispatch_control_key(request, body)
    if _repeat_dispatch_control(record, "repair_terminal", key):
        return record.public_dict()
    acknowledged = bool(
        record.acknowledged_at
        or record.completion_delivery_class == "acknowledged"
    )
    if not acknowledged:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "completion_not_acknowledged",
                "message": "Terminal repair requires durable completion acknowledgement.",
            },
        )
    previous = record.state
    record.control_operations[key] = "repair_terminal"
    record.completion_delivery_class = "acknowledged"
    record.completion_next_retry_at = None
    record.last_error = None
    record.lifecycle_inconsistencies.append(
        {
            "kind": "legacy_terminal_record_repaired",
            "previous_state": previous,
            "normalized_state": "completed",
            "observed_at": datetime.now(UTC).isoformat(),
            "idempotency_key": key,
        }
    )
    ledger.transition(
        record,
        "completed",
        "Acknowledged legacy completion normalized to terminal state.",
        detail={"previous_state": previous, "repair": True},
    )
    return record.public_dict()


def _require_dispatch_access(request: Request, record: DispatchRecord) -> None:
    if getattr(request.state, "instance_authenticated", False) is True:
        return
    user = require_user(request)
    if (
        request.app.state.ctx.settings.auth_required
        and record.principal_id != get_principal_id(request)
        and getattr(user, "role", None) != "admin"
    ):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "insufficient_authorization",
                "message": "This principal does not own the dispatch-linked session.",
            },
        )


@router.post("/fleet/dispatch-jobs/{dispatch_id}/prompt")
async def prompt_dispatch_session(
    request: Request, dispatch_id: str, body: DispatchFollowupBody
) -> dict[str, Any]:
    record = _dispatch_store(request).get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    _require_dispatch_access(request, record)
    key = body.idempotency_key.strip()
    if not key:
        raise HTTPException(status_code=400, detail="Idempotency-Key cannot be empty")
    if not record.session_id:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "linked_session_missing",
                "message": "The dispatch has not durably linked a target session yet.",
                "recoverable": True,
            },
        )
    if record.state in {"failed", "cancelled"}:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "linked_session_unavailable",
                "message": f"Dispatch in {record.state} cannot accept follow-up work.",
                "recoverable": record.recoverable,
            },
        )
    fingerprint = hashlib.sha256(
        json.dumps(
            {"message": body.message, "action": body.action},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    prior = record.followup_operations.get(key)
    if prior:
        if prior.get("fingerprint") != fingerprint:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "idempotency_conflict",
                    "message": "This follow-up key was used for a different prompt.",
                },
            )
        return {**dict(prior.get("response") or {}), "duplicate": True}
    result = await _peer_agent_json(
        request,
        record.target_instance_id,
        "POST",
        f"sessions/{record.session_id}/prompt",
        body={
            "message": body.message,
            "action": body.action,
            "card_id": record.card_id,
            "project_id": record.project_id,
            "dispatch_id": record.dispatch_id,
            "idempotency_key": key,
        },
    )
    if not isinstance(result, dict) or not result.get("accepted"):
        raise HTTPException(
            status_code=502,
            detail={
                "code": "followup_not_acknowledged",
                "message": "The target did not durably acknowledge the follow-up prompt.",
                "recoverable": True,
            },
        )
    public = {
        key: result.get(key)
        for key in (
            "stop_reason",
            "queued",
            "started",
            "accepted",
            "accepted_event",
            "prompt_id",
            "dispatch_id",
            "session_id",
            "duplicate",
        )
    }
    public["authority_instance_id"] = record.authority_instance_id
    record.followup_operations[key] = {
        "fingerprint": fingerprint,
        "response": public,
    }
    await _offload_request(
        request,
        "dispatch.followup_ack",
        _dispatch_store(request).transition,
        record,
        record.state,
        "Linked session follow-up durably acknowledged.",
        detail={
            "session_id": record.session_id,
            "prompt_id": result.get("prompt_id"),
        },
    )
    return public


def _dispatch_control_key(request: Request, body: DispatchControlBody | None) -> str:
    header_key = request.headers.get("idempotency-key")
    if not isinstance(header_key, str):
        header_key = None
    key = (
        header_key or (body.idempotency_key if body else None) or str(uuid4())
    ).strip()
    if not key:
        raise HTTPException(status_code=400, detail="Idempotency-Key cannot be empty")
    return key


def _repeat_dispatch_control(
    record: DispatchRecord, operation: str, idempotency_key: str
) -> bool:
    previous = record.control_operations.get(idempotency_key)
    if previous is None:
        return False
    if previous != operation:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "idempotency_conflict",
                "message": "This idempotency key was already used for a different dispatch operation.",
                "dispatch_id": record.dispatch_id,
            },
        )
    return True


@router.post("/fleet/dispatch-jobs/{dispatch_id}/retry", status_code=202)
async def _retry_dispatch_api(
    request: Request,
    dispatch_id: str,
    body: DispatchControlBody | None = None,
) -> dict[str, Any]:
    require_user(request)
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    idempotency_key = _dispatch_control_key(request, body)
    if _repeat_dispatch_control(record, "retry", idempotency_key):
        return record.public_dict()
    if record.state not in {"failed", "cancelled"} or not record.recoverable:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "dispatch_not_retryable",
                "message": f"Dispatch in {record.state} cannot be retried safely.",
            },
        )
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    candidates = await _placement_candidates(request, list(fleet.list_instances()))
    placement: PlacementService = ctx.require_service("placement_service")
    provider = record.capacity_provider or record.request_payload.get("provider")
    original = dict(record.placement_decision or {})
    policy_service = _policy_service(request)
    explicit_policies = ctx.store.list_instance_participation_policies(record.realm_id)
    policy_enforcement_active = bool(
        explicit_policies
        or ctx.store.list_instance_groups(
            record.realm_id, include_archived=True
        )
        or ctx.store.list_placement_defaults(record.realm_id)
    )
    for candidate in candidates:
        policy, explicit = policy_service.effective_policy(
            record.realm_id, candidate.instance_id
        )
        candidate.participation_policy = policy
        candidate.participation_policy_explicit = explicit
        # This is a retry of an already resolved placement operation. Preserve
        # its exact candidate scope and target instead of re-expanding a group.
        candidate.group_membership = (
            "included"
            if candidate.instance_id == record.target_instance_id
            else "not_in_requested_group"
        )
        candidate.group_id = original.get("resolved_group_id")
    try:
        decision = await _offload_request(
            request,
            "fleet.retry_placement_resolve",
            placement.resolve,
            PlacementRequest(
                realm_id=record.realm_id,
                fleet_id=ctx.settings.fleet_id,
                instance_id=record.target_instance_id,
                card_id=record.card_id,
                provider=provider,
                model_id=record.request_payload.get("model_id"),
                repository_ids=[
                    str(item.get("repository_id"))
                    for item in (
                        (record.materialization_plan or {})
                        .get("requirements", {})
                        .get("repositories", [])
                    )
                    if item.get("repository_id")
                ],
                workload_profile=str(
                    original.get("workload_profile")
                    or (record.materialization_plan or {}).get("profile")
                    or "research"
                ),
                project_id=record.project_id,
                dispatch_intent=DispatchIntent(
                    original.get("dispatch_intent")
                    or DispatchIntent.AUTOMATIC.value
                ),
                requested_group_id=original.get("requested_group_id"),
                resolved_group_id=original.get("resolved_group_id"),
                resolved_group_name=original.get("resolved_group_name"),
                group_version=original.get("group_version"),
                default_source="idempotent_retry_original_target",
                principal_id=record.principal_id,
                participation_override_reason=original.get(
                    "participation_override_reason"
                ),
                policy_enforcement_active=policy_enforcement_active,
                allow_concurrent=True,
            ),
            candidates,
        )
        capacity = _capacity_admission_from_decision(
            decision.model_dump(mode="json"),
            provider=provider,
            override=False,
            override_reason=None,
        )
        if capacity is None:
            raise PlacementError(
                "capacity_unavailable",
                "The original target did not return fresh capacity data.",
            )
        revalidation = decision.model_dump(mode="json")
        revalidation["original_resolved_at"] = (
            record.placement_resolved_at.isoformat()
            if record.placement_resolved_at
            else None
        )
        revalidation["retry_revalidated_at"] = datetime.now(UTC).isoformat()
        record.placement_decision = revalidation
        record = await _offload_request(
            request,
            "dispatch.retry_capacity_admission",
            ledger.retry_with_capacity,
            record,
            capacity,
            idempotency_key=idempotency_key,
        )
    except PlacementError as exc:
        raise _placement_http_error(exc) from exc
    except DispatchCapacityExhausted as exc:
        raise HTTPException(status_code=409, detail=exc.detail) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "dispatch_retry_race",
                "message": str(exc),
                "recoverable": True,
            },
        ) from exc
    worker = request.app.state.ctx.services.get("dispatch_worker")
    if worker:
        worker.wake()
    return record.public_dict()


def retry_dispatch(
    request: Request,
    dispatch_id: str,
    body: DispatchControlBody | None = None,
) -> dict[str, Any]:
    """Synchronous internal compatibility helper; HTTP uses fresh async probes."""

    require_user(request)
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    idempotency_key = _dispatch_control_key(request, body)
    if _repeat_dispatch_control(record, "retry", idempotency_key):
        return record.public_dict()
    if record.state not in {"failed", "cancelled"} or not record.recoverable:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "dispatch_not_retryable",
                "message": f"Dispatch in {record.state} cannot be retried safely.",
            },
        )
    if record.capacity_limit:
        capacity = CapacityAdmission(
            limit=record.capacity_limit,
            source=record.capacity_source or "unknown",
            provider=record.capacity_provider,
            observed_active=record.capacity_observed_active,
            observed_queued=record.capacity_observed_queued,
            observed_reservations=record.capacity_observed_reservations,
        )
        try:
            record = ledger.retry_with_capacity(
                record, capacity, idempotency_key=idempotency_key
            )
        except DispatchCapacityExhausted as exc:
            raise HTTPException(status_code=409, detail=exc.detail) from exc
    else:
        # Legacy records predate reservations. Preserve their retry behavior;
        # every newly admitted record carries capacity metadata.
        record.cancel_requested = False
        record.last_error = None
        record.error_code = None
        record.control_operations[idempotency_key] = "retry"
        ledger.transition(record, "queued", "Operator queued a safe retry.")
    worker = request.app.state.ctx.services.get("dispatch_worker")
    if worker:
        worker.wake()
    return record.public_dict()


@router.post("/fleet/dispatch-jobs/{dispatch_id}/reconcile", status_code=202)
async def reconcile_dispatch_completion(
    request: Request, dispatch_id: str
) -> dict[str, Any]:
    """Idempotently repair a stranded card-linked completion."""
    require_user(request)
    record = _dispatch_store(request).get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    _require_dispatch_access(request, record)
    outbox = request.app.state.ctx.services.get("completion_outbox")
    if (
        record.completion_payload is not None
        and record.state in {"completion_pending", "completed"}
        and (
            record.reconciliation_state in {"conflict_requires_resolution", "pending"}
            or record.completion_delivery_class
            in {"transport_exhausted", "permanent_failure", "semantic_conflict"}
        )
    ):
        if not outbox:
            raise HTTPException(
                status_code=503,
                detail={"code": "completion_outbox_unavailable", "recoverable": True},
            )
        try:
            return outbox.retry_delivery(dispatch_id).public_dict()
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
    reconciler = request.app.state.ctx.services.get("completion_reconciler")
    if not reconciler:
        raise HTTPException(
            status_code=503,
            detail={"code": "reconciliation_unavailable", "recoverable": True},
        )
    try:
        repaired = await reconciler.retry(dispatch_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return repaired.public_dict()


@router.post("/fleet/dispatch-jobs/{dispatch_id}/cancel", status_code=202)
def cancel_dispatch(
    request: Request,
    dispatch_id: str,
    body: DispatchControlBody | None = None,
) -> dict[str, Any]:
    require_user(request)
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    idempotency_key = _dispatch_control_key(request, body)
    if _repeat_dispatch_control(record, "cancel", idempotency_key):
        return record.public_dict()
    if record.state == "queued":
        record.control_operations[idempotency_key] = "cancel"
        ledger.transition(record, "cancelled", "Operator cancelled queued dispatch.")
        return record.public_dict()
    if record.state not in {
        "checking_sync",
        "materializing",
        "starting_session",
    }:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "dispatch_not_cancellable",
                "message": "Prompt acceptance has already made this dispatch non-cancellable.",
            },
        )
    record.cancel_requested = True
    record.control_operations[idempotency_key] = "cancel"
    ledger.transition(
        record,
        record.state,
        "Cancellation requested; the worker will stop at the next safe boundary.",
    )
    return record.public_dict()


@router.get("/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}")
async def authority_dispatch_status(
    request: Request, authority_instance_id: str, dispatch_id: str
) -> dict[str, Any]:
    require_user(request)
    if authority_instance_id == request.app.state.ctx.settings.instance_id:
        return get_dispatch(request, dispatch_id)
    if getattr(request.state, "instance_authenticated", False) is True:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "wrong_authority",
                "message": "The selected authority did not receive the routed request.",
            },
        )
    return await _peer_authority_json(
        request, authority_instance_id, "GET", f"dispatch-jobs/{dispatch_id}"
    )


@router.post(
    "/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/{operation}"
)
async def authority_dispatch_mutation(
    request: Request,
    authority_instance_id: str,
    dispatch_id: str,
    operation: Literal["retry", "cancel", "prompt"],
    body: dict[str, Any],
) -> dict[str, Any]:
    require_user(request)
    if authority_instance_id == request.app.state.ctx.settings.instance_id:
        if operation == "prompt":
            return await prompt_dispatch_session(
                request, dispatch_id, DispatchFollowupBody.model_validate(body)
            )
        control = DispatchControlBody.model_validate(body)
        return (retry_dispatch if operation == "retry" else cancel_dispatch)(
            request, dispatch_id, control
        )
    if getattr(request.state, "instance_authenticated", False) is True:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "wrong_authority",
                "message": "The selected authority did not receive the routed request.",
            },
        )
    return await _peer_authority_json(
        request,
        authority_instance_id,
        "POST",
        f"dispatch-jobs/{dispatch_id}/{operation}",
        body=body,
    )


@router.get("/fleet/instances/{instance_id}/dispatches")
async def target_dispatches(request: Request, instance_id: str) -> list[dict[str, Any]]:
    """Expose the target's completion outbox alongside authority-side progress."""
    require_user(request)
    inst = _fleet_instance_or_404(request, instance_id)
    client = request.app.state.ctx.services.get("fleet_http_client")
    owns_client = client is None
    client = client or httpx.AsyncClient(timeout=5.0)
    try:
        response = await _fleet_http(
            request,
            "http.fleet_dispatch_status",
            client.get(
                f"{inst.url.rstrip('/')}/api/fleet/dispatch-jobs",
                params={"target_instance_id": instance_id, "limit": 100},
                headers=_peer_headers(request),
                timeout=5.0,
            ),
            timeout=5.0,
        )
        response.raise_for_status()
        payload = await _response_json(request, response)
        return payload if isinstance(payload, list) else []
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Target dispatch status unavailable: {exc}",
        ) from exc
    finally:
        if owns_client:
            await client.aclose()


@router.api_route(
    "/fleet/instances/{instance_id}/agent/{agent_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    include_in_schema=False,
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
        from pa.server.shutdown import wait_for_shutdown_or

        async def relay() -> AsyncIterator[bytes]:
            try:
                try:
                    iterator = upstream.aiter_raw().__aiter__()
                    while True:
                        try:
                            stopping, chunk = await wait_for_shutdown_or(
                                anext(iterator)
                            )
                        except StopAsyncIteration:
                            break
                        if stopping:
                            break
                        assert chunk is not None
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
    if request.method != "GET" and upstream.status_code < 400:
        cache_for(request.app.state.ctx.settings.data_dir).invalidate(
            instance_id,
            "activity",
            "repositories",
        )
    return Response(
        content=content,
        status_code=upstream.status_code,
        headers=response_headers,
    )


def _local_session_route(request: Request, session_id: str) -> dict[str, Any] | None:
    ctx = request.app.state.ctx
    session = ctx.store.get_session(session_id)
    if not session:
        return None
    manager = ctx.services.get("instance_agent")
    runtime = manager.get(session_id) if manager else None
    live = bool(runtime and not getattr(runtime, "_closed", False))
    ended = session.status in {"closed", "quiesced"}
    return {
        "session_id": session_id,
        "state": "live" if live else "expired" if ended else "recoverable",
        "live": live,
        "recoverable": not live and not ended,
        "api_base": "/api/agent",
        "owner": {
            "instance_id": session.origin_instance_id or ctx.settings.instance_id,
            "instance_name": session.origin_instance_name or ctx.settings.instance_name,
        },
        "provider": {
            "id": session.agent_name,
            "session_id": session.external_session_id,
        },
        "history_url": f"/api/agent/history/{session_id}",
        "recovery_url": (
            f"/api/agent/sessions/{session_id}/recover"
            if not live and not ended
            else None
        ),
    }


@router.get("/fleet/session-route/{session_id}")
async def resolve_session_route(
    request: Request,
    session_id: str,
    owner_instance_id: str | None = None,
) -> dict[str, Any]:
    """Resolve a durable PA session to its owning fleet instance."""
    require_user(request)
    ctx = request.app.state.ctx
    local = _local_session_route(request, session_id)
    if local:
        return local

    dispatch_store = ctx.services.get("dispatch_store")
    dispatch = dispatch_store.by_session(session_id) if dispatch_store else None
    owner_id = owner_instance_id or (dispatch.target_instance_id if dispatch else None)
    owner_name = dispatch.target_instance_name if dispatch else None
    if not owner_id:
        return {
            "session_id": session_id,
            "state": "missing",
            "live": False,
            "recoverable": False,
            "message": "This agent session was deleted or has expired.",
        }
    if owner_id == ctx.settings.instance_id:
        return {
            "session_id": session_id,
            "state": "missing",
            "live": False,
            "recoverable": False,
            "owner": {
                "instance_id": owner_id,
                "instance_name": ctx.settings.instance_name,
            },
            "message": "This agent session was deleted or has expired.",
        }

    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    owner = fleet.get_instance(owner_id)
    api_base = f"/api/fleet/instances/{quote(owner_id, safe='-._~')}/agent"
    if not owner:
        return {
            "session_id": session_id,
            "state": "owner_unreachable",
            "live": False,
            "recoverable": True,
            "api_base": api_base,
            "owner": {
                "instance_id": owner_id,
                "instance_name": owner_name or owner_id,
            },
            "message": "The session owner is not currently registered. Retry after it reconnects.",
        }
    try:
        history = await asyncio.wait_for(
            _peer_agent_json(
                request,
                owner_id,
                "GET",
                f"history/{session_id}",
                params={"limit": 1},
                timeout=SESSION_ROUTE_TIMEOUT,
            ),
            timeout=SESSION_ROUTE_TIMEOUT,
        )
    except TimeoutError:
        return {
            "session_id": session_id,
            "state": "owner_unreachable",
            "live": False,
            "recoverable": True,
            "api_base": api_base,
            "owner": {
                "instance_id": owner_id,
                "instance_name": owner.name,
            },
            "message": "The session owner is responding slowly. Durable history may still be available; PA will retry.",
        }
    except HTTPException as exc:
        if exc.status_code in {502, 503, 504}:
            return {
                "session_id": session_id,
                "state": "owner_unreachable",
                "live": False,
                "recoverable": True,
                "api_base": api_base,
                "owner": {
                    "instance_id": owner_id,
                    "instance_name": owner.name,
                },
                "message": "The session owner is temporarily unreachable. Retry when it reconnects.",
            }
        if exc.status_code in {404, 410}:
            return {
                "session_id": session_id,
                "state": "missing" if exc.status_code == 404 else "expired",
                "live": False,
                "recoverable": False,
                "api_base": api_base,
                "owner": {
                    "instance_id": owner_id,
                    "instance_name": owner.name,
                },
                "message": (
                    "This agent session was deleted or has expired."
                    if exc.status_code == 404
                    else "This session has ended; its retained history is unavailable."
                ),
            }
        logger.warning(
            "Remote session route failed owner=%s session=%s status=%s",
            owner_id,
            session_id,
            exc.status_code,
        )
        return {
            "session_id": session_id,
            "state": "owner_unreachable",
            "live": False,
            "recoverable": True,
            "api_base": api_base,
            "owner": {
                "instance_id": owner_id,
                "instance_name": owner.name,
            },
            "message": "The session owner could not provide live state. Durable history may still be available; PA will retry.",
        }
    if not isinstance(history, dict) or not isinstance(history.get("session"), dict):
        logger.warning(
            "Remote session route received invalid history owner=%s session=%s",
            owner_id,
            session_id,
        )
        return {
            "session_id": session_id,
            "state": "owner_unreachable",
            "live": False,
            "recoverable": True,
            "api_base": api_base,
            "owner": {
                "instance_id": owner_id,
                "instance_name": owner.name,
            },
            "message": "The session owner returned incomplete live state. Durable history may still be available; PA will retry.",
        }
    session = history["session"]
    live = bool(history.get("live"))
    ended = session.get("status") in {"closed", "quiesced"}
    return {
        "session_id": session_id,
        "state": "live" if live else "expired" if ended else "recoverable",
        "live": live,
        "recoverable": not live and not ended,
        "api_base": api_base,
        "owner": {
            "instance_id": owner_id,
            "instance_name": owner.name,
        },
        "provider": {
            "id": session.get("agent_name"),
            "session_id": session.get("external_session_id"),
        },
        "history_url": f"{api_base}/history/{session_id}",
        "recovery_url": (
            f"{api_base}/sessions/{session_id}/recover"
            if not live and not ended
            else None
        ),
    }


async def _proxy_agent_providers(
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
    client = request.app.state.ctx.services.get("fleet_http_client")
    owns_client = client is None
    client = client or httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=5.0),
        limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
    )
    try:
        resp = await _fleet_http(
            request,
            "http.fleet_provider_proxy",
            client.request(method, url, headers=headers, json=body, timeout=120.0),
            timeout=125.0,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Peer unreachable: {exc}") from exc
    finally:
        if owns_client:
            await client.aclose()
    if resp.status_code >= 400:
        try:
            payload = await _response_json(request, resp)
            detail = (
                payload.get("detail", payload) if isinstance(payload, dict) else payload
            )
        except ValueError:
            detail = resp.text[:500]
        raise HTTPException(status_code=resp.status_code, detail=detail)
    return await _response_json(request, resp)


async def _proxy_workspace_reconcile(
    request: Request,
    instance_id: str,
    body: dict,
) -> dict:
    require_user(request)
    inst = _fleet_instance_or_404(request, instance_id)
    settings = request.app.state.ctx.settings
    headers: dict[str, str] = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    url = f"{inst.url.rstrip('/')}/api/workspaces/reconcile"
    client = request.app.state.ctx.services.get("fleet_http_client")
    owns_client = client is None
    client = client or httpx.AsyncClient(
        timeout=httpx.Timeout(305.0, connect=5.0),
        limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
    )
    try:
        resp = await _fleet_http(
            request,
            "http.fleet_workspace_reconcile",
            client.post(url, headers=headers, json=body, timeout=305.0),
            timeout=310.0,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Peer unreachable: {exc}") from exc
    finally:
        if owns_client:
            await client.aclose()
    if resp.status_code >= 400:
        try:
            payload = await _response_json(request, resp)
            detail = (
                payload.get("detail", payload) if isinstance(payload, dict) else payload
            )
        except ValueError:
            detail = resp.text[:500]
        raise HTTPException(status_code=resp.status_code, detail=detail)
    return await _response_json(request, resp)


@router.post("/fleet/instances/{instance_id}/workspaces/reconcile")
async def fleet_workspace_reconcile(
    request: Request,
    instance_id: str,
    body: dict,
):
    payload = await _proxy_workspace_reconcile(request, instance_id, body)
    cache_for(request.app.state.ctx.settings.data_dir).invalidate(
        instance_id,
        "activity",
        "repositories",
    )
    return payload


@router.get("/fleet/instances/{instance_id}/agent-providers")
async def fleet_agent_providers(request: Request, instance_id: str):
    return await _proxy_agent_providers(request, instance_id, "GET", "")


@router.get("/fleet/instances/{instance_id}/agent-providers/{provider_id}")
async def fleet_agent_provider(request: Request, instance_id: str, provider_id: str):
    return await _proxy_agent_providers(request, instance_id, "GET", f"/{provider_id}")


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/install")
async def fleet_agent_provider_install(
    request: Request, instance_id: str, provider_id: str
):
    return await _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/install"
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/update")
async def fleet_agent_provider_update(
    request: Request, instance_id: str, provider_id: str
):
    return await _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/update"
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/configure")
async def fleet_agent_provider_configure(
    request: Request, instance_id: str, provider_id: str, body: dict
):
    return await _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/configure", body=body
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/probe")
async def fleet_agent_provider_probe(
    request: Request, instance_id: str, provider_id: str
):
    return await _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/probe"
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/login-jobs")
async def fleet_agent_provider_login_start(
    request: Request, instance_id: str, provider_id: str, body: dict
):
    return await _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/login-jobs", body=body
    )


@router.post(
    "/fleet/instances/{instance_id}/agent-providers/{provider_id}/codex-cli/install"
)
async def fleet_agent_provider_codex_cli_install(
    request: Request, instance_id: str, provider_id: str
):
    return await _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/codex-cli/install"
    )


@router.get(
    "/fleet/instances/{instance_id}/agent-providers/{provider_id}/login-jobs/{job_id}"
)
async def fleet_agent_provider_login_status(
    request: Request, instance_id: str, provider_id: str, job_id: str
):
    return await _proxy_agent_providers(
        request, instance_id, "GET", f"/{provider_id}/login-jobs/{job_id}"
    )


@router.get(
    "/fleet/instances/{instance_id}/agent-providers/{provider_id}/login-jobs/{job_id}/events"
)
async def fleet_agent_provider_login_events(
    request: Request, instance_id: str, provider_id: str, job_id: str, after: int = 0
):
    return await _proxy_agent_providers(
        request,
        instance_id,
        "GET",
        f"/{provider_id}/login-jobs/{job_id}/events?after={after}",
    )


@router.post(
    "/fleet/instances/{instance_id}/agent-providers/{provider_id}/login-jobs/{job_id}/cancel"
)
async def fleet_agent_provider_login_cancel(
    request: Request, instance_id: str, provider_id: str, job_id: str
):
    return await _proxy_agent_providers(
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
            dispatch_capacity=settings.dispatch_capacity,
            dispatch_provider_capacities=dict(settings.dispatch_provider_capacities),
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
        ctx.register_service(
            "placement_service",
            PlacementService(RoundRobinCursorStore(settings.data_dir)),
        )
        ctx.register_service("fleet_policy", FleetPolicyService(ctx.store))

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
        pages.register(
            PageDefinition(
                id="workshop",
                path="/workshop",
                label="Workshop",
                icon="workshop",
                template="pages/workshop.html",
                nav_order=45,
                context_builder=_workshop_context,
            )
        )

    async def on_startup(self, app, ctx: AppContext) -> None:
        async_runtime = ctx.require_service("async_runtime")
        fleet_http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=5.0),
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
        )
        ctx.register_service("fleet_http_client", fleet_http_client)
        recoverable = await async_runtime.run_blocking(
            "fleet.update_recovery",
            prepare_update_job_recovery,
            ctx.require_service("fleet_registry"),
            ctx.require_service("fleet_update_job_store"),
        )
        for job in recoverable:
            start_update_job(
                ctx.settings,
                ctx.require_service("fleet_update_job_store"),
                job,
                async_runtime=async_runtime,
                http_client=fleet_http_client,
            )
        dispatch_worker = DispatchWorker(
            ctx.require_service("dispatch_store"),
            lambda record: _process_remote_dispatch(app, record),
            async_runtime=async_runtime,
        )
        dispatch_worker.start()
        ctx.register_service("dispatch_worker", dispatch_worker)
        outbox = CompletionOutbox(
            ctx.require_service("dispatch_store"),
            ctx.settings.sync_token,
            async_runtime=async_runtime,
            disposition_notifier=ctx.require_service(
                "instance_agent"
            ).record_card_disposition_status,
        )
        ctx.register_service("completion_outbox", outbox)
        agent = ctx.require_service("instance_agent")
        reconciler = CompletionReconciler(
            ctx.require_service("dispatch_store"),
            agent,
            outbox,
            ctx.store,
            lambda: ctx.services.get("pr_supervisor"),
        )
        await reconciler.recover()
        reconciler.start()
        ctx.register_service("completion_reconciler", reconciler)
        agent.completion_handler = reconciler.handle_completion
        from pa.instance.session_lifecycle import SessionLifecyclePolicy

        lifecycle = SessionLifecyclePolicy(agent, ctx.services)
        ctx.register_service("session_lifecycle", lifecycle)
        if ctx.settings.agent_enabled:
            lifecycle.start()
        progress_service = ProgressService(
            ctx.require_service("dispatch_store"),
            instance_id=ctx.settings.instance_id,
            token=ctx.settings.sync_token,
            async_runtime=async_runtime,
            session_manager=agent,
        )
        ctx.register_service("progress_service", progress_service)
        agent.progress_handler = progress_service.observe
        progress_service.start()
        outbox.start()

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        lifecycle = ctx.services.get("session_lifecycle")
        if lifecycle:
            await lifecycle.close()
        dispatch_worker = ctx.services.get("dispatch_worker")
        if dispatch_worker:
            await dispatch_worker.close()
        reconciler = ctx.services.get("completion_reconciler")
        if reconciler:
            await reconciler.close()
        progress_service = ctx.services.get("progress_service")
        if progress_service:
            await progress_service.close()
        outbox = ctx.services.get("completion_outbox")
        if outbox:
            await outbox.close(timeout=5.0)
        client = ctx.services.get("fleet_http_client")
        if client:
            await client.aclose()

    def api_routers(self):
        return [("/api", router, ["fleet"])]

    def ui_routers(self):
        return [ui_router]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def list_instance_groups(
            realm_id: str | None = None, include_archived: bool = False
        ) -> list[dict]:
            """List immutable built-in and operator-defined worker groups."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/fleet/instance-groups",
                params={
                    "realm": realm_id,
                    "include_archived": include_archived,
                },
            )

        @mcp.tool()
        def get_instance_group(
            group_id: str, realm_id: str | None = None
        ) -> dict | None:
            """Get one worker group with stable-ID membership and exclusions."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/fleet/instance-groups/{group_id}",
                params={"realm": realm_id},
                allow_not_found=True,
            )

        @mcp.tool()
        def create_instance_group(
            name: str,
            description: str = "",
            realm_id: str = "default",
            included_instance_ids: list[str] | None = None,
            excluded_instance_ids: list[str] | None = None,
            selector: dict[str, Any] | None = None,
            permitted_placement_policies: list[str] | None = None,
            visible_project_ids: list[str] | None = None,
        ) -> dict:
            """Create a synchronized reusable fleet selection scope."""
            payload = {
                "realm_id": realm_id,
                "name": name,
                "description": description,
                "included_instance_ids": included_instance_ids or [],
                "excluded_instance_ids": excluded_instance_ids or [],
                "selector": selector or {},
                "visible_project_ids": visible_project_ids or [],
            }
            if permitted_placement_policies is not None:
                payload["permitted_placement_policies"] = (
                    permitted_placement_policies
                )
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/fleet/instance-groups",
                json=payload,
            )

        @mcp.tool()
        def update_instance_group(
            group_id: str,
            changes: dict[str, Any],
            realm_id: str | None = None,
        ) -> dict:
            """Update group rules using the expected_version in changes when supplied."""
            return request_local_pa(
                ctx.settings,
                "PATCH",
                f"/api/fleet/instance-groups/{group_id}",
                params={"realm": realm_id},
                json=changes,
            )

        @mcp.tool()
        def archive_instance_group(
            group_id: str, realm_id: str | None = None
        ) -> dict:
            """Archive a custom group without allowing defaults to fall back."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/fleet/instance-groups/{group_id}/archive",
                params={"realm": realm_id},
            )

        @mcp.tool()
        def delete_instance_group(
            group_id: str, realm_id: str | None = None
        ) -> dict | None:
            """Delete a custom group; references remain visibly unavailable."""
            return request_local_pa(
                ctx.settings,
                "DELETE",
                f"/api/fleet/instance-groups/{group_id}",
                params={"realm": realm_id},
            )

        @mcp.tool()
        def set_instance_group_member(
            group_id: str,
            instance_id: str,
            *,
            included: bool = True,
            excluded: bool = False,
            realm_id: str | None = None,
        ) -> dict:
            """Add/remove a stable instance ID from explicit membership or exclusions."""
            collection = "exclusions" if excluded else "members"
            return request_local_pa(
                ctx.settings,
                "PUT" if included else "DELETE",
                f"/api/fleet/instance-groups/{group_id}/{collection}/{instance_id}",
                params={"realm": realm_id},
            )

        @mcp.tool()
        def preview_instance_group(
            group_id: str,
            workload_profile: str = "research",
            project_id: str | None = None,
            policy: PlacementPolicy = PlacementPolicy.BEST_MATCH,
        ) -> dict:
            """Preview expanded membership plus policy/readiness rejection reasons."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/fleet/instance-groups/{group_id}/preview",
                params={
                    "workload_profile": workload_profile,
                    "project_id": project_id,
                    "policy": policy.value,
                },
            )

        @mcp.tool()
        def get_instance_participation_policy(
            instance_id: str, realm_id: str | None = None
        ) -> dict:
            """Get an instance's effective participation policy and summary."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/fleet/instances/{instance_id}/participation-policy",
                params={"realm": realm_id},
            )

        @mcp.tool()
        def update_instance_participation_policy(
            instance_id: str,
            changes: dict[str, Any],
            realm_id: str | None = None,
        ) -> dict:
            """Update a policy; enabling work requires confirmation fields."""
            return request_local_pa(
                ctx.settings,
                "PUT",
                f"/api/fleet/instances/{instance_id}/participation-policy",
                params={"realm": realm_id},
                json=changes,
            )

        @mcp.tool()
        def set_placement_default_group(
            group_id: str,
            realm_id: str | None = None,
            project_id: str | None = None,
            workload_profile: str | None = None,
        ) -> dict:
            """Set a realm/project/profile default without all-instance fallback."""
            return request_local_pa(
                ctx.settings,
                "PUT",
                "/api/fleet/placement-defaults",
                json={
                    "group_id": group_id,
                    "realm_id": realm_id,
                    "project_id": project_id,
                    "workload_profile": workload_profile,
                },
            )

        @mcp.tool()
        def list_placement_default_groups(
            realm_id: str | None = None,
        ) -> list[dict]:
            """List synchronized realm/project/profile group defaults."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/fleet/placement-defaults",
                params={"realm": realm_id},
            )

        @mcp.tool()
        def delete_placement_default_group(
            realm_id: str | None = None,
            project_id: str | None = None,
            workload_profile: str | None = None,
        ) -> None:
            """Delete one exact default scope without silently selecting all peers."""
            request_local_pa(
                ctx.settings,
                "DELETE",
                "/api/fleet/placement-defaults",
                params={
                    "realm": realm_id,
                    "project_id": project_id,
                    "workload_profile": workload_profile,
                },
            )

        @mcp.tool()
        def migrate_instance_participation_policies(
            realm_id: str | None = None, apply: bool = False
        ) -> dict:
            """Preview or deliberately apply the compatibility policy migration."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/fleet/participation-migration",
                json={"realm_id": realm_id, "apply": apply},
            )

        @mcp.tool()
        def preview_fleet_placement(
            policy: PlacementPolicy,
            card_id: str | None = None,
            group_id: str | None = None,
            instance_id: str | None = None,
            project_id: str | None = None,
            workload_profile: str = "research",
            provider: str | None = None,
            model_id: str | None = None,
            required_capabilities: list[str] | None = None,
        ) -> dict:
            """Resolve and explain candidates without admitting a dispatch."""
            if instance_id and group_id:
                raise ValueError("named preview cannot also specify group_id")
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/fleet/placement/preview",
                json={
                    "card_id": card_id,
                    "project_id": project_id,
                    "target_instance_id": instance_id,
                    "placement_policy": None if instance_id else policy.value,
                    "group_id": group_id,
                    "provider": provider,
                    "model_id": model_id,
                    "required_capabilities": required_capabilities or [],
                    "execution_contract": {
                        "version": 1,
                        "profile": workload_profile,
                        "confirmed": True,
                        "requirements": {},
                    },
                },
            )

        @mcp.tool()
        def list_fleet_policy_audit(
            realm_id: str | None = None,
            entity_type: str | None = None,
            entity_id: str | None = None,
            limit: int = 200,
        ) -> list[dict]:
            """List policy/group/default mutations and resolved placement decisions."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/fleet/policy-audit",
                params={
                    "realm": realm_id,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "limit": limit,
                },
            )

        @mcp.tool()
        def dispatch_card(
            card_id: str,
            idempotency_key: str,
            instance_id: str | None = None,
            policy: PlacementPolicy | None = None,
            group_id: str | None = None,
            message: str = "",
            authority_instance_id: str | None = None,
            provider: str | None = None,
            model_id: str | None = None,
            mode_id: str | None = None,
            effort: str | None = None,
            allow_concurrent: bool = False,
            capacity_override: bool = False,
            capacity_override_reason: str | None = None,
            participation_override: bool = False,
            participation_override_reason: str | None = None,
            execution_contract: dict[str, Any] | None = None,
        ) -> dict:
            """Resolve a concrete target or policy and durably dispatch a card."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            if bool(instance_id) == bool(policy):
                raise ValueError("specify exactly one instance_id or policy")
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/fleet/dispatch",
                json={
                    "authority_instance_id": authority_instance_id,
                    "card_id": card_id,
                    "target_instance_id": instance_id,
                    "placement_policy": (
                        policy.value if isinstance(policy, PlacementPolicy) else policy
                    ),
                    "group_id": group_id,
                    "message": message,
                    "provider": provider,
                    "model_id": model_id,
                    "mode_id": mode_id,
                    "effort": effort,
                    "allow_concurrent": allow_concurrent,
                    "capacity_override": capacity_override,
                    "capacity_override_reason": capacity_override_reason,
                    "participation_override": participation_override,
                    "participation_override_reason": (
                        participation_override_reason
                    ),
                    "execution_contract": execution_contract,
                    "idempotency_key": key,
                },
            )

        @mcp.tool()
        def dispatch_card_to_instance(
            card_id: str,
            instance_id: str,
            idempotency_key: str,
            message: str = "",
            authority_instance_id: str | None = None,
            provider: str | None = None,
            model_id: str | None = None,
            mode_id: str | None = None,
            effort: str | None = None,
            cwd: str | None = None,
            config: dict[str, str | bool] | None = None,
            allow_concurrent: bool = False,
            capacity_override: bool = False,
            capacity_override_reason: str | None = None,
            participation_override: bool = False,
            participation_override_reason: str | None = None,
            execution_contract: dict[str, Any] | None = None,
        ) -> dict:
            """Durably and idempotently dispatch an authoritative card to a fleet instance."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            payload = {
                "authority_instance_id": authority_instance_id,
                "card_id": card_id,
                "message": message,
                "provider": provider,
                "model_id": model_id,
                "mode_id": mode_id,
                "effort": effort,
                "cwd": cwd,
                "config": config or {},
                "idempotency_key": key,
            }
            if execution_contract is not None:
                payload["execution_contract"] = execution_contract
            if allow_concurrent:
                payload["allow_concurrent"] = True
            if capacity_override:
                payload["capacity_override"] = True
                payload["capacity_override_reason"] = capacity_override_reason
            if participation_override:
                payload["participation_override"] = True
                payload["participation_override_reason"] = (
                    participation_override_reason
                )
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/fleet/instances/{instance_id}/agent/start",
                json=payload,
            )

        @mcp.tool()
        def get_dispatch(
            dispatch_id: str, authority_instance_id: str | None = None
        ) -> dict | None:
            """Get normalized durable dispatch, session, authority, target, and card-version state."""
            return request_local_pa(
                ctx.settings,
                "GET",
                (
                    f"/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}"
                    if authority_instance_id
                    else f"/api/fleet/dispatch-jobs/{dispatch_id}"
                ),
                allow_not_found=True,
            )

        @mcp.tool()
        def report_dispatch_progress(
            dispatch_id: str,
            phase: Literal[
                "investigating",
                "planning",
                "implementing",
                "testing",
                "opening_pr",
                "waiting_ci",
                "addressing_review",
                "merging",
                "blocked",
                "retrying",
                "turn_ended",
                "completed",
            ],
            summary: str,
            idempotency_key: str,
            branch: str | None = None,
            commit_sha: str | None = None,
            pr_url: str | None = None,
            pr_number: int | None = None,
            changed_file_count: int | None = None,
            blockers: list[str] | None = None,
            retry_reason: str | None = None,
            operator_input: str | None = None,
        ) -> dict:
            """Emit a sanitized structured checkpoint for a linked durable dispatch."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/fleet/dispatch-jobs/{dispatch_id}/checkpoint",
                json={
                    "schema_version": PROGRESS_SCHEMA_VERSION,
                    "phase": phase,
                    "summary": summary,
                    "branch": branch,
                    "commit_sha": commit_sha,
                    "pr_url": pr_url,
                    "pr_number": pr_number,
                    "changed_file_count": changed_file_count,
                    "blockers": blockers or [],
                    "retry_reason": retry_reason,
                    "operator_input": operator_input,
                    "idempotency_key": key,
                },
            )

        @mcp.tool()
        def retry_dispatch(
            dispatch_id: str,
            idempotency_key: str,
            authority_instance_id: str | None = None,
        ) -> dict:
            """Idempotently queue a safe retry through the durable dispatch control plane."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            return request_local_pa(
                ctx.settings,
                "POST",
                (
                    f"/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/retry"
                    if authority_instance_id
                    else f"/api/fleet/dispatch-jobs/{dispatch_id}/retry"
                ),
                json={"idempotency_key": key},
            )

        @mcp.tool()
        def cancel_dispatch(
            dispatch_id: str,
            idempotency_key: str,
            authority_instance_id: str | None = None,
        ) -> dict:
            """Idempotently request cancellation at a safe durable dispatch boundary."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            return request_local_pa(
                ctx.settings,
                "POST",
                (
                    f"/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/cancel"
                    if authority_instance_id
                    else f"/api/fleet/dispatch-jobs/{dispatch_id}/cancel"
                ),
                json={"idempotency_key": key},
            )

        @mcp.tool()
        def prompt_dispatch_session(
            dispatch_id: str,
            message: str,
            idempotency_key: str,
            action: Literal["append", "prepend", "interrupt"] = "append",
            authority_instance_id: str | None = None,
        ) -> dict:
            """Durably prompt the live session linked to a dispatch without exposing CSRF state."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            path = (
                f"/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/prompt"
                if authority_instance_id
                else f"/api/fleet/dispatch-jobs/{dispatch_id}/prompt"
            )
            return request_local_pa(
                ctx.settings,
                "POST",
                path,
                json={
                    "message": message,
                    "action": action,
                    "idempotency_key": key,
                },
            )

        @mcp.tool()
        def get_post_turn_action_catalog() -> dict:
            """List versioned follow-up actions, schemas, policy, and loop budgets."""
            return request_local_pa(
                ctx.settings, "GET", "/api/fleet/post-turn/action-catalog"
            )

        @mcp.tool()
        def get_dispatch_turn_end(dispatch_id: str) -> dict | None:
            """Read neutral turn-end snapshots, evaluations, actions, and diagnostics."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/fleet/dispatch-jobs/{dispatch_id}/turn-end",
                allow_not_found=True,
            )

        @mcp.tool()
        def repair_terminal_dispatch(
            dispatch_id: str, idempotency_key: str
        ) -> dict:
            """Audit and normalize an acknowledged legacy target record to terminal."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/fleet/dispatch-jobs/{dispatch_id}/repair-terminal",
                json={"idempotency_key": key},
            )
