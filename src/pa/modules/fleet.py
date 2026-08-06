"""Fleet management, realms, membership, and remote install APIs."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from urllib.parse import quote
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field, field_validator, model_validator

from pa.acp.configuration import SessionConfigurationRequest
from pa.acp.environment import (
    assigned_service_mcp_environment,
    assigned_service_session_capability,
)
from pa.acp.providers.registry import get_provider
from pa.attachments import (
    CHUNK_BYTES,
    AttachmentError,
    AttachmentStore,
    manifest_digest,
)
from pa.auth.middleware import get_principal_id, require_user
from pa.collaboration.models import CollaborationMode, PolicyInput
from pa.core.async_runtime import AsyncRuntime
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.io import atomic_write_json
from pa.core.logging import redact_log_text
from pa.core.ui.instance_identity import (
    canonicalize_dispatch_public,
    current_instance_name,
)
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import (
    AgentSession,
    CardAttachment,
    CardCreate,
    CardKind,
    CardLane,
    CardUpdate,
    FleetInstance,
    KnowledgeEntry,
    RealmRole,
)
from pa.domain.notifications import (
    InteractionChoice,
    InteractionKind,
    InteractionRequest,
    NotificationAction,
    NotificationCreate,
    NotificationPriority,
    NotificationType,
    NotificationVisibility,
)
from pa.execution.dispatch import (
    CapacityAdmission,
    CompletionOutbox,
    ConcurrentCardDispatch,
    DispatchCapacityExhausted,
    DispatchEvent,
    DispatchIdempotencyConflict,
    DispatchQueueFull,
    DispatchRecord,
    DispatchStore,
    DispatchWorker,
    GoalDispatchProvenance,
    goal_admission_validation_proof,
    goal_dispatch_execution_identity_valid,
    goal_dispatch_materialization_binding_valid,
    goal_dispatch_placement_decision_digest,
    goal_dispatch_placement_input_digest,
    goal_dispatch_placement_input_snapshot,
    goal_dispatch_record_placement_input_valid,
)
from pa.execution.disposition import decide_card_disposition
from pa.execution.post_turn import (
    EvidenceReferenceV1,
    FollowupActionName,
    FollowupActionStatus,
    PostTurnEvaluationV1,
    PostTurnEvaluator,
    TurnEndSnapshotV1,
    action_catalog,
    is_authorized_same_session_continuation,
    mark_record_only_actions,
)
from pa.execution.profiles import (
    ExecutionContract,
    MaterializationPlan,
    resolve_materialization_plan,
)
from pa.execution.progress import (
    PROGRESS_SCHEMA_VERSION,
    SUPPORTED_PROGRESS_VERSIONS,
    CompletionReportV1,
    DispatchProgressEventV1,
    DispatchProgressHeartbeatV1,
    ExplicitProgressCheckpointV1,
    OperatorInputRequestV1,
    ProgressKind,
    ProgressService,
    sanitize_completion_report,
    sanitize_text,
)
from pa.execution.reconciliation import CompletionReconciler
from pa.fleet.bootstrap import (
    BootstrapJob,
    BootstrapJobStore,
    BootstrapRequest,
    BootstrapState,
    PhaseState,
    accept_bootstrap_input,
    discover_target,
    run_bootstrap_job,
)
from pa.fleet.capacity import normalize_activity_capacity
from pa.fleet.control_plane import build_control_plane_status
from pa.fleet.convergence import MembershipConvergenceStore
from pa.fleet.credentials import CredentialRotationStore
from pa.fleet.credentials import router as credential_router
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
from pa.fleet.workshop import build_workshop_snapshot, workshop_semantic_snapshot
from pa.goals.advanced_models import (
    GoalActionDisposition,
    GoalActionRequest,
    GoalAssignedServiceScope,
    GoalReservationState,
    GoalResourceClaim,
    GoalUsage,
    GovernanceMutationContext,
    ResourceAccess,
)
from pa.goals.assigned_projection import assigned_goal_projection
from pa.goals.governance import (
    GoalAssignedServiceAuthorization,
    GoalAssignedServiceCredentialError,
    GoalGovernanceConflict,
)
from pa.goals.materialization import (
    GoalExecutionIdentityV1,
    GoalMaterializationEnvelopeV1,
    GoalMaterializationReceiptV1,
    GoalMaterializationResourceClaimV1,
    canonical_materialization_digest,
)
from pa.goals.models import (
    AssignedServiceGoalAuditCreate,
    AssignedServiceGoalEvidenceCreate,
    AssignedServiceGoalProposalCreate,
    GoalActorRole,
)
from pa.network.peer_table import PeerTable

logger = logging.getLogger(__name__)

FLEET_HEALTH_TIMEOUT = 3.0
FLEET_DETAIL_TIMEOUT = 5.0
FLEET_AGGREGATE_TIMEOUT = 9.0
SESSION_ROUTE_TIMEOUT = 3.0
ASSIGNED_SERVICE_CREDENTIAL_TTL_SECONDS = 86_400

router = APIRouter()
router.include_router(credential_router)
ui_router = APIRouter()
_peer_update_task: asyncio.Task[Any] | None = None
_peer_update_task_operation_id: str | None = None
_bootstrap_tasks: dict[str, asyncio.Task[Any]] = {}


class AssignedServiceProxyRequest(BaseModel):
    """Identity-free payload forwarded only between authenticated PA instances."""

    model_config = {"extra": "forbid"}

    payload: dict[str, Any] = Field(default_factory=dict)
    expected_version: int | None = Field(default=None, ge=0)
    policy_revision: int | None = Field(default=None, ge=1)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)


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
    mode_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        pattern=r"\S",
    )
    collaboration_mode: CollaborationMode | None = None
    collaboration_risk: str = "low"
    collaboration_ambiguous: bool = False
    collaboration_unattended: bool = False
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
    priority: int = Field(default=0, ge=-10, le=10)
    goal_provenance: GoalDispatchProvenance | None = None

    @field_validator("provider", "model_id", "mode_id", mode="before")
    @classmethod
    def normalize_optional_selector(cls, value):
        if value is None:
            return None
        normalized = str(value).strip()
        # Legacy form serialization emitted the Python display value. It is
        # never a stable provider/model ID, so preserve automatic selection.
        return None if not normalized or normalized.casefold() == "none" else normalized


class FleetDispatchBody(RemoteAgentStartBody):
    """Authority-side dispatch target or placement policy."""

    target_instance_id: str | None = None
    placement_policy: PlacementPolicy | None = None
    group_id: str | None = None
    required_capabilities: list[str] = Field(default_factory=list)
    required_mcp_servers: list[str] = Field(default_factory=list)
    optional_mcp_servers: list[str] = Field(default_factory=list)
    goal_id: str | None = None
    goal_version: int | None = Field(default=None, ge=1)
    goal_policy_revision: int | None = Field(default=None, ge=1)
    goal_fencing_token: int | None = Field(default=None, ge=1)
    goal_action_reservation_id: str | None = None
    goal_actor_principal: str | None = None

    @model_validator(mode="after")
    def normalize_goal_provenance(self) -> FleetDispatchBody:
        legacy = {
            "goal_id": self.goal_id,
            "goal_version": self.goal_version,
            "policy_revision": self.goal_policy_revision,
            "fencing_token": self.goal_fencing_token,
            "action_reservation_id": self.goal_action_reservation_id,
            "actor_principal": self.goal_actor_principal,
        }
        supplied = {key: value for key, value in legacy.items() if value is not None}
        if self.goal_provenance is not None:
            mismatched = {
                key
                for key, value in supplied.items()
                if getattr(self.goal_provenance, key) != value
            }
            if mismatched:
                raise ValueError(
                    "flat and typed goal provenance disagree: "
                    + ", ".join(sorted(mismatched))
                )
            return self
        if not supplied:
            return self
        missing = sorted(key for key, value in legacy.items() if value is None)
        if missing:
            raise ValueError(
                "incomplete goal dispatch provenance: " + ", ".join(missing)
            )
        self.goal_provenance = GoalDispatchProvenance(
            **legacy,
            authority_instance_id=self.authority_instance_id or "",
        )
        return self


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


class DispatchPriorityBody(BaseModel):
    priority: int = Field(ge=-10, le=10)
    idempotency_key: str


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
    provider: str | None = None
    model_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        pattern=r"\S",
    )
    mode_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        pattern=r"\S",
    )
    execution_contract: dict[str, Any] | None = None
    session_id: str | None = None
    progress_versions: list[int] = Field(default_factory=list, max_length=10)
    attachment_manifest: list[CardAttachment] = Field(default_factory=list)
    attachment_digest: str | None = None
    materialization_plan: dict[str, Any] | None = None
    goal_provenance: GoalDispatchProvenance | None = None


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
    auxiliary = "writer_lock" not in request.app.state.ctx.services
    service = DispatchStore(
        request.app.state.ctx.settings.data_dir,
        read_only=auxiliary,
        deferred_read_only=auxiliary,
    )
    request.app.state.ctx.register_service("dispatch_store", service)
    return service


def _assigned_mcp_environment_for_session(
    settings,
    ledger: DispatchStore,
    session: AgentSession,
) -> dict[str, str] | None:
    """Derive a restricted MCP binding from target-local durable ownership."""

    dispatch_id = str(session.dispatch_id or "").strip()
    if not dispatch_id:
        return None
    record = ledger.get(dispatch_id)
    if record is None or record.goal_provenance is None:
        return None
    provenance = record.goal_provenance
    if (
        record.dispatch_id != dispatch_id
        or record.session_id != session.id
        or record.target_instance_id != settings.instance_id
        or record.authority_instance_id != session.authority_instance_id
        or provenance.authority_instance_id != record.authority_instance_id
        or provenance.resolved_target_instance_id != record.target_instance_id
        or record.state in {"failed", "cancelled", "completed", "acknowledged"}
        or record.acknowledged_at is not None
    ):
        raise RuntimeError(
            "governed session does not match its durable assigned dispatch binding"
        )
    return assigned_service_mcp_environment(
        dispatch_id=record.dispatch_id,
        session_id=session.id,
    )


def _dispatch_public(request: Request, record: DispatchRecord) -> dict[str, Any]:
    """Resolve current membership names without mutating dispatch snapshots."""
    return canonicalize_dispatch_public(request.app.state.ctx, record)


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
    policy, explicit = _policy_service(request).effective_policy(realm_id, instance_id)
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
    old_effective = (
        old_allowed - set(old.denied_profiles) - set(old.hard_denied_profiles)
    )
    new_effective = (
        new_allowed - set(new.denied_profiles) - set(new.hard_denied_profiles)
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


def _target_goal_materialization_binding_valid(
    body: DispatchMaterializeBody,
    bound_plan: MaterializationPlan,
) -> bool:
    """Verify the authority envelope again at the target materialization sink."""

    provenance = body.goal_provenance
    if provenance is None:
        return True
    envelope = provenance.materialization_envelope
    receipt = provenance.materialization_receipt
    provider_id = str(body.provider or "").strip().lower()
    expected_receipt = (
        GoalMaterializationReceiptV1(
            envelope_digest=str(envelope.digest),
            target_instance_id=body.target_instance_id,
            provider_id=provider_id,
            model_id=body.model_id,
            mode_id=body.mode_id,
            materialization_plan_digest=canonical_materialization_digest(
                bound_plan.model_dump(mode="json")
            ),
        )
        if envelope is not None and provider_id
        else None
    )
    repository_ids = tuple(
        sorted(str(item["repository_id"]) for item in bound_plan.repositories)
    )
    attachment_ids = tuple(
        sorted(item.attachment_id for item in body.attachment_manifest)
    )
    attachment_classes = tuple(
        sorted({item.media_type.strip().lower() for item in body.attachment_manifest})
    )
    expected_claims = tuple(
        [
            GoalMaterializationResourceClaimV1(
                key=f"fleet-dispatch:{provenance.requested_placement_target}"
            ),
            *[
                GoalMaterializationResourceClaimV1(key=f"repository:{repository_id}")
                for repository_id in repository_ids
            ],
        ]
    )
    return bool(
        envelope is not None
        and receipt is not None
        and expected_receipt is not None
        and receipt == expected_receipt
        and envelope.repository_ids == repository_ids
        and envelope.attachment_ids == attachment_ids
        and envelope.attachment_classes == attachment_classes
        and envelope.resource_claims == expected_claims
        and envelope.execution_contract_digest
        == canonical_materialization_digest(body.execution_contract)
        and all(
            str(getattr(item.state, "value", item.state)) == "active"
            for item in body.attachment_manifest
        )
    )


def _target_goal_execution_identity_transition(
    recorded: DispatchRecord,
    body: DispatchMaterializeBody,
) -> GoalDispatchProvenance | None:
    """Accept only the monotonic stage-to-session identity transition."""

    current = recorded.goal_provenance
    incoming = body.goal_provenance
    if current is None or incoming is None:
        if current == incoming:
            return current
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_dispatch_provenance_mismatch",
                "recoverable": False,
            },
        )

    current_stage = _goal_materialization_stage_provenance(current)
    incoming_stage = _goal_materialization_stage_provenance(incoming)
    retry_transition = False
    if current_stage != incoming_stage:
        mutable_retry_fields = {
            "goal_version",
            "policy_revision",
            "action_reservation_id",
            "reservation_attempt",
            "retry_idempotency_key",
            "released_at",
            "release_reason",
            "execution_identity",
        }
        current_base = current.model_dump(
            mode="python",
            exclude=mutable_retry_fields,
        )
        incoming_base = incoming.model_dump(
            mode="python",
            exclude=mutable_retry_fields,
        )
        retry_transition = bool(
            current_base == incoming_base
            and incoming.action_reservation_id != current.action_reservation_id
            and incoming.reservation_attempt == current.reservation_attempt + 1
            and incoming.max_reservation_attempts == current.max_reservation_attempts
            and incoming.reservation_attempt <= incoming.max_reservation_attempts
            and incoming.goal_version >= current.goal_version
            and incoming.policy_revision >= current.policy_revision
            and incoming.retry_idempotency_key
            and incoming.released_at is None
            and incoming.release_reason is None
        )
        if not retry_transition:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_dispatch_provenance_mismatch",
                    "recoverable": False,
                },
            )
        current = incoming.model_copy(
            update={"execution_identity": current.execution_identity}
        )

    recorded_session_id = str(recorded.session_id or "").strip()
    incoming_session_id = str(body.session_id or "").strip()
    if recorded_session_id:
        stage_replay_before_authority_session_checkpoint = bool(
            not incoming_session_id
            and incoming.execution_identity is None
            and not retry_transition
        )
        if (
            incoming_session_id != recorded_session_id
            and not stage_replay_before_authority_session_checkpoint
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_execution_identity_mismatch",
                    "message": ("Target execution identity refers to another session."),
                    "recoverable": False,
                },
            )
    elif incoming_session_id:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": "Target session was not durably allocated.",
                "recoverable": False,
            },
        )

    current_identity = current.execution_identity
    incoming_identity = incoming.execution_identity
    if incoming_identity is not None:
        if not recorded_session_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_execution_identity_mismatch",
                    "message": "Execution identity requires a durable target session.",
                    "recoverable": False,
                },
            )
        expected = _expected_goal_dispatch_execution_identity(
            incoming,
            recorded_session_id,
        )
        if (
            not expected.allows_credential_upgrade_to(incoming_identity)
            or (
                incoming_identity.credential_digest is not None
                and not incoming_identity.credential_authenticated()
            )
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_execution_identity_mismatch",
                    "message": "Target execution identity is not canonical.",
                    "recoverable": False,
                },
            )

    if current_identity == incoming_identity:
        return current
    if current_identity is None and incoming_identity is not None:
        return incoming
    if (
        current_identity is not None
        and incoming_identity is not None
        and current_identity.allows_credential_upgrade_to(incoming_identity)
    ):
        return incoming
    if current_identity is not None and incoming_identity is None:
        # The worker deliberately replays the pre-session stage before an
        # identity-aware resume. Never downgrade the already bound target copy.
        return current
    raise HTTPException(
        status_code=409,
        detail={
            "code": "goal_execution_identity_mismatch",
            "message": "Target execution identity is already bound.",
            "recoverable": False,
        },
    )


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
    if body.goal_provenance is not None and body.materialization_plan is None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_materialization_binding_mismatch",
                "recoverable": False,
            },
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
        if not _target_goal_materialization_binding_valid(body, bound_plan):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_materialization_binding_mismatch",
                    "recoverable": False,
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
        target_provenance = _target_goal_execution_identity_transition(recorded, body)
        if target_provenance != recorded.goal_provenance:
            recorded.goal_provenance = target_provenance
            ledger.put(recorded)
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
            "execution_identity_digest": (
                target_provenance.execution_identity.digest
                if target_provenance is not None
                and target_provenance.execution_identity is not None
                else None
            ),
        }

    if (
        body.goal_provenance is not None
        and body.goal_provenance.execution_identity is not None
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": (
                    "Target execution identity requires an existing materialization stage."
                ),
                "recoverable": False,
            },
        )

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
        raise _dispatch_lookup_error("card", card_id or "", target=True)
    if body.project_id and not store.get_project(
        body.project_id, realm_id=body.realm_id
    ):
        raise _dispatch_lookup_error("project", body.project_id, target=True)
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
        goal_provenance=body.goal_provenance,
        request_payload={
            "provenance_version": body.provenance_version,
            "progress_versions": list(body.progress_versions),
            "provider": body.provider,
            "model_id": body.model_id,
            "mode_id": body.mode_id,
            "execution_contract": body.execution_contract,
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
) -> DispatchRecord:
    """Persist the neutral snapshot before running the read-only evaluator."""
    result = dict(
        result_override
        if result_override is not None
        else record.completion_payload or {}
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
        return record
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
    operator_requests = []
    if latest and latest.operator_input:
        operator_requests = [
            latest.operator_input.prompt
            if isinstance(latest.operator_input, OperatorInputRequestV1)
            else latest.operator_input
        ]
    current_card = _model_json(card)
    current_lane = str(current_card.get("lane") or "") or None
    authority_version = str(current_card.get("updated_at") or "") or record.card_version
    deliverables = report.model_dump(mode="json") if report else {}
    if report:
        deliverables["changed_files"] = latest.changed_file_count if latest else None
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
                record.acknowledged_at.isoformat() if record.acknowledged_at else None
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
            item.model_dump(mode="json")
            for item in (report.validations if report else [])
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
    record = ledger.put(record)

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
    record = ledger.put(record)
    evaluation = evaluator.evaluate(context)
    evaluation = evaluator.validate_result(
        evaluation,
        expected_context_digest=context.digest,
        expected_authority_version=authority_version,
    )
    mark_record_only_actions(evaluation)
    record.post_turn_evaluations.append(evaluation)
    record.post_turn_evaluations = record.post_turn_evaluations[-20:]
    return ledger.put(record)


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
    record.card_disposition_error = (
        None
        if record.card_disposition_payload
        else str(body.result.get("card_disposition_error") or "")[:1000] or None
    )
    record.reconciliation_state = "pending" if body.card_id else "not_applicable"
    record.reconciliation_reason = "Immutable agent-turn completion acknowledged."
    record.reconciliation_updated_at = record.completion_received_at
    record = ledger.transition(
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
        record = ledger.put(record)
        record = _record_post_turn_evaluation(
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
    record = ledger.put(record)
    record = _record_post_turn_evaluation(
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
        request.app.state.ctx.store.get_card(record.card_id, realm_id=record.realm_id)
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
            if str(item.get("prompt_id") or item.get("idempotency_key")) == body.turn_id
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
    record = ledger.transition(
        record,
        record.state,
        "Follow-up agent turn ended; immutable dispatch completion retained.",
        detail={
            "turn_id": body.turn_id,
            "dispatch_state_retained": record.state,
        },
    )
    record = _record_post_turn_evaluation(
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


def _assigned_local_dispatch(request: Request) -> DispatchRecord:
    """Authenticate one restricted local session capability and derive its dispatch."""

    capability = getattr(request.state, "assigned_session_capability", None) or ""
    session_id = request.headers.get("X-PA-Assigned-Session-ID", "").strip()
    asserted_dispatch_id = request.headers.get(
        "X-PA-Assigned-Dispatch-ID", ""
    ).strip()
    ctx = request.app.state.ctx
    session = ctx.store.get_session(session_id) if session_id else None
    durable_dispatch_id = str(getattr(session, "dispatch_id", None) or "").strip()
    ledger = _dispatch_store(request)
    record = ledger.get(durable_dispatch_id) if durable_dispatch_id else None
    manager = ctx.services.get("instance_agent")
    runtime = manager.get(session_id) if manager is not None and session_id else None
    expected = ""
    if session is not None and record is not None:
        expected = assigned_service_session_capability(
            secret=ctx.settings.session_secret,
            dispatch_id=record.dispatch_id,
            session_id=session.id,
            target_instance_id=ctx.settings.instance_id,
        )
    if (
        not capability
        or not expected
        or not hmac.compare_digest(capability, expected)
        or asserted_dispatch_id != durable_dispatch_id
        or record is None
        or record.dispatch_id != durable_dispatch_id
        or record.session_id != session.id
        or record.target_instance_id != ctx.settings.instance_id
        or session.authority_instance_id != record.authority_instance_id
        or session.status in {"closed", "quiesced", "configuration_failed"}
        or runtime is None
        or getattr(runtime, "_closed", False)
        or not getattr(runtime, "connected", False)
        or record.state in {"failed", "cancelled", "completed", "acknowledged"}
        or record.acknowledged_at is not None
        or record.goal_provenance is None
    ):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "invalid_assigned_session_capability",
                "message": (
                    "This tool requires the live restricted capability for its "
                    "exact local dispatch session."
                ),
            },
        )
    return record


async def _proxy_assigned_session_operation(
    request: Request,
    operation: str,
    *,
    payload: dict[str, Any] | None = None,
    expected_version: int | None = None,
    policy_revision: int | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    record = _assigned_local_dispatch(request)
    proxy_body = AssignedServiceProxyRequest(
        payload=payload or {},
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )
    if record.authority_instance_id == request.app.state.ctx.settings.instance_id:
        return await _apply_assigned_service_operation(
            request,
            record.dispatch_id,
            operation,
            proxy_body,
            trusted_caller_instance_id=request.app.state.ctx.settings.instance_id,
        )
    return await _peer_authority_json(
        request,
        record.authority_instance_id,
        "POST",
        f"dispatch-jobs/{quote(record.dispatch_id, safe='')}/assigned-service/{operation}",
        body=proxy_body.model_dump(mode="json"),
    )


@router.get("/goal-assigned-session/goal")
async def get_assigned_session_goal(
    request: Request,
    offset: Annotated[int, Query(ge=0, le=100_000)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> dict[str, Any]:
    return await _proxy_assigned_session_operation(
        request,
        "goal",
        payload={"offset": offset, "limit": limit},
    )


@router.get("/goal-assigned-session/dispatch")
async def get_assigned_session_dispatch(request: Request) -> dict[str, Any]:
    return await _proxy_assigned_session_operation(request, "dispatch")


@router.post("/goal-assigned-session/proposals")
async def proxy_assigned_session_proposal(
    request: Request,
    body: AssignedServiceGoalProposalCreate,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> dict[str, Any]:
    return await _proxy_assigned_session_operation(
        request,
        "proposals",
        payload=body.model_dump(mode="json"),
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )


@router.post("/goal-assigned-session/evidence")
async def proxy_assigned_session_evidence(
    request: Request,
    body: AssignedServiceGoalEvidenceCreate,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> dict[str, Any]:
    return await _proxy_assigned_session_operation(
        request,
        "evidence",
        payload=body.model_dump(mode="json"),
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )


@router.post("/goal-assigned-session/audit")
async def proxy_assigned_session_audit(
    request: Request,
    body: AssignedServiceGoalAuditCreate,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> dict[str, Any]:
    return await _proxy_assigned_session_operation(
        request,
        "audit",
        payload=body.model_dump(mode="json"),
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )


@router.post("/goal-assigned-session/progress")
async def proxy_assigned_session_progress(
    request: Request,
    body: ExplicitProgressCheckpointV1,
) -> dict[str, Any]:
    return await _proxy_assigned_session_operation(
        request,
        "progress",
        payload=body.model_dump(mode="json"),
        idempotency_key=body.idempotency_key,
    )


def _assigned_authority_dispatch(
    request: Request,
    dispatch_id: str,
    *,
    required_roles: set[GoalActorRole],
    sink: str,
    trusted_caller_instance_id: str | None = None,
) -> tuple[GoalAssignedServiceAuthorization, DispatchRecord]:
    """Derive the complete assigned identity from authority-owned durable state."""

    ctx = request.app.state.ctx
    ledger = _dispatch_store(request)
    record = ledger.get(dispatch_id)
    if trusted_caller_instance_id is None:
        _require_instance(request)
        caller = request.headers.get("X-PA-Origin-Instance-ID", "").strip()
        _fleet_instance_or_404(request, caller)
    else:
        caller = trusted_caller_instance_id
        if caller != ctx.settings.instance_id:
            raise HTTPException(
                status_code=403,
                detail={"code": "untrusted_assigned_dispatch_caller"},
            )
    if record is None:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    if (
        record.authority_instance_id != ctx.settings.instance_id
        or record.target_instance_id != caller
        or record.session_id is None
        or record.state in {"failed", "cancelled", "completed", "acknowledged"}
        or record.acknowledged_at is not None
    ):
        raise HTTPException(
            status_code=403,
            detail={"code": "assigned_dispatch_caller_mismatch"},
        )
    record = _validate_goal_dispatch_record(ctx, ledger, record, sink=sink)
    provenance = record.goal_provenance
    identity = (
        getattr(provenance, "execution_identity", None)
        if provenance is not None
        else None
    )
    envelope = (
        getattr(provenance, "materialization_envelope", None)
        if provenance is not None
        else None
    )
    try:
        scope = _goal_dispatch_assigned_service_scope(
            provenance,
            identity,
            record.dispatch_id,
        )
        authorization = ctx.require_service(
            "goal_governance"
        ).resolve_assigned_service_binding(scope, required_roles=required_roles)
    except (
        AttributeError,
        GoalAssignedServiceCredentialError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        raise HTTPException(
            status_code=403,
            detail={"code": "invalid_assigned_dispatch_binding"},
        ) from exc
    envelope_role = getattr(envelope, "service_role", None)
    envelope_role = (
        envelope_role.value if isinstance(envelope_role, GoalActorRole) else envelope_role
    )
    if (
        authorization.run is not None
        or provenance is None
        or envelope is None
        or getattr(envelope, "work_package_id", None) != scope.work_package_id
        or envelope_role != scope.service_role.value
        or provenance.action_reservation_id
        != authorization.work_package.action_reservation_id
        or provenance.fencing_token != scope.fencing_token
        or getattr(provenance, "resolved_target_instance_id", None)
        != scope.target_instance_id
        or str(getattr(provenance, "provider_id", None) or "").strip().lower()
        != scope.provider_id.strip().lower()
        or record.session_id != scope.session_id
        or record.target_instance_id != scope.target_instance_id
        or getattr(identity, "credential_digest", None)
        != authorization.binding.credential_digest
        or getattr(identity, "credential_expires_at", None)
        != authorization.binding.expires_at
    ):
        raise HTTPException(
            status_code=403,
            detail={"code": "assigned_dispatch_provenance_mismatch"},
        )
    request.state.principal_id = scope.assigned_service_principal
    request.state.authenticated_instance_id = scope.authority_instance_id
    return authorization, record


def _assigned_goal_projection(
    authorization: GoalAssignedServiceAuthorization,
    *,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, Any]:
    try:
        return assigned_goal_projection(
            authorization,
            offset=offset,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={"code": "invalid_assigned_goal_page"},
        ) from exc


def _assigned_dispatch_projection(record: DispatchRecord) -> dict[str, Any]:
    """Return bounded execution state without internal routing or identity data."""

    latest = record.latest_progress
    latest_progress = None
    if latest is not None:
        latest_progress = {
            "kind": latest.kind.value,
            "sequence": latest.sequence,
            "occurred_at": latest.occurred_at.isoformat(),
            "last_activity_at": latest.last_activity_at.isoformat(),
            "phase": latest.phase.value,
            "summary": latest.summary,
            "branch": latest.branch,
            "commit_sha": latest.commit_sha,
            "pr_url": latest.pr_url,
            "pr_number": latest.pr_number,
            "changed_file_count": latest.changed_file_count,
            "validations": [
                item.model_dump(mode="json") for item in latest.validations
            ],
            "blockers": latest.blockers,
            "retry_reason": latest.retry_reason,
        }
    return {
        "state": record.state,
        "stage_attempts": record.stage_attempts,
        "attempts": record.attempts,
        "last_error": str(record.last_error or "")[:8_000] or None,
        "error_code": record.error_code,
        "recoverable": record.recoverable,
        "progress": latest_progress,
        "created_at": record.created_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
    }


async def _apply_assigned_service_operation(
    request: Request,
    dispatch_id: str,
    operation: Literal[
        "goal", "dispatch", "proposals", "evidence", "audit", "progress"
    ],
    body: AssignedServiceProxyRequest,
    *,
    trusted_caller_instance_id: str | None = None,
) -> dict[str, Any]:
    required_roles = (
        {GoalActorRole.VERIFIER}
        if operation == "audit"
        else {GoalActorRole.EXECUTOR, GoalActorRole.VERIFIER}
    )
    authorization, record = _assigned_authority_dispatch(
        request,
        dispatch_id,
        required_roles=required_roles,
        sink=f"assigned-service-{operation}",
        trusted_caller_instance_id=trusted_caller_instance_id,
    )
    if operation == "goal":
        try:
            offset = int(body.payload.get("offset", 0))
            limit = int(body.payload.get("limit", 50))
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=422,
                detail={"code": "invalid_assigned_goal_page"},
            ) from exc
        return {
            "goal": _assigned_goal_projection(
                authorization,
                offset=offset,
                limit=limit,
            )
        }
    if operation == "dispatch":
        return {"dispatch": _assigned_dispatch_projection(record)}
    if operation == "progress":
        checkpoint = ExplicitProgressCheckpointV1.model_validate(body.payload)
        service = request.app.state.ctx.services.get("progress_service")
        if not service:
            raise HTTPException(
                status_code=503,
                detail={"code": "progress_reporting_unavailable"},
            )
        try:
            result = await service.explicit(
                record.dispatch_id,
                checkpoint,
                originating_instance_id=authorization.scope.target_instance_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if checkpoint.operator_input:
            # Progress itself is inert telemetry. Any operator-facing side effect
            # gets a fresh scope/fence decision after the asynchronous write.
            _, record = _assigned_authority_dispatch(
                request,
                dispatch_id,
                required_roles=required_roles,
                sink="assigned-service-progress-operator-input",
                trusted_caller_instance_id=trusted_caller_instance_id,
            )
            await _create_operator_input_notification(
                request,
                record,
                checkpoint.operator_input,
                idempotency_key=checkpoint.idempotency_key
                or f"checkpoint:{record.dispatch_id}:{result.sequence}",
                kind=InteractionKind.MCP_OPERATOR_INPUT,
            )
        return result.model_dump(mode="json")
    if (
        body.expected_version is None
        or body.policy_revision is None
        or not body.idempotency_key
    ):
        raise HTTPException(
            status_code=422,
            detail={"code": "assigned_mutation_context_missing"},
        )
    from pa.modules.goals import (
        apply_assigned_service_goal_audit,
        apply_assigned_service_goal_evidence,
        apply_assigned_service_goal_proposal,
    )

    common = {
        "expected_version": body.expected_version,
        "policy_revision": body.policy_revision,
        "idempotency_key": body.idempotency_key,
    }
    if operation == "proposals":
        return apply_assigned_service_goal_proposal(
            request,
            AssignedServiceGoalProposalCreate.model_validate(body.payload),
            authorization,
            **common,
        )
    if operation == "evidence":
        return apply_assigned_service_goal_evidence(
            request,
            AssignedServiceGoalEvidenceCreate.model_validate(body.payload),
            authorization,
            **common,
        )
    return apply_assigned_service_goal_audit(
        request,
        AssignedServiceGoalAuditCreate.model_validate(body.payload),
        authorization,
        **common,
    )


@router.post(
    "/fleet/dispatch-jobs/{dispatch_id}/assigned-service/{operation}"
)
async def apply_assigned_service_operation(
    request: Request,
    dispatch_id: str,
    operation: Literal[
        "goal", "dispatch", "proposals", "evidence", "audit", "progress"
    ],
    body: AssignedServiceProxyRequest,
) -> dict[str, Any]:
    return await _apply_assigned_service_operation(
        request,
        dispatch_id,
        operation,
        body,
    )


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
    if body.operator_input:
        await _create_operator_input_notification(
            request,
            record,
            body.operator_input,
            idempotency_key=body.idempotency_key
            or f"checkpoint:{dispatch_id}:{result.sequence}",
            kind=InteractionKind.MCP_OPERATOR_INPUT,
        )
    return result.model_dump(mode="json")
async def _create_operator_input_notification(
    request: Request,
    record: DispatchRecord,
    operator_input: str | OperatorInputRequestV1 | dict[str, Any],
    *,
    idempotency_key: str,
    kind: InteractionKind,
) -> dict[str, Any]:
    structured = (
        OperatorInputRequestV1.model_validate(operator_input)
        if isinstance(operator_input, dict)
        else operator_input
    )
    if isinstance(structured, str):
        prompt = structured
        request_id = idempotency_key
        response_schema = None
        choices = []
        allow_freeform = True
        allow_cancel = True
        sensitive = False
        deadline = None
    else:
        prompt = structured.prompt
        request_id = structured.request_id or idempotency_key
        response_schema = structured.response_schema
        choices = [
            InteractionChoice(
                id=item.id,
                label=item.label,
                description=item.description,
                value=item.value,
            )
            for item in structured.choices
        ]
        allow_freeform = structured.allow_freeform
        allow_cancel = structured.allow_cancel
        sensitive = structured.sensitive
        deadline = structured.deadline
    ctx = request.app.state.ctx
    data = NotificationCreate(
        realm_id=record.realm_id,
        visibility=NotificationVisibility.REALM,
        type=NotificationType.INTERACTION,
        priority=NotificationPriority.HIGH,
        title="Operator input requested",
        body=prompt,
        summary=prompt[:1000],
        card_id=record.card_id,
        session_id=record.session_id,
        dispatch_id=record.dispatch_id,
        project_id=record.project_id,
        destination_url=(
            f"/agent?session={record.session_id}" if record.session_id else "/fleet"
        ),
        owner_instance_id=record.target_instance_id,
        owner_url=(
            record.authority_url
            if record.target_instance_id == record.authority_instance_id
            else None
        ),
        distributable=True,
        deduplication_key=f"operator-input:{record.dispatch_id}:{request_id}",
        actions=[
            NotificationAction(
                id="respond",
                kind="respond",
                label="Respond",
                method="POST",
                input_schema=response_schema,
            )
        ],
        interaction=InteractionRequest(
            request_id=request_id,
            kind=kind,
            prompt=prompt,
            response_schema=response_schema,
            choices=choices,
            allow_freeform=allow_freeform,
            allow_cancel=allow_cancel,
            sensitive=sensitive,
            protocol_method="pa/report_dispatch_progress.operator_input",
            protocol_request_id=request_id,
            continuation_mode="prompt",
            deadline=deadline,
        ),
        expires_at=deadline,
    )
    create = ctx.require_service("notifications").create
    async_runtime = ctx.services.get("async_runtime")
    if async_runtime:
        notification = await async_runtime.run_blocking(
            "sqlite.notification_create",
            create,
            data,
            principal_id=get_principal_id(request),
        )
    else:
        notification = await asyncio.to_thread(
            create, data, principal_id=get_principal_id(request)
        )
    return notification.public_dict()


def _membership_convergence_snapshot(ctx: AppContext) -> dict[str, Any]:
    try:
        store = ctx.require_service("membership_convergence")
    except KeyError, RuntimeError:
        store = MembershipConvergenceStore(
            ctx.settings.data_dir, ctx.settings.instance_id
        )
    return store.snapshot()


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
    except KeyError, RuntimeError:
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
        "credential_rotation": CredentialRotationStore.public(
            CredentialRotationStore(settings.data_dir).load()
        )
        or {"status": "idle", "peers": {}},
        "membership_convergence": _membership_convergence_snapshot(ctx),
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


_workshop_refresh_lock = asyncio.Lock()
_workshop_refresh_tasks: dict[
    tuple[str, bool], asyncio.Task[dict[str, dict[str, dict[str, Any]]]]
] = {}


async def _refresh_workshop_dimensions(
    ctx: Any, instances: list[FleetInstance], *, force: bool
) -> dict[str, dict[str, dict[str, Any]]]:
    """Refresh the small Workshop projection as one coalesced fleet operation."""
    key = (str(ctx.settings.data_dir), force)
    async with _workshop_refresh_lock:
        task = _workshop_refresh_tasks.get(key)
        if task is None or task.done():
            active = [item for item in instances if item.lifecycle_state == "active"]

            async def refresh() -> dict[str, dict[str, dict[str, Any]]]:
                requests = [
                    (instance, dimension)
                    for instance in active
                    for dimension in (
                        "reachability",
                        "activity",
                        "providers",
                        "sync",
                    )
                ]
                results = await asyncio.gather(
                    *(
                        probe_dimension(ctx, instance, dimension, force=force)
                        for instance, dimension in requests
                    ),
                    return_exceptions=True,
                )
                observations: dict[str, dict[str, dict[str, Any]]] = {}
                for (instance, dimension), result in zip(
                    requests, results, strict=True
                ):
                    if isinstance(result, BaseException):
                        continue
                    observations.setdefault(instance.instance_id, {})[dimension] = (
                        result
                    )
                return observations

            task = asyncio.create_task(refresh())
            _workshop_refresh_tasks[key] = task
    try:
        return await asyncio.shield(task)
    finally:
        if task.done():
            async with _workshop_refresh_lock:
                if _workshop_refresh_tasks.get(key) is task:
                    _workshop_refresh_tasks.pop(key, None)


def _build_workshop(
    ctx: Any,
    instances: list[FleetInstance],
    routes: list[Any],
    observations: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> dict:
    fleet_overview = build_overview(ctx, instances, routes)
    if observations:
        for node in fleet_overview.get("nodes", []):
            probed = observations.get(str(node.get("id")))
            if probed:
                node.setdefault("dimensions", {}).update(probed)
    return build_workshop_snapshot(ctx, fleet_overview)


def _workshop_stream_iteration(
    snapshot: dict[str, Any], last_digest: str, sequence: int
) -> tuple[str, str, int]:
    digest = hashlib.sha256(
        json.dumps(
            workshop_semantic_snapshot(snapshot), sort_keys=True, default=str
        ).encode()
    ).hexdigest()
    if digest == last_digest:
        return ": workshop heartbeat\n\n", last_digest, sequence
    sequence += 1
    return (
        f"id: {sequence}\nevent: snapshot\ndata: "
        + json.dumps(snapshot, default=str)
        + "\n\n",
        digest,
        sequence,
    )


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
        dispatch_queue_capacity=settings.dispatch_queue_capacity,
        dispatch_provider_queue_capacities=dict(
            settings.dispatch_provider_queue_capacities
        ),
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


def _adopt_canonical_local_name(ctx: AppContext) -> bool:
    """Keep runtime/config/service identity behind the canonical UUID projection."""
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    member = fleet.get_instance(ctx.settings.instance_id)
    if member is None or member.name == ctx.settings.instance_name:
        return False
    from pa.domain.instance_config import update_instance_config
    from pa.fleet.join import refresh_service_env

    ctx.settings.instance_name = member.name
    update_instance_config(ctx.settings.data_dir, instance_name=member.name)
    refresh_service_env(ctx.settings)
    return True


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
        result["local_name_adopted"] = _adopt_canonical_local_name(ctx)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    result["routes"] = ctx.require_service("peer_table").reconcile_membership(
        ctx.require_service("fleet_registry").list_instances(),
        realms=list(ctx.settings.subscribed_realms),
        local_instance_id=ctx.settings.instance_id,
    )
    return result


@router.get("/fleet/membership/convergence")
def fleet_membership_convergence(request: Request) -> dict[str, Any]:
    """Expose durable per-peer generation delivery and actionable failures."""
    require_user(request)
    return request.app.state.ctx.require_service("membership_convergence").snapshot()


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
            dispatch_queue_capacity=ctx.settings.dispatch_queue_capacity,
            dispatch_provider_queue_capacities=dict(
                ctx.settings.dispatch_provider_queue_capacities
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
async def fleet_workshop(request: Request, refresh: bool = False) -> dict:
    """Return one canonical, presentation-ready Workshop snapshot."""
    require_user(request)
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    peer_table: PeerTable = ctx.require_service("peer_table")
    instances = list(fleet.list_instances())
    observations = await _refresh_workshop_dimensions(ctx, instances, force=refresh)
    return _build_workshop(ctx, instances, list(peer_table.all_routes()), observations)


@router.get("/fleet/workshop/events")
async def fleet_workshop_events(request: Request) -> StreamingResponse:
    """Stream one fleet-wide Workshop projection with bounded probe fallback."""
    require_user(request)
    ctx = request.app.state.ctx
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    peer_table: PeerTable = ctx.require_service("peer_table")

    async def stream() -> AsyncIterator[str]:
        last_digest = ""
        sequence = 0
        while not await request.is_disconnected():
            instances = list(fleet.list_instances())
            observations = await _refresh_workshop_dimensions(
                ctx, instances, force=False
            )
            snapshot = _build_workshop(
                ctx, instances, list(peer_table.all_routes()), observations
            )
            event, last_digest, sequence = _workshop_stream_iteration(
                snapshot, last_digest, sequence
            )
            yield event
            await asyncio.sleep(2.0)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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
    dispatch_queue_capacity = body.get("dispatch_queue_capacity")
    dispatch_provider_queue_capacities = body.get(
        "dispatch_provider_queue_capacities", {}
    )
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
            dispatch_queue_capacity=dispatch_queue_capacity,
            dispatch_provider_queue_capacities=(dispatch_provider_queue_capacities),
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


def _commit_local_canonical_name(ctx: AppContext, name: str) -> dict[str, Any]:
    """Persist the canonical local label across config, runtime, and service env."""
    from pa.domain.instance_config import update_instance_config
    from pa.fleet.join import refresh_service_env

    try:
        update_instance_config(ctx.settings.data_dir, instance_name=name)
        ctx.settings.instance_name = name
        service_refreshed = refresh_service_env(ctx.settings)
    except Exception as exc:  # noqa: BLE001 - every partial step needs recovery state
        return {
            "state": "recovery_required",
            "error": str(exc)[:500],
            "action": "Retry this rename after restoring write access to config/service state.",
        }
    return {
        "state": "committed",
        "service_environment_refreshed": service_refreshed,
    }


@router.post("/fleet/instances/{instance_id}/rename")
async def rename_instance(request: Request, instance_id: str, body: dict) -> dict:
    """Authorize, fence, audit, persist, and roll out a stable-UUID rename."""
    ctx = request.app.state.ctx
    principal = getattr(request.state, "principal_id", "")
    if getattr(request.state, "user", None):
        actor = principal or "user:local"
    elif getattr(request.state, "instance_authenticated", False):
        caller = request.headers.get("X-PA-Origin-Instance-ID", "")
        if caller != instance_id:
            raise HTTPException(
                status_code=403,
                detail="An instance may rename only its own stable UUID.",
            )
        actor = f"instance:{caller}"
    else:
        raise HTTPException(status_code=401, detail="Authentication required")
    if isinstance(body.get("expected_generation"), bool) or not isinstance(
        body.get("expected_generation"), int
    ):
        raise HTTPException(
            status_code=428,
            detail="expected_generation is required for a fenced rename.",
        )
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    try:
        renamed = fleet.rename_instance(
            instance_id,
            str(body.get("name") or ""),
            actor=actor,
            source=str(body.get("source") or "fleet.rename")[:120],
            expected_generation=body["expected_generation"],
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    local_state = (
        _commit_local_canonical_name(ctx, renamed.name)
        if instance_id == ctx.settings.instance_id
        else {"state": "not_local"}
    )
    ctx.require_service("peer_table").reconcile_membership(
        fleet.list_instances(),
        realms=list(ctx.settings.subscribed_realms),
        local_instance_id=ctx.settings.instance_id,
    )
    return {
        "instance": renamed.model_dump(mode="json"),
        "generation": fleet.generation,
        "local_transaction": local_state,
        "rollout": await _rollout_membership(request),
    }


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
        "dispatch_queue_capacity",
        "dispatch_provider_queue_capacities",
        "relay_enabled",
        "lifecycle_state",
        "credential_fingerprint",
        "expected_generation",
    }
    unknown = set(body) - allowed
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported membership fields: {sorted(unknown)}",
        )
    data = current.model_dump()
    renamed_member = False
    expected_generation = body.get("expected_generation")
    changes = {
        key: value for key, value in body.items() if key != "expected_generation"
    }
    data.update(changes)
    if data.get("lifecycle_state") not in {"active", "disabled"}:
        raise HTTPException(
            status_code=422, detail="lifecycle_state must be active or disabled"
        )
    try:
        if "name" in changes and changes["name"] != current.name:
            if isinstance(expected_generation, bool) or not isinstance(
                expected_generation, int
            ):
                raise HTTPException(
                    status_code=428,
                    detail="expected_generation is required for a fenced rename.",
                )
            current = fleet.rename_instance(
                instance_id,
                str(changes.pop("name")),
                actor=f"user:{get_principal_id(request)}",
                source="fleet.instance.patch",
                expected_generation=expected_generation,
            )
            renamed_member = True
            data = current.model_dump()
            data.update(changes)
        updated = (
            fleet.upsert_instance(
                FleetInstance.model_validate(data),
                actor=f"user:{get_principal_id(request)}",
            )
            if changes
            else current
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    local_transaction = (
        _commit_local_canonical_name(ctx, updated.name)
        if renamed_member and instance_id == ctx.settings.instance_id
        else {"state": "not_applicable"}
    )
    ctx.require_service("peer_table").reconcile_membership(
        fleet.list_instances(),
        realms=list(ctx.settings.subscribed_realms),
        local_instance_id=ctx.settings.instance_id,
    )
    return {
        "instance": updated.model_dump(mode="json"),
        "generation": fleet.generation,
        "local_transaction": local_transaction,
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
            except TimeoutError:
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
                except TimeoutError:
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
                        except TimeoutError:
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
                        except TimeoutError:
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


def _bootstrap_store(request: Request) -> BootstrapJobStore:
    return request.app.state.ctx.require_service("fleet_bootstrap_job_store")


def _bootstrap_public(job: BootstrapJob) -> dict[str, Any]:
    data = job.model_dump(mode="json")
    data["terminal"] = job.state.value in {
        "ready",
        "partially_ready",
        "blocked",
        "cancelled",
    }
    data["resume_supported"] = job.state in {
        BootstrapState.PLANNED,
        BootstrapState.RETRYABLE,
        BootstrapState.WAITING_INPUT,
    }
    data["log"] = "\n".join(event.message for event in job.log_events[-200:])
    return data


def _schedule_bootstrap(request: Request, job: BootstrapJob) -> BootstrapJob:
    existing = _bootstrap_tasks.get(job.job_id)
    if existing and not existing.done():
        return job
    ctx = request.app.state.ctx
    store = _bootstrap_store(request)
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    job.cancel_requested = False
    if job.state == BootstrapState.WAITING_INPUT and job.required_input:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "bootstrap_input_required",
                "required_input": job.required_input.model_dump(mode="json"),
            },
        )
    if job.state.value in {"ready", "partially_ready", "blocked", "cancelled"}:
        raise HTTPException(status_code=409, detail="Bootstrap job is terminal")

    async def runner() -> None:
        try:
            await run_bootstrap_job(
                ctx.settings,
                fleet,
                store,
                job,
                domain_store=ctx.store,
                author_instance_id=ctx.settings.instance_id,
                async_runtime=ctx.require_service("async_runtime"),
                http_client=ctx.services.get("fleet_http_client"),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record = job.phase_record(job.current_phase)
            record.state = PhaseState.FAILED
            record.completed_at = datetime.now(UTC)
            record.summary = redact_log_text(exc)
            record.recovery_action = (
                "Retry from the durable phase checkpoint; report the sanitized "
                "internal error if it repeats."
            )
            job.state = BootstrapState.RETRYABLE
            job.readiness_reason = record.summary
            store.secrets.clear(job.job_id)
            store.append(
                job,
                category="unexpected_failure",
                message=record.summary,
                phase=job.current_phase,
                level="error",
            )
            store.save(job)
        finally:
            _bootstrap_tasks.pop(job.job_id, None)

    _bootstrap_tasks[job.job_id] = asyncio.create_task(
        runner(), name=f"pa-fleet-bootstrap-{job.job_id}"
    )
    return job


@router.post("/fleet/bootstrap/discover")
async def bootstrap_discover(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    require_user(request)
    target = str(body.get("target") or "").strip()
    if not target:
        raise HTTPException(status_code=400, detail="target is required")
    try:
        discovery = await discover_target(target)
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail={"code": "target_discovery_failed", "message": str(exc)},
        ) from exc
    return {
        "schema_version": 1,
        "discovery": discovery.model_dump(mode="json"),
        "requires_host_key_confirmation": discovery.host_key_state != "known",
        "mutated": False,
    }


@router.get("/fleet/bootstrap-jobs")
def list_bootstrap_jobs(
    request: Request, include_terminal: bool = True
) -> list[dict[str, Any]]:
    require_user(request)
    return [
        _bootstrap_public(job)
        for job in _bootstrap_store(request).list(include_terminal=include_terminal)
    ]


@router.get("/fleet/bootstrap-jobs/incomplete")
def list_incomplete_bootstrap_jobs(request: Request) -> list[dict[str, Any]]:
    require_user(request)
    return [
        _bootstrap_public(job)
        for job in _bootstrap_store(request).list(include_terminal=False)
    ]


@router.post("/fleet/bootstrap-jobs", status_code=201)
async def create_bootstrap_job(
    request: Request, body: dict[str, Any]
) -> dict[str, Any]:
    require_user(request)
    raw = dict(body.get("request") or body)
    idempotency_key = str(
        body.get("idempotency_key") or raw.pop("idempotency_key", "")
    ).strip()
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="idempotency_key is required")
    target = str(raw.get("target") or "").strip()
    if not target:
        host = str(raw.get("host") or "").strip()
        user = str(raw.get("user") or "").strip()
        target = f"{user}@{host}" if user and host else host
        raw["target"] = target
    secrets = {
        "password": str(raw.pop("password", "") or ""),
        "passphrase": str(raw.pop("passphrase", "") or ""),
        "sudo_password": str(raw.pop("sudo_password", "") or ""),
    }
    auto_start = bool(body.get("start", False))
    try:
        discovery = await discover_target(target)
        raw.setdefault("host", discovery.host)
        raw.setdefault("user", discovery.user)
        raw.setdefault("port", discovery.port)
        if discovery.identity_files:
            raw.setdefault("identity_file", discovery.identity_files[0])
        raw.setdefault("proxy_jump", discovery.proxy_jump)
        bootstrap_request = BootstrapRequest.model_validate(raw)
        job, duplicate = _bootstrap_store(request).create(
            bootstrap_request,
            idempotency_key=idempotency_key,
            actor=get_principal_id(request),
            authority_instance_id=request.app.state.ctx.settings.instance_id,
            authority_url=(
                request.app.state.ctx.settings.instance_url
                or f"http://127.0.0.1:{request.app.state.ctx.settings.port}"
            ),
            discovery=discovery,
            secrets=secrets,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if auto_start:
        _schedule_bootstrap(request, job)
    response = _bootstrap_public(job)
    response["duplicate"] = duplicate
    return response


@router.get("/fleet/bootstrap-jobs/{job_id}")
def get_bootstrap_job(request: Request, job_id: str) -> dict[str, Any]:
    require_user(request)
    job = _bootstrap_store(request).get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Bootstrap job not found")
    return _bootstrap_public(job)


@router.post("/fleet/bootstrap-jobs/{job_id}/start", status_code=202)
def start_bootstrap(request: Request, job_id: str) -> dict[str, Any]:
    require_user(request)
    job = _bootstrap_store(request).get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Bootstrap job not found")
    _schedule_bootstrap(request, job)
    return _bootstrap_public(job)


@router.post("/fleet/bootstrap-jobs/{job_id}/resume", status_code=202)
def resume_bootstrap(request: Request, job_id: str) -> dict[str, Any]:
    return start_bootstrap(request, job_id)


@router.post("/fleet/bootstrap-jobs/{job_id}/retry", status_code=202)
def retry_bootstrap(request: Request, job_id: str) -> dict[str, Any]:
    require_user(request)
    store = _bootstrap_store(request)
    job = store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Bootstrap job not found")
    if job.state not in {BootstrapState.RETRYABLE, BootstrapState.BLOCKED}:
        raise HTTPException(
            status_code=409, detail="Only failed or blocked bootstrap jobs can retry"
        )
    current = job.phase_record(job.current_phase)
    current.state = PhaseState.PENDING
    current.completed_at = None
    job.state = BootstrapState.RETRYABLE
    job.readiness_reason = "Retry requested from the failed phase."
    store.save(job)
    _schedule_bootstrap(request, job)
    return _bootstrap_public(job)


@router.post("/fleet/bootstrap-jobs/{job_id}/cancel", status_code=202)
def cancel_bootstrap(request: Request, job_id: str) -> dict[str, Any]:
    require_user(request)
    store = _bootstrap_store(request)
    job = store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Bootstrap job not found")
    if job.state.value in {"ready", "partially_ready", "blocked", "cancelled"}:
        return _bootstrap_public(job)
    job.cancel_requested = True
    job.state = BootstrapState.CANCELLING
    job.readiness_reason = (
        "Cancellation requested; the job will stop at the next safe phase boundary."
    )
    store.append(
        job,
        category="cancellation_requested",
        message=job.readiness_reason,
        phase=job.current_phase,
        level="audit",
    )
    if not (
        _bootstrap_tasks.get(job.job_id) and not _bootstrap_tasks[job.job_id].done()
    ):
        job.state = BootstrapState.CANCELLED
        job.completed_at = datetime.now(UTC)
        store.secrets.clear(job.job_id)
        store.save(job)
    return _bootstrap_public(job)


@router.post("/fleet/bootstrap-jobs/{job_id}/input")
def submit_bootstrap_input(
    request: Request, job_id: str, body: dict[str, Any]
) -> dict[str, Any]:
    require_user(request)
    store = _bootstrap_store(request)
    job = store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Bootstrap job not found")
    kind = str(body.get("kind") or "")
    value = str(body.get("value") or "")
    try:
        job = accept_bootstrap_input(
            store,
            job,
            kind=kind,
            value=value,
            confirmed=bool(body.get("confirmed")),
            details=body.get("details")
            if isinstance(body.get("details"), dict)
            else {},
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    body.pop("value", None)
    return _bootstrap_public(job)


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
    raise HTTPException(
        status_code=404,
        detail={
            "code": "fleet_instance_not_found",
            "message": "The requested fleet instance is not in the authoritative membership.",
            "instance_id": instance_id,
            "recoverable": False,
        },
    )


def _dispatch_lookup_error(
    entity: str, entity_id: str, *, target: bool = False
) -> HTTPException:
    """Keep entity misses distinct from route/version 404s."""
    return HTTPException(
        status_code=409 if target else 404,
        detail={
            "code": f"{'target_' if target else ''}{entity}_not_found",
            "message": f"The required {entity} is not available in this realm projection.",
            f"{entity}_id": entity_id,
            "recoverable": target,
            "retry_after": 1 if target else None,
            "retry_after_convergence": target,
        },
        headers={"Retry-After": "1"} if target else None,
    )


def _peer_headers_ctx(ctx: AppContext) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "X-PA-Origin-Instance-ID": ctx.settings.instance_id,
    }
    if ctx.settings.sync_token:
        headers["Authorization"] = f"Bearer {ctx.settings.sync_token}"
    return headers


def _peer_headers(request: Request) -> dict[str, str]:
    return _peer_headers_ctx(request.app.state.ctx)


async def _deliver_membership(
    ctx: AppContext,
    client: httpx.AsyncClient,
    *,
    members: list[FleetInstance] | None = None,
    force: bool = False,
) -> list[dict[str, Any]]:
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    roster = members if members is not None else fleet.list_instances()
    generation = fleet.generation
    try:
        convergence = ctx.require_service("membership_convergence")
    except KeyError, RuntimeError:
        convergence = MembershipConvergenceStore(
            ctx.settings.data_dir, ctx.settings.instance_id
        )
    convergence.plan(generation, roster)
    by_id = {member.instance_id: member for member in roster}
    targets = convergence.snapshot()["peers"] if force else convergence.due(generation)
    if not ctx.settings.sync_token:
        for item in targets:
            convergence.failed(
                item["instance_id"],
                generation,
                "Fleet authentication is not configured.",
                incompatible=True,
            )
        return convergence.snapshot()["peers"]
    envelope = _signed_membership(ctx)
    for item in targets:
        member = by_id.get(item["instance_id"])
        if member is None:
            continue
        try:
            response = await client.post(
                f"{member.url.rstrip('/')}/api/fleet/membership/apply",
                json=envelope,
                headers=_peer_headers_ctx(ctx),
                timeout=FLEET_DETAIL_TIMEOUT,
            )
            if response.status_code >= 400:
                incompatible = response.status_code in {404, 405, 415, 422}
                raise RuntimeError(
                    f"HTTP {response.status_code}: {response.text[:200]}"
                    + (" [incompatible]" if incompatible else "")
                )
            applied_generation = int(
                (response.json() if response.content else {}).get(
                    "after_generation", generation
                )
            )
            if applied_generation < generation:
                raise RuntimeError(
                    f"peer acknowledged generation {applied_generation}, expected {generation}"
                )
            convergence.applied(member.instance_id, generation)
        except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
            detail = str(exc)
            convergence.failed(
                member.instance_id,
                generation,
                detail,
                incompatible=detail.endswith("[incompatible]"),
            )
    return convergence.snapshot()["peers"]


async def _rollout_membership(
    request: Request,
    *,
    members: list[FleetInstance] | None = None,
) -> list[dict[str, Any]]:
    """Push the complete signed roster and retain durable retry diagnostics."""
    async with _borrow_fleet_client(request, timeout=FLEET_DETAIL_TIMEOUT) as client:
        return await _deliver_membership(
            request.app.state.ctx, client, members=members, force=True
        )


async def _membership_convergence_loop(ctx: AppContext) -> None:
    """Retry offline/restarting/incompatible peers with persisted bounded backoff."""
    while True:
        try:
            client = ctx.require_service("fleet_http_client")
            await _deliver_membership(ctx, client)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Periodic fleet membership convergence failed")
        await asyncio.sleep(5.0)


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
        if resp.status_code == 404 and not isinstance(detail, dict):
            detail = {
                "code": "target_route_not_found",
                "message": "The target does not expose the dispatch materialization route.",
                "target_instance_id": instance_id,
                "target_status": resp.status_code,
                "recoverable": False,
                "upgrade_required": True,
            }
        elif isinstance(detail, dict):
            detail = {
                **detail,
                "target_instance_id": detail.get("target_instance_id") or instance_id,
                "target_correlation_id": resp.headers.get("X-Request-ID"),
            }
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
    actual_authority = response.headers.get("X-PA-Instance-ID", "").strip()
    if actual_authority != authority_instance_id:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "authority_identity_mismatch",
                "message": "The authority response did not prove the expected instance identity.",
                "recoverable": False,
            },
        )
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
                projection_head = next(
                    (
                        item.get("projection_head")
                        for item in refs
                        if item.get("realm_id") == realm_id
                    ),
                    None,
                )
                return {
                    "url": peer_url,
                    "status": "reachable" if head else "missing_head",
                    "head": head,
                    "projection_head": projection_head,
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
                status_code=503,
                detail={
                    "code": "target_projection_not_ready",
                    "message": "The selected target has not converged to the authoritative realm head.",
                    "realm_id": realm_id,
                    "authority_head": durable_head,
                    "target_head": target_observation["head"],
                    "target_instance_id": target_instance_id,
                    "recoverable": True,
                    "retry_after": 1,
                    "retry_after_convergence": True,
                    "recovery_url": f"/fleet?section=sync&realm={quote(realm_id)}",
                },
                headers={"Retry-After": "1"},
            )
        target_projection_head = target_observation.get("projection_head")
        if (
            target_projection_head is not None
            and target_projection_head != durable_head
        ):
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "target_projection_not_ready",
                    "message": "The target durable head is current but its projection is still catching up.",
                    "realm_id": realm_id,
                    "authority_head": durable_head,
                    "target_head": target_observation["head"],
                    "target_projection_head": target_projection_head,
                    "target_instance_id": target_instance_id,
                    "recoverable": True,
                    "retry_after": 1,
                    "retry_after_convergence": True,
                },
                headers={"Retry-After": "1"},
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
            "target_projection_head": target_projection_head,
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


async def _wait_for_dispatch_sync_health(
    request: Request,
    realm_id: str,
    target_instance_id: str,
    *,
    attempts: int = 60,
) -> dict[str, Any] | None:
    """Wait through ordinary projection lag before failing a dispatch."""
    for attempt in range(1, attempts + 1):
        try:
            return await _assert_dispatch_sync_health(
                request, realm_id, target_instance_id
            )
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, dict) else {}
            if (
                detail.get("code")
                not in {"authority_projection_stale", "target_projection_not_ready"}
                or not detail.get("recoverable")
                or attempt == attempts
            ):
                raise
            retry_after = 1.0
            if exc.headers:
                try:
                    retry_after = max(
                        0.1, min(float(exc.headers.get("Retry-After", "1")), 5.0)
                    )
                except TypeError, ValueError:
                    pass
            await asyncio.sleep(retry_after)


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


async def _refresh_queued_dispatch_readiness(
    app, record: DispatchRecord
) -> CapacityAdmission:
    """Recheck the fixed target contract before a waiting dispatch consumes a slot."""

    request = _dispatch_request(app)
    ctx = app.state.ctx
    ledger: DispatchStore = ctx.require_service("dispatch_store")
    try:
        record = await _offload_ctx(
            ctx,
            "goal.dispatch_execution_identity_restore",
            _restore_goal_dispatch_execution_identity,
            ctx,
            ledger,
            record,
        )
        record = await _validate_goal_dispatch_record_async(
            ctx, ledger, record, sink="queued-promotion"
        )
    except HTTPException as exc:
        release_authorized = _goal_admission_proof_valid(ctx, record)
        detail = exc.detail if isinstance(exc.detail, dict) else {}
        record = await _offload_ctx(
            ctx,
            "dispatch.goal_fence_failure",
            ledger.fail,
            record,
            str(detail.get("message") or detail or exc),
            code=str(detail.get("code") or "goal_governance_denied"),
            recoverable=False,
            detail=detail,
        )
        if release_authorized:
            await _release_goal_dispatch_reservation_async(
                ctx,
                ledger,
                record,
                outcome="promotion-denied",
                applied=False,
            )
        else:
            record.goal_admission_validation_state = "rejected"
            record.goal_admission_validation_error = (
                "Queued promotion failed exact admission proof validation; "
                "the authoritative reservation was quarantined without release."
            )
            await _offload_ctx(
                ctx,
                "dispatch.goal_fence_quarantine",
                ledger.put,
                record,
            )
        raise
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    inst = fleet.get_instance(record.target_instance_id)
    if not inst or inst.lifecycle_state != "active":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "queued_target_unavailable",
                "message": "The requested target is unavailable; PA will keep waiting without rerouting it.",
                "recoverable": True,
            },
        )
    candidates = await _placement_candidates(request, [inst])
    candidate = candidates[0]
    policy_service = _policy_service(request)
    policy, explicit = policy_service.effective_policy(
        record.realm_id, candidate.instance_id
    )
    candidate.participation_policy = policy
    candidate.participation_policy_explicit = explicit
    candidate.group_membership = "included"
    original = record.placement_decision or {}
    try:
        decision = await _offload_ctx(
            ctx,
            "fleet.queued_readiness_resolve",
            ctx.require_service("placement_service").resolve,
            PlacementRequest(
                realm_id=record.realm_id,
                fleet_id=ctx.settings.fleet_id,
                instance_id=record.target_instance_id,
                card_id=record.card_id,
                provider=record.capacity_provider
                or record.request_payload.get("provider"),
                model_id=record.request_payload.get("model_id"),
                workload_profile=str(original.get("workload_profile") or "research"),
                project_id=record.project_id,
                dispatch_intent=DispatchIntent(
                    original.get("dispatch_intent") or DispatchIntent.AUTOMATIC.value
                ),
                principal_id=record.principal_id,
                allow_concurrent=True,
                # Suppress only the placement queue-full rejection so the store can
                # atomically decide whether a slot is actually free.
                capacity_override=True,
            ),
            candidates,
        )
    except PlacementError as exc:
        raise _placement_http_error(exc) from exc
    capacity = _capacity_admission_from_decision(
        decision.model_dump(mode="json"),
        provider=record.capacity_provider or record.request_payload.get("provider"),
        override=False,
        override_reason=None,
    )
    if not capacity:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "queued_readiness_unavailable",
                "message": "Fresh target capacity could not be confirmed; PA will retry without rerouting.",
                "recoverable": True,
            },
        )
    return capacity


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

    cancelled = await _offload_ctx(ctx, "dispatch.cancel_check", check_and_transition)
    if cancelled:
        await _release_goal_dispatch_reservation_async(
            ctx,
            ledger,
            record,
            outcome="cancelled",
            applied=_goal_dispatch_was_applied(record),
        )
    return cancelled


def _goal_materialization_stage_provenance(
    provenance: GoalDispatchProvenance | None,
) -> GoalDispatchProvenance | None:
    """Project provenance to the immutable pre-session materialization stage."""

    if provenance is None or provenance.execution_identity is None:
        return provenance
    return provenance.model_copy(update={"execution_identity": None})


async def _synchronize_target_goal_execution_identity(
    request: Request,
    record: DispatchRecord,
    materialize_payload: dict[str, Any],
) -> None:
    """Durably bind the target copy before any identity-bearing session traffic."""

    provenance = record.goal_provenance
    identity = provenance.execution_identity if provenance is not None else None
    if identity is None:
        return
    session_id = str(record.session_id or "").strip()
    if not session_id or identity.session_id != session_id:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": "Authority execution identity is not bound to this session.",
                "recoverable": False,
            },
        )
    payload = dict(materialize_payload)
    payload["session_id"] = session_id
    payload["goal_provenance"] = provenance.model_dump(mode="json")
    acknowledged = await _peer_dispatch_json(
        request,
        record.target_instance_id,
        payload,
    )
    if not (
        isinstance(acknowledged, dict)
        and acknowledged.get("resolvable") is True
        and acknowledged.get("dispatch_id") == record.dispatch_id
        and acknowledged.get("session_id") == session_id
        and acknowledged.get("execution_identity_digest") == identity.digest
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "target_execution_identity_unconfirmed",
                "message": (
                    "The target did not durably acknowledge the exact execution identity."
                ),
                "recoverable": True,
            },
        )


async def _process_remote_dispatch(app, record: DispatchRecord) -> None:
    """Advance one persisted dispatch through independently auditable stages."""
    request = _dispatch_request(app)
    ctx = app.state.ctx
    settings = ctx.settings
    ledger: DispatchStore = ctx.require_service("dispatch_store")
    store = ctx.store

    record = await _offload_ctx(
        ctx,
        "goal.dispatch_execution_identity_restore",
        _restore_goal_dispatch_execution_identity,
        ctx,
        ledger,
        record,
    )
    record = await _validate_goal_dispatch_record_async(
        ctx, ledger, record, sink="worker-start"
    )

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
        sync_evidence = await _wait_for_dispatch_sync_health(
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
        if card.lane == CardLane.DONE:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "queued_card_terminal",
                    "message": "The card moved to Done while queued; the dispatch was not launched.",
                    "recoverable": False,
                },
            )
        prior_card_version = record.card_version
        if prior_card_version and prior_card_version != card.updated_at.isoformat():
            record.events.append(
                DispatchEvent(
                    seq=(record.events[-1].seq + 1 if record.events else 1),
                    state=record.state,
                    message="Card changed while queued; launching the latest authoritative snapshot without changing target or execution contract.",
                    detail={
                        "admitted_card_version": prior_card_version,
                        "launch_card_version": card.updated_at.isoformat(),
                    },
                )
            )
        record.card_version = card.updated_at.isoformat()
        record.card_snapshot = card.model_dump(mode="json")
        record.project_id = record.project_id or card.project_id
        if record.project_id:
            project = await _offload_ctx(
                ctx,
                "sqlite.project_read",
                store.get_project,
                record.project_id,
                realm_id=record.realm_id,
            )
            if not project or str(project.status) != "active":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "queued_project_unavailable",
                        "message": "The project was deleted or archived while queued; the dispatch was not launched.",
                        "recoverable": False,
                    },
                )
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
    materialized_attachments = list(card.attachments if card else [])
    materialized_card = record.card_snapshot
    if (
        record.goal_provenance is not None
        and record.goal_provenance.materialization_envelope is not None
    ):
        bound_attachment_ids = set(
            record.goal_provenance.materialization_envelope.attachment_ids
        )
        materialized_attachments = [
            item
            for item in materialized_attachments
            if item.attachment_id in bound_attachment_ids
        ]
        if materialized_card is not None:
            materialized_card = dict(materialized_card)
            materialized_card["attachments"] = [
                item.model_dump(mode="json") for item in materialized_attachments
            ]
    # The target materialization ledger is created before session allocation. A
    # crash-safe resume therefore replays the same stage-bound provenance; the
    # full execution identity is carried to the resumed session and prompt.
    materialization_provenance = _goal_materialization_stage_provenance(
        record.goal_provenance
    )
    materialize_payload = {
        "dispatch_id": record.dispatch_id,
        "mutation_id": record.mutation_id,
        "card": materialized_card,
        "card_version": record.card_version,
        "realm_id": record.realm_id,
        "project_id": record.project_id,
        "principal_id": record.principal_id,
        "provenance_version": 1,
        "authority_instance_id": record.authority_instance_id,
        "authority_instance_name": record.authority_instance_name,
        "authority_url": record.authority_url,
        "target_instance_id": record.target_instance_id,
        "provider": record.request_payload.get("provider"),
        "model_id": record.request_payload.get("model_id"),
        "mode_id": record.request_payload.get("mode_id"),
        "execution_contract": record.request_payload.get("execution_contract"),
        "session_id": record.resume_session_id if record.resume_requested else None,
        "progress_versions": SUPPORTED_PROGRESS_VERSIONS,
        "attachment_manifest": [
            item.model_dump(mode="json") for item in materialized_attachments
        ],
        "attachment_digest": manifest_digest(materialized_attachments),
        "materialization_plan": record.materialization_plan,
        "goal_provenance": (
            materialization_provenance.model_dump(mode="json")
            if materialization_provenance
            else None
        ),
    }
    manifest = [
        CardAttachment.model_validate(item)
        for item in materialize_payload["attachment_manifest"]
    ]
    record = await _validate_goal_dispatch_record_async(
        ctx, ledger, record, sink="target-materialization"
    )
    materialization_provenance = _goal_materialization_stage_provenance(
        record.goal_provenance
    )
    materialize_payload["goal_provenance"] = (
        materialization_provenance.model_dump(mode="json")
        if materialization_provenance
        else None
    )
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
    await _synchronize_target_goal_execution_identity(
        request,
        record,
        materialize_payload,
    )
    if await _dispatch_cancelled(ctx, ledger, record):
        return

    payload = dict(record.request_payload)
    await _offload_ctx(
        ctx,
        "dispatch.record_write",
        ledger.transition,
        record,
        "provisioning",
        "Provisioning the target workspace and execution environment.",
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
        "goal_provenance": (
            record.goal_provenance.model_dump(mode="json")
            if record.goal_provenance
            else None
        ),
        "resume": record.resume_requested,
        "resume_session_id": record.resume_session_id,
    }
    session_body = {
        key: value
        for key, value in session_body.items()
        if value not in (None, "", False)
    }
    record = await _validate_goal_dispatch_record_async(
        ctx, ledger, record, sink="session-allocation"
    )
    session_body["goal_provenance"] = (
        record.goal_provenance.model_dump(mode="json")
        if record.goal_provenance
        else None
    )
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
    # Persist the target-allocated identity before configuration checks. A
    # crash from this point forward can then bind governance and resume exactly
    # this session instead of trying to allocate another one.
    record.session_id = session_id
    await _offload_ctx(ctx, "dispatch.record_write", ledger.put, record)
    await _offload_ctx(
        ctx,
        "dispatch.record_write",
        ledger.transition,
        record,
        "starting_session",
        "Provider session allocated; validating configuration and linkage.",
        detail={"session_id": session_id},
    )
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
    record.goal_provenance = await _offload_ctx(
        ctx,
        "goal.dispatch_assigned_service_identity_bind",
        _bind_goal_dispatch_assigned_service_identity,
        ctx,
        record.goal_provenance,
        selected_authority=record.authority_instance_id,
        dispatch_id=record.dispatch_id,
        session_id=session_id,
    )
    if not goal_dispatch_execution_identity_valid(
        record,
        require_authenticated_credential=record.goal_provenance is not None,
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": "Assigned session lacks its final credential binding.",
                "recoverable": False,
            },
        )
    await _offload_ctx(ctx, "dispatch.record_write", ledger.put, record)
    await _synchronize_target_goal_execution_identity(
        request,
        record,
        materialize_payload,
    )
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
        record = await _validate_goal_dispatch_record_async(
            ctx, ledger, record, sink="prompt-delivery"
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
                "goal_provenance": (
                    record.goal_provenance.model_dump(mode="json")
                    if record.goal_provenance
                    else None
                ),
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
    await _release_goal_dispatch_reservation_async(
        ctx,
        ledger,
        record,
        outcome="started",
        applied=True,
    )


async def _placement_candidates(
    request: Request, instances: list[FleetInstance]
) -> list[PlacementCandidate]:
    ctx = request.app.state.ctx

    async def inspect(inst: FleetInstance) -> PlacementCandidate:
        (
            reachability,
            activity,
            providers,
            mcp_bootstrap,
            repositories,
        ) = await asyncio.gather(
            *(
                probe_dimension(ctx, inst, dimension, force=True)
                for dimension in (
                    "reachability",
                    "activity",
                    "providers",
                    "mcp_bootstrap",
                    "repositories",
                )
            )
        )
        dispatch_store = ctx.services.get("dispatch_store")
        if activity.get("state") == "fresh":
            authority_snapshot = (
                dispatch_store.capacity_snapshot(inst.instance_id)
                if isinstance(dispatch_store, DispatchStore)
                else None
            )
            value = normalize_activity_capacity(
                dict(activity.get("value") or {}),
                authority_snapshot=authority_snapshot,
            )
            activity = {**activity, "value": value}
        return PlacementCandidate(
            instance_id=inst.instance_id,
            name=inst.name,
            zone=inst.zone,
            lifecycle_state=inst.lifecycle_state,
            local=inst.instance_id == ctx.settings.instance_id,
            capabilities=list(inst.capabilities),
            dispatch_capacity=inst.dispatch_capacity,
            dispatch_provider_capacities=dict(inst.dispatch_provider_capacities),
            dispatch_queue_capacity=inst.dispatch_queue_capacity,
            dispatch_provider_queue_capacities=dict(
                inst.dispatch_provider_queue_capacities
            ),
            reachability=reachability,
            activity=activity,
            providers=providers,
            mcp_bootstrap=mcp_bootstrap,
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
    # An omitted provider means automatic target-compatible selection.  Do not
    # inject the authority host's default before placement (for example Cursor
    # when the selected worker is Codex-only).
    body.provider = (body.provider or "").strip().lower() or None
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
    if body.target_instance_id:
        # A named dispatch has no scheduling choice to make. Probing unrelated
        # peers adds several remote round trips to the admission path and can
        # outlive the MCP owner-channel deadline even though the selected target
        # is healthy and the durable dispatch is successfully admitted.
        instances = [
            instance
            for instance in instances
            if instance.instance_id == body.target_instance_id
        ]
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
        policy, explicit = policies.effective_policy(realm_id, candidate.instance_id)
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
        except TypeError, ValueError:
            candidate.participation_policy_supported = False
        candidate.self_protection = dict(
            activity.get("self_protective_participation") or {}
        )

    required_capabilities = sorted(
        set(body.required_capabilities)
        | {f"mcp:{name}" for name in body.required_mcp_servers}
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
            "optional_mcp_warnings": [
                {
                    "code": "optional_mcp_unavailable",
                    "server": name,
                    "message": f"Optional MCP server {name!r} is not available on this instance.",
                }
                for name in body.optional_mcp_servers
                if f"mcp:{name}"
                not in set(
                    next(
                        (
                            candidate.capabilities
                            for candidate in candidates
                            if candidate.instance_id == item.get("instance_id")
                        ),
                        [],
                    )
                )
            ],
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
    if body.provider is None:
        chosen = next(
            (
                item
                for item in decision.eligible_candidates
                if item.get("instance_id") == decision.chosen_instance_id
            ),
            None,
        )
        body.provider = str((chosen or {}).get("provider_id") or "").strip() or None
        if body.provider is None:
            raise PlacementError(
                "provider_unavailable",
                "The selected target has no concrete authenticated provider "
                "for automatic selection.",
                rejected_candidates=decision.rejected_candidates,
            )
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
        raise _dispatch_lookup_error("card", body.card_id)
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
        raise _dispatch_lookup_error("project", project_id)
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
    queue_capacity = detail.get("queue_capacity_detail") or {}
    global_workload = detail.get("global_workload") or {}
    provider_workload = detail.get("provider_workload") or {}
    observed_at = (detail.get("freshness") or {}).get("activity")
    queue_limit = detail.get("queue_capacity")
    if queue_limit is None:
        queue_limit = 100
    global_queue_limit = queue_capacity.get("global_limit")
    if global_queue_limit is None:
        global_queue_limit = queue_limit
    return CapacityAdmission(
        limit=int(detail.get("capacity") or capacity.get("limit")),
        source=str(capacity.get("source") or "unknown"),
        provider=provider.strip().lower() if provider else None,
        provider_specific=capacity.get("source") == "configured_provider",
        observed_active=int(detail.get("active") or 0),
        observed_queued=int(detail.get("queued") or 0),
        observed_reservations=int(detail.get("reserved") or 0),
        observed_at=observed_at or datetime.now(UTC),
        consumer_links=list(detail.get("consumer_links") or []),
        global_limit=int(capacity.get("global_limit") or detail.get("capacity")),
        provider_limit=(
            int(capacity["provider_limit"])
            if capacity.get("provider_limit") is not None
            else None
        ),
        observed_global_active=int(global_workload.get("active") or 0),
        observed_global_queued=int(global_workload.get("queued") or 0),
        observed_global_reservations=int(global_workload.get("reservations") or 0),
        observed_provider_active=int(provider_workload.get("active") or 0),
        observed_provider_queued=int(provider_workload.get("queued") or 0),
        observed_provider_reservations=int(provider_workload.get("reservations") or 0),
        queue_limit=int(queue_limit),
        queue_source=str(
            (detail.get("queue_capacity_detail") or {}).get("source")
            or "documented_default"
        ),
        queue_provider_specific=(queue_capacity.get("source") == "configured_provider"),
        observed_waiting=int(detail.get("queue_count") or 0),
        global_queue_limit=int(global_queue_limit),
        provider_queue_limit=(
            int(queue_capacity["provider_limit"])
            if queue_capacity.get("provider_limit") is not None
            else None
        ),
        observed_global_waiting=int(detail.get("global_queue_count") or 0),
        observed_provider_waiting=int(detail.get("provider_queue_count") or 0),
        override=override,
        override_reason=override_reason,
    )


def _placement_http_error(exc: PlacementError) -> HTTPException:
    queue_full = next(
        (
            item
            for item in exc.rejected_candidates
            if "dispatch_queue_full" in (item.get("rejection_codes") or [])
        ),
        None,
    )
    if queue_full is not None:
        current = int(queue_full.get("queue_count") or 0)
        maximum = int(queue_full.get("queue_capacity") or 0)
        return HTTPException(
            status_code=429,
            detail={
                "code": "dispatch_queue_full",
                "message": f"The durable dispatch queue is full ({current} of {maximum}).",
                "current_count": current,
                "maximum_count": maximum,
                "active_execution_capacity": queue_full.get("capacity"),
                "source": (queue_full.get("queue_capacity_detail") or {}).get("source"),
                "recoverable": True,
                "retry_after_seconds": 5,
                "retry_guidance": "Retry after queued work launches or is cancelled.",
                "remediation_options": [
                    "cancel unneeded queued dispatches",
                    "increase dispatch_queue_capacity",
                    "use a different eligible target",
                ],
                "rejected_candidates": exc.rejected_candidates,
                "recovery_url": "/fleet?section=operations",
            },
        )
    status = 404 if exc.code == "instance_not_found" else 409
    recovery: dict[str, Any] = {}
    if exc.code in {"provider_unavailable", "mcp_bootstrap_unavailable"}:
        recovery = {
            "retry_guidance": "Repair the target and retry with the same idempotency key.",
            "remediation_options": [
                "run pa doctor --verbose on the target instance",
                "retry after repairing/restarting the target",
                "choose an alternate eligible instance",
                "choose an alternate authenticated provider",
            ],
            "recovery_url": "/fleet?section=operations",
            "rejected_candidates": exc.rejected_candidates,
            "field_errors": (
                {
                    "provider": {
                        "code": "provider_unavailable",
                        "message": exc.message,
                        "recovery_choices": [
                            "select an authenticated provider",
                            "choose another target instance",
                            "refresh provider inventory",
                        ],
                    }
                }
                if exc.code == "provider_unavailable"
                else {}
            ),
        }
    return HTTPException(
        status_code=status,
        detail={
            "code": exc.code,
            "message": exc.message,
            "recoverable": exc.recoverable,
            **recovery,
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


def _goal_dispatch_services(ctx: AppContext, goal_id: str):
    goal_service = ctx.services.get("goal_service")
    governance = ctx.services.get("goal_governance")
    goal = goal_service.get(goal_id) if goal_service else None
    if goal is None or governance is None:
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_authority_unavailable", "recoverable": True},
        )
    return goal, governance


def _goal_governance_replay_context(
    governance,
    goal,
    context: GovernanceMutationContext,
) -> GovernanceMutationContext:
    """Recover the exact original autonomy version for an internal retry."""

    duplicate = governance._duplicate(goal.realm_id, context.idempotency_key)
    if duplicate is None:
        return context
    return context.model_copy(
        update={"expected_version": max(int(duplicate.get("version", 1)) - 1, 0)}
    )


def _goal_dispatch_lifecycle_owned(ctx: AppContext, record: DispatchRecord) -> bool:
    provenance = record.goal_provenance
    local_instance_id = str(
        getattr(getattr(ctx, "settings", None), "instance_id", "") or ""
    )
    return bool(
        provenance is not None
        and local_instance_id
        and record.authority_instance_id == local_instance_id
        and provenance.authority_instance_id == local_instance_id
    )


def _bind_effective_goal_dispatch_provider(
    body: RemoteAgentStartBody, default_provider: str
) -> None:
    """Persist one concrete provider before governed admission fingerprinting."""

    if body.goal_provenance is None:
        return
    body.provider = str(body.provider or default_provider).strip().lower() or None
    if body.provider is None:
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_provider_unresolved", "recoverable": False},
        )


def _goal_dispatch_placement_input(
    body: RemoteAgentStartBody,
    *,
    target_instance_id: str,
) -> tuple[str, dict[str, Any], str]:
    payload = body.model_dump(mode="json")
    if isinstance(body, FleetDispatchBody):
        requested_target = body.target_instance_id or (
            f"placement:{body.placement_policy or 'balanced'}"
        )
    else:
        payload["target_instance_id"] = target_instance_id
        payload["placement_policy"] = None
        requested_target = target_instance_id
    snapshot = goal_dispatch_placement_input_snapshot(payload)
    return (
        requested_target,
        snapshot,
        goal_dispatch_placement_input_digest(snapshot),
    )


def _persist_goal_dispatch_admission_trace(
    ctx: AppContext,
    ledger: DispatchStore,
    body: RemoteAgentStartBody,
    *,
    idempotency_key: str,
    request_fingerprint: str,
    target_instance_id: str,
    principal_id: str,
    placement_policy: str,
    idempotency_scope: str,
) -> tuple[DispatchRecord | None, bool]:
    """Write restart-recoverable evidence before governed admission can block."""

    if body.goal_provenance is None:
        return None, True
    settings = ctx.settings
    (
        _requested_target,
        placement_input,
        placement_input_digest,
    ) = _goal_dispatch_placement_input(body, target_instance_id=target_instance_id)
    record = DispatchRecord(
        mutation_id=str(uuid4()),
        idempotency_key=idempotency_key,
        request_fingerprint=request_fingerprint,
        placement_request_fingerprint=request_fingerprint,
        card_id=body.card_id,
        project_id=body.project_id,
        request_payload=body.model_dump(mode="json"),
        goal_provenance=body.goal_provenance,
        goal_placement_input=placement_input,
        goal_placement_input_digest=placement_input_digest,
        goal_admission_validation_state="pending",
        principal_id=principal_id,
        authority_instance_id=settings.instance_id,
        authority_instance_name=getattr(
            settings, "instance_name", settings.instance_id
        ),
        authority_url=str(getattr(settings, "instance_url", "") or ""),
        target_instance_id=target_instance_id,
        placement_policy=placement_policy,
        requested_priority=body.priority,
        allow_concurrent=body.allow_concurrent,
        state="admission_pending",
        events=[
            DispatchEvent(
                seq=1,
                state="admission_pending",
                message="Governed dispatch admission durably started.",
            )
        ],
    )
    return ledger.begin_admission(record, idempotency_scope=idempotency_scope)


def _admission_in_progress_error(record: DispatchRecord) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "code": "dispatch_admission_in_progress",
            "message": "The canonical governed admission for this idempotency key is still in progress.",
            "dispatch_id": record.dispatch_id,
            "recoverable": True,
        },
    )


def _fail_goal_dispatch_admission(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord | None,
    *,
    message: str,
    code: str,
    recoverable: bool,
    detail: dict[str, Any] | None = None,
) -> DispatchRecord | None:
    """Terminalize a pre-admission trace before releasing its exact goal hold."""

    if record is None:
        return None
    current = ledger.get(record.dispatch_id) or record
    if current.state != "admission_pending":
        return current
    current = ledger.fail(
        current,
        message,
        code=code,
        recoverable=recoverable,
        detail=detail,
    )
    if current.goal_provenance is not None and not _goal_admission_operation_bound(
        ctx, current
    ):
        current.goal_admission_validation_state = "rejected"
        current.goal_admission_validation_error = (
            "The staged provenance was not bound to this admission operation."
        )
        return ledger.put(current)
    return _release_goal_dispatch_reservation(
        ctx,
        ledger,
        current,
        outcome=f"admission-failed:{code}",
        applied=False,
    )


async def _fail_goal_dispatch_admission_request(
    request: Request,
    record: DispatchRecord | None,
    *,
    message: str,
    code: str,
    recoverable: bool,
    detail: dict[str, Any] | None = None,
) -> DispatchRecord | None:
    if record is None:
        return None
    return await _offload_request(
        request,
        "goal.dispatch_admission_failed",
        _fail_goal_dispatch_admission,
        request.app.state.ctx,
        _dispatch_store(request),
        record,
        message=message,
        code=code,
        recoverable=recoverable,
        detail=detail,
    )


async def _reject_goal_dispatch_admission(
    request: Request,
    record: DispatchRecord | None,
    error: HTTPException,
    *,
    default_code: str = "admission_rejected",
) -> None:
    """Make a normal pre-admission rejection terminal and restart-recoverable."""

    detail = error.detail if isinstance(error.detail, dict) else None
    code = str((detail or {}).get("code") or default_code)
    message = str((detail or {}).get("message") or error.detail or code)
    recoverable = bool((detail or {}).get("recoverable", error.status_code >= 500))
    await _fail_goal_dispatch_admission_request(
        request,
        record,
        message=message,
        code=code,
        recoverable=recoverable,
        detail=detail,
    )


def _validate_goal_dispatch_provenance(
    ctx: AppContext,
    provenance: GoalDispatchProvenance | None,
    selected_authority: str,
    *,
    sink: str,
    provider_id: str | None = None,
    target_instance_id: str | None = None,
    placement_input_digest: str | None = None,
    placement_decision_digest: str | None = None,
    denial_applied: bool = False,
) -> GoalDispatchProvenance | None:
    """Use the canonical governance state at every dispatch side-effect sink."""

    if provenance is None:
        return None
    effective_provider = str(provider_id or "").strip().lower()
    effective_target = str(
        target_instance_id or provenance.resolved_target_instance_id or ""
    ).strip()
    effective_input_digest = str(
        placement_input_digest or provenance.placement_input_digest or ""
    ).strip()
    effective_decision_digest = str(
        placement_decision_digest or provenance.placement_decision_digest or ""
    ).strip()
    envelope = provenance.materialization_envelope
    receipt = provenance.materialization_receipt
    execution_identity = provenance.execution_identity
    if not effective_provider:
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_provider_unresolved", "recoverable": False},
        )
    if provenance.released_at is not None:
        raise HTTPException(
            status_code=409,
            detail={"code": "released_goal_reservation", "recoverable": False},
        )
    if provenance.authority_instance_id != selected_authority:
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_authority_mismatch", "recoverable": False},
        )
    if (
        provenance.provider_id is not None
        and provenance.provider_id.strip().lower() != effective_provider
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_provider_mismatch", "recoverable": False},
        )
    if (
        not provenance.requested_placement_target
        or not effective_target
        or not effective_input_digest
        or not effective_decision_digest
        or provenance.placement_input_digest != effective_input_digest
        or provenance.resolved_target_instance_id != effective_target
        or provenance.placement_decision_digest != effective_decision_digest
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_placement_mismatch", "recoverable": False},
        )
    goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
    if (
        goal.control_authority_instance_id != selected_authority
        or not goal.lease.active()
        or goal.lease.holder_instance_id != selected_authority
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "stale_goal_fence", "recoverable": False},
        )
    if provenance.fencing_token != goal.lease.fencing_token:
        raise HTTPException(
            status_code=409,
            detail={"code": "stale_goal_fence", "recoverable": False},
        )
    eligible = set(
        goal.wakeup.eligible_instance_ids
        if goal.wakeup and goal.wakeup.eligible_instance_ids
        else goal.lease.eligible_instance_ids
    )
    if eligible and selected_authority not in eligible:
        raise HTTPException(
            status_code=403,
            detail={"code": "ineligible_goal_authority", "recoverable": False},
        )
    state = governance.get_state(goal.id)
    reservation = next(
        (
            item
            for item in state.action_reservations
            if item.id == provenance.action_reservation_id
        ),
        None,
    )
    reserved_provider = (
        str(reservation.request.provider_id or "").strip().lower()
        if reservation is not None
        else ""
    )
    if not reserved_provider or reserved_provider != effective_provider:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_governance_denied",
                "message": "the reservation does not bind this execution provider",
                "recoverable": False,
            },
        )
    if (
        reservation is None
        or reservation.state != GoalReservationState.APPLIED
        or reservation.action_class != provenance.action_class
        or reservation.actor_principal != provenance.actor_principal
        or reservation.authority_instance_id != selected_authority
        or reservation.goal_version != provenance.goal_version
        or reservation.policy_revision != provenance.policy_revision
        or reservation.fencing_token != provenance.fencing_token
        or reservation.request.operation_key != provenance.operation_key
        or reservation.request.requested_placement_target
        != provenance.requested_placement_target
        or reservation.request.placement_input_digest != effective_input_digest
        or reservation.request.resolved_target_instance_id != effective_target
        or reservation.request.placement_decision_digest != effective_decision_digest
        or envelope is None
        or receipt is None
        or receipt.envelope_digest != envelope.digest
        or receipt.target_instance_id != effective_target
        or receipt.provider_id.strip().lower() != effective_provider
        or reservation.request.materialization_envelope != envelope
        or reservation.request.materialization_receipt != receipt
        or reservation.request.execution_identity != execution_identity
        or (
            execution_identity is not None
            and (
                execution_identity.materialization_receipt_digest != receipt.digest
                or execution_identity.target_instance_id != effective_target
                or execution_identity.provider_id.strip().lower() != effective_provider
                or execution_identity.fencing_token != provenance.fencing_token
            )
        )
        or reservation.attempt != provenance.reservation_attempt
        or reservation.max_attempts != provenance.max_reservation_attempts
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "invalid_goal_reservation", "recoverable": False},
        )
    context = GovernanceMutationContext(
        actor_principal=provenance.actor_principal,
        authority_instance_id=selected_authority,
        idempotency_key=(
            f"goal-dispatch:{provenance.action_reservation_id}:validate:{sink}:"
            f"g{goal.version}:p{goal.policy.revision}:f{goal.lease.fencing_token}:"
            f"provider:{effective_provider}:target:{effective_target}:"
            f"placement:{effective_decision_digest}"
        )[:200],
        expected_version=state.version,
        policy_revision=goal.policy.revision,
        goal_version=goal.version,
        fencing_token=goal.lease.fencing_token,
    )
    try:
        _, reservation = governance.revalidate_action_sink(
            goal.id,
            provenance.action_reservation_id,
            _goal_governance_replay_context(governance, goal, context),
            action_class=provenance.action_class,
            provider_id=effective_provider,
            requested_placement_target=provenance.requested_placement_target,
            placement_input_digest=effective_input_digest,
            resolved_target_instance_id=effective_target,
            placement_decision_digest=effective_decision_digest,
            materialization_envelope=envelope,
            materialization_receipt=receipt,
            execution_identity=execution_identity,
            denial_actual_usage=(
                reservation.reserved_usage.model_copy(deep=True)
                if denial_applied
                else GoalUsage()
            ),
        )
    except GoalGovernanceConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_governance_denied",
                "message": str(exc),
                "recoverable": False,
            },
        ) from exc
    return provenance.model_copy(
        update={
            "goal_version": reservation.goal_version,
            "policy_revision": reservation.policy_revision,
            "fencing_token": reservation.fencing_token,
            "provider_id": effective_provider,
            "operation_key": reservation.request.operation_key,
            "requested_placement_target": (
                reservation.request.requested_placement_target
            ),
            "placement_input_digest": reservation.request.placement_input_digest,
            "resolved_target_instance_id": (
                reservation.request.resolved_target_instance_id
            ),
            "placement_decision_digest": (
                reservation.request.placement_decision_digest
            ),
            "materialization_envelope": (reservation.request.materialization_envelope),
            "materialization_receipt": reservation.request.materialization_receipt,
            "execution_identity": reservation.request.execution_identity,
            "reservation_attempt": reservation.attempt,
            "max_reservation_attempts": reservation.max_attempts,
        }
    )


def _goal_admission_operation_bound(
    ctx: AppContext,
    record: DispatchRecord,
) -> bool:
    provenance = record.goal_provenance
    operation_key = str(record.idempotency_key or "")
    if (
        provenance is None
        or not operation_key
        or provenance.operation_key != operation_key
    ):
        return False
    try:
        _goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
        state = governance.get_state(provenance.goal_id)
    except HTTPException, GoalGovernanceConflict:
        return False
    reservation = next(
        (
            item
            for item in state.action_reservations
            if item.id == provenance.action_reservation_id
        ),
        None,
    )
    base_bound = bool(
        reservation is not None
        and reservation.request.operation_key == operation_key
        and reservation.goal_id == provenance.goal_id
        and reservation.authority_instance_id == provenance.authority_instance_id
        and reservation.actor_principal == provenance.actor_principal
        and reservation.action_class == provenance.action_class
        and str(reservation.request.provider_id or "").strip().lower()
        == str(provenance.provider_id or "").strip().lower()
        and reservation.request.requested_placement_target
        == provenance.requested_placement_target
        and reservation.request.placement_input_digest
        == provenance.placement_input_digest
        and reservation.request.placement_input_digest
        == record.goal_placement_input_digest
        and provenance.materialization_envelope is not None
        and reservation.request.materialization_envelope
        == provenance.materialization_envelope
        and reservation.request.execution_identity == provenance.execution_identity
        and (
            (
                reservation.request.materialization_receipt is None
                and provenance.materialization_receipt is None
                and record.materialization_plan is None
            )
            or (
                provenance.materialization_receipt is not None
                and reservation.request.materialization_receipt
                == provenance.materialization_receipt
                and goal_dispatch_materialization_binding_valid(record)
            )
        )
        and goal_dispatch_record_placement_input_valid(record)
        and goal_dispatch_execution_identity_valid(record)
    )
    if not base_bound or reservation is None:
        return False
    requested_target = reservation.request.requested_placement_target
    resolved_target = reservation.request.resolved_target_instance_id
    decision_digest = reservation.request.placement_decision_digest
    if resolved_target is None and decision_digest is None:
        return bool(
            requested_target
            and record.target_instance_id == requested_target
            and record.placement_decision is None
            and provenance.resolved_target_instance_id is None
            and provenance.placement_decision_digest is None
        )
    return bool(
        resolved_target
        and decision_digest
        and record.target_instance_id == resolved_target
        and provenance.resolved_target_instance_id == resolved_target
        and provenance.placement_decision_digest == decision_digest
        and goal_dispatch_placement_decision_digest(record.placement_decision)
        == decision_digest
    )


def _bind_goal_dispatch_placement(
    ctx: AppContext,
    provenance: GoalDispatchProvenance | None,
    *,
    selected_authority: str,
    operation_key: str,
    target_instance_id: str,
    placement_input_digest: str,
    placement_decision: dict[str, Any],
) -> GoalDispatchProvenance | None:
    """Persist the resolved target and decision on the canonical reservation."""

    if provenance is None:
        return None
    requested_target = str(provenance.requested_placement_target or "").strip()
    if (
        not requested_target
        or provenance.placement_input_digest != placement_input_digest
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_placement_mismatch", "recoverable": False},
        )
    if str(placement_decision.get("chosen_instance_id") or "") != target_instance_id:
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_placement_mismatch", "recoverable": False},
        )
    if requested_target.startswith("placement:"):
        requested_policy = requested_target.partition(":")[2]
        if str(placement_decision.get("policy") or "") != requested_policy:
            raise HTTPException(
                status_code=409,
                detail={"code": "goal_placement_mismatch", "recoverable": False},
            )
    elif requested_target != target_instance_id:
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_placement_mismatch", "recoverable": False},
        )
    goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
    state = governance.get_state(goal.id)
    reservation = next(
        (
            item
            for item in state.action_reservations
            if item.id == provenance.action_reservation_id
        ),
        None,
    )
    if (
        provenance.authority_instance_id != selected_authority
        or reservation is None
        or reservation.state != GoalReservationState.APPLIED
        or reservation.goal_id != provenance.goal_id
        or reservation.action_class != provenance.action_class
        or reservation.actor_principal != provenance.actor_principal
        or reservation.authority_instance_id != selected_authority
        or reservation.goal_version != provenance.goal_version
        or reservation.policy_revision != provenance.policy_revision
        or reservation.fencing_token != provenance.fencing_token
        or reservation.request.operation_key != provenance.operation_key
        or provenance.operation_key != operation_key
        or str(reservation.request.provider_id or "").strip().lower()
        != str(provenance.provider_id or "").strip().lower()
        or reservation.request.requested_placement_target != requested_target
        or reservation.request.placement_input_digest != placement_input_digest
        or reservation.attempt != provenance.reservation_attempt
        or reservation.max_attempts != provenance.max_reservation_attempts
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "invalid_goal_reservation", "recoverable": False},
        )
    decision_digest = goal_dispatch_placement_decision_digest(placement_decision)
    binding_digest = hashlib.sha256(
        json.dumps(
            {
                "requested": requested_target,
                "input": placement_input_digest,
                "target": target_instance_id,
                "decision": decision_digest,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    try:
        placement_context = GovernanceMutationContext(
            actor_principal=provenance.actor_principal,
            authority_instance_id=selected_authority,
            idempotency_key=(
                f"goal-dispatch:{provenance.action_reservation_id}:"
                f"placement:{binding_digest}"
            )[:200],
            expected_version=state.version,
            policy_revision=goal.policy.revision,
            goal_version=goal.version,
            fencing_token=goal.lease.fencing_token,
        )
        _state, reservation = governance.bind_dispatch_placement(
            goal.id,
            provenance.action_reservation_id,
            _goal_governance_replay_context(
                governance,
                goal,
                placement_context,
            ),
            requested_placement_target=requested_target,
            placement_input_digest=placement_input_digest,
            resolved_target_instance_id=target_instance_id,
            placement_decision_digest=decision_digest,
        )
    except GoalGovernanceConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_placement_mismatch",
                "message": str(exc),
                "recoverable": False,
            },
        ) from exc
    return provenance.model_copy(
        update={
            "goal_version": reservation.goal_version,
            "policy_revision": reservation.policy_revision,
            "fencing_token": reservation.fencing_token,
            "requested_placement_target": (
                reservation.request.requested_placement_target
            ),
            "placement_input_digest": reservation.request.placement_input_digest,
            "resolved_target_instance_id": (
                reservation.request.resolved_target_instance_id
            ),
            "placement_decision_digest": (
                reservation.request.placement_decision_digest
            ),
        }
    )


def _canonical_goal_materialization_envelope(
    ctx: AppContext,
    provenance: GoalDispatchProvenance,
    *,
    body: RemoteAgentStartBody,
    card: Any,
    plan: MaterializationPlan,
) -> GoalMaterializationEnvelopeV1:
    """Reconstruct the pre-reservation envelope from canonical server state."""

    goal, _governance = _goal_dispatch_services(ctx, provenance.goal_id)
    bound_envelope = provenance.materialization_envelope
    package = next(
        (
            item
            for item in goal.work_packages
            if bound_envelope is not None and item.id == bound_envelope.work_package_id
        ),
        None,
    )
    if package is None or package.card_id != body.card_id:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_materialization_envelope_mismatch",
                "recoverable": False,
            },
        )
    service_role = "verifier" if package.role.value == "verifier" else "executor"
    active_attachments = [
        item
        for item in (card.attachments if card is not None else [])
        if str(getattr(item.state, "value", item.state)) == "active"
    ]
    repository_ids = tuple(str(item["repository_id"]) for item in plan.repositories)
    requested_target = str(provenance.requested_placement_target or "").strip()
    if not requested_target:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_materialization_envelope_mismatch",
                "recoverable": False,
            },
        )
    claims = (
        GoalMaterializationResourceClaimV1(
            key=f"fleet-dispatch:{requested_target}",
            access="shared",
            quantity=1,
            preemptible=True,
        ),
        *(
            GoalMaterializationResourceClaimV1(
                key=f"repository:{repository_id}",
                access="shared",
                quantity=1,
                preemptible=True,
            )
            for repository_id in repository_ids
        ),
    )
    return GoalMaterializationEnvelopeV1(
        work_package_id=package.id,
        service_role=service_role,
        repository_ids=repository_ids,
        data_scopes=tuple(goal.policy.data_scope),
        attachment_ids=tuple(item.attachment_id for item in active_attachments),
        attachment_classes=tuple(
            item.media_type.strip().lower() for item in active_attachments
        ),
        resource_claims=claims,
        execution_contract_digest=canonical_materialization_digest(
            body.execution_contract
        ),
    )


def _bind_goal_dispatch_materialization(
    ctx: AppContext,
    provenance: GoalDispatchProvenance | None,
    *,
    selected_authority: str,
    body: RemoteAgentStartBody,
    card: Any,
    plan: MaterializationPlan,
    target_instance_id: str,
) -> GoalDispatchProvenance | None:
    """Verify and durably bind one exact target-dependent materialization plan."""

    if provenance is None:
        return None
    envelope = provenance.materialization_envelope
    expected_envelope = _canonical_goal_materialization_envelope(
        ctx,
        provenance,
        body=body,
        card=card,
        plan=plan,
    )
    if envelope is None or envelope != expected_envelope:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_materialization_envelope_mismatch",
                "recoverable": False,
            },
        )
    provider_id = str(body.provider or "").strip().lower()
    if not provider_id:
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_provider_unresolved", "recoverable": False},
        )
    receipt = GoalMaterializationReceiptV1(
        envelope_digest=str(envelope.digest),
        target_instance_id=target_instance_id,
        provider_id=provider_id,
        model_id=body.model_id,
        mode_id=body.mode_id,
        materialization_plan_digest=canonical_materialization_digest(
            plan.model_dump(mode="json")
        ),
    )
    goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
    state = governance.get_state(goal.id)
    try:
        _state, reservation = governance.bind_dispatch_materialization(
            goal.id,
            provenance.action_reservation_id,
            GovernanceMutationContext(
                actor_principal=provenance.actor_principal,
                authority_instance_id=selected_authority,
                idempotency_key=(
                    f"goal-dispatch:{provenance.action_reservation_id}:"
                    f"materialization:{receipt.digest}"
                )[:200],
                expected_version=state.version,
                policy_revision=goal.policy.revision,
                goal_version=goal.version,
                fencing_token=goal.lease.fencing_token,
            ),
            envelope=envelope,
            receipt=receipt,
        )
    except GoalGovernanceConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_materialization_binding_mismatch",
                "message": str(exc),
                "recoverable": False,
            },
        ) from exc
    return provenance.model_copy(
        update={
            "goal_version": reservation.goal_version,
            "policy_revision": reservation.policy_revision,
            "fencing_token": reservation.fencing_token,
            "materialization_envelope": reservation.request.materialization_envelope,
            "materialization_receipt": reservation.request.materialization_receipt,
        }
    )


def _expected_goal_dispatch_execution_identity(
    provenance: GoalDispatchProvenance,
    session_id: str,
) -> GoalExecutionIdentityV1:
    """Derive the one role-correct identity allowed for an allocated session."""

    envelope = provenance.materialization_envelope
    receipt = provenance.materialization_receipt
    provider_id = str(provenance.provider_id or "").strip().lower()
    target_instance_id = str(provenance.resolved_target_instance_id or "").strip()
    if (
        envelope is None
        or receipt is None
        or receipt.envelope_digest != envelope.digest
        or not provider_id
        or not target_instance_id
        or receipt.provider_id.strip().lower() != provider_id
        or receipt.target_instance_id != target_instance_id
        or not session_id.strip()
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_prerequisite_missing",
                "recoverable": False,
            },
        )
    principal_scope = hashlib.sha256(
        (
            f"{provenance.goal_id}:{envelope.work_package_id}:"
            f"{envelope.service_role}:{provider_id}:{target_instance_id}:"
            f"{session_id}:{provenance.fencing_token}"
        ).encode()
    ).hexdigest()
    return GoalExecutionIdentityV1(
        work_package_id=envelope.work_package_id,
        service_role=envelope.service_role,
        assigned_service_principal=(
            f"service:goal-{envelope.service_role}:{principal_scope}"
        ),
        provider_id=provider_id,
        target_instance_id=target_instance_id,
        session_id=session_id,
        fencing_token=provenance.fencing_token,
        materialization_receipt_digest=str(receipt.digest),
    )


def _goal_dispatch_assigned_service_scope(
    provenance: GoalDispatchProvenance,
    identity: GoalExecutionIdentityV1,
    dispatch_id: str,
) -> GoalAssignedServiceScope:
    """Derive the sole credential scope from authority-owned dispatch identity."""

    return GoalAssignedServiceScope(
        goal_id=provenance.goal_id,
        work_package_id=identity.work_package_id,
        run_id=dispatch_id,
        session_id=identity.session_id,
        provider_id=identity.provider_id,
        target_instance_id=identity.target_instance_id,
        authority_instance_id=provenance.authority_instance_id,
        fencing_token=identity.fencing_token,
        assigned_service_principal=identity.assigned_service_principal,
        service_role=GoalActorRole(identity.service_role),
    )


def _bind_goal_dispatch_execution_identity(
    ctx: AppContext,
    provenance: GoalDispatchProvenance | None,
    *,
    selected_authority: str,
    session_id: str,
) -> GoalDispatchProvenance | None:
    """Persist the base session identity or restore its durable final upgrade."""

    if provenance is None:
        return None
    identity = _expected_goal_dispatch_execution_identity(provenance, session_id)
    if (
        provenance.execution_identity is not None
        and not identity.allows_credential_upgrade_to(
            provenance.execution_identity
        )
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": (
                    "Durable dispatch provenance contains another execution identity."
                ),
                "recoverable": False,
            },
        )
    goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
    state = governance.get_state(goal.id)
    existing = next(
        (
            item
            for item in state.action_reservations
            if item.id == provenance.action_reservation_id
        ),
        None,
    )
    if (
        existing is not None
        and existing.request.execution_identity is not None
        and identity.allows_credential_upgrade_to(
            existing.request.execution_identity
        )
    ):
        return provenance.model_copy(
            update={
                "goal_version": existing.goal_version,
                "policy_revision": existing.policy_revision,
                "fencing_token": existing.fencing_token,
                "execution_identity": existing.request.execution_identity,
            }
        )
    try:
        _state, reservation = governance.bind_dispatch_execution_identity(
            goal.id,
            provenance.action_reservation_id,
            GovernanceMutationContext(
                actor_principal=provenance.actor_principal,
                authority_instance_id=selected_authority,
                idempotency_key=(
                    f"goal-dispatch:{provenance.action_reservation_id}:"
                    f"execution-identity:{identity.digest}"
                )[:200],
                expected_version=state.version,
                policy_revision=goal.policy.revision,
                goal_version=goal.version,
                fencing_token=goal.lease.fencing_token,
            ),
            identity=identity,
        )
    except GoalGovernanceConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": str(exc),
                "recoverable": False,
            },
        ) from exc
    return provenance.model_copy(
        update={
            "goal_version": reservation.goal_version,
            "policy_revision": reservation.policy_revision,
            "fencing_token": reservation.fencing_token,
            "execution_identity": reservation.request.execution_identity,
        }
    )


def _bind_goal_dispatch_assigned_service_identity(
    ctx: AppContext,
    provenance: GoalDispatchProvenance | None,
    *,
    selected_authority: str,
    dispatch_id: str,
    session_id: str,
    issue_if_missing: bool = True,
) -> GoalDispatchProvenance | None:
    """Issue and bind the exact server-side grant before provider traffic."""

    provenance = _bind_goal_dispatch_execution_identity(
        ctx,
        provenance,
        selected_authority=selected_authority,
        session_id=session_id,
    )
    if provenance is None or provenance.execution_identity is None:
        return provenance
    identity = provenance.execution_identity
    scope = _goal_dispatch_assigned_service_scope(
        provenance,
        identity,
        dispatch_id,
    )
    goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
    if identity.credential_digest is not None:
        try:
            authorization = governance.resolve_assigned_service_binding(
                scope,
                required_roles={GoalActorRole.EXECUTOR, GoalActorRole.VERIFIER},
            )
        except GoalAssignedServiceCredentialError as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_execution_identity_mismatch",
                    "message": str(exc),
                    "recoverable": False,
                },
            ) from exc
        if (
            authorization.binding.credential_digest != identity.credential_digest
            or authorization.binding.expires_at != identity.credential_expires_at
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_execution_identity_mismatch",
                    "message": "Execution identity carries another credential grant.",
                    "recoverable": False,
                },
            )
        return provenance

    try:
        recovered = governance.recover_pending_assigned_service_binding(scope)
        if recovered is None:
            if not issue_if_missing:
                return provenance
            state = governance.get_state(goal.id)
            binding, credential = governance.issue_assigned_service_credential(
                scope,
                GovernanceMutationContext(
                    actor_principal=provenance.actor_principal,
                    authority_instance_id=selected_authority,
                    idempotency_key=(
                        f"goal-dispatch:{provenance.action_reservation_id}:"
                        f"assigned-service:{identity.digest}"
                    )[:200],
                    expected_version=state.version,
                    policy_revision=goal.policy.revision,
                    goal_version=goal.version,
                    fencing_token=goal.lease.fencing_token,
                ),
                ttl_seconds=ASSIGNED_SERVICE_CREDENTIAL_TTL_SECONDS,
            )
            # The provider receives only a target-local HMAC capability. PA needs no
            # plaintext Goal credential after proving this deterministic grant.
            del credential
        else:
            binding = recovered.binding
        final_identity = GoalExecutionIdentityV1.model_validate(
            {
                **identity.model_dump(mode="python", exclude={"digest"}),
                "credential_digest": binding.credential_digest,
                "credential_expires_at": binding.expires_at,
            }
        )
        state = governance.get_state(goal.id)
        _state, reservation = governance.bind_dispatch_execution_identity(
            goal.id,
            provenance.action_reservation_id,
            GovernanceMutationContext(
                actor_principal=provenance.actor_principal,
                authority_instance_id=selected_authority,
                idempotency_key=(
                    f"goal-dispatch:{provenance.action_reservation_id}:"
                    f"execution-identity:{final_identity.digest}"
                )[:200],
                expected_version=state.version,
                policy_revision=goal.policy.revision,
                goal_version=goal.version,
                fencing_token=goal.lease.fencing_token,
            ),
            identity=final_identity,
            assigned_service_binding=binding,
        )
    except (GoalAssignedServiceCredentialError, GoalGovernanceConflict) as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": str(exc),
                "recoverable": False,
            },
        ) from exc
    return provenance.model_copy(
        update={
            "goal_version": reservation.goal_version,
            "policy_revision": reservation.policy_revision,
            "fencing_token": reservation.fencing_token,
            "execution_identity": reservation.request.execution_identity,
        }
    )


def _restore_goal_dispatch_execution_identity(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
) -> DispatchRecord:
    """Repair only the exact durable session/identity crash cut."""

    provenance = record.goal_provenance
    if provenance is None:
        return record
    if record.session_id is None and provenance.execution_identity is None:
        return record
    goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
    reservation = next(
        (
            item
            for item in governance.get_state(goal.id).action_reservations
            if item.id == provenance.action_reservation_id
        ),
        None,
    )
    if reservation is None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": "The execution identity reservation no longer exists.",
                "recoverable": False,
            },
        )
    request = reservation.request
    if not (
        reservation.state == GoalReservationState.APPLIED
        and reservation.goal_id == provenance.goal_id
        and reservation.authority_instance_id
        == provenance.authority_instance_id
        == record.authority_instance_id
        and reservation.actor_principal == provenance.actor_principal
        and reservation.fencing_token == provenance.fencing_token
        and str(request.provider_id or "").strip().lower()
        == str(provenance.provider_id or "").strip().lower()
        and request.resolved_target_instance_id
        == provenance.resolved_target_instance_id
        == record.target_instance_id
        and request.materialization_envelope == provenance.materialization_envelope
        and request.materialization_receipt == provenance.materialization_receipt
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": (
                    "Durable dispatch and reservation identity bindings diverged."
                ),
                "recoverable": False,
            },
        )
    durable_identity = request.execution_identity
    provenance_identity = provenance.execution_identity
    session_ids = {
        str(value).strip()
        for value in (
            record.session_id,
            provenance_identity.session_id if provenance_identity else None,
            durable_identity.session_id if durable_identity else None,
        )
        if value is not None and str(value).strip()
    }
    if not session_ids:
        return record
    if len(session_ids) != 1:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": "Durable execution identity refers to another session.",
                "recoverable": False,
            },
        )
    session_id = next(iter(session_ids))
    expected = _expected_goal_dispatch_execution_identity(provenance, session_id)
    if any(
        identity is not None
        and not expected.allows_credential_upgrade_to(identity)
        for identity in (provenance_identity, durable_identity)
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": "Durable execution identity is not the canonical binding.",
                "recoverable": False,
            },
        )
    restored = _bind_goal_dispatch_execution_identity(
        ctx,
        provenance,
        selected_authority=record.authority_instance_id,
        session_id=session_id,
    )
    if restored is None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "recoverable": False,
            },
        )
    restored = _bind_goal_dispatch_assigned_service_identity(
        ctx,
        restored,
        selected_authority=record.authority_instance_id,
        dispatch_id=record.dispatch_id,
        session_id=session_id,
        issue_if_missing=False,
    )
    if restored is None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "recoverable": False,
            },
        )
    record.goal_provenance = restored
    record.session_id = session_id
    record.resume_requested = True
    record.resume_session_id = session_id
    if not goal_dispatch_execution_identity_valid(record):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "message": "Recovered execution identity failed exact validation.",
                "recoverable": False,
            },
        )
    ledger.put(record)
    return record


def _restore_goal_dispatch_placement_binding(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
) -> DispatchRecord:
    """Recover provenance only from an already-bound canonical reservation."""

    provenance = record.goal_provenance
    if provenance is None:
        return record
    try:
        _goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
        reservation = next(
            (
                item
                for item in governance.get_state(provenance.goal_id).action_reservations
                if item.id == provenance.action_reservation_id
            ),
            None,
        )
    except HTTPException, GoalGovernanceConflict:
        return record
    if reservation is None:
        return record
    request = reservation.request
    decision_digest = goal_dispatch_placement_decision_digest(record.placement_decision)
    if not (
        request.operation_key == record.idempotency_key == provenance.operation_key
        and reservation.goal_id == provenance.goal_id
        and reservation.authority_instance_id == provenance.authority_instance_id
        and reservation.actor_principal == provenance.actor_principal
        and reservation.action_class == provenance.action_class
        and str(request.provider_id or "").strip().lower()
        == str(provenance.provider_id or "").strip().lower()
        and request.requested_placement_target == provenance.requested_placement_target
        and request.placement_input_digest
        == provenance.placement_input_digest
        == record.goal_placement_input_digest
        and goal_dispatch_record_placement_input_valid(record)
        and request.resolved_target_instance_id == record.target_instance_id
        and request.placement_decision_digest == decision_digest
        and request.materialization_envelope == provenance.materialization_envelope
        and request.materialization_receipt == provenance.materialization_receipt
        and goal_dispatch_materialization_binding_valid(record)
        and request.execution_identity == provenance.execution_identity
        and goal_dispatch_execution_identity_valid(record)
        and request.resolved_target_instance_id
        and request.placement_decision_digest
    ):
        return record
    record.goal_provenance = provenance.model_copy(
        update={
            "resolved_target_instance_id": request.resolved_target_instance_id,
            "placement_decision_digest": request.placement_decision_digest,
        }
    )
    return ledger.put(record)


def _mark_goal_admission_validated(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
) -> DispatchRecord:
    provenance = record.goal_provenance
    if (
        not _goal_admission_operation_bound(ctx, record)
        or provenance is None
        or not provenance.resolved_target_instance_id
        or not provenance.placement_decision_digest
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_admission_operation_mismatch",
                "recoverable": False,
            },
        )
    record.goal_admission_validation_state = "validated"
    record.goal_admission_validated_at = datetime.now(UTC)
    record.goal_admission_validation_proof = goal_admission_validation_proof(record)
    record.goal_admission_validation_error = None
    return ledger.put(record)


def _goal_admission_proof_valid(
    ctx: AppContext,
    record: DispatchRecord,
) -> bool:
    return bool(
        record.goal_admission_validation_state == "validated"
        and record.goal_admission_validated_at is not None
        and record.goal_admission_validation_proof
        == goal_admission_validation_proof(record)
        and _goal_admission_operation_bound(ctx, record)
    )


def _reject_unvalidated_goal_admission(
    ledger: DispatchStore,
    record: DispatchRecord,
    message: str,
) -> DispatchRecord:
    record.goal_admission_validation_state = "rejected"
    record.goal_admission_validation_error = message[:1000]
    return ledger.fail(
        record,
        message,
        code="invalid_goal_admission_trace",
        recoverable=False,
        detail={"reservation_released": False, "validation_state": "rejected"},
    )


def _validate_goal_dispatch_record(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
    *,
    sink: str,
) -> DispatchRecord:
    if record.goal_provenance is not None and not _goal_dispatch_lifecycle_owned(
        ctx, record
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "target_goal_dispatch_copy_not_launchable",
                "recoverable": False,
            },
        )
    refreshed = _validate_goal_dispatch_provenance(
        ctx,
        record.goal_provenance,
        record.authority_instance_id,
        sink=sink,
        provider_id=record.request_payload.get("provider"),
        target_instance_id=record.target_instance_id,
        placement_input_digest=record.goal_placement_input_digest,
        placement_decision_digest=goal_dispatch_placement_decision_digest(
            record.placement_decision
        ),
        denial_applied=_goal_dispatch_was_applied(record),
    )
    if refreshed is not None and not goal_dispatch_materialization_binding_valid(
        record
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_materialization_binding_mismatch",
                "recoverable": False,
            },
        )
    if refreshed is not None and not goal_dispatch_execution_identity_valid(record):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_execution_identity_mismatch",
                "recoverable": False,
            },
        )
    if refreshed != record.goal_provenance:
        record.goal_provenance = refreshed
        ledger.put(record)
    return record


def _release_goal_action_provenance(
    ctx: AppContext,
    provenance: GoalDispatchProvenance,
    *,
    operation_id: str,
    outcome: str,
    applied: bool,
) -> GoalDispatchProvenance:
    """Release one authority-owned goal action and return durable provenance."""

    if provenance.authority_instance_id != ctx.settings.instance_id:
        return provenance
    goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
    state = governance.get_state(goal.id)
    reservation = next(
        (
            item
            for item in state.action_reservations
            if item.id == provenance.action_reservation_id
        ),
        None,
    )
    if reservation is None:
        raise GoalGovernanceConflict("goal dispatch reservation disappeared")
    if reservation.state != GoalReservationState.RELEASED:
        release_context = GovernanceMutationContext(
            actor_principal=(
                f"service:goal-dispatch-lifecycle:{reservation.authority_instance_id}"
            ),
            authority_instance_id=reservation.authority_instance_id,
            idempotency_key=(
                f"goal-dispatch:{operation_id}:release:{outcome}:"
                f"attempt:{reservation.attempt}"
            )[:200],
            expected_version=state.version,
            policy_revision=goal.policy.revision,
            goal_version=goal.version,
            fencing_token=reservation.fencing_token,
        )
        state = governance.reconcile_action_release(
            goal.id,
            reservation.id,
            release_context,
            actual_usage=(
                reservation.reserved_usage.model_copy(deep=True)
                if applied
                else GoalUsage()
            ),
            reason=f"fleet dispatch {outcome}",
        )
        reservation = next(
            item for item in state.action_reservations if item.id == reservation.id
        )
    return provenance.model_copy(
        update={
            "goal_version": reservation.goal_version,
            "policy_revision": reservation.policy_revision,
            "fencing_token": reservation.fencing_token,
            "released_at": reservation.released_at,
            "release_reason": reservation.release_reason,
        }
    )


def _release_goal_dispatch_reservation(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
    *,
    outcome: str,
    applied: bool,
) -> DispatchRecord:
    provenance = record.goal_provenance
    if provenance is None:
        return record
    if not _goal_dispatch_lifecycle_owned(ctx, record):
        return record
    record.goal_provenance = _release_goal_action_provenance(
        ctx,
        provenance,
        operation_id=record.dispatch_id,
        outcome=outcome,
        applied=applied,
    )
    ledger.put(record)
    return record


def _goal_dispatch_was_applied(record: DispatchRecord) -> bool:
    """Return whether an irreversible dispatch sink was durably reached."""

    return bool(
        record.state in {"running", "completion_pending", "completed"}
        or record.session_id
        or record.prompt_acknowledged_at
        or any(
            event.state in {"running", "completion_pending", "completed"}
            for event in record.events
        )
    )


def _goal_followup_operation_key(record: DispatchRecord, idempotency_key: str) -> str:
    digest = hashlib.sha256(idempotency_key.encode()).hexdigest()
    return f"dispatch-followup:{record.dispatch_id}:{digest}"[:200]


def _goal_provenance_from_reservation(
    base: GoalDispatchProvenance,
    reservation,
) -> GoalDispatchProvenance:
    return GoalDispatchProvenance(
        goal_id=base.goal_id,
        goal_version=reservation.goal_version,
        policy_revision=reservation.policy_revision,
        authority_instance_id=reservation.authority_instance_id,
        fencing_token=reservation.fencing_token or 0,
        action_reservation_id=reservation.id,
        operation_key=reservation.request.operation_key,
        requested_placement_target=(reservation.request.requested_placement_target),
        placement_input_digest=reservation.request.placement_input_digest,
        resolved_target_instance_id=(reservation.request.resolved_target_instance_id),
        placement_decision_digest=(reservation.request.placement_decision_digest),
        materialization_envelope=reservation.request.materialization_envelope,
        materialization_receipt=reservation.request.materialization_receipt,
        execution_identity=reservation.request.execution_identity,
        actor_principal=reservation.actor_principal,
        action_class=base.action_class,
        provider_id=reservation.request.provider_id,
        reservation_attempt=reservation.attempt,
        max_reservation_attempts=reservation.max_attempts,
    )


def _reserve_goal_dispatch_followup(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
    *,
    idempotency_key: str,
    fingerprint: str,
) -> GoalDispatchProvenance | None:
    """Durably stage, reserve, and apply one fresh governed follow-up action."""

    base = record.goal_provenance
    if base is None:
        return None
    if not _goal_dispatch_lifecycle_owned(ctx, record):
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_followup_wrong_authority", "recoverable": False},
        )
    if not goal_dispatch_materialization_binding_valid(record):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_followup_materialization_widening",
                "recoverable": False,
            },
        )
    if not goal_dispatch_execution_identity_valid(record):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_followup_execution_identity_mismatch",
                "recoverable": False,
            },
        )
    provider_id = (
        str(record.request_payload.get("provider") or base.provider_id or "")
        .strip()
        .lower()
    )
    if not provider_id:
        raise HTTPException(
            status_code=409,
            detail={"code": "goal_provider_unresolved", "recoverable": False},
        )
    goal, governance = _goal_dispatch_services(ctx, base.goal_id)
    if (
        not goal.lease.active()
        or goal.lease.holder_instance_id != base.authority_instance_id
        or goal.lease.fencing_token != base.fencing_token
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "stale_goal_fence", "recoverable": False},
        )
    operation_key = _goal_followup_operation_key(record, idempotency_key)
    operation = record.followup_operations.setdefault(
        idempotency_key,
        {
            "fingerprint": fingerprint,
            "state": "governance_pending",
            "operation_key": operation_key,
        },
    )
    if operation.get("fingerprint") != fingerprint:
        raise HTTPException(
            status_code=409,
            detail={"code": "idempotency_conflict", "recoverable": False},
        )
    operation["state"] = "governance_pending"
    operation["operation_key"] = operation_key
    ledger.put(record)

    state = governance.get_state(goal.id)
    reservation = next(
        (
            item
            for item in state.action_reservations
            if item.request.operation_key == operation_key
        ),
        None,
    )
    if reservation is None:
        try:
            state, decision = governance.authorize_action(
                goal.id,
                GoalActionRequest(
                    action_class=base.action_class,
                    operation_key=operation_key,
                    requested_placement_target=base.requested_placement_target,
                    placement_input_digest=base.placement_input_digest,
                    resolved_target_instance_id=base.resolved_target_instance_id,
                    placement_decision_digest=base.placement_decision_digest,
                    materialization_envelope=base.materialization_envelope,
                    materialization_receipt=base.materialization_receipt,
                    execution_identity=base.execution_identity,
                    delegated=True,
                    provider_id=provider_id,
                    estimate=GoalUsage(actions=1, dispatches=1),
                    resource_claims=[
                        GoalResourceClaim(
                            key=item.key,
                            access=ResourceAccess(item.access),
                            quantity=item.quantity,
                            preemptible=item.preemptible,
                            expires_at=item.expires_at,
                        )
                        for item in (
                            base.materialization_envelope.resource_claims
                            if base.materialization_envelope is not None
                            else ()
                        )
                    ],
                    max_attempts=1,
                ),
                GovernanceMutationContext(
                    actor_principal=base.actor_principal,
                    authority_instance_id=base.authority_instance_id,
                    idempotency_key=f"{operation_key}:reserve"[:200],
                    expected_version=state.version,
                    policy_revision=goal.policy.revision,
                    goal_version=goal.version,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
        except GoalGovernanceConflict as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_followup_governance_denied",
                    "message": str(exc),
                    "recoverable": False,
                },
            ) from exc
        if (
            decision.disposition != GoalActionDisposition.AUTHORIZED
            or not decision.reservation_id
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_followup_governance_denied",
                    "message": "; ".join(decision.reasons),
                    "disposition": decision.disposition.value,
                    "recoverable": False,
                },
            )
        reservation = next(
            item
            for item in state.action_reservations
            if item.id == decision.reservation_id
        )
    provenance = _goal_provenance_from_reservation(base, reservation)
    operation["goal_provenance"] = provenance.model_dump(mode="json")
    operation["state"] = (
        "reservation_applied"
        if reservation.state == GoalReservationState.APPLIED
        else "reserved"
    )
    ledger.put(record)
    if reservation.state == GoalReservationState.RELEASED:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_followup_reservation_released",
                "recoverable": False,
            },
        )
    if reservation.state != GoalReservationState.APPLIED:
        try:
            state, applied = governance.apply_action(
                goal.id,
                reservation.id,
                GovernanceMutationContext(
                    actor_principal=base.actor_principal,
                    authority_instance_id=base.authority_instance_id,
                    idempotency_key=f"{operation_key}:apply"[:200],
                    expected_version=state.version,
                    policy_revision=goal.policy.revision,
                    goal_version=goal.version,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
        except GoalGovernanceConflict as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_followup_governance_denied",
                    "message": str(exc),
                    "recoverable": False,
                },
            ) from exc
        if applied.disposition != GoalActionDisposition.AUTHORIZED:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_followup_governance_denied",
                    "message": "; ".join(applied.reasons),
                    "disposition": applied.disposition.value,
                    "recoverable": False,
                },
            )
        reservation = next(
            item for item in state.action_reservations if item.id == reservation.id
        )
        provenance = _goal_provenance_from_reservation(base, reservation)
    operation["goal_provenance"] = provenance.model_dump(mode="json")
    operation["state"] = "reservation_applied"
    ledger.put(record)
    return provenance


def _release_goal_dispatch_followup(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
    *,
    idempotency_key: str,
    outcome: str,
    applied: bool,
    final_state: str,
) -> DispatchRecord:
    operation = record.followup_operations.get(idempotency_key)
    if not operation or not operation.get("goal_provenance"):
        return record
    provenance = GoalDispatchProvenance.model_validate(operation["goal_provenance"])
    provenance = _release_goal_action_provenance(
        ctx,
        provenance,
        operation_id=f"{record.dispatch_id}:followup:{idempotency_key}",
        outcome=outcome,
        applied=applied,
    )
    operation["goal_provenance"] = provenance.model_dump(mode="json")
    operation["state"] = final_state
    operation["released_at"] = (
        provenance.released_at.isoformat() if provenance.released_at else None
    )
    ledger.put(record)
    return record


def _reconcile_goal_dispatch_followups(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
) -> DispatchRecord:
    """Close every authority-side governed follow-up crash window."""

    base = record.goal_provenance
    if base is None or not _goal_dispatch_lifecycle_owned(ctx, record):
        return record
    for idempotency_key, operation in list(record.followup_operations.items()):
        raw_provenance = operation.get("goal_provenance")
        if raw_provenance and raw_provenance.get("released_at"):
            continue
        if not raw_provenance and operation.get("state") != "governance_pending":
            continue
        goal, governance = _goal_dispatch_services(ctx, base.goal_id)
        state = governance.get_state(goal.id)
        reservation = None
        if raw_provenance:
            provenance = GoalDispatchProvenance.model_validate(raw_provenance)
            reservation = next(
                (
                    item
                    for item in state.action_reservations
                    if item.id == provenance.action_reservation_id
                ),
                None,
            )
        else:
            operation_key = str(operation.get("operation_key") or "")
            reservation = next(
                (
                    item
                    for item in state.action_reservations
                    if item.request.operation_key == operation_key
                ),
                None,
            )
            if reservation is None:
                operation["state"] = "failed"
                operation["error"] = {
                    "code": "goal_followup_governance_interrupted",
                    "message": "Follow-up governance ended before a reservation was created.",
                    "recoverable": True,
                }
                ledger.put(record)
                continue
            provenance = _goal_provenance_from_reservation(base, reservation)
            operation["goal_provenance"] = provenance.model_dump(mode="json")
        if reservation is None:
            operation["state"] = "failed"
            operation["error"] = {
                "code": "goal_followup_reservation_missing",
                "recoverable": False,
            }
            ledger.put(record)
            continue
        prior_state = str(operation.get("state") or "")
        accepted = prior_state in {"accepted", "accepted_pending_release"}
        cancelled = prior_state in {"cancelled", "cancelled_pending_release"}
        failed = prior_state in {"failed", "failed_pending_release"}
        applied = accepted or (
            not cancelled
            and not failed
            and reservation.state == GoalReservationState.APPLIED
        )
        final_state = (
            "accepted"
            if accepted
            else "cancelled"
            if cancelled
            else "failed"
            if failed
            else "interrupted"
        )
        operation["state"] = f"{final_state}_pending_release"
        if final_state == "interrupted":
            operation["error"] = {
                "code": "goal_followup_delivery_interrupted",
                "message": "Follow-up delivery outcome was interrupted; usage was conservatively accounted.",
                "recoverable": True,
            }
        ledger.put(record)
        record = _release_goal_dispatch_followup(
            ctx,
            ledger,
            record,
            idempotency_key=idempotency_key,
            outcome=f"followup-{final_state}",
            applied=applied,
            final_state=final_state,
        )
    return record


def _replace_goal_dispatch_reservation(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
    *,
    idempotency_key: str,
) -> DispatchRecord:
    provenance = record.goal_provenance
    if provenance is None:
        return record
    if not _goal_dispatch_lifecycle_owned(ctx, record):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_retry_wrong_authority",
                "recoverable": False,
            },
        )
    if not goal_dispatch_materialization_binding_valid(record):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_retry_materialization_widening",
                "recoverable": False,
            },
        )
    if not goal_dispatch_execution_identity_valid(record):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_retry_execution_identity_mismatch",
                "recoverable": False,
            },
        )
    if provenance.retry_idempotency_key == idempotency_key:
        if provenance.released_at is not None:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_retry_reservation_released",
                    "recoverable": True,
                },
            )
        return record
    goal, governance = _goal_dispatch_services(ctx, provenance.goal_id)
    if (
        not goal.lease.active()
        or goal.lease.holder_instance_id != provenance.authority_instance_id
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "stale_goal_fence", "recoverable": False},
        )
    state = governance.get_state(goal.id)
    context = GovernanceMutationContext(
        actor_principal=provenance.actor_principal,
        authority_instance_id=provenance.authority_instance_id,
        idempotency_key=(
            f"goal-dispatch:{record.dispatch_id}:retry:{idempotency_key}:reserve"
        )[:200],
        expected_version=state.version,
        policy_revision=goal.policy.revision,
        goal_version=goal.version,
        fencing_token=goal.lease.fencing_token,
    )
    try:
        state, replacement, decision = governance.replace_action_reservation(
            goal.id,
            provenance.action_reservation_id,
            _goal_governance_replay_context(governance, goal, context),
        )
        if (
            replacement is None
            or decision.disposition != GoalActionDisposition.AUTHORIZED
        ):
            raise GoalGovernanceConflict(
                "canonical governance denied the dispatch retry: "
                + "; ".join(decision.reasons)
            )
        apply_context = GovernanceMutationContext(
            actor_principal=provenance.actor_principal,
            authority_instance_id=provenance.authority_instance_id,
            idempotency_key=(
                f"goal-dispatch:{record.dispatch_id}:retry:{idempotency_key}:apply"
            )[:200],
            expected_version=state.version,
            policy_revision=goal.policy.revision,
            goal_version=goal.version,
            fencing_token=goal.lease.fencing_token,
        )
        state, applied = governance.apply_action(
            goal.id,
            replacement.id,
            _goal_governance_replay_context(governance, goal, apply_context),
        )
        if applied.disposition != GoalActionDisposition.AUTHORIZED:
            raise GoalGovernanceConflict(
                "canonical governance denied the dispatch retry at apply time: "
                + "; ".join(applied.reasons)
            )
        replacement = next(
            item for item in state.action_reservations if item.id == replacement.id
        )
    except GoalGovernanceConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "goal_retry_denied",
                "message": str(exc),
                "recoverable": False,
            },
        ) from exc
    record.goal_provenance = provenance.model_copy(
        update={
            "goal_version": replacement.goal_version,
            "policy_revision": replacement.policy_revision,
            "fencing_token": replacement.fencing_token,
            "action_reservation_id": replacement.id,
            "reservation_attempt": replacement.attempt,
            "max_reservation_attempts": replacement.max_attempts,
            "retry_idempotency_key": idempotency_key,
            "released_at": None,
            "release_reason": None,
        }
    )
    ledger.put(record)
    return record


async def _release_goal_dispatch_reservation_async(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
    *,
    outcome: str,
    applied: bool,
) -> DispatchRecord:
    if (
        record.goal_provenance is not None
        and record.goal_provenance.released_at is None
        and not goal_dispatch_execution_identity_valid(record)
        and (
            record.session_id is not None
            or record.goal_provenance.execution_identity is not None
        )
    ):
        # A worker can fail immediately after the target allocates a session.
        # Bind that durable identity before release, otherwise the released hold
        # can no longer be repaired into an attributable execution.
        record = await _offload_ctx(
            ctx,
            "goal.dispatch_execution_identity_restore",
            _restore_goal_dispatch_execution_identity,
            ctx,
            ledger,
            record,
        )
    return await _offload_ctx(
        ctx,
        "goal.dispatch_reservation_release",
        _release_goal_dispatch_reservation,
        ctx,
        ledger,
        record,
        outcome=outcome,
        applied=applied,
    )


async def _validate_goal_dispatch_record_async(
    ctx: AppContext,
    ledger: DispatchStore,
    record: DispatchRecord,
    *,
    sink: str,
) -> DispatchRecord:
    return await _offload_ctx(
        ctx,
        "goal.dispatch_reservation_validate",
        _validate_goal_dispatch_record,
        ctx,
        ledger,
        record,
        sink=sink,
    )


async def _reconcile_goal_dispatch_reservations(ctx: AppContext) -> None:
    """Repair crash windows between a durable dispatch state and its goal hold."""

    ledger: DispatchStore = ctx.require_service("dispatch_store")
    records = await _offload_ctx(
        ctx,
        "goal.dispatch_lifecycle_index",
        ledger.pending_goal_lifecycle,
        ctx.settings.instance_id,
        limit=100,
    )
    for record in records:
        if not _goal_dispatch_lifecycle_owned(ctx, record):
            continue
        try:
            record = await _offload_ctx(
                ctx,
                "goal.dispatch_followup_reconcile",
                _reconcile_goal_dispatch_followups,
                ctx,
                ledger,
                record,
            )
            if record.session_id or (
                record.goal_provenance is not None
                and record.goal_provenance.execution_identity is not None
            ):
                try:
                    record = await _offload_ctx(
                        ctx,
                        "goal.dispatch_execution_identity_restore",
                        _restore_goal_dispatch_execution_identity,
                        ctx,
                        ledger,
                        record,
                    )
                except HTTPException as exc:
                    detail = exc.detail if isinstance(exc.detail, dict) else {}
                    await _offload_ctx(
                        ctx,
                        "goal.dispatch_execution_identity_rejected",
                        ledger.fail,
                        record,
                        str(detail.get("message") or exc.detail),
                        code=str(
                            detail.get("code") or "goal_execution_identity_mismatch"
                        ),
                        recoverable=False,
                        detail=detail,
                    )
                    continue
            if record.state == "admission_pending":
                record = await _offload_ctx(
                    ctx,
                    "goal.dispatch_placement_binding_restore",
                    _restore_goal_dispatch_placement_binding,
                    ctx,
                    ledger,
                    record,
                )
                if not _goal_admission_proof_valid(ctx, record):
                    if not _goal_admission_operation_bound(ctx, record):
                        await _offload_ctx(
                            ctx,
                            "goal.dispatch_admission_rejected",
                            _reject_unvalidated_goal_admission,
                            ledger,
                            record,
                            "Staged goal provenance was not bound to this admission operation.",
                        )
                        continue
                    if (
                        record.goal_provenance is None
                        or not record.goal_provenance.resolved_target_instance_id
                        or not record.goal_provenance.placement_decision_digest
                    ):
                        record = await _offload_ctx(
                            ctx,
                            "goal.dispatch_admission_recovered",
                            ledger.fail,
                            record,
                            "Governed dispatch admission was interrupted before placement resolved.",
                            code="admission_interrupted",
                            recoverable=True,
                            detail={
                                "recovery": ("retry_with_fresh_governance_reservation"),
                                "admission_trace": True,
                                "placement_resolved": False,
                            },
                        )
                        await _release_goal_dispatch_reservation_async(
                            ctx,
                            ledger,
                            record,
                            outcome="admission-interrupted",
                            applied=False,
                        )
                        continue
                    try:
                        record.goal_provenance = await _offload_ctx(
                            ctx,
                            "goal.dispatch_admission_revalidate",
                            _validate_goal_dispatch_provenance,
                            ctx,
                            record.goal_provenance,
                            record.authority_instance_id,
                            sink="durable-admission",
                            provider_id=record.request_payload.get("provider"),
                            target_instance_id=record.target_instance_id,
                            placement_input_digest=(record.goal_placement_input_digest),
                            placement_decision_digest=(
                                goal_dispatch_placement_decision_digest(
                                    record.placement_decision
                                )
                            ),
                        )
                        record = await _offload_ctx(
                            ctx,
                            "goal.dispatch_admission_validation_proof",
                            _mark_goal_admission_validated,
                            ctx,
                            ledger,
                            record,
                        )
                    except HTTPException as exc:
                        detail = exc.detail if isinstance(exc.detail, dict) else {}
                        record = await _offload_ctx(
                            ctx,
                            "goal.dispatch_admission_revalidation_failed",
                            ledger.fail,
                            record,
                            str(detail.get("message") or exc.detail),
                            code=str(
                                detail.get("code")
                                or "goal_admission_revalidation_failed"
                            ),
                            recoverable=bool(detail.get("recoverable", False)),
                            detail=detail,
                        )
                        await _release_goal_dispatch_reservation_async(
                            ctx,
                            ledger,
                            record,
                            outcome="admission-revalidation-failed",
                            applied=False,
                        )
                        continue
                record = await _offload_ctx(
                    ctx,
                    "goal.dispatch_admission_recovered",
                    ledger.fail,
                    record,
                    "Governed dispatch admission was interrupted before durable admission.",
                    code="admission_interrupted",
                    recoverable=True,
                    detail={
                        "recovery": "retry_with_fresh_governance_reservation",
                        "admission_trace": True,
                    },
                )
                await _release_goal_dispatch_reservation_async(
                    ctx,
                    ledger,
                    record,
                    outcome="admission-interrupted",
                    applied=False,
                )
            elif record.state in {
                "running",
                "completion_pending",
                "completed",
                "failed",
                "cancelled",
            }:
                await _release_goal_dispatch_reservation_async(
                    ctx,
                    ledger,
                    record,
                    outcome=f"{record.state}-reconciled",
                    applied=_goal_dispatch_was_applied(record),
                )
        except Exception:
            # Preserve the explicit unreleased provenance. The dispatch worker's
            # bounded lifecycle scan will retry without preventing fleet startup.
            logger.exception(
                "Dispatch %s startup goal-lifecycle reconciliation failed",
                record.dispatch_id,
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
    preadmission_record: DispatchRecord | None = None
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
        if existing.state == "admission_pending":
            raise _admission_in_progress_error(existing)
        else:
            return {
                "accepted": True,
                "duplicate": True,
                "admission": "duplicate",
                "dispatch_id": existing.dispatch_id,
                "job_id": existing.dispatch_id,
                "dispatch": _dispatch_public(request, existing),
            }

    _bind_effective_goal_dispatch_provider(body, settings.agent_provider)
    if preadmission_record is None:
        preadmission_record, created = await _offload_request(
            request,
            "goal.dispatch_admission_trace",
            _persist_goal_dispatch_admission_trace,
            ctx,
            ledger,
            body,
            idempotency_key=idempotency_key,
            request_fingerprint=placement_fingerprint,
            target_instance_id=(
                body.target_instance_id
                or f"placement:{body.placement_policy or 'balanced'}"
            ),
            principal_id=get_principal_id(request),
            placement_policy=str(body.placement_policy or "named_instance"),
            idempotency_scope="authority",
        )
        if preadmission_record is not None and not created:
            raise _admission_in_progress_error(preadmission_record)

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
        error = _dispatch_lookup_error("card", body.card_id)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error
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
        error = _dispatch_lookup_error("project", project_id)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error
    principal_id = get_principal_id(request)
    if project and project.memberships:
        authorized = any(
            membership.principal_id == principal_id
            for membership in project.memberships
        )
        if not authorized and getattr(user, "role", None) != "admin":
            error = HTTPException(
                status_code=403,
                detail={
                    "code": "insufficient_authorization",
                    "message": "This principal is not authorized to dispatch work for the linked project.",
                    "recoverable": False,
                },
            )
            await _reject_goal_dispatch_admission(request, preadmission_record, error)
            raise error

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
        error = _placement_http_error(exc)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error from exc

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
        preadmission_record=preadmission_record,
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
    _submitted_payload, submitted_fingerprint, idempotency_key = (
        _named_dispatch_identity(request, instance_id, body, body.project_id)
    )
    body.idempotency_key = idempotency_key
    ledger = _dispatch_store(request)
    existing_record = await _offload_request(
        request,
        "dispatch.idempotency_read",
        ledger.by_idempotency,
        instance_id,
        idempotency_key,
    )
    preadmission_record: DispatchRecord | None = None
    if existing_record is not None:
        existing_fingerprint = (
            existing_record.placement_request_fingerprint
            or existing_record.request_fingerprint
        )
        if existing_fingerprint != submitted_fingerprint:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "idempotency_conflict",
                    "message": "This idempotency key was already used for different remote work.",
                    "dispatch_id": existing_record.dispatch_id,
                },
            )
        if existing_record.state == "admission_pending":
            raise _admission_in_progress_error(existing_record)
        else:
            return {
                "accepted": True,
                "duplicate": True,
                "dispatch_id": existing_record.dispatch_id,
                "job_id": existing_record.dispatch_id,
                "dispatch": _dispatch_public(request, existing_record),
            }
    _bind_effective_goal_dispatch_provider(body, ctx.settings.agent_provider)
    if preadmission_record is None:
        preadmission_record, created = await _offload_request(
            request,
            "goal.dispatch_admission_trace",
            _persist_goal_dispatch_admission_trace,
            ctx,
            ledger,
            body,
            idempotency_key=idempotency_key,
            request_fingerprint=submitted_fingerprint,
            target_instance_id=instance_id,
            principal_id=get_principal_id(request),
            placement_policy="named_instance",
            idempotency_scope="target",
        )
        if preadmission_record is not None and not created:
            raise _admission_in_progress_error(preadmission_record)
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
        error = _dispatch_lookup_error("card", body.card_id)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error
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
        error = _dispatch_lookup_error("project", project_id)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error
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
        error = _placement_http_error(exc)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error from exc
    body.provider = placement_body.provider
    return await _admit_remote_agent_work(
        request,
        instance_id,
        body,
        placement_decision=decision.model_dump(mode="json"),
        placement_request_fingerprint=submitted_fingerprint,
        preadmission_record=preadmission_record,
    )


def _named_dispatch_payload(
    instance_id: str,
    body: RemoteAgentStartBody,
    project_id: str | None,
) -> tuple[dict[str, Any], str]:
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
    fingerprint = hashlib.sha256(
        json.dumps(
            {"target_instance_id": instance_id, "payload": payload},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return payload, fingerprint


def _named_dispatch_identity(
    request: Request,
    instance_id: str,
    body: RemoteAgentStartBody,
    project_id: str | None,
) -> tuple[dict[str, Any], str, str]:
    payload, fingerprint = _named_dispatch_payload(instance_id, body, project_id)
    header_key = request.headers.get("idempotency-key")
    if not isinstance(header_key, str):
        header_key = None
    idempotency_key = (header_key or body.idempotency_key or str(uuid4())).strip()
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key cannot be empty")
    return payload, fingerprint, idempotency_key


async def _existing_named_dispatch(
    request: Request,
    instance_id: str,
    body: RemoteAgentStartBody,
    project_id: str | None,
) -> dict[str, Any] | None:
    """Deduplicate before placement/provisioning reads can block a replay."""
    _payload, fingerprint, idempotency_key = _named_dispatch_identity(
        request, instance_id, body, project_id
    )
    ledger = _dispatch_store(request)
    existing = await _offload_request(
        request,
        "dispatch.idempotency_read",
        ledger.by_idempotency,
        instance_id,
        idempotency_key,
    )
    if not existing:
        return None
    if (
        existing.placement_request_fingerprint or existing.request_fingerprint
    ) != fingerprint:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "idempotency_conflict",
                "message": "This idempotency key was already used for different remote work.",
                "dispatch_id": existing.dispatch_id,
            },
        )
    if existing.state == "admission_pending":
        return None
    return {
        "accepted": True,
        "duplicate": True,
        "dispatch_id": existing.dispatch_id,
        "job_id": existing.dispatch_id,
        "dispatch": _dispatch_public(request, existing),
    }


async def _admit_remote_agent_work(
    request: Request,
    instance_id: str,
    body: RemoteAgentStartBody,
    *,
    placement_decision: dict[str, Any] | None = None,
    placement_request_fingerprint: str | None = None,
    idempotency_scope: str = "target",
    preadmission_record: DispatchRecord | None = None,
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
    _submitted_payload, submitted_fingerprint, idempotency_key = (
        _named_dispatch_identity(request, instance_id, body, body.project_id)
    )
    body.idempotency_key = idempotency_key
    idempotency_fingerprint = placement_request_fingerprint or submitted_fingerprint
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
        existing_fingerprint = (
            existing.placement_request_fingerprint or existing.request_fingerprint
        )
        if existing_fingerprint != idempotency_fingerprint:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "idempotency_conflict",
                    "message": "This idempotency key was already used for different remote work.",
                    "dispatch_id": existing.dispatch_id,
                },
            )
        if (
            existing.state == "admission_pending"
            and preadmission_record is not None
            and preadmission_record.dispatch_id == existing.dispatch_id
        ):
            preadmission_record = existing
        elif existing.state == "admission_pending":
            raise _admission_in_progress_error(existing)
        else:
            return {
                "accepted": True,
                "duplicate": True,
                "dispatch_id": existing.dispatch_id,
                "job_id": existing.dispatch_id,
                "dispatch": _dispatch_public(request, existing),
            }

    _bind_effective_goal_dispatch_provider(body, settings.agent_provider)
    if preadmission_record is None:
        preadmission_record, created = await _offload_request(
            request,
            "goal.dispatch_admission_trace",
            _persist_goal_dispatch_admission_trace,
            ctx,
            ledger,
            body,
            idempotency_key=idempotency_key,
            request_fingerprint=idempotency_fingerprint,
            target_instance_id=instance_id,
            principal_id=get_principal_id(request),
            placement_policy=str(
                (placement_decision or {}).get("policy") or "named_instance"
            ),
            idempotency_scope=idempotency_scope,
        )
        if preadmission_record is not None and not created:
            raise _admission_in_progress_error(preadmission_record)
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
        error = _dispatch_lookup_error("card", body.card_id)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error
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
        error = _dispatch_lookup_error("project", project_id)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error
    try:
        inst = _fleet_instance_or_404(request, instance_id)
    except HTTPException as error:
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise
    authority_url = settings.instance_url
    if instance_id != settings.instance_id and (
        not authority_url
        or authority_url.startswith(("http://127.", "http://localhost"))
    ):
        error = HTTPException(
            status_code=409,
            detail={
                "code": "authority_unroutable",
                "message": "Configure a fleet-reachable instance_url before remote dispatch.",
                "recoverable": True,
            },
        )
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error
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
        error = HTTPException(
            status_code=409,
            detail={
                "code": "materialization_preflight_required",
                "message": plan.summary,
                "plan": plan.model_dump(mode="json"),
                "recoverable": True,
            },
        )
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error
    resolved_placement_decision = placement_decision or {
        "policy": "named_instance",
        "chosen_instance_id": instance_id,
        "chosen_instance_name": inst.name,
        "tie_breaking_reason": "The concrete API target was requested directly.",
    }
    if preadmission_record is not None:
        placement_input = dict(preadmission_record.goal_placement_input or {})
        placement_input["card_id"] = card.id if card else None
        placement_input["project_id"] = project_id
        preadmission_record.goal_placement_input = (
            goal_dispatch_placement_input_snapshot(placement_input)
        )
        preadmission_record.goal_placement_input_digest = (
            goal_dispatch_placement_input_digest(
                preadmission_record.goal_placement_input
            )
        )
        preadmission_record.target_instance_id = instance_id
        preadmission_record.target_instance_name = inst.name
        preadmission_record.request_payload = body.model_dump(mode="json")
        preadmission_record.goal_provenance = body.goal_provenance
        preadmission_record.placement_policy = str(
            resolved_placement_decision.get("policy") or "named_instance"
        )
        preadmission_record.placement_decision = resolved_placement_decision
        await _offload_request(
            request,
            "goal.dispatch_admission_trace_update",
            ledger.put,
            preadmission_record,
        )
    try:
        placement_input_digest = (
            preadmission_record.goal_placement_input_digest
            if preadmission_record is not None
            else _goal_dispatch_placement_input(
                body,
                target_instance_id=instance_id,
            )[2]
        )
        body.goal_provenance = _bind_goal_dispatch_placement(
            ctx,
            body.goal_provenance,
            selected_authority=selected_authority,
            operation_key=idempotency_key,
            target_instance_id=instance_id,
            placement_input_digest=placement_input_digest,
            placement_decision=resolved_placement_decision,
        )
        body.goal_provenance = _bind_goal_dispatch_materialization(
            ctx,
            body.goal_provenance,
            selected_authority=selected_authority,
            body=body,
            card=card,
            plan=plan,
            target_instance_id=instance_id,
        )
        if preadmission_record is not None:
            preadmission_record.goal_provenance = body.goal_provenance
            preadmission_record.materialization_plan = plan.model_dump(mode="json")
            preadmission_record.request_payload = body.model_dump(mode="json")
            await _offload_request(
                request,
                "goal.dispatch_placement_binding",
                ledger.put,
                preadmission_record,
            )
        body.goal_provenance = _validate_goal_dispatch_provenance(
            ctx,
            body.goal_provenance,
            selected_authority,
            sink="durable-admission",
            provider_id=body.provider,
            target_instance_id=instance_id,
            placement_input_digest=placement_input_digest,
            placement_decision_digest=goal_dispatch_placement_decision_digest(
                resolved_placement_decision
            ),
        )
    except HTTPException as error:
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise
    if preadmission_record is not None:
        preadmission_record.goal_provenance = body.goal_provenance
        preadmission_record.request_payload = body.model_dump(mode="json")
        await _offload_request(
            request,
            "goal.dispatch_admission_trace_validated",
            _mark_goal_admission_validated,
            ctx,
            ledger,
            preadmission_record,
        )
    payload, fingerprint = _named_dispatch_payload(instance_id, body, project_id)

    collaboration_service = ctx.services.get("collaboration")
    collaboration_decision = None
    if collaboration_service is not None:
        provider_id = body.provider or settings.agent_provider
        try:
            advertised_modes = list(
                get_provider(provider_id).default_spec().collaboration_modes
            )
        except KeyError:
            advertised_modes = []
        collaboration_decision = collaboration_service.resolve_dispatch_policy(
            PolicyInput(
                realm_id=realm_id,
                project_id=project_id,
                instance_id=instance_id,
                provider=provider_id,
                card_id=card.id if card else None,
                card_kind=(
                    card.kind.value
                    if card and hasattr(card.kind, "value")
                    else str(card.kind)
                    if card
                    else None
                ),
                card_tags=list(card.tags if card else []),
                capabilities=list(getattr(inst, "capabilities", []) or []),
                dispatch_intent=(
                    "automatic" if body.collaboration_unattended else "manual"
                ),
                risk=body.collaboration_risk,
                ambiguous=body.collaboration_ambiguous,
                unattended=body.collaboration_unattended,
                dispatch_override=body.collaboration_mode,
                supported_modes=advertised_modes,
            ),
            card_id=card.id if card else None,
        )
        payload["collaboration_mode"] = collaboration_decision.effective_mode.value
        payload["collaboration_decision"] = collaboration_decision.model_dump(
            mode="json"
        )
        payload_config = dict(payload.get("config") or {})
        if "plan" in advertised_modes:
            payload_config["collaboration_mode"] = (
                collaboration_decision.effective_mode.value
            )
        payload["config"] = payload_config

    record = DispatchRecord(
        dispatch_id=(
            preadmission_record.dispatch_id
            if preadmission_record is not None
            else str(uuid4())
        ),
        mutation_id=(
            preadmission_record.mutation_id
            if preadmission_record is not None
            else str(uuid4())
        ),
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint,
        placement_request_fingerprint=idempotency_fingerprint,
        card_id=card.id if card else None,
        project_id=project_id,
        realm_id=realm_id,
        card_version=card.updated_at.isoformat() if card else None,
        card_snapshot=card.model_dump(mode="json") if card else None,
        materialization_plan=plan.model_dump(mode="json"),
        request_payload=payload,
        goal_provenance=body.goal_provenance,
        goal_placement_input_digest=(
            preadmission_record.goal_placement_input_digest
            if preadmission_record is not None
            else placement_input_digest
        ),
        goal_placement_input=(
            preadmission_record.goal_placement_input
            if preadmission_record is not None
            else _goal_dispatch_placement_input(
                body,
                target_instance_id=instance_id,
            )[1]
        ),
        goal_admission_validation_state=(
            preadmission_record.goal_admission_validation_state
            if preadmission_record is not None
            else "not_required"
        ),
        goal_admission_validated_at=(
            preadmission_record.goal_admission_validated_at
            if preadmission_record is not None
            else None
        ),
        goal_admission_validation_proof=(
            preadmission_record.goal_admission_validation_proof
            if preadmission_record is not None
            else None
        ),
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
            resolved_placement_decision.get("policy") or "named_instance"
        ),
        placement_decision=resolved_placement_decision,
        placement_resolved_at=datetime.now(UTC),
        allow_concurrent=body.allow_concurrent,
        resume_requested=bool(body.resume_session_id),
        resume_session_id=body.resume_session_id,
        requested_priority=body.priority,
        state="admission_pending" if preadmission_record is not None else "queued",
        events=(
            list(preadmission_record.events) if preadmission_record is not None else []
        ),
        created_at=(
            preadmission_record.created_at
            if preadmission_record is not None
            else datetime.now(UTC)
        ),
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
        error = HTTPException(
            status_code=409,
            detail={
                "code": "idempotency_conflict",
                "message": "This idempotency key was already used for different remote work.",
                "dispatch_id": exc.existing.dispatch_id,
            },
        )
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error from exc
    except ConcurrentCardDispatch as exc:
        error = HTTPException(
            status_code=409,
            detail={
                "code": "card_dispatch_in_progress",
                "message": "This card already has an active durable dispatch. Open it or explicitly allow concurrent dispatch.",
                "dispatch_id": exc.existing.dispatch_id,
                "state": exc.existing.state,
                "recoverable": True,
            },
        )
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error from exc
    except DispatchCapacityExhausted as exc:
        logger.warning(
            "fleet capacity admission rejected target=%s provider=%s detail=%s",
            instance_id,
            body.provider,
            exc.detail,
        )
        error = HTTPException(status_code=409, detail=exc.detail)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error from exc
    except DispatchQueueFull as exc:
        logger.warning(
            "fleet dispatch queue admission rejected target=%s provider=%s detail=%s",
            instance_id,
            body.provider,
            exc.detail,
        )
        error = HTTPException(status_code=429, detail=exc.detail)
        await _reject_goal_dispatch_admission(request, preadmission_record, error)
        raise error from exc
    if duplicate:
        return {
            "accepted": True,
            "duplicate": True,
            "admission": "duplicate",
            "dispatch_id": record.dispatch_id,
            "job_id": record.dispatch_id,
            "dispatch": _dispatch_public(request, record),
        }
    if collaboration_service is not None and collaboration_decision is not None:
        collaboration_service.store.record_decision(
            collaboration_decision,
            dispatch_id=record.dispatch_id,
            card_id=record.card_id,
        )
    worker = ctx.services.get("dispatch_worker")
    if worker:
        # Let the ASGI handler serialize and send the durable 202 admission
        # before provisioning can consume the event loop or bounded I/O lane.
        # The polling worker remains restart-safe if this callback is lost.
        asyncio.get_running_loop().call_later(0.01, worker.wake)
    return {
        "accepted": True,
        "duplicate": False,
        "admission": (
            "queued"
            if record.state in {"waiting_capacity", "blocked"}
            else "launchable"
        ),
        "dispatch_id": record.dispatch_id,
        "job_id": record.dispatch_id,
        "dispatch": _dispatch_public(request, record),
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
        _dispatch_public(request, record)
        for record in _dispatch_store(request).list(
            target_instance_id=target_instance_id, limit=limit
        )
    ]


@router.get("/fleet/dispatch-queue")
def get_dispatch_queue(request: Request) -> dict[str, Any]:
    """Return bounded queue depth, age, blocked work, and scheduling order."""

    require_user(request)
    snapshot = _dispatch_store(request).queue_snapshot()
    settings = request.app.state.ctx.settings
    return {
        **snapshot,
        "capacity": settings.dispatch_queue_capacity,
        "provider_capacities": dict(settings.dispatch_provider_queue_capacities),
        "active_execution_capacity": settings.dispatch_capacity or 4,
    }


@router.post("/fleet/dispatch-jobs/{dispatch_id}/priority")
def update_dispatch_priority(
    request: Request, dispatch_id: str, body: DispatchPriorityBody
) -> dict[str, Any]:
    """Idempotently reprioritize waiting work with an immutable audit entry."""

    user = require_user(request)
    if getattr(user, "role", None) != "admin":
        raise HTTPException(
            status_code=403,
            detail={
                "code": "dispatch_priority_forbidden",
                "message": "Only an administrator may reprioritize queued work.",
            },
        )
    record = _dispatch_store(request).get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    try:
        updated = _dispatch_store(request).reprioritize(
            record,
            priority=body.priority,
            principal_id=get_principal_id(request),
            idempotency_key=body.idempotency_key,
        )
    except DispatchIdempotencyConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "idempotency_conflict", "dispatch_id": dispatch_id},
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "dispatch_not_waiting", "message": str(exc)},
        ) from exc
    return _dispatch_public(request, updated)


@router.get("/fleet/dispatch-jobs/{dispatch_id}")
def get_dispatch(request: Request, dispatch_id: str) -> dict[str, Any]:
    require_user(request)
    record = _dispatch_store(request).get(dispatch_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    return _dispatch_public(request, record)


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
        "evaluation_timeout_seconds": (settings.post_turn_evaluation_timeout_seconds),
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
        "lifecycle_diagnostics": _dispatch_public(request, record)[
            "lifecycle_diagnostics"
        ],
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


@router.post("/fleet/dispatch-jobs/{dispatch_id}/actions/{action_id}", status_code=202)
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
        (
            item
            for item in evaluation.recommended_actions
            if item.action_id == action_id
        ),
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
    if (
        action.name == FollowupActionName.PROMPT_SAME_SESSION
        and not action.human_approval_required
    ):
        inherited = is_authorized_same_session_continuation(
            action, decision=evaluation.decision, session_id=record.session_id
        )
        automatic_used = sum(
            1
            for prior_evaluation in record.post_turn_evaluations
            for prior_action in prior_evaluation.recommended_actions
            if prior_action.name == FollowupActionName.PROMPT_SAME_SESSION
            and any(event.get("automatic") for event in prior_action.audit)
        )
        budget = request.app.state.ctx.settings.post_turn_max_automatic_followups
        if not inherited or automatic_used >= budget:
            action.human_approval_required = True
            action.status_reason = (
                "Withheld for operator approval: continuation scope was not inherited "
                "or the automatic follow-up budget is exhausted. Approve this action "
                "to continue."
            )
        else:
            action.audit.append(
                {
                    "event": "authorized",
                    "at": datetime.now(UTC).isoformat(),
                    "executor": "pa.post-turn",
                    "automatic": True,
                    "authorization_basis": "original_implementation_dispatch",
                    "budget_used": automatic_used + 1,
                    "budget_maximum": budget,
                }
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
        elif action.name == FollowupActionName.REQUEST_OPERATOR_INPUT:
            operator_contract = {
                "request_id": action.parameters.get("request_id") or action.action_id,
                "prompt": (
                    action.parameters.get("prompt")
                    or action.parameters.get("question")
                    or action.parameters.get("reason")
                    or evaluation.operator_status_text
                    or "Operator input is required to continue."
                ),
                "response_schema": action.parameters.get("response_schema"),
                "choices": action.parameters.get("choices") or [],
                "allow_freeform": action.parameters.get("allow_freeform", True),
                "allow_cancel": action.parameters.get("allow_cancel", True),
                "sensitive": action.parameters.get("sensitive", False),
                "deadline": action.parameters.get("deadline"),
            }
            result = {
                "notification": await _create_operator_input_notification(
                    request,
                    record,
                    operator_contract,
                    idempotency_key=body.idempotency_key,
                    kind=InteractionKind.POST_TURN_OPERATOR_INPUT,
                )
            }
        else:
            # Wait, refresh, and escalation remain condition records. Operator
            # input is executable above and resumes the same session.
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
        return _dispatch_public(request, record)
    acknowledged = bool(
        record.acknowledged_at or record.completion_delivery_class == "acknowledged"
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
    record = ledger.transition(
        record,
        "completed",
        "Acknowledged legacy completion normalized to terminal state.",
        detail={"previous_state": previous, "repair": True},
    )
    return _dispatch_public(request, record)


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
        if prior.get("response"):
            return {**dict(prior.get("response") or {}), "duplicate": True}
        if prior.get("state") in {"failed", "cancelled", "interrupted"}:
            error = dict(prior.get("error") or {})
            raise HTTPException(
                status_code=int(error.pop("status_code", 409)),
                detail={
                    "code": "goal_followup_replay_terminal",
                    "previous_error": error,
                    "recoverable": False,
                },
            )

    ledger = _dispatch_store(request)
    followup_provenance: GoalDispatchProvenance | None = None
    try:
        followup_provenance = await _offload_request(
            request,
            "goal.dispatch_followup_reserve",
            _reserve_goal_dispatch_followup,
            request.app.state.ctx,
            ledger,
            record,
            idempotency_key=key,
            fingerprint=fingerprint,
        )
        if followup_provenance is not None:
            followup_provenance = await _offload_request(
                request,
                "goal.dispatch_followup_validate",
                _validate_goal_dispatch_provenance,
                request.app.state.ctx,
                followup_provenance,
                record.authority_instance_id,
                sink=f"followup-delivery:{key}",
                provider_id=record.request_payload.get("provider"),
                target_instance_id=record.target_instance_id,
                placement_input_digest=record.goal_placement_input_digest,
                placement_decision_digest=goal_dispatch_placement_decision_digest(
                    record.placement_decision
                ),
            )
            operation = record.followup_operations[key]
            operation["goal_provenance"] = followup_provenance.model_dump(mode="json")
            await _offload_request(
                request,
                "dispatch.followup_governance_update",
                ledger.put,
                record,
            )
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
                "goal_provenance": (
                    followup_provenance.model_dump(mode="json")
                    if followup_provenance
                    else None
                ),
            },
        )
    except Exception as exc:
        operation = record.followup_operations.setdefault(
            key,
            {"fingerprint": fingerprint},
        )
        operation["state"] = (
            "failed_pending_release" if operation.get("goal_provenance") else "failed"
        )
        detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
        operation["error"] = {
            "code": (
                str(detail.get("code") or "goal_followup_failed")
                if isinstance(detail, dict)
                else "goal_followup_failed"
            ),
            "message": (
                str(detail.get("message") or detail)
                if isinstance(detail, dict)
                else str(detail)
            )[:1000],
            "status_code": exc.status_code if isinstance(exc, HTTPException) else 502,
        }
        await _offload_request(
            request,
            "dispatch.followup_failed",
            ledger.put,
            record,
        )
        if operation.get("goal_provenance"):
            await _offload_request(
                request,
                "goal.dispatch_followup_release_failed",
                _release_goal_dispatch_followup,
                request.app.state.ctx,
                ledger,
                record,
                idempotency_key=key,
                outcome="followup-failed",
                applied=False,
                final_state="failed",
            )
        raise
    if not isinstance(result, dict) or not result.get("accepted"):
        error = HTTPException(
            status_code=502,
            detail={
                "code": "followup_not_acknowledged",
                "message": "The target did not durably acknowledge the follow-up prompt.",
                "recoverable": True,
            },
        )
        operation = record.followup_operations.setdefault(
            key, {"fingerprint": fingerprint}
        )
        operation["state"] = (
            "failed_pending_release" if operation.get("goal_provenance") else "failed"
        )
        operation["error"] = {**error.detail, "status_code": error.status_code}
        await _offload_request(request, "dispatch.followup_failed", ledger.put, record)
        if operation.get("goal_provenance"):
            await _offload_request(
                request,
                "goal.dispatch_followup_release_failed",
                _release_goal_dispatch_followup,
                request.app.state.ctx,
                ledger,
                record,
                idempotency_key=key,
                outcome="followup-not-acknowledged",
                applied=False,
                final_state="failed",
            )
        raise error
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
    operation = record.followup_operations.setdefault(key, {"fingerprint": fingerprint})
    operation.update(
        {
            "response": public,
            "state": "accepted_pending_release"
            if followup_provenance is not None
            else "accepted",
            "goal_provenance": (
                followup_provenance.model_dump(mode="json")
                if followup_provenance
                else None
            ),
        }
    )
    operation.pop("error", None)
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
    if followup_provenance is not None:
        await _offload_request(
            request,
            "goal.dispatch_followup_release_accepted",
            _release_goal_dispatch_followup,
            request.app.state.ctx,
            ledger,
            record,
            idempotency_key=key,
            outcome="followup-accepted",
            applied=True,
            final_state="accepted",
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
        return _dispatch_public(request, record)
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
        or ctx.store.list_instance_groups(record.realm_id, include_archived=True)
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
                    original.get("dispatch_intent") or DispatchIntent.AUTOMATIC.value
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
            "goal.dispatch_retry_reservation",
            _replace_goal_dispatch_reservation,
            ctx,
            ledger,
            record,
            idempotency_key=idempotency_key,
        )
        try:
            record = await _offload_request(
                request,
                "dispatch.retry_capacity_admission",
                ledger.retry_with_capacity,
                record,
                capacity,
                idempotency_key=idempotency_key,
            )
        except DispatchQueueFull, DispatchCapacityExhausted, ValueError:
            await _release_goal_dispatch_reservation_async(
                ctx,
                ledger,
                record,
                outcome="retry-admission-failed",
                applied=False,
            )
            raise
    except PlacementError as exc:
        raise _placement_http_error(exc) from exc
    except DispatchQueueFull as exc:
        raise HTTPException(status_code=429, detail=exc.detail) from exc
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
    return _dispatch_public(request, record)


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
        return _dispatch_public(request, record)
    if record.state not in {"failed", "cancelled"} or not record.recoverable:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "dispatch_not_retryable",
                "message": f"Dispatch in {record.state} cannot be retried safely.",
            },
        )
    record = _replace_goal_dispatch_reservation(
        request.app.state.ctx,
        ledger,
        record,
        idempotency_key=idempotency_key,
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
        except DispatchQueueFull as exc:
            _release_goal_dispatch_reservation(
                request.app.state.ctx,
                ledger,
                record,
                outcome="retry-admission-failed",
                applied=False,
            )
            raise HTTPException(status_code=429, detail=exc.detail) from exc
        except DispatchCapacityExhausted as exc:
            _release_goal_dispatch_reservation(
                request.app.state.ctx,
                ledger,
                record,
                outcome="retry-admission-failed",
                applied=False,
            )
            raise HTTPException(status_code=409, detail=exc.detail) from exc
    else:
        # Legacy records predate reservations. Preserve their retry behavior;
        # every newly admitted record carries capacity metadata.
        record.cancel_requested = False
        record.last_error = None
        record.error_code = None
        record.control_operations[idempotency_key] = "retry"
        record = ledger.transition(
            record, "queued", "Operator queued a safe retry."
        )
    worker = request.app.state.ctx.services.get("dispatch_worker")
    if worker:
        worker.wake()
    return _dispatch_public(request, record)


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
            return _dispatch_public(request, outbox.retry_delivery(dispatch_id))
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
    return _dispatch_public(request, repaired)


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
        return _dispatch_public(request, record)
    if record.state in {"waiting_capacity", "blocked", "queued"}:
        record.control_operations[idempotency_key] = "cancel"
        record = ledger.transition(
            record, "cancelled", "Operator cancelled queued dispatch."
        )
        record = _release_goal_dispatch_reservation(
            request.app.state.ctx,
            ledger,
            record,
            outcome="cancelled",
            applied=False,
        )
        return _dispatch_public(request, record)
    if record.state not in {
        "checking_sync",
        "materializing",
        "provisioning",
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
    record = ledger.transition(
        record,
        record.state,
        "Cancellation requested; the worker will stop at the next safe boundary.",
    )
    return _dispatch_public(request, record)


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
    # Agent event streams are intentionally unbounded; every other proxied
    # response must retain a finite read timeout so a stalled peer cannot pin a
    # request forever while the controller buffers its body.
    read_timeout = (
        None
        if proxied_path.endswith("/events") or proxied_path == "session-events"
        else 120.0
    )
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
        if name.lower()
        in {"content-type", "cache-control", "content-disposition", "retry-after"}
    }
    content_type = upstream.headers.get("content-type", "")
    if content_type.startswith("text/event-stream"):
        from pa.core.sse_observability import sse_connections
        from pa.server.shutdown import is_shutting_down

        async def relay() -> AsyncIterator[bytes]:
            pair_id = str(uuid4())
            client_id = request.query_params.get("client_id")
            scope = "all_live" if proxied_path == "session-events" else "single_session"
            downstream_id = sse_connections.open(
                endpoint="/api/fleet/instances/{instance_id}/agent/session-events",
                direction="downstream",
                client_id=client_id,
                peer_id=instance_id,
                session_scope=scope,
                paired_id=pair_id,
            )
            upstream_id = sse_connections.open(
                endpoint="/api/agent/" + proxied_path,
                direction="upstream",
                client_id=client_id,
                peer_id=instance_id,
                session_scope=scope,
                paired_id=pair_id,
            )
            outcome = "closed"
            next_chunk: asyncio.Task[bytes] | None = None
            try:
                reconnect_attempt = int(
                    request.query_params.get("reconnect_attempt") or 0
                )
            except TypeError, ValueError:
                reconnect_attempt = 0
            if reconnect_attempt > 0:
                sse_connections.increment("reconnecting")
            try:
                iterator = upstream.aiter_raw().__aiter__()
                try:
                    while True:
                        if next_chunk is None:
                            next_chunk = asyncio.create_task(anext(iterator))
                        done, _waiting = await asyncio.wait(
                            {next_chunk},
                            timeout=0.5,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if not done:
                            if is_shutting_down() or await request.is_disconnected():
                                outcome = "cancelled"
                                break
                            continue
                        task = next_chunk
                        next_chunk = None
                        try:
                            chunk = task.result()
                        except StopAsyncIteration:
                            break
                        yield chunk
                except (
                    httpx.RemoteProtocolError,
                    httpx.ReadError,
                    httpx.ConnectError,
                ):
                    outcome = "errored"
                    # Headers were already sent. A peer restart or half-close is
                    # represented as EOF while the finally block closes both legs.
                    logger.info(
                        "Peer %s closed agent event stream during restart",
                        instance_id,
                    )
                except asyncio.CancelledError:
                    outcome = "cancelled"
                    raise
                except Exception:
                    outcome = "errored"
                    logger.exception(
                        "Fleet agent event relay failed",
                        extra={
                            "peer_id": instance_id,
                            "agent_path": proxied_path,
                            "client_id": client_id,
                        },
                    )
                    raise
            finally:
                if next_chunk is not None:
                    next_chunk.cancel()
                    await asyncio.gather(next_chunk, return_exceptions=True)
                try:
                    await upstream.aclose()
                finally:
                    try:
                        await client.aclose()
                    finally:
                        sse_connections.close(upstream_id, outcome)
                        sse_connections.close(downstream_id, outcome)
                        logger.info(
                            "Fleet agent event relay closed",
                            extra={
                                "peer_id": instance_id,
                                "agent_path": proxied_path,
                                "client_id": client_id,
                                "outcome": outcome,
                            },
                        )

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
            "instance_name": current_instance_name(
                ctx,
                session.origin_instance_id or ctx.settings.instance_id,
                session.origin_instance_name or ctx.settings.instance_name,
            ),
            "instance_name_at_session_start": session.origin_instance_name,
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
                "instance_name": current_instance_name(ctx, owner_id, owner_name),
                "instance_name_at_dispatch": owner_name,
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
        canonical_self = fleet.get_instance(settings.instance_id)
        if canonical_self and canonical_self.name != settings.instance_name:
            from pa.domain.instance_config import update_instance_config
            from pa.fleet.join import refresh_service_env

            settings.instance_name = canonical_self.name
            update_instance_config(settings.data_dir, instance_name=canonical_self.name)
            refresh_service_env(settings)
        self_url = owner_public_url(settings)
        fleet.register_self(
            settings.instance_id,
            settings.instance_name,
            self_url,
            zone=settings.zone,
            capabilities=settings.capabilities,
            dispatch_capacity=settings.dispatch_capacity,
            dispatch_provider_capacities=dict(settings.dispatch_provider_capacities),
            dispatch_queue_capacity=settings.dispatch_queue_capacity,
            dispatch_provider_queue_capacities=dict(
                settings.dispatch_provider_queue_capacities
            ),
            relay_enabled=settings.relay_enabled,
        )
        ctx.register_service("fleet_registry", fleet)
        convergence = MembershipConvergenceStore(
            settings.data_dir, settings.instance_id
        )
        convergence.plan(fleet.generation, fleet.list_instances())
        ctx.register_service("membership_convergence", convergence)
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
            "fleet_bootstrap_job_store", BootstrapJobStore(settings.data_dir)
        )
        ctx.register_service(
            "fleet_update_job_store", FleetUpdateJobStore(settings.data_dir)
        )
        auxiliary = "writer_lock" not in ctx.services
        dispatch_store = DispatchStore(
            settings.data_dir,
            read_only=auxiliary,
            deferred_read_only=auxiliary,
        )
        ctx.register_service("dispatch_store", dispatch_store)
        ctx.register_service(
            "assigned_mcp_environment_resolver",
            lambda session: _assigned_mcp_environment_for_session(
                settings, dispatch_store, session
            ),
        )
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
        convergence_task = asyncio.create_task(
            _membership_convergence_loop(ctx),
            name="fleet-membership-convergence",
        )
        ctx.register_service("membership_convergence_task", convergence_task)
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
        await _reconcile_goal_dispatch_reservations(ctx)
        dispatch_worker = DispatchWorker(
            ctx.require_service("dispatch_store"),
            lambda record: _process_remote_dispatch(app, record),
            async_runtime=async_runtime,
            readiness=lambda record: _refresh_queued_dispatch_readiness(app, record),
            terminal=lambda record, outcome: _release_goal_dispatch_reservation_async(
                ctx,
                ctx.require_service("dispatch_store"),
                record,
                outcome=outcome,
                applied=_goal_dispatch_was_applied(record),
            ),
            lifecycle_recovery=lambda: _reconcile_goal_dispatch_reservations(ctx),
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
        bootstrap_store: BootstrapJobStore | None = ctx.services.get(
            "fleet_bootstrap_job_store"
        )
        if bootstrap_store:
            for job_id, task in list(_bootstrap_tasks.items()):
                job = bootstrap_store.get(job_id)
                if job and not task.done():
                    job.cancel_requested = True
                    job.state = BootstrapState.RETRYABLE
                    job.readiness_reason = "PA shut down during onboarding; resume from the durable checkpoint."
                    bootstrap_store.save(job)
                task.cancel()
            if _bootstrap_tasks:
                await asyncio.gather(*_bootstrap_tasks.values(), return_exceptions=True)
                _bootstrap_tasks.clear()
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
        convergence_task = ctx.services.get("membership_convergence_task")
        if convergence_task:
            convergence_task.cancel()
            await asyncio.gather(convergence_task, return_exceptions=True)
        client = ctx.services.get("fleet_http_client")
        if client:
            await client.aclose()
        dispatch_store = ctx.services.get("dispatch_store")
        if dispatch_store:
            await asyncio.to_thread(dispatch_store.close)

    def api_routers(self):
        return [("/api", router, ["fleet"])]

    def ui_routers(self):
        return [ui_router]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def discover_fleet_bootstrap_target(target: str) -> dict[str, Any]:
            """Resolve an SSH target and fingerprint without mutating the host."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/fleet/bootstrap/discover",
                json={"target": target},
            )

        @mcp.tool()
        def create_fleet_bootstrap_job(
            target: str,
            instance_name: str,
            instance_url: str,
            idempotency_key: str,
            realm: str = "default",
            worker_profile: str = "manual",
            providers: list[str] | None = None,
            repositories: list[str] | None = None,
            github_transport: str = "none",
            automatic_placement: bool = False,
            dispatch_capacity: int = 1,
            channel: str = "release",
            release_ref: str = "",
            existing_install_action: str = "install",
            smoke_dispatch: bool = False,
            smoke_card_id: str = "",
            start: bool = False,
        ) -> dict[str, Any]:
            """Create a durable, observable machine-onboarding plan."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/fleet/bootstrap-jobs",
                json={
                    "idempotency_key": idempotency_key,
                    "start": start,
                    "request": {
                        "target": target,
                        "instance_name": instance_name,
                        "instance_url": instance_url,
                        "realm": realm,
                        "worker_profile": worker_profile,
                        "providers": providers or [],
                        "repositories": repositories or [],
                        "github_transport": github_transport,
                        "automatic_placement": automatic_placement,
                        "dispatch_capacity": dispatch_capacity,
                        "channel": channel,
                        "release_ref": release_ref,
                        "existing_install_action": existing_install_action,
                        "smoke_dispatch": smoke_dispatch,
                        "smoke_card_id": smoke_card_id,
                    },
                },
            )

        @mcp.tool()
        def list_fleet_bootstrap_jobs(
            include_terminal: bool = True,
        ) -> list[dict[str, Any]]:
            """List durable onboarding jobs and incomplete machines."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/fleet/bootstrap-jobs",
                params={"include_terminal": include_terminal},
            )

        @mcp.tool()
        def get_fleet_bootstrap_job(job_id: str) -> dict[str, Any] | None:
            """Read phase, logs, required input, evidence, and readiness."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/fleet/bootstrap-jobs/{job_id}",
                allow_not_found=True,
            )

        @mcp.tool()
        def control_fleet_bootstrap_job(
            job_id: str,
            action: Literal["start", "resume", "retry", "cancel"],
        ) -> dict[str, Any]:
            """Start, safely cancel, resume, or retry a durable onboarding job."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/fleet/bootstrap-jobs/{job_id}/{action}",
            )

        @mcp.tool()
        def submit_fleet_bootstrap_input(
            job_id: str,
            kind: Literal[
                "host_key",
                "ssh_password",
                "key_passphrase",
                "sudo_password",
                "provider_login",
                "github_login",
                "operator_confirmation",
            ],
            value: str = "",
            confirmed: bool = False,
            details: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            """Submit short-lived protected input or explicit phase evidence."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/fleet/bootstrap-jobs/{job_id}/input",
                json={
                    "kind": kind,
                    "value": value,
                    "confirmed": confirmed,
                    "details": details or {},
                },
            )

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
                payload["permitted_placement_policies"] = permitted_placement_policies
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
        def archive_instance_group(group_id: str, realm_id: str | None = None) -> dict:
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
            collaboration_mode: CollaborationMode | None = None,
            collaboration_risk: str = "low",
            collaboration_ambiguous: bool = False,
            collaboration_unattended: bool = False,
            effort: str | None = None,
            allow_concurrent: bool = False,
            capacity_override: bool = False,
            capacity_override_reason: str | None = None,
            participation_override: bool = False,
            participation_override_reason: str | None = None,
            execution_contract: dict[str, Any] | None = None,
            priority: int = 0,
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
                    "collaboration_mode": (
                        collaboration_mode.value
                        if isinstance(collaboration_mode, CollaborationMode)
                        else collaboration_mode
                    ),
                    "collaboration_risk": collaboration_risk,
                    "collaboration_ambiguous": collaboration_ambiguous,
                    "collaboration_unattended": collaboration_unattended,
                    "effort": effort,
                    "allow_concurrent": allow_concurrent,
                    "capacity_override": capacity_override,
                    "capacity_override_reason": capacity_override_reason,
                    "participation_override": participation_override,
                    "participation_override_reason": (participation_override_reason),
                    "execution_contract": execution_contract,
                    "priority": priority,
                    "idempotency_key": key,
                },
                timeout_seconds=30.0,
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
            collaboration_mode: CollaborationMode | None = None,
            collaboration_risk: str = "low",
            collaboration_ambiguous: bool = False,
            collaboration_unattended: bool = False,
            effort: str | None = None,
            cwd: str | None = None,
            config: dict[str, str | bool] | None = None,
            allow_concurrent: bool = False,
            capacity_override: bool = False,
            capacity_override_reason: str | None = None,
            participation_override: bool = False,
            participation_override_reason: str | None = None,
            execution_contract: dict[str, Any] | None = None,
            priority: int = 0,
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
                "collaboration_mode": (
                    collaboration_mode.value
                    if isinstance(collaboration_mode, CollaborationMode)
                    else collaboration_mode
                ),
                "collaboration_risk": collaboration_risk,
                "collaboration_ambiguous": collaboration_ambiguous,
                "collaboration_unattended": collaboration_unattended,
                "effort": effort,
                "cwd": cwd,
                "config": config or {},
                "idempotency_key": key,
            }
            if priority:
                payload["priority"] = priority
            if execution_contract is not None:
                payload["execution_contract"] = execution_contract
            if allow_concurrent:
                payload["allow_concurrent"] = True
            if capacity_override:
                payload["capacity_override"] = True
                payload["capacity_override_reason"] = capacity_override_reason
            if participation_override:
                payload["participation_override"] = True
                payload["participation_override_reason"] = participation_override_reason
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/fleet/instances/{instance_id}/agent/start",
                json=payload,
                timeout_seconds=30.0,
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
        def get_assigned_dispatch() -> dict:
            """Read the dispatch bound to this assigned PA session."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/goal-assigned-session/dispatch",
            )

        @mcp.tool()
        def get_dispatch_queue() -> dict:
            """Return waiting, blocked, active, and queue-capacity state."""
            return request_local_pa(ctx.settings, "GET", "/api/fleet/dispatch-queue")

        @mcp.tool()
        def set_dispatch_priority(
            dispatch_id: str, priority: int, idempotency_key: str
        ) -> dict:
            """Idempotently reprioritize a waiting dispatch with audit history."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/fleet/dispatch-jobs/{dispatch_id}/priority",
                json={
                    "priority": priority,
                    "idempotency_key": idempotency_key,
                },
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
            operator_input: str | dict[str, Any] | None = None,
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
        def report_assigned_dispatch_progress(
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
            operator_input: str | dict[str, Any] | None = None,
        ) -> dict:
            """Report progress for the dispatch bound to this assigned session."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/goal-assigned-session/progress",
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
        def repair_terminal_dispatch(dispatch_id: str, idempotency_key: str) -> dict:
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
