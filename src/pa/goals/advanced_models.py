"""Typed contracts for advanced goal autonomy and portfolio governance."""

from __future__ import annotations

import copy
import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import BaseModel, Field, model_validator

from pa.goals.materialization import (
    GoalExecutionIdentityV1,
    GoalMaterializationEnvelopeV1,
    GoalMaterializationReceiptV1,
)
from pa.goals.models import (
    GoalActorRole,
    GoalCreate,
    GoalRateLimit,
    GoalReferenceId,
    normalize_legacy_goal_payload,
)


def _bounded_derived_reference(prefix: str, *parts: str) -> str:
    """Keep generated references valid without truncating identity material."""

    candidate = ":".join((prefix, *parts))
    if len(candidate) <= 200:
        return candidate
    digest = hashlib.sha256("\0".join(parts).encode()).hexdigest()
    return f"{prefix}:{digest}"


def normalize_legacy_governance_payload(
    entity_type: str,
    entity_id: Any,
    value: Any,
    *,
    realm_id: str = "default",
    legacy_entity_seed: str | None = None,
) -> Any:
    """Canonicalize known blank defaults only at the durable projection boundary."""

    if not isinstance(value, dict):
        return value
    payload = copy.deepcopy(value)
    if not isinstance(entity_id, str):
        return payload
    seed = entity_id.strip() if isinstance(entity_id, str) else ""
    seed = seed or str(
        uuid5(
            NAMESPACE_URL,
            "pa:goal-governance:"
            f"{realm_id}:{entity_type}:legacy-entity:"
            f"{legacy_entity_seed or 'projection-row'}",
        )
    )

    def blank(item: Any) -> bool:
        return isinstance(item, str) and not item.strip()

    def legacy_id(kind: str, index: int) -> str:
        return str(
            uuid5(
                NAMESPACE_URL,
                f"pa:goal-governance:{entity_type}:{seed}:legacy:{kind}:{index}",
            )
        )

    def clear_optional(container: Any, *fields: str) -> None:
        if not isinstance(container, dict):
            return
        for field in fields:
            if field in container and blank(container[field]):
                container[field] = None

    def drop_blank_refs(container: Any, *fields: str) -> None:
        if not isinstance(container, dict):
            return
        for field in fields:
            values = container.get(field)
            if isinstance(values, list):
                container[field] = [item for item in values if not blank(item)]

    def normalize_ids(items: Any, kind: str) -> list[str] | None:
        replacements: list[str] = []
        known = False
        if not isinstance(items, list):
            return replacements
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            if "id" in item and not blank(item["id"]):
                known = True
                continue
            replacement = legacy_id(kind, index)
            item["id"] = replacement
            replacements.append(replacement)
        return None if known and replacements else replacements

    def rewrite_blank_refs(values: Any, replacements: list[str] | None) -> Any:
        if not isinstance(values, list) or not replacements:
            return values
        cursor = 0
        rewritten: list[Any] = []
        for item in values:
            if not blank(item):
                rewritten.append(item)
                continue
            rewritten.append(replacements[cursor % len(replacements)])
            cursor += 1
        return rewritten

    def normalize_action_request(request: Any) -> None:
        clear_optional(
            request,
            "operation_digest",
            "provider_id",
            "approval_principal",
            "approval_interaction_id",
        )

    if entity_type == "goal_autonomy":
        if "goal_id" not in payload or blank(payload.get("goal_id")):
            payload["goal_id"] = seed
        if blank(payload.get("realm_id")):
            payload["realm_id"] = realm_id
        goal_id = str(payload.get("goal_id") or seed)
        strategies = payload.get("strategies")
        strategy_ids = normalize_ids(strategies, "strategy")
        if "selected_strategy_ids" in payload:
            payload["selected_strategy_ids"] = rewrite_blank_refs(
                payload.get("selected_strategy_ids"), strategy_ids
            )
        drop_blank_refs(payload, "derived_goal_ids")

        runs = payload.get("provider_runs")
        normalize_ids(runs, "provider-run")
        for run in runs if isinstance(runs, list) else []:
            if not isinstance(run, dict):
                continue
            if "goal_id" not in run or blank(run.get("goal_id")):
                run["goal_id"] = goal_id
            clear_optional(
                run,
                "strategy_id",
                "executor_principal",
                "reservation_id",
                "replaces_run_id",
                "launch_decision_id",
            )
            if blank(run.get("authority_instance_id")):
                run["authority_instance_id"] = "legacy"
            drop_blank_refs(
                run,
                "blocker_refs",
                "interaction_refs",
                "waiting_interaction_refs",
                "artifact_refs",
            )
            invocation = run.get("invocation")
            if isinstance(invocation, dict):
                if "canonical_goal_id" not in invocation or blank(
                    invocation.get("canonical_goal_id")
                ):
                    invocation["canonical_goal_id"] = goal_id
                if blank(invocation.get("provider_id")) and not blank(
                    run.get("provider_id")
                ):
                    invocation["provider_id"] = run.get("provider_id")

        decisions = payload.get("recent_decisions")
        decision_ids = normalize_ids(decisions, "action-decision")
        for decision in decisions if isinstance(decisions, list) else []:
            if not isinstance(decision, dict):
                continue
            if "goal_id" not in decision or blank(decision.get("goal_id")):
                decision["goal_id"] = goal_id
            clear_optional(decision, "authority_instance_id", "reservation_id")
            normalize_action_request(decision.get("request"))

        reservations = payload.get("action_reservations")
        reservation_ids = normalize_ids(reservations, "action-reservation")
        for index, reservation in enumerate(
            reservations if isinstance(reservations, list) else []
        ):
            if not isinstance(reservation, dict):
                continue
            reservation_id = str(
                reservation.get("id") or legacy_id("action-reservation", index)
            )
            if blank(reservation.get("idempotency_key")) or (
                "idempotency_key" not in reservation
            ):
                reservation["idempotency_key"] = _bounded_derived_reference(
                    "legacy-action-reservation", reservation_id
                )
            if (
                "decision_id" not in reservation
                or blank(reservation.get("decision_id"))
            ) and decision_ids:
                reservation["decision_id"] = decision_ids[index % len(decision_ids)]
            if "goal_id" not in reservation or blank(reservation.get("goal_id")):
                reservation["goal_id"] = goal_id
            normalize_action_request(reservation.get("request"))

        if isinstance(decisions, list) and reservation_ids:
            for index, decision in enumerate(decisions):
                if isinstance(decision, dict) and (
                    decision.get("reservation_id") is None
                    or blank(decision.get("reservation_id"))
                ):
                    decision["reservation_id"] = reservation_ids[
                        index % len(reservation_ids)
                    ]
        return payload

    if entity_type == "goal_governance_policy":
        if "id" not in payload or blank(payload.get("id")):
            payload["id"] = "organization"
        if blank(payload.get("realm_id")):
            payload["realm_id"] = realm_id
        standing = payload.get("standing_goal_policies")
        normalize_ids(standing, "standing-policy")
        for item in standing if isinstance(standing, list) else []:
            drop_blank_refs(item, "project_ids")
        return payload

    if entity_type == "goal_proposal":
        if "id" not in payload or blank(payload.get("id")):
            payload["id"] = seed
        if blank(payload.get("realm_id")):
            payload["realm_id"] = realm_id
        clear_optional(payload, "policy_id", "activated_goal_id")
        request = payload.get("request")
        clear_optional(request, "parent_goal_id", "parent_criterion_id")
        if isinstance(request, dict) and isinstance(request.get("goal"), dict):
            goal = normalize_legacy_goal_payload(
                request["goal"], fallback_goal_id=f"proposal:{seed}"
            )
            goal.pop("id", None)
            request["goal"] = goal
        return payload

    if entity_type == "goal_portfolio_review":
        if "id" not in payload or blank(payload.get("id")):
            payload["id"] = seed
        if blank(payload.get("realm_id")):
            payload["realm_id"] = realm_id
        drop_blank_refs(payload, "pending_proposal_ids")
        return payload

    return payload


class GoalActionRisk(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class GoalActionDisposition(StrEnum):
    AUTHORIZED = "authorized"
    REQUIRES_APPROVAL = "requires_approval"
    DENIED = "denied"
    BUDGET_EXHAUSTED = "budget_exhausted"
    RATE_LIMITED = "rate_limited"
    RESOURCE_CONFLICT = "resource_conflict"


class GoalReservationState(StrEnum):
    RESERVED = "reserved"
    APPLIED = "applied"
    RELEASED = "released"


class ResourceAccess(StrEnum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


class AllocationDisposition(StrEnum):
    ACTIVE = "active"
    QUEUED = "queued"
    PREEMPTED = "preempted"
    BLOCKED = "blocked"


class StrategyState(StrEnum):
    CANDIDATE = "candidate"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ABANDONED = "abandoned"


class ProposalKind(StrEnum):
    DERIVED_SUBGOAL = "derived_subgoal"
    TOP_LEVEL = "top_level"


class ProposalDisposition(StrEnum):
    PENDING_REVIEW = "pending_review"
    AUTO_ACTIVATED = "auto_activated"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


class ProviderGoalMode(StrEnum):
    NATIVE = "native"
    RECOVERABLE_TURN = "recoverable_turn"


class ProviderRunState(StrEnum):
    ASSIGNED = "assigned"
    RUNNING = "running"
    WAITING_OPERATOR = "waiting_operator"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class GoalUsage(BaseModel):
    actions: int = Field(default=0, ge=0)
    cost_usd: float = Field(default=0, ge=0)
    tokens: int = Field(default=0, ge=0)
    api_calls: int = Field(default=0, ge=0)
    storage_mb: float = Field(default=0, ge=0)
    dispatches: int = Field(default=0, ge=0)

    def plus(self, other: GoalUsage) -> GoalUsage:
        return GoalUsage(
            actions=self.actions + other.actions,
            cost_usd=self.cost_usd + other.cost_usd,
            tokens=self.tokens + other.tokens,
            api_calls=self.api_calls + other.api_calls,
            storage_mb=self.storage_mb + other.storage_mb,
            dispatches=self.dispatches + other.dispatches,
        )


class GoalRateWindow(BaseModel):
    key: GoalReferenceId
    started_at: datetime
    usage: GoalUsage = Field(default_factory=GoalUsage)


class GoalResourceClaim(BaseModel):
    key: str = Field(min_length=1, max_length=300, pattern=r"\S")
    access: ResourceAccess = ResourceAccess.SHARED
    quantity: float = Field(default=1, gt=0)
    preemptible: bool = True
    expires_at: datetime | None = None


class GoalActionRequest(BaseModel):
    action_class: GoalReferenceId
    operation_digest: str | None = Field(
        default=None,
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )
    operation_key: str | None = Field(default=None, min_length=1, max_length=200)
    requested_placement_target: str | None = Field(
        default=None, min_length=1, max_length=200
    )
    placement_input_digest: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    resolved_target_instance_id: str | None = Field(
        default=None, min_length=1, max_length=80
    )
    placement_decision_digest: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    materialization_envelope: GoalMaterializationEnvelopeV1 | None = None
    materialization_receipt: GoalMaterializationReceiptV1 | None = None
    execution_identity: GoalExecutionIdentityV1 | None = None
    risk: GoalActionRisk = GoalActionRisk.LOW
    reversible: bool = True
    delegated: bool = False
    external: bool = False
    audience: str | None = None
    repository: str | None = None
    data_scope: str | None = None
    provider_id: GoalReferenceId | None = None
    estimate: GoalUsage = Field(default_factory=lambda: GoalUsage(actions=1))
    resource_claims: list[GoalResourceClaim] = Field(default_factory=list)
    operator_approved: bool = False
    approval_principal: GoalReferenceId | None = None
    approval_interaction_id: GoalReferenceId | None = None
    max_attempts: int = Field(default=1, ge=1, le=20)

    @model_validator(mode="after")
    def approval_is_attributable(self) -> GoalActionRequest:
        if self.operator_approved and not self.approval_principal:
            raise ValueError("operator-approved actions require an approval principal")
        self.estimate.actions = max(self.estimate.actions, 1)
        return self


class GoalActionDecision(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    goal_id: GoalReferenceId
    action_class: GoalReferenceId
    disposition: GoalActionDisposition
    reasons: list[str] = Field(min_length=1)
    policy_revision: int = Field(ge=1)
    request: GoalActionRequest
    reserved_usage: GoalUsage = Field(default_factory=GoalUsage)
    decided_by: GoalReferenceId
    authority_instance_id: GoalReferenceId | None = None
    fencing_token: int | None = Field(default=None, ge=1)
    reservation_id: GoalReferenceId | None = None
    decided_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalActionApply(BaseModel):
    reservation_id: GoalReferenceId
    actual_usage: GoalUsage | None = None


class GoalActionRelease(BaseModel):
    reservation_id: GoalReferenceId
    actual_usage: GoalUsage | None = None
    reason: str = Field(min_length=1, max_length=500)


class GoalActionReservation(BaseModel):
    """A durable, fenced hold created before an autonomous side effect."""

    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    idempotency_key: GoalReferenceId
    decision_id: GoalReferenceId
    goal_id: GoalReferenceId
    action_class: GoalReferenceId
    actor_principal: GoalReferenceId
    authority_instance_id: GoalReferenceId
    policy_revision: int = Field(ge=1)
    goal_version: int = Field(ge=1)
    fencing_token: int | None = Field(default=None, ge=1)
    request: GoalActionRequest
    reserved_usage: GoalUsage = Field(default_factory=GoalUsage)
    actual_usage: GoalUsage = Field(default_factory=GoalUsage)
    resource_claims: list[GoalResourceClaim] = Field(default_factory=list)
    state: GoalReservationState = GoalReservationState.RESERVED
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    applied_at: datetime | None = None
    released_at: datetime | None = None
    release_reason: str = ""
    attempt: int = Field(default=1, ge=1, le=20)
    max_attempts: int = Field(default=1, ge=1, le=20)
    replaces_reservation_id: str | None = None
    renewal_count: int = Field(default=0, ge=0)
    renewed_at: datetime | None = None


class GoalStrategy(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    title: str = Field(min_length=1, max_length=300)
    hypothesis: str = Field(min_length=1)
    expected_outcome: str = Field(min_length=1)
    state: StrategyState = StrategyState.CANDIDATE
    score: float = Field(default=0, ge=0, le=1)
    allocated_cost_usd: float = Field(default=0, ge=0)
    allocated_tokens: int = Field(default=0, ge=0)
    risk: GoalActionRisk = GoalActionRisk.LOW
    evidence_ids: list[GoalReferenceId] = Field(default_factory=list)


class GoalStrategyPortfolioUpdate(BaseModel):
    strategies: list[GoalStrategy] = Field(min_length=1)
    selected_strategy_ids: list[GoalReferenceId] = Field(default_factory=list)
    reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_strategy_ids(self) -> GoalStrategyPortfolioUpdate:
        identifiers = [item.id for item in self.strategies]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("strategy ids must be unique")
        if len(self.selected_strategy_ids) != len(set(self.selected_strategy_ids)):
            raise ValueError("selected strategy ids must be unique")
        unknown = set(self.selected_strategy_ids) - set(identifiers)
        if unknown:
            raise ValueError(f"selected strategies are unknown: {sorted(unknown)}")
        return self


class ProviderGoalCapabilities(BaseModel):
    provider_id: GoalReferenceId
    native_command_candidates: list[str] = Field(default_factory=lambda: ["goal"])
    supports_native_goal: bool = True
    supports_recoverable_turns: bool = True
    progress_contract_version: int = Field(default=1, ge=1)


class ProviderGoalAssignment(BaseModel):
    provider_id: GoalReferenceId
    available_commands: list[str] = Field(default_factory=list)
    supports_session_load: bool = True
    strategy_id: GoalReferenceId | None = None
    estimated_usage: GoalUsage = Field(default_factory=lambda: GoalUsage(actions=1))
    materialization_envelope: GoalMaterializationEnvelopeV1 | None = None
    role: GoalActorRole = GoalActorRole.EXECUTOR
    replaces_run_id: GoalReferenceId | None = None
    max_attempts: int = Field(default=3, ge=1, le=20)


class ProviderGoalInvocation(BaseModel):
    provider_id: GoalReferenceId
    mode: ProviderGoalMode
    command_name: GoalReferenceId | None = None
    arguments: dict[str, Any] = Field(default_factory=dict)
    prompt: str
    progress_contract_version: int = Field(default=1, ge=1)
    canonical_goal_id: GoalReferenceId
    policy_revision: int = Field(ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProviderGoalRun(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    goal_id: GoalReferenceId
    provider_id: GoalReferenceId
    invocation: ProviderGoalInvocation
    strategy_id: GoalReferenceId | None = None
    role: GoalActorRole = GoalActorRole.EXECUTOR
    executor_principal: GoalReferenceId | None = None
    authority_instance_id: GoalReferenceId = "legacy"
    fencing_token: int | None = Field(default=None, ge=1)
    reservation_id: GoalReferenceId | None = None
    materialization_envelope: GoalMaterializationEnvelopeV1 | None = None
    materialization_receipt: GoalMaterializationReceiptV1 | None = None
    execution_identity: GoalExecutionIdentityV1 | None = None
    attempt: int = Field(default=1, ge=1)
    max_attempts: int = Field(default=3, ge=1, le=20)
    replaces_run_id: GoalReferenceId | None = None
    launch_decision_id: GoalReferenceId | None = None
    launched_at: datetime | None = None
    progress_credential_hash: str = ""
    state: ProviderRunState = ProviderRunState.ASSIGNED
    summary: str = ""
    reserved_usage: GoalUsage = Field(default_factory=GoalUsage)
    usage: GoalUsage = Field(default_factory=GoalUsage)
    blocker_refs: list[GoalReferenceId] = Field(default_factory=list)
    interaction_refs: list[GoalReferenceId] = Field(default_factory=list)
    wait_generation: int = Field(default=0, ge=0)
    waiting_interaction_refs: list[GoalReferenceId] = Field(default_factory=list)
    artifact_refs: list[GoalReferenceId] = Field(default_factory=list)
    evidence_claims: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def migrate_service_identity(self) -> ProviderGoalRun:
        service_role = self.role.value
        if not self.executor_principal:
            self.executor_principal = _bounded_derived_reference(
                f"service:goal-{service_role}", self.provider_id, self.id
            )
        if not self.reservation_id:
            self.reservation_id = _bounded_derived_reference(
                "legacy-provider-run", self.id
            )
        return self


class ProviderGoalProgress(BaseModel):
    run_id: GoalReferenceId
    state: ProviderRunState
    summary: str = Field(min_length=1)
    cumulative_usage: GoalUsage = Field(default_factory=GoalUsage)
    blocker_refs: list[GoalReferenceId] = Field(default_factory=list)
    interaction_refs: list[GoalReferenceId] = Field(default_factory=list)
    artifact_refs: list[GoalReferenceId] = Field(default_factory=list)
    evidence_claims: list[dict[str, Any]] = Field(default_factory=list)


class AssignedServiceProviderProgress(BaseModel):
    """Provider progress whose run identity is resolved from authentication."""

    state: ProviderRunState
    summary: str = Field(min_length=1)
    cumulative_usage: GoalUsage = Field(default_factory=GoalUsage)
    blocker_refs: list[GoalReferenceId] = Field(default_factory=list)
    interaction_refs: list[GoalReferenceId] = Field(default_factory=list)
    artifact_refs: list[GoalReferenceId] = Field(default_factory=list)
    evidence_claims: list[dict[str, Any]] = Field(default_factory=list)


class GoalAssignedServiceScope(BaseModel):
    """Exact execution identity authorized to receive one private credential."""

    goal_id: GoalReferenceId
    work_package_id: GoalReferenceId
    run_id: GoalReferenceId
    session_id: GoalReferenceId
    provider_id: GoalReferenceId
    target_instance_id: GoalReferenceId
    authority_instance_id: GoalReferenceId
    fencing_token: int = Field(ge=1)
    assigned_service_principal: GoalReferenceId
    service_role: GoalActorRole

    @model_validator(mode="after")
    def validate_assigned_role(self) -> GoalAssignedServiceScope:
        if self.service_role not in {
            GoalActorRole.EXECUTOR,
            GoalActorRole.VERIFIER,
        }:
            raise ValueError("assigned service credentials require executor or verifier role")
        expected = f"service:goal-{self.service_role.value}:"
        if not self.assigned_service_principal.startswith(expected):
            raise ValueError(
                "assigned service principal does not match its executor/verifier role"
            )
        return self


class GoalAssignedServiceCredential(BaseModel):
    """Durable credential digest and its complete, immutable authorization scope."""

    id: GoalReferenceId
    version: int = Field(default=1, ge=1)
    realm_id: GoalReferenceId
    scope: GoalAssignedServiceScope
    credential_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expires_at: datetime
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_expiry(self) -> GoalAssignedServiceCredential:
        if self.expires_at.tzinfo is None:
            raise ValueError("assigned service credential expiry must include a timezone")
        return self


class GoalAutonomyState(BaseModel):
    goal_id: GoalReferenceId
    realm_id: GoalReferenceId = "default"
    version: int = Field(default=0, ge=0)
    priority: int = Field(default=50, ge=0, le=100)
    strategies: list[GoalStrategy] = Field(default_factory=list)
    selected_strategy_ids: list[GoalReferenceId] = Field(default_factory=list)
    provider_runs: list[ProviderGoalRun] = Field(default_factory=list)
    usage: GoalUsage = Field(default_factory=GoalUsage)
    rate_windows: list[GoalRateWindow] = Field(default_factory=list)
    recent_decisions: list[GoalActionDecision] = Field(default_factory=list)
    action_reservations: list[GoalActionReservation] = Field(default_factory=list)
    resource_reservations: list[GoalResourceClaim] = Field(default_factory=list)
    derived_goal_ids: list[GoalReferenceId] = Field(default_factory=list)
    last_proposal_at: datetime | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_strategy_references(self) -> GoalAutonomyState:
        identifiers = [item.id for item in self.strategies]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("strategy ids must be unique")
        if len(self.selected_strategy_ids) != len(set(self.selected_strategy_ids)):
            raise ValueError("selected strategy ids must be unique")
        unknown = set(self.selected_strategy_ids) - set(identifiers)
        if unknown:
            raise ValueError(f"selected strategies are unknown: {sorted(unknown)}")
        return self


class GoalProposalRequest(BaseModel):
    kind: ProposalKind
    goal: GoalCreate
    category: str = Field(min_length=1, max_length=100)
    rationale: str = Field(min_length=1)
    parent_goal_id: GoalReferenceId | None = None
    parent_criterion_id: GoalReferenceId | None = None
    parent_risk: str | None = None
    requested_priority: int = Field(default=50, ge=0, le=100)

    @model_validator(mode="after")
    def validate_traceability(self) -> GoalProposalRequest:
        if self.kind == ProposalKind.DERIVED_SUBGOAL:
            if not self.parent_goal_id:
                raise ValueError("derived subgoals require a parent goal")
            if not self.parent_criterion_id and not self.parent_risk:
                raise ValueError(
                    "derived subgoals must trace to a parent criterion or risk"
                )
        elif self.parent_goal_id:
            raise ValueError("top-level proposals cannot name a parent goal")
        return self


class GoalProposal(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    realm_id: GoalReferenceId = "default"
    request: GoalProposalRequest
    proposed_by: GoalReferenceId
    disposition: ProposalDisposition = ProposalDisposition.PENDING_REVIEW
    policy_id: GoalReferenceId | None = None
    policy_version: int | None = None
    activated_goal_id: GoalReferenceId | None = None
    review_reason: str = ""
    version: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalProposalReview(BaseModel):
    approve: bool
    reason: str = Field(min_length=1)
    reviewer_principal: GoalReferenceId


class StandingGoalPolicy(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    categories: list[str] = Field(min_length=1)
    project_ids: list[GoalReferenceId] = Field(default_factory=list)
    max_priority: int = Field(default=50, ge=0, le=100)
    max_cost_usd: float | None = Field(default=None, ge=0)
    max_tokens: int | None = Field(default=None, ge=1)
    expires_at: datetime
    enabled: bool = True


class GoalResourceCapacity(BaseModel):
    key: str = Field(min_length=1, max_length=300, pattern=r"\S")
    capacity: float = Field(gt=0)


class GoalGovernancePolicy(BaseModel):
    id: GoalReferenceId = "organization"
    realm_id: GoalReferenceId = "default"
    version: int = Field(default=1, ge=1)
    max_active_goals: int = Field(default=25, ge=1, le=100_000)
    max_portfolio_cost_usd: float | None = Field(default=None, ge=0)
    max_portfolio_tokens: int | None = Field(default=None, ge=1)
    provider_rate_limits: dict[GoalReferenceId, list[GoalRateLimit]] = Field(
        default_factory=dict
    )
    resource_capacities: list[GoalResourceCapacity] = Field(default_factory=list)
    standing_goal_policies: list[StandingGoalPolicy] = Field(default_factory=list)
    required_review_risk: GoalActionRisk = GoalActionRisk.HIGH
    review_interval_seconds: int = Field(default=86_400, ge=60, le=31_536_000)
    authored_by: GoalReferenceId = "user:local"
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def unique_governance_keys(self) -> GoalGovernancePolicy:
        capacities = [item.key for item in self.resource_capacities]
        standing = [item.id for item in self.standing_goal_policies]
        if len(capacities) != len(set(capacities)):
            raise ValueError("resource-capacity keys must be unique")
        if len(standing) != len(set(standing)):
            raise ValueError("standing goal-policy ids must be unique")
        return self


class GoalPortfolioEntry(BaseModel):
    goal_id: GoalReferenceId
    priority_score: float
    disposition: AllocationDisposition
    reasons: list[str]
    resource_claims: list[GoalResourceClaim] = Field(default_factory=list)


class GoalPortfolioReviewRequest(BaseModel):
    reviewer_principal: GoalReferenceId
    independent: bool = True
    explanation: str = Field(min_length=1)


class GoalPortfolioReview(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    realm_id: GoalReferenceId = "default"
    version: int = Field(default=1, ge=1)
    governance_policy_id: GoalReferenceId
    governance_policy_version: int = Field(ge=1)
    reviewer_principal: GoalReferenceId
    independent: bool
    explanation: str
    allocations: list[GoalPortfolioEntry]
    total_usage: GoalUsage = Field(default_factory=GoalUsage)
    pending_proposal_ids: list[GoalReferenceId] = Field(default_factory=list)
    findings: list[str] = Field(default_factory=list)
    requires_operator_review: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GovernanceMutationContext(BaseModel):
    actor_principal: GoalReferenceId
    authority_instance_id: GoalReferenceId
    idempotency_key: GoalReferenceId
    expected_version: int = Field(ge=0)
    policy_revision: int = Field(ge=1)
    goal_version: int | None = Field(default=None, ge=1)
    fencing_token: int | None = Field(default=None, ge=1)
