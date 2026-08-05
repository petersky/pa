"""Typed contracts for advanced goal autonomy and portfolio governance."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from pa.goals.models import GoalActorRole, GoalCreate, GoalRateLimit


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
    key: str
    started_at: datetime
    usage: GoalUsage = Field(default_factory=GoalUsage)


class GoalResourceClaim(BaseModel):
    key: str = Field(min_length=1, max_length=300)
    access: ResourceAccess = ResourceAccess.SHARED
    quantity: float = Field(default=1, gt=0)
    preemptible: bool = True
    expires_at: datetime | None = None


class GoalActionRequest(BaseModel):
    action_class: str = Field(min_length=1, max_length=200)
    operation_digest: str | None = Field(
        default=None,
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )
    risk: GoalActionRisk = GoalActionRisk.LOW
    reversible: bool = True
    delegated: bool = False
    external: bool = False
    audience: str | None = None
    repository: str | None = None
    data_scope: str | None = None
    provider_id: str | None = None
    estimate: GoalUsage = Field(default_factory=lambda: GoalUsage(actions=1))
    resource_claims: list[GoalResourceClaim] = Field(default_factory=list)
    operator_approved: bool = False
    approval_principal: str | None = None
    approval_interaction_id: str | None = None

    @model_validator(mode="after")
    def approval_is_attributable(self) -> GoalActionRequest:
        if self.operator_approved and not self.approval_principal:
            raise ValueError("operator-approved actions require an approval principal")
        self.estimate.actions = max(self.estimate.actions, 1)
        return self


class GoalActionDecision(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    goal_id: str
    action_class: str
    disposition: GoalActionDisposition
    reasons: list[str] = Field(min_length=1)
    policy_revision: int = Field(ge=1)
    request: GoalActionRequest
    reserved_usage: GoalUsage = Field(default_factory=GoalUsage)
    decided_by: str
    authority_instance_id: str = ""
    fencing_token: int | None = Field(default=None, ge=1)
    reservation_id: str | None = None
    decided_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalActionApply(BaseModel):
    reservation_id: str = Field(min_length=1)
    actual_usage: GoalUsage | None = None


class GoalActionRelease(BaseModel):
    reservation_id: str = Field(min_length=1)
    actual_usage: GoalUsage | None = None
    reason: str = Field(min_length=1, max_length=500)


class GoalActionReservation(BaseModel):
    """A durable, fenced hold created before an autonomous side effect."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    idempotency_key: str = Field(default="", max_length=300)
    decision_id: str
    goal_id: str
    action_class: str
    actor_principal: str
    authority_instance_id: str
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


class GoalStrategy(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    title: str = Field(min_length=1, max_length=300)
    hypothesis: str = Field(min_length=1)
    expected_outcome: str = Field(min_length=1)
    state: StrategyState = StrategyState.CANDIDATE
    score: float = Field(default=0, ge=0, le=1)
    allocated_cost_usd: float = Field(default=0, ge=0)
    allocated_tokens: int = Field(default=0, ge=0)
    risk: GoalActionRisk = GoalActionRisk.LOW
    evidence_ids: list[str] = Field(default_factory=list)


class GoalStrategyPortfolioUpdate(BaseModel):
    strategies: list[GoalStrategy] = Field(min_length=1)
    selected_strategy_ids: list[str] = Field(default_factory=list)
    reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_strategy_ids(self) -> GoalStrategyPortfolioUpdate:
        identifiers = [item.id for item in self.strategies]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("strategy ids must be unique")
        unknown = set(self.selected_strategy_ids) - set(identifiers)
        if unknown:
            raise ValueError(f"selected strategies are unknown: {sorted(unknown)}")
        return self


class ProviderGoalCapabilities(BaseModel):
    provider_id: str
    native_command_candidates: list[str] = Field(default_factory=lambda: ["goal"])
    supports_native_goal: bool = True
    supports_recoverable_turns: bool = True
    progress_contract_version: int = Field(default=1, ge=1)


class ProviderGoalAssignment(BaseModel):
    provider_id: str = Field(min_length=1)
    available_commands: list[str] = Field(default_factory=list)
    supports_session_load: bool = True
    strategy_id: str | None = None
    estimated_usage: GoalUsage = Field(default_factory=lambda: GoalUsage(actions=1))
    role: GoalActorRole = GoalActorRole.EXECUTOR
    replaces_run_id: str | None = None
    max_attempts: int = Field(default=3, ge=1, le=20)


class ProviderGoalInvocation(BaseModel):
    provider_id: str
    mode: ProviderGoalMode
    command_name: str | None = None
    arguments: dict[str, Any] = Field(default_factory=dict)
    prompt: str
    progress_contract_version: int = Field(default=1, ge=1)
    canonical_goal_id: str
    policy_revision: int = Field(ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProviderGoalRun(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    goal_id: str
    provider_id: str
    invocation: ProviderGoalInvocation
    strategy_id: str | None = None
    role: GoalActorRole = GoalActorRole.EXECUTOR
    executor_principal: str = ""
    authority_instance_id: str = "legacy"
    fencing_token: int | None = Field(default=None, ge=1)
    reservation_id: str = ""
    attempt: int = Field(default=1, ge=1)
    max_attempts: int = Field(default=3, ge=1, le=20)
    replaces_run_id: str | None = None
    launch_decision_id: str | None = None
    launched_at: datetime | None = None
    progress_credential_hash: str = ""
    state: ProviderRunState = ProviderRunState.ASSIGNED
    summary: str = ""
    reserved_usage: GoalUsage = Field(default_factory=GoalUsage)
    usage: GoalUsage = Field(default_factory=GoalUsage)
    blocker_refs: list[str] = Field(default_factory=list)
    interaction_refs: list[str] = Field(default_factory=list)
    wait_generation: int = Field(default=0, ge=0)
    waiting_interaction_refs: list[str] = Field(default_factory=list)
    artifact_refs: list[str] = Field(default_factory=list)
    evidence_claims: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def migrate_service_identity(self) -> ProviderGoalRun:
        service_role = self.role.value
        if not self.executor_principal:
            self.executor_principal = (
                f"service:goal-{service_role}:{self.provider_id}:{self.id}"
            )
        if not self.reservation_id:
            self.reservation_id = f"legacy-provider-run:{self.id}"
        return self


class ProviderGoalProgress(BaseModel):
    run_id: str
    state: ProviderRunState
    summary: str = Field(min_length=1)
    cumulative_usage: GoalUsage = Field(default_factory=GoalUsage)
    blocker_refs: list[str] = Field(default_factory=list)
    interaction_refs: list[str] = Field(default_factory=list)
    artifact_refs: list[str] = Field(default_factory=list)
    evidence_claims: list[dict[str, Any]] = Field(default_factory=list)


class GoalAutonomyState(BaseModel):
    goal_id: str
    realm_id: str = "default"
    version: int = Field(default=0, ge=0)
    priority: int = Field(default=50, ge=0, le=100)
    strategies: list[GoalStrategy] = Field(default_factory=list)
    selected_strategy_ids: list[str] = Field(default_factory=list)
    provider_runs: list[ProviderGoalRun] = Field(default_factory=list)
    usage: GoalUsage = Field(default_factory=GoalUsage)
    rate_windows: list[GoalRateWindow] = Field(default_factory=list)
    recent_decisions: list[GoalActionDecision] = Field(default_factory=list)
    action_reservations: list[GoalActionReservation] = Field(default_factory=list)
    resource_reservations: list[GoalResourceClaim] = Field(default_factory=list)
    derived_goal_ids: list[str] = Field(default_factory=list)
    last_proposal_at: datetime | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalProposalRequest(BaseModel):
    kind: ProposalKind
    goal: GoalCreate
    category: str = Field(min_length=1, max_length=100)
    rationale: str = Field(min_length=1)
    parent_goal_id: str | None = None
    parent_criterion_id: str | None = None
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
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str = "default"
    request: GoalProposalRequest
    proposed_by: str
    disposition: ProposalDisposition = ProposalDisposition.PENDING_REVIEW
    policy_id: str | None = None
    policy_version: int | None = None
    activated_goal_id: str | None = None
    review_reason: str = ""
    version: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalProposalReview(BaseModel):
    approve: bool
    reason: str = Field(min_length=1)
    reviewer_principal: str = Field(min_length=1)


class StandingGoalPolicy(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    categories: list[str] = Field(min_length=1)
    project_ids: list[str] = Field(default_factory=list)
    max_priority: int = Field(default=50, ge=0, le=100)
    max_cost_usd: float | None = Field(default=None, ge=0)
    max_tokens: int | None = Field(default=None, ge=1)
    expires_at: datetime
    enabled: bool = True


class GoalResourceCapacity(BaseModel):
    key: str = Field(min_length=1, max_length=300)
    capacity: float = Field(gt=0)


class GoalGovernancePolicy(BaseModel):
    id: str = "organization"
    realm_id: str = "default"
    version: int = Field(default=1, ge=1)
    max_active_goals: int = Field(default=25, ge=1, le=100_000)
    max_portfolio_cost_usd: float | None = Field(default=None, ge=0)
    max_portfolio_tokens: int | None = Field(default=None, ge=1)
    provider_rate_limits: dict[str, list[GoalRateLimit]] = Field(default_factory=dict)
    resource_capacities: list[GoalResourceCapacity] = Field(default_factory=list)
    standing_goal_policies: list[StandingGoalPolicy] = Field(default_factory=list)
    required_review_risk: GoalActionRisk = GoalActionRisk.HIGH
    review_interval_seconds: int = Field(default=86_400, ge=60, le=31_536_000)
    authored_by: str = "user:local"
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
    goal_id: str
    priority_score: float
    disposition: AllocationDisposition
    reasons: list[str]
    resource_claims: list[GoalResourceClaim] = Field(default_factory=list)


class GoalPortfolioReviewRequest(BaseModel):
    reviewer_principal: str = Field(min_length=1)
    independent: bool = True
    explanation: str = Field(min_length=1)


class GoalPortfolioReview(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str = "default"
    version: int = Field(default=1, ge=1)
    governance_policy_id: str
    governance_policy_version: int = Field(ge=1)
    reviewer_principal: str
    independent: bool
    explanation: str
    allocations: list[GoalPortfolioEntry]
    total_usage: GoalUsage = Field(default_factory=GoalUsage)
    pending_proposal_ids: list[str] = Field(default_factory=list)
    findings: list[str] = Field(default_factory=list)
    requires_operator_review: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GovernanceMutationContext(BaseModel):
    actor_principal: str
    authority_instance_id: str
    idempotency_key: str = Field(min_length=1, max_length=200)
    expected_version: int = Field(ge=0)
    policy_revision: int = Field(ge=1)
    goal_version: int | None = Field(default=None, ge=1)
    fencing_token: int | None = Field(default=None, ge=1)
