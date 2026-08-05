from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


class GoalState(StrEnum):
    DRAFT = "draft"
    SHAPING = "shaping"
    READY = "ready"
    ACTIVE = "active"
    VERIFYING = "verifying"
    WAITING_OPERATOR = "waiting_operator"
    WAITING_EXTERNAL = "waiting_external"
    PAUSED = "paused"
    BLOCKED = "blocked"
    ACHIEVED = "achieved"
    ABANDONED = "abandoned"


class CriterionVerdict(StrEnum):
    PENDING = "pending"
    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    INCONCLUSIVE = "inconclusive"


class EvidenceKind(StrEnum):
    TEST = "test"
    ARTIFACT = "artifact"
    OBSERVATION = "observation"
    OPERATOR_ACCEPTANCE = "operator_acceptance"
    AUDIT = "audit"


class GoalActorRole(StrEnum):
    COORDINATOR = "coordinator"
    EXECUTOR = "executor"
    VERIFIER = "verifier"
    CRITIC = "critic"


class ProposalStatus(StrEnum):
    PENDING = "pending"
    AUTHORIZED = "authorized"
    OPERATOR_REQUIRED = "operator_required"
    REJECTED = "rejected"
    APPLIED = "applied"
    FAILED = "failed"


class AuthorizationOutcome(StrEnum):
    AUTHORIZE = "authorize"
    REQUIRE_OPERATOR = "require_operator"
    REJECT = "reject"


class WorkPackageState(StrEnum):
    PLANNED = "planned"
    READY = "ready"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    AWAITING_VERIFICATION = "awaiting_verification"
    VERIFIED = "verified"
    BLOCKED = "blocked"
    FAILED = "failed"
    CANCELLED = "cancelled"


class GoalDispatchAttemptState(StrEnum):
    STAGED = "staged"
    ADMITTED = "admitted"
    REJECTED = "rejected"


class GoalInteractionState(StrEnum):
    PENDING = "pending"
    ANSWERED = "answered"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class GoalDriftState(StrEnum):
    ON_TRACK = "on_track"
    DRIFTING = "drifting"
    STALLED = "stalled"


class GoalCriterion(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    description: str = Field(min_length=1)
    verification_method: str = Field(min_length=1)
    evidence_requirement: str = Field(min_length=1)
    verdict: CriterionVerdict = CriterionVerdict.PENDING
    evidence_ids: list[str] = Field(default_factory=list)
    freshness_seconds: int | None = Field(default=None, ge=1)
    required_evidence_kinds: list[EvidenceKind] = Field(default_factory=list)
    minimum_evidence_count: int = Field(default=1, ge=1, le=100)
    require_independent_verifier: bool = False
    explanation: str = ""


class GoalEvidence(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    criterion_ids: list[str] = Field(min_length=1)
    kind: EvidenceKind
    uri: str = ""
    summary: str = Field(min_length=1)
    provenance: dict[str, Any] = Field(default_factory=dict)
    content_hash: str = ""
    observed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None
    sensitivity: str = "internal"
    contradictory: bool = False
    recorded_by_principal: str = ""
    recorded_by_instance_id: str = ""
    producer_role: GoalActorRole | None = None
    producer_service_id: str | None = None


class GoalRateLimit(BaseModel):
    """A rolling hard limit evaluated before an autonomous action is reserved."""

    key: str = Field(min_length=1, max_length=100)
    window_seconds: int = Field(ge=1, le=31_536_000)
    max_actions: int | None = Field(default=None, ge=1)
    max_cost_usd: float | None = Field(default=None, ge=0)
    max_tokens: int | None = Field(default=None, ge=1)
    max_api_calls: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def require_a_limit(self) -> GoalRateLimit:
        if all(
            value is None
            for value in (
                self.max_actions,
                self.max_cost_usd,
                self.max_tokens,
                self.max_api_calls,
            )
        ):
            raise ValueError("a goal rate limit must constrain at least one metric")
        return self


class GoalBudget(BaseModel):
    max_cost_usd: float | None = Field(default=None, ge=0)
    max_tokens: int | None = Field(default=None, ge=1)
    max_api_calls: int | None = Field(default=None, ge=1)
    max_storage_mb: float | None = Field(default=None, ge=0)
    max_actions: int | None = Field(default=None, ge=1)
    max_dispatches: int | None = Field(default=None, ge=1)
    max_concurrency: int = Field(default=1, ge=1, le=256)
    deadline: datetime | None = None
    retry_limit: int = Field(default=3, ge=0)
    rate_limits: list[GoalRateLimit] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_rate_limit_keys(self) -> GoalBudget:
        keys = [item.key for item in self.rate_limits]
        if len(keys) != len(set(keys)):
            raise ValueError("goal rate-limit keys must be unique")
        return self


class GoalPolicy(BaseModel):
    revision: int = Field(default=1, ge=1)
    autonomy_level: int = Field(default=1, ge=1, le=5)
    permitted_actions: list[str] = Field(default_factory=list)
    prohibited_actions: list[str] = Field(default_factory=list)
    repository_scope: list[str] = Field(default_factory=list)
    data_scope: list[str] = Field(default_factory=list)
    require_operator_for: list[str] = Field(default_factory=list)
    max_action_risk: str = Field(default="low", pattern="^(low|medium|high|critical)$")
    allowed_provider_ids: list[str] = Field(default_factory=list)
    allow_derived_subgoals: bool = False
    auto_activate_derived_subgoals: bool = False
    allow_top_level_proposals: bool = False
    max_subgoal_depth: int = Field(default=2, ge=0, le=16)
    max_derived_subgoals: int = Field(default=10, ge=0, le=10_000)
    proposal_cooldown_seconds: int = Field(default=300, ge=0, le=31_536_000)
    authored_by: str = "user:local"
    effective_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalLease(BaseModel):
    holder_instance_id: str | None = None
    fencing_token: int = Field(default=0, ge=0)
    expires_at: datetime | None = None
    claim_id: str | None = None
    eligible_instance_ids: list[str] = Field(default_factory=list)
    acquired_at: datetime | None = None

    def active(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        return bool(
            self.holder_instance_id and self.expires_at and self.expires_at > now
        )


class GoalWakeup(BaseModel):
    wake_at: datetime
    reason: str = Field(min_length=1)
    eligible_instance_ids: list[str] = Field(default_factory=list)
    claimed_by_instance_id: str | None = None
    claimed_at: datetime | None = None


class GoalAudit(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    auditor_principal: str
    auditor_instance_id: str = ""
    verifier_service_id: str | None = None
    independent: bool = True
    verdict: CriterionVerdict
    criterion_verdicts: dict[str, CriterionVerdict]
    evidence_ids: list[str] = Field(default_factory=list)
    explanation: str = Field(min_length=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CreateWorkPackageAction(BaseModel):
    kind: Literal["create_work_package"] = "create_work_package"
    title: str = Field(min_length=1, max_length=300)
    objective: str = Field(min_length=1, max_length=16_000)
    criterion_ids: list[str] = Field(min_length=1)
    depends_on: list[str] = Field(default_factory=list)
    role: GoalActorRole = GoalActorRole.EXECUTOR
    card_id: str | None = None
    preferred_instance_id: str | None = None
    preferred_capabilities: list[str] = Field(default_factory=list)
    max_attempts: int = Field(default=3, ge=1, le=20)
    dispatch_when_ready: bool = True


class DispatchWorkPackageAction(BaseModel):
    kind: Literal["dispatch_work_package"] = "dispatch_work_package"
    work_package_id: str
    target_instance_id: str | None = None
    placement_policy: str | None = "best_match"
    group_id: str | None = None
    message: str = ""
    provider: str | None = None
    model_id: str | None = None
    mode_id: str | None = None
    priority: int = Field(default=0, ge=-10, le=10)

    @model_validator(mode="after")
    def validate_target(self) -> DispatchWorkPackageAction:
        if bool(self.target_instance_id) == bool(self.placement_policy):
            raise ValueError(
                "dispatch proposal requires exactly one target instance or placement policy"
            )
        return self


class GoalOperatorChoice(BaseModel):
    id: str = Field(min_length=1, max_length=200)
    label: str = Field(min_length=1, max_length=300)
    description: str | None = Field(default=None, max_length=1000)
    value: Any = None


class RequestOperatorAction(BaseModel):
    kind: Literal["request_operator"] = "request_operator"
    prompt: str = Field(min_length=1, max_length=8000)
    response_schema: dict[str, Any] | None = None
    choices: list[GoalOperatorChoice] = Field(default_factory=list, max_length=100)
    allow_freeform: bool = False
    allow_cancel: bool = True
    deadline: datetime | None = None

    @model_validator(mode="after")
    def validate_response_contract(self) -> RequestOperatorAction:
        if not self.response_schema and not self.choices and not self.allow_freeform:
            raise ValueError(
                "operator request needs a response schema, choices, or freeform input"
            )
        return self


class ReviseStrategyAction(BaseModel):
    kind: Literal["revise_strategy"] = "revise_strategy"
    summary: str = Field(min_length=1, max_length=8000)
    assumptions: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)


class RecordEvidenceAction(BaseModel):
    kind: Literal["record_evidence"] = "record_evidence"
    evidence: GoalEvidence
    criterion_verdicts: dict[str, CriterionVerdict] = Field(default_factory=dict)


class TransitionGoalAction(BaseModel):
    kind: Literal["transition_goal"] = "transition_goal"
    state: GoalState
    reason: str = Field(min_length=1)
    progress_summary: str | None = None


GoalProposalAction = Annotated[
    CreateWorkPackageAction
    | DispatchWorkPackageAction
    | RequestOperatorAction
    | ReviseStrategyAction
    | RecordEvidenceAction
    | TransitionGoalAction,
    Field(discriminator="kind"),
]


class GoalAuthorizationDecision(BaseModel):
    outcome: AuthorizationOutcome
    policy_revision: int = Field(ge=1)
    reason_code: str = Field(min_length=1)
    explanation: str = Field(min_length=1)
    decision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    decided_by_instance_id: str
    decided_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalProposal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    proposer_principal: str
    proposer_role: GoalActorRole
    action: GoalProposalAction
    rationale: str = Field(min_length=1, max_length=8000)
    expected_goal_version: int = Field(ge=1)
    policy_revision: int = Field(ge=1)
    status: ProposalStatus = ProposalStatus.PENDING
    authorization: GoalAuthorizationDecision | None = None
    applied_event_id: str | None = None
    error: str | None = Field(default=None, max_length=2000)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalProposalCreate(BaseModel):
    proposer_principal: str
    proposer_role: GoalActorRole
    action: GoalProposalAction
    rationale: str = Field(min_length=1, max_length=8000)
    expected_goal_version: int = Field(ge=1)
    policy_revision: int = Field(ge=1)


class GoalDispatchAttempt(BaseModel):
    generation: int = Field(ge=1)
    proposal_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1, max_length=200)
    request_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    dispatch_payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: GoalDispatchAttemptState = GoalDispatchAttemptState.STAGED
    reservation_id: str | None = Field(default=None, min_length=1)
    dispatch_id: str | None = Field(default=None, min_length=1)
    admission_receipt_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    fleet_lifecycle_owned: bool = False
    release_pending: bool = False
    error: str = Field(default="", max_length=2000)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_state(self) -> GoalDispatchAttempt:
        if self.state == GoalDispatchAttemptState.STAGED:
            if any(
                (
                    self.reservation_id,
                    self.dispatch_id,
                    self.admission_receipt_digest,
                    self.fleet_lifecycle_owned,
                    self.release_pending,
                    self.error,
                )
            ):
                raise ValueError("staged dispatch attempts cannot claim an outcome")
        elif self.state == GoalDispatchAttemptState.ADMITTED:
            if (
                not self.reservation_id
                or not self.dispatch_id
                or not self.admission_receipt_digest
                or not self.fleet_lifecycle_owned
                or self.release_pending
                or self.error
            ):
                raise ValueError(
                    "admitted dispatch attempts require one Fleet-owned receipt"
                )
        elif (
            not self.reservation_id
            or self.dispatch_id
            or self.admission_receipt_digest
            or self.fleet_lifecycle_owned
            or not self.release_pending
            or not self.error
        ):
            raise ValueError(
                "rejected dispatch attempts require one pending local release"
            )
        return self


class GoalWorkPackage(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    proposal_id: str
    title: str
    objective: str
    criterion_ids: list[str] = Field(min_length=1)
    depends_on: list[str] = Field(default_factory=list)
    role: GoalActorRole = GoalActorRole.EXECUTOR
    state: WorkPackageState = WorkPackageState.PLANNED
    card_id: str | None = None
    preferred_instance_id: str | None = None
    preferred_capabilities: list[str] = Field(default_factory=list)
    dispatch_when_ready: bool = True
    dispatch_ids: list[str] = Field(default_factory=list)
    session_id: str | None = None
    replacement_session_ids: list[str] = Field(default_factory=list)
    executor_service_id: str | None = None
    verifier_service_id: str | None = None
    action_reservation_id: str | None = None
    dispatch_attempt: GoalDispatchAttempt | None = None
    dispatch_admission_receipt_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    fleet_lifecycle_owned: bool = False
    attempts: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=3, ge=1, le=20)
    last_progress_fingerprint: str | None = None
    last_progress_at: datetime | None = None
    no_progress_cycles: int = Field(default=0, ge=0)
    result_summary: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalOperatorInteraction(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    proposal_id: str
    notification_id: str
    state: GoalInteractionState = GoalInteractionState.PENDING
    response_summary: str = ""
    response_principal: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    resolved_at: datetime | None = None


class GoalSupervision(BaseModel):
    cycle: int = Field(default=0, ge=0)
    event_cursor: str = ""
    drift_state: GoalDriftState = GoalDriftState.ON_TRACK
    drift_reasons: list[str] = Field(default_factory=list)
    no_progress_cycles: int = Field(default=0, ge=0)
    last_meaningful_progress_at: datetime | None = None
    last_cycle_at: datetime | None = None
    next_wakeup_at: datetime | None = None
    controller_session_id: str | None = None
    replacement_session_ids: list[str] = Field(default_factory=list)


class Goal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str = "default"
    project_id: str | None = None
    parent_goal_id: str | None = None
    owner_principal: str = "user:local"
    creation_source: str = "operator"
    objective: str = Field(min_length=1)
    motivation: str = ""
    constraints: list[str] = Field(default_factory=list)
    non_goals: list[str] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    criteria: list[GoalCriterion] = Field(min_length=1)
    evidence: list[GoalEvidence] = Field(default_factory=list)
    policy: GoalPolicy = Field(default_factory=GoalPolicy)
    budget: GoalBudget = Field(default_factory=GoalBudget)
    state: GoalState = GoalState.DRAFT
    revision: int = Field(default=1, ge=1)
    version: int = Field(default=1, ge=1)
    strategy_revision: int = Field(default=1, ge=1)
    progress_summary: str = ""
    lease: GoalLease = Field(default_factory=GoalLease)
    wakeup: GoalWakeup | None = None
    linked_card_ids: list[str] = Field(default_factory=list)
    linked_dispatch_ids: list[str] = Field(default_factory=list)
    audit: GoalAudit | None = None
    proposals: list[GoalProposal] = Field(default_factory=list)
    work_packages: list[GoalWorkPackage] = Field(default_factory=list)
    operator_interactions: list[GoalOperatorInteraction] = Field(default_factory=list)
    supervision: GoalSupervision = Field(default_factory=GoalSupervision)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_references(self) -> Goal:
        criterion_ids = [criterion.id for criterion in self.criteria]
        if len(criterion_ids) != len(set(criterion_ids)):
            raise ValueError("criterion ids must be unique")
        known = set(criterion_ids)
        evidence_by_id: dict[str, GoalEvidence] = {}
        for item in self.evidence:
            if item.id in evidence_by_id:
                raise ValueError("evidence ids must be unique")
            evidence_by_id[item.id] = item
            if len(item.criterion_ids) != len(set(item.criterion_ids)):
                raise ValueError("evidence criterion ids must be unique")
            if unknown := set(item.criterion_ids) - known:
                raise ValueError(
                    f"evidence references unknown criteria: {sorted(unknown)}"
                )
        for criterion in self.criteria:
            if len(criterion.evidence_ids) != len(set(criterion.evidence_ids)):
                raise ValueError("criterion evidence ids must be unique")
            if unknown := set(criterion.evidence_ids) - evidence_by_id.keys():
                raise ValueError(
                    f"criterion references unknown evidence: {sorted(unknown)}"
                )
            if mismatched := {
                evidence_id
                for evidence_id in criterion.evidence_ids
                if criterion.id not in evidence_by_id[evidence_id].criterion_ids
            }:
                raise ValueError(
                    "criterion references evidence that does not name the criterion: "
                    f"{sorted(mismatched)}"
                )
        evidence_ids_by_criterion = {
            criterion.id: set(criterion.evidence_ids) for criterion in self.criteria
        }
        for item in self.evidence:
            if missing := {
                criterion_id
                for criterion_id in item.criterion_ids
                if item.id not in evidence_ids_by_criterion[criterion_id]
            }:
                raise ValueError(
                    f"evidence is not linked by its named criteria: {sorted(missing)}"
                )
        proposal_ids = [proposal.id for proposal in self.proposals]
        if len(proposal_ids) != len(set(proposal_ids)):
            raise ValueError("proposal ids must be unique")
        proposal_id_set = set(proposal_ids)
        proposals_by_id = {proposal.id: proposal for proposal in self.proposals}
        work_ids = [package.id for package in self.work_packages]
        if len(work_ids) != len(set(work_ids)):
            raise ValueError("work package ids must be unique")
        work_id_set = set(work_ids)
        graph: dict[str, list[str]] = {}
        for package in self.work_packages:
            if package.proposal_id not in proposal_id_set:
                raise ValueError("work package references unknown proposal")
            attempt = package.dispatch_attempt
            if attempt is not None:
                proposal = proposals_by_id.get(attempt.proposal_id)
                if (
                    proposal is None
                    or not isinstance(proposal.action, DispatchWorkPackageAction)
                    or proposal.action.work_package_id != package.id
                ):
                    raise ValueError(
                        "dispatch attempt must reference its package proposal"
                    )
                expected_generation = (
                    package.attempts + 1
                    if attempt.state == GoalDispatchAttemptState.STAGED
                    else package.attempts
                )
                if attempt.generation != expected_generation:
                    raise ValueError(
                        "dispatch attempt generation does not match package attempts"
                    )
                if (
                    attempt.state == GoalDispatchAttemptState.ADMITTED
                    and (
                        package.action_reservation_id != attempt.reservation_id
                        or package.dispatch_admission_receipt_digest
                        != attempt.admission_receipt_digest
                        or attempt.dispatch_id not in package.dispatch_ids
                        or not package.fleet_lifecycle_owned
                    )
                ):
                    raise ValueError(
                        "admitted dispatch attempt is not bound to its package receipt"
                    )
                if (
                    attempt.state != GoalDispatchAttemptState.ADMITTED
                    and package.fleet_lifecycle_owned
                ):
                    raise ValueError(
                        "only admitted dispatch attempts can be Fleet lifecycle owned"
                    )
                if (
                    attempt.state == GoalDispatchAttemptState.REJECTED
                    and package.action_reservation_id != attempt.reservation_id
                ):
                    raise ValueError(
                        "rejected dispatch attempt is not bound to its package reservation"
                    )
            elif package.fleet_lifecycle_owned:
                raise ValueError(
                    "Fleet lifecycle ownership requires a durable dispatch attempt"
                )
            if unknown := set(package.criterion_ids) - known:
                raise ValueError(
                    f"work package references unknown criteria: {sorted(unknown)}"
                )
            if unknown := set(package.depends_on) - work_id_set:
                raise ValueError(
                    f"work package has unknown dependencies: {sorted(unknown)}"
                )
            if package.id in package.depends_on:
                raise ValueError("work package cannot depend on itself")
            graph[package.id] = package.depends_on
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(package_id: str) -> None:
            if package_id in visiting:
                raise ValueError("work package graph contains a cycle")
            if package_id in visited:
                return
            visiting.add(package_id)
            for dependency in graph[package_id]:
                visit(dependency)
            visiting.remove(package_id)
            visited.add(package_id)

        for package_id in work_ids:
            visit(package_id)
        executor_services = {
            package.executor_service_id
            for package in self.work_packages
            if package.executor_service_id
        }
        verifier_services = {
            package.verifier_service_id
            for package in self.work_packages
            if package.verifier_service_id
        }
        if overlap := executor_services & verifier_services:
            raise ValueError(
                "executor and verifier service identities must be distinct: "
                f"{sorted(overlap)}"
            )
        notification_ids: set[str] = set()
        for interaction in self.operator_interactions:
            if interaction.proposal_id not in proposal_id_set:
                raise ValueError("operator interaction references unknown proposal")
            if interaction.notification_id in notification_ids:
                raise ValueError("operator notification ids must be unique")
            notification_ids.add(interaction.notification_id)
        return self


class GoalCreate(BaseModel):
    realm_id: str = "default"
    project_id: str | None = None
    parent_goal_id: str | None = None
    owner_principal: str = "user:local"
    creation_source: str = "operator"
    objective: str = Field(min_length=1)
    motivation: str = ""
    constraints: list[str] = Field(default_factory=list)
    non_goals: list[str] = Field(default_factory=list)
    criteria: list[GoalCriterion] = Field(min_length=1)
    policy: GoalPolicy = Field(default_factory=GoalPolicy)
    budget: GoalBudget = Field(default_factory=GoalBudget)


class GoalRevision(BaseModel):
    objective: str | None = None
    motivation: str | None = None
    constraints: list[str] | None = None
    non_goals: list[str] | None = None
    criteria: list[GoalCriterion] | None = None
    policy: GoalPolicy | None = None
    budget: GoalBudget | None = None
    reason: str = Field(min_length=1)


class GoalMutationContext(BaseModel):
    actor_principal: str
    authority_instance_id: str
    idempotency_key: str = Field(min_length=1, max_length=200)
    expected_version: int = Field(ge=0)
    policy_revision: int = Field(ge=1)
    fencing_token: int | None = Field(default=None, ge=1)


class GoalTransition(BaseModel):
    state: GoalState
    reason: str = Field(min_length=1)
    progress_summary: str | None = None


class GoalEvidenceCreate(BaseModel):
    evidence: GoalEvidence
    criterion_verdicts: dict[str, CriterionVerdict] = Field(default_factory=dict)


class GoalAuditCreate(BaseModel):
    auditor_principal: str | None = Field(
        default=None,
        description=(
            "Deprecated identity assertion. The authenticated mutation principal is "
            "authoritative."
        ),
    )
    independent: bool = True
    criterion_verdicts: dict[str, CriterionVerdict]
    evidence_ids: list[str] = Field(default_factory=list)
    explanation: str = Field(min_length=1)


class GoalSupervisionCheckpoint(BaseModel):
    criteria: list[GoalCriterion]
    evidence: list[GoalEvidence]
    proposals: list[GoalProposal]
    work_packages: list[GoalWorkPackage]
    operator_interactions: list[GoalOperatorInteraction]
    supervision: GoalSupervision
    linked_card_ids: list[str]
    linked_dispatch_ids: list[str]
    assumptions: list[str]
    risks: list[str]
    strategy_revision: int = Field(ge=1)
    state: GoalState
    progress_summary: str = ""
    reason: str = Field(min_length=1)


class GoalEventRecord(BaseModel):
    id: str
    goal_id: str
    event_type: str
    actor_principal: str
    authority_instance_id: str
    policy_revision: int
    idempotency_key: str
    version: int
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
