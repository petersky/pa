from __future__ import annotations

import copy
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import BaseModel, Field, model_validator

from pa.goals.materialization import (
    GoalExecutionIdentityV1,
    GoalMaterializationEnvelopeV1,
    GoalMaterializationReceiptV1,
)

GoalReferenceId = Annotated[
    str,
    Field(min_length=1, max_length=200, pattern=r"\S"),
]


def _blank_reference(value: Any) -> bool:
    return isinstance(value, str) and not value.strip()


def _legacy_goal_identifier(goal_id: str, kind: str, index: int) -> str:
    return str(uuid5(NAMESPACE_URL, f"pa:goal:{goal_id}:legacy:{kind}:{index}"))


def normalize_legacy_goal_payload(
    value: Any,
    *,
    fallback_goal_id: Any = None,
    legacy_entity_seed: str | None = None,
) -> Any:
    """Canonicalize blank identifiers only while decoding a durable legacy event."""

    if not isinstance(value, dict):
        return value
    payload = copy.deepcopy(value)
    # A numeric/object fallback is corruption, not a legacy blank string.  Keep
    # the malformed value visible to the strict projection gate instead of
    # turning it into a plausible new identity.
    if fallback_goal_id is not None and not isinstance(fallback_goal_id, str):
        return payload
    raw_goal_id = payload.get("id", fallback_goal_id)
    if raw_goal_id is None or _blank_reference(raw_goal_id):
        raw_goal_id = (
            fallback_goal_id
            if isinstance(fallback_goal_id, str) and fallback_goal_id.strip()
            else None
        )
    if raw_goal_id is None:
        durable_seed = (
            legacy_entity_seed
            if isinstance(legacy_entity_seed, str) and legacy_entity_seed.strip()
            else "\0".join(
                (
                    str(payload.get("realm_id") or "default"),
                    str(payload.get("objective") or ""),
                    str(payload.get("created_at") or ""),
                )
            )
        )
        raw_goal_id = str(
            uuid5(
                NAMESPACE_URL,
                f"pa:legacy-goal:{durable_seed}",
            )
        )
    if "id" not in payload or _blank_reference(payload.get("id")):
        payload["id"] = raw_goal_id
    goal_id = str(raw_goal_id)

    def normalize_entities(field: str, kind: str) -> list[str] | None:
        replacements: list[str] = []
        known = False
        for index, item in enumerate(payload.get(field) or []):
            if not isinstance(item, dict):
                continue
            if "id" in item and not _blank_reference(item.get("id")):
                known = True
                continue
            replacement = _legacy_goal_identifier(goal_id, kind, index)
            item["id"] = replacement
            replacements.append(replacement)
        # Blank references cannot be assigned without loss when their entity
        # collection mixes already-known and synthetic identities.  Retain the
        # malformed references so strict Goal validation fails closed.
        return None if known and replacements else replacements

    criterion_ids = normalize_entities("criteria", "criterion")
    evidence_ids = normalize_entities("evidence", "evidence")
    proposal_ids = normalize_entities("proposals", "proposal")
    work_ids = normalize_entities("work_packages", "work-package")
    normalize_entities("operator_interactions", "operator-interaction")

    audit = payload.get("audit")
    if isinstance(audit, dict) and (
        "id" not in audit or _blank_reference(audit.get("id"))
    ):
        audit["id"] = _legacy_goal_identifier(goal_id, "audit", 0)

    def clear_optional_blank(container: Any, *fields: str) -> None:
        if not isinstance(container, dict):
            return
        for field in fields:
            if field in container and _blank_reference(container[field]):
                container[field] = None

    def drop_blank_items(container: Any, *fields: str) -> None:
        if not isinstance(container, dict):
            return
        for field in fields:
            values = container.get(field)
            if isinstance(values, list):
                container[field] = [
                    item for item in values if not _blank_reference(item)
                ]

    def rewrite_list(
        values: Any,
        replacements: list[str] | None,
        cursor: list[int],
        *,
        expand_single: bool = False,
    ) -> Any:
        if not isinstance(values, list) or not replacements:
            return values
        rewritten: list[Any] = []
        for item in values:
            if not _blank_reference(item):
                rewritten.append(item)
                continue
            if expand_single and len(values) == 1:
                rewritten.extend(replacements)
                cursor[0] += len(replacements)
                continue
            rewritten.append(replacements[cursor[0] % len(replacements)])
            cursor[0] += 1
        return rewritten

    def rewrite_dict_keys(values: Any, replacements: list[str] | None) -> Any:
        if not isinstance(values, dict) or not replacements:
            return values
        rewritten = {
            key: item for key, item in values.items() if not _blank_reference(key)
        }
        blank_items = [
            (key, item) for key, item in values.items() if _blank_reference(key)
        ]
        if len(blank_items) == 1:
            for replacement in replacements:
                rewritten[replacement] = blank_items[0][1]
        elif len(blank_items) == len(replacements):
            for replacement, (_, item) in zip(replacements, blank_items, strict=True):
                rewritten[replacement] = item
        else:
            # The relation is ambiguous. Preserve it so strict model validation
            # rejects the malformed event instead of silently corrupting values.
            rewritten.update(blank_items)
        return rewritten

    clear_optional_blank(payload, "project_id", "parent_goal_id")
    drop_blank_items(payload, "linked_card_ids", "linked_dispatch_ids")
    policy = payload.get("policy")
    drop_blank_items(policy, "allowed_provider_ids")
    lease = payload.get("lease")
    clear_optional_blank(lease, "holder_instance_id", "claim_id")
    drop_blank_items(lease, "eligible_instance_ids")
    wakeup = payload.get("wakeup")
    clear_optional_blank(wakeup, "claimed_by_instance_id")
    drop_blank_items(wakeup, "eligible_instance_ids")
    supervision = payload.get("supervision")
    clear_optional_blank(supervision, "controller_session_id")
    drop_blank_items(supervision, "replacement_session_ids")

    criterion_cursor = [0]
    evidence_cursor = [0]
    for criterion in payload.get("criteria") or []:
        if isinstance(criterion, dict) and "evidence_ids" in criterion:
            criterion["evidence_ids"] = rewrite_list(
                criterion.get("evidence_ids"), evidence_ids, evidence_cursor
            )
    for evidence in payload.get("evidence") or []:
        if isinstance(evidence, dict):
            evidence["criterion_ids"] = rewrite_list(
                evidence.get("criterion_ids"), criterion_ids, criterion_cursor
            )
            clear_optional_blank(
                evidence,
                "recorded_by_principal",
                "recorded_by_instance_id",
                "producer_service_id",
            )

    package_proposal_cursor = [0]
    package_criterion_cursor = [0]
    dependency_cursor = [0]
    for package in payload.get("work_packages") or []:
        if not isinstance(package, dict):
            continue
        rewritten = rewrite_list(
            [package.get("proposal_id")], proposal_ids, package_proposal_cursor
        )
        if rewritten:
            package["proposal_id"] = rewritten[0]
        package["criterion_ids"] = rewrite_list(
            package.get("criterion_ids"), criterion_ids, package_criterion_cursor
        )
        if "depends_on" in package:
            package["depends_on"] = rewrite_list(
                package.get("depends_on"), work_ids, dependency_cursor
            )
        clear_optional_blank(
            package,
            "card_id",
            "preferred_instance_id",
            "session_id",
            "executor_service_id",
            "verifier_service_id",
            "action_reservation_id",
        )
        drop_blank_items(package, "dispatch_ids", "replacement_session_ids")

    interaction_proposal_cursor = [0]
    for index, interaction in enumerate(payload.get("operator_interactions") or []):
        if not isinstance(interaction, dict):
            continue
        rewritten = rewrite_list(
            [interaction.get("proposal_id")],
            proposal_ids,
            interaction_proposal_cursor,
        )
        if rewritten:
            interaction["proposal_id"] = rewritten[0]
        clear_optional_blank(interaction, "response_principal")

    action_criterion_cursor = [0]
    action_dependency_cursor = [0]
    action_work_cursor = [0]
    for index, proposal in enumerate(payload.get("proposals") or []):
        if not isinstance(proposal, dict):
            continue
        clear_optional_blank(proposal, "applied_event_id")
        action = proposal.get("action")
        if not isinstance(action, dict):
            continue
        kind = action.get("kind")
        if kind == "create_work_package":
            action["criterion_ids"] = rewrite_list(
                action.get("criterion_ids"), criterion_ids, action_criterion_cursor
            )
            if "depends_on" in action:
                action["depends_on"] = rewrite_list(
                    action.get("depends_on"), work_ids, action_dependency_cursor
                )
            clear_optional_blank(action, "card_id", "preferred_instance_id")
        elif kind == "dispatch_work_package":
            rewritten = rewrite_list(
                [action.get("work_package_id")], work_ids, action_work_cursor
            )
            if rewritten:
                action["work_package_id"] = rewritten[0]
            clear_optional_blank(
                action,
                "target_instance_id",
                "group_id",
                "provider",
                "model_id",
                "mode_id",
            )
        elif kind == "record_evidence":
            evidence = action.get("evidence")
            if isinstance(evidence, dict):
                if "id" not in evidence or _blank_reference(evidence.get("id")):
                    evidence["id"] = _legacy_goal_identifier(
                        goal_id, "proposal-evidence", index
                    )
                evidence["criterion_ids"] = rewrite_list(
                    evidence.get("criterion_ids"),
                    criterion_ids,
                    action_criterion_cursor,
                )
            if "criterion_verdicts" in action:
                action["criterion_verdicts"] = rewrite_dict_keys(
                    action.get("criterion_verdicts"), criterion_ids
                )

    if isinstance(audit, dict):
        clear_optional_blank(audit, "auditor_instance_id", "verifier_service_id")
        audit["criterion_verdicts"] = rewrite_dict_keys(
            audit.get("criterion_verdicts"), criterion_ids
        )
        if "evidence_ids" in audit:
            audit["evidence_ids"] = rewrite_list(
                audit.get("evidence_ids"),
                evidence_ids,
                [0],
                expand_single=True,
            )
    return payload


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
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    description: str = Field(min_length=1)
    verification_method: str = Field(min_length=1)
    evidence_requirement: str = Field(min_length=1)
    verdict: CriterionVerdict = CriterionVerdict.PENDING
    evidence_ids: list[GoalReferenceId] = Field(default_factory=list)
    freshness_seconds: int | None = Field(default=None, ge=1)
    required_evidence_kinds: list[EvidenceKind] = Field(default_factory=list)
    minimum_evidence_count: int = Field(default=1, ge=1, le=100)
    require_independent_verifier: bool = False
    explanation: str = ""


class GoalEvidence(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    criterion_ids: list[GoalReferenceId] = Field(min_length=1)
    kind: EvidenceKind
    uri: str = ""
    summary: str = Field(min_length=1)
    provenance: dict[str, Any] = Field(default_factory=dict)
    content_hash: str = ""
    observed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None
    sensitivity: str = "internal"
    contradictory: bool = False
    recorded_by_principal: GoalReferenceId | None = None
    recorded_by_instance_id: GoalReferenceId | None = None
    producer_role: GoalActorRole | None = None
    producer_service_id: GoalReferenceId | None = None


class GoalRateLimit(BaseModel):
    """A rolling hard limit evaluated before an autonomous action is reserved."""

    key: GoalReferenceId = Field(max_length=100)
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
    allowed_provider_ids: list[GoalReferenceId] = Field(default_factory=list)
    allow_derived_subgoals: bool = False
    auto_activate_derived_subgoals: bool = False
    allow_top_level_proposals: bool = False
    max_subgoal_depth: int = Field(default=2, ge=0, le=16)
    max_derived_subgoals: int = Field(default=10, ge=0, le=10_000)
    proposal_cooldown_seconds: int = Field(default=300, ge=0, le=31_536_000)
    authored_by: GoalReferenceId = "user:local"
    effective_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalLease(BaseModel):
    holder_instance_id: GoalReferenceId | None = None
    fencing_token: int = Field(default=0, ge=0)
    expires_at: datetime | None = None
    claim_id: GoalReferenceId | None = None
    eligible_instance_ids: list[GoalReferenceId] = Field(default_factory=list)
    acquired_at: datetime | None = None

    def active(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        return bool(
            self.holder_instance_id and self.expires_at and self.expires_at > now
        )


class GoalWakeup(BaseModel):
    wake_at: datetime
    reason: str = Field(min_length=1)
    eligible_instance_ids: list[GoalReferenceId] = Field(default_factory=list)
    claimed_by_instance_id: GoalReferenceId | None = None
    claimed_at: datetime | None = None


class GoalAudit(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    auditor_principal: GoalReferenceId
    auditor_instance_id: GoalReferenceId | None = None
    verifier_service_id: GoalReferenceId | None = None
    independent: bool = True
    verdict: CriterionVerdict
    criterion_verdicts: dict[GoalReferenceId, CriterionVerdict]
    evidence_ids: list[GoalReferenceId] = Field(default_factory=list)
    explanation: str = Field(min_length=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CreateWorkPackageAction(BaseModel):
    kind: Literal["create_work_package"] = "create_work_package"
    title: str = Field(min_length=1, max_length=300)
    objective: str = Field(min_length=1, max_length=16_000)
    criterion_ids: list[GoalReferenceId] = Field(min_length=1)
    depends_on: list[GoalReferenceId] = Field(default_factory=list)
    role: GoalActorRole = GoalActorRole.EXECUTOR
    card_id: GoalReferenceId | None = None
    preferred_instance_id: GoalReferenceId | None = None
    preferred_capabilities: list[str] = Field(default_factory=list)
    max_attempts: int = Field(default=3, ge=1, le=20)
    dispatch_when_ready: bool = True


class DispatchWorkPackageAction(BaseModel):
    kind: Literal["dispatch_work_package"] = "dispatch_work_package"
    work_package_id: GoalReferenceId
    target_instance_id: GoalReferenceId | None = None
    placement_policy: str | None = "best_match"
    group_id: GoalReferenceId | None = None
    message: str = ""
    provider: GoalReferenceId | None = None
    model_id: GoalReferenceId | None = None
    mode_id: GoalReferenceId | None = None
    priority: int = Field(default=0, ge=-10, le=10)

    @model_validator(mode="after")
    def validate_target(self) -> DispatchWorkPackageAction:
        if bool(self.target_instance_id) == bool(self.placement_policy):
            raise ValueError(
                "dispatch proposal requires exactly one target instance or placement policy"
            )
        return self


class GoalOperatorChoice(BaseModel):
    id: GoalReferenceId
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
    criterion_verdicts: dict[GoalReferenceId, CriterionVerdict] = Field(
        default_factory=dict
    )


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
    decided_by_instance_id: GoalReferenceId
    decided_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalProposal(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    proposer_principal: GoalReferenceId
    proposer_role: GoalActorRole
    action: GoalProposalAction
    rationale: str = Field(min_length=1, max_length=8000)
    expected_goal_version: int = Field(ge=1)
    policy_revision: int = Field(ge=1)
    status: ProposalStatus = ProposalStatus.PENDING
    authorization: GoalAuthorizationDecision | None = None
    applied_event_id: GoalReferenceId | None = None
    error: str | None = Field(default=None, max_length=2000)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalProposalCreate(BaseModel):
    proposer_principal: GoalReferenceId
    proposer_role: GoalActorRole
    action: GoalProposalAction
    rationale: str = Field(min_length=1, max_length=8000)
    expected_goal_version: int = Field(ge=1)
    policy_revision: int = Field(ge=1)


class GoalDispatchAttempt(BaseModel):
    generation: int = Field(ge=1)
    proposal_id: GoalReferenceId
    idempotency_key: GoalReferenceId
    request_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    dispatch_payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: GoalDispatchAttemptState = GoalDispatchAttemptState.STAGED
    reservation_id: GoalReferenceId | None = None
    dispatch_id: GoalReferenceId | None = None
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
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    proposal_id: GoalReferenceId
    title: str
    objective: str
    criterion_ids: list[GoalReferenceId] = Field(min_length=1)
    depends_on: list[GoalReferenceId] = Field(default_factory=list)
    role: GoalActorRole = GoalActorRole.EXECUTOR
    state: WorkPackageState = WorkPackageState.PLANNED
    card_id: GoalReferenceId | None = None
    preferred_instance_id: GoalReferenceId | None = None
    preferred_capabilities: list[str] = Field(default_factory=list)
    dispatch_when_ready: bool = True
    dispatch_ids: list[GoalReferenceId] = Field(default_factory=list)
    session_id: GoalReferenceId | None = None
    replacement_session_ids: list[GoalReferenceId] = Field(default_factory=list)
    executor_service_id: GoalReferenceId | None = None
    verifier_service_id: GoalReferenceId | None = None
    action_reservation_id: GoalReferenceId | None = None
    dispatch_attempt: GoalDispatchAttempt | None = None
    dispatch_admission_receipt_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    fleet_lifecycle_owned: bool = False
    materialization_envelope: GoalMaterializationEnvelopeV1 | None = None
    materialization_receipt: GoalMaterializationReceiptV1 | None = None
    execution_identity: GoalExecutionIdentityV1 | None = None
    attempts: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=3, ge=1, le=20)
    last_progress_fingerprint: str | None = None
    last_progress_at: datetime | None = None
    no_progress_cycles: int = Field(default=0, ge=0)
    result_summary: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalOperatorInteraction(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    proposal_id: GoalReferenceId
    notification_id: GoalReferenceId
    state: GoalInteractionState = GoalInteractionState.PENDING
    response_summary: str = ""
    response_principal: GoalReferenceId | None = None
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
    controller_session_id: GoalReferenceId | None = None
    replacement_session_ids: list[GoalReferenceId] = Field(default_factory=list)


class Goal(BaseModel):
    id: GoalReferenceId = Field(default_factory=lambda: str(uuid4()))
    realm_id: GoalReferenceId = "default"
    project_id: GoalReferenceId | None = None
    parent_goal_id: GoalReferenceId | None = None
    owner_principal: GoalReferenceId = "user:local"
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
    linked_card_ids: list[GoalReferenceId] = Field(default_factory=list)
    linked_dispatch_ids: list[GoalReferenceId] = Field(default_factory=list)
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
                if attempt.state == GoalDispatchAttemptState.ADMITTED and (
                    package.action_reservation_id != attempt.reservation_id
                    or package.dispatch_admission_receipt_digest
                    != attempt.admission_receipt_digest
                    or attempt.dispatch_id not in package.dispatch_ids
                    or not package.fleet_lifecycle_owned
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
    realm_id: GoalReferenceId = "default"
    project_id: GoalReferenceId | None = None
    parent_goal_id: GoalReferenceId | None = None
    owner_principal: GoalReferenceId = "user:local"
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
    actor_principal: GoalReferenceId
    authority_instance_id: GoalReferenceId
    idempotency_key: GoalReferenceId
    expected_version: int = Field(ge=0)
    policy_revision: int = Field(ge=1)
    fencing_token: int | None = Field(default=None, ge=1)


class GoalTransition(BaseModel):
    state: GoalState
    reason: str = Field(min_length=1)
    progress_summary: str | None = None


class GoalEvidenceCreate(BaseModel):
    evidence: GoalEvidence
    criterion_verdicts: dict[GoalReferenceId, CriterionVerdict] = Field(
        default_factory=dict
    )


class GoalAuditCreate(BaseModel):
    auditor_principal: GoalReferenceId | None = Field(
        default=None,
        description=(
            "Deprecated identity assertion. The authenticated mutation principal is "
            "authoritative."
        ),
    )
    independent: bool = True
    criterion_verdicts: dict[GoalReferenceId, CriterionVerdict]
    evidence_ids: list[GoalReferenceId] = Field(default_factory=list)
    explanation: str = Field(min_length=1)


class GoalSupervisionCheckpoint(BaseModel):
    criteria: list[GoalCriterion]
    evidence: list[GoalEvidence]
    proposals: list[GoalProposal]
    work_packages: list[GoalWorkPackage]
    operator_interactions: list[GoalOperatorInteraction]
    supervision: GoalSupervision
    linked_card_ids: list[GoalReferenceId]
    linked_dispatch_ids: list[GoalReferenceId]
    assumptions: list[str]
    risks: list[str]
    strategy_revision: int = Field(ge=1)
    state: GoalState
    progress_summary: str = ""
    reason: str = Field(min_length=1)


class GoalEventRecord(BaseModel):
    id: GoalReferenceId
    goal_id: GoalReferenceId
    event_type: str
    actor_principal: GoalReferenceId
    authority_instance_id: GoalReferenceId
    policy_revision: int
    idempotency_key: GoalReferenceId
    version: int
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
