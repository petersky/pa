from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
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


class GoalCriterion(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    description: str = Field(min_length=1)
    verification_method: str = Field(min_length=1)
    evidence_requirement: str = Field(min_length=1)
    verdict: CriterionVerdict = CriterionVerdict.PENDING
    evidence_ids: list[str] = Field(default_factory=list)
    freshness_seconds: int | None = Field(default=None, ge=1)
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


class GoalBudget(BaseModel):
    max_cost_usd: float | None = Field(default=None, ge=0)
    max_tokens: int | None = Field(default=None, ge=1)
    max_dispatches: int | None = Field(default=None, ge=1)
    max_concurrency: int = Field(default=1, ge=1, le=256)
    deadline: datetime | None = None
    retry_limit: int = Field(default=3, ge=0)


class GoalPolicy(BaseModel):
    revision: int = Field(default=1, ge=1)
    autonomy_level: int = Field(default=1, ge=1, le=5)
    permitted_actions: list[str] = Field(default_factory=list)
    prohibited_actions: list[str] = Field(default_factory=list)
    repository_scope: list[str] = Field(default_factory=list)
    data_scope: list[str] = Field(default_factory=list)
    require_operator_for: list[str] = Field(default_factory=list)
    authored_by: str = "user:local"
    effective_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalLease(BaseModel):
    holder_instance_id: str | None = None
    fencing_token: int = Field(default=0, ge=0)
    expires_at: datetime | None = None

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
    independent: bool = True
    verdict: CriterionVerdict
    criterion_verdicts: dict[str, CriterionVerdict]
    evidence_ids: list[str] = Field(default_factory=list)
    explanation: str = Field(min_length=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


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
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_references(self) -> Goal:
        criterion_ids = [criterion.id for criterion in self.criteria]
        if len(criterion_ids) != len(set(criterion_ids)):
            raise ValueError("criterion ids must be unique")
        known = set(criterion_ids)
        evidence_ids: set[str] = set()
        for item in self.evidence:
            if item.id in evidence_ids:
                raise ValueError("evidence ids must be unique")
            evidence_ids.add(item.id)
            if unknown := set(item.criterion_ids) - known:
                raise ValueError(
                    f"evidence references unknown criteria: {sorted(unknown)}"
                )
        for criterion in self.criteria:
            if unknown := set(criterion.evidence_ids) - evidence_ids:
                raise ValueError(
                    f"criterion references unknown evidence: {sorted(unknown)}"
                )
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
    auditor_principal: str
    independent: bool = True
    criterion_verdicts: dict[str, CriterionVerdict]
    evidence_ids: list[str] = Field(default_factory=list)
    explanation: str = Field(min_length=1)


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
