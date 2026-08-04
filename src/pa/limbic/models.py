from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _normalized_name(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    if not value:
        raise ValueError("canonical names must contain a letter or number")
    return value


def _minimize_secrets(value: Any) -> Any:
    markers = ("secret", "token", "password", "credential", "authorization")
    if isinstance(value, dict):
        return {
            key: _minimize_secrets(item)
            for key, item in value.items()
            if not any(marker in str(key).lower() for marker in markers)
        }
    if isinstance(value, list):
        return [_minimize_secrets(item) for item in value]
    return value


class Sensitivity(StrEnum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


SENSITIVITY_RANK = {
    Sensitivity.PUBLIC: 0,
    Sensitivity.INTERNAL: 1,
    Sensitivity.CONFIDENTIAL: 2,
    Sensitivity.RESTRICTED: 3,
}


class SignalSource(StrEnum):
    SYSTEM = "system"
    OPERATOR = "operator"
    AGENT = "agent"
    CHANNEL = "channel"
    INTEGRATION = "integration"
    TIMER = "timer"


class SignalEnvelope(BaseModel):
    """Channel-neutral, immutable input to appraisal."""

    schema_version: int = Field(default=1, ge=1)
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str = "default"
    source: SignalSource
    event_class: str = Field(min_length=1)
    subject_type: str = Field(min_length=1)
    subject_id: str = Field(min_length=1)
    actor_principal: str = "system:unknown"
    goal_refs: list[str] = Field(default_factory=list)
    channel: str | None = None
    correlation_id: str | None = None
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    received_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    sensitivity: Sensitivity = Sensitivity.INTERNAL
    trusted_control: bool = False
    content: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    content_hash: str = ""
    dedupe_key: str = ""

    @model_validator(mode="after")
    def canonicalize(self) -> SignalEnvelope:
        self.event_class = _normalized_name(self.event_class)
        self.subject_type = _normalized_name(self.subject_type)
        self.goal_refs = sorted(set(self.goal_refs))
        if not self.content_hash:
            self.content_hash = _canonical_hash(
                {"content": self.content, "metadata": self.metadata}
            )
        if not self.dedupe_key:
            self.dedupe_key = _canonical_hash(
                {
                    "realm": self.realm_id,
                    "source": self.source,
                    "class": self.event_class,
                    "subject": [self.subject_type, self.subject_id],
                    "correlation": self.correlation_id,
                    "content_hash": self.content_hash,
                }
            )
        return self

    def appraisal_features(self) -> dict[str, Any]:
        """Return minimized, secret-resistant features for an optional model."""

        include_content = self.sensitivity in {
            Sensitivity.PUBLIC,
            Sensitivity.INTERNAL,
        }
        return {
            "schema_version": self.schema_version,
            "source": self.source.value,
            "event_class": self.event_class,
            "subject_type": self.subject_type,
            "goal_count": len(self.goal_refs),
            "trusted_control": self.trusted_control,
            "sensitivity": self.sensitivity.value,
            "content": self.content[:1000] if include_content else "[redacted]",
            "metadata": _minimize_secrets(self.metadata),
            "content_hash": self.content_hash,
        }


class Urgency(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


URGENCY_RANK = {
    Urgency.LOW: 0,
    Urgency.NORMAL: 1,
    Urgency.HIGH: 2,
    Urgency.CRITICAL: 3,
}


class Valence(StrEnum):
    ROUTINE = "routine"
    OPPORTUNITY = "opportunity"
    RISK = "risk"


class Novelty(StrEnum):
    DUPLICATE = "duplicate"
    EXPECTED = "expected"
    NEW = "new"
    UNKNOWN = "unknown"


class ProcessingPath(StrEnum):
    FAST = "fast_path"
    SLOW = "slow_deliberation"
    QUEUE = "durable_queue"
    BYPASS = "deterministic_bypass"


class Appraisal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    signal_id: str
    salience: float = Field(ge=0, le=1)
    urgency: Urgency
    valence: Valence
    novelty: Novelty
    confidence: float = Field(ge=0, le=1)
    goal_refs: list[str] = Field(default_factory=list)
    intent: str
    risk_classes: list[str] = Field(default_factory=list)
    recommended_path: ProcessingPath
    wake: list[str] = Field(default_factory=list)
    dedupe_key: str
    reason: str
    evaluator: str
    evaluator_version: str
    deterministic_bypass: str | None = None
    model_used: bool = False
    shadow: Appraisal | None = None
    input_features: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class RouteDecision(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    signal_id: str
    appraisal_id: str
    path: ProcessingPath
    preliminary: bool
    allowed_actions: list[str]
    wake: list[str] = Field(default_factory=list)
    policy_version: str = "limbic-routing-v1"
    reason: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class AppraisalResult(BaseModel):
    signal: SignalEnvelope
    appraisal: Appraisal
    route: RouteDecision
    deduplicated: bool = False


class ReplayCase(BaseModel):
    name: str
    signal: SignalEnvelope
    expected_path: ProcessingPath
    expected_bypass: str | None = None
    expected_urgency: Urgency | None = None


class ReplayCaseResult(BaseModel):
    name: str
    expected_path: ProcessingPath
    actual_path: ProcessingPath
    matched: bool
    reasons: list[str] = Field(default_factory=list)


class ReplayReport(BaseModel):
    evaluator_version: str
    total: int
    matched: int
    accuracy: float
    missed_escalations: int
    false_escalations: int
    cases: list[ReplayCaseResult]


class MemoryTier(StrEnum):
    SENSORY = "sensory"
    WORKING = "working"
    EPISODIC = "episodic"
    SEMANTIC = "semantic"
    PROCEDURAL = "procedural"


class MemoryProvenance(BaseModel):
    source_type: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    actor_principal: str = Field(min_length=1)
    observed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    content_hash: str = ""
    transformation: str = "original"
    verified: bool = False


class MemoryRecord(BaseModel):
    schema_version: int = Field(default=1, ge=1)
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str = "default"
    tier: MemoryTier
    subject: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    value: Any
    summary: str = Field(min_length=1)
    goal_id: str | None = None
    owner_principal: str = "user:local"
    allowed_principals: list[str] = Field(default_factory=list)
    sensitivity: Sensitivity = Sensitivity.INTERNAL
    provenance: MemoryProvenance
    confidence: float = Field(default=1.0, ge=0, le=1)
    contradiction: bool = False
    contradiction_ids: list[str] = Field(default_factory=list)
    supersedes: str | None = None
    superseded_by: str | None = None
    expires_at: datetime | None = None
    version: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def apply_retention_and_hash(self) -> MemoryRecord:
        self.subject = self.subject.strip()
        self.predicate = _normalized_name(self.predicate)
        self.allowed_principals = sorted(set(self.allowed_principals))
        self.contradiction_ids = sorted(set(self.contradiction_ids))
        if self.expires_at is None and self.tier == MemoryTier.SENSORY:
            self.expires_at = self.created_at + timedelta(hours=1)
        if self.expires_at is None and self.tier == MemoryTier.WORKING:
            self.expires_at = self.created_at + timedelta(hours=24)
        if not self.provenance.content_hash:
            self.provenance.content_hash = _canonical_hash(self.value)
        return self

    def active(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        return self.superseded_by is None and (
            self.expires_at is None or self.expires_at > now
        )


class MemoryMutationContext(BaseModel):
    actor_principal: str
    authority_instance_id: str
    idempotency_key: str = Field(min_length=1, max_length=200)


class MemoryQuery(BaseModel):
    realm_id: str = "default"
    requester_principal: str
    goal_ids: list[str] = Field(default_factory=list)
    tiers: list[MemoryTier] = Field(default_factory=lambda: list(MemoryTier))
    query: str = ""
    max_sensitivity: Sensitivity = Sensitivity.INTERNAL
    include_contradictions: bool = False
    include_superseded: bool = False
    include_expired: bool = False
    limit: int = Field(default=20, ge=1, le=200)


class RetrievedMemory(BaseModel):
    record: MemoryRecord
    relevance: float = Field(ge=0, le=1)
    instruction_trusted: bool = False
    retrieval_reason: str


class WorkingMemoryPacket(BaseModel):
    schema_version: int = 1
    realm_id: str
    requester_principal: str
    goal_ids: list[str]
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    retrieval_policy: str = "scoped-memory-v1"
    memories: list[RetrievedMemory]
