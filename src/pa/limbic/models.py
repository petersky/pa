from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator


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


def _normalized_content(value: str) -> str:
    return (
        unicodedata.normalize("NFKC", value).replace("\r\n", "\n").replace("\r", "\n")
    )


_SECRET_VALUE = re.compile(
    r"(?i)(?:bearer\s+[a-z0-9._~+/-]+|"
    r"(?:secret|token|password|credential|authorization)\s*[:=]\s*\S+)"
)


def _redact_text(value: str) -> str:
    return _SECRET_VALUE.sub("[redacted]", value)


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
    if isinstance(value, str):
        return _redact_text(value)
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


class ControlAuthority(StrEnum):
    UNTRUSTED = "untrusted"
    OPERATOR = "authenticated_operator"
    INTEGRATION = "authenticated_integration"
    AUTHORITY = "authenticated_authority"


class ControlEvent(StrEnum):
    OPERATOR_STOP = "operator_stop"
    SECURITY_REVOCATION = "security_revocation"
    DATA_INTEGRITY_ALARM = "data_integrity_alarm"
    LEASE_FENCING = "lease_fencing"
    HARD_RESOURCE_LIMIT = "hard_resource_limit"


class ControlTransport(StrEnum):
    UNTRUSTED = "untrusted"
    AUTHENTICATED_SESSION = "authenticated_session"
    VERIFIED_WEBHOOK = "verified_webhook"
    AUTHENTICATED_INSTANCE = "authenticated_instance"
    INTERNAL_AUTHORITY = "internal_authority"


_AUTHORITY_IDENTITY_FIELDS = {
    ControlAuthority.OPERATOR: "principal_id",
    ControlAuthority.INTEGRATION: "integration_id",
    ControlAuthority.AUTHORITY: "authority_instance_id",
}
_AUTHORITY_TRANSPORTS = {
    ControlAuthority.UNTRUSTED: frozenset({ControlTransport.UNTRUSTED}),
    ControlAuthority.OPERATOR: frozenset({ControlTransport.AUTHENTICATED_SESSION}),
    ControlAuthority.INTEGRATION: frozenset({ControlTransport.VERIFIED_WEBHOOK}),
    ControlAuthority.AUTHORITY: frozenset(
        {
            ControlTransport.AUTHENTICATED_INSTANCE,
            ControlTransport.INTERNAL_AUTHORITY,
        }
    ),
}
_AUTHORITY_SOURCES = {
    ControlAuthority.OPERATOR: SignalSource.OPERATOR,
    ControlAuthority.INTEGRATION: SignalSource.INTEGRATION,
    ControlAuthority.AUTHORITY: SignalSource.SYSTEM,
}


class VerifiedControlProvenance(BaseModel):
    """Validated control claim that requires a server issuer before it is trusted."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    authority: ControlAuthority = ControlAuthority.UNTRUSTED
    control_event: ControlEvent | None = None
    principal_id: str | None = None
    integration_id: str | None = None
    authority_instance_id: str | None = None
    transport: ControlTransport = ControlTransport.UNTRUSTED
    _issuer: object | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def validate_proof(self) -> VerifiedControlProvenance:
        identities = {
            "principal_id": self.principal_id,
            "integration_id": self.integration_id,
            "authority_instance_id": self.authority_instance_id,
        }
        supplied_fields = {field for field, value in identities.items() if value}
        if self.authority == ControlAuthority.UNTRUSTED:
            if self.control_event is not None or supplied_fields:
                raise ValueError(
                    "untrusted provenance cannot assert a control event or identity"
                )
            if self.transport not in _AUTHORITY_TRANSPORTS[self.authority]:
                raise ValueError("untrusted provenance requires untrusted transport")
            return self

        identity_field = _AUTHORITY_IDENTITY_FIELDS[self.authority]
        if self.control_event is None or supplied_fields != {identity_field}:
            raise ValueError(
                "verified control provenance requires only the authority identity and event"
            )
        if self.transport not in _AUTHORITY_TRANSPORTS[self.authority]:
            raise ValueError("control transport does not authenticate this authority")
        return self

    @property
    def trusted(self) -> bool:
        return self.authority != ControlAuthority.UNTRUSTED

    @classmethod
    def _issue(cls, issuer: object, **values: Any) -> VerifiedControlProvenance:
        provenance = cls(**values)
        provenance._issuer = issuer
        return provenance

    def _issued_by(self, issuer: object) -> bool:
        return self._issuer is issuer

    @property
    def expected_source(self) -> SignalSource | None:
        return _AUTHORITY_SOURCES.get(self.authority)

    def canonical_scope(self) -> str:
        identity_field = _AUTHORITY_IDENTITY_FIELDS.get(self.authority)
        identity = getattr(self, identity_field) if identity_field else "untrusted"
        fingerprint = _canonical_hash(
            {
                "authority": self.authority,
                "identity": identity,
                "transport": self.transport,
                "event": self.control_event,
            }
        )[:20]
        return f"{self.authority.value}:{fingerprint}"


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
    # Deprecated caller claim. LimbicService always overwrites it from verified
    # transport context before appraisal and persistence.
    trusted_control: bool = False
    control_provenance: str = "untrusted"
    content: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    content_hash: str = ""
    dedupe_key: str = ""
    hash_version: str = "input"

    @model_validator(mode="after")
    def canonicalize(self) -> SignalEnvelope:
        self.event_class = _normalized_name(self.event_class)
        self.subject_type = _normalized_name(self.subject_type)
        self.goal_refs = sorted(set(self.goal_refs))
        # Caller hash values are treated as input only. Server-canonical records
        # retain their original digest when their raw content is redacted for
        # persistence and replay.
        if self.hash_version != "limbic-v2" or not (
            self.content_hash and self.dedupe_key
        ):
            self._set_server_hashes(self.control_provenance)
        return self

    def canonicalized_for(
        self, provenance: VerifiedControlProvenance | None = None
    ) -> SignalEnvelope:
        """Return a server-canonical copy; caller hashes and trust flags are ignored."""

        verified = provenance or VerifiedControlProvenance()
        canonical = self.model_copy(
            deep=True,
            update={
                "trusted_control": verified.trusted,
                "control_provenance": verified.canonical_scope(),
                "content_hash": "",
                "dedupe_key": "",
                "hash_version": "limbic-v2",
            },
        )
        canonical._set_server_hashes(verified.canonical_scope())
        return canonical

    def _set_server_hashes(self, provenance_scope: str) -> None:
        self.content_hash = _canonical_hash(
            {
                "content": _normalized_content(self.content),
                "metadata": self.metadata,
                "trusted_provenance": provenance_scope,
            }
        )
        self.dedupe_key = _canonical_hash(
            {
                "realm": self.realm_id,
                "source": self.source,
                "class": self.event_class,
                "subject": [self.subject_type, self.subject_id],
                "correlation": self.correlation_id,
                "content_hash": self.content_hash,
                "trusted_provenance": provenance_scope,
            }
        )

    def appraisal_features(self) -> dict[str, Any]:
        """Return content-free, secret-resistant features safe for durable audit."""

        safe_metadata = {
            key: value
            for key, value in self.metadata.items()
            if key in {
                "deep_review",
                "failure_count",
                "retrieval_hits",
                "promotion_candidate",
            }
            and isinstance(value, (bool, int, float))
        }
        return {
            "schema_version": self.schema_version,
            "source": self.source.value,
            "event_class": self.event_class,
            "subject_type": self.subject_type,
            "goal_count": len(self.goal_refs),
            "trusted_control": self.trusted_control,
            "control_provenance": self.control_provenance,
            "sensitivity": self.sensitivity.value,
            "content": "[redacted]",
            "metadata": safe_metadata,
            "content_hash": self.content_hash,
        }

    def provider_features(self) -> dict[str, Any]:
        """Return minimized features for the optional, untrusted model provider."""

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
            "sensitivity": self.sensitivity.value,
            "content": (
                _redact_text(_normalized_content(self.content)[:1000])
                if include_content
                else "[redacted]"
            ),
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


class AppraisalDiagnostic(BaseModel):
    code: str
    category: str
    redacted: bool = True


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
    diagnostics: list[AppraisalDiagnostic] = Field(default_factory=list)
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
    duration_ms: float = Field(default=0, ge=0)
    shadow_mode: bool = False
    retrieval_hits: int = Field(default=0, ge=0)
    usefulness_score: float | None = Field(default=None, ge=0, le=1)


class ReplayCase(BaseModel):
    name: str
    signal: SignalEnvelope
    expected_path: ProcessingPath
    expected_bypass: str | None = None
    expected_urgency: Urgency | None = None


class ReplayCaseResult(BaseModel):
    name: str
    expected_path: ProcessingPath | None = None
    actual_path: ProcessingPath | None = None
    expected_bypass: str | None = None
    actual_bypass: str | None = None
    expected_urgency: Urgency | None = None
    actual_urgency: Urgency | None = None
    matched: bool
    escalation_outcome: str | None = None
    reasons: list[str] = Field(default_factory=list)


class ReplayReport(BaseModel):
    evaluator_version: str
    status: str
    total: int
    matched: int
    accuracy: float | None
    true_positives: int
    true_negatives: int
    false_positives: int
    false_negatives: int
    missed_escalations: int
    false_escalations: int
    invalid_cases: int = 0
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
