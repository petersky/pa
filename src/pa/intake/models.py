from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class Channel(StrEnum):
    WEB = "web"
    TELEGRAM = "telegram"
    DISCORD = "discord"


class IntakeDirection(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"


class IntakeKind(StrEnum):
    MESSAGE = "message"
    MESSAGE_EDIT = "message_edit"
    REACTION = "reaction"
    COMMAND = "command"
    RECEIPT = "receipt"


class Modality(StrEnum):
    TEXT = "text"
    IMAGE = "image"
    VOICE = "voice"
    AUDIO = "audio"
    VIDEO = "video"
    FILE = "file"
    REACTION = "reaction"


class IntakeVisibility(StrEnum):
    PRIVATE = "private"
    THREAD = "thread"
    GROUP = "group"
    CHANNEL = "channel"


class IdentityConfidence(StrEnum):
    UNVERIFIED = "unverified"
    PROVIDER_VERIFIED = "provider_verified"
    LINKED = "linked"


class Sensitivity(StrEnum):
    PUBLIC = "public"
    INTERNAL = "internal"
    SENSITIVE = "sensitive"
    RESTRICTED = "restricted"


class IntakeDisposition(StrEnum):
    ACCEPTED = "accepted"
    QUARANTINED = "quarantined"
    REJECTED = "rejected"
    REDACTED = "redacted"


class ArtifactState(StrEnum):
    REFERENCED = "referenced"
    STORED = "stored"
    QUARANTINED = "quarantined"
    REDACTED = "redacted"


class ReceiptState(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    DELIVERED = "delivered"
    READ = "read"
    FAILED = "failed"


class SenderIdentity(BaseModel):
    channel_user_id: str = Field(min_length=1, max_length=256)
    username: str | None = Field(default=None, max_length=256)
    display_name: str | None = Field(default=None, max_length=512)
    principal_id: str | None = Field(default=None, max_length=256)
    confidence: IdentityConfidence = IdentityConfidence.UNVERIFIED
    is_bot: bool = False


class ThreadContext(BaseModel):
    conversation_id: str = Field(min_length=1, max_length=256)
    thread_id: str | None = Field(default=None, max_length=256)
    parent_conversation_id: str | None = Field(default=None, max_length=256)
    reply_to_message_id: str | None = Field(default=None, max_length=256)


class ReplyCapabilities(BaseModel):
    can_reply: bool = True
    can_edit: bool = False
    can_react: bool = False
    can_report_progress: bool = False
    maximum_text_length: int = Field(default=4096, ge=1, le=100_000)


class RetentionPolicy(BaseModel):
    policy: Literal["ephemeral", "standard", "audit", "legal_hold"] = "standard"
    raw_expires_at: datetime | None = Field(
        default_factory=lambda: datetime.now(UTC) + timedelta(days=7)
    )
    canonical_expires_at: datetime | None = Field(
        default_factory=lambda: datetime.now(UTC) + timedelta(days=90)
    )
    redacted_at: datetime | None = None
    redaction_reason: str | None = Field(default=None, max_length=1000)

    @model_validator(mode="after")
    def validate_boundaries(self) -> RetentionPolicy:
        if self.policy == "legal_hold":
            self.raw_expires_at = None
            self.canonical_expires_at = None
        if (
            self.raw_expires_at
            and self.canonical_expires_at
            and self.raw_expires_at > self.canonical_expires_at
        ):
            raise ValueError("raw retention cannot exceed canonical retention")
        return self


class SecurityAssessment(BaseModel):
    authenticated: bool = False
    allowlisted: bool = False
    identity_linked: bool = False
    content_type_valid: bool = True
    size_valid: bool = True
    malware_scan: Literal["not_applicable", "clean", "suspicious", "pending"] = (
        "not_applicable"
    )
    prompt_injection_detected: bool = False
    untrusted_content: bool = True
    disposition: IntakeDisposition = IntakeDisposition.ACCEPTED
    reasons: list[str] = Field(default_factory=list, max_length=32)


class DerivedRepresentation(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    kind: Literal["transcript", "ocr", "description", "index", "embedding_ref"]
    text: str | None = Field(default=None, max_length=100_000)
    artifact_ref: str | None = Field(default=None, max_length=512)
    derived_from_artifact_id: str = Field(min_length=1, max_length=256)
    processor: str = Field(min_length=1, max_length=256)
    processor_version: str = Field(min_length=1, max_length=128)
    confidence: float | None = Field(default=None, ge=0, le=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def require_content(self) -> DerivedRepresentation:
        if not self.text and not self.artifact_ref:
            raise ValueError(
                "a derived representation requires text or an artifact_ref"
            )
        return self


class IntakeArtifact(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    modality: Modality
    provider_file_id: str | None = Field(default=None, max_length=1024)
    source_url: str | None = Field(default=None, max_length=4096)
    filename: str | None = Field(default=None, max_length=512)
    media_type: str | None = Field(default=None, max_length=256)
    size: int | None = Field(default=None, ge=0, le=10 * 1024 * 1024 * 1024)
    sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    blob_ref: str | None = Field(default=None, max_length=512)
    storage_instance_id: str | None = Field(default=None, max_length=256)
    state: ArtifactState = ArtifactState.REFERENCED
    quarantine_reason: str | None = Field(default=None, max_length=1000)
    duration_seconds: float | None = Field(default=None, ge=0, le=7 * 24 * 3600)
    width: int | None = Field(default=None, ge=1, le=100_000)
    height: int | None = Field(default=None, ge=1, le=100_000)
    representations: list[DerivedRepresentation] = Field(
        default_factory=list, max_length=32
    )

    @model_validator(mode="after")
    def validate_storage(self) -> IntakeArtifact:
        stored = bool(self.sha256 and self.blob_ref and self.size is not None)
        if self.state == ArtifactState.STORED and not stored:
            raise ValueError("stored artifacts require size, sha256, and blob_ref")
        return self


class DeliveryReceipt(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    state: ReceiptState = ReceiptState.PENDING
    provider_message_id: str | None = Field(default=None, max_length=256)
    provider_delivery_id: str | None = Field(default=None, max_length=256)
    detail: str | None = Field(default=None, max_length=1000)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class IntakeEnvelope(BaseModel):
    schema_version: Literal[1] = 1
    id: str = Field(default_factory=lambda: str(uuid4()))
    version: int = Field(default=1, ge=1)
    direction: IntakeDirection = IntakeDirection.INBOUND
    channel: Channel
    kind: IntakeKind = IntakeKind.MESSAGE
    channel_message_id: str = Field(min_length=1, max_length=256)
    correlation_id: str = Field(default_factory=lambda: str(uuid4()), max_length=256)
    in_reply_to_envelope_id: str | None = Field(default=None, max_length=256)
    sender: SenderIdentity
    thread: ThreadContext
    realm_id: str = Field(default="default", min_length=1, max_length=256)
    project_id: str | None = Field(default=None, max_length=256)
    goal_ids: list[str] = Field(default_factory=list, max_length=64)
    visibility: IntakeVisibility = IntakeVisibility.PRIVATE
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    received_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    locale: str | None = Field(default=None, max_length=64)
    text: str | None = Field(default=None, max_length=32_000)
    modalities: list[Modality] = Field(default_factory=list, max_length=16)
    artifacts: list[IntakeArtifact] = Field(default_factory=list, max_length=20)
    reaction: str | None = Field(default=None, max_length=256)
    reply_capabilities: ReplyCapabilities = Field(default_factory=ReplyCapabilities)
    sensitivity: Sensitivity = Sensitivity.INTERNAL
    security: SecurityAssessment = Field(default_factory=SecurityAssessment)
    retention: RetentionPolicy = Field(default_factory=RetentionPolicy)
    raw_payload_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    raw_blob_ref: str | None = Field(default=None, max_length=512)
    raw_storage_instance_id: str | None = Field(default=None, max_length=256)
    receipts: list[DeliveryReceipt] = Field(default_factory=list, max_length=64)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("modalities")
    @classmethod
    def unique_modalities(cls, value: list[Modality]) -> list[Modality]:
        return list(dict.fromkeys(value))

    @model_validator(mode="after")
    def validate_content(self) -> IntakeEnvelope:
        if self.kind in {
            IntakeKind.MESSAGE,
            IntakeKind.MESSAGE_EDIT,
            IntakeKind.COMMAND,
        }:
            if not self.text and not self.artifacts:
                raise ValueError("message intake requires text or an artifact")
        if self.kind == IntakeKind.REACTION and not self.reaction:
            raise ValueError("reaction intake requires a reaction value")
        if self.text and Modality.TEXT not in self.modalities:
            self.modalities.insert(0, Modality.TEXT)
        for artifact in self.artifacts:
            if artifact.modality not in self.modalities:
                self.modalities.append(artifact.modality)
        return self


class IntakeMutationContext(BaseModel):
    actor_principal: str = Field(min_length=1, max_length=256)
    authority_instance_id: str = Field(min_length=1, max_length=256)
    idempotency_key: str = Field(min_length=1, max_length=256)
    expected_version: int | None = Field(default=None, ge=1)


class WebIntakeCreate(BaseModel):
    message: str = Field(default="", max_length=32_000)
    channel_message_id: str | None = Field(default=None, max_length=256)
    conversation_id: str = Field(default="web", min_length=1, max_length=256)
    thread_id: str | None = Field(default=None, max_length=256)
    reply_to_message_id: str | None = Field(default=None, max_length=256)
    realm_id: str = Field(default="default", min_length=1, max_length=256)
    project_id: str | None = Field(default=None, max_length=256)
    goal_ids: list[str] = Field(default_factory=list, max_length=64)
    locale: str | None = Field(default=None, max_length=64)


class CorrelatedResponseCreate(BaseModel):
    text: str = Field(min_length=1, max_length=32_000)
    target_channel: Channel | None = None
    target_conversation_id: str | None = Field(default=None, max_length=256)
    target_thread_id: str | None = Field(default=None, max_length=256)
    reply_to_message_id: str | None = Field(default=None, max_length=256)


class ReceiptCreate(BaseModel):
    state: ReceiptState
    provider_message_id: str | None = Field(default=None, max_length=256)
    provider_delivery_id: str | None = Field(default=None, max_length=256)
    detail: str | None = Field(default=None, max_length=1000)


class RepresentationCreate(BaseModel):
    representation: DerivedRepresentation


class RedactionCreate(BaseModel):
    reason: str = Field(min_length=1, max_length=1000)
    redact_text: bool = True
    redact_artifacts: bool = True
    redact_identity: bool = False


class IdentityBinding(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    version: int = Field(default=1, ge=1)
    realm_id: str = Field(default="default", min_length=1, max_length=256)
    channel: Channel
    channel_user_id: str = Field(min_length=1, max_length=256)
    principal_id: str = Field(min_length=1, max_length=256)
    conversation_ids: list[str] = Field(default_factory=list, max_length=64)
    verification_method: Literal["one_time_code", "operator"] = "one_time_code"
    verified_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    revoked_at: datetime | None = None


class LinkChallenge(BaseModel):
    channel: Channel
    realm_id: str = "default"
    expires_in_seconds: int = Field(default=600, ge=60, le=3600)


class LinkChallengeResult(BaseModel):
    channel: Channel
    code: str
    expires_at: datetime


class LinkVerification(BaseModel):
    channel: Channel
    code: str = Field(min_length=6, max_length=64)
    channel_user_id: str = Field(min_length=1, max_length=256)
    conversation_id: str = Field(min_length=1, max_length=256)
