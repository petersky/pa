from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class CollaborationMode(StrEnum):
    DEFAULT = "default"
    PLAN = "plan"


class PolicyStrategy(StrEnum):
    ALWAYS_DEFAULT = "always_default"
    ALWAYS_PLAN = "always_plan_first"
    AUTOMATIC = "automatic"
    CONDITIONAL = "conditional"


class PlanFallback(StrEnum):
    DEFAULT = "default"
    CANCEL = "cancel"
    ESCALATE = "escalate"


class PolicyScope(StrEnum):
    FLEET = "fleet"
    REALM = "realm"
    PROJECT = "project"
    INSTANCE = "instance"
    PROVIDER = "provider"


class PlanLifecycle(BaseModel):
    max_turns: int = Field(default=3, ge=1, le=20)
    expires_minutes: int = Field(default=60, ge=1, le=10_080)
    max_questions: int = Field(default=5, ge=0, le=50)
    require_user_approval: bool = True
    unattended_auto_approve: bool = False
    unavailable_user_fallback: PlanFallback = PlanFallback.ESCALATE


class CollaborationPolicy(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    scope_type: PolicyScope
    scope_id: str = "default"
    provider: str | None = None
    strategy: PolicyStrategy = PolicyStrategy.ALWAYS_DEFAULT
    mandatory_mode: CollaborationMode | None = None
    allowed_modes: list[CollaborationMode] = Field(
        default_factory=lambda: [CollaborationMode.DEFAULT, CollaborationMode.PLAN]
    )
    allow_agent_transitions: bool = True
    allowed_transitions: list[str] = Field(
        default_factory=lambda: ["default:plan", "plan:default"]
    )
    plan_first_card_kinds: list[str] = Field(default_factory=list)
    plan_first_tags: list[str] = Field(default_factory=list)
    plan_first_capabilities: list[str] = Field(default_factory=list)
    plan_first_intents: list[str] = Field(default_factory=list)
    automatic_risk_levels: list[str] = Field(default_factory=lambda: ["high"])
    automatic_on_ambiguity: bool = True
    lifecycle: PlanLifecycle = Field(default_factory=PlanLifecycle)
    version: int = Field(default=1, ge=1)
    enabled: bool = True
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("allowed_modes")
    @classmethod
    def unique_modes(cls, value: list[CollaborationMode]) -> list[CollaborationMode]:
        result = list(dict.fromkeys(value))
        if not result:
            raise ValueError("allowed_modes cannot be empty")
        return result


class PolicyInput(BaseModel):
    realm_id: str = "default"
    project_id: str | None = None
    instance_id: str
    provider: str
    card_id: str | None = None
    card_kind: str | None = None
    card_tags: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)
    dispatch_intent: str = "manual"
    risk: str = "low"
    ambiguous: bool = False
    unattended: bool = False
    user_preference: CollaborationMode | None = None
    dispatch_override: CollaborationMode | None = None
    supported_modes: list[CollaborationMode] = Field(default_factory=list)


class PolicyDecision(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    effective_mode: CollaborationMode
    requested_mode: CollaborationMode | None = None
    source: str
    source_policy_id: str | None = None
    source_policy_version: int | None = None
    mandatory: bool = False
    rationale: str
    inputs: dict[str, Any] = Field(default_factory=dict)
    lifecycle: PlanLifecycle = Field(default_factory=PlanLifecycle)
    decided_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TransitionStatus(StrEnum):
    APPROVED_APPLIED = "approved_applied"
    APPROVED_PENDING = "approved_pending_next_turn"
    REJECTED = "rejected"
    UNSUPPORTED = "unsupported"
    STALE = "stale"
    FAILED = "failed"


class ModeTransitionRequest(BaseModel):
    requested_mode: CollaborationMode
    purpose: str = Field(min_length=3, max_length=2_000)
    intended_next_action: str = Field(min_length=3, max_length=2_000)
    session_id: str
    dispatch_id: str | None = None
    card_id: str | None = None
    authority_instance_id: str | None = None
    authority_version: str | None = None
    idempotency_key: str = Field(min_length=1, max_length=200)
    actor: str = "agent"

    @field_validator(
        "session_id", "dispatch_id", "card_id", "authority_instance_id", mode="before"
    )
    @classmethod
    def strip_ids(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value


class ModeTransitionResult(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    status: TransitionStatus
    requested_mode: CollaborationMode
    effective_mode: CollaborationMode
    reason: str
    pending: bool = False
    duplicate: bool = False
    policy_decision_id: str | None = None
    authority_version: str | None = None
    applied_at: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CommandOrigin(StrEnum):
    PROVIDER = "provider"
    PA = "pa"


class CommandAvailability(StrEnum):
    AVAILABLE = "available"
    DISABLED = "disabled"


class SessionCommand(BaseModel):
    id: str
    name: str
    description: str = ""
    origin: CommandOrigin
    provider: str | None = None
    input_hint: str | None = None
    input_required: bool = False
    arguments: list[dict[str, Any]] = Field(default_factory=list)
    action: dict[str, Any] | None = None
    availability: CommandAvailability = CommandAvailability.AVAILABLE
    disabled_reason: str | None = None
    capability_requirements: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        normalized = value.strip().removeprefix("/")
        if not normalized or any(char.isspace() for char in normalized):
            raise ValueError("command name must be non-empty and contain no whitespace")
        return normalized


class CommandCatalog(BaseModel):
    session_id: str
    provider: str
    generation: int = Field(ge=1)
    connection_generation: int = Field(default=1, ge=1)
    commands: list[SessionCommand] = Field(default_factory=list)
    digest: str
    active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ExecuteCommandRequest(BaseModel):
    session_id: str
    name: str
    arguments: str | dict[str, Any] | None = None
    catalog_generation: int | None = None
    dispatch_id: str | None = None
    card_id: str | None = None
    authority_instance_id: str | None = None
    authority_version: str | None = None
    idempotency_key: str = Field(min_length=1, max_length=200)
    actor: str = "user"


class CommandResultStatus(StrEnum):
    APPLIED = "applied"
    FORWARDED = "forwarded"
    REJECTED = "rejected"
    UNSUPPORTED = "unsupported"
    STALE = "stale"
    FAILED = "failed"


class CommandResult(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str
    command_name: str
    status: CommandResultStatus
    reason: str
    effective_configuration: dict[str, Any] = Field(default_factory=dict)
    mode_result: ModeTransitionResult | None = None
    duplicate: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PolicyWrite(BaseModel):
    policy: CollaborationPolicy
    expected_version: int | None = None


class CollaborationState(BaseModel):
    session_id: str
    supported_modes: list[CollaborationMode]
    current_mode: CollaborationMode
    pending_transition: ModeTransitionResult | None = None
    effective_policy: CollaborationPolicy | None = None
    policy_decision: PolicyDecision | None = None
    command_catalog_generation: int | None = None
    provider: str
    execution_mode_id: str | None = None

    @model_validator(mode="after")
    def modes_are_consistent(self) -> CollaborationState:
        if self.supported_modes and self.current_mode not in self.supported_modes:
            # Recovered legacy sessions may predate collaboration capability.
            self.supported_modes = list(
                dict.fromkeys([self.current_mode, *self.supported_modes])
            )
        return self
