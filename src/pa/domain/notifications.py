"""Durable fleet notification and correlated user-interaction contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_INTERACTION_BYTES = 128 * 1024
MAX_NOTIFICATION_BYTES = 256 * 1024
MAX_RESPONSE_BYTES = 64 * 1024


class NotificationType(StrEnum):
    INTERACTION = "interaction"
    DISPATCH_FAILURE = "dispatch_failure"
    SYNC_CONFLICT = "sync_conflict"
    PR_EVENT = "pr_event"
    CI_EVENT = "ci_event"
    REVIEW_EVENT = "review_event"
    SECURITY = "security"
    UPGRADE = "upgrade"
    SERVICE_HEALTH = "service_health"
    GENERAL = "general"


class NotificationPriority(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class NotificationSeverity(StrEnum):
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class NotificationVisibility(StrEnum):
    PRINCIPAL = "principal"
    REALM = "realm"


class InteractionKind(StrEnum):
    ACP_PERMISSION = "acp_permission"
    ACP_ELICITATION = "acp_elicitation"
    MCP_OPERATOR_INPUT = "mcp_operator_input"
    POST_TURN_OPERATOR_INPUT = "post_turn_operator_input"
    FINAL_OUTPUT_FALLBACK = "final_output_fallback"
    APPROVAL = "approval"
    CHOICE = "choice"
    FREEFORM = "freeform"
    STRUCTURED = "structured"


class InteractionState(StrEnum):
    OUTSTANDING = "outstanding"
    ANSWERED = "answered"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    SUPERSEDED = "superseded"
    DELIVERY_PENDING = "delivery_pending"
    DELIVERED = "delivered"
    FAILED = "failed"


TERMINAL_INTERACTION_STATES = {
    InteractionState.CANCELLED,
    InteractionState.EXPIRED,
    InteractionState.SUPERSEDED,
    InteractionState.DELIVERED,
}


class DeliveryState(StrEnum):
    LOCAL = "local"
    REMOTE = "remote"
    PENDING = "pending"
    DELIVERED = "delivered"
    UNREACHABLE = "unreachable"
    FAILED = "failed"


class InteractionChoice(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=200)
    label: str = Field(min_length=1, max_length=300)
    description: str | None = Field(default=None, max_length=1000)
    value: Any = None


class NotificationAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=100)
    kind: Literal["respond", "acknowledge", "resolve", "navigate"]
    label: str = Field(min_length=1, max_length=200)
    href: str | None = Field(default=None, max_length=2000)
    method: str | None = Field(default=None, max_length=10)
    input_schema: dict[str, Any] | None = None
    enabled: bool = True


class InteractionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(default_factory=lambda: str(uuid4()))
    kind: InteractionKind
    state: InteractionState = InteractionState.OUTSTANDING
    prompt: str = Field(min_length=1, max_length=8000)
    response_schema: dict[str, Any] | None = None
    choices: list[InteractionChoice] = Field(default_factory=list, max_length=100)
    allow_freeform: bool = False
    allow_cancel: bool = True
    sensitive: bool = False
    protocol_method: str | None = Field(default=None, max_length=200)
    protocol_request_id: str | None = Field(default=None, max_length=300)
    continuation_mode: Literal["protocol", "prompt", "none"] = "protocol"
    deadline: datetime | None = None
    responded_at: datetime | None = None
    response_principal: str | None = None
    response: Any = None
    response_summary: str | None = Field(default=None, max_length=1000)
    delivery_attempts: int = Field(default=0, ge=0)
    delivered_at: datetime | None = None
    delivery_error: str | None = Field(default=None, max_length=1000)

    @model_validator(mode="after")
    def validate_input_contract(self) -> InteractionRequest:
        if (
            not self.choices
            and not self.allow_freeform
            and not self.response_schema
            and self.kind
            not in {InteractionKind.APPROVAL, InteractionKind.ACP_PERMISSION}
        ):
            raise ValueError(
                "interaction must accept a choice, freeform, or schema response"
            )
        if len(self.model_dump_json().encode()) > MAX_INTERACTION_BYTES:
            raise ValueError("interaction request exceeds 128 KB")
        return self


class Notification(BaseModel):
    """One bounded, sync-safe notification projection.

    ``version`` is authority-monotonic and is the deterministic winner for
    concurrent sync updates. ``deduplication_key`` identifies the logical notice
    across retries while ``idempotency_keys`` fences repeated mutations.
    """

    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: str(uuid4()))
    version: int = Field(default=1, ge=1)
    realm_id: str = Field(default="default", min_length=1, max_length=200)
    visibility: NotificationVisibility = NotificationVisibility.REALM
    principal_id: str | None = Field(default=None, max_length=300)
    type: NotificationType = NotificationType.GENERAL
    severity: NotificationSeverity = NotificationSeverity.INFO
    priority: NotificationPriority = NotificationPriority.NORMAL
    title: str = Field(min_length=1, max_length=300)
    body: str = Field(default="", max_length=16000)
    summary: str = Field(default="", max_length=1000)
    source_instance_id: str | None = Field(default=None, max_length=200)
    source_instance_name: str | None = Field(default=None, max_length=300)
    card_id: str | None = Field(default=None, max_length=200)
    session_id: str | None = Field(default=None, max_length=200)
    dispatch_id: str | None = Field(default=None, max_length=200)
    project_id: str | None = Field(default=None, max_length=200)
    pr_number: int | None = Field(default=None, gt=0)
    watch_id: str | None = Field(default=None, max_length=200)
    source_url: str | None = Field(default=None, max_length=2000)
    destination_url: str | None = Field(default=None, max_length=2000)
    owner_instance_id: str | None = Field(default=None, max_length=200)
    owner_url: str | None = Field(default=None, max_length=2000)
    distributable: bool = True
    capability: str | None = Field(default=None, max_length=300)
    actions: list[NotificationAction] = Field(default_factory=list, max_length=20)
    interaction: InteractionRequest | None = None
    deduplication_key: str | None = Field(default=None, max_length=500)
    coalesced_count: int = Field(default=1, ge=1)
    idempotency_keys: list[str] = Field(default_factory=list, max_length=128)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    read_at: datetime | None = None
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None
    expires_at: datetime | None = None
    delivery_state: DeliveryState = DeliveryState.LOCAL
    delivery_updated_at: datetime | None = None

    @model_validator(mode="after")
    def validate_visibility(self) -> Notification:
        if (
            self.visibility == NotificationVisibility.PRINCIPAL
            and not self.principal_id
        ):
            raise ValueError("principal-visible notifications require principal_id")
        if self.type == NotificationType.INTERACTION and not self.interaction:
            raise ValueError("interaction notifications require an interaction request")
        if len(self.model_dump_json().encode()) > MAX_NOTIFICATION_BYTES:
            raise ValueError("notification exceeds 256 KB")
        return self

    @property
    def outstanding(self) -> bool:
        if self.resolved_at is not None:
            return False
        if self.expires_at and self.expires_at <= datetime.now(UTC):
            return False
        return bool(
            self.interaction
            and self.interaction.state
            in {
                InteractionState.OUTSTANDING,
                InteractionState.DELIVERY_PENDING,
                InteractionState.FAILED,
            }
        )

    def public_dict(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        interaction = data.get("interaction")
        if interaction and interaction.get("sensitive"):
            interaction["response"] = None
            if interaction.get("responded_at"):
                interaction["response_summary"] = "Sensitive response recorded"
        return data


class NotificationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    realm_id: str = "default"
    visibility: NotificationVisibility = NotificationVisibility.REALM
    principal_id: str | None = None
    type: NotificationType = NotificationType.GENERAL
    severity: NotificationSeverity = NotificationSeverity.INFO
    priority: NotificationPriority = NotificationPriority.NORMAL
    title: str = Field(min_length=1, max_length=300)
    body: str = Field(default="", max_length=16000)
    summary: str = Field(default="", max_length=1000)
    source_instance_id: str | None = None
    source_instance_name: str | None = None
    card_id: str | None = None
    session_id: str | None = None
    dispatch_id: str | None = None
    project_id: str | None = None
    pr_number: int | None = Field(default=None, gt=0)
    watch_id: str | None = None
    source_url: str | None = None
    destination_url: str | None = None
    owner_instance_id: str | None = None
    owner_url: str | None = None
    distributable: bool = True
    capability: str | None = None
    actions: list[NotificationAction] = Field(default_factory=list)
    interaction: InteractionRequest | None = None
    deduplication_key: str | None = None
    expires_at: datetime | None = None


class InteractionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1, max_length=300)
    choice_id: str | None = Field(default=None, max_length=200)
    value: Any = None
    fields: dict[str, Any] | None = None
    cancel: bool = False

    @model_validator(mode="after")
    def exactly_one_response_shape(self) -> InteractionResponse:
        supplied = sum(
            [
                self.choice_id is not None,
                self.value is not None,
                self.fields is not None,
                self.cancel,
            ]
        )
        if supplied != 1:
            raise ValueError(
                "provide exactly one of choice_id, value, fields, or cancel"
            )
        if len(self.model_dump_json().encode()) > MAX_RESPONSE_BYTES:
            raise ValueError("interaction response exceeds 64 KB")
        return self
