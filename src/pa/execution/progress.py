"""Versioned, secret-safe progress reporting for durable fleet dispatches."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime

logger = logging.getLogger(__name__)

PROGRESS_SCHEMA_VERSION = 1
SUPPORTED_PROGRESS_VERSIONS = [PROGRESS_SCHEMA_VERSION]
MAX_PROGRESS_EVENTS = 64
MAX_PROGRESS_SEEN_KEYS = 512
MAX_PROGRESS_SUMMARY = 500
MAX_PROGRESS_DETAIL = 240
MAX_VALIDATION_COMMAND = 2_000
MAX_PROGRESS_VALIDATIONS = 20
MAX_PROGRESS_TOOL_DETAILS = 10
# Progress payload limits are encoded UTF-8 byte limits, not display limits.
MAX_PROGRESS_PAYLOAD_BYTES = 64_000
MAX_FINAL_REPORT_BYTES = 64_000
PROGRESS_HEARTBEAT_SECONDS = 15.0
PROGRESS_RETRY_SECONDS = 3.0
DERIVED_PROGRESS_COALESCE_SECONDS = 60.0
PROGRESS_LIVE_SECONDS = 45
PROGRESS_DELAYED_SECONDS = 120

_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(authorization|api[_-]?key|token|secret|password|passwd|cookie)"
    r"(\s*[:=]\s*)([^\s,;]+)"
)
_BEARER = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_KNOWN_TOKEN = re.compile(
    r"\b(?:gh[opusr]_[A-Za-z0-9_]{12,}|sk-[A-Za-z0-9_-]{12,}|"
    r"xox[baprs]-[A-Za-z0-9-]{12,})\b"
)
_URL_CREDENTIALS = re.compile(r"(https?://)([^/@\s:]+):([^/@\s]+)@")
_PRIVATE_KEY = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
    re.DOTALL,
)


def _sanitize_ingress_text(value: Any, *, limit: int) -> str:
    """Redact and normalize untrusted text before field constraints run."""
    text = str(value or "")
    text = _PRIVATE_KEY.sub("[REDACTED PRIVATE KEY]", text)
    text = _BEARER.sub("Bearer [REDACTED]", text)
    text = _KNOWN_TOKEN.sub("[REDACTED TOKEN]", text)
    text = _SECRET_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]", text
    )
    text = _URL_CREDENTIALS.sub(r"\1[REDACTED]@", text)
    text = " ".join(text.replace("\x00", "").split())
    return text[:limit].strip()


class ProgressPhase(StrEnum):
    INVESTIGATING = "investigating"
    PLANNING = "planning"
    IMPLEMENTING = "implementing"
    TESTING = "testing"
    OPENING_PR = "opening_pr"
    WAITING_CI = "waiting_ci"
    ADDRESSING_REVIEW = "addressing_review"
    MERGING = "merging"
    BLOCKED = "blocked"
    RETRYING = "retrying"
    TURN_ENDED = "turn_ended"
    # Legacy wire value accepted from older peers.
    COMPLETED = "completed"


class ProgressKind(StrEnum):
    CHECKPOINT = "checkpoint"
    HEARTBEAT = "heartbeat"
    FINAL = "final"


class ProgressValidationV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: str = Field(max_length=MAX_VALIDATION_COMMAND)
    status: Literal["queued", "running", "passed", "failed", "cancelled", "unknown"]
    summary: str | None = Field(default=None, max_length=MAX_PROGRESS_DETAIL)
    duration_ms: int | None = Field(default=None, ge=0)

    @field_validator("command", mode="before")
    @classmethod
    def sanitize_command(cls, value: Any) -> str:
        return _sanitize_ingress_text(value, limit=MAX_VALIDATION_COMMAND)

    @field_validator("summary", mode="before")
    @classmethod
    def sanitize_validation_summary(cls, value: Any) -> str | None:
        return (
            _sanitize_ingress_text(value, limit=MAX_PROGRESS_DETAIL)
            if value is not None
            else None
        )


class ProgressToolDetailV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(max_length=MAX_PROGRESS_DETAIL)
    kind: str | None = Field(default=None, max_length=80)
    status: str | None = Field(default=None, max_length=80)
    result: str | None = Field(default=None, max_length=MAX_PROGRESS_DETAIL)


class OperatorInputChoiceV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=200)
    label: str = Field(min_length=1, max_length=300)
    description: str | None = Field(default=None, max_length=1000)
    value: Any = None


class OperatorInputRequestV1(BaseModel):
    """Structured operator-input request accepted alongside legacy strings."""

    model_config = ConfigDict(extra="forbid")

    request_id: str | None = Field(default=None, max_length=300)
    prompt: str = Field(min_length=1, max_length=8000)
    response_schema: dict[str, Any] | None = None
    choices: list[OperatorInputChoiceV1] = Field(default_factory=list, max_length=100)
    allow_freeform: bool = True
    allow_cancel: bool = True
    sensitive: bool = False
    deadline: datetime | None = None


class CompletionReportV1(BaseModel):
    """Sanitized final evidence, separate from the card-disposition decision."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = PROGRESS_SCHEMA_VERSION
    outcome: str = Field(min_length=1, max_length=2000)
    branch: str | None = Field(default=None, max_length=240)
    commit_sha: str | None = Field(default=None, max_length=80)
    pr_url: str | None = Field(default=None, max_length=500)
    pr_number: int | None = Field(default=None, gt=0)
    validations: list[ProgressValidationV1] = Field(
        default_factory=list, max_length=MAX_PROGRESS_VALIDATIONS
    )
    ci_evidence: list[str] = Field(default_factory=list, max_length=40)
    review_evidence: list[str] = Field(default_factory=list, max_length=40)
    merge_commit_sha: str | None = Field(default=None, max_length=80)
    blockers: list[str] = Field(default_factory=list, max_length=20)
    card_disposition: dict[str, Any] | None = None
    resulting_lane: str | None = Field(default=None, max_length=40)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def payload_is_bounded(self) -> CompletionReportV1:
        if len(self.model_dump_json().encode()) > MAX_FINAL_REPORT_BYTES:
            raise ValueError("completion report exceeds the 64 KB payload limit")
        return self


class DispatchProgressEventV1(BaseModel):
    """One meaningful checkpoint with immutable fleet provenance."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = PROGRESS_SCHEMA_VERSION
    kind: Literal[ProgressKind.CHECKPOINT, ProgressKind.FINAL] = ProgressKind.CHECKPOINT
    card_id: str | None = None
    dispatch_id: str
    acp_session_id: str
    originating_instance_id: str
    authority_instance_id: str
    authority_version: str | None = None
    sequence: int = Field(ge=1)
    idempotency_key: str = Field(min_length=1, max_length=200)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_activity_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    phase: ProgressPhase
    summary: str = Field(min_length=1, max_length=MAX_PROGRESS_SUMMARY)
    branch: str | None = Field(default=None, max_length=240)
    commit_sha: str | None = Field(default=None, max_length=80)
    pr_url: str | None = Field(default=None, max_length=500)
    pr_number: int | None = Field(default=None, gt=0)
    changed_file_count: int | None = Field(default=None, ge=0)
    validations: list[ProgressValidationV1] = Field(
        default_factory=list, max_length=MAX_PROGRESS_VALIDATIONS
    )
    blockers: list[str] = Field(default_factory=list, max_length=20)
    retry_reason: str | None = Field(default=None, max_length=MAX_PROGRESS_DETAIL)
    operator_input: str | OperatorInputRequestV1 | None = None
    tool_details: list[ProgressToolDetailV1] = Field(
        default_factory=list, max_length=MAX_PROGRESS_TOOL_DETAILS
    )
    delivered_at: datetime | None = None
    delivery_attempts: int = Field(default=0, ge=0)
    delivery_error: str | None = Field(default=None, max_length=MAX_PROGRESS_DETAIL)

    @field_validator("summary", mode="before")
    @classmethod
    def sanitize_summary_ingress(cls, value: Any) -> str:
        return _sanitize_ingress_text(value, limit=MAX_PROGRESS_SUMMARY)

    @model_validator(mode="after")
    def payload_is_bounded(self) -> DispatchProgressEventV1:
        if len(self.model_dump_json().encode()) > MAX_PROGRESS_PAYLOAD_BYTES:
            raise ValueError("progress checkpoint exceeds the 64 KB payload limit")
        return self

    def transport_dict(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={
                "delivered_at",
                "delivery_attempts",
                "delivery_error",
            },
        )


class DispatchProgressHeartbeatV1(BaseModel):
    """Replaceable freshness signal; heartbeats never enter activity history."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = PROGRESS_SCHEMA_VERSION
    kind: Literal[ProgressKind.HEARTBEAT] = ProgressKind.HEARTBEAT
    card_id: str | None = None
    dispatch_id: str
    acp_session_id: str
    originating_instance_id: str
    authority_instance_id: str
    authority_version: str | None = None
    sequence: int = Field(ge=1)
    idempotency_key: str = Field(min_length=1, max_length=200)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    phase: ProgressPhase
    summary: str = Field(min_length=1, max_length=MAX_PROGRESS_SUMMARY)
    delivered_at: datetime | None = None
    delivery_attempts: int = Field(default=0, ge=0)
    delivery_error: str | None = Field(default=None, max_length=MAX_PROGRESS_DETAIL)

    @field_validator("summary", mode="before")
    @classmethod
    def sanitize_summary_ingress(cls, value: Any) -> str:
        return _sanitize_ingress_text(value, limit=MAX_PROGRESS_SUMMARY)

    @model_validator(mode="after")
    def payload_is_bounded(self) -> DispatchProgressHeartbeatV1:
        if len(self.model_dump_json().encode()) > MAX_PROGRESS_PAYLOAD_BYTES:
            raise ValueError("progress heartbeat exceeds the 64 KB payload limit")
        return self

    def transport_dict(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={
                "delivered_at",
                "delivery_attempts",
                "delivery_error",
            },
        )


class ExplicitProgressCheckpointV1(BaseModel):
    """Allowlisted explicit checkpoint accepted from a linked agent/operator."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = PROGRESS_SCHEMA_VERSION
    phase: ProgressPhase
    summary: str = Field(min_length=1, max_length=4000)
    branch: str | None = Field(default=None, max_length=1000)
    commit_sha: str | None = Field(default=None, max_length=200)
    pr_url: str | None = Field(default=None, max_length=1000)
    pr_number: int | None = Field(default=None, gt=0)
    changed_file_count: int | None = Field(default=None, ge=0)
    validations: list[ProgressValidationV1] = Field(
        default_factory=list, max_length=MAX_PROGRESS_VALIDATIONS
    )
    blockers: list[str] = Field(default_factory=list, max_length=20)
    retry_reason: str | None = Field(default=None, max_length=1000)
    operator_input: str | OperatorInputRequestV1 | None = None
    idempotency_key: str | None = Field(default=None, max_length=200)

    @field_validator("summary", mode="before")
    @classmethod
    def sanitize_summary_ingress(cls, value: Any) -> str:
        return _sanitize_ingress_text(value, limit=MAX_PROGRESS_SUMMARY)

    @model_validator(mode="after")
    def payload_is_bounded(self) -> ExplicitProgressCheckpointV1:
        if len(self.model_dump_json().encode()) > MAX_PROGRESS_PAYLOAD_BYTES:
            raise ValueError(
                "explicit progress checkpoint exceeds the 64 KB payload limit"
            )
        return self


class ProgressIngestResult(BaseModel):
    accepted: bool
    status: Literal["accepted", "duplicate", "coalesced", "late", "conflict"]
    dispatch_id: str
    sequence: int
    idempotency_key: str
    replay_of_status: Literal["accepted", "coalesced", "late", "conflict"] | None = None


def sanitize_text(value: Any, *, limit: int = MAX_PROGRESS_SUMMARY) -> str:
    """Remove common credentials and bound deliberate user-visible text."""
    return _sanitize_ingress_text(value, limit=limit)


def sanitize_operator_input(
    value: str | OperatorInputRequestV1,
) -> str | OperatorInputRequestV1:
    if isinstance(value, str):
        return sanitize_text(value, limit=1000)
    return value.model_copy(
        update={
            "prompt": sanitize_text(value.prompt, limit=8000),
            "choices": [
                item.model_copy(
                    update={
                        "label": sanitize_text(item.label, limit=300),
                        "description": (
                            sanitize_text(item.description, limit=1000)
                            if item.description
                            else None
                        ),
                    }
                )
                for item in value.choices
            ],
        }
    )


def sanitize_validation(value: ProgressValidationV1) -> ProgressValidationV1:
    return value.model_copy(
        update={
            "command": sanitize_text(value.command, limit=MAX_VALIDATION_COMMAND),
            "summary": (
                sanitize_text(value.summary, limit=MAX_PROGRESS_DETAIL)
                if value.summary
                else None
            ),
        }
    )


def _sanitize_json(value: Any, *, depth: int = 0) -> Any:
    """Bound arbitrary evidence while preserving its JSON-compatible structure."""
    if depth >= 5:
        return "[TRUNCATED]"
    if isinstance(value, str):
        return sanitize_text(value, limit=500)
    if isinstance(value, dict):
        return {
            sanitize_text(key, limit=80): _sanitize_json(item, depth=depth + 1)
            for key, item in list(value.items())[:50]
        }
    if isinstance(value, list | tuple):
        return [_sanitize_json(item, depth=depth + 1) for item in value[:50]]
    if value is None or isinstance(value, bool | int | float):
        return value
    return sanitize_text(value, limit=500)


def sanitize_completion_report(report: CompletionReportV1) -> CompletionReportV1:
    """Redact and bound completion evidence before durable storage or transport."""
    return report.model_copy(
        update={
            "outcome": sanitize_text(report.outcome, limit=2000),
            "branch": sanitize_text(report.branch, limit=240)
            if report.branch
            else None,
            "commit_sha": sanitize_text(report.commit_sha, limit=80)
            if report.commit_sha
            else None,
            "pr_url": sanitize_text(report.pr_url, limit=500)
            if report.pr_url
            else None,
            "validations": [
                sanitize_validation(item)
                for item in report.validations[:MAX_PROGRESS_VALIDATIONS]
            ],
            "ci_evidence": [
                sanitize_text(item, limit=240) for item in report.ci_evidence[:40]
            ],
            "review_evidence": [
                sanitize_text(item, limit=240) for item in report.review_evidence[:40]
            ],
            "merge_commit_sha": (
                sanitize_text(report.merge_commit_sha, limit=80)
                if report.merge_commit_sha
                else None
            ),
            "blockers": [
                sanitize_text(item, limit=MAX_PROGRESS_DETAIL)
                for item in report.blockers[:20]
            ],
            "card_disposition": (
                _sanitize_json(report.card_disposition)
                if report.card_disposition
                else None
            ),
            "resulting_lane": (
                sanitize_text(report.resulting_lane, limit=40)
                if report.resulting_lane
                else None
            ),
        }
    )


def sanitize_progress_event(
    event: DispatchProgressEventV1,
) -> DispatchProgressEventV1:
    return event.model_copy(
        update={
            "summary": sanitize_text(event.summary),
            "branch": sanitize_text(event.branch, limit=240) if event.branch else None,
            "commit_sha": sanitize_text(event.commit_sha, limit=80)
            if event.commit_sha
            else None,
            "pr_url": sanitize_text(event.pr_url, limit=500) if event.pr_url else None,
            "validations": [
                sanitize_validation(item)
                for item in event.validations[:MAX_PROGRESS_VALIDATIONS]
            ],
            "blockers": [
                sanitize_text(item, limit=MAX_PROGRESS_DETAIL)
                for item in event.blockers[:20]
                if sanitize_text(item, limit=MAX_PROGRESS_DETAIL)
            ],
            "retry_reason": (
                sanitize_text(event.retry_reason, limit=MAX_PROGRESS_DETAIL)
                if event.retry_reason
                else None
            ),
            "operator_input": (
                sanitize_operator_input(event.operator_input)
                if event.operator_input
                else None
            ),
            "tool_details": [
                item.model_copy(
                    update={
                        "title": sanitize_text(item.title, limit=MAX_PROGRESS_DETAIL),
                        "kind": sanitize_text(item.kind, limit=80)
                        if item.kind
                        else None,
                        "status": sanitize_text(item.status, limit=80)
                        if item.status
                        else None,
                        "result": sanitize_text(item.result, limit=MAX_PROGRESS_DETAIL)
                        if item.result
                        else None,
                    }
                )
                for item in event.tool_details[:MAX_PROGRESS_TOOL_DETAILS]
            ],
            "delivery_error": (
                sanitize_text(event.delivery_error, limit=MAX_PROGRESS_DETAIL)
                if event.delivery_error
                else None
            ),
        }
    )


def sanitize_heartbeat(
    heartbeat: DispatchProgressHeartbeatV1,
) -> DispatchProgressHeartbeatV1:
    return heartbeat.model_copy(
        update={
            "summary": sanitize_text(heartbeat.summary),
            "delivery_error": (
                sanitize_text(heartbeat.delivery_error, limit=MAX_PROGRESS_DETAIL)
                if heartbeat.delivery_error
                else None
            ),
        }
    )


def progress_freshness(
    *,
    last_activity_at: datetime | None,
    dispatch_state: str,
    last_error: str | None,
    protocol_version: int | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(UTC)
    if dispatch_state in {"completed", "acknowledged"}:
        state = "completed"
    elif dispatch_state == "failed":
        state = "failed"
    elif protocol_version is None:
        state = "disconnected"
    elif last_activity_at is None:
        state = "delayed"
    else:
        age = max(0, int((current - last_activity_at).total_seconds()))
        if age <= PROGRESS_LIVE_SECONDS:
            state = "live"
        elif age <= PROGRESS_DELAYED_SECONDS:
            state = "delayed"
        else:
            state = "stale"
    if last_error and state not in {"completed", "failed"}:
        state = "disconnected"
    age_seconds = (
        max(0, int((current - last_activity_at).total_seconds()))
        if last_activity_at
        else None
    )
    return {
        "state": state,
        "last_activity_at": (
            last_activity_at.isoformat() if last_activity_at else None
        ),
        "age_seconds": age_seconds,
        "live_seconds": PROGRESS_LIVE_SECONDS,
        "stale_seconds": PROGRESS_DELAYED_SECONDS,
    }


def phase_for_update(update: dict[str, Any]) -> ProgressPhase:
    event_type = str(update.get("type") or "").lower()
    title = sanitize_text(update.get("title") or update.get("summary") or "").lower()
    status = str(update.get("status") or "").lower()
    combined = f"{event_type} {title} {status}"
    if event_type == "turn_completed":
        return ProgressPhase.TURN_ENDED
    if "review" in combined or "comment" in combined:
        return ProgressPhase.ADDRESSING_REVIEW
    if "merge" in combined:
        return ProgressPhase.MERGING
    if "pull request" in combined or re.search(r"\bpr\b", combined):
        return ProgressPhase.OPENING_PR
    if "ci" in combined or "check run" in combined or "workflow" in combined:
        return ProgressPhase.WAITING_CI
    if any(
        word in combined
        for word in (
            "pytest",
            "unittest",
            "test",
            "ruff",
            "lint",
            "compile",
            "validation",
        )
    ):
        return ProgressPhase.TESTING
    if event_type == "plan":
        return ProgressPhase.PLANNING
    if any(
        word in combined
        for word in ("apply_patch", "write", "edit", "implement", "create file")
    ):
        return ProgressPhase.IMPLEMENTING
    if any(
        word in combined
        for word in ("retry", "reconnect", "recover", "connection_lost")
    ):
        return ProgressPhase.RETRYING
    if status in {"failed", "error", "blocked"}:
        return ProgressPhase.BLOCKED
    if event_type in {"tool_call", "tool_call_update"}:
        return ProgressPhase.INVESTIGATING
    text = sanitize_text(update.get("text") or "").lower()
    if any(word in text for word in ("implement", "editing", "adding", "updating")):
        return ProgressPhase.IMPLEMENTING
    if any(word in text for word in ("test", "validate", "checking")):
        return ProgressPhase.TESTING
    if any(word in text for word in ("plan", "design")):
        return ProgressPhase.PLANNING
    return ProgressPhase.INVESTIGATING


def derived_checkpoint(
    update: dict[str, Any],
) -> (
    tuple[
        ProgressPhase,
        str,
        list[ProgressToolDetailV1],
        list[ProgressValidationV1],
    ]
    | None
):
    """Derive only from visible commentary and allowlisted tool lifecycle fields."""
    event_type = str(update.get("type") or "")
    if event_type == "agent_thought_chunk":
        return None
    phase = phase_for_update(update)
    if event_type == "plan":
        entries = update.get("entries")
        entries = entries if isinstance(entries, list) else []
        return phase, f"Plan updated ({len(entries)} steps).", [], []
    if event_type in {"tool_call", "tool_call_update"}:
        title = sanitize_text(
            update.get("title") or "Tool activity",
            limit=MAX_PROGRESS_DETAIL,
        )
        status = sanitize_text(update.get("status") or "", limit=80) or None
        kind = sanitize_text(update.get("kind") or "", limit=80) or None
        summary = sanitize_text(
            title if not status else f"{title} · {status}",
            limit=MAX_PROGRESS_SUMMARY,
        )
        normalized_status = str(status or "").lower().replace("-", "_")
        validation_status = {
            "pending": "queued",
            "queued": "queued",
            "in_progress": "running",
            "running": "running",
            "completed": "passed",
            "complete": "passed",
            "succeeded": "passed",
            "success": "passed",
            "failed": "failed",
            "error": "failed",
            "cancelled": "cancelled",
            "canceled": "cancelled",
        }.get(normalized_status, "unknown")
        validations = (
            [
                ProgressValidationV1(
                    command=sanitize_text(title, limit=MAX_VALIDATION_COMMAND),
                    status=validation_status,
                    summary=sanitize_text(summary, limit=MAX_PROGRESS_DETAIL),
                )
            ]
            if phase == ProgressPhase.TESTING
            else []
        )
        return (
            phase,
            summary,
            [ProgressToolDetailV1(title=title, kind=kind, status=status)],
            validations,
        )
    if event_type == "agent_message_chunk":
        text = sanitize_text(update.get("text") or "")
        return (phase, text, [], []) if text else None
    if event_type == "turn_completed":
        result_value = update.get("result")
        result = result_value if isinstance(result_value, dict) else {}
        disposition_value = result.get("card_disposition")
        disposition = disposition_value if isinstance(disposition_value, dict) else {}
        summary = sanitize_text(
            disposition.get("outcome") or update.get("summary") or "Agent turn ended."
        )
        return ProgressPhase.TURN_ENDED, summary, [], []
    if event_type == "connection_lost":
        return (
            ProgressPhase.RETRYING,
            "Agent connection was lost; recovery is required.",
            [],
            [],
        )
    return None


class ProgressService:
    """Derive, persist, heartbeat, and retry progress delivery to the authority."""

    def __init__(
        self,
        store: Any,
        *,
        instance_id: str,
        token: str,
        async_runtime: AsyncRuntime | None = None,
        session_manager: Any = None,
        retry_seconds: float = PROGRESS_RETRY_SECONDS,
        heartbeat_seconds: float = PROGRESS_HEARTBEAT_SECONDS,
    ) -> None:
        self.store = store
        self.instance_id = instance_id
        self.token = token
        self.async_runtime = async_runtime
        self.session_manager = session_manager
        self.retry_seconds = retry_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._closing = False
        self._client: httpx.AsyncClient | None = None
        self._message_buffers: dict[tuple[str, str], str] = {}
        self._last_checkpoint_at: dict[str, datetime] = {}
        self._last_heartbeat_at: dict[str, datetime] = {}
        # Bound progress derivation before it can submit work to the shared pool.
        self._observe_slots = asyncio.Semaphore(4)
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._session_records: dict[str, tuple[datetime, Any]] = {}
        self._observations: dict[str, int] = {}
        self._coalesced_observations: dict[str, int] = {}
        self._derived_fingerprints: dict[tuple[str, str], tuple[str, datetime]] = {}
        self._meaningful_phases: dict[str, ProgressPhase] = {}
        self._observe_waiters = 0
        self._observe_max_waiters = 0
        self._malformed_sessions_warned: set[str] = set()

    async def _offload(self, operation: str, call, *args, **kwargs):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    def _http_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=3.0, read=10.0, write=10.0, pool=2.0),
                limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
            )
        return self._client

    def start(self) -> None:
        if not self._task or self._task.done():
            self._closing = False
            self._task = asyncio.create_task(self._run())

    def wake(self) -> None:
        self._wake.set()

    async def close(self) -> None:
        self._closing = True
        self._wake.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except TimeoutError:
                self._task.cancel()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def observe(self, session_id: str, update: dict[str, Any]) -> None:
        lock = self._session_locks.setdefault(session_id, asyncio.Lock())
        self._observe_waiters += 1
        self._observe_max_waiters = max(
            self._observe_max_waiters, self._observe_waiters
        )
        try:
            async with self._observe_slots, lock:
                self._observations[session_id] = (
                    self._observations.get(session_id, 0) + 1
                )
                while len(self._observations) > 256:
                    self._observations.pop(next(iter(self._observations)))
                try:
                    await self._observe(session_id, update)
                except (ValidationError, TypeError, ValueError) as exc:
                    if (
                        session_id not in self._malformed_sessions_warned
                        and len(self._malformed_sessions_warned) < 256
                    ):
                        logger.warning(
                            "Ignoring malformed progress update session=%s error=%s",
                            sanitize_text(session_id, limit=80),
                            sanitize_text(exc, limit=160),
                        )
                        self._malformed_sessions_warned.add(session_id)
        finally:
            self._observe_waiters -= 1
            if len(self._session_locks) > 256:
                for key, candidate in list(self._session_locks.items()):
                    if key != session_id and not candidate.locked():
                        self._session_locks.pop(key, None)
                    if len(self._session_locks) <= 256:
                        break

    def snapshot(self) -> dict[str, Any]:
        return {
            "limit": 4,
            "waiting": self._observe_waiters,
            "max_waiting": self._observe_max_waiters,
            "observations_by_session": dict(self._observations),
            "coalesced_observations_by_session": dict(self._coalesced_observations),
            "cached_sessions": len(self._session_records),
        }

    def _coalesce_derived_update(
        self,
        session_id: str,
        update: dict[str, Any],
        derived: tuple[
            ProgressPhase,
            str,
            list[ProgressToolDetailV1],
            list[ProgressValidationV1],
        ],
        *,
        now: datetime,
    ) -> bool:
        """Suppress repeated ACP lifecycle snapshots before any durable writes."""
        event_type = str(update.get("type") or "")
        if event_type not in {"plan", "tool_call", "tool_call_update"}:
            return False
        if event_type == "plan":
            source = "plan"
        else:
            source = sanitize_text(
                update.get("tool_call_id")
                or f"{update.get('kind') or ''}:{update.get('title') or ''}",
                limit=MAX_PROGRESS_DETAIL,
            )
            source = f"tool:{source or 'unknown'}"
        phase, summary, tool_details, validations = derived
        fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "phase": phase.value,
                    "summary": summary,
                    "tools": [item.model_dump(mode="json") for item in tool_details],
                    "validations": [
                        item.model_dump(mode="json") for item in validations
                    ],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        key = (session_id, source)
        previous = self._derived_fingerprints.get(key)
        self._derived_fingerprints[key] = (fingerprint, now)
        while len(self._derived_fingerprints) > 512:
            self._derived_fingerprints.pop(next(iter(self._derived_fingerprints)))
        if not previous or previous[0] != fingerprint:
            return False
        if (now - previous[1]).total_seconds() > DERIVED_PROGRESS_COALESCE_SECONDS:
            return False
        self._coalesced_observations[session_id] = (
            self._coalesced_observations.get(session_id, 0) + 1
        )
        while len(self._coalesced_observations) > 256:
            self._coalesced_observations.pop(next(iter(self._coalesced_observations)))
        return True

    async def _observe(self, session_id: str, update: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        cached = self._session_records.get(session_id)
        if cached and (now - cached[0]).total_seconds() < 5.0:
            record = cached[1]
        else:
            record = await self._offload(
                "progress.dispatch_read", self.store.by_session, session_id
            )
            self._session_records[session_id] = (now, record)
            while len(self._session_records) > 256:
                self._session_records.pop(next(iter(self._session_records)))
        if not record or record.progress_protocol_version != PROGRESS_SCHEMA_VERSION:
            return
        event_type = str(update.get("type") or "")
        checkpoint_update = update
        if event_type == "agent_message_chunk":
            message_id = str(update.get("message_id") or "current")
            key = (session_id, message_id)
            buffer = self._message_buffers.get(key, "")
            buffer = (buffer + str(update.get("text") or ""))[-4000:]
            self._message_buffers[key] = buffer
            while len(self._message_buffers) > 256:
                self._message_buffers.pop(next(iter(self._message_buffers)))
            last = self._last_checkpoint_at.get(session_id)
            should_emit = bool(update.get("final")) or (
                len(buffer) >= 80
                and (
                    "\n" in str(update.get("text") or "")
                    or (last is None or (now - last).total_seconds() >= 8)
                )
            )
            if not should_emit:
                await self._heartbeat(record, phase_for_update(update), "Agent active.")
                return
            checkpoint_update = {**update, "text": buffer}
            self._message_buffers.pop(key, None)
        derived = derived_checkpoint(checkpoint_update)
        if derived:
            phase, summary, tool_details, validations = derived
            if summary:
                if self._coalesce_derived_update(
                    session_id,
                    checkpoint_update,
                    derived,
                    now=now,
                ):
                    await self._heartbeat(record, phase, "Agent active.")
                    return
                # Tool lifecycle is replaceable detail, not a human milestone. Keep
                # its latest live state in the heartbeat and persist only completed
                # validations/failures. Neutral tools must not erase an explicit or
                # previously meaningful implementation phase.
                if event_type in {"tool_call", "tool_call_update"}:
                    status = str(checkpoint_update.get("status") or "").lower()
                    meaningful = self._meaningful_phases.get(session_id)
                    effective_phase = (
                        phase
                        if phase
                        in {
                            ProgressPhase.TESTING,
                            ProgressPhase.BLOCKED,
                            ProgressPhase.RETRYING,
                            ProgressPhase.OPENING_PR,
                            ProgressPhase.WAITING_CI,
                            ProgressPhase.ADDRESSING_REVIEW,
                            ProgressPhase.MERGING,
                        }
                        else meaningful
                        or (
                            record.latest_progress.phase
                            if record.latest_progress
                            else phase
                        )
                    )
                    durable_validation = bool(validations) and status in {
                        "completed",
                        "complete",
                        "succeeded",
                        "success",
                        "failed",
                        "error",
                        "cancelled",
                        "canceled",
                    }
                    if not durable_validation and phase != ProgressPhase.BLOCKED:
                        await self._heartbeat(record, effective_phase, summary)
                        return
                await self._checkpoint(
                    record,
                    phase=phase,
                    summary=summary,
                    tool_details=tool_details,
                    validations=validations,
                    final=event_type == "turn_completed",
                    result=(
                        update.get("result")
                        if isinstance(update.get("result"), dict)
                        else {}
                    ),
                )
                self._last_checkpoint_at[session_id] = now
                if event_type not in {"tool_call", "tool_call_update"}:
                    self._meaningful_phases[session_id] = phase
                return
        fallback_phase = self._meaningful_phases.get(
            session_id, phase_for_update(update)
        )
        await self._heartbeat(record, fallback_phase, "Agent active.")

    async def explicit(
        self,
        dispatch_id: str,
        checkpoint: ExplicitProgressCheckpointV1,
        *,
        originating_instance_id: str | None = None,
    ) -> ProgressIngestResult:
        record = await self._offload(
            "progress.dispatch_read", self.store.get, dispatch_id
        )
        if not record:
            raise ValueError("Dispatch not found")
        self._meaningful_phases[record.session_id or dispatch_id] = checkpoint.phase
        return await self._checkpoint(
            record,
            phase=checkpoint.phase,
            summary=checkpoint.summary,
            branch=checkpoint.branch,
            commit_sha=checkpoint.commit_sha,
            pr_url=checkpoint.pr_url,
            pr_number=checkpoint.pr_number,
            changed_file_count=checkpoint.changed_file_count,
            validations=checkpoint.validations,
            blockers=checkpoint.blockers,
            retry_reason=checkpoint.retry_reason,
            operator_input=checkpoint.operator_input,
            explicit_key=checkpoint.idempotency_key,
            originating_instance_id=originating_instance_id,
            final=checkpoint.phase
            in {ProgressPhase.TURN_ENDED, ProgressPhase.COMPLETED},
        )

    async def _checkpoint(
        self,
        record: Any,
        *,
        phase: ProgressPhase,
        summary: str,
        branch: str | None = None,
        commit_sha: str | None = None,
        pr_url: str | None = None,
        pr_number: int | None = None,
        changed_file_count: int | None = None,
        validations: list[ProgressValidationV1] | None = None,
        blockers: list[str] | None = None,
        retry_reason: str | None = None,
        operator_input: str | OperatorInputRequestV1 | None = None,
        tool_details: list[ProgressToolDetailV1] | None = None,
        explicit_key: str | None = None,
        originating_instance_id: str | None = None,
        final: bool = False,
        result: dict[str, Any] | None = None,
    ) -> ProgressIngestResult:
        sequence = await self._offload(
            "progress.sequence_allocate",
            self.store.allocate_progress_sequence,
            record.dispatch_id,
        )
        digest = hashlib.sha256(
            json.dumps(
                {
                    "dispatch_id": record.dispatch_id,
                    "session_id": record.session_id,
                    "sequence": sequence,
                    "phase": phase.value,
                    "summary": sanitize_text(summary),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()[:24]
        normalized_validations = [
            ProgressValidationV1(
                command=sanitize_text(item.command, limit=MAX_VALIDATION_COMMAND),
                status=item.status,
                summary=(
                    sanitize_text(item.summary, limit=MAX_PROGRESS_DETAIL)
                    if item.summary
                    else None
                ),
                duration_ms=item.duration_ms,
            )
            for item in list(validations or [])[:MAX_PROGRESS_VALIDATIONS]
        ]
        normalized_tools = [
            ProgressToolDetailV1(
                title=sanitize_text(item.title, limit=MAX_PROGRESS_DETAIL),
                kind=sanitize_text(item.kind, limit=80) if item.kind else None,
                status=sanitize_text(item.status, limit=80) if item.status else None,
                result=(
                    sanitize_text(item.result, limit=MAX_PROGRESS_DETAIL)
                    if item.result
                    else None
                ),
            )
            for item in list(tool_details or [])[:MAX_PROGRESS_TOOL_DETAILS]
        ]
        event = DispatchProgressEventV1(
            kind=ProgressKind.FINAL if final else ProgressKind.CHECKPOINT,
            card_id=record.card_id,
            dispatch_id=record.dispatch_id,
            acp_session_id=record.session_id or "",
            originating_instance_id=originating_instance_id or self.instance_id,
            authority_instance_id=record.authority_instance_id,
            authority_version=record.card_version,
            sequence=sequence,
            idempotency_key=explicit_key or f"progress:{sequence}:{digest}",
            phase=phase,
            summary=sanitize_text(summary, limit=MAX_PROGRESS_SUMMARY),
            branch=sanitize_text(branch, limit=240) if branch else None,
            commit_sha=(sanitize_text(commit_sha, limit=80) if commit_sha else None),
            pr_url=sanitize_text(pr_url, limit=500) if pr_url else None,
            pr_number=pr_number,
            changed_file_count=changed_file_count,
            validations=normalized_validations,
            blockers=[
                sanitize_text(item, limit=MAX_PROGRESS_DETAIL)
                for item in list(blockers or [])[:20]
                if sanitize_text(item, limit=MAX_PROGRESS_DETAIL)
            ],
            retry_reason=(
                sanitize_text(retry_reason, limit=MAX_PROGRESS_DETAIL)
                if retry_reason
                else None
            ),
            operator_input=(
                sanitize_operator_input(operator_input) if operator_input else None
            ),
            tool_details=normalized_tools,
        )
        ingest = await self._offload(
            "progress.checkpoint_write",
            self.store.ingest_progress,
            event,
            delivered=self.instance_id == record.authority_instance_id,
        )
        if final:
            report = await self._offload(
                "progress.final_report_build",
                self.store.build_final_report,
                record.dispatch_id,
                result or {},
            )
            if report:
                await self._offload(
                    "progress.final_report_write",
                    self.store.set_final_report,
                    record.dispatch_id,
                    report,
                )
        self._wake.set()
        return ingest

    async def _heartbeat(self, record: Any, phase: ProgressPhase, summary: str) -> None:
        now = datetime.now(UTC)
        last = self._last_heartbeat_at.get(record.dispatch_id)
        if last and (now - last).total_seconds() < self.heartbeat_seconds:
            return
        sequence = await self._offload(
            "progress.sequence_allocate",
            self.store.allocate_progress_sequence,
            record.dispatch_id,
        )
        heartbeat = sanitize_heartbeat(
            DispatchProgressHeartbeatV1(
                card_id=record.card_id,
                dispatch_id=record.dispatch_id,
                acp_session_id=record.session_id or "",
                originating_instance_id=self.instance_id,
                authority_instance_id=record.authority_instance_id,
                authority_version=record.card_version,
                sequence=sequence,
                idempotency_key=f"heartbeat:{record.dispatch_id}:{sequence}",
                phase=phase,
                summary=summary,
            )
        )
        await self._offload(
            "progress.heartbeat_write",
            self.store.ingest_heartbeat,
            heartbeat,
            delivered=self.instance_id == record.authority_instance_id,
        )
        self._last_heartbeat_at[record.dispatch_id] = now
        self._wake.set()

    async def _queue_periodic_heartbeats(self) -> None:
        if not self.session_manager:
            return
        for runtime in self.session_manager.list_runtimes():
            if not runtime.prompting:
                continue
            record = await self._offload(
                "progress.dispatch_read", self.store.by_session, runtime.session_id
            )
            if record and record.progress_protocol_version == PROGRESS_SCHEMA_VERSION:
                phase = (
                    record.latest_progress.phase
                    if record.latest_progress
                    else ProgressPhase.INVESTIGATING
                )
                summary = (
                    record.latest_progress.summary
                    if record.latest_progress
                    else "Agent active."
                )
                await self._heartbeat(record, phase, summary)

    async def _run(self) -> None:
        while not self._closing:
            try:
                await self._queue_periodic_heartbeats()
                pending = await self._offload(
                    "progress.pending_read",
                    self.store.pending_progress,
                    self.instance_id,
                )
                for record, payload in pending:
                    await self._send(record, payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Progress delivery loop failed")
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.retry_seconds)
            except TimeoutError:
                pass

    async def _send(
        self,
        record: Any,
        payload: DispatchProgressEventV1 | DispatchProgressHeartbeatV1,
    ) -> None:
        if record.authority_instance_id == self.instance_id:
            await self._offload(
                "progress.delivery_ack",
                self.store.mark_progress_delivered,
                record.dispatch_id,
                payload.idempotency_key,
            )
            return
        headers = {
            "Idempotency-Key": payload.idempotency_key,
            "X-PA-Origin-Instance-ID": self.instance_id,
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            request = self._http_client().post(
                f"{record.authority_url.rstrip('/')}/api/fleet/dispatch/"
                f"{record.dispatch_id}/progress",
                json=payload.transport_dict(),
                headers=headers,
            )
            response = (
                await self.async_runtime.observe(
                    "http.dispatch_progress", request, timeout=15.0
                )
                if self.async_runtime
                else await request
            )
            if response.status_code in {200, 208}:
                await self._offload(
                    "progress.delivery_ack",
                    self.store.mark_progress_delivered,
                    record.dispatch_id,
                    payload.idempotency_key,
                )
                return
            error = f"HTTP {response.status_code}: {response.text[:200]}"
        except Exception as exc:  # noqa: BLE001 - delivery failures remain retryable
            error = f"{type(exc).__name__}: {exc}"
        await self._offload(
            "progress.delivery_fail",
            self.store.mark_progress_delivery_failed,
            record.dispatch_id,
            payload.idempotency_key,
            sanitize_text(error, limit=MAX_PROGRESS_DETAIL),
        )
