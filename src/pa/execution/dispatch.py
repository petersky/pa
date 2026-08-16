"""Durable, idempotent fleet dispatch and completion mutations."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import random
import sqlite3
import time
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Any, Literal
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

from pa.core.async_runtime import (
    AsyncRuntime,
    AsyncRuntimeClosed,
    BlockingOperationTimeout,
    BlockingQueueFull,
)
from pa.core.io import atomic_write_json, atomic_write_text
from pa.execution.post_turn import PostTurnEvaluationV1, TurnEndSnapshotV1
from pa.execution.progress import (
    MAX_PROGRESS_EVENTS,
    MAX_PROGRESS_SEEN_KEYS,
    CompletionReportV1,
    DispatchProgressEventV1,
    DispatchProgressHeartbeatV1,
    ProgressIngestResult,
    progress_freshness,
    sanitize_completion_report,
    sanitize_heartbeat,
    sanitize_progress_event,
    sanitize_text,
)
from pa.goals.materialization import (
    GoalExecutionIdentityV1,
    GoalMaterializationEnvelopeV1,
    GoalMaterializationReceiptV1,
    canonical_materialization_digest,
)

logger = logging.getLogger(__name__)

DISPATCH_STAGES = {
    "admission_pending",
    "waiting_capacity",
    "blocked",
    "queued",
    "checking_sync",
    "materializing",
    "provisioning",
    "starting_session",
    "delivering_prompt",
    "running",
    "failed",
    "completion_pending",
    "completed",
    "cancelled",
}
TERMINAL_DISPATCH_STATES = {"failed", "completed", "cancelled"}
CAPACITY_RESERVATION_STATES = {
    "queued",
    "checking_sync",
    "materializing",
    "provisioning",
    "starting_session",
    "delivering_prompt",
}
QUEUE_CONSUMING_STATES = {"waiting_capacity", "blocked"}
CAPACITY_RESERVATION_TTL = timedelta(hours=1)
CAPACITY_CONSUMER_LINK_LIMIT = 256
RECOVERABLE_DISPATCH_STATES = {
    "checking_sync",
    "materializing",
    "provisioning",
    "starting_session",
    "delivering_prompt",
    # Legacy records written by the synchronous implementation.
    "dispatching",
    "dispatched",
    "materialized",
}


class DispatchEvent(BaseModel):
    seq: int
    state: str
    message: str
    detail: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalDispatchProvenance(BaseModel):
    """Durable governance authority carried to every dispatch side-effect sink."""

    goal_id: str = Field(min_length=1, max_length=80)
    goal_version: int = Field(ge=1)
    policy_revision: int = Field(ge=1)
    authority_instance_id: str = Field(min_length=1, max_length=80)
    fencing_token: int = Field(ge=1)
    action_reservation_id: str = Field(min_length=1, max_length=80)
    operation_key: str | None = Field(default=None, min_length=1, max_length=200)
    requested_placement_target: str | None = Field(
        default=None, min_length=1, max_length=200
    )
    placement_input_digest: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    resolved_target_instance_id: str | None = Field(
        default=None, min_length=1, max_length=80
    )
    placement_decision_digest: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    materialization_envelope: GoalMaterializationEnvelopeV1 | None = None
    materialization_receipt: GoalMaterializationReceiptV1 | None = None
    execution_identity: GoalExecutionIdentityV1 | None = None
    actor_principal: str = Field(min_length=1, max_length=300)
    action_class: Literal["dispatch_work_package"] = "dispatch_work_package"
    provider_id: str | None = Field(default=None, min_length=1, max_length=100)
    reservation_attempt: int = Field(default=1, ge=1, le=20)
    max_reservation_attempts: int = Field(default=1, ge=1, le=20)
    retry_idempotency_key: str | None = Field(default=None, max_length=200)
    released_at: datetime | None = None
    release_reason: str | None = Field(default=None, max_length=500)


class DispatchRecord(BaseModel):
    dispatch_id: str = Field(default_factory=lambda: str(uuid4()))
    mutation_id: str
    idempotency_key: str | None = None
    request_fingerprint: str | None = None
    placement_request_fingerprint: str | None = None
    card_id: str | None = None
    project_id: str | None = None
    realm_id: str = "default"
    card_version: str | None = None
    card_snapshot: dict[str, Any] | None = None
    sync_evidence: dict[str, Any] | None = None
    attachment_evidence: dict[str, Any] | None = None
    materialization_plan: dict[str, Any] | None = None
    request_payload: dict[str, Any] = Field(default_factory=dict)
    goal_provenance: GoalDispatchProvenance | None = None
    goal_placement_input: dict[str, Any] | None = None
    goal_placement_input_digest: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    goal_admission_validation_state: Literal[
        "not_required", "pending", "validated", "rejected"
    ] = "not_required"
    goal_admission_validated_at: datetime | None = None
    goal_admission_validation_proof: str | None = None
    goal_admission_validation_error: str | None = None
    principal_id: str = "user:local"
    authority_instance_id: str
    authority_instance_name: str | None = None
    authority_url: str
    target_instance_id: str
    target_instance_name: str | None = None
    placement_policy: str = "named_instance"
    placement_decision: dict[str, Any] | None = None
    placement_resolved_at: datetime | None = None
    capacity_limit: int | None = None
    capacity_source: str | None = None
    capacity_provider: str | None = None
    capacity_observed_active: int = 0
    capacity_observed_queued: int = 0
    capacity_observed_reservations: int = 0
    capacity_reserved_at: datetime | None = None
    capacity_reservation_expires_at: datetime | None = None
    capacity_released_at: datetime | None = None
    capacity_release_reason: str | None = None
    capacity_override: bool = False
    capacity_override_reason: str | None = None
    queue_limit: int = Field(default=100, ge=0, le=10_000)
    queue_source: str = "documented_default"
    queue_provider_specific: bool = False
    queue_observed_count: int = Field(default=0, ge=0)
    queue_admitted_at: datetime | None = None
    queue_launched_at: datetime | None = None
    queue_position: int | None = Field(default=None, ge=1)
    queue_wait_reason: str | None = None
    queue_blocked_code: str | None = None
    requested_priority: int = Field(default=0, ge=-10, le=10)
    scheduling_class: str | None = None
    scheduling_class_sequence: int = Field(default=1, ge=1)
    queue_audit: list[dict[str, Any]] = Field(default_factory=list)
    allow_concurrent: bool = False
    session_id: str | None = None
    resume_requested: bool = False
    resume_session_id: str | None = None
    state: str = "queued"
    stage_attempts: int = 0
    attempts: int = 0
    last_error: str | None = None
    error_code: str | None = None
    recoverable: bool = True
    cancel_requested: bool = False
    control_operations: dict[str, str] = Field(default_factory=dict)
    terminal_repair_reservation: dict[str, Any] | None = None
    followup_operations: dict[str, dict[str, Any]] = Field(default_factory=dict)
    prompt_acknowledged_at: datetime | None = None
    prompt_ack: dict[str, Any] | None = None
    knowledge_recorded_at: datetime | None = None
    completion_payload: dict[str, Any] | None = None
    completion_envelope: dict[str, Any] | None = None
    completion_received_at: datetime | None = None
    card_disposition_payload: dict[str, Any] | None = None
    card_disposition_status: str | None = None
    card_disposition_reason: str | None = None
    card_disposition_error: str | None = None
    card_lane_before: str | None = None
    card_lane_after: str | None = None
    reconciliation_state: str = "not_requested"
    reconciliation_reason: str | None = None
    reconciliation_condition: str | None = None
    reconciliation_last_dependency_error: str | None = None
    reconciliation_recovery_action: str | None = None
    reconciliation_recoverable: bool = False
    reconciliation_attempts: int = 0
    reconciliation_prompt_count: int = 0
    reconciliation_prompt_id: str | None = None
    reconciliation_next_retry_at: datetime | None = None
    reconciliation_updated_at: datetime | None = None
    reconciliation_current_card: dict[str, Any] | None = None
    completion_delivery_class: str | None = None
    completion_next_retry_at: datetime | None = None
    acknowledged_at: datetime | None = None
    events: list[DispatchEvent] = Field(default_factory=list)
    progress_protocol_version: int | None = None
    progress_next_sequence: int = 1
    progress_events: list[DispatchProgressEventV1] = Field(default_factory=list)
    progress_heartbeat: DispatchProgressHeartbeatV1 | None = None
    progress_seen_keys: list[str] = Field(default_factory=list)
    progress_conflicts: int = 0
    progress_authority_history: list[dict[str, Any]] = Field(default_factory=list)
    final_report: CompletionReportV1 | None = None
    turn_end_snapshots: list[TurnEndSnapshotV1] = Field(default_factory=list)
    post_turn_context_digests: dict[str, str] = Field(default_factory=dict)
    post_turn_evaluations: list[PostTurnEvaluationV1] = Field(default_factory=list)
    followup_turns: list[dict[str, Any]] = Field(default_factory=list)
    lifecycle_inconsistencies: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @property
    def latest_progress(self) -> DispatchProgressEventV1 | None:
        return max(
            self.progress_events,
            key=lambda event: (event.sequence, event.occurred_at),
            default=None,
        )

    @property
    def accepts_late_completion_after_terminal_repair(self) -> bool:
        """Whether immutable completion may supersede an abandonment repair."""
        return bool(
            self.state == "cancelled"
            and self.error_code == "legacy_abandoned_dispatch_retired"
            and self.acknowledged_at is None
            and self.completion_payload is None
            and any(
                operation == "repair_terminal:abandoned_without_acknowledgement"
                for operation in self.control_operations.values()
            )
        )

    def public_dict(self) -> dict[str, Any]:
        data = self.model_dump(
            mode="json",
            exclude={
                "request_payload",
                "card_snapshot",
                "progress_seen_keys",
                "progress_next_sequence",
            },
        )
        data["can_retry"] = self.state in {"failed", "cancelled"} and self.recoverable
        data["can_cancel"] = self.state in {
            "waiting_capacity",
            "blocked",
            "queued",
            "checking_sync",
            "materializing",
            "provisioning",
            "starting_session",
        }
        data["collaboration"] = {
            "requested_mode": self.request_payload.get("collaboration_mode"),
            "decision": self.request_payload.get("collaboration_decision"),
        }
        data["queue"] = {
            "waiting": self.state in QUEUE_CONSUMING_STATES,
            "position": self.queue_position,
            "scheduling_class": self.scheduling_class,
            "requested_priority": self.requested_priority,
            "reason": self.queue_wait_reason,
            "blocked_code": self.queue_blocked_code,
            "admitted_at": (
                self.queue_admitted_at.isoformat() if self.queue_admitted_at else None
            ),
            "launched_at": (
                self.queue_launched_at.isoformat() if self.queue_launched_at else None
            ),
            "capacity": self.queue_limit,
            "observed_count": self.queue_observed_count,
            "estimated_eligibility_at": None,
            "estimate_reason": "Completion times are not predictable enough for a defensible ETA."
            if self.state in QUEUE_CONSUMING_STATES
            else None,
        }
        data["completion_outbox"] = {
            "pending": self.state == "completion_pending",
            "attempts": self.attempts,
            "last_error": self.last_error
            if self.completion_delivery_class != "acknowledged"
            else None,
            "classification": self.completion_delivery_class,
            "next_retry_at": (
                self.completion_next_retry_at.isoformat()
                if self.completion_next_retry_at
                else None
            ),
        }
        data["agent_turn"] = {
            "ended": self.completion_payload is not None,
            # Mixed-version compatibility: this legacy field means lifecycle
            # termination only and never implies that the card outcome succeeded.
            "completed": self.completion_payload is not None,
            "stop_reason": (self.completion_payload or {}).get("stop_reason"),
        }
        data["dispatch_completion"] = {
            "completed": (
                self.acknowledged_at is not None
                or (
                    self.state in {"completed", "acknowledged"}
                    and self.completion_delivery_class != "semantic_conflict"
                )
            ),
            "acknowledged_at": self.acknowledged_at.isoformat()
            if self.acknowledged_at
            else None,
        }
        data["effective_state"] = (
            "completed"
            if data["dispatch_completion"]["completed"]
            and self.state not in {"failed", "cancelled"}
            else self.state
        )
        data["card_completion"] = {
            "status": self.card_disposition_status or "not_requested",
            "lane_before": self.card_lane_before,
            "lane_after": self.card_lane_after,
            "reason": self.card_disposition_reason,
            "extraction_error": self.card_disposition_error,
        }
        data["card_reconciliation"] = {
            "state": self.reconciliation_state,
            "reason": self.reconciliation_reason,
            "disposition_error": self.card_disposition_error,
            "condition": self.reconciliation_condition,
            "last_dependency_error": self.reconciliation_last_dependency_error,
            "recovery_action": self.reconciliation_recovery_action,
            "recoverable": self.reconciliation_recoverable,
            "attempts": self.reconciliation_attempts,
            "prompt_count": self.reconciliation_prompt_count,
            "prompt_id": self.reconciliation_prompt_id,
            "next_retry_at": (
                self.reconciliation_next_retry_at.isoformat()
                if self.reconciliation_next_retry_at
                else None
            ),
            "updated_at": (
                self.reconciliation_updated_at.isoformat()
                if self.reconciliation_updated_at
                else None
            ),
            "current_card": self.reconciliation_current_card,
        }
        latest_evaluation = (
            self.post_turn_evaluations[-1] if self.post_turn_evaluations else None
        )
        data["turn_end"] = (
            self.turn_end_snapshots[-1].model_dump(mode="json")
            if self.turn_end_snapshots
            else None
        )
        data["post_turn_evaluation"] = (
            latest_evaluation.model_dump(mode="json") if latest_evaluation else None
        )
        data["evaluated_outcome"] = (
            _evaluated_outcome(latest_evaluation)
            if latest_evaluation
            else "needs_evaluation"
        )
        data["followup_state"] = {
            "turns": list(self.followup_turns[-20:]),
            "scheduled": bool(
                latest_evaluation
                and any(
                    action.status.value in {"approved", "executed"}
                    and action.name.value not in {"no_action", "record_turn_outcome"}
                    for action in latest_evaluation.recommended_actions
                )
            ),
        }
        data["lifecycle_diagnostics"] = {
            "consistent": not self.lifecycle_inconsistencies,
            "issues": list(self.lifecycle_inconsistencies[-20:]),
            "acknowledged_completion_wins": self.acknowledged_at is not None,
        }
        latest = self.latest_progress
        heartbeat = self.progress_heartbeat
        last_activity = max(
            (
                value
                for value in (
                    latest.last_activity_at if latest else None,
                    heartbeat.occurred_at if heartbeat else None,
                )
                if value is not None
            ),
            default=None,
        )
        sequences = sorted({event.sequence for event in self.progress_events})
        sequence_gap = bool(
            sequences and sequences != list(range(sequences[0], sequences[-1] + 1))
        )
        progress_delivery_error = next(
            (
                payload.delivery_error
                for payload in [
                    heartbeat,
                    *reversed(self.progress_events),
                ]
                if payload and payload.delivery_error
            ),
            None,
        )
        data["progress"] = {
            "schema_version": self.progress_protocol_version,
            "supported_versions": [1],
            "latest": latest.model_dump(mode="json") if latest else None,
            "heartbeat": heartbeat.model_dump(mode="json") if heartbeat else None,
            "freshness": progress_freshness(
                last_activity_at=last_activity,
                dispatch_state=data["effective_state"],
                last_error=self.last_error or progress_delivery_error,
                protocol_version=self.progress_protocol_version,
            ),
            "delivery_error": progress_delivery_error,
            "checkpoint_count": len(self.progress_events),
            "sequence_gap": sequence_gap,
            "conflicts": self.progress_conflicts,
            "reporting": (
                "structured"
                if self.progress_protocol_version == 1
                else "lifecycle_only"
            ),
        }
        return data


def _canonical_digest(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()


def goal_dispatch_placement_input_snapshot(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Normalize only the inputs that can affect governed fleet placement."""

    def selector(name: str) -> Any:
        value = payload.get(name)
        return value.value if hasattr(value, "value") else value

    return {
        "card_id": selector("card_id"),
        "project_id": selector("project_id"),
        "target_instance_id": selector("target_instance_id"),
        "placement_policy": selector("placement_policy"),
        "group_id": selector("group_id"),
        "provider": str(selector("provider") or "").strip().lower() or None,
        "model_id": selector("model_id"),
        "mode_id": selector("mode_id"),
        "required_capabilities": sorted(
            {str(item) for item in (payload.get("required_capabilities") or [])}
        ),
        "required_mcp_servers": sorted(
            {str(item) for item in (payload.get("required_mcp_servers") or [])}
        ),
        "optional_mcp_servers": sorted(
            {str(item) for item in (payload.get("optional_mcp_servers") or [])}
        ),
        "execution_contract": payload.get("execution_contract"),
        "capacity_override": bool(payload.get("capacity_override")),
        "participation_override": bool(payload.get("participation_override")),
    }


def goal_dispatch_placement_input_digest(payload: dict[str, Any]) -> str:
    """Hash the server-canonical governed placement input."""

    return _canonical_digest(goal_dispatch_placement_input_snapshot(payload))


def goal_dispatch_record_placement_input_valid(record: DispatchRecord) -> bool:
    """Reject mutation of any placement-affecting launch field after binding."""

    snapshot = record.goal_placement_input
    digest = record.goal_placement_input_digest
    if snapshot is None or not digest:
        return False
    canonical_snapshot = goal_dispatch_placement_input_snapshot(snapshot)
    if _canonical_digest(canonical_snapshot) != digest:
        return False
    current = dict(canonical_snapshot)
    for field in canonical_snapshot:
        if field in record.request_payload:
            current[field] = record.request_payload[field]
    return goal_dispatch_placement_input_digest(current) == digest


def goal_dispatch_materialization_binding_valid(record: DispatchRecord) -> bool:
    """Verify the immutable envelope and receipt against this exact launch plan."""

    provenance = record.goal_provenance
    if provenance is None:
        return True
    envelope = provenance.materialization_envelope
    receipt = provenance.materialization_receipt
    plan = record.materialization_plan
    if envelope is None or receipt is None or plan is None:
        return False
    provider_id = str(record.request_payload.get("provider") or "").strip().lower()
    if not provider_id:
        return False
    if (
        canonical_materialization_digest(
            record.request_payload.get("execution_contract")
        )
        != envelope.execution_contract_digest
    ):
        return False
    if str(plan.get("target_instance_id") or "") != record.target_instance_id:
        return False
    expected = GoalMaterializationReceiptV1(
        envelope_digest=str(envelope.digest),
        target_instance_id=record.target_instance_id,
        provider_id=provider_id,
        model_id=record.request_payload.get("model_id"),
        mode_id=record.request_payload.get("mode_id"),
        materialization_plan_digest=canonical_materialization_digest(plan),
    )
    return bool(
        receipt == expected
        and receipt.envelope_digest == envelope.digest
        and provenance.resolved_target_instance_id == receipt.target_instance_id
    )


def goal_dispatch_execution_identity_valid(
    record: DispatchRecord,
    *,
    require_authenticated_credential: bool = False,
) -> bool:
    """Bind an allocated governed session to its exact execution identity."""

    provenance = record.goal_provenance
    if provenance is None:
        return True
    identity = provenance.execution_identity
    if record.session_id is None:
        return identity is None
    receipt = provenance.materialization_receipt
    envelope = provenance.materialization_envelope
    if identity is None or receipt is None or envelope is None:
        return False
    valid = bool(
        identity.materialization_receipt_digest == receipt.digest
        and identity.work_package_id == envelope.work_package_id
        and identity.service_role == envelope.service_role
        and identity.provider_id.strip().lower()
        == str(record.request_payload.get("provider") or "").strip().lower()
        and identity.target_instance_id == record.target_instance_id
        and identity.session_id == record.session_id
        and identity.fencing_token == provenance.fencing_token
    )
    if require_authenticated_credential:
        return valid and identity.credential_authenticated()
    return valid


def goal_dispatch_placement_decision_digest(
    decision: dict[str, Any] | None,
) -> str:
    """Hash the complete resolved placement decision, including its evidence."""

    return _canonical_digest(decision)


def goal_admission_validation_proof(record: DispatchRecord) -> str:
    """Bind durable validation to one operation, target, and placement result."""

    provenance = record.goal_provenance
    return _canonical_digest(
        {
            "dispatch_id": record.dispatch_id,
            "mutation_id": record.mutation_id,
            "idempotency_key": record.idempotency_key,
            "placement_request_fingerprint": record.placement_request_fingerprint,
            "placement_input": record.goal_placement_input,
            "placement_input_digest": record.goal_placement_input_digest,
            "target_instance_id": record.target_instance_id,
            "placement_policy": record.placement_policy,
            "placement_decision_digest": goal_dispatch_placement_decision_digest(
                record.placement_decision
            ),
            "provider": record.request_payload.get("provider"),
            "materialization_plan": record.materialization_plan,
            "goal_provenance": (
                provenance.model_dump(mode="json", exclude={"execution_identity"})
                if provenance is not None
                else None
            ),
        }
    )


def _evaluated_outcome(evaluation: PostTurnEvaluationV1) -> str:
    decision = evaluation.decision.value
    if decision == "outcome_achieved":
        return "attempt_succeeded"
    if decision in {
        "further_agent_work_needed",
        "waiting_on_external_condition",
        "operator_input_required",
        "followup_record_required",
    }:
        return "attempt_blocked"
    if decision in {"retryable_runtime_failure", "nonretryable_failure"}:
        return "attempt_failed"
    return "needs_evaluation"


class CapacityAdmission(BaseModel):
    """Fresh placement utilization rechecked atomically with ledger admission."""

    limit: int = Field(ge=1, le=256)
    source: str
    provider: str | None = None
    provider_specific: bool = False
    observed_active: int = Field(default=0, ge=0)
    observed_queued: int = Field(default=0, ge=0)
    observed_reservations: int = Field(default=0, ge=0)
    observed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    consumer_links: list[dict[str, Any]] = Field(default_factory=list)
    global_limit: int | None = Field(default=None, ge=1, le=256)
    provider_limit: int | None = Field(default=None, ge=1, le=256)
    observed_global_active: int | None = Field(default=None, ge=0)
    observed_global_queued: int | None = Field(default=None, ge=0)
    observed_global_reservations: int | None = Field(default=None, ge=0)
    observed_provider_active: int | None = Field(default=None, ge=0)
    observed_provider_queued: int | None = Field(default=None, ge=0)
    observed_provider_reservations: int | None = Field(default=None, ge=0)
    queue_limit: int | None = Field(default=None, ge=0, le=10_000)
    global_queue_limit: int | None = Field(default=None, ge=0, le=10_000)
    provider_queue_limit: int | None = Field(default=None, ge=0, le=10_000)
    queue_source: str = "documented_default"
    queue_provider_specific: bool = False
    observed_waiting: int = Field(default=0, ge=0)
    observed_global_waiting: int | None = Field(default=None, ge=0)
    observed_provider_waiting: int | None = Field(default=None, ge=0)
    override: bool = False
    override_reason: str | None = None


class DispatchStore:
    """Transactional incremental ledger shared by dispatch, progress, and outbox.

    Dispatch metadata is one bounded JSON document per dispatch. High-rate progress,
    heartbeat, and idempotency receipt state is normalized so a mutation appends or
    updates only its own rows in a WAL transaction. The legacy JSON ledger is an
    immutable migration source and rollback artifact after verification.
    """

    SCHEMA_VERSION = 2
    LEGACY_BACKUP_SUFFIX = ".pre-sqlite-backup"
    RECEIPT_REPLAY_DAYS = 30
    MAX_PROGRESS_BYTES_PER_DISPATCH = 4 * 1024 * 1024

    def __init__(
        self,
        data_dir: Path,
        *,
        fault_injector: Callable[[str], None] | None = None,
        read_only: bool = False,
        deferred_read_only: bool = False,
    ) -> None:
        if deferred_read_only and not read_only:
            raise ValueError("deferred dispatch storage must be read-only")
        self.path = data_dir / "dispatch_mutations.json"
        self.db_path = data_dir / "dispatch_mutations.db"
        self.backup_path = self.path.with_name(
            self.path.name + self.LEGACY_BACKUP_SUFFIX
        )
        self.metrics_path = data_dir / "dispatch_queue_metrics.json"
        self._records: dict[str, DispatchRecord] = {}
        self._latest_card_records: dict[str, DispatchRecord] = {}
        self._latest_session_records: dict[tuple[str, str], DispatchRecord] = {}
        self._latest_session_records_global: dict[str, DispatchRecord] = {}
        self._history_counts: dict[tuple[str, str], int] = {}
        self._capacity_records_by_target: dict[str, tuple[DispatchRecord, ...]] = {}
        self._goal_lifecycle_records: dict[str, DispatchRecord] = {}
        self._lock = RLock()
        # Database writers are serialized by ``_lock``. Indexed readers only
        # need to exclude the very short post-commit publication window, not a
        # potentially fsync-bound SQLite transaction.
        self._index_lock = RLock()
        self._index_writer_waiting = False
        self._fault_injector = fault_injector
        self._commit_latencies_ms: deque[float] = deque(maxlen=4096)
        self._checkpoint_latencies_ms: deque[float] = deque(maxlen=256)
        self._commits = 0
        self._write_rows = 0
        self._retention_actions = 0
        self._queued_writers = 0
        self._read_only = bool(read_only)
        self._deferred_read_only = bool(deferred_read_only)
        self._conn: sqlite3.Connection | None = None
        if self._read_only and not self._deferred_read_only:
            self._open_read_only()
        elif not self._read_only:
            self._open_writer()
        try:
            metrics = (
                {}
                if self._deferred_read_only
                else json.loads(self.metrics_path.read_text())
            )
            self._queue_rejections = max(0, int(metrics.get("rejections") or 0))
        except OSError, ValueError, TypeError:
            self._queue_rejections = 0
        if not self._deferred_read_only:
            self._load()

    @property
    def read_only(self) -> bool:
        return self._read_only

    @property
    def deferred_read_only(self) -> bool:
        return self._deferred_read_only

    def _open_writer(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        previous_read_only = self._read_only
        self._conn = sqlite3.connect(
            self.db_path, timeout=30.0, check_same_thread=False
        )
        self._read_only = False
        try:
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=30000")
            self._ensure_schema()
            self._migrate_legacy_if_needed()
        except BaseException:
            self._conn.close()
            self._conn = None
            self._read_only = previous_read_only
            raise

    def _open_read_only(self) -> None:
        """Open an existing database without issuing PRAGMA, DDL, or metadata writes."""

        if not self.db_path.exists():
            self._conn = None
            return
        self._conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro",
            uri=True,
            timeout=5.0,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row

    def promote_writer(self) -> None:
        """Upgrade the facade only after the process owns the PA data-dir writer lock."""

        with self._lock:
            if not self._read_only:
                return
            was_deferred = self._deferred_read_only
            if self._conn is not None:
                self._conn.close()
            self._conn = None
            try:
                self._open_writer()
                self._load()
            except BaseException:
                if self._conn is not None:
                    self._conn.close()
                self._conn = None
                self._read_only = True
                self._deferred_read_only = was_deferred
                raise
            self._deferred_read_only = False

    def _require_readable(self) -> None:
        if self._deferred_read_only:
            raise DispatchStoreReadOnlyError(
                "dispatch reads are deferred in this auxiliary process; "
                "use the running PA server API"
            )

    def _require_writer(self) -> sqlite3.Connection:
        if self._read_only or self._conn is None:
            raise DispatchStoreReadOnlyError(
                "dispatch storage is read-only in this auxiliary process"
            )
        return self._conn

    def _fault(self, boundary: str) -> None:
        if self._fault_injector:
            self._fault_injector(boundary)

    @contextmanager
    def _transaction(self, *, durability: str = "normal"):
        """Commit one mutation and record time only after SQLite acknowledges it."""
        conn = self._require_writer()
        self._queued_writers += 1
        started = time.perf_counter()
        committed = False
        try:
            if durability == "full":
                conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                self._fault("commit_before")
                conn.commit()
                committed = True
                self._fault("commit_after")
            except BaseException as exc:
                if committed:
                    exc.__dict__["pa_transaction_committed"] = True
                else:
                    conn.rollback()
                raise
        finally:
            if durability == "full":
                conn.execute("PRAGMA synchronous=NORMAL")
            self._queued_writers = max(0, self._queued_writers - 1)
            elapsed = (time.perf_counter() - started) * 1000
            if committed:
                self._commit_latencies_ms.append(elapsed)
                self._commits += 1

    def _ensure_schema(self) -> None:
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS dispatch_meta "
            "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        existing = self._conn.execute(
            "SELECT value FROM dispatch_meta WHERE key='schema_version'"
        ).fetchone()
        if existing:
            try:
                observed_version = int(existing["value"])
            except (TypeError, ValueError) as exc:
                raise RuntimeError("invalid dispatch database schema version") from exc
            if observed_version > self.SCHEMA_VERSION:
                raise RuntimeError(
                    "dispatch database was written by a newer PA version; "
                    "refusing unsafe downgrade"
                )
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dispatch_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS dispatches (
                dispatch_id TEXT PRIMARY KEY,
                card_id TEXT,
                project_id TEXT,
                session_id TEXT,
                authority_instance_id TEXT NOT NULL,
                target_instance_id TEXT NOT NULL,
                state TEXT NOT NULL,
                idempotency_key TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS dispatches_card_recent
                ON dispatches(card_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS dispatches_project_recent
                ON dispatches(project_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS dispatches_session_recent
                ON dispatches(session_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS dispatches_authority_idem
                ON dispatches(authority_instance_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS dispatches_target_state_recent
                ON dispatches(target_instance_id, state, updated_at DESC);
            CREATE INDEX IF NOT EXISTS dispatches_target_idem
                ON dispatches(target_instance_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS dispatches_state_recent
                ON dispatches(state, updated_at DESC);

            CREATE TABLE IF NOT EXISTS dispatch_progress_events (
                dispatch_id TEXT NOT NULL REFERENCES dispatches(dispatch_id)
                    ON DELETE CASCADE,
                sequence INTEGER NOT NULL,
                idempotency_key TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                byte_size INTEGER NOT NULL,
                protected INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL,
                PRIMARY KEY(dispatch_id, idempotency_key),
                UNIQUE(dispatch_id, sequence)
            );
            CREATE INDEX IF NOT EXISTS progress_dispatch_recent
                ON dispatch_progress_events(dispatch_id, occurred_at DESC);

            CREATE TABLE IF NOT EXISTS dispatch_heartbeats (
                dispatch_id TEXT PRIMARY KEY REFERENCES dispatches(dispatch_id)
                    ON DELETE CASCADE,
                sequence INTEGER NOT NULL,
                idempotency_key TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS heartbeat_recent
                ON dispatch_heartbeats(occurred_at DESC);

            CREATE TABLE IF NOT EXISTS dispatch_receipts (
                dispatch_id TEXT NOT NULL REFERENCES dispatches(dispatch_id)
                    ON DELETE CASCADE,
                idempotency_key TEXT NOT NULL,
                kind TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                payload_hash TEXT NOT NULL,
                result_status TEXT NOT NULL,
                accepted INTEGER NOT NULL,
                result_json TEXT,
                created_at TEXT NOT NULL,
                replay_expires_at TEXT,
                PRIMARY KEY(dispatch_id, idempotency_key)
            );
            CREATE INDEX IF NOT EXISTS receipts_expiry
                ON dispatch_receipts(replay_expires_at);
            """
        )
        receipt_columns = {
            row["name"]
            for row in self._conn.execute(
                "PRAGMA table_info(dispatch_receipts)"
            ).fetchall()
        }
        if "result_json" not in receipt_columns:
            self._conn.execute(
                "ALTER TABLE dispatch_receipts ADD COLUMN result_json TEXT"
            )
        self._conn.execute(
            "INSERT INTO dispatch_meta(key,value) VALUES('schema_version',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(self.SCHEMA_VERSION),),
        )
        self._conn.commit()

    @staticmethod
    def _checksum(raw: bytes) -> str:
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _progress_protected(event: DispatchProgressEventV1) -> bool:
        return bool(
            event.kind.value == "final"
            or event.operator_input
            or event.blockers
            or event.phase.value
            in {
                "blocked",
                "turn_ended",
                "completed",
                "opening_pr",
                "waiting_ci",
                "addressing_review",
                "merging",
            }
        )

    @staticmethod
    def _core_payload(record: DispatchRecord) -> str:
        return json.dumps(
            record.model_dump(
                mode="json",
                exclude={
                    "progress_events",
                    "progress_heartbeat",
                    "progress_seen_keys",
                },
            ),
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def _validated_legacy_record(
        cls, key: str, value: Any
    ) -> DispatchRecord | None:
        if not isinstance(value, dict):
            logger.warning("Ignoring malformed persisted dispatch %s", key)
            return None
        candidate = dict(value)
        events: list[dict[str, Any]] = []
        for raw_event in candidate.get("progress_events") or []:
            try:
                events.append(
                    DispatchProgressEventV1.model_validate(raw_event).model_dump(
                        mode="json"
                    )
                )
            except (ValueError, TypeError):
                logger.warning(
                    "Ignoring malformed historical progress event for dispatch %s",
                    key,
                )
        candidate["progress_events"] = events
        for field, model in (
            ("progress_heartbeat", DispatchProgressHeartbeatV1),
            ("final_report", CompletionReportV1),
        ):
            raw = candidate.get(field)
            if raw is None:
                continue
            try:
                candidate[field] = model.model_validate(raw).model_dump(mode="json")
            except (ValueError, TypeError):
                logger.warning("Ignoring malformed historical %s for %s", field, key)
                candidate[field] = None
        try:
            return DispatchRecord.model_validate(candidate)
        except (ValueError, TypeError) as exc:
            logger.warning(
                "Ignoring malformed persisted dispatch %s (%s)",
                key,
                exc.__class__.__name__,
            )
            return None

    def _migration_meta(self) -> dict[str, Any] | None:
        if self._conn is None:
            return None
        try:
            row = self._conn.execute(
                "SELECT value FROM dispatch_meta WHERE key='legacy_migration'"
            ).fetchone()
        except sqlite3.OperationalError:
            return None
        return json.loads(row["value"]) if row else None

    def _migrate_legacy_if_needed(self) -> None:
        migration = self._migration_meta()
        if migration and migration.get("state") == "verified":
            if self.path.exists():
                checksum = self._checksum(self.path.read_bytes())
                if checksum != migration.get("source_sha256"):
                    raise RuntimeError(
                        "legacy dispatch_mutations.json changed after verified SQLite "
                        "migration; refusing mixed-version startup"
                    )
            return
        existing = self._conn.execute("SELECT COUNT(*) FROM dispatches").fetchone()[0]
        if (
            migration
            and migration.get("state") == "not_needed"
            and self.path.exists()
            and existing
        ):
            raise RuntimeError(
                "legacy dispatch_mutations.json appeared after SQLite initialization; "
                "refusing mixed-version startup"
            )
        if not self.path.exists():
            if existing:
                return
            state = {
                "state": "not_needed",
                "schema_version": self.SCHEMA_VERSION,
                "verified_at": datetime.now(UTC).isoformat(),
            }
            with self._transaction(durability="full") as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO dispatch_meta(key,value) VALUES(?,?)",
                    ("legacy_migration", json.dumps(state, sort_keys=True)),
                )
            return

        raw = self.path.read_bytes()
        checksum = self._checksum(raw)
        try:
            payload = json.loads(raw)
        except (ValueError, TypeError) as exc:
            raise RuntimeError("legacy dispatch ledger is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise TypeError("legacy dispatch ledger root must be an object")

        if self.backup_path.exists():
            if self._checksum(self.backup_path.read_bytes()) != checksum:
                raise RuntimeError("legacy dispatch backup checksum mismatch")
        else:
            atomic_write_text(self.backup_path, raw.decode("utf-8"))
        self._fault("migration_backup_verified")

        records = [
            record
            for key, value in payload.items()
            if (record := self._validated_legacy_record(str(key), value)) is not None
        ]
        expected_events = sum(len(record.progress_events) for record in records)
        expected_heartbeats = sum(record.progress_heartbeat is not None for record in records)
        expected_receipts = sum(len(set(record.progress_seen_keys)) for record in records)
        expected_finals = sum(record.final_report is not None for record in records)
        self._fault("migration_before_commit")
        with self._transaction(durability="full") as conn:
            conn.execute("DELETE FROM dispatches")
            for record in records:
                self._persist_record_conn(conn, record)
                for event in record.progress_events:
                    self._persist_event_conn(conn, event)
                if record.progress_heartbeat:
                    self._persist_heartbeat_conn(conn, record.progress_heartbeat)
                event_by_key = {
                    event.idempotency_key: event for event in record.progress_events
                }
                heartbeat = record.progress_heartbeat
                for key in set(record.progress_seen_keys):
                    item = event_by_key.get(key)
                    kind = "progress"
                    sequence = item.sequence if item else 0
                    if heartbeat and heartbeat.idempotency_key == key:
                        kind, sequence = "heartbeat", heartbeat.sequence
                    legacy_result = ProgressIngestResult(
                        accepted=True,
                        status="accepted",
                        dispatch_id=record.dispatch_id,
                        sequence=sequence,
                        idempotency_key=key,
                    )
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO dispatch_receipts(
                            dispatch_id,idempotency_key,kind,sequence,payload_hash,
                            result_status,accepted,result_json,created_at,replay_expires_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,NULL)
                        """,
                        (
                            record.dispatch_id,
                            key,
                            kind,
                            sequence,
                            "legacy",
                            "accepted",
                            1,
                            legacy_result.model_dump_json(),
                            record.updated_at.isoformat(),
                        ),
                    )
            observed = {
                "dispatches": conn.execute("SELECT COUNT(*) FROM dispatches").fetchone()[0],
                "progress_events": conn.execute(
                    "SELECT COUNT(*) FROM dispatch_progress_events"
                ).fetchone()[0],
                "heartbeats": conn.execute(
                    "SELECT COUNT(*) FROM dispatch_heartbeats"
                ).fetchone()[0],
                "receipts": conn.execute(
                    "SELECT COUNT(*) FROM dispatch_receipts"
                ).fetchone()[0],
                "final_reports": sum(record.final_report is not None for record in records),
            }
            expected = {
                "dispatches": len(records),
                "progress_events": expected_events,
                "heartbeats": expected_heartbeats,
                "receipts": expected_receipts,
                "final_reports": expected_finals,
            }
            if observed != expected:
                raise RuntimeError(
                    f"dispatch migration reconciliation failed expected={expected} "
                    f"observed={observed}"
                )
            state = {
                "state": "verified",
                "schema_version": self.SCHEMA_VERSION,
                "source": self.path.name,
                "backup": self.backup_path.name,
                "source_sha256": checksum,
                "backup_sha256": checksum,
                "counts": observed,
                "verified_at": datetime.now(UTC).isoformat(),
            }
            conn.execute(
                "INSERT OR REPLACE INTO dispatch_meta(key,value) VALUES(?,?)",
                ("legacy_migration", json.dumps(state, sort_keys=True)),
            )
        self._fault("migration_after_commit")

    @staticmethod
    def _prefer_card_record(candidate: DispatchRecord, current: DispatchRecord) -> bool:
        active_states = {
            "waiting_capacity",
            "blocked",
            "queued",
            "checking_sync",
            "materializing",
            "provisioning",
            "starting_session",
            "delivering_prompt",
            "dispatching",
            "dispatched",
            "materialized",
            "running",
            "completion_pending",
        }
        return (candidate.state in active_states) > (
            current.state in active_states
        ) or (
            (candidate.state in active_states) == (current.state in active_states)
            and candidate.updated_at > current.updated_at
        )

    def _rebuild_latest_card_records_locked(self) -> None:
        selected: dict[str, DispatchRecord] = {}
        for record in self._records.values():
            if not record.card_id:
                continue
            current = selected.get(record.card_id)
            if current is None or self._prefer_card_record(record, current):
                selected[record.card_id] = record
        self._latest_card_records = selected

    @staticmethod
    def _snapshot(record: DispatchRecord) -> DispatchRecord:
        return record.model_copy(deep=True)

    def _update_latest_card_record_locked(
        self,
        candidate: DispatchRecord,
        previous: DispatchRecord | None = None,
    ) -> None:
        if previous and previous.card_id and previous.card_id != candidate.card_id:
            peers = [
                record
                for record in self._records.values()
                if record.card_id == previous.card_id
            ]
            if peers:
                selected = peers[0]
                for record in peers[1:]:
                    if self._prefer_card_record(record, selected):
                        selected = record
                self._latest_card_records[previous.card_id] = selected
            else:
                self._latest_card_records.pop(previous.card_id, None)
        if not candidate.card_id:
            return
        current = self._latest_card_records.get(candidate.card_id)
        if current is None or self._prefer_card_record(candidate, current):
            self._latest_card_records[candidate.card_id] = candidate
            return
        if current.dispatch_id == candidate.dispatch_id:
            if previous and previous.state == candidate.state:
                self._latest_card_records[candidate.card_id] = candidate
                return
            peers = [
                record
                for record in self._records.values()
                if record.card_id == candidate.card_id
            ]
            selected = candidate
            for record in peers:
                if self._prefer_card_record(record, selected):
                    selected = record
            self._latest_card_records[candidate.card_id] = selected

    def _rebuild_latest_session_records_locked(self) -> None:
        selected: dict[tuple[str, str], DispatchRecord] = {}
        global_selected: dict[str, DispatchRecord] = {}
        for record in self._records.values():
            if not record.session_id:
                continue
            key = (record.realm_id, record.session_id)
            current = selected.get(key)
            if current is None or self._prefer_card_record(record, current):
                selected[key] = record
            global_current = global_selected.get(record.session_id)
            if global_current is None or self._prefer_card_record(
                record, global_current
            ):
                global_selected[record.session_id] = record
        self._latest_session_records = selected
        self._latest_session_records_global = global_selected

    def _update_latest_session_record_locked(
        self,
        candidate: DispatchRecord,
        previous: DispatchRecord | None = None,
    ) -> None:
        changed_identity = bool(
            previous
            and (previous.realm_id, previous.session_id)
            != (candidate.realm_id, candidate.session_id)
        )
        if changed_identity or (
            previous
            and previous.session_id
            and previous.state != candidate.state
            and self._latest_session_records_global.get(previous.session_id)
            and self._latest_session_records_global[previous.session_id].dispatch_id
            == candidate.dispatch_id
        ):
            self._rebuild_latest_session_records_locked()
            return
        if not candidate.session_id:
            return
        key = (candidate.realm_id, candidate.session_id)
        current = self._latest_session_records.get(key)
        if (
            current is None
            or current.dispatch_id == candidate.dispatch_id
            or self._prefer_card_record(candidate, current)
        ):
            self._latest_session_records[key] = candidate
        global_current = self._latest_session_records_global.get(candidate.session_id)
        if (
            global_current is None
            or global_current.dispatch_id == candidate.dispatch_id
            or self._prefer_card_record(candidate, global_current)
        ):
            self._latest_session_records_global[candidate.session_id] = candidate

    def _rebuild_history_counts_locked(self) -> None:
        counts: dict[tuple[str, str], int] = {}
        for record in self._records.values():
            if record.card_id:
                key = (record.realm_id, record.card_id)
                counts[key] = counts.get(key, 0) + 1
        self._history_counts = counts

    def _rebuild_capacity_records_locked(self) -> None:
        indexed: dict[str, list[DispatchRecord]] = {}
        capacity_states = CAPACITY_RESERVATION_STATES | QUEUE_CONSUMING_STATES
        for record in self._records.values():
            if record.state not in capacity_states:
                continue
            indexed.setdefault(record.target_instance_id, []).append(record)
        self._capacity_records_by_target = {
            target: tuple(
                sorted(records, key=lambda item: item.updated_at, reverse=True)
            )
            for target, records in indexed.items()
        }

    @staticmethod
    def _goal_lifecycle_record_pending(record: DispatchRecord) -> bool:
        lifecycle_states = {
            "admission_pending",
            "running",
            "completion_pending",
            "completed",
            "failed",
            "cancelled",
        }
        execution_identity_check_pending = bool(
            record.goal_provenance is not None
            and record.goal_provenance.released_at is None
            and record.state in RECOVERABLE_DISPATCH_STATES
            and (
                record.session_id is not None
                or record.goal_provenance.execution_identity is not None
            )
        )
        base_pending = bool(
            record.goal_provenance is not None
            and record.goal_provenance.released_at is None
            and record.goal_admission_validation_state != "rejected"
            and record.state in lifecycle_states
        )
        followup_pending = any(
            operation.get("state") == "governance_pending"
            or (
                operation.get("goal_provenance") is not None
                and not (operation.get("goal_provenance") or {}).get("released_at")
            )
            for operation in record.followup_operations.values()
        )
        return base_pending or followup_pending or execution_identity_check_pending

    def _rebuild_goal_lifecycle_records_locked(self) -> None:
        self._goal_lifecycle_records = {
            record.dispatch_id: record
            for record in self._records.values()
            if self._goal_lifecycle_record_pending(record)
        }

    def _update_goal_lifecycle_record_locked(
        self,
        candidate: DispatchRecord,
    ) -> None:
        if self._goal_lifecycle_record_pending(candidate):
            self._goal_lifecycle_records[candidate.dispatch_id] = candidate
        else:
            self._goal_lifecycle_records.pop(candidate.dispatch_id, None)

    def _update_history_count_locked(
        self,
        record: DispatchRecord,
        previous: DispatchRecord | None,
    ) -> None:
        previous_key = (
            (previous.realm_id, previous.card_id)
            if previous and previous.card_id
            else None
        )
        next_key = (record.realm_id, record.card_id) if record.card_id else None
        if previous_key == next_key:
            return
        if previous_key:
            remaining = self._history_counts.get(previous_key, 1) - 1
            if remaining > 0:
                self._history_counts[previous_key] = remaining
            else:
                self._history_counts.pop(previous_key, None)
        if next_key:
            self._history_counts[next_key] = self._history_counts.get(next_key, 0) + 1

    def _update_capacity_record_locked(
        self,
        candidate: DispatchRecord,
        previous: DispatchRecord | None,
    ) -> None:
        targets = {candidate.target_instance_id}
        if previous:
            targets.add(previous.target_instance_id)
        capacity_states = CAPACITY_RESERVATION_STATES | QUEUE_CONSUMING_STATES
        for target in targets:
            records = [
                record
                for record in self._capacity_records_by_target.get(target, ())
                if record.dispatch_id != candidate.dispatch_id
            ]
            if (
                candidate.target_instance_id == target
                and candidate.state in capacity_states
            ):
                records.append(candidate)
            if records:
                self._capacity_records_by_target[target] = tuple(
                    sorted(records, key=lambda item: item.updated_at, reverse=True)
                )
            else:
                self._capacity_records_by_target.pop(target, None)

    def _publish_records_locked(self, records: list[DispatchRecord]) -> None:
        self._index_writer_waiting = True
        try:
            with self._index_lock:
                for source in records:
                    candidate = self._snapshot(source)
                    previous = self._records.get(candidate.dispatch_id)
                    self._records[candidate.dispatch_id] = candidate
                    self._update_history_count_locked(candidate, previous)
                    self._update_latest_card_record_locked(candidate, previous)
                    self._update_latest_session_record_locked(candidate, previous)
                    self._update_capacity_record_locked(candidate, previous)
                    self._update_goal_lifecycle_record_locked(candidate)
                self._refresh_queue_positions_locked()
        finally:
            self._index_writer_waiting = False

    def _yield_to_index_writer(self) -> None:
        # CPython locks do not promise waiter fairness. A hot polling reader can
        # otherwise reacquire the index continuously and starve the short
        # post-commit publication section (or the SQLite thread reacquiring the
        # interpreter after an I/O call). One scheduler yield preserves reader
        # availability without making it wait behind the whole writer queue.
        if self._queued_writers:
            time.sleep(0.0001)
        while self._index_writer_waiting:
            time.sleep(0)

    def _record_queue_rejection_locked(self) -> None:
        self._require_writer()
        self._queue_rejections += 1
        atomic_write_json(
            self.metrics_path,
            {
                "rejections": self._queue_rejections,
                "updated_at": datetime.now(UTC).isoformat(),
            },
        )

    def _load_legacy_snapshot(self) -> dict[str, DispatchRecord]:
        try:
            payload = json.loads(self.path.read_text())
        except (OSError, ValueError, TypeError):
            return {}
        if not isinstance(payload, dict):
            return {}
        return {
            str(key): record
            for key, value in payload.items()
            if (record := self._validated_legacy_record(str(key), value)) is not None
        }

    def _load(self) -> None:
        if self._conn is None:
            self._records = self._load_legacy_snapshot()
            self._rebuild_latest_card_records_locked()
            self._rebuild_latest_session_records_locked()
            self._rebuild_history_counts_locked()
            self._rebuild_capacity_records_locked()
            self._rebuild_goal_lifecycle_records_locked()
            self._refresh_queue_positions_locked()
            return
        if self._read_only:
            try:
                schema = self._conn.execute(
                    "SELECT value FROM dispatch_meta WHERE key='schema_version'"
                ).fetchone()
            except sqlite3.OperationalError:
                self._records = self._load_legacy_snapshot()
                self._rebuild_latest_card_records_locked()
                self._rebuild_latest_session_records_locked()
                self._rebuild_history_counts_locked()
                self._rebuild_capacity_records_locked()
                self._rebuild_goal_lifecycle_records_locked()
                self._refresh_queue_positions_locked()
                return
            if schema and int(schema["value"]) > self.SCHEMA_VERSION:
                raise RuntimeError(
                    "dispatch database was written by a newer PA version; "
                    "refusing unsafe downgrade"
                )

        records: dict[str, DispatchRecord] = {}
        for row in self._conn.execute(
            "SELECT dispatch_id, payload_json FROM dispatches"
        ).fetchall():
            try:
                records[row["dispatch_id"]] = DispatchRecord.model_validate_json(
                    row["payload_json"]
                )
            except (ValueError, TypeError):
                logger.exception("Invalid dispatch row %s", row["dispatch_id"])
                raise RuntimeError("dispatch database contains an invalid record")

        for row in self._conn.execute(
            "SELECT dispatch_id, payload_json FROM dispatch_progress_events "
            "ORDER BY dispatch_id, sequence, occurred_at, idempotency_key"
        ).fetchall():
            record = records.get(row["dispatch_id"])
            if record:
                record.progress_events.append(
                    DispatchProgressEventV1.model_validate_json(row["payload_json"])
                )
        for row in self._conn.execute(
            "SELECT dispatch_id, payload_json FROM dispatch_heartbeats"
        ).fetchall():
            record = records.get(row["dispatch_id"])
            if record:
                record.progress_heartbeat = DispatchProgressHeartbeatV1.model_validate_json(
                    row["payload_json"]
                )
        for dispatch_id, record in records.items():
            rows = self._conn.execute(
                "SELECT idempotency_key FROM dispatch_receipts WHERE dispatch_id=? "
                "ORDER BY created_at DESC LIMIT ?",
                (dispatch_id, MAX_PROGRESS_SEEN_KEYS),
            ).fetchall()
            record.progress_seen_keys = [row["idempotency_key"] for row in reversed(rows)]
        self._records = records
        self._rebuild_latest_card_records_locked()
        migrated = False
        self._rebuild_latest_session_records_locked()
        self._rebuild_history_counts_locked()
        self._rebuild_capacity_records_locked()
        self._rebuild_goal_lifecycle_records_locked()
        self._refresh_queue_positions_locked()
        for record in self._records.values():
            if (
                record.card_id
                and record.state in {"completed", "acknowledged"}
                and record.card_disposition_status is None
            ):
                record.card_disposition_status = "legacy_unrecorded"
                record.card_disposition_reason = (
                    "This dispatch completed before the card-disposition contract; "
                    "the stored card lane was left unchanged."
                )
                migrated = True
        if migrated and not self._read_only:
            self._save(self._records.values())

    def _persist_record_conn(
        self, conn: sqlite3.Connection, record: DispatchRecord
    ) -> None:
        conn.execute(
            """
            INSERT INTO dispatches(
                dispatch_id,card_id,project_id,session_id,authority_instance_id,
                target_instance_id,state,idempotency_key,created_at,updated_at,payload_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(dispatch_id) DO UPDATE SET
                card_id=excluded.card_id,
                project_id=excluded.project_id,
                session_id=excluded.session_id,
                authority_instance_id=excluded.authority_instance_id,
                target_instance_id=excluded.target_instance_id,
                state=excluded.state,
                idempotency_key=excluded.idempotency_key,
                created_at=excluded.created_at,
                updated_at=excluded.updated_at,
                payload_json=excluded.payload_json
            """,
            (
                record.dispatch_id,
                record.card_id,
                record.project_id,
                record.session_id,
                record.authority_instance_id,
                record.target_instance_id,
                record.state,
                record.idempotency_key,
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                self._core_payload(record),
            ),
        )

    def _persist_event_conn(
        self, conn: sqlite3.Connection, event: DispatchProgressEventV1
    ) -> None:
        payload = event.model_dump_json()
        conn.execute(
            """
            INSERT INTO dispatch_progress_events(
                dispatch_id,sequence,idempotency_key,occurred_at,byte_size,protected,payload_json
            ) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(dispatch_id,idempotency_key) DO UPDATE SET
                sequence=excluded.sequence,
                occurred_at=excluded.occurred_at,
                byte_size=excluded.byte_size,
                protected=excluded.protected,
                payload_json=excluded.payload_json
            """,
            (
                event.dispatch_id,
                event.sequence,
                event.idempotency_key,
                event.occurred_at.isoformat(),
                len(payload.encode()),
                int(self._progress_protected(event)),
                payload,
            ),
        )

    @staticmethod
    def _persist_heartbeat_conn(
        conn: sqlite3.Connection, heartbeat: DispatchProgressHeartbeatV1
    ) -> None:
        conn.execute(
            """
            INSERT INTO dispatch_heartbeats(
                dispatch_id,sequence,idempotency_key,occurred_at,payload_json
            ) VALUES(?,?,?,?,?)
            ON CONFLICT(dispatch_id) DO UPDATE SET
                sequence=excluded.sequence,
                idempotency_key=excluded.idempotency_key,
                occurred_at=excluded.occurred_at,
                payload_json=excluded.payload_json
            """,
            (
                heartbeat.dispatch_id,
                heartbeat.sequence,
                heartbeat.idempotency_key,
                heartbeat.occurred_at.isoformat(),
                heartbeat.model_dump_json(),
            ),
        )

    @staticmethod
    def _canonical_json(value: Any) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)

    @classmethod
    def _payload_hash(cls, payload: BaseModel | dict[str, Any]) -> str:
        if isinstance(payload, (DispatchProgressEventV1, DispatchProgressHeartbeatV1)):
            value = payload.transport_dict()
        elif isinstance(payload, BaseModel):
            value = payload.model_dump(mode="json")
        else:
            value = payload
        return hashlib.sha256(cls._canonical_json(value).encode()).hexdigest()

    def _receipt_row(self, dispatch_id: str, idempotency_key: str):
        if self._conn is None:
            return None
        return self._conn.execute(
            """
            SELECT kind,sequence,payload_hash,result_status,accepted,result_json
            FROM dispatch_receipts
            WHERE dispatch_id=? AND idempotency_key=?
            """,
            (dispatch_id, idempotency_key),
        ).fetchone()

    def _replay_progress_receipt(
        self,
        payload: DispatchProgressEventV1 | DispatchProgressHeartbeatV1,
    ) -> ProgressIngestResult | None:
        row = self._receipt_row(payload.dispatch_id, payload.idempotency_key)
        if row is None:
            return None
        payload_hash = self._payload_hash(payload)
        if row["payload_hash"] not in {"legacy", payload_hash}:
            raise DispatchReceiptConflict(
                payload.dispatch_id,
                payload.idempotency_key,
                "the idempotency key was committed for a different payload",
            )
        if row["kind"] not in {payload.kind.value, "progress"}:
            raise DispatchReceiptConflict(
                payload.dispatch_id,
                payload.idempotency_key,
                "the idempotency key was committed for a different mutation kind",
            )
        if row["result_json"]:
            return ProgressIngestResult.model_validate_json(row["result_json"])
        return ProgressIngestResult(
            accepted=bool(row["accepted"]),
            status=row["result_status"],
            dispatch_id=payload.dispatch_id,
            sequence=int(row["sequence"]),
            idempotency_key=payload.idempotency_key,
        )

    def _persist_receipt_conn(
        self,
        conn: sqlite3.Connection,
        payload: DispatchProgressEventV1 | DispatchProgressHeartbeatV1,
        result: ProgressIngestResult,
    ) -> None:
        conn.execute(
            """
            INSERT INTO dispatch_receipts(
                dispatch_id,idempotency_key,kind,sequence,payload_hash,
                result_status,accepted,result_json,created_at,replay_expires_at
            ) VALUES(?,?,?,?,?,?,?,?,?,NULL)
            """,
            (
                payload.dispatch_id,
                payload.idempotency_key,
                payload.kind.value,
                payload.sequence,
                self._payload_hash(payload),
                result.status,
                int(result.accepted),
                result.model_dump_json(),
                datetime.now(UTC).isoformat(),
            ),
        )

    def _replay_control_receipt(
        self,
        dispatch_id: str,
        idempotency_key: str,
        operation: str,
        payload: dict[str, Any],
    ) -> DispatchRecord | None:
        row = self._receipt_row(dispatch_id, idempotency_key)
        if row is None:
            return None
        expected_hash = self._payload_hash(
            {"operation": operation, "payload": payload}
        )
        if row["kind"] != f"control:{operation}" or row["payload_hash"] != expected_hash:
            raise DispatchReceiptConflict(
                dispatch_id,
                idempotency_key,
                "the control idempotency key was committed for different parameters",
            )
        if not row["result_json"]:
            raise RuntimeError("committed control receipt is missing its canonical result")
        return DispatchRecord.model_validate_json(row["result_json"])

    def _persist_control_receipt_conn(
        self,
        conn: sqlite3.Connection,
        record: DispatchRecord,
        *,
        idempotency_key: str,
        operation: str,
        payload: dict[str, Any],
    ) -> None:
        conn.execute(
            """
            INSERT INTO dispatch_receipts(
                dispatch_id,idempotency_key,kind,sequence,payload_hash,
                result_status,accepted,result_json,created_at,replay_expires_at
            ) VALUES(?,?,?,?,?,?,?,?,?,NULL)
            """,
            (
                record.dispatch_id,
                idempotency_key,
                f"control:{operation}",
                0,
                self._payload_hash({"operation": operation, "payload": payload}),
                "accepted",
                1,
                record.model_dump_json(),
                datetime.now(UTC).isoformat(),
            ),
        )

    def _save(
        self, records: DispatchRecord | Any | None = None, *, durability: str = "normal"
    ) -> None:
        """Persist only explicitly changed dispatch rows, never the full ledger."""
        if records is None:
            raise RuntimeError("incremental dispatch save requires explicit records")
        if isinstance(records, DispatchRecord):
            selected = [records]
        else:
            selected = list(records)
        try:
            with self._transaction(durability=durability) as conn:
                for record in selected:
                    self._persist_record_conn(conn, record)
        except BaseException as exc:
            if getattr(exc, "pa_transaction_committed", False):
                self._publish_records_locked(selected)
                self._write_rows += len(selected)
            raise
        self._publish_records_locked(selected)
        self._write_rows += len(selected)

    def _save_control(
        self,
        record: DispatchRecord,
        *,
        idempotency_key: str,
        operation: str,
        payload: dict[str, Any],
        durability: str = "full",
    ) -> DispatchRecord:
        try:
            with self._transaction(durability=durability) as conn:
                self._persist_record_conn(conn, record)
                self._persist_control_receipt_conn(
                    conn,
                    record,
                    idempotency_key=idempotency_key,
                    operation=operation,
                    payload=payload,
                )
        except BaseException as exc:
            if getattr(exc, "pa_transaction_committed", False):
                self._publish_records_locked([record])
                self._write_rows += 2
            raise
        self._publish_records_locked([record])
        self._write_rows += 2
        return self._snapshot(self._records[record.dispatch_id])

    def get(self, dispatch_id: str) -> DispatchRecord | None:
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            record = self._records.get(dispatch_id)
            return self._snapshot(record) if record else None

    def list(
        self,
        *,
        target_instance_id: str | None = None,
        realm_id: str | None = None,
        card_id: str | None = None,
        limit: int = 100,
        deep: bool = True,
    ) -> list[DispatchRecord]:
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            self._refresh_queue_positions_locked()
            records = list(self._records.values())
        if target_instance_id:
            records = [
                record
                for record in records
                if record.target_instance_id == target_instance_id
            ]
        if realm_id:
            records = [record for record in records if record.realm_id == realm_id]
        if card_id:
            records = [record for record in records if record.card_id == card_id]
        selected = sorted(records, key=lambda record: record.updated_at, reverse=True)[
            :limit
        ]
        if deep:
            return [self._snapshot(record) for record in selected]
        return [record.model_copy(deep=False) for record in selected]

    def pending_goal_lifecycle(
        self, authority_instance_id: str, *, limit: int = 100
    ) -> list[DispatchRecord]:
        """Return bounded indexed authority-owned goal lifecycle work."""

        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            records = [
                self._snapshot(record)
                for record in self._goal_lifecycle_records.values()
                if record.authority_instance_id == authority_instance_id
                and record.goal_provenance is not None
                and record.goal_provenance.authority_instance_id
                == authority_instance_id
            ]
        records.sort(key=lambda record: record.updated_at)
        return records[: max(1, min(limit, 1000))]

    def latest_by_card(
        self, card_ids: set[str], *, realm_id: str | None = None
    ) -> dict[str, DispatchRecord]:
        """Return one useful dispatch per requested card without copying history."""
        self._require_readable()
        if not card_ids:
            return {}
        self._yield_to_index_writer()
        with self._index_lock:
            return {
                card_id: self._snapshot(self._latest_card_records[card_id])
                for card_id in card_ids
                if card_id in self._latest_card_records
                and (
                    realm_id is None
                    or self._latest_card_records[card_id].realm_id == realm_id
                )
            }

    def history_counts(self, card_ids: set[str], *, realm_id: str) -> dict[str, int]:
        """Return maintained dispatch-history counts for bounded card ids."""
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            return {
                card_id: self._history_counts.get((realm_id, card_id), 0)
                for card_id in card_ids
            }

    def latest_by_session(
        self, session_ids: set[str], *, realm_id: str
    ) -> dict[str, DispatchRecord]:
        """Return the active-preferred newest dispatch for each requested session."""
        self._require_readable()
        if not session_ids:
            return {}
        self._yield_to_index_writer()
        with self._index_lock:
            return {
                session_id: self._snapshot(
                    self._latest_session_records[(realm_id, session_id)]
                )
                for session_id in session_ids
                if (realm_id, session_id) in self._latest_session_records
            }

    def current_card_ids(self, *, realm_id: str, limit: int) -> list[str]:
        """Return bounded card ids with current dispatch work in one realm."""
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            records = [
                record
                for record in self._latest_card_records.values()
                if record.realm_id == realm_id
                and record.state
                not in {"completed", "acknowledged", "failed", "cancelled"}
            ]
        records.sort(key=lambda record: record.updated_at, reverse=True)
        return [record.card_id for record in records[:limit] if record.card_id]

    def capacity_snapshot(self, target_instance_id: str) -> dict[str, Any]:
        """Return indexed authority reservations and waiting work for one target."""

        self._require_readable()
        if not self._read_only:
            self.expire_capacity_reservations(target_instance_id=target_instance_id)
        self._yield_to_index_writer()
        with self._index_lock:
            records = self._capacity_records_by_target.get(target_instance_id, ())
            reservations = [
                record
                for record in records
                if record.state in CAPACITY_RESERVATION_STATES
            ]
            waiting = [
                record for record in records if record.state in QUEUE_CONSUMING_STATES
            ]
            providers: dict[str, dict[str, int]] = {}
            for record in records:
                provider = (record.capacity_provider or "unknown").lower()
                counts = providers.setdefault(
                    provider, {"dispatch_reservations": 0, "dispatch_waiting": 0}
                )
                if record.state in CAPACITY_RESERVATION_STATES:
                    counts["dispatch_reservations"] += 1
                if record.state in QUEUE_CONSUMING_STATES:
                    counts["dispatch_waiting"] += 1
            projected = reservations[:CAPACITY_CONSUMER_LINK_LIMIT]
            return {
                "dispatch_reservations": len(reservations),
                "dispatch_waiting": len(waiting),
                "provider_concurrency": providers,
                "reservation_links": [
                    {
                        "kind": "dispatch",
                        "dispatch_id": record.dispatch_id,
                        "card_id": record.card_id,
                        "href": (
                            f"/?card={record.card_id}" if record.card_id else "/fleet"
                        ),
                        "state": record.state,
                        "slots": 1,
                    }
                    for record in projected
                ],
                "reservation_links_omitted": max(0, len(reservations) - len(projected)),
            }

    def by_session(self, session_id: str) -> DispatchRecord | None:
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            record = self._latest_session_records_global.get(session_id)
            return self._snapshot(record) if record else None

    def queue_completion_payload(
        self, session_id: str, payload: dict[str, Any]
    ) -> bool:
        """Atomically queue completion against the latest durable session record."""
        with self._lock:
            self._require_writer()
            stored = self._latest_session_records_global.get(session_id)
            record = self._snapshot(stored) if stored else None
            if (
                record
                and record.acknowledged_at
                and record.state in {"completed", "acknowledged"}
            ):
                followup = next(
                    (
                        item
                        for item in reversed(record.followup_turns)
                        if item.get("state") == "accepted"
                    ),
                    None,
                )
                if not followup:
                    return False
                followup.update(
                    {
                        "state": "ended",
                        "ended_at": datetime.now(UTC).isoformat(),
                        "stop_reason": payload.get("stop_reason"),
                        "result": payload,
                        "final_report": (
                            record.final_report.model_dump(mode="json")
                            if record.final_report
                            else None
                        ),
                        "delivery_state": "pending",
                        "delivery_attempts": 0,
                        "next_retry_at": None,
                    }
                )
                self.put(record)
                return True
            late_repair_completion = bool(
                record and record.accepts_late_completion_after_terminal_repair
            )
            if not record or (
                record.state not in {"running", "completion_pending"}
                and not late_repair_completion
            ):
                return False
            reservation = record.terminal_repair_reservation if record else None
            if reservation and reservation.get("state") == "prepared":
                reservation = copy.deepcopy(reservation)
                reservation.update(
                    {
                        "state": "superseded_by_completion",
                        "superseded_at": datetime.now(UTC).isoformat(),
                    }
                )
                record.terminal_repair_reservation = reservation
                record.lifecycle_inconsistencies.append(
                    {
                        "kind": "terminal_repair_reservation_superseded",
                        "observed_at": datetime.now(UTC).isoformat(),
                        "reason": (
                            "Immutable completion arrived before the target-side "
                            "repair reservation was consumed."
                        ),
                        "reservation_id": reservation.get("reservation_id"),
                    }
                )
                record.lifecycle_inconsistencies = record.lifecycle_inconsistencies[
                    -50:
                ]
            if late_repair_completion:
                record.lifecycle_inconsistencies.append(
                    {
                        "kind": "terminal_repair_superseded_by_completion",
                        "observed_at": datetime.now(UTC).isoformat(),
                        "reason": (
                            "Immutable completion arrived after abandonment repair and "
                            "was preserved for authority acknowledgement."
                        ),
                    }
                )
                record.lifecycle_inconsistencies = record.lifecycle_inconsistencies[
                    -50:
                ]
                record.error_code = None
            record.completion_payload = payload
            record.last_error = None
            record.completion_delivery_class = "pending"
            record.completion_next_retry_at = None
            self.transition(
                record,
                "completion_pending",
                (
                    "Agent turn ended; dispatch completion queued for delivery to "
                    "the authority."
                ),
            )
            return True

    def by_idempotency(
        self, target_instance_id: str, idempotency_key: str
    ) -> DispatchRecord | None:
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            record = max(
                (
                    record
                    for record in self._records.values()
                    if record.target_instance_id == target_instance_id
                    and record.idempotency_key == idempotency_key
                ),
                key=lambda item: item.updated_at,
                default=None,
            )
            return self._snapshot(record) if record else None

    def by_authority_idempotency(
        self, authority_instance_id: str, idempotency_key: str
    ) -> DispatchRecord | None:
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            record = max(
                (
                    record
                    for record in self._records.values()
                    if record.authority_instance_id == authority_instance_id
                    and record.idempotency_key == idempotency_key
                ),
                key=lambda item: item.updated_at,
                default=None,
            )
            return self._snapshot(record) if record else None

    def find_operation_by_idempotency(
        self, idempotency_key: str, *, realm_id: str | None = None
    ) -> tuple[str, DispatchRecord] | None:
        """Find a durable dispatch admission or control operation by its key."""
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            matches: list[tuple[str, DispatchRecord]] = []
            for record in self._records.values():
                if realm_id is not None and record.realm_id != realm_id:
                    continue
                if record.idempotency_key == idempotency_key:
                    matches.append(("dispatch.create", record))
                control = record.control_operations.get(idempotency_key)
                if control:
                    matches.append((f"dispatch.{control}", record))
                if idempotency_key in record.followup_operations:
                    matches.append(("dispatch.followup", record))
            if not matches:
                return None
            operation, record = max(
                matches,
                key=lambda item: item[1].updated_at,
            )
            return operation, self._snapshot(record)

    @staticmethod
    def _class_key(record: DispatchRecord) -> str:
        return "|".join(
            (
                record.principal_id or "unknown",
                record.project_id or "no-project",
                record.capacity_provider or "any-provider",
                record.target_instance_id,
            )
        )

    def _refresh_queue_positions_locked(self) -> None:
        waiting = sorted(
            (
                item
                for item in self._records.values()
                if item.state in QUEUE_CONSUMING_STATES
            ),
            key=lambda item: (
                -item.requested_priority,
                item.scheduling_class_sequence,
                item.queue_admitted_at or item.created_at,
                item.dispatch_id,
            ),
        )
        for position, item in enumerate(waiting, 1):
            item.queue_position = position
        for item in self._records.values():
            if item.state not in QUEUE_CONSUMING_STATES:
                item.queue_position = None

    def _constraint_counts_locked(
        self,
        record: DispatchRecord,
        capacity: CapacityAdmission,
        *,
        exclude_dispatch_id: str | None = None,
    ) -> tuple[bool, bool, int, int, int | None]:
        """Evaluate global and provider execution/queue constraints together."""

        records = [
            item
            for item in self._records.values()
            if item.dispatch_id != exclude_dispatch_id
            and item.target_instance_id == record.target_instance_id
        ]

        def local_counts(*, provider_only: bool) -> tuple[int, int, int]:
            scoped = [
                item
                for item in records
                if not provider_only or item.capacity_provider == capacity.provider
            ]
            return (
                sum(item.state == "running" for item in scoped),
                sum(item.state in CAPACITY_RESERVATION_STATES for item in scoped),
                sum(item.state in QUEUE_CONSUMING_STATES for item in scoped),
            )

        global_running, global_reservations, global_waiting = local_counts(
            provider_only=False
        )
        provider_running, provider_reservations, provider_waiting = local_counts(
            provider_only=True
        )
        global_limit = capacity.global_limit or capacity.limit
        global_consumed = max(
            capacity.observed_global_active
            if capacity.observed_global_active is not None
            else capacity.observed_active,
            global_running,
        ) + max(
            capacity.observed_global_reservations
            if capacity.observed_global_reservations is not None
            else capacity.observed_reservations,
            global_reservations,
        )
        provider_consumed = 0
        if capacity.provider_limit is not None:
            provider_consumed = max(
                capacity.observed_provider_active or 0,
                provider_running,
            ) + max(
                capacity.observed_provider_reservations or 0,
                provider_reservations,
            )
        execution_full = global_consumed >= global_limit or (
            capacity.provider_limit is not None
            and provider_consumed >= capacity.provider_limit
        )

        global_queue_limit = (
            capacity.global_queue_limit
            if capacity.global_queue_limit is not None
            else capacity.queue_limit
        )
        provider_queue_limit = capacity.provider_queue_limit
        global_queue_count = max(
            capacity.observed_global_waiting
            if capacity.observed_global_waiting is not None
            else capacity.observed_waiting,
            global_waiting,
        )
        provider_queue_count = max(
            capacity.observed_provider_waiting or 0,
            provider_waiting,
        )
        queue_full = bool(
            global_queue_limit is not None and global_queue_count >= global_queue_limit
        ) or bool(
            provider_queue_limit is not None
            and provider_queue_count >= provider_queue_limit
        )
        effective_queue_count = (
            provider_queue_count
            if provider_queue_limit is not None
            and provider_queue_count >= provider_queue_limit
            else global_queue_count
        )
        effective_queue_limit = (
            provider_queue_limit
            if provider_queue_limit is not None
            and provider_queue_count >= provider_queue_limit
            else global_queue_limit
        )
        effective_reservations = max(
            capacity.observed_reservations,
            (
                provider_reservations
                if capacity.provider_specific
                else global_reservations
            ),
        )
        return (
            execution_full,
            queue_full,
            effective_queue_count,
            effective_reservations,
            effective_queue_limit,
        )

    def admit(
        self,
        record: DispatchRecord,
        *,
        idempotency_scope: str = "authority",
        capacity: CapacityAdmission | None = None,
    ) -> tuple[DispatchRecord, bool]:
        """Atomically deduplicate and prevent unsafe concurrent card dispatch."""
        with self._lock:
            self._require_writer()
            if idempotency_scope == "target":
                existing = next(
                    (
                        item
                        for item in self._records.values()
                        if item.target_instance_id == record.target_instance_id
                        and item.idempotency_key == record.idempotency_key
                    ),
                    None,
                )
            else:
                existing = next(
                    (
                        item
                        for item in self._records.values()
                        if item.authority_instance_id == record.authority_instance_id
                        and item.idempotency_key == record.idempotency_key
                    ),
                    None,
                )
            pending_existing: DispatchRecord | None = None
            if existing:
                if (
                    existing.state == "admission_pending"
                    and existing.dispatch_id == record.dispatch_id
                    and existing.mutation_id == record.mutation_id
                ):
                    if (
                        existing.placement_request_fingerprint
                        != record.placement_request_fingerprint
                    ):
                        raise DispatchIdempotencyConflict(existing)
                    pending_existing = existing
                else:
                    if existing.request_fingerprint != record.request_fingerprint:
                        raise DispatchIdempotencyConflict(existing)
                    return self._snapshot(existing), True

            if pending_existing is not None and pending_existing.goal_provenance:
                provenance = record.goal_provenance
                if (
                    pending_existing.goal_admission_validation_state != "validated"
                    or not pending_existing.goal_admission_validation_proof
                    or pending_existing.goal_admission_validation_proof
                    != goal_admission_validation_proof(pending_existing)
                    or not goal_dispatch_record_placement_input_valid(pending_existing)
                    or not goal_dispatch_materialization_binding_valid(pending_existing)
                    or not goal_dispatch_execution_identity_valid(pending_existing)
                    or provenance is None
                    or provenance != pending_existing.goal_provenance
                    or record.goal_admission_validation_state != "validated"
                    or record.goal_admission_validated_at
                    != pending_existing.goal_admission_validated_at
                    or record.goal_admission_validation_proof
                    != pending_existing.goal_admission_validation_proof
                    or record.goal_admission_validation_proof
                    != goal_admission_validation_proof(record)
                    or not goal_dispatch_record_placement_input_valid(record)
                    or not goal_dispatch_materialization_binding_valid(record)
                    or not goal_dispatch_execution_identity_valid(record)
                    or provenance.resolved_target_instance_id
                    != record.target_instance_id
                    or provenance.placement_input_digest
                    != record.goal_placement_input_digest
                    or provenance.placement_decision_digest
                    != goal_dispatch_placement_decision_digest(
                        record.placement_decision
                    )
                ):
                    raise ValueError(
                        "governed admission trace must be canonically validated before promotion"
                    )

            self.expire_capacity_reservations()
            record = self._snapshot(record)

            if record.card_id and not record.allow_concurrent:
                active = next(
                    (
                        item
                        for item in self._records.values()
                        if item.dispatch_id != record.dispatch_id
                        and item.card_id == record.card_id
                        and item.realm_id == record.realm_id
                        and item.state
                        not in {"failed", "completed", "cancelled", "acknowledged"}
                    ),
                    None,
                )
                if active:
                    raise ConcurrentCardDispatch(self._snapshot(active))

            admission_state = "queued"
            if capacity:
                now = datetime.now(UTC)
                execution_full, queue_full, queue_count, reserved, queue_max = (
                    self._constraint_counts_locked(
                        record,
                        capacity,
                        exclude_dispatch_id=(
                            pending_existing.dispatch_id
                            if pending_existing is not None
                            else None
                        ),
                    )
                )
                record.capacity_limit = capacity.limit
                record.capacity_source = capacity.source
                record.capacity_provider = capacity.provider
                record.capacity_observed_active = capacity.observed_active
                record.capacity_observed_queued = capacity.observed_queued
                record.capacity_observed_reservations = capacity.observed_reservations
                record.capacity_override = capacity.override
                record.capacity_override_reason = capacity.override_reason
                record.queue_limit = (
                    capacity.queue_limit if capacity.queue_limit is not None else 0
                )
                record.queue_source = capacity.queue_source
                record.queue_provider_specific = capacity.queue_provider_specific
                record.queue_observed_count = queue_count
                record.scheduling_class = self._class_key(record)
                class_sequences = [
                    item.scheduling_class_sequence
                    for item in self._records.values()
                    if item.scheduling_class == record.scheduling_class
                    and item.state not in TERMINAL_DISPATCH_STATES
                ]
                record.scheduling_class_sequence = max(class_sequences, default=0) + 1
                if execution_full and not capacity.override:
                    if capacity.queue_limit is None:
                        raise DispatchCapacityExhausted(
                            limit=capacity.limit,
                            source=capacity.source,
                            provider=capacity.provider,
                            active=capacity.observed_active,
                            queued=capacity.observed_queued,
                            reservations=reserved,
                            observed_at=capacity.observed_at,
                            consumer_links=capacity.consumer_links,
                        )
                    if queue_full:
                        self._record_queue_rejection_locked()
                        raise DispatchQueueFull(
                            limit=queue_max
                            if queue_max is not None
                            else capacity.queue_limit,
                            source=capacity.queue_source,
                            provider=capacity.provider,
                            current=queue_count,
                            active_capacity=capacity.limit,
                            observed_at=capacity.observed_at,
                        )
                    admission_state = "waiting_capacity"
                    record.queue_admitted_at = now
                    record.queue_wait_reason = (
                        f"All {capacity.limit} execution slots are occupied; "
                        "waiting for capacity."
                    )
                    record.queue_audit.append(
                        {
                            "action": "admitted",
                            "at": now.isoformat(),
                            "priority": record.requested_priority,
                            "scheduling_class": record.scheduling_class,
                        }
                    )
                else:
                    record.capacity_reserved_at = now
                    record.capacity_reservation_expires_at = (
                        now + CAPACITY_RESERVATION_TTL
                    )

            record.state = admission_state
            record.events.append(
                DispatchEvent(
                    seq=(record.events[-1].seq + 1 if record.events else 1),
                    state=admission_state,
                    message=(
                        "Dispatch durably queued until execution capacity is available."
                        if admission_state == "waiting_capacity"
                        else "Dispatch admitted for background execution."
                    ),
                )
            )
            record.updated_at = datetime.now(UTC)
            self._save(record, durability="full")
            if capacity:
                logger.info(
                    "fleet capacity reservation admitted dispatch=%s target=%s "
                    "provider=%s limit=%s source=%s active=%s queued=%s "
                    "reservations=%s override=%s",
                    record.dispatch_id,
                    record.target_instance_id,
                    capacity.provider,
                    capacity.limit,
                    capacity.source,
                    capacity.observed_active,
                    capacity.observed_queued,
                    record.capacity_observed_reservations,
                    capacity.override,
                )
                if capacity.override:
                    logger.warning(
                        "fleet capacity override dispatch=%s target=%s reason=%s",
                        record.dispatch_id,
                        record.target_instance_id,
                        capacity.override_reason,
                    )
            return self._snapshot(self._records[record.dispatch_id]), False

    def put(self, record: DispatchRecord) -> DispatchRecord:
        with self._lock:
            self._require_writer()
            existing = self._records.get(record.dispatch_id)
            if existing and existing.mutation_id != record.mutation_id:
                raise ValueError("dispatch id already belongs to another mutation")
            existing_reservation = (
                existing.terminal_repair_reservation if existing else None
            )
            existing_repair_controls = {
                key: operation
                for key, operation in (
                    existing.control_operations.items() if existing else ()
                )
                if operation
                in {
                    "repair_terminal",
                    "repair_terminal:abandoned_without_acknowledgement",
                }
            }
            terminal_repair_present = bool(
                existing
                and (
                    existing_repair_controls
                    or (
                        existing_reservation
                        and existing_reservation.get("state")
                        in {
                            "prepared",
                            "committed",
                            "superseded_by_completion",
                            "aborted",
                        }
                    )
                )
            )
            if terminal_repair_present:
                assert existing is not None
                changed_fields: list[str] = []
                if any(
                    record.control_operations.get(key) != value
                    for key, value in existing_repair_controls.items()
                ):
                    changed_fields.append("control_operations")
                repair_diagnostics = [
                    item
                    for item in existing.lifecycle_inconsistencies
                    if str(item.get("kind") or "").startswith("terminal_repair")
                    or item.get("kind")
                    in {
                        "legacy_abandoned_dispatch_retired",
                        "legacy_terminal_record_repaired",
                    }
                ]
                if any(
                    item not in record.lifecycle_inconsistencies
                    for item in repair_diagnostics
                ):
                    changed_fields.append("lifecycle_inconsistencies")
                repair_events = [
                    event
                    for event in existing.events
                    if bool(event.detail.get("repair"))
                ]
                if any(event not in record.events for event in repair_events):
                    changed_fields.append("events")
                if (
                    existing.capacity_released_at != record.capacity_released_at
                    or existing.capacity_release_reason
                    != record.capacity_release_reason
                ):
                    changed_fields.append("capacity_released_at")
                incoming_reservation = record.terminal_repair_reservation
                if existing_reservation:
                    if (
                        not incoming_reservation
                        or incoming_reservation.get("reservation_id")
                        != existing_reservation.get("reservation_id")
                    ):
                        changed_fields.append("terminal_repair_reservation")
                    else:
                        allowed_reservation_states = {
                            "prepared": {
                                "prepared",
                                "committed",
                                "superseded_by_completion",
                                "aborted",
                            },
                            "committed": {
                                "committed",
                                "superseded_by_completion",
                            },
                            "superseded_by_completion": {
                                "superseded_by_completion"
                            },
                            "aborted": {"aborted"},
                        }
                        existing_state = str(
                            existing_reservation.get("state") or ""
                        )
                        incoming_state = str(
                            incoming_reservation.get("state") or ""
                        )
                        if incoming_state not in allowed_reservation_states.get(
                            existing_state, {existing_state}
                        ):
                            changed_fields.append(
                                "terminal_repair_reservation"
                            )
                allowed_terminal_transitions = {
                    "cancelled": {
                        "cancelled",
                        "completion_pending",
                        "completed",
                        "acknowledged",
                    },
                    "completed": {"completed", "acknowledged"},
                    "failed": {"failed"},
                }
                if (
                    existing.state in TERMINAL_DISPATCH_STATES
                    and record.state
                    not in allowed_terminal_transitions[existing.state]
                ):
                    changed_fields.append("state")
                if not existing.recoverable and record.recoverable:
                    changed_fields.append("recoverable")
                for field in (
                    "acknowledged_at",
                    "completion_payload",
                    "completion_envelope",
                    "completion_received_at",
                ):
                    if (
                        getattr(existing, field) is not None
                        and getattr(record, field) is None
                    ):
                        changed_fields.append(field)
                changed_fields = list(dict.fromkeys(changed_fields))
                if changed_fields:
                    raise DispatchCompareConflict(
                        dispatch_id=record.dispatch_id,
                        changed_fields=changed_fields,
                    )
            record = self._snapshot(record)
            if existing:
                # Progress rows and their delivery state are maintained by the
                # normalized ingestion/outbox methods.  A caller can legitimately
                # hold a lifecycle snapshot while a heartbeat or delivery update
                # commits; never let that stale snapshot erase the newer rows when
                # it later advances the dispatch state.
                record.progress_events = [
                    event.model_copy(deep=True) for event in existing.progress_events
                ]
                record.progress_heartbeat = (
                    existing.progress_heartbeat.model_copy(deep=True)
                    if existing.progress_heartbeat
                    else None
                )
                record.progress_seen_keys = list(existing.progress_seen_keys)
                record.progress_next_sequence = max(
                    record.progress_next_sequence, existing.progress_next_sequence
                )
                record.progress_conflicts = max(
                    record.progress_conflicts, existing.progress_conflicts
                )
                if existing.progress_protocol_version is not None:
                    record.progress_protocol_version = (
                        existing.progress_protocol_version
                    )
                if (
                    existing.progress_authority_history
                    != record.progress_authority_history
                ):
                    record.progress_authority_history = copy.deepcopy(
                        existing.progress_authority_history
                    )
                    record.authority_instance_id = existing.authority_instance_id
                    record.authority_url = existing.authority_url
                    record.card_version = existing.card_version
            record.updated_at = datetime.now(UTC)
            self._save(record, durability="full")
            return self._snapshot(self._records[record.dispatch_id])

    def mutate_current(
        self,
        dispatch_id: str,
        *,
        mutate: Callable[[DispatchRecord], bool | None],
    ) -> DispatchRecord:
        """Atomically mutate the latest durable record without a detached snapshot.

        The callback must use only evidence already supplied by the caller and must
        not perform external reads. This is the commit primitive for lifecycle
        publications, such as completion acknowledgement, that must observe and
        preserve a concurrently committed terminal-repair reservation.
        """

        with self._lock:
            self._require_writer()
            stored = self._records.get(dispatch_id)
            if stored is None:
                raise ValueError("dispatch not found")
            record = self._snapshot(stored)
            if mutate(record) is False:
                return self._snapshot(stored)
            record.updated_at = datetime.now(UTC)
            self._save(record, durability="full")
            return self._snapshot(self._records[dispatch_id])

    def compare_and_mutate(
        self,
        expected: DispatchRecord,
        mutate: Callable[[DispatchRecord], bool | None],
    ) -> DispatchRecord:
        """Mutate one fresh record only if its complete durable snapshot is unchanged.

        ``get()`` returns a detached snapshot, so callers that perform external
        evidence checks must not later pass that snapshot to ``put()``. This
        operation re-reads under the writer lock, compares every durable field
        except the derived write timestamp, and gives the mutator a fresh copy.
        A concurrent lifecycle, completion, progress, provenance, or event write
        therefore fails closed instead of being overwritten.
        The backing record is compared again after the callback so re-entrant
        writes during evidence reads are detected for mutating and read-only
        callbacks alike.
        """

        with self._lock:
            self._require_writer()
            stored = self._records.get(expected.dispatch_id)
            if stored is None:
                raise ValueError("dispatch not found")
            if stored.mutation_id != expected.mutation_id:
                raise ValueError("dispatch id already belongs to another mutation")
            expected_payload = expected.model_dump(mode="json", exclude={"updated_at"})
            current_payload = stored.model_dump(mode="json", exclude={"updated_at"})
            changed_fields = sorted(
                field
                for field in set(expected_payload) | set(current_payload)
                if expected_payload.get(field) != current_payload.get(field)
            )
            if changed_fields:
                raise DispatchCompareConflict(
                    dispatch_id=expected.dispatch_id,
                    changed_fields=changed_fields,
                )
            current = self._snapshot(stored)
            changed = mutate(current)
            latest = self._records.get(expected.dispatch_id)
            if latest is None:
                raise DispatchCompareConflict(
                    dispatch_id=expected.dispatch_id,
                    changed_fields=["dispatch_id"],
                )
            latest_payload = latest.model_dump(mode="json", exclude={"updated_at"})
            callback_changed_fields = sorted(
                field
                for field in set(current_payload) | set(latest_payload)
                if current_payload.get(field) != latest_payload.get(field)
            )
            if callback_changed_fields:
                raise DispatchCompareConflict(
                    dispatch_id=expected.dispatch_id,
                    changed_fields=callback_changed_fields,
                )
            if changed is False:
                return self._snapshot(latest)
            current.updated_at = datetime.now(UTC)
            self._save(current, durability="full")
            return self._snapshot(self._records[current.dispatch_id])

    def begin_admission(
        self,
        record: DispatchRecord,
        *,
        idempotency_scope: str,
    ) -> tuple[DispatchRecord, bool]:
        """Atomically create or replay one scoped pre-admission trace."""

        with self._lock:
            self._require_writer()
            if idempotency_scope == "target":
                existing = next(
                    (
                        item
                        for item in self._records.values()
                        if item.target_instance_id == record.target_instance_id
                        and item.idempotency_key == record.idempotency_key
                    ),
                    None,
                )
            else:
                existing = next(
                    (
                        item
                        for item in self._records.values()
                        if item.authority_instance_id == record.authority_instance_id
                        and item.idempotency_key == record.idempotency_key
                    ),
                    None,
                )
            if existing is not None:
                if (
                    existing.placement_request_fingerprint
                    != record.placement_request_fingerprint
                ):
                    raise DispatchIdempotencyConflict(existing)
                return self._snapshot(existing), False
            record = self._snapshot(record)
            record.updated_at = datetime.now(UTC)
            self._save(record, durability="full")
            return self._snapshot(self._records[record.dispatch_id]), True

    def retry_with_capacity(
        self,
        record: DispatchRecord,
        capacity: CapacityAdmission,
        *,
        idempotency_key: str,
    ) -> DispatchRecord:
        """Atomically renew the same target reservation for a safe retry."""

        with self._lock:
            self._require_writer()
            control_payload = {"capacity": capacity.model_dump(mode="json")}
            replayed = self._replay_control_receipt(
                record.dispatch_id,
                idempotency_key,
                "retry",
                control_payload,
            )
            if replayed:
                return replayed
            self.expire_capacity_reservations()
            stored = self._records.get(record.dispatch_id)
            current = self._snapshot(stored) if stored else None
            if not current or current.mutation_id != record.mutation_id:
                raise ValueError("dispatch changed before retry admission")
            if current.state not in {"failed", "cancelled"} or not current.recoverable:
                raise ValueError(f"dispatch in {current.state} is not retryable")
            now = datetime.now(UTC)
            execution_full, queue_full, queue_count, reserved, queue_max = (
                self._constraint_counts_locked(
                    current, capacity, exclude_dispatch_id=current.dispatch_id
                )
            )
            current.capacity_limit = capacity.limit
            current.capacity_source = capacity.source
            current.capacity_provider = capacity.provider
            current.capacity_observed_active = capacity.observed_active
            current.capacity_observed_queued = capacity.observed_queued
            current.capacity_observed_reservations = capacity.observed_reservations
            current.queue_limit = (
                capacity.queue_limit if capacity.queue_limit is not None else 0
            )
            current.queue_source = capacity.queue_source
            current.queue_provider_specific = capacity.queue_provider_specific
            current.queue_observed_count = queue_count
            current.capacity_released_at = None
            current.capacity_release_reason = None
            current.capacity_override = capacity.override
            current.capacity_override_reason = capacity.override_reason
            current.cancel_requested = False
            current.last_error = None
            current.error_code = None
            current.control_operations[idempotency_key] = "retry"
            if execution_full and not capacity.override:
                if capacity.queue_limit is None:
                    raise DispatchCapacityExhausted(
                        limit=capacity.limit,
                        source=capacity.source,
                        provider=capacity.provider,
                        active=capacity.observed_active,
                        queued=capacity.observed_queued,
                        reservations=reserved,
                        observed_at=capacity.observed_at,
                        consumer_links=capacity.consumer_links,
                    )
                if queue_full:
                    self._record_queue_rejection_locked()
                    raise DispatchQueueFull(
                        limit=queue_max
                        if queue_max is not None
                        else capacity.queue_limit,
                        source=capacity.queue_source,
                        provider=capacity.provider,
                        current=queue_count,
                        active_capacity=capacity.limit,
                        observed_at=capacity.observed_at,
                    )
                current.state = "waiting_capacity"
                current.queue_admitted_at = current.queue_admitted_at or now
                current.queue_wait_reason = (
                    "Waiting for execution capacity after retry."
                )
            else:
                current.state = "queued"
                current.capacity_reserved_at = now
                current.capacity_reservation_expires_at = now + CAPACITY_RESERVATION_TTL
            current.events.append(
                DispatchEvent(
                    seq=(current.events[-1].seq + 1 if current.events else 1),
                    state=current.state,
                    message=(
                        "Operator durably queued a safe retry until capacity is available."
                        if current.state == "waiting_capacity"
                        else "Operator queued a safe retry with a fresh capacity reservation."
                    ),
                )
            )
            current.updated_at = now
            return self._save_control(
                current,
                idempotency_key=idempotency_key,
                operation="retry",
                payload=control_payload,
            )

    def allocate_progress_sequence(self, dispatch_id: str) -> int:
        """Allocate a durable per-dispatch sequence across restarts and callbacks."""
        with self._lock:
            stored = self._records.get(dispatch_id)
            if not stored:
                raise ValueError("dispatch not found")
            record = self._snapshot(stored)
            sequence = max(1, record.progress_next_sequence)
            record.progress_next_sequence = sequence + 1
            record.updated_at = datetime.now(UTC)
            self._save(record)
            return sequence

    @staticmethod
    def _validate_progress_provenance(
        record: DispatchRecord,
        payload: DispatchProgressEventV1 | DispatchProgressHeartbeatV1,
    ) -> None:
        prior_authorities = {
            (
                str(item.get("authority_instance_id") or ""),
                str(item.get("authority_version") or ""),
            )
            for item in record.progress_authority_history
        }
        authority_matches = (
            payload.authority_instance_id == record.authority_instance_id
            and (
                record.card_version in {None, ""}
                or payload.authority_version in {None, "", record.card_version}
            )
        ) or (
            payload.authority_instance_id,
            str(payload.authority_version or ""),
        ) in prior_authorities
        mismatches = {
            field: {"expected": expected, "actual": actual}
            for field, expected, actual in (
                ("dispatch_id", record.dispatch_id, payload.dispatch_id),
                ("card_id", record.card_id, payload.card_id),
                ("session_id", record.session_id, payload.acp_session_id),
                (
                    "originating_instance_id",
                    record.target_instance_id,
                    payload.originating_instance_id,
                ),
            )
            if expected not in {None, ""}
            and actual not in {None, ""}
            and expected != actual
        }
        if not authority_matches:
            mismatches["authority"] = {
                "expected": {
                    "instance_id": record.authority_instance_id,
                    "version": record.card_version,
                },
                "actual": {
                    "instance_id": payload.authority_instance_id,
                    "version": payload.authority_version,
                },
            }
        if mismatches:
            raise ValueError(f"progress provenance mismatch: {mismatches}")
        if record.progress_protocol_version not in {None, payload.schema_version}:
            raise ValueError("unsupported progress schema version")

    def transfer_progress_authority(
        self,
        dispatch_id: str,
        *,
        authority_instance_id: str,
        authority_url: str,
        authority_version: str | None,
    ) -> DispatchRecord:
        """Fence future events to a new authority without rewriting provenance."""
        with self._lock:
            self._require_writer()
            stored = self._records.get(dispatch_id)
            if not stored:
                raise ValueError("dispatch not found")
            record = self._snapshot(stored)
            if (
                record.authority_instance_id == authority_instance_id
                and record.card_version == authority_version
            ):
                return self._snapshot(record)
            record.progress_authority_history.append(
                {
                    "authority_instance_id": record.authority_instance_id,
                    "authority_version": record.card_version,
                    "authority_url": record.authority_url,
                    "last_sequence": record.progress_next_sequence - 1,
                    "transferred_at": datetime.now(UTC).isoformat(),
                }
            )
            record.progress_authority_history = record.progress_authority_history[-20:]
            record.authority_instance_id = authority_instance_id
            record.authority_url = authority_url
            record.card_version = authority_version
            record.updated_at = datetime.now(UTC)
            self._save(record, durability="full")
            return self._snapshot(self._records[record.dispatch_id])

    def _commit_progress_mutation(
        self,
        record: DispatchRecord,
        payload: DispatchProgressEventV1 | DispatchProgressHeartbeatV1,
        result: ProgressIngestResult,
        *,
        persist_payload: bool,
        removed: list[DispatchProgressEventV1] | None = None,
    ) -> ProgressIngestResult:
        removed = removed or []
        row_count = 2 + int(persist_payload) + len(removed)
        try:
            with self._transaction() as conn:
                if persist_payload:
                    if isinstance(payload, DispatchProgressHeartbeatV1):
                        self._persist_heartbeat_conn(conn, payload)
                    else:
                        self._persist_event_conn(conn, payload)
                self._persist_receipt_conn(conn, payload, result)
                for stale in removed:
                    conn.execute(
                        "DELETE FROM dispatch_progress_events "
                        "WHERE dispatch_id=? AND idempotency_key=?",
                        (record.dispatch_id, stale.idempotency_key),
                    )
                self._persist_record_conn(conn, record)
        except BaseException as exc:
            if getattr(exc, "pa_transaction_committed", False):
                self._publish_records_locked([record])
                self._write_rows += row_count
                self._retention_actions += len(removed)
            raise
        self._publish_records_locked([record])
        self._write_rows += row_count
        self._retention_actions += len(removed)
        return result

    def ingest_progress(
        self, event: DispatchProgressEventV1, *, delivered: bool = False
    ) -> ProgressIngestResult:
        """Idempotently retain a bounded, safely reorderable checkpoint history."""
        sanitized = sanitize_progress_event(event)
        with self._lock:
            self._require_writer()
            stored = self._records.get(sanitized.dispatch_id)
            if not stored:
                raise ValueError("dispatch not found")
            replayed = self._replay_progress_receipt(sanitized)
            if replayed:
                return replayed
            self._validate_progress_provenance(stored, sanitized)
            if delivered and sanitized.delivered_at is None:
                sanitized.delivered_at = datetime.now(UTC)
            record = self._snapshot(stored)
            same_sequence = next(
                (
                    existing
                    for existing in record.progress_events
                    if existing.sequence == sanitized.sequence
                ),
                None,
            )
            durable_sequence = self._require_writer().execute(
                "SELECT 1 FROM dispatch_receipts WHERE dispatch_id=? AND sequence=? "
                "AND kind IN ('checkpoint','final','progress') LIMIT 1",
                (record.dispatch_id, sanitized.sequence),
            ).fetchone()
            if same_sequence or durable_sequence:
                record.progress_conflicts += 1
                record.progress_seen_keys.append(sanitized.idempotency_key)
                record.progress_seen_keys = record.progress_seen_keys[
                    -MAX_PROGRESS_SEEN_KEYS:
                ]
                record.updated_at = datetime.now(UTC)
                result = ProgressIngestResult(
                    accepted=False,
                    status="conflict",
                    dispatch_id=record.dispatch_id,
                    sequence=sanitized.sequence,
                    idempotency_key=sanitized.idempotency_key,
                )
                return self._commit_progress_mutation(
                    record, sanitized, result, persist_payload=False
                )
            latest = record.latest_progress
            if (
                not self._progress_protected(sanitized)
                and latest
                and latest.phase == sanitized.phase
                and latest.summary == sanitized.summary
                and abs((sanitized.occurred_at - latest.occurred_at).total_seconds())
                <= 5
            ):
                record.progress_seen_keys.append(sanitized.idempotency_key)
                record.progress_seen_keys = record.progress_seen_keys[
                    -MAX_PROGRESS_SEEN_KEYS:
                ]
                record.updated_at = datetime.now(UTC)
                result = ProgressIngestResult(
                    accepted=True,
                    status="coalesced",
                    dispatch_id=record.dispatch_id,
                    sequence=sanitized.sequence,
                    idempotency_key=sanitized.idempotency_key,
                )
                return self._commit_progress_mutation(
                    record, sanitized, result, persist_payload=False
                )
            previous_max = max(
                (item.sequence for item in record.progress_events), default=0
            )
            record.progress_events.append(sanitized)
            record.progress_events.sort(
                key=lambda item: (item.sequence, item.occurred_at, item.idempotency_key)
            )
            removed: list[DispatchProgressEventV1] = []
            retained_bytes = sum(
                len(item.model_dump_json().encode())
                for item in record.progress_events
            )
            while (
                len(record.progress_events) > MAX_PROGRESS_EVENTS
                or retained_bytes > self.MAX_PROGRESS_BYTES_PER_DISPATCH
            ):
                candidate = next(
                    (
                        item
                        for item in record.progress_events
                        if not self._progress_protected(item)
                    ),
                    None,
                )
                if candidate is None:
                    break
                record.progress_events.remove(candidate)
                removed.append(candidate)
                retained_bytes -= len(candidate.model_dump_json().encode())
            record.progress_seen_keys.append(sanitized.idempotency_key)
            record.progress_seen_keys = record.progress_seen_keys[
                -MAX_PROGRESS_SEEN_KEYS:
            ]
            record.progress_protocol_version = sanitized.schema_version
            record.progress_next_sequence = max(
                record.progress_next_sequence, sanitized.sequence + 1
            )
            record.updated_at = datetime.now(UTC)
            result = ProgressIngestResult(
                accepted=True,
                status="late" if sanitized.sequence < previous_max else "accepted",
                dispatch_id=record.dispatch_id,
                sequence=sanitized.sequence,
                idempotency_key=sanitized.idempotency_key,
            )
            return self._commit_progress_mutation(
                record,
                sanitized,
                result,
                persist_payload=True,
                removed=removed,
            )

    def ingest_heartbeat(
        self,
        heartbeat: DispatchProgressHeartbeatV1,
        *,
        delivered: bool = False,
    ) -> ProgressIngestResult:
        """Replace the freshness signal without appending activity history."""
        sanitized = sanitize_heartbeat(heartbeat)
        with self._lock:
            self._require_writer()
            stored = self._records.get(sanitized.dispatch_id)
            if not stored:
                raise ValueError("dispatch not found")
            replayed = self._replay_progress_receipt(sanitized)
            if replayed:
                return replayed
            self._validate_progress_provenance(stored, sanitized)
            record = self._snapshot(stored)
            current = record.progress_heartbeat
            if current and sanitized.sequence < current.sequence:
                record.progress_seen_keys.append(sanitized.idempotency_key)
                record.progress_seen_keys = record.progress_seen_keys[
                    -MAX_PROGRESS_SEEN_KEYS:
                ]
                record.updated_at = datetime.now(UTC)
                result = ProgressIngestResult(
                    accepted=True,
                    status="late",
                    dispatch_id=record.dispatch_id,
                    sequence=sanitized.sequence,
                    idempotency_key=sanitized.idempotency_key,
                )
                return self._commit_progress_mutation(
                    record, sanitized, result, persist_payload=False
                )
            if delivered and sanitized.delivered_at is None:
                sanitized.delivered_at = datetime.now(UTC)
            record.progress_heartbeat = sanitized
            record.progress_seen_keys.append(sanitized.idempotency_key)
            record.progress_seen_keys = record.progress_seen_keys[
                -MAX_PROGRESS_SEEN_KEYS:
            ]
            record.progress_protocol_version = sanitized.schema_version
            record.progress_next_sequence = max(
                record.progress_next_sequence, sanitized.sequence + 1
            )
            record.updated_at = datetime.now(UTC)
            result = ProgressIngestResult(
                accepted=True,
                status="accepted",
                dispatch_id=record.dispatch_id,
                sequence=sanitized.sequence,
                idempotency_key=sanitized.idempotency_key,
            )
            # NORMAL WAL commits acknowledge process-crash durability without
            # issuing an fsync for every replaceable observational heartbeat.
            return self._commit_progress_mutation(
                record, sanitized, result, persist_payload=True
            )

    def pending_progress(
        self, originating_instance_id: str
    ) -> list[
        tuple[
            DispatchRecord,
            DispatchProgressEventV1 | DispatchProgressHeartbeatV1,
        ]
    ]:
        self._require_readable()
        pending: list[
            tuple[
                DispatchRecord,
                DispatchProgressEventV1 | DispatchProgressHeartbeatV1,
            ]
        ] = []
        with self._lock:
            for record in self._records.values():
                if record.target_instance_id != originating_instance_id:
                    continue
                for event in record.progress_events:
                    if event.delivered_at is None:
                        snapshot = self._snapshot(record)
                        pending_event = next(
                            item
                            for item in snapshot.progress_events
                            if item.idempotency_key == event.idempotency_key
                        )
                        pending.append((snapshot, pending_event))
                heartbeat = record.progress_heartbeat
                if heartbeat and heartbeat.delivered_at is None:
                    snapshot = self._snapshot(record)
                    if snapshot.progress_heartbeat:
                        pending.append((snapshot, snapshot.progress_heartbeat))
        return sorted(
            pending,
            key=lambda pair: (
                pair[1].occurred_at,
                pair[1].sequence,
                pair[1].idempotency_key,
            ),
        )

    def mark_progress_delivered(self, dispatch_id: str, idempotency_key: str) -> None:
        with self._lock:
            self._require_writer()
            stored = self._records.get(dispatch_id)
            if not stored:
                return
            record = self._snapshot(stored)
            payloads: list[DispatchProgressEventV1 | DispatchProgressHeartbeatV1] = (
                list(record.progress_events)
            )
            if record.progress_heartbeat:
                payloads.append(record.progress_heartbeat)
            payload = next(
                (item for item in payloads if item.idempotency_key == idempotency_key),
                None,
            )
            if not payload:
                return
            payload.delivered_at = datetime.now(UTC)
            payload.delivery_error = None
            record.updated_at = datetime.now(UTC)
            try:
                with self._transaction() as conn:
                    if isinstance(payload, DispatchProgressHeartbeatV1):
                        self._persist_heartbeat_conn(conn, payload)
                    else:
                        self._persist_event_conn(conn, payload)
                    self._persist_record_conn(conn, record)
            except BaseException as exc:
                if getattr(exc, "pa_transaction_committed", False):
                    self._publish_records_locked([record])
                    self._write_rows += 2
                raise
            self._publish_records_locked([record])
            self._write_rows += 2

    def mark_progress_delivery_failed(
        self, dispatch_id: str, idempotency_key: str, error: str
    ) -> None:
        with self._lock:
            self._require_writer()
            stored = self._records.get(dispatch_id)
            if not stored:
                return
            record = self._snapshot(stored)
            payloads: list[DispatchProgressEventV1 | DispatchProgressHeartbeatV1] = (
                list(record.progress_events)
            )
            if record.progress_heartbeat:
                payloads.append(record.progress_heartbeat)
            payload = next(
                (item for item in payloads if item.idempotency_key == idempotency_key),
                None,
            )
            if not payload:
                return
            payload.delivery_attempts += 1
            payload.delivery_error = sanitize_text(error, limit=240)
            record.updated_at = datetime.now(UTC)
            try:
                with self._transaction() as conn:
                    if isinstance(payload, DispatchProgressHeartbeatV1):
                        self._persist_heartbeat_conn(conn, payload)
                    else:
                        self._persist_event_conn(conn, payload)
                    self._persist_record_conn(conn, record)
            except BaseException as exc:
                if getattr(exc, "pa_transaction_committed", False):
                    self._publish_records_locked([record])
                    self._write_rows += 2
                raise
            self._publish_records_locked([record])
            self._write_rows += 2

    def build_final_report(
        self, dispatch_id: str, result: dict[str, Any]
    ) -> CompletionReportV1 | None:
        self._require_readable()
        with self._lock:
            record = self._records.get(dispatch_id)
            if not record:
                return None
            latest = record.latest_progress
            metadata_source = next(
                (
                    event
                    for event in reversed(record.progress_events)
                    if any(
                        (
                            event.branch,
                            event.commit_sha,
                            event.pr_url,
                            event.pr_number,
                        )
                    )
                ),
                latest,
            )
            validations: list[Any] = []
            for event in record.progress_events:
                for validation in event.validations:
                    if validation not in validations:
                        validations.append(validation)
            disposition = result.get("card_disposition")
            outcome = sanitize_text(
                (disposition.get("outcome") if isinstance(disposition, dict) else None)
                or (latest.summary if latest else None)
                or "Agent turn ended.",
                limit=2000,
            )
            return CompletionReportV1(
                outcome=outcome,
                branch=metadata_source.branch if metadata_source else None,
                commit_sha=metadata_source.commit_sha if metadata_source else None,
                pr_url=metadata_source.pr_url if metadata_source else None,
                pr_number=metadata_source.pr_number if metadata_source else None,
                validations=validations[:20],
                blockers=list(latest.blockers if latest else []),
                card_disposition=(
                    disposition if isinstance(disposition, dict) else None
                ),
                resulting_lane=(
                    disposition.get("lane") if isinstance(disposition, dict) else None
                ),
            )

    def set_final_report(
        self, dispatch_id: str, report: CompletionReportV1
    ) -> DispatchRecord:
        with self._lock:
            self._require_writer()
            stored = self._records.get(dispatch_id)
            if not stored:
                raise ValueError("dispatch not found")
            record = self._snapshot(stored)
            record.final_report = sanitize_completion_report(report)
            record.updated_at = datetime.now(UTC)
            self._save(record, durability="full")
            return self._snapshot(self._records[record.dispatch_id])

    def transition(
        self,
        record: DispatchRecord,
        state: str,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
    ) -> DispatchRecord:
        if state not in DISPATCH_STAGES:
            raise ValueError(f"unknown dispatch state: {state}")
        previous_state = record.state
        requested_state = state
        if (
            record.acknowledged_at is not None
            and previous_state in {"completed", "acknowledged"}
            and state not in {"completed", "acknowledged"}
        ):
            record.lifecycle_inconsistencies.append(
                {
                    "kind": "terminal_dispatch_regression_prevented",
                    "previous_state": previous_state,
                    "requested_state": state,
                    "observed_at": datetime.now(UTC).isoformat(),
                    "acknowledged_at": record.acknowledged_at.isoformat(),
                }
            )
            record.lifecycle_inconsistencies = record.lifecycle_inconsistencies[-50:]
            state = previous_state
            detail = {
                **(detail or {}),
                "requested_state": requested_state,
                "retained_state": previous_state,
                "acknowledged_completion_wins": True,
            }
        record.state = state
        if (
            previous_state in CAPACITY_RESERVATION_STATES
            and state not in CAPACITY_RESERVATION_STATES
            and record.capacity_reserved_at
            and not record.capacity_released_at
        ):
            record.capacity_released_at = datetime.now(UTC)
            record.capacity_release_reason = state
        record.events.append(
            DispatchEvent(
                seq=(record.events[-1].seq + 1 if record.events else 1),
                state=state,
                message=message,
                detail=detail or {},
            )
        )
        return self.put(record)

    def record_followup_started(
        self,
        record: DispatchRecord,
        *,
        idempotency_key: str,
        prompt_id: str | None,
        event_id: str | None,
        event_seq: int | None,
    ) -> DispatchRecord:
        """Record follow-up activity without mutating dispatch completion."""
        if any(
            item.get("idempotency_key") == idempotency_key
            for item in record.followup_turns
        ):
            return record
        now = datetime.now(UTC)
        record.followup_turns.append(
            {
                "idempotency_key": idempotency_key,
                "prompt_id": prompt_id,
                "event_id": event_id,
                "event_seq": event_seq,
                "state": "accepted",
                "accepted_at": now.isoformat(),
                "session_id": record.session_id,
            }
        )
        record.followup_turns = record.followup_turns[-100:]
        return self.transition(
            record,
            record.state,
            "Follow-up turn durably accepted; dispatch completion remains terminal.",
            detail={
                "followup": True,
                "prompt_id": prompt_id,
                "dispatch_state_retained": record.state,
            },
        )

    def fail(
        self,
        record: DispatchRecord,
        message: str,
        *,
        code: str = "dispatch_failed",
        recoverable: bool = True,
        detail: dict[str, Any] | None = None,
    ) -> DispatchRecord:
        record.last_error = message
        record.error_code = code
        record.recoverable = recoverable
        return self.transition(record, "failed", message, detail=detail)

    def expire_capacity_reservations(
        self,
        *,
        now: datetime | None = None,
        target_instance_id: str | None = None,
    ) -> list[DispatchRecord]:
        """Fail timed-out pre-start work and durably release its slot."""

        checked_at = now or datetime.now(UTC)
        expired: list[DispatchRecord] = []
        with self._lock:
            self._require_writer()
            candidates = (
                self._capacity_records_by_target.get(target_instance_id, ())
                if target_instance_id
                else tuple(
                    record
                    for records in self._capacity_records_by_target.values()
                    for record in records
                )
            )
            for stored in candidates:
                if (
                    stored.state not in CAPACITY_RESERVATION_STATES
                    or stored.capacity_reservation_expires_at is None
                    or stored.capacity_reservation_expires_at > checked_at
                ):
                    continue
                record = self._snapshot(stored)
                previous_state = record.state
                record.state = "failed"
                record.last_error = (
                    "Capacity reservation timed out before execution started; "
                    "retry to obtain a fresh reservation."
                )
                record.error_code = "capacity_reservation_timeout"
                record.recoverable = True
                record.capacity_released_at = checked_at
                record.capacity_release_reason = "timeout"
                record.events.append(
                    DispatchEvent(
                        seq=(record.events[-1].seq + 1 if record.events else 1),
                        state="failed",
                        message=record.last_error,
                        detail={"previous_state": previous_state},
                        created_at=checked_at,
                    )
                )
                record.updated_at = checked_at
                expired.append(record)
            if expired:
                self._save(expired, durability="full")
                logger.warning(
                    "fleet capacity reservations timed out count=%s dispatches=%s",
                    len(expired),
                    [record.dispatch_id for record in expired],
                )
        return expired

    def waiting(self) -> list[DispatchRecord]:
        self._require_readable()
        with self._lock:
            self._refresh_queue_positions_locked()
            waiting = [
                self._snapshot(record)
                for record in self._records.values()
                if record.state in QUEUE_CONSUMING_STATES
            ]
        return sorted(
            waiting,
            key=lambda item: (
                -item.requested_priority,
                item.scheduling_class_sequence,
                item.queue_admitted_at or item.created_at,
                item.dispatch_id,
            ),
        )

    def promote_waiting(
        self, record: DispatchRecord, capacity: CapacityAdmission | None = None
    ) -> bool:
        """Atomically consume a newly free slot without changing target contract."""

        with self._lock:
            self._require_writer()
            stored = self._records.get(record.dispatch_id)
            if not stored or stored.state not in QUEUE_CONSUMING_STATES:
                return False
            current = self._snapshot(stored)
            effective = capacity or CapacityAdmission(
                limit=current.capacity_limit or 1,
                source=current.capacity_source or "stored_admission",
                provider=current.capacity_provider,
                provider_specific=bool(current.capacity_provider),
                observed_active=current.capacity_observed_active,
                observed_queued=current.capacity_observed_queued,
                observed_reservations=current.capacity_observed_reservations,
                queue_limit=current.queue_limit,
                queue_source=current.queue_source,
                queue_provider_specific=current.queue_provider_specific,
                observed_waiting=current.queue_observed_count,
            )
            execution_full, _queue_full, queue_count, _reserved, _queue_max = (
                self._constraint_counts_locked(
                    current, effective, exclude_dispatch_id=current.dispatch_id
                )
            )
            current.capacity_limit = effective.limit
            current.capacity_source = effective.source
            current.capacity_observed_active = effective.observed_active
            current.capacity_observed_queued = effective.observed_queued
            current.capacity_observed_reservations = effective.observed_reservations
            current.queue_observed_count = queue_count
            if execution_full and not effective.override:
                current.state = "waiting_capacity"
                current.queue_blocked_code = None
                current.queue_wait_reason = (
                    f"All {effective.limit} execution slots remain occupied."
                )
                current.updated_at = datetime.now(UTC)
                self._save(current)
                return False
            now = datetime.now(UTC)
            current.state = "queued"
            current.queue_launched_at = now
            current.queue_wait_reason = None
            current.queue_blocked_code = None
            current.capacity_reserved_at = now
            current.capacity_reservation_expires_at = now + CAPACITY_RESERVATION_TTL
            current.events.append(
                DispatchEvent(
                    seq=(current.events[-1].seq + 1 if current.events else 1),
                    state="queued",
                    message="Execution capacity became available; dispatch promoted exactly once.",
                    created_at=now,
                )
            )
            current.queue_audit.append({"action": "promoted", "at": now.isoformat()})
            current.updated_at = now
            self._save(current, durability="full")
            return True

    def block_waiting(
        self, record: DispatchRecord, *, code: str, reason: str
    ) -> DispatchRecord:
        with self._lock:
            self._require_writer()
            stored = self._records.get(record.dispatch_id)
            if not stored or stored.state not in QUEUE_CONSUMING_STATES:
                return self._snapshot(stored) if stored else record
            current = self._snapshot(stored)
            current.state = "blocked"
            current.queue_blocked_code = sanitize_text(code, limit=120)
            current.queue_wait_reason = sanitize_text(reason, limit=500)
            current.updated_at = datetime.now(UTC)
            self._save(current, durability="full")
            return self._snapshot(self._records[current.dispatch_id])

    def reprioritize(
        self,
        record: DispatchRecord,
        *,
        priority: int,
        principal_id: str,
        idempotency_key: str,
    ) -> DispatchRecord:
        with self._lock:
            self._require_writer()
            control_payload = {"priority": priority, "principal_id": principal_id}
            replayed = self._replay_control_receipt(
                record.dispatch_id,
                idempotency_key,
                "reprioritize",
                control_payload,
            )
            if replayed:
                return replayed
            stored = self._records.get(record.dispatch_id)
            if not stored or stored.state not in QUEUE_CONSUMING_STATES:
                raise ValueError("only waiting dispatches can be reprioritized")
            current = self._snapshot(stored)
            operation = f"priority:{priority}"
            previous = current.control_operations.get(idempotency_key)
            if previous and previous != operation:
                raise DispatchIdempotencyConflict(self._snapshot(current))
            if previous == operation:
                return self._snapshot(current)
            old = current.requested_priority
            current.requested_priority = priority
            current.control_operations[idempotency_key] = operation
            current.queue_audit.append(
                {
                    "action": "priority_changed",
                    "at": datetime.now(UTC).isoformat(),
                    "principal_id": principal_id,
                    "from": old,
                    "to": priority,
                    "idempotency_key": idempotency_key,
                }
            )
            current.updated_at = datetime.now(UTC)
            return self._save_control(
                current,
                idempotency_key=idempotency_key,
                operation="reprioritize",
                payload=control_payload,
            )

    def queue_snapshot(self) -> dict[str, Any]:
        self._require_readable()
        waiting = self.waiting()
        now = datetime.now(UTC)
        blocked = sum(item.state == "blocked" for item in waiting)
        ages = [
            max(
                0.0, (now - (item.queue_admitted_at or item.created_at)).total_seconds()
            )
            for item in waiting
        ]
        wait_times = [
            max(
                0.0,
                (
                    (item.queue_launched_at or now)
                    - (item.queue_admitted_at or item.created_at)
                ).total_seconds(),
            )
            for item in self.list(limit=1000)
            if item.queue_admitted_at
        ]
        records = self.list(limit=1000)
        admissions = sum(item.queue_admitted_at is not None for item in records)
        launches = sum(item.queue_launched_at is not None for item in records)
        launch_failures = sum(
            item.queue_launched_at is not None and item.state == "failed"
            for item in records
        )
        starvation = sum(age >= 3600 for age in ages)
        return {
            "queued": len(waiting) - blocked,
            "blocked": blocked,
            "total": len(waiting),
            "oldest_age_seconds": round(max(ages), 3) if ages else None,
            "records": [item.public_dict() for item in waiting],
            "metrics": {
                "depth": len(waiting),
                "age_seconds": round(max(ages), 3) if ages else 0.0,
                "admissions_total": admissions,
                "rejections_total": self._queue_rejections,
                "launches_total": launches,
                "launch_failures_total": launch_failures,
                "wait_time_average_seconds": (
                    round(sum(wait_times) / len(wait_times), 3) if wait_times else 0.0
                ),
                "wait_time_max_seconds": round(max(wait_times), 3)
                if wait_times
                else 0.0,
                "starvation_count": starvation,
            },
            "alerts": (["dispatch_queue_starvation"] if starvation else []),
            "storage": self.storage_metrics(),
        }

    @staticmethod
    def _percentile(values: deque[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int(len(ordered) * percentile) - 1))
        return round(ordered[index], 3)

    def storage_metrics(self) -> dict[str, Any]:
        """Expose bounded ledger health without deserializing historical payloads."""
        with self._lock:
            if self._deferred_read_only:
                counts = {
                    "available": False,
                    "dispatches": None,
                    "progress_events": None,
                    "heartbeats": None,
                    "receipts": None,
                    "final_reports": None,
                }
            else:
                try:
                    counts = {
                        "dispatches": self._conn.execute(
                            "SELECT COUNT(*) FROM dispatches"
                        ).fetchone()[0],
                        "progress_events": self._conn.execute(
                            "SELECT COUNT(*) FROM dispatch_progress_events"
                        ).fetchone()[0],
                        "heartbeats": self._conn.execute(
                            "SELECT COUNT(*) FROM dispatch_heartbeats"
                        ).fetchone()[0],
                        "receipts": self._conn.execute(
                            "SELECT COUNT(*) FROM dispatch_receipts"
                        ).fetchone()[0],
                        "final_reports": sum(
                            record.final_report is not None
                            for record in self._records.values()
                        ),
                    }
                except (AttributeError, sqlite3.OperationalError):
                    counts = {
                        "dispatches": len(self._records),
                        "progress_events": sum(
                            len(record.progress_events)
                            for record in self._records.values()
                        ),
                        "heartbeats": sum(
                            record.progress_heartbeat is not None
                            for record in self._records.values()
                        ),
                        "receipts": sum(
                            len(record.progress_seen_keys)
                            for record in self._records.values()
                        ),
                        "final_reports": sum(
                            record.final_report is not None
                            for record in self._records.values()
                        ),
                    }
            sizes = {}
            for label, path in (
                ("database", self.db_path),
                ("wal", Path(str(self.db_path) + "-wal")),
                ("shm", Path(str(self.db_path) + "-shm")),
                ("legacy_source", self.path),
                ("legacy_backup", self.backup_path),
            ):
                try:
                    sizes[label] = path.stat().st_size
                except OSError:
                    sizes[label] = 0
            migration = (
                {"state": "deferred", "available": False}
                if self._deferred_read_only
                else self._migration_meta() or {"state": "unknown"}
            )
            return {
                "schema_version": self.SCHEMA_VERSION,
                "mode": (
                    "deferred_read_only"
                    if self._deferred_read_only
                    else "read_only" if self._read_only else "writer"
                ),
                "journal_mode": (
                    "unopened"
                    if self._deferred_read_only
                    else "existing" if self._read_only else "wal"
                ),
                "synchronous": "unchanged" if self._read_only else "normal",
                "store_bytes": sum(
                    sizes[key] for key in ("database", "wal", "shm")
                ),
                "bytes": sizes,
                "rows": counts,
                "writes": {
                    "commits": self._commits,
                    "rows": self._write_rows,
                    "queue_depth": self._queued_writers,
                    "latency_ms": {
                        "p50": self._percentile(self._commit_latencies_ms, 0.50),
                        "p99": self._percentile(self._commit_latencies_ms, 0.99),
                        "max": round(max(self._commit_latencies_ms, default=0.0), 3),
                    },
                },
                "checkpoint": {
                    "runs": len(self._checkpoint_latencies_ms),
                    "cost_ms_p99": self._percentile(
                        self._checkpoint_latencies_ms, 0.99
                    ),
                    "cost_ms_max": round(
                        max(self._checkpoint_latencies_ms, default=0.0), 3
                    ),
                },
                "retention": {
                    "actions": self._retention_actions,
                    "max_events_per_dispatch": MAX_PROGRESS_EVENTS,
                    "max_bytes_per_dispatch": self.MAX_PROGRESS_BYTES_PER_DISPATCH,
                    "receipt_replay_days": self.RECEIPT_REPLAY_DAYS,
                },
                "migration": migration,
            }

    def checkpoint(self, *, truncate: bool = False) -> dict[str, Any]:
        """Run an explicit observable WAL checkpoint outside mutation commits."""
        with self._lock:
            conn = self._require_writer()
            started = time.perf_counter()
            row = conn.execute(
                f"PRAGMA wal_checkpoint({'TRUNCATE' if truncate else 'PASSIVE'})"
            ).fetchone()
            elapsed = (time.perf_counter() - started) * 1000
            self._checkpoint_latencies_ms.append(elapsed)
            return {
                "busy": int(row[0]),
                "wal_pages": int(row[1]),
                "checkpointed_pages": int(row[2]),
                "duration_ms": round(elapsed, 3),
                "truncated": truncate,
            }

    def compact(self, *, now: datetime | None = None) -> dict[str, int]:
        """Expire closed-horizon detail while fencing active/evaluator evidence."""
        current = now or datetime.now(UTC)
        cutoff = current - timedelta(days=self.RECEIPT_REPLAY_DAYS)
        removed_events = 0
        removed_receipts = 0
        with self._lock:
            self._require_writer()
            safe: list[DispatchRecord] = []
            for stored in self._records.values():
                if not (
                    stored.state in TERMINAL_DISPATCH_STATES | {"acknowledged"}
                    and stored.acknowledged_at
                    and stored.final_report
                    and stored.post_turn_evaluations
                    and not any(
                        turn.get("delivery_state") == "pending"
                        for turn in stored.followup_turns
                    )
                    and all(
                        event.delivered_at is not None for event in stored.progress_events
                    )
                    and (
                        stored.progress_heartbeat is None
                        or stored.progress_heartbeat.delivered_at is not None
                    )
                ):
                    continue
                safe.append(self._snapshot(stored))
            if not safe:
                return {"events": 0, "receipts": 0}
            try:
                with self._transaction() as conn:
                    for record in safe:
                        stale = [
                            event
                            for event in record.progress_events
                            if event.occurred_at < cutoff
                            and not self._progress_protected(event)
                        ]
                        for event in stale:
                            conn.execute(
                                "DELETE FROM dispatch_progress_events "
                                "WHERE dispatch_id=? AND idempotency_key=?",
                                (record.dispatch_id, event.idempotency_key),
                            )
                            record.progress_events.remove(event)
                        protected_keys = {
                            event.idempotency_key for event in record.progress_events
                        }
                        if record.progress_heartbeat:
                            protected_keys.add(record.progress_heartbeat.idempotency_key)
                        if protected_keys:
                            placeholders = ",".join("?" for _ in protected_keys)
                            cursor = conn.execute(
                                "DELETE FROM dispatch_receipts WHERE dispatch_id=? "
                                f"AND created_at<? AND idempotency_key NOT IN ({placeholders})",
                                (record.dispatch_id, cutoff.isoformat(), *protected_keys),
                            )
                        else:
                            cursor = conn.execute(
                                "DELETE FROM dispatch_receipts WHERE dispatch_id=? AND created_at<?",
                                (record.dispatch_id, cutoff.isoformat()),
                            )
                        removed_events += len(stale)
                        removed_receipts += max(0, cursor.rowcount)
                        self._persist_record_conn(conn, record)
            except BaseException as exc:
                if getattr(exc, "pa_transaction_committed", False):
                    self._publish_records_locked(safe)
                raise
            self._publish_records_locked(safe)
            self._retention_actions += removed_events + removed_receipts
            self._write_rows += removed_events + removed_receipts + len(safe)
        return {"events": removed_events, "receipts": removed_receipts}

    def export_legacy_json(self, destination: Path) -> dict[str, Any]:
        """Create an atomic downgrade/rollback export with every durable receipt."""
        conn = self._require_writer()
        if destination.resolve() in {self.path.resolve(), self.backup_path.resolve()}:
            raise ValueError("refusing to overwrite migration source or verified backup")
        with self._lock:
            payload: dict[str, Any] = {}
            for dispatch_id, record in self._records.items():
                data = record.model_dump(mode="json")
                data["progress_seen_keys"] = [
                    row["idempotency_key"]
                    for row in conn.execute(
                        "SELECT idempotency_key FROM dispatch_receipts "
                        "WHERE dispatch_id=? ORDER BY created_at,idempotency_key",
                        (dispatch_id,),
                    ).fetchall()
                ]
                payload[dispatch_id] = data
            text = json.dumps(payload, indent=2) + "\n"
            atomic_write_text(destination, text)
            return {
                "path": str(destination),
                "sha256": self._checksum(text.encode()),
                "dispatches": len(payload),
                "progress_events": sum(
                    len(record.progress_events) for record in self._records.values()
                ),
                "receipts": sum(
                    len(data["progress_seen_keys"]) for data in payload.values()
                ),
            }

    def close(self) -> None:
        """Drain committed WAL state and close the store without dropping writes."""
        with self._lock:
            if getattr(self, "_conn", None) is None:
                return
            if not self._read_only:
                self.checkpoint(truncate=True)
            self._conn.close()
            self._conn = None

    def runnable(self) -> list[DispatchRecord]:
        self.expire_capacity_reservations()
        # Stored observations are sufficient for locally tracked slot releases.
        # The worker additionally refreshes target readiness for external changes.
        for record in self.waiting():
            if record.state == "waiting_capacity" and not record.placement_decision:
                self.promote_waiting(record)
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            return [
                self._snapshot(record)
                for record in self._records.values()
                if record.state == "queued"
            ]

    def pending(self) -> list[DispatchRecord]:
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            return [
                self._snapshot(record)
                for record in self._records.values()
                if record.state == "completion_pending"
            ]

    def pending_followup_turns(self) -> list[tuple[DispatchRecord, dict[str, Any]]]:
        pending: list[tuple[DispatchRecord, dict[str, Any]]] = []
        now = datetime.now(UTC)
        self._require_readable()
        self._yield_to_index_writer()
        with self._index_lock:
            candidates = [
                record
                for record in self._records.values()
                if any(
                    turn.get("delivery_state") == "pending"
                    for turn in record.followup_turns
                )
            ]
        for record in candidates:
            snapshot = self._snapshot(record)
            for turn in snapshot.followup_turns:
                if turn.get("delivery_state") != "pending":
                    continue
                retry_at = turn.get("next_retry_at")
                if retry_at and datetime.fromisoformat(str(retry_at)) > now:
                    continue
                pending.append((snapshot, turn))
        return pending

    def reconcile_interrupted(self) -> list[DispatchRecord]:
        """Make pre-restart work retryable without losing its identity or session."""
        reconciled: list[DispatchRecord] = []
        for record in self.list(limit=1000):
            if record.state not in RECOVERABLE_DISPATCH_STATES:
                continue
            if not record.request_payload:
                record = self.fail(
                    record,
                    "This legacy dispatch was interrupted before durable job details were recorded; retry it from Fleet Operations.",
                    code="orphaned_legacy_dispatch",
                )
                reconciled.append(record)
                continue
            previous_state = record.state
            record.cancel_requested = False
            record.last_error = None
            record.error_code = None
            record = self.transition(
                record,
                "queued",
                "Recovered interrupted dispatch after restart.",
                detail={"previous_state": previous_state},
            )
            reconciled.append(record)
        return reconciled


class DispatchIdempotencyConflict(ValueError):
    def __init__(self, existing: DispatchRecord) -> None:
        super().__init__("idempotency key already belongs to different remote work")
        self.existing = existing


class DispatchCompareConflict(ValueError):
    """A detached dispatch snapshot changed before its atomic mutation."""

    def __init__(self, *, dispatch_id: str, changed_fields: list[str]) -> None:
        super().__init__("dispatch changed before atomic mutation")
        self.dispatch_id = dispatch_id
        self.changed_fields = list(changed_fields)


class DispatchReceiptConflict(ValueError):
    def __init__(self, dispatch_id: str, idempotency_key: str, message: str) -> None:
        super().__init__(message)
        self.dispatch_id = dispatch_id
        self.idempotency_key = idempotency_key


class DispatchStoreReadOnlyError(RuntimeError):
    pass


class ConcurrentCardDispatch(ValueError):
    def __init__(self, existing: DispatchRecord) -> None:
        super().__init__("card already has an active durable dispatch")
        self.existing = existing


class DispatchCapacityExhausted(ValueError):
    def __init__(
        self,
        *,
        limit: int,
        source: str,
        provider: str | None,
        active: int,
        queued: int,
        reservations: int,
        observed_at: datetime,
        consumer_links: list[dict[str, Any]],
    ) -> None:
        super().__init__("dispatch capacity is exhausted")
        self.detail = {
            "code": "capacity_exhausted",
            "message": (
                f"Capacity is exhausted: {active} working + {reservations} "
                f"reserved of {limit} {source} slots; {queued} prompts are "
                "queued behind existing sessions and do not consume slots."
            ),
            "limit": limit,
            "source": source,
            "provider": provider,
            "active_consumers": active,
            "queued_prompts": queued,
            "reservations": reservations,
            "consumed": active + reservations,
            "observed_at": observed_at.isoformat(),
            "consumer_links": consumer_links,
            "recoverable": True,
            "recovery_url": "/fleet?section=overview",
        }


class DispatchQueueFull(ValueError):
    def __init__(
        self,
        *,
        limit: int,
        source: str,
        provider: str | None,
        current: int,
        active_capacity: int,
        observed_at: datetime,
    ) -> None:
        super().__init__("dispatch queue is full")
        self.detail = {
            "code": "dispatch_queue_full",
            "message": (
                f"The durable dispatch queue is full ({current} of {limit}) while "
                f"all {active_capacity} execution slots are occupied."
            ),
            "current_count": current,
            "maximum_count": limit,
            "active_execution_capacity": active_capacity,
            "source": source,
            "provider": provider,
            "observed_at": observed_at.isoformat(),
            "recoverable": True,
            "retry_after_seconds": 5,
            "retry_guidance": "Retry after queued work launches or is cancelled.",
            "remediation_options": [
                "cancel unneeded queued dispatches",
                "increase dispatch_queue_capacity",
                "use a different eligible target",
            ],
            "recovery_url": "/fleet?section=operations",
        }


class DispatchWorker:
    """Supervise durable dispatch consumption on an isolated control lane."""

    def __init__(
        self,
        store: DispatchStore,
        handler: Callable[[DispatchRecord], Awaitable[None]],
        *,
        concurrency: int = 4,
        async_runtime: AsyncRuntime | None = None,
        retry_seconds: float = 0.1,
        retry_max_seconds: float = 5.0,
        rng: random.Random | None = None,
        readiness: Callable[[DispatchRecord], Awaitable[CapacityAdmission]]
        | None = None,
        terminal: Callable[[DispatchRecord, str], Awaitable[None]] | None = None,
        lifecycle_recovery: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.store, self.handler = store, handler
        self.concurrency, self.async_runtime = max(1, concurrency), async_runtime
        # This bounded control lane cannot be consumed by transcript/progress traffic.
        self._control_runtime = (
            AsyncRuntime(
                max_workers=2, max_queue=16, default_timeout=10.0, slow_call_seconds=1.0
            )
            if async_runtime
            else None
        )
        self.retry_seconds = max(0.01, retry_seconds)
        self.retry_max_seconds = max(self.retry_seconds, retry_max_seconds)
        self.rng = rng or random.Random()
        self.readiness = readiness
        self.terminal = terminal
        self.lifecycle_recovery = lifecycle_recovery
        self._last_lifecycle_recovery_at: float | None = None
        self._runner: asyncio.Task[None] | None = None
        self._active: dict[str, asyncio.Task[None]] = {}
        self._wake, self._closing = asyncio.Event(), False
        self.state, self.generation, self.restart_count = "stopped", 0, 0
        self.last_successful_poll_at: datetime | None = None
        self.last_failure_at: datetime | None = None
        self.last_failure_type: str | None = None
        self.last_failure_message: str | None = None
        self.queued_dispatch_count, self.oldest_queued_at = 0, None
        self.poll_failures = 0

    def start(self) -> None:
        if not self._runner or self._runner.done():
            self._closing, self.state = False, "starting"
            self._runner = asyncio.create_task(
                self._supervise(), name="pa-dispatch-supervisor"
            )

    async def _offload(self, operation: str, call, *args, **kwargs):
        if self._control_runtime:
            return await self._control_runtime.run_blocking(
                operation, call, *args, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    def wake(self) -> None:
        self.start()
        self._wake.set()

    async def close(self) -> None:
        self._closing, self.state = True, "stopped"
        self._wake.set()
        tasks = [task for task in self._active.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._runner:
            self._runner.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(self._runner, return_exceptions=True), timeout=1.0
                )
            except TimeoutError:
                self._runner.cancel()
        if self._control_runtime:
            await self._control_runtime.close(drain_timeout=1.0)
        self.state = "stopped"

    def _record_failure(self, exc: BaseException) -> None:
        self.last_failure_at = datetime.now(UTC)
        self.last_failure_type = type(exc).__name__[:120]
        self.last_failure_message = sanitize_text(
            str(exc) or type(exc).__name__, limit=240
        )
        self.poll_failures += 1

    def _backoff(self, failures: int) -> float:
        ceiling = min(
            self.retry_max_seconds,
            self.retry_seconds * (2 ** min(max(0, failures - 1), 10)),
        )
        return self.rng.uniform(ceiling / 2, ceiling)

    async def _wait(self, timeout: float) -> None:
        self._wake.clear()
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=timeout)
        except TimeoutError:
            pass

    async def _supervise(self) -> None:
        while not self._closing:
            self.generation += 1
            self.state = "starting"
            try:
                await self._run_generation()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self._record_failure(exc)
                self.state = "failed"
                logger.exception(
                    "Dispatch worker generation terminated generation=%s failure_type=%s failure_message=%s",
                    self.generation,
                    self.last_failure_type,
                    self.last_failure_message,
                )
            if self._closing:
                break
            self.restart_count += 1
            self.state = "backing_off"
            await self._wait(self._backoff(self.restart_count))
        self.state = "stopped"

    async def _run_generation(self) -> None:
        await self._offload(
            "dispatch.reconcile_interrupted", self.store.reconcile_interrupted
        )
        consecutive_failures = 0
        while not self._closing:
            finished = [task for task in self._active.values() if task.done()]
            for task in finished:
                if not task.cancelled():
                    task.exception()
            self._active = {
                key: task for key, task in self._active.items() if not task.done()
            }
            available = self.concurrency - len(self._active)
            try:
                expired = await self._offload(
                    "dispatch.expire_capacity_reservations",
                    self.store.expire_capacity_reservations,
                )
                if self.terminal:
                    for expired_record in expired:
                        try:
                            await self.terminal(expired_record, "capacity-expired")
                        except Exception:
                            logger.exception(
                                "Dispatch %s capacity-expiry lifecycle callback failed",
                                expired_record.dispatch_id,
                            )
                if self.lifecycle_recovery:
                    loop_now = asyncio.get_running_loop().time()
                    if (
                        self._last_lifecycle_recovery_at is None
                        or loop_now - self._last_lifecycle_recovery_at >= 30.0
                    ):
                        self._last_lifecycle_recovery_at = loop_now
                        try:
                            await self.lifecycle_recovery()
                        except Exception:
                            logger.exception(
                                "Dispatch goal-lifecycle recovery pass failed"
                            )
                if self.readiness:
                    waiting = await self._offload(
                        "dispatch.waiting_read", self.store.waiting
                    )
                    refreshed_scopes: set[tuple[str, str | None]] = set()
                    for waiting_record in waiting:
                        scope = (
                            waiting_record.target_instance_id,
                            waiting_record.capacity_provider,
                        )
                        if scope in refreshed_scopes:
                            continue
                        refreshed_scopes.add(scope)
                        try:
                            refreshed = await self.readiness(waiting_record)
                            await self._offload(
                                "dispatch.promote_waiting",
                                self.store.promote_waiting,
                                waiting_record,
                                refreshed,
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:  # noqa: BLE001 - readiness adapters
                            detail = getattr(exc, "detail", None)
                            if isinstance(detail, dict):
                                code = str(detail.get("code") or "target_not_ready")
                                reason = str(detail.get("message") or detail)
                            else:
                                code = type(exc).__name__
                                reason = (
                                    str(exc)
                                    or "Target readiness could not be confirmed."
                                )
                            await self._offload(
                                "dispatch.block_waiting",
                                self.store.block_waiting,
                                waiting_record,
                                code=code,
                                reason=reason,
                            )
                runnable = await self._offload(
                    "dispatch.runnable_read", self.store.runnable
                )
            except asyncio.CancelledError:
                raise
            except (
                BlockingQueueFull,
                BlockingOperationTimeout,
                AsyncRuntimeClosed,
                TimeoutError,
                OSError,
            ) as exc:
                consecutive_failures += 1
                self._record_failure(exc)
                self.state = "backing_off"
                delay = self._backoff(consecutive_failures)
                logger.warning(
                    "Dispatch polling deferred generation=%s failure_type=%s failure_message=%s retry_seconds=%.3f",
                    self.generation,
                    self.last_failure_type,
                    self.last_failure_message,
                    delay,
                )
                await self._wait(delay)
                continue
            consecutive_failures = 0
            self.last_successful_poll_at = datetime.now(UTC)
            self.queued_dispatch_count = len(runnable)
            self.oldest_queued_at = min(
                (record.created_at for record in runnable), default=None
            )
            self.state = "running"
            for record in runnable:
                if available <= 0:
                    break
                if record.dispatch_id in self._active:
                    continue
                task = asyncio.create_task(
                    self._execute(record), name=f"pa-dispatch-{record.dispatch_id}"
                )
                self._active[record.dispatch_id] = task
                available -= 1
            await self._wait(0.2 if self._active else 2.0)

    def snapshot(self) -> dict[str, Any]:
        age = (
            max(0.0, (datetime.now(UTC) - self.oldest_queued_at).total_seconds())
            if self.oldest_queued_at
            else None
        )
        live = bool(self._runner and not self._runner.done() and not self._closing)
        unhealthy = self.queued_dispatch_count > 0 and (
            not live or self.state not in {"starting", "running", "backing_off"}
        )
        return {
            "state": self.state,
            "live": live,
            "generation": self.generation,
            "restart_count": self.restart_count,
            "last_successful_poll_at": self.last_successful_poll_at.isoformat()
            if self.last_successful_poll_at
            else None,
            "last_failure_at": self.last_failure_at.isoformat()
            if self.last_failure_at
            else None,
            "last_failure": {
                "type": self.last_failure_type,
                "message": self.last_failure_message,
            }
            if self.last_failure_type
            else None,
            "queued_dispatch_count": self.queued_dispatch_count,
            "waiting_dispatch_count": self.store.queue_snapshot()["total"],
            "oldest_queued_age_seconds": round(age, 3) if age is not None else None,
            "active_dispatch_count": len(self._active),
            "poll_failures": self.poll_failures,
            "unhealthy": unhealthy,
            "warning": "Durable queued dispatches have no live polling consumer."
            if unhealthy
            else None,
            "control_lane": self._control_runtime.snapshot()
            if self._control_runtime
            else None,
        }

    async def _execute(self, record: DispatchRecord) -> None:
        record.stage_attempts += 1
        await self._offload("dispatch.record_write", self.store.put, record)
        try:
            await self.handler(record)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Dispatch %s failed", record.dispatch_id)
            detail = getattr(exc, "detail", None)
            code, recoverable, message = "dispatch_failed", True, str(detail or exc)
            if isinstance(detail, dict):
                code, recoverable, message = (
                    str(detail.get("code") or code),
                    bool(detail.get("recoverable", True)),
                    str(detail.get("message") or detail),
                )
            failed = await self._offload(
                "dispatch.record_fail",
                self.store.fail,
                record,
                message,
                code=code,
                recoverable=recoverable,
                detail=detail if isinstance(detail, dict) else {},
            )
            if self.terminal:
                try:
                    await self.terminal(failed, "failed")
                except Exception:
                    logger.exception(
                        "Dispatch %s terminal lifecycle callback failed",
                        record.dispatch_id,
                    )


class CompletionOutbox:
    """Retries completion until the authoritative origin acknowledges it."""

    def __init__(
        self,
        store: DispatchStore,
        token: str,
        *,
        retry_seconds: float = 5.0,
        retry_max_seconds: float = 300.0,
        max_attempts: int = 12,
        rng: random.Random | None = None,
        async_runtime: AsyncRuntime | None = None,
        disposition_notifier: Callable[[str, dict[str, Any]], Awaitable[Any] | Any]
        | None = None,
    ) -> None:
        self.store = store
        self.token = token
        self.retry_seconds = retry_seconds
        self.retry_max_seconds = max(retry_seconds, retry_max_seconds)
        self.max_attempts = max(1, max_attempts)
        self.rng = rng or random.Random()
        self.async_runtime = async_runtime
        self.disposition_notifier = disposition_notifier
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._closing = False
        self._client: httpx.AsyncClient | None = None

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

    def queue(self, session_id: str, payload: dict[str, Any]) -> bool:
        queued = self.store.queue_completion_payload(session_id, payload)
        if queued:
            self._wake.set()
        return queued

    def retry_delivery(self, dispatch_id: str) -> DispatchRecord:
        """Re-arm preserved completion evidence without changing its payload."""
        record = self.store.get(dispatch_id)
        if not record or record.completion_payload is None:
            raise ValueError("dispatch has no preserved completion evidence")
        if record.acknowledged_at:
            record.lifecycle_inconsistencies.append(
                {
                    "kind": "completion_replay_prevented",
                    "observed_at": datetime.now(UTC).isoformat(),
                    "classification": record.completion_delivery_class,
                    "reason": (
                        "Acknowledged immutable completion cannot be replayed; "
                        "card reconciliation is a separate state machine."
                    ),
                }
            )
            record.lifecycle_inconsistencies = record.lifecycle_inconsistencies[-50:]
            return self.store.put(record)
        record.completion_delivery_class = "operator_retry"
        record.completion_next_retry_at = None
        record.reconciliation_condition = "operator_retry"
        record = self.store.transition(
            record,
            "completion_pending",
            "Operator re-armed preserved completion evidence for delivery.",
            detail={"previous_attempts": record.attempts},
        )
        self._wake.set()
        return record

    async def drain(self, timeout: float = 5.0) -> None:
        async def wait_empty() -> None:
            while await self._offload(
                "dispatch.completion_pending_read", self.store.pending
            ) or await self._offload(
                "dispatch.followup_pending_read",
                self.store.pending_followup_turns,
            ):
                self._wake.set()
                await asyncio.sleep(0.05)

        try:
            await asyncio.wait_for(wait_empty(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    async def close(self, timeout: float = 5.0) -> None:
        await self.drain(timeout)
        self._closing = True
        self._wake.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=1.0)
            except asyncio.TimeoutError:
                self._task.cancel()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _run(self) -> None:
        while not self._closing:
            pending = await self._offload(
                "dispatch.completion_pending_read", self.store.pending
            )
            followups = await self._offload(
                "dispatch.followup_pending_read", self.store.pending_followup_turns
            )
            if not pending and not followups:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), self.retry_seconds)
                except asyncio.TimeoutError:
                    pass
                continue
            for record in pending:
                if record.completion_delivery_class in {
                    "transport_exhausted",
                    "permanent_failure",
                    "semantic_conflict",
                }:
                    continue
                if (
                    record.completion_next_retry_at
                    and record.completion_next_retry_at > datetime.now(UTC)
                ):
                    continue
                await self._send(record)
            for record, turn in followups:
                await self._send_followup(record, turn)
            await asyncio.sleep(min(self.retry_seconds, 1.0))

    async def _send_followup(
        self, record: DispatchRecord, turn: dict[str, Any]
    ) -> None:
        turn["delivery_attempts"] = int(turn.get("delivery_attempts") or 0) + 1
        self.store.put(record)
        key = (
            f"{record.mutation_id}:turn:"
            f"{turn.get('prompt_id') or turn.get('idempotency_key')}"
        )
        headers = {"Idempotency-Key": key}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            response = await self._http_client().post(
                f"{record.authority_url.rstrip('/')}/api/fleet/dispatch/"
                f"{record.dispatch_id}/turn-end",
                headers=headers,
                json={
                    "mutation_id": record.mutation_id,
                    "source_instance_id": record.target_instance_id,
                    "session_id": record.session_id,
                    "turn_id": str(
                        turn.get("prompt_id") or turn.get("idempotency_key")
                    ),
                    "result": turn.get("result") or {},
                    "final_report": turn.get("final_report"),
                },
            )
            if response.status_code >= 400:
                raise httpx.HTTPStatusError(
                    f"HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
            turn["delivery_state"] = "acknowledged"
            turn["delivered_at"] = datetime.now(UTC).isoformat()
            turn["delivery_error"] = None
            turn["next_retry_at"] = None
        except (httpx.HTTPError, ValueError) as exc:
            turn["delivery_error"] = sanitize_text(exc, limit=500)
            if int(turn["delivery_attempts"]) >= self.max_attempts:
                turn["delivery_state"] = "failed"
                turn["next_retry_at"] = None
            else:
                delay = min(
                    self.retry_max_seconds,
                    self.retry_seconds
                    * (2 ** max(0, int(turn["delivery_attempts"]) - 1)),
                )
                turn["next_retry_at"] = (
                    datetime.now(UTC) + timedelta(seconds=delay)
                ).isoformat()
        self.store.put(record)

    def _schedule_retry(self, record: DispatchRecord, error: str) -> None:
        record.last_error = sanitize_text(error, limit=500)
        if record.attempts >= self.max_attempts:
            record.completion_delivery_class = "transport_exhausted"
            record.completion_next_retry_at = None
            return
        ceiling = min(
            self.retry_max_seconds,
            self.retry_seconds * (2 ** max(0, record.attempts - 1)),
        )
        delay = self.rng.uniform(ceiling / 2, ceiling)
        record.completion_delivery_class = "transport_retry"
        record.completion_next_retry_at = datetime.now(UTC) + timedelta(seconds=delay)

    async def _send(self, record: DispatchRecord) -> None:
        record.attempts += 1
        await self._offload("dispatch.record_write", self.store.put, record)
        headers = {"Idempotency-Key": record.mutation_id}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            request = self._http_client().post(
                f"{record.authority_url.rstrip('/')}/api/fleet/dispatch/{record.dispatch_id}/complete",
                json={
                    "mutation_id": record.mutation_id,
                    "card_id": record.card_id,
                    "realm_id": record.realm_id,
                    "card_version": record.card_version,
                    "source_instance_id": record.target_instance_id,
                    "session_id": record.session_id,
                    "result": record.completion_payload or {},
                    "disposition": (record.completion_payload or {}).get(
                        "card_disposition"
                    ),
                    "final_report": (
                        record.final_report.model_dump(mode="json")
                        if record.final_report
                        else None
                    ),
                },
                headers=headers,
            )
            response = (
                await self.async_runtime.observe(
                    "http.dispatch_completion", request, timeout=15.0
                )
                if self.async_runtime
                else await request
            )
            if response.status_code in {200, 208}:
                try:
                    acknowledgement = await self._offload(
                        "dispatch.response_json", response.json
                    )
                except ValueError:
                    acknowledgement = {}
                disposition = acknowledgement.get("card_disposition") or {}
                if isinstance(disposition, dict):
                    record.card_disposition_status = disposition.get("status")
                    record.card_disposition_reason = disposition.get("reason")
                    record.card_lane_before = disposition.get("lane_before")
                    record.card_lane_after = disposition.get("lane_after")
                record.acknowledged_at = datetime.now(UTC)
                record.last_error = None
                record.completion_delivery_class = "acknowledged"
                record.completion_next_retry_at = None
                await self._offload(
                    "dispatch.record_complete",
                    self.store.transition,
                    record,
                    "completed",
                    (
                        "Authority acknowledged dispatch completion separately "
                        "from card disposition."
                    ),
                )
                if self.disposition_notifier and record.session_id:
                    notice = {
                        "contract": (record.completion_payload or {}).get(
                            "card_disposition"
                        ),
                        "persistence": "durable",
                        "authority_acknowledged": True,
                        "acknowledged_at": record.acknowledged_at.isoformat(),
                        "status": record.card_disposition_status or "invalid",
                        "reason": record.card_disposition_reason,
                        "lane_before": record.card_lane_before,
                        "lane_after": record.card_lane_after,
                        "reconciliation_state": record.reconciliation_state,
                    }
                    try:
                        result = self.disposition_notifier(record.session_id, notice)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        # The authority ACK is already durable. A local transcript
                        # failure must not cause the immutable completion to be
                        # redelivered or misrepresented as unacknowledged.
                        logger.exception(
                            "Failed to persist card-disposition acknowledgement"
                        )
            else:
                try:
                    detail = (
                        await self._offload("dispatch.response_json", response.json)
                    ).get("detail") or {}
                except ValueError, AttributeError:
                    detail = {}
                code = str(detail.get("code") or "")
                message = f"HTTP {response.status_code}: {response.text[:500]}"
                if response.status_code == 409 and code in {
                    "authority_version_conflict",
                    "authority_card_missing",
                }:
                    record.last_error = sanitize_text(message, limit=500)
                    record.completion_delivery_class = "semantic_conflict"
                    record.completion_next_retry_at = None
                    record.reconciliation_state = "conflict_requires_resolution"
                    record.reconciliation_reason = f"A mixed-version authority rejected immutable completion before reconciliation: {code}."
                    record.reconciliation_condition = (
                        "peer_protocol_upgrade_or_operator_retry"
                    )
                    record.reconciliation_last_dependency_error = record.last_error
                    record.reconciliation_recoverable = True
                    record.reconciliation_updated_at = datetime.now(UTC)
                    await self._offload(
                        "dispatch.record_complete",
                        self.store.transition,
                        record,
                        "completed",
                        "Agent turn ended; legacy authority reconciliation needs attention.",
                    )
                elif response.status_code == 409 or response.status_code in {
                    401,
                    403,
                    404,
                    422,
                }:
                    record.last_error = sanitize_text(message, limit=500)
                    record.completion_delivery_class = "permanent_failure"
                    record.completion_next_retry_at = None
                    record.recoverable = False
                    await self._offload("dispatch.record_write", self.store.put, record)
                else:
                    self._schedule_retry(record, message)
                    await self._offload("dispatch.record_write", self.store.put, record)
        except httpx.HTTPError as exc:
            self._schedule_retry(record, str(exc))
            await self._offload("dispatch.record_write", self.store.put, record)
