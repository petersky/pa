"""Durable, idempotent fleet dispatch and completion mutations."""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

from pa.core.async_runtime import (
    AsyncRuntime,
    AsyncRuntimeClosed,
    BlockingOperationTimeout,
    BlockingQueueFull,
)
from pa.core.io import atomic_write_json
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

logger = logging.getLogger(__name__)

DISPATCH_STAGES = {
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
                    and action.name.value
                    not in {"no_action", "record_turn_outcome"}
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
    """Atomic JSON ledger shared by dispatch admission, worker, and outbox."""

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "dispatch_mutations.json"
        self.metrics_path = data_dir / "dispatch_queue_metrics.json"
        self._records: dict[str, DispatchRecord] = {}
        self._lock = RLock()
        try:
            metrics = json.loads(self.metrics_path.read_text())
            self._queue_rejections = max(0, int(metrics.get("rejections") or 0))
        except (OSError, ValueError, TypeError):
            self._queue_rejections = 0
        self._load()

    def _record_queue_rejection_locked(self) -> None:
        self._queue_rejections += 1
        atomic_write_json(
            self.metrics_path,
            {"rejections": self._queue_rejections, "updated_at": datetime.now(UTC).isoformat()},
        )

    def _load(self) -> None:
        try:
            payload = json.loads(self.path.read_text())
        except (OSError, ValueError, TypeError):
            self._records = {}
            return
        if not isinstance(payload, dict):
            self._records = {}
            return
        migrated = False
        records: dict[str, DispatchRecord] = {}
        for key, value in payload.items():
            if not isinstance(value, dict):
                logger.warning("Ignoring malformed persisted dispatch %s", key)
                migrated = True
                continue
            candidate = dict(value)
            # Progress is observational evidence, not dispatch identity. Keep a
            # dispatch usable even when one historical progress item no longer
            # validates; valid siblings are retained and rewritten below.
            valid_events = []
            for raw_event in candidate.get("progress_events") or []:
                try:
                    valid_events.append(
                        DispatchProgressEventV1.model_validate(raw_event)
                    )
                except (ValueError, TypeError):
                    logger.warning(
                        "Ignoring malformed historical progress event for dispatch %s",
                        key,
                    )
                    migrated = True
            candidate["progress_events"] = [
                event.model_dump(mode="json") for event in valid_events
            ]
            for optional_field, model in (
                ("progress_heartbeat", DispatchProgressHeartbeatV1),
                ("final_report", CompletionReportV1),
            ):
                raw = candidate.get(optional_field)
                if raw is None:
                    continue
                try:
                    candidate[optional_field] = model.model_validate(raw).model_dump(
                        mode="json"
                    )
                except (ValueError, TypeError):
                    candidate[optional_field] = None
                    logger.warning(
                        "Ignoring malformed historical %s for dispatch %s",
                        optional_field,
                        key,
                    )
                    migrated = True
            try:
                records[str(key)] = DispatchRecord.model_validate(candidate)
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "Ignoring malformed persisted dispatch %s (%s)",
                    key,
                    exc.__class__.__name__,
                )
                migrated = True
        self._records = records
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
        if migrated:
            self._save()

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            self.path,
            {
                key: value.model_dump(mode="json")
                for key, value in self._records.items()
            },
        )

    def get(self, dispatch_id: str) -> DispatchRecord | None:
        with self._lock:
            return self._records.get(dispatch_id)

    def list(
        self, *, target_instance_id: str | None = None, limit: int = 100
    ) -> list[DispatchRecord]:
        with self._lock:
            self._refresh_queue_positions_locked()
            records = list(self._records.values())
        if target_instance_id:
            records = [
                record
                for record in records
                if record.target_instance_id == target_instance_id
            ]
        return sorted(records, key=lambda record: record.updated_at, reverse=True)[
            :limit
        ]

    def capacity_snapshot(self, target_instance_id: str) -> dict[str, Any]:
        """Return authority-local reservations and waiting work for one target."""

        self.expire_capacity_reservations()
        with self._lock:
            records = [
                record
                for record in self._records.values()
                if record.target_instance_id == target_instance_id
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
            return {
                "dispatch_reservations": sum(
                    record.state in CAPACITY_RESERVATION_STATES for record in records
                ),
                "dispatch_waiting": sum(
                    record.state in QUEUE_CONSUMING_STATES for record in records
                ),
                "provider_concurrency": providers,
            }

    def by_session(self, session_id: str) -> DispatchRecord | None:
        with self._lock:
            matches = sorted(
                (r for r in self._records.values() if r.session_id == session_id),
                key=lambda record: record.updated_at,
                reverse=True,
            )
        return next(
            (
                record
                for record in matches
                if record.state not in {"completed", "acknowledged", "cancelled"}
            ),
            matches[0] if matches else None,
        )

    def by_idempotency(
        self, target_instance_id: str, idempotency_key: str
    ) -> DispatchRecord | None:
        with self._lock:
            return next(
                (
                    record
                    for record in self._records.values()
                    if record.target_instance_id == target_instance_id
                    and record.idempotency_key == idempotency_key
                ),
                None,
            )

    def by_authority_idempotency(
        self, authority_instance_id: str, idempotency_key: str
    ) -> DispatchRecord | None:
        with self._lock:
            return next(
                (
                    record
                    for record in self._records.values()
                    if record.authority_instance_id == authority_instance_id
                    and record.idempotency_key == idempotency_key
                ),
                None,
            )

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
        global_consumed = (
            max(
                capacity.observed_global_active
                if capacity.observed_global_active is not None
                else capacity.observed_active,
                global_running,
            )
            + (
                capacity.observed_global_queued
                if capacity.observed_global_queued is not None
                else capacity.observed_queued
            )
            + max(
                capacity.observed_global_reservations
                if capacity.observed_global_reservations is not None
                else capacity.observed_reservations,
                global_reservations,
            )
        )
        provider_consumed = 0
        if capacity.provider_limit is not None:
            provider_consumed = (
                max(
                    capacity.observed_provider_active or 0,
                    provider_running,
                )
                + (capacity.observed_provider_queued or 0)
                + max(
                    capacity.observed_provider_reservations or 0,
                    provider_reservations,
                )
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
            global_queue_limit is not None
            and global_queue_count >= global_queue_limit
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
            self.expire_capacity_reservations()
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
            if existing:
                if existing.request_fingerprint != record.request_fingerprint:
                    raise DispatchIdempotencyConflict(existing)
                return existing, True

            if record.card_id and not record.allow_concurrent:
                active = next(
                    (
                        item
                        for item in self._records.values()
                        if item.card_id == record.card_id
                        and item.realm_id == record.realm_id
                        and item.state
                        not in {"failed", "completed", "cancelled", "acknowledged"}
                    ),
                    None,
                )
                if active:
                    raise ConcurrentCardDispatch(active)

            admission_state = "queued"
            if capacity:
                now = datetime.now(UTC)
                execution_full, queue_full, queue_count, reserved, queue_max = (
                    self._constraint_counts_locked(record, capacity)
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
                            limit=queue_max if queue_max is not None else capacity.queue_limit,
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
                    seq=1,
                    state=admission_state,
                    message=(
                        "Dispatch durably queued until execution capacity is available."
                        if admission_state == "waiting_capacity"
                        else "Dispatch admitted for background execution."
                    ),
                )
            )
            record.updated_at = datetime.now(UTC)
            self._records[record.dispatch_id] = record
            self._refresh_queue_positions_locked()
            self._save()
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
            return record, False

    def put(self, record: DispatchRecord) -> DispatchRecord:
        with self._lock:
            existing = self._records.get(record.dispatch_id)
            if existing and existing.mutation_id != record.mutation_id:
                raise ValueError("dispatch id already belongs to another mutation")
            record.updated_at = datetime.now(UTC)
            self._records[record.dispatch_id] = record
            self._save()
        return record

    def retry_with_capacity(
        self,
        record: DispatchRecord,
        capacity: CapacityAdmission,
        *,
        idempotency_key: str,
    ) -> DispatchRecord:
        """Atomically renew the same target reservation for a safe retry."""

        with self._lock:
            self.expire_capacity_reservations()
            current = self._records.get(record.dispatch_id)
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
                        limit=queue_max if queue_max is not None else capacity.queue_limit,
                        source=capacity.queue_source,
                        provider=capacity.provider,
                        current=queue_count,
                        active_capacity=capacity.limit,
                        observed_at=capacity.observed_at,
                    )
                current.state = "waiting_capacity"
                current.queue_admitted_at = current.queue_admitted_at or now
                current.queue_wait_reason = "Waiting for execution capacity after retry."
            else:
                current.state = "queued"
                current.capacity_reserved_at = now
                current.capacity_reservation_expires_at = (
                    now + CAPACITY_RESERVATION_TTL
                )
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
            self._refresh_queue_positions_locked()
            self._save()
            return current

    def allocate_progress_sequence(self, dispatch_id: str) -> int:
        """Allocate a durable per-dispatch sequence across restarts and callbacks."""
        with self._lock:
            record = self._records.get(dispatch_id)
            if not record:
                raise ValueError("dispatch not found")
            sequence = max(1, record.progress_next_sequence)
            record.progress_next_sequence = sequence + 1
            record.updated_at = datetime.now(UTC)
            self._save()
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
            record = self._records.get(dispatch_id)
            if not record:
                raise ValueError("dispatch not found")
            if (
                record.authority_instance_id == authority_instance_id
                and record.card_version == authority_version
            ):
                return record
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
            self._save()
            return record

    def ingest_progress(
        self, event: DispatchProgressEventV1, *, delivered: bool = False
    ) -> ProgressIngestResult:
        """Idempotently retain a bounded, safely reorderable checkpoint history."""
        sanitized = sanitize_progress_event(event)
        with self._lock:
            record = self._records.get(sanitized.dispatch_id)
            if not record:
                raise ValueError("dispatch not found")
            self._validate_progress_provenance(record, sanitized)
            if delivered and sanitized.delivered_at is None:
                sanitized.delivered_at = datetime.now(UTC)
            if sanitized.idempotency_key in record.progress_seen_keys:
                return ProgressIngestResult(
                    accepted=True,
                    status="duplicate",
                    dispatch_id=record.dispatch_id,
                    sequence=sanitized.sequence,
                    idempotency_key=sanitized.idempotency_key,
                )
            same_sequence = next(
                (
                    existing
                    for existing in record.progress_events
                    if existing.sequence == sanitized.sequence
                ),
                None,
            )
            if same_sequence:
                record.progress_conflicts += 1
                record.progress_seen_keys.append(sanitized.idempotency_key)
                record.progress_seen_keys = record.progress_seen_keys[
                    -MAX_PROGRESS_SEEN_KEYS:
                ]
                record.updated_at = datetime.now(UTC)
                self._save()
                return ProgressIngestResult(
                    accepted=False,
                    status="conflict",
                    dispatch_id=record.dispatch_id,
                    sequence=sanitized.sequence,
                    idempotency_key=sanitized.idempotency_key,
                )
            latest = record.latest_progress
            if (
                latest
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
                self._save()
                return ProgressIngestResult(
                    accepted=True,
                    status="coalesced",
                    dispatch_id=record.dispatch_id,
                    sequence=sanitized.sequence,
                    idempotency_key=sanitized.idempotency_key,
                )
            previous_max = max(
                (item.sequence for item in record.progress_events), default=0
            )
            record.progress_events.append(sanitized)
            record.progress_events.sort(
                key=lambda item: (item.sequence, item.occurred_at, item.idempotency_key)
            )
            record.progress_events = record.progress_events[-MAX_PROGRESS_EVENTS:]
            record.progress_seen_keys.append(sanitized.idempotency_key)
            record.progress_seen_keys = record.progress_seen_keys[
                -MAX_PROGRESS_SEEN_KEYS:
            ]
            record.progress_protocol_version = sanitized.schema_version
            record.progress_next_sequence = max(
                record.progress_next_sequence, sanitized.sequence + 1
            )
            record.updated_at = datetime.now(UTC)
            self._save()
            return ProgressIngestResult(
                accepted=True,
                status="late" if sanitized.sequence < previous_max else "accepted",
                dispatch_id=record.dispatch_id,
                sequence=sanitized.sequence,
                idempotency_key=sanitized.idempotency_key,
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
            record = self._records.get(sanitized.dispatch_id)
            if not record:
                raise ValueError("dispatch not found")
            self._validate_progress_provenance(record, sanitized)
            if sanitized.idempotency_key in record.progress_seen_keys:
                return ProgressIngestResult(
                    accepted=True,
                    status="duplicate",
                    dispatch_id=record.dispatch_id,
                    sequence=sanitized.sequence,
                    idempotency_key=sanitized.idempotency_key,
                )
            current = record.progress_heartbeat
            if current and sanitized.sequence < current.sequence:
                record.progress_seen_keys.append(sanitized.idempotency_key)
                record.progress_seen_keys = record.progress_seen_keys[
                    -MAX_PROGRESS_SEEN_KEYS:
                ]
                self._save()
                return ProgressIngestResult(
                    accepted=True,
                    status="late",
                    dispatch_id=record.dispatch_id,
                    sequence=sanitized.sequence,
                    idempotency_key=sanitized.idempotency_key,
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
            self._save()
            return ProgressIngestResult(
                accepted=True,
                status="accepted",
                dispatch_id=record.dispatch_id,
                sequence=sanitized.sequence,
                idempotency_key=sanitized.idempotency_key,
            )

    def pending_progress(
        self, originating_instance_id: str
    ) -> list[
        tuple[
            DispatchRecord,
            DispatchProgressEventV1 | DispatchProgressHeartbeatV1,
        ]
    ]:
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
                        pending.append((record, event))
                heartbeat = record.progress_heartbeat
                if heartbeat and heartbeat.delivered_at is None:
                    pending.append((record, heartbeat))
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
            record = self._records.get(dispatch_id)
            if not record:
                return
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
            self._save()

    def mark_progress_delivery_failed(
        self, dispatch_id: str, idempotency_key: str, error: str
    ) -> None:
        with self._lock:
            record = self._records.get(dispatch_id)
            if not record:
                return
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
            self._save()

    def build_final_report(
        self, dispatch_id: str, result: dict[str, Any]
    ) -> CompletionReportV1 | None:
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
            record = self._records.get(dispatch_id)
            if not record:
                raise ValueError("dispatch not found")
            record.final_report = sanitize_completion_report(report)
            record.updated_at = datetime.now(UTC)
            self._save()
            return record

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
        if previous_state in QUEUE_CONSUMING_STATES or state in QUEUE_CONSUMING_STATES:
            with self._lock:
                self._refresh_queue_positions_locked()
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
        self, *, now: datetime | None = None
    ) -> list[DispatchRecord]:
        """Fail timed-out pre-start work and durably release its slot."""

        checked_at = now or datetime.now(UTC)
        expired: list[DispatchRecord] = []
        with self._lock:
            for record in self._records.values():
                if (
                    record.state not in CAPACITY_RESERVATION_STATES
                    or record.capacity_reservation_expires_at is None
                    or record.capacity_reservation_expires_at > checked_at
                ):
                    continue
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
                self._save()
                logger.warning(
                    "fleet capacity reservations timed out count=%s dispatches=%s",
                    len(expired),
                    [record.dispatch_id for record in expired],
                )
        return expired

    def waiting(self) -> list[DispatchRecord]:
        with self._lock:
            self._refresh_queue_positions_locked()
            waiting = [
                record
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
            current = self._records.get(record.dispatch_id)
            if not current or current.state not in QUEUE_CONSUMING_STATES:
                return False
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
                self._refresh_queue_positions_locked()
                self._save()
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
            current.queue_audit.append(
                {"action": "promoted", "at": now.isoformat()}
            )
            current.updated_at = now
            self._refresh_queue_positions_locked()
            self._save()
            return True

    def block_waiting(
        self, record: DispatchRecord, *, code: str, reason: str
    ) -> DispatchRecord:
        with self._lock:
            current = self._records.get(record.dispatch_id)
            if not current or current.state not in QUEUE_CONSUMING_STATES:
                return current or record
            current.state = "blocked"
            current.queue_blocked_code = sanitize_text(code, limit=120)
            current.queue_wait_reason = sanitize_text(reason, limit=500)
            current.updated_at = datetime.now(UTC)
            self._refresh_queue_positions_locked()
            self._save()
            return current

    def reprioritize(
        self,
        record: DispatchRecord,
        *,
        priority: int,
        principal_id: str,
        idempotency_key: str,
    ) -> DispatchRecord:
        with self._lock:
            current = self._records.get(record.dispatch_id)
            if not current or current.state not in QUEUE_CONSUMING_STATES:
                raise ValueError("only waiting dispatches can be reprioritized")
            operation = f"priority:{priority}"
            previous = current.control_operations.get(idempotency_key)
            if previous and previous != operation:
                raise DispatchIdempotencyConflict(current)
            if previous == operation:
                return current
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
            self._refresh_queue_positions_locked()
            self._save()
            return current

    def queue_snapshot(self) -> dict[str, Any]:
        waiting = self.waiting()
        now = datetime.now(UTC)
        blocked = sum(item.state == "blocked" for item in waiting)
        ages = [
            max(0.0, (now - (item.queue_admitted_at or item.created_at)).total_seconds())
            for item in waiting
        ]
        wait_times = [
            max(
                0.0,
                ((item.queue_launched_at or now) - (item.queue_admitted_at or item.created_at)).total_seconds(),
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
                "wait_time_max_seconds": round(max(wait_times), 3) if wait_times else 0.0,
                "starvation_count": starvation,
            },
            "alerts": (["dispatch_queue_starvation"] if starvation else []),
        }

    def runnable(self) -> list[DispatchRecord]:
        self.expire_capacity_reservations()
        # Stored observations are sufficient for locally tracked slot releases.
        # The worker additionally refreshes target readiness for external changes.
        for record in self.waiting():
            if record.state == "waiting_capacity" and not record.placement_decision:
                self.promote_waiting(record)
        return [record for record in self.list(limit=1000) if record.state == "queued"]

    def pending(self) -> list[DispatchRecord]:
        return [
            record
            for record in self.list(limit=1000)
            if record.state == "completion_pending"
        ]

    def pending_followup_turns(self) -> list[tuple[DispatchRecord, dict[str, Any]]]:
        pending: list[tuple[DispatchRecord, dict[str, Any]]] = []
        now = datetime.now(UTC)
        for record in self.list(limit=1000):
            for turn in record.followup_turns:
                if turn.get("delivery_state") != "pending":
                    continue
                retry_at = turn.get("next_retry_at")
                if retry_at and datetime.fromisoformat(str(retry_at)) > now:
                    continue
                pending.append((record, turn))
        return pending

    def reconcile_interrupted(self) -> list[DispatchRecord]:
        """Make pre-restart work retryable without losing its identity or session."""
        reconciled: list[DispatchRecord] = []
        for record in self.list(limit=1000):
            if record.state not in RECOVERABLE_DISPATCH_STATES:
                continue
            if not record.request_payload:
                self.fail(
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
            self.transition(
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
                f"Capacity is exhausted: {active} working + {queued} queued + "
                f"{reservations} reserved of {limit} {source} slots."
            ),
            "limit": limit,
            "source": source,
            "provider": provider,
            "active_consumers": active,
            "queued_prompts": queued,
            "reservations": reservations,
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
                                reason = str(exc) or "Target readiness could not be confirmed."
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
            await self._offload(
                "dispatch.record_fail",
                self.store.fail,
                record,
                message,
                code=code,
                recoverable=recoverable,
                detail=detail if isinstance(detail, dict) else {},
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
        record = self.store.by_session(session_id)
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
            self.store.put(record)
            self._wake.set()
            return True
        if not record or record.state not in {"running", "completion_pending"}:
            return False
        record.completion_payload = payload
        record.last_error = None
        record.completion_delivery_class = "pending"
        record.completion_next_retry_at = None
        self.store.transition(
            record,
            "completion_pending",
            "Agent turn ended; dispatch completion queued for delivery to the authority.",
        )
        self._wake.set()
        return True

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
            self.store.put(record)
            return record
        record.completion_delivery_class = "operator_retry"
        record.completion_next_retry_at = None
        record.reconciliation_condition = "operator_retry"
        self.store.transition(
            record,
            "completion_pending",
            "Operator re-armed preserved completion evidence for delivery.",
            detail={"previous_attempts": record.attempts},
        )
        self._wake.set()
        return record

    async def drain(self, timeout: float = 5.0) -> None:
        async def wait_empty() -> None:
            while (
                await self._offload(
                    "dispatch.completion_pending_read", self.store.pending
                )
                or await self._offload(
                    "dispatch.followup_pending_read",
                    self.store.pending_followup_turns,
                )
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
