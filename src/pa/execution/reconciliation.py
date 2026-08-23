"""Bounded recovery for card-linked turns that omit a disposition."""

from __future__ import annotations

import asyncio
import copy
import logging
import random
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from pa.acp.final_message import assemble_final_assistant_message
from pa.core.background import BackgroundTaskSupervisor
from pa.domain.models import CardLane
from pa.execution.dispatch import (
    CompletionOutbox,
    DispatchEvent,
    DispatchRecord,
    DispatchStore,
)
from pa.execution.disposition import extract_card_disposition, parse_card_disposition
from pa.prompts import PROMPTS

if TYPE_CHECKING:
    from pa.domain.store import Store
    from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime

logger = logging.getLogger(__name__)

RECONCILIATION_SOURCE_PREFIX = "card-reconciliation:"
RECONCILIATION_TERMINAL_STATES = {
    "not_required",
    "resolved",
    "skipped_closed",
    "skipped_non_resumable",
    "failed",
    "exhausted",
    "already_satisfied",
}


class CompletionReconciler:
    """Prompt one resumable session at most twice, with durable diagnostics."""

    def __init__(
        self,
        dispatch_store: DispatchStore,
        agent: AgentSessionManager,
        completion_outbox: CompletionOutbox,
        card_store: Store,
        supervisor_service: Callable[[], Any | None],
        *,
        retry_seconds: float = 5.0,
        max_attempts: int = 30,
        retry_max_seconds: float = 300.0,
        rng: random.Random | None = None,
    ) -> None:
        self.dispatch_store = dispatch_store
        self.agent = agent
        self.completion_outbox = completion_outbox
        self.card_store = card_store
        self.supervisor_service = supervisor_service
        self.retry_seconds = max(0.01, retry_seconds)
        self.max_attempts = max(1, max_attempts)
        self.retry_max_seconds = max(self.retry_seconds, retry_max_seconds)
        self.rng = rng or random.Random()
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._lock = asyncio.Lock()
        self._closing = False
        self._supervisor = BackgroundTaskSupervisor(
            "completion-reconciler",
            self._run,
            self.dispatch_store.db_path.parent / "completion_reconciler_worker.json",
        )

    async def _offload(self, operation: str, call, *args, **kwargs):
        runtime = getattr(self.agent, "async_runtime", None)
        if runtime:
            return await runtime.run_blocking(operation, call, *args, **kwargs)
        return await asyncio.to_thread(call, *args, **kwargs)

    def start(self) -> None:
        self._closing = False
        self._supervisor.start()
        self._task = self._supervisor._task

    async def close(self) -> None:
        self._closing = True
        self._wake.set()
        await self._supervisor.close()

    async def recover(self) -> None:
        """Adopt pre-restart missing dispositions before the outbox can send."""
        records = await self._offload(
            "reconciliation.dispatch_read", self.dispatch_store.list, limit=1000
        )
        for record in records:
            if not record.card_id or record.state not in {
                "running",
                "completion_pending",
            }:
                continue
            if record.completion_payload is None:
                continue
            value = (record.completion_payload or {}).get("card_disposition")
            disposition, _error = parse_card_disposition(value)
            if disposition or record.reconciliation_state != "not_requested":
                continue
            recovered = await self._recover_transcript_completion(
                record,
                prompt_id=(record.completion_payload or {}).get("queued_prompt_id"),
                source=(record.completion_payload or {}).get("prompt_source"),
            )
            if recovered:
                payload = {**(record.completion_payload or {}), **recovered}
                record.completion_payload = payload
                record.card_disposition_error = payload.get("card_disposition_error")
                await self._save(record)
                recovered_disposition, _recovered_error = parse_card_disposition(
                    payload.get("card_disposition")
                )
                if recovered_disposition:
                    await self._handle_completion(str(record.session_id), payload)
                    continue
            record.reconciliation_state = "pending"
            error = record.card_disposition_error or (
                record.completion_payload or {}
            ).get("card_disposition_error")
            record.reconciliation_reason = self._missing_disposition_reason(
                "Recovered a completed card turn without a valid disposition",
                error,
            )
            record.reconciliation_recoverable = True
            record.reconciliation_updated_at = datetime.now(UTC)
            await self._transition(
                record,
                "running",
                "Recovered missing card disposition for bounded reconciliation.",
            )
        self._wake.set()

    async def retry(self, dispatch_id: str) -> DispatchRecord:
        """Idempotently re-arm a legacy or exhausted reconciliation."""
        async with self._lock:
            record = await self._offload(
                "reconciliation.dispatch_read", self.dispatch_store.get, dispatch_id
            )
            if not record:
                raise KeyError(dispatch_id)
            if not record.card_id:
                raise ValueError("dispatch is not linked to a card")
            if record.reconciliation_state in {"resolved", "not_required"}:
                return record
            if record.state in {"completed", "acknowledged"} and record.acknowledged_at:
                card = await self._offload(
                    "reconciliation.card_read",
                    self.card_store.get_card,
                    record.card_id,
                    realm_id=record.realm_id,
                )
                if card and getattr(card, "lane", None) == CardLane.DONE:
                    record.reconciliation_state = "already_satisfied"
                    record.reconciliation_reason = (
                        "The authoritative card is already durably Done; preserved "
                        "the acknowledged completion without prompting the session."
                    )
                    record.reconciliation_condition = "authoritative_card_done"
                    record.reconciliation_recoverable = False
                    record.reconciliation_next_retry_at = None
                    record.reconciliation_updated_at = datetime.now(UTC)
                    await self._transition(
                        record,
                        record.state,
                        "Normalized legacy reconciliation warning from authoritative Done evidence.",
                        detail={"card_lane": "done", "prompted": False},
                    )
                    return record
            record.reconciliation_state = "pending"
            record.reconciliation_reason = "Operator requested durable reconciliation."
            record.reconciliation_condition = "operator_retry"
            record.reconciliation_last_dependency_error = None
            record.reconciliation_recovery_action = (
                "Re-check exact-head completion evidence and replay acknowledgement."
            )
            record.reconciliation_recoverable = True
            record.reconciliation_attempts = 0
            record.reconciliation_next_retry_at = None
            record.reconciliation_updated_at = datetime.now(UTC)
            await self._transition(
                record,
                record.state,
                "Operator re-armed card completion reconciliation.",
                detail={"idempotent": True},
            )
            await self._advance(record)
            self._wake.set()
            canonical = await self._offload(
                "reconciliation.dispatch_read",
                self.dispatch_store.get,
                dispatch_id,
            )
            if not canonical:
                raise KeyError(dispatch_id)
            return canonical

    async def handle_completion(self, session_id: str, payload: dict[str, Any]) -> bool:
        """Route one turn completion to delivery or one reconciliation prompt."""
        async with self._lock:
            return await self._handle_completion(session_id, payload)

    async def _handle_completion(
        self, session_id: str, payload: dict[str, Any]
    ) -> bool:
        record = await self._offload(
            "reconciliation.dispatch_read",
            self.dispatch_store.by_session,
            session_id,
        )
        if (
            record
            and record.card_id
            and record.acknowledged_at
            and record.state in {"completed", "acknowledged"}
        ):
            # Ordinary follow-up turns have their own durable delivery and
            # evaluation path. They never reopen or replay the dispatch envelope.
            return await self._queue_delivery(session_id, payload)
        if record and record.accepts_late_completion_after_terminal_repair:
            # A completion callback already existed when a closed-session repair
            # won its local race. The immutable completion must reopen delivery
            # instead of being discarded because the interim state is cancelled.
            return await self._queue_delivery(session_id, payload)
        if (
            not record
            or not record.card_id
            or record.state
            not in {
                "running",
                "completion_pending",
            }
        ):
            return False

        disposition, parse_error = parse_card_disposition(
            payload.get("card_disposition")
        )
        payload_error = str(payload.get("card_disposition_error") or "")[:1000] or None
        error = payload_error or parse_error
        is_followup = payload.get(
            "prompt_source"
        ) == f"{RECONCILIATION_SOURCE_PREFIX}{record.dispatch_id}" or (
            record.reconciliation_prompt_id
            and payload.get("queued_prompt_id") == record.reconciliation_prompt_id
        )
        if disposition:
            payload["card_disposition"] = disposition.model_dump(mode="json")
            payload.pop("card_disposition_error", None)
            record.card_disposition_error = None
            record.reconciliation_state = "resolved" if is_followup else "not_required"
            record.reconciliation_reason = (
                "The one reconciliation turn returned a valid disposition."
                if is_followup
                else "The ended turn supplied a valid disposition."
            )
            record.reconciliation_recoverable = False
            record.reconciliation_updated_at = datetime.now(UTC)
            await self._save(record)
            return await self._queue_delivery(session_id, payload)

        previous_payload = record.completion_payload or {}
        if (
            record.reconciliation_state in {"pending", "blocked"}
            and not is_followup
            and (
                payload == previous_payload
                or (
                    payload.get("queued_prompt_id")
                    and payload.get("queued_prompt_id")
                    == previous_payload.get("queued_prompt_id")
                )
            )
        ):
            return True
        if record.reconciliation_state in RECONCILIATION_TERMINAL_STATES:
            return False
        if record.reconciliation_state == "prompted":
            if not is_followup:
                return False
            record.completion_payload = payload
            record.card_disposition_error = error
            record.reconciliation_parse_errors.append(error or "missing payload")
            final_text = str(payload.get("final_outcome_text") or "")
            record.reconciliation_final_excerpt = (
                final_text[:500].replace("\x00", "�") or "<empty>"
            )
            if record.reconciliation_prompt_count < 2:
                record.reconciliation_state = "pending"
                record.reconciliation_reason = (
                    "The first reconciliation prompt was malformed; one final "
                    f"same-session JSON-only retry is pending: {error or 'missing payload'}"
                )[:1000]
                record.reconciliation_recoverable = True
                record.reconciliation_updated_at = datetime.now(UTC)
                saved = await self._save(record)
                await self._advance(saved)
                return True
            record.reconciliation_state = "failed"
            record.reconciliation_reason = (
                "Both reconciliation prompts completed without a valid "
                f"pa.card-disposition/v1 payload: {error or 'missing payload'}"
            )[:1000]
            record.reconciliation_recoverable = False
            record.reconciliation_updated_at = datetime.now(UTC)
            await self._save(record)
            return await self._queue_delivery(session_id, payload)

        record.completion_payload = payload
        record.card_disposition_error = error
        record.reconciliation_state = "pending"
        record.reconciliation_reason = self._missing_disposition_reason(
            "The completed card turn omitted a valid pa.card-disposition/v1 payload",
            error,
        )
        record.reconciliation_recoverable = True
        record.reconciliation_updated_at = datetime.now(UTC)
        saved = await self._save(record)
        reservation = saved.terminal_repair_reservation or {}
        if saved.accepts_late_completion_after_terminal_repair or reservation.get(
            "state"
        ) in {"prepared", "committed"}:
            return await self._queue_delivery(session_id, payload)
        await self._advance(saved)
        return True

    async def _queue_delivery(self, session_id: str, payload: dict[str, Any]) -> bool:
        queued = await self._offload(
            "reconciliation.completion_queue",
            self.completion_outbox.queue,
            session_id,
            payload,
        )
        return bool(queued)

    async def _run(self, heartbeat: Callable[[], None] | None = None) -> None:
        while not self._closing:
            if heartbeat:
                heartbeat()
            records = await self._offload(
                "reconciliation.dispatch_read",
                self.dispatch_store.list,
                limit=1000,
            )
            now = datetime.now(UTC)
            for record in records:
                if record.reconciliation_state == "prompted":
                    try:
                        async with self._lock:
                            await self._recover_prompted(record)
                    except Exception:
                        logger.exception(
                            "Prompted card reconciliation recovery failed for dispatch %s",
                            record.dispatch_id,
                        )
                    continue
                if record.reconciliation_state not in {"pending", "blocked"}:
                    continue
                if (
                    record.reconciliation_next_retry_at
                    and record.reconciliation_next_retry_at > now
                ):
                    continue
                try:
                    async with self._lock:
                        await self._advance(record)
                except Exception:
                    logger.exception(
                        "Card completion reconciliation failed for dispatch %s",
                        record.dispatch_id,
                    )
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), self.retry_seconds)
            except TimeoutError:
                pass

    def worker_health(self) -> dict[str, Any]:
        return self._supervisor.health()

    async def _recover_prompted(self, record: DispatchRecord) -> None:
        """Recover a reconciler turn that finished immediately before restart."""
        current = await self._offload(
            "reconciliation.dispatch_read",
            self.dispatch_store.get,
            record.dispatch_id,
        )
        if not current or current.reconciliation_state != "prompted":
            return
        runtime = self.agent.get(current.session_id) if current.session_id else None
        if runtime and self._existing_prompt(runtime, current.dispatch_id):
            return
        prompt_id = current.reconciliation_prompt_id
        payload = await self._recover_transcript_completion(
            current,
            prompt_id=prompt_id,
            source=f"{RECONCILIATION_SOURCE_PREFIX}{current.dispatch_id}",
        )
        if payload:
            await self._handle_completion(str(current.session_id), payload)
            return

        session = await self._offload(
            "reconciliation.session_read",
            self.agent.store.get_session,
            current.session_id,
        )
        if not session or session.status in {
            "closed",
            "configuration_failed",
            "provisioning_failed",
        }:
            await self._skip_and_deliver(
                current,
                "failed",
                "The single reconciliation prompt was interrupted by a terminal session.",
            )
        elif runtime and runtime.connected:
            await self._skip_and_deliver(
                current,
                "failed",
                "The durable reconciliation prompt was no longer queued and no completed turn could be recovered.",
            )

    async def _recover_transcript_completion(
        self,
        record: DispatchRecord,
        *,
        prompt_id: str | None,
        source: str | None,
    ) -> dict[str, Any] | None:
        """Recover one exact completed turn and its final assistant message."""
        transcript_reader = getattr(
            self.agent.store, "list_transcript_events_before", None
        )
        if not transcript_reader or not record.session_id or not prompt_id:
            return None
        events = await self._offload(
            "reconciliation.transcript_read",
            transcript_reader,
            record.session_id,
            limit=5000,
        )
        ordered = sorted(events, key=lambda event: event.seq)
        start_seq = next(
            (
                event.seq
                for event in reversed(ordered)
                if event.event_type == "user_message"
                and event.payload.get("id") == prompt_id
            ),
            None,
        )
        if start_seq is None and source:
            start_seq = next(
                (
                    event.seq
                    for event in reversed(ordered)
                    if event.event_type == "user_message"
                    and event.payload.get("source") == source
                ),
                None,
            )
        if start_seq is None:
            return None
        completed = next(
            (
                event
                for event in ordered
                if event.seq > start_seq
                and event.event_type == "turn_completed"
                and event.payload.get("queued_prompt_id") == prompt_id
            ),
            None,
        )
        if not completed:
            return None
        message_events = [
            event
            for event in ordered
            if start_seq < event.seq < completed.seq
        ]
        final_text = assemble_final_assistant_message(message_events)
        disposition, error = extract_card_disposition(final_text)
        payload = {
            **completed.payload,
            "final_outcome_text": final_text[:8000],
        }
        if source:
            payload["prompt_source"] = source
        if disposition:
            payload["card_disposition"] = disposition
        elif error:
            payload["card_disposition_error"] = error[:1000]
        return payload

    @staticmethod
    def _missing_disposition_reason(prefix: str, error: Any) -> str:
        detail = str(error or "").strip()
        if detail:
            return f"{prefix}: {detail}"[:1000]
        return f"{prefix}: missing payload"[:1000]

    async def _advance(self, record: DispatchRecord) -> None:
        current = await self._offload(
            "reconciliation.dispatch_read",
            self.dispatch_store.get,
            record.dispatch_id,
        )
        if (
            not current
            or current.reconciliation_state not in {"pending", "blocked"}
            or current.state not in {"running", "completion_pending"}
        ):
            return
        record = current
        session = await self._offload(
            "reconciliation.session_read",
            self.agent.store.get_session,
            record.session_id,
        )
        runtime = self.agent.get(record.session_id) if record.session_id else None
        if session and session.status in {
            "configuration_failed",
            "provisioning_failed",
        }:
            await self._skip_and_deliver(
                record,
                "failed",
                f"The linked session is terminally failed ({session.status}).",
            )
            return
        if (
            not session
            or session.status == "closed"
            or (runtime and getattr(runtime, "_closed", False))
        ):
            await self._skip_and_deliver(
                record,
                "skipped_closed",
                "The linked session is closed, so reconciliation was not prompted.",
            )
            return
        if not session.external_session_id:
            await self._skip_and_deliver(
                record,
                "skipped_non_resumable",
                "The linked session has no resumable provider identity.",
            )
            return
        if not runtime or not runtime.connected:
            await self._block(
                record,
                "The resumable linked session is not currently available.",
            )
            return

        existing = self._existing_prompt(
            runtime,
            record.dispatch_id,
            exclude_id=(
                record.reconciliation_prompt_id
                if record.reconciliation_prompt_count == 1
                and record.reconciliation_parse_errors
                else None
            ),
        )
        if existing:
            record.reconciliation_state = "prompted"
            record.reconciliation_reason = (
                "The durable reconciliation prompt is queued or in flight."
            )
            record.reconciliation_prompt_count = max(
                1, record.reconciliation_prompt_count + 1
            )
            record.reconciliation_prompt_id = existing.id
            if existing.id not in record.reconciliation_prompt_ids:
                record.reconciliation_prompt_ids.append(existing.id)
            record.reconciliation_recoverable = False
            record.reconciliation_next_retry_at = None
            record.reconciliation_updated_at = datetime.now(UTC)
            await self._save(record)
            return

        ready, reason = await self._services_ready(record)
        if not ready:
            await self._block(record, reason)
            return

        source = f"{RECONCILIATION_SOURCE_PREFIX}{record.dispatch_id}"
        rendered = PROMPTS.render(
            "card.reconciliation.disposition",
            provider=session.agent_name,
        )
        prompt_text = rendered.text
        if record.reconciliation_prompt_count:
            prompt_text += (
                "\n\nThis is the one final retry. The preceding extraction error was:\n"
                f"{record.card_disposition_error or 'missing payload'}\n"
                "The preceding final response began:\n"
                f"{record.reconciliation_final_excerpt or '<empty>'}\n\n"
                "Return exactly one JSON object, no progress prose, no Markdown."
            )
        try:
            item = runtime.enqueue(
                prompt_text,
                action="prepend",
                card_id=record.card_id,
                project_id=record.project_id,
                source=source,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            await self._block(
                record,
                f"The reconciliation prompt could not be durably queued: {exc}",
            )
            return
        record.reconciliation_state = "prompted"
        record.reconciliation_reason = (
            "Queued the one final same-session JSON-only retry after a malformed "
            f"disposition: {record.card_disposition_error or 'missing payload'}"
            if record.reconciliation_prompt_count
            else "Queued the first same-session disposition reconciliation prompt."
        )[:1000]
        record.reconciliation_recoverable = False
        record.reconciliation_prompt_count += 1
        record.reconciliation_prompt_id = item.id
        record.reconciliation_prompt_ids.append(item.id)
        record.reconciliation_next_retry_at = None
        record.reconciliation_updated_at = datetime.now(UTC)
        await self._transition(
            record,
            "running",
            "Queued one post-turn card-disposition reconciliation prompt.",
            detail={"prompt_id": item.id, "session_id": record.session_id},
        )

    async def _services_ready(self, record: DispatchRecord) -> tuple[bool, str]:
        try:
            card = await self._offload(
                "reconciliation.card_read",
                self.card_store.get_card,
                record.card_id,
                realm_id=record.realm_id,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            return False, f"PA card tooling is temporarily unavailable: {exc}"
        if not card:
            return False, "PA card tooling cannot currently resolve the linked card."
        service = self.supervisor_service()
        if not service:
            return False, "PA PR-supervisor tooling is not ready."
        try:
            health = await self._offload(
                "reconciliation.supervisor_health", service.authority_health
            )
        except (OSError, RuntimeError, ValueError) as exc:
            return False, f"PA PR-supervisor tooling is temporarily unavailable: {exc}"
        state = str((health or {}).get("state") or "unavailable")
        if state != "ready":
            return False, f"PA PR-supervisor tooling is {state}."
        return True, "PA card and PR-supervisor tooling are ready."

    async def _block(self, record: DispatchRecord, reason: str) -> None:
        record.reconciliation_attempts += 1
        record.reconciliation_updated_at = datetime.now(UTC)
        lowered = reason.lower()
        if "card tooling" in lowered:
            condition = "pa_unavailable"
        elif "supervisor" in lowered:
            condition = "authority_unavailable"
        elif "provider" in lowered:
            condition = "missing_provider_thread"
        else:
            condition = "unavailable_service"
        record.reconciliation_condition = condition
        record.reconciliation_last_dependency_error = reason[:1000]
        record.reconciliation_recovery_action = (
            "Retry exact-head completion reconciliation when the dependency recovers."
        )
        if record.reconciliation_attempts >= self.max_attempts:
            record.reconciliation_state = "exhausted"
            record.reconciliation_reason = (
                f"Reconciliation retry budget exhausted: {reason}"
            )[:1000]
            record.reconciliation_recoverable = False
            record.reconciliation_next_retry_at = None
            await self._save(record)
            await self._queue_delivery(
                str(record.session_id), record.completion_payload or {}
            )
            return
        record.reconciliation_state = "blocked"
        record.reconciliation_reason = reason[:1000]
        record.reconciliation_recoverable = True
        delay = min(
            self.retry_max_seconds,
            self.retry_seconds * (2 ** min(record.reconciliation_attempts - 1, 10)),
        )
        delay = max(self.retry_seconds, delay * self.rng.uniform(0.8, 1.2))
        record.reconciliation_next_retry_at = datetime.now(UTC) + timedelta(
            seconds=delay
        )
        await self._transition(
            record,
            "running",
            "Card-disposition reconciliation is recoverably blocked.",
            detail={
                "reason": record.reconciliation_reason,
                "attempt": record.reconciliation_attempts,
                "max_attempts": self.max_attempts,
                "condition": record.reconciliation_condition,
                "next_retry_at": record.reconciliation_next_retry_at.isoformat(),
                "last_dependency_error": record.reconciliation_last_dependency_error,
                "recovery_action": record.reconciliation_recovery_action,
            },
        )

    async def _skip_and_deliver(
        self, record: DispatchRecord, state: str, reason: str
    ) -> None:
        record.reconciliation_state = state
        record.reconciliation_reason = reason
        record.reconciliation_recoverable = False
        record.reconciliation_next_retry_at = None
        record.reconciliation_updated_at = datetime.now(UTC)
        await self._save(record)
        await self._queue_delivery(
            str(record.session_id), record.completion_payload or {}
        )

    @staticmethod
    def _existing_prompt(
        runtime: AgentSessionRuntime,
        dispatch_id: str,
        exclude_id: str | None = None,
    ) -> Any | None:
        source = f"{RECONCILIATION_SOURCE_PREFIX}{dispatch_id}"
        candidates = list(runtime._queue)
        if runtime._in_flight:
            candidates.append(runtime._in_flight)
        return next(
            (
                item
                for item in candidates
                if item.source == source and (not exclude_id or item.id != exclude_id)
            ),
            None,
        )

    @staticmethod
    def _merge_reconciliation_fields(
        current: DispatchRecord, source: DispatchRecord
    ) -> None:
        for field in (
            "card_disposition_error",
            "reconciliation_state",
            "reconciliation_reason",
            "reconciliation_condition",
            "reconciliation_last_dependency_error",
            "reconciliation_recovery_action",
            "reconciliation_recoverable",
            "reconciliation_attempts",
            "reconciliation_prompt_count",
            "reconciliation_prompt_id",
            "reconciliation_prompt_ids",
            "reconciliation_parse_errors",
            "reconciliation_final_excerpt",
            "reconciliation_next_retry_at",
            "reconciliation_updated_at",
            "reconciliation_current_card",
        ):
            setattr(current, field, copy.deepcopy(getattr(source, field)))

    async def _save(self, record: DispatchRecord) -> DispatchRecord:
        def merge(current: DispatchRecord) -> bool:
            self._merge_reconciliation_fields(current, record)
            reservation = current.terminal_repair_reservation or {}
            repair_fenced = bool(
                current.accepts_late_completion_after_terminal_repair
                or reservation.get("state") in {"prepared", "committed"}
            )
            if (
                not repair_fenced
                and current.completion_payload is None
                and record.completion_payload is not None
            ):
                current.completion_payload = copy.deepcopy(record.completion_payload)
            return True

        return await self._offload(
            "reconciliation.dispatch_write",
            self.dispatch_store.mutate_current,
            record.dispatch_id,
            mutate=merge,
        )

    async def _transition(
        self,
        record: DispatchRecord,
        state: str,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
    ) -> None:
        def merge_and_transition(current: DispatchRecord) -> bool:
            self._merge_reconciliation_fields(current, record)
            reservation = current.terminal_repair_reservation or {}
            if reservation.get("state") in {"prepared", "committed"}:
                return True
            current.state = state
            current.events.append(
                DispatchEvent(
                    seq=(current.events[-1].seq + 1 if current.events else 1),
                    state=state,
                    message=message,
                    detail=detail or {},
                )
            )
            return True

        await self._offload(
            "reconciliation.dispatch_write",
            self.dispatch_store.mutate_current,
            record.dispatch_id,
            mutate=merge_and_transition,
        )
