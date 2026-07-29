"""Bounded recovery for card-linked turns that omit a disposition."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from pa.execution.dispatch import CompletionOutbox, DispatchRecord, DispatchStore
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
}


class CompletionReconciler:
    """Prompt one resumable session once, with durable retry and diagnostics."""

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

    async def _offload(self, operation: str, call, *args, **kwargs):
        runtime = getattr(self.agent, "async_runtime", None)
        if runtime:
            return await runtime.run_blocking(operation, call, *args, **kwargs)
        return await asyncio.to_thread(call, *args, **kwargs)

    def start(self) -> None:
        if not self._task or self._task.done():
            self._closing = False
            self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        self._closing = True
        self._wake.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=1.0)
            except TimeoutError:
                self._task.cancel()

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
            record.reconciliation_state = "pending"
            record.reconciliation_reason = (
                "Recovered a completed card turn without a valid disposition."
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
            return record

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

        disposition, error = parse_card_disposition(payload.get("card_disposition"))
        is_followup = payload.get(
            "prompt_source"
        ) == f"{RECONCILIATION_SOURCE_PREFIX}{record.dispatch_id}" or (
            record.reconciliation_prompt_id
            and payload.get("queued_prompt_id") == record.reconciliation_prompt_id
        )
        if disposition:
            payload["card_disposition"] = disposition.model_dump(mode="json")
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
            record.reconciliation_state = "failed"
            record.reconciliation_reason = (
                "The single reconciliation prompt completed without a valid "
                f"pa.card-disposition/v1 payload: {error or 'missing payload'}"
            )[:1000]
            record.reconciliation_recoverable = False
            record.reconciliation_updated_at = datetime.now(UTC)
            await self._save(record)
            return await self._queue_delivery(session_id, payload)

        record.completion_payload = payload
        record.reconciliation_state = "pending"
        record.reconciliation_reason = (
            "The completed card turn omitted a valid pa.card-disposition/v1 payload."
        )
        record.reconciliation_recoverable = True
        record.reconciliation_updated_at = datetime.now(UTC)
        await self._save(record)
        await self._advance(record)
        return True

    async def _queue_delivery(self, session_id: str, payload: dict[str, Any]) -> bool:
        queued = await self._offload(
            "reconciliation.completion_queue",
            self.completion_outbox.queue,
            session_id,
            payload,
        )
        return bool(queued)

    async def _run(self) -> None:
        while not self._closing:
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
        events = await self._offload(
            "reconciliation.transcript_read",
            self.agent.store.list_transcript_events_before,
            current.session_id,
            limit=5000,
        )
        prompt_id = current.reconciliation_prompt_id
        start_seq = next(
            (
                event.seq
                for event in reversed(events)
                if event.event_type == "user_message"
                and (
                    event.payload.get("id") == prompt_id
                    or event.payload.get("source")
                    == f"{RECONCILIATION_SOURCE_PREFIX}{current.dispatch_id}"
                )
            ),
            None,
        )
        completed = next(
            (
                event
                for event in events
                if start_seq is not None
                and event.seq > start_seq
                and event.event_type == "turn_completed"
                and event.payload.get("queued_prompt_id") == prompt_id
            ),
            None,
        )
        if completed:
            chunks = [
                event.payload
                for event in events
                if start_seq < event.seq < completed.seq
                and event.event_type == "agent_message_chunk"
            ]
            final_text = "".join(
                str(chunk.get("text") or "")
                for chunk in chunks
                if chunk.get("phase") == "final"
            ).strip()
            if not final_text:
                final_text = "".join(
                    str(chunk.get("text") or "") for chunk in chunks
                ).strip()
            disposition, error = extract_card_disposition(final_text)
            payload = {
                **completed.payload,
                "prompt_source": (
                    f"{RECONCILIATION_SOURCE_PREFIX}{current.dispatch_id}"
                ),
            }
            if disposition:
                payload["card_disposition"] = disposition
            elif error:
                payload["card_disposition_error"] = error[:1000]
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

        existing = self._existing_prompt(runtime, record.dispatch_id)
        if existing:
            record.reconciliation_state = "prompted"
            record.reconciliation_reason = (
                "The durable reconciliation prompt is queued or in flight."
            )
            record.reconciliation_prompt_count = 1
            record.reconciliation_prompt_id = existing.id
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
        try:
            item = runtime.enqueue(
                rendered.text,
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
            "Queued exactly one same-session disposition reconciliation prompt."
        )
        record.reconciliation_recoverable = False
        record.reconciliation_prompt_count = 1
        record.reconciliation_prompt_id = item.id
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
    def _existing_prompt(runtime: AgentSessionRuntime, dispatch_id: str) -> Any | None:
        source = f"{RECONCILIATION_SOURCE_PREFIX}{dispatch_id}"
        candidates = list(runtime._queue)
        if runtime._in_flight:
            candidates.append(runtime._in_flight)
        return next((item for item in candidates if item.source == source), None)

    async def _save(self, record: DispatchRecord) -> None:
        await self._offload(
            "reconciliation.dispatch_write", self.dispatch_store.put, record
        )

    async def _transition(
        self,
        record: DispatchRecord,
        state: str,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
    ) -> None:
        await self._offload(
            "reconciliation.dispatch_write",
            self.dispatch_store.transition,
            record,
            state,
            message,
            detail=detail,
        )
