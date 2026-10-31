"""Automatic durable agent-session closure and retention policy."""

from __future__ import annotations

import asyncio
import functools
import heapq
import logging
import threading
from collections import Counter, OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

from pa.domain.models import AgentSession, CardLane
from pa.execution.dispatch import TERMINAL_DISPATCH_STATES
from pa.execution.reconciliation import RECONCILIATION_TERMINAL_STATES
from pa.pr_supervisor.models import PRWatchStatus

logger = logging.getLogger(__name__)

_SINGLE_PURPOSE_LABELS = frozenset(
    {"advisor", "coordinator", "device-login", "login", "operational", "pr-executor"}
)
_SINGLE_PURPOSE_LABEL_PREFIXES = ("card-enrichment:",)
_ACTIVE_WATCH_STATUSES = frozenset({PRWatchStatus.ACTIVE, PRWatchStatus.BLOCKED})


class _WatchLinkIndex:
    """Index one sweep's immutable watch links, preserving read order and duplicates."""

    def __init__(self, watches: list[Any]) -> None:
        self._sessions: dict[str | None, list[tuple[int, Any]]] = defaultdict(list)
        self._cards: dict[str | None, list[tuple[int, Any]]] = defaultdict(list)
        for position, watch in enumerate(watches):
            row = (position, watch)
            self._sessions[watch.originating_session_id].append(row)
            self._cards[watch.card_id].append(row)

    @classmethod
    def read(cls, store: Any) -> _WatchLinkIndex:
        return cls(store.lifecycle_watch_links())

    def for_session(self, session: AgentSession) -> list[Any]:
        rows = heapq.merge(
            self._sessions.get(session.id, ()),
            self._cards.get(session.card_id, ()) if session.card_id else (),
            key=lambda row: row[0],
        )
        result = []
        previous = -1
        for position, watch in rows:
            # A row linked by both keys occurs once, as in the original OR.
            # Distinct rows remain even if their three field values are equal.
            if position != previous:
                result.append(watch)
            previous = position
        return result


class _DispatchLinkIndex:
    """Keep one sweep's copied dispatch rows in their original OR-link order."""

    def __init__(self, dispatches: list[Any]) -> None:
        self._sessions: dict[str | None, list[tuple[int, Any]]] = defaultdict(list)
        self._dispatches: dict[str | None, list[tuple[int, Any]]] = defaultdict(list)
        for position, dispatch in enumerate(dispatches):
            row = (position, dispatch)
            self._sessions[dispatch.session_id].append(row)
            self._dispatches[dispatch.dispatch_id].append(row)

    @classmethod
    def read(cls, store: Any) -> _DispatchLinkIndex:
        return cls(store.list(limit=10000, deep=False))

    def for_session(self, session: AgentSession) -> list[Any]:
        rows = heapq.merge(
            self._sessions.get(session.id, ()),
            self._dispatches.get(session.dispatch_id, ()) if session.dispatch_id else (),
            key=lambda row: row[0],
        )
        result = []
        previous = -1
        for position, dispatch in rows:
            if position != previous:
                result.append(dispatch)
            previous = position
        return result


class _LeaseLinkIndex:
    """Group one sweep's fresh lease models without filtering states or order."""

    def __init__(self, leases: list[Any]) -> None:
        self._sessions: dict[str | None, list[Any]] = defaultdict(list)
        for lease in leases:
            self._sessions[lease.session_id].append(lease)

    @classmethod
    def read(cls, manager: Any) -> _LeaseLinkIndex:
        return cls(manager.list())

    def for_session(self, session: AgentSession) -> list[Any]:
        return self._sessions.get(session.id, [])


class SessionLifecyclePolicy:
    """Prove sessions safe to close, then use existing durable close semantics."""

    def __init__(self, manager: Any, services: dict[str, Any]) -> None:
        self.manager = manager
        self.services = services
        self.metrics: Counter[str] = Counter()
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._closing = False
        self._lock = asyncio.Lock()
        self._close_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pa-session-lifecycle"
        )
        self._attempt = 0
        self.diagnostics: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def start(self) -> None:
        if not self._task or self._task.done():
            self._closing = False
            self._task = asyncio.create_task(self._run())
            self._wake.set()

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
        self._close_executor.shutdown(wait=False, cancel_futures=True)

    async def _run(self) -> None:
        while not self._closing:
            try:
                await self.run_once()
            except Exception:
                logger.exception("Agent session lifecycle sweep failed")
            self._wake.clear()
            try:
                await asyncio.wait_for(
                    self._wake.wait(),
                    timeout=self.manager.settings.agent_session_sweep_seconds,
                )
            except TimeoutError:
                pass

    async def run_once(self, *, now: datetime | None = None, card_id: str | None = None) -> dict[str, int]:
        now = now or datetime.now(UTC)
        if self._lock.locked():
            self.metrics["coalesced:sweep_active"] += 1
            return dict(self.metrics)
        async with self._lock:
            sessions = await self._offload(
                "session_lifecycle.sessions",
                self.manager.store.list_sessions,
                exclude_statuses=("closed",),
            )
            dispatch_store = self.services.get("dispatch_store")
            dispatches = (
                await self._offload(
                    "session_lifecycle.dispatches",
                    _DispatchLinkIndex.read,
                    dispatch_store,
                )
                if dispatch_store
                else _DispatchLinkIndex([])
            )
            supervisor = self.services.get("pr_supervisor")
            watches = (
                await self._offload(
                    "session_lifecycle.watches",
                    _WatchLinkIndex.read,
                    supervisor.store,
                )
                if supervisor and getattr(supervisor, "store", None)
                else _WatchLinkIndex([])
            )
            leases = await self._offload(
                "session_lifecycle.leases", _LeaseLinkIndex.read,
                self.manager.workspace_manager,
            )
            closed_ids: list[str] = []
            for session in sessions:
                if card_id and session.card_id != card_id:
                    continue
                decision, reason = await self._decision(
                    session,
                    sessions=sessions,
                    dispatches=dispatches,
                    watches=watches,
                    leases=leases,
                    now=now,
                )
                self.metrics[f"{decision}:{reason}"] += 1
                if decision == "release":
                    try:
                        released = await self.manager.release_session_process(
                            session.id, reason=reason
                        )
                    except Exception:
                        self.metrics["deferred:provider_release_failed"] += 1
                        logger.exception(
                            "Idle provider release failed for session %s", session.id
                        )
                    else:
                        if released:
                            self.metrics["provider_released"] += 1
                            self.metrics[f"provider_released:{reason}"] += 1
                    continue
                if decision == "retire":
                    card_id = session.card_id
                    if card_id:
                        retired = await self._offload(
                            "session_lifecycle.retire_card",
                            self.manager.store.unlink_session_card,
                            session.id,
                            card_id,
                            reason=reason,
                            principal_id="system:session_lifecycle",
                        )
                        runtime = self.manager.get(session.id)
                        if runtime:
                            runtime.session.card_id = retired.card_id
                            runtime.session.item_id = retired.item_id
                            runtime.session.project_id = retired.project_id
                        self.metrics["association_retired"] += 1
                    continue
                if decision != "close":
                    continue
                if session.purpose in {"automated_run", "one_shot_job"}:
                    if reason.startswith("workflow_"):
                        session.workflow_state = reason.removeprefix("workflow_")
                    elif reason in {"dispatch_completed", "single_purpose_finished"}:
                        session.workflow_state = "succeeded"
                    elif "cancel" in reason:
                        session.workflow_state = "cancelled"
                    else:
                        session.workflow_state = "failed"
                    session.workflow_outcome = {
                        **dict(session.workflow_outcome or {}),
                        "reason": reason,
                        "completed_at": now.isoformat(),
                    }
                    await self._offload(
                        "session_lifecycle.workflow_outcome",
                        self.manager.store.save_session,
                        session,
                    )
                    runtime_for_outcome = self.manager.get(session.id)
                    if runtime_for_outcome:
                        runtime_for_outcome.session = session
                self._attempt += 1
                attempt = self._attempt
                session_leases = [
                    lease for lease in leases.for_session(session)
                    if lease.session_id == session.id
                ]
                detail: dict[str, Any] = {
                    "session_id": session.id,
                    "close_reason": reason,
                    "queue_state": "empty",
                    "lease_state": (
                        ",".join(sorted({lease.state for lease in session_leases}))
                        or "none"
                    ),
                    "worker_owner": None,
                    "attempt_fence": attempt,
                    "terminal_result": "submitted",
                }
                self.diagnostics[session.id] = detail
                self.diagnostics.move_to_end(session.id)
                while len(self.diagnostics) > 100:
                    self.diagnostics.popitem(last=False)

                def close_durable():
                    detail["worker_owner"] = threading.current_thread().name
                    return self.manager.store.close_session(
                        session.id,
                        reason=f"auto:{reason}",
                        closed_at=now,
                        lock_timeout_seconds=1.0,
                        busy_timeout_ms=1000,
                        diagnostics=detail,
                    )

                try:
                    loop = asyncio.get_running_loop()
                    closed, prior = await loop.run_in_executor(
                        self._close_executor, close_durable
                    )
                except Exception as exc:
                    self.metrics["deferred:close_contention"] += 1
                    logger.warning(
                        "Agent session lifecycle close deferred",
                        extra={
                            **detail,
                            "error_type": type(exc).__name__,
                        },
                    )
                    continue
                changed = closed is not None and prior is not None
                runtime = self.manager.get(session.id)
                if changed and runtime:
                    runtime._closed = True
                    runtime.session.status = "closed"
                    runtime.session.updated_at = now
                    connection = runtime.connection
                    runtime.connection = None
                    if connection:
                        try:
                            await asyncio.wait_for(connection.disconnect(), timeout=2.0)
                        except Exception:
                            logger.warning(
                                "Provider disconnect after durable auto-close failed",
                                extra={"session_id": session.id, "close_reason": reason},
                                exc_info=True,
                            )
                    self.manager._runtimes.pop(session.id, None)
                if changed:
                    closed_ids.append(session.id)
                    self.metrics["auto_closed"] += 1
                    self.metrics[f"auto_closed:{reason}"] += 1
            if closed_ids:
                await self.manager.reconcile_closed_sessions(closed_ids)
            return dict(self.metrics)

    def snapshot(self) -> dict[str, Any]:
        """Return redacted control-plane diagnostics; transcript data is excluded."""
        return {
            "metrics": dict(self.metrics),
            "active": self._lock.locked(),
            "attempts": list(self.diagnostics.values()),
        }

    async def _offload(self, _operation: str, call, *args, **kwargs):
        """Keep lifecycle work off the shared control-plane blocking pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._close_executor, functools.partial(call, *args, **kwargs)
        )

    async def _decision(
        self,
        session: AgentSession,
        *,
        sessions: list[AgentSession],
        dispatches: list[Any] | _DispatchLinkIndex,
        watches: list[Any] | _WatchLinkIndex,
        leases: list[Any] | _LeaseLinkIndex,
        now: datetime,
    ) -> tuple[str, str]:
        if session.status == "closed":
            return "skipped", "already_closed"
        pending = getattr(self.manager.store, "pending_restart_continuation", None)
        if callable(pending) and await self._offload(
            "session_lifecycle.restart_pending", pending, session_id=session.id,
        ):
            return "retained", "restart_continuation_pending"
        runtime = self.manager.get(session.id)
        if runtime:
            if runtime.prompting or runtime._queue:
                return "retained", "prompt_pending"
            if runtime._pending_permissions:
                return "retained", "permission_pending"
            if getattr(runtime, "_pending_elicitations", None):
                return "retained", "elicitation_pending"
            if runtime._transcript_buffer or not runtime._transcript_queue.empty():
                return "deferred", "transcript_delivery_pending"
        durable = dict((session.config_json or {}).get("durable_runtime") or {})
        if durable.get("in_flight") or durable.get("queued_prompts"):
            return "retained", "durable_prompt_pending"
        if durable.get("pending_permissions") or durable.get("pending_elicitations"):
            return "retained", "durable_interaction_pending"
        if durable.get("pending_interaction"):
            return "retained", "requested_input_pending"

        dispatch_candidates = (
            dispatches.for_session(session)
            if isinstance(dispatches, _DispatchLinkIndex) else dispatches
        )
        linked_dispatches = [
            item
            for item in dispatch_candidates
            if item.session_id == session.id
            or (session.dispatch_id and item.dispatch_id == session.dispatch_id)
        ]
        for dispatch in linked_dispatches:
            if dispatch.state not in TERMINAL_DISPATCH_STATES:
                return "retained", "dispatch_active"
            from pa.execution.followup import dispatch_followup_pending
            if dispatch_followup_pending(dispatch):
                return "retained", "followup_pending"
            if dispatch.state == "completed" and (
                not dispatch.acknowledged_at
                or dispatch.completion_delivery_class != "acknowledged"
            ):
                return "deferred", "completion_delivery_pending"
            if dispatch.reconciliation_state not in RECONCILIATION_TERMINAL_STATES:
                return "retained", "reconciliation_active"
            if dispatch.state in {"failed", "cancelled"} and dispatch.recoverable:
                return "retained", "dispatch_recoverable"

        watch_candidates = (
            watches.for_session(session) if isinstance(watches, _WatchLinkIndex) else watches
        )
        linked_watches = [
            watch
            for watch in watch_candidates
            if watch.originating_session_id == session.id
            or (session.card_id and watch.card_id == session.card_id)
        ]
        if any(watch.status in _ACTIVE_WATCH_STATUSES for watch in linked_watches):
            return "retained", "actionable_pr_watch"

        lease_candidates = (
            leases.for_session(session) if isinstance(leases, _LeaseLinkIndex) else leases
        )
        session_leases = [
            lease
            for lease in lease_candidates
            if lease.session_id == session.id and lease.state != "cleaned"
        ]
        for lease in session_leases:
            try:
                dirty, untracked = await self._offload(
                    "session_lifecycle.workspace_status",
                    self.manager.workspace_manager._status,
                    lease.worktree_path,
                )
            except Exception:
                return "retained", "workspace_status_unknown"
            if dirty or untracked:
                return "retained", "workspace_uncommitted"
            if lease.state in {"provisioning", "cleanup_blocked"}:
                return "retained", "workspace_obligation"

        retention = timedelta(
            hours=self.manager.settings.agent_session_idle_retention_hours
        )
        # Conversations are durable product objects.  Inactivity may release an
        # eligible provider process, but card lifecycle and newer conversations
        # never archive, close, or unlink them.
        if session.purpose == "chat":
            if session.archived_at is not None:
                return "retained", "conversation_archived"
            last_human_activity = session.human_activity_at or session.updated_at
            if runtime and now - last_human_activity >= retention:
                return "release", "idle_process_retention_expired"
            return "retained", "conversation_available"

        # Runs remain active across any number of provider turns.  Only their
        # owning workflow outcome, after the obligation guards above have
        # settled, may finish them.
        if session.purpose == "automated_run":
            if session.workflow_state in {
                "succeeded",
                "failed",
                "cancelled",
                "validation_failed",
            }:
                return "close", f"workflow_{session.workflow_state}"
            if linked_dispatches and all(
                item.state == "completed" for item in linked_dispatches
            ):
                return "close", "dispatch_completed"
            if linked_dispatches and all(
                item.state in {"failed", "cancelled"} and not item.recoverable
                for item in linked_dispatches
            ):
                state = linked_dispatches[-1].state
                return "close", f"dispatch_{state}"
            return "retained", "workflow_active"

        if session.purpose == "one_shot_job":
            if session.workflow_state in {
                "succeeded",
                "failed",
                "cancelled",
                "validation_failed",
            }:
                return "close", f"workflow_{session.workflow_state}"
            if session.status == "idle" and (session.metrics_json or {}).get("turns"):
                return "close", "single_purpose_finished"
            return "retained", "job_active"

        newer = [
            candidate
            for candidate in sessions
            if candidate.id != session.id
            and candidate.status != "closed"
            and (
                (session.dispatch_id and candidate.dispatch_id == session.dispatch_id)
                or (
                    session.card_id
                    and candidate.card_id == session.card_id
                    and candidate.label == session.label
                )
            )
            and (candidate.updated_at, candidate.id) > (session.updated_at, session.id)
        ]
        if newer:
            return "close", "superseded_duplicate"
        if session.lifecycle_owner == "dispatch" and linked_dispatches and all(
            item.state == "completed" for item in linked_dispatches
        ):
            return "close", "dispatch_completed"
        if session.lifecycle_owner == "dispatch" and linked_dispatches and all(
            item.state in {"failed", "cancelled"} and not item.recoverable
            for item in linked_dispatches
        ):
            return "close", "dispatch_terminal"
        if session.lifecycle_owner == "dispatch" and linked_watches and all(
            watch.status not in _ACTIVE_WATCH_STATUSES for watch in linked_watches
        ):
            return "close", "pr_watch_terminal"
        card = (
            await self._offload(
                "session_lifecycle.card",
                self.manager.store.get_card,
                session.card_id,
                realm_id=session.realm_id,
            )
            if session.card_id
            else None
        )
        if session.card_id and card is None:
            return "close", "card_deleted"
        card_completed = bool(card and card.lane == CardLane.DONE)
        if card_completed and session.lifecycle_owner == "standalone":
            return "retire", "associated_card_terminal"
        single_purpose = session.label in _SINGLE_PURPOSE_LABELS or str(
            session.label or ""
        ).startswith(_SINGLE_PURPOSE_LABEL_PREFIXES)
        if single_purpose and session.status == "idle":
            return "close", "single_purpose_finished"
        if str(session.label or "").startswith(_SINGLE_PURPOSE_LABEL_PREFIXES):
            return "close", "single_purpose_terminal"
        if session.status == "idle" and now - session.updated_at >= retention:
            return "close", "idle_retention_expired"
        if card_completed:
            return "retained", "card_completed_followup_window"
        return "retained", "followup_window"
