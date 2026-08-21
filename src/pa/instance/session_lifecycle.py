"""Automatic durable agent-session closure and retention policy."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
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

    async def run_once(self, *, now: datetime | None = None) -> dict[str, int]:
        now = now or datetime.now(UTC)
        async with self._lock:
            sessions = await self.manager._offload(
                "session_lifecycle.sessions",
                self.manager.store.list_sessions,
                exclude_statuses=("closed",),
            )
            dispatch_store = self.services.get("dispatch_store")
            dispatches = (
                await self.manager._offload(
                    "session_lifecycle.dispatches",
                    dispatch_store.list,
                    limit=10000,
                    deep=False,
                )
                if dispatch_store
                else []
            )
            supervisor = self.services.get("pr_supervisor")
            watches = (
                await self.manager._offload(
                    "session_lifecycle.watches",
                    supervisor.store.list_watches,
                    include_retired=True,
                )
                if supervisor and getattr(supervisor, "store", None)
                else []
            )
            leases = await self.manager._offload(
                "session_lifecycle.leases", self.manager.workspace_manager.list
            )
            closed_ids: list[str] = []
            for session in sessions:
                decision, reason = await self._decision(
                    session,
                    sessions=sessions,
                    dispatches=dispatches,
                    watches=watches,
                    leases=leases,
                    now=now,
                )
                self.metrics[f"{decision}:{reason}"] += 1
                if decision == "retire":
                    card_id = session.card_id
                    if card_id:
                        retired = await self.manager._offload(
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
                runtime = self.manager.get(session.id)
                if runtime and not getattr(runtime, "_closed", False):
                    changed = await runtime.close(
                        reason=f"auto:{reason}", reconcile_workspace=False
                    )
                    self.manager._runtimes.pop(session.id, None)
                else:
                    closed, prior = await self.manager._offload(
                        "session_lifecycle.close",
                        self.manager.store.close_session,
                        session.id,
                        reason=f"auto:{reason}",
                        closed_at=now,
                    )
                    changed = closed is not None and prior is not None
                if changed:
                    closed_ids.append(session.id)
                    self.metrics["auto_closed"] += 1
                    self.metrics[f"auto_closed:{reason}"] += 1
            if closed_ids:
                await self.manager.reconcile_closed_sessions(closed_ids)
            return dict(self.metrics)

    async def _decision(
        self,
        session: AgentSession,
        *,
        sessions: list[AgentSession],
        dispatches: list[Any],
        watches: list[Any],
        leases: list[Any],
        now: datetime,
    ) -> tuple[str, str]:
        if session.status == "closed":
            return "skipped", "already_closed"
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

        linked_dispatches = [
            item
            for item in dispatches
            if item.session_id == session.id
            or (session.dispatch_id and item.dispatch_id == session.dispatch_id)
        ]
        for dispatch in linked_dispatches:
            if dispatch.state not in TERMINAL_DISPATCH_STATES:
                return "retained", "dispatch_active"
            if dispatch.state == "completed" and (
                not dispatch.acknowledged_at
                or dispatch.completion_delivery_class != "acknowledged"
            ):
                return "deferred", "completion_delivery_pending"
            if dispatch.reconciliation_state not in RECONCILIATION_TERMINAL_STATES:
                return "retained", "reconciliation_active"
            if dispatch.state in {"failed", "cancelled"} and dispatch.recoverable:
                return "retained", "dispatch_recoverable"

        linked_watches = [
            watch
            for watch in watches
            if watch.originating_session_id == session.id
            or (session.card_id and watch.card_id == session.card_id)
        ]
        if any(watch.status in _ACTIVE_WATCH_STATUSES for watch in linked_watches):
            return "retained", "actionable_pr_watch"

        session_leases = [
            lease
            for lease in leases
            if lease.session_id == session.id and lease.state != "cleaned"
        ]
        for lease in session_leases:
            try:
                dirty, untracked = await self.manager._offload(
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
            await self.manager._offload(
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
        retention = timedelta(
            hours=self.manager.settings.agent_session_idle_retention_hours
        )
        if session.status == "idle" and now - session.updated_at >= retention:
            return "close", "idle_retention_expired"
        if card_completed:
            return "retained", "card_completed_followup_window"
        return "retained", "followup_window"
