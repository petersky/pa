"""Authority-side liveness for remote dispatches that are ``running``.

A remote dispatch is expired only before it starts. Once running, its target is
trusted to push progress and a completion; if the target loses the dispatch,
crashes, or can never deliver, nothing would ever move the authority's record.
The sweeper asks the target what it knows (idempotent GET) when a running remote
dispatch has been silent for ``stale_seconds`` and then:

* adopts the target's completion if it has one;
* confirms the dispatch is still running (which counts as activity);
* fails the dispatch recoverably, notifies, and optionally re-offers it, if the
  target no longer knows it, reports it dead, or has been unreachable for
  ``lost_seconds``.

A dispatch whose target negotiated the lease protocol carries a lease
(``lease_expires_at``), renewed by every progress event, heartbeat and confirmed
probe. For it the lease, not an inferred "last progress time", is the liveness
signal: nothing is probed while the lease is fresh, and once it has expired the
same probe runs. A target that confirms the dispatch is still running renews the
lease (a quiet but healthy long turn is never failed); a target that cannot
confirm it is failed recoverably once the expiry is ``lease_grace_seconds`` old.
Dispatches without a lease (older targets) keep the time-based behaviour above.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pa.core.background import BackgroundTaskSupervisor
from pa.execution.dispatch import DispatchRecord, DispatchStore

logger = logging.getLogger(__name__)

DEAD_SESSION_STATUSES = {
    "closed", "recovery_blocked", "provisioning_failed", "configuration_failed",
}
DEAD_DISPATCH_STATES = {"failed", "cancelled"}


@dataclass
class TargetStatus:
    """What a target said about one dispatch (or why it could not say)."""

    reachable: bool
    # True/False when the target answered authoritatively; None when it could not
    # (an older peer without the status endpoint, or a malformed answer).
    known: bool | None = None
    state: str | None = None
    session_status: str | None = None
    completion: dict[str, Any] | None = None
    error: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


def last_activity(record: DispatchRecord) -> datetime:
    """The most recent sign of life the authority has recorded for ``record``."""
    stamps: list[datetime] = [record.created_at]
    if record.events:
        stamps.append(record.events[-1].created_at)
    if record.prompt_acknowledged_at:
        stamps.append(record.prompt_acknowledged_at)
    latest = record.latest_progress
    if latest is not None:
        stamps.extend([latest.occurred_at, latest.last_activity_at])
    if record.progress_heartbeat is not None:
        stamps.append(record.progress_heartbeat.occurred_at)
    return max(stamp if stamp.tzinfo else stamp.replace(tzinfo=UTC) for stamp in stamps)


def lease_expired_for(record: DispatchRecord, now: datetime) -> float | None:
    """Seconds past the lease expiry (negative while fresh); ``None`` without a lease."""
    if not record.lease_protocol or record.lease_expires_at is None:
        return None
    expires = record.lease_expires_at
    expires = expires if expires.tzinfo else expires.replace(tzinfo=UTC)
    return (now - expires).total_seconds()


class RunningLivenessSweeper:
    def __init__(
        self,
        store: DispatchStore,
        *,
        instance_id: str,
        probe: Callable[[DispatchRecord], Awaitable[TargetStatus]],
        adopt: Callable[[DispatchRecord, dict[str, Any]], Awaitable[Any]],
        fail_lost: Callable[[DispatchRecord, str, str], Awaitable[DispatchRecord]],
        reoffer: Callable[[DispatchRecord], Awaitable[Any]] | None = None,
        notify: Callable[[DispatchRecord], Any] | None = None,
        stale_seconds: float | Callable[[], float] = 1800.0,
        lost_seconds: float | Callable[[], float] = 21600.0,
        reoffer_enabled: bool | Callable[[], bool] = True,
        lease_grace_seconds: float | Callable[[], float] = 300.0,
        interval_seconds: float = 60.0,
        offload: Callable[..., Awaitable[Any]] | None = None,
    ) -> None:
        self.store = store
        self.instance_id = instance_id
        self.probe, self.adopt, self.fail_lost = probe, adopt, fail_lost
        self.reoffer, self.notify = reoffer, notify
        self._stale, self._lost_seconds, self._reoffer_enabled = (
            stale_seconds, lost_seconds, reoffer_enabled,
        )
        self._lease_grace = lease_grace_seconds
        self.interval_seconds = interval_seconds
        self._offload_call = offload
        self._closing = False
        self._supervisor = BackgroundTaskSupervisor(
            "dispatch-liveness-sweeper", self._run,
            self.store.db_path.parent / "dispatch_liveness_worker.json",
        )

    @staticmethod
    def _value(source: Any) -> Any:
        return source() if callable(source) else source

    async def _offload(self, operation: str, call, *args, **kwargs):
        if self._offload_call is not None:
            return await self._offload_call(operation, call, *args, **kwargs)
        return await asyncio.to_thread(call, *args, **kwargs)

    def start(self) -> None:
        self._closing = False
        self._supervisor.start()

    async def close(self) -> None:
        self._closing = True
        await self._supervisor.close()

    def worker_health(self) -> dict[str, Any]:
        return self._supervisor.health()

    async def _run(self, heartbeat: Callable[[], None] | None = None) -> None:
        while not self._closing:
            if heartbeat:
                heartbeat()
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Running-dispatch liveness sweep failed")
            await asyncio.sleep(self.interval_seconds)

    async def sweep_once(self, now: datetime | None = None) -> list[tuple[str, str]]:
        """One pass. Returns ``(dispatch_id, outcome)`` for every record acted on."""
        stale = float(self._value(self._stale))
        if stale <= 0:
            return []
        lost_after = max(stale, float(self._value(self._lost_seconds)))
        now = now or datetime.now(UTC)
        records = await self._offload(
            "dispatch.liveness_read", self.store.running_remote_records, self.instance_id
        )
        outcomes: list[tuple[str, str]] = []
        lease_grace = max(0.0, float(self._value(self._lease_grace)))
        for record in records:
            silent = (now - last_activity(record)).total_seconds()
            expired_for = lease_expired_for(record, now)
            if expired_for is not None:
                # The lease is the liveness signal for this dispatch.
                if expired_for < 0:
                    continue
            elif silent < stale:
                continue
            try:
                outcome = await self._examine(
                    record, silent, lost_after, expired_for, lease_grace
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Liveness check failed for dispatch %s", record.dispatch_id)
                continue
            if outcome:
                outcomes.append((record.dispatch_id, outcome))
        return outcomes

    async def _examine(
        self,
        record: DispatchRecord,
        silent: float,
        lost_after: float,
        expired_for: float | None = None,
        lease_grace: float = 0.0,
    ) -> str | None:
        status = await self.probe(record)
        if status.completion is not None:
            await self.adopt(record, status.completion)
            return "adopted_completion"
        if status.reachable and status.known is False:
            return await self._lost(record, "The target no longer has this dispatch.", "remote_dispatch_unknown")
        if status.reachable and (
            status.state in DEAD_DISPATCH_STATES
            or status.session_status in DEAD_SESSION_STATUSES
        ):
            return await self._lost(
                record,
                f"The target reports the dispatch is {status.state or 'not running'}"
                f" (session {status.session_status or 'unknown'}) without a completion.",
                "remote_dispatch_dead",
            )
        if status.reachable and status.known:
            await self._offload(
                "dispatch.liveness_confirm", self.store.note_target_confirmed,
                record.dispatch_id, "Target confirmed the dispatch is still running.",
            )
            return "confirmed_running"
        if expired_for is not None:
            if expired_for >= lease_grace:
                why = status.error or "no usable answer"
                return await self._lost(
                    record,
                    f"The target's lease expired {int(expired_for // 60)} minutes ago "
                    f"without renewal and it could not confirm the dispatch ({why}).",
                    "remote_dispatch_lease_expired",
                )
            return None
        if silent >= lost_after:
            why = status.error or "no usable answer"
            return await self._lost(
                record,
                f"No progress for {int(silent // 60)} minutes and the target could not "
                f"confirm the dispatch ({why}).",
                "remote_dispatch_unreachable",
            )
        return None

    async def _lost(self, record: DispatchRecord, reason: str, code: str) -> str:
        failed = await self.fail_lost(record, reason, code)
        if failed.state != "failed":
            return "settled_elsewhere"
        if self.notify:
            try:
                await self._offload("dispatch.liveness_notify", self.notify, failed)
            except Exception:
                logger.exception("Lost-dispatch notification failed for %s", record.dispatch_id)
        if self.reoffer is not None and bool(self._value(self._reoffer_enabled)):
            try:
                await self.reoffer(failed)
            except Exception:
                logger.exception("Re-offering lost dispatch %s failed", record.dispatch_id)
                return "failed_reoffer_error"
            return "failed_and_reoffered"
        return "failed"
