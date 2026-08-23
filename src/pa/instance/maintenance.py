"""Periodic local cruft cleanup for transcripts, mutation receipts, and dispatch evidence."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from pa.execution.dispatch import DispatchStoreReadOnlyError

logger = logging.getLogger(__name__)

_MAINTENANCE_TIMEOUT_SECONDS = 120.0


def run_maintenance(
    settings: Any,
    store: Any,
    dispatch_store: Any | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Apply retention to local projection/dispatch state without deleting cards."""
    now = now or datetime.now(UTC)
    transcript_cutoff = now - timedelta(days=int(settings.transcript_retention_days))
    mutation_cutoff = now - timedelta(
        days=int(settings.mutation_operation_retention_days)
    )
    result: dict[str, Any] = {
        "ran_at": now.isoformat(),
        "transcript_cutoff": transcript_cutoff.isoformat(),
        "mutation_cutoff": mutation_cutoff.isoformat(),
        "transcript_events_deleted": store.prune_closed_session_transcripts(
            before=transcript_cutoff
        ),
        "mutation_operations_deleted": store.prune_mutation_operations(
            before=mutation_cutoff
        ),
        "dispatch_compact": {"events": 0, "receipts": 0},
        "sqlite": {},
        "transcript_storage": {},
    }
    if hasattr(store, "migrate_legacy_transcripts"):
        result["transcript_migration"] = store.migrate_legacy_transcripts()
    if dispatch_store is not None:
        try:
            result["dispatch_compact"] = dispatch_store.compact(now=now)
        except DispatchStoreReadOnlyError:
            result["dispatch_compact"] = {"skipped": "read_only"}
    result["sqlite"] = store.optimize()
    if hasattr(store, "transcript_storage_metrics"):
        result["transcript_storage"] = store.transcript_storage_metrics()
    return result


class InstanceMaintenanceService:
    """Sweep closed-session transcripts and compactable dispatch evidence."""

    def __init__(
        self,
        settings: Any,
        store: Any,
        services: dict[str, Any],
        *,
        async_runtime: Any | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.services = services
        self.async_runtime = async_runtime
        self.last_started_at: datetime | None = None
        self.last_finished_at: datetime | None = None
        self.last_error: str | None = None
        self.last_result: dict[str, Any] | None = None
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._closing = False
        self._lock = asyncio.Lock()

    def start(self) -> None:
        if not self._task or self._task.done():
            self._closing = False
            self._task = asyncio.create_task(self._run(), name="pa-instance-maintenance")
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

    def snapshot(self) -> dict[str, Any]:
        return {
            "running": bool(self._task and not self._task.done()),
            "interval_seconds": int(self.settings.maintenance_interval_seconds),
            "transcript_retention_days": int(self.settings.transcript_retention_days),
            "mutation_operation_retention_days": int(
                self.settings.mutation_operation_retention_days
            ),
            "last_started_at": (
                self.last_started_at.isoformat() if self.last_started_at else None
            ),
            "last_finished_at": (
                self.last_finished_at.isoformat() if self.last_finished_at else None
            ),
            "last_error": self.last_error,
            "last_result": self.last_result,
            "transcript_storage": (
                self.store.transcript_storage_metrics()
                if hasattr(self.store, "transcript_storage_metrics")
                else None
            ),
        }

    async def _run(self) -> None:
        while not self._closing:
            try:
                await self.run_once()
            except Exception:
                logger.exception("Instance maintenance sweep failed")
            self._wake.clear()
            try:
                await asyncio.wait_for(
                    self._wake.wait(),
                    timeout=float(self.settings.maintenance_interval_seconds),
                )
            except TimeoutError:
                pass

    async def run_once(self, *, now: datetime | None = None) -> dict[str, Any]:
        async with self._lock:
            started = datetime.now(UTC)
            self.last_started_at = started
            self.last_error = None
            try:
                result = await self._call(
                    run_maintenance,
                    self.settings,
                    self.store,
                    self.services.get("dispatch_store"),
                    now=now,
                )
            except Exception as exc:
                self.last_error = str(exc)
                self.last_finished_at = datetime.now(UTC)
                raise
            self.last_result = result
            self.last_finished_at = datetime.now(UTC)
            logger.info(
                "Instance maintenance removed %s transcript events, %s mutation receipts, "
                "%s dispatch events, %s dispatch receipts",
                result.get("transcript_events_deleted"),
                result.get("mutation_operations_deleted"),
                (result.get("dispatch_compact") or {}).get("events"),
                (result.get("dispatch_compact") or {}).get("receipts"),
            )
            return result

    async def _call(self, fn, *args, **kwargs):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                "maintenance.run",
                fn,
                *args,
                timeout=_MAINTENANCE_TIMEOUT_SECONDS,
                **kwargs,
            )
        return await asyncio.to_thread(fn, *args, **kwargs)
