"""Startup and daily retention, SQLite integrity checks, and space reclamation."""

from __future__ import annotations

import asyncio
import logging
import shutil
import sqlite3
import threading
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pa.execution.dispatch import DispatchStoreReadOnlyError

logger = logging.getLogger(__name__)

_MAINTENANCE_TIMEOUT_SECONDS = 120.0


def _maintain_database(
    path: Path, *, stop: threading.Event | None = None
) -> dict[str, Any]:
    """Maintain one existing database using SQLite's transactional write lock.

    A short busy timeout yields to other database users. VACUUM rewrites the
    existing file transactionally; it never swaps files underneath live readers.
    """
    result: dict[str, Any] = {"compacted": False, "status": "ok"}
    if stop is not None and stop.is_set():
        return {**result, "status": "deferred", "reason": "stopping"}
    with closing(sqlite3.connect(
        path.resolve().as_uri() + "?mode=rw", uri=True,
        timeout=0.1, isolation_level=None,
    )) as conn:
        if stop is not None:
            conn.set_progress_handler(lambda: int(stop.is_set()), 100_000)

        def sizes() -> dict[str, int]:
            page_size = conn.execute("PRAGMA page_size").fetchone()[0]
            pages = conn.execute("PRAGMA page_count").fetchone()[0]
            free = conn.execute("PRAGMA freelist_count").fetchone()[0]
            return {
                "page_count": pages, "page_size": page_size,
                "freelist_count": free,
                "bytes": pages * page_size, "free_bytes": free * page_size,
            }

        def check_integrity() -> None:
            if conn.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise RuntimeError(f"Database integrity check failed: {path.name}")

        try:
            before = sizes()
            result.update(before=before, after=before, **before)
            check_integrity()
            result["quick_check"] = "ok"
            # This is a fresh connection: consider all tables for planner stats.
            conn.execute("PRAGMA optimize(0x10002)")
            checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            result.update(zip(
                ("wal_busy", "wal_log", "wal_checkpointed"), checkpoint
            ))
            if checkpoint[0]:
                return {**result, "status": "deferred", "reason": "busy"}
            before = sizes()
            result.update(before=before, after=before, **before)
            if before["free_bytes"]:
                # SQLite can need twice the original size for its copy/journal.
                if shutil.disk_usage(path.parent).free < 2 * before["bytes"]:
                    return {
                        **result, "status": "deferred",
                        "reason": "insufficient_disk_space",
                    }
                if stop is not None and stop.is_set():
                    return {**result, "status": "deferred", "reason": "stopping"}
                conn.execute("VACUUM")
                result["compacted"] = True
                check_integrity()
                checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                result.update(zip(
                    ("wal_busy", "wal_log", "wal_checkpointed"), checkpoint
                ))
                if checkpoint[0]:
                    result.update(status="deferred", reason="busy")
            after = sizes()
            result.update(after=after, **after)
            return result
        except sqlite3.OperationalError as exc:
            code = getattr(exc, "sqlite_errorcode", 0) & 0xff
            if code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                return {**result, "status": "deferred", "reason": "busy"}
            if code == sqlite3.SQLITE_INTERRUPT and stop is not None and stop.is_set():
                return {**result, "status": "deferred", "reason": "stopping"}
            raise


def compact_database(settings: Any) -> dict[str, Any]:
    """Reclaim SQLite free pages while holding the server's exclusive lock."""
    from pa.core.writer_lock import DataDirWriterLock

    path = settings.db_path
    if not path.is_file():
        raise FileNotFoundError(f"Database does not exist: {path}")
    lock = DataDirWriterLock(settings.data_dir)
    lock.acquire()
    try:
        result = _maintain_database(path)
        if result["status"] == "deferred":
            raise RuntimeError(f"Database compaction deferred: {result['reason']}")
        return result
    finally:
        lock.release()


def run_maintenance(
    settings: Any,
    store: Any,
    dispatch_store: Any | None = None,
    *,
    now: datetime | None = None,
    stop: threading.Event | None = None,
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
    result["sqlite"] = _maintain_database(store.db_path, stop=stop)
    result["transcript_sqlite"] = _maintain_database(
        store.transcripts.db_path, stop=stop
    )
    if (stop is None or not stop.is_set()) and hasattr(store, "transcript_storage_metrics"):
        result["transcript_storage"] = store.transcript_storage_metrics()
    return result


class InstanceMaintenanceService:
    """Run one sweep at startup, then at the configured daily interval."""

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
        self._active_run: asyncio.Task[dict[str, Any]] | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if not self._task or self._task.done():
            self._closing = False
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="pa-instance-maintenance")
            self._wake.set()

    def wake(self) -> None:
        self._wake.set()

    async def close(self) -> None:
        self._closing = True
        self._stop.set()
        self._wake.set()
        # Shield the actual worker even if the server's shutdown budget expires.
        # The progress callback interrupts SQLite and rolls VACUUM back safely.
        if self._task:
            await asyncio.shield(self._task)
        if self._active_run:
            await asyncio.gather(asyncio.shield(self._active_run), return_exceptions=True)

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
                self.store.transcript_storage_status()
                if hasattr(self.store, "transcript_storage_status")
                else None
            ),
        }

    async def _run(self) -> None:
        while not self._closing:
            self._wake.clear()
            try:
                await self.run_once()
            except Exception:
                logger.exception("Instance maintenance sweep failed")
            if self._closing:
                break
            try:
                await asyncio.wait_for(
                    self._wake.wait(),
                    timeout=float(self.settings.maintenance_interval_seconds),
                )
            except TimeoutError:
                pass

    async def run_once(self, *, now: datetime | None = None) -> dict[str, Any]:
        if self._closing:
            raise RuntimeError("Instance maintenance is stopping")
        # Manual requests and the timer share a worker. A disconnected request
        # must not release ownership while its SQLite thread is still running.
        if self._active_run is None or self._active_run.done():
            self._active_run = asyncio.create_task(
                self._execute(now=now), name="pa-instance-maintenance-sweep"
            )
            self._active_run.add_done_callback(
                lambda task: task.exception() if not task.cancelled() else None
            )
        return await asyncio.shield(self._active_run)

    async def _execute(self, *, now: datetime | None = None) -> dict[str, Any]:
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
                stop=self._stop,
            )
        except Exception as exc:
            self.last_error = str(exc)
            self.last_finished_at = datetime.now(UTC)
            raise
        self.last_result = result
        self.last_finished_at = datetime.now(UTC)
        logger.info(
            "Instance maintenance removed %s transcript events, %s mutation receipts, "
            "%s dispatch events, %s dispatch receipts; database maintenance: %s",
            result.get("transcript_events_deleted"),
            result.get("mutation_operations_deleted"),
            (result.get("dispatch_compact") or {}).get("events"),
            (result.get("dispatch_compact") or {}).get("receipts"),
            {key: {field: value.get(field) for field in ("compacted", "status", "reason")}
             for key in ("sqlite", "transcript_sqlite")
             if isinstance(value := result.get(key), dict)},
        )
        return result

    async def _call(self, fn, *args, **kwargs):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                "maintenance.run",
                fn,
                *args,
                timeout=_MAINTENANCE_TIMEOUT_SECONDS,
                wait_for_completion=True,
                **kwargs,
            )
        return await asyncio.to_thread(fn, *args, **kwargs)
