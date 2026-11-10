"""Startup and daily retention, SQLite integrity checks, and space reclamation."""

from __future__ import annotations

import asyncio
import logging
import shutil
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pa.core.sqlite_metrics import ObservedConnection
from pa.core.performance import capture_retention_phases, capture_sqlite_calls
from pa.execution.dispatch import DispatchStoreReadOnlyError

logger = logging.getLogger(__name__)

_MAINTENANCE_TIMEOUT_SECONDS = 120.0

_PhaseTimes = dict[str, dict[str, Any]]


@contextmanager
def _maintenance_phase(phases: _PhaseTimes, name: str) -> Iterator[None]:
    """Fixed internal phase names; wall/CPU include nested work, not I/O bytes."""
    wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
    phase = phases.setdefault(name, {
        "calls": 0, "errors": 0, "wall_ms": 0.0, "thread_cpu_ms": 0.0,
    })
    errors = 0
    try:
        with capture_sqlite_calls(phase.setdefault("sqlite_call_kinds", {})):
            yield
    except BaseException:
        errors = 1
        raise
    finally:
        phase["calls"] += 1
        phase["errors"] += errors
        finished = time.perf_counter_ns()
        phase["wall_ms"] += (finished - wall) / 1_000_000
        phase["thread_cpu_ms"] += (time.thread_time_ns() - cpu) / 1_000_000
        # Fixed-name, bounded intervals align cached maintenance with worker
        # and lock observations from this process. Repeated calls retain an
        # enclosing interval, including gaps; this is not summed lock hold.
        phase["started_at_monotonic"] = min(
            phase.get("started_at_monotonic", wall / 1_000_000_000),
            wall / 1_000_000_000,
        )
        phase["finished_at_monotonic"] = max(
            phase.get("finished_at_monotonic", finished / 1_000_000_000),
            finished / 1_000_000_000,
        )


@contextmanager
def _maintenance_connection(path: Path, phases: _PhaseTimes) -> Iterator[sqlite3.Connection]:
    with _maintenance_phase(phases, "connect"):
        conn = sqlite3.connect(
            path.resolve().as_uri() + "?mode=rw", uri=True,
            timeout=0.1, isolation_level=None, factory=ObservedConnection,
        )
    try:
        yield conn
    finally:
        with _maintenance_phase(phases, "close"):
            conn.close()


def _maintain_database(
    path: Path, *, stop: threading.Event | None = None
) -> dict[str, Any]:
    """Maintain one existing database using SQLite's transactional write lock.

    A short busy timeout yields to other database users. VACUUM rewrites the
    existing file transactionally; it never swaps files underneath live readers.
    """
    phases: _PhaseTimes = {}
    with _maintenance_phase(phases, "total"):
        return _maintain_database_measured(path, stop=stop, phases=phases)


def _maintain_database_measured(
    path: Path, *, stop: threading.Event | None, phases: _PhaseTimes,
) -> dict[str, Any]:
    # Keep the mapping by reference: close/total finish after a return, including
    # deferred copies of result. Readers see their completed timings, too.
    result: dict[str, Any] = {"compacted": False, "status": "ok", "phases": phases}
    result["sqlite_call_kind_scope"] = (
        "Exclusive innermost measured span; up to eight static call kinds, "
        "attempt counts, wall/thread CPU totals and paired maximum-call interval. "
        "No SQL/bindings/row values retained. Successful native busy wait and "
        "physical I/O remain unseparated; existing phase totals are inclusive. "
        "Connect/close, executescript and native context-manager implicit "
        "commit/rollback remain outside these observed call records."
    )
    if stop is not None and stop.is_set():
        return {**result, "status": "deferred", "reason": "stopping"}
    with _maintenance_connection(path, phases) as conn:
        if stop is not None:
            conn.set_progress_handler(lambda: int(stop.is_set()), 100_000)

        def sizes(name: str) -> dict[str, int]:
            with _maintenance_phase(phases, name):
                page_size = conn.execute("PRAGMA page_size").fetchone()[0]
                pages = conn.execute("PRAGMA page_count").fetchone()[0]
                free = conn.execute("PRAGMA freelist_count").fetchone()[0]
                return {
                    "page_count": pages, "page_size": page_size,
                    "freelist_count": free,
                    "bytes": pages * page_size, "free_bytes": free * page_size,
                }

        def check_integrity(name: str) -> None:
            with _maintenance_phase(phases, name):
                if conn.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise RuntimeError(f"Database integrity check failed: {path.name}")

        def checkpoint(name: str) -> tuple[int, int, int]:
            with _maintenance_phase(phases, name):
                return conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()

        try:
            before = sizes("size_initial")
            result.update(before=before, after=before, **before)
            check_integrity("integrity_before")
            result["quick_check"] = "ok"
            # This is a fresh connection: consider all tables for planner stats.
            with _maintenance_phase(phases, "optimize"):
                conn.execute("PRAGMA optimize(0x10002)")
            checkpoint_result = checkpoint("checkpoint_before")
            result.update(zip(
                ("wal_busy", "wal_log", "wal_checkpointed"), checkpoint_result
            ))
            if checkpoint_result[0]:
                return {**result, "status": "deferred", "reason": "busy"}
            before = sizes("size_pre_compact")
            result.update(before=before, after=before, **before)
            if before["free_bytes"]:
                # SQLite can need twice the original size for its copy/journal.
                with _maintenance_phase(phases, "disk_space"):
                    free_disk_bytes = shutil.disk_usage(path.parent).free
                if free_disk_bytes < 2 * before["bytes"]:
                    return {
                        **result, "status": "deferred",
                        "reason": "insufficient_disk_space",
                    }
                if stop is not None and stop.is_set():
                    return {**result, "status": "deferred", "reason": "stopping"}
                with _maintenance_phase(phases, "vacuum"):
                    conn.execute("VACUUM")
                result["compacted"] = True
                check_integrity("integrity_after")
                checkpoint_result = checkpoint("checkpoint_after")
                result.update(zip(
                    ("wal_busy", "wal_log", "wal_checkpointed"), checkpoint_result
                ))
                if checkpoint_result[0]:
                    result.update(status="deferred", reason="busy")
            after = sizes("size_final")
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


STALE_UPLOAD_SECONDS = 3600


def prune_chat_images(settings: Any, store: Any, *, cutoff: datetime) -> dict[str, int]:
    """Delete retained chat images that no transcript can show any more.

    Images live in ``chat-images/<session id>/``. They follow the transcript's own retention:
    a closed conversation past the cutoff, or a directory whose conversation no longer exists
    and has sat untouched past the cutoff. Half-written uploads are removed after an hour.
    """
    root = Path(settings.data_dir) / "chat-images"
    removed = {"directories": 0, "files": 0}
    if store is None or not hasattr(store, "get_session") or not root.is_dir():
        return removed
    now = time.time()
    limit = cutoff.timestamp()
    for directory in root.iterdir():
        try:
            if directory.is_symlink() or not directory.is_dir():
                continue
            session = store.get_session(directory.name)
            expired = (
                directory.stat().st_mtime < limit if session is None
                else session.status == "closed" and session.updated_at.timestamp() < limit
            )
            if expired:
                removed["files"] += sum(1 for item in directory.rglob("*") if item.is_file())
                shutil.rmtree(directory)
                removed["directories"] += 1
                continue
            for partial in directory.glob(".tmp-*"):
                if now - partial.stat().st_mtime > STALE_UPLOAD_SECONDS:
                    partial.unlink(missing_ok=True)
                    removed["files"] += 1
        except OSError:
            logger.warning("Could not prune chat images in %s", directory, exc_info=True)
    return removed


def run_maintenance(
    settings: Any,
    store: Any,
    dispatch_store: Any | None = None,
    *,
    now: datetime | None = None,
    stop: threading.Event | None = None,
) -> dict[str, Any]:
    """Apply retention to local projection/dispatch state without deleting cards."""
    phases: _PhaseTimes = {}
    with _maintenance_phase(phases, "total"):
        return _run_maintenance_measured(
            settings, store, dispatch_store, now=now, stop=stop, phases=phases,
        )


def _run_maintenance_measured(
    settings: Any, store: Any, dispatch_store: Any | None, *,
    now: datetime | None, stop: threading.Event | None, phases: _PhaseTimes,
) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    transcript_cutoff = now - timedelta(days=int(settings.transcript_retention_days))
    mutation_cutoff = now - timedelta(
        days=int(settings.mutation_operation_retention_days)
    )
    with capture_retention_phases(phases), _maintenance_phase(phases, "transcript_retention"):
        transcript_deleted = store.prune_closed_session_transcripts(before=transcript_cutoff)
        # Retained images follow their transcript's retention, so they share its phase.
        chat_images_deleted = prune_chat_images(settings, store, cutoff=transcript_cutoff)
    with _maintenance_phase(phases, "mutation_retention"):
        mutation_deleted = store.prune_mutation_operations(before=mutation_cutoff)
    result: dict[str, Any] = {
        "ran_at": now.isoformat(),
        "chat_images_deleted": chat_images_deleted,
        "transcript_cutoff": transcript_cutoff.isoformat(),
        "mutation_cutoff": mutation_cutoff.isoformat(),
        "transcript_events_deleted": transcript_deleted,
        "mutation_operations_deleted": mutation_deleted,
        "dispatch_compact": {"events": 0, "receipts": 0},
        "sqlite": {},
        "transcript_storage": {},
        "phases": phases,
        "sqlite_call_kind_scope": (
            "Exclusive innermost measured span; up to eight static call kinds. "
            "Existing phase totals are inclusive; call counts include iterator "
            "exhaustion, which is not an error. No SQL/parameters/results retained."
        ),
    }
    if hasattr(store, "migrate_legacy_transcripts"):
        # Migration includes its own fresh health snapshot. The final health
        # read below remains distinct; do not substitute one for the other.
        with _maintenance_phase(phases, "transcript_migration_and_health"):
            result["transcript_migration"] = store.migrate_legacy_transcripts()
    if dispatch_store is not None:
        try:
            with _maintenance_phase(phases, "dispatch_compact"):
                result["dispatch_compact"] = dispatch_store.compact(now=now)
        except DispatchStoreReadOnlyError:
            result["dispatch_compact"] = {"skipped": "read_only"}
    with _maintenance_phase(phases, "projection_database"):
        result["sqlite"] = _maintain_database(store.db_path, stop=stop)
    with _maintenance_phase(phases, "transcript_database"):
        result["transcript_sqlite"] = _maintain_database(
            store.transcripts.db_path, stop=stop
        )
    if (stop is None or not stop.is_set()) and hasattr(store, "transcript_storage_metrics"):
        with _maintenance_phase(phases, "transcript_health_final"):
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
        startup_ready: asyncio.Future[None] | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.services = services
        self.async_runtime = async_runtime
        self._startup_ready = startup_ready
        self.last_started_at: datetime | None = None
        self.last_finished_at: datetime | None = None
        self.last_error: str | None = None
        self.last_result: dict[str, Any] | None = None
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._closing = False
        self._active_run: asyncio.Task[dict[str, Any]] | None = None
        self._stop = threading.Event()

    def _startup_settled(self, _task: asyncio.Future[None]) -> None:
        self._wake.set()

    def start(self) -> None:
        if not self._task or self._task.done():
            self._closing = False
            self._stop.clear()
            if self._startup_ready is not None:
                self._startup_ready.add_done_callback(self._startup_settled)
            self._task = asyncio.create_task(self._run(), name="pa-instance-maintenance")
            self._wake.set()

    def wake(self) -> None:
        self._wake.set()

    async def close(self) -> None:
        self._closing = True
        if self._startup_ready is not None:
            self._startup_ready.remove_done_callback(self._startup_settled)
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
            "waiting_for_startup": bool(
                self._startup_ready is not None and not self._startup_ready.done()
                and not self._closing
            ),
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
            # Automatic full compaction must not contend with the durable
            # startup/recovery work that establishes admission. A wake is not
            # permission to cross this barrier. Explicit run_once stays usable.
            if self._startup_ready is not None:
                if not self._startup_ready.done():
                    await self._wake.wait()
                    continue
                if self._startup_ready.cancelled():
                    break
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
