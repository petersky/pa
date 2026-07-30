from __future__ import annotations

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pa.config import Settings
from pa.telemetry.collector import ResourceCollector, SessionTarget, build_collector
from pa.telemetry.models import Metric, MetricQuality, TelemetrySample
from pa.telemetry.storage import TelemetryStorage

logger = logging.getLogger(__name__)


class TelemetryService:
    """Bounded, failure-isolated sampler and persistence pipeline."""

    def __init__(
        self,
        settings: Settings,
        *,
        storage: TelemetryStorage,
        agent_manager: Any = None,
        collector: ResourceCollector | None = None,
        queue_size: int = 32,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.agent_manager = agent_manager
        self.collector = collector or build_collector(
            instance_id=settings.instance_id,
            instance_name=settings.instance_name,
            database_path=Path(settings.telemetry_database_path),
            pa_pid=os.getpid(),
        )
        self.restart_id = str(uuid4())
        self._queue: asyncio.Queue[list[TelemetrySample] | None] = asyncio.Queue(
            maxsize=queue_size
        )
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pa-telemetry"
        )
        self._executor_closed = False
        self._sample_task: asyncio.Task | None = None
        self._writer_task: asyncio.Task | None = None
        self._stopping = False
        self._latest: dict[tuple[str, str], TelemetrySample] = {}
        self.started_at: datetime | None = None
        self.last_collection_at: datetime | None = None
        self.last_persisted_at: datetime | None = None
        self.last_collection_error: str | None = None
        self.last_storage_error: str | None = None
        self.dropped_samples = 0
        self.collection_failures = 0
        self.storage_failures = 0
        self.samples_collected = 0
        self.samples_persisted = 0

    async def _run(self, call, /, *args, **kwargs):
        if self._executor_closed:
            raise RuntimeError("telemetry service is closed")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: call(*args, **kwargs))

    def _session_targets(self) -> list[SessionTarget]:
        if not self.settings.telemetry_per_session_enabled or not self.agent_manager:
            return []
        result: list[SessionTarget] = []
        try:
            runtimes = self.agent_manager.list_runtimes()
        except AttributeError, RuntimeError:
            return result
        for runtime in runtimes:
            if getattr(runtime, "_closed", False):
                continue
            connection = getattr(runtime, "connection", None)
            process = getattr(connection, "_proc", None) if connection else None
            pid = getattr(process, "pid", None)
            if not isinstance(pid, int) or pid <= 0:
                continue
            if getattr(process, "returncode", None) is not None:
                continue
            poll = getattr(process, "poll", None)
            if callable(poll) and poll() is not None:
                continue
            session = runtime.session
            result.append(
                SessionTarget(
                    session_id=session.id,
                    root_pid=pid,
                    provider_id=session.agent_name,
                    card_id=session.card_id,
                    project_id=session.project_id,
                    realm_id=session.realm_id,
                    principal_id=session.principal_id,
                )
            )
        return result

    async def start(self) -> None:
        if self._executor_closed:
            raise RuntimeError("closed telemetry service cannot be restarted")
        if not self.settings.telemetry_enabled or self._sample_task:
            return
        self._stopping = False
        self.started_at = datetime.now(UTC)
        self._writer_task = asyncio.create_task(
            self._writer_loop(), name="pa-telemetry-writer"
        )
        self._sample_task = asyncio.create_task(
            self._sample_loop(), name="pa-telemetry-sampler"
        )

    async def stop(self, *, close: bool = False) -> None:
        """Stop collection, optionally closing the service for process shutdown."""
        self._stopping = True
        if self._sample_task:
            self._sample_task.cancel()
            try:
                await self._sample_task
            except asyncio.CancelledError:
                pass
            self._sample_task = None
        if self._writer_task:
            try:
                self._queue.put_nowait(None)
            except asyncio.QueueFull:
                try:
                    dropped = self._queue.get_nowait()
                    if dropped:
                        self.dropped_samples += len(dropped)
                    self._queue.task_done()
                except asyncio.QueueEmpty:
                    pass
                self._queue.put_nowait(None)
            try:
                await asyncio.wait_for(self._writer_task, timeout=8.0)
            except TimeoutError:
                self._writer_task.cancel()
                logger.warning("Telemetry writer did not stop within its bound")
            self._writer_task = None
        if close and not self._executor_closed:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor_closed = True

    async def collect_once(self, *, persist: bool = False) -> list[TelemetrySample]:
        targets = self._session_targets()
        samples = await self._run(
            self.collector.collect, restart_id=self.restart_id, sessions=targets
        )
        if samples:
            samples[0].metrics["agents.concurrent"] = Metric(
                value=len(targets),
                unit="sessions",
                quality=MetricQuality.MEASURED,
                source="PA live session registry",
            )
        self.last_collection_at = datetime.now(UTC)
        self.last_collection_error = None
        self.samples_collected += len(samples)
        for sample in samples:
            self._latest[(sample.scope_type, sample.scope_id)] = sample
        if persist:
            self._enqueue(samples)
        return samples

    def _enqueue(self, samples: list[TelemetrySample]) -> None:
        try:
            self._queue.put_nowait(samples)
        except asyncio.QueueFull:
            self.dropped_samples += len(samples)

    async def _sample_loop(self) -> None:
        next_persist = time.monotonic()
        next_prune = time.monotonic()
        while not self._stopping:
            tick = time.monotonic()
            try:
                persist = tick >= next_persist
                await self.collect_once(persist=persist)
                if persist:
                    next_persist = (
                        tick + self.settings.telemetry_persistence_interval_seconds
                    )
                if tick >= next_prune:
                    await self._run(
                        self.storage.prune,
                        raw_retention_hours=self.settings.telemetry_raw_retention_hours,
                        rollup_retention_hours=(
                            self.settings.telemetry_rollup_retention_hours
                        ),
                        max_database_bytes=self.settings.telemetry_max_database_bytes,
                    )
                    next_prune = tick + 3600
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.collection_failures += 1
                self.last_collection_error = str(exc)[:1000]
                logger.warning(
                    "Telemetry sampling failed without affecting PA", exc_info=True
                )
            delay = max(
                0.05,
                self.settings.telemetry_live_interval_seconds
                - (time.monotonic() - tick),
            )
            await asyncio.sleep(delay)

    async def _writer_loop(self) -> None:
        while True:
            batch = await self._queue.get()
            try:
                if batch is None:
                    return
                try:
                    written = await self._run(self.storage.insert_samples, batch)
                    self.samples_persisted += written
                    self.last_persisted_at = datetime.now(UTC)
                    self.last_storage_error = None
                except Exception as exc:
                    self.storage_failures += 1
                    self.dropped_samples += len(batch)
                    self.last_storage_error = str(exc)[:1000]
                    logger.warning(
                        "Telemetry persistence failed without affecting PA",
                        exc_info=True,
                    )
            finally:
                self._queue.task_done()

    def live(
        self,
        *,
        scope_type: str | None = None,
        scope_id: str | None = None,
        principal_id: str | None = None,
        auth_required: bool = False,
    ) -> dict:
        now = datetime.now(UTC)
        samples = []
        for (sample_scope_type, sample_scope_id), sample in self._latest.items():
            if scope_type and sample_scope_type != scope_type:
                continue
            if scope_id and sample_scope_id != scope_id:
                continue
            if (
                auth_required
                and sample.scope_type == "session"
                and sample.principal_id != principal_id
            ):
                continue
            data = sample.public_dict()
            age = max(0.0, (now - sample.timestamp).total_seconds())
            data["freshness"] = {
                "age_seconds": age,
                "state": (
                    "fresh"
                    if age <= self.settings.telemetry_live_interval_seconds * 2.5
                    else "stale"
                ),
                "expected_interval_seconds": (
                    self.settings.telemetry_live_interval_seconds
                ),
            }
            samples.append(data)
        return {
            "enabled": self.settings.telemetry_enabled,
            "restart_id": self.restart_id,
            "samples": sorted(
                samples, key=lambda item: (item["scope_type"], item["scope_id"])
            ),
            "health": self.health(include_storage=False),
        }

    def health(self, *, include_storage: bool = True) -> dict:
        state = (
            "disabled"
            if not self.settings.telemetry_enabled
            else "degraded"
            if self.last_collection_error or self.last_storage_error
            else "warming"
            if self.last_collection_at is None
            else "ready"
        )
        result = {
            "state": state,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_collection_at": (
                self.last_collection_at.isoformat() if self.last_collection_at else None
            ),
            "last_persisted_at": (
                self.last_persisted_at.isoformat() if self.last_persisted_at else None
            ),
            "last_collection_error": self.last_collection_error,
            "last_storage_error": self.last_storage_error,
            "queue_depth": self._queue.qsize(),
            "queue_capacity": self._queue.maxsize,
            "dropped_samples": self.dropped_samples,
            "collection_failures": self.collection_failures,
            "storage_failures": self.storage_failures,
            "samples_collected": self.samples_collected,
            "samples_persisted": self.samples_persisted,
            "platform": self.collector.platform,
            "per_session_enabled": self.settings.telemetry_per_session_enabled,
        }
        if include_storage:
            result["storage"] = self.storage.status()
        return result

    def effective_config(self) -> dict:
        fields = (
            "telemetry_enabled",
            "telemetry_live_interval_seconds",
            "telemetry_persistence_interval_seconds",
            "telemetry_raw_retention_hours",
            "telemetry_rollup_retention_hours",
            "telemetry_max_database_bytes",
            "telemetry_database_path",
            "telemetry_per_session_enabled",
            "telemetry_ui_refresh_seconds",
            "telemetry_default_report_range",
        )
        result = {}
        for field in fields:
            value = getattr(self.settings, field)
            result[field.removeprefix("telemetry_")] = {
                "value": str(value) if isinstance(value, Path) else value,
                "source": (
                    "environment_or_instance_config"
                    if field in self.settings.model_fields_set
                    else "default"
                ),
            }
        return result
