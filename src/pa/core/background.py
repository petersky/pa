"""Durable supervision for essential single-instance background loops."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _safe_failure_kind(error: BaseException) -> str:
    if isinstance(error, OSError):
        return "io_error"
    if isinstance(error, RuntimeError):
        return "runtime_error"
    if isinstance(error, (TypeError, ValueError)):
        return "invalid_data"
    return "exception"


class BackgroundTaskSupervisor:
    """Restart a failed or stalled loop while exposing privacy-safe liveness."""

    def __init__(
        self,
        name: str,
        runner: Callable[[Callable[[], None]], Awaitable[None]],
        diagnostics_path: Path,
        *,
        stale_seconds: float = 90.0,
        backoff_seconds: float = 0.25,
        backoff_max_seconds: float = 30.0,
        rng: random.Random | None = None,
    ) -> None:
        self.name = name
        self.runner = runner
        self.path = diagnostics_path
        self.stale_seconds = max(0.05, stale_seconds)
        self.backoff_seconds = max(0.01, backoff_seconds)
        self.backoff_max_seconds = max(self.backoff_seconds, backoff_max_seconds)
        self.rng = rng or random.Random()
        self._task: asyncio.Task[None] | None = None
        self._worker: asyncio.Task[None] | None = None
        self._closing = False
        self._last_progress_monotonic = 0.0
        self._last_save_monotonic = 0.0
        self._save_task: asyncio.Task[None] | None = None
        self._save_lock = asyncio.Lock()
        self._clock = time.monotonic
        self._state: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text())
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError):
            return {}

    async def _save(self) -> None:
        async with self._save_lock:
            snapshot = json.dumps(self._state, sort_keys=True, indent=2) + "\n"

            def write() -> None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                temporary = self.path.with_suffix(self.path.suffix + ".tmp")
                temporary.write_text(snapshot)
                temporary.replace(self.path)

            await asyncio.to_thread(write)

    async def _save_diagnostics(self) -> None:
        """Diagnostic persistence must not terminate essential supervision."""
        try:
            await self._save()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            kind = _safe_failure_kind(exc)
            self._state.update(
                diagnostics_state="write_failed",
                diagnostics_failure_count=int(self._state.get("diagnostics_failure_count") or 0) + 1,
                last_diagnostics_failure_at=datetime.now(UTC).isoformat(),
                last_diagnostics_failure_kind=kind,
            )
            logger.error("%s diagnostic persistence failed (%s); supervision continues", self.name, kind)
        else:
            self._state["diagnostics_state"] = "ready"

    def heartbeat(self) -> None:
        now = self._clock()
        self._last_progress_monotonic = now
        self._state["last_progress_at"] = datetime.now(UTC).isoformat()
        self._state["state"] = "ready"
        if (
            now - self._last_save_monotonic >= min(10.0, self.stale_seconds / 3)
            and (not self._save_task or self._save_task.done())
        ):
            self._last_save_monotonic = now
            self._save_task = asyncio.create_task(self._save_diagnostics())

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        # Never create a second controller around an orphaned live worker.
        # Diagnosis/recovery must preserve that worker's unknown effects.
        if self._worker and not self._worker.done():
            return
        self._closing = False
        self._task = asyncio.create_task(self._supervise(), name=f"{self.name}-supervisor")

    async def close(self) -> None:
        self._closing = True
        for task in (self._worker, self._task):
            if task and not task.done():
                task.cancel()
        tasks = [task for task in (self._worker, self._task) if task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._save_task:
            await asyncio.gather(self._save_task, return_exceptions=True)
        self._worker = self._task = None

    async def _supervise(self) -> None:
        failures = 0
        while not self._closing:
            self._last_progress_monotonic = self._clock()
            self._state.update(
                state="starting", started_at=datetime.now(UTC).isoformat()
            )
            await self._save_diagnostics()
            self._worker = asyncio.create_task(
                self.runner(self.heartbeat), name=f"{self.name}-worker"
            )
            failure_kind = "unexpected_exit"
            failure_detail = "worker returned before shutdown"
            try:
                while not self._worker.done():
                    await asyncio.sleep(min(1.0, self.stale_seconds / 2))
                    age = self._clock() - self._last_progress_monotonic
                    if age > self.stale_seconds:
                        failure_kind = "stalled"
                        failure_detail = f"no progress for {age:.1f}s"
                        self._worker.cancel()
                        await asyncio.gather(self._worker, return_exceptions=True)
                        break
                if self._worker.done() and not self._worker.cancelled():
                    exc = self._worker.exception()
                    if exc:
                        failure_kind = type(exc).__name__
                        failure_detail = f"worker raised {failure_kind}"
            except asyncio.CancelledError:
                if self._worker and not self._worker.done():
                    self._worker.cancel()
                    await asyncio.gather(self._worker, return_exceptions=True)
                raise
            if self._closing:
                break
            failures += 1
            self._state.update(
                state="restarting",
                failure_count=int(self._state.get("failure_count") or 0) + 1,
                last_failure_at=datetime.now(UTC).isoformat(),
                last_failure_kind=failure_kind,
                last_failure_detail=failure_detail,
            )
            await self._save_diagnostics()
            logger.error("%s %s; restarting", self.name, failure_detail)
            delay = min(
                self.backoff_max_seconds,
                self.backoff_seconds * (2 ** min(failures - 1, 8)),
            )
            await asyncio.sleep(delay * self.rng.uniform(0.8, 1.2))

    def health(self) -> dict[str, Any]:
        now = self._clock()
        age = max(0.0, now - self._last_progress_monotonic)
        alive = bool(self._worker and not self._worker.done())
        stale = not alive or age > self.stale_seconds
        controller_alive = bool(self._task and not self._task.done())
        controller_state = "closed" if self._closing else "not_started"
        controller_failure = None
        if self._task is not None:
            if self._task.cancelled():
                controller_state = "cancelled"
            elif self._task.done():
                error = self._task.exception()
                controller_state = "failed" if error else "exited"
                controller_failure = _safe_failure_kind(error) if error else None
            else:
                controller_state = "running"
        return {
            **self._state,
            "name": self.name,
            "state": "stale" if stale else "ready",
            "alive": alive,
            "supervisor_alive": controller_alive,
            "supervisor_state": controller_state,
            "supervisor_failure_kind": controller_failure,
            "orphaned_worker": alive and not controller_alive,
            "closing": self._closing,
            "stale": stale,
            "progress_age_seconds": round(age, 3),
            "stale_after_seconds": self.stale_seconds,
        }
