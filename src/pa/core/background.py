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

    def heartbeat(self) -> None:
        now = asyncio.get_running_loop().time()
        self._last_progress_monotonic = now
        self._state["last_progress_at"] = datetime.now(UTC).isoformat()
        self._state["state"] = "ready"
        if (
            now - self._last_save_monotonic >= min(10.0, self.stale_seconds / 3)
            and (not self._save_task or self._save_task.done())
        ):
            self._last_save_monotonic = now
            self._save_task = asyncio.create_task(self._save())

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._closing = False
        self._task = asyncio.create_task(self._supervise(), name=f"{self.name}-supervisor")

    async def close(self) -> None:
        self._closing = True
        for task in (self._worker, self._task):
            if task and not task.done():
                task.cancel()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)
        if self._save_task:
            await asyncio.gather(self._save_task, return_exceptions=True)
        self._worker = self._task = None

    async def _supervise(self) -> None:
        failures = 0
        while not self._closing:
            self._last_progress_monotonic = asyncio.get_running_loop().time()
            self._state.update(
                state="starting", started_at=datetime.now(UTC).isoformat()
            )
            await self._save()
            self._worker = asyncio.create_task(
                self.runner(self.heartbeat), name=f"{self.name}-worker"
            )
            failure_kind = "unexpected_exit"
            failure_detail = "worker returned before shutdown"
            try:
                while not self._worker.done():
                    await asyncio.sleep(min(1.0, self.stale_seconds / 2))
                    age = asyncio.get_running_loop().time() - self._last_progress_monotonic
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
            await self._save()
            logger.error("%s %s; restarting", self.name, failure_detail)
            delay = min(
                self.backoff_max_seconds,
                self.backoff_seconds * (2 ** min(failures - 1, 8)),
            )
            await asyncio.sleep(delay * self.rng.uniform(0.8, 1.2))

    def health(self) -> dict[str, Any]:
        now = time.monotonic()
        age = max(0.0, now - self._last_progress_monotonic)
        alive = bool(self._worker and not self._worker.done())
        stale = not alive or age > self.stale_seconds
        return {
            **self._state,
            "name": self.name,
            "state": "stale" if stale else "ready",
            "alive": alive,
            "stale": stale,
            "progress_age_seconds": round(age, 3),
            "stale_after_seconds": self.stale_seconds,
        }
