"""Bounded off-loop execution and event-loop responsiveness telemetry.

Synchronous libraries remain useful for SQLite, Git, and durable filesystem
operations, but they must never run directly in an ASGI/MCP coroutine.  This
module owns PA's deliberately small legacy worker pool and keeps timed-out or
cancelled calls charged to that pool until the underlying thread really exits.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import math
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from typing import Any, Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BlockingQueueFull(RuntimeError):
    """The bounded legacy executor cannot accept more work."""


class BlockingOperationTimeout(TimeoutError):
    """A caller deadline expired while legacy work continues off-loop."""


class AsyncRuntimeClosed(RuntimeError):
    """New work was submitted after shutdown began."""


@dataclass
class OperationMetrics:
    submitted: int = 0
    completed: int = 0
    failed: int = 0
    timed_out: int = 0
    cancelled: int = 0
    rejected: int = 0
    active: int = 0
    queued: int = 0
    max_active: int = 0
    max_queued: int = 0
    total_queue_ms: float = 0.0
    total_runtime_ms: float = 0.0
    max_runtime_ms: float = 0.0


class AsyncRuntime:
    """Own bounded blocking work and report loop/request responsiveness."""

    def __init__(
        self,
        *,
        max_workers: int = 8,
        max_queue: int = 64,
        default_timeout: float = 30.0,
        slow_call_seconds: float = 0.5,
        lag_interval_seconds: float = 0.1,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        if max_queue < 0:
            raise ValueError("max_queue cannot be negative")
        self.max_workers = max_workers
        self.max_queue = max_queue
        self.default_timeout = default_timeout
        self.slow_call_seconds = slow_call_seconds
        self.lag_interval_seconds = lag_interval_seconds
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="pa-blocking",
        )
        self._slots = asyncio.Semaphore(max_workers)
        self._admission_lock = asyncio.Lock()
        self._operations: defaultdict[str, OperationMetrics] = defaultdict(
            OperationMetrics
        )
        self._pending: set[asyncio.Future[Any]] = set()
        self._active = 0
        self._queued = 0
        self._closing = False
        self._lag_task: asyncio.Task[None] | None = None
        self._lag_samples_ms: deque[float] = deque(maxlen=600)
        self._lag_max_ms = 0.0
        self._request_count = 0
        self._request_total_ms = 0.0
        self._request_max_ms = 0.0
        self._request_slow: deque[dict[str, Any]] = deque(maxlen=50)

    async def start(self) -> None:
        if self._lag_task and not self._lag_task.done():
            return
        self._lag_task = asyncio.create_task(
            self._monitor_loop_lag(), name="pa-event-loop-lag"
        )

    async def close(self, *, drain_timeout: float = 5.0) -> None:
        self._closing = True
        if self._lag_task and not self._lag_task.done():
            self._lag_task.cancel()
            await asyncio.gather(self._lag_task, return_exceptions=True)
        pending = set(self._pending)
        if pending:
            await asyncio.wait(pending, timeout=max(0.0, drain_timeout))
        # Running Python threads are not forcibly cancellable.  cancel_futures
        # prevents queued executor work from starting; remaining running calls
        # retain their own resource locks and finish normally.
        self._executor.shutdown(wait=False, cancel_futures=True)

    async def run_blocking(
        self,
        operation: str,
        call: Callable[..., T],
        /,
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> T:
        """Run one legacy call without blocking the event loop.

        Caller cancellation and timeout are prompt, but cannot stop arbitrary
        Python/native code already running in a thread.  Its slot therefore
        remains occupied until the real future completes, preventing false
        capacity and thread-pool exhaustion.
        """

        loop = asyncio.get_running_loop()
        queued_at = loop.time()
        effective_timeout = self.default_timeout if timeout is None else timeout
        deadline_at = (
            None if effective_timeout is None else queued_at + effective_timeout
        )
        metrics = self._operations[operation]
        async with self._admission_lock:
            if self._closing:
                raise AsyncRuntimeClosed("async runtime is closing")
            if self._active + self._queued >= self.max_workers + self.max_queue:
                metrics.rejected += 1
                raise BlockingQueueFull(
                    f"blocking queue is full for {operation!r} "
                    f"({self._active} active, {self._queued} queued)"
                )
            self._queued += 1
            metrics.submitted += 1
            metrics.queued += 1
            metrics.max_queued = max(metrics.max_queued, metrics.queued)

        try:
            if deadline_at is None:
                await self._slots.acquire()
            else:
                async with asyncio.timeout(max(0.0, deadline_at - loop.time())):
                    await self._slots.acquire()
        except TimeoutError as exc:
            async with self._admission_lock:
                self._queued -= 1
                metrics.queued -= 1
                metrics.timed_out += 1
            raise BlockingOperationTimeout(
                f"blocking operation {operation!r} exceeded "
                f"{effective_timeout:.3f}s while waiting for capacity"
            ) from exc
        except asyncio.CancelledError:
            async with self._admission_lock:
                self._queued -= 1
                metrics.queued -= 1
                metrics.cancelled += 1
            raise

        started_at = loop.time()
        runtime_started_at = time.perf_counter()
        async with self._admission_lock:
            self._queued -= 1
            metrics.queued -= 1
            self._active += 1
            metrics.active += 1
            metrics.max_active = max(metrics.max_active, metrics.active)
            metrics.total_queue_ms += (started_at - queued_at) * 1000

        bound = functools.partial(call, *args, **kwargs)
        try:
            future = loop.run_in_executor(self._executor, bound)
        except BaseException:
            self._finish_submission(operation, None, runtime_started_at)
            raise
        self._pending.add(future)
        future.add_done_callback(
            lambda done: self._finish_submission(operation, done, runtime_started_at)
        )

        try:
            if deadline_at is None:
                return await asyncio.shield(future)
            remaining = max(0.0, deadline_at - loop.time())
            async with asyncio.timeout(remaining):
                return await asyncio.shield(future)
        except TimeoutError as exc:
            metrics.timed_out += 1
            raise BlockingOperationTimeout(
                f"blocking operation {operation!r} exceeded "
                f"{effective_timeout:.3f}s"
            ) from exc
        except asyncio.CancelledError:
            metrics.cancelled += 1
            raise

    async def observe(
        self,
        operation: str,
        awaitable: Awaitable[T],
        *,
        timeout: float | None = None,
    ) -> T:
        """Measure cancellable native-async work with an optional deadline."""

        metrics = self._operations[operation]
        metrics.submitted += 1
        metrics.active += 1
        metrics.max_active = max(metrics.max_active, metrics.active)
        started = time.perf_counter()
        try:
            if timeout is None:
                result = await awaitable
            else:
                async with asyncio.timeout(timeout):
                    result = await awaitable
        except TimeoutError:
            metrics.timed_out += 1
            metrics.failed += 1
            raise
        except asyncio.CancelledError:
            metrics.cancelled += 1
            raise
        except BaseException:
            metrics.failed += 1
            raise
        else:
            metrics.completed += 1
            return result
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000
            metrics.active -= 1
            metrics.total_runtime_ms += elapsed_ms
            metrics.max_runtime_ms = max(metrics.max_runtime_ms, elapsed_ms)

    def _finish_submission(
        self,
        operation: str,
        future: asyncio.Future[Any] | None,
        started_at: float,
    ) -> None:
        metrics = self._operations[operation]
        if future is not None:
            self._pending.discard(future)
        self._active -= 1
        metrics.active -= 1
        self._slots.release()
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        metrics.total_runtime_ms += elapsed_ms
        metrics.max_runtime_ms = max(metrics.max_runtime_ms, elapsed_ms)
        if future is None:
            metrics.failed += 1
            return
        if future.cancelled():
            # The waiting coroutine records cancellation. A caller cancelled
            # before executor shutdown may also leave this future queued, so
            # counting here would report the same cancellation twice.
            pass
        elif future.exception() is not None:
            metrics.failed += 1
        else:
            metrics.completed += 1
        if elapsed_ms >= self.slow_call_seconds * 1000:
            logger.warning(
                "Slow off-loop operation operation=%s elapsed_ms=%.1f",
                operation,
                elapsed_ms,
            )

    async def _monitor_loop_lag(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            target = loop.time() + self.lag_interval_seconds
            await asyncio.sleep(self.lag_interval_seconds)
            lag_ms = max(0.0, (loop.time() - target) * 1000)
            self._lag_samples_ms.append(lag_ms)
            self._lag_max_ms = max(self._lag_max_ms, lag_ms)

    def record_request(self, path: str, status: int, elapsed_ms: float) -> None:
        self._request_count += 1
        self._request_total_ms += elapsed_ms
        self._request_max_ms = max(self._request_max_ms, elapsed_ms)
        if elapsed_ms >= self.slow_call_seconds * 1000:
            self._request_slow.append(
                {"path": path, "status": status, "elapsed_ms": round(elapsed_ms, 3)}
            )

    @staticmethod
    def _percentile(values: deque[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = max(0, math.ceil(percentile * len(ordered)) - 1)
        return ordered[index]

    def snapshot(self) -> dict[str, Any]:
        operations = {}
        for name, metrics in sorted(self._operations.items()):
            data = asdict(metrics)
            data["total_queue_ms"] = round(data["total_queue_ms"], 3)
            data["total_runtime_ms"] = round(data["total_runtime_ms"], 3)
            data["max_runtime_ms"] = round(data["max_runtime_ms"], 3)
            operations[name] = data
        return {
            "executor": {
                "max_workers": self.max_workers,
                "max_queue": self.max_queue,
                "active": self._active,
                "queued": self._queued,
                "closing": self._closing,
            },
            "event_loop": {
                "samples": len(self._lag_samples_ms),
                "latest_lag_ms": round(
                    self._lag_samples_ms[-1] if self._lag_samples_ms else 0.0, 3
                ),
                "p95_lag_ms": round(
                    self._percentile(self._lag_samples_ms, 0.95), 3
                ),
                "max_lag_ms": round(self._lag_max_ms, 3),
            },
            "requests": {
                "count": self._request_count,
                "average_ms": round(
                    self._request_total_ms / self._request_count
                    if self._request_count
                    else 0.0,
                    3,
                ),
                "max_ms": round(self._request_max_ms, 3),
                "slow": list(self._request_slow),
            },
            "operations": operations,
        }
