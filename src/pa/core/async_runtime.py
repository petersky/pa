"""Bounded off-loop execution and event-loop responsiveness telemetry.

Synchronous libraries remain useful for SQLite, Git, and durable filesystem
operations, but they must never run directly in an ASGI/MCP coroutine.  This
module owns PA's deliberately small legacy worker pool and keeps timed-out or
cancelled calls charged to that pool until the underlying thread really exits.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
import math
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, fields
from typing import Any, Awaitable, Callable, TypeVar

from pa.core.operation_budget import OperationBudget, OperationDeadline, current_operation
from pa.core.sqlite_metrics import sqlite_connection_scope
from pa.core.performance import (
    SAMPLE_LIMIT, OperationObservation, PerformanceMetrics, current_observation, distribution,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# These operations are deliberately off-loop and include normal filesystem,
# SQLite, object traversal, or Git latency. Keep collecting their exact metrics,
# but reserve warnings for durations that indicate genuine degradation.
_SLOW_OPERATION_SECONDS = {
    "dispatch.runnable_read": 2.0,
    "filesystem.github_credentials_read": 2.0,
    "session_lifecycle.sessions": 2.0,
    "maintenance.run": 30.0,
    "sync.object_collect": 2.0,
    "sync.object_list": 2.0,
    "workspace.project_provision": 5.0,
    "workspace.reconcile_terminal_state": 2.0,
}


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
    total_wait_ms: float = 0.0
    max_wait_ms: float = 0.0
    total_lock_wait_ms: float = 0.0
    total_runtime_ms: float = 0.0
    max_runtime_ms: float = 0.0
    slow_calls: int = 0
    runtime_samples_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    queue_samples_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    python_lock_wait_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    python_lock_hold_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    outer_lock_hold_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    worker_service_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    executor_dispatch_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    completion_delivery_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    thread_cpu_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    sqlite_statement_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    sqlite_commit_ms: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    sqlite_calls: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    sqlite_rows: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    sqlite_aggregate_items: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    sqlite_aggregate_bytes: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_LIMIT))
    sqlite_busy_failures: int = 0


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
        operation_budgets: dict[str, dict[str, float]] | None = None,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        if max_queue < 0:
            raise ValueError("max_queue cannot be negative")
        self.operation_budgets = {key: OperationBudget(**value) for key, value in (operation_budgets or {}).items()}
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
        self._owned: dict[str, asyncio.Task[Any]] = {}
        self._accepting_owned = True
        self._active_evidence: dict[asyncio.Future[Any], tuple[str, OperationDeadline]] = {}
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
        self._slow_operations: deque[dict[str, Any]] = deque(maxlen=64)
        self._slow_lock_timelines: deque[dict[str, Any]] = deque(maxlen=64)
        self._request_samples_ms: deque[float] = deque(maxlen=SAMPLE_LIMIT)
        self._request_bytes = 0
        self._request_routes: dict[str, dict] = {}
        self._stream_requests = 0
        self.work_metrics = PerformanceMetrics()

    async def start(self) -> None:
        if self._lag_task and not self._lag_task.done():
            return
        self._lag_task = asyncio.create_task(
            self._monitor_loop_lag(), name="pa-event-loop-lag"
        )

    def reset_lag_samples(self) -> None:
        """Drop collected lag so a caller can measure one load window."""
        self._lag_samples_ms.clear()
        self._lag_max_ms = 0.0

    async def close(self, *, drain_timeout: float = 5.0) -> None:
        self._accepting_owned = False
        if self._lag_task and not self._lag_task.done():
            self._lag_task.cancel()
            await asyncio.gather(self._lag_task, return_exceptions=True)
        owned = set(self._owned.values())
        if owned:
            await asyncio.wait(owned, timeout=max(0.0, drain_timeout))
        self._closing = True
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
        on_commit: Callable[[T], None] | None = None,
        wait_for_completion: bool = False,
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
        policy = self.operation_budgets.get(operation) if timeout is None else None
        evidence = OperationDeadline(policy, time.monotonic()) if policy else None
        if evidence:
            deadline_at = loop.time() + max(0, evidence.expires_at() - time.monotonic())
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
                waited_ms = (loop.time() - queued_at) * 1000
                metrics.total_queue_ms += waited_ms
                metrics.total_wait_ms += waited_ms
                metrics.max_wait_ms = max(metrics.max_wait_ms, waited_ms)
            raise BlockingOperationTimeout(
                f"blocking operation {operation!r} exceeded "
                f"{(policy.queue_seconds if policy else effective_timeout):.3f}s while waiting for capacity"
            ) from exc
        except asyncio.CancelledError:
            async with self._admission_lock:
                self._queued -= 1
                metrics.queued -= 1
                metrics.cancelled += 1
                waited_ms = (loop.time() - queued_at) * 1000
                metrics.total_queue_ms += waited_ms
                metrics.total_wait_ms += waited_ms
                metrics.max_wait_ms = max(metrics.max_wait_ms, waited_ms)
            raise

        started_at = loop.time()
        runtime_started_at = time.perf_counter()
        async with self._admission_lock:
            self._queued -= 1
            metrics.queued -= 1
            self._active += 1
            metrics.active += 1
            metrics.max_active = max(metrics.max_active, metrics.active)
            queue_ms = (started_at - queued_at) * 1000
            metrics.total_queue_ms += queue_ms
            metrics.queue_samples_ms.append(queue_ms)

        if evidence:
            evidence.execution_started(time.monotonic())
        observation = OperationObservation(queue_wait_ms=queue_ms)
        def invoke():
            observation.worker_started_at = time.perf_counter()
            cpu_started = time.thread_time()
            token = current_operation.set(evidence)
            observation_token = current_observation.set(observation)
            try:
                with sqlite_connection_scope():
                    return call(*args, **kwargs)
            finally:
                observation.thread_cpu_ms = (time.thread_time() - cpu_started) * 1000
                observation.worker_finished_at = time.perf_counter()
                current_observation.reset(observation_token)
                current_operation.reset(token)
        context = contextvars.copy_context()
        bound = functools.partial(context.run, invoke)
        try:
            future = loop.run_in_executor(self._executor, bound)
        except BaseException:
            self._finish_submission(operation, None, runtime_started_at)
            raise
        self._pending.add(future)
        if evidence:
            self._active_evidence[future] = (operation, evidence)
        future.add_done_callback(
            lambda done: self._finish_submission(operation, done, runtime_started_at, evidence, observation)
        )
        if on_commit is not None:
            def committed(done):
                # The executor owns this callback, independently of the HTTP or
                # MCP waiter. A late successful write must still schedule work.
                if not done.cancelled() and done.exception() is None:
                    try:
                        on_commit(done.result())
                    except Exception:
                        logger.exception("Post-commit scheduling failed operation=%s", operation)
            future.add_done_callback(committed)

        try:
            if deadline_at is None or wait_for_completion:
                return await asyncio.shield(future)
            if evidence:
                while not future.done():
                    remaining = evidence.expires_at() - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(f"{operation}: {evidence.phase} deadline expired")
                    await asyncio.wait({future}, timeout=min(remaining, 1.0))
                return future.result()
            remaining = max(0.0, deadline_at - loop.time())
            async with asyncio.timeout(remaining):
                return await asyncio.shield(future)
        except TimeoutError as exc:
            if future.done() and not future.cancelled():
                return future.result()  # The operation itself failed; this is not a wait timeout.
            metrics.timed_out += 1
            raise BlockingOperationTimeout(
                f"blocking operation {operation!r} exceeded "
                f"{effective_timeout:.3f}s" if evidence is None else
                f"blocking operation {operation!r} wait expired in {evidence.phase}; "
                "underlying execution continues until its authoritative result"
            ) from exc
        except asyncio.CancelledError:
            metrics.cancelled += 1
            raise
        finally:
            waited_ms = (loop.time() - queued_at) * 1000
            metrics.total_wait_ms += waited_ms
            metrics.max_wait_ms = max(metrics.max_wait_ms, waited_ms)

    async def run_owned(
        self, key: str, factory: Callable[[], Awaitable[T]], *, wait_timeout: float = 120.0
    ) -> T:
        """Keep an admitted mutation and its post-commit work alive after disconnect.

        The operation's own durable idempotency check remains authoritative; this
        registry coalesces concurrent waits and owns the task until it finishes.
        """
        task = self._owned.get(key)
        if task is None:
            if not self._accepting_owned:
                raise AsyncRuntimeClosed("async runtime is closing")
            if len(self._owned) >= self.max_workers + self.max_queue:
                raise BlockingQueueFull("owned mutation capacity unavailable")
            task = asyncio.create_task(factory(), name="pa-owned-mutation")
            self._owned[key] = task

            def finished(done):
                if self._owned.get(key) is done:
                    self._owned.pop(key, None)
                if not done.cancelled():
                    done.exception()  # Retrieve failures even when every waiter left.

            task.add_done_callback(finished)
        try:
            async with asyncio.timeout(wait_timeout):
                return await asyncio.shield(task)
        except TimeoutError as exc:
            if task.done():
                return task.result()
            raise BlockingOperationTimeout(
                "Mutation is still pending; the client wait expired. "
                "Recover the authoritative result using the same idempotency key."
            ) from exc

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
            metrics.runtime_samples_ms.append(elapsed_ms)

    def _finish_submission(
        self,
        operation: str,
        future: asyncio.Future[Any] | None,
        started_at: float,
        evidence: OperationDeadline | None = None,
        observation: OperationObservation | None = None,
    ) -> None:
        metrics = self._operations[operation]
        if evidence:
            metrics.total_lock_wait_ms += evidence.lock_wait_seconds * 1000
        if observation:
            metrics.python_lock_wait_ms.append(observation.python_lock_wait_ms)
            metrics.python_lock_hold_ms.append(observation.python_lock_hold_ms)
            metrics.outer_lock_hold_ms.append(observation.outer_lock_hold_ms)
            metrics.sqlite_statement_ms.append(observation.sqlite_statement_ms)
            metrics.sqlite_commit_ms.append(observation.sqlite_commit_ms)
            metrics.sqlite_calls.append(observation.sqlite_calls)
            metrics.sqlite_rows.append(observation.sqlite_rows)
            metrics.sqlite_aggregate_items.append(observation.sqlite_aggregate_items)
            metrics.sqlite_aggregate_bytes.append(observation.sqlite_aggregate_bytes)
            metrics.sqlite_busy_failures += observation.sqlite_busy_failures
            if observation.worker_finished_at:
                metrics.worker_service_ms.append(
                    (observation.worker_finished_at - observation.worker_started_at) * 1000
                )
                metrics.executor_dispatch_ms.append(
                    (observation.worker_started_at - started_at) * 1000
                )
                metrics.completion_delivery_ms.append(
                    (time.perf_counter() - observation.worker_finished_at) * 1000
                )
                metrics.thread_cpu_ms.append(observation.thread_cpu_ms)
        if future is not None:
            self._pending.discard(future)
            self._active_evidence.pop(future, None)
        self._active -= 1
        metrics.active -= 1
        self._slots.release()
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        metrics.total_runtime_ms += elapsed_ms
        metrics.max_runtime_ms = max(metrics.max_runtime_ms, elapsed_ms)
        metrics.runtime_samples_ms.append(elapsed_ms)
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
        warning_seconds = _SLOW_OPERATION_SECONDS.get(
            operation, self.slow_call_seconds
        )
        if elapsed_ms >= warning_seconds * 1000:
            metrics.slow_calls += 1
            phases = {"operation": operation, "elapsed_ms": round(elapsed_ms, 3),
                      "observed_at_monotonic": round(time.perf_counter(), 6)}
            if observation is not None:
                for name in ("queue_wait_ms", "thread_cpu_ms", "python_lock_wait_ms", "python_lock_hold_ms", "outer_lock_hold_ms",
                             "sqlite_statement_ms", "sqlite_commit_ms", "sqlite_calls", "sqlite_rows",
                             "sqlite_aggregate_items", "sqlite_aggregate_bytes", "sqlite_busy_failures"):
                    phases[name] = round(getattr(observation, name), 3)
                phases["lock_events_total"] = observation.lock_events_total
                if observation.lock_events:
                    self._slow_lock_timelines.append({
                        "operation": operation,
                        "observed_at_monotonic": phases["observed_at_monotonic"],
                        "lock_events_total": observation.lock_events_total,
                        "events": [dict(item) for item in observation.lock_events],
                    })
                if observation.worker_finished_at:
                    phases.update(
                        worker_service_ms=round((observation.worker_finished_at - observation.worker_started_at) * 1000, 3),
                        executor_dispatch_ms=round((observation.worker_started_at - started_at) * 1000, 3),
                        completion_delivery_ms=round((time.perf_counter() - observation.worker_finished_at) * 1000, 3),
                    )
            self._slow_operations.append(phases)
            logger.warning(
                "Slow off-loop operation operation=%s elapsed_ms=%.1f",
                operation,
                elapsed_ms,
                extra={"performance_phases": phases},
            )

    async def _monitor_loop_lag(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            target = loop.time() + self.lag_interval_seconds
            await asyncio.sleep(self.lag_interval_seconds)
            lag_ms = max(0.0, (loop.time() - target) * 1000)
            self._lag_samples_ms.append(lag_ms)
            self._lag_max_ms = max(self._lag_max_ms, lag_ms)

    def record_request(
        self, path: str, status: int, elapsed_ms: float, *,
        response_bytes: int = 0, streaming: bool = False,
    ) -> None:
        if streaming:
            self._stream_requests += 1
            return
        self._request_count += 1
        self._request_total_ms += elapsed_ms
        self._request_max_ms = max(self._request_max_ms, elapsed_ms)
        self._request_samples_ms.append(elapsed_ms)
        self._request_bytes += response_bytes
        # HTTP middleware supplies route templates, never card/session IDs or
        # query strings. Cap cardinality even for third-party routes.
        if path not in self._request_routes and len(self._request_routes) >= 127:
            path = "__other__"
        route = self._request_routes.setdefault(path, {
            "count": 0, "response_bytes": 0,
            "latencies": deque(maxlen=SAMPLE_LIMIT),
            "sizes": deque(maxlen=SAMPLE_LIMIT),
        })
        route["count"] += 1
        route["response_bytes"] += response_bytes
        route["latencies"].append(elapsed_ms)
        route["sizes"].append(response_bytes)
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
            data = {item.name: getattr(metrics, item.name) for item in fields(metrics)
                    if not isinstance(getattr(metrics, item.name), deque)}
            data["runtime_ms"] = distribution(list(metrics.runtime_samples_ms))
            data["queue_ms"] = distribution(list(metrics.queue_samples_ms))
            data["python_lock_wait_ms"] = distribution(list(metrics.python_lock_wait_ms))
            data["python_lock_hold_ms"] = distribution(list(metrics.python_lock_hold_ms))
            for phase in ("worker_service_ms", "executor_dispatch_ms", "completion_delivery_ms", "thread_cpu_ms", "outer_lock_hold_ms",
                          "sqlite_statement_ms", "sqlite_commit_ms", "sqlite_calls", "sqlite_rows",
                          "sqlite_aggregate_items", "sqlite_aggregate_bytes"):
                data[phase] = distribution(list(getattr(metrics, phase)))
            data["total_queue_ms"] = round(data["total_queue_ms"], 3)
            data["total_runtime_ms"] = round(data["total_runtime_ms"], 3)
            data["max_runtime_ms"] = round(data["max_runtime_ms"], 3)
            operations[name] = data
        return {
            "slow_operations": {"capacity": 64, "scope": "last completed slow worker invocations; queue time separate from elapsed",
                                "lock_note": "measured lock sections are inclusive; nested hold intervals may overlap",
                                "lock_events_note": "last64 measured sections per slow invocation; total includes evicted sections; outer hold is the union of outermost measured sections, not CPU time",
                                "observations": [dict(item) for item in self._slow_operations]},
            "slow_lock_timelines": {"capacity": 64, "events_per_invocation": 64,
                                    "observations": [{**item, "events": [dict(event) for event in item["events"]]}
                                                     for item in self._slow_lock_timelines]},
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
                "window": "last_256_non_sse_requests_per_route",
                "count": self._request_count,
                "average_ms": round(
                    self._request_total_ms / self._request_count
                    if self._request_count
                    else 0.0,
                    3,
                ),
                "max_ms": round(self._request_max_ms, 3),
                "slow": list(self._request_slow),
                "latency_ms": distribution(list(self._request_samples_ms)),
                "response_bytes": self._request_bytes,
                "excluded_sse_requests": self._stream_requests,
                "routes": {path: {
                    "count": item["count"], "response_bytes": item["response_bytes"],
                    "latency_ms": distribution(list(item["latencies"])),
                    "response_size_bytes": distribution(list(item["sizes"])),
                } for path, item in list(self._request_routes.items())},
            },
            "owned_mutations": {"pending": len(self._owned)},
            "work": self.work_metrics.snapshot(),
            "active_operations": [
                {"operation": name, "phase": evidence.phase,
                 "elapsed_ms": round((time.monotonic() - evidence.accepted_at) * 1000, 3),
                 "lock_wait_ms": round((evidence.lock_wait_seconds + (
                     time.monotonic() - evidence.lock_started if evidence.lock_started is not None else 0
                 )) * 1000, 3)}
                for name, evidence in list(self._active_evidence.values())
            ],
            "operations": operations,
        }
