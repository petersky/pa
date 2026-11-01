"""Bounded numeric diagnostics. Never retain SQL, parameters or payload bodies."""
from __future__ import annotations

import math
import time
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Lock, get_ident

SAMPLE_LIMIT = 256
FILESYSTEM_PHASES = (
    'file_lock_wait_ms',
    'file_lock_hold_ms',
    'file_lock_release_ms',
    'atomic_write_calls',
    'atomic_write_bytes',
    'file_fsync_calls',
    'file_fsync_ms',
    'directory_fsync_calls',
    'directory_fsync_ms',
    'fsync_failures',
    'atomic_replace_ms',
    'object_get_calls',
    'object_get_bytes',
    'object_get_ms',
    'object_put_files',
    'object_put_bytes',
)


@dataclass
class OperationObservation:
    queue_wait_ms: float = 0.0
    python_lock_wait_ms: float = 0.0
    python_lock_hold_ms: float = 0.0
    python_lock_release_ms: float = 0.0
    outer_lock_hold_ms: float = 0.0
    outer_lock_release_ms: float = 0.0
    lock_depth: int = 0
    lock_events_total: int = 0
    lock_events: list[dict] = field(default_factory=list)
    worker_started_at: float = 0.0
    worker_finished_at: float = 0.0
    thread_cpu_ms: float = 0.0
    sqlite_statement_ms: float = 0.0
    sqlite_commit_ms: float = 0.0
    sqlite_statement_thread_cpu_ms: float = 0.0
    sqlite_commit_thread_cpu_ms: float = 0.0
    sqlite_calls: int = 0
    sqlite_rows: int = 0
    sqlite_aggregate_items: int = 0
    sqlite_aggregate_bytes: int = 0
    sqlite_busy_failures: int = 0
    file_lock_wait_ms: float = 0.0
    file_lock_hold_ms: float = 0.0
    file_lock_release_ms: float = 0.0
    atomic_write_calls: int = 0
    atomic_write_bytes: int = 0
    file_fsync_calls: int = 0
    file_fsync_ms: float = 0.0
    directory_fsync_calls: int = 0
    directory_fsync_ms: float = 0.0
    fsync_failures: int = 0
    atomic_replace_ms: float = 0.0
    object_get_calls: int = 0
    object_get_bytes: int = 0
    object_get_ms: float = 0.0
    object_put_files: int = 0
    object_put_bytes: int = 0




current_observation: ContextVar[OperationObservation | None] = ContextVar("pa_performance", default=None)

# Only explicitly instrumented maintenance spans retain these numeric records.
# Nested spans own their calls exclusively; the existing scalar totals remain
# inclusive. No SQL, bindings, row values, identifiers or caller names survive.
SQLITE_CALL_KINDS = frozenset({
    "execute", "executemany", "fetchone", "fetchmany", "fetchall", "__next__", "commit",
})


@dataclass
class _SqliteCallScope:
    bucket: dict
    thread_id: int
    active: bool = True


current_sqlite_call_scope: ContextVar[_SqliteCallScope | None] = ContextVar("pa_sqlite_call_scope", default=None)


def sqlite_call_bucket():
    scope = current_sqlite_call_scope.get()
    return scope.bucket if scope is not None and scope.active and scope.thread_id == get_ident() else None


@contextmanager
def capture_sqlite_calls(bucket: dict):
    scope = _SqliteCallScope(bucket, get_ident())
    token = current_sqlite_call_scope.set(scope)
    try:
        yield
    finally:
        scope.active = False
        current_sqlite_call_scope.reset(token)


def record_sqlite_call(bucket, kind, elapsed, cpu_elapsed, started, finished, error):
    kind = kind if isinstance(kind, str) and kind in SQLITE_CALL_KINDS else "other"
    item = bucket.get(kind)
    if item is None:
        item = bucket[kind] = {
            "calls": 0, "errors": 0, "wall_ms": 0.0,
            "thread_cpu_ms": 0.0, "max_wall_ms": -1.0,
        }
    item["calls"] += 1
    item["errors"] += error
    item["wall_ms"] += elapsed
    item["thread_cpu_ms"] += cpu_elapsed
    if elapsed > item["max_wall_ms"]:
        item.update(
            max_wall_ms=elapsed, max_call_thread_cpu_ms=cpu_elapsed,
            max_started_at_monotonic=started, max_finished_at_monotonic=finished,
        )


# These are inclusive spans inside one maintenance sweep, never user labels.
# Keep only aggregates: no per-session samples, identifiers, SQL or payloads.
RETENTION_PHASES = frozenset({
    "retention_selection", "retention_canonical", "retention_transaction",
    "retention_discovery", "retention_object_collection", "retention_mirror",
})
_retention_phases: ContextVar[dict | None] = ContextVar("pa_retention_phases", default=None)
_RETENTION_COUNTERS = (
    "sqlite_calls", "sqlite_rows", "sqlite_statement_ms", "sqlite_commit_ms",
    "sqlite_statement_thread_cpu_ms", "sqlite_commit_thread_cpu_ms",
    "sqlite_busy_failures", "python_lock_wait_ms", "python_lock_hold_ms",
    "outer_lock_hold_ms",
)


@contextmanager
def capture_retention_phases(phases: dict):
    """Bind only this worker's sweep; reset even on interruption or nesting."""
    token = _retention_phases.set(phases)
    try:
        yield
    finally:
        _retention_phases.reset(token)


@contextmanager
def retention_phase(name: str):
    phases = _retention_phases.get()
    if phases is None:
        yield
        return
    if name not in RETENTION_PHASES:
        raise ValueError("Unknown internal retention phase")
    observation = current_observation.get()
    before = {key: getattr(observation, key) for key in _RETENTION_COUNTERS} if observation else {}
    wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
    result = phases.setdefault(name, {"calls": 0, "errors": 0, "wall_ms": 0.0, "thread_cpu_ms": 0.0})
    errors = 0
    try:
        with capture_sqlite_calls(result.setdefault("sqlite_call_kinds", {})):
            yield
    except BaseException:
        errors = 1
        raise
    finally:
        result["calls"] += 1
        result["errors"] += errors
        result["wall_ms"] += (time.perf_counter_ns() - wall) / 1_000_000
        result["thread_cpu_ms"] += (time.thread_time_ns() - cpu) / 1_000_000
        for key, value in before.items():
            result[key] = result.get(key, 0) + getattr(observation, key) - value


def distribution(values) -> dict:
    ordered = sorted(values)
    return {
        "samples": len(ordered),
        "max": round(ordered[-1], 3) if ordered else None,
        **{f"p{p}": round(ordered[math.ceil(len(ordered) * p / 100) - 1], 3)
           if ordered else None for p in (50, 95, 99)},
    }


class PerformanceMetrics:
    """Lifetime totals plus percentiles over the last 256 observations per phase.

    Names are internal constants, never user-controlled query/route strings.
    A separate lock permits status reads while the storage writer is busy.
    """

    def __init__(self):
        self._lock = Lock()
        self._samples: dict[str, deque] = {}
        self._totals: dict[str, float] = {}
        self._counts: dict[str, int] = {}

    def record(self, name: str, value: float) -> None:
        with self._lock:
            self._samples.setdefault(name, deque(maxlen=SAMPLE_LIMIT)).append(value)
            self._totals[name] = self._totals.get(name, 0) + value
            self._counts[name] = self._counts.get(name, 0) + 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "window": "last_256_observations_per_metric",
                "metrics": {name: {**distribution(values), "count": self._counts[name],
                                   "total": round(self._totals[name], 3)}
                            for name, values in self._samples.items()},
            }
