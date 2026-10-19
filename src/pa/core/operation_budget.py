"""Monotonic operation deadlines driven by internal work evidence, not heartbeats."""
from __future__ import annotations

import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from pa.core.performance import current_observation


@dataclass(frozen=True)
class OperationBudget:
    queue_seconds: float = 120.0
    idle_seconds: float = 120.0
    lock_seconds: float = 300.0
    absolute_seconds: float = 600.0

    def __post_init__(self):
        if any(not 0 < value <= 3600 for value in (
            self.queue_seconds, self.idle_seconds, self.lock_seconds, self.absolute_seconds
        )):
            raise ValueError("Operation budgets must be positive and at most 3600 seconds")


def default_operation_budgets() -> dict[str, dict[str, float]]:
    return {
        name: {"queue_seconds": 120, "idle_seconds": 120,
               "lock_seconds": 300, "absolute_seconds": 600}
        for name in ("intake.web_prompt", "sqlite.restart_handoff_create",
                     "sqlite.transcript_append", "agent.quiesce_snapshot_write")
    }


def validate_operation_budgets(value: dict) -> dict:
    try:
        return {name: asdict(OperationBudget(**budget)) for name, budget in value.items()}
    except TypeError as exc:
        raise ValueError("Unknown operation budget field") from exc


class OperationDeadline:
    def __init__(self, budget: OperationBudget, now: float):
        self.budget = budget
        self.accepted_at = now
        self.last_progress = now
        self.phase = "queued"
        self.lock_started = None
        self.lock_wait_seconds = 0.0

    def execution_started(self, now: float):
        self.phase = "executing"
        self.last_progress = now

    def progress(self, now: float, *, kind: str = "work"):
        if kind == "work":
            self.last_progress = now
        # Transport heartbeats and arbitrary activity are not work evidence.

    def lock_wait(self, now: float):
        if self.lock_started is None:
            self.lock_started = now
        self.phase = "waiting_for_lock"

    def lock_acquired(self, now: float):
        if self.lock_started is not None:
            self.lock_wait_seconds += now - self.lock_started
        self.lock_started = None
        self.phase = "executing"
        self.progress(now)

    def expires_at(self):
        if self.phase == "queued":
            stage = self.accepted_at + self.budget.queue_seconds
        elif self.phase == "waiting_for_lock":
            stage = self.lock_started + self.budget.lock_seconds
        else:
            stage = self.last_progress + self.budget.idle_seconds
        return min(stage, self.accepted_at + self.budget.absolute_seconds)


current_operation: ContextVar[OperationDeadline | None] = ContextVar("pa_operation", default=None)


def report_work_progress():
    deadline = current_operation.get()
    if deadline:
        deadline.progress(time.monotonic())


@contextmanager
def measured_lock(lock, *, name: str = "unnamed", filesystem: bool = False, track_deadline: bool = True):
    # Previously raw locks can opt into metrics without changing which existing
    # idle/lock budget applies. Existing callers keep their deadline contract.
    deadline = current_operation.get() if track_deadline else None
    observation = current_observation.get()
    started = time.monotonic() if observation else 0
    if deadline:
        deadline.lock_wait(time.monotonic())
    lock.acquire()
    if deadline:
        deadline.lock_acquired(time.monotonic())
    acquired = time.monotonic() if observation else 0
    if observation:
        if filesystem:
            observation.file_lock_wait_ms += (acquired - started) * 1000
        else:
            observation.python_lock_wait_ms += (acquired - started) * 1000
        depth = observation.lock_depth
        observation.lock_depth += 1
    try:
        yield
    finally:
        release_started = time.monotonic() if observation else 0
        lock.release()
        if observation:
            finished = time.monotonic()
            # Preserve the existing inclusive hold totals. This new interval
            # separates unlock work and scheduling around it; it is not proof
            # that the lock remained held until the caller resumed.
            release_ms = (finished - release_started) * 1000
            hold_ms = (finished - acquired) * 1000
            if filesystem:
                observation.file_lock_hold_ms += hold_ms
                observation.file_lock_release_ms += release_ms
            else:
                observation.python_lock_hold_ms += hold_ms
                observation.python_lock_release_ms += release_ms
            observation.lock_depth -= 1
            if depth == 0:
                observation.outer_lock_hold_ms += hold_ms
                observation.outer_lock_release_ms += release_ms
            observation.lock_events_total += 1
            observation.lock_events.append({
                # Only fixed subsystem labels may enter diagnostics, never
                # caller-controlled identifiers, paths or lock representations.
                "name": name if name in {"projection.mutation", "transcript.write", "sync.refs", "sync.refs_file"} else "unnamed",
                "started_at_monotonic": round(started, 6),
                "acquired_at_monotonic": round(acquired, 6),
                "release_started_at_monotonic": round(release_started, 6),
                "finished_at_monotonic": round(finished, 6),
                "wait_ms": round((acquired - started) * 1000, 3),
                "held_before_release_ms": round((release_started - acquired) * 1000, 3),
                "release_and_reschedule_ms": round(release_ms, 3),
                "hold_ms": round(hold_ms, 3), "nesting_depth": depth,
            })
            del observation.lock_events[:-64]
