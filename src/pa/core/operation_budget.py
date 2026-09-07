"""Monotonic operation deadlines driven by internal work evidence, not heartbeats."""
from __future__ import annotations

import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass


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
def measured_lock(lock):
    deadline = current_operation.get()
    if deadline:
        deadline.lock_wait(time.monotonic())
    lock.acquire()
    if deadline:
        deadline.lock_acquired(time.monotonic())
    try:
        yield
    finally:
        lock.release()
