"""Bounded numeric diagnostics. Never retain SQL, parameters or payload bodies."""
from __future__ import annotations

import math
from collections import deque
from contextvars import ContextVar
from dataclasses import dataclass
from threading import Lock

SAMPLE_LIMIT = 256


@dataclass
class OperationObservation:
    python_lock_wait_ms: float = 0.0
    python_lock_hold_ms: float = 0.0
    worker_started_at: float = 0.0
    worker_finished_at: float = 0.0
    thread_cpu_ms: float = 0.0
    sqlite_statement_ms: float = 0.0
    sqlite_commit_ms: float = 0.0
    sqlite_calls: int = 0
    sqlite_busy_failures: int = 0


current_observation: ContextVar[OperationObservation | None] = ContextVar("pa_performance", default=None)


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
