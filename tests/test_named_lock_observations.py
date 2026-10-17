"""Bounded lock timelines distinguish inclusive nesting from outer wall hold."""
import threading

import pytest

from pa.core.async_runtime import AsyncRuntime
from pa.core.operation_budget import measured_lock
from pa.core.performance import OperationObservation, current_observation


def test_nested_reentrant_holds_are_not_added_to_outer_wall_time(monkeypatch):
    times = iter(range(6))
    monkeypatch.setattr("pa.core.operation_budget.time.monotonic", lambda: next(times))
    observation = OperationObservation()
    token = current_observation.set(observation)
    lock = threading.RLock()
    try:
        with measured_lock(lock, name="projection.mutation"):
            with measured_lock(lock, name="projection.mutation"):
                pass
    finally:
        current_observation.reset(token)
    assert observation.python_lock_wait_ms == 2000
    assert observation.python_lock_hold_ms == 5000
    assert observation.outer_lock_hold_ms == 4000
    assert [event["nesting_depth"] for event in observation.lock_events] == [1, 0]
    assert observation.lock_depth == 0


def test_exception_releases_lock_and_timeline_is_bounded_and_sanitized():
    observation = OperationObservation()
    token = current_observation.set(observation)
    lock = threading.Lock()
    try:
        with pytest.raises(ValueError), measured_lock(lock, name="private path or payload"):
            raise ValueError("fixture")
        assert lock.acquire(blocking=False)
        lock.release()
        for _ in range(70):
            with measured_lock(lock, name="transcript.write"):
                pass
    finally:
        current_observation.reset(token)
    assert observation.lock_events_total == 71
    assert len(observation.lock_events) == 64
    assert observation.lock_depth == 0
    assert "private" not in str(observation.lock_events)


@pytest.mark.asyncio
async def test_runtime_retains_named_timeline_without_exposing_mutable_history():
    runtime = AsyncRuntime(slow_call_seconds=0)
    def work():
        with measured_lock(threading.Lock(), name="projection.mutation"):
            pass
    try:
        await runtime.run_blocking("fixture.named_lock", work)
        await __import__("asyncio").sleep(0)
        snapshot = runtime.snapshot()
        phases = snapshot["slow_lock_timelines"]["observations"][-1]
        assert phases["events"][0]["name"] == "projection.mutation"
        assert phases["lock_events_total"] == 1
        assert snapshot["operations"]["fixture.named_lock"]["outer_lock_hold_ms"]["samples"] == 1
        phases["events"][0]["name"] = "caller mutation"
        assert runtime.snapshot()["slow_lock_timelines"]["observations"][-1]["events"][0]["name"] == "projection.mutation"
    finally:
        await runtime.close()
