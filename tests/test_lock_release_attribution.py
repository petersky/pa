"""A lock can be available while its releasing caller has not resumed."""
import threading
from types import SimpleNamespace

import pytest

import pa.core.operation_budget as budget
from pa.core.performance import OperationObservation, current_observation


@pytest.mark.parametrize("filesystem", [False, True])
def test_post_unlock_time_is_separate_without_erasing_inclusive_hold(monkeypatch, filesystem):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(budget, "time", SimpleNamespace(monotonic=lambda: clock.now))
    native = threading.Lock()

    class Lock:
        def acquire(self):
            native.acquire()
            clock.now += 1

        def release(self):
            native.release()
            # Another caller can acquire the lock during this interval.
            assert native.acquire(blocking=False)
            native.release()
            clock.now += 3

    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with budget.measured_lock(Lock(), filesystem=filesystem):
            clock.now += 2
    finally:
        current_observation.reset(token)
    prefix = "file" if filesystem else "python"
    assert getattr(observation, prefix + "_lock_wait_ms") == 1000
    assert getattr(observation, prefix + "_lock_hold_ms") == 5000
    assert getattr(observation, prefix + "_lock_release_ms") == 3000
    assert observation.outer_lock_hold_ms == 5000
    assert observation.outer_lock_release_ms == 3000
    event = observation.lock_events[0]
    assert event["hold_ms"] == 5000
    assert event["held_before_release_ms"] == 2000
    assert event["release_and_reschedule_ms"] == 3000
    assert event["release_started_at_monotonic"] == 3
    assert event["finished_at_monotonic"] == 6

    token = current_observation.set(observation)
    try:
        for _ in range(70):
            with budget.measured_lock(Lock(), filesystem=filesystem):
                pass
    finally:
        current_observation.reset(token)
    assert observation.lock_events_total == 71
    assert len(observation.lock_events) == 64
    assert getattr(observation, prefix + "_lock_release_ms") == 213000
    assert observation.outer_lock_release_ms == 213000


def test_no_observation_does_not_add_clock_calls_or_change_release(monkeypatch):
    def denied():
        raise AssertionError("No diagnostic clock needed without an observation")
    monkeypatch.setattr(budget, "time", SimpleNamespace(monotonic=denied))
    lock = threading.Lock()
    with pytest.raises(ValueError), budget.measured_lock(lock):
        raise ValueError("body failure")
    assert lock.acquire(blocking=False)
    lock.release()
