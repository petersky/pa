"""Repository flock observations retain native exclusion and deadline semantics."""
import fcntl
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from pa.config import Settings
from pa.core.operation_budget import current_operation
from pa.core.performance import OperationObservation, current_observation
from pa.repository.workspace import WorkspaceManager


@pytest.fixture
def manager(tmp_path):
    return WorkspaceManager(Settings(data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces"), SimpleNamespace())


def test_numeric_timeline_and_deadline_are_passive(manager, monkeypatch):
    import pa.core.operation_budget as budget
    times = iter([1.0, 1.25, 1.75, 2.0])
    monkeypatch.setattr(budget.time, "monotonic", lambda: next(times))
    observation = OperationObservation()
    deadline = SimpleNamespace(lock_wait=lambda _: pytest.fail("deadline changed"),
                               lock_acquired=lambda _: pytest.fail("deadline changed"))
    token = current_observation.set(observation)
    guard = current_operation.set(deadline)
    try:
        with manager._repository_lock("private-repository-key"):
            pass
    finally:
        current_operation.reset(guard)
        current_observation.reset(token)
    assert observation.file_lock_wait_ms == 250
    assert observation.file_lock_hold_ms == 750
    assert observation.file_lock_release_ms == 250
    assert observation.outer_lock_hold_ms == 750
    assert observation.python_lock_wait_ms == observation.python_lock_hold_ms == 0
    assert observation.lock_depth == 0 and observation.lock_events_total == 1
    event = observation.lock_events[0]
    assert event["name"] == "workspace.repository_file"
    assert event["held_before_release_ms"] == 500
    assert event["started_at_monotonic"] == 1.0
    assert "private-repository-key" not in json.dumps(event)
    assert str(manager.lock_root) not in json.dumps(event)


def test_native_contention_and_exception_release(manager, monkeypatch):
    import pa.repository.workspace as workspace
    original = workspace.fcntl.flock
    attempting = threading.Event()
    entered = threading.Event()
    observation = OperationObservation()

    def flock(fd, mode):
        if threading.current_thread().name.startswith("waiter") and mode == fcntl.LOCK_EX:
            attempting.set()
        return original(fd, mode)

    monkeypatch.setattr(workspace.fcntl, "flock", flock)

    def waiter():
        token = current_observation.set(observation)
        try:
            with manager._repository_lock("shared"):
                entered.set()
                raise ValueError("fixture body failed")
        finally:
            current_observation.reset(token)

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="waiter") as pool:
        with manager._repository_lock("shared"):
            future = pool.submit(waiter)
            assert attempting.wait(5)
            assert not entered.is_set()
        with pytest.raises(ValueError, match="fixture body failed"):
            future.result(timeout=5)
    assert entered.is_set()
    assert observation.file_lock_wait_ms > 0
    assert observation.file_lock_hold_ms >= observation.file_lock_release_ms >= 0
    assert observation.lock_depth == 0 and observation.lock_events_total == 1
    # A different open file description can acquire after the exceptional body.
    with (manager.lock_root / "shared.lock").open("a+") as handle:
        original(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        original(handle.fileno(), fcntl.LOCK_UN)


@pytest.mark.parametrize("failed_mode", [fcntl.LOCK_EX, fcntl.LOCK_UN])
def test_native_lock_errors_propagate_and_handle_closes(manager, monkeypatch, failed_mode):
    import pa.repository.workspace as workspace
    original = workspace.fcntl.flock
    calls = []

    def flock(fd, mode):
        calls.append((fd, mode))
        if mode == failed_mode:
            raise OSError("fixture flock failure")
        return original(fd, mode)

    monkeypatch.setattr(workspace.fcntl, "flock", flock)
    with pytest.raises(OSError, match="fixture flock failure"):
        with manager._repository_lock("error"):
            pass
    assert [mode for _, mode in calls] == ([fcntl.LOCK_EX] if failed_mode == fcntl.LOCK_EX
                                          else [fcntl.LOCK_EX, fcntl.LOCK_UN])
    import os
    with pytest.raises(OSError):
        os.fstat(calls[0][0])
