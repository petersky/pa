"""Git observations are numeric, bounded and passive across native outcomes."""
import json
import asyncio
import subprocess
import threading
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.core.performance import OperationObservation, current_observation
from pa.repository.workspace import WorkspaceManager, WorkspaceProvisioningError
from tests.test_repository_git_environment import isolated_git  # noqa: F401


@pytest.fixture
def manager(tmp_path):
    return WorkspaceManager(Settings(data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces"), SimpleNamespace())


@contextmanager
def observation():
    value = OperationObservation()
    token = current_observation.set(value)
    try:
        yield value
    finally:
        current_observation.reset(token)


def test_existing_positional_observation_fields_keep_their_meaning():
    value = OperationObservation(12.5)
    assert value.queue_wait_ms == 12.5
    assert value.git_subprocess_calls == value.git_subprocess_ms == value.git_fetch_ms == 0


@pytest.mark.parametrize("args,fetch", [
    (("fetch", "private-remote"), True),
    (("-C", "private-tree", "fetch", "private-remote"), True),
    (("-C", "fetch", "status"), False),
    (("clone", "private-remote", "fetch"), False),
])
def test_numeric_fetch_subset_and_unchanged_invocation(manager, args, fetch):
    result = subprocess.CompletedProcess([], 0, "private-output", "private-stderr")

    def run(argv, **kwargs):
        if (argv != ["git", "-c", "credential.interactive=never", *args]
            or kwargs["timeout"] != 7 or kwargs["check"] is not False
            or kwargs["env"].get("GIT_TERMINAL_PROMPT") != "0"
            or kwargs["env"].get("GIT_LFS_SKIP_SMUDGE") != "1"
            or kwargs["env"].get("FIXTURE_EXTRA") != "yes"):
            raise AssertionError("Git invocation contract changed")
        return result

    with observation() as value, patch("pa.repository.workspace.subprocess.run", run):
        with patch("pa.core.performance.time.perf_counter", side_effect=[1.0, 1.125]):
            assert manager._git(*args, timeout=7, env_extra={"FIXTURE_EXTRA": "yes"}) is result
    assert value.git_subprocess_calls == 1
    assert value.git_subprocess_ms == 125
    assert value.git_fetch_ms == (125 if fetch else 0)
    assert "private" not in json.dumps(vars(value))


@pytest.mark.parametrize("check", [False, True])
def test_nonzero_result_preserves_check_semantics(manager, check):
    result = subprocess.CompletedProcess([], 1, "", "fixture Git rejection")
    with observation() as value, patch("pa.repository.workspace.subprocess.run", return_value=result):
        if check:
            with pytest.raises(WorkspaceProvisioningError, match="fixture Git rejection"):
                manager._git("status", check=check)
        else:
            assert manager._git("status", check=check) is result
    assert value.git_subprocess_calls == 1 and value.git_subprocess_ms >= 0
    assert value.git_fetch_ms == 0


@pytest.mark.parametrize("error", [OSError("fixture unavailable"),
    subprocess.TimeoutExpired("fixture", 7), KeyboardInterrupt()])
def test_attempts_count_on_error_and_interruption_without_changing_errors(manager, error):
    expected = WorkspaceProvisioningError if isinstance(error, (OSError, subprocess.TimeoutExpired)) else KeyboardInterrupt
    with observation() as value, patch("pa.repository.workspace.subprocess.run", side_effect=error):
        with pytest.raises(expected):
            manager._git("fetch")
    assert value.git_subprocess_calls == 1
    assert value.git_subprocess_ms == value.git_fetch_ms >= 0


def test_real_local_git_under_native_lock(manager, tmp_path, isolated_git):
    remote, client = tmp_path / "remote.git", tmp_path / "client"
    manager._git("init", "--bare", str(remote))
    manager._git("init", str(client))
    manager._git("-C", str(client), "-c", "user.name=Fixture", "-c",
                 "user.email=fixture@example.test", "commit", "--allow-empty", "-m", "fixture")
    manager._git("-C", str(client), "push", str(remote), "HEAD:refs/heads/fixture-main")
    with observation() as value:
        with manager._repository_lock("private-key"):
            manager._git("-C", str(client), "fetch", str(remote), "refs/heads/fixture-main")
            manager._git("-C", str(client), "rev-parse", "--git-dir")
    assert value.git_subprocess_calls == 2
    assert 0 < value.git_fetch_ms <= value.git_subprocess_ms <= value.file_lock_hold_ms
    assert value.lock_events[0]["name"] == "workspace.repository_file"
    assert "private-key" not in json.dumps(vars(value))


@pytest.mark.asyncio
async def test_worker_distributions_totals_and_slow_records_are_bounded(manager):
    runtime = AsyncRuntime(slow_call_seconds=0)
    try:
        with patch("pa.repository.workspace.subprocess.run", return_value=subprocess.CompletedProcess([], 0, "secret-fixture-output", "")):
            for _ in range(260):
                await runtime.run_blocking("fixture.git", manager._git, "fetch", "private-remote")
        snapshot = runtime.snapshot()
        value = snapshot["operations"]["fixture.git"]
        assert value["git_totals"]["git_subprocess_calls"] == 260
        assert value["git_subprocess_calls"]["samples"] == 256
        assert value["git_subprocess_calls"]["p99"] == 1
        assert value["git_fetch_ms"]["max"] >= 0
        assert snapshot["slow_operations"]["observations"][-1]["git_subprocess_calls"] == 1
        assert "private-remote" not in json.dumps(snapshot)
        assert "secret-fixture-output" not in json.dumps(snapshot)
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_cancelled_caller_keeps_git_worker_charged_until_completion(manager):
    entered, release = threading.Event(), threading.Event()
    runtime = AsyncRuntime()

    def run(*args, **kwargs):
        entered.set()
        if not release.wait(5):
            raise AssertionError("fixture release not delivered")
        return subprocess.CompletedProcess([], 0, "", "")

    try:
        with patch("pa.repository.workspace.subprocess.run", run):
            task = asyncio.create_task(runtime.run_blocking("fixture.git_cancel", manager._git, "fetch"))
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert runtime.snapshot()["executor"]["active"] == 1
            release.set()
            await runtime.close()
        value = runtime.snapshot()["operations"]["fixture.git_cancel"]
        assert value["active"] == 0
        assert value["git_totals"]["git_subprocess_calls"] == 1
        assert value["git_subprocess_ms"]["samples"] == 1
    finally:
        release.set()
        await runtime.close()
