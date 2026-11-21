import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from pa.core.async_runtime import BlockingOperationTimeout
from pa.core.background import BackgroundTaskSupervisor
from pa.fleet.control_plane import build_control_plane_status


@pytest.mark.asyncio
async def test_unexpected_worker_exception_is_restarted(tmp_path: Path) -> None:
    attempts = 0
    recovered = asyncio.Event()

    async def runner(heartbeat) -> None:
        nonlocal attempts
        attempts += 1
        heartbeat()
        if attempts == 1:
            raise RuntimeError("private payload must not be needed to recover")
        recovered.set()
        while True:
            heartbeat()
            await asyncio.sleep(0.01)

    path = tmp_path / "worker.json"
    supervisor = BackgroundTaskSupervisor(
        "test-worker", runner, path, backoff_seconds=0.01, stale_seconds=1
    )
    supervisor.start()
    await asyncio.wait_for(recovered.wait(), 1)
    assert attempts == 2
    assert supervisor.health()["state"] == "ready"
    persisted = json.loads(path.read_text())
    assert persisted["failure_count"] == 1
    assert persisted["last_failure_kind"] == "RuntimeError"
    await supervisor.close()


@pytest.mark.asyncio
async def test_stalled_worker_is_cancelled_and_restarted(tmp_path: Path) -> None:
    attempts = 0
    recovered = asyncio.Event()

    async def runner(heartbeat) -> None:
        nonlocal attempts
        attempts += 1
        heartbeat()
        if attempts == 1:
            await asyncio.Event().wait()
        recovered.set()
        while True:
            heartbeat()
            await asyncio.sleep(0.01)

    supervisor = BackgroundTaskSupervisor(
        "test-worker",
        runner,
        tmp_path / "worker.json",
        backoff_seconds=0.01,
        stale_seconds=0.05,
    )
    supervisor.start()
    await asyncio.wait_for(recovered.wait(), 1)
    assert attempts == 2
    assert supervisor.health()["state"] == "ready"
    await supervisor.close()


@pytest.mark.asyncio
async def test_executor_capacity_timeout_restarts_worker(tmp_path: Path) -> None:
    attempts = 0
    recovered = asyncio.Event()

    async def runner(heartbeat) -> None:
        nonlocal attempts
        attempts += 1
        heartbeat()
        if attempts == 1:
            raise BlockingOperationTimeout("bounded capacity timeout")
        recovered.set()
        while True:
            heartbeat()
            await asyncio.sleep(0.01)

    supervisor = BackgroundTaskSupervisor(
        "capacity-worker",
        runner,
        tmp_path / "worker.json",
        backoff_seconds=0.01,
        stale_seconds=1,
    )
    supervisor.start()
    await asyncio.wait_for(recovered.wait(), 1)
    assert attempts == 2
    assert json.loads((tmp_path / "worker.json").read_text())[
        "last_failure_kind"
    ] == "BlockingOperationTimeout"
    await supervisor.close()


def test_control_plane_health_degrades_for_stale_worker() -> None:
    settings = type(
        "Settings",
        (),
        {
            "pr_supervisor_authority_url": None,
            "fleet_owner_url": None,
            "instance_url": "http://local",
            "fleet_id": "fleet",
            "instance_id": "instance",
        },
    )()
    status = build_control_plane_status(
        settings,
        worker_health={"completion_outbox": {"stale": True, "state": "stale"}},
    )
    assert status["background_workers_state"] == "degraded"
    assert "completion_outbox" in status["warnings"][-1]


@pytest.mark.asyncio
@pytest.mark.parametrize('failures', [1, 100])
async def test_diagnostic_write_failure_does_not_kill_controller_or_worker(tmp_path, failures):
    ready = asyncio.Event()
    attempts = 0
    async def runner(heartbeat):
        nonlocal attempts
        attempts += 1
        heartbeat()
        if attempts == 1:
            raise RuntimeError('PRIVATE_WORKER_DETAIL')
        ready.set()
        while True:
            heartbeat()
            await asyncio.sleep(.01)
    supervisor = BackgroundTaskSupervisor('test', runner, tmp_path/'state.json',
        backoff_seconds=.01, stale_seconds=1)
    original = supervisor._save
    writes = 0
    async def persist():
        nonlocal writes
        writes += 1
        if writes <= failures:
            raise OSError('PRIVATE_STORAGE_DETAIL')
        await original()
    try:
        with patch.object(supervisor, '_save', side_effect=persist):
            supervisor.start()
            await asyncio.wait_for(ready.wait(), 1)
            health = supervisor.health()
            assert health['alive'] and health['supervisor_alive']
            assert health['diagnostics_failure_count'] >= 1
            assert 'PRIVATE' not in json.dumps(health)
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_controller_health_distinguishes_not_started_failed_and_closed(tmp_path):
    supervisor = BackgroundTaskSupervisor('test', AsyncMock(), tmp_path/'state.json')
    assert supervisor.health()['supervisor_state'] == 'not_started'
    async def failed():
        raise RuntimeError('PRIVATE_CONTROLLER_DETAIL')
    supervisor._task = asyncio.create_task(failed())
    await asyncio.gather(supervisor._task, return_exceptions=True)
    health = supervisor.health()
    assert health['supervisor_state'] == 'failed' and not health['supervisor_alive']
    assert health['supervisor_failure_kind'] == 'runtime_error'
    assert 'PRIVATE' not in json.dumps(health)
    await supervisor.close()
    assert supervisor.health()['supervisor_state'] == 'closed'


@pytest.mark.asyncio
async def test_health_and_heartbeat_use_one_clock(tmp_path):
    supervisor = BackgroundTaskSupervisor('test', AsyncMock(), tmp_path/'state.json')
    supervisor._clock = lambda: 100.0
    with patch.object(supervisor, '_save', new_callable=AsyncMock):
        supervisor.heartbeat()
        supervisor._clock = lambda: 103.0
        assert supervisor.health()['progress_age_seconds'] == 3.0
        await supervisor.close()


@pytest.mark.asyncio
async def test_start_does_not_duplicate_an_orphaned_live_worker(tmp_path):
    supervisor = BackgroundTaskSupervisor('test', AsyncMock(), tmp_path/'state.json')
    supervisor._task = asyncio.create_task(asyncio.sleep(0))
    await supervisor._task
    supervisor._worker = asyncio.create_task(asyncio.Event().wait())
    old = supervisor._task
    try:
        supervisor.start()
        assert supervisor._task is old
        assert supervisor.health()['orphaned_worker']
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_diagnostics_cancellation_still_stops_controller(tmp_path):
    supervisor = BackgroundTaskSupervisor('test', AsyncMock(), tmp_path/'state.json')
    with patch.object(supervisor, '_save', side_effect=asyncio.CancelledError):
        supervisor.start()
        await asyncio.gather(supervisor._task, return_exceptions=True)
        assert supervisor._task.cancelled()
        assert supervisor.health()['supervisor_state'] == 'cancelled'
        assert supervisor._worker is None
    await supervisor.close()
