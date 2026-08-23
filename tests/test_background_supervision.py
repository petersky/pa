import asyncio
import json
from pathlib import Path

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
