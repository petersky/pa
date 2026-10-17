"""Runtime telemetry remains available while ordinary worker capacity is busy."""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime, AsyncRuntimeClosed
from pa.modules.instance import InstanceModule, async_runtime_status


def context(tmp_path, services):
    return SimpleNamespace(settings=Settings(data_dir=tmp_path), services=services,
        store=SimpleNamespace(), require_service=services.__getitem__,
        register_service=services.__setitem__)


@pytest.mark.asyncio
async def test_ordinary_worker_saturation_does_not_starve_runtime_http(tmp_path, monkeypatch):
    main = AsyncRuntime(max_workers=1, max_queue=1)
    services = {"async_runtime": main, "writer_lock": object()}
    ctx = context(tmp_path, services)
    maintenance = SimpleNamespace(start=lambda: None, close=AsyncMock())
    monkeypatch.setattr("pa.instance.maintenance.InstanceMaintenanceService", lambda *a, **k: maintenance)
    module = InstanceModule()
    await module.on_startup(None, ctx)
    entered, release = threading.Event(), threading.Event()

    def ordinary_work():
        entered.set()
        assert release.wait(10)
        return "ordinary work retained"

    task = asyncio.create_task(main.run_blocking("fixture.background", ordinary_work))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        app = FastAPI()
        app.state.ctx = ctx
        app.add_api_route("/api/runtime", async_runtime_status, methods=["GET"])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
            # Same five-second deadline as the actual route; the ordinary
            # worker cannot finish until after this response is verified.
            response = await asyncio.wait_for(client.get("/api/runtime"), timeout=5)
        assert response.status_code == 200
        snapshot = response.json()
        assert snapshot["executor"]["active"] == 1
        assert snapshot["diagnostics_executor"]["executor"]["max_workers"] == 1
        assert snapshot["diagnostics_executor"]["executor"]["max_queue"] == ctx.settings.blocking_queue_limit
        assert snapshot["diagnostics_executor"]["operations"]["runtime.status_snapshot"]["submitted"] == 1
        assert not task.done() and not release.is_set()
        assert "runtime.status_snapshot" not in main.snapshot()["operations"]
    finally:
        release.set()
        assert await task == "ordinary work retained"
        await module.on_shutdown(None, ctx)
        await main.close()
    with pytest.raises(AsyncRuntimeClosed):
        await services["runtime_diagnostics"].run_blocking("fixture.after_close", lambda: None)


@pytest.mark.asyncio
async def test_auxiliary_startup_never_creates_diagnostic_workers(tmp_path):
    ctx = context(tmp_path, {})
    await InstanceModule().on_startup(None, ctx)
    assert "runtime_diagnostics" not in ctx.services


@pytest.mark.asyncio
async def test_diagnostic_pool_closes_even_when_maintenance_teardown_fails(tmp_path, monkeypatch):
    ctx = context(tmp_path, {"writer_lock": object()})
    maintenance = SimpleNamespace(start=lambda: None, close=AsyncMock(side_effect=RuntimeError("fixture teardown")))
    monkeypatch.setattr("pa.instance.maintenance.InstanceMaintenanceService", lambda *a, **k: maintenance)
    module = InstanceModule()
    await module.on_startup(None, ctx)
    with pytest.raises(RuntimeError, match="fixture teardown"):
        await module.on_shutdown(None, ctx)
    with pytest.raises(AsyncRuntimeClosed):
        await ctx.services["runtime_diagnostics"].run_blocking("fixture.after_close", lambda: None)
