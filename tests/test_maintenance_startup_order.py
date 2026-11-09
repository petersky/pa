"""Automatic maintenance follows startup; manual ownership remains independent."""
import asyncio
import threading
import weakref
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel, reset_kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.instance.maintenance import InstanceMaintenanceService
from pa.modules.instance import InstanceModule


@pytest.mark.asyncio
async def test_wakes_cannot_start_sweep_before_barrier_and_close_does_not_cancel_owner(tmp_path):
    ready = asyncio.get_running_loop().create_future()
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {}, startup_ready=ready)
    with patch.object(service, '_call', new_callable=AsyncMock, return_value={}) as work:
        service.start()
        for _ in range(3):
            service.wake()
            await asyncio.sleep(0)
        assert service.snapshot()['waiting_for_startup']
        work.assert_not_awaited()
        await asyncio.wait_for(service.close(), 1)
        assert not ready.done()  # Closing maintenance cannot cancel shared startup.
        assert not service.snapshot()['waiting_for_startup']
        work.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_manual_sweep_runs_while_startup_pending_without_losing_automatic_sweep(tmp_path):
    ready = asyncio.get_running_loop().create_future()
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {}, startup_ready=ready)
    automatic_ran = asyncio.Event()

    async def work(*args, **kwargs):
        if ready.done():
            automatic_ran.set()
        return {}

    with patch.object(service, '_call', new_callable=AsyncMock, side_effect=work) as calls:
        service.start()
        try:
            await service.run_once()
            assert calls.await_count == 1 and not ready.done()
            ready.set_result(None)
            await asyncio.wait_for(automatic_ran.wait(), 1)
            assert calls.await_count == 2
        finally:
            await service.close()


@pytest.mark.asyncio
async def test_cancelled_startup_never_starts_automatic_sweep(tmp_path):
    ready = asyncio.get_running_loop().create_future()
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {}, startup_ready=ready)
    with patch.object(service, '_call', new_callable=AsyncMock, return_value={}) as calls:
        service.start()
        ready.cancel()
        await asyncio.wait_for(service._task, 1)
        await service.close()
        calls.assert_not_awaited()


@pytest.mark.asyncio
async def test_close_detaches_pending_callback_and_restart_reattaches_it(tmp_path):
    ready = asyncio.get_running_loop().create_future()
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {}, startup_ready=ready)
    ran = asyncio.Event()

    async def work(*args, **kwargs):
        ran.set()
        return {}

    with patch.object(service, '_call', new_callable=AsyncMock, side_effect=work) as calls:
        service.start()
        await service.close()
        calls.assert_not_awaited()
        service.start()
        ready.set_result(None)
        await asyncio.wait_for(ran.wait(), 1)
        assert calls.await_count == 1
        await service.close()

    pending = asyncio.get_running_loop().create_future()
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {}, startup_ready=pending)
    service.start()
    await service.close()
    owner = weakref.ref(service)
    del service
    assert owner() is None  # No collector-policy change or forced collection.
    assert not pending.done()


@pytest.mark.asyncio
@pytest.mark.parametrize('schema_fails', [False, True])
@pytest.mark.parametrize('agent_settlement', ['success', 'error', 'cancelled'])
async def test_kernel_releases_maintenance_only_after_schema_and_agent_settlement(
    tmp_path, schema_fails, agent_settlement,
):
    schema_entered, schema_release = threading.Event(), threading.Event()
    agent_release, maintenance_ran = asyncio.Event(), asyncio.Event()

    async def agent_start(**kwargs):
        await agent_release.wait()
        if agent_settlement == 'error':
            raise ValueError('private recovery failure')

    agent = SimpleNamespace(browser=SimpleNamespace(async_runtime=None), _accepting=True,
                            _quiescing=False, connected=False, start=AsyncMock(side_effect=agent_start),
                            stop=AsyncMock(), list_runtimes=lambda: [], quiesce=AsyncMock())
    kernel = Kernel.boot(settings=Settings(data_dir=tmp_path/'data', workspace_root=tmp_path/'workspaces',
                                         agent_enabled=True), load_modules=False, claim_writer=True)
    kernel.registry.register(InstanceModule())
    app = FastAPI()
    original = app.openapi

    def schema():
        schema_entered.set()
        assert schema_release.wait(5)
        if schema_fails:
            raise ValueError('private schema failure')
        return original()

    async def maintenance(*args, **kwargs):
        maintenance_ran.set()
        return {}

    with patch('pa.instance.agent_session.get_instance_agent', return_value=agent), \
         patch.object(app, 'openapi', schema), \
         patch.object(InstanceMaintenanceService, '_call', new_callable=AsyncMock,
                      side_effect=maintenance) as work:
        startup = asyncio.create_task(kernel.startup(app))
        try:
            assert await asyncio.to_thread(schema_entered.wait, 5)
            service = kernel.ctx.require_service('instance_maintenance')
            service.wake()
            await asyncio.sleep(0)
            work.assert_not_awaited()
            assert service.snapshot()['waiting_for_startup']
            schema_release.set()
            if schema_fails:
                with pytest.raises(ValueError, match='private schema failure'):
                    await startup
                agent_release.set()
                await kernel.ctx.require_service('agent_start_task')
                await asyncio.sleep(0)
                work.assert_not_awaited()
            else:
                await startup
                work.assert_not_awaited()
                assert app.state.ready_openapi_warmed
                if agent_settlement == 'cancelled':
                    kernel.ctx.require_service('agent_start_task').cancel()
                else:
                    agent_release.set()
                await asyncio.wait_for(maintenance_ran.wait(), 1)
                assert work.await_count == 1
                expected = {'success': 'idle', 'error': 'error', 'cancelled': 'cancelled'}
                assert kernel.ctx.require_service('agent_lifecycle')['phase'] == expected[agent_settlement]
        finally:
            schema_release.set()
            agent_release.set()
            await asyncio.gather(startup, return_exceptions=True)
            await kernel.shutdown(app)
            if schema_fails:
                assert kernel.ctx.require_service('maintenance_startup_ready').cancelled()
                work.assert_not_awaited()
            reset_instance_agent()
            reset_store()
            reset_settings()
            reset_kernel()
