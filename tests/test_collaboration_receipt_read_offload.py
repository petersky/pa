"""Private ledger reads; no provider configuration or admission effects."""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from pa.collaboration.models import CommandResult, ExecuteCommandRequest, ModeTransitionRequest, ModeTransitionResult
from pa.collaboration.service import CollaborationService
from pa.collaboration.store import IdempotencyConflict
from pa.core.async_runtime import AsyncRuntime


@pytest_asyncio.fixture(params=[True, False], ids=['bounded', 'fallback'])
async def env(tmp_path, request):
    service = CollaborationService(SimpleNamespace(data_dir=tmp_path, instance_id='fixture'), SimpleNamespace())
    executor = AsyncRuntime(max_workers=1)
    if request.param:
        service.bind_runtime(SimpleNamespace(async_runtime=executor))
    session = SimpleNamespace(id='session')
    transition = ModeTransitionRequest(session_id='session', requested_mode='default', purpose='Private receipt', intended_next_action='Observe duplicate', idempotency_key='transition')
    result = ModeTransitionResult(status='approved_applied', requested_mode='default', effective_mode='default', reason='Already applied')
    service.store.save_mode_request(transition, result)
    command = ExecuteCommandRequest(session_id='session', name='pa:status', idempotency_key='command')
    cmd = CommandResult(session_id='session', command_name='pa:status', status='applied', reason='Already applied')
    service.store.save_command_result(command.model_dump(mode='json', exclude={'idempotency_key'}), cmd, idempotency_key='command')
    try:
        yield SimpleNamespace(service=service, session=session, executor=executor, requests={'transition':transition, 'command':command}, results={'transition':result, 'command':cmd})
    finally:
        await executor.close()


def route(env, family):
    if family == 'transition':
        return env.service.request_transition, 'get_mode_request'
    return env.service.execute_command, 'get_command_result'


@pytest.mark.asyncio
@pytest.mark.parametrize('family', ['transition', 'command'])
async def test_receipt_read_keeps_loop_progress_and_exact_duplicate(env, family):
    invoke, method = route(env, family)
    read = getattr(env.service.store, method)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    threads = []
    def blocked(*args):
        threads.append(threading.get_ident()); entered.set()
        try:
            assert release.wait(5)
            return read(*args)
        finally:
            finished.set()
    with patch.object(env.service.store, method, side_effect=blocked), patch.object(env.service, '_notify', new_callable=AsyncMock) as notify:
        task = asyncio.create_task(invoke(env.session, env.requests[family]))
        try:
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
            assert not task.done()  # The original storage read still gates the response.
            assert threads == [threads[0]] and threads[0] != threading.get_ident()
            release.set()
            result = await asyncio.wait_for(task, 3)
            assert result == env.results[family].model_copy(update={'duplicate':True})
            notify.assert_not_awaited()
        finally:
            release.set()
            await asyncio.to_thread(finished.wait, 2)
            if not task.done():
                task.cancel(); await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('family', ['transition', 'command'])
async def test_cancellation_during_read_never_advances_effect(env, family):
    invoke, method = route(env, family)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    read = getattr(env.service.store, method)
    def blocked(*args):
        entered.set()
        try:
            assert release.wait(5)
            return read(*args)
        finally:
            finished.set()
    with patch.object(env.service.store, method, side_effect=blocked), patch.object(env.service, '_notify', new_callable=AsyncMock) as notify, patch.object(env.service.store, 'save_mode_request') as mode_save, patch.object(env.service.store, 'save_command_result') as command_save:
        task = asyncio.create_task(invoke(env.session, env.requests[family]))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            notify.assert_not_awaited(); mode_save.assert_not_called(); command_save.assert_not_called()
        finally:
            release.set(); assert await asyncio.to_thread(finished.wait, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize('family', ['transition', 'command'])
async def test_conflicting_key_preserves_original_receipt(env, family):
    invoke, method = route(env, family)
    request = env.requests[family].model_copy(update={'purpose':'Different purpose'} if family == 'transition' else {'arguments':'different'})
    with pytest.raises(IdempotencyConflict):
        await invoke(env.session, request)
    # An incompatible retry cannot replace the original durable result.
    result = await invoke(env.session, env.requests[family])
    assert result == env.results[family].model_copy(update={'duplicate':True})


@pytest.mark.asyncio
@pytest.mark.parametrize('family', ['transition', 'command'])
async def test_read_error_propagates_before_any_effect(env, family):
    invoke, method = route(env, family)
    with patch.object(env.service.store, method, side_effect=RuntimeError('private receipt read failed')), patch.object(env.service, '_notify', new_callable=AsyncMock) as notify, patch.object(env.service.store, 'save_mode_request') as mode_save, patch.object(env.service.store, 'save_command_result') as command_save:
        with pytest.raises(RuntimeError, match='private receipt read failed'):
            await invoke(env.session, env.requests[family])
        notify.assert_not_awaited(); mode_save.assert_not_called(); command_save.assert_not_called()
