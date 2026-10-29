"""Private ledger reads; no provider configuration or admission effects."""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from pa.collaboration.models import CommandResult, ExecuteCommandRequest, ModeTransitionRequest, ModeTransitionResult, TransitionStatus
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
        assert service._receipt_guards == {}


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


@pytest.mark.asyncio
@pytest.mark.parametrize('family', ['transition', 'command'])
@pytest.mark.parametrize('conflicting', [False, True])
async def test_matching_first_requests_share_one_durable_effect(env, family, conflicting):
    invoke, method = route(env, family)
    # Use a new, absent key and a stale request: only private durable receipts
    # and a mocked notification are involved, never provider configuration.
    request = env.requests[family].model_copy(update={'idempotency_key':'absent'})
    other = request.model_copy(update=({'purpose':'conflict'} if family == 'transition' else {'arguments':'conflict'}) if conflicting else {})
    env.session.config_json = {}
    if env.service.manager is not None:
        env.service.manager.get = lambda sid: None
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    read = getattr(env.service.store, method)
    reads = []
    def blocked(*args):
        result = read(*args); reads.append(result)
        if len(reads) == 1:
            entered.set()
            try: assert release.wait(5)
            finally: finished.set()
        return result
    with patch.object(env.service.store, method, side_effect=blocked), patch.object(env.service, '_validate_provenance', return_value=(TransitionStatus.STALE,'Private stale request',None)), patch.object(env.service, '_validate_command_provenance', return_value='Private stale request'), patch.object(env.service, '_notify', new_callable=AsyncMock) as notify:
        first = asyncio.create_task(invoke(env.session, request))
        second = None
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            second = asyncio.create_task(invoke(env.session, other))
            # The second caller joins the same key before the first read ends.
            for _ in range(10):
                if next(iter(env.service._receipt_guards.values()))[1] == 2: break
                await asyncio.sleep(0)
            assert next(iter(env.service._receipt_guards.values()))[1] == 2
            assert len(reads) == 1 and reads[0] is None
            release.set()
            result = await asyncio.wait_for(first, 3)
            if conflicting:
                with pytest.raises(IdempotencyConflict): await second
            else:
                duplicate = await asyncio.wait_for(second, 3)
                assert duplicate == result.model_copy(update={'duplicate':True})
            assert len(reads) == 2 and reads[1] is not None
            notify.assert_awaited_once()
        finally:
            release.set(); await asyncio.to_thread(finished.wait, 2)
            for task in (first, second):
                if task is not None and not task.done(): task.cancel()
            await asyncio.gather(*(task for task in (first, second) if task is not None), return_exceptions=True)
    assert env.service._receipt_guards == {}


@pytest.mark.asyncio
@pytest.mark.parametrize('family', ['transition', 'command'])
async def test_cancelled_waiter_releases_key_reference_without_reading(env, family):
    invoke, method = route(env, family)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    read = getattr(env.service.store, method)
    reads = []
    def blocked(*args):
        reads.append(args); entered.set()
        try:
            assert release.wait(5)
            return read(*args)
        finally: finished.set()
    with patch.object(env.service.store, method, side_effect=blocked):
        owner = asyncio.create_task(invoke(env.session, env.requests[family])); waiter = None
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            waiter = asyncio.create_task(invoke(env.session, env.requests[family]))
            for _ in range(10):
                if next(iter(env.service._receipt_guards.values()))[1] == 2: break
                await asyncio.sleep(0)
            assert next(iter(env.service._receipt_guards.values()))[1] == 2
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError): await waiter
            assert next(iter(env.service._receipt_guards.values()))[1] == 1
            assert len(reads) == 1
            release.set(); assert (await owner).duplicate
        finally:
            release.set(); await asyncio.to_thread(finished.wait, 2)
            for task in (owner, waiter):
                if task is not None and not task.done(): task.cancel()
            await asyncio.gather(*(task for task in (owner, waiter) if task is not None), return_exceptions=True)
    assert env.service._receipt_guards == {}


@pytest.mark.asyncio
async def test_receipt_guards_do_not_block_other_keys_sessions_or_families(env):
    async with env.service._receipt_guard('mode', 'session', 'key'):
        for identity in [('mode','session','other'), ('mode','other','key'), ('command','session','key')]:
            async def enter():
                async with env.service._receipt_guard(*identity): return True
            assert await asyncio.wait_for(enter(), 1)
        assert len(env.service._receipt_guards) == 1
    assert env.service._receipt_guards == {}
