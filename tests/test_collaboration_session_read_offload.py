"""Private authority-read checks; collaboration effects remain mocked."""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import HTTPException

from pa.core.async_runtime import AsyncRuntime
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.modules import collaboration as module


@pytest_asyncio.fixture
async def env(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    store.save_session(AgentSession(id="session", agent_name="fixture", principal_id="user:fixture"))
    executor = AsyncRuntime(max_workers=1)
    result = SimpleNamespace(model_dump=lambda **_: {"fixture": True})
    service = SimpleNamespace(request_transition=AsyncMock(return_value=result), execute_command=AsyncMock(return_value=result))
    manager = SimpleNamespace(get=lambda _: None, store=store)
    services = {"async_runtime": executor, "instance_agent": manager, "collaboration": service}
    ctx = SimpleNamespace(services=services, require_service=services.__getitem__, settings=SimpleNamespace(auth_required=True))
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)), state=SimpleNamespace())
    with patch.object(module, "require_user", return_value=SimpleNamespace(role="editor")), patch.object(module, "get_principal_id", return_value="user:fixture"):
        try:
            yield SimpleNamespace(store=store, executor=executor, manager=manager, service=service, services=services, request=request)
        finally:
            await executor.close()


async def invoke(env, family):
    if family == "transition":
        body = module.ModeTransitionRequest(requested_mode="default", purpose="Private fixture", intended_next_action="Private fixture", session_id="session", idempotency_key="fixture")
        return await module.request_collaboration_transition(env.request, "session", body)
    return await module.execute_session_command(env.request, "session", module.CommandExecutionBody(name="pa:status", idempotency_key="fixture"))


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["transition", "command"])
@pytest.mark.parametrize("native", [True, False])
async def test_fallback_authority_read_yields_off_loop(env, family, native):
    if not native:
        env.services.pop("async_runtime")
    read = env.store.get_session
    threads = []
    with patch.object(env.store, "get_session", side_effect=lambda sid: threads.append(threading.get_ident()) or read(sid)):
        assert await invoke(env, family) == {"fixture": True}
    assert len(threads) == 1 and threads[0] != threading.get_ident()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["transition", "command"])
async def test_cancelled_read_never_reaches_collaboration_effect(env, family):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    read = env.store.get_session
    def blocked(sid):
        entered.set()
        try:
            assert release.wait(5)
            return read(sid)
        finally:
            finished.set()
    with patch.object(env.store, "get_session", side_effect=blocked):
        task = asyncio.create_task(invoke(env, family))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            assert await asyncio.to_thread(finished.wait, 2)
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    env.service.request_transition.assert_not_awaited()
    env.service.execute_command.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["transition", "command"])
@pytest.mark.parametrize("missing", [True, False])
async def test_missing_or_foreign_session_rejects_before_effect(env, family, missing):
    session = None if missing else AgentSession(id="session", agent_name="fixture", principal_id="user:other")
    with patch.object(env.store, "get_session", return_value=session):
        with pytest.raises(HTTPException) as error:
            await invoke(env, family)
    assert error.value.status_code == (404 if missing else 403)
    env.service.request_transition.assert_not_awaited()
    env.service.execute_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_runtime_keeps_owner_snapshot_and_skips_storage(env):
    live = env.store.get_session("session")
    env.manager.get = lambda _: SimpleNamespace(session=live)
    with patch.object(env.store, "get_session", side_effect=AssertionError("Live snapshot must win")):
        assert await invoke(env, "command") == {"fixture": True}
    assert env.service.execute_command.await_args.args[0] is live


@pytest.mark.asyncio
async def test_next_call_rechecks_persisted_ownership(env):
    assert await invoke(env, "command") == {"fixture": True}
    session = env.store.get_session("session")
    session.principal_id = "user:other"
    env.store.save_session(session)
    with pytest.raises(HTTPException) as error:
        await invoke(env, "command")
    assert error.value.status_code == 403
    assert env.service.execute_command.await_count == 1


@pytest.mark.asyncio
async def test_empty_runtime_snapshot_keeps_original_missing_guard(env):
    env.manager.get = lambda _: SimpleNamespace(session=None)
    with pytest.raises(HTTPException) as error:
        await invoke(env, "command")
    assert error.value.status_code == 404
    env.service.execute_command.assert_not_awaited()
