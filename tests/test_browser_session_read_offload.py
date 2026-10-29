"""Browser authority reads must yield before any browser effect."""
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
from pa.modules import browser

OWNER = "12345678-1234-4234-8234-123456789abc"
TARGET = "12345678-1234-4234-8234-123456789abd"


@pytest_asyncio.fixture
async def env(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    for sid in [OWNER, TARGET]:
        store.save_session(AgentSession(id=sid, agent_name="fixture", principal_id="user:fixture"))
    executor = AsyncRuntime(max_workers=1)
    manager = SimpleNamespace(state=AsyncMock(return_value={"state": "fixture"}),
                              share=AsyncMock(return_value={"share": "fixture"}))
    services = {"async_runtime": executor, "browser_sessions": manager}
    ctx = SimpleNamespace(store=store, settings=SimpleNamespace(instance_id="local"),
                          services=services, require_service=services.__getitem__)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)))
    with patch.object(browser, "get_principal_id", return_value="user:fixture"):
        try:
            yield SimpleNamespace(store=store, executor=executor, manager=manager,
                                  services=services, request=request)
        finally:
            await executor.close()


def body(operation):
    return browser.AutomationBody(agent_session_id=OWNER,
                                  authorized_session_id=TARGET if operation == "share" else None)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["state", "share"])
@pytest.mark.parametrize("native", [True, False])
async def test_native_session_reads_run_off_loop(env, operation, native):
    if not native:
        env.services.pop("async_runtime")
    read = env.store.get_session
    threads = []
    with patch.object(env.store, "get_session", side_effect=lambda sid: threads.append(threading.get_ident()) or read(sid)):
        result = await browser.browser_automation(env.request, operation, body(operation))
    assert result == ({"state": "fixture"} if operation == "state" else {"share": "fixture"})
    assert len(threads) == (1 if operation == "state" else 2)
    assert all(t != threading.get_ident() for t in threads)


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_id", [OWNER, TARGET])
async def test_cancelled_authority_read_never_reaches_browser_effect(env, blocked_id):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    read = env.store.get_session

    def blocked(sid):
        if sid != blocked_id:
            return read(sid)
        entered.set()
        try:
            assert release.wait(5)
            return read(sid)
        finally:
            finished.set()

    with patch.object(env.store, "get_session", side_effect=blocked):
        task = asyncio.create_task(browser.browser_automation(env.request, "share", body("share")))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            env.manager.share.assert_not_awaited()
        finally:
            release.set()
            assert await asyncio.to_thread(finished.wait, 2)
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    env.manager.share.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_owner_id_rejects_before_storage_or_effect(env):
    with patch.object(env.store, "get_session") as read:
        with pytest.raises(HTTPException) as error:
            await browser.browser_automation(env.request, "state", browser.AutomationBody(agent_session_id="short"))
    assert error.value.status_code == 422 and error.value.detail["code"] == "ownership_failure"
    read.assert_not_called()
    env.manager.state.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("denied_id", [OWNER, TARGET])
async def test_missing_session_stops_before_browser_effect(env, denied_id):
    read = env.store.get_session
    with patch.object(env.store, "get_session", side_effect=lambda sid: None if sid == denied_id else read(sid)):
        with pytest.raises(HTTPException) as error:
            await browser.browser_automation(env.request, "share", body("share"))
    assert error.value.status_code == 403 and error.value.detail["code"] == "ownership_failure"
    env.manager.share.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign_id", [OWNER, TARGET])
async def test_foreign_session_stops_before_browser_effect(env, foreign_id):
    session = env.store.get_session(foreign_id)
    session.principal_id = "user:other"
    env.store.save_session(session)
    with pytest.raises(HTTPException) as error:
        await browser.browser_automation(env.request, "share", body("share"))
    assert error.value.status_code == 403 and error.value.detail["code"] == "ownership_failure"
    env.manager.share.assert_not_awaited()


@pytest.mark.asyncio
async def test_each_operation_rechecks_native_ownership(env):
    await browser.browser_automation(env.request, "state", body("state"))
    env.manager.state.reset_mock()
    changed = env.store.get_session(OWNER)
    changed.principal_id = "user:other"
    env.store.save_session(changed)
    with pytest.raises(HTTPException) as error:
        await browser.browser_automation(env.request, "state", body("state"))
    assert error.value.status_code == 403
    env.manager.state.assert_not_awaited()
