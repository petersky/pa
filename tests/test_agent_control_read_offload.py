"""Session-control storage reads must not execute on the event-loop thread."""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from pa.domain.models import AgentSession, RestartHandoff
from pa.modules import agent_chat


@pytest.fixture
def control():
    handoff = RestartHandoff(session_id="s", idempotency_key="fixture", continuation_prompt_id="fixture-continuation")
    runtime = SimpleNamespace(snapshot=lambda: {"session_id": "s", "complete": {"retained": True}})
    manager = SimpleNamespace(
        store=SimpleNamespace(get_session=lambda _: None), get=lambda _: None,
        quiescing=False, startup_complete=True,
        request_restart_handoff=AsyncMock(return_value=handoff),
        edit_restart_handoff=AsyncMock(return_value=handoff),
        retry_restart_handoff=AsyncMock(return_value=handoff),
        recover_session=AsyncMock(return_value=runtime),
    )
    ctx = SimpleNamespace(settings=SimpleNamespace(auth_required=True, instance_id="local"),
                          services={}, require_service=lambda _: manager)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)),
                              state=SimpleNamespace(user=SimpleNamespace(role="user")))
    with patch.object(agent_chat, "get_principal_id", return_value="user:owner"):
        yield manager, request


def invoke(name, request):
    if name == "request":
        return agent_chat.request_restart_handoff(request, "s", agent_chat.RestartHandoffBody(
            continuation_prompt="private fixture", idempotency_key="fixture"))
    if name == "edit":
        return agent_chat.edit_restart_handoff(request, "s", "handoff", agent_chat.RestartHandoffEditBody(continuation_prompt="fixture"))
    if name == "retry":
        return agent_chat.retry_restart_handoff(request, "s", "handoff")
    if name == "recover":
        return agent_chat.recover_session(request, "s")
    if name == "queue":
        return agent_chat.queue_pause(request, "s")
    if name == "events":
        return agent_chat.session_events(request, "s")
    if name == "cancel":
        return agent_chat.session_cancel(request, "s")
    if name == "permission":
        return agent_chat.session_permission(request, "s", "request", agent_chat.PermissionBody(allow=True))
    if name == "model":
        return agent_chat.session_model(request, "s", agent_chat.ModelBody(model_id="fixture"))
    if name == "mode":
        return agent_chat.session_mode(request, "s", agent_chat.ModeBody(mode_id="fixture"))
    if name == "config":
        return agent_chat.session_config(request, "s", agent_chat.ConfigBody(config_id="fixture", value="fixture"))
    if name == "resume":
        return agent_chat.queue_resume(request, "s")
    if name == "reorder":
        return agent_chat.queue_reorder(request, "s", agent_chat.ReorderBody(prompt_ids=[]))
    if name == "remove":
        return agent_chat.queue_remove(request, "s", "prompt")
    return agent_chat.get_session_observability(request, "s")


@pytest.mark.parametrize("name", ["request", "edit", "retry", "recover", "queue", "observability",
                                  "events", "cancel", "permission", "model", "mode", "config", "resume", "reorder", "remove"])
@pytest.mark.asyncio
async def test_missing_session_lookup_runs_on_worker(control, name):
    manager, request = control
    threads = []
    manager.store.get_session = lambda _: threads.append(threading.get_ident()) or None
    loop_thread = threading.get_ident()
    with pytest.raises(HTTPException) as caught:
        await invoke(name, request)
    assert caught.value.status_code == 404
    assert len(threads) == 1 and threads[0] != loop_thread
    for method in [manager.request_restart_handoff, manager.edit_restart_handoff,
                   manager.retry_restart_handoff, manager.recover_session]:
        method.assert_not_awaited()


@pytest.mark.asyncio
async def test_complete_local_observability_is_built_on_worker(control):
    manager, request = control
    manager.store.get_session = lambda _: AgentSession(id="s", agent_name="fixture")
    threads = []
    expected = {"turns": [{"id": "retained"}], "queue": {"count": 2}, "nested": {"proof": [1, 2]}}
    with patch.object(agent_chat, "_observability", side_effect=lambda *a: threads.append(threading.get_ident()) or expected):
        assert await agent_chat.get_session_observability(request, "s") == expected
    assert len(threads) == 1 and threads[0] != threading.get_ident()


@pytest.mark.asyncio
async def test_missing_session_dispatch_lookup_runs_on_worker(control):
    _, request = control
    threads = []
    request.app.state.ctx.services["dispatch_store"] = SimpleNamespace(
        by_session=lambda _: threads.append(threading.get_ident()) or None)
    with pytest.raises(HTTPException) as caught:
        await agent_chat.get_session_observability(request, "s")
    assert caught.value.status_code == 404
    assert len(threads) == 1 and threads[0] != threading.get_ident()


@pytest.mark.parametrize("name", ["request", "edit", "retry", "recover"])
@pytest.mark.parametrize("principal,admin,allowed", [("user:owner", False, True), ("user:other", False, False), ("user:other", True, True)])
@pytest.mark.asyncio
async def test_owner_admin_and_denial_semantics_remain(control, name, principal, admin, allowed):
    manager, request = control
    manager.store.get_session = lambda _: AgentSession(id="s", agent_name="fixture", principal_id=principal)
    request.state.user.role = "admin" if admin else "user"
    if allowed:
        value = await invoke(name, request)
        assert value["session_id"] == "s"
    else:
        with pytest.raises(HTTPException) as caught:
            await invoke(name, request)
        assert caught.value.status_code == 403
        for method in [manager.request_restart_handoff, manager.edit_restart_handoff,
                       manager.retry_restart_handoff, manager.recover_session]:
            method.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_queue_control_keeps_loop_ownership_and_avoids_storage(control):
    manager, request = control
    calls = []
    runtime = SimpleNamespace(_closed=False, pause_queue=lambda: calls.append(threading.get_ident()))
    manager.get = lambda _: runtime
    manager.store.get_session = Mock(side_effect=AssertionError("read durable state over live runtime"))
    assert await agent_chat.queue_pause(request, "s") == {"queue_paused": True}
    assert calls == [threading.get_ident()]
    manager.store.get_session.assert_not_called()


@pytest.mark.parametrize("target,status", [("local", 503), ("remote", 200)])
@pytest.mark.asyncio
async def test_observability_routing_is_unchanged(control, target, status):
    _, request = control
    request.app.state.ctx.services["dispatch_store"] = SimpleNamespace(
        by_session=lambda _: SimpleNamespace(target_instance_id=target, dispatch_id="dispatch"))
    remote = AsyncMock(return_value={"session_id": "s", "complete": [1, 2, 3]})
    with patch("pa.modules.fleet._peer_agent_json", remote):
        if status == 503:
            with pytest.raises(HTTPException) as caught:
                await agent_chat.get_session_observability(request, "s")
            assert caught.value.status_code == status
            assert caught.value.detail["code"] == "target_session_projection_missing"
            remote.assert_not_awaited()
        else:
            value = await agent_chat.get_session_observability(request, "s")
            assert value == {"session_id": "s", "complete": [1, 2, 3], "routing": {
                "source": "durable_dispatch", "dispatch_id": "dispatch", "target_instance_id": "remote"}}


@pytest.mark.asyncio
@pytest.mark.parametrize("session", [None, AgentSession(id="s", agent_name="fixture", status="closed"),
                                    AgentSession(id="s", agent_name="fixture", status="idle", purpose="chat")])
async def test_missing_runtime_response_matches_sync_snapshot_reader(control, session):
    manager, request = control
    manager.store.get_session = lambda _: session
    with pytest.raises(HTTPException) as sync:
        agent_chat._runtime_or_404(request, "s")
    with pytest.raises(HTTPException) as asynchronous:
        await agent_chat._runtime_or_404_async(request, "s")
    assert (asynchronous.value.status_code, asynchronous.value.detail) == (sync.value.status_code, sync.value.detail)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["request", "edit", "retry", "recover"])
async def test_cancelled_authorization_read_never_starts_effect(control, name):
    manager, request = control
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def read(_):
        entered.set()
        try:
            assert release.wait(5)
            return AgentSession(id="s", agent_name="fixture", principal_id="user:owner")
        finally:
            finished.set()

    manager.store.get_session = read
    task = asyncio.create_task(invoke(name, request))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        for method in [manager.request_restart_handoff, manager.edit_restart_handoff,
                       manager.retry_restart_handoff, manager.recover_session]:
            method.assert_not_awaited()
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("draining", [False, True])
async def test_pending_missing_runtime_read_revalidates_publication_and_drain(control, draining):
    manager, request = control
    entered, release = threading.Event(), threading.Event()
    runtimes = {}
    manager.get = runtimes.get
    runtime = SimpleNamespace(_closed=False, pause_queue=Mock())

    def read(_):
        entered.set()
        assert release.wait(5)
        return None

    manager.store.get_session = read
    task = asyncio.create_task(agent_chat.queue_pause(request, "s"))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        runtimes["s"] = runtime
        manager.quiescing = draining
        release.set()
        if draining:
            with pytest.raises(HTTPException) as caught:
                await task
            assert caught.value.status_code == 503
            assert caught.value.detail["code"] == "agent_draining"
            runtime.pause_queue.assert_not_called()
        else:
            assert await task == {"queue_paused": True}
            runtime.pause_queue.assert_called_once_with()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["request", "edit", "retry", "recover"])
async def test_pending_authorization_read_revalidates_drain_before_effect(control, name):
    manager, request = control
    entered, release = threading.Event(), threading.Event()

    def read(_):
        entered.set()
        assert release.wait(5)
        return AgentSession(id="s", agent_name="fixture", principal_id="user:owner")

    manager.store.get_session = read
    task = asyncio.create_task(invoke(name, request))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        manager.quiescing = True
        release.set()
        with pytest.raises(HTTPException) as caught:
            await task
        assert caught.value.status_code == 503
        assert caught.value.detail["code"] == "agent_draining"
        for method in [manager.request_restart_handoff, manager.edit_restart_handoff,
                       manager.retry_restart_handoff, manager.recover_session]:
            method.assert_not_awaited()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
