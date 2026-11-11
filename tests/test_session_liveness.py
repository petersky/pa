"""Agent turns survive provider errors and hangs so a card can run unattended."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pa.acp.errors import TurnInactivityTimeout
from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance import agent_session as agent_session_module
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from tests.test_agent_queue_server import FakeConnection, delivered, events, settle, wait_for


class ScriptedConnection(FakeConnection):
    """Provider whose Nth attempts fail with scripted errors before succeeding."""

    def __init__(self):
        super().__init__()
        self.script: list[BaseException] = []
        self.last_interruption = None
        self.stop_reason = "end_turn"

    async def prompt(self, text, **kwargs):
        self.prompts.append(text)
        self.entered.set()
        if self.script:
            raise self.script.pop(0)
        if self.hold is not None:
            await self.hold.wait()
        return self.stop_reason


@pytest.fixture
def env(tmp_path):
    settings = Settings(data_dir=tmp_path, agent_enabled=False).model_copy(
        update={
            "agent_retry_base_seconds": 0.05,
            "agent_retry_cap_seconds": 0.2,
            "agent_retry_max_attempts": 3,
            "agent_rate_limit_notify_after_seconds": 0,
        }
    )
    store = CardProjection(tmp_path / "queue.db")
    manager = AgentSessionManager(settings, store)
    session = store.save_session(AgentSession(agent_name="codex", purpose="chat", card_id="card-1"))
    runtime = AgentSessionRuntime(manager, session)
    runtime.connection = ScriptedConnection()
    manager._runtimes[session.id] = runtime
    manager.request_recovery = Mock()
    return SimpleNamespace(
        store=store, manager=manager, runtime=runtime, conn=runtime.connection, tmp=tmp_path
    )


@pytest.fixture
def stalls(monkeypatch):
    """Capture every notify_stall the session layer raises."""
    calls: list[dict] = []

    def fake(target, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(id="n")

    monkeypatch.setattr("pa.notifications.stall.notify_stall", fake)
    return calls


def attach_notifications(env):
    env.manager.notification_service = SimpleNamespace(list_authorized=lambda **kw: [])


# --- provider errors are retried ---------------------------------------------

@pytest.mark.asyncio
async def test_rate_limit_is_retried_with_backoff_and_prompt_is_never_lost(env, stalls):
    rt = env.runtime
    env.conn.script = [RuntimeError("429 Too Many Requests: rate limit exceeded")] * 2
    rt.enqueue("one", source="ui")
    await wait_for(lambda: len(events(env, "turn_completed")) == 1, timeout=10)
    assert delivered(env) == ["one", "one", "one"]
    failed = events(env, "prompt_failed")
    assert [e.payload["attempt"] for e in failed] == [1, 2]
    assert all(e.payload["retryable"] is True for e in failed)
    assert failed[0].payload["failure"]["code"] == "rate_limited"
    assert failed[1].payload["retry_in_seconds"] >= failed[0].payload["retry_in_seconds"]
    assert not rt._queue and rt._retry is None and rt._retry_not_before == 0.0
    assert stalls == [], "a retried rate limit that clears quickly must not alert the owner"


@pytest.mark.asyncio
async def test_backoff_blocks_new_enqueues_from_hammering_the_provider(env, stalls):
    rt = env.runtime
    rt._retry_not_before = 0.0
    env.manager.settings = env.manager.settings.model_copy(update={"agent_retry_base_seconds": 0.5, "agent_retry_cap_seconds": 0.5})
    rt.settings = env.manager.settings
    env.conn.script = [RuntimeError("rate limit exceeded")]
    rt.enqueue("one", source="ui")
    await wait_for(lambda: len(env.conn.prompts) == 1)
    await wait_for(lambda: rt._retry is not None)
    rt.enqueue("two", source="ui")  # must wait for the timer, not retry at once
    await asyncio.sleep(0.2)
    assert len(env.conn.prompts) == 1
    await wait_for(lambda: len(events(env, "turn_completed")) == 2, timeout=10)
    assert delivered(env) == ["one", "one", "two"], "order is preserved: the failed prompt goes first"


@pytest.mark.asyncio
async def test_rate_limits_never_give_up_but_overload_does_after_the_cap(env, stalls):
    attach_notifications(env)
    rt = env.runtime
    env.conn.script = [RuntimeError("429 rate limit")] * 7  # more than max_attempts=3
    rt.enqueue("one", source="ui")
    await wait_for(lambda: len(events(env, "turn_completed")) == 1, timeout=15)
    assert len(env.conn.prompts) == 8
    assert stalls == []

    env.conn.script = [RuntimeError("503 Service Unavailable")] * 5
    rt.enqueue("two", source="ui")
    await wait_for(lambda: len(stalls) == 1, timeout=15)
    await settle(rt)
    assert stalls[0]["kind"] == "prompt_failed"
    assert "3 attempts" in stalls[0]["body"]
    assert len(env.conn.prompts) == 8 + 3, "bounded errors stop at agent_retry_max_attempts"
    gave_up = events(env, "prompt_failed")[-1].payload
    assert gave_up["retryable"] is False and gave_up["gave_up_after_attempts"] == 3


@pytest.mark.asyncio
async def test_long_rate_limit_wait_notifies_once_per_prompt(env, stalls):
    attach_notifications(env)
    env.manager.settings = env.manager.settings.model_copy(update={"agent_rate_limit_notify_after_seconds": 0.1})
    rt = env.runtime
    rt.settings = env.manager.settings
    env.conn.script = [RuntimeError("429 usage limit reached")] * 5
    rt.enqueue("one", source="ui")
    await wait_for(lambda: len(events(env, "turn_completed")) == 1, timeout=15)
    assert len(stalls) == 1
    assert stalls[0]["kind"] == "prompt_failed"
    assert stalls[0]["dedupe_key"] == f"{rt.session_id}:{events(env, 'user_message')[0].payload['id']}"
    assert stalls[0]["severity"] == "warning"


@pytest.mark.asyncio
async def test_terminal_failure_notifies_once_and_is_not_retried(env, stalls):
    attach_notifications(env)
    rt = env.runtime
    env.conn.script = [RuntimeError("401 Unauthorized: invalid api key")]
    rt.enqueue("one", source="ui")
    await settle(rt)
    await asyncio.sleep(0.3)
    assert len(env.conn.prompts) == 1 and rt._retry_task is None
    assert [s["kind"] for s in stalls] == ["prompt_failed"]
    assert stalls[0]["severity"] == "error"
    assert events(env, "prompt_failed")[0].payload.get("retryable") is None


@pytest.mark.asyncio
async def test_stop_during_backoff_holds_and_resume_retries_immediately(env, stalls):
    rt = env.runtime
    env.manager.settings = env.manager.settings.model_copy(update={"agent_retry_base_seconds": 30, "agent_retry_cap_seconds": 30})
    rt.settings = env.manager.settings
    env.conn.script = [RuntimeError("rate limit exceeded")]
    rt.enqueue("one", source="ui")
    await wait_for(lambda: rt._retry is not None)
    await settle(rt)
    assert len(env.conn.prompts) == 1 and [q.message for q in rt._queue] == ["one"]
    rt.resume_queue()  # the operator asks for a retry now
    await wait_for(lambda: len(events(env, "turn_completed")) == 1, timeout=5)
    assert delivered(env) == ["one", "one"]


# --- retryable prompts survive restart ----------------------------------------

@pytest.mark.asyncio
async def test_retryable_failed_prompt_is_kept_across_restart_terminal_is_dropped(env, stalls):
    rt = env.runtime
    env.manager.settings = env.manager.settings.model_copy(update={"agent_retry_base_seconds": 60, "agent_retry_cap_seconds": 60})
    rt.settings = env.manager.settings
    env.conn.script = [RuntimeError("429 rate limit")]
    waiting = rt.enqueue("one", source="ui")
    await wait_for(lambda: rt._retry is not None)
    await settle(rt)
    snapshot = env.manager._snapshot_from_persisted(env.store.get_session(rt.session_id))
    assert [q.id for q in snapshot.queued_prompts] == [waiting.id], "restart must not drop a rate-limited prompt"

    env.conn.script = [RuntimeError("401 Unauthorized")]
    rt._clear_retry()
    rt.resume_queue()
    await settle(rt)
    snapshot = env.manager._snapshot_from_persisted(env.store.get_session(rt.session_id))
    assert snapshot.queued_prompts == [], "a terminally failed prompt is still dropped"


# --- hung turns and connection loss -------------------------------------------

@pytest.mark.asyncio
async def test_hung_turn_kill_requeues_via_recovery_and_raises_hung_turn(env, stalls):
    attach_notifications(env)
    rt = env.runtime
    env.conn.script = [TurnInactivityTimeout(1800, 60)]
    item = rt.enqueue("one", source="ui")
    await settle(rt)
    env.manager.request_recovery.assert_called()
    assert rt._interrupted is item, "the prompt stays recoverable, exactly as for a connection loss"
    lost = events(env, "connection_lost")[0].payload
    assert lost["reason"] == "inactivity_timeout" and lost["queued_prompt_id"] == item.id
    assert [s["kind"] for s in stalls] == ["hung_turn"]
    assert stalls[0]["dedupe_key"] == f"{rt.session_id}:{item.id}"
    assert stalls[0]["subject_ids"]["card_id"] == "card-1"


@pytest.mark.asyncio
async def test_plain_connection_loss_raises_connection_lost_stall(env, stalls):
    attach_notifications(env)
    rt = env.runtime
    env.conn.script = [ConnectionError("connection closed")]
    item = rt.enqueue("one", source="ui")
    await settle(rt)
    assert [s["kind"] for s in stalls] == ["connection_lost"]
    assert stalls[0]["dedupe_key"] == f"{rt.session_id}:{item.id}"
    assert "reason" not in events(env, "connection_lost")[0].payload


@pytest.mark.asyncio
async def test_graceful_inactivity_cancel_is_distinguishable_on_turn_completed(env, stalls):
    attach_notifications(env)
    rt = env.runtime
    env.conn.stop_reason = "cancelled"
    env.conn.last_interruption = {
        "reason": "inactivity_timeout", "idle_seconds": 1805.0, "outcome": "cancelled",
    }
    item = rt.enqueue("one", source="ui")
    await settle(rt)
    completed = events(env, "turn_completed")[0].payload
    assert completed["stop_reason"] == "cancelled"
    assert completed["reason"] == "inactivity_timeout"
    assert completed["queued_prompt_id"] == item.id
    assert [s["kind"] for s in stalls] == ["hung_turn"]


# --- the card completion callback ---------------------------------------------

@pytest.mark.asyncio
async def test_failed_completion_callback_is_retried_before_giving_up(env, monkeypatch):
    monkeypatch.setattr(agent_session_module, "COMPLETION_RETRY_DELAYS", (0.01, 0.01, 0.01))
    calls = []

    async def flaky(session_id, payload):
        calls.append(payload)
        if len(calls) < 3:
            raise OSError("authority outbox unavailable")
        return True

    env.manager.completion_handler = flaky
    env.runtime.enqueue("one", source="ui", card_id="card-1")
    await settle(env.runtime)
    assert len(calls) == 3
    assert {c["queued_prompt_id"] for c in calls} == {calls[0]["queued_prompt_id"]}


@pytest.mark.asyncio
async def test_completion_callback_that_never_recovers_does_not_fail_the_turn(env, monkeypatch):
    monkeypatch.setattr(agent_session_module, "COMPLETION_RETRY_DELAYS", (0.01, 0.01, 0.01))
    handler = AsyncMock(side_effect=OSError("down"))
    env.manager.completion_handler = handler
    env.runtime.enqueue("one", source="ui", card_id="card-1")
    await settle(env.runtime)
    assert handler.await_count == 4
    assert not env.runtime._queue and len(events(env, "turn_completed")) == 1
    assert not events(env, "prompt_failed")


# --- permission handler failure ------------------------------------------------

@pytest.mark.asyncio
async def test_failed_permission_attempt_leaves_no_phantom_pending_request(env):
    rt = env.runtime
    notices = SimpleNamespace(create=Mock(side_effect=TimeoutError("offload timed out")))
    env.manager.notification_service = notices
    env.manager.should_auto_approve_async = AsyncMock(return_value=False)
    request = {"request_id": "r1", "options": [{"optionId": "allow", "kind": "allow_once"}], "tool_call": {"title": "run"}}
    with pytest.raises(TimeoutError):
        await rt._on_permission("ext", request)
    assert rt._pending_permissions == {} and rt._permission_requests == {}
    assert rt._permission_notification_ids == {}


@pytest.mark.asyncio
async def test_unreadable_auto_approve_preference_asks_a_human_instead_of_failing(env):
    rt = env.runtime
    env.manager.should_auto_approve_async = AsyncMock(side_effect=TimeoutError("preference read timed out"))
    env.manager.notification_service = None
    request = {"request_id": "r1", "options": [{"optionId": "allow", "kind": "allow_once"}], "tool_call": {"title": "run"}}
    task = asyncio.ensure_future(rt._on_permission("ext", request))
    await wait_for(lambda: "r1" in rt._pending_permissions)
    assert not task.done(), "waits for a human; neither auto-approved nor denied"
    task.cancel()


@pytest.mark.asyncio
async def test_unavailable_permission_handler_raises_a_stall(env, stalls):
    attach_notifications(env)
    await env.runtime._on_permission_unavailable(
        "ext", {"request_id": "r9", "tool_call": {"title": "git push"}}, TimeoutError("db busy")
    )
    assert stalls[0]["kind"] == "other"
    assert "git push" in stalls[0]["body"]
    assert stalls[0]["dedupe_key"].endswith("permission:r9")
