"""Server-side prompt queue semantics: Stop holds the queue, no loss, no double delivery."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.instance.quiesce import QueuedPrompt, QuiesceSnapshot


class FakeConnection:
    """Provider stand-in: records deliveries and lets a test hold a turn open."""

    def __init__(self):
        self.connected = True
        self.prompts: list[str] = []
        self.cancels = 0
        self.last_usage = None
        self.last_memory_candidate = False
        self.models = self.modes = self.config_options = self.pa_mcp_health = None
        self.session_cwd = None
        self.hold: asyncio.Event | None = None
        self.entered = asyncio.Event()
        self.raise_on_prompt: BaseException | None = None

    async def prompt(self, text, **kwargs):
        self.prompts.append(text)
        self.entered.set()
        if self.raise_on_prompt is not None:
            raise self.raise_on_prompt
        if self.hold is not None:
            await self.hold.wait()
        return "end_turn"

    async def cancel(self):
        self.cancels += 1
        if self.hold is not None:
            self.hold.set()

    async def disconnect(self):
        self.connected = False


@pytest.fixture
def env(tmp_path):
    store = CardProjection(tmp_path / "queue.db")
    manager = AgentSessionManager(Settings(data_dir=tmp_path, agent_enabled=False), store)
    session = store.save_session(AgentSession(agent_name="codex", purpose="chat"))
    runtime = AgentSessionRuntime(manager, session)
    runtime.connection = FakeConnection()
    manager._runtimes[session.id] = runtime
    return SimpleNamespace(store=store, manager=manager, runtime=runtime, conn=runtime.connection, tmp=tmp_path)


def durable(env):
    return dict((env.store.get_session(env.runtime.session_id).config_json or {})["durable_runtime"])


def events(env, kind):
    env.runtime._flush_transcript()
    return [e for e in env.store.list_transcript_events_before(env.runtime.session_id, limit=500) if e.event_type == kind]


async def settle(runtime):
    task = runtime._drain_task
    if task:
        await asyncio.wait_for(task, 10)




async def wait_for(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.01)


def delivered(env):
    """Marker word of each prompt the provider received, in delivery order."""
    import re

    markers = ("one", "two", "three", "four", "late", "new")
    return [next(m for m in markers if re.search(rf"(?<![A-Za-z]){m}\s*$", p.strip())) for p in env.conn.prompts]


async def hold_first_turn(env):
    env.conn.hold = asyncio.Event()
    env.runtime.enqueue("one", source="ui")
    await asyncio.wait_for(env.conn.entered.wait(), 5)
    assert env.runtime.prompting


# --- 1. Stop holds the queue -------------------------------------------------

@pytest.mark.asyncio
async def test_stop_pauses_queue_end_to_end_and_resume_continues(env):
    from pa.modules.agent_chat import session_cancel

    rt = env.runtime
    await hold_first_turn(env)
    rt.enqueue("two", source="ui")
    rt.enqueue("three", source="ui")
    request = MagicMock()
    with patch("pa.modules.agent_chat._runtime_or_404_async", new=AsyncMock(return_value=rt)):
        response = await session_cancel(request, rt.session_id)
    assert response == {"ok": True, "queue_paused": True}
    await settle(rt)
    assert rt.queue_paused and delivered(env) == ["one"]  # nothing after Stop
    assert [q.message for q in rt._queue] == ["two", "three"]
    assert durable(env)["queue_paused"] is True
    cancelled = events(env, "cancelled")
    assert cancelled and cancelled[-1].payload["pause_queue"] is True
    assert events(env, "queue_paused"), "client needs an explicit queue_paused signal"
    # Resume is the only thing that restarts delivery.
    env.conn.hold = None
    rt.resume_queue()
    await settle(rt)
    assert delivered(env) == ["one", "two", "three"]


@pytest.mark.asyncio
async def test_stop_with_nothing_queued_does_not_strand_the_next_message(env):
    from pa.modules.agent_chat import session_cancel

    rt = env.runtime
    await hold_first_turn(env)
    with patch("pa.modules.agent_chat._runtime_or_404_async", new=AsyncMock(return_value=rt)):
        response = await session_cancel(MagicMock(), rt.session_id)  # nothing to hold
    await settle(rt)
    assert response["queue_paused"] is False and not rt.queue_paused
    env.conn.hold = None
    rt.enqueue("two", source="ui")
    await settle(rt)
    assert delivered(env) == ["one", "two"]


@pytest.mark.asyncio
async def test_internal_interrupt_does_not_pause_queue(env):
    rt = env.runtime
    await hold_first_turn(env)
    rt.enqueue("two", source="ui")
    await rt.cancel(pause_queue=False)
    assert not rt.queue_paused
    assert not events(env, "queue_paused")


@pytest.mark.asyncio
async def test_automation_does_not_resume_a_user_paused_queue(env):
    rt = env.runtime
    await hold_first_turn(env)
    rt.enqueue("late", source="ui")
    await rt.cancel()
    await settle(rt)
    env.conn.hold = None
    before = list(env.conn.prompts)
    rt.enqueue("two", source="pr-supervisor")
    await rt.admit_dispatch_prompt("three", prompt_id="dispatch-1", source="dispatch:x")
    await rt.set_control_mode("human")
    await rt.set_control_mode("automation")
    result = await rt.prompt("four", source="card-reconciliation:x", wait=True)
    await asyncio.sleep(0.1)
    assert env.conn.prompts == before, "automation must not deliver while the user holds the queue"
    assert result == "blocked"
    assert rt.queue_paused
    assert {q.message for q in rt._queue} == {"late", "two", "three", "four"}


# --- 2. Stop in the pre-provider window -------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("window", ["composition", "delivery_guard"])
async def test_stop_before_provider_delivery_keeps_prompt_and_holds_queue(env, window):
    rt = env.runtime
    gate = asyncio.Event()
    reached = asyncio.Event()
    real_offload = rt._offload

    async def slow_supersede(item):
        reached.set()
        await gate.wait()

    async def slow_guard(operation, call, *args, **kwargs):
        if operation == "sqlite.recovery_delivery_guard":
            reached.set()
            await gate.wait()
        return await real_offload(operation, call, *args, **kwargs)

    if window == "composition":
        rt._supersede_obsolete_final_input_fallbacks = slow_supersede
    else:
        rt._offload = slow_guard
    rt.enqueue("one", source="ui")
    rt.enqueue("two", source="ui")
    await asyncio.wait_for(reached.wait(), 5)
    assert rt.prompting and not env.conn.prompts  # the pre-provider window
    await rt.cancel()
    gate.set()
    await settle(rt)
    assert env.conn.prompts == [], "a prompt cancelled before delivery must not be delivered"
    assert [q.message for q in rt._queue] == ["one", "two"]  # kept at head, in order
    assert rt.queue_paused and not rt.prompting
    state = durable(env)
    assert state["in_flight"] is None
    assert [q["message"] for q in state["queued_prompts"]] == ["one", "two"]
    # Resume delivers it exactly once, then the rest.
    rt.resume_queue()
    await settle(rt)
    assert delivered(env) == ["one", "two"]


# --- 3. A completed prompt is never redelivered after a post-turn failure ----

@pytest.mark.asyncio
async def test_post_turn_failure_does_not_requeue_delivered_prompt(env):
    rt = env.runtime
    real = rt._drain_transcripts
    failed = []

    async def flaky(*args, **kwargs):
        if env.conn.prompts and not failed and events(env, "turn_completed"):
            failed.append(True)
            raise OSError("transcript store unavailable after delivery")
        return await real(*args, **kwargs)

    rt._drain_transcripts = flaky
    rt.enqueue("one", source="ui")
    rt.enqueue("two", source="ui")
    await settle(rt)
    assert failed
    assert [q.message for q in rt._queue if q.message == "one"] == [], "delivered prompt was requeued"
    rt.enqueue("late", source="ui")
    await settle(rt)
    assert delivered(env).count("one") == 1
    assert delivered(env) == ["one", "two", "late"]
    assert any("transcript store unavailable" in str(e.payload.get("message")) for e in events(env, "error"))


@pytest.mark.asyncio
async def test_failing_optional_final_input_notification_does_not_fail_the_turn(env):
    rt = env.runtime
    rt._surface_final_input_fallback = AsyncMock(side_effect=OSError("notification store down"))
    rt.enqueue("one", source="ui")
    rt.enqueue("two", source="ui")
    await settle(rt)
    assert delivered(env) == ["one", "two"]
    assert not rt._queue and not events(env, "error")


@pytest.mark.asyncio
async def test_prompt_the_provider_never_received_is_requeued_at_head(env):
    rt = env.runtime

    async def boom(item):
        raise OSError("disk full before delivery")

    rt._supersede_obsolete_final_input_fallbacks = boom
    rt.enqueue("one", source="ui")
    rt.enqueue("two", source="ui")
    await settle(rt)
    assert env.conn.prompts == []
    assert [q.message for q in rt._queue] == ["one", "two"]


# --- 4. _in_flight never leaks ---------------------------------------------

@pytest.mark.asyncio
async def test_pre_provider_failure_clears_in_flight_everywhere(env):
    rt = env.runtime

    async def boom(item):
        raise OSError("cannot supersede")

    rt._supersede_obsolete_final_input_fallbacks = boom
    rt.enqueue("one", source="ui")
    await settle(rt)
    assert rt._in_flight is None and not rt.prompting
    state = durable(env)
    assert state["in_flight"] is None
    assert [q["message"] for q in state["queued_prompts"]] == ["one"]
    ids = [q["id"] for q in state["queued_prompts"]]
    assert ids == [rt._queue[0].id]  # in exactly one place


# --- 5. Connection loss keeps the prompt recoverable exactly once -----------

@pytest.mark.asyncio
async def test_connection_loss_keeps_interrupted_prompt_for_recovery_once(env):
    rt = env.runtime
    env.manager.request_recovery = Mock()
    env.conn.raise_on_prompt = ConnectionError("connection closed")
    item = rt.enqueue("one", source="ui")
    await settle(rt)
    env.manager.request_recovery.assert_called()
    state = durable(env)
    assert state["in_flight"] and state["in_flight"]["id"] == item.id
    assert not state["queued_prompts"]
    snapshot = env.manager._snapshot_from_persisted(env.store.get_session(rt.session_id))
    assert snapshot.in_flight and snapshot.in_flight.id == item.id
    env.manager._prepare_workspace = AsyncMock(return_value={})
    env.manager._runtimes.pop(rt.session_id)
    with patch.object(AgentSessionRuntime, "start", new=AsyncMock()) as start:
        await env.manager._resume_from_snapshot(snapshot, QuiesceSnapshot())
    queued = start.await_args.kwargs["queued_prompts"]
    assert [q.id for q in queued] == [item.id]
    assert queued[0].source == "recovery" and "one" in queued[0].message
    assert queued[0].message.count("PA recovered this queued turn") == 1


# --- 6/7. A new prompt never jumps the queue; starting prompt is visible ------

@pytest.mark.asyncio
async def test_new_prompt_does_not_jump_a_non_empty_queue(env):
    rt = env.runtime
    rt._start_drain = Mock()  # a drain that stopped on a blocked item
    rt.enqueue("one", source="ui")
    result = await rt.prompt("new", source="ui", wait=False, prompt_id="new-id")
    assert result == "queued"
    assert [q.message for q in rt._queue] == ["one", "new"]
    queued = [e for e in events(env, "queue_enqueued") if e.payload["id"] == "new-id"][0].payload
    assert queued["source"] == "ui" and "priority" in queued and "turn_reason" in queued and "supersedes" in queued


@pytest.mark.asyncio
async def test_new_prompt_during_dequeue_window_queues_behind_and_starting_is_visible(env):
    rt = env.runtime
    gate = asyncio.Event()
    reached = asyncio.Event()
    real = rt._refresh_restart_receipt

    async def slow(item):
        if item.message == "one" and not reached.is_set():
            reached.set()
            await gate.wait()
        await real(item)

    rt._refresh_restart_receipt = slow
    rt.enqueue("one", source="ui")
    await asyncio.wait_for(reached.wait(), 5)
    snapshot = rt.snapshot(include_transcript=False)
    assert snapshot["starting"] and snapshot["starting"]["message"] == "one"
    assert snapshot["in_flight"] is not None or snapshot["starting"]
    with pytest.raises(RuntimeError):
        await rt.release_provider(reason="idle")
    result = await rt.prompt("new", source="ui", wait=False, prompt_id="new-id")
    assert result == "queued"
    gate.set()
    await settle(rt)
    assert delivered(env) == ["one", "new"]
    assert rt.snapshot(include_transcript=False)["starting"] is None


# --- 8. Acceptance event is published only after the durable checkpoint -------

@pytest.mark.asyncio
async def test_enqueue_checkpoint_failure_publishes_no_acceptance(env):
    rt = env.runtime
    rt._start_drain = Mock()
    live = rt.subscribe()
    with patch.object(rt, "_checkpoint_runtime", side_effect=OSError("db locked")):
        with pytest.raises(OSError):
            rt.enqueue("one", source="ui", prompt_id="retry-id")
    assert not rt._queue
    assert live.empty(), "queue_enqueued was published for a prompt that is not queued"
    assert not events(env, "queue_enqueued")
    assert env.store.get_prompt_acceptance(rt.session_id, "retry-id") is None
    item = rt.enqueue("one", source="ui", prompt_id="retry-id")  # the retry is a real enqueue
    assert [e.payload["id"] for e in events(env, "queue_enqueued")] == [item.id]


# --- 9. reorder validation and manual order ----------------------------------

def test_reorder_dedupes_and_ignores_unknown_ids(env):
    rt = env.runtime
    rt._start_drain = Mock()
    a, b, c = (rt.enqueue(m, source="ui") for m in ("one", "two", "three"))
    rt.reorder_queue([c.id, c.id, "ghost", a.id, a.id])
    assert [q.id for q in rt._queue] == [c.id, a.id, b.id]
    assert len({q.id for q in rt._queue}) == len(rt._queue) == 3
    assert events(env, "queue_reordered")[-1].payload["ids"] == [c.id, a.id, b.id]


def test_reorder_api_rejects_duplicate_ids():
    from pydantic import ValidationError

    from pa.modules.agent_chat import ReorderBody

    with pytest.raises(ValidationError):
        ReorderBody(prompt_ids=["a", "a"])
    assert ReorderBody(prompt_ids=["a", "b"]).prompt_ids == ["a", "b"]


def test_manual_cross_priority_order_survives_next_enqueue(env):
    rt = env.runtime
    rt._start_drain = Mock()
    auto = rt.enqueue("one", source="pr-supervisor")
    human = rt.enqueue("two", source="ui")
    rt.reorder_queue([auto.id, human.id])
    late = rt.enqueue("three", source="ui")
    assert [q.id for q in rt._queue] == [auto.id, human.id, late.id], "enqueue reshuffled a manual order"
    automatic = rt.enqueue("four", source="pr-supervisor")
    assert [q.id for q in rt._queue] == [auto.id, human.id, late.id, automatic.id]
    payload = events(env, "queue_reordered")[-1].payload
    assert payload["order"] == "manual"


def test_manual_order_survives_restart(env):
    rt = env.runtime
    rt._start_drain = Mock()
    auto = rt.enqueue("one", source="pr-supervisor")
    human = rt.enqueue("two", source="ui")
    rt.reorder_queue([auto.id, human.id])
    snapshot = env.manager._snapshot_from_persisted(env.store.get_session(rt.session_id))
    fresh = AgentSessionRuntime(env.manager, env.store.get_session(rt.session_id))
    fresh._restore_queue(snapshot.queued_prompts)
    assert [q.id for q in fresh._queue] == [auto.id, human.id]


# --- 10. Remove / resume error mapping ----------------------------------------

def test_remove_queued_removes_persists_and_reports(env):
    rt = env.runtime
    rt._start_drain = Mock()
    a, b = rt.enqueue("one", source="ui"), rt.enqueue("two", source="ui")
    assert rt.remove_queued(a.id) is True
    assert rt.remove_queued(a.id) is False
    assert [q.id for q in rt._queue] == [b.id]
    assert [q["id"] for q in durable(env)["queued_prompts"]] == [b.id]
    assert [e.payload["id"] for e in events(env, "queue_removed")] == [a.id]


@pytest.mark.asyncio
async def test_remove_of_started_prompt_is_distinguishable(env):
    from fastapi import HTTPException

    from pa.modules.agent_chat import queue_remove

    rt = env.runtime
    await hold_first_turn(env)
    started = rt._in_flight.id
    with patch("pa.modules.agent_chat._runtime_or_404_async", new=AsyncMock(return_value=rt)):
        with pytest.raises(HTTPException) as started_error:
            await queue_remove(MagicMock(), rt.session_id, started)
        with pytest.raises(HTTPException) as missing_error:
            await queue_remove(MagicMock(), rt.session_id, "never-existed")
    assert started_error.value.status_code == 409
    assert started_error.value.detail["code"] == "already_started"
    assert missing_error.value.status_code == 404
    env.conn.hold.set()
    await settle(rt)
    with patch("pa.modules.agent_chat._runtime_or_404_async", new=AsyncMock(return_value=rt)):
        with pytest.raises(HTTPException) as done_error:
            await queue_remove(MagicMock(), rt.session_id, started)  # already delivered
    assert done_error.value.detail["code"] == "already_started"


@pytest.mark.asyncio
async def test_queue_resume_maps_delivery_fence_to_409(env):
    from fastapi import HTTPException

    from pa.modules.agent_chat import queue_resume

    rt = env.runtime
    with patch("pa.modules.agent_chat._runtime_or_404_async", new=AsyncMock(return_value=rt)), patch.object(
        env.store, "require_session_delivery_allowed", side_effect=ValueError("recovery fenced")
    ):
        with pytest.raises(HTTPException) as error:
            await queue_resume(MagicMock(), rt.session_id)
    assert error.value.status_code == 409
    assert error.value.detail["code"] == "delivery_not_allowed"


# --- 11. Transcript backlog overflow ------------------------------------------

@pytest.mark.asyncio
async def test_transcript_backlog_overflow_persists_pause_and_keeps_popped_item(env):
    rt = env.runtime
    rt._start_drain = Mock()
    item = rt.enqueue("one", source="ui")
    rt._start_drain = AgentSessionRuntime._start_drain.__get__(rt)
    with patch.object(rt, "_flush_transcript"):
        rt._transcript_buffer.extend([MagicMock()] * 4096)
        with pytest.raises(RuntimeError, match="backlog"):
            rt._append_transcript("agent_message", {"text": "x"})
        assert rt.queue_paused
        assert durable(env)["queue_paused"] is True
        assert any(getattr(e, "event_type", "") == "queue_paused" for e in rt._transcript_buffer)
        rt._queue_paused = False
        await rt._drain_queue()  # the dequeue event itself overflows
    assert env.conn.prompts == []
    assert [q.id for q in rt._queue] == [item.id], "popped item was dropped by the overflow"


# --- Resume / reorder while a drain is active ---------------------------------

@pytest.mark.asyncio
async def test_reorder_during_active_drain_controls_delivery_order(env):
    rt = env.runtime
    await hold_first_turn(env)
    a, b, c = (rt.enqueue(m, source="ui") for m in ("two", "three", "four"))
    rt.reorder_queue([c.id, b.id, a.id])
    env.conn.hold.set()
    env.conn.hold = None
    await settle(rt)
    assert delivered(env) == ["one", "four", "three", "two"]


@pytest.mark.asyncio
async def test_pause_then_resume_during_active_turn_continues_once(env):
    rt = env.runtime
    await hold_first_turn(env)
    rt.enqueue("two", source="ui")
    rt.pause_queue()
    rt.resume_queue()  # drain task is still alive inside the first turn
    env.conn.hold.set()
    env.conn.hold = None
    await settle(rt)
    assert delivered(env) == ["one", "two"]
    rt.pause_queue()
    rt.enqueue("three", source="ui")
    await asyncio.sleep(0.05)
    assert delivered(env) == ["one", "two"]
    rt.resume_queue()
    await settle(rt)
    assert delivered(env) == ["one", "two", "three"]
