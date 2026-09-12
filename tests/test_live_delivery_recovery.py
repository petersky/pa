from __future__ import annotations

import asyncio
import json
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime, BlockingOperationTimeout
from pa.core.operation_budget import OperationBudget, OperationDeadline, measured_lock
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager
from pa.modules.agent_chat import multiplexed_session_events
from tests.test_agent_chat_sse import _FakeRuntime, _FakeStore


def test_virtual_deadlines_separate_queue_progress_lock_idle_and_absolute_cap():
    budget = OperationBudget(queue_seconds=120, idle_seconds=40, lock_seconds=180, absolute_seconds=300)
    clock = OperationDeadline(budget, 0)
    assert clock.expires_at() == 120  # Queue is valid well beyond 30 seconds.
    clock.execution_started(83)
    assert clock.expires_at() == 123
    clock.progress(110)
    assert clock.expires_at() == 150
    clock.progress(149, kind="heartbeat")
    assert clock.expires_at() == 150  # A true idle hang still expires.
    clock.lock_wait(120)
    assert clock.expires_at() == 300
    clock.lock_acquired(250)
    assert clock.lock_wait_seconds == 130
    clock.progress(290)
    assert clock.expires_at() == 300  # Even real progress cannot defeat the cap.


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [True, False])
async def test_late_handoff_commit_schedules_exact_receipt_after_waiter_leaves(tmp_path, cancel):
    store = CardProjection(tmp_path / "pa.db")
    store.save_session(AgentSession(id="exact-session", agent_name="codex"))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    executor = manager.async_runtime = AsyncRuntime(default_timeout=0.04)
    started, release = threading.Event(), threading.Event()
    create = store.create_restart_handoff
    scheduled = []

    def delayed(receipt):
        started.set()
        assert release.wait(2)
        return create(receipt)

    try:
        with patch.object(store, "create_restart_handoff", side_effect=delayed), patch.object(
            manager, "_schedule_restart_handoff", side_effect=scheduled.append
        ):
            waiter = asyncio.create_task(manager.request_restart_handoff(
                session_id="exact-session", continuation_prompt="Continue the exact task.", idempotency_key="stable-key",
            ))
            await asyncio.to_thread(started.wait, 1)
            if cancel:
                waiter.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiter
            else:
                with pytest.raises(BlockingOperationTimeout):
                    await waiter
            assert not scheduled
            release.set()
            for _ in range(100):
                if scheduled:
                    break
                await asyncio.sleep(0.01)
            receipts = store.list_restart_handoffs()
            assert len(receipts) == 1
            assert scheduled == [receipts[0].id]
            assert receipts[0].status == "requested"  # Scheduling is not execution success.
            assert receipts[0].continuation_prompt_id == "restart-handoff:" + receipts[0].id
    finally:
        release.set()
        await executor.close()


@pytest.mark.asyncio
async def test_owned_mutation_completes_postcommit_after_caller_cancel():
    executor = AsyncRuntime()
    release = asyncio.Event()
    completed = []

    async def mutate():
        await release.wait()
        completed.append("commit")
        await asyncio.sleep(0)
        completed.append("published")
        return "authoritative-result"

    first = asyncio.create_task(executor.run_owned("same-key", mutate))
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    retry = asyncio.create_task(executor.run_owned("same-key", mutate))
    release.set()
    assert await retry == "authoritative-result"
    assert completed == ["commit", "published"]
    await executor.close()


@pytest.mark.asyncio
async def test_slow_replay_cannot_stall_other_session_progress_and_final():
    slow, live = _FakeRuntime(), _FakeRuntime()
    slow.session_id, live.session_id = "slow", "live"
    slow._seq, live._seq = 1, 0
    blocked = asyncio.Event()
    original = __import__("pa.modules.agent_chat", fromlist=["_runtime_offload"])._runtime_offload

    async def offload(runtime, *args, **kwargs):
        if runtime is slow:
            await blocked.wait()
        return await original(runtime, *args, **kwargs)

    manager = MagicMock()
    manager.list_runtimes.return_value = [slow, live]
    request = MagicMock()
    request.query_params = {"after": json.dumps({"slow": 0})}
    request.headers = {}
    request.is_disconnected = AsyncMock(return_value=False)
    with patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager), patch(
        "pa.modules.agent_chat._runtime_offload", side_effect=offload
    ):
        response = await multiplexed_session_events(request)
        stream = response.body_iterator
        assert "event: ready" in await anext(stream)
        next_event = asyncio.create_task(anext(stream))
        for _ in range(20):
            if live._subscribers:
                break
            await asyncio.sleep(0)
        for seq, text in enumerate(["Exact progress.", "Exact final."], 1):
            live._subscribers[0].put_nowait({"session_id": "live", "seq": seq,
                "type": "agent_message_chunk", "payload": {"text": text}})
            chunk = await asyncio.wait_for(next_event, 0.5)
            assert text in chunk
            if seq == 1:
                next_event = asyncio.create_task(anext(stream))
        await stream.aclose()
        assert not live._subscribers and not slow._subscribers


@pytest.mark.asyncio
async def test_reconnect_reports_missing_final_when_live_ring_and_disk_lag():
    runtime = _FakeRuntime()
    runtime.session_id, runtime._seq = "late-final", 12
    manager = MagicMock()
    manager.list_runtimes.return_value = [runtime]
    request = MagicMock()
    request.query_params = {"after": json.dumps({runtime.session_id: 10})}
    request.headers = {}
    request.is_disconnected = AsyncMock(return_value=False)
    with patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager):
        response = await multiplexed_session_events(request)
        stream = response.body_iterator
        assert "event: ready" in await anext(stream)
        recovery = await asyncio.wait_for(anext(stream), 0.5)
        assert "event: stream_recovery" in recovery
        assert '"target_seq": 12' in recovery
        assert '"after_seq": 10' in recovery
        assert "id:" not in recovery  # Control traffic cannot advance Last-Event-ID.
        await stream.aclose()
        assert not runtime._subscribers


@pytest.mark.asyncio
async def test_measured_lock_wait_is_distinct_from_executor_queue_and_client_wait():
    executor = AsyncRuntime(operation_budgets={"mutation": {
        "idle_seconds": 0.01, "lock_seconds": 1, "queue_seconds": 1, "absolute_seconds": 2,
    }})
    lock = threading.Lock()
    lock.acquire()

    def mutation():
        with measured_lock(lock):
            return "done"

    task = asyncio.create_task(executor.run_blocking("mutation", mutation))
    try:
        await asyncio.sleep(0.05)
        assert not task.done()
        lock.release()
        assert await task == "done"
        stats = executor.snapshot()["operations"]["mutation"]
        assert stats["total_lock_wait_ms"] >= 30
        assert stats["total_wait_ms"] >= stats["total_lock_wait_ms"]
    finally:
        if lock.locked():
            lock.release()
        await executor.close()


@pytest.mark.asyncio
async def test_watchdog_recovers_receipt_when_postcommit_callback_was_lost(tmp_path):
    from pa.domain.models import RestartHandoff

    store = CardProjection(tmp_path / "pa.db")
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager._startup_complete = True
    receipt = store.create_restart_handoff(RestartHandoff(
        session_id="exact", idempotency_key="same", continuation_prompt="Continue.",
        continuation_prompt_id="immutable-prompt", instance_id="local",
    ))
    manager._execute_restart_handoff = AsyncMock(side_effect=lambda _: asyncio.sleep(1))
    with patch.object(manager, "_schedule_restart_handoff") as schedule:
        await manager._recover_unscheduled_restart_handoffs()
        schedule.assert_called_once_with(receipt.id)
    assert store.get_restart_handoff(receipt.id).continuation_prompt_id == "immutable-prompt"


@pytest.mark.parametrize("state", ["pending", "running", "failed", "cancelled", "succeeded"])
def test_followup_outcome_reads_exact_operation_state_not_dispatch_existence(tmp_path, state):
    from pa.execution.dispatch import DispatchRecord, DispatchStore
    from pa.modules.items import operation_outcome_api

    record = DispatchRecord(
        mutation_id="mutation", authority_instance_id="local", authority_url="http://local",
        target_instance_id="local",
        realm_id="default", dispatch_id="dispatch", session_id="session", card_id="card",
        state="completed", followup_operations={"followup-key": {
            "state": state, "prompt_id": "exact-prompt", "error": {"message": "intake failed"} if state == "failed" else None,
        }},
    )
    store = CardProjection(tmp_path / "pa.db")
    ledger = DispatchStore(tmp_path / "dispatch")
    ledger.put(record)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(
        settings=SimpleNamespace(primary_realm="default"), services={"dispatch_store": ledger},
    ))))
    try:
        with patch("pa.modules.items.get_store", return_value=store):
            result = operation_outcome_api(request, "followup-key", owner="dispatch")
    finally:
        ledger.close()
    assert result["status"] == state
    assert result["result"]["prompt_id"] == "exact-prompt"
    assert result["recovery_state"] != "durable_dispatch_record_found"


def test_agent_page_does_not_hydrate_hidden_sessions_or_cold_bodies(tmp_path):
    from pa.modules.ui_shell import _agent_context

    store = CardProjection(tmp_path / "pa.db")
    settings = Settings(data_dir=tmp_path, agent_enabled=False)
    manager = AgentSessionManager(settings, store)
    manager._startup_complete = True
    for index in range(300):
        store.save_session(AgentSession(
            id=f"session-{index}", agent_name="codex", purpose="chat" if index < 3 else "automated_run",
        ))
    store.append_transcript_events([
        TranscriptEvent(session_id=f"session-{index}", seq=1, event_type="tool_call",
                        payload={"text": "Large tool response " * 5000})
        for index in range(300)
    ])
    ctx = SimpleNamespace(settings=settings, store=store, services={}, require_service=lambda _: manager)
    request = SimpleNamespace(query_params={}, app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)))
    durations = []
    with patch.object(store.transcripts, "_payload", side_effect=AssertionError("cold body hydration")), patch.object(
        store, "transcript_lifecycle_summary", wraps=store.transcript_lifecycle_summary
    ) as summary:
        for _ in range(5):
            started = time.perf_counter()
            result = _agent_context(request)
            durations.append(time.perf_counter() - started)
    assert len(result["sessions"]) == 3
    assert set(result["session_details"]) == {"session-0", "session-1", "session-2"}
    assert summary.call_count == 15
    assert max(durations) < 1.0


@pytest.mark.asyncio
async def test_wait_timeout_is_not_owned_mutation_failure():
    executor = AsyncRuntime()
    release = asyncio.Event()
    result = []

    async def operation():
        await release.wait()
        result.append("committed and published")
        return "done"

    try:
        with pytest.raises(BlockingOperationTimeout):
            await executor.run_owned("exact", operation, wait_timeout=0.01)
        assert executor.snapshot()["owned_mutations"]["pending"] == 1
        retry = asyncio.create_task(executor.run_owned("exact", operation))
        release.set()
        assert await retry == "done"
        assert result == ["committed and published"]
    finally:
        release.set()
        await executor.close()


@pytest.mark.asyncio
async def test_fleet_cache_readers_do_not_wait_for_slow_durable_snapshot(tmp_path):
    from pa.fleet.overview import FleetOverviewCache
    from pa.core.io import atomic_write_json

    cache = FleetOverviewCache(tmp_path)
    started, release = threading.Event(), threading.Event()

    def slow_write(path, payload):
        started.set()
        assert release.wait(2)
        atomic_write_json(path, payload)

    with patch("pa.fleet.overview.atomic_write_json", side_effect=slow_write):
        writer = asyncio.create_task(asyncio.to_thread(
            cache.put, "local", "sync", {"state": "fresh", "value": {"head": "exact"}},
        ))
        try:
            await asyncio.to_thread(started.wait, 1)
            result = await asyncio.wait_for(asyncio.to_thread(cache.get, "local", "sync"), 0.2)
            assert result["value"]["head"] == "exact"
        finally:
            release.set()
            await writer
    assert FleetOverviewCache(tmp_path).get("local", "sync")["value"]["head"] == "exact"


@pytest.mark.asyncio
async def test_transcript_batch_survives_cancellation_during_retry_backoff():
    from pa.instance.agent_session import AgentSessionRuntime

    runtime = object.__new__(AgentSessionRuntime)
    runtime.store = SimpleNamespace(append_transcript_events=lambda batch: None)
    runtime._transcript_queue = asyncio.Queue()
    runtime._transcript_buffer = []
    batch = [TranscriptEvent(session_id="exact", seq=1, event_type="agent_message",
                             payload={"text": "Preserve this final body."})]
    runtime._transcript_queue.put_nowait(batch)
    failed = asyncio.Event()

    async def unavailable(*args, **kwargs):
        failed.set()
        raise OSError("temporary write failure")

    runtime._offload = unavailable
    writer = asyncio.create_task(runtime._write_transcripts())
    await failed.wait()
    writer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await writer
    assert runtime._transcript_buffer == batch
    assert runtime._transcript_buffer[0] is batch[0]
    await asyncio.wait_for(runtime._transcript_queue.join(), 0.2)


@pytest.mark.asyncio
@pytest.mark.parametrize("delayed_stage", ["intake", "transcript"])
async def test_real_prompt_route_finishes_delayed_commit_after_http_cancel(tmp_path, delayed_stage):
    from pa.instance.agent_session import AgentSessionRuntime
    from pa.modules.agent_chat import PromptBody, session_prompt

    store = CardProjection(tmp_path / "pa.db")
    settings = Settings(data_dir=tmp_path, auth_required=False)
    manager = AgentSessionManager(settings, store)
    executor = manager.async_runtime = AsyncRuntime(default_timeout=0.04)
    session = store.save_session(AgentSession(id="chat", agent_name="codex", purpose="chat"))
    runtime = AgentSessionRuntime(manager, session)
    started, release = threading.Event(), threading.Event()

    def ingest(**kwargs):
        if delayed_stage == "intake":
            started.set()
            assert release.wait(2)
        return SimpleNamespace(id="intake", correlation_id="correlation", security=SimpleNamespace(
            disposition=SimpleNamespace(value="accepted")))

    append = store.append_transcript_events

    def append_delayed(batch):
        if delayed_stage == "transcript":
            started.set()
            assert release.wait(2)
        return append(batch)

    store.append_transcript_events = append_delayed
    drain = runtime._drain_transcripts

    async def short_default_drain(**kwargs):
        # Shorten only the old default drain deadline; owned acknowledgements
        # must explicitly retain the write beyond this deadline.
        await drain(timeout=kwargs.get("timeout", 0.01))

    runtime._drain_transcripts = short_default_drain

    async def admit(message, **kwargs):
        runtime._append_transcript("user_message", {"message": message, "id": kwargs["prompt_id"], "images": []})
        return "started"

    runtime.prompt = AsyncMock(side_effect=admit)
    services = {"async_runtime": executor, "intake_service": SimpleNamespace(ingest_web_prompt=ingest)}
    request = SimpleNamespace(headers={}, state=SimpleNamespace(instance_authenticated=False, user=None),
        app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(settings=settings, services=services))))
    body = PromptBody(message="Exact accepted prompt", client_prompt_id="client-stable")
    try:
        with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime), patch(
            "pa.modules.agent_chat.get_principal_id", return_value="user:local"
        ):
            caller = asyncio.create_task(session_prompt(request, session.id, body))
            assert await asyncio.to_thread(started.wait, 1)
            caller.cancel()
            with pytest.raises(asyncio.CancelledError):
                await caller
            # Wait past the executor's old deadline while the actual write owns
            # the request, then reconnect with exactly the same prompt identity.
            await asyncio.sleep(0.06)
            retry = asyncio.create_task(session_prompt(request, session.id, body))
            release.set()
            response = await asyncio.wait_for(retry, 1)
            assert response["accepted"] is True
            assert response["prompt_id"] == "client-stable"
            runtime.prompt.assert_awaited_once()
            assert store.get_prompt_acceptance(session.id, "client-stable").payload["message"] == body.message
    finally:
        release.set()
        await executor.close()


@pytest.mark.asyncio
async def test_ungoverned_followup_response_timeout_remains_recoverable(tmp_path):
    from pa.execution.dispatch import DispatchRecord, DispatchStore
    from pa.modules.fleet import DispatchFollowupBody, prompt_dispatch_session
    from tests.test_dispatch_consistency import request_for

    ledger = DispatchStore(tmp_path)
    record = DispatchRecord(dispatch_id="dispatch", mutation_id="mutation",
        authority_instance_id="authority", authority_url="http://authority",
        target_instance_id="target", session_id="session", state="running")
    ledger.put(record)
    request = request_for(Settings(data_dir=tmp_path, instance_id="authority"),
                          MagicMock(), {"dispatch_store": ledger})
    request.state.instance_authenticated = True
    with patch("pa.modules.fleet._require_dispatch_prompt_protocol", AsyncMock()), patch(
        "pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=TimeoutError("response lost"))
    ):
        with pytest.raises(TimeoutError):
            await prompt_dispatch_session(request, "dispatch", DispatchFollowupBody(
                message="Exact continuation", idempotency_key="same-key"))
    outcome = ledger.get("dispatch").followup_operations["same-key"]
    assert outcome["state"] == "delivery_ambiguous"
    assert outcome["error"]["recoverable"] is True
