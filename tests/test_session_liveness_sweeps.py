"""Heartbeat decay, lost-completion sweep and the stall wiring of the session layer."""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from pa.acp.errors import TurnInactivityTimeout
from pa.config import Settings, reset_settings
from pa.core.kernel import reset_kernel
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.notifications import SESSION_ATTENTION_NOTICE_CAPABILITY, NotificationPriority
from pa.domain.store import reset_store
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.progress import PROGRESS_SCHEMA_VERSION, ProgressService
from pa.execution.reconciliation import CompletionReconciler
from pa.instance.agent_session import (
    AgentSessionManager,
    AgentSessionRuntime,
    reset_instance_agent,
)
from tests.test_agent_queue_server import events, settle
from tests.test_completion_reconciliation import FakeOutbox, FakeSupervisor, disposition
from tests.test_session_liveness import ScriptedConnection
from tests.test_telegram_notifications import boot  # noqa: F401


@pytest.fixture(autouse=True)
def _reset():
    reset_kernel(); reset_settings(); reset_store(); reset_instance_agent()
    yield
    reset_instance_agent(); reset_store(); reset_settings(); reset_kernel()


# --- the progress heartbeat only claims "active" while output is flowing --------

def heartbeat_service(quiet, window):
    runtime = SimpleNamespace(
        prompting=True, session_id="s1", provider_quiet_seconds=lambda: quiet
    )
    manager = SimpleNamespace(
        settings=SimpleNamespace(agent_heartbeat_activity_window_seconds=window),
        list_runtimes=lambda: [runtime],
    )
    record = SimpleNamespace(
        progress_protocol_version=PROGRESS_SCHEMA_VERSION, latest_progress=None
    )
    store = MagicMock()
    store.by_session.return_value = record
    service = ProgressService(store, instance_id="i", token="", session_manager=manager)
    service._heartbeat = AsyncMock()
    return service


@pytest.mark.asyncio
async def test_hung_provider_stops_heartbeating_so_freshness_decays():
    service = heartbeat_service(quiet=900.0, window=300.0)
    await service._queue_periodic_heartbeats()
    service._heartbeat.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("quiet,window", [(20.0, 300.0), (900.0, 0.0), (None, 300.0)])
async def test_active_unknown_or_unbounded_providers_keep_heartbeating(quiet, window):
    service = heartbeat_service(quiet=quiet, window=window)
    await service._queue_periodic_heartbeats()
    service._heartbeat.assert_awaited_once()


def test_runtime_reports_silence_and_zero_while_waiting_on_a_human(tmp_path):
    from pa.acp.client import TurnActivity

    manager = AgentSessionManager(Settings(data_dir=tmp_path, agent_enabled=False), MagicMock())
    runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
    runtime.connection = SimpleNamespace(activity=TurnActivity())
    runtime.connection.activity.last -= 500
    assert runtime.provider_quiet_seconds() >= 500
    with runtime.connection.activity.human_wait():
        assert runtime.provider_quiet_seconds() == 0.0
    runtime.connection = SimpleNamespace()
    assert runtime.provider_quiet_seconds() is None


# --- lost completion callback is replayed ---------------------------------------

def sweep_fixture(tmp_path, *, completed_age_s=120):
    ledger = DispatchStore(tmp_path)
    ledger.put(DispatchRecord(
        dispatch_id="dispatch-1", mutation_id="m1", card_id="card-1", project_id="p",
        authority_instance_id="a", authority_url="http://a", target_instance_id="t",
        session_id="session-1", state="running",
        initial_prompt_operation={"prompt_id": "initial", "state": "accepted"},
    ))
    when = datetime.now(UTC) - timedelta(seconds=completed_age_s)
    log = [
        TranscriptEvent(session_id="session-1", seq=1, event_type="user_message", payload={"id": "initial", "source": "dispatch"}),
        TranscriptEvent(session_id="session-1", seq=2, event_type="agent_message_chunk", payload={"text": json.dumps(disposition("done")), "phase": "final_answer", "message_id": "m"}),
        TranscriptEvent(session_id="session-1", seq=3, event_type="turn_completed", payload={"queued_prompt_id": "initial", "stop_reason": "end_turn"}, created_at=when),
    ]
    store = SimpleNamespace(
        get_session=lambda _id: AgentSession(id="session-1", agent_name="codex", status="idle"),
        list_transcript_events_before=lambda _id, limit: log,
        find_prompt_completion=lambda _id, prompt: log[2] if prompt == "initial" else None,
    )
    agent = SimpleNamespace(async_runtime=None, store=store, get=lambda _id: None)
    outbox = FakeOutbox()
    reconciler = CompletionReconciler(
        ledger, agent, outbox, MagicMock(), lambda: FakeSupervisor("ready"),
        sweep_seconds=1, sweep_min_age_seconds=30,
    )
    return ledger, outbox, reconciler


@pytest.mark.asyncio
async def test_sweep_replays_a_completion_the_session_layer_failed_to_deliver(tmp_path):
    ledger, outbox, reconciler = sweep_fixture(tmp_path)
    assert await reconciler._sweep_lost_completions() == 1
    assert len(outbox.payloads) == 1
    session_id, payload = outbox.payloads[0]
    assert session_id == "session-1" and payload["queued_prompt_id"] == "initial"
    assert payload["card_disposition"]["lane"] == "done"


@pytest.mark.asyncio
async def test_sweep_leaves_a_fresh_completion_to_the_normal_callback(tmp_path):
    _ledger, outbox, reconciler = sweep_fixture(tmp_path, completed_age_s=2)
    assert await reconciler._sweep_lost_completions() == 0
    assert outbox.payloads == []


@pytest.mark.asyncio
async def test_sweep_ignores_dispatches_whose_completion_already_arrived(tmp_path):
    ledger, outbox, reconciler = sweep_fixture(tmp_path)
    record = ledger.get("dispatch-1")
    record.completion_payload = {"queued_prompt_id": "initial"}
    ledger.put(record)
    assert await reconciler._sweep_lost_completions() == 0
    assert outbox.payloads == []


@pytest.mark.asyncio
async def test_sweep_is_throttled(tmp_path):
    _ledger, _outbox, reconciler = sweep_fixture(tmp_path)
    await reconciler._sweep_lost_completions()
    reader = Mock(side_effect=AssertionError("swept again immediately"))
    reconciler.dispatch_store = SimpleNamespace(lost_completion_candidates=reader)
    assert await reconciler._sweep_lost_completions() == 0


# --- stall wiring: a hung turn reaches the owner's Telegram chat ----------------

def test_hung_turn_notifies_the_owner_through_the_real_stall_stack(tmp_path):
    kernel, fake = boot(tmp_path)
    manager = AgentSessionManager(kernel.ctx.settings, kernel.ctx.store)
    manager.notification_service = kernel.ctx.services.get("notifications")
    manager.request_recovery = Mock()
    session = kernel.ctx.store.save_session(
        AgentSession(agent_name="codex", purpose="chat", card_id="card-1")
    )

    async def scenario():
        runtime = AgentSessionRuntime(manager, session)
        runtime.connection = ScriptedConnection()
        runtime.connection.script = [TurnInactivityTimeout(1800, 60)]
        manager._runtimes[session.id] = runtime
        runtime.enqueue("one", source="ui")
        await settle(runtime)

    asyncio.run(scenario())
    found = [
        n for n in kernel.ctx.store.list_notifications(realm_id="default", limit=50)
        if n.capability == "stall:hung_turn"
    ]
    assert len(found) == 1
    assert found[0].priority == NotificationPriority.HIGH
    assert found[0].session_id == session.id and found[0].card_id == "card-1"
    assert [a.id for a in found[0].actions] == ["resume", "take_over", "dismiss"]
    kernel.ctx.require_service("telegram_push").drain()
    assert len(fake.sent) == 1


# --- adapters keep their existing notification identity ---------------------------

def test_attention_notice_goes_through_notify_stall_and_keeps_its_identity(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(
        "pa.notifications.stall.notify_stall",
        lambda target, **kwargs: seen.append(kwargs) or SimpleNamespace(id="n"),
    )
    manager = AgentSessionManager(Settings(data_dir=tmp_path, agent_enabled=False), MagicMock())
    manager.notification_service = SimpleNamespace()
    session = AgentSession(id="s1", agent_name="codex", card_id="c1", principal_id="user:local")
    assert manager._create_attention_notice(
        session=session, key="session-recovery-blocked:s1:x", title="t", summary="sum", body="b"
    )
    (call,) = seen
    assert call["kind"] == "recovery_blocked"
    assert call["deduplication_key"] == "session-recovery-blocked:s1:x"
    assert call["capability"] == SESSION_ATTENTION_NOTICE_CAPABILITY
    assert call["priority"] == "high" and call["private_to"] == "user:local"
    assert call["subject_ids"]["session_id"] == "s1"


def test_pr_closed_notification_is_a_dismissable_stall_with_its_old_dedupe_key():
    from pa.pr_supervisor.service import PRSupervisor

    created = []
    service = SimpleNamespace(
        create=lambda data, **kwargs: created.append((data, kwargs)) or data,
    )
    owner = SimpleNamespace(_notifications=service, settings=SimpleNamespace(instance_id="inst"))
    watch = SimpleNamespace(
        id="watch-1", realm_id="default", card_id="card-1", originating_session_id="s1",
        project_id="p", pr_number=17, repository="o/r", pr_url="http://pr",
    )
    PRSupervisor._raise_closed_notification(owner, watch)
    (data, kwargs), = created
    assert data.deduplication_key == "pr-supervisor:watch-1:closed-unmerged"
    assert data.capability == "stall:pr_closed"
    assert [a.id for a in data.actions] == ["dismiss"]
    assert data.watch_id == "watch-1" and data.pr_number == 17 and data.source_url == "http://pr"
    assert kwargs == {"principal_id": "instance:pr-supervisor", "instance_id": "inst"}
