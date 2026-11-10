"""A turn may run as long as it needs; only INACTIVITY is bounded."""
from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pa.acp import client as client_mod
from pa.acp.client import AgentConnection, PAClient
from pa.acp.errors import ACPConnectTimeout, TurnInactivityTimeout, classify_acp_failure
from pa.acp.providers.base import AgentProviderSpec
from pa.config import Settings
from pa.domain.models import AgentSession


def make_connection(tmp_path: Path, **settings) -> AgentConnection:
    store = MagicMock()
    connection = AgentConnection(Settings(data_dir=tmp_path, **settings), store, agent_name="codex")
    connection.session = AgentSession(
        id="session-1", agent_name="codex", external_session_id="provider-session"
    )
    connection._client = PAClient(
        store,
        activity=connection.activity,
        on_permission=None,
    )
    connection._conn = MagicMock()
    connection._conn._conn = SimpleNamespace(_closed=False, _disconnected=False)
    connection._proc = MagicMock()
    connection.proc = connection._proc
    connection._ctx = MagicMock()
    connection._ctx.__aexit__ = AsyncMock()
    return connection


def message_chunk() -> dict:
    return {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "x"}}


@pytest.mark.asyncio
async def test_hung_turn_is_cancelled_then_killed_and_reads_as_connection_loss(tmp_path):
    c = make_connection(
        tmp_path,
        agent_turn_inactivity_timeout_seconds=0.3,
        agent_turn_cancel_grace_seconds=0.2,
    )
    cancels = []

    async def never(**kwargs):
        await asyncio.Event().wait()

    async def ignore_cancel(**kwargs):
        cancels.append(kwargs)

    c._conn.prompt = never
    c._conn.cancel = ignore_cancel
    with pytest.raises(TurnInactivityTimeout) as raised:
        await asyncio.wait_for(c.prompt("work"), 10)
    assert isinstance(raised.value, ConnectionError)  # takes the recovery path
    assert cancels, "session/cancel is sent before the provider is killed"
    c.proc.kill.assert_called()
    assert c.session.status == "disconnected"
    assert c.last_interruption["reason"] == "inactivity_timeout"
    assert c.last_interruption["outcome"] == "killed"


@pytest.mark.asyncio
async def test_cancel_honoured_inside_grace_returns_cancelled_with_reason(tmp_path):
    c = make_connection(
        tmp_path,
        agent_turn_inactivity_timeout_seconds=0.2,
        agent_turn_cancel_grace_seconds=5,
    )
    done = asyncio.Event()

    async def prompt(**kwargs):
        await done.wait()
        return SimpleNamespace(stop_reason="cancelled", usage=None)

    async def cancel(**kwargs):
        done.set()

    c._conn.prompt = prompt
    c._conn.cancel = cancel
    assert await asyncio.wait_for(c.prompt("work"), 10) == "cancelled"
    assert c.last_interruption["reason"] == "inactivity_timeout"
    assert c.last_interruption["outcome"] == "cancelled"
    c.proc.kill.assert_not_called()


@pytest.mark.asyncio
async def test_real_output_keeps_a_long_turn_alive_beyond_the_deadline(tmp_path):
    c = make_connection(tmp_path, agent_turn_inactivity_timeout_seconds=0.4)

    async def prompt(**kwargs):
        for _ in range(12):  # 1.2 s total, 3x the inactivity deadline
            await asyncio.sleep(0.1)
            await c._client.session_update("provider-session", message_chunk())
        return SimpleNamespace(stop_reason="end_turn", usage=None)

    c._conn.prompt = prompt
    assert await asyncio.wait_for(c.prompt("work"), 10) == "end_turn"
    assert c.last_interruption is None


@pytest.mark.asyncio
async def test_bookkeeping_updates_do_not_keep_a_hung_turn_alive(tmp_path):
    c = make_connection(
        tmp_path,
        agent_turn_inactivity_timeout_seconds=0.3,
        agent_turn_cancel_grace_seconds=0.1,
    )

    async def prompt(**kwargs):
        while True:
            await asyncio.sleep(0.05)
            await c._client.session_update(
                "provider-session",
                {"sessionUpdate": "available_commands_update", "availableCommands": []},
            )

    c._conn.prompt = prompt
    c._conn.cancel = AsyncMock()
    with pytest.raises(TurnInactivityTimeout):
        await asyncio.wait_for(c.prompt("work"), 10)


@pytest.mark.asyncio
async def test_waiting_for_a_human_permission_is_not_inactivity(tmp_path):
    c = make_connection(
        tmp_path,
        agent_turn_inactivity_timeout_seconds=0.3,
        agent_turn_cancel_grace_seconds=0.1,
    )

    async def human(session_id, request):
        await asyncio.sleep(1.0)  # 3x the deadline, waiting on a person
        return {"outcome": {"outcome": "selected", "optionId": "allow"}}

    c._client.on_permission = human

    async def prompt(**kwargs):
        response = await c._client.request_permission(
            [{"optionId": "allow", "kind": "allow_once", "name": "Allow"}],
            "provider-session",
            {"toolCallId": "t1"},
        )
        assert response.outcome.outcome == "selected"
        return SimpleNamespace(stop_reason="end_turn", usage=None)

    c._conn.prompt = prompt
    assert await asyncio.wait_for(c.prompt("work"), 10) == "end_turn"
    assert c.last_interruption is None


@pytest.mark.asyncio
async def test_zero_disables_the_inactivity_deadline(tmp_path):
    c = make_connection(tmp_path, agent_turn_inactivity_timeout_seconds=0)

    async def prompt(**kwargs):
        await asyncio.sleep(0.4)
        return SimpleNamespace(stop_reason="end_turn", usage=None)

    c._conn.prompt = prompt
    assert await c.prompt("work") == "end_turn"


@pytest.mark.asyncio
async def test_failed_prompt_on_live_transport_persists_idle_not_prompting(tmp_path):
    c = make_connection(tmp_path)
    saved = []
    c.store.save_session = lambda session: saved.append(session.status)
    c._conn.prompt = AsyncMock(side_effect=RuntimeError("429 usage limit reached"))
    with pytest.raises(RuntimeError):
        await c.prompt("work")
    assert c.session.status == "idle"
    assert saved[-1] == "idle", saved


def spec() -> AgentProviderSpec:
    return AgentProviderSpec(id="generic", display_name="Generic", command="agent")


def connect_double(**steps):
    acp = MagicMock()
    acp.initialize = AsyncMock(return_value={"agentCapabilities": {}})
    acp.new_session = AsyncMock(return_value=SimpleNamespace(session_id="p"))
    for name, value in steps.items():
        setattr(acp, name, value)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=(acp, MagicMock()))
    context.__aexit__ = AsyncMock()
    return acp, context


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["initialize", "new_session"])
async def test_connect_steps_time_out_into_a_classified_retryable_failure(tmp_path, stage):
    async def hang(*args, **kwargs):
        await asyncio.Event().wait()

    acp, context = connect_double(**{stage: hang})
    c = AgentConnection(
        Settings(data_dir=tmp_path, agent_enabled=True, agent_connect_timeout_seconds=0.2),
        MagicMock(),
        provider_spec=spec(),
    )
    with (
        patch("pa.acp.client.spawn_agent", return_value=context),
        patch("pa.acp.client.pa_mcp_servers", return_value=[]),
    ):
        with pytest.raises(ACPConnectTimeout) as raised:
            await asyncio.wait_for(c.connect(existing_session=AgentSession(id="s", agent_name="generic")), 10)
    detail = classify_acp_failure(raised.value, stage="session_recovery")
    assert detail["code"] == "connect_timeout"
    assert detail["recoverable"] is True and detail["retryable"] is True
    context.__aexit__.assert_awaited()  # the wedged provider was stopped


@pytest.mark.asyncio
async def test_connect_timeout_zero_leaves_connect_unbounded(tmp_path):
    async def slow_init(*args, **kwargs):
        await asyncio.sleep(0.3)
        return {"agentCapabilities": {}}

    acp, context = connect_double(initialize=slow_init)
    c = AgentConnection(
        Settings(data_dir=tmp_path, agent_enabled=True, agent_connect_timeout_seconds=0),
        MagicMock(),
        provider_spec=spec(),
    )
    with (
        patch("pa.acp.client.spawn_agent", return_value=context),
        patch("pa.acp.client.pa_mcp_servers", return_value=[]),
    ):
        session = await c.connect(existing_session=AgentSession(id="s", agent_name="generic"))
    assert session.external_session_id == "p"


# --- permission handler failure is never a silent deny -----------------------

@pytest.fixture
def fast_permission_retries(monkeypatch):
    monkeypatch.setattr(client_mod, "_PERMISSION_RETRY_DELAYS", (0.01, 0.01, 0.01))
    monkeypatch.setattr(client_mod, "_PERMISSION_UNAVAILABLE_POLL_SECONDS", 0.05)


OPTIONS = [{"optionId": "allow", "kind": "allow_once", "name": "Allow"}]


@pytest.mark.asyncio
async def test_transient_permission_handler_failure_is_retried_not_denied(fast_permission_retries):
    calls = []

    async def flaky(session_id, request):
        calls.append(1)
        if len(calls) < 3:
            raise TimeoutError("offload timed out reading the auto-approve preference")
        return {"outcome": {"outcome": "selected", "optionId": "allow"}}

    client = PAClient(MagicMock(), on_permission=flaky)
    response = await client.request_permission(OPTIONS, "s", {"toolCallId": "t"})
    assert response.outcome.outcome == "selected"
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_persistent_permission_handler_failure_stays_pending_and_reports(fast_permission_retries):
    reported = []
    alive = {"value": True}

    async def broken(session_id, request):
        raise TimeoutError("notification store offline")

    client = PAClient(
        MagicMock(),
        on_permission=broken,
        on_permission_unavailable=lambda sid, req, exc: reported.append(str(exc)),
    )
    client.transport_alive = lambda: alive["value"]
    task = asyncio.ensure_future(client.request_permission(OPTIONS, "s", {"toolCallId": "t"}))
    await asyncio.sleep(0.5)
    assert not task.done(), "the permission must stay pending for a human, not be denied"
    assert len(reported) == 1
    alive["value"] = False  # the provider went away: nothing is left to answer
    response = await asyncio.wait_for(task, 5)
    assert response.outcome.outcome == "cancelled"
