"""Initial admission, recovery, and superseded-provider lifecycle regressions."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from pa.acp.client import AgentConnection
from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.execution.session_presentation import build_session_presentation
from pa.instance.agent_session import (
    AgentSessionManager, AgentSessionRuntime, SessionAdmissionInProgress,
)
from pa.instance.quiesce import QueuedPrompt
from tests.test_execution_selection import candidate


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["workspace", "provider"])
async def test_recovery_cannot_start_a_second_provider_during_initial_admission(tmp_path, phase):
    store = CardProjection(tmp_path / "pa.db")
    manager = AgentSessionManager(Settings(data_dir=tmp_path, agent_provider="codex"), store)
    entered = asyncio.Event()
    release = asyncio.Event()
    spec = MagicMock(id="codex", env={})
    prepare = manager._prepare_workspace

    async def gated_workspace(session, **kwargs):
        if phase == "workspace":
            store.save_session(session)
            entered.set()
            await release.wait()
        return await prepare(session, **kwargs)

    manager._prepare_workspace = gated_workspace

    async def start(runtime, **kwargs):
        assert store.get_session(runtime.session_id).execution_binding["cwd"]
        if phase == "provider":
            entered.set()
            await release.wait()
        runtime.connection = MagicMock(connected=True)
        runtime.session.external_session_id = "one-native-session"

    with patch(
        "pa.execution.selection_service.SelectionService.local_catalog",
        new=AsyncMock(return_value=[candidate(instance_id=manager.settings.instance_id)]),
    ), patch(
        "pa.instance.agent_session.resolve_agent_provider",
        return_value=SimpleNamespace(provider_id="codex", spec=spec, source="instance"),
    ), patch.object(AgentSessionRuntime, "start", autospec=True, side_effect=start) as spawn:
        task = asyncio.create_task(manager.create_session(session_id="initial", label="initial"))
        try:
            await asyncio.wait_for(entered.wait(), 10)
            assert store.get_session("initial").status == (
                "provisioning" if phase == "workspace" else "connecting"
            )
            assert manager.get("initial") is None
            # This is the pre-publication window in which the production
            # coordinator launched two providers 4.3 seconds apart.
            await manager._recovery_once()
            assert not manager._recovery_tasks
            with pytest.raises(SessionAdmissionInProgress):
                await manager.recover_session("initial")
            assert spawn.await_count == (0 if phase == "workspace" else 1)
        finally:
            release.set()
            runtime = await task
        assert manager.get("initial") is runtime
        assert spawn.await_count == 1
        assert not manager._admitting_sessions
        assert len(store.list_session_execution_binding_history("initial")) == 1


@pytest.mark.asyncio
async def test_explicit_recovery_checkpoints_original_queue_and_pause(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    settings = Settings(data_dir=tmp_path, agent_provider="codex")
    prompt = QueuedPrompt(id="original-prompt", message="preserve", source="restart-handoff:receipt")
    session = store.save_session(AgentSession(
        id="restore", agent_name="codex", external_session_id="original-native",
        status="quiesced", config_json={"durable_runtime": {
            "queued_prompts": [prompt.model_dump(mode="json")], "queue_paused": True,
        }},
    ))
    manager = AgentSessionManager(settings, store)
    manager._prepare_workspace = AsyncMock(return_value={})
    connection = MagicMock(agent_name="codex", connected=True)
    connection.connect = AsyncMock(return_value=session)
    with patch("pa.instance.agent_session.AgentConnection", return_value=connection):
        runtime = await manager.recover_session(session.id)
    assert connection.connect.await_args.kwargs["require_restore"] is True
    assert connection.connect.await_args.kwargs["resume_external_id"] == "original-native"
    assert [p.id for p in runtime._queue] == [prompt.id]
    durable = store.get_session(session.id).config_json["durable_runtime"]
    assert durable["queue_paused"] is True
    assert [p["id"] for p in durable["queued_prompts"]] == [prompt.id]
    assert runtime._drain_task is None


@pytest.mark.asyncio
async def test_superseded_connection_exit_cannot_disconnect_current_owner(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    settings = Settings(data_dir=tmp_path)
    old = AgentSession(id="same-session", agent_name="codex", external_session_id="same-native",
                       config_json={"provider_connection_id": "old"})
    current = old.model_copy(deep=True)
    current.status = "prompting"
    current.config_json = {"provider_connection_id": "current", "durable_runtime": {
        "queued_prompts": [{"id": "keep-current-work"}],
    }}
    store.save_session(current)
    stale = AgentConnection(settings, store)
    stale.session = old
    stale._connection_id = "old"
    await stale.disconnect()
    store.save_session(old, expected_connection_id="old")
    # A late runtime checkpoint is fenced too, including after disconnect.
    manager = AgentSessionManager(settings, store)
    runtime = AgentSessionRuntime(manager, old)
    runtime._checkpoint_runtime(lifecycle="disconnected")
    assert store.get_session(old.id).status == "prompting"
    assert store.get_session(old.id).config_json == current.config_json
    owner = AgentConnection(settings, store)
    owner.session = current
    owner._connection_id = "current"
    await owner.disconnect()
    assert store.get_session(old.id).status == "disconnected"


@pytest.mark.parametrize("connected", [True, False])
def test_historical_success_does_not_hide_queued_followup(connected):
    prompt = QueuedPrompt(id="followup", message="continue")
    session = AgentSession(
        id="historical", agent_name="codex", purpose="automated_run",
        workflow_state="succeeded", config_json={"durable_runtime": {
            "queued_prompts": [prompt.model_dump(mode="json")],
        }},
    )
    runtime = SimpleNamespace(
        connected=True, prompting=False, _closed=False,
        _queue=[prompt], _in_flight=None,
    ) if connected else None
    result = build_session_presentation(session, runtime=runtime)
    assert result["display_status"] == ("Queued" if connected else "Restoring your work")
    assert result["workflow"]["state"] == "succeeded"
    assert result["turn"]["state"] == "queued"


@pytest.mark.asyncio
async def test_recovery_protocol_reports_owned_admission_as_retryable_conflict(tmp_path):
    from pa.modules.agent_chat import recover_session

    manager = MagicMock()
    manager.store.get_session.return_value = AgentSession(id="starting", agent_name="codex")
    manager.recover_session = AsyncMock(side_effect=SessionAdmissionInProgress("Admission in progress"))
    request = MagicMock()
    request.app.state.ctx.settings.auth_required = False
    with patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager), patch(
        "pa.modules.agent_chat.get_principal_id", return_value="owner"
    ), pytest.raises(HTTPException) as response:
        await recover_session(request, "starting")
    assert response.value.status_code == 409
    assert response.value.detail["code"] == "session_admission_in_progress"
    assert response.value.detail["recoverable"] is True
