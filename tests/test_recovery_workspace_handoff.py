"""An idle lost source can release ownership without losing retained work."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from pa.config import Settings
from pa.domain.models import AgentSession, CardCreate
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.modules.agent_chat import router, session_close
from pa.repository.workspace import WorkspaceLease, workspace_blocks_admission


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    monkeypatch.delenv("PA_WORKSPACE_ROOT", raising=False)
    settings = Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces")
    settings.data_dir.mkdir()
    store = CardProjection(settings.data_dir / "pa.db")
    card = store.create_card(CardCreate(title="Unfinished Operations"), via_log=False)
    source = store.save_session(AgentSession(
        id="lost-source", agent_name="codex", status="recovery_blocked",
        purpose="automated_run", card_id=card.id, dispatch_id="failed-origin",
        principal_id="user:local", external_session_id="lost-native-context",
    ))
    ledger = DispatchStore(settings.data_dir)
    origin = ledger.put(DispatchRecord(
        dispatch_id=source.dispatch_id, mutation_id="original-request",
        authority_instance_id=settings.instance_id, authority_url="http://pa.test",
        target_instance_id=settings.instance_id, card_id=card.id,
        session_id=source.id, state="failed", principal_id=source.principal_id,
        realm_id=source.realm_id,
    ))
    manager = AgentSessionManager(settings, store, dispatch_store=ledger)
    path = settings.workspace_root / "retained-source"
    path.mkdir()
    (path / "preserved.txt").write_text("original work and history")
    lease = WorkspaceLease(
        id="source-lease", repository_id="repo", repository_url="https://example.test/pa",
        card_id=card.id, session_id=source.id, worktree_path=str(path),
        cache_path=str(settings.workspace_root / "cache"), branch="source-branch",
        base_sha="source-base", fencing_token=19, state="ready", stage="verified",
        expires_at=datetime.now(UTC) + timedelta(days=1),
    )
    manager.workspace_manager._save(lease)
    manager.workspace_manager.collect_garbage = MagicMock(side_effect=AssertionError("no collection"))
    request = SimpleNamespace(state=SimpleNamespace(principal_id="user:local"))
    with patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager):
        yield SimpleNamespace(manager=manager, store=store, card=card, source=source,
                              ledger=ledger, origin=origin, path=path, request=request)


@pytest.mark.asyncio
async def test_retire_source_releases_lease_preserves_history_and_replays(recovery):
    r = recovery
    before = r.ledger.get(r.origin.dispatch_id).model_dump()
    assert workspace_blocks_admission(r.manager.workspace_manager.admission_workspaces()[0])
    for _ in range(2):
        result = await session_close(r.request, r.source.id, preserve_workspace=True)
        assert result["workspace_preserved"] and result["leases_expired"] == 1
        assert r.store.get_session(r.source.id).status == "closed"
        assert not workspace_blocks_admission(r.manager.workspace_manager.admission_workspaces()[0])
        assert (r.path / "preserved.txt").read_text() == "original work and history"
        lease, = r.manager.workspace_manager.list()
        assert (lease.state, lease.branch, lease.fencing_token, lease.completed) == (
            "ready", "source-branch", 19, False,
        )
        assert r.ledger.get(r.origin.dispatch_id).model_dump() == before
    events = r.store.list_transcript_events(r.source.id)
    assert [e.payload["reason"] for e in events if e.event_type == "session_closed"] == [
        "workspace_preserving_recovery_close",
    ]
    assert r.store.get_session(r.source.id).external_session_id == "lost-native-context"
    r.manager.workspace_manager.collect_garbage.assert_not_called()
    assert not r.manager._admitting_sessions


def test_http_close_explicit_preservation_query_releases_ownership(recovery):
    r = recovery
    app = FastAPI()
    app.include_router(router, prefix="/api")
    with TestClient(app) as client:
        response = client.post(f"/api/agent/sessions/{r.source.id}/close?preserve_workspace=true")
    assert response.status_code == 200
    assert response.json()["workspace_preserved"] is True
    assert r.store.get_session(r.source.id).status == "closed"
    assert not workspace_blocks_admission(r.manager.workspace_manager.admission_workspaces()[0])
    assert (r.path / "preserved.txt").exists()
    r.manager.workspace_manager.collect_garbage.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("busy", ["durable_queue", "durable_turn", "runtime_turn", "runtime_queue", "admission"])
async def test_preserving_close_rejects_active_or_queued_work_without_changes(recovery, busy):
    r = recovery
    if busy.startswith("durable"):
        r.source.config_json["durable_runtime"] = {
            "queued_prompts" if busy == "durable_queue" else "in_flight": [{"id":"keep-work"}],
        }
        r.store.save_session(r.source)
    elif busy == "admission":
        r.manager._admitting_sessions.add(r.source.id)
    else:
        runtime = AgentSessionRuntime(r.manager, r.source)
        r.manager._runtimes[r.source.id] = runtime
        if busy == "runtime_turn":
            runtime._in_flight = MagicMock()
        else:
            runtime._queue.append(MagicMock())
    before = r.store.get_session(r.source.id).model_dump()
    leases = r.manager.workspace_manager.list()
    with pytest.raises(HTTPException) as denied:
        await session_close(r.request, r.source.id, preserve_workspace=True)
    assert denied.value.status_code == 409
    assert r.store.get_session(r.source.id).model_dump() == before
    assert r.manager.workspace_manager.list() == leases
    assert (r.path / "preserved.txt").exists()
    r.manager.workspace_manager.collect_garbage.assert_not_called()


@pytest.mark.asyncio
async def test_live_blocked_source_closes_under_prompt_and_admission_fences(recovery):
    r = recovery
    runtime = AgentSessionRuntime(r.manager, r.source)
    r.manager._runtimes[r.source.id] = runtime
    actual_close = runtime.close

    async def fenced_close(**kwargs):
        assert runtime._prompt_admission_lock.locked()
        assert r.source.id in r.manager._admitting_sessions
        assert kwargs["reconcile_workspace"] is False
        return await actual_close(**kwargs)

    runtime.close = AsyncMock(side_effect=fenced_close)
    result = await session_close(r.request, r.source.id, preserve_workspace=True)
    assert result["workspace_preserved"]
    assert r.manager.get(r.source.id) is None
    assert r.store.get_session(r.source.id).status == "closed"
    assert not r.manager._admitting_sessions
    assert (r.path / "preserved.txt").exists()
    r.manager.workspace_manager.collect_garbage.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["principal", "dispatch_running", "dispatch_binding", "pending_settings"])
async def test_retirement_requires_owned_terminal_origin(recovery, invalid):
    r = recovery
    if invalid == "principal":
        r.request.state.principal_id = "another-user"
    elif invalid == "dispatch_running":
        r.origin.state = "running"
        r.ledger.put(r.origin)
    elif invalid == "pending_settings":
        r.source.config_json["execution_pending_settings"] = {"state": "pending"}
        r.store.save_session(r.source)
    else:
        r.source.dispatch_id = "missing-dispatch"
        r.store.save_session(r.source)
    with pytest.raises(HTTPException) as denied:
        await session_close(r.request, r.source.id, preserve_workspace=True)
    assert denied.value.status_code == (403 if invalid == "principal" else 409)
    assert r.store.get_session(r.source.id).status == "recovery_blocked"
    assert workspace_blocks_admission(r.manager.workspace_manager.admission_workspaces()[0])
    assert not r.manager._admitting_sessions


@pytest.mark.asyncio
async def test_pending_restart_cannot_silently_rematerialize_cleaned_context(recovery):
    from pa.domain.models import RestartHandoff
    from pa.instance.agent_session import WorkspaceBindingMismatch
    store, card, source, manager = recovery.store, recovery.card, recovery.source, recovery.manager
    lease = manager.workspace_manager.list()[0]
    store.create_restart_handoff(RestartHandoff(session_id=source.id, card_id=card.id,
        idempotency_key="saved", continuation_prompt="saved", continuation_prompt_id="exact", status="restarting"))
    lease.state = "cleaned"
    manager.workspace_manager._save(lease)
    before = store.get_session(source.id).model_dump()
    with pytest.raises(WorkspaceBindingMismatch, match="lost its original leased workspace"):
        await manager._prepare_workspace(source, requested_cwd=source.cwd, provider_id="codex")
    assert store.get_session(source.id).model_dump() == before
    assert manager.workspace_manager.get(lease.repository_id, source.id).state == "cleaned"


@pytest.mark.asyncio
async def test_native_close_reports_pending_restart_conflict(recovery):
    from pa.domain.models import RestartHandoff
    r = recovery
    r.store.create_restart_handoff(RestartHandoff(session_id=r.source.id, card_id=r.card.id,
        idempotency_key="saved", continuation_prompt="saved", continuation_prompt_id="exact", status="restarting"))
    with pytest.raises(HTTPException) as failure:
        await session_close(r.request, r.source.id, preserve_workspace=True)
    assert failure.value.status_code == 409
    assert failure.value.detail["code"] == "restart_continuation_pending"
    assert r.store.get_session(r.source.id).status == "recovery_blocked"
