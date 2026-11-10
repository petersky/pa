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
def empty_retry(reserved):
    r = reserved
    sibling = r.source.model_copy(update={
        "id": "empty-retry", "external_session_id": None, "execution_binding": {},
        "config_json": {"provisioning": {"state": "failed"}},
    }, deep=True)
    r.store.save_session(sibling)
    r.sibling = sibling
    return r


@pytest.mark.asyncio
async def test_canonically_retired_empty_retry_is_not_an_owner_candidate(empty_retry):
    r = empty_retry
    closed = await session_close(r.request, r.sibling.id)
    assert closed["ok"] and not closed["live"]
    result = await session_close(r.request, r.source.id, preserve_workspace=True)
    assert result["workspace_preserved"] and result["leases_expired"] == 1
    current = r.ledger.get(r.origin.dispatch_id)
    assert current.session_id == r.source.id
    assert current.events[-1].detail["retired_empty_retry_ids"] == [r.sibling.id]
    assert (r.path / "preserved.txt").read_text() == "original work and history"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["unclosed", "external", "binding", "lease", "turn", "reservation", "runtime", "ownership", "queue", "cwd", "artifact", "settings"])
async def test_empty_retry_exclusion_rejects_any_unretired_or_competing_work(empty_retry, case):
    from pa.domain.models import TranscriptEvent
    r = empty_retry
    if case != "unclosed":
        await session_close(r.request, r.sibling.id)
    sibling = r.store.get_session(r.sibling.id)
    if case == "external":
        sibling.external_session_id = "retained-provider"
    elif case == "binding":
        sibling.execution_binding = {"version": 1}
    elif case == "ownership":
        sibling.authority_instance_id = "another-authority"
    elif case == "queue":
        sibling.config_json["durable_runtime"] = {"queued_prompts": [{"id": "work"}]}
    elif case == "cwd":
        sibling.cwd = str(r.path)
    elif case == "artifact":
        sibling.config_json["execution_context"] = {"artifact_workspace": {"path": str(r.path)}}
    elif case == "settings":
        sibling.config_json["execution_pending_settings"] = {"state": "pending"}
    elif case == "lease":
        lease, = r.manager.workspace_manager.list()
        r.manager.workspace_manager._save(lease.model_copy(update={"id": "other-lease", "session_id": sibling.id}))
    elif case == "turn":
        r.store.append_transcript_events([TranscriptEvent(session_id=sibling.id, seq=2,
            event_type="prompt_accepted", payload={"prompt_id": "work"})])
    elif case == "reservation":
        r.manager._admitting_sessions.add(sibling.id)
    elif case == "runtime":
        r.manager._runtimes[sibling.id] = AgentSessionRuntime(r.manager, sibling)
    r.store.save_session(sibling)
    before = r.ledger.get(r.origin.dispatch_id).model_dump()
    with pytest.raises(HTTPException):
        await session_close(r.request, r.source.id, preserve_workspace=True)
    assert r.ledger.get(r.origin.dispatch_id).model_dump() == before
    assert r.store.get_session(r.source.id).status == "recovery_blocked"
    assert (r.path / "preserved.txt").exists()


@pytest.fixture
def reserved(recovery):
    r = recovery
    r.source.authority_instance_id = r.origin.authority_instance_id
    r.source.origin_instance_id = r.origin.target_instance_id
    r.source.label = f"card:{r.card.id}:dispatch:{r.origin.dispatch_id}"
    lease, = r.manager.workspace_manager.list()
    r.source.config_json["execution_context"] = {
        "session_id": r.source.id, "dispatch_id": r.origin.dispatch_id,
        "card_id": r.card.id, "project_id": r.origin.project_id,
        "realm_id": r.origin.realm_id, "principal_id": r.origin.principal_id,
        "authority_instance": {"id": r.origin.authority_instance_id},
        "instance": {"id": r.origin.target_instance_id},
        "repositories": [{"lease_id": lease.id, "repository_id": lease.repository_id,
                          "repository_url": lease.repository_url,
                          "worktree_path": lease.worktree_path, "branch": lease.branch,
                          "base_sha": lease.base_sha, "fencing_token": lease.fencing_token}],
    }
    r.source.execution_binding = {
        "version": 1, "execution_card_id": r.card.id,
        "execution_project_id": r.origin.project_id,
        "origin_instance_id": r.origin.target_instance_id,
        "repository_ids": [lease.repository_id], "lease_ids": [lease.id],
        "worktree_paths": [lease.worktree_path], "branch": lease.branch,
        "base_sha": lease.base_sha,
    }
    r.store.save_session(r.source)
    r.origin.session_id = None
    r.origin.materialization_plan = {"repositories": [
        {"repository_id": lease.repository_id, "url": lease.repository_url}]}
    r.ledger.put(r.origin)
    return r


@pytest.mark.asyncio
async def test_reserved_before_dispatch_publication_can_retire_and_replay(reserved):
    r = reserved
    for _ in range(2):
        result = await session_close(r.request, r.source.id, preserve_workspace=True)
        assert result["workspace_preserved"]
    current = r.ledger.get(r.origin.dispatch_id)
    assert current.session_id == r.source.id
    assert current.state == "failed"
    assert len(current.events) == 1
    assert current.events[0].detail["outcome_inferred"] is False
    assert r.store.get_session(r.source.id).status == "closed"
    assert (r.path / "preserved.txt").read_text() == "original work and history"


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["authority", "origin", "realm", "principal", "card", "project",
    "label", "context", "lease_fence", "lease_path", "binding", "duplicate", "runtime", "admission", "queue", "prompt"])
async def test_lost_link_retirement_rejects_ambiguous_or_busy_source(reserved, invalid, monkeypatch):
    r = reserved
    if invalid in {"authority", "origin", "realm", "principal", "card", "project"}:
        field = {"authority": "authority_instance_id", "origin": "origin_instance_id",
                 "realm": "realm_id", "principal": "principal_id", "card": "card_id",
                 "project": "project_id"}[invalid]
        setattr(r.source, field, "other")
    elif invalid == "label":
        r.source.label = "same-card-is-not-proof"
    elif invalid == "context":
        r.source.config_json["execution_context"]["dispatch_id"] = "another"
    elif invalid == "binding":
        r.source.execution_binding["execution_card_id"] = "different"
    elif invalid.startswith("lease"):
        repository = r.source.config_json["execution_context"]["repositories"][0]
        repository["fencing_token" if invalid == "lease_fence" else "worktree_path"] = "different"
    elif invalid == "duplicate":
        r.store.save_session(r.source.model_copy(update={"id": "duplicate", "label": "other"}))
    elif invalid == "runtime":
        r.manager._runtimes[r.source.id] = AgentSessionRuntime(r.manager, r.source)
    elif invalid == "admission":
        r.manager._admitting_sessions.add(r.source.id)
    elif invalid == "queue":
        r.source.config_json["durable_runtime"] = {"queued_prompts": [{"id": "work"}]}
    elif invalid == "prompt":
        r.origin.initial_prompt_operation = {"id": "delivery-unknown"}
        r.ledger.put(r.origin)
    r.store.save_session(r.source)
    if invalid == "binding":
        read = r.store.get_session
        monkeypatch.setattr(r.store, "get_session", lambda key: r.source if key == r.source.id else read(key))
    before = r.ledger.get(r.origin.dispatch_id).model_dump()
    with pytest.raises(HTTPException):
        await session_close(r.request, r.source.id, preserve_workspace=True)
    assert r.ledger.get(r.origin.dispatch_id).model_dump() == before
    assert r.store.get_session(r.source.id).status == "recovery_blocked"
    assert (r.path / "preserved.txt").exists()


@pytest.mark.asyncio
async def test_reserved_repair_cas_preserves_concurrent_dispatch_write(reserved):
    r = reserved
    compare = r.ledger.compare_and_mutate
    def race(expected, mutate):
        current = r.ledger.get(expected.dispatch_id)
        current.state = "queued"
        r.ledger.put(current)
        return compare(expected, mutate)
    r.ledger.compare_and_mutate = race
    with pytest.raises(HTTPException) as failure:
        await session_close(r.request, r.source.id, preserve_workspace=True)
    assert failure.value.detail["code"] == "reserved_source_compare_conflict"
    assert r.ledger.get(r.origin.dispatch_id).session_id is None
    assert r.ledger.get(r.origin.dispatch_id).state == "queued"
    assert r.store.get_session(r.source.id).status == "recovery_blocked"


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
