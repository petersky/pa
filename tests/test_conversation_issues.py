"""Discussion remains inert; work admission survives retries and lost receipts."""
import asyncio
import json
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.domain.conversation_issues import ConversationIssues, Issue, IssueConflict, IssueEdit, StartIssues
from pa.domain.models import AgentSession, CardCreate, ProjectCreate
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager
from pa.modules.conversation_issues import edit_issues, get_issues, start_issues, _preferences
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


@pytest.fixture
def setup(tmp_path):
    log = EventLog(ObjectStore(tmp_path / "objects"), tmp_path, "test")
    store = CardProjection(tmp_path / "pa.db", log)
    settings = Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "worktrees", instance_id="test")
    manager = AgentSessionManager(settings, store)
    store.save_session(AgentSession(id="discussion", agent_name="codex", purpose="chat", principal_id="user:test", mode_id="read-only"))
    app = FastAPI()
    app.state.ctx = SimpleNamespace(settings=settings, store=store, services={}, require_service=lambda _: manager)
    def request(key="start-1234", principal="user:test"):
        return Request({"type": "http", "app": app, "headers": [(b"idempotency-key", key.encode())],
                        "state": {"user": SimpleNamespace(role="user"), "principal_id": principal}})
    return store, manager, request, app


def ten_issues():
    return IssueEdit(revision=0, issues=[Issue(id=f"issue-{i}", title=f"Issue {i}", group="Small fixes" if i < 2 else "", deferred=i == 9) for i in range(10)])


@pytest.mark.asyncio
async def test_discuss_edit_group_defer_no_cards_and_owner_isolation(setup):
    store, _, req, _ = setup
    state = await edit_issues(req(), "discussion", ten_issues())
    assert len(state["issues"]) == 10 and state["revision"] == 1
    assert not store.list_cards()  # No card forms or execution during discussion.
    assert await edit_issues(req(), "discussion", ten_issues()) == state  # lost-save retry
    with pytest.raises(HTTPException) as denied:
        await get_issues(req(principal="user:other"), "discussion")
    assert denied.value.status_code == 403
    with pytest.raises(HTTPException) as stale:
        await edit_issues(req(), "discussion", IssueEdit(revision=0, issues=[]))
    assert stale.value.status_code == 409


@pytest.mark.asyncio
async def test_grouped_start_exactly_once_and_restore_after_lost_dispatch_receipt(setup, monkeypatch):
    store, _, req, _ = setup
    await edit_issues(req(), "discussion", ten_issues())
    admitted = {}
    calls = []
    lose_receipt = True
    async def dispatch(request, body):
        nonlocal lose_receipt
        key = request.headers["idempotency-key"]
        calls.append(body)
        assert body.mode_id is None  # The worker resolves its own purpose default.
        assert not body.cwd and not body.allow_concurrent and not body.capacity_override
        admitted.setdefault(key, "dispatch-one")
        if lose_receipt:
            lose_receipt = False
            raise OSError("lost acknowledgement after admission")
        return {"dispatch_id": admitted[key]}
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    body = StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-0", "issue-1"])
    first = await start_issues(req(), "discussion", body)
    assert first["requests"][0]["state"] == "admitted"
    card_id = first["requests"][0]["groups"][0]["card_id"]
    assert len(store.list_cards()) == 1
    # Reload from persisted notebook, then retry the same request.
    restored = await get_issues(req(), "discussion")
    assert restored["requests"][0]["submission"] == body.model_dump()
    assert restored["requests"][0]["realm_id"] == "default"
    assert "context" not in restored["requests"][0]
    again = await start_issues(req(), "discussion", body)
    assert again["requests"][0]["state"] == "admitted"
    assert again["requests"][0]["groups"][0]["card_id"] == card_id
    assert len(admitted) == 1 and len(store.list_cards()) == 1
    assert store.get_session("discussion").card_id is None  # discussion never becomes a worker
    await start_issues(req(), "discussion", body)
    assert len(calls) == 2  # second retry joins durable receipt, no dispatch call
    with pytest.raises(HTTPException):
        await start_issues(req("another-start"), "discussion", body.model_copy(update={"request_id": "another-start", "revision": 2}))
    with pytest.raises(HTTPException):
        await edit_issues(req(), "discussion", IssueEdit(revision=2, issues=[]))


@pytest.mark.asyncio
async def test_admission_ack_is_independent_of_slow_work_and_coalesces(setup, monkeypatch):
    store, _, req, app = setup
    executor = AsyncRuntime()
    app.state.ctx.services["async_runtime"] = executor
    ready = asyncio.Event()
    calls = 0
    async def dispatch(request, body):
        nonlocal calls
        calls += 1
        await ready.wait()
        return {"dispatch_id": "slow-dispatch"}
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    await edit_issues(req(), "discussion", ten_issues())
    body = StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-2"])
    first, second = await asyncio.gather(start_issues(req(), "discussion", body), start_issues(req(), "discussion", body))
    assert first["requests"][0]["state"] == second["requests"][0]["state"] == "admitting"
    assert calls == 1 and len(store.list_cards()) == 1
    ready.set()
    result = await start_issues(req(), "discussion", body)
    assert result["requests"][0]["state"] == "admitted"
    assert calls == 1
    await executor.close()


@pytest.mark.asyncio
async def test_legacy_missing_chat_mode_does_not_block_worker_default(setup, monkeypatch):
    store, _, req, _ = setup
    dispatch = AsyncMock(return_value={"dispatch_id": "legacy-worker"})
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    await edit_issues(req(), "discussion", ten_issues())
    with pytest.raises(HTTPException):
        await start_issues(req(), "discussion", StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-9"]))
    session = store.get_session("discussion")
    session.mode_id = None
    store.save_session(session)
    result = await start_issues(req(), "discussion", StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-0"]))
    assert result["requests"][0]["state"] == "admitted"
    assert dispatch.call_args.args[1].mode_id is None
    assert store.get_session("discussion").mode_id is None


def test_preferences_keep_explicit_false_and_all_hard_constraints():
    session = AgentSession(agent_name="codex", config_json={"execution_selection": {
        "resolved_inputs": {"model": {"intent": "required", "value": "chosen"}, "options.flag": {"intent": "required", "value": False}},
        "constraints": [{"instance_ids": ["a", "b"], "max_cost_usd": 3}, {"instance_ids": ["b", "c"], "max_cost_usd": 1, "min_context_tokens": 1000}],
    }})
    prefs = _preferences(session)
    assert prefs["model"]["value"] == "chosen"
    assert prefs["options"]["flag"]["value"] is False
    assert prefs["hard_constraints"]["instance_ids"] == ["b"]
    assert prefs["hard_constraints"]["max_cost_usd"] == 1
    assert prefs["hard_constraints"]["min_context_tokens"] == 1000


@pytest.mark.asyncio
async def test_first_selected_work_inherits_primary_card_project_and_worker_default(setup, monkeypatch):
    from pa.modules.fleet import _apply_dispatch_mode_default
    store, _, req, _ = setup
    project = store.create_project(ProjectCreate(title="PA Core"))
    source = store.create_card(CardCreate(title="Canonical source", project_id=project.id, auto_enrich=False))
    session = store.get_session("discussion")
    session.card_id, session.mode_id = source.id, "agent-full-access"
    session.model_id = "gpt-6.1-sol[xhigh]"
    session.config_json = {"execution_selection": {"resolved_inputs": {
        "model": {"intent": "required", "value": "gpt-6.1-sol"},
        "reasoning": {"intent": "required", "value": "xhigh"},
        "options.fast-mode": {"intent": "required", "value": "on"},
    }}}
    store.save_session(session)
    await edit_issues(req(), "discussion", ten_issues())
    seen = []
    async def dispatch(request, body):
        _apply_dispatch_mode_default(body)
        seen.append(body)
        assert body.project_id == project.id
        assert body.mode_id == "workspace-write"
        assert body.model_id == "gpt-6.1-sol[xhigh]"
        assert body.execution_preferences.options["fast-mode"].value == "on"
        return {"dispatch_id": "first-attempt"}
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    body = StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-0"])
    first = await start_issues(req(), "discussion", body)
    assert first["requests"][0]["state"] == "admitted"
    card = store.get_card(first["requests"][0]["groups"][0]["card_id"])
    assert card.project_id == project.id and card.parent_id == source.id
    await start_issues(req(), "discussion", body)
    assert len(seen) == 1
    assert store.get_session("discussion").mode_id == "agent-full-access"
    assert store.get_session("discussion").project_id is None


@pytest.mark.asyncio
async def test_public_issue_round_trip_preserves_started_claims_and_strict_fields(setup, monkeypatch):
    from pydantic import ValidationError
    store, _, req, _ = setup
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", AsyncMock(return_value={"dispatch_id": "one"}))
    await edit_issues(req(), "discussion", ten_issues())
    await start_issues(req(), "discussion", StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-0"]))
    state = await get_issues(req(), "discussion")
    assert "request_id" not in state["editable_issues"][0]
    state["issues"][1]["title"] = "Refined unstarted issue"
    state["issues"][1]["request_id"] = "cannot-claim-work"
    result = await edit_issues(req(), "discussion", IssueEdit(revision=state["revision"], issues=state["issues"]))
    assert result["issues"][0]["request_id"] == "start-1234"
    assert "request_id" not in result["issues"][1]
    with pytest.raises(ValidationError):
        IssueEdit(revision=0, issues=[{"id": "one", "title": "One", "unknown": True}])
    result["issues"][0]["title"] = "Cannot edit frozen work"
    with pytest.raises(HTTPException):
        await edit_issues(req(), "discussion", IssueEdit(revision=result["revision"], issues=result["issues"]))
    assert len(store.list_cards()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", [False, True])
async def test_pending_admission_recovers_exact_identity_but_denial_is_not_retried(setup, monkeypatch, denied):
    _, _, req, _ = setup
    calls = []
    async def dispatch(request, body):
        calls.append((request.headers["idempotency-key"], body.model_dump(mode="json")))
        if denied or len(calls) == 1:
            raise HTTPException(409, {"code": "policy_denied" if denied else "dispatch_admission_in_progress", "dispatch_id": "same-dispatch"})
        return {"dispatch_id": "same-dispatch"}
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    await edit_issues(req(), "discussion", ten_issues())
    result = await start_issues(req(), "discussion", StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-0"]))
    assert result["requests"][0]["state"] == ("blocked" if denied else "admitted")
    assert len(calls) == (1 if denied else 2)
    assert all(call == calls[0] for call in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_dispatch", [False, True])
async def test_legacy_frozen_chat_mode_normalizes_only_before_first_admission(setup, monkeypatch, existing_dispatch):
    from pa.modules.conversation_issues import _start_context
    store, manager, req, app = setup
    await edit_issues(req(), "discussion", ten_issues())
    body = StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-0"])
    context = await _start_context(req(), manager, store.get_session("discussion"))
    context["dispatch"]["mode_id"] = "agent-full-access"  # Pre-repair notebook.
    notes = ConversationIssues(store)
    notes.admit("discussion", body, context)
    app.state.ctx.services["dispatch_store"] = SimpleNamespace(
        by_authority_idempotency=lambda instance, key: object() if existing_dispatch else None)
    calls = []
    async def dispatch(request, data):
        calls.append(data.model_dump(mode="json"))
        if len(calls) == 1:
            raise OSError("lost receipt")
        return {"dispatch_id": "canonical"}
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    result = await start_issues(req(), "discussion", body)
    assert result["requests"][0]["state"] == "admitted"
    assert len(calls) == 2 and calls[0] == calls[1]
    assert calls[0]["mode_id"] == ("agent-full-access" if existing_dispatch else None)
    assert "dispatch_inputs" not in result["requests"][0]["groups"][0]
    await start_issues(req(), "discussion", body)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_card_event_recovers_after_crash_before_notebook_receipt(setup, monkeypatch):
    store, _, req, _ = setup
    await edit_issues(req(), "discussion", ten_issues())
    original = store.complete_operation
    lost = False
    def lose_receipt(key, result, **kwargs):
        nonlocal lost
        if not lost:
            lost = True
            raise OSError("crash after canonical event")
        return original(key, result, **kwargs)
    monkeypatch.setattr(store, "complete_operation", lose_receipt)
    dispatch = AsyncMock(return_value={"dispatch_id": "one"})
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    body = StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-0"])
    first = await start_issues(req(), "discussion", body)
    assert first["requests"][0]["state"] == "blocked"
    assert len(store.list_cards()) == 1
    retry = await start_issues(req(), "discussion", body)
    assert retry["requests"][0]["state"] == "admitted", retry["requests"][0]
    assert len(store.list_cards()) == 1 and dispatch.await_count == 1


@pytest.mark.skipif(not shutil.which("node"), reason="node required")
def test_client_retry_identity_and_late_response_thread_isolation():
    result = subprocess.run(["node", "tests/conversation_issues_node_harness.js"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_restored_receipt_exposes_frozen_realm_without_execution_context():
    state = {"revision": 2, "issues": [], "requests": [{
        "request_id": "saved-start", "state": "admitted", "fingerprint": "private",
        "context": {"realm_id": "research", "instance_id": "remote-owner", "dispatch": {"model_id": "private"}},
        "groups": [{"card_id": "existing-card", "issues": [], "dispatch_inputs": {}}],
    }]}
    receipt = ConversationIssues.public(state)["requests"][0]
    assert receipt["realm_id"] == "research"
    assert "context" not in receipt and "fingerprint" not in receipt
    assert receipt["groups"] == [{"card_id": "existing-card"}]
    assert "realm_id" not in state["requests"][0]  # no migration of retained notebooks
