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
from pa.domain.models import AgentSession, CardCreate
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
        assert body.mode_id == "read-only"
        assert not body.cwd and not body.allow_concurrent and not body.capacity_override
        admitted.setdefault(key, "dispatch-one")
        if lose_receipt:
            lose_receipt = False
            raise OSError("lost acknowledgement after admission")
        return {"dispatch_id": admitted[key]}
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    body = StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-0", "issue-1"])
    first = await start_issues(req(), "discussion", body)
    assert first["requests"][0]["state"] == "blocked"
    card_id = first["requests"][0]["groups"][0]["card_id"]
    assert len(store.list_cards()) == 1
    # Reload from persisted notebook, then retry the same request.
    restored = await get_issues(req(), "discussion")
    assert restored["requests"][0]["submission"] == body.model_dump()
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
async def test_missing_mode_and_deferred_selection_cannot_start(setup, monkeypatch):
    store, _, req, _ = setup
    dispatch = AsyncMock()
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    await edit_issues(req(), "discussion", ten_issues())
    with pytest.raises(HTTPException):
        await start_issues(req(), "discussion", StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-9"]))
    session = store.get_session("discussion")
    session.mode_id = None
    store.save_session(session)
    with pytest.raises(HTTPException) as error:
        await start_issues(req(), "discussion", StartIssues(request_id="start-1234", revision=1, issue_ids=["issue-0"]))
    assert error.value.detail["code"] == "conversation_mode_unknown"
    assert not store.list_cards() and not dispatch.called


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
