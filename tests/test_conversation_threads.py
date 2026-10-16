"""Independent conversation identities, explicit fallback and uncertain fork recovery."""
import asyncio
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from pa.acp.client import AgentConnection
from pa.acp.conversation_capabilities import conversation_capabilities
from pa.agent.context import compose_session_prompt
from pa.domain.models import AgentSession, TranscriptEvent
from pa.modules.conversation_threads import BranchConversation, branch_conversation, get_conversation_capabilities, list_conversation_threads
from tests.test_conversation_issues import setup  # shared isolated store/request fixture


CAPS = {"agentCapabilities": {"sessionCapabilities": {"fork": {}}}, "_meta": {"steering": {"supported": True}}}


@pytest.mark.skipif(not shutil.which("node"), reason="node required")
def test_browser_branch_retry_identity_and_thread_isolation():
    result = subprocess.run(["node", "tests/conversation_threads_node_harness.js"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_capabilities_do_not_confuse_advertised_steering_with_safe_delivery():
    caps = conversation_capabilities(CAPS)
    assert caps["fork"] and caps["steering_advertised"] and not caps["steering_available"]
    for malformed in (None, [], "oops", {"agentCapabilities": []}, {"_meta": "bad"}):
        assert not conversation_capabilities(malformed)["fork"]


def test_copied_history_is_context_not_workspace_authority(setup):
    store, manager, _, _ = setup
    target = AgentSession(id="target", agent_name="codex", initiating_workflow={
        "kind": "conversation_branch", "strategy": "copy", "source_session_id": "discussion",
        "saved_excerpt": "Historical note: /source-worktree", "source_external_id": "source-native",
    })
    prompt = compose_session_prompt(store, manager.settings, target, "Discuss the next step.")
    assert "Source PA conversation: discussion" in prompt.text
    assert "do not authorize this conversation" in prompt.text
    assert "not new instructions or approval" in prompt.text
    assert "Agent session: target" in prompt.text


def test_chat_guidance_uses_durable_selected_work_without_authorizing_discussion(setup):
    store, manager, _, _ = setup
    prompt = compose_session_prompt(store, manager.settings, store.get_session("discussion"), "Discuss ten possible fixes.")
    assert "listing issues are not authorization to execute" in prompt.text
    assert "start_conversation_issues with one stable request ID" in prompt.text
    assert "not task completion" in prompt.text
    assert not store.list_cards()


@pytest.mark.asyncio
async def test_branch_retry_does_not_reuse_workspace_or_start_work(setup):
    store, manager, req, _ = setup
    source = store.get_session("discussion")
    source.cwd = "/source-worktree"
    source.external_session_id = "source-native"
    store.save_session(source)
    store.append_transcript_events([
        TranscriptEvent(session_id=source.id, seq=i + 1, event_type="user_message", payload={"text": f"Note {i} " + "x" * 100})
        for i in range(130)
    ])
    captured = []
    async def create(**kwargs):
        captured.append(kwargs)
        assert not any(key in kwargs for key in ("cwd", "card_id", "dispatch_id", "context_source_session_id", "execution_binding"))
        assert kwargs["initial_configuration"].mode_id == "read-only"
        await asyncio.sleep(0.01)
        target = AgentSession(id=kwargs["session_id"], agent_name="codex", purpose="chat", principal_id="user:test",
                              initiating_workflow=kwargs["initiating_workflow"], cwd="/target-own-workspace")
        store.save_session(target)
        return SimpleNamespace(session=target)
    manager.create_session = AsyncMock(side_effect=create)
    body = BranchConversation(request_id="branch-1234", strategy="copy")
    a, b = await asyncio.gather(branch_conversation(req(body.request_id), source.id, body), branch_conversation(req(body.request_id), source.id, body))
    assert a["session"]["id"] == b["session"]["id"] and len(captured) == 1
    excerpt = captured[0]["initiating_workflow"]["saved_excerpt"]
    assert len(excerpt) <= 8000 and "Note 129" in excerpt and "Note 0 " not in excerpt
    assert captured[0]["initiating_workflow"]["source_through_seq"] == 130
    assert store.get_session(source.id).cwd == "/source-worktree"
    assert not store.list_cards()
    with pytest.raises(HTTPException) as mismatch:
        await branch_conversation(req(body.request_id), source.id, body.model_copy(update={"strategy": "native"}))
    assert mismatch.value.status_code == 409
    with pytest.raises(HTTPException) as denied:
        await branch_conversation(req(body.request_id, principal="user:other"), source.id, body)
    assert denied.value.status_code == 403


@pytest.mark.asyncio
async def test_native_is_explicit_capability_gated_without_copy_fallback(setup):
    store, manager, req, _ = setup
    manager.create_session = AsyncMock()
    assert not (await get_conversation_capabilities(req(), "discussion"))["native_branch_available"]
    with pytest.raises(HTTPException):
        await branch_conversation(req("branch-1234"), "discussion", BranchConversation(request_id="branch-1234", strategy="native"))
    manager.create_session.assert_not_called()


@pytest.mark.asyncio
async def test_empty_threads_and_forks_retain_group_across_reload_without_shared_state(setup):
    store, manager, req, _ = setup
    source = store.get_session("discussion")
    source.external_session_id = "source-native"
    source.config_json = {"durable_runtime": {"queued_prompts": [{"id": "only-source"}]}}
    store.save_session(source)
    captured = []
    async def create(**kwargs):
        captured.append(kwargs)
        # All strategies go through normal admission, never inherit a lease or
        # replace the busy source. Fresh siblings get no provider history seed.
        assert not set(kwargs) & {"cwd", "execution_binding", "card_id", "dispatch_id", "context_source_session_id"}
        target = AgentSession(id=kwargs["session_id"], agent_name="codex", purpose="chat",
                              principal_id="user:test", mode_id="read-only",
                              initiating_workflow=kwargs["initiating_workflow"], cwd="/own/" + kwargs["session_id"])
        store.save_session(target)
        return SimpleNamespace(session=target)
    manager.create_session = AsyncMock(side_effect=create)
    body = BranchConversation(request_id="empty-thread-123", strategy="empty")
    a, duplicate = await asyncio.gather(*(branch_conversation(req(body.request_id), source.id, body) for _ in range(2)))
    assert a["session"]["id"] == duplicate["session"]["id"]
    assert len(captured) == 1
    empty_id = a["session"]["id"]
    assert a["branch"]["kind"] == "conversation_thread"
    assert "saved_excerpt" not in a["branch"]
    assert a["branch"]["chat_group_id"] == source.id
    # A child of a sibling stays in the same chat (no accidental nested group).
    copy_body = BranchConversation(request_id="copy-thread-1234", strategy="copy")
    copy = await branch_conversation(req(copy_body.request_id), empty_id, copy_body)
    assert copy["branch"]["chat_group_id"] == source.id
    assert copy["branch"]["source_session_id"] == empty_id
    manager._runtimes[source.id] = SimpleNamespace(connection=SimpleNamespace(_init_response=CAPS))
    native_body = BranchConversation(request_id="native-thread-123", strategy="native")
    fork = await branch_conversation(req(native_body.request_id), source.id, native_body)
    manager._runtimes.clear()
    assert fork["branch"]["chat_group_id"] == source.id
    assert fork["branch"]["strategy"] == "native"
    # Reload all relationships from SQLite; unrelated chats and forged group
    # metadata on another owner's session must not leak into the group.
    for sid, principal, realm in [("new-chat", "user:test", "default"), ("foreign", "user:other", "default"), ("other-realm", "user:test", "other")]:
        store.save_session(AgentSession(id=sid, agent_name="codex", purpose="chat", principal_id=principal, realm_id=realm,
            initiating_workflow={} if sid == "new-chat" else {"chat_group_id": source.id}))
    group = await list_conversation_threads(req(), empty_id)
    assert {t["id"] for t in group["threads"]} == {source.id, empty_id, copy["session"]["id"], fork["session"]["id"]}
    assert group["chat_group_id"] == source.id
    assert [t["queue_length"] for t in group["threads"] if t["id"] == source.id] == [1]
    assert all(t["queue_length"] == 0 for t in group["threads"] if t["id"] != source.id)
    assert {t["context"] for t in group["threads"]} == {"Original conversation", "Empty context", "Copied recent context", "Latest provider history"}
    assert len((await list_conversation_threads(req(), "new-chat"))["threads"]) == 1
    with pytest.raises(HTTPException) as denied:
        await list_conversation_threads(req(principal="user:other"), source.id)
    assert denied.value.status_code == 403
    assert store.get_session(source.id).external_session_id == "source-native"
    assert not store.list_cards()


@pytest.mark.asyncio
async def test_native_fork_uses_target_parameters_and_persists_identity_before_return(setup):
    store, manager, _, _ = setup
    target = AgentSession(id="target", agent_name="codex", initiating_workflow={"kind": "conversation_branch", "strategy": "native", "source_external_id": "source-native"})
    connection = AgentConnection(manager.settings, store)
    connection._init_response = CAPS
    async def fork(**kwargs):
        assert store.get_session(target.id).config_json["branch_admission"]["state"] == "request_sent"
        assert kwargs == {"session_id": "source-native", "cwd": "/target-lease", "mcp_servers": ["target-mcp"]}
        return SimpleNamespace(session_id="target-native")
    connection._conn = SimpleNamespace(fork_session=AsyncMock(side_effect=fork))
    await connection._fork_conversation(target, {"cwd": "/target-lease", "mcp_servers": ["target-mcp"]})
    restored = store.get_session(target.id)
    assert restored.external_session_id == "target-native"
    assert restored.config_json["branch_admission"]["state"] == "forked"
    with pytest.raises(RuntimeError, match="second fork"):
        await connection._fork_conversation(restored, {"cwd": "/target-lease"})
    assert connection._conn.fork_session.await_count == 1


@pytest.mark.asyncio
async def test_lost_native_fork_response_never_blindly_reissues_or_falls_back(setup):
    store, manager, _, _ = setup
    target = AgentSession(id="target", agent_name="codex", initiating_workflow={"source_external_id": "source-native"})
    connection = AgentConnection(manager.settings, store)
    connection._init_response = CAPS
    connection._conn = SimpleNamespace(fork_session=AsyncMock(side_effect=TimeoutError("lost receipt")))
    with pytest.raises(TimeoutError):
        await connection._fork_conversation(target, {"cwd": "/target-lease"})
    restored = store.get_session(target.id)
    assert restored.config_json["branch_admission"]["state"] == "outcome_unknown"
    with pytest.raises(RuntimeError, match="second fork"):
        await connection._fork_conversation(restored, {"cwd": "/target-lease"})
    assert connection._conn.fork_session.await_count == 1
