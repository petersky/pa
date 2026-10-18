"""Provenance is display context, never source-session authority."""
from datetime import UTC, datetime

import pytest
from fastapi import HTTPException

from pa.domain.conversation_origin import conversation_origin
from pa.domain.models import AgentSession
from pa.instance.agent_session import AgentSessionRuntime
from pa.modules.conversation_threads import get_conversation_origin
from tests.test_conversation_issues import setup


def branch(store, **changes):
    session = AgentSession(
        id="branch", agent_name="codex", purpose="chat", principal_id="user:test",
        initiating_workflow={"kind": "conversation_branch", "strategy": "native",
            "source_session_id": "discussion", "source_title": "Original plan",
            "saved_excerpt": "private excerpt", "source_external_id": "private-provider-id"},
        config_json={"branch_admission": {"state": "forked"}},
    ).model_copy(update=changes)
    store.save_session(session)
    return session


@pytest.mark.asyncio
async def test_source_link_tracks_rename_archive_and_missing_source_without_leaking_context(setup):
    store, _, req, _ = setup
    target = branch(store)
    source = store.get_session("discussion")
    source.title = "Renamed source"
    source.archived_at = datetime.now(UTC)
    source.origin_instance_id = "source-owner"
    store.save_session(source)
    origin = (await get_conversation_origin(req(), target.id))["origin"]
    assert origin["source_available"] and origin["source_archived"]
    assert origin["source_title"] == "Renamed source"
    assert origin["source_instance_id"] == "source-owner"
    assert origin["native_state"] == "forked"
    assert "private" not in str(origin)
    target.initiating_workflow["source_session_id"] = "missing"
    store.save_session(target)
    unavailable = (await get_conversation_origin(req(), target.id))["origin"]
    assert unavailable["source_available"] is False
    assert unavailable["source_title"] == "Original plan"


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"principal_id": "user:other"}, {"realm_id": "other"}])
async def test_provenance_does_not_grant_source_access(setup, changes):
    store, _, req, _ = setup
    target = branch(store)
    source = store.get_session("discussion").model_copy(update=changes | {"title": "Private source title"})
    store.save_session(source)
    origin = (await get_conversation_origin(req(), target.id))["origin"]
    assert not origin["source_available"]
    assert origin["source_title"] == "Original plan"
    with pytest.raises(HTTPException) as denied:
        await get_conversation_origin(req(principal="user:other"), target.id)
    assert denied.value.status_code == 403


def test_old_branches_and_unknown_native_outcomes_remain_distinct_from_copy_and_empty(setup):
    store, _, _, _ = setup
    target = branch(store)
    target.initiating_workflow.pop("source_title")
    target.config_json["branch_admission"]["state"] = "outcome_unknown"
    native = conversation_origin(target)
    assert native["default_title"] == "Branch of Conversation discussi"
    assert native["native_state"] == "outcome_unknown"
    for strategy, label in [("copy", "Context copy"), ("empty", "Fresh chat")]:
        target.initiating_workflow["strategy"] = strategy
        value = conversation_origin(target)
        assert value["label"] == label and value["native_state"] is None
    assert conversation_origin(store.get_session("discussion")) is None


@pytest.mark.parametrize("strategy", [[], {}, 1, None, "unknown"])
def test_unrecognized_workflow_metadata_does_not_break_session_lists(setup, strategy):
    store, _, _, _ = setup
    target = branch(store)
    target.initiating_workflow["strategy"] = strategy
    assert conversation_origin(target) is None
    target.initiating_workflow["strategy"] = "native"
    target.config_json["branch_admission"] = "legacy-unknown"
    assert conversation_origin(target)["native_state"] is None


@pytest.mark.parametrize("strategy", ["native", "copy", "empty", None])
@pytest.mark.parametrize("include_transcript", [True, False])
def test_runtime_snapshots_preserve_origin_for_recovery_and_live_metadata(setup, strategy, include_transcript):
    store, manager, _, _ = setup
    target = branch(store)
    target.initiating_workflow["strategy"] = strategy
    runtime = AgentSessionRuntime(manager, target)

    snapshot = runtime.snapshot(include_transcript=include_transcript)

    assert snapshot["session"]["conversation_origin"] == conversation_origin(target)
    assert snapshot["session"]["id"] == target.id
    assert ("transcript" in snapshot) is include_transcript
    # Display metadata is derived, never persisted back into session state.
    assert "conversation_origin" not in target.model_dump()
