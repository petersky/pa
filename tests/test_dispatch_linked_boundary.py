"""Reserved dispatch identities must pass through the linked context fence."""
import asyncio

import pytest
from fastapi import HTTPException

from pa.modules.agent_chat import CreateSessionBody, create_session
from pa.execution.selection import SelectionError
from tests.test_dispatch_notification_binding import admitted, _reset_singletons  # noqa: F401


@pytest.mark.parametrize("admitted", ["linked"], indirect=True)
def test_reserved_linked_dispatch_fences_source_before_provider_and_replays(admitted):
    r = admitted
    target = r.store.get_session(r.successor.id)
    source = r.store.get_session(r.linked_source.id)
    boundary = target.config_json["execution_context_boundary_from"]
    assert boundary["source_session_id"] == source.id
    assert boundary["source_dispatch_id"] == source.dispatch_id
    assert boundary["target_session_id"] == r.record.session_id == target.id
    assert source.config_json["execution_context_boundary"]["target_session_id"] == target.id
    assert source.status == "closed"
    assert target.initiating_workflow["context_boundary"]["id"] == boundary["id"]
    assert r.manager.dispatch_store.get(source.dispatch_id).state == "failed"
    leases = r.manager.workspace_manager.list(card_id=r.card.id)
    assert len(leases) == 1 and leases[0].session_id == target.id
    asyncio.run(r.dispatch(r.manager.dispatch_store.get(r.record.dispatch_id)))
    assert len(r.start_calls) == 1
    assert r.store.get_session(target.id).config_json["execution_context_boundary_from"] == boundary
    assert r.store.get_session(target.id).initiating_workflow["context_boundary"] == boundary
    with pytest.raises(SelectionError) as denied:
        asyncio.run(r.manager.create_session(
            session_id="different-reservation", context_source_session_id=source.id,
            dispatch_id=r.record.dispatch_id, card_id=r.card.id,
            project_id=r.card.project_id, principal_id="user:local",
        ))
    assert denied.value.code == "context_boundary_target_conflict"
    assert len(r.start_calls) == 1


@pytest.mark.parametrize("admitted", ["linked"], indirect=True)
@pytest.mark.parametrize("tamper", ["target", "source"])
def test_existing_target_cannot_skip_linked_boundary_validation(admitted, tamper):
    r = admitted
    subject = r.store.get_session(r.successor.id if tamper == "target" else r.linked_source.id)
    subject.config_json.pop("execution_context_boundary_from" if tamper == "target" else "execution_context_boundary")
    r.store.save_session(subject)
    with pytest.raises(HTTPException) as denied:
        asyncio.run(create_session(r.request, CreateSessionBody(
            dispatch_id=r.record.dispatch_id, card_id=r.card.id,
            project_id=r.card.project_id, provider="codex",
        )))
    assert denied.value.detail["code"] == "dispatch_context_boundary_missing"
    assert len(r.start_calls) == 1


def test_reserved_boundary_cannot_adopt_source_or_unrelated_existing_target(admitted):
    r = admitted
    for reserved in (r.old.id, r.successor.id):
        with pytest.raises(SelectionError) as denied:
            asyncio.run(r.manager.create_session(
                session_id=reserved, context_source_session_id=r.old.id,
                dispatch_id="new-linked-dispatch", card_id=r.card.id,
                project_id=r.card.project_id, principal_id="user:local",
            ))
        assert denied.value.code == "context_boundary_target_conflict"
    assert not r.store.get_session(r.old.id).config_json.get("execution_context_boundary")
    assert len(r.start_calls) == 1
