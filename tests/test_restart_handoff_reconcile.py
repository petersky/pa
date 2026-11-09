from unittest.mock import MagicMock, patch
from types import SimpleNamespace
import asyncio

import pytest
from fastapi import HTTPException

from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.instance.restart_lifecycle import RestartTransitionConflict
from pa.modules.agent_chat import reconcile_restart_handoff, RestartHandoffReconcileBody
from tests.test_restart_transition_cas import receipt, completion


def dismissed(store):
    original = receipt(store, status="failed", failure_stage="resuming", error="provider failed")
    return store.dismiss_restart_handoff(original.id, session_id=original.session_id)


def reconcile(store, current, **overrides):
    args = dict(session_id=current.session_id, expected_version=current.phase_version,
                owner_instance_id="owner", principal_id="user:operator")
    args.update(overrides)
    return store.reconcile_restart_handoff(current.id, **args)


def test_unknown_stays_dismissed_and_audit_contains_no_prompt(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    current = dismissed(store)
    result = reconcile(store, current)
    assert result["effect"] == "unknown"
    assert result["replayed"] is False
    assert result["next_action"] == "audit_native_history_and_dismissal"
    assert store.get_restart_handoff(current.id) == current
    events = store.list_transcript_events(current.session_id)
    assert events[-1].event_type == "restart_handoff_reconciled"
    assert events[-1].payload["principal_id"] == "user:operator"
    assert "Continue" not in str(events[-1].payload)
    assert not store.pending_restart_continuation(session_id=current.session_id)


def test_closed_session_completion_settles_same_receipt_without_reopen(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    current = dismissed(store)
    session = store.save_session(AgentSession(id=current.session_id, agent_name="codex", status="closed"))
    completion(store)
    result = reconcile(store, current)
    assert result["effect"] == "confirmed"
    settled = store.get_restart_handoff(current.id)
    assert settled.status == "continuation_delivered"
    assert settled.continuation_prompt_id == current.continuation_prompt_id
    assert settled.attempts == current.attempts
    assert settled.transition_history[-1]["status"] == "dismissed"
    assert store.get_session(session.id).status == "closed"
    assert len(store.list_restart_handoffs()) == 1
    with pytest.raises(RestartTransitionConflict):
        reconcile(store, current)


@pytest.mark.parametrize("overrides", [dict(expected_version=999), dict(owner_instance_id="foreign"), dict(session_id="foreign"), dict(principal_id="")])
def test_reconciliation_fences(tmp_path, overrides):
    store = CardProjection(tmp_path / "pa.db")
    current = dismissed(store)
    completion(store)
    with pytest.raises(ValueError):
        reconcile(store, current, **overrides)
    assert store.get_restart_handoff(current.id) == current


def test_other_prompt_and_acceptance_are_not_completion(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    current = dismissed(store)
    store.append_transcript_events([
        TranscriptEvent(session_id=current.session_id, seq=1, event_type="turn_completed", payload={"queued_prompt_id": "other"}),
        TranscriptEvent(session_id=current.session_id, seq=2, event_type="queue_enqueued", payload={"id":current.continuation_prompt_id}),
    ])
    assert reconcile(store, current)["effect"] == "unknown"
    assert store.get_restart_handoff(current.id) == current


def test_route_checks_principal_before_reconciliation():
    request = MagicMock()
    request.app.state.ctx.settings.auth_required = True
    request.state.user.role = "member"
    store = MagicMock()
    store.get_session.return_value = SimpleNamespace(principal_id="user:owner")
    mgr = SimpleNamespace(store=store)
    with patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=mgr), patch("pa.modules.agent_chat.get_principal_id", return_value="user:other"):
        with pytest.raises(HTTPException) as error:
            asyncio.run(reconcile_restart_handoff(request, "session", "receipt", RestartHandoffReconcileBody(expected_version=6)))
    assert error.value.status_code == 403
    store.reconcile_restart_handoff.assert_not_called()


def test_route_uses_server_owner_and_authenticated_principal(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    current = dismissed(store)
    store.save_session(AgentSession(id=current.session_id, agent_name="codex", principal_id="user:owner", status="closed"))
    request = MagicMock()
    request.app.state.ctx.settings.auth_required = True
    request.app.state.ctx.settings.instance_id = "owner"
    request.state.user.role = "member"
    mgr = SimpleNamespace(store=store)
    with patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=mgr), patch("pa.modules.agent_chat.get_principal_id", return_value="user:owner"):
        result = asyncio.run(reconcile_restart_handoff(request, current.session_id, current.id, RestartHandoffReconcileBody(expected_version=current.phase_version)))
        assert result["effect"] == "unknown"
        request.app.state.ctx.settings.instance_id = "foreign-owner"
        with pytest.raises(HTTPException) as error:
            asyncio.run(reconcile_restart_handoff(request, current.session_id, current.id, RestartHandoffReconcileBody(expected_version=current.phase_version)))
        assert error.value.status_code == 409
    events = store.list_transcript_events(current.session_id)
    assert len(events) == 1
    assert events[0].payload["principal_id"] == "user:owner"
