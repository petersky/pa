"""Bounded authorization and turn results survive PA bookkeeping boundaries."""
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from pa.agent.context import compose_session_prompt
from pa.domain.conversation_issues import ConversationIssues, Issue, IssueEdit, StartIssues
from pa.domain.models import AgentSession, CardCreate, CardLane
from pa.execution.dispatch import DispatchRecord
from pa.execution.disposition import decide_card_disposition, extract_card_disposition
from pa.execution.post_turn import FollowupActionName, PostTurnDecision, PostTurnEvaluator
from pa.modules.conversation_issues import edit_issues, get_issues, start_issues
from tests.test_conversation_issues import setup
from tests.test_local_operator_acceptance import operator_app
from tests.test_post_turn_evaluation import context_for, snapshot


@pytest.mark.asyncio
async def test_later_start_keeps_one_receipt_and_authorized_scope_on_successor(setup, monkeypatch):
    store, manager, req, _ = setup
    captured = store.create_card(CardCreate(title="Captured proposal", body="Creation only: implementation has not been authorized."))
    dispatch = AsyncMock(return_value={"dispatch_id": "worker-1"})
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    await edit_issues(req(), "discussion", IssueEdit(revision=0, issues=[
        Issue(id="fix", title="Fix the selected behavior", detail="Earlier notes: creation only.", deferred=True),
        Issue(id="unrelated", title="Unrelated Inbox idea", deferred=True),
    ]))
    assert store.get_card(captured.id).lane == CardLane.INBOX
    assert dispatch.await_count == 0
    body = StartIssues(request_id="selected-fix-v1", revision=1, issue_ids=["fix"])
    first = await start_issues(req("selected-fix-v1"), "discussion", body)
    card_id = first["requests"][0]["groups"][0]["card_id"]
    source = store.get_session("discussion")
    source.status = "closed"
    store.save_session(source)
    # Reload and exact retries recover the same request, not another worker.
    assert (await get_issues(req(), source.id))["requests"] == first["requests"]
    assert (await start_issues(req(body.request_id), source.id, body))["requests"] == first["requests"]
    assert dispatch.await_count == 1
    assert body.request_id in dispatch.call_args.args[1].message
    assert "Older create-only/deferred notes" in dispatch.call_args.args[1].message
    assert len(store.list_cards()) == 2
    assert not first["issues"][1].get("request_id")
    assert not first["issues"][0]["deferred"] and first["issues"][1]["deferred"]
    for sid in ("worker", "successor"):
        worker = AgentSession(id=sid, agent_name="codex", purpose="automated_run", card_id=card_id, mode_id="workspace-write")
        store.save_session(worker)
        composed = compose_session_prompt(store, manager.settings, worker, "Resume the selected work.")
        receipt = next(p.text for p in composed.prompts if p.key == "agent.context.authorized_work")
        assert body.request_id in receipt and "discussion" in receipt
        assert "fix: Fix the selected behavior" in receipt and "Unrelated" not in receipt
        assert "Older create-only/deferred notes" in receipt
        assert store.get_session(sid).mode_id == "workspace-write"
    assert not ConversationIssues(store).starts_for_cards({captured.id}, realm_id="default")
    assert not ConversationIssues(store).starts_for_cards({card_id}, realm_id="other")


@pytest.mark.parametrize("version", [1, 2])
def test_inbox_is_an_independent_valid_disposition(version):
    value = {"contract": f"pa.card-disposition/v{version}", "lane": "inbox",
             "outcome": "Requirements captured in Inbox. Implementation remains unstarted; the user can select work later.",
             "evidence": {"integration_required": False}}
    if version == 2:
        value["completion_requirement_revision"] = "req-1"
    parsed, error = extract_card_disposition(json.dumps(value))
    assert error is None and parsed["lane"] == "inbox"
    assert parsed["outcome"] == value["outcome"]
    decision = decide_card_disposition(value, current_lane=CardLane.ACTIVE,
                                      completion_requirement={"revision": "req-1"})
    assert decision.applied_lane == CardLane.INBOX and not decision.cleanup_eligible
    item = snapshot(disposition=value, final_outcome_text=value["outcome"], card_lane_after="inbox")
    evaluated = PostTurnEvaluator().evaluate(context_for(item, card={"lane": "inbox"}))
    assert evaluated.decision == PostTurnDecision.FOLLOWUP_RECORD_REQUIRED
    assert [a.name for a in evaluated.recommended_actions] == [FollowupActionName.RECORD_TURN_OUTCOME]
    assert evaluated.lane_appropriate and "Requirements captured" in evaluated.operator_status_text


def test_http_creation_turn_preserves_inbox_independently_of_dispatch_completion(operator_app):
    with TestClient(operator_app) as client:
        ctx = operator_app.state.ctx
        card = ctx.store.create_card(CardCreate(title="Create only", lane="active"))
        ledger = ctx.services["dispatch_store"]
        ledger.put(DispatchRecord(dispatch_id="capture", mutation_id="capture-turn", card_id=card.id,
            realm_id="default", card_version=card.updated_at.isoformat(), card_snapshot=card.model_dump(mode="json"),
            authority_instance_id=ctx.settings.instance_id, authority_url="http://testserver", target_instance_id="target",
            session_id="capture-session", state="running"))
        disposition = {"contract": "pa.card-disposition/v1", "lane": "inbox", "outcome": "Requirements captured; implementation remains unstarted.", "evidence": {"integration_required": False}}
        body = {"mutation_id": "capture-turn", "card_id": card.id, "realm_id": "default", "card_version": card.updated_at.isoformat(),
                "source_instance_id": "target", "session_id": "capture-session", "result": {"card_disposition": disposition}, "disposition": disposition}
        headers = {"Authorization": "Bearer " + ctx.settings.sync_token, "Idempotency-Key": "capture-turn"}
        first = client.post("/api/fleet/dispatch/capture/complete", json=body, headers=headers)
        assert first.status_code == 200, first.text
        again = client.post("/api/fleet/dispatch/capture/complete", json=body, headers=headers)
        assert again.status_code == 200 and again.json()["duplicate"]
        assert ctx.store.get_card(card.id).lane == CardLane.INBOX
        completed = ledger.get("capture")
        assert completed.acknowledged_at and completed.state == "completed"
        assert completed.post_turn_evaluations[-1].decision == PostTurnDecision.FOLLOWUP_RECORD_REQUIRED


def test_mcp_start_retries_use_the_same_http_identity(monkeypatch):
    from pa.mcp.tools.agent_chat import register_mcp
    registered, calls = {}, []
    def tool(*args, **kwargs):
        def decorate(fn):
            registered[fn.__name__] = fn
            return fn
        return decorate
    def request(*args, **kwargs):
        calls.append((args, kwargs))
        return {"request_id": kwargs["json"]["request_id"]}
    monkeypatch.setattr("pa.mcp.local_api.request_local_pa", request)
    register_mcp(SimpleNamespace(tool=tool), SimpleNamespace(settings=object()))
    for _ in range(2):
        assert registered["start_conversation_issues"]("source", 4, ["selected"], "start-same") == {"request_id": "start-same"}
    assert calls[0] == calls[1]
    assert calls[0][1]["headers"]["Idempotency-Key"] == "start-same"


@pytest.mark.asyncio
@pytest.mark.parametrize("valid", [True, False])
async def test_status_reconciliation_preserves_substantive_result(tmp_path, valid):
    from tests.test_completion_reconciliation import CompletionReconciliationTests
    ledger, _, _, outbox, reconciler = CompletionReconciliationTests().make_fixture(tmp_path)
    original = "Research found two causes. Requirements are captured in Inbox; implementation remains unstarted."
    await reconciler.handle_completion("session-1", {"final_outcome_text": original})
    for _ in range(1 if valid else 2):
        record = ledger.get("dispatch-1")
        result = {"queued_prompt_id": record.reconciliation_prompt_id, "final_outcome_text": "Administrative status repair"}
        if valid:
            result["card_disposition"] = {"contract": "pa.card-disposition/v1", "lane": "inbox", "outcome": original, "evidence": {"integration_required": False}}
        else:
            result["card_disposition_error"] = "invalid response"
        await reconciler.handle_completion("session-1", result)
    assert outbox.payloads[-1][1]["final_outcome_text"] == original
    if not valid:
        assert "card_disposition" not in outbox.payloads[-1][1]
        assert ledger.get("dispatch-1").reconciliation_final_excerpt == "Administrative status repair"


def test_successor_dispatch_retains_original_cross_instance_instruction(operator_app, monkeypatch):
    remote = AsyncMock(return_value={"dispatch_id": "successor"})
    monkeypatch.setattr("pa.modules.fleet.start_remote_agent_work", remote)
    with TestClient(operator_app) as client:
        ctx = operator_app.state.ctx
        card = ctx.store.create_card(CardCreate(title="Selected remote fix", lane="active"))
        item = snapshot(card_id=card.id, authority_version=card.updated_at.isoformat(),
                        blockers=["Sandbox denied repository access"])
        evaluation = PostTurnEvaluator().evaluate(context_for(item, card=card.model_dump(mode="json")))
        action = next(a for a in evaluation.recommended_actions if a.name == FollowupActionName.REDISPATCH_CARD)
        original = "Selected scope: fix status. Source conversation: source. Request: original-start."
        ctx.services["dispatch_store"].put(DispatchRecord(
            dispatch_id=item.dispatch_id, mutation_id="original", card_id=card.id, session_id=item.session_id,
            realm_id="default", authority_instance_id=ctx.settings.instance_id, authority_url="http://testserver",
            target_instance_id="target", state="completed", request_payload={"message": original},
            post_turn_evaluations=[evaluation], turn_end_snapshots=[item],
        ))
        client.get("/api/health")
        headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}
        body = {"evaluation_id": evaluation.evaluation_id, "action_id": action.action_id,
                "expected_authority_version": evaluation.observed_authority_version, "idempotency_key": "successor-same-scope",
                "approval": {"target_digest": action.target_digest, "payload_digest": action.payload_digest,
                             "authority_version": evaluation.observed_authority_version,
                             "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(), "principal_id": "user:local"}}
        response = client.post(f"/api/fleet/dispatch-jobs/{item.dispatch_id}/actions/{action.action_id}", json=body, headers=headers)
        assert response.status_code == 202, response.text
        assert original in remote.call_args.args[2].message
        assert remote.call_args.args[2].card_id == card.id
        again = client.post(f"/api/fleet/dispatch-jobs/{item.dispatch_id}/actions/{action.action_id}", json=body, headers=headers)
        assert again.status_code == 202 and remote.await_count == 1
