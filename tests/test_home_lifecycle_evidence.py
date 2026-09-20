"""Render real Home/Work projections from durable, isolated lifecycle records."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession, CardCreate, CardLane, CardUpdate
from pa.domain.store import reset_store
from pa.execution.dispatch import DispatchRecord
from pa.execution.progress import CompletionReportV1
from pa.instance.agent_session import reset_instance_agent
from pa.modules.items import _presentation_context_for_cards

LOCAL = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"
OLD = datetime(2026, 9, 6, tzinfo=UTC)


@pytest.fixture
def app(tmp_path):
    reset_settings()
    reset_store()
    reset_instance_agent()
    app = Kernel.boot(settings=Settings(
        data_dir=tmp_path, instance_id=LOCAL, agent_enabled=False,
        telemetry_enabled=False, auth_required=False, sync_token="test-only",
    )).build_app()
    with TestClient(app) as client:
        app.state.test_client = client
        yield app
    reset_instance_agent()
    reset_store()
    reset_settings()


def record_for(app, card, **updates):
    values = dict(
        mutation_id=str(uuid4()), card_id=card.id, card_version=card.updated_at.isoformat(),
        authority_instance_id=LOCAL, target_instance_id=LOCAL, authority_url="http://local",
        state="completed", created_at=OLD, updated_at=OLD,
    )
    values.update(updates)
    record = DispatchRecord(**values)
    app.state.ctx.services["dispatch_store"].put(record)
    return record


def view(app, card):
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": [],
                       "query_string": b"", "app": app})
    return _presentation_context_for_cards(request, [card])[2][card.id]


def test_six_done_cards_with_paused_queues_or_stale_recovery_are_outcomes(app):
    for i in range(6):
        card = app.state.ctx.store.create_card(CardCreate(title=f"Settled {i}", lane=CardLane.DONE))
        session = AgentSession(
            id=str(uuid4()), agent_name="codex", card_id=card.id, purpose="automated_run",
            status="disconnected" if i < 3 else "closed", workflow_state="succeeded",
            origin_instance_id=LOCAL,
            config_json={"durable_runtime": {
                "lifecycle": "recoverable_interrupted", "queue_paused": i < 3,
                "queued_prompts": [{"id": f"p-{n}", "source": "ui"}
                                   for n in range([12, 5, 2, 0, 0, 0][i])],
            }}, recovery_json={"attempts": 3, "next_retry_at": OLD.isoformat()},
        )
        app.state.ctx.store.save_session(session)
        record_for(app, card, session_id=session.id)
        result = view(app, card)
        assert result["group"] == "outcome"
        assert not result["in_motion"]
    html = app.state.test_client.get("/partials/home/sections").text
    assert html.count('data-attention-group="outcome"') == 6
    assert 'data-attention-group="motion"' not in html
    assert "Restoring your work" not in html


@pytest.mark.parametrize("signal", ["queued", "running", "decision"])
def test_done_card_preserves_actual_new_followup_and_pending_decision(app, signal):
    card = app.state.ctx.store.create_card(CardCreate(title="New followup", lane=CardLane.DONE))
    durable = {"queued_prompts": [{"id": "new", "source": "ui"}]}
    if signal == "decision":
        durable = {"pending_interaction": {"kind": "input", "action": "Choose the next step."}}
    session = AgentSession(id=str(uuid4()), agent_name="codex", card_id=card.id,
                           purpose="automated_run", workflow_state="succeeded",
                           origin_instance_id=LOCAL, config_json={"durable_runtime": durable})
    app.state.ctx.store.save_session(session)
    record_for(app, card, session_id=session.id)
    runtime = None
    if signal == "running":
        runtime = SimpleNamespace(_closed=False, connected=True, prompting=True,
                                  _queue=[], _in_flight=SimpleNamespace(id="new"))
    agent = SimpleNamespace(get=lambda _: runtime, startup_complete=True)
    with patch.dict(app.state.ctx.services, instance_agent=agent):
        result = view(app, card)
    assert result["group"] == ("attention" if signal == "decision" else "motion")


def test_new_disposition_replaces_parser_failure_without_erasing_history(app):
    card = app.state.ctx.store.create_card(CardCreate(title="Awaiting acceptance", lane=CardLane.WAITING))
    record = record_for(app, card, reconciliation_state="failed",
                        reconciliation_updated_at=OLD,
                        reconciliation_reason="Could not recover reconciliation prompt",
                        card_disposition_error="Old JSON parser error",
                        final_report=CompletionReportV1(
                            created_at=OLD + timedelta(days=1), outcome="Awaiting production acceptance.",
                            resulting_lane="waiting", card_disposition={
                                "contract": "pa.card-disposition/v1", "lane": "waiting",
                                "outcome": "Awaiting production acceptance.", "evidence": {},
                            }))
    result = view(app, card)
    assert result["state"] == "turn_ended"
    assert result["historical_reconciliation"]
    assert result["summary"] == "Awaiting production acceptance."
    client = app.state.test_client
    html = client.get("/partials/home/sections").text
    assert "Work unfinished" in html
    assert "Awaiting production acceptance." in html
    assert "Old JSON parser error" not in html
    assert "Time unavailable" not in html
    work = client.get("/partials/cards?attention=outcome").text
    assert "Work unfinished" in work
    assert app.state.ctx.services["dispatch_store"].get(record.dispatch_id).card_disposition_error == "Old JSON parser error"


@pytest.mark.parametrize("invalid", ["other_card", "older", "malformed", "wrong_lane", "retry_scheduled", "no_disposition"])
def test_unresolved_reconciliation_is_not_hidden_by_unrelated_or_weak_evidence(app, invalid):
    card = app.state.ctx.store.create_card(CardCreate(title="Unresolved", lane=CardLane.ACTIVE))
    disposition = {"contract": "pa.card-disposition/v1", "lane": "active", "outcome": "Still working", "evidence": {}}
    report = CompletionReportV1(created_at=OLD + timedelta(days=1), outcome="Later report",
                               resulting_lane="active", card_disposition=disposition)
    if invalid == "older":
        report.created_at = OLD - timedelta(seconds=1)
    elif invalid == "malformed":
        report.card_disposition = {"lane": "active"}
    elif invalid == "wrong_lane":
        report.resulting_lane = "done"
    elif invalid == "no_disposition":
        report.card_disposition = None
    record = record_for(app, card, reconciliation_state="failed", reconciliation_updated_at=OLD,
                        reconciliation_reason="Still unresolved", final_report=report,
                        reconciliation_next_retry_at=OLD + timedelta(days=1) if invalid == "retry_scheduled" else None)
    if invalid == "other_card":
        from pa.core.ui.work_presentation import present_work_item
        public = record.public_dict()
        public["card_id"] = "other"
        result = present_work_item(card, dispatch=public)
    else:
        result = view(app, card)
    assert result["attention_code"] == "reconciliation_failure"
    assert not result["historical_reconciliation"]


@pytest.mark.parametrize("outcome", [
    "Acceptance failed: p95 17.167/17.264/17.619ms exceeds 16.67ms.",
    "Feasibility review ended; implementation and acceptance remain outstanding.",
])
def test_legacy_active_card_never_invents_success_from_turn_end(app, outcome):
    card = app.state.ctx.store.create_card(CardCreate(title="Unfinished legacy work", lane=CardLane.ACTIVE))
    assert card.completion_requirement is None
    record_for(app, card, final_report=CompletionReportV1(outcome=outcome))
    html = app.state.test_client.get("/partials/home/sections").text
    assert "Work unfinished" in html
    assert outcome in html
    assert 'data-contextual-work-action="retry"' not in html
    assert view(app, card)["tone"] != "success"


def test_actual_report_blocker_is_actionable_after_turn_end(app):
    card = app.state.ctx.store.create_card(CardCreate(title="Failed acceptance", lane=CardLane.ACTIVE))
    record_for(app, card, final_report=CompletionReportV1(outcome="Turn ended", blockers=["p95 exceeds 16.67ms"]))
    assert view(app, card)["summary"] == "p95 exceeds 16.67ms"
    assert view(app, card)["attention_code"] == "explicit_blocker"


@pytest.mark.parametrize("context", ["exact", "edit", "child", "other_attempt", "conditional", "quoted"])
def test_retry_availability_is_not_current_retry_advice(app, context):
    attempt = str(uuid4())
    body = "Unrelated later edit."
    if context in {"exact", "other_attempt", "conditional", "quoted"}:
        body = "Do not retry stale parent dispatch" + (attempt if context != "other_attempt" else str(uuid4()))
        body += " until tomorrow." if context == "conditional" else "."
    if context == "quoted":
        body = 'An earlier note said: "' + body + '" This is historical narrative.'
    card = app.state.ctx.store.create_card(CardCreate(title="Parent plan", lane=CardLane.ACTIVE))
    record_for(app, card, dispatch_id=attempt, state="failed", recoverable=True, last_error="Earlier admission failed")
    card = app.state.ctx.store.update_card(card.id, CardUpdate(body=body))
    if context == "child":
        app.state.ctx.store.create_card(CardCreate(title="Child complete", parent_id=card.id, lane=CardLane.DONE))
    result = view(app, card)
    assert result["state"] == "retry_required"
    assert not result["historical_reconciliation"]
    assert result["attention"]
    assert result["action"]["kind"] != "retry"
    assert result["relative_time"] != "Time unavailable"


def test_home_evidence_queries_are_batched_and_body_free(app):
    store = app.state.ctx.store
    for i in range(110):
        attempt = str(uuid4())
        card = store.create_card(CardCreate(title=f"Failure {i}", body="Private body " * 1000 +
                                            f"\nDo not retry dispatch {attempt}."))
        record_for(app, card, dispatch_id=attempt, state="failed")
    with (
        patch.object(store, "list_cards", side_effect=AssertionError("full card scan")),
        patch.object(store, "get_card", side_effect=AssertionError("card N+1")),
        patch.object(store, "get_session", side_effect=AssertionError("session N+1")),
        patch.object(app.state.ctx.services["dispatch_store"], "latest_by_card",
                     wraps=app.state.ctx.services["dispatch_store"].latest_by_card) as queries,
    ):
        response = app.state.test_client.get("/partials/home/sections")
    assert response.status_code == 200
    assert queries.call_count == 2
    assert max(len(call.args[0]) for call in queries.call_args_list) <= 100
    assert "Private body" not in response.text
    assert "Showing 6 of 110 actionable cards" in response.text


@pytest.mark.parametrize("dispatch_state,reconciliation,label", [
    ("completion_pending", "not_requested", "Completion pending"),
    ("completed", "pending", "Reconciliation pending"),
])
@pytest.mark.parametrize("scheduled", [False, True])
def test_home_preserves_completion_obligations_without_inventing_motion(app, dispatch_state, reconciliation, label, scheduled):
    card = app.state.ctx.store.create_card(CardCreate(title="Pending completion work", lane=CardLane.DONE))
    session = AgentSession(id=str(uuid4()), agent_name="codex", card_id=card.id,
                           purpose="automated_run", workflow_state="succeeded", status="closed",
                           origin_instance_id=LOCAL)
    app.state.ctx.store.save_session(session)
    record_for(app, card, session_id=session.id, state=dispatch_state,
               reconciliation_state=reconciliation,
               completion_next_retry_at=OLD if scheduled and dispatch_state == "completion_pending" else None,
               reconciliation_next_retry_at=OLD if scheduled and reconciliation == "pending" else None)
    result = view(app, card)
    assert result["state_label"] == label
    assert not result["can_dispatch"]
    assert "No live agent turn" in result["summary"]
    assert result["in_motion"] is scheduled
    assert result["freshness"] != "live"
    html = app.state.test_client.get("/partials/home/sections").text
    assert label in html
    assert ('data-attention-group="motion"' in html) is scheduled


def test_home_placeholder_and_loaded_outcomes_use_the_same_terminology(app):
    for path in ("/", "/partials/home/sections"):
        html = app.state.test_client.get(path).text
        assert "Execution outcomes" in html
        assert "Terminal work" not in html


def test_august_pending_metadata_is_visible_without_new_false_home_activity(app):
    # Exact retained lifecycle shape: Done + acknowledged, but old unscheduled
    # reconciliation and an active workflow on a closed, empty session.
    from pa.execution.session_presentation import build_session_presentation
    card = app.state.ctx.store.create_card(CardCreate(title="August pending reconciliation", lane=CardLane.DONE))
    at = datetime(2026, 8, 22, 17, 35, 21, tzinfo=UTC)
    session = AgentSession(id="b0882ad0-4093-4477-8e28-969d9774fbd8", agent_name="codex", card_id=card.id,
                           purpose="automated_run", workflow_state="active", status="closed",
                           updated_at=at, origin_instance_id=LOCAL,
                           config_json={"durable_runtime": {"queued_prompts": [], "in_flight": None}})
    app.state.ctx.store.save_session(session)
    record = record_for(app, card, dispatch_id="299ccd12-61b6-491b-b3eb-4f218e8d21e4",
                        session_id=session.id, state="completed", acknowledged_at=at,
                        updated_at=at, reconciliation_state="pending",
                        reconciliation_updated_at=at, reconciliation_next_retry_at=None)
    session_view = build_session_presentation(session, dispatch=record)
    assert session_view["display_status"] == "Reconciliation pending"
    assert session_view["next_automatic_action"] is None
    result = view(app, card)
    assert not result["in_motion"]
    assert result["state_label"] == "Reconciliation pending"
    assert result["action"]["label"] == "Inspect completion"
    assert result["occurred_at"] == at.isoformat()
    for path in ("/partials/home/sections", "/partials/cards?attention=outcome"):
        html = app.state.test_client.get(path).text
        assert "Reconciliation pending" in html
        assert 'data-attention-group="motion"' not in html
        assert "No live agent turn or scheduled processing is confirmed" in html
    assert app.state.ctx.services["dispatch_store"].get(record.dispatch_id).reconciliation_state == "pending"


@pytest.mark.parametrize("signal", ["queued", "live", "decision"])
def test_actual_completion_prompt_and_decision_override_pending_metadata(app, signal):
    card = app.state.ctx.store.create_card(CardCreate(title="Actual completion processing", lane=CardLane.DONE))
    prompt = {"id": "completion-prompt", "source": "card-reconciliation:attempt"}
    durable = {"queued_prompts": [prompt]} if signal == "queued" else {}
    if signal == "decision":
        durable["pending_interaction"] = {"kind": "input", "action": "Confirm acceptance."}
    session = AgentSession(id=str(uuid4()), agent_name="codex", card_id=card.id,
                           purpose="automated_run", workflow_state="active", status="closed",
                           origin_instance_id=LOCAL, config_json={"durable_runtime": durable})
    app.state.ctx.store.save_session(session)
    record_for(app, card, session_id=session.id, state="completed", acknowledged_at=OLD,
               reconciliation_state="prompted", reconciliation_prompt_id=prompt["id"])
    runtime = SimpleNamespace(_closed=False, connected=True, prompting=True,
                              _queue=[], _in_flight=SimpleNamespace(id=prompt["id"])) if signal == "live" else None
    with patch.dict(app.state.ctx.services, instance_agent=SimpleNamespace(get=lambda _: runtime, startup_complete=True, connected=runtime is not None)):
        result = view(app, card)
        html = app.state.test_client.get("/partials/home/sections").text
    group = "attention" if signal == "decision" else "motion"
    assert result["group"] == group
    assert f'data-attention-group="{group}"' in html
