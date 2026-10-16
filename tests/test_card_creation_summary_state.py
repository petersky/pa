"""Known disabled metadata is settled before the create receipt is published."""
from unittest.mock import patch
from datetime import datetime

from pa.domain.models import CardCreate
from pa.domain.projection import CardProjection
from tests.test_execution_selection_api import selection_app


def test_unconfigured_create_has_one_stable_canonical_version_and_exact_replay(selection_app, tmp_path):
    client, app, _ = selection_app
    store = app.state.ctx.store
    log = store.event_log
    request = {"title": "Stable creation", "body": "Keep the version fence", "auto_enrich": False}
    response = client.post("/api/cards", json=request, headers={"Idempotency-Key": "stable-summary-create"})
    assert response.status_code == 201, response.text
    created = response.json()
    assert created["summary_status"] == "disabled"
    event = log.find_operation_event("default", "stable-summary-create")[2]
    assert event.operation_result["summary_status"] == "disabled"
    assert event.operation_result["updated_at"] == created["updated_at"]
    assert store.get_card(created["id"]).updated_at == datetime.fromisoformat(created["updated_at"])
    # Force the delayed summary task to finish after a caller has read its
    # version. There must be no redundant disabled-state event to invalidate it.
    summary = app.state.ctx.require_service("card_summary_service")
    with patch.object(summary, "_update", side_effect=AssertionError("redundant disabled write")):
        client.portal.call(summary.request, created["id"], "default")
    assert store.get_card(created["id"]).updated_at == datetime.fromisoformat(created["updated_at"])
    replay = client.post("/api/cards", json=request, headers={"Idempotency-Key": "stable-summary-create"})
    assert replay.json() == created
    rebuilt = CardProjection(tmp_path / "summary-receipt-rebuild.db", log)
    rebuilt.rebuild_from_log("default")
    result = rebuilt.get_operation_outcome("stable-summary-create")["result"]
    assert result["summary_status"] == created["summary_status"]
    assert result["updated_at"] == created["updated_at"]
    rebuilt.close_thread_connection()


def test_supplied_or_configured_summaries_keep_existing_initial_state(selection_app):
    _, app, _ = selection_app
    summary = app.state.ctx.require_service("card_summary_service")
    assert summary.initial_summary_state(CardCreate(title="Manual", summary="User summary")) == {}
    with patch.object(summary, "_configured_from_files", return_value=object()):
        assert summary.initial_summary_state(CardCreate(title="Configured")) == {}
    with patch.object(summary, "_configured_from_files", side_effect=OSError("fixture unavailable")):
        assert summary.initial_summary_state(CardCreate(title="Unknown configuration")) == {}
