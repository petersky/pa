from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from pa.domain.models import CardCreate
from pa.execution.selection import SelectionConstraints, SelectionError, SelectionPolicy
from pa.execution.selection_audit import record_summary
from pa.execution.selection_jobs import summary_selection
from tests.test_execution_selection_api import selection_app  # noqa: F401


def test_summary_retry_binding_current_constraints_and_reported_identity(selection_app):  # noqa: F811 - shared pytest fixture
    _client, app, service = selection_app
    ctx = app.state.ctx
    card = ctx.store.create_card(CardCreate(title="Summary policy", auto_enrich=False))
    configuration = SimpleNamespace(
        provider="openai",
        base_url="https://example.invalid/v1",
        api_key="test-secret",
        auth_source="dedicated",
        model="summary-alias",
        enabled=True,
    )
    args = {"input_hash": "same-input", "prompt_version": "summary-test"}
    first = summary_selection(ctx, card, configuration, **args)
    configuration.model = "changed-default"
    assert summary_selection(ctx, card, configuration, **args) == first
    record_summary(
        ctx,
        card,
        first,
        attempt=1,
        completed=True,
        latency_ms=10,
        confirmation={"model_id": "reported-snapshot"},
        attempted_at=datetime.now(UTC),
    )
    recorded = service.store.attempts(first["decision_id"])[0]
    assert recorded["provider_confirmation"]["state"] == "reported_identity_differs"
    assert recorded["provider_confirmation"]["requested_model_confirmed"] is False
    assert service.store.evidence(card.realm_id, "user:local")[0].tests_passed is None
    service.store.save_policy(
        card.realm_id,
        SelectionPolicy(constraints=SelectionConstraints(models=["different-model"])),
        expected_revision=1,
    )
    with pytest.raises(SelectionError):
        summary_selection(ctx, card, configuration, **args)
    service.store.save_policy(card.realm_id, SelectionPolicy(), expected_revision=2)
    configuration.api_key = "rotated-secret"
    with pytest.raises(SelectionError, match="original connection"):
        summary_selection(ctx, card, configuration, **args)
    regenerated = summary_selection(ctx, card, configuration, **args, force=True)
    assert regenerated["decision_id"] != first["decision_id"]
    assert regenerated["selected"]["model"] == "changed-default"
