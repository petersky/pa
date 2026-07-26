from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate
from pa.domain.store import reset_store
from pa.execution.dispatch import DispatchRecord
from pa.instance.agent_session import reset_instance_agent


@pytest.fixture(autouse=True)
def _reset_pa_singletons():
    reset_settings()
    reset_store()
    reset_instance_agent()
    yield
    reset_instance_agent()
    reset_store()
    reset_settings()


def _record(card_id: str, state: str) -> DispatchRecord:
    record = DispatchRecord(
        mutation_id=f"mutation-{card_id}-{state}",
        idempotency_key=f"key-{card_id}-{state}",
        request_fingerprint=f"fingerprint-{card_id}-{state}",
        placement_request_fingerprint=f"fingerprint-{card_id}-{state}",
        card_id=card_id,
        authority_instance_id="local",
        authority_url="http://pa.test:8080",
        target_instance_id="local",
        target_instance_name="Local",
        placement_policy="best_match",
        placement_decision={
            "policy": "best_match",
            "chosen_instance_id": "local",
            "chosen_instance_name": "Local",
            "tie_breaking_reason": "Highest deterministic readiness score.",
        },
    )
    record.state = state
    if state == "running":
        record.session_id = f"session-{card_id}"
    if state == "failed":
        record.last_error = "Target disappeared before admission."
        record.error_code = "instance_unavailable"
        record.recoverable = True
    return record


def test_card_modal_distinguishes_local_start_preference_and_durable_dispatch() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = Kernel.boot(
            settings=Settings(
                data_dir=Path(tmp),
                instance_id="local",
                instance_name="Local",
                instance_url="http://pa.test:8080",
                agent_enabled=False,
                subscribed_realms=["default"],
                peers=[],
            )
        ).build_app()
        with TestClient(app) as client:
            card = app.state.ctx.store.create_card(CardCreate(title="Modal dispatch"))
            detail = client.get(f"/partials/cards/{card.id}/detail")
            agent = client.get(f"/partials/cards/{card.id}/agent")

        assert detail.status_code == 200
        assert "Preferred instance" in detail.text
        assert "routing metadata only" in detail.text
        assert "Any eligible instance" not in detail.text
        assert agent.status_code == 200
        template = (
            Path(__file__).parents[1]
            / "src/pa/server/templates/partials/card-detail-agent.html"
        ).read_text()
        assert "Start local agent" in template
        assert "Resume local agent" in template
        assert "Durable fleet dispatch" in agent.text
        assert "data-card-dispatch-form" in agent.text
        assert "policy:best_match" in agent.text
        assert "policy:least_busy" in agent.text
        assert "policy:round_robin" in agent.text
        assert "policy:random_eligible" in agent.text
        assert "Local instance — Local" in agent.text
        assert "slots used" in agent.text
        assert "data-card-dispatch-utilization" in agent.text
        assert "documented default" in agent.text


def test_card_modal_renders_dispatch_progress_retry_and_session_links() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = Kernel.boot(
            settings=Settings(
                data_dir=Path(tmp),
                instance_id="local",
                instance_name="Local",
                instance_url="http://pa.test:8080",
                agent_enabled=False,
                subscribed_realms=["default"],
                peers=[],
            )
        ).build_app()
        with TestClient(app) as client:
            store = app.state.ctx.require_service("dispatch_store")
            rendered = {}
            for state in (
                "queued",
                "starting_session",
                "running",
                "failed",
                "completed",
            ):
                card = app.state.ctx.store.create_card(CardCreate(title=state))
                record = _record(card.id, state)
                store.put(record)
                response = client.get(f"/partials/cards/{card.id}/agent")
                assert response.status_code == 200
                rendered[state] = response.text

        assert "queued" in rendered["queued"]
        assert "starting session" in rendered["starting_session"]
        assert "Open durable session" in rendered["running"]
        assert "Retry dispatch" in rendered["failed"]
        assert "Target disappeared before admission." in rendered["failed"]
        assert "completed" in rendered["completed"]
        assert "Highest deterministic readiness score." in rendered["running"]

        script = (
            Path(__file__).parents[1] / "src/pa/server/static/js/spa.js"
        ).read_text()
        assert 'fetch("/api/fleet/dispatch"' in script
        assert "pollCardDispatch" in script
        assert "card_dispatch_in_progress" not in script
        assert "no_eligible_instance" in script
