from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate, CardLane
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
            card = app.state.ctx.store.create_card(
                CardCreate(title="Modal dispatch", lane=CardLane.ACTIVE)
            )
            detail = client.get(f"/partials/cards/{card.id}/detail")
            agent = client.get(f"/partials/cards/{card.id}/agent")
            dispatch = client.get(f"/partials/cards/{card.id}/dispatch")
            home = client.get("/")
            work = client.get("/partials/cards?lane=active")

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
        assert "data-card-dispatch-form" not in agent.text
        assert "Durable fleet dispatch" not in agent.text
        assert 'data-card-dispatch-open' in detail.text
        assert dispatch.status_code == 200
        assert 'aria-labelledby="card-dispatch-dialog-title"' in home.text
        assert "data-card-dispatch-form" in dispatch.text
        assert "policy:best_match" in dispatch.text
        assert "policy:least_busy" in dispatch.text
        assert "policy:round_robin" in dispatch.text
        assert "policy:random_eligible" in dispatch.text
        assert "Local instance — Local" in dispatch.text
        assert "slots used" in dispatch.text
        assert "data-card-dispatch-utilization" in dispatch.text
        assert 'name="provider" data-dispatch-provider' in dispatch.text
        assert 'name="model_id" data-dispatch-model' in dispatch.text
        assert 'type="application/json" data-card-dispatch-inventory' in dispatch.text
        assert 'name="provider" placeholder=' not in dispatch.text
        assert 'name="model_id" placeholder=' not in dispatch.text
        assert "documented default" in dispatch.text
        assert 'data-card-dispatch-open' in home.text
        assert 'data-card-dispatch-open' in work.text


def test_card_modal_separates_prompt_backlog_from_execution_slots() -> None:
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
        overview = {
            "nodes": [
                {
                    "id": "local",
                    "name": "Local",
                    "dispatch_capacity": 4,
                    "capabilities": [],
                    "dimensions": {
                        "activity": {
                            "state": "fresh",
                            "value": {
                                "queued_prompts": 9,
                                "capacity": {
                                    "consumed": 1,
                                    "limit": 4,
                                    "source": "configured",
                                },
                            },
                        },
                        "providers": {
                            "state": "fresh",
                            "value": [],
                        },
                    },
                }
            ]
        }
        with (
            patch("pa.fleet.overview.build_overview", return_value=overview),
            TestClient(app) as client,
        ):
            card = app.state.ctx.store.create_card(
                CardCreate(title="Backlogged target")
            )
            response = client.get(f"/partials/cards/{card.id}/dispatch")

    assert response.status_code == 200
    assert "1/4 slots used · 9 prompts queued" in response.text
    assert 'data-capacity-eligible="true"' in response.text


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
                response = client.get(f"/partials/cards/{card.id}/dispatch")
                assert response.status_code == 200
                rendered[state] = response.text

        assert "queued" in rendered["queued"]
        assert "starting session" in rendered["starting_session"]
        assert "Open durable session" in rendered["running"]
        assert "Retry dispatch" in rendered["failed"]
        assert "Target disappeared before admission." in rendered["failed"]
        assert "completed" in rendered["completed"]
        assert "Highest deterministic readiness score." in rendered["running"]
        assert "Dispatch in progress…" in rendered["running"]
        assert "disabled" in rendered["running"]

        script = (
            Path(__file__).parents[1] / "src/pa/server/static/js/spa.js"
        ).read_text()
        assert 'fetch("/api/fleet/dispatch"' in script
        assert "pollCardDispatch" in script
        assert 'dispatchError.code === "card_dispatch_in_progress"' in script
        assert "no_eligible_instance" in script
        assert 'form.elements.model_id.value !== "None"' in script
        assert "refreshCardDispatchSelectors" in script
        assert "candidate.queued" in script
        assert '" queued; "' in script


def test_existing_dispatch_hides_card_item_action_but_detail_opens_status() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = Kernel.boot(settings=Settings(data_dir=Path(tmp), instance_id="local", instance_name="Local", instance_url="http://pa.test:8080", agent_enabled=False, subscribed_realms=["default"], peers=[])).build_app()
        with TestClient(app) as client:
            eligible = app.state.ctx.store.create_card(CardCreate(title="Eligible card"))
            dispatched = app.state.ctx.store.create_card(CardCreate(title="Already dispatched"))
            app.state.ctx.require_service("dispatch_store").put(_record(dispatched.id, "running"))
            cards = client.get("/partials/cards?lane=inbox")
            detail = client.get(f"/partials/cards/{dispatched.id}/detail")

        assert f'aria-label="Dispatch {eligible.title}"' in cards.text
        assert f'aria-label="Dispatch {dispatched.title}"' not in cards.text
        assert "data-card-dispatch-open" in detail.text


def test_shared_modal_preserves_focus_and_workshop_uses_live_dispatch_state() -> None:
    root = Path(__file__).parents[1] / "src/pa/server"
    script = (root / "static/js/spa.js").read_text()
    workshop = (root / "static/js/workshop.js").read_text()
    assert "cardDispatchDialogOpener.focus()" in script
    assert 'addEventListener("cancel"' in script
    assert script.rstrip().endswith("})();")
    assert 'id="card-dispatch-dialog-title"' in (root / "templates/partials/card-dispatch.html").read_text()
    assert "if (card.can_dispatch)" in workshop
    assert "card.dispatch_unavailable_reason" in workshop
    assert "window.PACardDispatch.open" in workshop
    order_button = workshop.split("function orderButton", 1)[1].split(
        "function renderQuery", 1
    )[0]
    assert "data-workshop-dispatch" not in order_button
