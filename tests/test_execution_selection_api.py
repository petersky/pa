from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate, ProjectCreate
from pa.domain.store import reset_store
from pa.execution.selection_service import service_for
from pa.instance.agent_session import reset_instance_agent
from tests.test_execution_selection import candidate


@pytest.fixture
def selection_app(tmp_path):
    reset_settings()
    reset_store()
    reset_instance_agent()
    app = Kernel.boot(
        settings=Settings(
            data_dir=tmp_path, instance_id="local", agent_enabled=False, peers=[]
        )
    ).build_app()
    with TestClient(app) as client:
        service = service_for(app.state.ctx)
        service.store.save_catalog(
            "local", [candidate(observed_at=datetime.now(UTC)).model_dump(mode="json")]
        )
        client.get("/")
        client.headers["X-CSRF-Token"] = client.cookies.get("pa_csrf", "")
        yield client, app, service
    reset_instance_agent()
    reset_store()
    reset_settings()


def test_create_edit_preview_roundtrip_and_stale_defaults(selection_app):
    client, _app, _service = selection_app
    preferences = {"model": {"intent": "required", "value": "model-a"}}
    response = client.post(
        "/api/cards",
        json={
            "title": "selection test",
            "execution_preferences": preferences,
            "auto_enrich": False,
        },
        headers={"Idempotency-Key": "selection-create"},
    )
    assert response.status_code == 201, response.text
    card = response.json()
    assert card["execution_preferences"]["model"] == preferences["model"]
    preview = client.post("/api/execution/preview", json={"card_id": card["id"]})
    assert preview.status_code == 200, preview.text
    assert preview.json()["provenance"]["model"] == "card"
    assert preview.json()["provider_confirmation"]["state"] == "pending"
    current = client.get(f"/api/cards/{card['id']}").json()
    change = client.patch(
        f"/api/cards/{card['id']}",
        json={
            "execution_preferences": {"model": {"intent": "automatic"}},
            "updated_at": current["updated_at"],
            "field_intent": ["execution_preferences"],
        },
        headers={"Idempotency-Key": "selection-edit"},
    )
    assert change.status_code == 200, change.text
    assert (
        client.get(f"/api/cards/{card['id']}").json()["execution_preferences"]["model"][
            "intent"
        ]
        == "automatic"
    )
    stale = client.post(
        "/api/execution/preview",
        json={"card_id": card["id"], "expected_card_version": card["updated_at"]},
    )
    assert stale.status_code == 409
    override = client.post(
        "/api/execution/preview",
        json={
            "card_id": card["id"],
            "execution_preferences": {
                "model": {"intent": "required", "value": "no-such-model"}
            },
        },
    )
    assert override.status_code == 409
    assert override.json()["detail"]["execution_selection"]["alternatives"][0][
        "rejections"
    ] == ["required_model_incompatible_or_unknown"]
    assert (
        client.get(f"/api/cards/{card['id']}").json()["execution_preferences"]["model"][
            "intent"
        ]
        == "automatic"
    )


def test_automatic_catalog_reuses_fresh_evidence(selection_app):
    client, _app, _service = selection_app
    with patch(
        "pa.acp.providers.resolve.list_provider_summaries_bounded",
        side_effect=AssertionError("must not probe a fresh catalog"),
    ):
        for _ in range(3):
            assert client.get("/api/execution/catalog").status_code == 200


def test_card_project_policy_cannot_be_replaced_by_callers(selection_app):
    from pa.execution.selection import SelectionError

    client, app, service = selection_app
    project = app.state.ctx.store.create_project(
        ProjectCreate(
            title="Restricted",
            tool_config={"execution_constraints": {"models": ["forbidden-here"]}},
        )
    )
    card = app.state.ctx.store.create_card(
        CardCreate(
            title="Owned project policy",
            project_id=project.id,
            auto_enrich=False,
        )
    )
    mismatch = client.post(
        "/api/execution/preview", json={"card_id": card.id, "project_id": "another"}
    )
    assert mismatch.status_code == 409
    assert (
        client.get(
            f"/api/execution/defaults?card_id={card.id}&project_id=another"
        ).status_code
        == 409
    )
    with pytest.raises(SelectionError):
        service.resolve(
            candidates=[candidate()],
            principal="user:local",
            realm="default",
            surface="execution",
            card=card,
            project_config={},
        )


def test_receipt_attempt_pagination_is_owned_and_complete(selection_app):
    client, _app, service = selection_app
    result = service.resolve(
        candidates=[candidate()],
        principal="user:local",
        realm="default",
        surface="execution",
        persist=True,
    )
    key = result["decision_id"]
    for index in range(103):
        service.store.record_attempt(f"a-{index}", key, {"index": index})
    first = client.get(f"/api/execution/decisions/{key}").json()
    from pa.execution.selection import validate_reuse

    assert validate_reuse(first["decision"])["decision_id"] == key
    assert first["attempts_page"] == {
        "offset": 0,
        "limit": 100,
        "total": 103,
        "next_offset": 100,
    }
    last = client.get(f"/api/execution/decisions/{key}?offset=100&limit=50").json()
    assert last["attempts"] == [{"index": i} for i in range(100, 103)]
    assert last["attempts_page"]["next_offset"] is None
    assert client.get(f"/api/execution/decisions/{key}?limit=101").status_code == 422


def test_policy_compare_and_swap_and_narrow_controls_render(selection_app):
    client, app, _service = selection_app
    policy = client.get("/api/execution/policy").json()
    result = client.put(
        "/api/execution/policy",
        json={"expected_revision": policy["revision"], "policy": policy},
    )
    assert result.status_code == 200, result.text
    assert result.json()["revision"] == 2
    assert (
        client.put(
            "/api/execution/policy", json={"expected_revision": 1, "policy": policy}
        ).status_code
        == 409
    )
    card = app.state.ctx.store.create_card(CardCreate(title="test", auto_enrich=False))
    for url in [
        "/partials/cards/new",
        f"/partials/cards/{card.id}/detail",
        f"/partials/cards/{card.id}/dispatch",
    ]:
        response = client.get(url)
        assert response.status_code == 200, response.text
        assert "data-execution-preferences" in response.text
