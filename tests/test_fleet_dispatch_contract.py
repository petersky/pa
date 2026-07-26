from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
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


def _app(path: Path):
    return Kernel.boot(
        settings=Settings(
            data_dir=path,
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
        )
    ).build_app()


def test_openapi_documents_policy_or_target_idempotency_and_hybrid_auth() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = _app(Path(tmp))
        schema = app.openapi()
    operation = schema["paths"]["/api/fleet/dispatch"]["post"]
    assert "target_instance_id" in operation["description"]
    assert "placement_policy" in operation["description"]
    assert "same target for idempotent retries" in operation["description"]
    assert any(
        parameter["name"] == "Idempotency-Key" for parameter in operation["parameters"]
    )
    assert {"instanceBearer": []} in operation["security"]
    assert operation["responses"]["202"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("/DispatchAdmission")


def test_authority_movement_forwards_unresolved_request_to_selected_authority() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = _app(Path(tmp))
        forwarded = {
            "accepted": True,
            "duplicate": False,
            "dispatch_id": "dispatch-1",
            "dispatch": {
                "dispatch_id": "dispatch-1",
                "target_instance_id": "target",
                "state": "queued",
            },
        }
        with (
            patch(
                "pa.modules.fleet._peer_authority_json",
                autospec=True,
                return_value=forwarded,
            ) as proxy,
            TestClient(app) as client,
        ):
            assert client.get("/").status_code == 200
            response = client.post(
                "/api/fleet/dispatch",
                headers={"X-CSRF-Token": client.cookies.get("pa_csrf")},
                json={
                    "authority_instance_id": "peer-authority",
                    "card_id": "card-on-authority",
                    "placement_policy": "best_match",
                    "idempotency_key": "authority-move-1",
                },
            )
    assert response.status_code == 202, response.text
    assert response.json() == forwarded
    assert proxy.await_count == 1
    assert proxy.await_args.args[1:4] == ("peer-authority", "POST", "dispatch")
    assert proxy.await_args.kwargs["body"]["placement_policy"] == "best_match"
