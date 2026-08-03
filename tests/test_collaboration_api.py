from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession
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


def test_collaboration_state_and_command_catalog_are_stable_http_contracts() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = _app(Path(tmp))
        with TestClient(app) as client:
            app.state.ctx.store.save_session(
                AgentSession(
                    id="session-1",
                    agent_name="cursor",
                    authority_instance_id="local",
                    principal_id="user:local",
                    config_json={"values": {"collaboration_mode": "default"}},
                )
            )
            state = client.get("/api/agent/sessions/session-1/collaboration")
            catalog = client.get("/api/agent/sessions/session-1/commands")

    assert state.status_code == 200, state.text
    assert state.json()["supported_modes"] == ["default"]
    assert state.json()["execution_mode_id"] is None
    assert catalog.status_code == 200, catalog.text
    assert catalog.json()["state"] == "ready"
    assert {item["name"] for item in catalog.json()["commands"]} >= {
        "pa:plan",
        "pa:implement",
        "pa:status",
    }


def test_policy_resolution_api_records_provider_fallback_explanation() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = _app(Path(tmp))
        with TestClient(app) as client:
            assert client.get("/").status_code == 200
            csrf = client.cookies.get("pa_csrf")
            saved = client.put(
                "/api/agent/collaboration/policies/plan-cursor",
                headers={"X-CSRF-Token": csrf},
                json={
                    "policy": {
                        "id": "plan-cursor",
                        "scope_type": "provider",
                        "scope_id": "cursor",
                        "strategy": "always_plan_first",
                    }
                },
            )
            decision = client.post(
                "/api/agent/collaboration/policy/resolve",
                headers={"X-CSRF-Token": csrf},
                json={
                    "instance_id": "local",
                    "provider": "cursor",
                    "supported_modes": ["default"],
                },
            )

    assert saved.status_code == 200, saved.text
    assert decision.status_code == 200, decision.text
    assert decision.json()["effective_mode"] == "default"
    assert decision.json()["source_policy_id"] == "plan-cursor"
    assert "does not advertise" in decision.json()["rationale"]
