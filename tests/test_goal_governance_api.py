from __future__ import annotations

import tempfile
from pathlib import Path

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


def test_advanced_goal_http_contract_and_dashboard() -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        assert client.get("/").status_code == 200
        csrf = client.cookies.get("pa_csrf")
        mutation_headers = {
            "X-CSRF-Token": csrf,
            "Idempotency-Key": "create-governed-goal",
            "X-PA-Actor": "user:local",
            "X-PA-Authority-Instance": "local",
        }
        created = client.post(
            "/api/goals",
            params={"expected_version": 0, "policy_revision": 1},
            headers=mutation_headers,
            json={
                "objective": "Exercise the Phase 5 API",
                "criteria": [
                    {
                        "description": "API is governed",
                        "verification_method": "HTTP contract test",
                        "evidence_requirement": "passing response assertions",
                    }
                ],
                "policy": {
                    "revision": 1,
                    "autonomy_level": 3,
                    "permitted_actions": ["code.edit"],
                    "repository_scope": ["petersky/pa"],
                },
                "budget": {"max_cost_usd": 2, "max_actions": 2},
            },
        )
        assert created.status_code == 201, created.text
        goal_id = created.json()["id"]
        decision = client.post(
            f"/api/goals/{goal_id}/actions/authorize",
            params={
                "expected_version": 0,
                "goal_version": 1,
                "policy_revision": 1,
            },
            headers={
                **mutation_headers,
                "Idempotency-Key": "authorize-edit",
            },
            json={
                "action_class": "code.edit",
                "repository": "petersky/pa",
                "estimate": {"actions": 1, "cost_usd": 1},
            },
        )
        providers = client.get("/api/goal-governance/providers")
        portfolio = client.get("/api/goal-governance/portfolio")
        dashboard = client.get("/goals")

    assert decision.status_code == 200, decision.text
    assert decision.json()["decision"]["disposition"] == "authorized"
    assert {item["provider_id"] for item in providers.json()} >= {
        "codex",
        "claude",
        "kimi",
    }
    assert portfolio.status_code == 200
    assert portfolio.json()["goals"][0]["autonomy"]["version"] == 1
    assert dashboard.status_code == 200
    assert "Organization portfolio" in dashboard.text
    assert "Priority 50" in dashboard.text
