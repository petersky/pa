"""Operator controls on the Goals page: pause, activate, retry (HTTP + page)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.goals.models import GoalMutationContext, GoalState, GoalTransition
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


def _app(path: Path, **overrides):
    return Kernel.boot(
        settings=Settings(
            data_dir=path,
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
            sync_token="",
            **overrides,
        )
    ).build_app()


class Api:
    def __init__(self, client: TestClient) -> None:
        assert client.get("/").status_code == 200
        self.client = client
        self.csrf = client.cookies.get("pa_csrf")
        self.count = 0

    def post(self, path: str, body: dict, *, version: int, policy: int = 1):
        self.count += 1
        return self.client.post(
            path,
            params={"expected_version": version, "policy_revision": policy},
            headers={
                "X-CSRF-Token": self.csrf,
                "Idempotency-Key": f"op-{self.count}",
            },
            json=body,
        )

    def create(self, policy: dict | None = None) -> dict:
        body = {
            "objective": "Pause must always work",
            "criteria": [
                {
                    "description": "stops",
                    "verification_method": "api",
                    "evidence_requirement": "paused state",
                }
            ],
        }
        if policy is not None:
            body["policy"] = policy
        created = self.post("/api/goals", body, version=0)
        assert created.status_code == 201, created.text
        return created.json()

    def fresh(self, goal: dict) -> dict:
        # The live supervisor may have checkpointed the goal in the meantime.
        return self.client.get(f"/api/goals/{goal['id']}").json()["goal"]

    def transition(self, goal: dict, state: str):
        goal = self.fresh(goal)
        return self.post(
            f"/api/goals/{goal['id']}/transition",
            {"state": state, "reason": f"operator {state}"},
            version=goal["version"],
            policy=goal["policy"]["revision"],
        )


def test_pause_works_whatever_the_goal_policy_permits_or_prohibits() -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        api = Api(client)
        goal = api.create(
            {
                "revision": 1,
                "autonomy_level": 4,
                # Nothing here covers a pause, and transitions are prohibited.
                "permitted_actions": ["code.edit"],
                "prohibited_actions": ["transition_goal"],
            }
        )
        service = client.app.state.ctx.require_service("goal_service")
        ready = service.transition(
            goal["id"],
            GoalTransition(state=GoalState.READY, reason="set up"),
            GoalMutationContext(
                actor_principal="user:local",
                authority_instance_id="local",
                idempotency_key="setup-ready",
                expected_version=goal["version"],
                policy_revision=1,
            ),
        )
        # An ordinary transition is correctly refused by that policy ...
        blocked = api.transition(ready.model_dump(mode="json"), "active")
        assert blocked.status_code == 409, blocked.text

        # ... but the deliberate stop control is not.
        paused = api.transition(ready.model_dump(mode="json"), "paused")

        assert paused.status_code == 200, paused.text
        assert paused.json()["state"] == "paused"


def test_verifier_independence_setting_reaches_the_supervisor() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        with TestClient(_app(Path(tmp))) as client:
            supervisor = client.app.state.ctx.require_service("goal_supervisor")
            assert supervisor.verifier_independence == "single_user"
    reset_settings()
    reset_store()
    reset_instance_agent()
    with tempfile.TemporaryDirectory() as tmp:
        with TestClient(
            _app(Path(tmp), goal_verifier_independence="strict")
        ) as client:
            supervisor = client.app.state.ctx.require_service("goal_supervisor")
            assert supervisor.verifier_independence == "strict"


def test_stall_thresholds_are_generous_time_based_settings() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        with TestClient(_app(Path(tmp))) as client:
            supervisor = client.app.state.ctx.require_service("goal_supervisor")
            # Hours of silence, not a handful of 30-second supervision cycles.
            assert supervisor.no_progress_seconds == 2 * 60 * 60
            assert supervisor.stalled_seconds == 8 * 60 * 60
