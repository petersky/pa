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
            assert supervisor.verifier_independence() == "relaxed"
    reset_settings()
    reset_store()
    reset_instance_agent()
    with tempfile.TemporaryDirectory() as tmp:
        with TestClient(
            _app(Path(tmp), completion_verifier_independence="strict")
        ) as client:
            supervisor = client.app.state.ctx.require_service("goal_supervisor")
            assert supervisor.verifier_independence() == "strict"


def test_stall_thresholds_are_generous_time_based_settings() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        with TestClient(_app(Path(tmp))) as client:
            supervisor = client.app.state.ctx.require_service("goal_supervisor")
            # Hours of silence, not a handful of 30-second supervision cycles.
            assert supervisor.no_progress_seconds == 2 * 60 * 60
            assert supervisor.stalled_seconds == 8 * 60 * 60


# --------------------------------------------------------------------------
# The Goals page and its operator controls
# --------------------------------------------------------------------------


@pytest.fixture
def quiet_supervisor(monkeypatch):
    """Keep the background supervisor from touching goals the test shapes."""

    from pa.goals.supervisor import GoalSupervisor

    monkeypatch.setattr(GoalSupervisor, "run_once", lambda self, goal_id=None: [])


def test_a_goal_created_without_a_policy_can_plan_and_ask_before_dispatch(
    quiet_supervisor,
) -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        api = Api(client)
        goal = api.create()
        explicit = api.create(
            {"revision": 1, "autonomy_level": 1, "permitted_actions": []}
        )

    policy = goal["policy"]
    assert policy["autonomy_level"] == 3
    for action in (
        "request_operator",
        "create_work_package",
        "dispatch_work_package",
        "audit_goal",
        "revise_goal",
        "transition_goal",
    ):
        assert action in policy["permitted_actions"]
    # An explicitly supplied policy is never overridden.
    assert explicit["policy"]["autonomy_level"] == 1
    assert explicit["policy"]["permitted_actions"] == []


def test_activate_goes_draft_to_ready_to_active(quiet_supervisor) -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        api = Api(client)
        goal = api.create()
        # What the page used to send: draft straight to active.
        direct = api.transition(goal, "active")
        assert direct.status_code == 409, direct.text
        assert "invalid goal transition: draft -> active" in direct.text

        page = client.get("/goals").text
        assert 'data-goal-transition="active"' in page
        assert 'data-goal-via="ready"' in page

        ready = api.transition(goal, "ready")
        assert ready.status_code == 200, ready.text
        active = api.transition(ready.json(), "active")
        assert active.status_code == 200, active.text
        assert active.json()["state"] == "active"
        # A ready goal activates in one step.
        assert 'data-goal-via="ready"' not in client.get("/goals").text


def _blocked_goal(client: TestClient, api: Api) -> dict:
    """An active goal that the supervisor blocked on a terminally failed package."""

    from pa.goals.models import (
        CreateWorkPackageAction,
        GoalActorRole,
        GoalDriftState,
        GoalProposal,
        GoalSupervisionCheckpoint,
        GoalWorkPackage,
        ProposalStatus,
        WorkPackageState,
    )

    goal = api.create()
    for state in ("ready", "active"):
        response = api.transition(goal, state)
        assert response.status_code == 200, response.text
        goal = response.json()
    service = client.app.state.ctx.require_service("goal_service")

    def context(version: int, key: str, fencing: int | None = None):
        return GoalMutationContext(
            actor_principal="service:goal-supervisor:local",
            authority_instance_id="local",
            idempotency_key=key,
            expected_version=version,
            policy_revision=1,
            fencing_token=fencing,
        )

    leased = service.acquire_lease(
        goal["id"], context(goal["version"], "test-lease"), ttl_seconds=3600
    )
    current = service.get(goal["id"])
    plan = GoalProposal(
        id="plan",
        proposer_principal="service:goal-supervisor:local",
        proposer_role=GoalActorRole.COORDINATOR,
        action=CreateWorkPackageAction(
            title="Implement",
            objective="Do it",
            criterion_ids=[current.criteria[0].id],
        ),
        rationale="test",
        expected_goal_version=1,
        policy_revision=1,
        status=ProposalStatus.APPLIED,
    )
    package = GoalWorkPackage(
        proposal_id="plan",
        title="Implement",
        objective="Do it",
        criterion_ids=[current.criteria[0].id],
        state=WorkPackageState.FAILED,
        attempts=3,
        max_attempts=3,
        result_summary="provider crashed",
    )
    current.supervision.parked_reason = "Executor work 'Implement' failed after 3 attempt(s)."
    current.supervision.drift_state = GoalDriftState.ON_TRACK
    blocked = service.checkpoint_supervision(
        goal["id"],
        GoalSupervisionCheckpoint(
            criteria=current.criteria,
            evidence=[],
            proposals=[plan],
            work_packages=[package],
            operator_interactions=[],
            supervision=current.supervision,
            linked_card_ids=[],
            linked_dispatch_ids=[],
            assumptions=[],
            risks=[],
            strategy_revision=1,
            state=GoalState.BLOCKED,
            progress_summary="blocked",
            reason="test",
        ),
        context(leased.version, "test-block", leased.lease.fencing_token),
    )
    return blocked.model_dump(mode="json")


def test_blocked_goal_offers_resume_and_retry_and_retry_works(
    quiet_supervisor,
) -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        api = Api(client)
        blocked = _blocked_goal(client, api)
        assert blocked["state"] == "blocked"
        page = client.get("/goals").text
        assert "data-goal-retry=" in page
        assert 'data-goal-reason="Resumed by operator"' in page
        assert "failed after 3 attempt(s)" in page  # the reason is on the page

        # The supervisor holds the goal's lease; the operator's control still works.
        retried = api.post(
            f"/api/goals/{blocked['id']}/work-packages/"
            f"{blocked['work_packages'][0]['id']}/retry",
            {},
            version=blocked["version"],
        )

        assert retried.status_code == 200, retried.text
        assert retried.json()["state"] == "active"
        assert retried.json()["work_packages"][0]["state"] == "ready"
        assert retried.json()["supervision"]["parked_reason"] == ""
        # Only blocked or failed work can be retried.
        again = api.post(
            f"/api/goals/{blocked['id']}/work-packages/"
            f"{blocked['work_packages'][0]['id']}/retry",
            {},
            version=retried.json()["version"],
        )
        assert again.status_code == 409, again.text


def test_policy_form_and_audit_form_follow_the_settings(quiet_supervisor) -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        api = Api(client)
        goal = api.create()
        evidence = api.post(
            f"/api/goals/{goal['id']}/evidence",
            {
                "evidence": {
                    "criterion_ids": [goal["criteria"][0]["id"]],
                    "kind": "test",
                    "summary": "passed",
                }
            },
            version=goal["version"],
        )
        assert evidence.status_code == 200, evidence.text
        page = client.get("/goals").text
        assert "data-goal-policy-form" in page
        assert 'name="autonomy_level"' in page
        assert 'value="3" selected' in page
        assert 'name="permitted_action" value="dispatch_work_package" checked' in page
        # By default the owner cannot audit their own goal, so the page says so
        # instead of offering a form that can only fail.
        assert "data-goal-audit-form" not in page
        assert "goal_owner_may_audit" in page

    reset_settings()
    reset_store()
    reset_instance_agent()
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp), goal_owner_may_audit=True)) as client,
    ):
        api = Api(client)
        goal = api.create()
        api.post(
            f"/api/goals/{goal['id']}/evidence",
            {
                "evidence": {
                    "criterion_ids": [goal["criteria"][0]["id"]],
                    "kind": "test",
                    "summary": "passed",
                }
            },
            version=goal["version"],
        )
        page = client.get("/goals").text
        assert "data-goal-audit-form" in page
        assert 'name="criterion_id"' in page


def test_pause_over_http_works_while_the_supervisor_holds_the_lease(
    quiet_supervisor,
) -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        api = Api(client)
        blocked = _blocked_goal(client, api)
        service = client.app.state.ctx.require_service("goal_service")
        assert service.get(blocked["id"]).lease.active()

        paused = api.transition(blocked, "paused")

        assert paused.status_code == 200, paused.text
        assert paused.json()["state"] == "paused"
