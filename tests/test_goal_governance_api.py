from __future__ import annotations

import copy
import json
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.modules.goals import _projection_conflicts_public


@pytest.fixture(autouse=True)
def _reset_pa_singletons():
    reset_settings()
    reset_store()
    reset_instance_agent()
    yield
    reset_instance_agent()
    reset_store()
    reset_settings()


def _app(path: Path, *, sync_token: str | None = None):
    return Kernel.boot(
        settings=Settings(
            data_dir=path,
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
            sync_token=sync_token or "",
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


def test_goal_identity_and_authority_come_from_the_authenticated_request() -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        assert client.get("/").status_code == 200
        csrf = client.cookies.get("pa_csrf")
        headers = {
            "X-CSRF-Token": csrf,
            "Idempotency-Key": "create-audit-goal",
            "X-PA-Actor": "agent:spoofed",
            "X-PA-Authority-Instance": "forged-instance",
        }
        created = client.post(
            "/api/goals",
            params={"expected_version": 0, "policy_revision": 1},
            headers=headers,
            json={
                "objective": "Bind the audit principal",
                "owner_principal": "user:operator",
                "criteria": [
                    {
                        "description": "Audit identity is authenticated",
                        "verification_method": "HTTP contract test",
                        "evidence_requirement": "passing response assertions",
                    }
                ],
            },
        )
        assert created.status_code == 201, created.text
        goal = created.json()
        assert goal["owner_principal"] == "user:local"
        assert goal["policy"]["authored_by"] == "user:local"
        criterion_id = goal["criteria"][0]["id"]
        evidence = client.post(
            f"/api/goals/{goal['id']}/evidence",
            params={"expected_version": 1, "policy_revision": 1},
            headers={**headers, "Idempotency-Key": "record-audit-evidence"},
            json={
                "evidence": {
                    "criterion_ids": [criterion_id],
                    "kind": "test",
                    "summary": "Authenticated audit contract passed",
                }
            },
        )
        assert evidence.status_code == 200, evidence.text
        assert evidence.json()["evidence"][0]["recorded_by_principal"] == "user:local"
        assert (
            evidence.json()["evidence"][0]["recorded_by_instance_id"] == "local"
        )
        evidence_id = evidence.json()["evidence"][0]["id"]
        audit_payload = {
            "auditor_principal": "agent:spoofed",
            "criterion_verdicts": {criterion_id: "satisfied"},
            "evidence_ids": [evidence_id],
            "explanation": "Verify the authenticated request boundary",
        }
        rejected = client.post(
            f"/api/goals/{goal['id']}/audit",
            params={"expected_version": 2, "policy_revision": 1},
            headers={**headers, "Idempotency-Key": "reject-spoofed-auditor"},
            json=audit_payload,
        )
        assert rejected.status_code == 409, rejected.text

        audit_payload.pop("auditor_principal")
        owner_audit = client.post(
            f"/api/goals/{goal['id']}/audit",
            params={"expected_version": 2, "policy_revision": 1},
            headers={**headers, "Idempotency-Key": "authenticated-auditor"},
            json=audit_payload,
        )
        detail = client.get(f"/api/goals/{goal['id']}")
        autonomy = client.get(f"/api/goals/{goal['id']}/autonomy")

    assert owner_audit.status_code == 409, owner_audit.text
    assert "independent of the goal owner" in owner_audit.json()["detail"]
    assert detail.status_code == 200, detail.text
    assert all(
        event["authority_instance_id"] == "local"
        for event in detail.json()["events"]
    )
    assert autonomy.status_code == 200, autonomy.text
    assert all(
        item["authority_instance_id"] == "local"
        for item in autonomy.json()["action_reservations"]
    )


def test_blank_goal_reference_ids_are_rejected_at_the_http_boundary() -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        assert client.get("/").status_code == 200
        csrf = client.cookies.get("pa_csrf")
        headers = {
            "X-CSRF-Token": csrf,
            "Idempotency-Key": "blank-reference-goal",
        }
        blank_criterion = client.post(
            "/api/goals",
            params={"expected_version": 0, "policy_revision": 1},
            headers=headers,
            json={
                "objective": "Reject blank graph references",
                "criteria": [
                    {
                        "id": "   ",
                        "description": "ids are nonblank",
                        "verification_method": "HTTP validation",
                        "evidence_requirement": "422 response",
                    }
                ],
            },
        )
        assert blank_criterion.status_code == 422, blank_criterion.text

        created = client.post(
            "/api/goals",
            params={"expected_version": 0, "policy_revision": 1},
            headers={**headers, "Idempotency-Key": "valid-reference-goal"},
            json={
                "objective": "Reject blank evidence references",
                "criteria": [
                    {
                        "description": "evidence ids are nonblank",
                        "verification_method": "HTTP validation",
                        "evidence_requirement": "422 response",
                    }
                ],
            },
        )
        assert created.status_code == 201, created.text
        goal = created.json()
        blank_evidence = client.post(
            f"/api/goals/{goal['id']}/evidence",
            params={"expected_version": 1, "policy_revision": 1},
            headers={**headers, "Idempotency-Key": "blank-evidence-id"},
            json={
                "evidence": {
                    "id": "",
                    "criterion_ids": [goal["criteria"][0]["id"]],
                    "kind": "test",
                    "summary": "This must not enter the event ledger",
                }
            },
        )
        blank_criterion_reference = client.post(
            f"/api/goals/{goal['id']}/evidence",
            params={"expected_version": 1, "policy_revision": 1},
            headers={**headers, "Idempotency-Key": "blank-criterion-reference"},
            json={
                "evidence": {
                    "criterion_ids": ["\t"],
                    "kind": "test",
                    "summary": "Blank references are invalid",
                }
            },
        )

    assert blank_evidence.status_code == 422, blank_evidence.text
    assert blank_criterion_reference.status_code == 422, blank_criterion_reference.text


def test_goal_mutation_retry_releases_crash_stranded_reservation() -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        assert client.get("/").status_code == 200
        csrf = client.cookies.get("pa_csrf")
        headers = {
            "X-CSRF-Token": csrf,
            "Idempotency-Key": "crash-safe-goal",
        }
        created = client.post(
            "/api/goals",
            params={"expected_version": 0, "policy_revision": 1},
            headers=headers,
            json={
                "objective": "Release a reservation after process recovery",
                "criteria": [
                    {
                        "description": "retry is terminally reconciled",
                        "verification_method": "crash injection",
                        "evidence_requirement": "one durable evidence record",
                    }
                ],
            },
        )
        goal = created.json()
        governance = client.app.state.ctx.require_service("goal_governance")
        original_release = governance.release_action
        crash_once = True

        def crash_after_commit(*args, **kwargs):
            nonlocal crash_once
            if crash_once and kwargs.get("reason") == "goal mutation committed":
                crash_once = False
                raise RuntimeError("injected crash before terminal release")
            return original_release(*args, **kwargs)

        governance.release_action = crash_after_commit
        evidence_request = {
            "params": {"expected_version": 1, "policy_revision": 1},
            "headers": {
                **headers,
                "Idempotency-Key": "crash-after-evidence-commit",
            },
            "json": {
                "evidence": {
                    "criterion_ids": [goal["criteria"][0]["id"]],
                    "kind": "test",
                    "summary": "The evidence commit itself is durable",
                }
            },
        }
        with pytest.raises(RuntimeError, match="injected crash"):
            client.post(f"/api/goals/{goal['id']}/evidence", **evidence_request)
        governance.release_action = original_release

        retried = client.post(f"/api/goals/{goal['id']}/evidence", **evidence_request)
        autonomy = client.get(f"/api/goals/{goal['id']}/autonomy")
        detail = client.get(f"/api/goals/{goal['id']}")

    assert retried.status_code == 200, retried.text
    assert len(retried.json()["evidence"]) == 1
    state = autonomy.json()
    assert state["usage"]["actions"] == 1
    assert len(state["action_reservations"]) == 1
    assert state["action_reservations"][0]["state"] == "released"
    assert state["action_reservations"][0]["release_reason"] == (
        "goal mutation committed"
    )
    assert [item["event_type"] for item in detail.json()["events"]].count(
        "goal.evidence_recorded"
    ) == 1


def test_goal_mutation_retry_replaces_a_released_precommit_attempt() -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        assert client.get("/").status_code == 200
        csrf = client.cookies.get("pa_csrf")
        created = client.post(
            "/api/goals",
            params={"expected_version": 0, "policy_revision": 1},
            headers={
                "X-CSRF-Token": csrf,
                "Idempotency-Key": "precommit-retry-goal",
            },
            json={
                "objective": "Retry a failed precommit mutation",
                "criteria": [
                    {
                        "description": "replacement reservation succeeds",
                        "verification_method": "injected precommit failure",
                        "evidence_requirement": "one durable evidence record",
                    }
                ],
            },
        )
        goal = created.json()
        service = client.app.state.ctx.require_service("goal_service")
        original_add = service.add_evidence
        fail_once = True

        def fail_before_commit(*args, **kwargs):
            nonlocal fail_once
            if fail_once:
                fail_once = False
                raise RuntimeError("injected precommit failure")
            return original_add(*args, **kwargs)

        service.add_evidence = fail_before_commit
        request = {
            "params": {"expected_version": 1, "policy_revision": 1},
            "headers": {
                "X-CSRF-Token": csrf,
                "Idempotency-Key": "retry-precommit-evidence",
            },
            "json": {
                "evidence": {
                    "criterion_ids": [goal["criteria"][0]["id"]],
                    "kind": "test",
                    "summary": "Replacement attempt committed",
                }
            },
        }
        with pytest.raises(RuntimeError, match="injected precommit"):
            client.post(f"/api/goals/{goal['id']}/evidence", **request)
        service.add_evidence = original_add
        retried = client.post(f"/api/goals/{goal['id']}/evidence", **request)
        autonomy = client.get(f"/api/goals/{goal['id']}/autonomy").json()

    assert retried.status_code == 200, retried.text
    assert len(autonomy["action_reservations"]) == 2
    assert [item["state"] for item in autonomy["action_reservations"]] == [
        "released",
        "released",
    ]
    assert autonomy["action_reservations"][0]["actual_usage"]["actions"] == 0
    assert autonomy["action_reservations"][1]["actual_usage"]["actions"] == 1
    assert autonomy["usage"]["actions"] == 1


def test_completed_goal_mutation_replays_exact_payload_and_rejects_changed_body(
) -> None:
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp))) as client,
    ):
        assert client.get("/").status_code == 200
        csrf = client.cookies.get("pa_csrf")
        created = client.post(
            "/api/goals",
            params={"expected_version": 0, "policy_revision": 1},
            headers={
                "X-CSRF-Token": csrf,
                "Idempotency-Key": "completed-replay-goal",
            },
            json={
                "objective": "Replay one completed mutation exactly",
                "criteria": [
                    {
                        "description": "payload identity is stable",
                        "verification_method": "API replay",
                        "evidence_requirement": "one evidence record",
                    }
                ],
            },
        )
        goal = created.json()
        request = {
            "params": {"expected_version": 1, "policy_revision": 1},
            "headers": {
                "X-CSRF-Token": csrf,
                "Idempotency-Key": "completed-evidence-replay",
            },
            "json": {
                "evidence": {
                    "criterion_ids": [goal["criteria"][0]["id"]],
                    "kind": "test",
                    "summary": "Canonical completed evidence",
                }
            },
        }
        first = client.post(f"/api/goals/{goal['id']}/evidence", **request)
        exact = client.post(f"/api/goals/{goal['id']}/evidence", **request)
        changed_request = copy.deepcopy(request)
        changed_request["json"]["evidence"]["summary"] = "Changed evidence body"
        changed = client.post(
            f"/api/goals/{goal['id']}/evidence", **changed_request
        )
        detail = client.get(f"/api/goals/{goal['id']}").json()

    assert first.status_code == 200, first.text
    assert exact.status_code == 200, exact.text
    assert len(exact.json()["evidence"]) == 1
    assert changed.status_code == 409, changed.text
    assert "different governed mutation" in changed.json()["detail"]
    assert [item["event_type"] for item in detail["events"]].count(
        "goal.evidence_recorded"
    ) == 1


def test_provider_launch_credential_rejects_shared_fleet_origin_spoof() -> None:
    fleet_token = "shared-fleet-token-with-enough-entropy-123456789"
    with (
        tempfile.TemporaryDirectory() as tmp,
        TestClient(_app(Path(tmp), sync_token=fleet_token)) as client,
    ):
        assert client.get("/").status_code == 200
        csrf = client.cookies.get("pa_csrf")
        base_headers = {"X-CSRF-Token": csrf}
        created = client.post(
            "/api/goals",
            params={"expected_version": 0, "policy_revision": 1},
            headers={**base_headers, "Idempotency-Key": "provider-goal"},
            json={
                "objective": "Bind provider progress to its launched run",
                "criteria": [
                    {
                        "description": "provider identity is non-spoofable",
                        "verification_method": "run credential",
                        "evidence_requirement": "authenticated progress",
                    }
                ],
                "policy": {
                    "revision": 1,
                    "autonomy_level": 4,
                    "permitted_actions": ["provider.goal.assign"],
                    "allowed_provider_ids": ["codex"],
                },
                "budget": {"max_actions": 5, "max_concurrency": 1},
            },
        )
        assert created.status_code == 201, created.text
        goal = created.json()
        leased = client.post(
            f"/api/goals/{goal['id']}/lease",
            params={
                "expected_version": goal["version"],
                "policy_revision": 1,
                "ttl_seconds": 120,
            },
            headers={**base_headers, "Idempotency-Key": "provider-lease"},
        )
        assert leased.status_code == 200, leased.text
        goal = leased.json()
        fence_headers = {
            **base_headers,
            "X-PA-Goal-Fencing-Token": str(goal["lease"]["fencing_token"]),
        }
        assigned = client.post(
            f"/api/goals/{goal['id']}/providers/assign",
            params={
                "expected_version": 0,
                "goal_version": goal["version"],
                "policy_revision": 1,
            },
            headers={**fence_headers, "Idempotency-Key": "provider-assign"},
            json={
                "provider_id": "codex",
                "available_commands": ["goal"],
                "estimated_usage": {"actions": 1},
            },
        )
        assert assigned.status_code == 200, assigned.text
        run = assigned.json()["run"]
        assert "invocation" not in run
        assert run["launch_required"] is True
        autonomy_before_launch = client.get(f"/api/goals/{goal['id']}/autonomy").json()
        unlaunched = next(
            item
            for item in autonomy_before_launch["provider_runs"]
            if item["id"] == run["id"]
        )
        assert "invocation" not in unlaunched
        assert "progress_credential_hash" not in unlaunched
        assert unlaunched["launch_required"] is True
        portfolio = client.get("/api/goal-governance/portfolio").json()
        portfolio_state = next(
            item["autonomy"]
            for item in portfolio["goals"]
            if item["goal"]["id"] == goal["id"]
        )
        portfolio_run = next(
            item for item in portfolio_state["provider_runs"] if item["id"] == run["id"]
        )
        assert "invocation" not in portfolio_run
        assert "progress_credential_hash" not in portfolio_run
        launched = client.post(
            f"/api/goals/{goal['id']}/providers/{run['id']}/launch",
            params={
                "expected_version": assigned.json()["autonomy_version"],
                "goal_version": goal["version"],
                "policy_revision": 1,
            },
            headers={**fence_headers, "Idempotency-Key": "provider-launch"},
        )
        assert launched.status_code == 200, launched.text
        launch_body = launched.json()
        credential = launch_body["progress_credential"]
        forged_authority_header_replay = client.post(
            f"/api/goals/{goal['id']}/providers/{run['id']}/launch",
            params={
                "expected_version": assigned.json()["autonomy_version"],
                "goal_version": goal["version"],
                "policy_revision": 1,
            },
            headers={
                **base_headers,
                "X-PA-Authority-Instance": "different-instance",
                "X-PA-Goal-Fencing-Token": str(goal["lease"]["fencing_token"]),
                "Idempotency-Key": "provider-launch",
            },
        )
        valid_replay = client.post(
            f"/api/goals/{goal['id']}/providers/{run['id']}/launch",
            params={
                "expected_version": assigned.json()["autonomy_version"],
                "goal_version": goal["version"],
                "policy_revision": 1,
            },
            headers={**fence_headers, "Idempotency-Key": "provider-launch"},
        )
        assert (
            forged_authority_header_replay.status_code == 200
        ), forged_authority_header_replay.text
        assert (
            forged_authority_header_replay.json()["progress_credential"] == credential
        )
        assert valid_replay.status_code == 200, valid_replay.text
        assert valid_replay.json()["progress_credential"] == credential
        progress_params = {
            "expected_version": launch_body["autonomy_version"],
            "goal_version": goal["version"],
            "policy_revision": 1,
        }
        progress_body = {
            "run_id": run["id"],
            "state": "completed",
            "summary": "Provider claim only",
            "cumulative_usage": {"actions": 1},
        }
        spoofed = client.post(
            f"/api/goals/{goal['id']}/providers/progress",
            params=progress_params,
            headers={
                "Authorization": f"Bearer {fleet_token}",
                "X-PA-Origin-Instance-ID": "local",
                "Idempotency-Key": "spoofed-progress",
            },
            json=progress_body,
        )
        accepted = client.post(
            f"/api/goals/{goal['id']}/providers/progress",
            params=progress_params,
            headers={
                "Authorization": f"GoalRun {credential}",
                "X-PA-Origin-Instance-ID": "different-instance",
                "Idempotency-Key": "valid-progress",
            },
            json=progress_body,
        )

    assert spoofed.status_code == 403, spoofed.text
    assert spoofed.json()["detail"]["code"] == "invalid_provider_run_credential"
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["provider_runs"][0]["state"] == "completed"


def test_projection_conflict_reads_do_not_bypass_provider_launch_gate() -> None:
    autonomy = {
        "goal_id": "goal-a",
        "provider_runs": [
            {
                "id": "run-a",
                "launched_at": None,
                "invocation": {"prompt": "execute before apply"},
                "progress_credential_hash": "sensitive-derived-value",
            }
        ],
    }
    public = _projection_conflicts_public(
        [
            {
                "canonical_payload": json.dumps(autonomy),
                "competing_payload": json.dumps(autonomy),
            }
        ]
    )
    for field in ("canonical_payload", "competing_payload"):
        provider_run = json.loads(public[0][field])["provider_runs"][0]
        assert "invocation" not in provider_run
        assert "progress_credential_hash" not in provider_run
