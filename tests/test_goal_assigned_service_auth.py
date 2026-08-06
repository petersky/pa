from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from pa.acp.environment import (
    assigned_service_mcp_environment,
    assigned_service_session_capability,
)
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession, FleetInstance
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.execution.dispatch import (
    DispatchRecord,
    GoalDispatchProvenance,
)
from pa.goals.advanced_models import (
    GoalAssignedServiceScope,
    GoalUsage,
    GovernanceMutationContext,
    ProviderGoalAssignment,
)
from pa.goals.governance import (
    GoalAssignedServiceCredentialError,
    GoalGovernanceService,
)
from pa.goals.models import (
    CreateWorkPackageAction,
    GoalActorRole,
    GoalBudget,
    GoalCreate,
    GoalCriterion,
    GoalEvidence,
    GoalEvidenceCreate,
    GoalMutationContext,
    GoalPolicy,
    GoalProposalCreate,
    GoalSupervisionCheckpoint,
    GoalWorkPackage,
    WorkPackageState,
)
from pa.goals.service import GoalService
from pa.instance.agent_session import reset_instance_agent
from pa.modules.fleet import (
    AssignedServiceProxyRequest,
    _apply_assigned_service_operation,
    _assigned_goal_projection,
    _assigned_local_dispatch,
    _assigned_mcp_environment_for_session,
    _peer_authority_json,
)
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


@pytest.fixture(autouse=True)
def _reset_pa_singletons():
    reset_settings()
    reset_store()
    reset_instance_agent()
    yield
    reset_instance_agent()
    reset_store()
    reset_settings()


@dataclass
class SeededAssignedServices:
    goal_id: str
    criterion_id: str
    other_criterion_id: str
    executor_run_id: str
    verifier_run_id: str
    executor_token: str
    verifier_token: str
    executor_scope: GoalAssignedServiceScope
    verifier_scope: GoalAssignedServiceScope


class _FakeMcp:
    def __init__(self) -> None:
        self.functions: dict[str, object] = {}

    def tool(self):
        def register(fn):
            self.functions[fn.__name__] = fn
            return fn

        return register


def _goal_context(goal, key: str, *, actor: str = "user:operator"):
    return GoalMutationContext(
        actor_principal=actor,
        authority_instance_id="authority-a",
        idempotency_key=key,
        expected_version=goal.version,
        policy_revision=goal.policy.revision,
        fencing_token=(goal.lease.fencing_token or None),
    )


def _governance_context(goal, state, key: str):
    return GovernanceMutationContext(
        actor_principal="agent:supervisor",
        authority_instance_id="authority-a",
        idempotency_key=key,
        expected_version=state.version,
        goal_version=goal.version,
        policy_revision=goal.policy.revision,
        fencing_token=goal.lease.fencing_token,
    )


def _seed_assigned_services(
    goals: GoalService,
    governance: GoalGovernanceService,
    *,
    target_instance_id: str = "authority-a",
    credential_ttl_seconds: int = 3600,
    verifier_all_criteria: bool = True,
) -> SeededAssignedServices:
    criterion = GoalCriterion(
        description="assigned service identity is authoritative",
        verification_method="authenticated API probe",
        evidence_requirement="one exact evidence record",
    )
    other_criterion = GoalCriterion(
        description="another package's private criterion",
        verification_method="separate authenticated probe",
        evidence_requirement="one separately scoped evidence record",
    )
    goal = goals.create(
        GoalCreate(
            objective="Exercise assigned Goal services",
            criteria=[criterion, other_criterion],
            policy=GoalPolicy(
                autonomy_level=4,
                permitted_actions=[
                    "provider.goal.assign",
                    "request_operator",
                    "record_evidence",
                    "audit_goal",
                    "revise_strategy",
                    "transition_goal",
                ],
                allowed_provider_ids=["codex"],
            ),
            budget=GoalBudget(max_actions=20, max_concurrency=10),
        ),
        GoalMutationContext(
            actor_principal="user:operator",
            authority_instance_id="authority-a",
            idempotency_key="create-assigned-service-goal",
            expected_version=0,
            policy_revision=1,
        ),
    )
    proposal_ids: list[str] = []
    for role in (GoalActorRole.EXECUTOR, GoalActorRole.VERIFIER):
        package_criteria = (
            [criterion.id, other_criterion.id]
            if role == GoalActorRole.VERIFIER and verifier_all_criteria
            else [criterion.id]
        )
        goal = goals.submit_proposal(
            goal.id,
            GoalProposalCreate(
                proposer_principal="user:operator",
                proposer_role=GoalActorRole.COORDINATOR,
                action=CreateWorkPackageAction(
                    title=f"{role.value.title()} package",
                    objective=f"Run the {role.value} path",
                    criterion_ids=package_criteria,
                    role=role,
                ),
                rationale=f"Seed the {role.value} assignment.",
                expected_goal_version=goal.version,
                policy_revision=goal.policy.revision,
            ),
            _goal_context(goal, f"seed-{role.value}-proposal"),
        )
        proposal_ids.append(goal.proposals[-1].id)
    goal = goals.acquire_lease(
        goal.id,
        _goal_context(goal, "acquire-assigned-service-lease", actor="agent:supervisor"),
        ttl_seconds=3600,
    )

    runs = []
    state = governance.get_state(goal.id)
    for role in (GoalActorRole.EXECUTOR, GoalActorRole.VERIFIER):
        state, run, decision = governance.assign_provider(
            goal.id,
            ProviderGoalAssignment(
                provider_id="codex",
                role=role,
                estimated_usage=GoalUsage(actions=1),
            ),
            _governance_context(goal, state, f"assign-{role.value}-run"),
        )
        assert run is not None and decision.reservation_id
        state, run, _decision = governance.launch_provider(
            goal.id,
            run.id,
            _governance_context(goal, state, f"launch-{role.value}-run"),
        )
        runs.append(run)

    executor_package = GoalWorkPackage(
        proposal_id=proposal_ids[0],
        title="Executor package",
        objective="Produce the evidence",
        criterion_ids=[criterion.id],
        role=GoalActorRole.EXECUTOR,
        state=WorkPackageState.RUNNING,
        session_id="executor-session",
        executor_service_id=runs[0].executor_principal,
    )
    verifier_package = GoalWorkPackage(
        proposal_id=proposal_ids[1],
        title="Verifier package",
        objective="Audit the evidence independently",
        criterion_ids=(
            [criterion.id, other_criterion.id]
            if verifier_all_criteria
            else [criterion.id]
        ),
        depends_on=[executor_package.id],
        role=GoalActorRole.VERIFIER,
        state=WorkPackageState.RUNNING,
        session_id="verifier-session",
        verifier_service_id=runs[1].executor_principal,
    )
    goal = goals.checkpoint_supervision(
        goal.id,
        GoalSupervisionCheckpoint(
            criteria=goal.criteria,
            evidence=goal.evidence,
            proposals=goal.proposals,
            work_packages=[executor_package, verifier_package],
            operator_interactions=goal.operator_interactions,
            supervision=goal.supervision,
            linked_card_ids=goal.linked_card_ids,
            linked_dispatch_ids=goal.linked_dispatch_ids,
            assumptions=goal.assumptions,
            risks=goal.risks,
            strategy_revision=goal.strategy_revision,
            state=goal.state,
            progress_summary=goal.progress_summary,
            reason="Persist exact assigned service packages",
        ),
        _goal_context(
            goal,
            "checkpoint-assigned-service-packages",
            actor="agent:supervisor",
        ),
    )

    scopes = [
        GoalAssignedServiceScope(
            goal_id=goal.id,
            work_package_id=package.id,
            run_id=run.id,
            session_id=package.session_id,
            provider_id=run.provider_id,
            target_instance_id=target_instance_id,
            authority_instance_id="authority-a",
            fencing_token=goal.lease.fencing_token,
            assigned_service_principal=run.executor_principal,
            service_role=run.role,
        )
        for package, run in zip(
            (executor_package, verifier_package), runs, strict=True
        )
    ]
    tokens: list[str] = []
    state = governance.get_state(goal.id)
    for scope in scopes:
        binding, token = governance.issue_assigned_service_credential(
            scope,
            _governance_context(
                goal,
                state,
                f"issue-{scope.service_role.value}-credential",
            ),
            ttl_seconds=credential_ttl_seconds,
        )
        assert token not in binding.model_dump_json()
        assert binding.credential_digest
        tokens.append(token)
    return SeededAssignedServices(
        goal_id=goal.id,
        criterion_id=criterion.id,
        other_criterion_id=other_criterion.id,
        executor_run_id=runs[0].id,
        verifier_run_id=runs[1].id,
        executor_token=tokens[0],
        verifier_token=tokens[1],
        executor_scope=scopes[0],
        verifier_scope=scopes[1],
    )


def _app(
    path: Path,
    *,
    instance_id: str = "authority-a",
    instance_name: str = "Authority A",
    instance_url: str = "http://authority-a.test:8080",
    sync_token: str = "",
):
    kernel = Kernel.boot(
        settings=Settings(
            data_dir=path,
            instance_id=instance_id,
            instance_name=instance_name,
            instance_url=instance_url,
            fleet_id="assigned-proxy-fleet",
            sync_token=sync_token,
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
            auth_required=True,
        )
    )
    app = kernel.build_app()
    # Most tests enter lifespan through TestClient. The two-instance proxy probe
    # drives the authority through ASGITransport without a second event loop.
    app.state.kernel = kernel
    app.state.ctx = kernel.ctx
    return app


def test_assigned_routes_derive_executor_and_verifier_identity() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(Path(tmp))) as client:
        goals = client.app.state.ctx.require_service("goal_service")
        governance = client.app.state.ctx.require_service("goal_governance")
        seeded = _seed_assigned_services(goals, governance)

        goal = goals.get(seeded.goal_id)
        spoofed = client.post(
            "/api/goal-assigned-service/proposals",
            params={"expected_version": goal.version, "policy_revision": 1},
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": "spoof-assigned-proposal",
            },
            json={
                "proposer_principal": seeded.verifier_scope.assigned_service_principal,
                "proposer_role": "verifier",
                "action": {
                    "kind": "request_operator",
                    "prompt": "Need one bounded answer",
                    "allow_freeform": True,
                },
                "rationale": "Exercise the assigned proposal path.",
                "expected_goal_version": goal.version,
                "policy_revision": 1,
            },
        )
        assert spoofed.status_code == 422, spoofed.text

        nested_identity = client.post(
            "/api/goal-assigned-service/proposals",
            params={"expected_version": goal.version, "policy_revision": 1},
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": "reject-nested-evidence-identity",
            },
            json={
                "action": {
                    "kind": "record_evidence",
                    "evidence": {
                        "criterion_ids": [seeded.criterion_id],
                        "kind": "test",
                        "summary": "Caller identity must not enter the schema.",
                        "recorded_by_principal": "user:forged",
                    },
                },
                "rationale": "Exercise nested identity rejection.",
                "expected_goal_version": goal.version,
                "policy_revision": 1,
            },
        )
        assert nested_identity.status_code == 422, nested_identity.text

        proposal = client.post(
            "/api/goal-assigned-service/proposals",
            params={"expected_version": goal.version, "policy_revision": 1},
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": "assigned-executor-proposal",
                "X-PA-Actor": seeded.verifier_scope.assigned_service_principal,
                "X-PA-Authority-Instance": "forged-authority",
                "X-PA-Goal-Fencing-Token": "999999",
            },
            json={
                "action": {
                    "kind": "request_operator",
                    "prompt": "Need one bounded answer",
                    "allow_freeform": True,
                },
                "rationale": "Exercise the assigned proposal path.",
                "expected_goal_version": goal.version,
                "policy_revision": 1,
            },
        )
        assert proposal.status_code == 202, proposal.text
        persisted = proposal.json()["proposals"][-1]
        assert persisted["proposer_principal"] == (
            seeded.executor_scope.assigned_service_principal
        )
        assert persisted["proposer_role"] == "executor"

        # The proposal wake is intentional; wait for that bounded supervision
        # checkpoint before starting the next separately idempotent mutation.
        time.sleep(0.5)
        goal = goals.get(seeded.goal_id)
        cross_package = client.post(
            "/api/goal-assigned-service/evidence",
            params={"expected_version": goal.version, "policy_revision": 1},
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": "reject-cross-package-evidence",
            },
            json={
                "evidence": {
                    "criterion_ids": [seeded.other_criterion_id],
                    "kind": "test",
                    "summary": "Must remain outside the executor package.",
                }
            },
        )
        assert cross_package.status_code == 403, cross_package.text

        evidence = client.post(
            "/api/goal-assigned-service/evidence",
            params={"expected_version": goal.version, "policy_revision": 1},
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": "assigned-executor-evidence",
            },
            json={
                "evidence": {
                    "criterion_ids": [seeded.criterion_id],
                    "kind": "test",
                    "summary": "The exact assigned-service probe passed.",
                    "provenance": {
                        "authority_instance_id": "forged-authority",
                        "provider_id": "forged-provider",
                    },
                },
                "criterion_verdicts": {seeded.criterion_id: "satisfied"},
            },
        )
        assert evidence.status_code == 200, evidence.text
        recorded = evidence.json()["evidence"][-1]
        assert recorded["producer_role"] == "executor"
        assert recorded["producer_service_id"] == (
            seeded.executor_scope.assigned_service_principal
        )
        assert recorded["provenance"]["authority_instance_id"] == "authority-a"
        assert recorded["provenance"]["provider_id"] == "codex"

        goal = goals.get(seeded.goal_id)
        goal = goals.add_evidence(
            goal.id,
            GoalEvidenceCreate(
                evidence=GoalEvidence(
                    criterion_ids=[seeded.other_criterion_id],
                    kind="test",
                    summary="The owner separately satisfied the verifier-only criterion.",
                ),
                criterion_verdicts={seeded.other_criterion_id: "satisfied"},
            ),
            _goal_context(goal, "owner-verifier-only-evidence"),
        )
        other_evidence = goal.evidence[-1]
        audit_body = {
            "criterion_verdicts": {
                seeded.criterion_id: "satisfied",
                seeded.other_criterion_id: "satisfied",
            },
            "evidence_ids": [recorded["id"], other_evidence.id],
            "explanation": "Independently verify the assigned executor evidence.",
        }
        executor_audit = client.post(
            "/api/goal-assigned-service/audit",
            params={"expected_version": goal.version, "policy_revision": 1},
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": "reject-executor-audit",
            },
            json=audit_body,
        )
        assert executor_audit.status_code == 403, executor_audit.text

        verifier_audit = client.post(
            "/api/goal-assigned-service/audit",
            params={"expected_version": goal.version, "policy_revision": 1},
            headers={
                "Authorization": f"GoalRun {seeded.verifier_token}",
                "Idempotency-Key": "assigned-verifier-audit",
            },
            json=audit_body,
        )
        assert verifier_audit.status_code == 200, verifier_audit.text
        audit = verifier_audit.json()["audit"]
        assert audit["auditor_principal"] == (
            seeded.verifier_scope.assigned_service_principal
        )
        assert audit["verifier_service_id"] == (
            seeded.verifier_scope.assigned_service_principal
        )


@pytest.mark.parametrize(
    "action",
    (
        {
            "kind": "request_operator",
            "prompt": "Choose a bounded next step.",
            "allow_freeform": True,
        },
        {
            "kind": "revise_strategy",
            "summary": "Use the bounded fallback strategy.",
        },
        {
            "kind": "transition_goal",
            "state": "blocked",
            "reason": "Wait for the bounded dependency.",
        },
        {
            "kind": "record_evidence",
            "evidence": {
                "criterion_ids": ["__assigned_criterion__"],
                "kind": "test",
                "summary": "Nested assigned evidence is canonicalized.",
            },
        },
    ),
)
def test_strict_assigned_proposal_actions_reach_canonical_route(action: dict) -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(Path(tmp))) as client:
        goals = client.app.state.ctx.require_service("goal_service")
        governance = client.app.state.ctx.require_service("goal_governance")
        seeded = _seed_assigned_services(goals, governance)
        goal = goals.get(seeded.goal_id)
        payload = dict(action)
        if payload["kind"] == "record_evidence":
            payload["evidence"] = {
                **payload["evidence"],
                "criterion_ids": [seeded.criterion_id],
            }
        response = client.post(
            "/api/goal-assigned-service/proposals",
            params={
                "expected_version": goal.version,
                "policy_revision": goal.policy.revision,
            },
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": f"strict-positive-{payload['kind']}",
            },
            json={
                "action": payload,
                "rationale": "Exercise strict validation followed by canonical conversion.",
                "expected_goal_version": goal.version,
                "policy_revision": goal.policy.revision,
            },
        )

        assert response.status_code == 202, response.text
        persisted = response.json()["proposals"][-1]
        assert persisted["action"]["kind"] == payload["kind"]
        assert persisted["proposer_principal"] == (
            seeded.executor_scope.assigned_service_principal
        )
        if payload["kind"] == "record_evidence":
            assert persisted["action"]["evidence"]["provenance"][
                "work_package_id"
            ] == seeded.executor_scope.work_package_id


def test_assigned_credential_rejects_cross_scope_and_terminal_run() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(Path(tmp))) as client:
        goals = client.app.state.ctx.require_service("goal_service")
        governance = client.app.state.ctx.require_service("goal_governance")
        seeded = _seed_assigned_services(goals, governance)

        with pytest.raises(GoalAssignedServiceCredentialError, match="another goal"):
            governance.resolve_assigned_service_credential(
                seeded.executor_token,
                expected_goal_id="another-goal",
            )
        with pytest.raises(GoalAssignedServiceCredentialError, match="another run"):
            governance.resolve_assigned_service_credential(
                seeded.executor_token,
                expected_run_id=seeded.verifier_run_id,
            )

        goal = goals.get(seeded.goal_id)
        state = governance.get_state(seeded.goal_id)
        cross_run = client.post(
            f"/api/goals/{seeded.goal_id}/providers/progress",
            params={
                "expected_version": state.version,
                "goal_version": goal.version,
                "policy_revision": 1,
            },
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": "reject-cross-run-credential",
            },
            json={
                "run_id": seeded.verifier_run_id,
                "state": "running",
                "summary": "Must not cross provider-run scope.",
            },
        )
        assert cross_run.status_code == 403, cross_run.text

        completed = client.post(
            "/api/goal-assigned-service/progress",
            params={
                "expected_autonomy_version": state.version,
                "goal_version": goal.version,
                "policy_revision": 1,
            },
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": "complete-assigned-executor-run",
            },
            json={
                "state": "completed",
                "summary": "Assigned executor completed.",
                "cumulative_usage": {"actions": 1},
            },
        )
        assert completed.status_code == 200, completed.text
        rejected = client.post(
            "/api/goal-assigned-service/evidence",
            params={"expected_version": goal.version, "policy_revision": 1},
            headers={
                "Authorization": f"GoalRun {seeded.executor_token}",
                "Idempotency-Key": "terminal-run-evidence",
            },
            json={
                "evidence": {
                    "criterion_ids": [seeded.criterion_id],
                    "kind": "test",
                    "summary": "Must not be accepted after terminal state.",
                }
            },
        )
        assert rejected.status_code == 403, rejected.text


def test_assigned_goal_read_is_scoped_bounded_and_paginated() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(Path(tmp))) as client:
        goals = client.app.state.ctx.require_service("goal_service")
        governance = client.app.state.ctx.require_service("goal_governance")
        seeded = _seed_assigned_services(goals, governance)
        authorization = governance.resolve_assigned_service_credential(
            seeded.verifier_token
        )
        authorization.goal.constraints = [f"constraint-{index}" for index in range(60)]
        authorization.goal.criteria[0].description = "x" * 9_000

        first = _assigned_goal_projection(authorization, offset=0, limit=1)
        second = _assigned_goal_projection(authorization, offset=1, limit=1)

        assert len(first["criteria"]) == len(second["criteria"]) == 1
        assert first["page"] == {
            "offset": 0,
            "limit": 1,
            "next_offset": 1,
            "criteria_total": 2,
            "evidence_total": 0,
        }
        assert second["page"]["next_offset"] is None
        assert len(first["constraints"]) == 50
        assert first["context_totals"]["constraints"] == 60
        assert len(first["criteria"][0]["description"]) == 8_000
        serialized = repr(first)
        for forbidden in (
            "authority-a",
            seeded.verifier_scope.assigned_service_principal,
            seeded.verifier_scope.session_id,
            seeded.verifier_scope.work_package_id,
        ):
            assert forbidden not in serialized


def test_subset_verifier_cannot_completion_audit_unseen_criteria() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(Path(tmp))) as client:
        goals = client.app.state.ctx.require_service("goal_service")
        governance = client.app.state.ctx.require_service("goal_governance")
        seeded = _seed_assigned_services(
            goals,
            governance,
            verifier_all_criteria=False,
        )
        goal = goals.get(seeded.goal_id)
        before = len(goals.events(seeded.goal_id))
        response = client.post(
            "/api/goal-assigned-service/audit",
            params={
                "expected_version": goal.version,
                "policy_revision": goal.policy.revision,
            },
            headers={
                "Authorization": f"GoalRun {seeded.verifier_token}",
                "Idempotency-Key": "reject-subset-verifier-completion-audit",
            },
            json={
                "criterion_verdicts": {seeded.criterion_id: "satisfied"},
                "evidence_ids": [],
                "explanation": "A subset verifier cannot close the whole goal.",
            },
        )

        assert response.status_code == 409, response.text
        assert "every criterion" in response.text
        assert len(goals.events(seeded.goal_id)) == before


def test_assigned_credential_rejects_expiry_fence_session_and_provider_drift() -> None:
    cases = ("expired", "fence", "session", "provider")
    for case in cases:
        with tempfile.TemporaryDirectory() as tmp:
            now = [datetime(2026, 8, 5, 12, tzinfo=UTC)]
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            log = EventLog(objects, root, "authority-a")
            projection = CardProjection(root / "authority.db", log)
            goals = GoalService(projection, "authority-a", clock=lambda: now[0])
            governance = GoalGovernanceService(
                projection,
                "authority-a",
                goals,
                clock=lambda: now[0],
                progress_token_secret="assigned-service-test-secret",
            )
            seeded = _seed_assigned_services(
                goals,
                governance,
                credential_ttl_seconds=1,
            )
            if case == "expired":
                now[0] += timedelta(seconds=2)
            elif case == "fence":
                goal = goals.get(seeded.goal_id)
                goal = goals.release_lease(
                    goal.id,
                    _goal_context(
                        goal,
                        "release-assigned-service-lease",
                        actor="agent:supervisor",
                    ),
                )
                goals.acquire_lease(
                    goal.id,
                    _goal_context(
                        goal,
                        "rotate-assigned-service-fence",
                        actor="agent:supervisor",
                    ),
                    ttl_seconds=3600,
                )
            elif case == "session":
                goal = goals.get(seeded.goal_id)
                packages = [item.model_copy(deep=True) for item in goal.work_packages]
                packages[0].session_id = "replacement-session"
                goals.checkpoint_supervision(
                    goal.id,
                    GoalSupervisionCheckpoint(
                        criteria=goal.criteria,
                        evidence=goal.evidence,
                        proposals=goal.proposals,
                        work_packages=packages,
                        operator_interactions=goal.operator_interactions,
                        supervision=goal.supervision,
                        linked_card_ids=goal.linked_card_ids,
                        linked_dispatch_ids=goal.linked_dispatch_ids,
                        assumptions=goal.assumptions,
                        risks=goal.risks,
                        strategy_revision=goal.strategy_revision,
                        state=goal.state,
                        progress_summary=goal.progress_summary,
                        reason="Simulate a durable replacement session",
                    ),
                    _goal_context(
                        goal,
                        "replace-assigned-service-session",
                        actor="agent:supervisor",
                    ),
                )
            else:
                goal = goals.get(seeded.goal_id)
                state = governance.get_state(goal.id)

                def change_provider(_goal, current):
                    run = next(
                        item
                        for item in current.provider_runs
                        if item.id == seeded.executor_run_id
                    )
                    run.provider_id = "cursor"
                    return {"run_id": run.id, "provider_id": run.provider_id}

                governance._mutate_state(
                    goal.id,
                    _governance_context(goal, state, "drift-provider-binding"),
                    "goal_governance.test_provider_drifted",
                    change_provider,
                    operation={"run_id": seeded.executor_run_id},
                )
            with pytest.raises(GoalAssignedServiceCredentialError):
                governance.resolve_assigned_service_credential(
                    seeded.executor_token
                )


def test_partitioned_target_cannot_commit_control_authority_credential() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root, "authority-a")
        authority_projection = CardProjection(root / "authority.db", log)
        authority_goals = GoalService(authority_projection, "authority-a")
        authority_governance = GoalGovernanceService(
            authority_projection,
            "authority-a",
            authority_goals,
            progress_token_secret="partition-test-secret",
        )
        seeded = _seed_assigned_services(
            authority_goals,
            authority_governance,
            target_instance_id="target-b",
        )

        target_projection = CardProjection(root / "target.db", log)
        target_projection.rebuild_from_log("default")
        target_goals = GoalService(target_projection, "target-b")
        target_governance = GoalGovernanceService(
            target_projection,
            "target-b",
            target_goals,
            progress_token_secret="partition-test-secret",
        )
        before_target = len(target_goals.events(seeded.goal_id))
        with pytest.raises(
            GoalAssignedServiceCredentialError,
            match="control authority",
        ):
            target_governance.resolve_assigned_service_credential(
                seeded.executor_token
            )
        assert len(target_goals.events(seeded.goal_id)) == before_target

        authorization = authority_governance.resolve_assigned_service_credential(
            seeded.executor_token
        )
        goal = authority_goals.get(seeded.goal_id)
        before_authority = len(authority_goals.events(seeded.goal_id))
        authority_goals.add_evidence(
            goal.id,
            GoalEvidenceCreate(
                evidence=GoalEvidence(
                    criterion_ids=[seeded.criterion_id],
                    kind="test",
                    summary="Only the control authority records this evidence.",
                )
            ),
            GoalMutationContext(
                actor_principal=authorization.scope.assigned_service_principal,
                authority_instance_id=authorization.scope.authority_instance_id,
                idempotency_key="authority-only-assigned-write",
                expected_version=goal.version,
                policy_revision=goal.policy.revision,
                fencing_token=authorization.scope.fencing_token,
            ),
        )
        events = authority_goals.events(seeded.goal_id)
        assert len(events) == before_authority + 1
        assert sum(
            event["event_type"] == "goal.evidence_recorded" for event in events
        ) == 1


@pytest.mark.asyncio
async def test_progress_handoff_between_telemetry_and_notification_fails_closed() -> None:
    authorization = MagicMock()
    authorization.scope.target_instance_id = "target-b"
    record = MagicMock()
    record.dispatch_id = "dispatch-bound"
    progress_result = MagicMock(sequence=7)
    progress_result.model_dump.return_value = {"sequence": 7}
    progress = SimpleNamespace(explicit=AsyncMock(return_value=progress_result))
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                ctx=SimpleNamespace(services={"progress_service": progress})
            )
        ),
        state=SimpleNamespace(),
    )
    handoff = HTTPException(
        status_code=403,
        detail={"code": "assigned_dispatch_provenance_mismatch"},
    )
    with (
        patch(
            "pa.modules.fleet._assigned_authority_dispatch",
            side_effect=[(authorization, record), handoff],
        ) as resolve,
        patch(
            "pa.modules.fleet._create_operator_input_notification",
            new_callable=AsyncMock,
        ) as notify,
        pytest.raises(HTTPException) as raised,
    ):
        await _apply_assigned_service_operation(
            request,
            "dispatch-bound",
            "progress",
            AssignedServiceProxyRequest(
                payload={
                    "schema_version": 1,
                    "phase": "blocked",
                    "summary": "Waiting for an operator decision.",
                    "operator_input": "Choose the bounded next action.",
                    "idempotency_key": "progress-before-handoff",
                }
            ),
        )

    assert raised.value.status_code == 403
    assert resolve.call_count == 2
    progress.explicit.assert_awaited_once()
    notify.assert_not_awaited()


def test_target_session_capability_is_bound_to_live_durable_dispatch(
    tmp_path: Path,
) -> None:
    app = _app(
        tmp_path,
        instance_id="target-b",
        instance_name="Target B",
        instance_url="http://target-b.test:8080",
    )
    ctx = app.state.ctx
    dispatch_id = "dispatch-bound"
    session_id = "session-bound"
    record = DispatchRecord(
        dispatch_id=dispatch_id,
        mutation_id="target-local-binding",
        authority_instance_id="authority-a",
        authority_url="http://authority-a.test:8080",
        target_instance_id="target-b",
        session_id=session_id,
        state="running",
        goal_provenance=GoalDispatchProvenance(
            goal_id="goal-bound",
            goal_version=1,
            policy_revision=1,
            authority_instance_id="authority-a",
            fencing_token=4,
            action_reservation_id="reservation-bound",
            actor_principal="agent:supervisor",
            provider_id="codex",
            resolved_target_instance_id="target-b",
        ),
    )
    ctx.require_service("dispatch_store").put(record)
    session = AgentSession(
        id=session_id,
        agent_name="codex",
        authority_instance_id="authority-a",
        dispatch_id=dispatch_id,
        status="connected",
    )
    ctx.store.save_session(session)
    runtime = SimpleNamespace(connected=True, _closed=False)
    ctx.services["instance_agent"] = SimpleNamespace(get=lambda _id: runtime)
    capability = assigned_service_session_capability(
        secret=ctx.settings.session_secret,
        dispatch_id=dispatch_id,
        session_id=session_id,
        target_instance_id="target-b",
    )

    def request(*, token: str = capability, asserted: str = dispatch_id):
        return SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)),
            state=SimpleNamespace(assigned_session_capability=token),
            headers={
                "X-PA-Assigned-Session-ID": session_id,
                "X-PA-Assigned-Dispatch-ID": asserted,
            },
        )

    assert _assigned_local_dispatch(request()).dispatch_id == dispatch_id
    assert _assigned_mcp_environment_for_session(
        ctx.settings,
        ctx.require_service("dispatch_store"),
        session,
    ) == assigned_service_mcp_environment(
        dispatch_id=dispatch_id,
        session_id=session_id,
    )
    for forged in (
        request(token="pas1." + "0" * 64),
        request(asserted="another-dispatch"),
    ):
        with pytest.raises(HTTPException) as rejected:
            _assigned_local_dispatch(forged)
        assert rejected.value.status_code == 403

    runtime.connected = False
    with pytest.raises(HTTPException) as disconnected:
        _assigned_local_dispatch(request())
    assert disconnected.value.status_code == 403

    record.state = "completed"
    ctx.require_service("dispatch_store").put(record)
    with pytest.raises(RuntimeError, match="durable assigned dispatch binding"):
        _assigned_mcp_environment_for_session(
            ctx.settings,
            ctx.require_service("dispatch_store"),
            session,
        )


@pytest.mark.asyncio
async def test_authority_proxy_rejects_response_identity_mismatch(tmp_path: Path) -> None:
    app = _app(
        tmp_path,
        instance_id="target-b",
        instance_name="Target B",
        instance_url="http://target-b.test:8080",
        sync_token="fleet-token",
    )
    ctx = app.state.ctx
    ctx.require_service("fleet_registry").upsert_instance(
        FleetInstance(
            instance_id="authority-a",
            name="Authority A",
            url="http://authority-a.test:8080",
        ),
        actor="test",
    )
    response = httpx.Response(
        200,
        request=httpx.Request("POST", "http://authority-a.test:8080/api/fleet/test"),
        headers={"X-PA-Instance-ID": "impostor"},
        json={"accepted": True},
    )
    client = SimpleNamespace(request=AsyncMock(return_value=response))
    ctx.services["fleet_http_client"] = client
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)),
    )

    with pytest.raises(HTTPException) as rejected:
        await _peer_authority_json(
            request,
            "authority-a",
            "POST",
            "dispatch-jobs/dispatch-bound/assigned-service/evidence",
            body={"idempotency_key": "identity-mismatch"},
        )

    assert rejected.value.status_code == 502
    assert rejected.value.detail["code"] == "authority_identity_mismatch"
