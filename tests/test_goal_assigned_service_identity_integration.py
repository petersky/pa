from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from pa.acp.environment import assigned_service_session_capability
from pa.config import reset_settings
from pa.domain.models import AgentSession, FleetInstance
from pa.domain.store import reset_store
from pa.execution.dispatch import (
    DispatchRecord,
    DispatchStore,
    GoalDispatchProvenance,
    goal_admission_validation_proof,
)
from pa.goals.advanced_models import (
    GoalActionDisposition,
    GoalAssignedServiceCredential,
    GoalAssignedServiceScope,
    GovernanceMutationContext,
)
from pa.goals.governance import (
    ASSIGNED_SERVICE_CREDENTIAL_ENTITY,
    GoalGovernanceConflict,
    GoalGovernanceService,
)
from pa.goals.materialization import GoalExecutionIdentityV1
from pa.goals.models import (
    GoalEvidence,
    GoalEvidenceCreate,
    GoalMutationContext,
    GoalRevision,
    GoalSupervisionCheckpoint,
    WorkPackageState,
)
from pa.goals.projection import list_governance_payloads
from pa.instance.agent_session import reset_instance_agent
from pa.modules.fleet import (
    ASSIGNED_SERVICE_CREDENTIAL_TTL_SECONDS,
    DispatchMaterializeBody,
    _assigned_goal_projection,
    _bind_goal_dispatch_assigned_service_identity,
    _bind_goal_dispatch_execution_identity,
    _proxy_assigned_session_operation,
    _restore_goal_dispatch_execution_identity,
    _target_goal_execution_identity_transition,
)
from tests import test_goal_dispatch_provenance as _provenance_tests
from tests.test_goal_assigned_service_auth import (
    _app as assigned_service_app,
)
from tests.test_goal_assigned_service_auth import (
    _seed_assigned_services,
)


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
class _DispatchSeed:
    helper: object
    goals: object
    governance: GoalGovernanceService
    goal: object
    provenance: GoalDispatchProvenance
    ledger: object
    ctx: object
    record: DispatchRecord
    base_identity: GoalExecutionIdentityV1
    scope: GoalAssignedServiceScope
    binding: GoalAssignedServiceCredential
    final_identity: GoalExecutionIdentityV1


def _governance_context(
    goal,
    governance: GoalGovernanceService,
    key: str,
) -> GovernanceMutationContext:
    return GovernanceMutationContext(
        actor_principal=_provenance_tests.GoalDispatchProvenanceTests.actor,
        authority_instance_id="instance-a",
        idempotency_key=key,
        expected_version=governance.get_state(goal.id).version,
        goal_version=goal.version,
        policy_revision=goal.policy.revision,
        fencing_token=goal.lease.fencing_token,
    )


def _goal_context(goal, key: str) -> GoalMutationContext:
    return GoalMutationContext(
        actor_principal=_provenance_tests.GoalDispatchProvenanceTests.actor,
        authority_instance_id="instance-a",
        idempotency_key=key,
        expected_version=goal.version,
        policy_revision=goal.policy.revision,
        fencing_token=goal.lease.fencing_token,
    )


def _replace_dispatch_after_policy_expansion(
    helper,
    goals,
    governance: GoalGovernanceService,
    goal,
    provenance: GoalDispatchProvenance,
) -> tuple[object, GoalDispatchProvenance]:
    """Keep the dispatch fully governed while allowing its service to add evidence."""

    state = governance.get_state(goal.id)
    prior = next(
        item
        for item in state.action_reservations
        if item.id == provenance.action_reservation_id
    )
    governance.release_action(
        goal.id,
        prior.id,
        _governance_context(goal, governance, "release-pre-evidence-policy"),
        reason="Replace the fixture reservation under the expanded test policy.",
    )
    policy = goal.policy.model_copy(deep=True)
    policy.revision += 1
    policy.permitted_actions = sorted(
        {*policy.permitted_actions, "record_evidence"}
    )
    goal = goals.revise(
        goal.id,
        GoalRevision(
            policy=policy,
            budget=goal.budget.model_copy(update={"max_concurrency": 2}),
            reason="Allow the assigned dispatch to record its scoped evidence.",
        ),
        _goal_context(goal, "expand-dispatch-test-policy"),
    )

    request = prior.request.model_copy(deep=True)
    request.operation_key = "dispatch-with-evidence"
    request.materialization_receipt = None
    request.execution_identity = None
    state, decision = governance.authorize_action(
        goal.id,
        request,
        _governance_context(goal, governance, "reserve-dispatch-with-evidence"),
    )
    assert decision.disposition == GoalActionDisposition.AUTHORIZED
    assert decision.reservation_id is not None
    state, applied = governance.apply_action(
        goal.id,
        decision.reservation_id,
        _governance_context(
            goal,
            governance,
            "apply-dispatch-with-evidence",
        ).model_copy(
            update={"expected_version": state.version}
        ),
    )
    assert applied.disposition == GoalActionDisposition.AUTHORIZED
    reservation = next(
        item
        for item in state.action_reservations
        if item.id == decision.reservation_id
    )
    assert prior.request.materialization_envelope is not None
    assert prior.request.materialization_receipt is not None
    state, reservation = governance.bind_dispatch_materialization(
        goal.id,
        reservation.id,
        _governance_context(
            goal,
            governance,
            "bind-dispatch-with-evidence-materialization",
        ).model_copy(
            update={"expected_version": state.version}
        ),
        envelope=prior.request.materialization_envelope,
        receipt=prior.request.materialization_receipt,
    )
    return goal, provenance.model_copy(
        update={
            "goal_version": reservation.goal_version,
            "policy_revision": reservation.policy_revision,
            "fencing_token": reservation.fencing_token,
            "action_reservation_id": reservation.id,
            "operation_key": reservation.request.operation_key,
            "requested_placement_target": (
                reservation.request.requested_placement_target
            ),
            "placement_input_digest": reservation.request.placement_input_digest,
            "resolved_target_instance_id": (
                reservation.request.resolved_target_instance_id
            ),
            "placement_decision_digest": (
                reservation.request.placement_decision_digest
            ),
            "materialization_envelope": (
                reservation.request.materialization_envelope
            ),
            "materialization_receipt": reservation.request.materialization_receipt,
            "provider_id": reservation.request.provider_id,
            "reservation_attempt": reservation.attempt,
            "max_reservation_attempts": reservation.max_attempts,
        }
    )


def _checkpoint_dispatch_package(
    goals,
    goal,
    provenance: GoalDispatchProvenance,
    record: DispatchRecord,
    identity: GoalExecutionIdentityV1,
    *,
    key: str,
):
    packages = [item.model_copy(deep=True) for item in goal.work_packages]
    package = next(
        item
        for item in packages
        if item.id == identity.work_package_id
    )
    package.dispatch_ids = [record.dispatch_id]
    package.session_id = identity.session_id
    package.action_reservation_id = provenance.action_reservation_id
    package.materialization_envelope = provenance.materialization_envelope
    package.materialization_receipt = provenance.materialization_receipt
    package.execution_identity = identity
    package.state = WorkPackageState.RUNNING
    package.attempts = max(1, package.attempts)
    if package.role.value == "verifier":
        package.verifier_service_id = identity.assigned_service_principal
    else:
        package.executor_service_id = identity.assigned_service_principal
    return goals.checkpoint_supervision(
        goal.id,
        GoalSupervisionCheckpoint.model_validate(
            {
                **goal.model_dump(mode="python"),
                "work_packages": packages,
                "linked_dispatch_ids": sorted(
                    {*goal.linked_dispatch_ids, record.dispatch_id}
                ),
                "reason": "Persist the exact assigned dispatch identity.",
            }
        ),
        _goal_context(goal, key),
    )


def _renew_dispatch_binding(seed: _DispatchSeed | SimpleNamespace, key: str) -> None:
    provenance = seed.record.goal_provenance
    assert provenance is not None
    state, reservation = seed.governance.revalidate_action_sink(
        seed.goal.id,
        provenance.action_reservation_id,
        _governance_context(seed.goal, seed.governance, key),
        action_class="dispatch_work_package",
        provider_id=str(provenance.provider_id),
        requested_placement_target=str(provenance.requested_placement_target),
        placement_input_digest=str(provenance.placement_input_digest),
        resolved_target_instance_id=str(provenance.resolved_target_instance_id),
        placement_decision_digest=str(provenance.placement_decision_digest),
        materialization_envelope=provenance.materialization_envelope,
        materialization_receipt=provenance.materialization_receipt,
        execution_identity=provenance.execution_identity,
    )
    seed.provenance = provenance.model_copy(
        update={
            "goal_version": reservation.goal_version,
            "policy_revision": reservation.policy_revision,
            "fencing_token": reservation.fencing_token,
            "execution_identity": reservation.request.execution_identity,
        }
    )
    seed.record.goal_provenance = seed.provenance
    seed.record.goal_admission_validation_proof = goal_admission_validation_proof(
        seed.record
    )
    seed.ledger.put(seed.record)


def _seed_dispatch(
    root: Path,
    *,
    allow_evidence: bool = False,
) -> _DispatchSeed:
    root.mkdir(parents=True, exist_ok=True)
    helper = _provenance_tests.GoalDispatchProvenanceTests(
        methodName="test_typed_provenance_survives_every_remote_payload_model"
    )
    goals, original_governance, goal, provenance, ledger, ctx = helper._fixture(
        root,
        persist_work_package=True,
    )
    governance = GoalGovernanceService(
        original_governance.store,
        "instance-a",
        goals,
        progress_token_secret="assigned-identity-integration-secret",
    )
    ctx.services["goal_governance"] = governance
    if allow_evidence:
        goal, provenance = _replace_dispatch_after_policy_expansion(
            helper,
            goals,
            governance,
            goal,
            provenance,
        )

    record = helper._record(provenance, state="running")
    if allow_evidence:
        record.idempotency_key = provenance.operation_key
    record.session_id = "session-a"
    record.goal_provenance = _bind_goal_dispatch_execution_identity(
        ctx,
        record.goal_provenance,
        selected_authority="instance-a",
        session_id=record.session_id,
    )
    assert record.goal_provenance is not None
    assert record.goal_provenance.execution_identity is not None
    base_identity = record.goal_provenance.execution_identity
    ledger.put(record)

    goal = _checkpoint_dispatch_package(
        goals,
        goals.get(goal.id),
        record.goal_provenance,
        record,
        base_identity,
        key="checkpoint-base-dispatch-identity",
    )
    draft = SimpleNamespace(
        helper=helper,
        goals=goals,
        governance=governance,
        goal=goal,
        provenance=record.goal_provenance,
        ledger=ledger,
        ctx=ctx,
        record=record,
    )
    _renew_dispatch_binding(draft, "renew-base-dispatch-binding")
    provenance = draft.provenance
    record = draft.record

    scope = GoalAssignedServiceScope(
        goal_id=goal.id,
        work_package_id=base_identity.work_package_id,
        run_id=record.dispatch_id,
        session_id=base_identity.session_id,
        provider_id=base_identity.provider_id,
        target_instance_id=base_identity.target_instance_id,
        authority_instance_id="instance-a",
        fencing_token=base_identity.fencing_token,
        assigned_service_principal=base_identity.assigned_service_principal,
        service_role=base_identity.service_role,
    )
    binding, credential = governance.issue_assigned_service_credential(
        scope,
        _governance_context(
            goal,
            governance,
            (
                f"goal-dispatch:{provenance.action_reservation_id}:"
                f"assigned-service:{base_identity.digest}"
            )[:200],
        ),
        ttl_seconds=ASSIGNED_SERVICE_CREDENTIAL_TTL_SECONDS,
    )
    del credential
    final_identity = GoalExecutionIdentityV1.model_validate(
        {
            **base_identity.model_dump(mode="python", exclude={"digest"}),
            "credential_digest": binding.credential_digest,
            "credential_expires_at": binding.expires_at,
        }
    )
    return _DispatchSeed(
        helper=helper,
        goals=goals,
        governance=governance,
        goal=goal,
        provenance=provenance,
        ledger=ledger,
        ctx=ctx,
        record=record,
        base_identity=base_identity,
        scope=scope,
        binding=binding,
        final_identity=final_identity,
    )


def _credential_entity_ids(seed: _DispatchSeed) -> list[str]:
    return sorted(
        item["id"]
        for item in list_governance_payloads(
            seed.governance.store,
            "default",
            ASSIGNED_SERVICE_CREDENTIAL_ENTITY,
        )
    )


def _reservation_identity(seed: _DispatchSeed) -> GoalExecutionIdentityV1 | None:
    state = seed.governance.get_state(seed.goal.id)
    return next(
        item
        for item in state.action_reservations
        if item.id == seed.provenance.action_reservation_id
    ).request.execution_identity


def _bind_final(seed: _DispatchSeed, key: str):
    context = _governance_context(seed.goal, seed.governance, key)
    state, reservation = seed.governance.bind_dispatch_execution_identity(
        seed.goal.id,
        seed.provenance.action_reservation_id,
        context,
        identity=seed.final_identity,
        assigned_service_binding=seed.binding,
    )
    return context, state, reservation


def test_execution_identity_allows_only_atomic_same_core_credential_upgrade(
    tmp_path: Path,
) -> None:
    seed = _seed_dispatch(tmp_path)
    credential_ids = _credential_entity_ids(seed)
    before_events = len(seed.governance.state_events(seed.goal.id))

    context, state, reservation = _bind_final(seed, "bind-final-execution-identity")
    assert reservation.request.execution_identity == seed.final_identity
    assert reservation.request.execution_identity.credential_authenticated()
    version = state.version

    replayed_state, replayed = seed.governance.bind_dispatch_execution_identity(
        seed.goal.id,
        seed.provenance.action_reservation_id,
        context,
        identity=seed.final_identity,
        assigned_service_binding=seed.binding,
    )
    assert replayed_state.version == version
    assert replayed.request.execution_identity == seed.final_identity
    assert len(seed.governance.state_events(seed.goal.id)) == before_events + 1
    assert _credential_entity_ids(seed) == credential_ids

    base_payload = seed.base_identity.model_dump(mode="python", exclude={"digest"})
    with pytest.raises(ValidationError, match="populated atomically"):
        GoalExecutionIdentityV1.model_validate(
            {**base_payload, "credential_digest": "a" * 64}
        )
    with pytest.raises(ValidationError, match="populated atomically"):
        GoalExecutionIdentityV1.model_validate(
            {**base_payload, "credential_expires_at": seed.binding.expires_at}
        )

    mutations = {
        "session": {"session_id": "session-forged"},
        "principal": {
            "assigned_service_principal": "service:goal-executor:forged"
        },
        "provider": {"provider_id": "cursor"},
        "target": {"target_instance_id": "instance-c"},
        "fence": {"fencing_token": seed.base_identity.fencing_token + 1},
    }
    for name, update in mutations.items():
        candidate = GoalExecutionIdentityV1.model_validate(
            {
                **seed.final_identity.model_dump(
                    mode="python",
                    exclude={"digest"},
                ),
                **update,
            }
        )
        with pytest.raises(GoalGovernanceConflict):
            seed.governance.bind_dispatch_execution_identity(
                seed.goal.id,
                seed.provenance.action_reservation_id,
                _governance_context(
                    seed.goal,
                    seed.governance,
                    f"reject-final-{name}-rewrite",
                ),
                identity=candidate,
                assigned_service_binding=seed.binding,
            )
        assert _reservation_identity(seed) == seed.final_identity
    assert _credential_entity_ids(seed) == credential_ids


def test_final_identity_crash_cuts_repair_without_new_session_or_grant(
    tmp_path: Path,
) -> None:
    issued_only = _seed_dispatch(tmp_path / "issued-only")
    issued_ids = _credential_entity_ids(issued_only)
    assert _reservation_identity(issued_only) == issued_only.base_identity
    repaired = _restore_goal_dispatch_execution_identity(
        issued_only.ctx,
        issued_only.ledger,
        issued_only.record,
    )
    assert repaired.session_id == "session-a"
    assert repaired.goal_provenance is not None
    assert repaired.goal_provenance.execution_identity == issued_only.final_identity
    assert _reservation_identity(issued_only) == issued_only.final_identity
    assert _credential_entity_ids(issued_only) == issued_ids
    assert (
        _restore_goal_dispatch_execution_identity(
            issued_only.ctx,
            issued_only.ledger,
            repaired,
        ).goal_provenance.execution_identity
        == issued_only.final_identity
    )

    reservation_final = _seed_dispatch(tmp_path / "reservation-final")
    reservation_ids = _credential_entity_ids(reservation_final)
    _bind_final(reservation_final, "bind-reservation-before-record-crash")
    assert reservation_final.record.goal_provenance is not None
    assert (
        reservation_final.record.goal_provenance.execution_identity
        == reservation_final.base_identity
    )
    repaired = _restore_goal_dispatch_execution_identity(
        reservation_final.ctx,
        reservation_final.ledger,
        reservation_final.record,
    )
    assert repaired.session_id == "session-a"
    assert repaired.goal_provenance is not None
    assert repaired.goal_provenance.execution_identity == reservation_final.final_identity
    assert _reservation_identity(reservation_final) == reservation_final.final_identity
    assert _credential_entity_ids(reservation_final) == reservation_ids

    for starting_identity in (None, reservation_final.base_identity):
        target_record = reservation_final.record.model_copy(deep=True)
        assert target_record.goal_provenance is not None
        target_record.goal_provenance = target_record.goal_provenance.model_copy(
            update={"execution_identity": starting_identity}
        )
        incoming = reservation_final.provenance.model_copy(
            update={"execution_identity": reservation_final.final_identity}
        )
        transitioned = _target_goal_execution_identity_transition(
            target_record,
            _materialize_body(target_record, incoming),
        )
        assert transitioned is not None
        assert transitioned.execution_identity == reservation_final.final_identity
        assert target_record.session_id == "session-a"
    assert _credential_entity_ids(reservation_final) == reservation_ids


def _materialize_body(
    record: DispatchRecord,
    provenance: GoalDispatchProvenance,
) -> DispatchMaterializeBody:
    return DispatchMaterializeBody(
        dispatch_id=record.dispatch_id,
        mutation_id=record.mutation_id,
        realm_id=record.realm_id,
        authority_instance_id=record.authority_instance_id,
        authority_url=record.authority_url,
        target_instance_id=record.target_instance_id,
        provider="codex",
        session_id=record.session_id,
        materialization_plan=record.materialization_plan,
        goal_provenance=provenance,
    )


def test_target_materialization_accepts_only_monotonic_final_identity(
    tmp_path: Path,
) -> None:
    seed = _seed_dispatch(tmp_path)
    final_provenance = seed.provenance.model_copy(
        update={"execution_identity": seed.final_identity}
    )
    for starting_identity in (None, seed.base_identity):
        record = seed.record.model_copy(deep=True)
        assert record.goal_provenance is not None
        record.goal_provenance = record.goal_provenance.model_copy(
            update={"execution_identity": starting_identity}
        )
        transitioned = _target_goal_execution_identity_transition(
            record,
            _materialize_body(record, final_provenance),
        )
        assert transitioned is not None
        assert transitioned.execution_identity == seed.final_identity

    bound = seed.record.model_copy(deep=True)
    bound.goal_provenance = final_provenance
    with pytest.raises(HTTPException) as downgrade:
        _target_goal_execution_identity_transition(
            bound,
            _materialize_body(
                bound,
                seed.provenance.model_copy(
                    update={"execution_identity": seed.base_identity}
                ),
            ),
        )
    assert downgrade.value.detail["code"] == "goal_execution_identity_mismatch"

    cross_core = GoalExecutionIdentityV1.model_validate(
        {
            **seed.final_identity.model_dump(mode="python", exclude={"digest"}),
            "session_id": "another-session",
        }
    )
    base = seed.record.model_copy(deep=True)
    base.goal_provenance = seed.provenance
    with pytest.raises(HTTPException) as mismatch:
        _target_goal_execution_identity_transition(
            base,
            _materialize_body(
                base,
                seed.provenance.model_copy(
                    update={"execution_identity": cross_core}
                ),
            ),
        )
    assert mismatch.value.detail["code"] == "goal_execution_identity_mismatch"


def _checkpoint_final_package(seed: _DispatchSeed) -> None:
    assert seed.record.goal_provenance is not None
    seed.goal = _checkpoint_dispatch_package(
        seed.goals,
        seed.goals.get(seed.goal.id),
        seed.record.goal_provenance,
        seed.record,
        seed.final_identity,
        key="checkpoint-final-dispatch-identity",
    )
    _renew_dispatch_binding(seed, "renew-final-dispatch-binding")


def _target_request(
    ctx,
    record: DispatchRecord,
    *,
    session_id: str | None = None,
    target_instance_id: str | None = None,
):
    session_id = session_id or str(record.session_id)
    capability = assigned_service_session_capability(
        secret=ctx.settings.session_secret,
        dispatch_id=record.dispatch_id,
        session_id=session_id,
        target_instance_id=target_instance_id or ctx.settings.instance_id,
    )
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)),
        state=SimpleNamespace(assigned_session_capability=capability),
        headers={
            "X-PA-Assigned-Session-ID": session_id,
            "X-PA-Assigned-Dispatch-ID": record.dispatch_id,
        },
    )


async def _exercise_live_target_proxy_commits_once_on_authority_and_never_on_target(
    tmp_path: Path,
) -> None:
    seed = _seed_dispatch(tmp_path / "governance", allow_evidence=True)
    final = _bind_goal_dispatch_assigned_service_identity(
        seed.ctx,
        seed.provenance,
        selected_authority="instance-a",
        dispatch_id=seed.record.dispatch_id,
        session_id="session-a",
    )
    assert final is not None and final.execution_identity == seed.final_identity
    seed.provenance = final
    seed.record.goal_provenance = final
    seed.ledger.put(seed.record)
    _checkpoint_final_package(seed)

    authority_app = assigned_service_app(
        tmp_path / "authority-app",
        instance_id="instance-a",
        instance_name="Authority A",
        instance_url="http://instance-a.test:8080",
        sync_token="fleet-token",
    )
    authority_ctx = authority_app.state.ctx
    authority_ctx.services["goal_service"] = seed.goals
    authority_ctx.services["goal_governance"] = seed.governance
    authority_ctx.services["dispatch_store"] = seed.ledger
    authority_ctx.require_service("fleet_registry").upsert_instance(
        FleetInstance(
            instance_id="instance-b",
            name="Target B",
            url="http://instance-b.test:8080",
        ),
        actor="test",
    )

    target_app = assigned_service_app(
        tmp_path / "target-app",
        instance_id="instance-b",
        instance_name="Target B",
        instance_url="http://instance-b.test:8080",
        sync_token="fleet-token",
    )
    target_ctx = target_app.state.ctx
    target_ctx.require_service("fleet_registry").upsert_instance(
        FleetInstance(
            instance_id="instance-a",
            name="Authority A",
            url="http://instance-a.test:8080",
        ),
        actor="test",
    )
    target_ctx.services["dispatch_store"] = DispatchStore(
        tmp_path / "target-dispatch-ledger"
    )
    target_record = seed.record.model_copy(deep=True)
    target_ctx.require_service("dispatch_store").put(target_record)
    target_ctx.store.save_session(
        AgentSession(
            id="session-a",
            agent_name="codex",
            authority_instance_id="instance-a",
            dispatch_id=target_record.dispatch_id,
            status="connected",
        )
    )
    runtime = SimpleNamespace(connected=True, _closed=False)
    target_ctx.services["instance_agent"] = SimpleNamespace(
        get=lambda session_id: runtime if session_id == "session-a" else None
    )

    before_authority = sum(
        event["event_type"] == "goal.evidence_recorded"
        for event in seed.goals.events(seed.goal.id)
    )
    target_goals = target_ctx.require_service("goal_service")
    before_target = len(target_goals.events(seed.goal.id))
    goal = seed.goals.get(seed.goal.id)
    transport = httpx.ASGITransport(app=authority_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://instance-a.test:8080",
    ) as client:
        target_ctx.services.pop("async_runtime", None)
        target_ctx.services["fleet_http_client"] = client
        response = await _proxy_assigned_session_operation(
            _target_request(target_ctx, target_record),
            "evidence",
            payload={
                "evidence": {
                    "criterion_ids": [goal.criteria[0].id],
                    "kind": "test",
                    "summary": "The live target-to-authority proxy passed.",
                }
            },
            expected_version=goal.version,
            policy_revision=goal.policy.revision,
            idempotency_key="live-target-authority-evidence",
        )
        assert response["accepted"] is True
        assert response["operation"] == "evidence"
        assert response["goal"]["evidence"][-1]["summary"] == (
            "The live target-to-authority proxy passed."
        )

        for forged in (
            _target_request(target_ctx, target_record, session_id="forged-session"),
            _target_request(
                target_ctx,
                target_record,
                target_instance_id="instance-c",
            ),
        ):
            with pytest.raises(HTTPException) as rejected:
                await _proxy_assigned_session_operation(
                    forged,
                    "goal",
                    payload={"offset": 0, "limit": 1},
                )
            assert rejected.value.status_code == 403

    after_authority = sum(
        event["event_type"] == "goal.evidence_recorded"
        for event in seed.goals.events(seed.goal.id)
    )
    assert after_authority == before_authority + 1
    assert len(target_goals.events(seed.goal.id)) == before_target


def test_live_target_proxy_commits_once_on_authority_and_never_on_target(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _exercise_live_target_proxy_commits_once_on_authority_and_never_on_target(
            tmp_path
        )
    )


def test_mixed_scope_evidence_id_is_absent_from_assigned_goal_projection(
    tmp_path: Path,
) -> None:
    app = assigned_service_app(tmp_path)
    goals = app.state.ctx.require_service("goal_service")
    governance = app.state.ctx.require_service("goal_governance")
    seeded = _seed_assigned_services(goals, governance)
    goal = goals.get(seeded.goal_id)
    goal = goals.add_evidence(
        goal.id,
        GoalEvidenceCreate(
            evidence=GoalEvidence(
                criterion_ids=[seeded.criterion_id, seeded.other_criterion_id],
                kind="test",
                summary="Mixed-package evidence must remain entirely hidden.",
            )
        ),
        GoalMutationContext(
            actor_principal="user:operator",
            authority_instance_id="authority-a",
            idempotency_key="add-mixed-scope-evidence",
            expected_version=goal.version,
            policy_revision=goal.policy.revision,
            fencing_token=goal.lease.fencing_token,
        ),
    )
    mixed_id = goal.evidence[-1].id
    authorization = governance.resolve_assigned_service_credential(
        seeded.executor_token
    )

    projection = _assigned_goal_projection(authorization)

    assert mixed_id not in {item["id"] for item in projection["evidence"]}
    assert all(
        mixed_id not in criterion["evidence_ids"]
        for criterion in projection["criteria"]
    )
