from __future__ import annotations

import asyncio
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from pa.domain.projection import CardProjection
from pa.execution.dispatch import (
    DispatchIdempotencyConflict,
    DispatchRecord,
    DispatchStore,
    DispatchWorker,
    GoalDispatchProvenance,
    goal_admission_validation_proof,
    goal_dispatch_placement_decision_digest,
    goal_dispatch_placement_input_digest,
    goal_dispatch_placement_input_snapshot,
)
from pa.execution.profiles import MaterializationPlan
from pa.goals.advanced_models import (
    GoalActionDisposition,
    GoalActionRequest,
    GoalReservationState,
    GoalResourceClaim,
    GoalUsage,
    GovernanceMutationContext,
    ResourceAccess,
)
from pa.goals.governance import GoalGovernanceConflict, GoalGovernanceService
from pa.goals.materialization import (
    GoalExecutionIdentityV1,
    GoalMaterializationEnvelopeV1,
    GoalMaterializationReceiptV1,
    GoalMaterializationResourceClaimV1,
    canonical_materialization_digest,
)
from pa.goals.models import (
    CreateWorkPackageAction,
    DispatchWorkPackageAction,
    GoalActorRole,
    GoalBudget,
    GoalCreate,
    GoalCriterion,
    GoalMutationContext,
    GoalPolicy,
    GoalProposal,
    GoalProposalCreate,
    GoalRevision,
    GoalState,
    GoalSupervisionCheckpoint,
    GoalTransition,
    GoalWorkPackage,
    GoalWakeup,
    ProposalStatus,
)
from pa.goals.service import GoalService
from pa.goals.supervisor import GoalSupervisor
from pa.modules.agent_chat import CreateSessionBody, PromptBody
from pa.modules.fleet import (
    DispatchFollowupBody,
    DispatchMaterializeBody,
    FleetDispatchBody,
    RemoteAgentStartBody,
    _bind_effective_goal_dispatch_provider,
    _bind_goal_dispatch_execution_identity,
    _bind_goal_dispatch_materialization,
    _bind_goal_dispatch_placement,
    _fail_goal_dispatch_admission,
    _goal_admission_proof_valid,
    _goal_materialization_stage_provenance,
    _mark_goal_admission_validated,
    _persist_goal_dispatch_admission_trace,
    _reconcile_goal_dispatch_followups,
    _reconcile_goal_dispatch_reservations,
    _refresh_queued_dispatch_readiness,
    _release_goal_dispatch_reservation,
    _release_goal_dispatch_reservation_async,
    _replace_goal_dispatch_reservation,
    _reserve_goal_dispatch_followup,
    _restore_goal_dispatch_execution_identity,
    _synchronize_target_goal_execution_identity,
    _validate_goal_dispatch_provenance,
    _validate_goal_dispatch_record,
    prompt_dispatch_session,
)
from pa.modules.fleet import (
    router as fleet_router,
)
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


class _Context:
    def __init__(
        self, services: dict[str, object], *, instance_id: str = "instance-a"
    ) -> None:
        self.services = services
        self.settings = SimpleNamespace(
            instance_id=instance_id,
            auth_required=False,
        )

    def require_service(self, name: str):
        return self.services[name]


class GoalDispatchProvenanceTests(unittest.TestCase):
    actor = "service:goal-supervisor:instance-a"

    @staticmethod
    def _placement_decision(target: str = "instance-b") -> dict[str, str]:
        return {
            "policy": "named_instance",
            "chosen_instance_id": target,
            "chosen_instance_name": target,
            "tie_breaking_reason": "The concrete test target was requested directly.",
        }

    @staticmethod
    def _materialization_plan(target: str = "instance-b") -> dict[str, object]:
        return {
            "contract_version": 1,
            "profile": "research",
            "profile_source": "dispatch_override",
            "requirements": {
                "repository_required": False,
                "repositories": [],
                "attachments": False,
                "browser": False,
                "external_tools": [],
                "required_capabilities": [],
                "writable_artifact_workspace": True,
                "network_policy": "provider-default",
                "expected_deliverables": [],
            },
            "target_instance_id": target,
            "repositories": [],
            "workspace": {"kind": "artifact"},
            "missing_dependencies": [],
            "stale_dependencies": [],
            "confirmation_required": False,
            "summary": "Canonical test materialization.",
        }

    @classmethod
    def _materialization_receipt(
        cls,
        envelope: GoalMaterializationEnvelopeV1,
        target: str = "instance-b",
        provider: str = "codex",
    ) -> GoalMaterializationReceiptV1:
        return GoalMaterializationReceiptV1(
            envelope_digest=str(envelope.digest),
            target_instance_id=target,
            provider_id=provider,
            materialization_plan_digest=canonical_materialization_digest(
                cls._materialization_plan(target)
            ),
        )

    def _fixture(
        self,
        root: Path,
        *,
        max_attempts: int = 2,
        max_actions: int = 10,
        max_dispatches: int = 10,
        operation_key: str = "dispatch-a",
        reservation_provider: str | None = "codex",
        allowed_providers: list[str] | None = None,
        target_instance_id: str = "instance-b",
        placement_bound: bool = True,
        package_role: GoalActorRole = GoalActorRole.EXECUTOR,
        persist_work_package: bool = False,
    ):
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root, "instance-a")
        projection = CardProjection(root / "projection.db", log)
        goals = GoalService(projection, "instance-a")
        governance = GoalGovernanceService(projection, "instance-a", goals)
        goal = goals.create(
            GoalCreate(
                objective="Ship governed fleet work",
                criteria=[
                    GoalCriterion(
                        description="independently verified",
                        verification_method="focused tests",
                        evidence_requirement="passing output",
                    )
                ],
                policy=GoalPolicy(
                    autonomy_level=4,
                    permitted_actions=["dispatch_work_package"],
                    max_action_risk="medium",
                    allowed_provider_ids=allowed_providers or ["codex"],
                ),
                budget=GoalBudget(
                    max_actions=max_actions,
                    max_dispatches=max_dispatches,
                    max_concurrency=1,
                    retry_limit=max_attempts - 1,
                ),
            ),
            GoalMutationContext(
                actor_principal="user:operator",
                authority_instance_id="instance-a",
                idempotency_key="create-goal",
                expected_version=0,
                policy_revision=1,
            ),
        )
        goal = goals.acquire_lease(
            goal.id,
            GoalMutationContext(
                actor_principal=self.actor,
                authority_instance_id="instance-a",
                idempotency_key="claim-goal",
                expected_version=goal.version,
                policy_revision=1,
            ),
            ttl_seconds=600,
        )
        proposal = GoalProposal(
            proposer_principal=self.actor,
            proposer_role=GoalActorRole.COORDINATOR,
            action=CreateWorkPackageAction(
                title="Execute governed fleet work",
                objective=goal.objective,
                criterion_ids=[goal.criteria[0].id],
                role=package_role,
            ),
            rationale="Exercise an exact governed work package.",
            expected_goal_version=goal.version,
            policy_revision=goal.policy.revision,
            status=ProposalStatus.APPLIED,
        )
        work_package = GoalWorkPackage(
            proposal_id=proposal.id,
            title="Execute governed fleet work",
            objective=goal.objective,
            criterion_ids=[goal.criteria[0].id],
            role=package_role,
        )
        if persist_work_package:
            goal = goals.checkpoint_supervision(
                goal.id,
                GoalSupervisionCheckpoint.model_validate(
                    {
                        **goal.model_dump(mode="python"),
                        "proposals": [*goal.proposals, proposal],
                        "work_packages": [work_package],
                        "reason": "Materialize the governed test work package.",
                    }
                ),
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="checkpoint-work-package",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
        placement_input_digest = goal_dispatch_placement_input_digest(
            {
                "target_instance_id": target_instance_id,
                "provider": reservation_provider,
            }
        )
        placement_decision_digest = goal_dispatch_placement_decision_digest(
            self._placement_decision(target_instance_id)
        )
        envelope = GoalMaterializationEnvelopeV1(
            work_package_id=work_package.id,
            service_role=(
                "verifier" if package_role == GoalActorRole.VERIFIER else "executor"
            ),
            resource_claims=(
                GoalMaterializationResourceClaimV1(
                    key=f"fleet-dispatch:{target_instance_id}"
                ),
            ),
            execution_contract_digest=canonical_materialization_digest(None),
        )
        state, decision = governance.authorize_action(
            goal.id,
            GoalActionRequest(
                action_class="dispatch_work_package",
                operation_key=operation_key,
                requested_placement_target=target_instance_id,
                placement_input_digest=placement_input_digest,
                resolved_target_instance_id=(
                    target_instance_id if placement_bound else None
                ),
                placement_decision_digest=(
                    placement_decision_digest if placement_bound else None
                ),
                materialization_envelope=envelope,
                delegated=True,
                provider_id=reservation_provider,
                estimate=GoalUsage(actions=1, dispatches=1),
                resource_claims=[
                    GoalResourceClaim(
                        key=f"fleet-dispatch:{target_instance_id}",
                        access=ResourceAccess.SHARED,
                    )
                ],
                max_attempts=max_attempts,
            ),
            self._governance_context(goal, 0, "reserve-dispatch"),
        )
        self.assertEqual(decision.disposition, GoalActionDisposition.AUTHORIZED)
        reservation_id = decision.reservation_id or ""
        state, applied = governance.apply_action(
            goal.id,
            reservation_id,
            self._governance_context(goal, state.version, "apply-dispatch"),
        )
        self.assertEqual(applied.disposition, GoalActionDisposition.AUTHORIZED)
        reservation = next(
            item for item in state.action_reservations if item.id == reservation_id
        )
        if placement_bound and reservation_provider is not None:
            receipt = self._materialization_receipt(
                envelope,
                target_instance_id,
                reservation_provider,
            )
            state, reservation = governance.bind_dispatch_materialization(
                goal.id,
                reservation.id,
                self._governance_context(
                    goal,
                    state.version,
                    "bind-dispatch-materialization",
                ),
                envelope=envelope,
                receipt=receipt,
            )
        provenance = GoalDispatchProvenance(
            goal_id=goal.id,
            goal_version=reservation.goal_version,
            policy_revision=reservation.policy_revision,
            authority_instance_id=reservation.authority_instance_id,
            fencing_token=reservation.fencing_token or 0,
            action_reservation_id=reservation.id,
            operation_key=reservation.request.operation_key,
            requested_placement_target=(reservation.request.requested_placement_target),
            placement_input_digest=reservation.request.placement_input_digest,
            resolved_target_instance_id=(
                reservation.request.resolved_target_instance_id
            ),
            placement_decision_digest=(reservation.request.placement_decision_digest),
            materialization_envelope=reservation.request.materialization_envelope,
            materialization_receipt=reservation.request.materialization_receipt,
            actor_principal=reservation.actor_principal,
            provider_id=reservation.request.provider_id,
            reservation_attempt=reservation.attempt,
            max_reservation_attempts=reservation.max_attempts,
        )
        ledger = DispatchStore(root / "ledger")
        ctx = _Context(
            {
                "goal_service": goals,
                "goal_governance": governance,
                "dispatch_store": ledger,
            }
        )
        return goals, governance, goal, provenance, ledger, ctx

    def _governance_context(
        self, goal, version: int, key: str
    ) -> GovernanceMutationContext:
        return GovernanceMutationContext(
            actor_principal=self.actor,
            authority_instance_id="instance-a",
            idempotency_key=key,
            expected_version=version,
            policy_revision=goal.policy.revision,
            goal_version=goal.version,
            fencing_token=goal.lease.fencing_token,
        )

    @staticmethod
    def _record(provenance: GoalDispatchProvenance, *, state: str = "queued"):
        placement_input = goal_dispatch_placement_input_snapshot(
            {
                "target_instance_id": "instance-b",
                "provider": "codex",
            }
        )
        record = DispatchRecord(
            mutation_id="mutation-a",
            idempotency_key="dispatch-a",
            request_fingerprint="fingerprint-a",
            materialization_plan=GoalDispatchProvenanceTests._materialization_plan(),
            request_payload={
                "provider": "codex",
                "message": "Do the work",
                "execution_contract": None,
            },
            goal_provenance=provenance,
            goal_placement_input=placement_input,
            goal_placement_input_digest=provenance.placement_input_digest,
            principal_id="user:operator",
            authority_instance_id="instance-a",
            authority_url="http://instance-a",
            target_instance_id="instance-b",
            placement_policy="named_instance",
            placement_decision=GoalDispatchProvenanceTests._placement_decision(),
            state=state,
        )
        record.goal_admission_validation_state = "validated"
        record.goal_admission_validated_at = datetime.now(UTC)
        record.goal_admission_validation_proof = goal_admission_validation_proof(record)
        return record

    def _bind_trace_placement(
        self,
        ctx: _Context,
        ledger: DispatchStore,
        trace: DispatchRecord,
        *,
        target_instance_id: str = "instance-b",
    ) -> DispatchRecord:
        decision = self._placement_decision(target_instance_id)
        trace.target_instance_id = target_instance_id
        trace.placement_policy = "named_instance"
        trace.placement_decision = decision
        ledger.put(trace)
        trace.goal_provenance = _bind_goal_dispatch_placement(
            ctx,
            trace.goal_provenance,
            selected_authority="instance-a",
            operation_key=trace.idempotency_key or "",
            target_instance_id=target_instance_id,
            placement_input_digest=trace.goal_placement_input_digest or "",
            placement_decision=decision,
        )
        assert trace.goal_provenance is not None
        goals = ctx.require_service("goal_service")
        governance = ctx.require_service("goal_governance")
        goal = goals.get(trace.goal_provenance.goal_id)
        assert goal is not None
        state = governance.get_state(goal.id)
        envelope = trace.goal_provenance.materialization_envelope
        assert envelope is not None
        receipt = self._materialization_receipt(envelope, target_instance_id)
        state, reservation = governance.bind_dispatch_materialization(
            goal.id,
            trace.goal_provenance.action_reservation_id,
            self._governance_context(
                goal,
                state.version,
                f"bind-trace-materialization:{target_instance_id}",
            ),
            envelope=envelope,
            receipt=receipt,
        )
        trace.goal_provenance = trace.goal_provenance.model_copy(
            update={
                "materialization_receipt": (reservation.request.materialization_receipt)
            }
        )
        trace.materialization_plan = self._materialization_plan(target_instance_id)
        trace.request_payload["execution_contract"] = None
        return ledger.put(trace)

    @staticmethod
    def _app(ctx: _Context):
        return SimpleNamespace(state=SimpleNamespace(ctx=ctx))

    def test_typed_provenance_survives_every_remote_payload_model(self) -> None:
        provenance = GoalDispatchProvenance(
            goal_id="goal-a",
            goal_version=3,
            policy_revision=2,
            authority_instance_id="instance-a",
            fencing_token=7,
            action_reservation_id="reservation-a",
            actor_principal=self.actor,
            reservation_attempt=2,
            max_reservation_attempts=4,
        )
        fleet = FleetDispatchBody(
            authority_instance_id="instance-a",
            target_instance_id="instance-b",
            provider="codex",
            goal_provenance=provenance,
        )
        remote = RemoteAgentStartBody.model_validate(fleet.model_dump(mode="json"))
        materialize = DispatchMaterializeBody(
            dispatch_id="dispatch-a",
            mutation_id="mutation-a",
            realm_id="default",
            authority_instance_id="instance-a",
            authority_url="http://instance-a",
            target_instance_id="instance-b",
            goal_provenance=remote.goal_provenance,
        )
        session = CreateSessionBody(
            dispatch_id="dispatch-a", goal_provenance=materialize.goal_provenance
        )
        prompt = PromptBody(
            dispatch_id="dispatch-a",
            message="Do the work",
            goal_provenance=session.goal_provenance,
        )
        self.assertEqual(prompt.goal_provenance, provenance)

        legacy = FleetDispatchBody(
            authority_instance_id="instance-a",
            goal_id="goal-a",
            goal_version=3,
            goal_policy_revision=2,
            goal_fencing_token=7,
            goal_action_reservation_id="reservation-a",
            goal_actor_principal=self.actor,
        )
        self.assertEqual(
            legacy.goal_provenance,
            provenance.model_copy(
                update={
                    "reservation_attempt": 1,
                    "max_reservation_attempts": 1,
                }
            ),
        )
        with self.assertRaisesRegex(ValidationError, "flat and typed"):
            FleetDispatchBody(
                authority_instance_id="instance-a",
                goal_id="different-goal",
                goal_provenance=provenance,
            )

    def test_remote_mode_id_is_bounded_before_governance_mutation(self) -> None:
        app = FastAPI()
        app.include_router(fleet_router, prefix="/api")
        with patch("pa.modules.fleet._persist_goal_dispatch_admission_trace") as mutate:
            with TestClient(app) as client:
                response = client.post(
                    "/api/fleet/instances/instance-b/agent/start",
                    json={"mode_id": "x" * 201},
                )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["detail"][0]["loc"][-1], "mode_id")
        mutate.assert_not_called()
        self.assertEqual(RemoteAgentStartBody(mode_id="  code  ").mode_id, "code")

    def test_fleet_reconstructs_attachment_envelope_before_binding_receipt(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, _governance, _goal, provenance, _ledger, ctx = self._fixture(
                Path(tmp), persist_work_package=True
            )
            body = RemoteAgentStartBody(
                provider="codex",
                mode_id="   ",
                execution_contract=None,
                goal_provenance=provenance,
            )
            self.assertIsNone(body.mode_id)
            plan = MaterializationPlan.model_validate(self._materialization_plan())
            exact = _bind_goal_dispatch_materialization(
                ctx,
                provenance,
                selected_authority="instance-a",
                body=body,
                card=SimpleNamespace(attachments=[]),
                plan=plan,
                target_instance_id="instance-b",
            )
            assert exact is not None
            self.assertEqual(
                exact.materialization_receipt,
                provenance.materialization_receipt,
            )
            assert exact.materialization_receipt is not None
            self.assertIsNone(exact.materialization_receipt.mode_id)
            attachment = SimpleNamespace(
                attachment_id="attachment-a",
                media_type="application/pdf",
                state="active",
            )
            with self.assertRaises(HTTPException) as raised:
                _bind_goal_dispatch_materialization(
                    ctx,
                    provenance,
                    selected_authority="instance-a",
                    body=body,
                    card=SimpleNamespace(attachments=[attachment]),
                    plan=plan,
                    target_instance_id="instance-b",
                )
            self.assertEqual(
                raised.exception.detail["code"],
                "goal_materialization_envelope_mismatch",
            )

    def test_queue_revalidation_renews_goal_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, goal, provenance, _ledger, ctx = self._fixture(Path(tmp))
            revised = goals.revise(
                goal.id,
                GoalRevision(
                    objective="Ship the freshly revised governed fleet work",
                    reason="Clarify the outcome without changing policy",
                ),
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="revise-objective",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            refreshed = _validate_goal_dispatch_provenance(
                ctx,
                provenance,
                "instance-a",
                sink="queued-promotion",
                provider_id="codex",
            )
            assert refreshed is not None
            self.assertEqual(refreshed.goal_version, revised.version)
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.APPLIED)
            self.assertEqual(reservation.renewal_count, 1)

    def test_omitted_transport_provider_requires_concrete_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                _goals,
                governance,
                goal,
                provenance,
                _ledger,
                ctx,
            ) = self._fixture(Path(tmp), reservation_provider="codex")
            body = RemoteAgentStartBody(goal_provenance=provenance)
            _bind_effective_goal_dispatch_provider(body, "codex")
            self.assertEqual(body.provider, "codex")
            refreshed = _validate_goal_dispatch_provenance(
                ctx,
                provenance,
                "instance-a",
                sink="durable-admission",
                provider_id=body.provider,
            )
            assert refreshed is not None
            self.assertEqual(refreshed.provider_id, "codex")
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.request.provider_id, "codex")

        with tempfile.TemporaryDirectory() as tmp:
            (
                _goals,
                _governance,
                _goal,
                provenance,
                _ledger,
                ctx,
            ) = self._fixture(Path(tmp), reservation_provider=None)
            body = RemoteAgentStartBody(goal_provenance=provenance)
            _bind_effective_goal_dispatch_provider(body, "codex")
            with self.assertRaises(HTTPException) as absent:
                _validate_goal_dispatch_provenance(
                    ctx,
                    provenance,
                    "instance-a",
                    sink="durable-admission",
                    provider_id=body.provider,
                )
            self.assertEqual(absent.exception.detail["code"], "goal_governance_denied")

        with tempfile.TemporaryDirectory() as tmp:
            (
                _goals,
                _governance,
                _goal,
                provenance,
                _ledger,
                ctx,
            ) = self._fixture(
                Path(tmp),
                reservation_provider="cursor",
                allowed_providers=["codex", "cursor"],
            )
            body = RemoteAgentStartBody(goal_provenance=provenance)
            _bind_effective_goal_dispatch_provider(body, "codex")
            with self.assertRaises(HTTPException) as raised:
                _validate_goal_dispatch_provenance(
                    ctx,
                    provenance,
                    "instance-a",
                    sink="durable-admission",
                    provider_id=body.provider,
                )
            self.assertEqual(raised.exception.detail["code"], "goal_provider_mismatch")

    def test_supervisor_resolves_provider_before_reserve_and_apply(self) -> None:
        def exercise(default_provider: str):
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            goals, governance, goal, _provenance, _ledger, _ctx = self._fixture(
                Path(tmp.name)
            )
            initial = governance.get_state(goal.id).action_reservations[-1]
            governance.reconcile_action_release(
                goal.id,
                initial.id,
                self._governance_context(
                    goal,
                    governance.get_state(goal.id).version,
                    f"release-initial-{default_provider}-hold",
                ),
                actual_usage=GoalUsage(),
                reason="test setup",
            )
            policy = goal.policy.model_copy(deep=True)
            policy.revision += 1
            policy.permitted_actions = [
                "create_work_package",
                "dispatch_work_package",
            ]
            goal = goals.revise(
                goal.id,
                GoalRevision(
                    policy=policy,
                    reason="Allow the setup package to be created canonically.",
                ),
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key=f"policy-{default_provider}",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            goal = goals.transition(
                goal.id,
                GoalTransition(state=GoalState.READY, reason="ready for execution"),
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key=f"ready-{default_provider}",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            goal = goals.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal=self.actor,
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=CreateWorkPackageAction(
                        title="Exercise concrete provider",
                        objective="Drive the real supervisor dispatch path",
                        criterion_ids=[goal.criteria[0].id],
                    ),
                    rationale="The provider must be bound before governance.",
                    expected_goal_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key=f"proposal-{default_provider}",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            calls: list[dict] = []

            def dispatch(payload: dict) -> dict:
                calls.append(payload)
                return {"dispatch_id": f"dispatch-{default_provider}"}

            supervisor = GoalSupervisor(
                goals,
                governance.store,
                "instance-a",
                governance=governance,
                dispatch=dispatch,
                default_provider=default_provider,
            )
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            return goals, governance, goal.id, calls

        _goals, governance, goal_id, calls = exercise("codex")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["provider"], "codex")
        applied = [
            item
            for item in governance.get_state(goal_id).action_reservations
            if item.state == GoalReservationState.APPLIED
        ]
        self.assertEqual(len(applied), 1)
        self.assertEqual(applied[0].request.provider_id, "codex")
        self.assertEqual(
            applied[0].request.requested_placement_target,
            "placement:best_match",
        )
        self.assertIsNotNone(applied[0].request.placement_input_digest)
        self.assertIsNone(applied[0].request.resolved_target_instance_id)
        self.assertEqual(
            calls[0]["goal_provenance"]["action_reservation_id"],
            applied[0].id,
        )
        self.assertEqual(
            calls[0]["goal_provenance"]["placement_input_digest"],
            applied[0].request.placement_input_digest,
        )

        goals, governance, goal_id, calls = exercise("cursor")
        self.assertEqual(calls, [])
        denied = goals.get(goal_id)
        assert denied is not None
        dispatch_proposal = next(
            item
            for item in denied.proposals
            if isinstance(item.action, DispatchWorkPackageAction)
        )
        self.assertEqual(dispatch_proposal.status, ProposalStatus.FAILED)
        self.assertIn("canonical governance denied", dispatch_proposal.error or "")
        self.assertFalse(
            any(
                item.state != GoalReservationState.RELEASED
                for item in governance.get_state(goal_id).action_reservations
            )
        )

    def test_policy_change_fails_queue_and_durably_releases_hold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="waiting_capacity")
            ledger.put(record)
            policy = goal.policy.model_copy(deep=True)
            policy.revision += 1
            policy.permitted_actions = []
            goals.revise(
                goal.id,
                GoalRevision(
                    policy=policy,
                    reason="Revoke unattended fleet dispatch",
                ),
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="revoke-dispatch",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )

            with self.assertRaises(HTTPException) as raised:
                asyncio.run(_refresh_queued_dispatch_readiness(self._app(ctx), record))
            self.assertEqual(raised.exception.detail["code"], "goal_governance_denied")
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None and persisted.goal_provenance is not None
            self.assertEqual(persisted.state, "failed")
            self.assertIsNotNone(persisted.goal_provenance.released_at)

    def test_wrong_target_queued_promotion_quarantines_without_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="waiting_capacity")
            ledger.put(record)
            record.target_instance_id = "instance-c"
            ledger.put(record)

            with self.assertRaises(HTTPException) as raised:
                asyncio.run(_refresh_queued_dispatch_readiness(self._app(ctx), record))
            self.assertEqual(
                raised.exception.detail["code"],
                "goal_placement_mismatch",
            )
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None and persisted.goal_provenance is not None
            self.assertEqual(persisted.state, "failed")
            self.assertEqual(persisted.goal_admission_validation_state, "rejected")
            self.assertIsNone(persisted.goal_provenance.released_at)
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.APPLIED)

    def test_lease_takeover_blocks_promotion_but_allows_exact_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="waiting_capacity")
            ledger.put(record)
            released = goals.release_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="release-a",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            handed_off = goals.schedule_wakeup(
                goal.id,
                GoalWakeup(
                    wake_at=datetime.now(UTC),
                    reason="authority-authored takeover",
                    eligible_instance_ids=["instance-b"],
                ),
                GoalMutationContext(
                    actor_principal="user:operator",
                    authority_instance_id="instance-a",
                    idempotency_key="handoff-b",
                    expected_version=released.version,
                    policy_revision=released.policy.revision,
                ),
            )
            GoalService(goals.store, "instance-b").acquire_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal="service:goal-supervisor:instance-b",
                    authority_instance_id="instance-b",
                    idempotency_key="claim-b",
                    expected_version=handed_off.version,
                    policy_revision=handed_off.policy.revision,
                ),
                ttl_seconds=600,
            )

            with self.assertRaises(HTTPException) as raised:
                asyncio.run(_refresh_queued_dispatch_readiness(self._app(ctx), record))
            self.assertEqual(raised.exception.detail["code"], "stale_goal_fence")
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None and persisted.goal_provenance is not None
            self.assertIsNotNone(persisted.goal_provenance.released_at)

    def test_same_instance_reacquisition_does_not_upgrade_stale_fence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="waiting_capacity")
            ledger.put(record)
            released = goals.release_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="release-fence-one",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            reacquired = goals.acquire_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="claim-fence-two",
                    expected_version=released.version,
                    policy_revision=released.policy.revision,
                ),
                ttl_seconds=600,
            )
            self.assertGreater(
                reacquired.lease.fencing_token,
                provenance.fencing_token,
            )

            with self.assertRaises(HTTPException) as raised:
                asyncio.run(_refresh_queued_dispatch_readiness(self._app(ctx), record))
            self.assertEqual(raised.exception.detail["code"], "stale_goal_fence")
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.fencing_token, provenance.fencing_token)
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)

    def test_preledger_lease_loss_reconciles_hold_after_version_race(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, goal, _provenance, ledger, _ctx = self._fixture(
                Path(tmp)
            )
            # The fixture reservation models a prior dispatch; clear it so the
            # supervisor can reserve the pre-ledger attempt under max_concurrency=1.
            initial = governance.get_state(goal.id).action_reservations[-1]
            governance.reconcile_action_release(
                goal.id,
                initial.id,
                self._governance_context(
                    goal,
                    governance.get_state(goal.id).version,
                    "release-fixture-reservation",
                ),
                actual_usage=GoalUsage(),
                reason="test setup",
            )
            supervisor = GoalSupervisor(
                goals,
                governance.store,
                "instance-a",
                governance=governance,
            )
            original_release = governance.reconcile_action_release
            release_calls = 0

            # Use the canonical conflict type/message that a competing governance
            # event produces between the state read and release commit.
            def version_racing_release(*args, **kwargs):
                nonlocal release_calls
                release_calls += 1
                if release_calls == 1:
                    raise GoalGovernanceConflict(
                        "expected autonomy version 3, current version 4"
                    )
                return original_release(*args, **kwargs)

            governance.reconcile_action_release = version_racing_release

            def rejected_before_ledger():
                current = goals.get(goal.id)
                assert current is not None
                released = goals.release_lease(
                    goal.id,
                    GoalMutationContext(
                        actor_principal=self.actor,
                        authority_instance_id="instance-a",
                        idempotency_key="preledger-release-a",
                        expected_version=current.version,
                        policy_revision=current.policy.revision,
                        fencing_token=current.lease.fencing_token,
                    ),
                )
                handed_off = goals.schedule_wakeup(
                    goal.id,
                    GoalWakeup(
                        wake_at=datetime.now(UTC),
                        reason="authority-authored pre-ledger takeover",
                        eligible_instance_ids=["instance-b"],
                    ),
                    GoalMutationContext(
                        actor_principal="user:operator",
                        authority_instance_id="instance-a",
                        idempotency_key="preledger-handoff-b",
                        expected_version=released.version,
                        policy_revision=released.policy.revision,
                    ),
                )
                GoalService(goals.store, "instance-b").acquire_lease(
                    goal.id,
                    GoalMutationContext(
                        actor_principal="service:goal-supervisor:instance-b",
                        authority_instance_id="instance-b",
                        idempotency_key="preledger-claim-b",
                        expected_version=handed_off.version,
                        policy_revision=handed_off.policy.revision,
                    ),
                    ttl_seconds=600,
                )
                raise RuntimeError("fleet admission rejected the stale fence")

            with self.assertRaisesRegex(RuntimeError, "stale fence"):
                supervisor._governed_action(
                    goal,
                    "preledger-dispatch",
                    GoalActionRequest(
                        action_class="dispatch_work_package",
                        delegated=True,
                        provider_id="codex",
                        estimate=GoalUsage(actions=1, dispatches=1),
                    ),
                    rejected_before_ledger,
                    defer_release=True,
                )
            self.assertEqual(release_calls, 2)
            active = [
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.state != GoalReservationState.RELEASED
            ]
            self.assertEqual(active, [])
            self.assertEqual(ledger.list(limit=10), [])

    def test_target_copy_cannot_release_or_retry_authority_hold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, authority_ctx = self._fixture(
                Path(tmp)
            )
            record = self._record(provenance, state="running")
            record.session_id = "target-session"
            ledger.put(record)
            target_ctx = _Context(authority_ctx.services, instance_id="instance-b")
            unchanged = _release_goal_dispatch_reservation(
                target_ctx,
                ledger,
                record,
                outcome="target-running",
                applied=True,
            )
            assert unchanged.goal_provenance is not None
            self.assertIsNone(unchanged.goal_provenance.released_at)
            asyncio.run(_reconcile_goal_dispatch_reservations(target_ctx))
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.APPLIED)
            self.assertEqual(ledger.pending_goal_lifecycle("instance-b"), [])
            with self.assertRaises(HTTPException) as retry:
                _replace_goal_dispatch_reservation(
                    target_ctx,
                    ledger,
                    record,
                    idempotency_key="target-retry",
                )
            self.assertEqual(
                retry.exception.detail["code"], "goal_retry_wrong_authority"
            )

    def test_prompt_denial_after_session_allocation_keeps_applied_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="starting_session")
            record.session_id = "allocated-session"
            ledger.put(record)
            policy = goal.policy.model_copy(deep=True)
            policy.revision += 1
            policy.permitted_actions = []
            goals.revise(
                goal.id,
                GoalRevision(policy=policy, reason="Revoke before prompt delivery"),
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="late-policy-revocation",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            with self.assertRaises(HTTPException):
                _validate_goal_dispatch_record(
                    ctx,
                    ledger,
                    record,
                    sink="prompt-delivery",
                )
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)
            self.assertEqual(reservation.actual_usage.actions, 1)
            self.assertEqual(reservation.actual_usage.dispatches, 1)

    def test_restart_repairs_exact_session_identity_once_and_resumes_same_session(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(root)
            record = self._record(provenance, state="starting_session")
            record.session_id = "allocated-session"
            ledger.put(record)

            reloaded = DispatchStore(root / "ledger")
            ctx.services["dispatch_store"] = reloaded
            version_before = governance.get_state(goal.id).version
            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))

            recovered = reloaded.get(record.dispatch_id)
            assert recovered is not None and recovered.goal_provenance is not None
            identity = recovered.goal_provenance.execution_identity
            envelope = recovered.goal_provenance.materialization_envelope
            assert identity is not None and envelope is not None
            self.assertEqual(identity.work_package_id, envelope.work_package_id)
            self.assertEqual(identity.service_role, envelope.service_role)
            self.assertEqual(identity.session_id, "allocated-session")
            self.assertEqual(identity.target_instance_id, "instance-b")
            self.assertEqual(identity.provider_id, "codex")
            self.assertEqual(identity.fencing_token, provenance.fencing_token)
            self.assertTrue(recovered.resume_requested)
            self.assertEqual(recovered.resume_session_id, "allocated-session")
            version_after_bind = governance.get_state(goal.id).version
            self.assertEqual(version_after_bind, version_before + 1)

            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))
            replayed = reloaded.get(record.dispatch_id)
            assert replayed is not None and replayed.goal_provenance is not None
            self.assertEqual(replayed.goal_provenance.execution_identity, identity)
            self.assertEqual(governance.get_state(goal.id).version, version_after_bind)

    def test_authority_checkpoints_target_identity_before_identity_traffic(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, _governance, _goal, provenance, _ledger, ctx = self._fixture(
                Path(tmp)
            )
            bound = _bind_goal_dispatch_execution_identity(
                ctx,
                provenance,
                selected_authority="instance-a",
                session_id="session-a",
            )
            assert bound is not None and bound.execution_identity is not None
            record = self._record(bound, state="starting_session")
            record.session_id = "session-a"
            acknowledged = {
                "resolvable": True,
                "dispatch_id": record.dispatch_id,
                "session_id": "session-a",
                "execution_identity_digest": bound.execution_identity.digest,
            }
            request = MagicMock()
            with patch(
                "pa.modules.fleet._peer_dispatch_json",
                AsyncMock(return_value=acknowledged),
            ) as peer:
                asyncio.run(
                    _synchronize_target_goal_execution_identity(
                        request,
                        record,
                        {"goal_provenance": None, "session_id": None},
                    )
                )

            sent = peer.await_args.args[2]
            self.assertEqual(sent["session_id"], "session-a")
            self.assertEqual(
                sent["goal_provenance"]["execution_identity"]["digest"],
                bound.execution_identity.digest,
            )

    def test_restart_rejects_mismatched_partial_execution_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(root)
            bound = _bind_goal_dispatch_execution_identity(
                ctx,
                provenance,
                selected_authority="instance-a",
                session_id="allocated-session",
            )
            assert bound is not None
            exact = bound.execution_identity
            envelope = bound.materialization_envelope
            receipt = bound.materialization_receipt
            assert exact is not None and envelope is not None and receipt is not None
            mismatched = GoalExecutionIdentityV1(
                work_package_id=envelope.work_package_id,
                service_role=envelope.service_role,
                assigned_service_principal=("service:goal-executor:mismatched-session"),
                provider_id=exact.provider_id,
                target_instance_id=exact.target_instance_id,
                session_id="different-session",
                fencing_token=exact.fencing_token,
                materialization_receipt_digest=str(receipt.digest),
            )
            record = self._record(
                bound.model_copy(update={"execution_identity": mismatched}),
                state="starting_session",
            )
            record.session_id = "allocated-session"
            ledger.put(record)

            reloaded = DispatchStore(root / "ledger")
            ctx.services["dispatch_store"] = reloaded
            version_before = governance.get_state(goal.id).version
            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))

            rejected = reloaded.get(record.dispatch_id)
            assert rejected is not None
            self.assertEqual(rejected.state, "failed")
            self.assertEqual(rejected.error_code, "goal_execution_identity_mismatch")
            self.assertFalse(rejected.recoverable)
            self.assertEqual(governance.get_state(goal.id).version, version_before)

    def test_verifier_identity_is_role_correct_and_role_mutation_fails_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, _governance, _goal, provenance, ledger, ctx = self._fixture(
                Path(tmp), package_role=GoalActorRole.VERIFIER
            )
            bound = _bind_goal_dispatch_execution_identity(
                ctx,
                provenance,
                selected_authority="instance-a",
                session_id="verifier-session",
            )
            assert bound is not None and bound.execution_identity is not None
            identity = bound.execution_identity
            envelope = bound.materialization_envelope
            assert envelope is not None
            self.assertEqual(identity.work_package_id, envelope.work_package_id)
            self.assertEqual(identity.service_role, "verifier")
            self.assertTrue(
                identity.assigned_service_principal.startswith("service:goal-verifier:")
            )

            mutated_envelope = GoalMaterializationEnvelopeV1.model_validate(
                {
                    **envelope.model_dump(mode="python", exclude={"digest"}),
                    "service_role": "executor",
                }
            )
            mutated = bound.model_copy(
                update={"materialization_envelope": mutated_envelope}
            )
            with self.assertRaises(HTTPException) as bind_error:
                _bind_goal_dispatch_execution_identity(
                    ctx,
                    mutated,
                    selected_authority="instance-a",
                    session_id="verifier-session",
                )
            self.assertFalse(bind_error.exception.detail["recoverable"])

            retry_record = self._record(mutated, state="failed")
            with self.assertRaises(HTTPException) as retry_error:
                _replace_goal_dispatch_reservation(
                    ctx,
                    ledger,
                    retry_record,
                    idempotency_key="role-mutated-retry",
                )
            self.assertEqual(
                retry_error.exception.detail["code"],
                "goal_retry_materialization_widening",
            )

    def test_authority_followup_uses_fresh_reservation_replays_and_releases(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="running")
            record.session_id = "session-a"
            record.goal_provenance = _bind_goal_dispatch_execution_identity(
                ctx,
                record.goal_provenance,
                selected_authority="instance-a",
                session_id="session-a",
            )
            assert record.goal_provenance is not None
            assert record.goal_provenance.execution_identity is not None
            self.assertEqual(
                record.goal_provenance.execution_identity.session_id,
                "session-a",
            )
            self.assertFalse(
                record.goal_provenance.execution_identity.credential_authenticated()
            )
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx,
                ledger,
                record,
                outcome="initial-running",
                applied=True,
            )
            request = MagicMock()
            request.app.state.ctx = ctx
            request.headers = {}
            request.state.instance_authenticated = False
            request.state.user = SimpleNamespace(role="admin")
            acknowledged = {
                "accepted": True,
                "session_id": "session-a",
                "dispatch_id": record.dispatch_id,
                "prompt_id": "prompt-followup-a",
                "duplicate": False,
            }
            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_agent_json",
                    AsyncMock(return_value=acknowledged),
                ) as peer,
            ):
                first = asyncio.run(
                    prompt_dispatch_session(
                        request,
                        record.dispatch_id,
                        DispatchFollowupBody(
                            message="Continue under current policy.",
                            idempotency_key="followup-a",
                        ),
                    )
                )
                replay = asyncio.run(
                    prompt_dispatch_session(
                        request,
                        record.dispatch_id,
                        DispatchFollowupBody(
                            message="Continue under current policy.",
                            idempotency_key="followup-a",
                        ),
                    )
                )
            self.assertTrue(first["accepted"])
            self.assertTrue(replay["duplicate"])
            self.assertEqual(peer.await_count, 1)
            sent = peer.await_args.kwargs["body"]["goal_provenance"]
            self.assertNotEqual(
                sent["action_reservation_id"],
                provenance.action_reservation_id,
            )
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            operation = persisted.followup_operations["followup-a"]
            self.assertEqual(operation["state"], "accepted")
            self.assertIsNotNone(operation["goal_provenance"]["released_at"])
            reservations = governance.get_state(goal.id).action_reservations
            self.assertEqual(len(reservations), 2)
            self.assertTrue(
                all(
                    item.state == GoalReservationState.RELEASED for item in reservations
                )
            )
            self.assertEqual(reservations[-1].actual_usage.actions, 1)
            self.assertEqual(reservations[-1].actual_usage.dispatches, 1)

    def test_followup_budget_policy_and_fence_denials_never_call_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                Path(tmp), max_actions=1, max_dispatches=1
            )
            record = self._record(provenance, state="running")
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx, ledger, record, outcome="initial-running", applied=True
            )
            with self.assertRaises(HTTPException) as budget:
                _reserve_goal_dispatch_followup(
                    ctx,
                    ledger,
                    record,
                    idempotency_key="budget-denied",
                    fingerprint="budget-fingerprint",
                )
            self.assertEqual(
                budget.exception.detail["code"], "goal_followup_governance_denied"
            )

        with tempfile.TemporaryDirectory() as tmp:
            goals, _governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="running")
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx, ledger, record, outcome="initial-running", applied=True
            )
            policy = goal.policy.model_copy(deep=True)
            policy.revision += 1
            policy.permitted_actions = []
            goals.revise(
                goal.id,
                GoalRevision(policy=policy, reason="Revoke provider follow-ups"),
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="revoke-followups",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            with self.assertRaises(HTTPException) as policy_denial:
                _reserve_goal_dispatch_followup(
                    ctx,
                    ledger,
                    record,
                    idempotency_key="policy-denied",
                    fingerprint="policy-fingerprint",
                )
            self.assertEqual(
                policy_denial.exception.detail["code"],
                "goal_followup_governance_denied",
            )

        with tempfile.TemporaryDirectory() as tmp:
            goals, _governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="running")
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx, ledger, record, outcome="initial-running", applied=True
            )
            released = goals.release_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="release-followup-fence",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            goals.acquire_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal=self.actor,
                    authority_instance_id="instance-a",
                    idempotency_key="reacquire-followup-fence",
                    expected_version=released.version,
                    policy_revision=released.policy.revision,
                ),
                ttl_seconds=600,
            )
            with self.assertRaises(HTTPException) as fence:
                _reserve_goal_dispatch_followup(
                    ctx,
                    ledger,
                    record,
                    idempotency_key="fence-denied",
                    fingerprint="fence-fingerprint",
                )
            self.assertEqual(fence.exception.detail["code"], "stale_goal_fence")

    def test_cancelled_followup_recovery_releases_without_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="running")
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx, ledger, record, outcome="initial-running", applied=True
            )
            fresh = _reserve_goal_dispatch_followup(
                ctx,
                ledger,
                record,
                idempotency_key="cancelled-followup",
                fingerprint="cancelled-fingerprint",
            )
            assert fresh is not None
            record.followup_operations["cancelled-followup"]["state"] = (
                "cancelled_pending_release"
            )
            ledger.put(record)

            _reconcile_goal_dispatch_followups(ctx, ledger, record)

            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            operation = persisted.followup_operations["cancelled-followup"]
            self.assertEqual(operation["state"], "cancelled")
            self.assertIsNotNone(operation["goal_provenance"]["released_at"])
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == fresh.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)
            self.assertEqual(reservation.actual_usage.actions, 0)
            self.assertEqual(reservation.actual_usage.dispatches, 0)

    def test_retry_uses_fresh_bounded_reservation_and_persists_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                Path(tmp), max_attempts=2
            )
            record = self._record(provenance, state="failed")
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx, ledger, record, outcome="failed", applied=False
            )
            record = _replace_goal_dispatch_reservation(
                ctx, ledger, record, idempotency_key="retry-one"
            )
            retry_provenance = record.goal_provenance
            assert retry_provenance is not None
            self.assertNotEqual(
                retry_provenance.action_reservation_id,
                provenance.action_reservation_id,
            )
            self.assertEqual(retry_provenance.reservation_attempt, 2)
            assert retry_provenance.materialization_envelope is not None
            assert provenance.materialization_envelope is not None
            assert retry_provenance.materialization_receipt is not None
            assert provenance.materialization_receipt is not None
            self.assertEqual(
                retry_provenance.materialization_envelope.digest,
                provenance.materialization_envelope.digest,
            )
            self.assertEqual(
                retry_provenance.materialization_receipt.digest,
                provenance.materialization_receipt.digest,
            )
            before_replay = governance.get_state(goal.id).version
            replayed = _replace_goal_dispatch_reservation(
                ctx, ledger, record, idempotency_key="retry-one"
            )
            self.assertEqual(replayed.goal_provenance, retry_provenance)
            self.assertEqual(governance.get_state(goal.id).version, before_replay)

            reloaded = DispatchStore(Path(tmp) / "ledger").get(record.dispatch_id)
            assert reloaded is not None
            self.assertEqual(reloaded.goal_provenance, retry_provenance)
            record = _release_goal_dispatch_reservation(
                ctx, ledger, record, outcome="retry-failed", applied=False
            )
            with self.assertRaises(HTTPException) as same_key:
                _replace_goal_dispatch_reservation(
                    ctx, ledger, record, idempotency_key="retry-one"
                )
            self.assertEqual(
                same_key.exception.detail["code"],
                "goal_retry_reservation_released",
            )
            with self.assertRaises(HTTPException) as exhausted:
                _replace_goal_dispatch_reservation(
                    ctx, ledger, record, idempotency_key="retry-two"
                )
            self.assertEqual(exhausted.exception.detail["code"], "goal_retry_denied")

    def test_post_session_retry_resumes_exact_identity_after_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                root, max_attempts=2
            )
            record = self._record(provenance, state="failed")
            record.session_id = "session-old"
            record.goal_provenance = _bind_goal_dispatch_execution_identity(
                ctx,
                provenance,
                selected_authority="instance-a",
                session_id="session-old",
            )
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx,
                ledger,
                record,
                outcome="failed-after-session",
                applied=True,
            )
            record = _replace_goal_dispatch_reservation(
                ctx,
                ledger,
                record,
                idempotency_key="retry-existing-session",
            )
            retry_provenance = record.goal_provenance
            assert retry_provenance is not None
            original_identity = retry_provenance.execution_identity
            original_receipt = retry_provenance.materialization_receipt
            assert original_identity is not None and original_receipt is not None
            ledger.transition(record, "queued", "Retry admitted before crash.")

            reloaded = DispatchStore(root / "ledger")
            ctx.services["dispatch_store"] = reloaded
            recovered = reloaded.get(record.dispatch_id)
            assert recovered is not None
            recovered = _restore_goal_dispatch_execution_identity(
                ctx,
                reloaded,
                recovered,
            )
            assert recovered.goal_provenance is not None
            self.assertTrue(recovered.resume_requested)
            self.assertEqual(recovered.resume_session_id, "session-old")
            self.assertEqual(
                recovered.goal_provenance.execution_identity,
                original_identity,
            )
            self.assertEqual(
                recovered.goal_provenance.materialization_receipt,
                original_receipt,
            )
            materialization_projection = _goal_materialization_stage_provenance(
                recovered.goal_provenance
            )
            assert materialization_projection is not None
            self.assertIsNone(materialization_projection.execution_identity)
            self.assertEqual(
                materialization_projection.materialization_receipt,
                original_receipt,
            )

            version_before_replay = governance.get_state(goal.id).version
            replayed = _restore_goal_dispatch_execution_identity(
                ctx,
                reloaded,
                recovered,
            )
            self.assertEqual(
                replayed.goal_provenance.execution_identity,
                original_identity,
            )
            self.assertEqual(
                governance.get_state(goal.id).version,
                version_before_replay,
            )
            with self.assertRaises(HTTPException) as changed_session:
                _bind_goal_dispatch_execution_identity(
                    ctx,
                    recovered.goal_provenance,
                    selected_authority="instance-a",
                    session_id="session-new",
                )
            self.assertEqual(
                changed_session.exception.detail["code"],
                "goal_execution_identity_mismatch",
            )

    def test_retry_rejects_materialization_plan_widening(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, _governance, _goal, provenance, ledger, ctx = self._fixture(
                Path(tmp), max_attempts=2
            )
            record = self._record(provenance, state="failed")
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx,
                ledger,
                record,
                outcome="failed",
                applied=False,
            )
            assert record.materialization_plan is not None
            record.materialization_plan["summary"] = "widened after reservation"
            ledger.put(record)
            with self.assertRaises(HTTPException) as raised:
                _replace_goal_dispatch_reservation(
                    ctx,
                    ledger,
                    record,
                    idempotency_key="retry-widened",
                )
            self.assertEqual(
                raised.exception.detail["code"],
                "goal_retry_materialization_widening",
            )

    def test_followup_rejects_materialization_plan_widening(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, _governance, _goal, provenance, ledger, ctx = self._fixture(
                Path(tmp)
            )
            record = self._record(provenance, state="running")
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx,
                ledger,
                record,
                outcome="started",
                applied=True,
            )
            record.request_payload["model_id"] = "widened-model"
            ledger.put(record)
            with self.assertRaises(HTTPException) as raised:
                _reserve_goal_dispatch_followup(
                    ctx,
                    ledger,
                    record,
                    idempotency_key="followup-widened",
                    fingerprint="followup-widened-fingerprint",
                )
            self.assertEqual(
                raised.exception.detail["code"],
                "goal_followup_materialization_widening",
            )

    def test_retry_replay_survives_bounded_decision_projection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                Path(tmp), max_attempts=2
            )
            record = self._record(provenance, state="failed")
            ledger.put(record)
            record = _release_goal_dispatch_reservation(
                ctx, ledger, record, outcome="failed", applied=False
            )
            record = _replace_goal_dispatch_reservation(
                ctx, ledger, record, idempotency_key="durable-retry"
            )
            replacement = record.goal_provenance
            assert replacement is not None
            state = governance.get_state(goal.id)
            discovery_key = "discover-existing-retry"
            discovery_expected_version = state.version
            discovered_state, discovered, discovered_decision = (
                governance.replace_action_reservation(
                    goal.id,
                    provenance.action_reservation_id,
                    GovernanceMutationContext(
                        actor_principal=self.actor,
                        authority_instance_id="instance-a",
                        idempotency_key=discovery_key,
                        expected_version=discovery_expected_version,
                        policy_revision=goal.policy.revision,
                        goal_version=goal.version,
                        fencing_token=goal.lease.fencing_token,
                    ),
                )
            )
            assert discovered is not None
            self.assertEqual(
                discovered.id,
                replacement.action_reservation_id,
            )

            # Evict both replacement decisions from the bounded recent projection.
            state = discovered_state
            for index in range(205):
                state, _decision = governance.authorize_action(
                    goal.id,
                    GoalActionRequest(action_class="projection-eviction-probe"),
                    GovernanceMutationContext(
                        actor_principal=self.actor,
                        authority_instance_id="instance-a",
                        idempotency_key=f"evict-decision-{index}",
                        expected_version=state.version,
                        policy_revision=goal.policy.revision,
                        goal_version=goal.version,
                        fencing_token=goal.lease.fencing_token,
                    ),
                )
            self.assertNotIn(
                discovered_decision.id,
                {item.id for item in state.recent_decisions},
            )
            replay_state, replayed, replayed_decision = (
                governance.replace_action_reservation(
                    goal.id,
                    provenance.action_reservation_id,
                    GovernanceMutationContext(
                        actor_principal=self.actor,
                        authority_instance_id="instance-a",
                        idempotency_key=discovery_key,
                        expected_version=discovery_expected_version,
                        policy_revision=goal.policy.revision,
                        goal_version=goal.version,
                        fencing_token=goal.lease.fencing_token,
                    ),
                )
            )
            assert replayed is not None
            self.assertEqual(replayed.id, discovered.id)
            self.assertEqual(replayed_decision.id, discovered_decision.id)
            self.assertEqual(replay_state.version, state.version)

    def test_startup_recovery_releases_started_hold_not_queued_hold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="running")
            ledger.put(record)
            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None and persisted.goal_provenance is not None
            self.assertIsNotNone(persisted.goal_provenance.released_at)
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)
            self.assertEqual(reservation.actual_usage.dispatches, 1)

    def test_terminal_recovery_binds_identity_before_release_and_preserves_control(
        self,
    ) -> None:
        for prebound in (False, True):
            with self.subTest(prebound=prebound), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                _goals, governance, goal, provenance, ledger, ctx = self._fixture(root)
                record = self._record(provenance, state="failed")
                record.session_id = "terminal-session"
                if prebound:
                    record.goal_provenance = _bind_goal_dispatch_execution_identity(
                        ctx,
                        provenance,
                        selected_authority="instance-a",
                        session_id="terminal-session",
                    )
                ledger.put(record)
                original_identity = (
                    record.goal_provenance.execution_identity
                    if record.goal_provenance is not None
                    else None
                )

                reloaded = DispatchStore(root / "ledger")
                ctx.services["dispatch_store"] = reloaded
                asyncio.run(_reconcile_goal_dispatch_reservations(ctx))

                persisted = reloaded.get(record.dispatch_id)
                assert persisted is not None and persisted.goal_provenance is not None
                recovered_identity = persisted.goal_provenance.execution_identity
                assert recovered_identity is not None
                self.assertEqual(recovered_identity.session_id, "terminal-session")
                if original_identity is not None:
                    self.assertEqual(recovered_identity, original_identity)
                self.assertIsNotNone(persisted.goal_provenance.released_at)
                reservation = next(
                    item
                    for item in governance.get_state(goal.id).action_reservations
                    if item.id == provenance.action_reservation_id
                )
                self.assertEqual(reservation.state, GoalReservationState.RELEASED)
                self.assertEqual(
                    reservation.request.execution_identity,
                    recovered_identity,
                )

    def test_live_worker_terminal_binds_session_identity_before_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(Path(tmp))
            record = self._record(provenance, state="queued")
            ledger.put(record)

            async def fail_after_session_allocation(current: DispatchRecord) -> None:
                current.session_id = "fast-terminal-session"
                ledger.transition(
                    current,
                    "starting_session",
                    "Session allocated immediately before provider failure.",
                )
                raise RuntimeError("provider failed after session allocation")

            worker = DispatchWorker(
                ledger,
                fail_after_session_allocation,
                terminal=lambda current, outcome: (
                    _release_goal_dispatch_reservation_async(
                        ctx,
                        ledger,
                        current,
                        outcome=outcome,
                        applied=True,
                    )
                ),
            )
            asyncio.run(worker._execute(record))

            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None and persisted.goal_provenance is not None
            identity = persisted.goal_provenance.execution_identity
            assert identity is not None
            self.assertEqual(identity.session_id, "fast-terminal-session")
            self.assertIsNotNone(persisted.goal_provenance.released_at)
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)
            self.assertEqual(reservation.request.execution_identity, identity)

    def test_atomic_admission_begin_deduplicates_authority_and_target_races(
        self,
    ) -> None:
        for scope in ("authority", "target"):
            with self.subTest(scope=scope), tempfile.TemporaryDirectory() as tmp:
                operation_key = f"race-{scope}"
                _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                    Path(tmp), operation_key=operation_key, placement_bound=False
                )
                body = RemoteAgentStartBody(
                    provider="codex",
                    idempotency_key=operation_key,
                    goal_provenance=provenance,
                )
                barrier = Barrier(2)

                def begin():
                    barrier.wait()
                    return _persist_goal_dispatch_admission_trace(
                        ctx,
                        ledger,
                        body.model_copy(deep=True),
                        idempotency_key=operation_key,
                        request_fingerprint="same-submitted-fingerprint",
                        target_instance_id="instance-b",
                        principal_id="user:operator",
                        placement_policy="named_instance",
                        idempotency_scope=scope,
                    )

                with ThreadPoolExecutor(max_workers=2) as pool:
                    first, second = list(pool.map(lambda _index: begin(), range(2)))
                first_record, first_created = first
                second_record, second_created = second
                assert first_record is not None and second_record is not None
                self.assertEqual(first_record.dispatch_id, second_record.dispatch_id)
                self.assertEqual(first_record.mutation_id, second_record.mutation_id)
                self.assertEqual({first_created, second_created}, {False, True})
                self.assertEqual(len(ledger.list(limit=10)), 1)
                reloaded = DispatchStore(Path(tmp) / "ledger")
                persisted = reloaded.get(first_record.dispatch_id)
                assert persisted is not None
                self.assertEqual(persisted.mutation_id, first_record.mutation_id)
                reservation = next(
                    item
                    for item in governance.get_state(goal.id).action_reservations
                    if item.id == provenance.action_reservation_id
                )
                self.assertEqual(reservation.state, GoalReservationState.APPLIED)

    def test_atomic_admission_begin_rejects_same_key_different_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, _governance, _goal, provenance, ledger, ctx = self._fixture(
                Path(tmp), operation_key="conflict-key", placement_bound=False
            )
            body = RemoteAgentStartBody(
                provider="codex",
                idempotency_key="conflict-key",
                goal_provenance=provenance,
            )
            first, created = _persist_goal_dispatch_admission_trace(
                ctx,
                ledger,
                body,
                idempotency_key="conflict-key",
                request_fingerprint="first-fingerprint",
                target_instance_id="instance-b",
                principal_id="user:operator",
                placement_policy="named_instance",
                idempotency_scope="authority",
            )
            assert first is not None
            self.assertTrue(created)
            with self.assertRaises(DispatchIdempotencyConflict) as conflict:
                _persist_goal_dispatch_admission_trace(
                    ctx,
                    ledger,
                    body,
                    idempotency_key="conflict-key",
                    request_fingerprint="different-fingerprint",
                    target_instance_id="instance-b",
                    principal_id="user:operator",
                    placement_policy="named_instance",
                    idempotency_scope="authority",
                )
            self.assertEqual(conflict.exception.existing.dispatch_id, first.dispatch_id)
            self.assertEqual(len(ledger.list(limit=10)), 1)

    def test_admission_trace_promotes_same_identity_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                Path(tmp), operation_key="admission-key", placement_bound=False
            )
            body = RemoteAgentStartBody(
                provider="codex",
                idempotency_key="admission-key",
                goal_provenance=provenance,
            )
            trace, created = _persist_goal_dispatch_admission_trace(
                ctx,
                ledger,
                body,
                idempotency_key="admission-key",
                request_fingerprint="submitted-fingerprint",
                target_instance_id="instance-b",
                principal_id="user:operator",
                placement_policy="named_instance",
                idempotency_scope="authority",
            )
            assert trace is not None
            self.assertTrue(created)
            trace = self._bind_trace_placement(ctx, ledger, trace)
            trace.goal_provenance = _validate_goal_dispatch_provenance(
                ctx,
                trace.goal_provenance,
                "instance-a",
                sink="durable-admission",
                provider_id="codex",
            )
            trace = _mark_goal_admission_validated(ctx, ledger, trace)
            stripped = trace.model_copy(deep=True)
            stripped.goal_provenance = None
            with self.assertRaisesRegex(ValueError, "canonically validated"):
                ledger.admit(stripped, idempotency_scope="authority")
            unchanged = ledger.get(trace.dispatch_id)
            assert unchanged is not None
            self.assertEqual(unchanged.state, "admission_pending")
            self.assertEqual(unchanged.goal_provenance, trace.goal_provenance)
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.APPLIED)
            promoted = DispatchRecord(
                **trace.model_dump(
                    mode="python",
                    exclude={"request_fingerprint", "request_payload", "state"},
                ),
                request_fingerprint="resolved-fingerprint",
                request_payload={"provider": "codex", "message": "Do the work"},
                state="admission_pending",
            )
            admitted, duplicate = ledger.admit(
                promoted,
                idempotency_scope="authority",
            )
            self.assertFalse(duplicate)
            self.assertEqual(admitted.dispatch_id, trace.dispatch_id)
            self.assertEqual(admitted.mutation_id, trace.mutation_id)
            self.assertEqual(admitted.state, "queued")
            self.assertEqual(
                [event.state for event in admitted.events],
                ["admission_pending", "queued"],
            )
            self.assertEqual(len(ledger.list(limit=10)), 1)

    def test_restart_reclaims_crashed_admission_trace_and_same_key_replays_failed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                root, operation_key="crash-key", placement_bound=False
            )
            trace, created = _persist_goal_dispatch_admission_trace(
                ctx,
                ledger,
                RemoteAgentStartBody(
                    provider="codex",
                    idempotency_key="crash-key",
                    goal_provenance=provenance,
                ),
                idempotency_key="crash-key",
                request_fingerprint="crash-fingerprint",
                target_instance_id="instance-b",
                principal_id="user:operator",
                placement_policy="named_instance",
                idempotency_scope="authority",
            )
            assert trace is not None
            self.assertTrue(created)
            reloaded = DispatchStore(root / "ledger")
            ctx.services["dispatch_store"] = reloaded

            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))

            recovered = reloaded.get(trace.dispatch_id)
            assert recovered is not None and recovered.goal_provenance is not None
            self.assertEqual(recovered.state, "failed")
            self.assertEqual(recovered.error_code, "admission_interrupted")
            self.assertTrue(recovered.recoverable)
            self.assertIsNotNone(recovered.goal_provenance.released_at)
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)
            replay = reloaded.by_authority_idempotency("instance-a", "crash-key")
            assert replay is not None
            self.assertEqual(replay.dispatch_id, trace.dispatch_id)
            self.assertEqual(replay.state, "failed")

    def test_forged_unvalidated_admission_trace_never_releases_other_hold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                Path(tmp),
                operation_key="legitimate-operation",
                placement_bound=False,
            )
            trace, created = _persist_goal_dispatch_admission_trace(
                ctx,
                ledger,
                RemoteAgentStartBody(
                    provider="codex",
                    idempotency_key="forged-operation",
                    goal_provenance=provenance,
                ),
                idempotency_key="forged-operation",
                request_fingerprint="forged-fingerprint",
                target_instance_id="instance-b",
                principal_id="user:attacker",
                placement_policy="named_instance",
                idempotency_scope="authority",
            )
            assert trace is not None
            self.assertTrue(created)
            with self.assertRaises(HTTPException) as binding:
                _bind_goal_dispatch_placement(
                    ctx,
                    trace.goal_provenance,
                    selected_authority="instance-a",
                    operation_key=trace.idempotency_key or "",
                    target_instance_id="instance-b",
                    placement_input_digest=trace.goal_placement_input_digest or "",
                    placement_decision=self._placement_decision(),
                )
            self.assertEqual(
                binding.exception.detail["code"],
                "invalid_goal_reservation",
            )

            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))

            rejected = ledger.get(trace.dispatch_id)
            assert rejected is not None and rejected.goal_provenance is not None
            self.assertEqual(rejected.state, "failed")
            self.assertEqual(rejected.error_code, "invalid_goal_admission_trace")
            self.assertEqual(rejected.goal_admission_validation_state, "rejected")
            self.assertIsNone(rejected.goal_provenance.released_at)
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.APPLIED)
            self.assertIsNone(reservation.request.resolved_target_instance_id)
            self.assertEqual(ledger.pending_goal_lifecycle("instance-a"), [])

    def test_wrong_target_trace_neither_promotes_nor_releases_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                Path(tmp),
                operation_key="wrong-target-operation",
                placement_bound=False,
            )
            trace, created = _persist_goal_dispatch_admission_trace(
                ctx,
                ledger,
                RemoteAgentStartBody(
                    provider="codex",
                    idempotency_key="wrong-target-operation",
                    goal_provenance=provenance,
                ),
                idempotency_key="wrong-target-operation",
                request_fingerprint="wrong-target-fingerprint",
                target_instance_id="instance-b",
                principal_id="user:operator",
                placement_policy="named_instance",
                idempotency_scope="authority",
            )
            assert trace is not None
            self.assertTrue(created)
            trace.target_instance_id = "instance-c"
            ledger.put(trace)

            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))

            rejected = ledger.get(trace.dispatch_id)
            assert rejected is not None
            self.assertEqual(rejected.state, "failed")
            self.assertEqual(rejected.error_code, "invalid_goal_admission_trace")
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.APPLIED)

    def test_post_marker_target_or_decision_mutation_blocks_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                Path(tmp),
                operation_key="post-marker-target-mutation",
                placement_bound=False,
            )
            trace, created = _persist_goal_dispatch_admission_trace(
                ctx,
                ledger,
                RemoteAgentStartBody(
                    provider="codex",
                    idempotency_key="post-marker-target-mutation",
                    goal_provenance=provenance,
                ),
                idempotency_key="post-marker-target-mutation",
                request_fingerprint="post-marker-fingerprint",
                target_instance_id="instance-b",
                principal_id="user:operator",
                placement_policy="named_instance",
                idempotency_scope="authority",
            )
            assert trace is not None
            self.assertTrue(created)
            trace = self._bind_trace_placement(ctx, ledger, trace)
            trace.goal_provenance = _validate_goal_dispatch_provenance(
                ctx,
                trace.goal_provenance,
                "instance-a",
                sink="durable-admission",
                provider_id="codex",
            )
            trace = _mark_goal_admission_validated(ctx, ledger, trace)
            original_proof = trace.goal_admission_validation_proof

            original_decision = dict(trace.placement_decision or {})
            trace.placement_decision = {
                **original_decision,
                "policy": "least_busy",
            }
            ledger.put(trace)
            self.assertFalse(_goal_admission_proof_valid(ctx, trace))
            with self.assertRaisesRegex(ValueError, "canonically validated"):
                ledger.admit(trace, idempotency_scope="authority")
            trace.placement_decision = original_decision
            ledger.put(trace)
            self.assertTrue(_goal_admission_proof_valid(ctx, trace))

            trace.request_payload["model_id"] = "model-b"
            ledger.put(trace)
            self.assertFalse(_goal_admission_proof_valid(ctx, trace))
            with self.assertRaisesRegex(ValueError, "canonically validated"):
                ledger.admit(trace, idempotency_scope="authority")
            trace.request_payload.pop("model_id")
            ledger.put(trace)
            self.assertTrue(_goal_admission_proof_valid(ctx, trace))

            trace.target_instance_id = "instance-c"
            ledger.put(trace)
            self.assertFalse(_goal_admission_proof_valid(ctx, trace))
            self.assertNotEqual(
                original_proof,
                goal_admission_validation_proof(trace),
            )
            with self.assertRaisesRegex(ValueError, "canonically validated"):
                ledger.admit(trace, idempotency_scope="authority")

            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))
            rejected = ledger.get(trace.dispatch_id)
            assert rejected is not None
            self.assertEqual(rejected.state, "failed")
            self.assertEqual(rejected.error_code, "invalid_goal_admission_trace")
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.APPLIED)

    def test_recovery_replays_validation_after_crash_before_proof_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                root, operation_key="validation-crash", placement_bound=False
            )
            trace, created = _persist_goal_dispatch_admission_trace(
                ctx,
                ledger,
                RemoteAgentStartBody(
                    provider="codex",
                    idempotency_key="validation-crash",
                    goal_provenance=provenance,
                ),
                idempotency_key="validation-crash",
                request_fingerprint="validation-crash-fingerprint",
                target_instance_id="instance-b",
                principal_id="user:operator",
                placement_policy="named_instance",
                idempotency_scope="authority",
            )
            assert trace is not None
            self.assertTrue(created)
            trace = self._bind_trace_placement(ctx, ledger, trace)
            refreshed = _validate_goal_dispatch_provenance(
                ctx,
                trace.goal_provenance,
                "instance-a",
                sink="durable-admission",
                provider_id="codex",
            )
            assert refreshed is not None
            before = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            before_renewals = before.renewal_count
            self.assertEqual(trace.goal_admission_validation_state, "pending")
            reloaded = DispatchStore(root / "ledger")
            ctx.services["dispatch_store"] = reloaded

            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))

            recovered = reloaded.get(trace.dispatch_id)
            assert recovered is not None and recovered.goal_provenance is not None
            self.assertEqual(recovered.state, "failed")
            self.assertEqual(recovered.goal_admission_validation_state, "validated")
            self.assertIsNotNone(recovered.goal_admission_validation_proof)
            self.assertIsNotNone(recovered.goal_provenance.released_at)
            after = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(after.renewal_count, before_renewals)
            self.assertEqual(after.state, GoalReservationState.RELEASED)

    def test_recovery_crash_after_terminal_trace_remains_indexed_until_release(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                root,
                operation_key="release-crash-key",
                placement_bound=False,
            )
            trace, created = _persist_goal_dispatch_admission_trace(
                ctx,
                ledger,
                RemoteAgentStartBody(
                    provider="codex",
                    idempotency_key="release-crash-key",
                    goal_provenance=provenance,
                ),
                idempotency_key="release-crash-key",
                request_fingerprint="release-crash-fingerprint",
                target_instance_id="instance-b",
                principal_id="user:operator",
                placement_policy="named_instance",
                idempotency_scope="authority",
            )
            assert trace is not None
            self.assertTrue(created)
            with patch(
                "pa.modules.fleet._release_goal_dispatch_reservation",
                side_effect=RuntimeError("injected release crash"),
            ):
                asyncio.run(_reconcile_goal_dispatch_reservations(ctx))

            stranded = ledger.get(trace.dispatch_id)
            assert stranded is not None and stranded.goal_provenance is not None
            self.assertEqual(stranded.state, "failed")
            self.assertEqual(stranded.error_code, "admission_interrupted")
            self.assertIsNone(stranded.goal_provenance.released_at)
            self.assertEqual(
                [
                    item.dispatch_id
                    for item in ledger.pending_goal_lifecycle("instance-a")
                ],
                [trace.dispatch_id],
            )
            replay = ledger.by_authority_idempotency("instance-a", "release-crash-key")
            assert replay is not None
            self.assertEqual(replay.state, "failed")

            asyncio.run(_reconcile_goal_dispatch_reservations(ctx))
            closed = ledger.get(trace.dispatch_id)
            assert closed is not None and closed.goal_provenance is not None
            self.assertIsNotNone(closed.goal_provenance.released_at)
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)

    def test_normal_admission_denial_terminalizes_trace_before_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _goals, governance, goal, provenance, ledger, ctx = self._fixture(
                Path(tmp), operation_key="denied-key", placement_bound=False
            )
            trace, created = _persist_goal_dispatch_admission_trace(
                ctx,
                ledger,
                RemoteAgentStartBody(
                    provider="codex",
                    idempotency_key="denied-key",
                    goal_provenance=provenance,
                ),
                idempotency_key="denied-key",
                request_fingerprint="denied-fingerprint",
                target_instance_id="instance-b",
                principal_id="user:operator",
                placement_policy="named_instance",
                idempotency_scope="authority",
            )
            assert trace is not None
            self.assertTrue(created)
            failed = _fail_goal_dispatch_admission(
                ctx,
                ledger,
                trace,
                message="Materialization preflight denied this request.",
                code="materialization_preflight_required",
                recoverable=True,
            )
            assert failed is not None and failed.goal_provenance is not None
            self.assertEqual(failed.state, "failed")
            self.assertEqual(failed.error_code, "materialization_preflight_required")
            self.assertIsNotNone(failed.goal_provenance.released_at)
            reservation = next(
                item
                for item in governance.get_state(goal.id).action_reservations
                if item.id == provenance.action_reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)


if __name__ == "__main__":
    unittest.main()
