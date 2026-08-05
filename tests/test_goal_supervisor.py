from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from pa.domain.notifications import InteractionState, Notification
from pa.domain.projection import CardProjection
from pa.execution.progress import CompletionReportV1, ProgressValidationV1
from pa.goals.advanced_models import GoalActionDisposition, GoalActionRequest
from pa.goals.authorization import authorize_proposal
from pa.goals.governance import GoalGovernanceConflict
from pa.goals.models import (
    AuthorizationOutcome,
    CreateWorkPackageAction,
    CriterionVerdict,
    DispatchWorkPackageAction,
    EvidenceKind,
    Goal,
    GoalActorRole,
    GoalAuditCreate,
    GoalCreate,
    GoalCriterion,
    GoalEvidence,
    GoalEvidenceCreate,
    GoalInteractionState,
    GoalMutationContext,
    GoalPolicy,
    GoalProposal,
    GoalProposalCreate,
    GoalState,
    GoalTransition,
    ProposalStatus,
    TransitionGoalAction,
    WorkPackageState,
)
from pa.goals.service import GoalConflict, GoalService
from pa.goals.supervisor import GoalSupervisor
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


class FakeDispatchStore:
    def __init__(self) -> None:
        self.records: dict[str, SimpleNamespace] = {}

    def get(self, dispatch_id: str):
        return self.records.get(dispatch_id)


class ProjectionNotifications:
    def __init__(self, projection: CardProjection) -> None:
        self.projection = projection

    def create(
        self,
        data,
        *,
        principal_id: str,
        instance_id: str,
    ) -> Notification:
        values = data.model_dump(exclude={"id"}, mode="python")
        notification = Notification(id=data.id or str(uuid4()), **values)
        return self.projection.save_notification(
            notification,
            principal_id=principal_id,
            instance_id=instance_id,
        )


def dispatch_record(
    dispatch_id: str,
    state: str,
    *,
    session_id: str | None = None,
    final_report: CompletionReportV1 | None = None,
    last_error: str | None = None,
):
    return SimpleNamespace(
        dispatch_id=dispatch_id,
        state=state,
        session_id=session_id,
        final_report=final_report,
        last_error=last_error,
        latest_progress=None,
    )


class GoalSupervisorTests(unittest.TestCase):
    def _services(self, tmp: str):
        root = Path(tmp)
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root, "instance-a")
        projection = CardProjection(root / "projection.db", log)
        return GoalService(projection, "instance-a"), projection

    @staticmethod
    def _ctx(goal_version: int, key: str, fence: int | None = None):
        return GoalMutationContext(
            actor_principal="agent:coordinator",
            authority_instance_id="instance-a",
            idempotency_key=key,
            expected_version=goal_version,
            policy_revision=1,
            fencing_token=fence,
        )

    @staticmethod
    def _goal_create(criterion: GoalCriterion) -> GoalCreate:
        return GoalCreate(
            objective="Ship supervised orchestration",
            owner_principal="user:operator",
            criteria=[criterion],
            policy=GoalPolicy(
                autonomy_level=4,
                permitted_actions=[
                    "create_work_package",
                    "dispatch_work_package",
                    "request_operator",
                    "record_evidence",
                    "revise_strategy",
                    "transition_goal",
                ],
            ),
        )

    def test_authorization_is_deterministic_and_role_policy_bounded(self) -> None:
        criterion = GoalCriterion(
            description="verified",
            verification_method="tests",
            evidence_requirement="passing validation",
        )
        goal = Goal(**self._goal_create(criterion).model_dump(mode="python"))
        proposal = GoalProposal(
            proposer_principal="agent:coordinator",
            proposer_role=GoalActorRole.COORDINATOR,
            action=CreateWorkPackageAction(
                title="Implement",
                objective="Implement the bounded feature",
                criterion_ids=[criterion.id],
            ),
            rationale="The criterion needs implementation.",
            expected_goal_version=goal.version,
            policy_revision=goal.policy.revision,
        )
        first = authorize_proposal(goal, proposal, instance_id="instance-a")
        second = authorize_proposal(goal, proposal, instance_id="instance-b")
        self.assertEqual(first.outcome, AuthorizationOutcome.AUTHORIZE)
        self.assertEqual(first.decision_hash, second.decision_hash)

        forbidden = proposal.model_copy(
            update={
                "id": "forbidden",
                "proposer_role": GoalActorRole.VERIFIER,
                "action": DispatchWorkPackageAction(
                    work_package_id="missing",
                    placement_policy="balanced",
                ),
            }
        )
        decision = authorize_proposal(goal, forbidden, instance_id="instance-a")
        self.assertEqual(decision.outcome, AuthorizationOutcome.REJECT)
        self.assertEqual(decision.reason_code, "role_forbidden")

        approval_goal = goal.model_copy(deep=True)
        approval_goal.policy.require_operator_for = ["create_*"]
        decision = authorize_proposal(approval_goal, proposal, instance_id="instance-a")
        self.assertEqual(decision.outcome, AuthorizationOutcome.REQUIRE_OPERATOR)

        invalid_transition = proposal.model_copy(
            update={
                "id": "invalid-transition",
                "action": TransitionGoalAction(
                    state=GoalState.ACHIEVED,
                    reason="cannot skip the lifecycle",
                ),
            }
        )
        decision = authorize_proposal(
            goal, invalid_transition, instance_id="instance-a"
        )
        self.assertEqual(decision.outcome, AuthorizationOutcome.REJECT)
        self.assertEqual(decision.reason_code, "invalid_transition")

    def test_proposal_principal_must_match_authenticated_actor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _projection = self._services(tmp)
            criterion = GoalCriterion(
                description="attributable proposal",
                verification_method="identity check",
                evidence_requirement="matching actor",
            )
            goal = service.create(self._goal_create(criterion), self._ctx(0, "create"))
            with self.assertRaisesRegex(GoalConflict, "authenticated mutation actor"):
                service.submit_proposal(
                    goal.id,
                    GoalProposalCreate(
                        proposer_principal="agent:someone-else",
                        proposer_role=GoalActorRole.COORDINATOR,
                        action=CreateWorkPackageAction(
                            title="Spoofed",
                            objective="Must be rejected",
                            criterion_ids=[criterion.id],
                        ),
                        rationale="Identity must be durable.",
                        expected_goal_version=goal.version,
                        policy_revision=1,
                    ),
                    self._ctx(goal.version, "spoofed"),
                )

    def test_governed_action_replay_requires_a_still_applied_hold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="replay exactly once",
                verification_method="durable reservation state",
                evidence_requirement="one side effect",
            )
            goal = service.create(
                self._goal_create(criterion), self._ctx(0, "create-replay-goal")
            )
            supervisor = GoalSupervisor(service, projection, "instance-a")
            goal = service.acquire_lease(
                goal.id,
                supervisor._context(goal, "lease-replay-goal"),
                ttl_seconds=120,
            )
            request = GoalActionRequest(action_class="create_work_package")
            failed_calls: list[str] = []

            def fail_once():
                failed_calls.append("failed")
                raise RuntimeError("side effect failed before checkpoint")

            with self.assertRaisesRegex(RuntimeError, "before checkpoint"):
                supervisor._governed_action(
                    goal,
                    "governed-crash-replay",
                    request,
                    fail_once,
                )
            with self.assertRaisesRegex(
                GoalGovernanceConflict, "remain applied"
            ):
                supervisor._governed_action(
                    goal,
                    "governed-crash-replay",
                    request,
                    lambda: failed_calls.append("must-not-run"),
                )
            self.assertEqual(failed_calls, ["failed"])

            resumable = service.create(
                self._goal_create(criterion), self._ctx(0, "create-resume-goal")
            )
            resumable = service.acquire_lease(
                resumable.id,
                supervisor._context(resumable, "lease-resume-goal"),
                ttl_seconds=120,
            )
            state = supervisor.governance.get_state(resumable.id)
            state, reserved = supervisor.governance.authorize_action(
                resumable.id,
                request,
                supervisor._governance_context(
                    resumable,
                    state.version,
                    "governed-applied-resume:reserve",
                ),
            )
            self.assertEqual(reserved.disposition, GoalActionDisposition.AUTHORIZED)
            state, applied = supervisor.governance.apply_action(
                resumable.id,
                reserved.reservation_id or "",
                supervisor._governance_context(
                    resumable,
                    state.version,
                    "governed-applied-resume:apply",
                ),
            )
            self.assertEqual(applied.disposition, GoalActionDisposition.AUTHORIZED)
            resumed_calls: list[str] = []
            result = supervisor._governed_action(
                resumable,
                "governed-applied-resume",
                request,
                lambda: resumed_calls.append("resumed") or "completed",
            )
            self.assertEqual(result, "completed")
            self.assertEqual(resumed_calls, ["resumed"])

    def test_supervisor_materializes_card_and_dispatches_authorized_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="implementation complete",
                verification_method="independent tests",
                evidence_requirement="passing checks",
            )
            goal = service.create(self._goal_create(criterion), self._ctx(0, "create"))
            goal = service.transition(
                goal.id,
                GoalTransition(state=GoalState.READY, reason="shaped"),
                self._ctx(goal.version, "ready"),
            )
            goal = service.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal="agent:coordinator",
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=CreateWorkPackageAction(
                        title="Implement supervisor",
                        objective="Build the durable supervisor",
                        criterion_ids=[criterion.id],
                    ),
                    rationale="Implementation is required.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            calls: list[dict] = []

            def dispatch(payload: dict) -> dict:
                calls.append(payload)
                return {"dispatch_id": "dispatch-1"}

            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=FakeDispatchStore(),
                dispatch=dispatch,
            )
            first = supervisor.run_once(goal.id)[0]
            self.assertEqual(len(first.work_packages), 1)
            package = first.work_packages[0]
            self.assertIsNotNone(package.card_id)
            self.assertIn(package.card_id, first.linked_card_ids)
            card = projection.get_card(package.card_id, realm_id="default")
            assert card is not None
            self.assertIn(f"goal-work-package:{package.id}", card.tags)

            second = supervisor.run_once(goal.id)[0]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["card_id"], package.card_id)
            self.assertEqual(second.work_packages[0].state, WorkPackageState.DISPATCHED)
            self.assertEqual(second.linked_dispatch_ids, ["dispatch-1"])
            self.assertEqual(second.state, GoalState.ACTIVE)

    def test_failed_dispatch_recovers_with_replacement_session_and_attempt(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="recoverable",
                verification_method="replacement dispatch",
                evidence_requirement="new session",
            )
            goal = service.create(self._goal_create(criterion), self._ctx(0, "create"))
            goal = service.transition(
                goal.id,
                GoalTransition(state=GoalState.READY, reason="ready"),
                self._ctx(goal.version, "ready"),
            )
            goal = service.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal="agent:coordinator",
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=CreateWorkPackageAction(
                        title="Recover work",
                        objective="Exercise replacement recovery",
                        criterion_ids=[criterion.id],
                    ),
                    rationale="Recovery must be tested.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            dispatched: list[str] = []

            def dispatch(_payload: dict) -> dict:
                dispatch_id = f"dispatch-{len(dispatched) + 1}"
                dispatched.append(dispatch_id)
                return {"dispatch_id": dispatch_id}

            records = FakeDispatchStore()
            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=records,
                dispatch=dispatch,
            )
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            records.records["dispatch-1"] = dispatch_record(
                "dispatch-1",
                "failed",
                session_id="session-dead",
                last_error="provider thread disappeared",
            )
            recovered = supervisor.run_once(goal.id)[0]
            self.assertEqual(recovered.work_packages[0].state, WorkPackageState.READY)
            self.assertIn(
                "session-dead",
                recovered.work_packages[0].replacement_session_ids,
            )

            # One cycle authorizes the replacement dispatch; the next applies it.
            supervisor.run_once(goal.id)
            replacement = service.get(goal.id)
            assert replacement is not None
            self.assertEqual(dispatched, ["dispatch-1", "dispatch-2"])
            self.assertEqual(replacement.work_packages[0].attempts, 2)
            self.assertEqual(
                replacement.work_packages[0].dispatch_ids,
                ["dispatch-1", "dispatch-2"],
            )

    def test_verifier_role_requires_passing_validation_before_criterion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="independently verified",
                verification_method="focused suite",
                evidence_requirement="passed command",
                require_independent_verifier=True,
            )
            goal = service.create(self._goal_create(criterion), self._ctx(0, "create"))
            goal = service.transition(
                goal.id,
                GoalTransition(state=GoalState.READY, reason="ready"),
                self._ctx(goal.version, "ready"),
            )
            goal = service.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal="agent:coordinator",
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=CreateWorkPackageAction(
                        title="Implement",
                        objective="Implement the feature",
                        criterion_ids=[criterion.id],
                    ),
                    rationale="Start execution.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            dispatched: list[str] = []

            def dispatch(_payload: dict) -> dict:
                dispatch_id = f"dispatch-{len(dispatched) + 1}"
                dispatched.append(dispatch_id)
                return {"dispatch_id": dispatch_id}

            records = FakeDispatchStore()
            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=records,
                dispatch=dispatch,
            )
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            records.records["dispatch-1"] = dispatch_record(
                "dispatch-1", "completed", session_id="executor-session"
            )
            with_verifier_proposal = supervisor.run_once(goal.id)[0]
            self.assertEqual(
                with_verifier_proposal.work_packages[0].state,
                WorkPackageState.AWAITING_VERIFICATION,
            )

            # Authorize/create the verifier, then authorize/dispatch its ready card.
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            current = service.get(goal.id)
            assert current is not None
            verifier = next(
                item
                for item in current.work_packages
                if item.role == GoalActorRole.VERIFIER
            )
            if not verifier.dispatch_ids:
                current = supervisor.run_once(goal.id)[0]
                verifier = next(
                    item
                    for item in current.work_packages
                    if item.role == GoalActorRole.VERIFIER
                )
            verifier_dispatch = verifier.dispatch_ids[-1]
            records.records[verifier_dispatch] = dispatch_record(
                verifier_dispatch,
                "completed",
                session_id="verifier-session",
                final_report=CompletionReportV1(
                    outcome="Independent focused suite passed.",
                    validations=[
                        ProgressValidationV1(
                            command="pytest focused",
                            status="passed",
                            summary="all passed",
                        )
                    ],
                ),
            )
            verified = supervisor.run_once(goal.id)[0]
            self.assertEqual(verified.criteria[0].verdict, CriterionVerdict.SATISFIED)
            self.assertTrue(verified.evidence)
            self.assertEqual(verified.work_packages[0].state, WorkPackageState.VERIFIED)
            executor = next(
                item
                for item in verified.work_packages
                if item.role == GoalActorRole.EXECUTOR
            )
            verifier = next(
                item
                for item in verified.work_packages
                if item.role == GoalActorRole.VERIFIER
            )
            self.assertNotEqual(
                executor.executor_service_id, verifier.verifier_service_id
            )
            self.assertEqual(
                verified.evidence[-1].producer_service_id,
                verifier.verifier_service_id,
            )
            with self.assertRaisesRegex(GoalConflict, "assigned independent verifier"):
                service.add_evidence(
                    verified.id,
                    GoalEvidenceCreate(
                        evidence=GoalEvidence(
                            criterion_ids=[criterion.id],
                            kind=EvidenceKind.AUDIT,
                            summary="A forged verifier must not be accepted",
                        )
                    ),
                    GoalMutationContext(
                        actor_principal="service:goal-verifier:forged",
                        authority_instance_id="instance-a",
                        idempotency_key="forged-verifier-evidence",
                        expected_version=verified.version,
                        policy_revision=verified.policy.revision,
                        fencing_token=verified.lease.fencing_token,
                    ),
                )
            spoofed = service.add_evidence(
                verified.id,
                GoalEvidenceCreate(
                    evidence=GoalEvidence(
                        criterion_ids=[criterion.id],
                        kind=EvidenceKind.AUDIT,
                        summary="A supervisor cannot assert verifier provenance",
                        producer_role=GoalActorRole.VERIFIER,
                        producer_service_id=verifier.verifier_service_id,
                    )
                ),
                GoalMutationContext(
                    actor_principal=supervisor.service_principal,
                    authority_instance_id="instance-a",
                    idempotency_key="spoofed-verifier-fields",
                    expected_version=verified.version,
                    policy_revision=verified.policy.revision,
                    fencing_token=verified.lease.fencing_token,
                ),
            )
            spoofed_evidence = spoofed.evidence[-1]
            self.assertIsNone(spoofed_evidence.producer_role)
            self.assertIsNone(spoofed_evidence.producer_service_id)
            with self.assertRaisesRegex(GoalConflict, "independent verifier evidence"):
                service.audit(
                    spoofed.id,
                    GoalAuditCreate(
                        criterion_verdicts={criterion.id: CriterionVerdict.SATISFIED},
                        evidence_ids=[spoofed_evidence.id],
                        explanation="The forged body must not pass verification",
                    ),
                    GoalMutationContext(
                        actor_principal="agent:independent-auditor",
                        authority_instance_id="instance-a",
                        idempotency_key="reject-spoofed-verifier-fields",
                        expected_version=spoofed.version,
                        policy_revision=spoofed.policy.revision,
                        fencing_token=spoofed.lease.fencing_token,
                    ),
                )

    def test_operator_approval_survives_as_durable_correlated_interaction(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="operator approved",
                verification_method="correlated response",
                evidence_requirement="durable notification",
            )
            create = self._goal_create(criterion)
            create.policy.require_operator_for = ["create_work_package"]
            goal = service.create(create, self._ctx(0, "create"))
            goal = service.transition(
                goal.id,
                GoalTransition(state=GoalState.READY, reason="ready"),
                self._ctx(goal.version, "ready"),
            )
            goal = service.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal="agent:coordinator",
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=CreateWorkPackageAction(
                        title="Approved package",
                        objective="Wait for the durable approval response",
                        criterion_ids=[criterion.id],
                        dispatch_when_ready=False,
                    ),
                    rationale="Policy requires an operator.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                notification_service=ProjectionNotifications(projection),
            )
            waiting = supervisor.run_once(goal.id)[0]
            self.assertEqual(
                waiting.proposals[0].status, ProposalStatus.OPERATOR_REQUIRED
            )
            self.assertEqual(len(waiting.operator_interactions), 1)
            link = waiting.operator_interactions[0]
            notification = projection.get_notification(
                link.notification_id, realm_id="default"
            )
            assert notification is not None and notification.interaction is not None
            notification.interaction.state = InteractionState.ANSWERED
            notification.interaction.response = {
                "choice_id": "approve",
                "value": True,
            }
            notification.interaction.response_summary = "Approved"
            notification.version += 1
            projection.save_notification(
                notification,
                principal_id="user:operator",
                instance_id="instance-a",
            )

            resumed = supervisor.run_once(goal.id)[0]
            self.assertEqual(
                resumed.operator_interactions[0].state,
                GoalInteractionState.ANSWERED,
            )
            self.assertEqual(resumed.proposals[0].status, ProposalStatus.APPLIED)
            self.assertEqual(len(resumed.work_packages), 1)

    def test_stalled_progress_creates_critic_work_and_blocks_goal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="no stalls",
                verification_method="progress heartbeats",
                evidence_requirement="changing checkpoint",
            )
            goal = service.create(self._goal_create(criterion), self._ctx(0, "create"))
            goal = service.transition(
                goal.id,
                GoalTransition(state=GoalState.READY, reason="ready"),
                self._ctx(goal.version, "ready"),
            )
            goal = service.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal="agent:coordinator",
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=CreateWorkPackageAction(
                        title="Potentially stalled",
                        objective="Remain unchanged to exercise drift detection",
                        criterion_ids=[criterion.id],
                    ),
                    rationale="Test drift.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            records = FakeDispatchStore()
            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=records,
                dispatch=lambda _payload: {"dispatch_id": "dispatch-stalled"},
                no_progress_cycles=1,
                stalled_cycles=2,
            )
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            records.records["dispatch-stalled"] = dispatch_record(
                "dispatch-stalled", "running", session_id="session-stalled"
            )
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            stalled = supervisor.run_once(goal.id)[0]
            self.assertEqual(stalled.state, GoalState.BLOCKED)
            self.assertEqual(stalled.supervision.drift_state.value, "stalled")
            self.assertTrue(
                any(
                    isinstance(item.action, CreateWorkPackageAction)
                    and item.action.role == GoalActorRole.CRITIC
                    for item in stalled.proposals
                )
            )

            dispatched_before = list(stalled.linked_dispatch_ids)
            after = supervisor.run_once(goal.id)[0]
            executor = next(
                item
                for item in after.work_packages
                if item.role == GoalActorRole.EXECUTOR
            )
            self.assertEqual(executor.state, WorkPackageState.BLOCKED)
            self.assertEqual(after.linked_dispatch_ids, dispatched_before)


if __name__ == "__main__":
    unittest.main()
