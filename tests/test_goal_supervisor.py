from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from pa.domain.notifications import InteractionState, Notification
from pa.domain.projection import CardProjection
from pa.execution.dispatch import (
    DispatchEvent,
    DispatchRecord,
    GoalDispatchProvenance,
    goal_admission_validation_proof,
    goal_dispatch_placement_decision_digest,
    goal_dispatch_placement_input_digest,
    goal_dispatch_placement_input_snapshot,
)
from pa.execution.progress import CompletionReportV1, ProgressValidationV1
from pa.goals.advanced_models import (
    GoalActionDisposition,
    GoalActionRequest,
    GoalReservationState,
    GovernanceMutationContext,
)
from pa.goals.authorization import authorize_proposal
from pa.goals.governance import GoalGovernanceConflict
from pa.goals.materialization import GoalExecutionIdentityV1
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
    GoalDispatchAttemptState,
    GoalEvidence,
    GoalEvidenceCreate,
    GoalInteractionState,
    GoalMutationContext,
    GoalPolicy,
    GoalProposal,
    GoalProposalCreate,
    GoalState,
    GoalTransition,
    GoalWorkPackage,
    ProposalStatus,
    RecordEvidenceAction,
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

    def by_authority_idempotency(
        self,
        authority_instance_id: str,
        idempotency_key: str,
    ):
        return next(
            (
                record
                for record in self.records.values()
                if getattr(record, "authority_instance_id", None)
                == authority_instance_id
                and getattr(record, "idempotency_key", None) == idempotency_key
            ),
            None,
        )


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
    def _services(self, tmp: str, *, now=None):
        root = Path(tmp)
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root, "instance-a")
        projection = CardProjection(root / "projection.db", log)
        return GoalService(projection, "instance-a", clock=now), projection

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
            with self.assertRaisesRegex(
                GoalConflict, "role does not match the authenticated actor assignment"
            ):
                service.submit_proposal(
                    goal.id,
                    GoalProposalCreate(
                        proposer_principal="agent:coordinator",
                        proposer_role=GoalActorRole.VERIFIER,
                        action=RecordEvidenceAction(
                            evidence=GoalEvidence(
                                criterion_ids=[criterion.id],
                                kind=EvidenceKind.TEST,
                                summary="An ordinary actor cannot self-label as verifier.",
                            )
                        ),
                        rationale="Exercise role derivation.",
                        expected_goal_version=goal.version,
                        policy_revision=goal.policy.revision,
                    ),
                    self._ctx(goal.version, "spoofed-role"),
                )

    def test_record_evidence_proposals_use_the_central_ingestion_boundary(
        self,
    ) -> None:
        clock = [datetime(2026, 8, 5, 12, tzinfo=UTC)]

        def exercise(
            tmp: str,
            *,
            evidence: GoalEvidence,
            verdicts: dict[str, CriterionVerdict],
            criteria: list[GoalCriterion],
            key: str,
        ) -> Goal:
            service, projection = self._services(tmp, now=lambda: clock[0])
            create = self._goal_create(criteria[0]).model_copy(
                update={"criteria": criteria}
            )
            goal = service.create(create, self._ctx(0, f"create-{key}"))
            goal = service.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal="agent:coordinator",
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=RecordEvidenceAction(
                        evidence=evidence,
                        criterion_verdicts=verdicts,
                    ),
                    rationale="Exercise the canonical evidence boundary.",
                    expected_goal_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
                self._ctx(goal.version, f"proposal-{key}"),
            )
            result = GoalSupervisor(
                service,
                projection,
                "instance-a",
                now=lambda: clock[0],
            ).run_once(goal.id)
            self.assertEqual(len(result), 1)
            return result[0]

        with tempfile.TemporaryDirectory() as tmp:
            criterion = GoalCriterion(
                description="server-derived evidence provenance",
                verification_method="identity check",
                evidence_requirement="unspoofable recorder and producer",
            )
            applied = exercise(
                tmp,
                evidence=GoalEvidence(
                    criterion_ids=[criterion.id],
                    kind=EvidenceKind.TEST,
                    summary="The body attempts to impersonate a verifier.",
                    observed_at=clock[0],
                    recorded_by_principal="service:goal-verifier:forged",
                    recorded_by_instance_id="forged-instance",
                    producer_role=GoalActorRole.VERIFIER,
                    producer_service_id="service:goal-verifier:forged",
                ),
                verdicts={criterion.id: CriterionVerdict.SATISFIED},
                criteria=[criterion],
                key="spoofed-provenance",
            )
            self.assertEqual(applied.proposals[0].status, ProposalStatus.APPLIED)
            recorded = applied.evidence[0]
            self.assertEqual(
                recorded.recorded_by_principal,
                "service:goal-supervisor:instance-a",
            )
            self.assertEqual(recorded.recorded_by_instance_id, "instance-a")
            self.assertIsNone(recorded.producer_role)
            self.assertIsNone(recorded.producer_service_id)

        invalid_cases = (
            (
                "future",
                lambda first, _second: GoalEvidence(
                    criterion_ids=[first.id],
                    kind=EvidenceKind.TEST,
                    summary="Future observation",
                    observed_at=clock[0] + timedelta(minutes=6),
                ),
                lambda first, _second: {first.id: CriterionVerdict.SATISFIED},
                "cannot be in the future",
            ),
            (
                "expiry",
                lambda first, _second: GoalEvidence(
                    criterion_ids=[first.id],
                    kind=EvidenceKind.TEST,
                    summary="Invalid expiry",
                    observed_at=clock[0],
                    expires_at=clock[0],
                ),
                lambda first, _second: {first.id: CriterionVerdict.SATISFIED},
                "expiry must follow",
            ),
            (
                "unmapped",
                lambda first, _second: GoalEvidence(
                    criterion_ids=[first.id],
                    kind=EvidenceKind.TEST,
                    summary="Verdict is outside the evidence mapping",
                    observed_at=clock[0],
                ),
                lambda _first, second: {second.id: CriterionVerdict.SATISFIED},
                "mapped to each criterion",
            ),
        )
        for key, evidence_factory, verdict_factory, error in invalid_cases:
            with self.subTest(case=key), tempfile.TemporaryDirectory() as tmp:
                first = GoalCriterion(
                    description=f"{key} primary criterion",
                    verification_method="validation",
                    evidence_requirement="valid evidence",
                )
                second = GoalCriterion(
                    description=f"{key} secondary criterion",
                    verification_method="validation",
                    evidence_requirement="mapped evidence",
                )
                failed = exercise(
                    tmp,
                    evidence=evidence_factory(first, second),
                    verdicts=verdict_factory(first, second),
                    criteria=[first, second],
                    key=key,
                )
                self.assertEqual(failed.proposals[0].status, ProposalStatus.FAILED)
                self.assertIn(error, failed.proposals[0].error or "")
                self.assertEqual(failed.evidence, [])
                self.assertTrue(
                    all(
                        item.verdict == CriterionVerdict.PENDING
                        for item in failed.criteria
                    )
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
            with self.assertRaisesRegex(GoalGovernanceConflict, "remain applied"):
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

    def test_missing_completion_evidence_is_rejected_in_the_first_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="achievement is evidenced",
                verification_method="independent audit",
                evidence_requirement="fresh mapped proof",
            )
            goal = service.create(
                self._goal_create(criterion), self._ctx(0, "create-completion-gate")
            )
            for state in (GoalState.READY, GoalState.ACTIVE, GoalState.VERIFYING):
                goal = service.transition(
                    goal.id,
                    GoalTransition(state=state, reason="advance to verification"),
                    self._ctx(goal.version, f"completion-gate-{state.value}"),
                )
            goal = service.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal="agent:coordinator",
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=TransitionGoalAction(
                        state=GoalState.ACHIEVED,
                        reason="Claim achievement without proof",
                    ),
                    rationale="Exercise the completion gate.",
                    expected_goal_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
                GoalMutationContext(
                    actor_principal="agent:coordinator",
                    authority_instance_id="instance-a",
                    idempotency_key="propose-unsupported-achievement",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
            )
            supervisor = GoalSupervisor(service, projection, "instance-a")

            first = supervisor.run_once(goal.id)[0]
            proposal = first.proposals[0]
            self.assertEqual(proposal.status, ProposalStatus.REJECTED)
            assert proposal.authorization is not None
            self.assertEqual(
                proposal.authorization.reason_code,
                "completion_requirements_unsatisfied",
            )
            self.assertIn("audit", proposal.authorization.explanation)
            version = first.version
            self.assertEqual(supervisor.run_once(goal.id), [])
            persisted = service.get(goal.id)
            assert persisted is not None
            self.assertEqual(persisted.version, version)
            self.assertEqual(persisted.proposals[0].status, ProposalStatus.REJECTED)

    def test_apply_rechecks_completion_evidence_freshness_before_transition(
        self,
    ) -> None:
        clock = [datetime(2026, 8, 5, tzinfo=UTC)]
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp, now=lambda: clock[0])
            criterion = GoalCriterion(
                description="achievement stays fresh",
                verification_method="recent focused suite",
                evidence_requirement="proof less than one second old",
                freshness_seconds=1,
            )
            goal = service.create(
                self._goal_create(criterion),
                self._ctx(0, "create-apply-freshness-gate"),
            )
            evidence = GoalEvidence(
                criterion_ids=[criterion.id],
                kind=EvidenceKind.TEST,
                summary="Initially fresh proof",
                observed_at=clock[0],
            )
            goal = service.add_evidence(
                goal.id,
                GoalEvidenceCreate(evidence=evidence),
                self._ctx(goal.version, "record-fresh-apply-proof"),
            )
            goal = service.audit(
                goal.id,
                GoalAuditCreate(
                    criterion_verdicts={criterion.id: CriterionVerdict.SATISFIED},
                    evidence_ids=[evidence.id],
                    explanation="Proof is fresh at authorization time",
                ),
                GoalMutationContext(
                    actor_principal="agent:independent-auditor",
                    authority_instance_id="instance-a",
                    idempotency_key="audit-fresh-apply-proof",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
            )
            for state in (GoalState.READY, GoalState.ACTIVE, GoalState.VERIFYING):
                goal = service.transition(
                    goal.id,
                    GoalTransition(state=state, reason="advance to verification"),
                    self._ctx(goal.version, f"fresh-apply-{state.value}"),
                )
            goal = service.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal="agent:coordinator",
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=TransitionGoalAction(
                        state=GoalState.ACHIEVED,
                        reason="Apply only while evidence remains fresh",
                    ),
                    rationale="The audit is currently satisfied.",
                    expected_goal_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
                GoalMutationContext(
                    actor_principal="agent:coordinator",
                    authority_instance_id="instance-a",
                    idempotency_key="propose-fresh-achievement",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
            )

            def mark_authorized(current: Goal) -> dict:
                current.proposals[0].status = ProposalStatus.AUTHORIZED
                return {"proposal_id": current.proposals[0].id}

            goal = service._mutate(
                goal.id,
                self._ctx(goal.version, "stage-authorized-achievement"),
                "goal.test_proposal_staged",
                mark_authorized,
            )
            clock[0] += timedelta(seconds=2)
            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                now=lambda: clock[0],
            )
            result = supervisor.run_once(goal.id)[0]
            self.assertEqual(result.state, GoalState.VERIFYING)
            self.assertEqual(result.proposals[0].status, ProposalStatus.FAILED)
            self.assertIn("stale evidence", result.proposals[0].error or "")
            version = result.version
            self.assertEqual(supervisor.run_once(goal.id), [])
            persisted = service.get(goal.id)
            assert persisted is not None
            self.assertEqual(persisted.version, version)

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
                default_provider="codex",
            )
            first = supervisor.run_once(goal.id)[0]
            self.assertEqual(len(first.work_packages), 1)
            package = first.work_packages[0]
            self.assertIsNotNone(package.card_id)
            self.assertIn(package.card_id, first.linked_card_ids)
            card = projection.get_card(package.card_id, realm_id="default")
            assert card is not None
            self.assertIn(f"goal-work-package:{package.id}", card.tags)

            staged = supervisor.run_once(goal.id)[0]
            self.assertEqual(len(calls), 0)
            self.assertIsNotNone(staged.work_packages[0].dispatch_attempt)
            second = supervisor.run_once(goal.id)[0]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["card_id"], package.card_id)
            self.assertEqual(calls[0]["placement_policy"], "best_match")
            self.assertEqual(
                calls[0]["goal_provenance"]["requested_placement_target"],
                "placement:best_match",
            )
            self.assertEqual(
                calls[0]["goal_provenance"]["placement_input_digest"],
                goal_dispatch_placement_input_digest(calls[0]),
            )
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
                default_provider="codex",
            )
            supervisor.run_once(goal.id)
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

            # Clear the released receipt, then create, stage, and apply replacement.
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            replacement = service.get(goal.id)
            assert replacement is not None
            self.assertEqual(dispatched, ["dispatch-1", "dispatch-2"])
            self.assertEqual(replacement.work_packages[0].attempts, 2)
            self.assertEqual(
                replacement.work_packages[0].dispatch_ids,
                ["dispatch-1", "dispatch-2"],
            )

    def test_pre_admission_dispatch_failures_are_durably_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="bounded admission",
                verification_method="durable retry ledger",
                evidence_requirement="no unbounded replacement proposals",
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
                        title="Bound admission failures",
                        objective="Stop retrying after the package limit",
                        criterion_ids=[criterion.id],
                        max_attempts=2,
                    ),
                    rationale="Admission failures must consume durable attempts.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            calls: list[str] = []

            def reject_before_admission(payload: dict) -> dict:
                calls.append(payload["idempotency_key"])
                return {
                    "accepted": False,
                    "error": "target rejected before creating a dispatch",
                }

            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=FakeDispatchStore(),
                dispatch=reject_before_admission,
                default_provider="codex",
            )
            exhausted = None
            for _ in range(7):
                processed = supervisor.run_once(goal.id)
                if processed:
                    exhausted = processed[0]
            assert exhausted is not None
            for _ in range(3):
                supervisor.run_once(goal.id)

            package = exhausted.work_packages[0]
            self.assertEqual(package.attempts, package.max_attempts)
            self.assertEqual(package.state, WorkPackageState.FAILED)
            self.assertIn("retry limit", package.result_summary.lower())
            self.assertEqual(
                calls,
                [
                    f"goal:{goal.id}:work:{package.id}:attempt:1",
                    f"goal:{goal.id}:work:{package.id}:attempt:2",
                ],
            )
            dispatch_proposals = [
                item
                for item in exhausted.proposals
                if isinstance(item.action, DispatchWorkPackageAction)
                and item.action.work_package_id == package.id
            ]
            self.assertEqual(len(dispatch_proposals), package.max_attempts)
            self.assertTrue(
                all(item.status == ProposalStatus.FAILED for item in dispatch_proposals)
            )
            autonomy = supervisor.governance.get_state(goal.id)
            dispatch_reservations = [
                item
                for item in autonomy.action_reservations
                if item.action_class == "dispatch_work_package"
            ]
            self.assertEqual(len(dispatch_reservations), package.max_attempts)
            self.assertTrue(
                all(item.state.value == "released" for item in dispatch_reservations)
            )

    def test_pre_admission_crash_reuses_the_same_external_attempt_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="crash-safe admission",
                verification_method="idempotent replay",
                evidence_requirement="one canonical dispatch",
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
                        title="Replay one admission",
                        objective="Reuse the attempt key after a local crash",
                        criterion_ids=[criterion.id],
                        max_attempts=2,
                    ),
                    rationale="External admission must be exactly replayable.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            external: dict[str, str] = {}
            keys: list[str] = []

            def crash_after_external_commit(payload: dict) -> dict:
                key = payload["idempotency_key"]
                keys.append(key)
                dispatch_id = external.setdefault(key, "dispatch-canonical")
                if len(keys) == 1:
                    raise RuntimeError("local process stopped after external commit")
                return {"dispatch_id": dispatch_id}

            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=FakeDispatchStore(),
                dispatch=crash_after_external_commit,
                default_provider="codex",
            )
            supervisor.run_once(goal.id)
            staged = supervisor.run_once(goal.id)[0]
            self.assertIsNotNone(staged.work_packages[0].dispatch_attempt)
            self.assertEqual(supervisor.run_once(goal.id), [])
            reloaded = service.get(goal.id)
            assert reloaded is not None
            self.assertEqual(reloaded.work_packages[0].attempts, 0)
            ambiguous_attempt = reloaded.work_packages[0].dispatch_attempt
            assert ambiguous_attempt is not None
            self.assertEqual(
                ambiguous_attempt.state,
                GoalDispatchAttemptState.STAGED,
            )
            autonomy = supervisor.governance.get_state(goal.id)
            dispatch_reservations = [
                item
                for item in autonomy.action_reservations
                if item.action_class == "dispatch_work_package"
            ]
            self.assertEqual(len(dispatch_reservations), 1)
            self.assertEqual(
                dispatch_reservations[0].state,
                GoalReservationState.APPLIED,
            )
            replayed = supervisor.run_once(goal.id)[0]
            self.assertEqual(keys[0], keys[1])
            package = replayed.work_packages[0]
            self.assertEqual(package.attempts, 1)
            self.assertEqual(
                package.dispatch_ids,
                ["dispatch-canonical"],
            )
            admitted_attempt = package.dispatch_attempt
            assert admitted_attempt is not None
            self.assertEqual(
                admitted_attempt.state,
                GoalDispatchAttemptState.ADMITTED,
            )
            self.assertEqual(
                admitted_attempt.dispatch_id,
                "dispatch-canonical",
            )
            self.assertEqual(
                admitted_attempt.admission_receipt_digest,
                package.dispatch_admission_receipt_digest,
            )
            self.assertTrue(admitted_attempt.fleet_lifecycle_owned)
            self.assertTrue(package.fleet_lifecycle_owned)
            autonomy = supervisor.governance.get_state(goal.id)
            dispatch_reservations = [
                item
                for item in autonomy.action_reservations
                if item.action_class == "dispatch_work_package"
            ]
            self.assertEqual(len(dispatch_reservations), 1)
            self.assertEqual(
                dispatch_reservations[0].state,
                GoalReservationState.APPLIED,
            )

    def test_admitted_dispatch_replays_after_goal_checkpoint_crash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="checkpoint-safe admission",
                verification_method="idempotent replay",
                evidence_requirement="one canonical dispatch",
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
                        title="Checkpoint one admission",
                        objective="Replay after the Goal checkpoint crashes",
                        criterion_ids=[criterion.id],
                        max_attempts=2,
                    ),
                    rationale="Admission and Goal persistence are separate commits.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            external: dict[str, str] = {}
            keys: list[str] = []

            def canonical_dispatch(payload: dict) -> dict:
                key = payload["idempotency_key"]
                keys.append(key)
                return {"dispatch_id": external.setdefault(key, "dispatch-canonical")}

            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=FakeDispatchStore(),
                dispatch=canonical_dispatch,
                default_provider="codex",
            )
            supervisor.run_once(goal.id)
            staged = supervisor.run_once(goal.id)[0]
            self.assertEqual(
                staged.work_packages[0].dispatch_attempt.state,
                GoalDispatchAttemptState.STAGED,
            )

            original_checkpoint = service.checkpoint_supervision
            checkpoint_crashed = False

            def crash_before_admitted_checkpoint(goal_id, checkpoint, context):
                nonlocal checkpoint_crashed
                package = checkpoint.work_packages[0]
                if (
                    not checkpoint_crashed
                    and package.dispatch_attempt is not None
                    and package.dispatch_attempt.state
                    == GoalDispatchAttemptState.ADMITTED
                ):
                    checkpoint_crashed = True
                    raise RuntimeError("crash before admitted Goal checkpoint")
                return original_checkpoint(goal_id, checkpoint, context)

            service.checkpoint_supervision = crash_before_admitted_checkpoint
            self.assertEqual(supervisor.run_once(goal.id), [])
            durable = service.get(goal.id)
            assert durable is not None
            package = durable.work_packages[0]
            self.assertEqual(package.attempts, 0)
            self.assertEqual(package.dispatch_ids, [])
            assert package.dispatch_attempt is not None
            self.assertEqual(
                package.dispatch_attempt.state,
                GoalDispatchAttemptState.STAGED,
            )
            autonomy = supervisor.governance.get_state(goal.id)
            dispatch_reservations = [
                item
                for item in autonomy.action_reservations
                if item.action_class == "dispatch_work_package"
            ]
            self.assertEqual(len(dispatch_reservations), 1)
            self.assertEqual(
                dispatch_reservations[0].state,
                GoalReservationState.APPLIED,
            )

            service.checkpoint_supervision = original_checkpoint
            replayed = supervisor.run_once(goal.id)[0]
            self.assertEqual(keys, [keys[0], keys[0]])
            package = replayed.work_packages[0]
            self.assertEqual(package.attempts, 1)
            self.assertEqual(package.dispatch_ids, ["dispatch-canonical"])
            assert package.dispatch_attempt is not None
            self.assertEqual(
                package.dispatch_attempt.state,
                GoalDispatchAttemptState.ADMITTED,
            )
            self.assertTrue(package.fleet_lifecycle_owned)
            self.assertEqual(
                package.dispatch_attempt.admission_receipt_digest,
                package.dispatch_admission_receipt_digest,
            )
            autonomy = supervisor.governance.get_state(goal.id)
            self.assertEqual(
                dispatch_reservations[0].id,
                package.dispatch_attempt.reservation_id,
            )
            reservation = next(
                item
                for item in autonomy.action_reservations
                if item.id == package.dispatch_attempt.reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.APPLIED)

    def test_rejected_dispatch_recovers_post_checkpoint_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="rejection release is recoverable",
                verification_method="durable rejection checkpoint",
                evidence_requirement="one post-checkpoint hold release",
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
                        title="Checkpoint one rejection",
                        objective="Release only after rejection is durable",
                        criterion_ids=[criterion.id],
                        max_attempts=1,
                    ),
                    rationale="A release crash must preserve the rejection ledger.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            keys: list[str] = []

            def reject(payload: dict) -> dict:
                keys.append(payload["idempotency_key"])
                return {"accepted": False, "error": "definite pre-admission reject"}

            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=FakeDispatchStore(),
                dispatch=reject,
                default_provider="codex",
            )
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)

            original_release = supervisor._reconcile_governed_release
            release_crashed = False

            def crash_first_rejected_release(*args, **kwargs):
                nonlocal release_crashed
                if not release_crashed and str(
                    kwargs.get("idempotency_key", "")
                ).endswith(":release-rejected"):
                    release_crashed = True
                    raise RuntimeError("crash during post-checkpoint release")
                return original_release(*args, **kwargs)

            supervisor._reconcile_governed_release = crash_first_rejected_release
            self.assertEqual(supervisor.run_once(goal.id), [])
            durable = service.get(goal.id)
            assert durable is not None
            package = durable.work_packages[0]
            self.assertEqual(package.attempts, 1)
            self.assertEqual(package.state, WorkPackageState.FAILED)
            attempt = package.dispatch_attempt
            assert attempt is not None
            self.assertEqual(attempt.state, GoalDispatchAttemptState.REJECTED)
            self.assertTrue(attempt.release_pending)
            dispatch_proposal = next(
                item
                for item in durable.proposals
                if isinstance(item.action, DispatchWorkPackageAction)
            )
            self.assertEqual(dispatch_proposal.status, ProposalStatus.FAILED)
            autonomy = supervisor.governance.get_state(goal.id)
            reservation = next(
                item
                for item in autonomy.action_reservations
                if item.id == attempt.reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.APPLIED)

            supervisor._reconcile_governed_release = original_release
            released_cycle = supervisor.run_once(goal.id)[0]
            self.assertIsNotNone(released_cycle.work_packages[0].dispatch_attempt)
            self.assertEqual(keys, [keys[0]])
            autonomy = supervisor.governance.get_state(goal.id)
            reservation = next(
                item
                for item in autonomy.action_reservations
                if item.id == attempt.reservation_id
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)

            cleared = supervisor.run_once(goal.id)[0]
            self.assertIsNone(cleared.work_packages[0].dispatch_attempt)
            self.assertFalse(cleared.work_packages[0].fleet_lifecycle_owned)
            self.assertEqual(keys, [keys[0]])

    def test_fast_terminal_dispatch_recovers_from_exact_fleet_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="fast terminal admission",
                verification_method="durable Fleet ledger",
                evidence_requirement="exact validated admission record",
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
                        title="Recover a fast terminal dispatch",
                        objective="Persist the exact admitted Fleet record",
                        criterion_ids=[criterion.id],
                    ),
                    rationale="Fleet may finish before the Goal checkpoint.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            records = FakeDispatchStore()
            calls: list[str] = []
            supervisor: GoalSupervisor

            def fast_terminal(payload: dict) -> dict:
                calls.append(payload["idempotency_key"])
                provenance = GoalDispatchProvenance.model_validate(
                    payload["goal_provenance"]
                )
                autonomy = supervisor.governance.get_state(goal.id)
                reservation = next(
                    item
                    for item in autonomy.action_reservations
                    if item.id == provenance.action_reservation_id
                )
                placement_decision = {
                    "policy": "best_match",
                    "chosen_instance_id": "instance-b",
                    "chosen_instance_name": "worker-b",
                    "eligible_instance_ids": ["instance-b"],
                    "tie_breaking_reason": "exact test placement",
                }
                current_goal = service.get(goal.id)
                assert current_goal is not None
                autonomy, reservation = supervisor.governance.bind_dispatch_placement(
                    goal.id,
                    reservation.id,
                    GovernanceMutationContext(
                        actor_principal=reservation.actor_principal,
                        authority_instance_id=reservation.authority_instance_id,
                        idempotency_key=f"test-bind:{reservation.id}",
                        expected_version=autonomy.version,
                        policy_revision=current_goal.policy.revision,
                        goal_version=current_goal.version,
                        fencing_token=reservation.fencing_token,
                    ),
                    requested_placement_target="placement:best_match",
                    placement_input_digest=provenance.placement_input_digest or "",
                    resolved_target_instance_id="instance-b",
                    placement_decision_digest=(
                        goal_dispatch_placement_decision_digest(placement_decision)
                    ),
                )
                bound_provenance = provenance.model_copy(
                    update={
                        "resolved_target_instance_id": "instance-b",
                        "placement_decision_digest": (
                            reservation.request.placement_decision_digest
                        ),
                    }
                )
                placement_input = goal_dispatch_placement_input_snapshot(payload)
                request_payload = {
                    "card_id": payload["card_id"],
                    "project_id": payload["project_id"],
                    "title": None,
                    "message": payload["message"],
                    "provider": payload["provider"],
                    "model_id": payload["model_id"],
                    "mode_id": payload["mode_id"],
                    "collaboration_risk": payload["collaboration_risk"],
                    "collaboration_ambiguous": False,
                    "collaboration_unattended": True,
                    "effort": None,
                    "cwd": None,
                    "capacity_override_reason": None,
                    "participation_override_reason": None,
                    "priority": payload["priority"],
                    "goal_provenance": bound_provenance.model_dump(mode="json"),
                }
                record = DispatchRecord(
                    dispatch_id="dispatch-fast-terminal",
                    mutation_id="mutation-fast-terminal",
                    idempotency_key=payload["idempotency_key"],
                    request_fingerprint="f" * 64,
                    placement_request_fingerprint="p" * 64,
                    card_id=payload["card_id"],
                    project_id=payload["project_id"],
                    request_payload=request_payload,
                    goal_provenance=bound_provenance,
                    goal_placement_input=placement_input,
                    goal_placement_input_digest=(
                        goal_dispatch_placement_input_digest(placement_input)
                    ),
                    goal_admission_validation_state="validated",
                    goal_admission_validated_at=datetime.now(UTC),
                    principal_id="service:goal-supervisor:instance-a",
                    authority_instance_id="instance-a",
                    authority_url="http://instance-a.test",
                    target_instance_id="instance-b",
                    placement_policy="best_match",
                    placement_decision=placement_decision,
                    state="completed",
                    events=[
                        DispatchEvent(
                            seq=1,
                            state="admission_pending",
                            message="admission started",
                        ),
                        DispatchEvent(
                            seq=2,
                            state="queued",
                            message="admission committed",
                        ),
                        DispatchEvent(
                            seq=3,
                            state="completed",
                            message="work finished quickly",
                        ),
                    ],
                )
                record.goal_admission_validation_proof = (
                    goal_admission_validation_proof(record)
                )
                records.records[record.dispatch_id] = record
                supervisor._reconcile_governed_release(
                    goal.id,
                    reservation.id,
                    actual_usage=reservation.reserved_usage,
                    reason="Fleet observed a fast terminal dispatch",
                    idempotency_key=f"test-release:{reservation.id}",
                )
                autonomy = supervisor.governance.get_state(goal.id)
                released = next(
                    item
                    for item in autonomy.action_reservations
                    if item.id == reservation.id
                )
                record.goal_provenance = bound_provenance.model_copy(
                    update={
                        "released_at": released.released_at,
                        "release_reason": released.release_reason,
                    }
                )
                records.records[record.dispatch_id] = record
                return {"dispatch_id": record.dispatch_id}

            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=records,
                dispatch=fast_terminal,
                default_provider="codex",
            )
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)

            original_checkpoint = service.checkpoint_supervision
            checkpoint_crashed = False

            def crash_before_admitted_checkpoint(goal_id, checkpoint, context):
                nonlocal checkpoint_crashed
                attempt = checkpoint.work_packages[0].dispatch_attempt
                if (
                    not checkpoint_crashed
                    and attempt is not None
                    and attempt.state == GoalDispatchAttemptState.ADMITTED
                ):
                    checkpoint_crashed = True
                    raise RuntimeError("crash before admitted Goal checkpoint")
                return original_checkpoint(goal_id, checkpoint, context)

            service.checkpoint_supervision = crash_before_admitted_checkpoint
            self.assertEqual(supervisor.run_once(goal.id), [])
            durable = service.get(goal.id)
            assert durable is not None
            assert durable.work_packages[0].dispatch_attempt is not None
            self.assertEqual(
                durable.work_packages[0].dispatch_attempt.state,
                GoalDispatchAttemptState.STAGED,
            )
            autonomy = supervisor.governance.get_state(goal.id)
            reservation = next(
                item
                for item in autonomy.action_reservations
                if item.action_class == "dispatch_work_package"
            )
            self.assertEqual(reservation.state, GoalReservationState.RELEASED)

            service.checkpoint_supervision = original_checkpoint
            recovered = supervisor.run_once(goal.id)[0]
            package = recovered.work_packages[0]
            self.assertEqual(calls, [calls[0]])
            self.assertEqual(package.dispatch_ids, ["dispatch-fast-terminal"])
            assert package.dispatch_attempt is not None
            self.assertEqual(
                package.dispatch_attempt.state,
                GoalDispatchAttemptState.ADMITTED,
            )
            self.assertEqual(
                package.dispatch_attempt.admission_receipt_digest,
                package.dispatch_admission_receipt_digest,
            )

    def test_released_dispatch_without_exact_record_never_reopens_fleet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, projection = self._services(tmp)
            criterion = GoalCriterion(
                description="released unknown dispatch",
                verification_method="authority ledger lookup",
                evidence_requirement="no ungoverned Fleet replay",
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
                        title="Do not reopen Fleet",
                        objective="Require an exact authority ledger record",
                        criterion_ids=[criterion.id],
                    ),
                    rationale="A released hold cannot authorize a new call.",
                    expected_goal_version=goal.version,
                    policy_revision=1,
                ),
                self._ctx(goal.version, "proposal"),
            )
            records = FakeDispatchStore()
            calls: list[str] = []
            supervisor: GoalSupervisor

            def release_without_record(payload: dict) -> dict:
                calls.append(payload["idempotency_key"])
                reservation_id = payload["goal_provenance"]["action_reservation_id"]
                supervisor._reconcile_governed_release(
                    goal.id,
                    reservation_id,
                    actual_usage=GoalActionRequest(
                        action_class="dispatch_work_package"
                    ).estimate,
                    reason="simulated external terminal without a local record",
                    idempotency_key=f"test-release:{reservation_id}",
                )
                raise RuntimeError("response lost after release")

            supervisor = GoalSupervisor(
                service,
                projection,
                "instance-a",
                dispatch_store=records,
                dispatch=release_without_record,
                default_provider="codex",
            )
            supervisor.run_once(goal.id)
            supervisor.run_once(goal.id)
            self.assertEqual(supervisor.run_once(goal.id), [])
            self.assertEqual(supervisor.run_once(goal.id), [])
            self.assertEqual(calls, [calls[0]])
            durable = service.get(goal.id)
            assert durable is not None
            package = durable.work_packages[0]
            self.assertEqual(package.attempts, 0)
            assert package.dispatch_attempt is not None
            self.assertEqual(
                package.dispatch_attempt.state,
                GoalDispatchAttemptState.STAGED,
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
                default_provider="codex",
            )
            supervisor.run_once(goal.id)
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
            self.assertEqual(verified.criteria[0].verdict, CriterionVerdict.PENDING)
            self.assertFalse(verified.evidence)
            self.assertIn(
                "not independent",
                next(
                    item
                    for item in verified.work_packages
                    if item.role == GoalActorRole.VERIFIER
                ).result_summary,
            )

    def test_verifier_requires_separate_session_target_and_provider(self) -> None:
        criterion = GoalCriterion(
            description="independently verified",
            verification_method="focused suite",
            evidence_requirement="passing validation",
        )
        goal = Goal(**self._goal_create(criterion).model_dump(mode="python"))
        executor = GoalWorkPackage(
            proposal_id="executor-proposal",
            title="Execute",
            objective="Implement",
            criterion_ids=[criterion.id],
            role=GoalActorRole.EXECUTOR,
            state=WorkPackageState.AWAITING_VERIFICATION,
        )
        verifier = GoalWorkPackage(
            proposal_id="verifier-proposal",
            title="Verify",
            objective="Verify independently",
            criterion_ids=[criterion.id],
            depends_on=[executor.id],
            role=GoalActorRole.VERIFIER,
            state=WorkPackageState.RUNNING,
        )
        goal.work_packages = [executor, verifier]
        goal.lease.fencing_token = 1
        report = CompletionReportV1(
            outcome="Independent suite passed.",
            validations=[
                ProgressValidationV1(
                    command="pytest focused",
                    status="passed",
                    summary="all passed",
                )
            ],
        )

        def identity(
            principal: str,
            provider: str,
            target: str,
            session: str,
        ) -> GoalExecutionIdentityV1:
            return GoalExecutionIdentityV1(
                assigned_service_principal=principal,
                provider_id=provider,
                target_instance_id=target,
                session_id=session,
                fencing_token=1,
                materialization_receipt_digest="a" * 64,
            )

        executor_identity = identity(
            "service:executor",
            "codex",
            "instance-a",
            "executor-session",
        )
        cases = {
            "missing real session": None,
            "same session": identity(
                "service:verifier-session",
                "cursor",
                "instance-b",
                "executor-session",
            ),
            "same target": identity(
                "service:verifier-target",
                "cursor",
                "instance-a",
                "verifier-session",
            ),
            "same provider": identity(
                "service:verifier-provider",
                "codex",
                "instance-b",
                "verifier-session",
            ),
            "truly separate": identity(
                "service:verifier-separate",
                "cursor",
                "instance-b",
                "verifier-session",
            ),
        }
        supervisor = object.__new__(GoalSupervisor)
        supervisor.service_principal = "service:goal-supervisor:instance-a"
        supervisor.instance_id = "instance-a"

        def ingest_evidence_snapshot(
            current: Goal,
            data: GoalEvidenceCreate,
            *,
            context: GoalMutationContext,
            now: datetime,
        ) -> None:
            current.evidence.append(data.evidence)
            for criterion_id, verdict in data.criterion_verdicts.items():
                criterion = next(
                    item for item in current.criteria if item.id == criterion_id
                )
                criterion.verdict = verdict

        supervisor.service = SimpleNamespace(
            ingest_evidence_snapshot=ingest_evidence_snapshot
        )
        for label, verifier_identity in cases.items():
            with self.subTest(case=label):
                current = goal.model_copy(deep=True)
                current_executor, current_verifier = current.work_packages
                current_executor.execution_identity = executor_identity
                current_executor.executor_service_id = (
                    executor_identity.assigned_service_principal
                )
                current_verifier.execution_identity = verifier_identity
                current_verifier.verifier_service_id = (
                    verifier_identity.assigned_service_principal
                    if verifier_identity is not None
                    else None
                )
                record = SimpleNamespace(
                    dispatch_id="verifier-dispatch",
                    session_id=(
                        verifier_identity.session_id
                        if verifier_identity is not None
                        else None
                    ),
                    final_report=report,
                )
                changed = supervisor._complete_package(
                    current,
                    current_verifier,
                    record,
                    datetime.now(UTC),
                )
                self.assertTrue(changed)
                if label == "truly separate":
                    self.assertEqual(
                        current.criteria[0].verdict,
                        CriterionVerdict.SATISFIED,
                    )
                    self.assertEqual(
                        current_verifier.state,
                        WorkPackageState.VERIFIED,
                    )
                else:
                    self.assertEqual(
                        current.criteria[0].verdict,
                        CriterionVerdict.PENDING,
                    )
                    self.assertIn("not independent", current_verifier.result_summary)

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
                default_provider="codex",
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
