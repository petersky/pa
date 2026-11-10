"""Regression tests for the ways a durable goal's supervisor could wedge.

Each test drives the real GoalService/GoalSupervisor/GoalGovernanceService on a
throwaway projection; nothing here boots PA.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from pa.domain.notifications import (
    InteractionState,
    Notification,
    NotificationPriority,
)
from pa.domain.projection import CardProjection
from pa.execution.progress import CompletionReportV1, ProgressValidationV1
from pa.goals.advanced_models import (
    GoalActionDisposition,
    GoalActionRequest,
    GovernanceMutationContext,
)
from pa.goals.governance import GoalGovernanceService
from pa.goals.materialization import GoalExecutionIdentityV1
from pa.goals.models import (
    CreateWorkPackageAction,
    DispatchWorkPackageAction,
    Goal,
    GoalActorRole,
    GoalCreate,
    GoalCriterion,
    GoalInteractionState,
    GoalMutationContext,
    GoalPolicy,
    GoalProposalCreate,
    GoalRevision,
    GoalState,
    GoalTransition,
    ProposalStatus,
    WorkPackageState,
)
from pa.goals.service import GoalConflict, GoalService
from pa.goals.supervisor import GoalSupervisor
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore

OPERATOR = "user:operator"
INSTANCE = "instance-a"
FULL_ACTIONS = [
    "request_operator",
    "record_evidence",
    "revise_strategy",
    "create_work_package",
    "dispatch_work_package",
    "transition_goal",
    "audit_goal",
]


class FakeDispatchStore:
    def __init__(self) -> None:
        self.records: dict[str, SimpleNamespace] = {}

    def get(self, dispatch_id: str):
        return self.records.get(dispatch_id)

    def by_authority_idempotency(self, authority_instance_id, idempotency_key):
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


def dispatch_record(
    dispatch_id: str,
    state: str,
    *,
    session_id: str | None = None,
    final_report: CompletionReportV1 | None = None,
    last_error: str | None = None,
    progress_heartbeat=None,
):
    return SimpleNamespace(
        dispatch_id=dispatch_id,
        state=state,
        session_id=session_id,
        final_report=final_report,
        last_error=last_error,
        latest_progress=None,
        progress_heartbeat=progress_heartbeat,
    )


def passing_report(outcome: str = "Independent suite passed.") -> CompletionReportV1:
    return CompletionReportV1(
        outcome=outcome,
        validations=[
            ProgressValidationV1(
                command="pytest focused", status="passed", summary="all passed"
            )
        ],
    )


def finish_dispatch(
    h: "Harness",
    package,
    *,
    role: str,
    session: str,
    principal: str,
    provider: str = "codex",
    target: str = INSTANCE,
    state: str = "completed",
    report: CompletionReportV1 | None = None,
) -> None:
    """Make Fleet report a terminal dispatch for the package's latest dispatch id."""

    dispatch_id = package.dispatch_ids[-1]
    record = dispatch_record(
        dispatch_id, state, session_id=session, final_report=report
    )
    record.goal_provenance = SimpleNamespace(
        action_reservation_id=package.action_reservation_id,
        materialization_envelope=package.materialization_envelope,
        materialization_receipt=None,
        execution_identity=GoalExecutionIdentityV1(
            work_package_id=package.id,
            service_role=role,
            assigned_service_principal=f"service:goal-{role}:{principal}",
            provider_id=provider,
            target_instance_id=target,
            session_id=session,
            fencing_token=1,
            materialization_receipt_digest="a" * 64,
        ),
    )
    h.records.records[dispatch_id] = record


class RecordingNotifications:
    """NotificationService stand-in over a real projection; records every create."""

    def __init__(self, projection: CardProjection) -> None:
        self.store = projection
        self.created: list = []

    def create(self, data, *, principal_id: str, instance_id: str | None = None):
        self.created.append(data)
        existing = (
            self.store.find_notification_by_dedup(
                data.realm_id, data.deduplication_key
            )
            if data.deduplication_key
            else None
        )
        if existing is not None:
            return existing
        values = data.model_dump(exclude={"id"}, mode="python")
        notification = Notification(id=data.id or str(uuid4()), **values)
        return self.store.save_notification(
            notification,
            principal_id=principal_id,
            instance_id=instance_id or INSTANCE,
        )

    def stalls(self, kind: str | None = None):
        return [
            item
            for item in self.created
            if (item.capability or "").startswith("stall:")
            and (kind is None or item.capability == f"stall:{kind}")
        ]


class Harness:
    def __init__(self, tmp: str, *, start: datetime | None = None, **kwargs) -> None:
        root = Path(tmp)
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root, INSTANCE)
        self.projection = CardProjection(root / "projection.db", log)
        self.clock = [start or datetime(2026, 11, 1, 12, tzinfo=UTC)]
        self.service = GoalService(
            self.projection, INSTANCE, clock=lambda: self.clock[0]
        )
        self.governance = GoalGovernanceService(
            self.projection, INSTANCE, self.service, clock=lambda: self.clock[0]
        )
        self.notifications = RecordingNotifications(self.projection)
        self.records = FakeDispatchStore()
        self.dispatched: list[dict] = []
        self.supervisor = GoalSupervisor(
            self.service,
            self.projection,
            INSTANCE,
            notification_service=self.notifications,
            dispatch_store=self.records,
            dispatch=self._dispatch,
            governance=self.governance,
            default_provider="codex",
            now=lambda: self.clock[0],
            **kwargs,
        )

    def _dispatch(self, payload: dict) -> dict:
        self.dispatched.append(payload)
        return {"dispatch_id": f"dispatch-{len(self.dispatched)}"}

    def advance(self, seconds: float) -> None:
        self.clock[0] = self.clock[0] + timedelta(seconds=seconds)

    def ctx(self, goal: Goal | int, key: str, *, actor: str = OPERATOR):
        version = goal if isinstance(goal, int) else goal.version
        return GoalMutationContext(
            actor_principal=actor,
            authority_instance_id=INSTANCE,
            idempotency_key=key,
            expected_version=version,
            policy_revision=1 if isinstance(goal, int) else goal.policy.revision,
        )

    def create_goal(
        self,
        policy: GoalPolicy | None = None,
        *,
        state: GoalState = GoalState.ACTIVE,
        criteria: int = 1,
        key: str = "goal",
    ) -> Goal:
        goal = self.service.create(
            GoalCreate(
                objective="Ship supervised orchestration",
                criteria=[
                    GoalCriterion(
                        description=f"criterion {index}",
                        verification_method="independent tests",
                        evidence_requirement="passing checks",
                    )
                    for index in range(criteria)
                ],
                policy=policy or GoalPolicy(),
            ),
            self.ctx(0, key),
        )
        for target in (GoalState.READY, GoalState.ACTIVE):
            goal = self.service.transition(
                goal.id,
                GoalTransition(state=target, reason="activate"),
                self.ctx(goal, f"{key}-{target.value}"),
            )
            if target == state:
                break
        return goal

    def operator_transition(self, goal: Goal, state: GoalState, key: str) -> Goal:
        return self.service.transition(
            goal.id,
            GoalTransition(state=state, reason=f"operator {state.value}"),
            self.ctx(goal, key),
        )

    def run(self, goal_id: str) -> Goal:
        result = self.supervisor.run_once(goal_id)
        assert result, "supervisor cycle did not complete"
        return result[0]

    def get(self, goal_id: str) -> Goal:
        goal = self.service.get(goal_id)
        assert goal is not None
        return goal

    def answer(self, goal: Goal, index: int = -1, *, approve: bool = True) -> None:
        link = goal.operator_interactions[index]
        notification = self.projection.get_notification(
            link.notification_id, realm_id="default"
        )
        assert notification is not None and notification.interaction is not None
        notification.interaction.state = InteractionState.ANSWERED
        notification.interaction.response = {
            "choice_id": "approve" if approve else "reject",
            "value": approve,
        }
        notification.interaction.response_summary = (
            "Approved" if approve else "Rejected"
        )
        notification.interaction.response_principal = OPERATOR
        notification.version += 1
        self.projection.save_notification(
            notification, principal_id=OPERATOR, instance_id=INSTANCE
        )


class DefaultPolicyAdvancesTests(unittest.TestCase):
    """A1: a goal with the model-default policy must be able to ask for approval."""

    def test_default_policy_goal_asks_for_approval_instead_of_wedging(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.create_goal(GoalPolicy())  # autonomy 1, no permitted actions
            self.assertEqual(goal.policy.autonomy_level, 1)
            self.assertEqual(goal.policy.permitted_actions, [])

            after = h.run(goal.id)

            plan = after.proposals[0]
            self.assertEqual(plan.status, ProposalStatus.OPERATOR_REQUIRED)
            self.assertEqual(len(after.operator_interactions), 1)
            self.assertEqual(after.state, GoalState.WAITING_OPERATOR)
            notification = h.projection.get_notification(
                after.operator_interactions[0].notification_id, realm_id="default"
            )
            assert notification is not None
            self.assertEqual(notification.priority, NotificationPriority.HIGH)

    def test_default_policy_goal_advances_to_dispatch_once_operator_approves(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.create_goal(GoalPolicy())
            goal = h.run(goal.id)
            h.answer(goal)
            goal = h.run(goal.id)
            self.assertEqual(len(goal.work_packages), 1)
            # Dispatch is its own approval gate at autonomy 1.
            goal = h.run(goal.id)
            self.assertEqual(len(goal.operator_interactions), 2)
            self.assertEqual(h.dispatched, [])
            h.answer(goal)
            for _ in range(3):
                goal = h.run(goal.id)
            self.assertEqual(len(h.dispatched), 1)

    def test_governance_never_denies_asking_or_pausing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            policies = {
                "no permitted actions": GoalPolicy(),
                "propose autonomy": GoalPolicy(autonomy_level=2),
                "permitted list without the ask": GoalPolicy(
                    autonomy_level=4, permitted_actions=["code.edit"]
                ),
                "operator required for everything": GoalPolicy(
                    autonomy_level=4,
                    permitted_actions=["code.edit"],
                    require_operator_for=["*"],
                ),
                "everything prohibited": GoalPolicy(
                    autonomy_level=4,
                    permitted_actions=["code.edit"],
                    prohibited_actions=["*"],
                ),
            }
            for label, policy in policies.items():
                with self.subTest(policy=label):
                    for action_class in ("request_operator", "pause_goal"):
                        goal = h.create_goal(policy, key=f"gov-{label}-{action_class}")
                        version = h.governance.get_state(goal.id).version
                        state, decision = h.governance.authorize_action(
                            goal.id,
                            GoalActionRequest(action_class=action_class),
                            GovernanceMutationContext(
                                actor_principal=OPERATOR,
                                authority_instance_id=INSTANCE,
                                idempotency_key=f"{label}-{action_class}",
                                expected_version=version,
                                policy_revision=goal.policy.revision,
                                goal_version=goal.version,
                                fencing_token=None,
                            ),
                        )
                        version = state.version
                        self.assertEqual(
                            decision.disposition,
                            GoalActionDisposition.AUTHORIZED,
                            decision.reasons,
                        )
            # Executing actions stay gated: the exemption is only for asking/stopping.
            goal = h.create_goal(GoalPolicy(), key="still-gated")
            _state, decision = h.governance.authorize_action(
                goal.id,
                GoalActionRequest(action_class="dispatch_work_package"),
                GovernanceMutationContext(
                    actor_principal=OPERATOR,
                    authority_instance_id=INSTANCE,
                    idempotency_key="still-gated-dispatch",
                    expected_version=h.governance.get_state(goal.id).version,
                    policy_revision=goal.policy.revision,
                    goal_version=goal.version,
                ),
            )
            self.assertEqual(decision.disposition, GoalActionDisposition.DENIED)


class CycleFailureEscalationTests(unittest.TestCase):
    """A1: a cycle that keeps raising is counted, backed off and escalated."""

    def test_persistent_cycle_failure_backs_off_notifies_and_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, failure_notify_after=3)
            goal = h.create_goal(GoalPolicy(autonomy_level=4, permitted_actions=FULL_ACTIONS))
            real = h.supervisor._ensure_plan_proposal

            def boom(*_args, **_kwargs):
                raise RuntimeError("synthetic wedge")

            h.supervisor._ensure_plan_proposal = boom

            self.assertEqual(h.supervisor.run_once(goal.id), [])
            first = h.get(goal.id)
            self.assertEqual(first.supervision.consecutive_failures, 1)
            self.assertIn("synthetic wedge", first.supervision.last_failure_reason)
            self.assertEqual(
                first.supervision.retry_after, h.clock[0] + timedelta(seconds=30)
            )

            # The scheduled poll leaves a backing-off goal alone ...
            h.supervisor.run_once()
            self.assertEqual(h.get(goal.id).supervision.consecutive_failures, 1)

            # ... then retries with a doubling delay.
            h.advance(31)
            h.supervisor.run_once()
            second = h.get(goal.id)
            self.assertEqual(second.supervision.consecutive_failures, 2)
            self.assertEqual(
                second.supervision.retry_after, h.clock[0] + timedelta(seconds=60)
            )
            self.assertEqual(h.notifications.stalls(), [])

            h.advance(61)
            h.supervisor.run_once()
            third = h.get(goal.id)
            self.assertEqual(third.supervision.consecutive_failures, 3)
            stalls = h.notifications.stalls("manual_review")
            self.assertEqual(len(stalls), 1)
            self.assertIn("synthetic wedge", stalls[0].body)
            self.assertEqual(stalls[0].priority, NotificationPriority.HIGH)

            # Recovery clears the streak.
            h.supervisor._ensure_plan_proposal = real
            h.advance(121)
            h.supervisor.run_once()
            recovered = h.get(goal.id)
            self.assertEqual(recovered.supervision.consecutive_failures, 0)
            self.assertIsNone(recovered.supervision.retry_after)

    def test_a_manual_cycle_ignores_the_failure_backoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.create_goal(GoalPolicy(autonomy_level=4, permitted_actions=FULL_ACTIONS))
            real = h.supervisor._ensure_plan_proposal
            h.supervisor._ensure_plan_proposal = lambda *a, **k: (_ for _ in ()).throw(
                RuntimeError("once")
            )
            h.supervisor.run_once(goal.id)
            self.assertTrue(h.supervisor._backing_off(h.get(goal.id)))
            h.supervisor._ensure_plan_proposal = real
            self.assertEqual(len(h.supervisor.run_once(goal.id)), 1)


class PauseIsEnforcedTests(unittest.TestCase):
    """A4: a deliberately stopped goal takes no side effects."""

    @staticmethod
    def _policy() -> GoalPolicy:
        return GoalPolicy(autonomy_level=4, permitted_actions=FULL_ACTIONS)

    def test_pause_between_proposal_and_dispatch_prevents_the_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.create_goal(self._policy())
            h.run(goal.id)  # plan applied, dispatch proposal pending
            staged = h.run(goal.id)  # dispatch authorized and staged
            self.assertEqual(
                staged.work_packages[0].dispatch_attempt.state.value, "staged"
            )
            self.assertEqual(h.dispatched, [])

            paused = h.operator_transition(h.get(goal.id), GoalState.PAUSED, "pause")
            self.assertEqual(paused.state, GoalState.PAUSED)

            for _ in range(3):
                h.supervisor.run_once(goal.id)
                h.supervisor.run_once()
            self.assertEqual(h.dispatched, [])
            self.assertEqual(h.get(goal.id).state, GoalState.PAUSED)

            resumed = h.operator_transition(h.get(goal.id), GoalState.ACTIVE, "resume")
            self.assertEqual(resumed.state, GoalState.ACTIVE)
            h.run(goal.id)
            self.assertEqual(len(h.dispatched), 1)

    def test_pause_before_an_authorized_plan_is_applied_creates_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.create_goal(self._policy())
            goal = h.service.submit_proposal(
                goal.id,
                GoalProposalCreate(
                    proposer_principal=OPERATOR,
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=CreateWorkPackageAction(
                        title="Operator package",
                        objective="Should wait for the resume",
                        criterion_ids=[goal.criteria[0].id],
                        dispatch_when_ready=False,
                    ),
                    rationale="operator",
                    expected_goal_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
                h.ctx(goal, "operator-proposal"),
            )
            h.operator_transition(h.get(goal.id), GoalState.PAUSED, "pause")

            h.supervisor.run_once(goal.id)

            paused = h.get(goal.id)
            self.assertEqual(paused.work_packages, [])
            self.assertEqual(paused.linked_card_ids, [])
            self.assertEqual(paused.proposals[0].status, ProposalStatus.PENDING)

    def test_a_stale_copy_of_a_paused_goal_cannot_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.create_goal(self._policy())
            h.run(goal.id)
            h.run(goal.id)  # dispatch staged, not yet sent
            stale = h.get(goal.id)
            leased = h.supervisor._claim(stale)
            h.operator_transition(h.get(goal.id), GoalState.PAUSED, "pause")

            # The cycle still holds the pre-pause copy (the operator paused while
            # it was running): its side effect must re-read the goal and refuse.
            with self.assertRaises(GoalConflict):
                h.supervisor._cycle(leased)  # its checkpoint loses to the pause

            self.assertEqual(h.dispatched, [])
            self.assertEqual(h.get(goal.id).state, GoalState.PAUSED)

    def test_operator_pause_is_not_blocked_by_the_supervisor_lease(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.run(h.create_goal(self._policy()).id)
            self.assertTrue(goal.lease.active(h.clock[0]))

            paused = h.operator_transition(goal, GoalState.PAUSED, "pause-with-lease")

            self.assertEqual(paused.state, GoalState.PAUSED)

    def test_paused_goal_does_not_spin_the_supervisor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.create_goal(self._policy())
            h.run(goal.id)  # leaves a pending dispatch proposal
            h.operator_transition(h.get(goal.id), GoalState.PAUSED, "pause")
            before = h.get(goal.id)

            self.assertEqual(h.supervisor.run_once(), [])

            self.assertEqual(h.get(goal.id).version, before.version)


class DeadEndsAreParkedTests(unittest.TestCase):
    """A2/A7/A9: a goal that cannot proceed stops, says why, and tells the owner."""

    def test_unappliable_plan_blocks_the_goal_with_a_notification(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            # Autonomy 4 but no permitted action classes: the plan proposal is
            # authorized, then refused by canonical governance when applied.
            goal = h.create_goal(GoalPolicy(autonomy_level=4))

            parked = h.run(goal.id)

            self.assertEqual(parked.state, GoalState.BLOCKED)
            self.assertIn("cannot be planned", parked.supervision.parked_reason)
            self.assertIn("permitted", parked.supervision.parked_reason)
            self.assertEqual(parked.proposals[0].status, ProposalStatus.FAILED)
            stalls = h.notifications.stalls("manual_review")
            self.assertEqual(len(stalls), 1)
            self.assertIn("autonomy level and permitted actions", stalls[0].body)

            # It stays put: no spinning, no new proposals, no checkpoint churn.
            version = h.get(goal.id).version
            for _ in range(3):
                self.assertEqual(h.supervisor.run_once(), [])
            self.assertEqual(h.get(goal.id).version, version)
            self.assertEqual(len(h.get(goal.id).proposals), 1)

            # Fixing the policy and resuming plans again.
            current = h.get(goal.id)
            h.service.revise(
                goal.id,
                GoalRevision(
                    reason="grant the actions",
                    policy=GoalPolicy(
                        revision=2, autonomy_level=4, permitted_actions=FULL_ACTIONS
                    ),
                ),
                h.ctx(current, "fix-policy"),
            )
            current = h.get(goal.id)
            h.service.transition(
                goal.id,
                GoalTransition(state=GoalState.ACTIVE, reason="resume"),
                h.ctx(current, "resume"),
            )
            resumed = h.run(goal.id)
            self.assertEqual(len(resumed.work_packages), 1)
            self.assertEqual(resumed.supervision.parked_reason, "")

    def test_rejected_plan_pauses_the_goal_and_resume_asks_again(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.create_goal(GoalPolicy())
            goal = h.run(goal.id)
            self.assertEqual(len(goal.operator_interactions), 1)
            h.answer(goal, approve=False)

            paused = h.run(goal.id)

            self.assertEqual(paused.state, GoalState.PAUSED)
            self.assertIn("rejected", paused.supervision.parked_reason)
            self.assertEqual(paused.proposals[0].status, ProposalStatus.REJECTED)
            self.assertEqual(h.supervisor.run_once(), [])
            self.assertEqual(len(h.get(goal.id).operator_interactions), 1)

            h.service.transition(
                goal.id,
                GoalTransition(state=GoalState.ACTIVE, reason="resume"),
                h.ctx(h.get(goal.id), "resume"),
            )
            asked_again = h.run(goal.id)
            self.assertEqual(len(asked_again.operator_interactions), 2)
            self.assertEqual(len(asked_again.proposals), 2)
            self.assertEqual(asked_again.state, GoalState.WAITING_OPERATOR)

    def test_terminally_failed_package_blocks_the_goal_and_notifies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)
            goal = h.create_goal(
                GoalPolicy(autonomy_level=4, permitted_actions=FULL_ACTIONS)
            )
            for _ in range(60):
                current = h.get(goal.id)
                if current.state == GoalState.BLOCKED:
                    break
                for dispatch_id in current.linked_dispatch_ids:
                    h.records.records[dispatch_id] = dispatch_record(
                        dispatch_id, "failed", last_error="provider crashed"
                    )
                h.supervisor.run_once(goal.id)

            blocked = h.get(goal.id)
            package = blocked.work_packages[0]
            self.assertEqual(package.state, WorkPackageState.FAILED)
            self.assertEqual(blocked.state, GoalState.BLOCKED)
            self.assertIn("failed after", blocked.supervision.parked_reason)
            stalls = h.notifications.stalls("dispatch_failed")
            self.assertEqual(len(stalls), 1)
            self.assertIn(package.title, stalls[0].body)
            # It is not retried silently.
            count = len(h.dispatched)
            for _ in range(3):
                h.supervisor.run_once(goal.id)
            self.assertEqual(len(h.dispatched), count)


class VerifierIndependenceTests(unittest.TestCase):
    """A3: verification must be satisfiable and must never wedge the supervisor."""

    @staticmethod
    def _run_to_verifier_dispatch(h: Harness, goal: Goal) -> Goal:
        for _ in range(4):
            current = h.run(goal.id)
            if current.work_packages and current.work_packages[0].dispatch_ids:
                break
        executor = h.get(goal.id).work_packages[0]
        finish_dispatch(
            h, executor, role="executor", session="exec-session", principal="exec"
        )
        for _ in range(12):
            current = h.run(goal.id)
            verifier = next(
                (
                    item
                    for item in current.work_packages
                    if item.role == GoalActorRole.VERIFIER
                ),
                None,
            )
            if verifier is not None and verifier.dispatch_ids:
                return current
        raise AssertionError("verifier was never dispatched")

    def test_single_user_independence_accepts_the_same_provider_and_machine(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, verifier_independence="single_user")
            goal = h.create_goal(
                GoalPolicy(autonomy_level=4, permitted_actions=FULL_ACTIONS)
            )
            current = self._run_to_verifier_dispatch(h, goal)
            verifier = next(
                item
                for item in current.work_packages
                if item.role == GoalActorRole.VERIFIER
            )
            # Same provider, same machine; only the session and principal differ.
            finish_dispatch(
                h,
                verifier,
                role="verifier",
                session="verifier-session",
                principal="verify",
                report=passing_report(),
            )

            verified = h.run(goal.id)

            self.assertEqual(
                verified.criteria[0].verdict.value, "satisfied", verified.work_packages
            )
            self.assertTrue(
                all(
                    item.state == WorkPackageState.VERIFIED
                    for item in verified.work_packages
                )
            )
            self.assertEqual(verified.state, GoalState.VERIFYING)

    def test_single_user_independence_still_needs_a_different_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp, verifier_independence="single_user")
            goal = h.create_goal(
                GoalPolicy(autonomy_level=4, permitted_actions=FULL_ACTIONS)
            )
            current = self._run_to_verifier_dispatch(h, goal)
            verifier = next(
                item
                for item in current.work_packages
                if item.role == GoalActorRole.VERIFIER
            )
            finish_dispatch(
                h,
                verifier,
                role="verifier",
                session="exec-session",  # the executor's own session
                principal="verify",
                report=passing_report(),
            )

            after = h.run(goal.id)

            self.assertEqual(after.criteria[0].verdict.value, "pending")
            self.assertIn(
                "not independent",
                next(
                    item
                    for item in after.work_packages
                    if item.role == GoalActorRole.VERIFIER
                ).result_summary,
            )

    def test_unsatisfiable_strict_independence_neither_wedges_nor_stays_silent(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(tmp)  # strict is the constructor default (the old rule)
            goal = h.create_goal(
                GoalPolicy(autonomy_level=4, permitted_actions=FULL_ACTIONS)
            )
            current = self._run_to_verifier_dispatch(h, goal)
            for attempt in (1, 2):
                verifier = next(
                    item
                    for item in h.get(goal.id).work_packages
                    if item.role == GoalActorRole.VERIFIER
                )
                finish_dispatch(
                    h,
                    verifier,
                    role="verifier",
                    session=f"verifier-session-{attempt}",
                    principal=f"verify{attempt}",
                    report=passing_report(),
                )
                # Many cycles over the same completed record used to re-enter the
                # completion branch and abort every cycle with an idempotency
                # conflict after the first one.
                for _ in range(6):
                    h.supervisor.run_once(goal.id)
                    self.assertEqual(
                        h.get(goal.id).supervision.consecutive_failures,
                        0,
                        h.get(goal.id).supervision.last_failure_reason,
                    )
                if attempt == 1:
                    # executor + verifier + the one replacement verifier attempt
                    self.assertEqual(len(h.dispatched), 3)

            final = h.get(goal.id)
            verifier = next(
                item
                for item in final.work_packages
                if item.role == GoalActorRole.VERIFIER
            )
            self.assertEqual(verifier.state, WorkPackageState.FAILED)
            self.assertEqual(final.state, GoalState.BLOCKED)
            stalls = h.notifications.stalls("verification_failed")
            self.assertEqual(len(stalls), 1)
            self.assertIn("not independent", stalls[0].body)


if __name__ == "__main__":
    unittest.main()
