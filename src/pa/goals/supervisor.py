"""Event-driven supervisor for durable goals."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pa.domain.models import CardCreate, CardKind, CardLane
from pa.domain.notifications import (
    InteractionChoice,
    InteractionKind,
    InteractionRequest,
    InteractionState,
    NotificationCreate,
    NotificationPriority,
    NotificationSeverity,
    NotificationType,
)
from pa.execution.dispatch import (
    goal_admission_validation_proof,
    goal_dispatch_placement_decision_digest,
    goal_dispatch_placement_input_digest,
    goal_dispatch_record_placement_input_valid,
)
from pa.execution.profiles import (
    ExecutionContract,
    ExecutionProfile,
    ExecutionRequirements,
    RepositoryRequirement,
    resolve_materialization_plan,
)
from pa.goals.advanced_models import (
    GoalActionDisposition,
    GoalActionRequest,
    GoalReservationState,
    GoalResourceClaim,
    GoalUsage,
    GovernanceMutationContext,
    ResourceAccess,
)
from pa.goals.authorization import authorize_proposal
from pa.goals.governance import GoalGovernanceConflict, GoalGovernanceService
from pa.goals.materialization import (
    GoalMaterializationEnvelopeV1,
    GoalMaterializationResourceClaimV1,
    canonical_materialization_digest,
)
from pa.goals.models import (
    AuthorizationOutcome,
    CreateWorkPackageAction,
    CriterionVerdict,
    DispatchWorkPackageAction,
    EvidenceKind,
    Goal,
    GoalActorRole,
    GoalDispatchAttempt,
    GoalDispatchAttemptState,
    GoalDriftState,
    GoalEvidence,
    GoalEvidenceCreate,
    GoalInteractionState,
    GoalMutationContext,
    GoalOperatorInteraction,
    GoalProposal,
    GoalState,
    GoalSupervisionCheckpoint,
    GoalWorkPackage,
    ProposalStatus,
    RecordEvidenceAction,
    RequestOperatorAction,
    ReviseStrategyAction,
    TransitionGoalAction,
    WorkPackageState,
)
from pa.goals.service import GoalConflict, GoalService, goal_completion_findings

logger = logging.getLogger(__name__)


class GoalDispatchAmbiguous(RuntimeError):
    """The Fleet call may have committed and must be replayed with the same key."""


_TERMINAL_GOAL_STATES = {GoalState.ACHIEVED, GoalState.ABANDONED}
_TERMINAL_PROPOSALS = {
    ProposalStatus.APPLIED,
    ProposalStatus.FAILED,
    ProposalStatus.REJECTED,
}
_ACTIVE_WORK = {
    WorkPackageState.DISPATCHED,
    WorkPackageState.RUNNING,
    WorkPackageState.AWAITING_VERIFICATION,
}
_CYCLE_WORK = {
    WorkPackageState.PLANNED,
    WorkPackageState.READY,
    WorkPackageState.DISPATCHED,
    WorkPackageState.RUNNING,
    WorkPackageState.AWAITING_VERIFICATION,
    WorkPackageState.BLOCKED,
}
_TERMINAL_INTERACTIONS = {
    InteractionState.ANSWERED,
    InteractionState.CANCELLED,
    InteractionState.EXPIRED,
    InteractionState.SUPERSEDED,
    InteractionState.DELIVERED,
}
_MAX_PACKAGE_REPLACEMENTS = 20
_MAX_GOAL_REPLACEMENTS = 100
_MAX_GOAL_DISPATCH_IDS = 200


class GoalSupervisor:
    """Runs recoverable, fenced supervision cycles over durable Goal projections."""

    def __init__(
        self,
        service: GoalService,
        store,
        instance_id: str,
        *,
        notification_service=None,
        dispatch_store=None,
        dispatch: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        governance: GoalGovernanceService | None = None,
        now: Callable[[], datetime] | None = None,
        no_progress_cycles: int = 3,
        stalled_cycles: int = 6,
        lease_ttl_seconds: int = 90,
        default_provider: str | None = None,
    ) -> None:
        if stalled_cycles <= no_progress_cycles:
            raise ValueError("stalled_cycles must exceed no_progress_cycles")
        self.service = service
        self.store = store
        self.instance_id = instance_id
        self.notifications = notification_service
        self.dispatch_store = dispatch_store
        self.dispatch = dispatch
        self.now = now or (lambda: datetime.now(UTC))
        self.governance = governance or GoalGovernanceService(
            store, instance_id, service, clock=self.now
        )
        self.service_principal = f"service:goal-supervisor:{instance_id}"
        self.default_provider = str(default_provider or "").strip().lower()
        self._active_reservation_id: str | None = None
        self.no_progress_threshold = no_progress_cycles
        self.stalled_threshold = stalled_cycles
        self.lease_ttl_seconds = lease_ttl_seconds
        self._wakeup = threading.Event()

    def wake(self) -> None:
        """Wake the event loop after an externally committed goal change."""
        self._wakeup.set()

    def wait_for_wakeup(self, timeout_seconds: float) -> bool:
        """Wait for an event, retaining a bounded polling fallback for recovery."""
        signalled = self._wakeup.wait(timeout_seconds)
        self._wakeup.clear()
        return signalled

    def run_once(self, goal_id: str | None = None) -> list[Goal]:
        candidates = [self.service.get(goal_id)] if goal_id else self.service.list()
        processed: list[Goal] = []
        for candidate in candidates:
            if not candidate or candidate.state in _TERMINAL_GOAL_STATES:
                continue
            if not self._needs_cycle(candidate):
                continue
            try:
                leased = self._claim(candidate)
                if leased is None:
                    continue
                processed.append(self._cycle(leased))
            except GoalConflict:
                logger.info(
                    "Goal supervision lost a concurrent race for %s", candidate.id
                )
            except Exception:
                logger.exception("Goal supervision failed for %s", candidate.id)
        return processed

    def _needs_cycle(self, goal: Goal) -> bool:
        if any(package.dispatch_attempt is not None for package in goal.work_packages):
            return True
        if any(item.status not in _TERMINAL_PROPOSALS for item in goal.proposals):
            return True
        if any(
            item.state in _CYCLE_WORK
            or (
                item.state == WorkPackageState.FAILED
                and item.attempts < item.max_attempts
            )
            for item in goal.work_packages
        ):
            return True
        if any(
            item.state == GoalInteractionState.PENDING
            for item in goal.operator_interactions
        ):
            return True
        return bool(goal.wakeup and goal.wakeup.wake_at <= self.now())

    def _context(self, goal: Goal, key: str) -> GoalMutationContext:
        return GoalMutationContext(
            actor_principal=self.service_principal,
            authority_instance_id=self.instance_id,
            idempotency_key=key,
            expected_version=goal.version,
            policy_revision=goal.policy.revision,
            fencing_token=goal.lease.fencing_token or None,
        )

    def _governance_context(
        self, goal: Goal, expected_version: int, key: str
    ) -> GovernanceMutationContext:
        return GovernanceMutationContext(
            actor_principal=self.service_principal,
            authority_instance_id=self.instance_id,
            idempotency_key=key,
            expected_version=expected_version,
            policy_revision=goal.policy.revision,
            goal_version=goal.version,
            fencing_token=goal.lease.fencing_token or None,
        )

    def _replay_governance_context(
        self,
        goal: Goal,
        expected_version: int,
        key: str,
    ) -> GovernanceMutationContext:
        context = self._governance_context(goal, expected_version, key)
        duplicate = self.governance._duplicate(goal.realm_id, key)
        if duplicate is not None:
            context.expected_version = max(int(duplicate.get("version", 1)) - 1, 0)
        return context

    def _governed_action(
        self,
        goal: Goal,
        key: str,
        request: GoalActionRequest,
        operation: Callable[[], Any],
        *,
        defer_release: bool = False,
    ) -> Any:
        """Reserve, revalidate, apply, and release one side effect."""

        state = self.governance.get_state(goal.id)
        state, reservation_decision = self.governance.authorize_action(
            goal.id,
            request,
            self._replay_governance_context(goal, state.version, f"{key}:reserve"),
        )
        if reservation_decision.disposition != GoalActionDisposition.AUTHORIZED:
            raise GoalGovernanceConflict(
                "canonical governance denied the action: "
                + "; ".join(reservation_decision.reasons)
            )
        reservation_id = reservation_decision.reservation_id or ""
        state, apply_decision = self.governance.apply_action(
            goal.id,
            reservation_id,
            self._replay_governance_context(goal, state.version, f"{key}:apply"),
        )
        if apply_decision.disposition != GoalActionDisposition.AUTHORIZED:
            raise GoalGovernanceConflict(
                "canonical governance denied the action at apply time: "
                + "; ".join(apply_decision.reasons)
            )
        try:
            self._active_reservation_id = reservation_id
            result = operation()
        except BaseException:
            self._reconcile_governed_release(
                goal.id,
                reservation_id,
                actual_usage=GoalUsage(),
                reason="side effect failed before a durable result",
                idempotency_key=f"{key}:release-failed",
            )
            raise
        finally:
            self._active_reservation_id = None
        if defer_release and result:
            return result
        self._reconcile_governed_release(
            goal.id,
            reservation_id,
            actual_usage=request.estimate,
            reason="side effect applied",
            idempotency_key=f"{key}:release-applied",
        )
        return result

    def _reconcile_governed_release(
        self,
        goal_id: str,
        reservation_id: str,
        *,
        actual_usage: GoalUsage,
        reason: str,
        idempotency_key: str,
    ) -> None:
        """Release an exact owned hold even if the controller lease changed."""

        for attempt in range(3):
            state = self.governance.get_state(goal_id)
            reservation = next(
                (
                    item
                    for item in state.action_reservations
                    if item.id == reservation_id
                ),
                None,
            )
            if reservation is None:
                raise GoalGovernanceConflict(
                    "governed side-effect reservation disappeared before release"
                )
            if (
                reservation.actor_principal != self.service_principal
                or reservation.authority_instance_id != self.instance_id
            ):
                raise GoalGovernanceConflict(
                    "governed side-effect release does not own the reservation"
                )
            current_goal = self.service.get(goal_id)
            if current_goal is None:
                raise GoalGovernanceConflict(
                    "governed side-effect goal disappeared before release"
                )
            try:
                self.governance.reconcile_action_release(
                    goal_id,
                    reservation_id,
                    GovernanceMutationContext(
                        actor_principal=self.service_principal,
                        authority_instance_id=self.instance_id,
                        idempotency_key=idempotency_key[:200],
                        expected_version=state.version,
                        policy_revision=current_goal.policy.revision,
                        goal_version=current_goal.version,
                        fencing_token=reservation.fencing_token,
                    ),
                    actual_usage=actual_usage,
                    reason=reason,
                )
                return
            except GoalGovernanceConflict as exc:
                if attempt == 2 or "expected autonomy version" not in str(exc):
                    raise

    def _reconcile_dispatch_attempts(self, goal: Goal, now: datetime) -> bool:
        """Clear only attempts whose canonical governance hold is durably released."""

        state = self.governance.get_state(goal.id)
        reservations = {item.id: item for item in state.action_reservations}
        changed = False
        for package in goal.work_packages:
            attempt = package.dispatch_attempt
            if attempt is None or attempt.state == GoalDispatchAttemptState.STAGED:
                continue
            reservation = reservations.get(attempt.reservation_id or "")
            if reservation is None:
                raise GoalGovernanceConflict(
                    "durable dispatch attempt lost its governance reservation"
                )
            if reservation.state != GoalReservationState.RELEASED:
                continue
            if attempt.state == GoalDispatchAttemptState.ADMITTED:
                package.fleet_lifecycle_owned = False
            package.dispatch_attempt = None
            package.updated_at = now
            changed = True
        return changed

    def _release_rejected_dispatch_attempts(self, goal: Goal) -> None:
        """Release definite pre-admission failures only after their Goal checkpoint."""

        for package in goal.work_packages:
            attempt = package.dispatch_attempt
            if (
                attempt is None
                or attempt.state != GoalDispatchAttemptState.REJECTED
                or not attempt.release_pending
                or not attempt.reservation_id
            ):
                continue
            state = self.governance.get_state(goal.id)
            reservation = next(
                (
                    item
                    for item in state.action_reservations
                    if item.id == attempt.reservation_id
                ),
                None,
            )
            if reservation is None:
                raise GoalGovernanceConflict(
                    "rejected dispatch attempt lost its governance reservation"
                )
            if reservation.state == GoalReservationState.RELEASED:
                continue
            self._reconcile_governed_release(
                goal.id,
                reservation.id,
                actual_usage=GoalUsage(),
                reason="Fleet rejected the dispatch before admission",
                idempotency_key=f"{attempt.idempotency_key}:release-rejected"[:200],
            )

    def _assert_side_effect_fence(self, goal: Goal) -> None:
        current = self.service.get(goal.id)
        if current is None:
            raise GoalConflict("goal disappeared before its side effect")
        if not current.lease.active():
            raise GoalConflict("goal controller lease expired before its side effect")
        if (
            current.control_authority_instance_id != self.instance_id
            or current.lease.holder_instance_id != self.instance_id
            or current.lease.fencing_token != goal.lease.fencing_token
        ):
            raise GoalConflict("goal controller lost its fence before its side effect")
        eligible = set(
            current.wakeup.eligible_instance_ids
            if current.wakeup and current.wakeup.eligible_instance_ids
            else current.lease.eligible_instance_ids
        )
        if eligible and self.instance_id not in eligible:
            raise GoalConflict("goal controller is not eligible for this side effect")

    @staticmethod
    def _append_bounded(values: list[str], value: str, limit: int) -> None:
        if not value:
            return
        values[:] = [item for item in values if item != value]
        values.append(value)
        del values[:-limit]

    def _claim(self, goal: Goal) -> Goal | None:
        now = self.now()
        if goal.control_authority_instance_id != self.instance_id:
            return None
        if goal.lease.active(now):
            return goal if goal.lease.holder_instance_id == self.instance_id else None
        return self.service.acquire_lease(
            goal.id,
            self._context(
                goal,
                f"goal-supervisor:lease:{goal.id}:{goal.version}:"
                f"{goal.lease.fencing_token + 1}",
            ),
            ttl_seconds=self.lease_ttl_seconds,
        )

    def _cycle(self, source: Goal) -> Goal:
        goal = source.model_copy(deep=True)
        now = self.now()
        durably_admitted_packages = {
            package.id
            for package in source.work_packages
            if package.dispatch_attempt is not None
            and package.dispatch_attempt.state == GoalDispatchAttemptState.ADMITTED
        }
        meaningful = self._reconcile_dispatch_attempts(goal, now)
        meaningful = self._ingest_interactions(goal, now) or meaningful
        meaningful = self._authorize_pending(goal, now) or meaningful
        meaningful = self._apply_authorized(goal, now) or meaningful
        meaningful = (
            self._reconcile_dispatches(
                goal,
                now,
                durably_admitted_packages=durably_admitted_packages,
            )
            or meaningful
        )
        meaningful = self._advance_ready_work(goal, now) or meaningful
        meaningful = self._ensure_dispatch_proposals(goal, now) or meaningful
        meaningful = self._detect_drift(goal, now) or meaningful
        meaningful = self._advance_goal_state(goal) or meaningful

        goal.supervision.cycle += 1
        goal.supervision.last_cycle_at = now
        goal.supervision.next_wakeup_at = now + timedelta(seconds=30)
        if meaningful:
            goal.supervision.last_meaningful_progress_at = now
            goal.supervision.no_progress_cycles = 0
        elif goal.work_packages:
            goal.supervision.no_progress_cycles += 1
        goal.supervision.event_cursor = self._cursor(goal)

        checkpoint = GoalSupervisionCheckpoint(
            criteria=goal.criteria,
            evidence=goal.evidence,
            proposals=goal.proposals,
            work_packages=goal.work_packages,
            operator_interactions=goal.operator_interactions,
            supervision=goal.supervision,
            linked_card_ids=list(dict.fromkeys(goal.linked_card_ids)),
            linked_dispatch_ids=list(dict.fromkeys(goal.linked_dispatch_ids)),
            assumptions=goal.assumptions,
            risks=goal.risks,
            strategy_revision=goal.strategy_revision,
            state=goal.state,
            progress_summary=self._summary(goal),
            reason="event-driven supervisor cycle",
        )
        checkpointed = self.service.checkpoint_supervision(
            goal.id,
            checkpoint,
            self._context(
                source,
                f"goal-supervisor:checkpoint:{goal.id}:"
                f"{source.version}:{goal.supervision.cycle}",
            ),
        )
        self._release_rejected_dispatch_attempts(checkpointed)
        return checkpointed

    def _authorize_pending(self, goal: Goal, now: datetime) -> bool:
        changed = False
        for proposal in goal.proposals:
            if proposal.status != ProposalStatus.PENDING:
                continue
            proposal.authorization = authorize_proposal(
                goal,
                proposal,
                instance_id=self.instance_id,
                now=now,
            )
            proposal.updated_at = now
            if proposal.authorization.outcome == AuthorizationOutcome.AUTHORIZE:
                proposal.status = ProposalStatus.AUTHORIZED
            elif (
                proposal.authorization.outcome == AuthorizationOutcome.REQUIRE_OPERATOR
            ):
                proposal.status = ProposalStatus.OPERATOR_REQUIRED
                self._governed_action(
                    goal,
                    (
                        f"goal-supervisor:{goal.id}:proposal:{proposal.id}:"
                        f"approval:{goal.supervision.cycle + 1}"
                    ),
                    GoalActionRequest(action_class="request_operator"),
                    lambda proposal=proposal: self._ensure_interaction(
                        goal, proposal, approval=True
                    ),
                )
            else:
                proposal.status = ProposalStatus.REJECTED
                if isinstance(proposal.action, DispatchWorkPackageAction):
                    self._record_dispatch_failure(
                        goal,
                        proposal.action,
                        proposal.authorization.explanation,
                        now,
                    )
            changed = True
        return changed

    def _apply_authorized(self, goal: Goal, now: datetime) -> bool:
        changed = False
        for proposal in list(goal.proposals):
            if proposal.status != ProposalStatus.AUTHORIZED:
                continue
            try:
                action = proposal.action
                approval = next(
                    (
                        item
                        for item in goal.operator_interactions
                        if item.proposal_id == proposal.id
                        and item.state == GoalInteractionState.ANSWERED
                        and item.response_principal
                        and item.response_summary
                    ),
                    None,
                )

                def governed_request(
                    action_class: str, approval=approval, **kwargs: Any
                ) -> GoalActionRequest:
                    return GoalActionRequest(
                        action_class=action_class,
                        operator_approved=approval is not None,
                        approval_principal=(
                            approval.response_principal if approval else None
                        ),
                        approval_interaction_id=(approval.id if approval else None),
                        **kwargs,
                    )

                governance_key = (
                    f"goal-supervisor:{goal.id}:proposal:{proposal.id}:"
                    f"cycle:{goal.supervision.cycle + 1}"
                )
                if isinstance(action, CreateWorkPackageAction):
                    self._governed_action(
                        goal,
                        governance_key,
                        governed_request("create_work_package"),
                        lambda proposal=proposal, action=action: (
                            self._create_work_package(goal, proposal, action, now)
                        ),
                    )
                elif isinstance(action, DispatchWorkPackageAction):
                    package = self._dispatch_package(goal, action)
                    dispatch_provider = self._dispatch_provider(action)
                    dispatch_operation_key = self._dispatch_operation_key(goal, action)
                    materialization_envelope, execution_contract = (
                        self._plan_materialization_envelope(goal, action)
                    )
                    requested_target, placement_input_digest = (
                        self._dispatch_placement_binding(
                            goal,
                            action,
                            dispatch_provider,
                            execution_contract=execution_contract,
                        )
                    )
                    request = governed_request(
                        "dispatch_work_package",
                        operation_key=dispatch_operation_key,
                        requested_placement_target=requested_target,
                        placement_input_digest=placement_input_digest,
                        materialization_envelope=materialization_envelope,
                        delegated=True,
                        provider_id=dispatch_provider,
                        estimate=GoalUsage(actions=1, dispatches=1),
                        resource_claims=[
                            GoalResourceClaim(
                                key=item.key,
                                access=ResourceAccess(item.access),
                                quantity=item.quantity,
                                preemptible=item.preemptible,
                                expires_at=item.expires_at,
                            )
                            for item in materialization_envelope.resource_claims
                        ],
                        max_attempts=min(20, goal.budget.retry_limit + 1),
                    )
                    request_digest = self._dispatch_request_digest(request)
                    dispatch_payload_digest = self._dispatch_payload_digest(
                        goal,
                        action,
                        package,
                        dispatch_provider,
                        execution_contract,
                    )
                    attempt = package.dispatch_attempt
                    if attempt is None:
                        package.action_reservation_id = None
                        package.fleet_lifecycle_owned = False
                        package.dispatch_attempt = GoalDispatchAttempt(
                            generation=package.attempts + 1,
                            proposal_id=proposal.id,
                            idempotency_key=dispatch_operation_key,
                            request_digest=request_digest,
                            dispatch_payload_digest=dispatch_payload_digest,
                            created_at=now,
                            updated_at=now,
                        )
                        package.updated_at = now
                        changed = True
                        continue
                    if (
                        attempt.state != GoalDispatchAttemptState.STAGED
                        or attempt.proposal_id != proposal.id
                        or attempt.idempotency_key != dispatch_operation_key
                        or attempt.request_digest != request_digest
                        or attempt.dispatch_payload_digest != dispatch_payload_digest
                    ):
                        raise GoalDispatchAmbiguous(
                            "durable dispatch attempt no longer matches its exact request"
                        )
                    outcome = self._execute_staged_dispatch(
                        goal,
                        proposal,
                        action,
                        request,
                        now,
                    )
                    if outcome == GoalDispatchAttemptState.REJECTED:
                        proposal.status = ProposalStatus.FAILED
                        proposal.error = package.dispatch_attempt.error[:2000]
                        proposal.updated_at = now
                        changed = True
                        continue
                elif isinstance(action, RequestOperatorAction):
                    self._governed_action(
                        goal,
                        governance_key,
                        governed_request("request_operator"),
                        lambda proposal=proposal: self._ensure_interaction(
                            goal, proposal, approval=False
                        ),
                    )
                elif isinstance(action, ReviseStrategyAction):

                    def revise_strategy(action=action) -> None:
                        goal.assumptions = list(
                            dict.fromkeys([*goal.assumptions, *action.assumptions])
                        )
                        goal.risks = list(dict.fromkeys([*goal.risks, *action.risks]))
                        goal.strategy_revision += 1
                        goal.progress_summary = action.summary

                    self._governed_action(
                        goal,
                        governance_key,
                        governed_request("revise_strategy"),
                        revise_strategy,
                    )
                elif isinstance(action, RecordEvidenceAction):
                    self._governed_action(
                        goal,
                        governance_key,
                        governed_request("record_evidence"),
                        lambda action=action: self._record_evidence(goal, action, now),
                    )
                elif isinstance(action, TransitionGoalAction):

                    def transition(action=action) -> None:
                        if action.state == GoalState.ACHIEVED:
                            findings = goal_completion_findings(goal, now=now)
                            if findings:
                                raise GoalConflict(
                                    "completion requirements failed: "
                                    + "; ".join(findings)
                                )
                        goal.state = action.state
                        if action.progress_summary is not None:
                            goal.progress_summary = action.progress_summary

                    self._governed_action(
                        goal,
                        governance_key,
                        governed_request("transition_goal"),
                        transition,
                    )
                proposal.status = ProposalStatus.APPLIED
                proposal.applied_event_id = f"checkpoint:{goal.supervision.cycle + 1}"
                proposal.updated_at = now
                changed = True
            except GoalDispatchAmbiguous:
                raise
            except (GoalConflict, KeyError, RuntimeError, TypeError, ValueError) as exc:
                if isinstance(proposal.action, DispatchWorkPackageAction):
                    package = next(
                        (
                            item
                            for item in goal.work_packages
                            if item.id == proposal.action.work_package_id
                        ),
                        None,
                    )
                    if (
                        package is not None
                        and package.dispatch_attempt is not None
                        and package.dispatch_attempt.state
                        == GoalDispatchAttemptState.STAGED
                    ):
                        package.dispatch_attempt = None
                        package.fleet_lifecycle_owned = False
                    if package is not None and package.state not in {
                        WorkPackageState.DISPATCHED,
                        WorkPackageState.RUNNING,
                    }:
                        dispatch_proposals = sum(
                            isinstance(item.action, DispatchWorkPackageAction)
                            and item.action.work_package_id == package.id
                            for item in goal.proposals
                        )
                        if package.attempts < dispatch_proposals:
                            self._record_dispatch_failure(
                                goal,
                                proposal.action,
                                str(exc),
                                now,
                            )
                proposal.status = ProposalStatus.FAILED
                proposal.error = str(exc)[:2000]
                proposal.updated_at = now
                changed = True
        return changed

    def _create_work_package(
        self,
        goal: Goal,
        proposal: GoalProposal,
        action: CreateWorkPackageAction,
        now: datetime,
    ) -> GoalWorkPackage:
        package_id = str(
            uuid5(NAMESPACE_URL, f"pa-goal:{goal.id}:proposal:{proposal.id}")
        )
        existing = next(
            (item for item in goal.work_packages if item.id == package_id), None
        )
        if existing:
            return existing
        package = GoalWorkPackage(
            id=package_id,
            proposal_id=proposal.id,
            title=action.title,
            objective=action.objective,
            criterion_ids=action.criterion_ids,
            depends_on=action.depends_on,
            role=action.role,
            card_id=action.card_id,
            preferred_instance_id=action.preferred_instance_id,
            preferred_capabilities=action.preferred_capabilities,
            max_attempts=min(
                action.max_attempts,
                goal.budget.retry_limit + 1,
            ),
            dispatch_when_ready=action.dispatch_when_ready,
            action_reservation_id=self._active_reservation_id,
            created_at=now,
            updated_at=now,
        )
        package.card_id = self._materialize_card(goal, package)
        goal.work_packages.append(package)
        if package.card_id:
            goal.linked_card_ids.append(package.card_id)
        return package

    def _materialize_card(self, goal: Goal, package: GoalWorkPackage) -> str:
        self._assert_side_effect_fence(goal)
        if package.card_id:
            if not self.store.get_card(package.card_id, realm_id=goal.realm_id):
                raise ValueError("work-package card does not exist in the goal realm")
            return package.card_id
        marker = f"goal-work-package:{package.id}"
        existing = next(
            (
                item
                for item in self.store.list_cards(
                    realm_id=goal.realm_id, project_id=goal.project_id
                )
                if marker in item.tags
            ),
            None,
        )
        if existing:
            return existing.id
        card = self.store.create_card(
            CardCreate(
                realm_id=goal.realm_id,
                kind=CardKind.TASK,
                title=package.title,
                body=package.objective,
                lane=CardLane.ACTIVE,
                project_id=goal.project_id,
                tags=[
                    f"goal:{goal.id}",
                    marker,
                    f"goal-role:{package.role.value}",
                ],
                preferred_instance=package.preferred_instance_id,
                preferred_capabilities=package.preferred_capabilities,
            ),
            principal_id=self.service_principal,
            instance_id=self.instance_id,
        )
        return card.id

    def _execute_staged_dispatch(
        self,
        goal: Goal,
        proposal: GoalProposal,
        action: DispatchWorkPackageAction,
        request: GoalActionRequest,
        now: datetime,
    ) -> GoalDispatchAttemptState:
        """Replay one stable governance reservation around one ambiguous Fleet call."""
        self._assert_side_effect_fence(goal)
        package = self._dispatch_package(goal, action)
        attempt = package.dispatch_attempt
        if attempt is None or attempt.state != GoalDispatchAttemptState.STAGED:
            raise GoalConflict("dispatch execution requires one staged attempt")
        if not package.card_id:
            raise RuntimeError("work package has no materialized card")
        if package.state not in {
            WorkPackageState.READY,
            WorkPackageState.FAILED,
            WorkPackageState.BLOCKED,
        }:
            raise GoalConflict("work package is not ready for dispatch")
        if attempt.generation != package.attempts + 1:
            raise GoalConflict("dispatch generation is no longer current")
        if attempt.generation > package.max_attempts:
            raise RuntimeError("work-package retry limit is exhausted")

        governance_key = (
            f"goal-supervisor:{goal.id}:dispatch:{package.id}:"
            f"generation:{attempt.generation}:proposal:{proposal.id}"
        )
        reserve_key = f"{governance_key}:reserve"
        apply_key = f"{governance_key}:apply"
        state = self.governance.get_state(goal.id)
        matching_reservations = [
            item
            for item in state.action_reservations
            if item.idempotency_key == reserve_key
        ]
        if len(matching_reservations) > 1:
            raise GoalGovernanceConflict(
                "dispatch attempt has multiple canonical governance reservations"
            )
        reservation = matching_reservations[0] if matching_reservations else None
        reservation_preexisted = reservation is not None
        if reservation is None and not self.dispatch:
            raise RuntimeError("fleet dispatch service is unavailable")
        try:
            if reservation is None:
                state, reservation_decision = self.governance.authorize_action(
                    goal.id,
                    request,
                    self._governance_context(goal, state.version, reserve_key),
                )
                if reservation_decision.disposition != GoalActionDisposition.AUTHORIZED:
                    raise GoalGovernanceConflict(
                        "canonical governance denied the dispatch: "
                        + "; ".join(reservation_decision.reasons)
                    )
                reservation = next(
                    (
                        item
                        for item in state.action_reservations
                        if item.id == reservation_decision.reservation_id
                    ),
                    None,
                )
                if reservation is None:
                    raise GoalGovernanceConflict(
                        "authorized dispatch reservation was not durably projected"
                    )
            self._validate_staged_dispatch_reservation(
                goal,
                attempt,
                request,
                reservation,
                reserve_key=reserve_key,
            )
            if reservation.state == GoalReservationState.RESERVED:
                state, apply_decision = self.governance.apply_action(
                    goal.id,
                    reservation.id,
                    self._replay_governance_context(
                        goal,
                        state.version,
                        apply_key,
                    ),
                )
                if apply_decision.disposition != GoalActionDisposition.AUTHORIZED:
                    raise GoalGovernanceConflict(
                        "canonical governance denied the dispatch at apply time: "
                        + "; ".join(apply_decision.reasons)
                    )
                reservation = next(
                    item
                    for item in state.action_reservations
                    if item.id == reservation.id
                )
            if reservation.state not in {
                GoalReservationState.APPLIED,
                GoalReservationState.RELEASED,
            }:
                raise GoalGovernanceConflict(
                    "dispatch attempt does not have an applied governance reservation"
                )

            reconciled = self._reconcile_staged_dispatch_record(
                goal,
                action,
                request,
                reservation,
                now,
            )
            if reconciled is not None:
                return reconciled
            if reservation.state == GoalReservationState.RELEASED:
                raise GoalDispatchAmbiguous(
                    "released dispatch reservation has no exact durable admission record"
                )
            if not self.dispatch:
                raise GoalDispatchAmbiguous(
                    "applied dispatch reservation cannot be replayed while Fleet is unavailable"
                )
            self._active_reservation_id = reservation.id
            return self._dispatch_work_package(goal, action, now, reservation)
        except GoalDispatchAmbiguous:
            raise
        except Exception as exc:
            if reservation is not None:
                current = next(
                    (
                        item
                        for item in self.governance.get_state(
                            goal.id
                        ).action_reservations
                        if item.id == reservation.id
                    ),
                    reservation,
                )
                if (
                    not reservation_preexisted
                    or current.state == GoalReservationState.RESERVED
                ):
                    return self._record_rejected_dispatch(
                        package,
                        attempt,
                        reservation_id=current.id,
                        error=f"dispatch failed before calling Fleet: {exc}"[:2000],
                        now=now,
                    )
                if current.state == GoalReservationState.APPLIED:
                    raise GoalDispatchAmbiguous(
                        "an existing applied dispatch attempt could not be safely reconciled"
                    ) from exc
            raise
        finally:
            self._active_reservation_id = None

    def _dispatch_work_package(
        self,
        goal: Goal,
        action: DispatchWorkPackageAction,
        now: datetime,
        reservation,
    ) -> GoalDispatchAttemptState:
        """Call Fleet once; every callback exception is an ambiguous outcome."""

        self._assert_side_effect_fence(goal)
        package = self._dispatch_package(goal, action)
        attempt = package.dispatch_attempt
        if attempt is None or attempt.state != GoalDispatchAttemptState.STAGED:
            raise GoalConflict("dispatch call requires one staged attempt")
        reservation_id = self._active_reservation_id or ""
        if not reservation_id or reservation.id != reservation_id:
            raise GoalGovernanceConflict(
                "dispatch call requires one applied governance reservation"
            )
        dispatch_provider = self._dispatch_provider(action)
        materialization_envelope, execution_contract = (
            self._plan_materialization_envelope(goal, action)
        )
        requested_target, placement_input_digest = self._dispatch_placement_binding(
            goal,
            action,
            dispatch_provider,
            execution_contract=execution_contract,
        )
        current_reservation = next(
            (
                item
                for item in self.governance.get_state(goal.id).action_reservations
                if item.id == reservation_id
            ),
            None,
        )
        if (
            current_reservation is None
            or current_reservation.state != GoalReservationState.APPLIED
            or current_reservation.request.materialization_envelope
            != materialization_envelope
        ):
            raise GoalGovernanceConflict(
                "dispatch call requires its durably applied reservation"
            )
        package.materialization_envelope = materialization_envelope
        try:
            result = self.dispatch(
                self._dispatch_payload(
                    goal,
                    action,
                    package,
                    attempt,
                    current_reservation,
                    dispatch_provider=dispatch_provider,
                    requested_target=requested_target,
                    placement_input_digest=placement_input_digest,
                    execution_contract=execution_contract,
                )
            )
        except Exception as exc:
            raise GoalDispatchAmbiguous(
                "Fleet dispatch outcome is ambiguous; replay the staged attempt"
            ) from exc
        if not isinstance(result, dict):
            raise GoalDispatchAmbiguous(
                "Fleet dispatch returned an ambiguous non-object response"
            )
        if result.get("accepted") is False:
            error = str(result.get("error") or "fleet dispatch rejected")[:2000]
            return self._record_rejected_dispatch(
                package,
                attempt,
                reservation_id=reservation_id,
                error=error,
                now=now,
            )
        try:
            dispatch_id = str(
                result.get("dispatch_id")
                or result.get("job_id")
                or (result.get("dispatch") or {}).get("dispatch_id")
                or ""
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise GoalDispatchAmbiguous(
                "Fleet dispatch response could not be reconciled"
            ) from exc
        if not dispatch_id:
            raise GoalDispatchAmbiguous(
                "Fleet may have admitted the dispatch without returning its id"
            )
        return self._record_admitted_dispatch(
            goal,
            package,
            attempt,
            reservation_id=reservation_id,
            dispatch_id=dispatch_id,
            now=now,
        )

    def _reconcile_staged_dispatch_record(
        self,
        goal: Goal,
        action: DispatchWorkPackageAction,
        request: GoalActionRequest,
        reservation,
        now: datetime,
    ) -> GoalDispatchAttemptState | None:
        """Recover only from an exact authority-owned durable Fleet ledger record."""

        lookup = getattr(self.dispatch_store, "by_authority_idempotency", None)
        if not callable(lookup):
            return None
        package = self._dispatch_package(goal, action)
        attempt = package.dispatch_attempt
        assert attempt is not None
        record = lookup(self.instance_id, attempt.idempotency_key)
        if record is None:
            return None
        if not self._dispatch_record_matches_attempt(
            goal,
            action,
            request,
            reservation,
            record,
        ):
            raise GoalDispatchAmbiguous(
                "authority dispatch ledger record does not match the staged attempt"
            )
        admitted_states = {"queued", "waiting_capacity"}
        admitted = any(
            str(getattr(event, "state", "")) in admitted_states
            for event in (getattr(record, "events", None) or [])
        )
        if admitted and self._dispatch_record_has_valid_admission(record):
            return self._record_admitted_dispatch(
                goal,
                package,
                attempt,
                reservation_id=reservation.id,
                dispatch_id=str(record.dispatch_id),
                now=now,
            )
        if not admitted and str(getattr(record, "state", "")) in {
            "failed",
            "cancelled",
        }:
            return self._record_rejected_dispatch(
                package,
                attempt,
                reservation_id=reservation.id,
                error=str(
                    getattr(record, "last_error", None)
                    or "Fleet rejected the dispatch before durable admission"
                )[:2000],
                now=now,
            )
        raise GoalDispatchAmbiguous(
            "authority dispatch ledger has not durably resolved the staged admission"
        )

    def _dispatch_record_matches_attempt(
        self,
        goal: Goal,
        action: DispatchWorkPackageAction,
        request: GoalActionRequest,
        reservation,
        record,
    ) -> bool:
        package = self._dispatch_package(goal, action)
        attempt = package.dispatch_attempt
        provenance = getattr(record, "goal_provenance", None)
        if attempt is None or provenance is None:
            return False
        unbound_request = reservation.request.model_copy(
            update={
                "resolved_target_instance_id": None,
                "placement_decision_digest": None,
            }
        )
        dispatch_provider = self._dispatch_provider(action)
        record_payload_digest = self._dispatch_record_payload_digest(record)
        if record_payload_digest != attempt.dispatch_payload_digest:
            return False
        decision_digest = goal_dispatch_placement_decision_digest(
            getattr(record, "placement_decision", None)
        )
        return bool(
            getattr(record, "authority_instance_id", None) == self.instance_id
            and getattr(record, "idempotency_key", None) == attempt.idempotency_key
            and getattr(record, "card_id", None) == package.card_id
            and getattr(record, "project_id", None) == goal.project_id
            and reservation.id == provenance.action_reservation_id
            and reservation.goal_id == provenance.goal_id == goal.id
            and reservation.goal_version == provenance.goal_version
            and reservation.policy_revision == provenance.policy_revision
            and reservation.fencing_token == provenance.fencing_token
            and reservation.actor_principal == provenance.actor_principal
            and reservation.authority_instance_id
            == provenance.authority_instance_id
            == self.instance_id
            and reservation.action_class
            == provenance.action_class
            == "dispatch_work_package"
            and reservation.request.operation_key
            == provenance.operation_key
            == attempt.idempotency_key
            and reservation.request.requested_placement_target
            == provenance.requested_placement_target
            and reservation.request.placement_input_digest
            == provenance.placement_input_digest
            == getattr(record, "goal_placement_input_digest", None)
            and str(reservation.request.provider_id or "").strip().lower()
            == str(provenance.provider_id or "").strip().lower()
            == dispatch_provider
            and reservation.attempt == provenance.reservation_attempt
            and reservation.max_attempts == provenance.max_reservation_attempts
            and self._dispatch_request_digest(request) == attempt.request_digest
            and self._dispatch_request_digest(unbound_request) == attempt.request_digest
            and reservation.request.resolved_target_instance_id
            == provenance.resolved_target_instance_id
            == getattr(record, "target_instance_id", None)
            and reservation.request.placement_decision_digest
            == provenance.placement_decision_digest
            == decision_digest
            and goal_dispatch_record_placement_input_valid(record)
        )

    @staticmethod
    def _dispatch_record_has_valid_admission(record) -> bool:
        if (
            getattr(record, "goal_admission_validation_state", None) != "validated"
            or getattr(record, "goal_admission_validated_at", None) is None
            or not getattr(record, "goal_admission_validation_proof", None)
        ):
            return False
        if record.goal_admission_validation_proof == goal_admission_validation_proof(
            record
        ):
            return True
        provenance = getattr(record, "goal_provenance", None)
        if provenance is None or getattr(provenance, "released_at", None) is None:
            return False
        unreleased = record.model_copy(deep=True)
        unreleased.goal_provenance = provenance.model_copy(
            update={"released_at": None, "release_reason": None}
        )
        return (
            record.goal_admission_validation_proof
            == goal_admission_validation_proof(unreleased)
        )

    @staticmethod
    def _record_rejected_dispatch(
        package: GoalWorkPackage,
        attempt: GoalDispatchAttempt,
        *,
        reservation_id: str,
        error: str,
        now: datetime,
    ) -> GoalDispatchAttemptState:
        package.attempts = attempt.generation
        package.action_reservation_id = reservation_id
        package.state = (
            WorkPackageState.FAILED
            if package.attempts >= package.max_attempts
            else WorkPackageState.READY
        )
        package.result_summary = (
            f"Dispatch retry limit exhausted after attempt {attempt.generation}: "
            f"{error}"
            if package.state == WorkPackageState.FAILED
            else f"Dispatch attempt {attempt.generation} was rejected: {error}"
        )
        package.fleet_lifecycle_owned = False
        attempt.state = GoalDispatchAttemptState.REJECTED
        attempt.reservation_id = reservation_id
        attempt.release_pending = True
        attempt.error = error
        attempt.updated_at = now
        package.updated_at = now
        return GoalDispatchAttemptState.REJECTED

    def _record_admitted_dispatch(
        self,
        goal: Goal,
        package: GoalWorkPackage,
        attempt: GoalDispatchAttempt,
        *,
        reservation_id: str,
        dispatch_id: str,
        now: datetime,
    ) -> GoalDispatchAttemptState:
        receipt_digest = self._dispatch_admission_receipt_digest(
            dispatch_id=dispatch_id,
            idempotency_key=attempt.idempotency_key,
            request_digest=attempt.request_digest,
            reservation_id=reservation_id,
        )
        package.attempts = attempt.generation
        package.action_reservation_id = reservation_id
        self._append_bounded(package.dispatch_ids, dispatch_id, package.max_attempts)
        package.state = WorkPackageState.DISPATCHED
        package.dispatch_admission_receipt_digest = receipt_digest
        package.fleet_lifecycle_owned = True
        package.updated_at = now
        attempt.state = GoalDispatchAttemptState.ADMITTED
        attempt.reservation_id = reservation_id
        attempt.dispatch_id = dispatch_id
        attempt.admission_receipt_digest = receipt_digest
        attempt.fleet_lifecycle_owned = True
        attempt.updated_at = now
        self._append_bounded(
            goal.linked_dispatch_ids, dispatch_id, _MAX_GOAL_DISPATCH_IDS
        )
        return GoalDispatchAttemptState.ADMITTED

    def _validate_staged_dispatch_reservation(
        self,
        goal: Goal,
        attempt: GoalDispatchAttempt,
        request: GoalActionRequest,
        reservation,
        *,
        reserve_key: str,
    ) -> None:
        unbound_request = reservation.request.model_copy(
            update={
                "resolved_target_instance_id": None,
                "placement_decision_digest": None,
            }
        )
        if (
            reservation.idempotency_key != reserve_key
            or reservation.goal_id != goal.id
            or reservation.action_class != "dispatch_work_package"
            or reservation.actor_principal != self.service_principal
            or reservation.authority_instance_id != self.instance_id
            or reservation.request.operation_key != attempt.idempotency_key
            or self._dispatch_request_digest(request) != attempt.request_digest
            or self._dispatch_request_digest(unbound_request) != attempt.request_digest
        ):
            raise GoalGovernanceConflict(
                "durable dispatch reservation does not match its staged request"
            )

    def _dispatch_payload(
        self,
        goal: Goal,
        action: DispatchWorkPackageAction,
        package: GoalWorkPackage,
        attempt: GoalDispatchAttempt,
        reservation,
        *,
        dispatch_provider: str,
        requested_target: str,
        placement_input_digest: str,
        execution_contract: dict[str, Any],
    ) -> dict[str, Any]:
        materialization_envelope = reservation.request.materialization_envelope
        if materialization_envelope is None:
            raise GoalGovernanceConflict(
                "dispatch reservation has no materialization envelope"
            )
        return {
            "authority_instance_id": self.instance_id,
            "goal_provenance": {
                "goal_id": goal.id,
                "goal_version": reservation.goal_version,
                "policy_revision": reservation.policy_revision,
                "authority_instance_id": reservation.authority_instance_id,
                "fencing_token": reservation.fencing_token,
                "action_reservation_id": reservation.id,
                "operation_key": attempt.idempotency_key,
                "requested_placement_target": requested_target,
                "placement_input_digest": placement_input_digest,
                "materialization_envelope": materialization_envelope.model_dump(
                    mode="json"
                ),
                "actor_principal": reservation.actor_principal,
                "action_class": "dispatch_work_package",
                "provider_id": dispatch_provider,
                "reservation_attempt": reservation.attempt,
                "max_reservation_attempts": reservation.max_attempts,
            },
            "card_id": package.card_id,
            "project_id": goal.project_id,
            "target_instance_id": action.target_instance_id,
            "placement_policy": action.placement_policy,
            "group_id": action.group_id,
            "message": self._work_prompt(goal, package, action.message),
            "provider": dispatch_provider,
            "model_id": action.model_id,
            "mode_id": action.mode_id,
            "execution_contract": execution_contract,
            "priority": action.priority,
            "collaboration_unattended": True,
            "collaboration_risk": (
                "high"
                if package.role in {GoalActorRole.VERIFIER, GoalActorRole.CRITIC}
                else "medium"
            ),
            "idempotency_key": attempt.idempotency_key,
        }

    def _dispatch_provider(self, action: DispatchWorkPackageAction) -> str:
        provider = str(action.provider or self.default_provider).strip().lower()
        if not provider:
            raise RuntimeError(
                "governed fleet dispatch requires a concrete configured provider"
            )
        return provider

    def _plan_materialization_envelope(
        self,
        goal: Goal,
        action: DispatchWorkPackageAction,
    ) -> tuple[GoalMaterializationEnvelopeV1, dict[str, Any]]:
        """Resolve exact server-owned resources before governance reserves them."""

        package = next(
            (item for item in goal.work_packages if item.id == action.work_package_id),
            None,
        )
        if package is None or not package.card_id:
            raise ValueError("dispatch requires a materialized work-package card")
        card = self.store.get_card(package.card_id, realm_id=goal.realm_id)
        if card is None:
            raise ValueError("work-package card disappeared before materialization")
        active_attachments = [
            item
            for item in card.attachments
            if str(getattr(item.state, "value", item.state)) == "active"
        ]
        project = (
            self.store.get_project(goal.project_id, realm_id=goal.realm_id)
            if goal.project_id
            else None
        )
        project_repositories = (
            list(
                self.store.list_project_repositories(
                    goal.project_id,
                    realm_id=goal.realm_id,
                )
            )
            if goal.project_id
            else []
        )
        project_contract = dict(getattr(project, "tool_config", None) or {}).get(
            "execution_contract"
        )
        if project_contract:
            contract = ExecutionContract.model_validate(project_contract)
        else:
            repositories = tuple(
                RepositoryRequirement(
                    repository_id=repository.id,
                    branch=getattr(link, "branch", None),
                )
                for repository, link in project_repositories
            )
            contract = ExecutionContract(
                profile=(
                    ExecutionProfile.REPOSITORY
                    if repositories
                    else ExecutionProfile.RESEARCH
                ),
                requirements=ExecutionRequirements(
                    repository_required=bool(repositories),
                    repositories=list(repositories),
                    attachments=bool(active_attachments),
                    required_capabilities=list(package.preferred_capabilities),
                ),
                confirmed=True,
            )
        requirements = contract.requirements.model_copy(deep=True)
        requirements.attachments = bool(active_attachments)
        requirements.required_capabilities = sorted(
            set(requirements.required_capabilities)
            | set(package.preferred_capabilities)
        )
        contract = contract.model_copy(update={"requirements": requirements})
        explicit_repositories = [
            repository
            for requirement in contract.requirements.repositories
            if (
                repository := self.store.get_repository(
                    requirement.repository_id,
                    goal.realm_id,
                )
            )
            is not None
        ]
        requested_target = action.target_instance_id or (
            f"placement:{action.placement_policy or 'best_match'}"
        )
        plan = resolve_materialization_plan(
            requested=contract,
            card=card,
            project=project,
            project_repositories=project_repositories,
            explicit_repositories=explicit_repositories,
            target_instance_id=requested_target,
        )
        if not plan.admissible:
            raise ValueError(
                "materialization envelope is not admissible: " + plan.summary
            )
        repository_ids = tuple(str(item["repository_id"]) for item in plan.repositories)
        claims = [
            GoalMaterializationResourceClaimV1(
                key=f"fleet-dispatch:{requested_target}",
                access="shared",
                quantity=1,
                preemptible=True,
            ),
            *[
                GoalMaterializationResourceClaimV1(
                    key=f"repository:{repository_id}",
                    access="shared",
                    quantity=1,
                    preemptible=True,
                )
                for repository_id in repository_ids
            ],
        ]
        contract_payload = contract.model_dump(mode="json")
        envelope = GoalMaterializationEnvelopeV1(
            work_package_id=package.id,
            service_role=(
                "verifier" if package.role == GoalActorRole.VERIFIER else "executor"
            ),
            repository_ids=repository_ids,
            data_scopes=tuple(goal.policy.data_scope),
            attachment_ids=tuple(item.attachment_id for item in active_attachments),
            attachment_classes=tuple(
                item.media_type.strip().lower() for item in active_attachments
            ),
            resource_claims=tuple(claims),
            execution_contract_digest=canonical_materialization_digest(
                contract_payload
            ),
        )
        return envelope, contract_payload

    @staticmethod
    def _dispatch_package(
        goal: Goal,
        action: DispatchWorkPackageAction,
    ) -> GoalWorkPackage:
        package = next(
            (item for item in goal.work_packages if item.id == action.work_package_id),
            None,
        )
        if package is None:
            raise ValueError("dispatch references an unknown work package")
        return package

    @staticmethod
    def _dispatch_request_digest(request: GoalActionRequest) -> str:
        encoded = json.dumps(
            request.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    def _dispatch_payload_digest(
        self,
        goal: Goal,
        action: DispatchWorkPackageAction,
        package: GoalWorkPackage,
        dispatch_provider: str,
        execution_contract: dict[str, Any],
    ) -> str:
        return self._canonical_digest(
            {
                "card_id": package.card_id,
                "project_id": goal.project_id,
                "target_instance_id": action.target_instance_id,
                "placement_policy": action.placement_policy,
                "group_id": action.group_id,
                "provider": dispatch_provider,
                "model_id": action.model_id,
                "mode_id": action.mode_id,
                "required_capabilities": [],
                "required_mcp_servers": [],
                "optional_mcp_servers": [],
                "title": None,
                "message": self._work_prompt(goal, package, action.message),
                "collaboration_risk": (
                    "high"
                    if package.role in {GoalActorRole.VERIFIER, GoalActorRole.CRITIC}
                    else "medium"
                ),
                "collaboration_ambiguous": False,
                "collaboration_unattended": True,
                "effort": None,
                "cwd": None,
                "capacity_override": False,
                "capacity_override_reason": None,
                "participation_override": False,
                "participation_override_reason": None,
                "execution_contract": execution_contract,
                "priority": action.priority,
                "allow_concurrent": False,
                "resume_session_id": None,
            }
        )

    @classmethod
    def _dispatch_record_payload_digest(cls, record) -> str | None:
        placement = getattr(record, "goal_placement_input", None)
        request_payload = getattr(record, "request_payload", None)
        if not isinstance(placement, dict) or not isinstance(request_payload, dict):
            return None
        return cls._canonical_digest(
            {
                "card_id": placement.get("card_id"),
                "project_id": placement.get("project_id"),
                "target_instance_id": placement.get("target_instance_id"),
                "placement_policy": placement.get("placement_policy"),
                "group_id": placement.get("group_id"),
                "provider": placement.get("provider"),
                "model_id": placement.get("model_id"),
                "mode_id": placement.get("mode_id"),
                "required_capabilities": placement.get("required_capabilities") or [],
                "required_mcp_servers": placement.get("required_mcp_servers") or [],
                "optional_mcp_servers": placement.get("optional_mcp_servers") or [],
                "title": request_payload.get("title"),
                "message": request_payload.get("message"),
                "collaboration_risk": request_payload.get("collaboration_risk"),
                "collaboration_ambiguous": request_payload.get(
                    "collaboration_ambiguous"
                ),
                "collaboration_unattended": request_payload.get(
                    "collaboration_unattended"
                ),
                "effort": request_payload.get("effort"),
                "cwd": request_payload.get("cwd"),
                "capacity_override": bool(placement.get("capacity_override")),
                "capacity_override_reason": request_payload.get(
                    "capacity_override_reason"
                ),
                "participation_override": bool(placement.get("participation_override")),
                "participation_override_reason": request_payload.get(
                    "participation_override_reason"
                ),
                "execution_contract": placement.get("execution_contract"),
                "priority": request_payload.get("priority"),
                "allow_concurrent": bool(getattr(record, "allow_concurrent", False)),
                "resume_session_id": getattr(record, "resume_session_id", None),
            }
        )

    @staticmethod
    def _canonical_digest(payload: dict[str, Any]) -> str:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _dispatch_admission_receipt_digest(
        *,
        dispatch_id: str,
        idempotency_key: str,
        request_digest: str,
        reservation_id: str,
    ) -> str:
        encoded = json.dumps(
            {
                "contract": "pa.goal-dispatch-admission-receipt.v1",
                "dispatch_id": dispatch_id,
                "idempotency_key": idempotency_key,
                "request_digest": request_digest,
                "reservation_id": reservation_id,
                "fleet_lifecycle_owned": True,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _dispatch_placement_binding(
        goal: Goal,
        action: DispatchWorkPackageAction,
        provider: str,
        *,
        execution_contract: dict[str, Any],
    ) -> tuple[str, str]:
        package = next(
            (item for item in goal.work_packages if item.id == action.work_package_id),
            None,
        )
        if package is None or not package.card_id:
            raise ValueError("dispatch requires a materialized work-package card")
        requested_target = action.target_instance_id or (
            f"placement:{action.placement_policy or 'best_match'}"
        )
        placement_input = {
            "card_id": package.card_id,
            "project_id": goal.project_id,
            "target_instance_id": action.target_instance_id,
            "placement_policy": action.placement_policy,
            "group_id": action.group_id,
            "provider": provider,
            "model_id": action.model_id,
            "mode_id": action.mode_id,
            "execution_contract": execution_contract,
        }
        return requested_target, goal_dispatch_placement_input_digest(placement_input)

    @staticmethod
    def _dispatch_operation_key(
        goal: Goal,
        action: DispatchWorkPackageAction,
    ) -> str:
        package = next(
            (item for item in goal.work_packages if item.id == action.work_package_id),
            None,
        )
        if package is None:
            raise ValueError("dispatch references an unknown work package")
        if package.dispatch_attempt is not None:
            return package.dispatch_attempt.idempotency_key
        return (f"goal:{goal.id}:work:{package.id}:attempt:{package.attempts + 1}")[
            :200
        ]

    @staticmethod
    def _record_dispatch_failure(
        goal: Goal,
        action: DispatchWorkPackageAction,
        reason: str,
        now: datetime,
    ) -> None:
        package = next(
            (item for item in goal.work_packages if item.id == action.work_package_id),
            None,
        )
        if package is None or package.attempts >= package.max_attempts:
            return
        package.attempts += 1
        package.state = (
            WorkPackageState.FAILED
            if package.attempts >= package.max_attempts
            else WorkPackageState.READY
        )
        package.result_summary = (
            f"Dispatch retry limit exhausted after attempt {package.attempts}: {reason}"
            if package.state == WorkPackageState.FAILED
            else f"Dispatch attempt {package.attempts} failed before admission: {reason}"
        )[:8000]
        package.updated_at = now

    def _record_evidence(
        self,
        goal: Goal,
        action: RecordEvidenceAction,
        now: datetime,
    ) -> None:
        self.service.ingest_evidence_snapshot(
            goal,
            GoalEvidenceCreate(
                evidence=action.evidence,
                criterion_verdicts=action.criterion_verdicts,
            ),
            context=GoalMutationContext(
                actor_principal=self.service_principal,
                authority_instance_id=self.instance_id,
                idempotency_key=f"goal:{goal.id}:supervisor-record-evidence",
                expected_version=goal.version,
                policy_revision=goal.policy.revision,
                fencing_token=goal.lease.fencing_token,
            ),
            now=now,
        )

    def _advance_ready_work(self, goal: Goal, now: datetime) -> bool:
        changed = False
        by_id = {item.id: item for item in goal.work_packages}
        for package in goal.work_packages:
            if package.state != WorkPackageState.PLANNED:
                continue
            dependencies = [by_id[item] for item in package.depends_on]
            if package.role == GoalActorRole.VERIFIER:
                ready = all(
                    item.state
                    in {
                        WorkPackageState.AWAITING_VERIFICATION,
                        WorkPackageState.VERIFIED,
                    }
                    for item in dependencies
                )
            else:
                ready = all(
                    item.state == WorkPackageState.VERIFIED for item in dependencies
                )
            if ready:
                package.state = WorkPackageState.READY
                package.updated_at = now
                changed = True
        return changed

    def _ensure_dispatch_proposals(self, goal: Goal, now: datetime) -> bool:
        changed = False
        for package in goal.work_packages:
            if (
                package.state != WorkPackageState.READY
                or not package.dispatch_when_ready
                or package.dispatch_attempt is not None
            ):
                continue
            active = any(
                isinstance(item.action, DispatchWorkPackageAction)
                and item.action.work_package_id == package.id
                and item.status
                in {
                    ProposalStatus.PENDING,
                    ProposalStatus.AUTHORIZED,
                    ProposalStatus.OPERATOR_REQUIRED,
                }
                for item in goal.proposals
            )
            if active:
                continue
            prior_proposals = sum(
                isinstance(item.action, DispatchWorkPackageAction)
                and item.action.work_package_id == package.id
                for item in goal.proposals
            )
            if (
                package.attempts >= package.max_attempts
                or prior_proposals >= package.max_attempts
            ):
                package.attempts = max(
                    package.attempts,
                    min(prior_proposals, package.max_attempts),
                )
                package.state = WorkPackageState.FAILED
                package.result_summary = (
                    "Dispatch retry limit exhausted before a session was admitted."
                )
                package.updated_at = now
                changed = True
                continue
            generation = prior_proposals + 1
            proposal_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"pa-goal:{goal.id}:dispatch:{package.id}:{generation}",
                )
            )
            goal.proposals.append(
                GoalProposal(
                    id=proposal_id,
                    proposer_principal=self.service_principal,
                    proposer_role=GoalActorRole.COORDINATOR,
                    action=DispatchWorkPackageAction(
                        work_package_id=package.id,
                        target_instance_id=package.preferred_instance_id,
                        placement_policy=(
                            None if package.preferred_instance_id else "best_match"
                        ),
                    ),
                    rationale=(
                        f"Dependencies are satisfied for {package.role.value} work."
                    ),
                    expected_goal_version=goal.version,
                    policy_revision=goal.policy.revision,
                    created_at=now,
                    updated_at=now,
                )
            )
            changed = True
        return changed

    def _reconcile_dispatches(
        self,
        goal: Goal,
        now: datetime,
        *,
        durably_admitted_packages: set[str],
    ) -> bool:
        if not self.dispatch_store:
            return False
        changed = False
        for package in goal.work_packages:
            if not package.dispatch_ids:
                continue
            record = self.dispatch_store.get(package.dispatch_ids[-1])
            if not record:
                continue
            self._reconcile_dispatch_reservation(goal, package, record)
            provenance = getattr(record, "goal_provenance", None)
            if provenance is not None:
                if package.materialization_envelope != (
                    provenance.materialization_envelope
                ):
                    package.materialization_envelope = (
                        provenance.materialization_envelope
                    )
                    changed = True
                if package.materialization_receipt != (
                    provenance.materialization_receipt
                ):
                    package.materialization_receipt = provenance.materialization_receipt
                    changed = True
                if package.execution_identity != provenance.execution_identity:
                    package.execution_identity = provenance.execution_identity
                    changed = True
                if package.execution_identity is not None:
                    service_id = package.execution_identity.assigned_service_principal
                    if package.role == GoalActorRole.VERIFIER:
                        if package.verifier_service_id != service_id:
                            package.verifier_service_id = service_id
                            changed = True
                    elif package.executor_service_id != service_id:
                        package.executor_service_id = service_id
                        changed = True
            if (
                package.dispatch_attempt is not None
                and package.dispatch_attempt.state == GoalDispatchAttemptState.ADMITTED
                and package.id not in durably_admitted_packages
                and record.state in {"completed", "failed", "cancelled"}
            ):
                # First checkpoint the exact admission receipt. Terminal package
                # mutation is safe only in a later cycle after Fleet's release is
                # durably observable and the admission attempt has been cleared.
                continue
            fingerprint = self._dispatch_fingerprint(record)
            if fingerprint != package.last_progress_fingerprint:
                package.last_progress_fingerprint = fingerprint
                package.last_progress_at = now
                package.no_progress_cycles = 0
                changed = True
            elif record.state not in {"completed", "failed", "cancelled"}:
                package.no_progress_cycles += 1
            if record.session_id and record.session_id != package.session_id:
                if package.session_id:
                    self._append_bounded(
                        package.replacement_session_ids,
                        package.session_id,
                        _MAX_PACKAGE_REPLACEMENTS,
                    )
                    self._append_bounded(
                        goal.supervision.replacement_session_ids,
                        package.session_id,
                        _MAX_GOAL_REPLACEMENTS,
                    )
                package.session_id = record.session_id
                service_id = (
                    package.execution_identity.assigned_service_principal
                    if package.execution_identity is not None
                    else None
                )
                if package.role == GoalActorRole.VERIFIER:
                    package.verifier_service_id = service_id
                else:
                    package.executor_service_id = service_id
                changed = True
            if record.state == "running" and package.state != WorkPackageState.RUNNING:
                package.state = WorkPackageState.RUNNING
                changed = True
            elif record.state == "completed":
                if package.state == WorkPackageState.VERIFIED or (
                    package.role == GoalActorRole.EXECUTOR
                    and package.state == WorkPackageState.AWAITING_VERIFICATION
                ):
                    continue
                completed = self._governed_action(
                    goal,
                    (
                        f"goal-supervisor:{goal.id}:dispatch:{record.dispatch_id}:"
                        f"terminal:{package.state.value}"
                    ),
                    GoalActionRequest(
                        action_class=(
                            "record_evidence"
                            if package.role == GoalActorRole.VERIFIER
                            else "observe.dispatch.read"
                        )
                    ),
                    lambda package=package, record=record: self._complete_package(
                        goal, package, record, now
                    ),
                )
                changed = completed or changed
            elif record.state in {"failed", "cancelled"}:
                if record.session_id:
                    self._append_bounded(
                        package.replacement_session_ids,
                        record.session_id,
                        _MAX_PACKAGE_REPLACEMENTS,
                    )
                if package.attempts < package.max_attempts:
                    package.state = WorkPackageState.READY
                    package.result_summary = (
                        f"Replacement required after {record.state}: "
                        f"{record.last_error or 'no detail'}"
                    )
                else:
                    package.state = WorkPackageState.FAILED
                changed = True
            package.updated_at = now
        return changed

    def _reconcile_dispatch_reservation(self, goal, package, record) -> None:
        """Release a dispatch hold before any terminal supervisor mutation.

        Fleet owns the primary lifecycle release. This bounded reconciliation path
        closes legacy and crash windows where the supervisor can observe a running
        or terminal record before fleet has durably released its canonical hold.
        """

        if record.state not in {
            "running",
            "completion_pending",
            "completed",
            "failed",
            "cancelled",
        }:
            return
        provenance = getattr(record, "goal_provenance", None)
        reservation_id = (
            provenance.action_reservation_id
            if provenance is not None
            else package.action_reservation_id
        )
        if not reservation_id:
            return
        state = self.governance.get_state(goal.id)
        reservation = next(
            (item for item in state.action_reservations if item.id == reservation_id),
            None,
        )
        if reservation is None:
            raise GoalGovernanceConflict(
                "dispatch reconciliation could not find its action reservation"
            )
        if reservation.state == GoalReservationState.RELEASED:
            return
        if (
            reservation.actor_principal != self.service_principal
            or reservation.authority_instance_id != self.instance_id
        ):
            raise GoalGovernanceConflict(
                "dispatch reconciliation cannot release another controller's hold"
            )
        applied = record.state in {
            "running",
            "completion_pending",
            "completed",
        } or bool(getattr(record, "prompt_acknowledged_at", None))
        self.governance.reconcile_action_release(
            goal.id,
            reservation.id,
            GovernanceMutationContext(
                actor_principal=self.service_principal,
                authority_instance_id=self.instance_id,
                idempotency_key=(
                    f"goal-supervisor:{goal.id}:dispatch:{record.dispatch_id}:"
                    f"release:attempt:{reservation.attempt}"
                )[:200],
                expected_version=state.version,
                policy_revision=goal.policy.revision,
                goal_version=goal.version,
                fencing_token=reservation.fencing_token,
            ),
            actual_usage=(
                reservation.reserved_usage.model_copy(deep=True)
                if applied
                else GoalUsage()
            ),
            reason=f"supervisor observed fleet dispatch {record.state}",
        )

    def _complete_package(self, goal: Goal, package, record, now: datetime) -> bool:
        if package.state == WorkPackageState.VERIFIED:
            return False
        if package.role == GoalActorRole.VERIFIER:
            report = record.final_report
            dependencies = set(package.depends_on)
            dependency_packages = [
                item for item in goal.work_packages if item.id in dependencies
            ]
            executor_bindings = [
                (item, item.execution_identity)
                for item in dependency_packages
                if item.execution_identity is not None
            ]
            verifier_binding = package.execution_identity
            verifier_identity = (
                verifier_binding.assigned_service_principal
                if verifier_binding is not None
                else None
            )
            independent = bool(
                verifier_binding is not None
                and verifier_binding.work_package_id == package.id
                and verifier_binding.service_role == "verifier"
                and record.session_id == verifier_binding.session_id
                and dependency_packages
                and len(executor_bindings) == len(dependency_packages)
                and all(
                    identity.work_package_id == dependency.id
                    and identity.service_role
                    == (
                        "verifier"
                        if dependency.role == GoalActorRole.VERIFIER
                        else "executor"
                    )
                    and verifier_binding.session_id != identity.session_id
                    and verifier_binding.target_instance_id
                    != identity.target_instance_id
                    and verifier_binding.provider_id != identity.provider_id
                    and verifier_binding.assigned_service_principal
                    != identity.assigned_service_principal
                    for dependency, identity in executor_bindings
                )
            )
            passed = bool(
                report
                and not report.blockers
                and report.validations
                and all(item.status == "passed" for item in report.validations)
                and independent
            )
            if not passed:
                package.state = (
                    WorkPackageState.READY
                    if package.attempts < package.max_attempts
                    else WorkPackageState.FAILED
                )
                package.result_summary = (
                    "Verifier was not independent of the executor."
                    if not independent
                    else "Verifier did not provide passing evidence."
                )
                return True
            package.state = WorkPackageState.VERIFIED
            package.result_summary = report.outcome
            evidence = GoalEvidence(
                criterion_ids=package.criterion_ids,
                kind=EvidenceKind.AUDIT,
                summary=report.outcome,
                provenance={
                    "dispatch_id": record.dispatch_id,
                    "session_id": record.session_id,
                    "role": "verifier",
                    "validations": [
                        item.model_dump(mode="json") for item in report.validations
                    ],
                },
                observed_at=now,
            )
            package.verifier_service_id = verifier_identity
            self.service.ingest_evidence_snapshot(
                goal,
                GoalEvidenceCreate(
                    evidence=evidence,
                    criterion_verdicts={
                        criterion_id: CriterionVerdict.SATISFIED
                        for criterion_id in package.criterion_ids
                    },
                ),
                context=GoalMutationContext(
                    actor_principal=verifier_identity,
                    authority_instance_id=str(
                        getattr(record, "authority_instance_id", "") or self.instance_id
                    ),
                    idempotency_key=(
                        f"goal:{goal.id}:dispatch:{record.dispatch_id}:evidence"
                    ),
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                    fencing_token=goal.lease.fencing_token,
                ),
                now=now,
            )
            for item in goal.work_packages:
                if item.id in dependencies:
                    item.state = WorkPackageState.VERIFIED
                    item.updated_at = now
            return True
        if package.role == GoalActorRole.CRITIC:
            package.state = WorkPackageState.VERIFIED
            package.result_summary = (
                record.final_report.outcome
                if record.final_report
                else "Critic dispatch completed."
            )
            return True
        if package.state == WorkPackageState.AWAITING_VERIFICATION:
            return False
        package.state = WorkPackageState.AWAITING_VERIFICATION
        package.result_summary = (
            record.final_report.outcome
            if record.final_report
            else "Executor dispatch completed; independent verification required."
        )
        self._ensure_verifier_proposal(goal, package, now)
        return True

    def _ensure_verifier_proposal(
        self, goal: Goal, package: GoalWorkPackage, now: datetime
    ) -> None:
        proposal_id = str(
            uuid5(NAMESPACE_URL, f"pa-goal:{goal.id}:verify:{package.id}")
        )
        if any(item.id == proposal_id for item in goal.proposals):
            return
        goal.proposals.append(
            GoalProposal(
                id=proposal_id,
                proposer_principal=self.service_principal,
                proposer_role=GoalActorRole.COORDINATOR,
                action=CreateWorkPackageAction(
                    title=f"Verify: {package.title}",
                    objective=(
                        "Independently verify the linked executor result against every "
                        "assigned success criterion. Run concrete checks and report "
                        "passing validations; do not trust the executor's conclusion."
                    ),
                    criterion_ids=package.criterion_ids,
                    depends_on=[package.id],
                    role=GoalActorRole.VERIFIER,
                    max_attempts=2,
                ),
                rationale="Independent verification is required before criterion acceptance.",
                expected_goal_version=goal.version,
                policy_revision=goal.policy.revision,
                created_at=now,
                updated_at=now,
            )
        )

    def _detect_drift(self, goal: Goal, now: datetime) -> bool:
        reasons: list[str] = []
        stalled = [
            item
            for item in goal.work_packages
            if item.no_progress_cycles >= self.stalled_threshold
            and item.state in _ACTIVE_WORK
        ]
        drifting = [
            item
            for item in goal.work_packages
            if item.no_progress_cycles >= self.no_progress_threshold
            and item.state in _ACTIVE_WORK
        ]
        if stalled:
            reasons.append(
                "No meaningful progress from: "
                + ", ".join(sorted(item.title for item in stalled))
            )
        elif drifting:
            reasons.append(
                "Progress is repeating or stale for: "
                + ", ".join(sorted(item.title for item in drifting))
            )
        covered = {
            criterion_id
            for item in goal.work_packages
            if item.role != GoalActorRole.CRITIC
            for criterion_id in item.criterion_ids
        }
        missing = {item.id for item in goal.criteria} - covered
        if goal.state in {GoalState.ACTIVE, GoalState.VERIFYING} and missing:
            reasons.append(f"No work package covers criteria: {sorted(missing)}")
        state = (
            GoalDriftState.STALLED
            if stalled
            else GoalDriftState.DRIFTING
            if reasons
            else GoalDriftState.ON_TRACK
        )
        changed = (
            state != goal.supervision.drift_state
            or reasons != goal.supervision.drift_reasons
        )
        goal.supervision.drift_state = state
        goal.supervision.drift_reasons = reasons
        if stalled:
            for package in stalled:
                package.state = WorkPackageState.BLOCKED
                package.updated_at = now
            self._ensure_critic_proposal(goal, reasons, now)
        return changed

    def _ensure_critic_proposal(
        self, goal: Goal, reasons: list[str], now: datetime
    ) -> None:
        generation = goal.strategy_revision
        proposal_id = str(
            uuid5(NAMESPACE_URL, f"pa-goal:{goal.id}:critic:{generation}")
        )
        if any(item.id == proposal_id for item in goal.proposals):
            return
        goal.proposals.append(
            GoalProposal(
                id=proposal_id,
                proposer_principal=self.service_principal,
                proposer_role=GoalActorRole.COORDINATOR,
                action=CreateWorkPackageAction(
                    title=f"Critique stalled strategy for {goal.objective[:120]}",
                    objective=(
                        "Critique the current work graph and progress evidence. Identify "
                        "the concrete cause of drift and submit a typed revise_strategy "
                        f"proposal. Current signals: {'; '.join(reasons)}"
                    ),
                    criterion_ids=[item.id for item in goal.criteria],
                    role=GoalActorRole.CRITIC,
                    max_attempts=1,
                ),
                rationale="Stalled work requires an independent strategy critique.",
                expected_goal_version=goal.version,
                policy_revision=goal.policy.revision,
                created_at=now,
                updated_at=now,
            )
        )

    def _ensure_interaction(
        self, goal: Goal, proposal: GoalProposal, *, approval: bool
    ) -> GoalOperatorInteraction | None:
        self._assert_side_effect_fence(goal)
        existing = next(
            (
                item
                for item in goal.operator_interactions
                if item.proposal_id == proposal.id
            ),
            None,
        )
        if existing or not self.notifications:
            return existing
        action = proposal.action
        if approval:
            prompt = (
                f"Approve {action.kind} for goal '{goal.objective}'? "
                f"Rationale: {proposal.rationale}"
            )
            choices = [
                InteractionChoice(id="approve", label="Approve", value=True),
                InteractionChoice(id="reject", label="Reject", value=False),
            ]
            response_schema = None
            allow_freeform = False
            allow_cancel = True
            deadline = None
            kind = InteractionKind.APPROVAL
        elif isinstance(action, RequestOperatorAction):
            prompt = action.prompt
            choices = [
                InteractionChoice.model_validate(item.model_dump(mode="python"))
                for item in action.choices
            ]
            response_schema = action.response_schema
            allow_freeform = action.allow_freeform
            allow_cancel = action.allow_cancel
            deadline = action.deadline
            kind = InteractionKind.MCP_OPERATOR_INPUT
        else:
            return None
        notification = self.notifications.create(
            NotificationCreate(
                realm_id=goal.realm_id,
                type=NotificationType.INTERACTION,
                severity=NotificationSeverity.WARNING
                if approval
                else NotificationSeverity.INFO,
                priority=NotificationPriority.HIGH
                if approval
                else NotificationPriority.NORMAL,
                title=f"Goal operator input: {goal.objective[:180]}",
                body=prompt,
                summary=prompt[:1000],
                project_id=goal.project_id,
                interaction=InteractionRequest(
                    kind=kind,
                    prompt=prompt,
                    response_schema=response_schema,
                    choices=choices,
                    allow_freeform=allow_freeform,
                    allow_cancel=allow_cancel,
                    continuation_mode="none",
                    deadline=deadline,
                ),
                deduplication_key=f"goal:{goal.id}:proposal:{proposal.id}",
            ),
            principal_id=self.service_principal,
            instance_id=self.instance_id,
        )
        interaction = GoalOperatorInteraction(
            proposal_id=proposal.id,
            notification_id=notification.id,
        )
        goal.operator_interactions.append(interaction)
        return interaction

    def _ingest_interactions(self, goal: Goal, now: datetime) -> bool:
        changed = False
        by_proposal = {item.id: item for item in goal.proposals}
        for link in goal.operator_interactions:
            if link.state != GoalInteractionState.PENDING:
                continue
            notification = self.store.get_notification(
                link.notification_id, realm_id=goal.realm_id
            )
            if not notification or not notification.interaction:
                continue
            interaction = notification.interaction
            if interaction.state not in _TERMINAL_INTERACTIONS:
                continue
            proposal = by_proposal.get(link.proposal_id)
            if interaction.state in {
                InteractionState.CANCELLED,
                InteractionState.EXPIRED,
            }:
                link.state = (
                    GoalInteractionState.CANCELLED
                    if interaction.state == InteractionState.CANCELLED
                    else GoalInteractionState.EXPIRED
                )
                if proposal and proposal.status == ProposalStatus.OPERATOR_REQUIRED:
                    proposal.status = ProposalStatus.REJECTED
                    proposal.error = f"operator interaction {interaction.state.value}"
                    if isinstance(proposal.action, DispatchWorkPackageAction):
                        self._record_dispatch_failure(
                            goal,
                            proposal.action,
                            proposal.error,
                            now,
                        )
            else:
                link.state = GoalInteractionState.ANSWERED
                link.response_summary = interaction.response_summary or ""
                link.response_principal = interaction.response_principal
                if proposal and proposal.status == ProposalStatus.OPERATOR_REQUIRED:
                    response = interaction.response or {}
                    approved = (
                        isinstance(response, dict)
                        and response.get("choice_id") == "approve"
                    )
                    proposal.status = (
                        ProposalStatus.AUTHORIZED
                        if approved
                        else ProposalStatus.REJECTED
                    )
                    if not approved:
                        proposal.error = "operator rejected the proposal"
                        if isinstance(proposal.action, DispatchWorkPackageAction):
                            self._record_dispatch_failure(
                                goal,
                                proposal.action,
                                proposal.error,
                                now,
                            )
            link.resolved_at = now
            changed = True
        return changed

    def _advance_goal_state(self, goal: Goal) -> bool:
        previous = goal.state
        outstanding = any(
            item.state == GoalInteractionState.PENDING
            for item in goal.operator_interactions
        )
        if outstanding and goal.state in {GoalState.ACTIVE, GoalState.VERIFYING}:
            goal.state = GoalState.WAITING_OPERATOR
        elif not outstanding and goal.state == GoalState.WAITING_OPERATOR:
            goal.state = GoalState.ACTIVE
        elif goal.supervision.drift_state == GoalDriftState.STALLED and goal.state in {
            GoalState.ACTIVE,
            GoalState.VERIFYING,
        }:
            goal.state = GoalState.BLOCKED
        elif (
            goal.state == GoalState.BLOCKED
            and any(item.state in _ACTIVE_WORK for item in goal.work_packages)
            or goal.state == GoalState.READY
            and goal.work_packages
        ):
            goal.state = GoalState.ACTIVE
        elif (
            goal.state == GoalState.ACTIVE
            and goal.work_packages
            and all(
                item.state in {WorkPackageState.VERIFIED, WorkPackageState.CANCELLED}
                for item in goal.work_packages
            )
        ):
            goal.state = GoalState.VERIFYING
        return previous != goal.state

    @staticmethod
    def _dispatch_fingerprint(record) -> str:
        progress = record.latest_progress
        payload = {
            "state": record.state,
            "session_id": record.session_id,
            "error": record.last_error,
            "progress": (
                progress.model_dump(mode="json") if progress is not None else None
            ),
            "final": (
                record.final_report.model_dump(mode="json")
                if record.final_report is not None
                else None
            ),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    @staticmethod
    def _cursor(goal: Goal) -> str:
        payload = {
            "version": goal.version,
            "proposals": [(item.id, item.status.value) for item in goal.proposals],
            "work": [
                (
                    item.id,
                    item.state.value,
                    item.attempts,
                    item.no_progress_cycles,
                )
                for item in goal.work_packages
            ],
            "interactions": [
                (item.id, item.state.value) for item in goal.operator_interactions
            ],
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    @staticmethod
    def _work_prompt(goal: Goal, package: GoalWorkPackage, message: str) -> str:
        criteria = {
            item.id: item.description
            for item in goal.criteria
            if item.id in package.criterion_ids
        }
        role_instruction = {
            GoalActorRole.EXECUTOR: "Implement the bounded work and report evidence.",
            GoalActorRole.VERIFIER: (
                "Independently verify; run concrete checks and do not trust executor claims."
            ),
            GoalActorRole.CRITIC: (
                "Critique the strategy and submit a typed revision proposal."
            ),
            GoalActorRole.COORDINATOR: "Coordinate the bounded work graph.",
        }[package.role]
        return (
            f"Goal: {goal.objective}\n"
            f"Role: {package.role.value}. {role_instruction}\n"
            f"Work package: {package.objective}\n"
            f"Criteria: {json.dumps(criteria, sort_keys=True)}\n"
            f"{message}".strip()
        )

    @staticmethod
    def _summary(goal: Goal) -> str:
        counts: dict[str, int] = {}
        for package in goal.work_packages:
            counts[package.state.value] = counts.get(package.state.value, 0) + 1
        suffix = ", ".join(f"{key}={counts[key]}" for key in sorted(counts))
        if goal.supervision.drift_reasons:
            suffix += "; " + "; ".join(goal.supervision.drift_reasons)
        return suffix or "Awaiting authorized work proposals."
