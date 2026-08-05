"""Deterministic policy enforcement for advanced goal autonomy."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Callable
from datetime import UTC, datetime
from fnmatch import fnmatchcase
from typing import Any
from uuid import uuid4

from pa.domain.models import CardEvent, EventType
from pa.goals.advanced_models import (
    AllocationDisposition,
    GoalActionDecision,
    GoalActionDisposition,
    GoalActionRequest,
    GoalActionReservation,
    GoalActionRisk,
    GoalAutonomyState,
    GoalGovernancePolicy,
    GoalPortfolioEntry,
    GoalPortfolioReview,
    GoalPortfolioReviewRequest,
    GoalProposal,
    GoalProposalRequest,
    GoalProposalReview,
    GoalRateWindow,
    GoalReservationState,
    GoalResourceClaim,
    GoalStrategyPortfolioUpdate,
    GoalUsage,
    GovernanceMutationContext,
    ProposalDisposition,
    ProposalKind,
    ProviderGoalAssignment,
    ProviderGoalProgress,
    ProviderGoalRun,
    ProviderRunState,
    ResourceAccess,
)
from pa.goals.idempotency import operation_fingerprint, serialized_goal_mutation
from pa.goals.materialization import (
    GoalExecutionIdentityV1,
    GoalMaterializationEnvelopeV1,
    GoalMaterializationReceiptV1,
)
from pa.goals.models import (
    Goal,
    GoalCreate,
    GoalInteractionState,
    GoalMutationContext,
    GoalState,
)
from pa.goals.projection import (
    find_governance_event_by_idempotency,
    get_governance_payload,
    list_governance_events,
    list_governance_payloads,
)
from pa.goals.providers import get_goal_adapter
from pa.goals.service import GoalService

AUTONOMY_ENTITY = "goal_autonomy"
POLICY_ENTITY = "goal_governance_policy"
PROPOSAL_ENTITY = "goal_proposal"
REVIEW_ENTITY = "goal_portfolio_review"
CURRENT_REVIEW_ID = "current"
_RISK_RANK = {
    GoalActionRisk.LOW: 1,
    GoalActionRisk.MEDIUM: 2,
    GoalActionRisk.HIGH: 3,
    GoalActionRisk.CRITICAL: 4,
}
_TERMINAL_STATES = {GoalState.ACHIEVED, GoalState.ABANDONED}
_TERMINAL_PROVIDER_STATES = {
    ProviderRunState.COMPLETED,
    ProviderRunState.FAILED,
    ProviderRunState.CANCELLED,
}
_PROVIDER_TRANSITIONS = {
    ProviderRunState.ASSIGNED: {
        ProviderRunState.RUNNING,
        ProviderRunState.WAITING_OPERATOR,
        ProviderRunState.BLOCKED,
        ProviderRunState.COMPLETED,
        ProviderRunState.FAILED,
        ProviderRunState.CANCELLED,
    },
    ProviderRunState.RUNNING: {
        ProviderRunState.RUNNING,
        ProviderRunState.WAITING_OPERATOR,
        ProviderRunState.BLOCKED,
        ProviderRunState.COMPLETED,
        ProviderRunState.FAILED,
        ProviderRunState.CANCELLED,
    },
    ProviderRunState.WAITING_OPERATOR: {
        ProviderRunState.RUNNING,
        ProviderRunState.BLOCKED,
        ProviderRunState.COMPLETED,
        ProviderRunState.FAILED,
        ProviderRunState.CANCELLED,
    },
    ProviderRunState.BLOCKED: {
        ProviderRunState.FAILED,
        ProviderRunState.CANCELLED,
    },
}
_USAGE_METRICS = (
    "actions",
    "cost_usd",
    "tokens",
    "api_calls",
    "storage_mb",
    "dispatches",
)


class GoalGovernanceConflict(ValueError):
    pass


def _matches(value: str, patterns: list[str]) -> bool:
    return any(fnmatchcase(value, pattern) for pattern in patterns)


def _child_idempotency_key(parent: str, stage: str) -> str:
    digest = hashlib.sha256(parent.encode()).hexdigest()
    return f"goal-governance:{digest}:{stage}"


def _is_operator(principal: str) -> bool:
    return principal.startswith(("user:", "role:admin"))


def _budget_exceeded(current: GoalUsage, limit, metric: str) -> bool:
    maximum = getattr(limit, f"max_{metric}", None)
    return maximum is not None and getattr(current, metric) > maximum


def _replace_usage(
    total: GoalUsage, previous: GoalUsage, replacement: GoalUsage
) -> GoalUsage:
    return GoalUsage(
        **{
            metric: max(
                0,
                getattr(total, metric)
                - getattr(previous, metric)
                + getattr(replacement, metric),
            )
            for metric in _USAGE_METRICS
        }
    )


def _max_usage(first: GoalUsage, second: GoalUsage) -> GoalUsage:
    return GoalUsage(
        **{
            metric: max(getattr(first, metric), getattr(second, metric))
            for metric in _USAGE_METRICS
        }
    )


class GoalGovernanceService:
    def __init__(
        self,
        store,
        instance_id: str,
        goal_service: GoalService,
        *,
        clock: Callable[[], datetime] | None = None,
        progress_token_secret: str | None = None,
    ) -> None:
        self.store = store
        self.instance_id = instance_id
        self.goals = goal_service
        self._clock = clock or (lambda: datetime.now(UTC))
        self._progress_token_secret = progress_token_secret

    def provider_progress_credential(self, run: ProviderGoalRun) -> str:
        if not self._progress_token_secret:
            raise GoalGovernanceConflict(
                "provider progress credentials are unavailable on this authority"
            )
        message = "\0".join(
            (
                run.goal_id,
                run.id,
                run.executor_principal,
                run.authority_instance_id,
                str(run.fencing_token or ""),
            )
        )
        return hmac.new(
            self._progress_token_secret.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()

    def verify_provider_progress_credential(
        self, run: ProviderGoalRun, credential: str
    ) -> bool:
        if not credential or not run.progress_credential_hash:
            return False
        candidate = hashlib.sha256(credential.encode()).hexdigest()
        return hmac.compare_digest(candidate, run.progress_credential_hash)

    def get_state(self, goal_id: str) -> GoalAutonomyState:
        goal = self._require_goal(goal_id)
        payload = get_governance_payload(
            self.store, goal.realm_id, AUTONOMY_ENTITY, goal_id
        )
        state = (
            GoalAutonomyState.model_validate(payload)
            if payload
            else GoalAutonomyState(goal_id=goal.id, realm_id=goal.realm_id)
        )
        known_reservations = {item.id for item in state.action_reservations}
        for run in state.provider_runs:
            migrated_reserved_usage = run.reserved_usage.model_copy(deep=True)
            if (
                run.authority_instance_id == "legacy"
                and run.state not in _TERMINAL_PROVIDER_STATES
            ):
                previously_accounted = _max_usage(run.reserved_usage, run.usage)
                state.usage = _replace_usage(
                    state.usage, previously_accounted, run.usage
                )
                run.state = ProviderRunState.CANCELLED
                run.summary = (
                    f"{run.summary}; " if run.summary else ""
                ) + "cancelled during migration because the legacy run had no fence"
                run.reserved_usage = GoalUsage()
            if run.reservation_id in known_reservations:
                continue
            terminal = run.state in _TERMINAL_PROVIDER_STATES
            state.action_reservations.append(
                GoalActionReservation(
                    id=run.reservation_id,
                    idempotency_key=f"legacy-provider-run:{run.id}",
                    decision_id=f"legacy-provider-decision:{run.id}",
                    goal_id=goal.id,
                    action_class="provider.goal.assign",
                    actor_principal="service:goal-supervisor:legacy",
                    authority_instance_id=run.authority_instance_id,
                    policy_revision=run.invocation.policy_revision,
                    goal_version=goal.version,
                    fencing_token=run.fencing_token,
                    request=GoalActionRequest(
                        action_class="provider.goal.assign",
                        delegated=True,
                        provider_id=run.provider_id,
                        estimate=migrated_reserved_usage,
                    ),
                    reserved_usage=migrated_reserved_usage,
                    actual_usage=run.usage,
                    state=(
                        GoalReservationState.RELEASED
                        if terminal
                        else GoalReservationState.APPLIED
                    ),
                    created_at=run.created_at,
                    applied_at=run.created_at,
                    released_at=run.updated_at if terminal else None,
                    release_reason=(
                        f"migrated terminal provider run {run.state.value}"
                        if terminal
                        else ""
                    ),
                )
            )
            known_reservations.add(run.reservation_id)
        return state

    def state_events(self, goal_id: str) -> list[dict[str, Any]]:
        goal = self._require_goal(goal_id)
        return list_governance_events(
            self.store, goal.realm_id, AUTONOMY_ENTITY, goal_id
        )

    def get_policy(self, realm_id: str = "default") -> GoalGovernancePolicy | None:
        payload = get_governance_payload(
            self.store, realm_id, POLICY_ENTITY, "organization"
        )
        return GoalGovernancePolicy.model_validate(payload) if payload else None

    def effective_policy(self, realm_id: str = "default") -> GoalGovernancePolicy:
        return self.get_policy(realm_id) or GoalGovernancePolicy(realm_id=realm_id)

    @serialized_goal_mutation
    def put_policy(
        self, policy: GoalGovernancePolicy, context: GovernanceMutationContext
    ) -> GoalGovernancePolicy:
        policy = policy.model_copy(deep=True)
        fingerprint = operation_fingerprint(
            realm_id=policy.realm_id,
            entity_type=POLICY_ENTITY,
            entity_id="organization",
            event_type="goal_governance.policy_updated",
            operation=policy,
            context=context,
        )
        duplicate = self._duplicate(policy.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=POLICY_ENTITY,
                entity_id="organization",
                event_type="goal_governance.policy_updated",
                fingerprint=fingerprint,
            )
            current = self.get_policy(policy.realm_id)
            if current:
                return current
        current = self.get_policy(policy.realm_id)
        version = current.version if current else 0
        if context.expected_version != version:
            raise GoalGovernanceConflict(
                f"expected governance policy version {context.expected_version}, "
                f"current version {version}"
            )
        if policy.version != version + 1:
            raise GoalGovernanceConflict(
                "governance policy version must advance by one"
            )
        if context.policy_revision != policy.version:
            raise GoalGovernanceConflict(
                "governance mutation revision must match the new policy version"
            )
        if not _is_operator(context.actor_principal):
            raise GoalGovernanceConflict(
                "organization governance policies require an operator principal"
            )
        if policy.authored_by != context.actor_principal:
            raise GoalGovernanceConflict(
                "governance policy authored_by must match the mutation actor"
            )
        policy.updated_at = self._clock()
        self._commit_entity(
            policy.realm_id,
            POLICY_ENTITY,
            "organization",
            policy,
            "goal_governance.policy_updated",
            context,
            {"version": policy.version},
            operation_fingerprint_value=fingerprint,
        )
        return policy

    @serialized_goal_mutation
    def set_priority(
        self,
        goal_id: str,
        priority: int,
        reason: str,
        context: GovernanceMutationContext,
    ) -> GoalAutonomyState:
        if not 0 <= priority <= 100:
            raise GoalGovernanceConflict("goal priority must be between 0 and 100")

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            previous = state.priority
            state.priority = priority
            return {"from": previous, "to": priority, "reason": reason}

        return self._mutate_state(
            goal_id,
            context,
            "goal_governance.priority_changed",
            mutate,
            operation={"priority": priority, "reason": reason},
        )

    @serialized_goal_mutation
    def update_strategies(
        self,
        goal_id: str,
        update: GoalStrategyPortfolioUpdate,
        context: GovernanceMutationContext,
    ) -> GoalAutonomyState:
        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            allocated_cost = sum(item.allocated_cost_usd for item in update.strategies)
            allocated_tokens = sum(item.allocated_tokens for item in update.strategies)
            if (
                goal.budget.max_cost_usd is not None
                and allocated_cost > goal.budget.max_cost_usd
            ):
                raise GoalGovernanceConflict(
                    "strategy allocations exceed the goal cost budget"
                )
            if (
                goal.budget.max_tokens is not None
                and allocated_tokens > goal.budget.max_tokens
            ):
                raise GoalGovernanceConflict(
                    "strategy allocations exceed the goal token budget"
                )
            state.strategies = update.strategies
            state.selected_strategy_ids = update.selected_strategy_ids
            return {
                "strategy_ids": [item.id for item in update.strategies],
                "selected_strategy_ids": update.selected_strategy_ids,
                "reason": update.reason,
            }

        return self._mutate_state(
            goal_id,
            context,
            "goal_governance.strategies_updated",
            mutate,
            operation=update,
        )

    @serialized_goal_mutation
    def authorize_action(
        self,
        goal_id: str,
        request: GoalActionRequest,
        context: GovernanceMutationContext,
    ) -> tuple[GoalAutonomyState, GoalActionDecision]:
        goal = self._require_goal(goal_id)
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type="goal_governance.action_decided",
            operation=request,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type="goal_governance.action_decided",
                fingerprint=fingerprint,
            )
            state = self.get_state(goal_id)
            payload = duplicate.get("payload") or {}
            decision_id = str(payload.get("decision_id") or "")
            replayed = next(
                (item for item in state.recent_decisions if item.id == decision_id),
                None,
            )
            if replayed is None and isinstance(payload.get("decision"), dict):
                replayed = GoalActionDecision.model_validate(payload["decision"])
            if replayed is None:
                raise GoalGovernanceConflict(
                    "idempotent action decision is no longer in the bounded projection; consult the event ledger"
                )
            if (
                replayed.request != request
                or replayed.decided_by != context.actor_principal
                or replayed.authority_instance_id != context.authority_instance_id
                or replayed.fencing_token != context.fencing_token
            ):
                raise GoalGovernanceConflict(
                    "idempotent action replay no longer matches its exact reservation"
                )
            if replayed.disposition == GoalActionDisposition.AUTHORIZED:
                reservation = self._require_reservation(
                    state, replayed.reservation_id or ""
                )
                if (
                    reservation.request != request
                    or reservation.action_class != request.action_class
                    or reservation.actor_principal != context.actor_principal
                    or reservation.authority_instance_id
                    != context.authority_instance_id
                    or reservation.fencing_token != context.fencing_token
                ):
                    raise GoalGovernanceConflict(
                        "idempotent action replay no longer matches its exact reservation"
                    )
            return state, replayed
        decision: GoalActionDecision | None = None

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            nonlocal decision
            decision = self._evaluate_action(goal, state, request, context)
            state.recent_decisions = [*state.recent_decisions, decision][-200:]
            return {
                "decision_id": decision.id,
                "action_class": request.action_class,
                "disposition": decision.disposition.value,
                "reasons": decision.reasons,
                "decision": decision.model_dump(mode="json"),
            }

        state = self._mutate_state(
            goal_id,
            context,
            "goal_governance.action_decided",
            mutate,
            operation=request,
            operation_fingerprint_value=fingerprint,
        )
        assert decision is not None
        return state, decision

    @serialized_goal_mutation
    def apply_action(
        self,
        goal_id: str,
        reservation_id: str,
        context: GovernanceMutationContext,
        *,
        actual_usage: GoalUsage | None = None,
    ) -> tuple[GoalAutonomyState, GoalActionDecision]:
        """Revalidate a reservation immediately before its side effect is applied."""

        goal = self._require_goal(goal_id)
        apply_operation = {
            "reservation_id": reservation_id,
            "actual_usage": actual_usage,
        }
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type="goal_governance.action_applied",
            operation=apply_operation,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type="goal_governance.action_applied",
                fingerprint=fingerprint,
            )
            state = self.get_state(goal_id)
            payload = duplicate.get("payload") or {}
            decision_id = str(payload.get("decision_id") or "")
            replayed = next(
                (item for item in state.recent_decisions if item.id == decision_id),
                None,
            )
            if replayed is None and isinstance(payload.get("decision"), dict):
                replayed = GoalActionDecision.model_validate(payload["decision"])
            if replayed is None:
                raise GoalGovernanceConflict(
                    "idempotent apply decision is no longer in the bounded projection; consult the event ledger"
                )
            reservation = self._require_reservation(state, reservation_id)
            self._validate_reservation(
                goal,
                reservation,
                context,
                allow_stale_policy=True,
            )
            if reservation.state != GoalReservationState.APPLIED:
                raise GoalGovernanceConflict(
                    "idempotent apply replay requires its reservation to remain applied"
                )
            if (
                replayed.disposition != GoalActionDisposition.AUTHORIZED
                or replayed.reservation_id != reservation.id
                or replayed.action_class != reservation.action_class
                or replayed.request != reservation.request
                or replayed.decided_by != reservation.actor_principal
                or replayed.authority_instance_id != reservation.authority_instance_id
                or replayed.fencing_token != reservation.fencing_token
            ):
                raise GoalGovernanceConflict(
                    "idempotent apply replay no longer matches its exact reservation"
                )
            return state, replayed
        decision: GoalActionDecision | None = None

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            nonlocal decision
            reservation = self._require_reservation(state, reservation_id)
            self._validate_reservation(
                goal, reservation, context, allow_stale_policy=True
            )
            if reservation.state == GoalReservationState.RELEASED:
                raise GoalGovernanceConflict("action reservation is already released")
            if reservation.state == GoalReservationState.APPLIED:
                decision = next(
                    (
                        item
                        for item in reversed(state.recent_decisions)
                        if item.reservation_id == reservation.id
                    ),
                    None,
                )
                if decision is None:
                    raise GoalGovernanceConflict(
                        "applied reservation is missing its durable decision"
                    )
                return {
                    "reservation_id": reservation.id,
                    "disposition": decision.disposition.value,
                    "replayed": True,
                }

            available = state.model_copy(deep=True)
            previously_managed_claims = self._active_resource_claims(available)
            available.usage = _replace_usage(
                available.usage, reservation.reserved_usage, GoalUsage()
            )
            self._replace_rate_usage(
                goal,
                available,
                reservation.request,
                reservation.reserved_usage,
                GoalUsage(),
            )
            available.action_reservations = [
                item
                for item in available.action_reservations
                if item.id != reservation.id
            ]
            self._refresh_resource_reservations(
                available,
                previously_managed_claims=previously_managed_claims,
            )
            decision = self._evaluate_action(
                goal,
                available,
                reservation.request,
                context,
                reserve=False,
                exclude_reservation_id=reservation.id,
            )
            decision.reservation_id = reservation.id
            if reservation.goal_version != goal.version:
                decision.disposition = GoalActionDisposition.DENIED
                decision.reasons = [
                    "the action reservation targets a stale goal version",
                    *decision.reasons,
                ]
            state.recent_decisions = [*state.recent_decisions, decision][-200:]
            if decision.disposition != GoalActionDisposition.AUTHORIZED:
                self._release_reservation(
                    goal,
                    state,
                    reservation,
                    actual_usage=GoalUsage(),
                    reason=f"apply-time {decision.disposition.value}",
                )
            else:
                reservation.state = GoalReservationState.APPLIED
                reservation.applied_at = self._clock()
                if actual_usage is not None:
                    state.usage = _replace_usage(
                        state.usage,
                        reservation.reserved_usage,
                        actual_usage,
                    )
                    self._replace_rate_usage(
                        goal,
                        state,
                        reservation.request,
                        reservation.reserved_usage,
                        actual_usage,
                    )
                    reservation.actual_usage = actual_usage
            return {
                "reservation_id": reservation.id,
                "decision_id": decision.id,
                "disposition": decision.disposition.value,
                "reasons": decision.reasons,
                "decision": decision.model_dump(mode="json"),
            }

        state = self._mutate_state(
            goal_id,
            context,
            "goal_governance.action_applied",
            mutate,
            operation=apply_operation,
            operation_fingerprint_value=fingerprint,
        )
        assert decision is not None
        return state, decision

    @serialized_goal_mutation
    def release_action(
        self,
        goal_id: str,
        reservation_id: str,
        context: GovernanceMutationContext,
        *,
        actual_usage: GoalUsage | None = None,
        reason: str,
        reconcile_terminal: bool = False,
    ) -> GoalAutonomyState:
        """Release budget and resource holds on every terminal action path."""

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            reservation = self._require_reservation(state, reservation_id)
            self._validate_reservation(
                goal,
                reservation,
                context,
                allow_stale_policy=True,
                allow_recovery=True,
            )
            if reservation.state != GoalReservationState.RELEASED:
                resolved_usage = actual_usage
                if resolved_usage is None:
                    if any(
                        getattr(reservation.actual_usage, metric)
                        for metric in _USAGE_METRICS
                    ):
                        resolved_usage = reservation.actual_usage
                    elif reservation.state == GoalReservationState.APPLIED:
                        resolved_usage = reservation.reserved_usage
                    else:
                        resolved_usage = GoalUsage()
                self._release_reservation(
                    goal,
                    state,
                    reservation,
                    actual_usage=resolved_usage,
                    reason=reason,
                )
            elif reconcile_terminal and actual_usage is not None:
                state.usage = _replace_usage(
                    state.usage, reservation.actual_usage, actual_usage
                )
                self._replace_rate_usage(
                    goal,
                    state,
                    reservation.request,
                    reservation.actual_usage,
                    actual_usage,
                )
                reservation.actual_usage = actual_usage
                reservation.release_reason = reason[:500]
                reservation.released_at = self._clock()
            return {
                "reservation_id": reservation.id,
                "state": reservation.state.value,
                "reason": reservation.release_reason,
                "actual_usage": reservation.actual_usage.model_dump(mode="json"),
            }

        return self._mutate_state(
            goal_id,
            context,
            "goal_governance.action_released",
            mutate,
            operation={
                "reservation_id": reservation_id,
                "actual_usage": actual_usage,
                "reason": reason,
                "reconcile_terminal": reconcile_terminal,
            },
        )

    @serialized_goal_mutation
    def bind_dispatch_placement(
        self,
        goal_id: str,
        reservation_id: str,
        context: GovernanceMutationContext,
        *,
        requested_placement_target: str,
        placement_input_digest: str,
        resolved_target_instance_id: str,
        placement_decision_digest: str,
    ) -> tuple[GoalAutonomyState, GoalActionReservation]:
        """Bind one applied dispatch hold to the immutable placement result."""

        def exact_binding(reservation: GoalActionReservation) -> bool:
            request = reservation.request
            return bool(
                request.requested_placement_target == requested_placement_target
                and request.placement_input_digest == placement_input_digest
                and request.resolved_target_instance_id == resolved_target_instance_id
                and request.placement_decision_digest == placement_decision_digest
            )

        goal = self._require_goal(goal_id)
        operation = {
            "reservation_id": reservation_id,
            "requested_placement_target": requested_placement_target,
            "placement_input_digest": placement_input_digest,
            "resolved_target_instance_id": resolved_target_instance_id,
            "placement_decision_digest": placement_decision_digest,
        }
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type="goal_governance.dispatch_placement_bound",
            operation=operation,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type="goal_governance.dispatch_placement_bound",
                fingerprint=fingerprint,
            )
            state = self.get_state(goal_id)
            reservation = self._require_reservation(state, reservation_id)
            if not exact_binding(reservation):
                raise GoalGovernanceConflict(
                    "idempotent placement binding no longer matches its reservation"
                )
            return state, reservation

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            reservation = self._require_reservation(state, reservation_id)
            self._validate_reservation(goal, reservation, context)
            if reservation.state != GoalReservationState.APPLIED:
                raise GoalGovernanceConflict(
                    "placement binding requires an applied action reservation"
                )
            request = reservation.request
            if request.action_class != "dispatch_work_package":
                raise GoalGovernanceConflict(
                    "placement binding requires a dispatch-work-package reservation"
                )
            if request.requested_placement_target != requested_placement_target:
                raise GoalGovernanceConflict(
                    "dispatch reservation does not bind this placement request"
                )
            if request.placement_input_digest != placement_input_digest:
                raise GoalGovernanceConflict(
                    "dispatch reservation does not bind these placement inputs"
                )
            if requested_placement_target.startswith("placement:"):
                requested_policy = requested_placement_target.partition(":")[2]
                if not requested_policy:
                    raise GoalGovernanceConflict(
                        "dispatch reservation has an invalid placement policy binding"
                    )
            elif requested_placement_target != resolved_target_instance_id:
                raise GoalGovernanceConflict(
                    "named dispatch resolved to a different target instance"
                )
            existing = (
                request.resolved_target_instance_id,
                request.placement_decision_digest,
            )
            if existing != (None, None) and existing != (
                resolved_target_instance_id,
                placement_decision_digest,
            ):
                raise GoalGovernanceConflict(
                    "dispatch placement is already bound to another result"
                )
            request.resolved_target_instance_id = resolved_target_instance_id
            request.placement_decision_digest = placement_decision_digest
            return {
                "reservation_id": reservation.id,
                "requested_placement_target": requested_placement_target,
                "placement_input_digest": placement_input_digest,
                "resolved_target_instance_id": resolved_target_instance_id,
                "placement_decision_digest": placement_decision_digest,
            }

        state = self._mutate_state(
            goal_id,
            context,
            "goal_governance.dispatch_placement_bound",
            mutate,
            operation=operation,
            operation_fingerprint_value=fingerprint,
        )
        reservation = self._require_reservation(state, reservation_id)
        if not exact_binding(reservation):
            raise GoalGovernanceConflict(
                "durable placement binding does not match the requested result"
            )
        return state, reservation

    @serialized_goal_mutation
    def bind_dispatch_materialization(
        self,
        goal_id: str,
        reservation_id: str,
        context: GovernanceMutationContext,
        *,
        envelope: GoalMaterializationEnvelopeV1,
        receipt: GoalMaterializationReceiptV1,
    ) -> tuple[GoalAutonomyState, GoalActionReservation]:
        """Bind the exact target plan without mutating the reserved envelope."""

        def exact_binding(reservation: GoalActionReservation) -> bool:
            return bool(
                reservation.request.materialization_envelope == envelope
                and reservation.request.materialization_receipt == receipt
                and receipt.envelope_digest == envelope.digest
            )

        goal = self._require_goal(goal_id)
        operation = {
            "reservation_id": reservation_id,
            "envelope": envelope,
            "receipt": receipt,
        }
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type="goal_governance.dispatch_materialization_bound",
            operation=operation,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type="goal_governance.dispatch_materialization_bound",
                fingerprint=fingerprint,
            )
            state = self.get_state(goal_id)
            reservation = self._require_reservation(state, reservation_id)
            if not exact_binding(reservation):
                raise GoalGovernanceConflict(
                    "idempotent materialization binding no longer matches "
                    "its reservation"
                )
            return state, reservation

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            reservation = self._require_reservation(state, reservation_id)
            self._validate_reservation(goal, reservation, context)
            if reservation.state != GoalReservationState.APPLIED:
                raise GoalGovernanceConflict(
                    "materialization binding requires an applied action reservation"
                )
            if reservation.request.materialization_envelope != envelope:
                raise GoalGovernanceConflict(
                    "dispatch reservation does not bind this materialization envelope"
                )
            if receipt.envelope_digest != envelope.digest:
                raise GoalGovernanceConflict(
                    "materialization receipt belongs to another envelope"
                )
            if (
                receipt.target_instance_id
                != reservation.request.resolved_target_instance_id
                or receipt.provider_id.strip().lower()
                != str(reservation.request.provider_id or "").strip().lower()
            ):
                raise GoalGovernanceConflict(
                    "materialization receipt does not match the reserved execution"
                )
            existing = reservation.request.materialization_receipt
            if existing is not None and existing != receipt:
                raise GoalGovernanceConflict(
                    "dispatch materialization is already bound to another result"
                )
            reservation.request.materialization_receipt = receipt
            return {
                "reservation_id": reservation.id,
                "envelope_digest": envelope.digest,
                "receipt_digest": receipt.digest,
                "target_instance_id": receipt.target_instance_id,
                "provider_id": receipt.provider_id,
            }

        state = self._mutate_state(
            goal_id,
            context,
            "goal_governance.dispatch_materialization_bound",
            mutate,
            operation=operation,
            operation_fingerprint_value=fingerprint,
        )
        reservation = self._require_reservation(state, reservation_id)
        if not exact_binding(reservation):
            raise GoalGovernanceConflict(
                "durable materialization binding does not match the requested result"
            )
        return state, reservation

    @serialized_goal_mutation
    def bind_dispatch_execution_identity(
        self,
        goal_id: str,
        reservation_id: str,
        context: GovernanceMutationContext,
        *,
        identity: GoalExecutionIdentityV1,
    ) -> tuple[GoalAutonomyState, GoalActionReservation]:
        """Bind the allocated principal/session identity without credential material."""

        def exact_binding(reservation: GoalActionReservation) -> bool:
            return reservation.request.execution_identity == identity

        goal = self._require_goal(goal_id)
        operation = {
            "reservation_id": reservation_id,
            "identity": identity,
        }
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type="goal_governance.dispatch_execution_identity_bound",
            operation=operation,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type="goal_governance.dispatch_execution_identity_bound",
                fingerprint=fingerprint,
            )
            state = self.get_state(goal_id)
            reservation = self._require_reservation(state, reservation_id)
            if not exact_binding(reservation):
                raise GoalGovernanceConflict(
                    "idempotent execution-identity binding no longer matches"
                )
            return state, reservation

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            reservation = self._require_reservation(state, reservation_id)
            self._validate_reservation(goal, reservation, context)
            if reservation.state != GoalReservationState.APPLIED:
                raise GoalGovernanceConflict(
                    "execution identity requires an applied action reservation"
                )
            receipt = reservation.request.materialization_receipt
            envelope = reservation.request.materialization_envelope
            if (
                receipt is None
                or identity.materialization_receipt_digest != receipt.digest
            ):
                raise GoalGovernanceConflict(
                    "execution identity belongs to another materialization receipt"
                )
            if (
                envelope is None
                or identity.work_package_id != envelope.work_package_id
                or identity.service_role != envelope.service_role
            ):
                raise GoalGovernanceConflict(
                    "execution identity does not match the reserved work-package role"
                )
            if (
                identity.provider_id.strip().lower()
                != str(reservation.request.provider_id or "").strip().lower()
                or identity.target_instance_id
                != reservation.request.resolved_target_instance_id
                or identity.fencing_token != reservation.fencing_token
            ):
                raise GoalGovernanceConflict(
                    "execution identity does not match the reserved execution"
                )
            existing = reservation.request.execution_identity
            if existing is not None and existing != identity:
                raise GoalGovernanceConflict(
                    "dispatch execution identity is already bound to another session"
                )
            reservation.request.execution_identity = identity
            return {
                "reservation_id": reservation.id,
                "execution_identity_digest": identity.digest,
                "provider_id": identity.provider_id,
                "target_instance_id": identity.target_instance_id,
                "session_id": identity.session_id,
                "credential_bound": identity.credential_authenticated(),
            }

        state = self._mutate_state(
            goal_id,
            context,
            "goal_governance.dispatch_execution_identity_bound",
            mutate,
            operation=operation,
            operation_fingerprint_value=fingerprint,
        )
        reservation = self._require_reservation(state, reservation_id)
        if not exact_binding(reservation):
            raise GoalGovernanceConflict(
                "durable execution identity does not match the requested binding"
            )
        return state, reservation

    @serialized_goal_mutation
    def revalidate_action_sink(
        self,
        goal_id: str,
        reservation_id: str,
        context: GovernanceMutationContext,
        *,
        action_class: str,
        provider_id: str,
        requested_placement_target: str,
        placement_input_digest: str,
        resolved_target_instance_id: str,
        placement_decision_digest: str,
        materialization_envelope: GoalMaterializationEnvelopeV1,
        materialization_receipt: GoalMaterializationReceiptV1,
        execution_identity: GoalExecutionIdentityV1 | None = None,
        denial_actual_usage: GoalUsage | None = None,
    ) -> tuple[GoalAutonomyState, GoalActionReservation]:
        """Durably renew an applied hold against the current fenced goal envelope."""

        provider_id = provider_id.strip().lower()
        if not provider_id:
            raise GoalGovernanceConflict(
                "dispatch side effects require a concrete provider"
            )
        goal = self._require_goal(goal_id)
        operation = {
            "reservation_id": reservation_id,
            "action_class": action_class,
            "provider_id": provider_id,
            "requested_placement_target": requested_placement_target,
            "placement_input_digest": placement_input_digest,
            "resolved_target_instance_id": resolved_target_instance_id,
            "placement_decision_digest": placement_decision_digest,
            "denial_actual_usage": denial_actual_usage,
        }
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type="goal_governance.action_sink_revalidated",
            operation=operation,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type="goal_governance.action_sink_revalidated",
                fingerprint=fingerprint,
            )
            state = self.get_state(goal_id)
            reservation = self._require_reservation(state, reservation_id)
            if (
                reservation.state != GoalReservationState.APPLIED
                or reservation.action_class != action_class
                or reservation.actor_principal != context.actor_principal
                or reservation.authority_instance_id != context.authority_instance_id
                or reservation.fencing_token != context.fencing_token
                or (reservation.request.provider_id or "").strip().lower()
                != provider_id
                or reservation.request.requested_placement_target
                != requested_placement_target
                or reservation.request.placement_input_digest != placement_input_digest
                or reservation.request.resolved_target_instance_id
                != resolved_target_instance_id
                or reservation.request.placement_decision_digest
                != placement_decision_digest
                or reservation.request.materialization_envelope
                != materialization_envelope
                or reservation.request.materialization_receipt
                != materialization_receipt
                or reservation.request.execution_identity != execution_identity
            ):
                raise GoalGovernanceConflict(
                    "idempotent dispatch sink validation no longer matches its reservation"
                )
            return state, reservation
        decision: GoalActionDecision | None = None

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            nonlocal decision
            reservation = self._require_reservation(state, reservation_id)
            if reservation.state != GoalReservationState.APPLIED:
                raise GoalGovernanceConflict(
                    "dispatch side effects require an applied action reservation"
                )
            if reservation.action_class != action_class:
                raise GoalGovernanceConflict(
                    "action reservation does not authorize this side-effect class"
                )
            if reservation.authority_instance_id != context.authority_instance_id:
                raise GoalGovernanceConflict(
                    "action reservation belongs to another authority instance"
                )
            if reservation.actor_principal != context.actor_principal:
                raise GoalGovernanceConflict(
                    "action reservation belongs to another authenticated actor"
                )
            if reservation.fencing_token != context.fencing_token:
                raise GoalGovernanceConflict(
                    "action reservation belongs to a stale controller fence"
                )
            reserved_provider = (reservation.request.provider_id or "").strip().lower()
            if not reserved_provider or reserved_provider != provider_id:
                raise GoalGovernanceConflict(
                    "action reservation does not bind this execution provider"
                )
            if (
                reservation.request.requested_placement_target
                != requested_placement_target
                or reservation.request.placement_input_digest != placement_input_digest
                or reservation.request.resolved_target_instance_id
                != resolved_target_instance_id
                or reservation.request.placement_decision_digest
                != placement_decision_digest
                or reservation.request.materialization_envelope
                != materialization_envelope
                or reservation.request.materialization_receipt
                != materialization_receipt
                or reservation.request.execution_identity != execution_identity
            ):
                raise GoalGovernanceConflict(
                    "action reservation does not bind this placement result"
                )

            available = self._state_without_reservation(goal, state, reservation)
            decision = self._evaluate_action(
                goal,
                available,
                reservation.request,
                context,
                reserve=False,
                exclude_reservation_id=reservation.id,
            )
            decision.reservation_id = reservation.id
            state.recent_decisions = [*state.recent_decisions, decision][-200:]
            if decision.disposition != GoalActionDisposition.AUTHORIZED:
                self._release_reservation(
                    goal,
                    state,
                    reservation,
                    actual_usage=denial_actual_usage or GoalUsage(),
                    reason=f"side-effect revalidation {decision.disposition.value}",
                )
            else:
                renewed = (
                    reservation.goal_version != goal.version
                    or reservation.policy_revision != goal.policy.revision
                )
                reservation.goal_version = goal.version
                reservation.policy_revision = goal.policy.revision
                if renewed:
                    reservation.renewal_count += 1
                    reservation.renewed_at = self._clock()
            return {
                "reservation_id": reservation.id,
                "decision_id": decision.id,
                "disposition": decision.disposition.value,
                "goal_version": reservation.goal_version,
                "policy_revision": reservation.policy_revision,
                "fencing_token": reservation.fencing_token,
                "renewal_count": reservation.renewal_count,
                "provider_id": provider_id,
                "resolved_target_instance_id": resolved_target_instance_id,
                "placement_decision_digest": placement_decision_digest,
            }

        state = self._mutate_state(
            goal_id,
            context,
            "goal_governance.action_sink_revalidated",
            mutate,
            operation=operation,
            operation_fingerprint_value=fingerprint,
        )
        reservation = self._require_reservation(state, reservation_id)
        assert decision is not None
        if decision.disposition != GoalActionDisposition.AUTHORIZED:
            raise GoalGovernanceConflict(
                "canonical governance denied the dispatch side effect: "
                + "; ".join(decision.reasons)
            )
        return state, reservation

    @serialized_goal_mutation
    def replace_action_reservation(
        self,
        goal_id: str,
        reservation_id: str,
        context: GovernanceMutationContext,
    ) -> tuple[GoalAutonomyState, GoalActionReservation | None, GoalActionDecision]:
        """Create one bounded fresh hold for a retry of the same durable action."""

        goal = self._require_goal(goal_id)
        operation = {"reservation_id": reservation_id}
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type="goal_governance.action_reservation_replaced",
            operation=operation,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type="goal_governance.action_reservation_replaced",
                fingerprint=fingerprint,
            )
            state = self.get_state(goal_id)
            payload = duplicate.get("payload") or {}
            replacement_id = str(payload.get("replacement_reservation_id") or "")
            decision_id = str(payload.get("decision_id") or "")
            replacement = next(
                (
                    item
                    for item in state.action_reservations
                    if item.id == replacement_id
                ),
                None,
            )
            serialized = payload.get("decision")
            decision = (
                GoalActionDecision.model_validate(serialized)
                if isinstance(serialized, dict)
                else next(
                    (item for item in state.recent_decisions if item.id == decision_id),
                    None,
                )
            )
            if decision is None:
                raise GoalGovernanceConflict(
                    "idempotent retry decision is no longer in the bounded projection"
                )
            return state, replacement, decision

        replacement: GoalActionReservation | None = None
        decision: GoalActionDecision | None = None

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            nonlocal replacement, decision
            previous = self._require_reservation(state, reservation_id)
            if previous.authority_instance_id != context.authority_instance_id:
                raise GoalGovernanceConflict(
                    "retry reservation belongs to another authority instance"
                )
            if previous.actor_principal != context.actor_principal:
                raise GoalGovernanceConflict(
                    "retry reservation belongs to another authenticated actor"
                )
            existing = next(
                (
                    item
                    for item in state.action_reservations
                    if item.replaces_reservation_id == previous.id
                ),
                None,
            )
            if existing is not None:
                replacement = existing
                decision = next(
                    (
                        item
                        for item in state.recent_decisions
                        if item.id == existing.decision_id
                    ),
                    None,
                )
                if decision is None:
                    raise GoalGovernanceConflict(
                        "replacement reservation is missing its durable decision"
                    )
                return {
                    "decision_id": decision.id,
                    "replacement_reservation_id": existing.id,
                    "decision": decision.model_dump(mode="json"),
                    "replayed": True,
                }
            if previous.attempt >= previous.max_attempts:
                raise GoalGovernanceConflict("action retry limit is exhausted")
            if previous.state != GoalReservationState.RELEASED:
                self._release_reservation(
                    goal,
                    state,
                    previous,
                    actual_usage=GoalUsage(),
                    reason="superseded by a bounded retry",
                )
            decision = self._evaluate_action(goal, state, previous.request, context)
            state.recent_decisions = [*state.recent_decisions, decision][-200:]
            if decision.disposition == GoalActionDisposition.AUTHORIZED:
                replacement = self._require_reservation(
                    state, decision.reservation_id or ""
                )
                replacement.attempt = previous.attempt + 1
                replacement.max_attempts = previous.max_attempts
                replacement.replaces_reservation_id = previous.id
            return {
                "decision_id": decision.id,
                "disposition": decision.disposition.value,
                "reasons": decision.reasons,
                "previous_reservation_id": previous.id,
                "replacement_reservation_id": (
                    replacement.id if replacement is not None else None
                ),
                "decision": decision.model_dump(mode="json"),
            }

        state = self._mutate_state(
            goal_id,
            context,
            "goal_governance.action_reservation_replaced",
            mutate,
            operation=operation,
            operation_fingerprint_value=fingerprint,
        )
        assert decision is not None
        return state, replacement, decision

    @serialized_goal_mutation
    def reconcile_action_release(
        self,
        goal_id: str,
        reservation_id: str,
        context: GovernanceMutationContext,
        *,
        actual_usage: GoalUsage,
        reason: str,
    ) -> GoalAutonomyState:
        """Release an exact dispatch hold even after its controller lease ended."""

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            reservation = self._require_reservation(state, reservation_id)
            lifecycle_principal = (
                f"service:goal-dispatch-lifecycle:{reservation.authority_instance_id}"
            )
            if context.actor_principal not in {
                reservation.actor_principal,
                lifecycle_principal,
            } and not _is_operator(context.actor_principal):
                raise GoalGovernanceConflict(
                    "dispatch release actor is not authorized for this reservation"
                )
            if (
                not _is_operator(context.actor_principal)
                and context.authority_instance_id != reservation.authority_instance_id
            ):
                raise GoalGovernanceConflict(
                    "dispatch release came from another authority instance"
                )
            if (
                not _is_operator(context.actor_principal)
                and context.fencing_token != reservation.fencing_token
            ):
                raise GoalGovernanceConflict(
                    "dispatch release has the wrong reservation fencing token"
                )
            if reservation.state != GoalReservationState.RELEASED:
                self._release_reservation(
                    goal,
                    state,
                    reservation,
                    actual_usage=actual_usage,
                    reason=reason,
                )
            return {
                "reservation_id": reservation.id,
                "state": reservation.state.value,
                "reason": reservation.release_reason,
                "actual_usage": reservation.actual_usage.model_dump(mode="json"),
                "terminal_reconciliation": True,
            }

        return self._mutate_state(
            goal_id,
            context,
            "goal_governance.action_release_reconciled",
            mutate,
            operation={
                "reservation_id": reservation_id,
                "actual_usage": actual_usage,
                "reason": reason,
            },
            validate_goal_context=False,
        )

    @serialized_goal_mutation
    def assign_provider(
        self,
        goal_id: str,
        assignment: ProviderGoalAssignment,
        context: GovernanceMutationContext,
    ) -> tuple[GoalAutonomyState, ProviderGoalRun | None, GoalActionDecision]:
        goal = self._require_goal(goal_id)
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type="goal_governance.provider_assigned",
            operation=assignment,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type="goal_governance.provider_assigned",
                fingerprint=fingerprint,
            )
            state = self.get_state(goal_id)
            payload = duplicate.get("payload") or {}
            decision_id = str(payload.get("decision_id") or "")
            run_id = str(payload.get("run_id") or "")
            replayed_decision = next(
                (item for item in state.recent_decisions if item.id == decision_id),
                None,
            )
            replayed_run = next(
                (item for item in state.provider_runs if item.id == run_id), None
            )
            if replayed_decision is None and isinstance(payload.get("decision"), dict):
                replayed_decision = GoalActionDecision.model_validate(
                    payload["decision"]
                )
            if replayed_decision is None:
                raise GoalGovernanceConflict(
                    "idempotent provider decision is no longer in the bounded projection; consult the event ledger"
                )
            return state, replayed_run, replayed_decision
        run: ProviderGoalRun | None = None
        decision: GoalActionDecision | None = None

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            nonlocal run, decision
            if not goal.lease.active(self._clock()):
                raise GoalGovernanceConflict(
                    "provider assignment requires an active controller lease"
                )
            previous = None
            attempt = 1
            effective_max_attempts = min(
                assignment.max_attempts, goal.budget.retry_limit + 1
            )
            if assignment.replaces_run_id:
                previous = next(
                    (
                        item
                        for item in state.provider_runs
                        if item.id == assignment.replaces_run_id
                    ),
                    None,
                )
                if previous is None:
                    raise GoalGovernanceConflict(
                        "provider retry references an unknown prior run"
                    )
                if previous.state not in _TERMINAL_PROVIDER_STATES:
                    raise GoalGovernanceConflict(
                        "provider retry requires a terminal prior run"
                    )
                if previous.role != assignment.role:
                    raise GoalGovernanceConflict(
                        "provider retry cannot change executor/verifier role"
                    )
                if (
                    previous.materialization_envelope
                    != assignment.materialization_envelope
                ):
                    raise GoalGovernanceConflict(
                        "provider retry cannot widen its materialization envelope"
                    )
                if any(
                    item.replaces_run_id == previous.id for item in state.provider_runs
                ):
                    raise GoalGovernanceConflict(
                        "provider run already has a durable replacement"
                    )
                attempt = previous.attempt + 1
                effective_max_attempts = min(
                    previous.max_attempts, effective_max_attempts
                )
                if attempt > effective_max_attempts:
                    raise GoalGovernanceConflict("provider retry limit is exhausted")
            request = GoalActionRequest(
                action_class="provider.goal.assign",
                risk=GoalActionRisk.LOW,
                delegated=True,
                provider_id=assignment.provider_id,
                estimate=assignment.estimated_usage,
                materialization_envelope=assignment.materialization_envelope,
                resource_claims=(
                    [
                        GoalResourceClaim(
                            key=item.key,
                            access=ResourceAccess(item.access),
                            quantity=item.quantity,
                            preemptible=item.preemptible,
                            expires_at=item.expires_at,
                        )
                        for item in assignment.materialization_envelope.resource_claims
                    ]
                    if assignment.materialization_envelope is not None
                    else []
                ),
            )
            decision = self._evaluate_action(goal, state, request, context)
            state.recent_decisions = [*state.recent_decisions, decision][-200:]
            if decision.disposition == GoalActionDisposition.AUTHORIZED:
                adapter = get_goal_adapter(assignment.provider_id)
                invocation = adapter.prepare(goal, assignment)
                run_id = str(uuid4())
                service_role = assignment.role.value
                run = ProviderGoalRun(
                    id=run_id,
                    goal_id=goal.id,
                    provider_id=assignment.provider_id,
                    invocation=invocation,
                    strategy_id=assignment.strategy_id,
                    role=assignment.role,
                    executor_principal=(
                        f"service:goal-{service_role}:{assignment.provider_id}:{run_id}"
                    ),
                    authority_instance_id=context.authority_instance_id,
                    fencing_token=context.fencing_token,
                    reservation_id=decision.reservation_id or "",
                    materialization_envelope=assignment.materialization_envelope,
                    attempt=attempt,
                    max_attempts=effective_max_attempts,
                    replaces_run_id=previous.id if previous else None,
                    reserved_usage=assignment.estimated_usage,
                )
                state.provider_runs.append(run)
            return {
                "decision_id": decision.id,
                "disposition": decision.disposition.value,
                "run_id": run.id if run else None,
                "provider_id": assignment.provider_id,
                "role": assignment.role.value,
                "attempt": attempt,
                "mode": run.invocation.mode.value if run else None,
                "decision": decision.model_dump(mode="json"),
            }

        state = self._mutate_state(
            goal_id,
            context,
            "goal_governance.provider_assigned",
            mutate,
            operation=assignment,
            operation_fingerprint_value=fingerprint,
        )
        assert decision is not None
        return state, run, decision

    @serialized_goal_mutation
    def launch_provider(
        self,
        goal_id: str,
        run_id: str,
        context: GovernanceMutationContext,
    ) -> tuple[GoalAutonomyState, ProviderGoalRun, GoalActionDecision]:
        """Apply the run reservation at the final sink before exposing invocation."""

        goal = self._require_goal(goal_id)
        launch_operation = {"run_id": run_id}
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type="goal_governance.provider_launched",
            operation=launch_operation,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type="goal_governance.provider_launched",
                fingerprint=fingerprint,
            )
            state = self.get_state(goal_id)
            run = next(
                (item for item in state.provider_runs if item.id == run_id), None
            )
            if run is None or run.launched_at is None:
                raise GoalGovernanceConflict(
                    "idempotent provider launch is missing its durable run"
                )
            if (
                duplicate.get("actor_principal") != context.actor_principal
                or duplicate.get("authority_instance_id")
                != context.authority_instance_id
                or run.authority_instance_id != context.authority_instance_id
                or run.fencing_token != context.fencing_token
            ):
                raise GoalGovernanceConflict(
                    "idempotent provider launch belongs to another actor or authority"
                )
            payload = duplicate.get("payload") or {}
            decision_id = str(
                payload.get("decision_id") or run.launch_decision_id or ""
            )
            decision = next(
                (item for item in state.recent_decisions if item.id == decision_id),
                None,
            )
            if decision is None and isinstance(payload.get("decision"), dict):
                decision = GoalActionDecision.model_validate(payload["decision"])
            if decision is None:
                raise GoalGovernanceConflict(
                    "idempotent provider launch is missing its durable decision"
                )
            return state, run, decision

        state = self.get_state(goal_id)
        run = next((item for item in state.provider_runs if item.id == run_id), None)
        if run is None:
            raise GoalGovernanceConflict("provider launch references an unknown run")
        if run.state != ProviderRunState.ASSIGNED or run.launched_at is not None:
            raise GoalGovernanceConflict("provider run is not awaiting launch")
        state, decision = self.apply_action(
            goal_id,
            run.reservation_id,
            context.model_copy(
                update={
                    "idempotency_key": _child_idempotency_key(
                        context.idempotency_key, "provider-apply"
                    )
                }
            ),
        )
        if decision.disposition != GoalActionDisposition.AUTHORIZED:
            return state, run, decision

        launched: ProviderGoalRun | None = None

        def mutate(goal: Goal, current: GoalAutonomyState) -> dict[str, Any]:
            nonlocal launched
            launched = next(
                (item for item in current.provider_runs if item.id == run_id), None
            )
            if launched is None:
                raise GoalGovernanceConflict("provider run disappeared before launch")
            reservation = self._require_reservation(current, launched.reservation_id)
            if reservation.state != GoalReservationState.APPLIED:
                raise GoalGovernanceConflict(
                    "provider launch requires an applied action reservation"
                )
            launched.launch_decision_id = decision.id
            launched.launched_at = self._clock()
            if self._progress_token_secret:
                credential = self.provider_progress_credential(launched)
                launched.progress_credential_hash = hashlib.sha256(
                    credential.encode()
                ).hexdigest()
            launched.updated_at = self._clock()
            return {
                "run_id": launched.id,
                "decision_id": decision.id,
                "reservation_id": reservation.id,
                "decision": decision.model_dump(mode="json"),
            }

        state = self._mutate_state(
            goal_id,
            context.model_copy(update={"expected_version": state.version}),
            "goal_governance.provider_launched",
            mutate,
            operation=launch_operation,
            operation_fingerprint_value=fingerprint,
        )
        assert launched is not None
        return state, launched, decision

    @serialized_goal_mutation
    def ingest_provider_progress(
        self,
        goal_id: str,
        progress: ProviderGoalProgress,
        context: GovernanceMutationContext,
    ) -> GoalAutonomyState:
        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            run = next(
                (item for item in state.provider_runs if item.id == progress.run_id),
                None,
            )
            if not run:
                raise GoalGovernanceConflict(
                    "provider progress references an unknown run"
                )
            if context.actor_principal != run.executor_principal:
                raise GoalGovernanceConflict(
                    "provider progress actor does not match the assigned service identity"
                )
            if context.authority_instance_id != run.authority_instance_id:
                raise GoalGovernanceConflict(
                    "provider progress came from a different authority instance"
                )
            if context.fencing_token != run.fencing_token:
                raise GoalGovernanceConflict(
                    "provider progress has a stale fencing token"
                )
            reservation = self._require_reservation(state, run.reservation_id)
            if (
                run.launched_at is None
                or reservation.state != GoalReservationState.APPLIED
            ):
                raise GoalGovernanceConflict(
                    "provider progress requires a durably applied launch reservation"
                )
            if run.state in _TERMINAL_PROVIDER_STATES:
                raise GoalGovernanceConflict("terminal provider runs cannot be updated")
            if progress.state not in _PROVIDER_TRANSITIONS.get(run.state, set()):
                raise GoalGovernanceConflict(
                    f"invalid provider transition: {run.state.value} -> {progress.state.value}"
                )
            entering_operator_wait = (
                progress.state == ProviderRunState.WAITING_OPERATOR
                and run.state != ProviderRunState.WAITING_OPERATOR
            )
            requested_interactions = list(dict.fromkeys(progress.interaction_refs))
            interactions = {item.id: item for item in goal.operator_interactions}
            if entering_operator_wait and (
                not requested_interactions
                or any(
                    reference not in interactions
                    or interactions[reference].state != GoalInteractionState.PENDING
                    for reference in requested_interactions
                )
            ):
                raise GoalGovernanceConflict(
                    "provider wait requires durable pending operator interactions"
                )
            if (
                run.state == ProviderRunState.WAITING_OPERATOR
                and progress.state == ProviderRunState.RUNNING
            ):
                if (
                    not run.waiting_interaction_refs
                    or requested_interactions != run.waiting_interaction_refs
                ):
                    raise GoalGovernanceConflict(
                        "provider resume must cite the interactions bound to its wait generation"
                    )
                if any(
                    reference not in interactions
                    or interactions[reference].state != GoalInteractionState.ANSWERED
                    or not interactions[reference].response_principal
                    or not interactions[reference].response_summary
                    or interactions[reference].resolved_at is None
                    for reference in run.waiting_interaction_refs
                ):
                    raise GoalGovernanceConflict(
                        "provider resume requires durable answers for its wait generation"
                    )
            for metric in _USAGE_METRICS:
                if getattr(progress.cumulative_usage, metric) < getattr(
                    run.usage, metric
                ):
                    raise GoalGovernanceConflict(
                        f"provider cumulative usage cannot decrease {metric}"
                    )
            delta = GoalUsage(
                **{
                    metric: getattr(progress.cumulative_usage, metric)
                    - getattr(run.usage, metric)
                    for metric in _USAGE_METRICS
                }
            )
            previous_accounted = GoalUsage(
                **{
                    metric: max(
                        getattr(run.reserved_usage, metric),
                        getattr(run.usage, metric),
                    )
                    for metric in _USAGE_METRICS
                }
            )
            terminal = progress.state in _TERMINAL_PROVIDER_STATES
            next_accounted = (
                progress.cumulative_usage
                if terminal
                else GoalUsage(
                    **{
                        metric: max(
                            getattr(run.reserved_usage, metric),
                            getattr(progress.cumulative_usage, metric),
                        )
                        for metric in _USAGE_METRICS
                    }
                )
            )
            run.state = progress.state
            if entering_operator_wait:
                run.wait_generation += 1
                run.waiting_interaction_refs = requested_interactions
            run.summary = progress.summary
            run.usage = progress.cumulative_usage
            run.blocker_refs = progress.blocker_refs[-100:]
            run.interaction_refs = progress.interaction_refs[-100:]
            run.artifact_refs = progress.artifact_refs[-200:]
            run.evidence_claims = progress.evidence_claims[-200:]
            run.updated_at = self._clock()
            state.usage = _replace_usage(
                state.usage, previous_accounted, next_accounted
            )
            self._replace_rate_usage(
                goal,
                state,
                reservation.request,
                previous_accounted,
                next_accounted,
            )
            reservation.actual_usage = progress.cumulative_usage
            if terminal:
                previously_managed_claims = self._active_resource_claims(state)
                reservation.state = GoalReservationState.RELEASED
                reservation.released_at = self._clock()
                reservation.release_reason = f"provider run {progress.state.value}"
                run.reserved_usage = GoalUsage()
                self._refresh_resource_reservations(
                    state,
                    previously_managed_claims=previously_managed_claims,
                )
            exceeded = self._budget_reasons(goal, state.usage)
            if exceeded and not terminal:
                run.state = ProviderRunState.BLOCKED
                run.blocker_refs = list(
                    dict.fromkeys([*run.blocker_refs, "goal-budget-exhausted"])
                )
            return {
                "run_id": run.id,
                "state": run.state.value,
                "usage_delta": delta.model_dump(mode="json"),
                "budget_findings": exceeded,
                "provider_evidence_is_claim_only": True,
            }

        return self._mutate_state(
            goal_id,
            context,
            "goal_governance.provider_progressed",
            mutate,
            operation=progress,
        )

    @serialized_goal_mutation
    def propose_goal(
        self, request: GoalProposalRequest, context: GovernanceMutationContext
    ) -> GoalProposal:
        realm_id = request.goal.realm_id
        fingerprint = operation_fingerprint(
            realm_id=realm_id,
            entity_type=PROPOSAL_ENTITY,
            entity_id="<new>",
            event_type="goal_governance.goal_proposed",
            operation=request,
            context=context,
        )
        duplicate = self._duplicate(realm_id, context.idempotency_key)
        if duplicate:
            entity_id = str(duplicate.get("entity_id") or "")
            self._validate_replay(
                duplicate,
                entity_type=PROPOSAL_ENTITY,
                entity_id=entity_id,
                event_type="goal_governance.goal_proposed",
                fingerprint=fingerprint,
            )
            proposal = self.get_proposal(entity_id, realm_id=realm_id)
            if proposal:
                if (
                    proposal.review_reason
                    in {
                        "parent policy authorizes derived activation",
                        "standing operator policy matched",
                    }
                    and proposal.disposition == ProposalDisposition.PENDING_REVIEW
                ):
                    return self._activate_proposal(
                        proposal,
                        context,
                        disposition=ProposalDisposition.AUTO_ACTIVATED,
                        reason=proposal.review_reason,
                    )
                return proposal
        if context.expected_version != 0:
            raise GoalGovernanceConflict("new proposals require expected_version=0")
        proposal = GoalProposal(
            realm_id=realm_id,
            request=request,
            proposed_by=context.actor_principal,
        )
        auto, policy_id, policy_version, reason = self._proposal_policy(request)
        proposal.policy_id = policy_id
        proposal.policy_version = policy_version
        proposal.review_reason = reason
        self._commit_entity(
            realm_id,
            PROPOSAL_ENTITY,
            proposal.id,
            proposal,
            "goal_governance.goal_proposed",
            context,
            {"kind": request.kind.value, "automatic_activation": auto},
            operation_fingerprint_value=fingerprint,
        )
        if auto:
            proposal = self._activate_proposal(
                proposal,
                context,
                disposition=ProposalDisposition.AUTO_ACTIVATED,
                reason=reason,
            )
        return proposal

    def get_proposal(
        self, proposal_id: str, *, realm_id: str = "default"
    ) -> GoalProposal | None:
        payload = get_governance_payload(
            self.store, realm_id, PROPOSAL_ENTITY, proposal_id
        )
        return GoalProposal.model_validate(payload) if payload else None

    def list_proposals(self, realm_id: str = "default") -> list[GoalProposal]:
        return [
            GoalProposal.model_validate(item)
            for item in list_governance_payloads(self.store, realm_id, PROPOSAL_ENTITY)
        ]

    @serialized_goal_mutation
    def review_proposal(
        self,
        proposal_id: str,
        review: GoalProposalReview,
        context: GovernanceMutationContext,
        *,
        realm_id: str = "default",
    ) -> GoalProposal:
        proposal = self.get_proposal(proposal_id, realm_id=realm_id)
        if not proposal:
            raise KeyError(proposal_id)
        if review.approve:
            activation_key = f"{context.idempotency_key}:proposal-activated"
            activation_context = context.model_copy(
                update={"idempotency_key": activation_key}
            )
            fingerprint = operation_fingerprint(
                realm_id=realm_id,
                entity_type=PROPOSAL_ENTITY,
                entity_id=proposal_id,
                event_type="goal_governance.proposal_activated",
                operation=review,
                context=activation_context,
            )
            duplicate = self._duplicate(realm_id, activation_key)
            if duplicate:
                self._validate_replay(
                    duplicate,
                    entity_type=PROPOSAL_ENTITY,
                    entity_id=proposal_id,
                    event_type="goal_governance.proposal_activated",
                    fingerprint=fingerprint,
                )
                return self.get_proposal(proposal_id, realm_id=realm_id) or proposal
        else:
            fingerprint = operation_fingerprint(
                realm_id=realm_id,
                entity_type=PROPOSAL_ENTITY,
                entity_id=proposal_id,
                event_type="goal_governance.proposal_rejected",
                operation=review,
                context=context,
            )
            duplicate = self._duplicate(realm_id, context.idempotency_key)
            if duplicate:
                self._validate_replay(
                    duplicate,
                    entity_type=PROPOSAL_ENTITY,
                    entity_id=proposal_id,
                    event_type="goal_governance.proposal_rejected",
                    fingerprint=fingerprint,
                )
                return self.get_proposal(proposal_id, realm_id=realm_id) or proposal
        if context.expected_version != proposal.version:
            raise GoalGovernanceConflict(
                f"expected proposal version {context.expected_version}, "
                f"current version {proposal.version}"
            )
        if not _is_operator(context.actor_principal):
            raise GoalGovernanceConflict("proposal review requires an operator")
        if review.reviewer_principal != context.actor_principal:
            raise GoalGovernanceConflict("reviewer must match the mutation actor")
        if proposal.disposition != ProposalDisposition.PENDING_REVIEW:
            raise GoalGovernanceConflict("only pending proposals can be reviewed")
        if review.approve:
            return self._activate_proposal(
                proposal,
                context,
                disposition=ProposalDisposition.APPROVED,
                reason=review.reason,
                operation=review,
            )
        proposal.disposition = ProposalDisposition.REJECTED
        proposal.review_reason = review.reason
        proposal.version += 1
        proposal.updated_at = self._clock()
        self._commit_entity(
            realm_id,
            PROPOSAL_ENTITY,
            proposal.id,
            proposal,
            "goal_governance.proposal_rejected",
            context,
            {"reason": review.reason},
            operation_fingerprint_value=fingerprint,
        )
        return proposal

    def get_latest_review(
        self, realm_id: str = "default"
    ) -> GoalPortfolioReview | None:
        payload = get_governance_payload(
            self.store, realm_id, REVIEW_ENTITY, CURRENT_REVIEW_ID
        )
        return GoalPortfolioReview.model_validate(payload) if payload else None

    @serialized_goal_mutation
    def review_portfolio(
        self,
        request: GoalPortfolioReviewRequest,
        context: GovernanceMutationContext,
        *,
        realm_id: str = "default",
    ) -> GoalPortfolioReview:
        fingerprint = operation_fingerprint(
            realm_id=realm_id,
            entity_type=REVIEW_ENTITY,
            entity_id=CURRENT_REVIEW_ID,
            event_type="goal_governance.portfolio_reviewed",
            operation=request,
            context=context,
        )
        duplicate = self._duplicate(realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=REVIEW_ENTITY,
                entity_id=CURRENT_REVIEW_ID,
                event_type="goal_governance.portfolio_reviewed",
                fingerprint=fingerprint,
            )
            current = self.get_latest_review(realm_id)
            if current:
                return current
        previous = self.get_latest_review(realm_id)
        version = previous.version if previous else 0
        if context.expected_version != version:
            raise GoalGovernanceConflict(
                f"expected portfolio review version {context.expected_version}, "
                f"current version {version}"
            )
        if request.reviewer_principal != context.actor_principal:
            raise GoalGovernanceConflict(
                "portfolio reviewer must match the authenticated mutation actor"
            )
        if not request.independent:
            raise GoalGovernanceConflict("organization review must be independent")
        policy = self.effective_policy(realm_id)
        if context.policy_revision != policy.version:
            raise GoalGovernanceConflict(
                "portfolio review was not authorized by the active governance policy"
            )
        goals = [
            item
            for item in self.goals.list(realm_id=realm_id)
            if item.state not in _TERMINAL_STATES
        ]
        states = {item.id: self.get_state(item.id) for item in goals}
        allocations = self._allocate(goals, states, policy, previous)
        total = GoalUsage()
        for state in states.values():
            total = total.plus(state.usage)
        pending = [
            item.id
            for item in self.list_proposals(realm_id)
            if item.disposition == ProposalDisposition.PENDING_REVIEW
        ]
        findings: list[str] = []
        if (
            policy.max_portfolio_cost_usd is not None
            and total.cost_usd > policy.max_portfolio_cost_usd
        ):
            findings.append("portfolio cost budget is exhausted")
        if (
            policy.max_portfolio_tokens is not None
            and total.tokens > policy.max_portfolio_tokens
        ):
            findings.append("portfolio token budget is exhausted")
        queued = sum(
            item.disposition != AllocationDisposition.ACTIVE for item in allocations
        )
        if queued:
            findings.append(f"{queued} goals are not allocated to active resources")
        if pending:
            findings.append(f"{len(pending)} goal proposals await operator review")
        review = GoalPortfolioReview(
            realm_id=realm_id,
            version=version + 1,
            governance_policy_id=policy.id,
            governance_policy_version=policy.version,
            reviewer_principal=request.reviewer_principal,
            independent=request.independent,
            explanation=request.explanation,
            allocations=allocations,
            total_usage=total,
            pending_proposal_ids=pending,
            findings=findings,
            requires_operator_review=bool(findings),
        )
        self._commit_entity(
            realm_id,
            REVIEW_ENTITY,
            CURRENT_REVIEW_ID,
            review,
            "goal_governance.portfolio_reviewed",
            context,
            {
                "review_id": review.id,
                "findings": findings,
                "allocation_count": len(allocations),
            },
            operation_fingerprint_value=fingerprint,
        )
        return review

    def portfolio(self, realm_id: str = "default") -> dict[str, Any]:
        goals = self.goals.list(realm_id=realm_id)
        proposals = self.list_proposals(realm_id)
        return {
            "policy": self.effective_policy(realm_id).model_dump(mode="json"),
            "goals": [
                {
                    "goal": goal.model_dump(mode="json"),
                    "autonomy": self.get_state(goal.id).model_dump(mode="json"),
                }
                for goal in goals
            ],
            "proposals": [item.model_dump(mode="json") for item in proposals],
            "latest_review": (
                self.get_latest_review(realm_id).model_dump(mode="json")
                if self.get_latest_review(realm_id)
                else None
            ),
        }

    def _mutate_state(
        self,
        goal_id: str,
        context: GovernanceMutationContext,
        event_type: str,
        mutate: Callable[[Goal, GoalAutonomyState], dict[str, Any]],
        *,
        operation: Any = None,
        operation_fingerprint_value: str | None = None,
        validate_goal_context: bool = True,
    ) -> GoalAutonomyState:
        goal = self._require_goal(goal_id)
        fingerprint = operation_fingerprint_value or operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type=AUTONOMY_ENTITY,
            entity_id=goal_id,
            event_type=event_type,
            operation=operation,
            context=context,
        )
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=AUTONOMY_ENTITY,
                entity_id=goal_id,
                event_type=event_type,
                fingerprint=fingerprint,
            )
            return self.get_state(goal_id)
        if validate_goal_context:
            self._validate_goal_context(goal, context)
        state = self.get_state(goal_id)
        if context.expected_version != state.version:
            raise GoalGovernanceConflict(
                f"expected autonomy version {context.expected_version}, "
                f"current version {state.version}"
            )
        payload = mutate(goal, state)
        state.version += 1
        state.updated_at = self._clock()
        self._commit_entity(
            goal.realm_id,
            AUTONOMY_ENTITY,
            goal_id,
            state,
            event_type,
            context,
            payload,
            operation_fingerprint_value=fingerprint,
        )
        return state

    def _evaluate_action(
        self,
        goal: Goal,
        state: GoalAutonomyState,
        request: GoalActionRequest,
        context: GovernanceMutationContext,
        *,
        reserve: bool = True,
        exclude_reservation_id: str | None = None,
    ) -> GoalActionDecision:
        reasons: list[str] = []
        hard_denial = False
        approval_required = False
        policy = goal.policy
        valid_operator_approval = bool(
            request.operator_approved
            and request.approval_principal
            and _is_operator(request.approval_principal)
            and (
                request.approval_principal == context.actor_principal
                or self._has_correlated_operator_approval(goal, request)
            )
        )
        if _matches(request.action_class, policy.prohibited_actions):
            hard_denial = True
            reasons.append("the action is prohibited by the goal policy")
        safe_read = request.action_class.startswith(
            "observe"
        ) or request.action_class.endswith(".read")
        if (
            not policy.permitted_actions
            and not safe_read
            and not valid_operator_approval
        ):
            hard_denial = True
            reasons.append("the goal policy grants no executable action classes")
        elif (
            policy.permitted_actions
            and not safe_read
            and not _matches(request.action_class, policy.permitted_actions)
        ):
            hard_denial = True
            reasons.append("the action is outside the goal's permitted action classes")
        if (
            request.provider_id
            and policy.allowed_provider_ids
            and request.provider_id not in policy.allowed_provider_ids
        ):
            hard_denial = True
            reasons.append("the provider is outside the goal policy allowlist")
        if request.repository and request.repository not in policy.repository_scope:
            hard_denial = True
            reasons.append("the repository is outside the goal policy scope")
        if request.data_scope and request.data_scope not in policy.data_scope:
            hard_denial = True
            reasons.append("the data scope is outside the goal policy scope")
        envelope = request.materialization_envelope
        if envelope is not None:
            forbidden_repositories = sorted(
                set(envelope.repository_ids) - set(policy.repository_scope)
            )
            if forbidden_repositories:
                hard_denial = True
                reasons.append(
                    "the materialization envelope contains repositories outside "
                    "the goal policy scope: " + ", ".join(forbidden_repositories)
                )
            forbidden_data = sorted(set(envelope.data_scopes) - set(policy.data_scope))
            if forbidden_data:
                hard_denial = True
                reasons.append(
                    "the materialization envelope contains data outside the goal "
                    "policy scope: " + ", ".join(forbidden_data)
                )
            canonical_claims = [
                (
                    item.key,
                    item.access,
                    item.quantity,
                    item.preemptible,
                    item.expires_at,
                )
                for item in envelope.resource_claims
            ]
            requested_claims = sorted(
                (
                    item.key,
                    item.access.value,
                    item.quantity,
                    item.preemptible,
                    item.expires_at,
                )
                for item in request.resource_claims
            )
            if canonical_claims != requested_claims:
                hard_denial = True
                reasons.append(
                    "the action resource claims do not exactly match the "
                    "materialization envelope"
                )
            if (
                request.materialization_receipt is not None
                and request.materialization_receipt.envelope_digest != envelope.digest
            ):
                hard_denial = True
                reasons.append(
                    "the materialization receipt belongs to another envelope"
                )
            if request.execution_identity is not None and (
                request.materialization_receipt is None
                or request.execution_identity.materialization_receipt_digest
                != request.materialization_receipt.digest
                or request.execution_identity.work_package_id
                != envelope.work_package_id
                or request.execution_identity.service_role != envelope.service_role
            ):
                hard_denial = True
                reasons.append(
                    "the execution identity belongs to another materialization receipt"
                )
        elif (
            request.materialization_receipt is not None
            or request.execution_identity is not None
        ):
            hard_denial = True
            reasons.append(
                "materialization receipts and execution identities require an envelope"
            )
        if _matches(request.action_class, policy.require_operator_for):
            approval_required = True
            reasons.append("the goal policy requires operator approval for this action")
        if (
            _RISK_RANK[request.risk]
            > _RISK_RANK[GoalActionRisk(policy.max_action_risk)]
        ):
            approval_required = True
            reasons.append("the action risk exceeds the autonomous risk ceiling")
        if policy.autonomy_level == 1 and not safe_read:
            approval_required = True
            reasons.append("observe autonomy cannot execute this action")
        elif policy.autonomy_level == 2:
            approval_required = True
            reasons.append("propose autonomy requires approval before execution")
        elif policy.autonomy_level == 3 and (
            not request.reversible or request.delegated
        ):
            approval_required = True
            reasons.append(
                "reversible autonomy cannot delegate or take irreversible action"
            )
        elif policy.autonomy_level == 4 and request.external:
            approval_required = True
            reasons.append("delegated autonomy cannot address an external audience")
        if request.external and not request.audience:
            hard_denial = True
            reasons.append("external actions require an attributable audience")
        if request.operator_approved:
            if not valid_operator_approval:
                hard_denial = True
                reasons.append("operator approval provenance is invalid")
            else:
                approval_required = False
                reasons.append(
                    "an attributable operator approval satisfies the approval gate"
                )
        if hard_denial:
            disposition = GoalActionDisposition.DENIED
        elif approval_required:
            disposition = GoalActionDisposition.REQUIRES_APPROVAL
        else:
            projected = state.usage.plus(request.estimate)
            budget_reasons = self._budget_reasons(goal, projected)
            active_reservations = sum(
                item.state
                in {GoalReservationState.RESERVED, GoalReservationState.APPLIED}
                for item in state.action_reservations
            )
            if budget_reasons:
                disposition = GoalActionDisposition.BUDGET_EXHAUSTED
                reasons.extend(budget_reasons)
            else:
                rate_reasons = self._rate_limit_reasons(
                    goal,
                    state,
                    request,
                    exclude_reservation_id=exclude_reservation_id,
                )
                if rate_reasons:
                    disposition = GoalActionDisposition.RATE_LIMITED
                    reasons.extend(rate_reasons)
                elif active_reservations >= goal.budget.max_concurrency:
                    disposition = GoalActionDisposition.BUDGET_EXHAUSTED
                    reasons.append("the goal concurrency budget would be exceeded")
                else:
                    conflict_reasons = self._resource_conflict_reasons(
                        goal, request.resource_claims, current_state=state
                    )
                    if conflict_reasons:
                        disposition = GoalActionDisposition.RESOURCE_CONFLICT
                        reasons.extend(conflict_reasons)
                    else:
                        disposition = GoalActionDisposition.AUTHORIZED
                        reasons.append(
                            "the action is inside the active policy, budget, rate, and resource envelope"
                        )
        decision = GoalActionDecision(
            goal_id=goal.id,
            action_class=request.action_class,
            disposition=disposition,
            reasons=reasons or ["the deterministic policy produced no authorization"],
            policy_revision=goal.policy.revision,
            request=request,
            reserved_usage=(
                request.estimate
                if disposition == GoalActionDisposition.AUTHORIZED
                else GoalUsage()
            ),
            decided_by=context.actor_principal,
            authority_instance_id=context.authority_instance_id,
            fencing_token=context.fencing_token,
            decided_at=self._clock(),
        )
        if disposition == GoalActionDisposition.AUTHORIZED and reserve:
            previously_managed_claims = self._active_resource_claims(state)
            reservation = GoalActionReservation(
                idempotency_key=context.idempotency_key,
                decision_id=decision.id,
                goal_id=goal.id,
                action_class=request.action_class,
                actor_principal=context.actor_principal,
                authority_instance_id=context.authority_instance_id,
                policy_revision=goal.policy.revision,
                goal_version=goal.version,
                fencing_token=context.fencing_token,
                request=request.model_copy(deep=True),
                reserved_usage=request.estimate.model_copy(deep=True),
                resource_claims=[
                    item.model_copy(deep=True) for item in request.resource_claims
                ],
                max_attempts=request.max_attempts,
                created_at=self._clock(),
            )
            decision.reservation_id = reservation.id
            state.action_reservations.append(reservation)
            state.usage = projected
            self._refresh_resource_reservations(
                state,
                previously_managed_claims=previously_managed_claims,
            )
            self._reserve_rate_windows(goal, state, request)
        return decision

    def _has_correlated_operator_approval(
        self, goal: Goal, request: GoalActionRequest
    ) -> bool:
        """Verify an approval against a durable goal-to-notification link."""

        if not request.approval_interaction_id or not request.approval_principal:
            return False
        for link in goal.operator_interactions:
            if link.id != request.approval_interaction_id:
                continue
            if (
                link.state == GoalInteractionState.ANSWERED
                and link.response_principal == request.approval_principal
            ):
                return True
            notification = self.store.get_notification(
                link.notification_id, realm_id=goal.realm_id
            )
            interaction = notification.interaction if notification else None
            response = interaction.response if interaction else None
            return bool(
                interaction
                and interaction.state.value == "answered"
                and interaction.response_principal == request.approval_principal
                and isinstance(response, dict)
                and response.get("choice_id") == "approve"
            )
        return False

    @staticmethod
    def _require_reservation(
        state: GoalAutonomyState, reservation_id: str
    ) -> GoalActionReservation:
        reservation = next(
            (item for item in state.action_reservations if item.id == reservation_id),
            None,
        )
        if reservation is None:
            raise GoalGovernanceConflict("action reservation does not exist")
        return reservation

    def _state_without_reservation(
        self,
        goal: Goal,
        state: GoalAutonomyState,
        reservation: GoalActionReservation,
    ) -> GoalAutonomyState:
        available = state.model_copy(deep=True)
        accounted = (
            reservation.actual_usage
            if any(
                getattr(reservation.actual_usage, metric) for metric in _USAGE_METRICS
            )
            else reservation.reserved_usage
        )
        available.usage = _replace_usage(available.usage, accounted, GoalUsage())
        self._replace_rate_usage(
            goal,
            available,
            reservation.request,
            accounted,
            GoalUsage(),
        )
        available.action_reservations = [
            item for item in available.action_reservations if item.id != reservation.id
        ]
        self._refresh_resource_reservations(available)
        return available

    def _validate_reservation(
        self,
        goal: Goal,
        reservation: GoalActionReservation,
        context: GovernanceMutationContext,
        *,
        allow_stale_policy: bool = False,
        allow_recovery: bool = False,
    ) -> None:
        if reservation.goal_id != goal.id:
            raise GoalGovernanceConflict("action reservation belongs to another goal")
        if (
            not allow_stale_policy
            and reservation.policy_revision != goal.policy.revision
        ):
            raise GoalGovernanceConflict(
                "action reservation was authorized by a stale policy revision"
            )
        recovery_authorized = allow_recovery and (
            _is_operator(context.actor_principal)
            or (
                goal.lease.active(self._clock())
                and goal.lease.holder_instance_id == context.authority_instance_id
                and goal.lease.fencing_token == context.fencing_token
            )
        )
        if (
            not recovery_authorized
            and reservation.authority_instance_id != context.authority_instance_id
        ):
            raise GoalGovernanceConflict(
                "action reservation belongs to another authority instance"
            )
        if (
            not recovery_authorized
            and reservation.fencing_token != context.fencing_token
        ):
            raise GoalGovernanceConflict("action reservation has a stale fencing token")
        if (
            not recovery_authorized
            and context.actor_principal != reservation.actor_principal
        ):
            raise GoalGovernanceConflict(
                "action reservation belongs to another authenticated actor"
            )

    @staticmethod
    def _active_resource_claims(
        state: GoalAutonomyState,
    ) -> list[GoalResourceClaim]:
        return [
            claim
            for reservation in state.action_reservations
            if reservation.state
            in {GoalReservationState.RESERVED, GoalReservationState.APPLIED}
            for claim in reservation.resource_claims
        ]

    @classmethod
    def _refresh_resource_reservations(
        cls,
        state: GoalAutonomyState,
        *,
        previously_managed_claims: list[GoalResourceClaim] | None = None,
    ) -> None:
        """Rebuild managed claims without mistaking a removed claim for legacy.

        ``resource_reservations`` predates action reservations and can contain
        genuine legacy claims.  Callers that add or release a reservation pass
        the exact pre-mutation managed multiset so only those represented claim
        occurrences are removed; equal legacy or sibling claims remain intact.
        """

        represented = (
            previously_managed_claims
            if previously_managed_claims is not None
            else cls._active_resource_claims(state)
        )
        legacy = [claim.model_copy(deep=True) for claim in state.resource_reservations]
        for managed in represented:
            match = next(
                (index for index, claim in enumerate(legacy) if claim == managed),
                None,
            )
            if match is not None:
                legacy.pop(match)
        state.resource_reservations = legacy + [
            claim.model_copy(deep=True) for claim in cls._active_resource_claims(state)
        ]

    def _release_reservation(
        self,
        goal: Goal,
        state: GoalAutonomyState,
        reservation: GoalActionReservation,
        *,
        actual_usage: GoalUsage,
        reason: str,
    ) -> None:
        previously_managed_claims = self._active_resource_claims(state)
        previous = (
            reservation.actual_usage
            if any(
                getattr(reservation.actual_usage, metric) for metric in _USAGE_METRICS
            )
            else reservation.reserved_usage
        )
        state.usage = _replace_usage(state.usage, previous, actual_usage)
        self._replace_rate_usage(
            goal,
            state,
            reservation.request,
            previous,
            actual_usage,
        )
        reservation.actual_usage = actual_usage
        reservation.state = GoalReservationState.RELEASED
        reservation.released_at = self._clock()
        reservation.release_reason = reason[:500]
        self._refresh_resource_reservations(
            state,
            previously_managed_claims=previously_managed_claims,
        )

    def _budget_reasons(self, goal: Goal, projected: GoalUsage) -> list[str]:
        reasons: list[str] = []
        if goal.budget.deadline and self._clock() > goal.budget.deadline:
            reasons.append("the goal deadline has passed")
        for metric, label in (
            ("cost_usd", "cost"),
            ("tokens", "token"),
            ("api_calls", "API-call"),
            ("storage_mb", "storage"),
            ("actions", "action"),
            ("dispatches", "dispatch"),
        ):
            if _budget_exceeded(projected, goal.budget, metric):
                reasons.append(f"the goal {label} budget would be exceeded")
        organization = self.effective_policy(goal.realm_id)
        states = [
            GoalAutonomyState.model_validate(item)
            for item in list_governance_payloads(
                self.store, goal.realm_id, AUTONOMY_ENTITY
            )
            if item.get("goal_id") != goal.id
        ]
        total = projected
        for item in states:
            total = total.plus(item.usage)
        if (
            organization.max_portfolio_cost_usd is not None
            and total.cost_usd > organization.max_portfolio_cost_usd
        ):
            reasons.append("the organization portfolio cost budget would be exceeded")
        if (
            organization.max_portfolio_tokens is not None
            and total.tokens > organization.max_portfolio_tokens
        ):
            reasons.append("the organization portfolio token budget would be exceeded")
        return reasons

    def _active_rate_windows(
        self, state: GoalAutonomyState, *, now: datetime
    ) -> dict[str, GoalRateWindow]:
        return {
            item.key: item
            for item in state.rate_windows
            if any(
                limit.key == item.key
                and (now - item.started_at).total_seconds() < limit.window_seconds
                for limit in self._require_goal(state.goal_id).budget.rate_limits
            )
        }

    def _rate_limit_reasons(
        self,
        goal: Goal,
        state: GoalAutonomyState,
        request: GoalActionRequest,
        *,
        exclude_reservation_id: str | None = None,
    ) -> list[str]:
        now = self._clock()
        active = self._active_rate_windows(state, now=now)
        reasons: list[str] = []
        for limit in goal.budget.rate_limits:
            if limit.key != "*" and not fnmatchcase(request.action_class, limit.key):
                continue
            usage = active.get(
                limit.key, GoalRateWindow(key=limit.key, started_at=now)
            ).usage
            projected = usage.plus(request.estimate)
            for metric in ("actions", "cost_usd", "tokens", "api_calls"):
                if _budget_exceeded(projected, limit, metric):
                    reasons.append(
                        f"rate limit {limit.key!r} would exceed {metric} in its rolling window"
                    )
        if request.provider_id:
            organization = self.effective_policy(goal.realm_id)
            for limit in organization.provider_rate_limits.get(request.provider_id, []):
                usage = GoalUsage()
                threshold = now.timestamp() - limit.window_seconds
                for payload in list_governance_payloads(
                    self.store, goal.realm_id, AUTONOMY_ENTITY
                ):
                    other = GoalAutonomyState.model_validate(payload)
                    if other.action_reservations:
                        for reservation in other.action_reservations:
                            if (
                                reservation.id == exclude_reservation_id
                                or reservation.request.provider_id
                                != request.provider_id
                                or reservation.created_at.timestamp() < threshold
                                or (
                                    limit.key != "*"
                                    and not fnmatchcase(
                                        reservation.action_class, limit.key
                                    )
                                )
                            ):
                                continue
                            accounted = (
                                reservation.actual_usage
                                if reservation.state == GoalReservationState.RELEASED
                                else _max_usage(
                                    reservation.reserved_usage,
                                    reservation.actual_usage,
                                )
                            )
                            usage = usage.plus(accounted)
                        continue
                    # Legacy projections predate first-class reservations and contain
                    # one authorization decision per provider assignment.
                    for decision in other.recent_decisions:
                        if (
                            decision.disposition == GoalActionDisposition.AUTHORIZED
                            and decision.request.provider_id == request.provider_id
                            and decision.decided_at.timestamp() >= threshold
                            and (
                                limit.key == "*"
                                or fnmatchcase(decision.action_class, limit.key)
                            )
                        ):
                            usage = usage.plus(decision.reserved_usage)
                projected = usage.plus(request.estimate)
                for metric in ("actions", "cost_usd", "tokens", "api_calls"):
                    if _budget_exceeded(projected, limit, metric):
                        reasons.append(
                            f"provider {request.provider_id!r} organization rate limit "
                            f"{limit.key!r} would exceed {metric}"
                        )
        return reasons

    def _reserve_rate_windows(
        self, goal: Goal, state: GoalAutonomyState, request: GoalActionRequest
    ) -> None:
        now = self._clock()
        active = self._active_rate_windows(state, now=now)
        result: list[GoalRateWindow] = []
        for limit in goal.budget.rate_limits:
            existing = active.get(limit.key)
            if limit.key != "*" and not fnmatchcase(request.action_class, limit.key):
                if existing:
                    result.append(existing)
                continue
            window = existing or GoalRateWindow(key=limit.key, started_at=now)
            window.usage = window.usage.plus(request.estimate)
            result.append(window)
        state.rate_windows = result

    def _replace_rate_usage(
        self,
        goal: Goal,
        state: GoalAutonomyState,
        request: GoalActionRequest,
        previous: GoalUsage,
        replacement: GoalUsage,
    ) -> None:
        """Replace one reservation's contribution to active rolling windows."""

        now = self._clock()
        active = self._active_rate_windows(state, now=now)
        result: list[GoalRateWindow] = []
        for limit in goal.budget.rate_limits:
            existing = active.get(limit.key)
            if existing is None:
                continue
            if limit.key != "*" and not fnmatchcase(request.action_class, limit.key):
                result.append(existing)
                continue
            existing.usage = _replace_usage(existing.usage, previous, replacement)
            result.append(existing)
        state.rate_windows = result

    def _resource_conflict_reasons(
        self,
        goal: Goal,
        claims: list[GoalResourceClaim],
        *,
        current_state: GoalAutonomyState | None = None,
    ) -> list[str]:
        reasons: list[str] = []
        latest = self.get_latest_review(goal.realm_id)
        allocation = next(
            (
                item
                for item in (latest.allocations if latest else [])
                if item.goal_id == goal.id
            ),
            None,
        )
        if allocation and allocation.disposition != AllocationDisposition.ACTIVE:
            reasons.append("the latest portfolio review did not allocate this goal")
        if not claims:
            return reasons
        now = self._clock()
        existing: list[tuple[str, GoalResourceClaim]] = []
        if current_state is not None:
            existing.extend(
                (goal.id, claim)
                for claim in current_state.resource_reservations
                if not claim.expires_at or claim.expires_at > now
            )
        for payload in list_governance_payloads(
            self.store, goal.realm_id, AUTONOMY_ENTITY
        ):
            state = GoalAutonomyState.model_validate(payload)
            if state.goal_id == goal.id:
                continue
            existing.extend(
                (state.goal_id, claim)
                for claim in state.resource_reservations
                if not claim.expires_at or claim.expires_at > now
            )
        capacities = {
            item.key: item.capacity
            for item in self.effective_policy(goal.realm_id).resource_capacities
        }
        requested_by_key: dict[str, list[GoalResourceClaim]] = {}
        for claim in claims:
            requested_by_key.setdefault(claim.key, []).append(claim)
        for key, requested in requested_by_key.items():
            same = [(owner, item) for owner, item in existing if item.key == key]
            requested_exclusive = any(
                item.access == ResourceAccess.EXCLUSIVE for item in requested
            )
            if any(
                requested_exclusive or item.access == ResourceAccess.EXCLUSIVE
                for _, item in same
            ) or (requested_exclusive and len(requested) > 1):
                owners = sorted({owner for owner, _ in same})
                reasons.append(
                    f"resource {key!r} has incompatible exclusive claims"
                    + (f" with {', '.join(owners)}" if owners else " in this action")
                )
            capacity = capacities.get(key)
            if capacity is not None:
                used = sum(item.quantity for _, item in same)
                requested_quantity = sum(item.quantity for item in requested)
                if used + requested_quantity > capacity:
                    reasons.append(f"resource {key!r} capacity would be exceeded")
        return list(dict.fromkeys(reasons))

    def _proposal_policy(
        self, request: GoalProposalRequest
    ) -> tuple[bool, str | None, int | None, str]:
        now = self._clock()
        if request.kind == ProposalKind.DERIVED_SUBGOAL:
            parent = self._require_goal(request.parent_goal_id or "")
            if request.parent_criterion_id and request.parent_criterion_id not in {
                item.id for item in parent.criteria
            }:
                raise GoalGovernanceConflict(
                    "derived proposal references an unknown parent criterion"
                )
            if request.parent_risk and request.parent_risk not in parent.risks:
                raise GoalGovernanceConflict(
                    "derived proposal references an unknown parent risk"
                )
            self._validate_subgoal_envelope(parent, request.goal)
            depth = self._goal_depth(parent) + 1
            proposals = [
                item
                for item in self.list_proposals(parent.realm_id)
                if item.request.parent_goal_id == parent.id
                and item.disposition
                not in {ProposalDisposition.REJECTED, ProposalDisposition.EXPIRED}
            ]
            if not parent.policy.allow_derived_subgoals:
                return (
                    False,
                    None,
                    parent.policy.revision,
                    "parent policy requires review",
                )
            if depth > parent.policy.max_subgoal_depth:
                return (
                    False,
                    None,
                    parent.policy.revision,
                    "subgoal depth limit reached",
                )
            if len(proposals) >= parent.policy.max_derived_subgoals:
                return (
                    False,
                    None,
                    parent.policy.revision,
                    "derived subgoal quota reached",
                )
            if proposals and parent.policy.proposal_cooldown_seconds:
                latest = max(item.created_at for item in proposals)
                if (
                    now - latest
                ).total_seconds() < parent.policy.proposal_cooldown_seconds:
                    return (
                        False,
                        None,
                        parent.policy.revision,
                        "proposal cooldown is active",
                    )
            auto = parent.policy.auto_activate_derived_subgoals
            return (
                auto,
                None,
                parent.policy.revision,
                "parent policy authorizes derived activation"
                if auto
                else "operator review required",
            )
        policy = self.effective_policy(request.goal.realm_id)
        for standing in policy.standing_goal_policies:
            if not standing.enabled or standing.expires_at <= now:
                continue
            if request.category not in standing.categories:
                continue
            if (
                standing.project_ids
                and request.goal.project_id not in standing.project_ids
            ):
                continue
            if request.requested_priority > standing.max_priority:
                continue
            if standing.max_cost_usd is not None and (
                request.goal.budget.max_cost_usd is None
                or request.goal.budget.max_cost_usd > standing.max_cost_usd
            ):
                continue
            if standing.max_tokens is not None and (
                request.goal.budget.max_tokens is None
                or request.goal.budget.max_tokens > standing.max_tokens
            ):
                continue
            return True, standing.id, policy.version, "standing operator policy matched"
        return (
            False,
            policy.id,
            policy.version,
            "top-level proposals require operator review",
        )

    def _validate_subgoal_envelope(self, parent: Goal, child: GoalCreate) -> None:
        if child.realm_id != parent.realm_id or child.project_id != parent.project_id:
            raise GoalGovernanceConflict(
                "subgoals cannot expand realm or project scope"
            )
        if child.policy.autonomy_level > parent.policy.autonomy_level:
            raise GoalGovernanceConflict("subgoals cannot increase autonomy")
        if not set(child.policy.permitted_actions).issubset(
            parent.policy.permitted_actions
        ):
            raise GoalGovernanceConflict("subgoals cannot expand permitted actions")
        if not set(parent.policy.prohibited_actions).issubset(
            child.policy.prohibited_actions
        ):
            raise GoalGovernanceConflict("subgoals must inherit prohibited actions")
        for field in ("repository_scope", "data_scope", "allowed_provider_ids"):
            parent_values = set(getattr(parent.policy, field))
            child_values = set(getattr(child.policy, field))
            if child_values and not child_values.issubset(parent_values):
                raise GoalGovernanceConflict(f"subgoals cannot expand {field}")
        for metric in (
            "max_cost_usd",
            "max_tokens",
            "max_api_calls",
            "max_storage_mb",
            "max_actions",
            "max_dispatches",
        ):
            parent_limit = getattr(parent.budget, metric)
            child_limit = getattr(child.budget, metric)
            if parent_limit is not None and (
                child_limit is None or child_limit > parent_limit
            ):
                raise GoalGovernanceConflict(f"subgoal {metric} exceeds its parent")

    def _goal_depth(self, goal: Goal) -> int:
        depth = 0
        seen = {goal.id}
        current = goal
        while current.parent_goal_id:
            if current.parent_goal_id in seen:
                raise GoalGovernanceConflict("goal ancestry contains a cycle")
            seen.add(current.parent_goal_id)
            current = self._require_goal(current.parent_goal_id)
            depth += 1
        return depth

    def _activate_proposal(
        self,
        proposal: GoalProposal,
        context: GovernanceMutationContext,
        *,
        disposition: ProposalDisposition,
        reason: str,
        operation: Any = None,
    ) -> GoalProposal:
        activation_key = f"{context.idempotency_key}:proposal-activated"
        activation_context = context.model_copy(
            update={"idempotency_key": activation_key}
        )
        fingerprint = operation_fingerprint(
            realm_id=proposal.realm_id,
            entity_type=PROPOSAL_ENTITY,
            entity_id=proposal.id,
            event_type="goal_governance.proposal_activated",
            operation=(
                operation
                if operation is not None
                else {
                    "proposal_id": proposal.id,
                    "disposition": disposition,
                    "reason": reason,
                }
            ),
            context=activation_context,
        )
        duplicate = self._duplicate(proposal.realm_id, activation_key)
        if duplicate:
            self._validate_replay(
                duplicate,
                entity_type=PROPOSAL_ENTITY,
                entity_id=proposal.id,
                event_type="goal_governance.proposal_activated",
                fingerprint=fingerprint,
            )
            return (
                self.get_proposal(proposal.id, realm_id=proposal.realm_id) or proposal
            )
        data = proposal.request.goal.model_copy(deep=True)
        if proposal.request.kind == ProposalKind.DERIVED_SUBGOAL:
            data.parent_goal_id = proposal.request.parent_goal_id
            data.creation_source = "agent_derived"
        else:
            data.parent_goal_id = None
            data.creation_source = "agent_proposed"
        goal_context = GoalMutationContext(
            actor_principal=context.actor_principal,
            authority_instance_id=context.authority_instance_id,
            idempotency_key=f"{context.idempotency_key}:activate",
            expected_version=0,
            policy_revision=data.policy.revision,
        )
        activated = self.goals.create(data, goal_context)
        proposal.disposition = disposition
        proposal.activated_goal_id = activated.id
        proposal.review_reason = reason
        proposal.version += 1
        proposal.updated_at = self._clock()
        self._commit_entity(
            proposal.realm_id,
            PROPOSAL_ENTITY,
            proposal.id,
            proposal,
            "goal_governance.proposal_activated",
            activation_context,
            {"goal_id": activated.id, "disposition": disposition.value},
            operation_fingerprint_value=fingerprint,
        )
        return proposal

    def _allocate(
        self,
        goals: list[Goal],
        states: dict[str, GoalAutonomyState],
        policy: GoalGovernancePolicy,
        previous: GoalPortfolioReview | None,
    ) -> list[GoalPortfolioEntry]:
        now = self._clock()
        previous_active = {
            item.goal_id
            for item in (previous.allocations if previous else [])
            if item.disposition == AllocationDisposition.ACTIVE
        }

        def score(goal: Goal) -> float:
            value = float(states[goal.id].priority)
            if goal.state == GoalState.ACTIVE:
                value += 5
            if goal.budget.deadline:
                seconds = (goal.budget.deadline - now).total_seconds()
                if seconds <= 0:
                    value += 30
                elif seconds < 86_400:
                    value += 20
                elif seconds < 604_800:
                    value += 10
            return value

        ordered = sorted(goals, key=lambda item: (-score(item), item.id))
        capacity = {item.key: item.capacity for item in policy.resource_capacities}
        used: dict[str, float] = {}
        exclusive: set[str] = set()
        active_count = 0
        entries: list[GoalPortfolioEntry] = []
        for goal in ordered:
            state = states[goal.id]
            claims = [
                item
                for item in state.resource_reservations
                if not item.expires_at or item.expires_at > now
            ]
            reasons: list[str] = []
            disposition = AllocationDisposition.ACTIVE
            if goal.state in {GoalState.PAUSED, GoalState.BLOCKED}:
                disposition = AllocationDisposition.BLOCKED
                reasons.append(f"goal state is {goal.state.value}")
            elif active_count >= policy.max_active_goals:
                disposition = AllocationDisposition.QUEUED
                reasons.append("organization active-goal limit reached")
            else:
                for claim in claims:
                    if claim.key in exclusive or (
                        claim.access == ResourceAccess.EXCLUSIVE
                        and used.get(claim.key, 0) > 0
                    ):
                        disposition = AllocationDisposition.QUEUED
                        reasons.append(f"resource {claim.key!r} is already allocated")
                    if (
                        claim.key in capacity
                        and used.get(claim.key, 0) + claim.quantity
                        > capacity[claim.key]
                    ):
                        disposition = AllocationDisposition.QUEUED
                        reasons.append(f"resource {claim.key!r} lacks capacity")
            if disposition == AllocationDisposition.ACTIVE:
                active_count += 1
                reasons.append("priority and resource constraints admit this goal")
                for claim in claims:
                    used[claim.key] = used.get(claim.key, 0) + claim.quantity
                    if claim.access == ResourceAccess.EXCLUSIVE:
                        exclusive.add(claim.key)
            elif (
                goal.id in previous_active
                and disposition == AllocationDisposition.QUEUED
            ):
                disposition = AllocationDisposition.PREEMPTED
                reasons.append("a higher-priority allocation displaced this goal")
            entries.append(
                GoalPortfolioEntry(
                    goal_id=goal.id,
                    priority_score=score(goal),
                    disposition=disposition,
                    reasons=list(dict.fromkeys(reasons)),
                    resource_claims=claims,
                )
            )
        return entries

    def _validate_goal_context(
        self, goal: Goal, context: GovernanceMutationContext
    ) -> None:
        if goal.control_authority_instance_id is None:
            raise GoalGovernanceConflict(
                "goal has no durable control authority; rebuild legacy history before governance"
            )
        if context.authority_instance_id != goal.control_authority_instance_id:
            raise GoalGovernanceConflict(
                "stale or unauthorized control authority fencing token; "
                "route governance through the durable control authority"
            )
        if self.instance_id != goal.control_authority_instance_id:
            raise GoalGovernanceConflict(
                "goal governance must execute on the durable control authority instance"
            )
        if context.goal_version is not None and context.goal_version != goal.version:
            raise GoalGovernanceConflict(
                f"expected goal version {context.goal_version}, current version {goal.version}"
            )
        if context.policy_revision != goal.policy.revision:
            raise GoalGovernanceConflict(
                "governance mutation was not authorized by the active goal policy"
            )
        eligible = set(
            goal.wakeup.eligible_instance_ids
            if goal.wakeup and goal.wakeup.eligible_instance_ids
            else goal.lease.eligible_instance_ids
        )
        if eligible and context.authority_instance_id not in eligible:
            raise GoalGovernanceConflict(
                "authority instance is not eligible to govern this goal"
            )
        if goal.lease.active(self._clock()) and (
            goal.lease.holder_instance_id != context.authority_instance_id
            or goal.lease.fencing_token != context.fencing_token
        ):
            raise GoalGovernanceConflict(
                "stale or unauthorized goal controller fencing token"
            )
        if context.actor_principal.startswith("service:") and not goal.lease.active(
            self._clock()
        ):
            raise GoalGovernanceConflict(
                "service governance mutations require an active controller lease"
            )

    def _require_goal(self, goal_id: str) -> Goal:
        goal = self.goals.get(goal_id)
        if not goal:
            raise KeyError(goal_id)
        return goal

    def _duplicate(self, realm_id: str, key: str) -> dict[str, Any] | None:
        return find_governance_event_by_idempotency(self.store, realm_id, key)

    @staticmethod
    def _validate_replay(
        duplicate: dict[str, Any],
        *,
        entity_type: str,
        entity_id: str,
        event_type: str,
        fingerprint: str,
    ) -> None:
        if (
            duplicate.get("entity_type") != entity_type
            or duplicate.get("entity_id") != entity_id
        ):
            raise GoalGovernanceConflict(
                "idempotency key belongs to another governance entity"
            )
        if duplicate.get("event_type") != event_type:
            raise GoalGovernanceConflict(
                "idempotency key belongs to another governance operation"
            )
        recorded = str(duplicate.get("operation_fingerprint") or "")
        if not recorded:
            raise GoalGovernanceConflict(
                "legacy governance event cannot be replayed without an exact operation fingerprint"
            )
        if recorded != fingerprint:
            raise GoalGovernanceConflict(
                "idempotency key belongs to a different governance operation"
            )

    def _commit_entity(
        self,
        realm_id: str,
        entity_type: str,
        entity_id: str,
        entity,
        event_type: str,
        context: GovernanceMutationContext,
        payload: dict[str, Any],
        *,
        operation_fingerprint_value: str | None = None,
    ) -> None:
        version = int(getattr(entity, "version", 1))
        fingerprint = operation_fingerprint_value or operation_fingerprint(
            realm_id=realm_id,
            entity_type=entity_type,
            entity_id=entity_id,
            event_type=event_type,
            operation=payload,
            context=context,
        )
        self.store.commit_event(
            CardEvent(
                type=EventType.GOAL_GOVERNANCE_UPSERTED,
                realm_id=realm_id,
                author_principal=context.actor_principal,
                author_instance=context.authority_instance_id or self.instance_id,
                payload={
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "entity": entity.model_dump(mode="json"),
                    "governance_event": {
                        "event_type": event_type,
                        "actor_principal": context.actor_principal,
                        "authority_instance_id": context.authority_instance_id,
                        "policy_revision": context.policy_revision,
                        "idempotency_key": context.idempotency_key,
                        "version": version,
                        "operation_fingerprint": fingerprint,
                        "payload": payload,
                    },
                },
            )
        )
