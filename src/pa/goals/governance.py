"""Deterministic policy enforcement for advanced goal autonomy."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from fnmatch import fnmatchcase
from typing import Any

from pa.domain.models import CardEvent, EventType
from pa.goals.advanced_models import (
    AllocationDisposition,
    GoalActionDecision,
    GoalActionDisposition,
    GoalActionRequest,
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
from pa.goals.models import Goal, GoalCreate, GoalMutationContext, GoalState
from pa.goals.projection import (
    find_governance_event_by_idempotency,
    get_governance_payload,
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


class GoalGovernanceConflict(ValueError):
    pass


def _matches(value: str, patterns: list[str]) -> bool:
    return any(fnmatchcase(value, pattern) for pattern in patterns)


def _is_operator(principal: str) -> bool:
    return principal.startswith(("user:", "role:admin"))


def _budget_exceeded(current: GoalUsage, limit, metric: str) -> bool:
    maximum = getattr(limit, f"max_{metric}", None)
    return maximum is not None and getattr(current, metric) > maximum


class GoalGovernanceService:
    def __init__(
        self,
        store,
        instance_id: str,
        goal_service: GoalService,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = store
        self.instance_id = instance_id
        self.goals = goal_service
        self._clock = clock or (lambda: datetime.now(UTC))

    def get_state(self, goal_id: str) -> GoalAutonomyState:
        goal = self._require_goal(goal_id)
        payload = get_governance_payload(
            self.store, goal.realm_id, AUTONOMY_ENTITY, goal_id
        )
        return (
            GoalAutonomyState.model_validate(payload)
            if payload
            else GoalAutonomyState(goal_id=goal.id, realm_id=goal.realm_id)
        )

    def get_policy(self, realm_id: str = "default") -> GoalGovernancePolicy | None:
        payload = get_governance_payload(
            self.store, realm_id, POLICY_ENTITY, "organization"
        )
        return GoalGovernancePolicy.model_validate(payload) if payload else None

    def effective_policy(self, realm_id: str = "default") -> GoalGovernancePolicy:
        return self.get_policy(realm_id) or GoalGovernancePolicy(realm_id=realm_id)

    def put_policy(
        self, policy: GoalGovernancePolicy, context: GovernanceMutationContext
    ) -> GoalGovernancePolicy:
        duplicate = self._duplicate(policy.realm_id, context.idempotency_key)
        if duplicate:
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
        )
        return policy

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
            goal_id, context, "goal_governance.priority_changed", mutate
        )

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
            goal_id, context, "goal_governance.strategies_updated", mutate
        )

    def authorize_action(
        self,
        goal_id: str,
        request: GoalActionRequest,
        context: GovernanceMutationContext,
    ) -> tuple[GoalAutonomyState, GoalActionDecision]:
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
            }

        state = self._mutate_state(
            goal_id, context, "goal_governance.action_decided", mutate
        )
        assert decision is not None
        return state, decision

    def assign_provider(
        self,
        goal_id: str,
        assignment: ProviderGoalAssignment,
        context: GovernanceMutationContext,
    ) -> tuple[GoalAutonomyState, ProviderGoalRun | None, GoalActionDecision]:
        run: ProviderGoalRun | None = None
        decision: GoalActionDecision | None = None

        def mutate(goal: Goal, state: GoalAutonomyState) -> dict[str, Any]:
            nonlocal run, decision
            request = GoalActionRequest(
                action_class="provider.goal.assign",
                risk=GoalActionRisk.LOW,
                delegated=True,
                provider_id=assignment.provider_id,
                estimate=assignment.estimated_usage,
            )
            decision = self._evaluate_action(goal, state, request, context)
            state.recent_decisions = [*state.recent_decisions, decision][-200:]
            if decision.disposition == GoalActionDisposition.AUTHORIZED:
                adapter = get_goal_adapter(assignment.provider_id)
                invocation = adapter.prepare(goal, assignment)
                run = ProviderGoalRun(
                    goal_id=goal.id,
                    provider_id=assignment.provider_id,
                    invocation=invocation,
                    strategy_id=assignment.strategy_id,
                    reserved_usage=assignment.estimated_usage,
                )
                state.provider_runs.append(run)
            return {
                "decision_id": decision.id,
                "disposition": decision.disposition.value,
                "run_id": run.id if run else None,
                "provider_id": assignment.provider_id,
                "mode": run.invocation.mode.value if run else None,
            }

        state = self._mutate_state(
            goal_id, context, "goal_governance.provider_assigned", mutate
        )
        assert decision is not None
        return state, run, decision

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
            for metric in (
                "actions",
                "cost_usd",
                "tokens",
                "api_calls",
                "storage_mb",
                "dispatches",
            ):
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
                    for metric in (
                        "actions",
                        "cost_usd",
                        "tokens",
                        "api_calls",
                        "storage_mb",
                        "dispatches",
                    )
                }
            )
            reserved = run.reserved_usage
            run.state = progress.state
            run.summary = progress.summary
            run.usage = progress.cumulative_usage
            run.reserved_usage = GoalUsage()
            run.blocker_refs = progress.blocker_refs
            run.interaction_refs = progress.interaction_refs
            run.artifact_refs = progress.artifact_refs
            run.evidence_claims = progress.evidence_claims
            run.updated_at = self._clock()
            state.usage = GoalUsage(
                **{
                    metric: max(
                        0,
                        getattr(state.usage, metric)
                        - getattr(reserved, metric)
                        + getattr(delta, metric),
                    )
                    for metric in (
                        "actions",
                        "cost_usd",
                        "tokens",
                        "api_calls",
                        "storage_mb",
                        "dispatches",
                    )
                }
            )
            exceeded = self._budget_reasons(goal, state.usage)
            if exceeded and run.state not in {
                ProviderRunState.COMPLETED,
                ProviderRunState.FAILED,
                ProviderRunState.CANCELLED,
            }:
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
            goal_id, context, "goal_governance.provider_progressed", mutate
        )

    def propose_goal(
        self, request: GoalProposalRequest, context: GovernanceMutationContext
    ) -> GoalProposal:
        realm_id = request.goal.realm_id
        duplicate = self._duplicate(realm_id, context.idempotency_key)
        if duplicate:
            entity_id = str(duplicate.get("entity_id") or "")
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
        duplicate = self._duplicate(realm_id, context.idempotency_key)
        if duplicate:
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
        )
        return proposal

    def get_latest_review(
        self, realm_id: str = "default"
    ) -> GoalPortfolioReview | None:
        payload = get_governance_payload(
            self.store, realm_id, REVIEW_ENTITY, CURRENT_REVIEW_ID
        )
        return GoalPortfolioReview.model_validate(payload) if payload else None

    def review_portfolio(
        self,
        request: GoalPortfolioReviewRequest,
        context: GovernanceMutationContext,
        *,
        realm_id: str = "default",
    ) -> GoalPortfolioReview:
        duplicate = self._duplicate(realm_id, context.idempotency_key)
        if duplicate:
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
        if (
            not request.independent
            or request.reviewer_principal == context.actor_principal
        ):
            raise GoalGovernanceConflict(
                "organization review must be independent of the requesting actor"
            )
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
    ) -> GoalAutonomyState:
        goal = self._require_goal(goal_id)
        duplicate = self._duplicate(goal.realm_id, context.idempotency_key)
        if duplicate:
            if duplicate.get("entity_id") != goal_id:
                raise GoalGovernanceConflict(
                    "idempotency key belongs to another governance entity"
                )
            return self.get_state(goal_id)
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
        )
        return state

    def _evaluate_action(
        self,
        goal: Goal,
        state: GoalAutonomyState,
        request: GoalActionRequest,
        context: GovernanceMutationContext,
    ) -> GoalActionDecision:
        reasons: list[str] = []
        hard_denial = False
        approval_required = False
        policy = goal.policy
        if _matches(request.action_class, policy.prohibited_actions):
            hard_denial = True
            reasons.append("the action is prohibited by the goal policy")
        safe_read = request.action_class.startswith(
            "observe"
        ) or request.action_class.endswith(".read")
        if not policy.permitted_actions and not safe_read:
            hard_denial = True
            reasons.append("the goal policy grants no executable action classes")
        elif policy.permitted_actions and not _matches(
            request.action_class, policy.permitted_actions
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
            if (
                not _is_operator(context.actor_principal)
                or request.approval_principal != context.actor_principal
            ):
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
            if budget_reasons:
                disposition = GoalActionDisposition.BUDGET_EXHAUSTED
                reasons.extend(budget_reasons)
            else:
                rate_reasons = self._rate_limit_reasons(goal, state, request)
                if rate_reasons:
                    disposition = GoalActionDisposition.RATE_LIMITED
                    reasons.extend(rate_reasons)
                else:
                    conflict_reasons = self._resource_conflict_reasons(
                        goal, request.resource_claims
                    )
                    if conflict_reasons:
                        disposition = GoalActionDisposition.RESOURCE_CONFLICT
                        reasons.extend(conflict_reasons)
                    else:
                        disposition = GoalActionDisposition.AUTHORIZED
                        reasons.append(
                            "the action is inside the active policy, budget, rate, and resource envelope"
                        )
                        state.usage = projected
                        state.resource_reservations.extend(request.resource_claims)
                        self._reserve_rate_windows(goal, state, request)
        return GoalActionDecision(
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
            decided_at=self._clock(),
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
        self, goal: Goal, state: GoalAutonomyState, request: GoalActionRequest
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

    def _resource_conflict_reasons(
        self, goal: Goal, claims: list[GoalResourceClaim]
    ) -> list[str]:
        if not claims:
            return []
        now = self._clock()
        existing: list[tuple[str, GoalResourceClaim]] = []
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
        reasons: list[str] = []
        capacities = {
            item.key: item.capacity
            for item in self.effective_policy(goal.realm_id).resource_capacities
        }
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
        for claim in claims:
            same = [(owner, item) for owner, item in existing if item.key == claim.key]
            if any(
                claim.access == ResourceAccess.EXCLUSIVE
                or item.access == ResourceAccess.EXCLUSIVE
                for _, item in same
            ):
                reasons.append(
                    f"resource {claim.key!r} conflicts with "
                    + ", ".join(sorted({owner for owner, _ in same}))
                )
            capacity = capacities.get(claim.key)
            if capacity is not None:
                used = sum(item.quantity for _, item in same)
                if used + claim.quantity > capacity:
                    reasons.append(f"resource {claim.key!r} capacity would be exceeded")
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
    ) -> GoalProposal:
        activation_key = f"{context.idempotency_key}:proposal-activated"
        if self._duplicate(proposal.realm_id, activation_key):
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
        activation_context = context.model_copy(
            update={
                "idempotency_key": activation_key,
                "expected_version": proposal.version - 1,
            }
        )
        self._commit_entity(
            proposal.realm_id,
            PROPOSAL_ENTITY,
            proposal.id,
            proposal,
            "goal_governance.proposal_activated",
            activation_context,
            {"goal_id": activated.id, "disposition": disposition.value},
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
        if context.goal_version is not None and context.goal_version != goal.version:
            raise GoalGovernanceConflict(
                f"expected goal version {context.goal_version}, current version {goal.version}"
            )
        if context.policy_revision != goal.policy.revision:
            raise GoalGovernanceConflict(
                "governance mutation was not authorized by the active goal policy"
            )
        if goal.lease.active() and (
            goal.lease.holder_instance_id != context.authority_instance_id
            or goal.lease.fencing_token != context.fencing_token
        ):
            raise GoalGovernanceConflict(
                "stale or unauthorized goal controller fencing token"
            )

    def _require_goal(self, goal_id: str) -> Goal:
        goal = self.goals.get(goal_id)
        if not goal:
            raise KeyError(goal_id)
        return goal

    def _duplicate(self, realm_id: str, key: str) -> dict[str, Any] | None:
        return find_governance_event_by_idempotency(self.store, realm_id, key)

    def _commit_entity(
        self,
        realm_id: str,
        entity_type: str,
        entity_id: str,
        entity,
        event_type: str,
        context: GovernanceMutationContext,
        payload: dict[str, Any],
    ) -> None:
        version = int(getattr(entity, "version", 1))
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
                        "payload": payload,
                    },
                },
            )
        )
