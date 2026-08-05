"""Deterministic authorization for durable goal proposals."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from fnmatch import fnmatchcase
from typing import Any

from pa.goals.models import (
    AuthorizationOutcome,
    CreateWorkPackageAction,
    DispatchWorkPackageAction,
    Goal,
    GoalActorRole,
    GoalAuthorizationDecision,
    GoalProposal,
    GoalState,
    ProposalStatus,
    RecordEvidenceAction,
    TransitionGoalAction,
    WorkPackageState,
)
from pa.goals.service import goal_completion_findings, goal_transition_allowed

_ACTION_AUTONOMY = {
    "request_operator": 1,
    "record_evidence": 2,
    "revise_strategy": 2,
    "create_work_package": 3,
    "dispatch_work_package": 4,
    "transition_goal": 3,
}
_ROLE_ACTIONS = {
    GoalActorRole.COORDINATOR: frozenset(_ACTION_AUTONOMY),
    GoalActorRole.EXECUTOR: frozenset({"record_evidence", "request_operator"}),
    GoalActorRole.VERIFIER: frozenset(
        {"record_evidence", "request_operator", "transition_goal"}
    ),
    GoalActorRole.CRITIC: frozenset({"revise_strategy", "request_operator"}),
}
_ACTIVE_PACKAGE_STATES = {
    WorkPackageState.DISPATCHED,
    WorkPackageState.RUNNING,
}


def _matches(action: str, patterns: list[str]) -> bool:
    return any(fnmatchcase(action, pattern) for pattern in patterns)


def _decision_hash(
    goal: Goal,
    proposal: GoalProposal,
    outcome: AuthorizationOutcome,
    reason_code: str,
) -> str:
    payload: dict[str, Any] = {
        "goal_id": goal.id,
        "proposal_id": proposal.id,
        "proposal": proposal.model_dump(
            mode="json",
            exclude={"authorization", "status", "updated_at"},
        ),
        "policy": goal.policy.model_dump(mode="json"),
        "budget": goal.budget.model_dump(mode="json"),
        "outcome": outcome.value,
        "reason_code": reason_code,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def authorize_proposal(
    goal: Goal,
    proposal: GoalProposal,
    *,
    instance_id: str,
    now: datetime | None = None,
) -> GoalAuthorizationDecision:
    """Return the same decision for the same proposal and durable policy inputs."""

    action = proposal.action.kind
    outcome = AuthorizationOutcome.AUTHORIZE
    reason_code = "policy_authorized"
    explanation = "The active policy and role authorize this proposal."

    if proposal.status != ProposalStatus.PENDING:
        outcome = AuthorizationOutcome.REJECT
        reason_code = "proposal_not_pending"
        explanation = "Only pending proposals can be authorized."
    elif proposal.policy_revision != goal.policy.revision:
        outcome = AuthorizationOutcome.REJECT
        reason_code = "stale_policy_revision"
        explanation = "The proposal was authored against a stale policy revision."
    elif proposal.expected_goal_version > goal.version:
        outcome = AuthorizationOutcome.REJECT
        reason_code = "future_goal_version"
        explanation = "The proposal references a goal version that does not exist."
    elif action not in _ROLE_ACTIONS[proposal.proposer_role]:
        outcome = AuthorizationOutcome.REJECT
        reason_code = "role_forbidden"
        explanation = (
            f"The {proposal.proposer_role.value} role cannot propose {action}."
        )
    elif _matches(action, goal.policy.prohibited_actions):
        outcome = AuthorizationOutcome.REJECT
        reason_code = "policy_prohibited"
        explanation = f"The active policy explicitly prohibits {action}."
    elif goal.policy.permitted_actions and not _matches(
        action, goal.policy.permitted_actions
    ):
        outcome = AuthorizationOutcome.REJECT
        reason_code = "outside_permitted_actions"
        explanation = f"The active policy does not permit {action}."
    if outcome == AuthorizationOutcome.AUTHORIZE and (
        proposal.proposer_role == GoalActorRole.VERIFIER
        and proposal.proposer_principal == goal.owner_principal
    ):
        outcome = AuthorizationOutcome.REJECT
        reason_code = "verifier_not_independent"
        explanation = "The verifier must be independent of the goal owner."
    elif outcome == AuthorizationOutcome.AUTHORIZE and isinstance(
        proposal.action, CreateWorkPackageAction
    ):
        known_criteria = {item.id for item in goal.criteria}
        known_packages = {item.id for item in goal.work_packages}
        if unknown := set(proposal.action.criterion_ids) - known_criteria:
            outcome = AuthorizationOutcome.REJECT
            reason_code = "unknown_criteria"
            explanation = f"Unknown success criteria: {sorted(unknown)}."
        elif unknown := set(proposal.action.depends_on) - known_packages:
            outcome = AuthorizationOutcome.REJECT
            reason_code = "unknown_dependencies"
            explanation = f"Unknown work-package dependencies: {sorted(unknown)}."
    elif outcome == AuthorizationOutcome.AUTHORIZE and isinstance(
        proposal.action, DispatchWorkPackageAction
    ):
        package = next(
            (
                item
                for item in goal.work_packages
                if item.id == proposal.action.work_package_id
            ),
            None,
        )
        active = sum(
            item.state in _ACTIVE_PACKAGE_STATES for item in goal.work_packages
        )
        if package is None:
            outcome = AuthorizationOutcome.REJECT
            reason_code = "unknown_work_package"
            explanation = "The dispatch proposal references an unknown work package."
        elif package.state not in {
            WorkPackageState.READY,
            WorkPackageState.FAILED,
            WorkPackageState.BLOCKED,
        }:
            outcome = AuthorizationOutcome.REJECT
            reason_code = "work_package_not_dispatchable"
            explanation = (
                f"Work package state {package.state.value} is not dispatchable."
            )
        elif (
            goal.budget.max_dispatches is not None
            and len(goal.linked_dispatch_ids) >= goal.budget.max_dispatches
        ):
            outcome = AuthorizationOutcome.REJECT
            reason_code = "dispatch_budget_exhausted"
            explanation = "The goal dispatch budget is exhausted."
        elif active >= goal.budget.max_concurrency:
            outcome = AuthorizationOutcome.REJECT
            reason_code = "concurrency_budget_exhausted"
            explanation = "The goal concurrency budget is exhausted."
    elif outcome == AuthorizationOutcome.AUTHORIZE and isinstance(
        proposal.action, RecordEvidenceAction
    ):
        known_criteria = {item.id for item in goal.criteria}
        if unknown := set(proposal.action.evidence.criterion_ids) - known_criteria:
            outcome = AuthorizationOutcome.REJECT
            reason_code = "unknown_criteria"
            explanation = f"Evidence references unknown criteria: {sorted(unknown)}."
    elif outcome == AuthorizationOutcome.AUTHORIZE and isinstance(
        proposal.action, TransitionGoalAction
    ):
        if not goal_transition_allowed(goal.state, proposal.action.state):
            outcome = AuthorizationOutcome.REJECT
            reason_code = "invalid_transition"
            explanation = (
                f"The goal cannot transition from {goal.state.value} "
                f"to {proposal.action.state.value}."
            )
        elif proposal.action.state == GoalState.ACHIEVED:
            findings = goal_completion_findings(
                goal, now=now or datetime.now(UTC)
            )
            if findings:
                outcome = AuthorizationOutcome.REJECT
                reason_code = "completion_requirements_unsatisfied"
                explanation = "; ".join(findings)

    if outcome == AuthorizationOutcome.AUTHORIZE and _matches(
        action, goal.policy.require_operator_for
    ):
        outcome = AuthorizationOutcome.REQUIRE_OPERATOR
        reason_code = "operator_required_by_policy"
        explanation = f"The active policy requires operator approval for {action}."
    elif outcome == AuthorizationOutcome.AUTHORIZE and (
        goal.policy.autonomy_level < _ACTION_AUTONOMY[action]
    ):
        outcome = AuthorizationOutcome.REQUIRE_OPERATOR
        reason_code = "autonomy_threshold"
        explanation = (
            f"{action} requires autonomy level {_ACTION_AUTONOMY[action]}, "
            f"but the active policy grants {goal.policy.autonomy_level}."
        )

    return GoalAuthorizationDecision(
        outcome=outcome,
        policy_revision=goal.policy.revision,
        reason_code=reason_code,
        explanation=explanation,
        decision_hash=_decision_hash(goal, proposal, outcome, reason_code),
        decided_by_instance_id=instance_id,
    )
