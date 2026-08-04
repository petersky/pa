from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from pa.domain.models import CardEvent, EventType
from pa.goals.models import (
    CriterionVerdict,
    Goal,
    GoalAudit,
    GoalAuditCreate,
    GoalCreate,
    GoalEvidenceCreate,
    GoalMutationContext,
    GoalProposal,
    GoalProposalCreate,
    GoalRevision,
    GoalState,
    GoalSupervisionCheckpoint,
    GoalTransition,
    GoalWakeup,
)
from pa.goals.projection import (
    find_goal_event_by_idempotency,
    get_goal_payload,
    list_goal_events,
    list_goal_payloads,
)

_TRANSITIONS: dict[GoalState, set[GoalState]] = {
    GoalState.DRAFT: {GoalState.SHAPING, GoalState.READY, GoalState.ABANDONED},
    GoalState.SHAPING: {GoalState.READY, GoalState.PAUSED, GoalState.ABANDONED},
    GoalState.READY: {GoalState.ACTIVE, GoalState.PAUSED, GoalState.ABANDONED},
    GoalState.ACTIVE: {
        GoalState.VERIFYING,
        GoalState.WAITING_OPERATOR,
        GoalState.WAITING_EXTERNAL,
        GoalState.PAUSED,
        GoalState.BLOCKED,
        GoalState.ABANDONED,
    },
    GoalState.VERIFYING: {
        GoalState.ACTIVE,
        GoalState.WAITING_OPERATOR,
        GoalState.ACHIEVED,
        GoalState.BLOCKED,
    },
    GoalState.WAITING_OPERATOR: {
        GoalState.ACTIVE,
        GoalState.PAUSED,
        GoalState.ABANDONED,
    },
    GoalState.WAITING_EXTERNAL: {
        GoalState.ACTIVE,
        GoalState.PAUSED,
        GoalState.BLOCKED,
        GoalState.ABANDONED,
    },
    GoalState.PAUSED: {GoalState.READY, GoalState.ACTIVE, GoalState.ABANDONED},
    GoalState.BLOCKED: {GoalState.ACTIVE, GoalState.PAUSED, GoalState.ABANDONED},
    GoalState.ACHIEVED: set(),
    GoalState.ABANDONED: set(),
}


def goal_transition_allowed(current: GoalState, target: GoalState) -> bool:
    return target in _TRANSITIONS[current]


class GoalConflict(ValueError):
    pass


class GoalService:
    def __init__(self, store, instance_id: str) -> None:
        self.store = store
        self.instance_id = instance_id

    def create(self, data: GoalCreate, context: GoalMutationContext) -> Goal:
        if context.expected_version != 0:
            raise GoalConflict("goal creation requires expected_version=0")
        if context.policy_revision != data.policy.revision:
            raise GoalConflict(
                "mutation policy revision does not match the goal policy"
            )
        duplicate = find_goal_event_by_idempotency(
            self.store, data.realm_id, context.idempotency_key
        )
        if duplicate:
            goal = self.get(duplicate["goal_id"], realm_id=data.realm_id)
            if goal:
                return goal
        goal = Goal(**data.model_dump(mode="python"))
        return self._commit(goal, "goal.created", context, {"revision": goal.revision})

    def get(self, goal_id: str, *, realm_id: str | None = None) -> Goal | None:
        payload = get_goal_payload(self.store, goal_id, realm_id)
        return Goal.model_validate(payload) if payload else None

    def list(
        self, *, realm_id: str | None = None, state: GoalState | None = None
    ) -> list[Goal]:
        return [
            Goal.model_validate(item)
            for item in list_goal_payloads(
                self.store, realm_id, state.value if state else None
            )
        ]

    def events(self, goal_id: str) -> list[dict[str, Any]]:
        return list_goal_events(self.store, goal_id)

    def revise(
        self, goal_id: str, change: GoalRevision, context: GoalMutationContext
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            before = goal.revision
            fields = [
                key
                for key in change.__class__.model_fields
                if key != "reason" and getattr(change, key) is not None
            ]
            for key in fields:
                setattr(goal, key, getattr(change, key))
            goal.revision += 1
            if (
                change.policy is not None
                and change.policy.revision <= context.policy_revision
            ):
                raise GoalConflict("a policy change must advance the policy revision")
            return {
                "reason": change.reason,
                "prior_revision": before,
                "fields": sorted(fields),
            }

        return self._mutate(goal_id, context, "goal.revised", mutate)

    def transition(
        self, goal_id: str, change: GoalTransition, context: GoalMutationContext
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            if change.state not in _TRANSITIONS[goal.state]:
                raise GoalConflict(
                    f"invalid goal transition: {goal.state.value} -> {change.state.value}"
                )
            if change.state == GoalState.ACHIEVED:
                if not goal.audit or goal.audit.verdict != CriterionVerdict.SATISFIED:
                    raise GoalConflict(
                        "an independently satisfied audit is required before achievement"
                    )
                if any(
                    item.verdict != CriterionVerdict.SATISFIED for item in goal.criteria
                ):
                    raise GoalConflict(
                        "every success criterion must be satisfied before achievement"
                    )
            previous = goal.state
            goal.state = change.state
            if change.progress_summary is not None:
                goal.progress_summary = change.progress_summary
            return {
                "from": previous.value,
                "to": change.state.value,
                "reason": change.reason,
            }

        return self._mutate(goal_id, context, "goal.transitioned", mutate)

    def add_evidence(
        self, goal_id: str, change: GoalEvidenceCreate, context: GoalMutationContext
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            known = {item.id for item in goal.criteria}
            if unknown := set(change.evidence.criterion_ids) - known:
                raise GoalConflict(
                    f"evidence references unknown criteria: {sorted(unknown)}"
                )
            if any(item.id == change.evidence.id for item in goal.evidence):
                raise GoalConflict(
                    f"evidence id already exists on goal: {change.evidence.id}"
                )
            verdict_criteria = set(change.criterion_verdicts)
            if unknown := verdict_criteria - known:
                raise GoalConflict(
                    f"evidence verdicts reference unknown criteria: {sorted(unknown)}"
                )
            if unmapped := verdict_criteria - set(change.evidence.criterion_ids):
                raise GoalConflict(
                    "evidence verdicts require evidence mapped to each criterion: "
                    f"{sorted(unmapped)}"
                )
            goal.evidence.append(change.evidence)
            for criterion in goal.criteria:
                if criterion.id in change.evidence.criterion_ids:
                    criterion.evidence_ids.append(change.evidence.id)
                if criterion.id in change.criterion_verdicts:
                    criterion.verdict = change.criterion_verdicts[criterion.id]
            return {
                "evidence_id": change.evidence.id,
                "criterion_ids": change.evidence.criterion_ids,
            }

        return self._mutate(goal_id, context, "goal.evidence_recorded", mutate)

    def audit(
        self, goal_id: str, change: GoalAuditCreate, context: GoalMutationContext
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            if (
                change.auditor_principal is not None
                and change.auditor_principal != context.actor_principal
            ):
                raise GoalConflict(
                    "audit principal must match the authenticated mutation actor"
                )
            if (
                not change.independent
                or context.actor_principal == goal.owner_principal
            ):
                raise GoalConflict(
                    "completion audit must be independent of the goal owner"
                )
            known_criteria = {item.id for item in goal.criteria}
            if set(change.criterion_verdicts) != known_criteria:
                raise GoalConflict("audit must include a verdict for every criterion")
            if len(change.evidence_ids) != len(set(change.evidence_ids)):
                raise GoalConflict("audit evidence ids must be unique")
            evidence_by_id = {item.id: item for item in goal.evidence}
            if unknown := set(change.evidence_ids) - evidence_by_id.keys():
                raise GoalConflict(
                    f"audit references unknown evidence: {sorted(unknown)}"
                )
            missing_evidence = {
                criterion_id
                for criterion_id in known_criteria
                if not any(
                    criterion_id in evidence_by_id[evidence_id].criterion_ids
                    for evidence_id in change.evidence_ids
                )
            }
            if missing_evidence:
                raise GoalConflict(
                    "audit must include evidence mapped to every criterion: "
                    f"{sorted(missing_evidence)}"
                )
            verdict = (
                CriterionVerdict.SATISFIED
                if all(
                    value == CriterionVerdict.SATISFIED
                    for value in change.criterion_verdicts.values()
                )
                else CriterionVerdict.UNSATISFIED
            )
            goal.audit = GoalAudit(
                auditor_principal=context.actor_principal,
                independent=True,
                verdict=verdict,
                criterion_verdicts=change.criterion_verdicts,
                evidence_ids=change.evidence_ids,
                explanation=change.explanation,
            )
            for criterion in goal.criteria:
                criterion.verdict = change.criterion_verdicts[criterion.id]
            return {"audit_id": goal.audit.id, "verdict": verdict.value}

        return self._mutate(goal_id, context, "goal.audited", mutate)

    def acquire_lease(
        self, goal_id: str, context: GoalMutationContext, *, ttl_seconds: int = 60
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            now = datetime.now(UTC)
            if (
                goal.lease.active(now)
                and goal.lease.holder_instance_id != context.authority_instance_id
            ):
                raise GoalConflict("goal controller lease is held by another instance")
            goal.lease.holder_instance_id = context.authority_instance_id
            goal.lease.fencing_token += 1
            goal.lease.expires_at = now + timedelta(seconds=ttl_seconds)
            return {
                "holder_instance_id": context.authority_instance_id,
                "fencing_token": goal.lease.fencing_token,
                "ttl_seconds": ttl_seconds,
            }

        return self._mutate(
            goal_id, context, "goal.lease_acquired", mutate, require_fence=False
        )

    def release_lease(self, goal_id: str, context: GoalMutationContext) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            self._check_fence(goal, context)
            token = goal.lease.fencing_token
            goal.lease.holder_instance_id = None
            goal.lease.expires_at = None
            return {"fencing_token": token}

        return self._mutate(
            goal_id, context, "goal.lease_released", mutate, require_fence=False
        )

    def schedule_wakeup(
        self, goal_id: str, wakeup: GoalWakeup | None, context: GoalMutationContext
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            goal.wakeup = wakeup
            return {"wakeup": wakeup.model_dump(mode="json") if wakeup else None}

        return self._mutate(goal_id, context, "goal.wakeup_scheduled", mutate)

    def submit_proposal(
        self,
        goal_id: str,
        proposal: GoalProposalCreate,
        context: GoalMutationContext,
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            if proposal.proposer_principal != context.actor_principal:
                raise GoalConflict(
                    "proposal principal must match the authenticated mutation actor"
                )
            if proposal.expected_goal_version != goal.version:
                raise GoalConflict(
                    "proposal expected_goal_version does not match the durable goal"
                )
            if proposal.policy_revision != goal.policy.revision:
                raise GoalConflict(
                    "proposal was not authored against the active policy revision"
                )
            item = GoalProposal(**proposal.model_dump(mode="python"))
            goal.proposals.append(item)
            return {
                "proposal_id": item.id,
                "action": item.action.kind,
                "proposer_role": item.proposer_role.value,
            }

        return self._mutate(goal_id, context, "goal.proposal_submitted", mutate)

    def checkpoint_supervision(
        self,
        goal_id: str,
        checkpoint: GoalSupervisionCheckpoint,
        context: GoalMutationContext,
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            target_state = checkpoint.state
            if target_state != goal.state:
                if target_state not in _TRANSITIONS[goal.state]:
                    raise GoalConflict(
                        f"invalid goal transition: {goal.state.value} -> {target_state.value}"
                    )
                if target_state == GoalState.ACHIEVED and (
                    not goal.audit
                    or goal.audit.verdict != CriterionVerdict.SATISFIED
                    or any(
                        item.verdict != CriterionVerdict.SATISFIED
                        for item in checkpoint.criteria
                    )
                ):
                    raise GoalConflict(
                        "supervisor cannot achieve a goal without a satisfied independent audit"
                    )
            for key in checkpoint.__class__.model_fields:
                if key not in {"reason", "state"}:
                    setattr(
                        goal,
                        key,
                        getattr(checkpoint, key),
                    )
            goal.state = target_state
            Goal.model_validate(goal.model_dump(mode="python"))
            return {
                "reason": checkpoint.reason,
                "cycle": checkpoint.supervision.cycle,
                "proposal_statuses": {
                    item.id: item.status.value for item in checkpoint.proposals
                },
                "work_package_states": {
                    item.id: item.state.value for item in checkpoint.work_packages
                },
                "drift_state": checkpoint.supervision.drift_state.value,
            }

        return self._mutate(goal_id, context, "goal.supervision_checkpointed", mutate)

    def _mutate(
        self,
        goal_id: str,
        context: GoalMutationContext,
        event_type: str,
        mutate: Callable[[Goal], dict[str, Any]],
        *,
        require_fence: bool = True,
    ) -> Goal:
        goal = self.get(goal_id)
        if not goal:
            raise KeyError(goal_id)
        duplicate = find_goal_event_by_idempotency(
            self.store, goal.realm_id, context.idempotency_key
        )
        if duplicate:
            if duplicate["goal_id"] != goal_id:
                raise GoalConflict("idempotency key already belongs to another goal")
            return goal
        if context.expected_version != goal.version:
            raise GoalConflict(
                f"expected version {context.expected_version}, current version {goal.version}"
            )
        if context.policy_revision != goal.policy.revision:
            raise GoalConflict(
                "mutation was not authorized by the active policy revision"
            )
        if require_fence and goal.lease.active():
            self._check_fence(goal, context)
        payload = mutate(goal)
        goal.version += 1
        goal.updated_at = datetime.now(UTC)
        return self._commit(goal, event_type, context, payload)

    @staticmethod
    def _check_fence(goal: Goal, context: GoalMutationContext) -> None:
        if (
            goal.lease.holder_instance_id != context.authority_instance_id
            or context.fencing_token != goal.lease.fencing_token
        ):
            raise GoalConflict("stale or unauthorized controller fencing token")

    def _commit(
        self,
        goal: Goal,
        event_type: str,
        context: GoalMutationContext,
        payload: dict[str, Any],
    ) -> Goal:
        try:
            goal = Goal.model_validate(goal.model_dump(mode="python"))
        except ValueError as exc:
            raise GoalConflict(f"invalid goal mutation: {exc}") from exc
        event_payload = {
            "goal": goal.model_dump(mode="json"),
            "goal_event": {
                "goal_id": goal.id,
                "event_type": event_type,
                "actor_principal": context.actor_principal,
                "authority_instance_id": context.authority_instance_id,
                "policy_revision": context.policy_revision,
                "idempotency_key": context.idempotency_key,
                "version": goal.version,
                "payload": payload,
            },
        }
        self.store.commit_event(
            CardEvent(
                type=EventType.GOAL_UPSERTED,
                realm_id=goal.realm_id,
                author_principal=context.actor_principal,
                author_instance=context.authority_instance_id or self.instance_id,
                payload=event_payload,
            )
        )
        return goal
