from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from pa.domain.models import CardEvent, EventType
from pa.goals.idempotency import operation_fingerprint, serialized_goal_mutation
from pa.goals.models import (
    CriterionVerdict,
    Goal,
    GoalActorRole,
    GoalAudit,
    GoalAuditCreate,
    GoalCreate,
    GoalEvidence,
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
    list_goal_projection_conflicts,
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


def audit_evidence_findings(
    goal: Goal,
    verdicts: dict[str, CriterionVerdict],
    evidence_ids: list[str],
    *,
    now: datetime,
) -> list[str]:
    """Evaluate one immutable audit snapshot against the full evidence policy."""

    selected = {
        item.id: item for item in goal.evidence if item.id in set(evidence_ids)
    }
    executor_identities = {
        package.executor_service_id
        for package in goal.work_packages
        if package.executor_service_id
    }
    verifier_identities = {
        package.verifier_service_id
        for package in goal.work_packages
        if package.verifier_service_id
    }
    findings: list[str] = []
    for criterion in goal.criteria:
        mapped = [
            item for item in selected.values() if criterion.id in item.criterion_ids
        ]
        if len(mapped) < criterion.minimum_evidence_count:
            findings.append(
                "audit must include evidence mapped to every criterion; "
                f"criterion {criterion.id!r} requires at least "
                f"{criterion.minimum_evidence_count} evidence records"
            )
        contradictory = [
            item.id
            for item in goal.evidence
            if criterion.id in item.criterion_ids and item.contradictory
        ]
        if contradictory:
            findings.append(
                f"criterion {criterion.id!r} has contradictory evidence "
                f"{sorted(contradictory)}"
            )
        expired = [
            item.id for item in mapped if item.expires_at and item.expires_at <= now
        ]
        if expired:
            findings.append(
                f"criterion {criterion.id!r} has expired evidence {sorted(expired)}"
            )
        if criterion.freshness_seconds:
            stale = [
                item.id
                for item in mapped
                if (now - item.observed_at).total_seconds()
                > criterion.freshness_seconds
            ]
            if stale:
                findings.append(
                    f"criterion {criterion.id!r} has stale evidence {sorted(stale)}"
                )
        present_kinds = {item.kind for item in mapped}
        missing_kinds = set(criterion.required_evidence_kinds) - present_kinds
        if missing_kinds:
            findings.append(
                f"criterion {criterion.id!r} lacks required evidence kinds "
                f"{sorted(item.value for item in missing_kinds)}"
            )
        if criterion.require_independent_verifier and not any(
            item.producer_role == GoalActorRole.VERIFIER
            and item.producer_service_id
            and item.producer_service_id in verifier_identities
            and item.producer_service_id not in executor_identities
            for item in mapped
        ):
            findings.append(
                f"criterion {criterion.id!r} requires independent verifier evidence"
            )
        if verdicts.get(criterion.id) == CriterionVerdict.SATISFIED and not mapped:
            findings.append(
                f"criterion {criterion.id!r} cannot be satisfied without evidence"
            )
    return list(dict.fromkeys(findings))


def goal_completion_findings(goal: Goal, *, now: datetime) -> list[str]:
    """Return every reason the current goal snapshot cannot become achieved."""

    audit = goal.audit
    if audit is None:
        return ["an independently satisfied audit is required before achievement"]
    findings: list[str] = []
    if not audit.independent or audit.auditor_principal == goal.owner_principal:
        findings.append("completion audit is not independent of the goal owner")
    if audit.verdict != CriterionVerdict.SATISFIED:
        findings.append("completion audit is not satisfied")
    known_criteria = {item.id for item in goal.criteria}
    if set(audit.criterion_verdicts) != known_criteria:
        findings.append("completion audit does not cover every success criterion")
    if any(
        audit.criterion_verdicts.get(item.id) != CriterionVerdict.SATISFIED
        or item.verdict != CriterionVerdict.SATISFIED
        for item in goal.criteria
    ):
        findings.append("every success criterion must be satisfied before achievement")
    executor_identities = {
        item.executor_service_id for item in goal.work_packages if item.executor_service_id
    }
    verifier_identities = {
        item.verifier_service_id for item in goal.work_packages if item.verifier_service_id
    }
    if audit.auditor_principal in executor_identities:
        findings.append("completion auditor is also an executor service")
    if audit.verifier_service_id and (
        audit.verifier_service_id not in verifier_identities
        or audit.verifier_service_id in executor_identities
    ):
        findings.append("completion verifier is not an assigned independent service")
    findings.extend(
        audit_evidence_findings(
            goal,
            audit.criterion_verdicts,
            audit.evidence_ids,
            now=now,
        )
    )
    return list(dict.fromkeys(findings))


class GoalConflict(ValueError):
    pass


class GoalService:
    def __init__(
        self,
        store,
        instance_id: str,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = store
        self.instance_id = instance_id
        self._clock = clock or (lambda: datetime.now(UTC))

    @serialized_goal_mutation
    def create(self, data: GoalCreate, context: GoalMutationContext) -> Goal:
        if context.expected_version != 0:
            raise GoalConflict("goal creation requires expected_version=0")
        if context.policy_revision != data.policy.revision:
            raise GoalConflict(
                "mutation policy revision does not match the goal policy"
            )
        fingerprint = operation_fingerprint(
            realm_id=data.realm_id,
            entity_type="goal",
            entity_id="<new>",
            event_type="goal.created",
            operation=data,
            context=context,
        )
        duplicate = find_goal_event_by_idempotency(
            self.store, data.realm_id, context.idempotency_key
        )
        if duplicate:
            self._validate_replay(
                duplicate,
                goal_id=str(duplicate["goal_id"]),
                event_type="goal.created",
                fingerprint=fingerprint,
            )
            goal = self.get(duplicate["goal_id"], realm_id=data.realm_id)
            if goal:
                return goal
        if context.authority_instance_id != self.instance_id:
            raise GoalConflict(
                "goal creation must execute on its authenticated control authority instance"
            )
        create = data.model_copy(deep=True)
        create.owner_principal = context.actor_principal
        create.policy.authored_by = context.actor_principal
        goal = Goal(**create.model_dump(mode="python"))
        goal.control_authority_instance_id = context.authority_instance_id
        return self._commit(
            goal,
            "goal.created",
            context,
            {"revision": goal.revision},
            operation_fingerprint=fingerprint,
        )

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

    def conflicts(self, goal_id: str) -> list[dict[str, Any]]:
        goal = self.get(goal_id)
        return list_goal_projection_conflicts(
            self.store,
            realm_id=goal.realm_id if goal else None,
            entity_id=goal_id,
        )

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
                value = getattr(change, key)
                if key == "policy":
                    value = value.model_copy(deep=True)
                    value.authored_by = context.actor_principal
                setattr(goal, key, value)
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

        return self._mutate(
            goal_id,
            context,
            "goal.revised",
            mutate,
            operation=change,
        )

    def transition(
        self, goal_id: str, change: GoalTransition, context: GoalMutationContext
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            if change.state not in _TRANSITIONS[goal.state]:
                raise GoalConflict(
                    f"invalid goal transition: {goal.state.value} -> {change.state.value}"
                )
            if change.state == GoalState.ACHIEVED:
                findings = goal_completion_findings(goal, now=self._clock())
                if findings:
                    raise GoalConflict(
                        "completion evidence is no longer valid: "
                        + "; ".join(findings)
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

        return self._mutate(
            goal_id,
            context,
            "goal.transitioned",
            mutate,
            operation=change,
        )

    def add_evidence(
        self, goal_id: str, change: GoalEvidenceCreate, context: GoalMutationContext
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            evidence = self.ingest_evidence_snapshot(goal, change, context=context)
            return {
                "evidence_id": evidence.id,
                "criterion_ids": evidence.criterion_ids,
                "recorded_by_principal": evidence.recorded_by_principal,
                "recorded_by_instance_id": evidence.recorded_by_instance_id,
            }

        return self._mutate(
            goal_id,
            context,
            "goal.evidence_recorded",
            mutate,
            operation=change,
        )

    def ingest_evidence_snapshot(
        self,
        goal: Goal,
        change: GoalEvidenceCreate,
        *,
        context: GoalMutationContext,
        now: datetime | None = None,
    ) -> GoalEvidence:
        """Validate and record evidence on an in-memory goal checkpoint.

        Every evidence path, including autonomous proposals and dispatch completion,
        must pass through this boundary so provenance is derived from runtime
        identity rather than accepted from a proposal body.
        """

        evidence = change.evidence.model_copy(deep=True)
        known = {item.id for item in goal.criteria}
        if unknown := set(evidence.criterion_ids) - known:
            raise GoalConflict(
                f"evidence references unknown criteria: {sorted(unknown)}"
            )
        if any(item.id == evidence.id for item in goal.evidence):
            raise GoalConflict(f"evidence id already exists on goal: {evidence.id}")
        verdict_criteria = set(change.criterion_verdicts)
        if unknown := verdict_criteria - known:
            raise GoalConflict(
                f"evidence verdicts reference unknown criteria: {sorted(unknown)}"
            )
        if unmapped := verdict_criteria - set(evidence.criterion_ids):
            raise GoalConflict(
                "evidence verdicts require evidence mapped to each criterion: "
                f"{sorted(unmapped)}"
            )
        observed_now = now or self._clock()
        if evidence.observed_at > observed_now + timedelta(minutes=5):
            raise GoalConflict("evidence observed_at cannot be in the future")
        if evidence.expires_at and evidence.expires_at <= evidence.observed_at:
            raise GoalConflict("evidence expiry must follow its observation")

        evidence.recorded_by_principal = context.actor_principal
        evidence.recorded_by_instance_id = context.authority_instance_id
        # Producer identity is authoritative runtime provenance, never a
        # caller-supplied assertion from the evidence body.
        evidence.producer_role = None
        evidence.producer_service_id = None
        executor_identities = {
            item.executor_service_id
            for item in goal.work_packages
            if item.executor_service_id
        }
        verifier_identities = {
            item.verifier_service_id
            for item in goal.work_packages
            if item.verifier_service_id
        }
        if context.actor_principal.startswith("service:goal-verifier:"):
            if (
                context.actor_principal not in verifier_identities
                or context.actor_principal in executor_identities
            ):
                raise GoalConflict(
                    "verifier evidence requires an assigned independent verifier service"
                )
            evidence.producer_role = GoalActorRole.VERIFIER
            evidence.producer_service_id = context.actor_principal
        elif context.actor_principal.startswith("service:goal-executor:"):
            if (
                context.actor_principal not in executor_identities
                or context.actor_principal in verifier_identities
            ):
                raise GoalConflict(
                    "executor evidence requires the assigned executor service"
                )
            evidence.producer_role = GoalActorRole.EXECUTOR
            evidence.producer_service_id = context.actor_principal

        goal.evidence.append(evidence)
        for criterion in goal.criteria:
            if criterion.id in evidence.criterion_ids:
                criterion.evidence_ids.append(evidence.id)
            if criterion.id in change.criterion_verdicts:
                criterion.verdict = change.criterion_verdicts[criterion.id]
        return evidence

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
            findings = self._audit_evidence_findings(
                goal, change.criterion_verdicts, change.evidence_ids
            )
            if findings:
                raise GoalConflict(
                    "audit evidence policy failed: " + "; ".join(findings)
                )
            executor_identities = {
                item.executor_service_id
                for item in goal.work_packages
                if item.executor_service_id
            }
            verifier_identities = {
                item.verifier_service_id
                for item in goal.work_packages
                if item.verifier_service_id
            }
            if context.actor_principal in executor_identities:
                raise GoalConflict(
                    "completion auditor must be distinct from every executor service"
                )
            if context.actor_principal.startswith("service:goal-verifier:") and (
                context.actor_principal not in verifier_identities
                or context.actor_principal in executor_identities
            ):
                raise GoalConflict(
                    "completion auditor must be an assigned independent verifier service"
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
                auditor_instance_id=context.authority_instance_id,
                verifier_service_id=(
                    context.actor_principal
                    if context.actor_principal.startswith("service:goal-verifier:")
                    else None
                ),
                independent=True,
                verdict=verdict,
                criterion_verdicts=change.criterion_verdicts,
                evidence_ids=change.evidence_ids,
                explanation=change.explanation,
            )
            for criterion in goal.criteria:
                criterion.verdict = change.criterion_verdicts[criterion.id]
            return {"audit_id": goal.audit.id, "verdict": verdict.value}

        return self._mutate(
            goal_id,
            context,
            "goal.audited",
            mutate,
            operation=change,
        )

    def _audit_evidence_findings(
        self,
        goal: Goal,
        verdicts: dict[str, CriterionVerdict],
        evidence_ids: list[str],
    ) -> list[str]:
        """Evaluate the criterion policy against one immutable audit snapshot."""
        return audit_evidence_findings(
            goal,
            verdicts,
            evidence_ids,
            now=self._clock(),
        )

    def acquire_lease(
        self, goal_id: str, context: GoalMutationContext, *, ttl_seconds: int = 60
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            now = self._clock()
            eligible = sorted(
                set(
                    goal.wakeup.eligible_instance_ids
                    if goal.wakeup and goal.wakeup.eligible_instance_ids
                    else goal.lease.eligible_instance_ids
                )
            )
            if eligible and context.authority_instance_id not in eligible:
                raise GoalConflict(
                    "authority instance is not eligible to claim this goal"
                )
            if (
                goal.lease.active(now)
                and goal.lease.holder_instance_id != context.authority_instance_id
            ):
                raise GoalConflict("goal controller lease is held by another instance")
            goal.lease.holder_instance_id = context.authority_instance_id
            goal.lease.fencing_token += 1
            goal.lease.expires_at = now + timedelta(seconds=ttl_seconds)
            goal.lease.claim_id = context.idempotency_key
            goal.lease.eligible_instance_ids = eligible
            goal.lease.acquired_at = now
            if goal.wakeup:
                goal.wakeup.claimed_by_instance_id = context.authority_instance_id
                goal.wakeup.claimed_at = now
            return {
                "holder_instance_id": context.authority_instance_id,
                "fencing_token": goal.lease.fencing_token,
                "ttl_seconds": ttl_seconds,
            }

        return self._mutate(
            goal_id,
            context,
            "goal.lease_acquired",
            mutate,
            require_fence=False,
            operation={"ttl_seconds": ttl_seconds},
        )

    def release_lease(self, goal_id: str, context: GoalMutationContext) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            self._check_fence(goal, context)
            token = goal.lease.fencing_token
            goal.lease.holder_instance_id = None
            goal.lease.expires_at = None
            goal.lease.claim_id = None
            if goal.wakeup:
                goal.wakeup.claimed_by_instance_id = None
                goal.wakeup.claimed_at = None
            return {"fencing_token": token}

        return self._mutate(
            goal_id,
            context,
            "goal.lease_released",
            mutate,
            require_fence=False,
            operation={},
        )

    def schedule_wakeup(
        self, goal_id: str, wakeup: GoalWakeup | None, context: GoalMutationContext
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            candidate = wakeup.model_copy(deep=True) if wakeup else None
            eligible = sorted(set(candidate.eligible_instance_ids)) if candidate else []
            if candidate:
                candidate.eligible_instance_ids = eligible
            previous_authority = goal.control_authority_instance_id
            transferred = bool(
                candidate
                and eligible
                and previous_authority
                and previous_authority not in eligible
            )
            if transferred:
                goal.control_authority_instance_id = eligible[0]
                goal.lease.holder_instance_id = None
                goal.lease.expires_at = None
                goal.lease.claim_id = None
                goal.lease.acquired_at = None
                goal.lease.fencing_token += 1
                candidate.claimed_by_instance_id = None
                candidate.claimed_at = None
            goal.wakeup = candidate
            return {
                "wakeup": candidate.model_dump(mode="json") if candidate else None,
                "previous_control_authority_instance_id": previous_authority,
                "control_authority_instance_id": goal.control_authority_instance_id,
                "authority_transferred": transferred,
                "fencing_token": goal.lease.fencing_token,
            }

        return self._mutate(
            goal_id,
            context,
            "goal.wakeup_scheduled",
            mutate,
            operation=wakeup,
        )

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
            derived_role = self._proposal_role(goal, context.actor_principal)
            if proposal.proposer_role != derived_role:
                raise GoalConflict(
                    "proposal role does not match the authenticated actor assignment: "
                    f"expected {derived_role.value}"
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

        return self._mutate(
            goal_id,
            context,
            "goal.proposal_submitted",
            mutate,
            operation=proposal,
        )

    @staticmethod
    def _proposal_role(goal: Goal, actor_principal: str) -> GoalActorRole:
        for package in goal.work_packages:
            if (
                package.role == GoalActorRole.VERIFIER
                and package.verifier_service_id == actor_principal
            ):
                return GoalActorRole.VERIFIER
            if package.executor_service_id != actor_principal:
                continue
            if package.role == GoalActorRole.CRITIC:
                return GoalActorRole.CRITIC
            return GoalActorRole.EXECUTOR
        return GoalActorRole.COORDINATOR

    def checkpoint_supervision(
        self,
        goal_id: str,
        checkpoint: GoalSupervisionCheckpoint,
        context: GoalMutationContext,
    ) -> Goal:
        def mutate(goal: Goal) -> dict[str, Any]:
            target_state = checkpoint.state
            if (
                target_state != goal.state
                and target_state not in _TRANSITIONS[goal.state]
            ):
                raise GoalConflict(
                    f"invalid goal transition: {goal.state.value} -> {target_state.value}"
                )
            candidate = goal.model_copy(deep=True)
            for key in checkpoint.__class__.model_fields:
                if key not in {"reason", "state"}:
                    setattr(
                        candidate,
                        key,
                        getattr(checkpoint, key),
                    )
            candidate.state = target_state
            if target_state == GoalState.ACHIEVED:
                findings = goal_completion_findings(candidate, now=self._clock())
                if findings:
                    raise GoalConflict(
                        "supervisor completion requirements failed: "
                        + "; ".join(findings)
                    )
            candidate = Goal.model_validate(candidate.model_dump(mode="python"))
            for key in Goal.model_fields:
                setattr(goal, key, getattr(candidate, key))
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

        return self._mutate(
            goal_id,
            context,
            "goal.supervision_checkpointed",
            mutate,
            operation=checkpoint,
        )

    @serialized_goal_mutation
    def _mutate(
        self,
        goal_id: str,
        context: GoalMutationContext,
        event_type: str,
        mutate: Callable[[Goal], dict[str, Any]],
        *,
        require_fence: bool = True,
        operation: Any = None,
    ) -> Goal:
        goal = self.get(goal_id)
        if not goal:
            raise KeyError(goal_id)
        fingerprint = operation_fingerprint(
            realm_id=goal.realm_id,
            entity_type="goal",
            entity_id=goal_id,
            event_type=event_type,
            operation=operation,
            context=context,
        )
        duplicate = find_goal_event_by_idempotency(
            self.store, goal.realm_id, context.idempotency_key
        )
        if duplicate:
            self._validate_replay(
                duplicate,
                goal_id=goal_id,
                event_type=event_type,
                fingerprint=fingerprint,
            )
            return goal
        if context.expected_version != goal.version:
            raise GoalConflict(
                f"expected version {context.expected_version}, current version {goal.version}"
            )
        if context.policy_revision != goal.policy.revision:
            raise GoalConflict(
                "mutation was not authorized by the active policy revision"
            )
        if (
            goal.control_authority_instance_id
            and context.authority_instance_id != goal.control_authority_instance_id
        ):
            raise GoalConflict(
                "stale or unauthorized control authority fencing token; "
                "route the mutation through the durable control authority"
            )
        if goal.control_authority_instance_id is None:
            raise GoalConflict(
                "goal has no durable control authority; rebuild legacy history before mutation"
            )
        if self.instance_id != goal.control_authority_instance_id:
            raise GoalConflict(
                "goal mutation must execute on the durable control authority instance"
            )
        if require_fence and goal.lease.active(self._clock()):
            self._check_fence(goal, context)
        if require_fence and context.actor_principal.startswith("service:"):
            if not goal.lease.active(self._clock()):
                raise GoalConflict(
                    "service mutations require an active controller lease"
                )
            self._check_fence(goal, context)
        payload = mutate(goal)
        goal.version += 1
        goal.updated_at = self._clock()
        return self._commit(
            goal,
            event_type,
            context,
            payload,
            operation_fingerprint=fingerprint,
        )

    @staticmethod
    def _validate_replay(
        duplicate: dict[str, Any],
        *,
        goal_id: str,
        event_type: str,
        fingerprint: str,
    ) -> None:
        if duplicate["goal_id"] != goal_id:
            raise GoalConflict("idempotency key already belongs to another goal")
        if duplicate.get("event_type") != event_type:
            raise GoalConflict(
                "idempotency key already belongs to another goal operation"
            )
        recorded = str(duplicate.get("operation_fingerprint") or "")
        if not recorded:
            raise GoalConflict(
                "legacy idempotency event cannot be replayed without an exact operation fingerprint"
            )
        if recorded != fingerprint:
            raise GoalConflict(
                "idempotency key already belongs to a different goal operation"
            )

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
        *,
        operation_fingerprint: str,
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
                "operation_fingerprint": operation_fingerprint,
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
