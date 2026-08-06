"""Durable goal REST, MCP, and web surfaces."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from pa.auth.middleware import get_principal_id
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.ui.pages import PageDefinition
from pa.goals.advanced_models import (
    AssignedServiceProviderProgress,
    GoalActionApply,
    GoalActionDisposition,
    GoalActionRelease,
    GoalActionRequest,
    GoalGovernancePolicy,
    GoalPortfolioReviewRequest,
    GoalProposalRequest,
    GoalProposalReview,
    GoalReservationState,
    GoalStrategyPortfolioUpdate,
    GoalUsage,
    GovernanceMutationContext,
    ProviderGoalAssignment,
    ProviderGoalProgress,
)
from pa.goals.governance import (
    GoalAssignedServiceAuthorization,
    GoalAssignedServiceCredentialError,
    GoalGovernanceService,
)
from pa.goals.models import (
    AssignedServiceGoalAuditCreate,
    AssignedServiceGoalEvidenceCreate,
    AssignedServiceGoalProposalCreate,
    CreateWorkPackageAction,
    DispatchWorkPackageAction,
    GoalActorRole,
    GoalAuditCreate,
    GoalCreate,
    GoalEvidence,
    GoalEvidenceCreate,
    GoalMutationContext,
    GoalProposalCreate,
    GoalRevision,
    GoalState,
    GoalTransition,
    GoalWakeup,
    RecordEvidenceAction,
    RequestOperatorAction,
    ReviseStrategyAction,
    TransitionGoalAction,
)
from pa.goals.projection import find_goal_event_by_idempotency
from pa.goals.providers import list_goal_adapter_capabilities
from pa.goals.service import GoalConflict, GoalService
from pa.goals.supervisor import GoalSupervisor

router = APIRouter()
logger = logging.getLogger(__name__)


def _authoritative_instance_id(request: Request) -> str:
    """Resolve mutation authority from authenticated transport, never a header."""

    authenticated = getattr(request.state, "authenticated_instance_id", None)
    if getattr(request.state, "instance_authenticated", False) and authenticated:
        return str(authenticated)
    return request.app.state.ctx.settings.instance_id


def _service(request: Request) -> GoalService:
    return request.app.state.ctx.require_service("goal_service")


def _governance(request: Request) -> GoalGovernanceService:
    return request.app.state.ctx.require_service("goal_governance")


def _assigned_service_authorization(
    request: Request,
    *,
    required_roles: set[GoalActorRole] | None = None,
    expected_goal_id: str | None = None,
    expected_run_id: str | None = None,
) -> GoalAssignedServiceAuthorization:
    credential = getattr(request.state, "provider_run_credential", None) or ""
    try:
        authorization = _governance(request).resolve_assigned_service_credential(
            credential,
            required_roles=required_roles,
            expected_goal_id=expected_goal_id,
            expected_run_id=expected_run_id,
        )
    except (GoalAssignedServiceCredentialError, KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "invalid_assigned_service_credential",
                "message": (
                    "This operation requires a live credential for its exact "
                    "assigned Goal execution identity."
                ),
            },
        ) from exc
    scope = authorization.scope
    request.state.principal_id = scope.assigned_service_principal
    request.state.authenticated_instance_id = scope.authority_instance_id
    return authorization


def _assigned_goal_context(
    authorization: GoalAssignedServiceAuthorization,
    *,
    expected_version: int,
    policy_revision: int,
    idempotency_key: str,
) -> GoalMutationContext:
    scope = authorization.scope
    return GoalMutationContext(
        actor_principal=scope.assigned_service_principal,
        authority_instance_id=scope.authority_instance_id,
        idempotency_key=idempotency_key,
        expected_version=expected_version,
        policy_revision=policy_revision,
        fencing_token=scope.fencing_token,
    )


def _assigned_scope_violation(message: str) -> None:
    raise HTTPException(
        status_code=403,
        detail={
            "code": "assigned_work_package_scope_violation",
            "message": message,
        },
    )


def _require_assigned_criteria(
    authorization: GoalAssignedServiceAuthorization,
    criterion_ids,
    *,
    allow_empty: bool = False,
) -> None:
    allowed = set(authorization.work_package.criterion_ids)
    requested = set(criterion_ids)
    if (not requested and not allow_empty) or not requested.issubset(allowed):
        _assigned_scope_violation(
            "Assigned services may reference only criteria in their work package."
        )


def _assigned_evidence_provenance(
    authorization: GoalAssignedServiceAuthorization,
) -> dict[str, str | int]:
    scope = authorization.scope
    return {
        "source": "pa-assigned-service",
        "goal_id": scope.goal_id,
        "work_package_id": scope.work_package_id,
        "run_id": scope.run_id,
        "session_id": scope.session_id,
        "provider_id": scope.provider_id,
        "target_instance_id": scope.target_instance_id,
        "authority_instance_id": scope.authority_instance_id,
        "fencing_token": scope.fencing_token,
        "service_role": scope.service_role.value,
        "assigned_service_principal": scope.assigned_service_principal,
    }


def _assigned_goal_evidence(
    authorization: GoalAssignedServiceAuthorization,
    evidence,
) -> GoalEvidence:
    _require_assigned_criteria(authorization, evidence.criterion_ids)
    payload = evidence.model_dump(mode="python")
    # Caller provenance is deliberately discarded: this binding is evidence
    # about the exact governed execution, not an agent-authored identity claim.
    payload["provenance"] = _assigned_evidence_provenance(authorization)
    return GoalEvidence.model_validate(payload)


def _assigned_goal_proposal(
    authorization: GoalAssignedServiceAuthorization,
    body: AssignedServiceGoalProposalCreate,
) -> GoalProposalCreate:
    scope = authorization.scope
    action = body.action
    if action.kind == "record_evidence":
        _require_assigned_criteria(
            authorization, action.criterion_verdicts, allow_empty=True
        )
        action = RecordEvidenceAction(
            evidence=_assigned_goal_evidence(authorization, action.evidence),
            criterion_verdicts=action.criterion_verdicts,
        )
    elif action.kind == "create_work_package":
        _require_assigned_criteria(authorization, action.criterion_ids)
        action = CreateWorkPackageAction(
            **action.model_dump(mode="python"),
            role=scope.service_role,
        )
    elif action.kind == "dispatch_work_package":
        action = DispatchWorkPackageAction(
            **action.model_dump(mode="python"),
            work_package_id=scope.work_package_id,
            placement_policy="best_match",
            provider=scope.provider_id,
        )
    elif action.kind == "request_operator":
        action = RequestOperatorAction.model_validate(action.model_dump(mode="python"))
    elif action.kind == "revise_strategy":
        action = ReviseStrategyAction.model_validate(action.model_dump(mode="python"))
    elif action.kind == "transition_goal":
        action = TransitionGoalAction.model_validate(action.model_dump(mode="python"))
    return GoalProposalCreate(
        proposer_principal=scope.assigned_service_principal,
        proposer_role=scope.service_role,
        action=action,
        rationale=body.rationale,
        expected_goal_version=body.expected_goal_version,
        policy_revision=body.policy_revision,
    )


def _assigned_goal_evidence_change(
    authorization: GoalAssignedServiceAuthorization,
    body: AssignedServiceGoalEvidenceCreate,
) -> GoalEvidenceCreate:
    _require_assigned_criteria(
        authorization, body.criterion_verdicts, allow_empty=True
    )
    return GoalEvidenceCreate(
        evidence=_assigned_goal_evidence(authorization, body.evidence),
        criterion_verdicts=body.criterion_verdicts,
    )


def _assigned_goal_audit_change(
    authorization: GoalAssignedServiceAuthorization,
    body: AssignedServiceGoalAuditCreate,
) -> GoalAuditCreate:
    _require_assigned_criteria(
        authorization, body.criterion_verdicts, allow_empty=True
    )
    allowed = set(authorization.work_package.criterion_ids)
    evidence_by_id = {item.id: item for item in authorization.goal.evidence}
    if any(
        evidence_id not in evidence_by_id
        or not set(evidence_by_id[evidence_id].criterion_ids).issubset(allowed)
        for evidence_id in body.evidence_ids
    ):
        _assigned_scope_violation(
            "Assigned verifier audits may cite only evidence for their work-package criteria."
        )
    return GoalAuditCreate(
        auditor_principal=None,
        **body.model_dump(mode="python"),
    )


def _governance_context(
    request: Request,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    goal_version: int | None = None,
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
    fencing_token: Annotated[
        int | None, Header(alias="X-PA-Goal-Fencing-Token")
    ] = None,
) -> GovernanceMutationContext:
    authenticated_actor = get_principal_id(request) or "user:local"
    return GovernanceMutationContext(
        actor_principal=authenticated_actor,
        authority_instance_id=_authoritative_instance_id(request),
        idempotency_key=idempotency_key,
        expected_version=expected_version,
        policy_revision=policy_revision,
        goal_version=goal_version,
        fencing_token=fencing_token,
    )


def _context(
    request: Request,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
    fencing_token: Annotated[
        int | None, Header(alias="X-PA-Goal-Fencing-Token")
    ] = None,
) -> GoalMutationContext:
    authenticated_actor = get_principal_id(request) or "user:local"
    return GoalMutationContext(
        actor_principal=authenticated_actor,
        authority_instance_id=_authoritative_instance_id(request),
        idempotency_key=idempotency_key,
        expected_version=expected_version,
        policy_revision=policy_revision,
        fencing_token=fencing_token,
    )


def _run(operation):
    try:
        return operation()
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="goal not found") from exc
    except GoalConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _wake_supervisor(request: Request) -> None:
    supervisor: GoalSupervisor | None = request.app.state.ctx.services.get(
        "goal_supervisor"
    )
    if supervisor:
        supervisor.wake()


def _provider_run_public(run, *, include_invocation: bool = False) -> dict | None:
    if run is None:
        return None
    payload = run.model_dump(mode="json")
    payload.pop("progress_credential_hash", None)
    if not include_invocation:
        payload.pop("invocation", None)
        payload["launch_required"] = run.launched_at is None
    return payload


def _redact_unlaunched_provider_runs(payload: dict) -> dict:
    """Keep provider invocations behind the canonical launch/apply gate."""

    public = copy.deepcopy(payload)
    for run in public.get("provider_runs", []):
        if not isinstance(run, dict):
            continue
        run.pop("progress_credential_hash", None)
        if run.get("launched_at") is None:
            run.pop("invocation", None)
            run["launch_required"] = True
    return public


def _goal_portfolio_public(payload: dict) -> dict:
    public = copy.deepcopy(payload)
    for entry in public.get("goals", []):
        if isinstance(entry, dict) and isinstance(entry.get("autonomy"), dict):
            entry["autonomy"] = _redact_unlaunched_provider_runs(entry["autonomy"])
    return public


def _projection_conflicts_public(conflicts: list[dict]) -> list[dict]:
    public = copy.deepcopy(conflicts)
    for conflict in public:
        for field in ("canonical_payload", "competing_payload"):
            encoded = conflict.get(field)
            if not isinstance(encoded, str):
                continue
            try:
                payload = json.loads(encoded)
            except TypeError, ValueError:
                continue
            if isinstance(payload, dict) and isinstance(
                payload.get("provider_runs"), list
            ):
                conflict[field] = json.dumps(
                    _redact_unlaunched_provider_runs(payload),
                    sort_keys=True,
                    separators=(",", ":"),
                )
    return public


def _governed_goal_mutation(
    request: Request,
    goal_id: str,
    action_class: str,
    goal_context: GoalMutationContext,
    operation,
    *,
    operation_payload,
    delegated: bool = False,
    estimate: GoalUsage | None = None,
):
    governance = _governance(request)
    goal_service = _service(request)
    current_goal = goal_service.get(goal_id)
    goal_mutation_event = (
        find_goal_event_by_idempotency(
            goal_service.store, current_goal.realm_id, goal_context.idempotency_key
        )
        if current_goal
        else None
    )
    goal_mutation_exists = goal_mutation_event is not None
    actor = goal_context.actor_principal
    encoded_operation = (
        operation_payload.model_dump(mode="json", exclude_unset=True)
        if hasattr(operation_payload, "model_dump")
        else operation_payload
    )
    operation_digest = hashlib.sha256(
        json.dumps(
            {"action_class": action_class, "payload": encoded_operation},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    action = GoalActionRequest(
        action_class=action_class,
        operation_digest=operation_digest,
        delegated=delegated,
        estimate=estimate or GoalUsage(actions=1),
        operator_approved=actor.startswith(("user:", "role:admin")),
        approval_principal=(
            actor if actor.startswith(("user:", "role:admin")) else None
        ),
    )

    def governance_context(
        version: int,
        key: str,
        goal_version: int,
        policy_revision: int | None = None,
    ):
        return GovernanceMutationContext(
            actor_principal=actor,
            authority_instance_id=goal_context.authority_instance_id,
            idempotency_key=key,
            expected_version=version,
            policy_revision=policy_revision or goal_context.policy_revision,
            goal_version=goal_version,
            fencing_token=goal_context.fencing_token,
        )

    state = governance.get_state(goal_id)
    lifecycle_key = (
        "goal-mutation:"
        + hashlib.sha256(goal_context.idempotency_key.encode()).hexdigest()
    )
    related = sorted(
        (
            item
            for item in state.action_reservations
            if item.idempotency_key.startswith(f"{lifecycle_key}:reserve:")
        ),
        key=lambda item: (item.created_at, item.id),
    )
    if any(
        item.request != action
        or item.actor_principal != actor
        or item.authority_instance_id != goal_context.authority_instance_id
        or item.policy_revision != goal_context.policy_revision
        or item.goal_version != goal_context.expected_version
        or item.fencing_token != goal_context.fencing_token
        for item in related
    ):
        raise GoalConflict(
            "idempotency key already belongs to a different governed mutation"
        )
    attempt = 0
    if related:
        latest = related[-1]
        try:
            latest_attempt = int(latest.idempotency_key.rsplit(":", 1)[-1])
        except ValueError:
            latest_attempt = len(related) - 1
        if goal_mutation_exists:
            selected = next(
                (
                    item
                    for item in reversed(related)
                    if item.state == GoalReservationState.APPLIED
                    or item.release_reason == "goal mutation committed"
                ),
                latest,
            )
            try:
                attempt = int(selected.idempotency_key.rsplit(":", 1)[-1])
            except ValueError:
                attempt = latest_attempt
        elif latest.state != GoalReservationState.RELEASED:
            attempt = latest_attempt
        elif latest.release_reason == "goal mutation failed before commit":
            attempt = latest_attempt + 1
        else:
            attempt = latest_attempt

    reserve_key = f"{lifecycle_key}:reserve:{attempt}"
    apply_key = f"{lifecycle_key}:apply:{attempt}"
    release_failed_key = f"{lifecycle_key}:release-failed:{attempt}"
    release_applied_key = f"{lifecycle_key}:release-applied:{attempt}"
    if goal_mutation_exists:
        if not related or current_goal is None:
            raise GoalConflict(
                "durable goal mutation is missing its governed reservation identity"
            )
        selected = next(
            (
                item
                for item in reversed(related)
                if item.state == GoalReservationState.APPLIED
                or item.release_reason == "goal mutation committed"
            ),
            related[-1],
        )
        if selected.state == GoalReservationState.RESERVED:
            raise GoalConflict(
                "durable goal mutation has an unapplied governance reservation"
            )
        if (
            selected.state != GoalReservationState.RELEASED
            or selected.release_reason != "goal mutation committed"
            or selected.actual_usage != action.estimate
        ):
            current = governance.get_state(goal_id)
            governance.release_action(
                goal_id,
                selected.id,
                governance_context(
                    current.version,
                    release_applied_key,
                    current_goal.version,
                    current_goal.policy.revision,
                ),
                actual_usage=action.estimate,
                reason="goal mutation committed",
                reconcile_terminal=True,
            )
        return current_goal
    state, decision = governance.authorize_action(
        goal_id,
        action,
        governance_context(
            state.version,
            reserve_key,
            goal_context.expected_version,
        ),
    )
    if decision.disposition != GoalActionDisposition.AUTHORIZED:
        raise GoalConflict(
            "canonical governance denied the mutation: " + "; ".join(decision.reasons)
        )
    reservation_id = decision.reservation_id or ""
    state, applied = governance.apply_action(
        goal_id,
        reservation_id,
        governance_context(
            state.version,
            apply_key,
            goal_context.expected_version,
        ),
    )
    if applied.disposition != GoalActionDisposition.AUTHORIZED:
        raise GoalConflict(
            "canonical governance denied the mutation at apply time: "
            + "; ".join(applied.reasons)
        )
    try:
        result = operation()
    except BaseException:
        current_goal = _service(request).get(goal_id)
        current = governance.get_state(goal_id)
        governance.release_action(
            goal_id,
            reservation_id,
            governance_context(
                current.version,
                release_failed_key,
                current_goal.version if current_goal else goal_context.expected_version,
                current_goal.policy.revision if current_goal else None,
            ),
            actual_usage=GoalUsage(),
            reason="goal mutation failed before commit",
        )
        raise
    current_goal = _service(request).get(goal_id)
    current = governance.get_state(goal_id)
    governance.release_action(
        goal_id,
        reservation_id,
        governance_context(
            current.version,
            release_applied_key,
            current_goal.version if current_goal else goal_context.expected_version,
            current_goal.policy.revision if current_goal else None,
        ),
        actual_usage=action.estimate,
        reason="goal mutation committed",
        reconcile_terminal=goal_mutation_exists,
    )
    return result


def apply_assigned_service_goal_proposal(
    request: Request,
    body: AssignedServiceGoalProposalCreate,
    authorization: GoalAssignedServiceAuthorization,
    *,
    expected_version: int,
    policy_revision: int,
    idempotency_key: str,
) -> dict:
    scope = authorization.scope
    context = _assigned_goal_context(
        authorization,
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )
    proposal = _assigned_goal_proposal(authorization, body)
    result = _run(
        lambda: _governed_goal_mutation(
            request,
            scope.goal_id,
            proposal.action.kind,
            context,
            lambda: _service(request).submit_proposal(
                scope.goal_id, proposal, context
            ),
            operation_payload=proposal,
            delegated=proposal.action.kind == "dispatch_work_package",
        )
    )
    _wake_supervisor(request)
    return result.model_dump(mode="json")


@router.post("/goal-assigned-service/proposals", status_code=202)
def submit_assigned_service_goal_proposal(
    request: Request,
    body: AssignedServiceGoalProposalCreate,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    authorization = _assigned_service_authorization(
        request,
        required_roles={GoalActorRole.EXECUTOR, GoalActorRole.VERIFIER},
    )
    return apply_assigned_service_goal_proposal(
        request,
        body,
        authorization,
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )


def apply_assigned_service_goal_evidence(
    request: Request,
    body: AssignedServiceGoalEvidenceCreate,
    authorization: GoalAssignedServiceAuthorization,
    *,
    expected_version: int,
    policy_revision: int,
    idempotency_key: str,
) -> dict:
    scope = authorization.scope
    context = _assigned_goal_context(
        authorization,
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )
    change = _assigned_goal_evidence_change(authorization, body)
    return _run(
        lambda: _governed_goal_mutation(
            request,
            scope.goal_id,
            "record_evidence",
            context,
            lambda: _service(request).add_evidence(scope.goal_id, change, context),
            operation_payload=change,
        )
    ).model_dump(mode="json")


@router.post("/goal-assigned-service/evidence")
def record_assigned_service_goal_evidence(
    request: Request,
    body: AssignedServiceGoalEvidenceCreate,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    authorization = _assigned_service_authorization(
        request,
        required_roles={GoalActorRole.EXECUTOR, GoalActorRole.VERIFIER},
    )
    return apply_assigned_service_goal_evidence(
        request,
        body,
        authorization,
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )


def apply_assigned_service_goal_audit(
    request: Request,
    body: AssignedServiceGoalAuditCreate,
    authorization: GoalAssignedServiceAuthorization,
    *,
    expected_version: int,
    policy_revision: int,
    idempotency_key: str,
) -> dict:
    scope = authorization.scope
    context = _assigned_goal_context(
        authorization,
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )
    change = _assigned_goal_audit_change(authorization, body)
    return _run(
        lambda: _governed_goal_mutation(
            request,
            scope.goal_id,
            "audit_goal",
            context,
            lambda: _service(request).audit(scope.goal_id, change, context),
            operation_payload=change,
        )
    ).model_dump(mode="json")


@router.post("/goal-assigned-service/audit")
def audit_assigned_service_goal(
    request: Request,
    body: AssignedServiceGoalAuditCreate,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    authorization = _assigned_service_authorization(
        request,
        required_roles={GoalActorRole.VERIFIER},
    )
    return apply_assigned_service_goal_audit(
        request,
        body,
        authorization,
        expected_version=expected_version,
        policy_revision=policy_revision,
        idempotency_key=idempotency_key,
    )


@router.post("/goal-assigned-service/progress")
def ingest_assigned_service_goal_progress(
    request: Request,
    body: AssignedServiceProviderProgress,
    expected_autonomy_version: int,
    goal_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    authorization = _assigned_service_authorization(
        request,
        required_roles={GoalActorRole.EXECUTOR, GoalActorRole.VERIFIER},
    )
    scope = authorization.scope
    if authorization.run is None:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "assigned_service_progress_not_supported",
                "message": (
                    "This assigned Fleet run reports progress through its dispatch "
                    "channel, not the provider-run progress endpoint."
                ),
            },
        )
    context = GovernanceMutationContext(
        actor_principal=scope.assigned_service_principal,
        authority_instance_id=scope.authority_instance_id,
        idempotency_key=idempotency_key,
        expected_version=expected_autonomy_version,
        policy_revision=policy_revision,
        goal_version=goal_version,
        fencing_token=scope.fencing_token,
    )
    progress = ProviderGoalProgress(
        run_id=scope.run_id,
        **body.model_dump(mode="python"),
    )
    return _run(
        lambda: _governance(request).ingest_provider_progress(
            scope.goal_id, progress, context
        )
    ).model_dump(mode="json")


@router.get("/goals")
def list_goals(
    request: Request, realm: str | None = None, state: GoalState | None = None
):
    return [
        item.model_dump(mode="json")
        for item in _service(request).list(realm_id=realm, state=state)
    ]


@router.post("/goals", status_code=201)
def create_goal_explicit(
    request: Request,
    body: GoalCreate,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    expected_version: int = 0,
    policy_revision: int = 1,
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
):
    ctx = GoalMutationContext(
        actor_principal=get_principal_id(request) or "user:local",
        authority_instance_id=_authoritative_instance_id(request),
        idempotency_key=idempotency_key,
        expected_version=expected_version,
        policy_revision=policy_revision,
    )
    result = _run(lambda: _service(request).create(body, ctx))
    _wake_supervisor(request)
    return result.model_dump(mode="json")


@router.get("/goals/{goal_id}")
def get_goal(request: Request, goal_id: str, realm: str | None = None):
    goal = _service(request).get(goal_id, realm_id=realm)
    if not goal:
        raise HTTPException(status_code=404, detail="goal not found")
    return {
        "goal": goal.model_dump(mode="json"),
        "events": _service(request).events(goal_id),
        "projection_conflicts": _projection_conflicts_public(
            _service(request).conflicts(goal_id)
        ),
    }


@router.post("/goals/{goal_id}/proposals", status_code=202)
def submit_goal_proposal(
    request: Request,
    goal_id: str,
    body: GoalProposalCreate,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
    fencing_token: Annotated[
        int | None, Header(alias="X-PA-Goal-Fencing-Token")
    ] = None,
):
    ctx = _ctx(
        request,
        expected_version,
        policy_revision,
        idempotency_key,
        actor,
        authority,
        fencing_token,
    )
    result = _run(
        lambda: _governed_goal_mutation(
            request,
            goal_id,
            body.action.kind,
            ctx,
            lambda: _service(request).submit_proposal(goal_id, body, ctx),
            operation_payload=body,
            delegated=body.action.kind == "dispatch_work_package",
        )
    )
    _wake_supervisor(request)
    return result.model_dump(mode="json")


@router.post("/goals/{goal_id}/supervise")
async def supervise_goal(request: Request, goal_id: str):
    supervisor: GoalSupervisor = request.app.state.ctx.require_service(
        "goal_supervisor"
    )
    runtime = request.app.state.ctx.require_service("async_runtime")
    goals = await runtime.run_blocking(
        "goal_supervisor.manual_cycle", supervisor.run_once, goal_id
    )
    if goals:
        return goals[0].model_dump(mode="json")
    goal = _service(request).get(goal_id)
    if not goal:
        raise HTTPException(status_code=404, detail="goal not found")
    return goal.model_dump(mode="json")


def _ctx(
    request,
    expected_version,
    policy_revision,
    idempotency_key,
    actor,
    authority,
    fencing_token,
):
    return GoalMutationContext(
        actor_principal=get_principal_id(request) or "user:local",
        authority_instance_id=_authoritative_instance_id(request),
        idempotency_key=idempotency_key,
        expected_version=expected_version,
        policy_revision=policy_revision,
        fencing_token=fencing_token,
    )


def _mutation_headers(
    request,
    expected_version,
    policy_revision,
    idempotency_key,
    actor,
    authority,
    fencing_token,
):
    return _ctx(
        request,
        expected_version,
        policy_revision,
        idempotency_key,
        actor,
        authority,
        fencing_token,
    )


@router.post("/goals/{goal_id}/transition")
def transition_goal(
    request: Request,
    goal_id: str,
    body: GoalTransition,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
    fencing_token: Annotated[
        int | None, Header(alias="X-PA-Goal-Fencing-Token")
    ] = None,
):
    ctx = _ctx(
        request,
        expected_version,
        policy_revision,
        idempotency_key,
        actor,
        authority,
        fencing_token,
    )
    result = _run(
        lambda: _governed_goal_mutation(
            request,
            goal_id,
            "transition_goal",
            ctx,
            lambda: _service(request).transition(goal_id, body, ctx),
            operation_payload=body,
        )
    )
    _wake_supervisor(request)
    return result.model_dump(mode="json")


@router.post("/goals/{goal_id}/revisions")
def revise_goal(
    request: Request,
    goal_id: str,
    body: GoalRevision,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
    fencing_token: Annotated[
        int | None, Header(alias="X-PA-Goal-Fencing-Token")
    ] = None,
):
    ctx = _ctx(
        request,
        expected_version,
        policy_revision,
        idempotency_key,
        actor,
        authority,
        fencing_token,
    )
    return _run(
        lambda: _governed_goal_mutation(
            request,
            goal_id,
            "revise_goal",
            ctx,
            lambda: _service(request).revise(goal_id, body, ctx),
            operation_payload=body,
        )
    ).model_dump(mode="json")


@router.post("/goals/{goal_id}/evidence")
def record_goal_evidence(
    request: Request,
    goal_id: str,
    body: GoalEvidenceCreate,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
    fencing_token: Annotated[
        int | None, Header(alias="X-PA-Goal-Fencing-Token")
    ] = None,
):
    ctx = _ctx(
        request,
        expected_version,
        policy_revision,
        idempotency_key,
        actor,
        authority,
        fencing_token,
    )
    return _run(
        lambda: _governed_goal_mutation(
            request,
            goal_id,
            "record_evidence",
            ctx,
            lambda: _service(request).add_evidence(goal_id, body, ctx),
            operation_payload=body,
        )
    ).model_dump(mode="json")


@router.post("/goals/{goal_id}/audit")
def audit_goal(
    request: Request,
    goal_id: str,
    body: GoalAuditCreate,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
    fencing_token: Annotated[
        int | None, Header(alias="X-PA-Goal-Fencing-Token")
    ] = None,
):
    ctx = _ctx(
        request,
        expected_version,
        policy_revision,
        idempotency_key,
        get_principal_id(request),
        authority,
        fencing_token,
    )
    return _run(
        lambda: _governed_goal_mutation(
            request,
            goal_id,
            "audit_goal",
            ctx,
            lambda: _service(request).audit(goal_id, body, ctx),
            operation_payload=body,
        )
    ).model_dump(mode="json")


@router.post("/goals/{goal_id}/lease")
def acquire_goal_lease(
    request: Request,
    goal_id: str,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ttl_seconds: int = 60,
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
):
    ctx = _ctx(
        request,
        expected_version,
        policy_revision,
        idempotency_key,
        actor,
        authority,
        None,
    )
    return _run(
        lambda: _service(request).acquire_lease(goal_id, ctx, ttl_seconds=ttl_seconds)
    ).model_dump(mode="json")


@router.delete("/goals/{goal_id}/lease")
def release_goal_lease(
    request: Request,
    goal_id: str,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    fencing_token: Annotated[int, Header(alias="X-PA-Goal-Fencing-Token")],
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
):
    ctx = _ctx(
        request,
        expected_version,
        policy_revision,
        idempotency_key,
        actor,
        authority,
        fencing_token,
    )
    return _run(lambda: _service(request).release_lease(goal_id, ctx)).model_dump(
        mode="json"
    )


@router.put("/goals/{goal_id}/wakeup")
def schedule_goal_wakeup(
    request: Request,
    goal_id: str,
    body: GoalWakeup | None,
    expected_version: int,
    policy_revision: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[str | None, Header(alias="X-PA-Authority-Instance")] = None,
    fencing_token: Annotated[
        int | None, Header(alias="X-PA-Goal-Fencing-Token")
    ] = None,
):
    ctx = _ctx(
        request,
        expected_version,
        policy_revision,
        idempotency_key,
        actor,
        authority,
        fencing_token,
    )
    return _run(
        lambda: _governed_goal_mutation(
            request,
            goal_id,
            "schedule_wakeup",
            ctx,
            lambda: _service(request).schedule_wakeup(goal_id, body, ctx),
            operation_payload=body,
        )
    ).model_dump(mode="json")


@router.get("/goal-governance/providers")
def list_provider_goal_adapters():
    return [item.model_dump(mode="json") for item in list_goal_adapter_capabilities()]


@router.get("/goals/{goal_id}/autonomy")
def get_goal_autonomy(request: Request, goal_id: str):
    state = _run(lambda: _governance(request).get_state(goal_id))
    return _redact_unlaunched_provider_runs(state.model_dump(mode="json"))


@router.get("/goals/{goal_id}/autonomy/events")
def get_goal_autonomy_events(request: Request, goal_id: str):
    return _run(lambda: _governance(request).state_events(goal_id))


@router.put("/goals/{goal_id}/priority")
def set_goal_priority(
    request: Request,
    goal_id: str,
    priority: int,
    reason: str,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    return _run(
        lambda: _governance(request).set_priority(goal_id, priority, reason, context)
    ).model_dump(mode="json")


@router.put("/goals/{goal_id}/strategies")
def update_goal_strategies(
    request: Request,
    goal_id: str,
    body: GoalStrategyPortfolioUpdate,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    return _run(
        lambda: _governance(request).update_strategies(goal_id, body, context)
    ).model_dump(mode="json")


@router.post("/goals/{goal_id}/actions/authorize")
def authorize_goal_action(
    request: Request,
    goal_id: str,
    body: GoalActionRequest,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    state, decision = _run(
        lambda: _governance(request).authorize_action(goal_id, body, context)
    )
    return {
        "decision": decision.model_dump(mode="json"),
        "autonomy_version": state.version,
    }


@router.post("/goals/{goal_id}/actions/apply")
def apply_goal_action(
    request: Request,
    goal_id: str,
    body: GoalActionApply,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    state, decision = _run(
        lambda: _governance(request).apply_action(
            goal_id,
            body.reservation_id,
            context,
            actual_usage=body.actual_usage,
        )
    )
    return {
        "decision": decision.model_dump(mode="json"),
        "autonomy_version": state.version,
    }


@router.post("/goals/{goal_id}/actions/release")
def release_goal_action(
    request: Request,
    goal_id: str,
    body: GoalActionRelease,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    return _run(
        lambda: _governance(request).release_action(
            goal_id,
            body.reservation_id,
            context,
            actual_usage=body.actual_usage,
            reason=body.reason,
        )
    ).model_dump(mode="json")


@router.post("/goals/{goal_id}/providers/assign")
def assign_provider_goal(
    request: Request,
    goal_id: str,
    body: ProviderGoalAssignment,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    state, run, decision = _run(
        lambda: _governance(request).assign_provider(goal_id, body, context)
    )
    return {
        "run": _provider_run_public(run),
        "decision": decision.model_dump(mode="json"),
        "autonomy_version": state.version,
    }


@router.post("/goals/{goal_id}/providers/{run_id}/launch")
def launch_provider_goal(
    request: Request,
    goal_id: str,
    run_id: str,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    state, run, decision = _run(
        lambda: _governance(request).launch_provider(goal_id, run_id, context)
    )
    if decision.disposition != GoalActionDisposition.AUTHORIZED:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "provider_launch_denied",
                "reasons": decision.reasons,
                "decision_id": decision.id,
            },
        )
    return {
        "run": _provider_run_public(run, include_invocation=True),
        "progress_credential": _governance(request).provider_progress_credential(run),
        "decision": decision.model_dump(mode="json"),
        "autonomy_version": state.version,
    }


@router.post("/goals/{goal_id}/providers/progress")
def ingest_provider_goal_progress(
    request: Request,
    goal_id: str,
    body: ProviderGoalProgress,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    state = _run(lambda: _governance(request).get_state(goal_id))
    run = next((item for item in state.provider_runs if item.id == body.run_id), None)
    if run is None:
        raise HTTPException(status_code=404, detail="provider run not found")
    credential = getattr(request.state, "provider_run_credential", None) or ""
    assigned_authorization = None
    if credential.startswith("paas1."):
        assigned_authorization = _assigned_service_authorization(
            request,
            required_roles={GoalActorRole.EXECUTOR, GoalActorRole.VERIFIER},
            expected_goal_id=goal_id,
            expected_run_id=body.run_id,
        )
    elif not _governance(request).verify_provider_progress_credential(run, credential):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "invalid_provider_run_credential",
                "message": "Provider progress requires the run-scoped launch credential.",
            },
        )
    derived_run = assigned_authorization.run if assigned_authorization else run
    request.state.authenticated_instance_id = derived_run.authority_instance_id
    context = context.model_copy(
        update={
            "actor_principal": derived_run.executor_principal,
            "authority_instance_id": derived_run.authority_instance_id,
            "fencing_token": derived_run.fencing_token,
        }
    )
    return _run(
        lambda: _governance(request).ingest_provider_progress(goal_id, body, context)
    ).model_dump(mode="json")


@router.get("/goal-governance/policy")
def get_goal_governance_policy(request: Request, realm: str = "default"):
    return _governance(request).effective_policy(realm).model_dump(mode="json")


@router.put("/goal-governance/policy")
def put_goal_governance_policy(
    request: Request,
    body: GoalGovernancePolicy,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    return _run(lambda: _governance(request).put_policy(body, context)).model_dump(
        mode="json"
    )


@router.get("/goal-governance/proposals")
def list_goal_proposals(request: Request, realm: str = "default"):
    return [
        item.model_dump(mode="json")
        for item in _governance(request).list_proposals(realm)
    ]


@router.post("/goal-governance/proposals", status_code=201)
def propose_goal(
    request: Request,
    body: GoalProposalRequest,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
):
    return _run(lambda: _governance(request).propose_goal(body, context)).model_dump(
        mode="json"
    )


@router.post("/goal-governance/proposals/{proposal_id}/review")
def review_goal_proposal(
    request: Request,
    proposal_id: str,
    body: GoalProposalReview,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
    realm: str = "default",
):
    return _run(
        lambda: _governance(request).review_proposal(
            proposal_id, body, context, realm_id=realm
        )
    ).model_dump(mode="json")


@router.get("/goal-governance/portfolio")
def get_goal_portfolio(request: Request, realm: str = "default"):
    return _goal_portfolio_public(_governance(request).portfolio(realm))


@router.post("/goal-governance/portfolio/reviews", status_code=201)
def review_goal_portfolio(
    request: Request,
    body: GoalPortfolioReviewRequest,
    context: Annotated[GovernanceMutationContext, Depends(_governance_context)],
    realm: str = "default",
):
    return _run(
        lambda: _governance(request).review_portfolio(body, context, realm_id=realm)
    ).model_dump(mode="json")


def _goals_context(request: Request) -> dict:
    goals = request.app.state.ctx.require_service("goal_service").list(
        realm_id=request.app.state.ctx.settings.primary_realm
    )
    governance: GoalGovernanceService = request.app.state.ctx.require_service(
        "goal_governance"
    )
    realm_id = request.app.state.ctx.settings.primary_realm
    return {
        "goals": goals,
        "autonomy": {item.id: governance.get_state(item.id) for item in goals},
        "governance_policy": governance.effective_policy(realm_id),
        "portfolio_review": governance.get_latest_review(realm_id),
        "pending_proposal_count": sum(
            item.disposition.value == "pending_review"
            for item in governance.list_proposals(realm_id)
        ),
    }


class GoalsModule(Module):
    @property
    def name(self) -> str:
        return "goals"

    @property
    def description(self) -> str:
        return "Durable goals, evidence, autonomy, provider adapters, and governance"

    def on_load(self, ctx: AppContext) -> None:
        service = GoalService(ctx.store, ctx.settings.instance_id)
        ctx.register_service("goal_service", service)
        governance = GoalGovernanceService(
            ctx.store,
            ctx.settings.instance_id,
            service,
            progress_token_secret=ctx.settings.session_secret,
        )
        ctx.register_service(
            "goal_governance",
            governance,
        )
        from pa.mcp.local_api import request_local_pa

        def dispatch(payload: dict) -> dict:
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/fleet/dispatch",
                json=payload,
                timeout_seconds=30.0,
            )

        supervisor = GoalSupervisor(
            service,
            ctx.store,
            ctx.settings.instance_id,
            notification_service=ctx.services.get("notifications"),
            dispatch_store=ctx.services.get("dispatch_store"),
            dispatch=dispatch,
            governance=governance,
            default_provider=ctx.settings.agent_provider,
        )
        ctx.register_service("goal_supervisor", supervisor)
        self._supervisor_stop: asyncio.Event | None = None
        self._supervisor_task: asyncio.Task[None] | None = None
        ctx.require_service("pages").register(
            PageDefinition(
                id="goals",
                path="/goals",
                label="Goals",
                icon="work",
                template="pages/goals.html",
                nav_order=15,
                context_builder=_goals_context,
            )
        )

    async def on_startup(self, app, ctx: AppContext) -> None:
        supervisor: GoalSupervisor = ctx.require_service("goal_supervisor")
        self._supervisor_stop = asyncio.Event()

        async def supervise() -> None:
            while self._supervisor_stop and not self._supervisor_stop.is_set():
                try:
                    await asyncio.to_thread(supervisor.run_once)
                except Exception:
                    logger.exception("Durable goal supervision cycle failed")
                woke = await asyncio.to_thread(supervisor.wait_for_wakeup, 30)
                if not woke:
                    continue
                if self._supervisor_stop.is_set():
                    break

        self._supervisor_task = asyncio.create_task(supervise(), name="goal-supervisor")

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        if self._supervisor_stop:
            self._supervisor_stop.set()
            ctx.require_service("goal_supervisor").wake()
        if self._supervisor_task:
            try:
                await asyncio.wait_for(self._supervisor_task, timeout=5)
            except TimeoutError:
                self._supervisor_task.cancel()
                await asyncio.gather(self._supervisor_task, return_exceptions=True)

    def api_routers(self):
        return [("/api", router, ["goals"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def list_goals(
            realm: str | None = None, state: GoalState | None = None
        ) -> list[dict]:
            """List durable goals and their criterion coverage."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/goals",
                params={"realm": realm, "state": state},
            )

        @mcp.tool()
        def get_goal(goal_id: str, realm: str | None = None) -> dict:
            """Get a durable goal with its attributable event ledger."""
            return request_local_pa(
                ctx.settings, "GET", f"/api/goals/{goal_id}", params={"realm": realm}
            )

        @mcp.tool()
        def create_goal(
            goal: GoalCreate,
            idempotency_key: str,
            authority_instance_id: str,
            actor_principal: str = "user:local",
        ) -> dict:
            """Create a durable goal under policy revision 1."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/goals",
                params={"expected_version": 0, "policy_revision": goal.policy.revision},
                headers={
                    "Idempotency-Key": idempotency_key,
                    "X-PA-Actor": actor_principal,
                    "X-PA-Authority-Instance": authority_instance_id,
                },
                json=goal.model_dump(mode="json"),
            )

        @mcp.tool()
        def transition_goal(
            goal_id: str,
            transition: GoalTransition,
            expected_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            fencing_token: int | None = None,
            actor_principal: str = "user:local",
        ) -> dict:
            """Apply a lifecycle transition authorized by the active policy revision."""
            headers = {
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            }
            if fencing_token is not None:
                headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/transition",
                params={
                    "expected_version": expected_version,
                    "policy_revision": policy_revision,
                },
                headers=headers,
                json=transition.model_dump(mode="json"),
            )

        @mcp.tool()
        def get_goal_portfolio(realm: str = "default") -> dict:
            """Read organization policy, autonomy state, proposals, and review."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/goal-governance/portfolio",
                params={"realm": realm},
            )

        @mcp.tool()
        def get_goal_autonomy(goal_id: str) -> dict:
            """Read priority, strategies, usage, decisions, resources, and runs."""
            return request_local_pa(
                ctx.settings, "GET", f"/api/goals/{goal_id}/autonomy"
            )

        @mcp.tool()
        def authorize_goal_action(
            goal_id: str,
            action: GoalActionRequest,
            expected_autonomy_version: int,
            goal_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            fencing_token: int | None = None,
            actor_principal: str = "agent:supervisor",
        ) -> dict:
            """Reserve one action only when policy, budgets, rates, and resources allow."""
            headers = {
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            }
            if fencing_token is not None:
                headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/actions/authorize",
                params={
                    "expected_version": expected_autonomy_version,
                    "goal_version": goal_version,
                    "policy_revision": policy_revision,
                },
                headers=headers,
                json=action.model_dump(mode="json"),
            )

        @mcp.tool()
        def apply_goal_action_reservation(
            goal_id: str,
            apply: GoalActionApply,
            expected_autonomy_version: int,
            goal_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            fencing_token: int | None = None,
        ) -> dict:
            """Revalidate a durable reservation immediately before its side effect."""
            headers = {
                "Idempotency-Key": idempotency_key,
                "X-PA-Authority-Instance": authority_instance_id,
            }
            if fencing_token is not None:
                headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/actions/apply",
                params={
                    "expected_version": expected_autonomy_version,
                    "goal_version": goal_version,
                    "policy_revision": policy_revision,
                },
                headers=headers,
                json=apply.model_dump(mode="json"),
            )

        @mcp.tool()
        def release_goal_action_reservation(
            goal_id: str,
            release: GoalActionRelease,
            expected_autonomy_version: int,
            goal_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            fencing_token: int | None = None,
        ) -> dict:
            """Release a reservation after success, failure, cancellation, or abort."""
            headers = {
                "Idempotency-Key": idempotency_key,
                "X-PA-Authority-Instance": authority_instance_id,
            }
            if fencing_token is not None:
                headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/actions/release",
                params={
                    "expected_version": expected_autonomy_version,
                    "goal_version": goal_version,
                    "policy_revision": policy_revision,
                },
                headers=headers,
                json=release.model_dump(mode="json"),
            )

        @mcp.tool()
        def assign_provider_goal(
            goal_id: str,
            assignment: ProviderGoalAssignment,
            expected_autonomy_version: int,
            goal_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            fencing_token: int | None = None,
            actor_principal: str = "agent:supervisor",
        ) -> dict:
            """Translate a bounded PA goal into a provider-native or recoverable run."""
            headers = {
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            }
            if fencing_token is not None:
                headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/providers/assign",
                params={
                    "expected_version": expected_autonomy_version,
                    "goal_version": goal_version,
                    "policy_revision": policy_revision,
                },
                headers=headers,
                json=assignment.model_dump(mode="json"),
            )

        @mcp.tool()
        def ingest_provider_goal_progress(
            goal_id: str,
            progress: ProviderGoalProgress,
            expected_autonomy_version: int,
            goal_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            progress_credential: str,
            fencing_token: int | None = None,
        ) -> dict:
            """Ingest provider progress without treating provider claims as proof."""
            headers = {
                "Idempotency-Key": idempotency_key,
                "X-PA-Authority-Instance": authority_instance_id,
                "Authorization": f"GoalRun {progress_credential}",
            }
            if fencing_token is not None:
                headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/providers/progress",
                params={
                    "expected_version": expected_autonomy_version,
                    "goal_version": goal_version,
                    "policy_revision": policy_revision,
                },
                headers=headers,
                json=progress.model_dump(mode="json"),
            )

        @mcp.tool()
        def launch_provider_goal(
            goal_id: str,
            run_id: str,
            expected_autonomy_version: int,
            goal_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            fencing_token: int | None = None,
        ) -> dict:
            """Apply the final governance gate and return a runnable invocation."""
            headers = {
                "Idempotency-Key": idempotency_key,
                "X-PA-Authority-Instance": authority_instance_id,
            }
            if fencing_token is not None:
                headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/providers/{run_id}/launch",
                params={
                    "expected_version": expected_autonomy_version,
                    "goal_version": goal_version,
                    "policy_revision": policy_revision,
                },
                headers=headers,
            )

        @mcp.tool()
        def update_goal_strategies(
            goal_id: str,
            portfolio: GoalStrategyPortfolioUpdate,
            expected_autonomy_version: int,
            goal_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            fencing_token: int | None = None,
            actor_principal: str = "agent:supervisor",
        ) -> dict:
            """Replace the bounded strategy portfolio under optimistic fencing."""
            headers = {
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            }
            if fencing_token is not None:
                headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
            return request_local_pa(
                ctx.settings,
                "PUT",
                f"/api/goals/{goal_id}/strategies",
                params={
                    "expected_version": expected_autonomy_version,
                    "goal_version": goal_version,
                    "policy_revision": policy_revision,
                },
                headers=headers,
                json=portfolio.model_dump(mode="json"),
            )

        @mcp.tool()
        def propose_goal(
            proposal: GoalProposalRequest,
            idempotency_key: str,
            authority_instance_id: str,
            policy_revision: int,
            actor_principal: str = "agent:supervisor",
        ) -> dict:
            """Propose a traceable derived or top-level goal under standing policy."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/goal-governance/proposals",
                params={"expected_version": 0, "policy_revision": policy_revision},
                headers={
                    "Idempotency-Key": idempotency_key,
                    "X-PA-Actor": actor_principal,
                    "X-PA-Authority-Instance": authority_instance_id,
                },
                json=proposal.model_dump(mode="json"),
            )

        @mcp.tool()
        def review_goal_proposal(
            proposal_id: str,
            review: GoalProposalReview,
            expected_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            realm: str = "default",
            actor_principal: str = "user:local",
        ) -> dict:
            """Approve or reject one pending proposal with operator attribution."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goal-governance/proposals/{proposal_id}/review",
                params={
                    "realm": realm,
                    "expected_version": expected_version,
                    "policy_revision": policy_revision,
                },
                headers={
                    "Idempotency-Key": idempotency_key,
                    "X-PA-Actor": actor_principal,
                    "X-PA-Authority-Instance": authority_instance_id,
                },
                json=review.model_dump(mode="json"),
            )

        @mcp.tool()
        def review_goal_portfolio(
            review: GoalPortfolioReviewRequest,
            expected_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            realm: str = "default",
            actor_principal: str = "agent:supervisor",
        ) -> dict:
            """Record an independent organization-level allocation review."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/goal-governance/portfolio/reviews",
                params={
                    "realm": realm,
                    "expected_version": expected_version,
                    "policy_revision": policy_revision,
                },
                headers={
                    "Idempotency-Key": idempotency_key,
                    "X-PA-Actor": actor_principal,
                    "X-PA-Authority-Instance": authority_instance_id,
                },
                json=review.model_dump(mode="json"),
            )

        @mcp.tool()
        def set_goal_governance_policy(
            policy: GoalGovernancePolicy,
            expected_version: int,
            idempotency_key: str,
            authority_instance_id: str,
            actor_principal: str = "user:local",
        ) -> dict:
            """Set the next operator-authored organization governance revision."""
            return request_local_pa(
                ctx.settings,
                "PUT",
                "/api/goal-governance/policy",
                params={
                    "expected_version": expected_version,
                    "policy_revision": policy.version,
                },
                headers={
                    "Idempotency-Key": idempotency_key,
                    "X-PA-Actor": actor_principal,
                    "X-PA-Authority-Instance": authority_instance_id,
                },
                json=policy.model_dump(mode="json"),
            )

        @mcp.tool()
        def propose_goal_action(
            goal_id: str,
            proposal: GoalProposalCreate,
            expected_version: int,
            policy_revision: int,
            idempotency_key: str,
            authority_instance_id: str,
            fencing_token: int | None = None,
            actor_principal: str = "user:local",
        ) -> dict:
            """Submit a typed proposal for deterministic goal authorization."""
            headers = {
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            }
            if fencing_token is not None:
                headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/proposals",
                params={
                    "expected_version": expected_version,
                    "policy_revision": policy_revision,
                },
                headers=headers,
                json=proposal.model_dump(mode="json"),
            )

        @mcp.tool()
        def get_assigned_goal(offset: int = 0, limit: int = 50) -> dict:
            """Read the Goal bound to this assigned PA session."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/goal-assigned-session/goal",
                params={"offset": offset, "limit": limit},
            )

        @mcp.tool()
        def propose_assigned_goal_action(
            proposal: AssignedServiceGoalProposalCreate,
            expected_version: int,
            policy_revision: int,
            idempotency_key: str,
        ) -> dict:
            """Submit a proposal as this bridge's exact assigned Goal service."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/goal-assigned-session/proposals",
                params={
                    "expected_version": expected_version,
                    "policy_revision": policy_revision,
                },
                headers={"Idempotency-Key": idempotency_key},
                json=proposal.model_dump(mode="json"),
            )

        @mcp.tool()
        def record_assigned_goal_evidence(
            change: AssignedServiceGoalEvidenceCreate,
            expected_version: int,
            policy_revision: int,
            idempotency_key: str,
        ) -> dict:
            """Record evidence as this bridge's exact assigned Goal service."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/goal-assigned-session/evidence",
                params={
                    "expected_version": expected_version,
                    "policy_revision": policy_revision,
                },
                headers={"Idempotency-Key": idempotency_key},
                json=change.model_dump(mode="json"),
            )

        @mcp.tool()
        def audit_assigned_goal(
            audit: AssignedServiceGoalAuditCreate,
            expected_version: int,
            policy_revision: int,
            idempotency_key: str,
        ) -> dict:
            """Audit a Goal as this bridge's independently assigned verifier."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/goal-assigned-session/audit",
                params={
                    "expected_version": expected_version,
                    "policy_revision": policy_revision,
                },
                headers={"Idempotency-Key": idempotency_key},
                json=audit.model_dump(mode="json"),
            )

        @mcp.tool()
        def supervise_goal(goal_id: str) -> dict:
            """Run one fenced event-driven supervision cycle for a goal."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/supervise",
                timeout_seconds=60.0,
            )
