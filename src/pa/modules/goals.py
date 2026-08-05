"""Durable goal REST, MCP, and web surfaces."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from pa.auth.middleware import get_principal_id
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.ui.pages import PageDefinition
from pa.goals.advanced_models import (
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
from pa.goals.governance import GoalGovernanceService
from pa.goals.models import (
    GoalAuditCreate,
    GoalCreate,
    GoalEvidenceCreate,
    GoalMutationContext,
    GoalProposalCreate,
    GoalRevision,
    GoalState,
    GoalTransition,
    GoalWakeup,
)
from pa.goals.projection import find_goal_event_by_idempotency
from pa.goals.providers import list_goal_adapter_capabilities
from pa.goals.service import GoalConflict, GoalService
from pa.goals.supervisor import GoalSupervisor

router = APIRouter()
logger = logging.getLogger(__name__)


def _service(request: Request) -> GoalService:
    return request.app.state.ctx.require_service("goal_service")


def _governance(request: Request) -> GoalGovernanceService:
    return request.app.state.ctx.require_service("goal_governance")


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
        authority_instance_id=authority or request.app.state.ctx.settings.instance_id,
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
        authority_instance_id=authority or request.app.state.ctx.settings.instance_id,
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


def _governed_goal_mutation(
    request: Request,
    goal_id: str,
    action_class: str,
    goal_context: GoalMutationContext,
    operation,
    *,
    delegated: bool = False,
    estimate: GoalUsage | None = None,
):
    governance = _governance(request)
    goal_service = _service(request)
    current_goal = goal_service.get(goal_id)
    goal_mutation_exists = bool(
        current_goal
        and find_goal_event_by_idempotency(
            goal_service.store,
            current_goal.realm_id,
            goal_context.idempotency_key,
        )
    )
    actor = goal_context.actor_principal

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
    action = GoalActionRequest(
        action_class=action_class,
        delegated=delegated,
        estimate=estimate or GoalUsage(actions=1),
        operator_approved=actor.startswith(("user:", "role:admin")),
        approval_principal=(
            actor if actor.startswith(("user:", "role:admin")) else None
        ),
    )
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
        authority_instance_id=authority or request.app.state.ctx.settings.instance_id,
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
        "projection_conflicts": _service(request).conflicts(goal_id),
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
        authority_instance_id=authority or request.app.state.ctx.settings.instance_id,
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
    if not _governance(request).verify_provider_progress_credential(run, credential):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "invalid_provider_run_credential",
                "message": "Provider progress requires the run-scoped launch credential.",
            },
        )
    request.state.authenticated_instance_id = run.authority_instance_id
    context = context.model_copy(
        update={
            "actor_principal": run.executor_principal,
            "authority_instance_id": run.authority_instance_id,
            "fencing_token": run.fencing_token,
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
        def supervise_goal(goal_id: str) -> dict:
            """Run one fenced event-driven supervision cycle for a goal."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/goals/{goal_id}/supervise",
                timeout_seconds=60.0,
            )
