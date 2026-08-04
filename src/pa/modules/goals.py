"""Durable goal REST, MCP, and web surfaces."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.ui.pages import PageDefinition
from pa.goals.advanced_models import (
    GoalActionRequest,
    GoalGovernancePolicy,
    GoalPortfolioReviewRequest,
    GoalProposalRequest,
    GoalProposalReview,
    GoalStrategyPortfolioUpdate,
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
    GoalRevision,
    GoalState,
    GoalTransition,
    GoalWakeup,
)
from pa.goals.providers import list_goal_adapter_capabilities
from pa.goals.service import GoalConflict, GoalService

router = APIRouter()


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
    return GovernanceMutationContext(
        actor_principal=actor,
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
    return GoalMutationContext(
        actor_principal=actor,
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
        actor_principal=actor,
        authority_instance_id=authority or request.app.state.ctx.settings.instance_id,
        idempotency_key=idempotency_key,
        expected_version=expected_version,
        policy_revision=policy_revision,
    )
    return _run(lambda: _service(request).create(body, ctx)).model_dump(mode="json")


@router.get("/goals/{goal_id}")
def get_goal(request: Request, goal_id: str, realm: str | None = None):
    goal = _service(request).get(goal_id, realm_id=realm)
    if not goal:
        raise HTTPException(status_code=404, detail="goal not found")
    return {
        "goal": goal.model_dump(mode="json"),
        "events": _service(request).events(goal_id),
    }


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
        actor_principal=actor,
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
    return _run(lambda: _service(request).transition(goal_id, body, ctx)).model_dump(
        mode="json"
    )


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
    return _run(lambda: _service(request).revise(goal_id, body, ctx)).model_dump(
        mode="json"
    )


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
    return _run(lambda: _service(request).add_evidence(goal_id, body, ctx)).model_dump(
        mode="json"
    )


@router.post("/goals/{goal_id}/audit")
def audit_goal(
    request: Request,
    goal_id: str,
    body: GoalAuditCreate,
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
    return _run(lambda: _service(request).audit(goal_id, body, ctx)).model_dump(
        mode="json"
    )


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
        lambda: _service(request).schedule_wakeup(goal_id, body, ctx)
    ).model_dump(mode="json")


@router.get("/goal-governance/providers")
def list_provider_goal_adapters():
    return [item.model_dump(mode="json") for item in list_goal_adapter_capabilities()]


@router.get("/goals/{goal_id}/autonomy")
def get_goal_autonomy(request: Request, goal_id: str):
    return _run(lambda: _governance(request).get_state(goal_id)).model_dump(mode="json")


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
        "run": run.model_dump(mode="json") if run else None,
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
    return _governance(request).portfolio(realm)


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
        ctx.register_service(
            "goal_governance",
            GoalGovernanceService(
                ctx.store,
                ctx.settings.instance_id,
                service,
            ),
        )
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
            fencing_token: int | None = None,
            actor_principal: str = "agent:supervisor",
        ) -> dict:
            """Ingest provider progress without treating provider claims as proof."""
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
