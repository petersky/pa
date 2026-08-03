"""Durable goal REST, MCP, and web surfaces."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Request

from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.ui.pages import PageDefinition
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
from pa.goals.service import GoalConflict, GoalService

router = APIRouter()


def _service(request: Request) -> GoalService:
    return request.app.state.ctx.require_service("goal_service")


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


def _goals_context(request: Request) -> dict:
    goals = request.app.state.ctx.require_service("goal_service").list(
        realm_id=request.app.state.ctx.settings.primary_realm
    )
    return {"goals": goals}


class GoalsModule(Module):
    @property
    def name(self) -> str:
        return "goals"

    @property
    def description(self) -> str:
        return "Durable goals, criteria, evidence, controller leases, and wakeups"

    def on_load(self, ctx: AppContext) -> None:
        service = GoalService(ctx.store, ctx.settings.instance_id)
        ctx.register_service("goal_service", service)
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
