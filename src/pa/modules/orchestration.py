"""Goal orchestration REST API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.domain.models import CardKind
from pa.execution.orchestration import (
    GoalPlan,
    OrchestrationStore,
    Outcome,
    RoutingRequest,
    TaskPlan,
)

router = APIRouter()


def _store(request: Request) -> OrchestrationStore:
    return request.app.state.ctx.require_service("orchestration_store")


@router.get("/orchestration/goals")
def list_plans(request: Request, realm: str | None = None) -> list[dict[str, Any]]:
    return [
        plan.model_dump(mode="json")
        for plan in _store(request).list(realm_id=realm)
    ]


@router.post("/orchestration/goals", status_code=201)
def create_plan(request: Request, plan: GoalPlan) -> dict[str, Any]:
    card = request.app.state.ctx.store.get_card(
        plan.goal_card_id, realm_id=plan.realm_id
    )
    if not card or card.kind != CardKind.GOAL:
        raise HTTPException(
            status_code=422, detail="goal_card_id must reference a goal card"
        )
    cards = {
        task.card_id: request.app.state.ctx.store.get_card(
            task.card_id, realm_id=plan.realm_id
        )
        for task in plan.tasks
    }
    invalid = [
        card_id
        for card_id, value in cards.items()
        if not value or value.kind != CardKind.TASK
    ]
    if invalid:
        raise HTTPException(
            status_code=422,
            detail={"message": "tasks must reference task cards", "card_ids": invalid},
        )
    return _store(request).put(plan).model_dump(mode="json")


@router.get("/orchestration/goals/{plan_id}")
def get_plan(request: Request, plan_id: str) -> dict[str, Any]:
    plan = _store(request).get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="goal plan not found")
    return {
        "plan": plan.model_dump(mode="json"),
        "ready_tasks": [
            task.model_dump(mode="json")
            for task in _store(request).ready_tasks(plan)
        ],
        "metrics": _store(request).metrics(plan),
    }


@router.post("/orchestration/goals/{plan_id}/route")
def route_task(request: Request, plan_id: str, body: RoutingRequest) -> dict[str, Any]:
    plan = _store(request).get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="goal plan not found")
    try:
        return _store(request).route(plan, body).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/orchestration/goals/{plan_id}/tasks/{card_id}")
def update_task(
    request: Request, plan_id: str, card_id: str, body: TaskPlan
) -> dict[str, Any]:
    plan = _store(request).get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="goal plan not found")
    if body.card_id != card_id:
        raise HTTPException(status_code=422, detail="path and body card ids differ")
    try:
        return _store(request).update_task(plan, body).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/orchestration/goals/{plan_id}/outcomes", status_code=201)
def record_outcome(request: Request, plan_id: str, body: Outcome) -> dict[str, Any]:
    plan = _store(request).get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="goal plan not found")
    try:
        _store(request).record_outcome(plan, body)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "outcome": body.model_dump(mode="json"),
        "metrics": _store(request).metrics(plan),
    }


class OrchestrationModule(Module):
    @property
    def name(self) -> str:
        return "orchestration"

    @property
    def description(self) -> str:
        return "Goal decomposition, selective routing, quality gates, and cost metrics"

    def on_load(self, ctx: AppContext) -> None:
        ctx.register_service(
            "orchestration_store", OrchestrationStore(ctx.settings.data_dir)
        )

    def api_routers(self):
        return [("/api", router, ["orchestration"])]
