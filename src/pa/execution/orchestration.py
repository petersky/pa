"""Durable goal planning, routing, and outcome measurement."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from pa.core.io import atomic_write_json


class WorkRole(StrEnum):
    ADVISOR = "advisor"
    EXECUTOR = "executor"


class TaskState(StrEnum):
    PLANNED = "planned"
    READY = "ready"
    RUNNING = "running"
    BLOCKED = "blocked"
    REVIEW = "review"
    DONE = "done"
    FAILED = "failed"


class GateStatus(StrEnum):
    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"
    WAIVED = "waived"


class Budget(BaseModel):
    max_attempts: int = Field(default=3, ge=1)
    max_tokens: int | None = Field(default=None, ge=1)
    max_cost_usd: float | None = Field(default=None, ge=0)
    max_duration_seconds: int | None = Field(default=None, ge=1)


class Usage(BaseModel):
    attempts: int = Field(default=0, ge=0)
    tokens: int = Field(default=0, ge=0)
    cost_usd: float = Field(default=0, ge=0)
    duration_seconds: float = Field(default=0, ge=0)


class QualityGate(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    required: bool = True
    status: GateStatus = GateStatus.PENDING
    evidence: str = ""


class Artifact(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    kind: str
    uri: str
    label: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TaskPlan(BaseModel):
    card_id: str
    title: str = ""
    state: TaskState = TaskState.PLANNED
    depends_on: list[str] = Field(default_factory=list)
    gates: list[QualityGate] = Field(default_factory=list)
    budget: Budget = Field(default_factory=Budget)
    usage: Usage = Field(default_factory=Usage)
    retry_reasons: list[str] = Field(default_factory=list)
    escalation_reasons: list[str] = Field(default_factory=list)
    artifacts: list[Artifact] = Field(default_factory=list)


class RouteDecision(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    task_card_id: str
    role: WorkRole
    model: str | None = None
    reasons: list[str] = Field(default_factory=list)
    estimated_cost_usd: float | None = Field(default=None, ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class Outcome(BaseModel):
    task_card_id: str
    role: WorkRole
    success: bool
    quality_score: float | None = Field(default=None, ge=0, le=1)
    tokens: int = Field(default=0, ge=0)
    cost_usd: float = Field(default=0, ge=0)
    duration_seconds: float = Field(default=0, ge=0)
    recorded_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GoalPlan(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    goal_card_id: str
    realm_id: str = "default"
    tasks: list[TaskPlan] = Field(default_factory=list)
    budget: Budget = Field(default_factory=Budget)
    routes: list[RouteDecision] = Field(default_factory=list)
    outcomes: list[Outcome] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_graph(self) -> GoalPlan:
        ids = [task.card_id for task in self.tasks]
        if len(ids) != len(set(ids)):
            raise ValueError("task card ids must be unique")
        known = set(ids)
        for task in self.tasks:
            unknown = set(task.depends_on) - known
            if unknown:
                raise ValueError(
                    f"task {task.card_id} has unknown dependencies: {sorted(unknown)}"
                )
            if task.card_id in task.depends_on:
                raise ValueError(f"task {task.card_id} cannot depend on itself")
        visiting: set[str] = set()
        visited: set[str] = set()
        graph = {task.card_id: task.depends_on for task in self.tasks}

        def visit(card_id: str) -> None:
            if card_id in visiting:
                raise ValueError("task dependency graph contains a cycle")
            if card_id in visited:
                return
            visiting.add(card_id)
            for dependency in graph[card_id]:
                visit(dependency)
            visiting.remove(card_id)
            visited.add(card_id)

        for card_id in ids:
            visit(card_id)
        return self


class RoutingRequest(BaseModel):
    task_card_id: str
    ambiguity: bool = False
    planning_required: bool = False
    review_required: bool = False
    risk: str = "low"
    executor_failed_attempts: int = Field(default=0, ge=0)
    model: str | None = None
    estimated_cost_usd: float | None = Field(default=None, ge=0)


class OrchestrationStore:
    """Atomic ledger; PA's server remains its sole writer."""

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "goal_orchestration.json"
        self._lock = RLock()
        self._plans: dict[str, GoalPlan] = {}
        self._load()

    def _load(self) -> None:
        try:
            payload = json.loads(self.path.read_text())
            self._plans = {
                key: GoalPlan.model_validate(value) for key, value in payload.items()
            }
        except (OSError, ValueError):
            self._plans = {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            self.path,
            {key: plan.model_dump(mode="json") for key, plan in self._plans.items()},
        )

    def put(self, plan: GoalPlan) -> GoalPlan:
        with self._lock:
            plan.updated_at = datetime.now(UTC)
            self._plans[plan.id] = plan
            self._save()
            return plan

    def get(self, plan_id: str) -> GoalPlan | None:
        with self._lock:
            return self._plans.get(plan_id)

    def list(self, *, realm_id: str | None = None) -> list[GoalPlan]:
        with self._lock:
            plans = list(self._plans.values())
        if realm_id:
            plans = [plan for plan in plans if plan.realm_id == realm_id]
        return sorted(plans, key=lambda plan: plan.updated_at, reverse=True)

    def ready_tasks(self, plan: GoalPlan) -> list[TaskPlan]:
        completed = {
            task.card_id for task in plan.tasks if task.state == TaskState.DONE
        }
        plan_usage = self._plan_usage(plan)
        if not self._within_budget(plan_usage, plan.budget):
            return []
        return [
            task
            for task in plan.tasks
            if task.state in {TaskState.PLANNED, TaskState.READY}
            and set(task.depends_on) <= completed
            and self._within_budget(task.usage, task.budget)
        ]

    @staticmethod
    def _within_budget(usage: Usage, budget: Budget) -> bool:
        return (
            usage.attempts < budget.max_attempts
            and (budget.max_tokens is None or usage.tokens < budget.max_tokens)
            and (
                budget.max_cost_usd is None
                or usage.cost_usd < budget.max_cost_usd
            )
            and (
                budget.max_duration_seconds is None
                or usage.duration_seconds < budget.max_duration_seconds
            )
        )

    @staticmethod
    def _plan_usage(plan: GoalPlan) -> Usage:
        return Usage(
            attempts=sum(task.usage.attempts for task in plan.tasks),
            tokens=sum(task.usage.tokens for task in plan.tasks),
            cost_usd=sum(task.usage.cost_usd for task in plan.tasks),
            duration_seconds=sum(task.usage.duration_seconds for task in plan.tasks),
        )

    def update_task(self, plan: GoalPlan, task: TaskPlan) -> TaskPlan:
        index = next(
            (i for i, item in enumerate(plan.tasks) if item.card_id == task.card_id),
            None,
        )
        if index is None:
            raise ValueError("task card is not part of this goal plan")
        if task.state == TaskState.DONE:
            failed = [
                gate.name
                for gate in task.gates
                if gate.required and gate.status != GateStatus.PASSED
            ]
            if failed:
                raise ValueError(
                    f"required quality gates must pass before completion: {failed}"
                )
        candidate = plan.model_copy(deep=True)
        candidate.tasks[index] = task
        # Revalidate the graph after dependency edits before mutating the ledger.
        validated = GoalPlan.model_validate(candidate.model_dump(mode="python"))
        plan.tasks = validated.tasks
        self.put(plan)
        return plan.tasks[index]

    def route(self, plan: GoalPlan, request: RoutingRequest) -> RouteDecision:
        task = next(
            (item for item in plan.tasks if item.card_id == request.task_card_id), None
        )
        if not task:
            raise ValueError("task card is not part of this goal plan")
        reasons: list[str] = []
        if request.planning_required:
            reasons.append("planning")
        if request.review_required:
            reasons.append("review")
        if request.ambiguity:
            reasons.append("ambiguity")
        if request.risk.casefold() in {"high", "critical"}:
            reasons.append(f"{request.risk.casefold()} risk")
        if request.executor_failed_attempts >= 2:
            reasons.append("repeated executor failure")
        role = WorkRole.ADVISOR if reasons else WorkRole.EXECUTOR
        decision = RouteDecision(
            task_card_id=task.card_id,
            role=role,
            model=request.model,
            reasons=reasons or ["bounded work"],
            estimated_cost_usd=request.estimated_cost_usd,
        )
        plan.routes.append(decision)
        self.put(plan)
        return decision

    def record_outcome(self, plan: GoalPlan, outcome: Outcome) -> Outcome:
        task = next(
            (item for item in plan.tasks if item.card_id == outcome.task_card_id), None
        )
        if not task:
            raise ValueError("task card is not part of this goal plan")
        task.usage.attempts += 1
        task.usage.tokens += outcome.tokens
        task.usage.cost_usd += outcome.cost_usd
        task.usage.duration_seconds += outcome.duration_seconds
        plan.outcomes.append(outcome)
        self.put(plan)
        return outcome

    def metrics(self, plan: GoalPlan) -> dict[str, Any]:
        outcomes = plan.outcomes
        plan_usage = self._plan_usage(plan)
        scored = [
            item.quality_score
            for item in outcomes
            if item.quality_score is not None
        ]
        by_role: dict[str, dict[str, Any]] = {}
        for role in WorkRole:
            selected = [item for item in outcomes if item.role == role]
            role_scores = [
                item.quality_score
                for item in selected
                if item.quality_score is not None
            ]
            by_role[role.value] = {
                "runs": len(selected),
                "success_rate": (
                    sum(item.success for item in selected) / len(selected)
                    if selected
                    else None
                ),
                "average_quality": (
                    sum(role_scores) / len(role_scores) if role_scores else None
                ),
                "tokens": sum(item.tokens for item in selected),
                "cost_usd": sum(item.cost_usd for item in selected),
                "duration_seconds": sum(item.duration_seconds for item in selected),
            }
        return {
            "runs": len(outcomes),
            "success_rate": (
                sum(item.success for item in outcomes) / len(outcomes)
                if outcomes
                else None
            ),
            "average_quality": sum(scored) / len(scored) if scored else None,
            "tokens": sum(item.tokens for item in outcomes),
            "cost_usd": sum(item.cost_usd for item in outcomes),
            "duration_seconds": sum(item.duration_seconds for item in outcomes),
            "within_budget": self._within_budget(plan_usage, plan.budget),
            "by_role": by_role,
        }
