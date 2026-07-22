from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pydantic import ValidationError

from pa.execution.orchestration import (
    Budget,
    GateStatus,
    GoalPlan,
    OrchestrationStore,
    Outcome,
    QualityGate,
    RoutingRequest,
    TaskPlan,
    TaskState,
    WorkRole,
)


class GoalPlanTests(unittest.TestCase):
    def test_rejects_unknown_and_cyclic_dependencies(self) -> None:
        with self.assertRaises(ValidationError):
            GoalPlan(
                goal_card_id="goal",
                tasks=[TaskPlan(card_id="a", depends_on=["missing"])],
            )
        with self.assertRaises(ValidationError):
            GoalPlan(
                goal_card_id="goal",
                tasks=[
                    TaskPlan(card_id="a", depends_on=["b"]),
                    TaskPlan(card_id="b", depends_on=["a"]),
                ],
            )

    def test_ready_tasks_require_dependencies_and_retry_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = OrchestrationStore(Path(tmp))
            plan = GoalPlan(
                goal_card_id="goal",
                tasks=[
                    TaskPlan(card_id="a", state=TaskState.DONE),
                    TaskPlan(card_id="b", depends_on=["a"]),
                    TaskPlan(card_id="c", depends_on=["b"]),
                ],
            )
            self.assertEqual([task.card_id for task in store.ready_tasks(plan)], ["b"])
            plan.tasks[1].usage.attempts = plan.tasks[1].budget.max_attempts
            self.assertEqual(store.ready_tasks(plan), [])

    def test_goal_cost_budget_stops_further_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = OrchestrationStore(Path(tmp))
            plan = GoalPlan(
                goal_card_id="goal",
                budget=Budget(max_cost_usd=0.01),
                tasks=[TaskPlan(card_id="task")],
            )
            plan.tasks[0].usage.cost_usd = 0.01
            self.assertEqual(store.ready_tasks(plan), [])
            self.assertFalse(store.metrics(plan)["within_budget"])

    def test_quality_gate_escalation_retry_and_artifact_fields_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = OrchestrationStore(Path(tmp))
            task = TaskPlan(
                card_id="task",
                gates=[
                    QualityGate(
                        name="tests",
                        status=GateStatus.PASSED,
                        evidence="42 passed",
                    )
                ],
                retry_reasons=["transient CI failure"],
                escalation_reasons=["ambiguous acceptance criteria"],
            )
            saved = store.put(GoalPlan(goal_card_id="goal", tasks=[task]))
            loaded = OrchestrationStore(Path(tmp)).get(saved.id)
            assert loaded is not None
            self.assertEqual(loaded.tasks[0].gates[0].evidence, "42 passed")
            self.assertEqual(loaded.tasks[0].retry_reasons, ["transient CI failure"])

    def test_required_gate_blocks_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = OrchestrationStore(Path(tmp))
            plan = store.put(
                GoalPlan(
                    goal_card_id="goal",
                    tasks=[
                        TaskPlan(
                            card_id="task",
                            gates=[QualityGate(name="tests")],
                        )
                    ],
                )
            )
            completed = plan.tasks[0].model_copy(update={"state": TaskState.DONE})
            with self.assertRaisesRegex(ValueError, "quality gates"):
                store.update_task(plan, completed)
            completed.gates[0].status = GateStatus.PASSED
            self.assertEqual(store.update_task(plan, completed).state, TaskState.DONE)


class RoutingAndMetricsTests(unittest.TestCase):
    def test_routes_bounded_work_to_executor_and_ambiguity_to_advisor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = OrchestrationStore(Path(tmp))
            plan = store.put(
                GoalPlan(goal_card_id="goal", tasks=[TaskPlan(card_id="task")])
            )
            bounded = store.route(plan, RoutingRequest(task_card_id="task"))
            ambiguous = store.route(
                plan, RoutingRequest(task_card_id="task", ambiguity=True)
            )
            self.assertEqual(bounded.role, WorkRole.EXECUTOR)
            self.assertEqual(ambiguous.role, WorkRole.ADVISOR)
            self.assertEqual(ambiguous.reasons, ["ambiguity"])

    def test_measures_quality_and_cost_by_role(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = OrchestrationStore(Path(tmp))
            plan = store.put(
                GoalPlan(goal_card_id="goal", tasks=[TaskPlan(card_id="task")])
            )
            store.record_outcome(
                plan,
                Outcome(
                    task_card_id="task",
                    role=WorkRole.EXECUTOR,
                    success=True,
                    quality_score=0.9,
                    tokens=100,
                    cost_usd=0.02,
                    duration_seconds=4,
                ),
            )
            metrics = store.metrics(plan)
            self.assertEqual(metrics["success_rate"], 1)
            self.assertEqual(metrics["average_quality"], 0.9)
            self.assertEqual(metrics["by_role"]["executor"]["tokens"], 100)
            self.assertEqual(plan.tasks[0].usage.attempts, 1)


if __name__ == "__main__":
    unittest.main()
