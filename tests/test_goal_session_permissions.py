"""Goal-dispatched sessions use the goal owner's auto-approve preference."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from pa.goals.models import GoalCreate, GoalCriterion, GoalMutationContext
from pa.goals.service import GoalService
from tests.test_notifications import _agent_runtime, _kernel

OPTIONS = [
    {"optionId": "allow_once", "kind": "allow_once", "name": "Allow once"},
    {"optionId": "reject_once", "kind": "reject_once", "name": "Reject"},
]


def _goal_owned_by(kernel, owner: str) -> str:
    service = GoalService(kernel.ctx.store, kernel.ctx.settings.instance_id)
    goal = service.create(
        GoalCreate(
            objective="Owner decides permissions",
            criteria=[
                GoalCriterion(
                    description="c", verification_method="v", evidence_requirement="e"
                )
            ],
        ),
        GoalMutationContext(
            actor_principal=owner,
            authority_instance_id=kernel.ctx.settings.instance_id,
            idempotency_key="goal-permissions",
            expected_version=0,
            policy_revision=1,
        ),
    )
    return goal.id


def _permission(runtime) -> object:
    # Without auto-approval the request would wait for a human forever; bound it.
    return asyncio.run(
        asyncio.wait_for(
            runtime._on_permission(
                "external-session",
                {
                    "request_id": "permission-1",
                    "tool_call": {"toolCallId": "tool-1", "title": "Run tests"},
                    "options": OPTIONS,
                },
            ),
            timeout=5,
        )
    )


def _runtime_for(tmp_path: Path, *, goal_bound: bool):
    kernel = _kernel(tmp_path)
    runtime = _agent_runtime(kernel)
    # The owner prefers auto-approval; nobody else (and not the global default) does.
    runtime.manager.should_auto_approve_async = AsyncMock(
        side_effect=lambda principal: principal == "user:owner"
    )
    runtime.session.principal_id = "instance:remote-target"
    runtime.session.dispatch_id = "dispatch-1"
    goal_id = _goal_owned_by(kernel, "user:owner")
    runtime.manager.dispatch_store = SimpleNamespace(
        get=lambda dispatch_id: SimpleNamespace(
            goal_provenance=SimpleNamespace(goal_id=goal_id) if goal_bound else None,
            realm_id="default",
        )
    )
    return runtime


def test_goal_dispatched_session_follows_the_goal_owners_preference(
    tmp_path: Path,
) -> None:
    runtime = _runtime_for(tmp_path, goal_bound=True)

    result = _permission(runtime)

    assert result.outcome.option_id == "allow_once"
    runtime.manager.should_auto_approve_async.assert_awaited_once_with("user:owner")


def test_an_ordinary_dispatched_session_keeps_its_own_principal(
    tmp_path: Path,
) -> None:
    runtime = _runtime_for(tmp_path, goal_bound=False)
    runtime.manager.should_auto_approve_async.side_effect = None
    runtime.manager.should_auto_approve_async.return_value = False

    # No auto-approval: the request waits for a human, so run it only until the
    # approver has been resolved.
    async def attempt():
        task = asyncio.create_task(
            runtime._on_permission(
                "external-session",
                {
                    "request_id": "permission-2",
                    "tool_call": {"toolCallId": "tool-2", "title": "Run tests"},
                    "options": OPTIONS,
                },
            )
        )
        for _ in range(100):
            if runtime.manager.should_auto_approve_async.await_count:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(attempt())

    runtime.manager.should_auto_approve_async.assert_awaited_once_with(
        "instance:remote-target"
    )
