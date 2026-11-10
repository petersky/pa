"""The sites that now tell the owner when work is stuck (via notify_stall)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel, reset_kernel
from pa.domain.notifications import NotificationPriority, NotificationType
from pa.domain.store import reset_store
from pa.execution.dispatch import CompletionOutbox, DispatchRecord, DispatchStore, DispatchWorker
from pa.instance.agent_session import reset_instance_agent
from pa.notifications.stall import (
    notify_completion_undeliverable,
    notify_dispatch_failed,
)
from pa.scheduling.service import Scheduler
from pa.scheduling.store import Ledger

from tests.test_telegram_notifications import FakeTelegram, boot  # noqa: F401  (fixtures)


@pytest.fixture(autouse=True)
def _reset():
    reset_kernel()
    reset_settings()
    reset_store()
    reset_instance_agent()
    yield
    reset_instance_agent()
    reset_store()
    reset_settings()
    reset_kernel()


def stalls(kernel, kind=None):
    items = kernel.ctx.store.list_notifications(realm_id="default", limit=200)
    return [
        n
        for n in items
        if (n.capability or "").startswith("stall:")
        and (kind is None or n.capability == f"stall:{kind}")
    ]


def record(**kw) -> DispatchRecord:
    values = dict(
        dispatch_id="dispatch-1",
        mutation_id="mutation-1",
        card_id="card-1",
        realm_id="default",
        card_version="v1",
        authority_instance_id="authority",
        authority_url="http://authority",
        target_instance_id="target",
        session_id="session-1",
        state="running",
    )
    values.update(kw)
    return DispatchRecord(**values)


# ------------------------------------------------------------------- dispatch failure


def test_dispatch_worker_failure_notifies_with_retry_and_dedupes(tmp_path):
    kernel, fake = boot(tmp_path)
    ledger = DispatchStore(tmp_path / "ledger")

    async def handler(item):
        raise RuntimeError("workspace missing")

    worker = DispatchWorker(
        ledger, handler, stall_notifier=lambda failed: notify_dispatch_failed(kernel.ctx, failed)
    )
    item = record(state="queued")
    ledger.put(item)

    async def run():
        await worker._execute(item)
        again = ledger.get("dispatch-1")
        again.state = "queued"
        ledger.put(again)
        await worker._execute(again)

    asyncio.run(run())
    found = stalls(kernel, "dispatch_failed")
    assert len(found) == 1  # deduped per dispatch
    notice = found[0]
    assert notice.type == NotificationType.DISPATCH_FAILURE
    assert notice.priority == NotificationPriority.HIGH
    assert notice.dispatch_id == "dispatch-1" and notice.card_id == "card-1"
    assert "workspace missing" in notice.body
    assert [a.id for a in notice.actions] == ["retry", "dismiss"]
    assert notice.coalesced_count == 2
    kernel.ctx.require_service("telegram_push").drain()
    assert len(fake.sent) == 1  # one push for the incident


def test_dispatch_failure_notifier_error_does_not_break_failure_handling(tmp_path):
    ledger = DispatchStore(tmp_path)

    async def handler(item):
        raise RuntimeError("boom")

    def broken(failed):
        raise RuntimeError("notifier down")

    worker = DispatchWorker(ledger, handler, stall_notifier=broken)
    item = record(state="queued")
    ledger.put(item)
    asyncio.run(worker._execute(item))
    assert ledger.get("dispatch-1").state == "failed"


# --------------------------------------------------------------- completion outbox


@pytest.mark.parametrize("scenario", ["permanent_failure", "transport_exhausted"])
def test_completion_outbox_giving_up_notifies(tmp_path, scenario):
    kernel, _ = boot(tmp_path)
    ledger = DispatchStore(tmp_path / "ledger")
    ledger.put(record())
    outbox = CompletionOutbox(
        ledger,
        "secret",
        retry_seconds=0.01,
        max_attempts=1,
        stall_notifier=lambda failed: notify_completion_undeliverable(kernel.ctx, failed),
    )
    outbox.queue("session-1", {"stop_reason": "end_turn"})
    with patch("pa.execution.dispatch.httpx.AsyncClient") as client:
        if scenario == "permanent_failure":
            response = SimpleNamespace(status_code=403, text="forbidden", json=lambda: {})
            client.return_value.post = AsyncMock(return_value=response)
        else:
            client.return_value.post = AsyncMock(side_effect=httpx.ConnectError("offline"))
        asyncio.run(outbox._send(ledger.get("dispatch-1")))
        assert ledger.get("dispatch-1").completion_delivery_class == scenario
    found = stalls(kernel, "completion_undeliverable")
    assert len(found) == 1 and found[0].dispatch_id == "dispatch-1"
    assert found[0].type == NotificationType.DISPATCH_FAILURE and scenario in found[0].body


def test_completion_outbox_retryable_failure_stays_quiet(tmp_path):
    kernel, _ = boot(tmp_path)
    ledger = DispatchStore(tmp_path / "ledger")
    ledger.put(record())
    outbox = CompletionOutbox(
        ledger,
        "secret",
        retry_seconds=0.01,
        stall_notifier=lambda failed: notify_completion_undeliverable(kernel.ctx, failed),
    )
    outbox.queue("session-1", {"stop_reason": "end_turn"})
    with patch("pa.execution.dispatch.httpx.AsyncClient") as client:
        client.return_value.post = AsyncMock(side_effect=httpx.ConnectError("offline"))
        asyncio.run(outbox._send(ledger.get("dispatch-1")))
    assert stalls(kernel) == []


# ---------------------------------------------------------------- scheduled prompts


def test_scheduled_prompt_terminal_failure_notifies_owner_once(tmp_path):
    from tests.test_scheduling_delivery import spec

    kernel, fake = boot(tmp_path)
    ledger = Ledger(tmp_path / "s.db")
    schedule = ledger.create("user:local", "create", spec(), 0)
    oid = ledger.fire("user:local", schedule["id"], "fire", 1)["occurrence_id"]

    async def deliver(item):
        raise TimeoutError("destination unreachable")

    scheduler = Scheduler(kernel.ctx, None, ledger, deliver)

    async def run():
        for attempt in range(8):
            now = 10_000 * (attempt + 1)
            (pending,) = ledger.pending(now)
            await scheduler.deliver_item(pending, now, False)
            if attempt < 7:
                assert stalls(kernel) == []  # still retrying: nobody is bothered yet

    asyncio.run(run())
    assert ledger.history("user:local", schedule["id"])[0]["state"] == "failed"
    found = stalls(kernel, "schedule_failed")
    assert len(found) == 1
    notice = found[0]
    assert notice.priority == NotificationPriority.HIGH
    assert oid in notice.body and "destination_failure:TimeoutError" in notice.body
    assert notice.visibility.value == "principal" and notice.principal_id == "user:local"
    assert notice.deduplication_key.endswith(oid)
    assert [a.id for a in notice.actions] == ["retry", "dismiss"]


def test_scheduled_prompt_success_and_retry_create_nothing(tmp_path):
    from tests.test_scheduling_delivery import spec

    kernel, _ = boot(tmp_path)
    ledger = Ledger(tmp_path / "s.db")
    schedule = ledger.create("user:local", "create", spec(), 0)
    ledger.fire("user:local", schedule["id"], "fire", 1)

    async def deliver(item):
        return {"accepted": True}

    asyncio.run(Scheduler(kernel.ctx, None, ledger, deliver).cycle(600))
    assert stalls(kernel) == []


# ------------------------------------------------------------- startup wiring + escalation


def test_fleet_startup_wires_the_stall_notifiers(tmp_path):
    kernel, _ = boot(tmp_path)
    with TestClient(kernel.build_app()):
        assert kernel.ctx.require_service("dispatch_worker").stall_notifier is not None
        assert kernel.ctx.require_service("completion_outbox").stall_notifier is not None


def test_post_turn_escalation_notifies_the_owner(tmp_path):
    from pa.domain.models import CardCreate  # noqa: F401
    from pa.execution.post_turn import (
        FollowupActionName,
        FollowupActionV1,
        PostTurnEvaluator,
        SafetyClassification,
        mark_record_only_actions,
    )
    from pa.modules.fleet import FollowupActionExecutionBody, execute_post_turn_action
    from tests.test_post_turn_evaluation import context_for, snapshot

    kernel, fake = boot(tmp_path)
    ledger = DispatchStore(tmp_path / "ledger")
    kernel.ctx.services["dispatch_store"] = ledger
    evaluation = PostTurnEvaluator().evaluate(context_for(snapshot()))
    action = FollowupActionV1(
        name=FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW,
        parameters={"reason": "The evidence contradicts itself."},
        idempotency_key_inputs=["dispatch", "turn", "action"],
        safety=SafetyClassification.RECORD_ONLY,
        human_approval_required=False,
    )
    evaluation.recommended_actions = [action]
    # Round-trip so the content-addressed action ids are the ones a stored record has.
    evaluation = type(evaluation).model_validate(evaluation.model_dump(mode="json", by_alias=True))
    mark_record_only_actions(evaluation)
    action = evaluation.recommended_actions[0]
    item = record(state="completed")
    item.post_turn_evaluations.append(evaluation)
    ledger.put(item)
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(ctx=kernel.ctx)),
        state=SimpleNamespace(
            user=SimpleNamespace(id="local", role="admin"),
            principal_id="user:local",
            instance_authenticated=False,
        ),
        headers={},
    )
    body = FollowupActionExecutionBody(
        evaluation_id=evaluation.evaluation_id,
        action_id=action.action_id,
        expected_authority_version=evaluation.observed_authority_version,
        idempotency_key="escalate-1",
    )
    asyncio.run(execute_post_turn_action(request, "dispatch-1", action.action_id, body))
    found = stalls(kernel, "manual_review")
    assert len(found) == 1
    assert "contradicts itself" in found[0].body and found[0].dispatch_id == "dispatch-1"
    # Replaying the same action is idempotent and does not notify twice.
    asyncio.run(execute_post_turn_action(request, "dispatch-1", action.action_id, body))
    assert len(stalls(kernel, "manual_review")) == 1
