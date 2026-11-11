"""Post-turn follow-through: evidence survives, actions execute, stalls escalate."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from pa.config import Settings
from pa.domain.models import CardCreate
from pa.execution.dispatch import CompletionOutbox, DispatchRecord


def _boot(tmp_path, **overrides):
    from pa.core.kernel import Kernel
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent

    reset_store()
    reset_instance_agent()
    settings = Settings(
        data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces",
        agent_enabled=False,
        peers=[],
        sync_token="isolated-peer-token",
        **overrides,
    )
    return Kernel.boot(settings=settings).build_app(), settings


def test_outbox_ack_on_shared_store_keeps_turn_end_evidence(tmp_path):
    """Authority and target share one DispatchStore: the outbox's ack write must
    not erase the snapshot/evaluation the /complete handler just persisted."""
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent

    app, settings = _boot(tmp_path)
    with TestClient(app) as client:
        store = app.state.ctx.store
        card = store.create_card(CardCreate(title="worker outcome", lane="active"))
        ledger = app.state.ctx.services["dispatch_store"]
        ledger.put(
            DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="m-1",
                card_id=card.id,
                realm_id="default",
                card_version=card.updated_at.isoformat(),
                card_snapshot=card.model_dump(mode="json"),
                authority_instance_id=settings.instance_id,
                authority_url="http://testserver",
                target_instance_id=settings.instance_id,
                session_id="worker-session",
                state="completion_pending",
                completion_payload={"stop_reason": "end_turn"},
            )
        )
        outbox = CompletionOutbox(ledger, "isolated-peer-token")

        async def forward(url, **kwargs):
            return client.post(url, **kwargs)

        with patch.object(
            outbox, "_http_client", return_value=SimpleNamespace(post=forward)
        ):
            asyncio.run(outbox._send(ledger.get("dispatch-1")))
        done = ledger.get("dispatch-1")
        assert done.state == "completed"
        assert done.completion_delivery_class == "acknowledged"
        assert len(done.turn_end_snapshots) == 1
        assert len(done.post_turn_evaluations) == 1
        assert done.post_turn_context_digests
    reset_store()
    reset_instance_agent()


# ---------------------------------------------------------------------------
# stop_reason is read by the evaluator
# ---------------------------------------------------------------------------
from pa.execution.post_turn import (  # noqa: E402
    FollowupActionName,
    PostTurnDecision,
    PostTurnEvaluator,
    TurnEndSnapshotV1,
    is_authorized_same_session_continuation,
)


def _snapshot(**updates) -> TurnEndSnapshotV1:
    values = {
        "turn_id": "turn-1",
        "turn_sequence": 1,
        "dispatch_id": "dispatch-1",
        "session_id": "session-1",
        "card_id": "card-1",
        "authority_instance_id": "authority",
        "authority_version": "version-1",
        "originating_instance_id": "target",
        "stop_reason": "end_turn",
        "card_lane_before": "active",
        "card_lane_after": "active",
        "dispatch_state": "completed",
        "final_outcome_text": "Agent turn ended.",
        "provenance": {"provider": "codex"},
    }
    values.update(updates)
    return TurnEndSnapshotV1(**values)


def _evaluate(item: TurnEndSnapshotV1, lane: str = "active"):
    evaluator = PostTurnEvaluator()
    context = evaluator.build_context(
        item,
        card={"id": "card-1", "lane": lane, "updated_at": "version-1"},
        project=None,
        execution_contract=None,
        dispatch_history=[],
        prior_evaluations=[],
        watches=[],
        fleet_capabilities=[],
    )
    return evaluator.evaluate(context)


import pytest  # noqa: E402


@pytest.mark.parametrize("stop_reason", ["max_tokens", "max_turn_requests"])
def test_cut_off_turn_is_continued_in_the_same_session_without_approval(stop_reason):
    evaluation = _evaluate(_snapshot(stop_reason=stop_reason))
    assert evaluation.decision == PostTurnDecision.FURTHER_AGENT_WORK_NEEDED
    prompt = next(
        a
        for a in evaluation.recommended_actions
        if a.name == FollowupActionName.PROMPT_SAME_SESSION
    )
    assert not prompt.human_approval_required
    assert stop_reason in evaluation.rationale
    assert is_authorized_same_session_continuation(
        prompt, decision=evaluation.decision, session_id="session-1"
    )


@pytest.mark.parametrize("stop_reason", ["refusal", "cancelled"])
def test_refusal_and_cancelled_turns_need_review_not_a_clean_end(stop_reason):
    evaluation = _evaluate(_snapshot(stop_reason=stop_reason))
    assert evaluation.decision != PostTurnDecision.UNABLE_TO_DETERMINE
    names = {a.name for a in evaluation.recommended_actions}
    assert FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW in names
    assert FollowupActionName.PROMPT_SAME_SESSION not in names


def test_card_already_done_with_clean_stop_is_not_undeterminable():
    evaluation = _evaluate(
        _snapshot(card_lane_before="done", card_lane_after="done"), lane="done"
    )
    assert evaluation.decision == PostTurnDecision.OUTCOME_ACHIEVED


# ---------------------------------------------------------------------------
# The worker: executes eligible actions, escalates the rest
# ---------------------------------------------------------------------------
from datetime import UTC, datetime  # noqa: E402

from pa.execution.dispatch import DispatchStore  # noqa: E402
from pa.execution.post_turn import (  # noqa: E402
    FollowupActionStatus,
    automatic_followups_used,
    mark_record_only_actions,
)
from pa.execution.post_turn_worker import (  # noqa: E402
    PostTurnHooks,
    PostTurnWorker,
    downgrade_is_expected_churn,
)


def _settings(**overrides):
    values = {
        "post_turn_worker_enabled": True,
        "post_turn_max_automatic_followups": 25,
        "post_turn_retry_seconds": 15.0,
        "post_turn_escalation_threshold": 2,
        "post_turn_downgrade_feedback_max": 2,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _Hooks:
    def __init__(self, fail_execute: bool = False) -> None:
        self.executed: list[tuple[str, str]] = []
        self.prompts: list[tuple[str, str]] = []
        self.escalations: list[dict] = []
        self.fail_execute = fail_execute

    async def execute(self, record, evaluation, action, key):
        if self.fail_execute:
            raise RuntimeError("boom")
        self.executed.append((action.name.value, key))

    async def prompt(self, record, message, key):
        self.prompts.append((message, key))

    def escalate(self, record, **kwargs):
        self.escalations.append(kwargs)
        return True

    def hooks(self) -> PostTurnHooks:
        return PostTurnHooks(
            execute=self.execute, prompt=self.prompt, escalate=self.escalate
        )


def _record_with(tmp_path, snapshots, **fields) -> tuple[DispatchStore, DispatchRecord]:
    store = DispatchStore(tmp_path)
    evaluator = PostTurnEvaluator()
    evaluations = []
    for item in snapshots:
        context = evaluator.build_context(
            item,
            card={"id": "card-1", "lane": item.card_lane_after, "updated_at": "version-1"},
            project=None,
            execution_contract=None,
            dispatch_history=[],
            prior_evaluations=[],
            watches=[],
            fleet_capabilities=[],
        )
        evaluation = evaluator.evaluate(context)
        mark_record_only_actions(evaluation)
        evaluations.append(evaluation)
    record = DispatchRecord(
        dispatch_id="dispatch-1",
        mutation_id="m-1",
        card_id="card-1",
        realm_id="default",
        authority_instance_id="a",
        authority_url="http://a",
        target_instance_id="t",
        session_id="session-1",
        state="completed",
        acknowledged_at=datetime.now(UTC),
        turn_end_snapshots=list(snapshots),
        post_turn_evaluations=evaluations,
        **fields,
    )
    store.put(record)
    return store, store.get("dispatch-1")


def _worker(store, hooks, **settings) -> PostTurnWorker:
    return PostTurnWorker(store, hooks.hooks(), lambda: _settings(**settings))


@pytest.mark.asyncio
async def test_worker_runs_the_authorized_continuation_once(tmp_path):
    store, record = _record_with(
        tmp_path, [_snapshot(stop_reason="max_tokens")]
    )
    hooks = _Hooks()
    worker = _worker(store, hooks)
    await worker.run_once()
    assert [name for name, _ in hooks.executed] == ["prompt_same_session"]
    assert not hooks.escalations


@pytest.mark.asyncio
async def test_worker_escalates_when_the_automatic_budget_is_spent(tmp_path):
    store, record = _record_with(
        tmp_path, [_snapshot(stop_reason="max_tokens")]
    )
    hooks = _Hooks()
    worker = _worker(store, hooks, post_turn_max_automatic_followups=0)
    await worker.run_once()
    await worker.run_once()
    assert hooks.executed == []
    assert len(hooks.escalations) == 1
    assert hooks.escalations[0]["dedupe_key"].startswith("budget:")


@pytest.mark.asyncio
async def test_worker_surfaces_approval_gated_actions_and_never_executes_them(tmp_path):
    snap = _snapshot(
        blockers=["Bubblewrap sandbox denied repository access"],
        failures=[{"kind": "sandbox", "message": "permission denied", "recoverable": True}],
    )
    store, record = _record_with(tmp_path, [snap])
    hooks = _Hooks()
    worker = _worker(store, hooks)
    await worker.run_once()
    await worker.run_once()
    assert hooks.executed == []
    assert len(hooks.escalations) == 1
    assert "redispatch card" in hooks.escalations[0]["title"].lower()
    assert "needs your approval" in hooks.escalations[0]["body"]


@pytest.mark.asyncio
async def test_worker_escalates_repeated_unable_to_determine_once(tmp_path):
    first = _snapshot()
    second = _snapshot(turn_id="turn-2", turn_sequence=2)
    store, record = _record_with(tmp_path, [first, second])
    assert [e.decision for e in record.post_turn_evaluations] == [
        PostTurnDecision.UNABLE_TO_DETERMINE
    ] * 2
    hooks = _Hooks()
    worker = _worker(store, hooks)
    await worker.run_once()
    await worker.run_once()
    assert len(hooks.escalations) == 1
    assert hooks.escalations[0]["dedupe_key"].endswith("unable_to_determine")


@pytest.mark.asyncio
async def test_single_unable_to_determine_is_not_escalated(tmp_path):
    store, _ = _record_with(tmp_path, [_snapshot()])
    hooks = _Hooks()
    await _worker(store, hooks).run_once()
    assert hooks.escalations == []


@pytest.mark.asyncio
async def test_worker_off_switch_restores_record_only_behaviour(tmp_path):
    store, _ = _record_with(tmp_path, [_snapshot(stop_reason="max_tokens")])
    hooks = _Hooks()
    await _worker(store, hooks, post_turn_worker_enabled=False).run_once()
    assert hooks.executed == [] and hooks.escalations == []


@pytest.mark.asyncio
async def test_failing_execution_backs_off_then_escalates_at_threshold(tmp_path):
    store, _ = _record_with(tmp_path, [_snapshot(stop_reason="max_tokens")])
    hooks = _Hooks(fail_execute=True)
    worker = _worker(store, hooks, post_turn_escalation_threshold=2)
    await worker.run_once()
    assert hooks.escalations == []
    # Backed off: an immediate second pass does not hammer the action.
    await worker.run_once()
    assert hooks.escalations == []
    for key, (count, _when) in list(worker._failures.items()):
        worker._failures[key] = (count, datetime.now(UTC))
    await worker.run_once()
    assert len(hooks.escalations) == 1
    assert "after 2 attempts" in hooks.escalations[0]["body"]


@pytest.mark.asyncio
async def test_stale_evaluations_are_history_not_work(tmp_path):
    store, record = _record_with(tmp_path, [_snapshot(stop_reason="max_tokens")])
    from datetime import timedelta

    record.post_turn_evaluations[-1].created_at = datetime.now(UTC) - timedelta(days=1)
    store.put(record)
    hooks = _Hooks()
    await _worker(store, hooks).run_once()
    assert hooks.executed == []


def test_pr_in_flight_downgrades_are_expected_churn():
    assert downgrade_is_expected_churn(
        "Done was downgraded to Waiting: linked PR watch w1 is open, not merged"
    )
    assert downgrade_is_expected_churn(
        "Done was downgraded to Waiting: the watched head has not satisfied the stability window"
    )
    assert not downgrade_is_expected_churn(
        "Done was downgraded to Waiting: integration requirements were not "
        "explicitly ruled out and no linked PR watch exists"
    )
    assert not downgrade_is_expected_churn(
        "Done was downgraded to Waiting: linked PR watch w1 is closed, not merged"
    )


@pytest.mark.asyncio
async def test_downgraded_done_claim_gets_one_bounded_explanation(tmp_path):
    reason = (
        "Done was downgraded to Waiting: integration requirements were not "
        "explicitly ruled out and no linked PR watch exists"
    )
    store, _ = _record_with(
        tmp_path,
        [_snapshot(disposition={"lane": "done", "evidence": {}})],
        card_disposition_status="downgraded",
        card_disposition_reason=reason,
    )
    hooks = _Hooks()
    worker = _worker(store, hooks)
    await worker.run_once()
    await worker.run_once()
    assert len(hooks.prompts) == 1
    message = hooks.prompts[0][0]
    assert "no linked PR watch exists" in message
    assert "integration_required" in message


@pytest.mark.asyncio
async def test_downgrade_feedback_is_capped_per_card_and_skips_pr_churn(tmp_path):
    churn = "Done was downgraded to Waiting: linked PR watch w1 is open, not merged"
    store, _ = _record_with(
        tmp_path,
        [_snapshot()],
        card_disposition_status="downgraded",
        card_disposition_reason=churn,
    )
    hooks = _Hooks()
    await _worker(store, hooks).run_once()
    assert hooks.prompts == []

    real = (
        "Done was downgraded to Waiting: integration requirements were not "
        "explicitly ruled out and no linked PR watch exists"
    )
    current = store.get("dispatch-1")
    current.card_disposition_reason = real
    store.put(current)
    await _worker(store, hooks, post_turn_downgrade_feedback_max=0).run_once()
    assert hooks.prompts == []
    # Another dispatch of the same card already used the whole allowance.
    other = current.model_copy(deep=True)
    other.dispatch_id, other.mutation_id = "dispatch-0", "m-0"
    other.post_turn_action_receipts = {}
    store.put(other)
    await _worker(store, hooks, post_turn_downgrade_feedback_max=1).run_once()
    assert len(hooks.prompts) == 1  # only one of the two dispatches may use it


def test_complete_then_worker_continues_the_session_and_counts_the_budget(tmp_path):
    """End to end on a shared store: /complete evaluates, the worker prompts the
    bound session with the evaluator's reason, and the budget is consumed."""
    from unittest.mock import AsyncMock

    import pa.modules.fleet as fleet
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent

    app, settings = _boot(tmp_path, post_turn_retry_seconds=3600.0)
    with TestClient(app) as client:
        store = app.state.ctx.store
        card = store.create_card(CardCreate(title="worker outcome", lane="active"))
        ledger = app.state.ctx.services["dispatch_store"]
        ledger.put(
            DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="m-1",
                card_id=card.id,
                realm_id="default",
                card_version=card.updated_at.isoformat(),
                card_snapshot=card.model_dump(mode="json"),
                authority_instance_id=settings.instance_id,
                authority_url="http://testserver",
                target_instance_id=settings.instance_id,
                session_id="worker-session",
                state="completion_pending",
                completion_payload={"stop_reason": "max_tokens"},
            )
        )
        outbox = CompletionOutbox(ledger, "isolated-peer-token")

        async def forward(url, **kwargs):
            return client.post(url, **kwargs)

        with patch.object(
            outbox, "_http_client", return_value=SimpleNamespace(post=forward)
        ):
            asyncio.run(outbox._send(ledger.get("dispatch-1")))

        record = ledger.get("dispatch-1")
        assert record.post_turn_evaluations[-1].decision == (
            PostTurnDecision.FURTHER_AGENT_WORK_NEEDED
        )
        assert automatic_followups_used(record.post_turn_evaluations) == 0

        worker = fleet._build_post_turn_worker(app, app.state.ctx, None)
        sent = AsyncMock(return_value={"accepted": True})
        with patch.object(fleet, "prompt_dispatch_session", sent):
            assert asyncio.run(worker.run_once()) == 1
            assert asyncio.run(worker.run_once()) == 0

        assert sent.await_count == 1
        message = sent.await_args.args[2].message
        assert "Continue from where you left off" in message
        assert "stop_reason=max_tokens" in message
        record = ledger.get("dispatch-1")
        action = next(
            a
            for a in record.post_turn_evaluations[-1].recommended_actions
            if a.name == FollowupActionName.PROMPT_SAME_SESSION
        )
        assert action.status == FollowupActionStatus.EXECUTED
        assert automatic_followups_used(record.post_turn_evaluations) == 1
    reset_store()
    reset_instance_agent()


# ---------------------------------------------------------------------------
# A turn that never produced a disposition no longer ends quietly
# ---------------------------------------------------------------------------
def _reconciler_fixture(tmp_path, **kwargs):
    from tests.test_completion_reconciliation import CompletionReconciliationTests

    return CompletionReconciliationTests().make_fixture(tmp_path, **kwargs)


@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.asyncio
async def test_two_failed_reconciliation_prompts_park_the_card_and_notify(
    tmp_path, enabled
):
    ledger, runtime, _supervisor, outbox, reconciler = _reconciler_fixture(tmp_path)
    notices: list[tuple[str, str]] = []
    reconciler.escalate_missing_disposition = lambda: enabled
    reconciler.stall_notifier = lambda record, reason: notices.append(
        (record.dispatch_id, reason)
    )
    await reconciler.handle_completion("session-1", {})
    first = ledger.get("dispatch-1")
    await reconciler.handle_completion(
        "session-1",
        {"queued_prompt_id": first.reconciliation_prompt_id, "card_disposition_error": "x"},
    )
    second = ledger.get("dispatch-1")
    await reconciler.handle_completion(
        "session-1",
        {"queued_prompt_id": second.reconciliation_prompt_id, "card_disposition_error": "y"},
    )
    assert ledger.get("dispatch-1").reconciliation_state == "failed"
    delivered = outbox.payloads[-1][1]
    if enabled:
        assert delivered["card_disposition"]["lane"] == "waiting"
        assert "never reported a valid card disposition" in (
            delivered["card_disposition"]["outcome"]
        )
        assert len(notices) == 1 and notices[0][0] == "dispatch-1"
    else:
        # Old behaviour: delivered with no disposition and no notice.
        assert "card_disposition" not in delivered
        assert notices == []


@pytest.mark.asyncio
async def test_exhausted_blocked_reconciliation_parks_the_card_and_notifies(tmp_path):
    ledger, runtime, _supervisor, outbox, reconciler = _reconciler_fixture(
        tmp_path, supervisor_state="authority_unreachable", max_attempts=2
    )
    notices: list = []
    reconciler.escalate_missing_disposition = lambda: True
    reconciler.stall_notifier = lambda record, reason: notices.append(reason)
    await reconciler.handle_completion("session-1", {})
    blocked = ledger.get("dispatch-1")
    blocked.reconciliation_next_retry_at = None
    ledger.put(blocked)
    await reconciler._advance(blocked)
    assert ledger.get("dispatch-1").reconciliation_state == "exhausted"
    assert outbox.payloads[-1][1]["card_disposition"]["lane"] == "waiting"
    assert len(notices) == 1


def test_cancelled_turn_reaches_the_owner_as_a_manual_review_notification(tmp_path):
    """stop_reason=cancelled is evaluated, executed by the worker through the
    real action endpoint, and surfaces as a manual-review stall."""
    import pa.modules.fleet as fleet
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent

    app, settings = _boot(tmp_path, post_turn_retry_seconds=3600.0)
    with TestClient(app) as client:
        card = app.state.ctx.store.create_card(CardCreate(title="t", lane="active"))
        ledger = app.state.ctx.services["dispatch_store"]
        ledger.put(
            DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="m-1",
                card_id=card.id,
                realm_id="default",
                card_version=card.updated_at.isoformat(),
                card_snapshot=card.model_dump(mode="json"),
                authority_instance_id=settings.instance_id,
                authority_url="http://testserver",
                target_instance_id=settings.instance_id,
                session_id="worker-session",
                state="completion_pending",
                completion_payload={"stop_reason": "cancelled"},
            )
        )
        outbox = CompletionOutbox(ledger, "isolated-peer-token")

        async def forward(url, **kwargs):
            return client.post(url, **kwargs)

        with patch.object(
            outbox, "_http_client", return_value=SimpleNamespace(post=forward)
        ):
            asyncio.run(outbox._send(ledger.get("dispatch-1")))

        worker = fleet._build_post_turn_worker(app, app.state.ctx, None)
        assert asyncio.run(worker.run_once()) == 1
        record = ledger.get("dispatch-1")
        action = next(
            a
            for a in record.post_turn_evaluations[-1].recommended_actions
            if a.name == FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW
        )
        assert action.status == FollowupActionStatus.EXECUTED
        service = app.state.ctx.services["notifications"]
        found = service.store.find_notification_by_dedup(
            "default", f"stall:manual_review:{action.action_id}"
        )
        assert found is not None
        assert asyncio.run(worker.run_once()) == 0
    reset_store()
    reset_instance_agent()


def test_stale_whole_record_put_keeps_evaluator_evidence(tmp_path):
    """Any holder of a pre-evaluation snapshot (worker, outbox, reconciler) can
    still put() it without erasing what the authority wrote meanwhile."""
    store, record = _record_with(tmp_path, [_snapshot(stop_reason="max_tokens")])
    stale = DispatchRecord(
        **{
            **record.model_dump(),
            "turn_end_snapshots": [],
            "post_turn_evaluations": [],
            "post_turn_context_digests": {},
        }
    )
    stale.attempts += 1
    store.put(stale)
    kept = store.get("dispatch-1")
    assert kept.attempts == stale.attempts
    assert len(kept.turn_end_snapshots) == 1
    assert len(kept.post_turn_evaluations) == 1


def test_inactivity_cancelled_turn_is_continued_not_escalated():
    """A turn PA cancelled because the provider went silent is unfinished work.

    The session layer records it as stop_reason cancelled with reason
    inactivity_timeout; the snapshot maps that to ``inactivity_timeout`` so the
    evaluator continues the same session instead of asking for manual review.
    """
    from pa.modules.fleet import _snapshot_stop_reason

    assert _snapshot_stop_reason({"stop_reason": "cancelled", "reason": "inactivity_timeout"}) == "inactivity_timeout"
    assert _snapshot_stop_reason({"stop_reason": "cancelled"}) == "cancelled"
    assert _snapshot_stop_reason({"stop_reason": "end_turn", "reason": "inactivity_timeout"}) == "end_turn"
    evaluation = _evaluate(_snapshot(stop_reason="inactivity_timeout"))
    assert evaluation.decision == PostTurnDecision.FURTHER_AGENT_WORK_NEEDED
    names = {a.name for a in evaluation.recommended_actions}
    assert FollowupActionName.PROMPT_SAME_SESSION in names
    assert FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW not in names
