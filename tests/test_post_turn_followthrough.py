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
