from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import httpx
from pydantic import ValidationError

from pa.execution.dispatch import CompletionOutbox, DispatchRecord, DispatchStore
from pa.execution.post_turn import (
    ACTION_CATALOG_V1,
    EvidenceReferenceV1,
    FollowupActionName,
    FollowupActionV1,
    PostTurnDecision,
    PostTurnEvaluator,
    SafetyClassification,
    TurnEndSnapshotV1,
    action_catalog,
    is_authorized_same_session_continuation,
    mark_record_only_actions,
)


def snapshot(**updates) -> TurnEndSnapshotV1:
    values = {
        "turn_id": "turn-1",
        "turn_sequence": 1,
        "dispatch_id": "dispatch-1",
        "session_id": "session-1",
        "card_id": "card-1",
        "project_id": "project-1",
        "authority_instance_id": "authority",
        "authority_version": "version-1",
        "originating_instance_id": "target",
        "stop_reason": "end_turn",
        "session_status": "idle",
        "card_lane_before": "active",
        "card_lane_after": "active",
        "dispatch_state": "completed",
        "completion_delivery": {"classification": "acknowledged"},
        "final_outcome_text": "Agent turn ended.",
        "provenance": {"provider": "codex", "mode": "default"},
    }
    values.update(updates)
    return TurnEndSnapshotV1(**values)


def context_for(
    item: TurnEndSnapshotV1,
    *,
    card: dict | None = None,
    watches: list[dict] | None = None,
):
    return PostTurnEvaluator().build_context(
        item,
        card=card or {"id": "card-1", "lane": "active", "updated_at": "version-1"},
        project={"id": "project-1", "title": "PA Core"},
        execution_contract={"repository": "petersky/pa"},
        dispatch_history=[],
        prior_evaluations=[],
        watches=watches or [],
        fleet_capabilities=["git", "github"],
    )


class PostTurnEvaluatorTests(unittest.TestCase):
    def test_blocked_bubblewrap_turn_is_retryable_and_never_successful(self) -> None:
        item = snapshot(
            blockers=["Bubblewrap sandbox denied repository access"],
            failures=[
                {
                    "kind": "sandbox",
                    "message": "permission denied",
                    "recoverable": True,
                }
            ],
        )

        evaluation = PostTurnEvaluator().evaluate(context_for(item))

        self.assertEqual(
            evaluation.decision, PostTurnDecision.RETRYABLE_RUNTIME_FAILURE
        )
        self.assertNotIn("succeeded", evaluation.operator_status_text.casefold())
        redispatch = next(
            action
            for action in evaluation.recommended_actions
            if action.name == FollowupActionName.REDISPATCH_CARD
        )
        self.assertTrue(redispatch.human_approval_required)

    def test_incomplete_implementation_inherits_same_session_authorization(self) -> None:
        item = snapshot(
            disposition={"lane": "active"},
            disposition_status="accepted",
        )
        evaluation = PostTurnEvaluator().evaluate(context_for(item))
        action = next(
            candidate
            for candidate in evaluation.recommended_actions
            if candidate.name == FollowupActionName.PROMPT_SAME_SESSION
        )

        self.assertFalse(action.human_approval_required)
        self.assertEqual(
            action.preconditions["authorization_basis"],
            "original_implementation_dispatch",
        )
        self.assertTrue(
            is_authorized_same_session_continuation(
                action, decision=evaluation.decision, session_id=item.session_id
            )
        )

    def test_scope_expanding_same_session_prompt_remains_approval_gated(self) -> None:
        action = FollowupActionV1(
            name=FollowupActionName.PROMPT_SAME_SESSION,
            parameters={
                "purpose": "Deploy the result.",
                "prompt": "Deploy this implementation to production.",
                "session_id": "session-1",
            },
            preconditions={"authority_version": "version-1"},
            idempotency_key_inputs=["dispatch", "turn", "action"],
            safety=SafetyClassification.EXTERNAL_WRITE,
            human_approval_required=True,
        )
        self.assertFalse(
            is_authorized_same_session_continuation(
                action,
                decision=PostTurnDecision.FURTHER_AGENT_WORK_NEEDED,
                session_id="session-1",
            )
        )

    def test_exact_merged_pr_evidence_achieves_outcome(self) -> None:
        item = snapshot(
            card_lane_after="done",
            deliverables={
                "pr_url": "https://github.com/petersky/pa/pull/999",
                "merge_commit_sha": "c" * 40,
            },
            evidence=[
                EvidenceReferenceV1(
                    kind="merge_commit",
                    reference="c" * 40,
                    provenance="PR supervisor",
                )
            ],
        )
        watch = {
            "id": "watch-1",
            "status": "merged",
            "state": {"state": "merged", "merge_commit_sha": "c" * 40},
        }

        evaluation = PostTurnEvaluator().evaluate(
            context_for(item, card={"lane": "done"}, watches=[watch])
        )

        self.assertEqual(evaluation.decision, PostTurnDecision.OUTCOME_ACHIEVED)
        self.assertEqual(
            evaluation.recommended_actions[0].name, FollowupActionName.NO_ACTION
        )

    def test_end_turn_without_disposition_or_deliverables_is_unknown(self) -> None:
        evaluation = PostTurnEvaluator().evaluate(context_for(snapshot()))

        self.assertEqual(evaluation.decision, PostTurnDecision.UNABLE_TO_DETERMINE)
        self.assertGreaterEqual(len(evaluation.missing_or_ambiguous_evidence), 3)

    def test_catalog_and_action_validation_reject_executable_payloads(self) -> None:
        catalog = action_catalog()
        self.assertEqual(catalog["schema"], ACTION_CATALOG_V1)
        self.assertEqual(len(catalog["actions"]), 12)
        with self.assertRaises(ValidationError):
            FollowupActionV1(
                name="no_action",
                parameters={"reason": "done", "command": "rm -rf /"},
                idempotency_key_inputs=["dispatch", "turn", "action"],
                safety=SafetyClassification.RECORD_ONLY,
                human_approval_required=False,
            )
        with self.assertRaises(ValidationError):
            FollowupActionV1(
                name="unknown_action",
                parameters={"reason": "malicious"},
                idempotency_key_inputs=["dispatch", "turn", "action"],
                safety=SafetyClassification.RECORD_ONLY,
                human_approval_required=False,
            )

    def test_stale_authority_or_context_is_rejected(self) -> None:
        evaluator = PostTurnEvaluator()
        context = context_for(snapshot())
        evaluation = evaluator.evaluate(context)
        with self.assertRaisesRegex(ValueError, "context digest"):
            evaluator.validate_result(
                evaluation,
                expected_context_digest="0" * 64,
                expected_authority_version="version-1",
            )
        with self.assertRaisesRegex(ValueError, "authority version"):
            evaluator.validate_result(
                evaluation,
                expected_context_digest=context.digest,
                expected_authority_version="version-2",
            )

    def test_record_only_action_execution_is_idempotent(self) -> None:
        evaluation = PostTurnEvaluator().evaluate(context_for(snapshot()))
        mark_record_only_actions(evaluation)
        first = evaluation.model_dump(mode="json")
        mark_record_only_actions(evaluation)
        self.assertEqual(first, evaluation.model_dump(mode="json"))

    def test_operator_input_action_accepts_choice_and_structured_contracts(
        self,
    ) -> None:
        action = FollowupActionV1(
            name=FollowupActionName.REQUEST_OPERATOR_INPUT,
            parameters={
                "question": "Choose a target",
                "keep_lane": "waiting",
                "request_id": "target-1",
                "choices": [{"id": "staging", "label": "Staging", "value": "staging"}],
                "allow_freeform": False,
                "allow_cancel": True,
            },
            idempotency_key_inputs=["dispatch", "turn", "action"],
            safety=SafetyClassification.RECORD_ONLY,
            human_approval_required=False,
        )
        self.assertEqual(action.parameters["request_id"], "target-1")
        self.assertEqual(action.parameters["choices"][0]["id"], "staging")


class TerminalDispatchTests(unittest.TestCase):
    def test_acknowledged_completion_cannot_regress_to_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                authority_instance_id="authority",
                authority_url="https://authority.example",
                target_instance_id="target",
                session_id="session-1",
                state="completed",
                acknowledged_at=datetime.now(UTC),
                completion_delivery_class="acknowledged",
            )
            store.put(record)

            store.transition(record, "running", "Late follow-up activity.")

            persisted = store.get("dispatch-1")
            self.assertEqual(persisted.state, "completed")
            self.assertEqual(persisted.public_dict()["effective_state"], "completed")
            self.assertEqual(
                persisted.lifecycle_inconsistencies[-1]["kind"],
                "terminal_dispatch_regression_prevented",
            )

    def test_followup_activity_is_separate_from_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                authority_instance_id="authority",
                authority_url="https://authority.example",
                target_instance_id="target",
                session_id="session-1",
                state="completed",
                acknowledged_at=datetime.now(UTC),
            )
            store.put(record)

            store.record_followup_started(
                record,
                idempotency_key="followup-1",
                prompt_id="prompt-1",
                event_id="event-1",
                event_seq=10,
            )

            persisted = store.get("dispatch-1")
            self.assertEqual(persisted.state, "completed")
            self.assertEqual(persisted.followup_turns[0]["state"], "accepted")


class FollowupTurnDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_followup_turn_uses_separate_delivery_without_reopening(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                card_id="card-1",
                authority_instance_id="authority",
                authority_url="https://authority.example",
                target_instance_id="target",
                session_id="session-1",
                state="completed",
                acknowledged_at=datetime.now(UTC),
                completion_delivery_class="acknowledged",
            )
            store.put(record)
            store.record_followup_started(
                record,
                idempotency_key="followup-1",
                prompt_id="prompt-1",
                event_id="event-1",
                event_seq=10,
            )
            outbox = CompletionOutbox(store, "token")
            self.assertTrue(outbox.queue("session-1", {"stop_reason": "end_turn"}))

            def handler(request: httpx.Request) -> httpx.Response:
                self.assertTrue(request.url.path.endswith("/turn-end"))
                self.assertEqual(
                    request.headers["idempotency-key"],
                    "mutation-1:turn:prompt-1",
                )
                return httpx.Response(200, json={"acknowledged": True})

            outbox._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            pending = store.pending_followup_turns()
            self.assertEqual(len(pending), 1)
            await outbox._send_followup(*pending[0])

            persisted = store.get("dispatch-1")
            self.assertEqual(persisted.state, "completed")
            self.assertEqual(
                persisted.followup_turns[0]["delivery_state"], "acknowledged"
            )
            await outbox.close()
