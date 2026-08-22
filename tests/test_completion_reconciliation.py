from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from pa.acp.client import normalize_session_update
from pa.acp.final_message import assemble_final_assistant_message
from pa.domain.models import AgentSession, TranscriptEvent
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.disposition import extract_card_disposition
from pa.execution.reconciliation import (
    RECONCILIATION_SOURCE_PREFIX,
    CompletionReconciler,
)
from pa.instance.quiesce import QueuedPrompt


def disposition(lane: str = "active") -> dict:
    return {
        "contract": "pa.card-disposition/v1",
        "lane": lane,
        "outcome": f"Verified {lane} disposition.",
        "evidence": {
            "integration_required": lane != "active",
            "references": ["card:card-1"],
        },
    }


class FakeRuntime:
    def __init__(self, session: AgentSession) -> None:
        self.session = session
        self.connected = True
        self._closed = False
        self._queue: list[QueuedPrompt] = []
        self._in_flight = None
        self.enqueued = 0

    def enqueue(self, message: str, **kwargs) -> QueuedPrompt:
        self.enqueued += 1
        item = QueuedPrompt(
            message=message,
            session_id=self.session.id,
            card_id=kwargs.get("card_id"),
            project_id=kwargs.get("project_id"),
            source=kwargs.get("source") or "api",
            prompt_audit=kwargs.get("prompt_audit") or [],
        )
        self._queue.insert(0, item)
        return item


class FakeOutbox:
    def __init__(self) -> None:
        self._wake = asyncio.Event()
        self.payloads: list[tuple[str, dict]] = []

    def queue(self, session_id: str, payload: dict) -> bool:
        self.payloads.append((session_id, payload))
        return True


class FakeSupervisor:
    def __init__(self, state: str = "ready") -> None:
        self.state = state

    def authority_health(self) -> dict:
        return {"state": self.state}


class CompletionReconciliationTests(unittest.TestCase):
    def make_fixture(
        self,
        root: Path,
        *,
        session_status: str = "idle",
        resumable: bool = True,
        supervisor_state: str = "ready",
        max_attempts: int = 3,
    ):
        ledger = DispatchStore(root)
        ledger.put(
            DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                card_id="card-1",
                project_id="project-1",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-1",
                state="running",
            )
        )
        session = AgentSession(
            id="session-1",
            agent_name="codex",
            external_session_id="provider-session" if resumable else None,
            card_id="card-1",
            project_id="project-1",
            status=session_status,
        )
        runtime = FakeRuntime(session)
        agent = SimpleNamespace(
            async_runtime=None,
            store=SimpleNamespace(get_session=lambda _session_id: session),
            get=lambda _session_id: runtime,
        )
        card_store = MagicMock()
        card_store.get_card.return_value = SimpleNamespace(id="card-1")
        supervisor = FakeSupervisor(supervisor_state)
        outbox = FakeOutbox()
        reconciler = CompletionReconciler(
            ledger,
            agent,
            outbox,
            card_store,
            lambda: supervisor,
            retry_seconds=0.01,
            max_attempts=max_attempts,
        )
        return ledger, runtime, supervisor, outbox, reconciler

    def test_strict_machine_readable_extraction(self) -> None:
        value, error = extract_card_disposition(
            f"```json\n{json.dumps(disposition())}\n```"
        )
        prose, prose_error = extract_card_disposition(
            f"Done.\n{json.dumps(disposition('done'))}"
        )

        self.assertEqual(value["lane"], "active")
        self.assertIsNone(error)
        self.assertIsNone(prose)
        self.assertIn("exactly one JSON object", prose_error)

    def test_live_final_answer_excludes_commentary_and_other_messages(self) -> None:
        final_json = json.dumps(disposition("waiting"))
        updates = [
            normalize_session_update(
                {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "commentary-1",
                    "content": {"type": "text", "text": "I am checking the PR."},
                    "_meta": {"codex": {"phase": "commentary"}},
                }
            ),
            normalize_session_update(
                {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "stale-final",
                    "content": {"type": "text", "text": "{}"},
                    "_meta": {"codex": {"phase": "final"}},
                }
            ),
            normalize_session_update(
                {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "disposition-final",
                    "content": {"type": "text", "text": final_json},
                    "_meta": {"codex": {"phase": "final_answer"}},
                }
            ),
        ]

        final_text = assemble_final_assistant_message(updates)
        value, error = extract_card_disposition(final_text)

        self.assertEqual(final_text, final_json)
        self.assertEqual(value["lane"], "waiting")
        self.assertIsNone(error)
        self.assertEqual(updates[-1]["phase"], "final")

    def test_retained_final_phase_aliases_share_selection_rules(self) -> None:
        for phase in ("final", "final_answer"):
            with self.subTest(phase=phase):
                final_json = json.dumps(disposition())
                final_text = assemble_final_assistant_message(
                    [
                        {
                            "type": "agent_message_chunk",
                            "text": "commentary",
                            "phase": "commentary",
                            "message_id": "commentary",
                        },
                        {
                            "type": "agent_message_chunk",
                            "text": final_json,
                            "phase": phase,
                            "message_id": "final",
                        },
                    ]
                )
                self.assertEqual(final_text, final_json)

        final_json = json.dumps(disposition())
        midpoint = len(final_json) // 2
        self.assertEqual(
            assemble_final_assistant_message(
                [
                    {
                        "type": "agent_message_chunk",
                        "text": final_json[:midpoint],
                        "phase": "final",
                        "message_id": "same-final",
                    },
                    {
                        "type": "agent_message_chunk",
                        "text": final_json[midpoint:],
                        "phase": "final_answer",
                        "message_id": "same-final",
                    },
                ]
            ),
            final_json,
        )

    def test_waiting_disposition_allows_null_pr_watch_evidence(self) -> None:
        requested = disposition("waiting")
        requested["evidence"].update(
            {
                "integration_required": True,
                "pr_watch_id": None,
                "watched_head_sha": None,
                "merge_commit_sha": None,
            }
        )

        value, error = extract_card_disposition(json.dumps(requested))

        self.assertEqual(value["lane"], "waiting")
        self.assertIsNone(value["evidence"]["pr_watch_id"])
        self.assertIsNone(error)

    def test_successful_followup_resolves_and_delivers(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp)
                )
                await reconciler.handle_completion(
                    "session-1", {"queued_prompt_id": "initial"}
                )
                prompted = ledger.get("dispatch-1")
                self.assertEqual(prompted.reconciliation_state, "prompted")
                self.assertEqual(prompted.reconciliation_prompt_count, 1)

                await reconciler.handle_completion(
                    "session-1",
                    {
                        "queued_prompt_id": prompted.reconciliation_prompt_id,
                        "prompt_source": (f"{RECONCILIATION_SOURCE_PREFIX}dispatch-1"),
                        "card_disposition": disposition("active"),
                    },
                )

                resolved = ledger.get("dispatch-1")
                self.assertEqual(resolved.reconciliation_state, "resolved")
                self.assertEqual(runtime.enqueued, 1)
                self.assertEqual(
                    outbox.payloads[-1][1]["card_disposition"]["lane"], "active"
                )

        asyncio.run(run())

    def test_open_pr_waiting_disposition_is_delivered_as_waiting(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, _runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp)
                )
                await reconciler.handle_completion("session-1", {})
                prompt_id = ledger.get("dispatch-1").reconciliation_prompt_id
                await reconciler.handle_completion(
                    "session-1",
                    {
                        "queued_prompt_id": prompt_id,
                        "card_disposition": disposition("waiting"),
                    },
                )

                self.assertEqual(
                    outbox.payloads[-1][1]["card_disposition"]["lane"], "waiting"
                )
                self.assertEqual(
                    ledger.get("dispatch-1").reconciliation_state, "resolved"
                )

        asyncio.run(run())

    def test_blocked_service_recovers_and_prompts_once(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, supervisor, _outbox, reconciler = self.make_fixture(
                    Path(tmp), supervisor_state="authority_unreachable"
                )
                await reconciler.handle_completion("session-1", {})
                blocked = ledger.get("dispatch-1")
                self.assertEqual(blocked.reconciliation_state, "blocked")
                self.assertTrue(blocked.reconciliation_recoverable)
                self.assertEqual(runtime.enqueued, 0)

                supervisor.state = "ready"
                blocked.reconciliation_next_retry_at = None
                ledger.put(blocked)
                await reconciler._advance(blocked)

                recovered = ledger.get("dispatch-1")
                self.assertEqual(recovered.reconciliation_state, "prompted")
                self.assertEqual(recovered.reconciliation_prompt_count, 1)
                self.assertEqual(runtime.enqueued, 1)

        asyncio.run(run())

    def test_closed_session_is_not_prompted(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp), session_status="closed"
                )
                await reconciler.handle_completion("session-1", {})

                record = ledger.get("dispatch-1")
                self.assertEqual(record.reconciliation_state, "skipped_closed")
                self.assertEqual(runtime.enqueued, 0)
                self.assertEqual(len(outbox.payloads), 1)

        asyncio.run(run())

    def test_non_resumable_session_is_not_prompted(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp), resumable=False
                )
                await reconciler.handle_completion("session-1", {})

                record = ledger.get("dispatch-1")
                self.assertEqual(record.reconciliation_state, "skipped_non_resumable")
                self.assertEqual(runtime.enqueued, 0)
                self.assertEqual(len(outbox.payloads), 1)

        asyncio.run(run())

    def test_duplicate_completion_does_not_repeat_prompt(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, _outbox, reconciler = self.make_fixture(
                    Path(tmp)
                )
                await reconciler.handle_completion(
                    "session-1", {"queued_prompt_id": "initial"}
                )
                await reconciler.handle_completion(
                    "session-1", {"queued_prompt_id": "initial"}
                )

                self.assertEqual(runtime.enqueued, 1)
                self.assertEqual(
                    ledger.get("dispatch-1").reconciliation_prompt_count, 1
                )

        asyncio.run(run())

    def test_restart_adopts_durable_prompt_without_duplicate(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, runtime, supervisor, outbox, reconciler = self.make_fixture(
                    root
                )
                source = f"{RECONCILIATION_SOURCE_PREFIX}dispatch-1"
                existing = runtime.enqueue(
                    "durable reconciliation",
                    card_id="card-1",
                    project_id="project-1",
                    source=source,
                )
                record = ledger.get("dispatch-1")
                record.reconciliation_state = "pending"
                record.completion_payload = {}
                ledger.put(record)

                reloaded = DispatchStore(root)
                restarted = CompletionReconciler(
                    reloaded,
                    reconciler.agent,
                    outbox,
                    reconciler.card_store,
                    lambda: supervisor,
                    retry_seconds=0.01,
                )
                await restarted._advance(reloaded.get("dispatch-1"))

                adopted = reloaded.get("dispatch-1")
                self.assertEqual(adopted.reconciliation_state, "prompted")
                self.assertEqual(adopted.reconciliation_prompt_id, existing.id)
                self.assertEqual(adopted.reconciliation_prompt_count, 1)
                self.assertEqual(runtime.enqueued, 1)

        asyncio.run(run())

    def test_restart_recovers_completed_reconciliation_turn(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp)
                )
                await reconciler.handle_completion("session-1", {})
                record = ledger.get("dispatch-1")
                prompt_id = record.reconciliation_prompt_id
                runtime._queue.clear()
                events = [
                    TranscriptEvent(
                        session_id="session-1",
                        seq=1,
                        event_type="user_message",
                        payload={
                            "id": prompt_id,
                            "source": (f"{RECONCILIATION_SOURCE_PREFIX}dispatch-1"),
                        },
                    ),
                    TranscriptEvent(
                        session_id="session-1",
                        seq=2,
                        event_type="agent_message_chunk",
                        payload={
                            "text": "Rechecking the durable card state.",
                            "phase": "commentary",
                            "message_id": "commentary",
                        },
                    ),
                    TranscriptEvent(
                        session_id="session-1",
                        seq=3,
                        event_type="agent_message_chunk",
                        payload={
                            "text": json.dumps(disposition("waiting")),
                            "phase": "final_answer",
                            "message_id": "disposition",
                        },
                    ),
                    TranscriptEvent(
                        session_id="session-1",
                        seq=4,
                        event_type="turn_completed",
                        payload={
                            "queued_prompt_id": prompt_id,
                            "stop_reason": "end_turn",
                        },
                    ),
                ]
                reconciler.agent.store.list_transcript_events_before = (
                    lambda _session_id, limit: events
                )

                await reconciler._recover_prompted(record)

                self.assertEqual(
                    ledger.get("dispatch-1").reconciliation_state, "resolved"
                )
                self.assertEqual(
                    outbox.payloads[-1][1]["card_disposition"]["lane"], "waiting"
                )
                self.assertEqual(runtime.enqueued, 1)

        asyncio.run(run())

    def test_restart_recovers_valid_initial_turn_before_marking_absent(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, _runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp)
                )
                record = ledger.get("dispatch-1")
                record.completion_payload = {
                    "queued_prompt_id": "initial-prompt",
                    "prompt_source": "dispatch",
                }
                ledger.put(record)
                events = [
                    TranscriptEvent(
                        session_id="session-1",
                        seq=1,
                        event_type="user_message",
                        payload={"id": "old-prompt", "source": "dispatch"},
                    ),
                    TranscriptEvent(
                        session_id="session-1",
                        seq=2,
                        event_type="agent_message_chunk",
                        payload={
                            "text": json.dumps(disposition("active")),
                            "phase": "final",
                            "message_id": "old-final",
                        },
                    ),
                    TranscriptEvent(
                        session_id="session-1",
                        seq=3,
                        event_type="turn_completed",
                        payload={"queued_prompt_id": "old-prompt"},
                    ),
                    TranscriptEvent(
                        session_id="session-1",
                        seq=4,
                        event_type="user_message",
                        payload={"id": "initial-prompt", "source": "dispatch"},
                    ),
                    TranscriptEvent(
                        session_id="session-1",
                        seq=5,
                        event_type="agent_message_chunk",
                        payload={
                            "text": "The PR is still open.",
                            "phase": "commentary",
                            "message_id": "current-commentary",
                        },
                    ),
                    TranscriptEvent(
                        session_id="session-1",
                        seq=6,
                        event_type="agent_message_chunk",
                        payload={
                            "text": json.dumps(disposition("waiting")),
                            "phase": "final_answer",
                            "message_id": "current-final",
                        },
                    ),
                    TranscriptEvent(
                        session_id="session-1",
                        seq=7,
                        event_type="turn_completed",
                        payload={
                            "queued_prompt_id": "initial-prompt",
                            "stop_reason": "end_turn",
                        },
                    ),
                ]
                reconciler.agent.store.list_transcript_events_before = (
                    lambda _session_id, limit: events
                )

                await reconciler.recover()

                recovered = ledger.get("dispatch-1")
                self.assertEqual(recovered.reconciliation_state, "not_required")
                self.assertIsNone(recovered.card_disposition_error)
                self.assertEqual(
                    outbox.payloads[-1][1]["card_disposition"]["lane"], "waiting"
                )

        asyncio.run(run())

    def test_reconciliation_preserves_exact_extraction_error_in_public_state(
        self,
    ) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp)
                )
                await reconciler.handle_completion("session-1", {})
                prompted = ledger.get("dispatch-1")
                exact_error = (
                    "The final response was not exactly one JSON object: "
                    "Expecting value: line 1 column 1 (char 0)"
                )

                await reconciler.handle_completion(
                    "session-1",
                    {
                        "queued_prompt_id": prompted.reconciliation_prompt_id,
                        "prompt_source": (
                            f"{RECONCILIATION_SOURCE_PREFIX}dispatch-1"
                        ),
                        "card_disposition_error": exact_error,
                    },
                )

                retrying = ledger.get("dispatch-1")
                self.assertEqual(retrying.card_disposition_error, exact_error)
                self.assertIn(exact_error, retrying.reconciliation_reason)
                self.assertEqual(retrying.reconciliation_state, "prompted")
                self.assertEqual(retrying.reconciliation_prompt_count, 2)
                self.assertEqual(retrying.reconciliation_parse_errors, [exact_error])
                self.assertIn(exact_error, runtime._queue[0].message)
                self.assertEqual(
                    retrying.public_dict()["card_reconciliation"][
                        "disposition_error"
                    ],
                    exact_error,
                )
                self.assertEqual(len(outbox.payloads), 0)

        asyncio.run(run())

    def test_prose_plus_json_retries_once_then_valid_json_is_applied(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp)
                )
                await reconciler.handle_completion("session-1", {})
                first = ledger.get("dispatch-1")
                exact_error = (
                    "The final response was not exactly one JSON object: "
                    "Expecting value: line 1 column 1 (char 0)"
                )
                mixed = f"I will revalidate first.{json.dumps(disposition())}"
                await reconciler.handle_completion(
                    "session-1",
                    {
                        "queued_prompt_id": first.reconciliation_prompt_id,
                        "prompt_source": f"{RECONCILIATION_SOURCE_PREFIX}dispatch-1",
                        "card_disposition_error": exact_error,
                        "final_outcome_text": mixed,
                    },
                )
                retry = ledger.get("dispatch-1")
                self.assertEqual(retry.reconciliation_state, "prompted")
                self.assertEqual(retry.reconciliation_prompt_count, 2)
                self.assertEqual(len(retry.reconciliation_prompt_ids), 2)
                self.assertIn(exact_error, runtime._queue[0].message)
                self.assertIn("I will revalidate first.", runtime._queue[0].message)
                self.assertIn(
                    "no progress prose, no Markdown", runtime._queue[0].message
                )

                await reconciler.handle_completion(
                    "session-1",
                    {
                        "queued_prompt_id": retry.reconciliation_prompt_id,
                        "prompt_source": f"{RECONCILIATION_SOURCE_PREFIX}dispatch-1",
                        "card_disposition": disposition("done"),
                    },
                )
                applied = ledger.get("dispatch-1")
                self.assertEqual(applied.reconciliation_state, "resolved")
                self.assertEqual(applied.reconciliation_prompt_count, 2)
                self.assertEqual(
                    outbox.payloads[-1][1]["card_disposition"]["lane"], "done"
                )

        asyncio.run(run())

    def test_empty_or_unparsable_twice_fails_without_third_prompt(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp)
                )
                await reconciler.handle_completion("session-1", {})
                first = ledger.get("dispatch-1")
                await reconciler.handle_completion(
                    "session-1",
                    {
                        "queued_prompt_id": first.reconciliation_prompt_id,
                        "card_disposition_error": "empty final response",
                        "final_outcome_text": "",
                    },
                )
                second = ledger.get("dispatch-1")
                await reconciler.handle_completion(
                    "session-1",
                    {
                        "queued_prompt_id": second.reconciliation_prompt_id,
                        "card_disposition_error": "still not JSON",
                        "final_outcome_text": "not json",
                    },
                )
                failed = ledger.get("dispatch-1")
                self.assertEqual(failed.reconciliation_state, "failed")
                self.assertEqual(failed.reconciliation_prompt_count, 2)
                self.assertEqual(runtime.enqueued, 2)
                self.assertEqual(
                    failed.reconciliation_parse_errors,
                    ["empty final response", "still not JSON"],
                )
                self.assertEqual(len(outbox.payloads), 1)

        asyncio.run(run())

    def test_first_parse_failure_does_not_retry_when_tooling_unavailable(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, supervisor, _outbox, reconciler = self.make_fixture(
                    Path(tmp)
                )
                await reconciler.handle_completion("session-1", {})
                first = ledger.get("dispatch-1")
                supervisor.state = "unavailable"
                await reconciler.handle_completion(
                    "session-1",
                    {
                        "queued_prompt_id": first.reconciliation_prompt_id,
                        "card_disposition_error": "not JSON",
                    },
                )
                blocked = ledger.get("dispatch-1")
                self.assertEqual(blocked.reconciliation_state, "blocked")
                self.assertEqual(blocked.reconciliation_prompt_count, 1)
                self.assertEqual(runtime.enqueued, 1)

        asyncio.run(run())

    def test_operator_retry_normalizes_acknowledged_done_without_prompt(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, _outbox, reconciler = self.make_fixture(
                    Path(tmp), session_status="closed"
                )
                record = ledger.get("dispatch-1")
                record.state = "completed"
                record.acknowledged_at = datetime.now(UTC)
                record.reconciliation_state = "failed"
                record.completion_payload = {"outcome": "already acknowledged"}
                ledger.put(record)
                reconciler.card_store.get_card.return_value = SimpleNamespace(
                    id="card-1", lane="done"
                )

                normalized = await reconciler.retry("dispatch-1")

                self.assertEqual(normalized.reconciliation_state, "already_satisfied")
                self.assertEqual(normalized.state, "completed")
                self.assertEqual(
                    normalized.completion_payload,
                    {"outcome": "already acknowledged"},
                )
                self.assertEqual(runtime.enqueued, 0)

        asyncio.run(run())

    def test_retry_exhaustion_is_terminal_and_does_not_prompt(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, _supervisor, outbox, reconciler = self.make_fixture(
                    Path(tmp),
                    supervisor_state="authority_unreachable",
                    max_attempts=2,
                )
                await reconciler.handle_completion("session-1", {})
                blocked = ledger.get("dispatch-1")
                blocked.reconciliation_next_retry_at = None
                ledger.put(blocked)
                await reconciler._advance(blocked)

                exhausted = ledger.get("dispatch-1")
                self.assertEqual(exhausted.reconciliation_state, "exhausted")
                self.assertFalse(exhausted.reconciliation_recoverable)
                self.assertEqual(runtime.enqueued, 0)
                self.assertEqual(len(outbox.payloads), 1)

        asyncio.run(run())

    def test_outage_records_structured_exponential_retry(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, _runtime, _supervisor, _outbox, reconciler = self.make_fixture(
                    Path(tmp), supervisor_state="authority_unreachable"
                )
                reconciler.rng = MagicMock()
                reconciler.rng.uniform.return_value = 1.0
                reconciler.retry_seconds = 2.0
                await reconciler.handle_completion("session-1", {})
                first = ledger.get("dispatch-1")
                first_delay = (
                    first.reconciliation_next_retry_at - first.reconciliation_updated_at
                ).total_seconds()
                self.assertEqual(
                    first.reconciliation_condition, "authority_unavailable"
                )
                self.assertIn("unreachable", first.reconciliation_last_dependency_error)
                self.assertIn("Retry exact-head", first.reconciliation_recovery_action)
                first.reconciliation_next_retry_at = None
                ledger.put(first)
                await reconciler._advance(first)
                second = ledger.get("dispatch-1")
                second_delay = (
                    second.reconciliation_next_retry_at
                    - second.reconciliation_updated_at
                ).total_seconds()
                self.assertGreaterEqual(first_delay, 1.9)
                self.assertGreaterEqual(second_delay, 3.9)

        asyncio.run(run())

    def test_operator_retry_is_idempotent_and_recovers_after_outage(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                ledger, runtime, supervisor, _outbox, reconciler = self.make_fixture(
                    Path(tmp), supervisor_state="authority_unreachable", max_attempts=1
                )
                await reconciler.handle_completion("session-1", {})
                self.assertEqual(
                    ledger.get("dispatch-1").reconciliation_state, "exhausted"
                )
                supervisor.state = "ready"
                first = await reconciler.retry("dispatch-1")
                second = await reconciler.retry("dispatch-1")
                self.assertEqual(first.reconciliation_state, "prompted")
                self.assertEqual(
                    second.reconciliation_prompt_id, first.reconciliation_prompt_id
                )
                self.assertEqual(runtime.enqueued, 1)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
