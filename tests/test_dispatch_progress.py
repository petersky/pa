from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from pydantic import ValidationError

from pa.config import Settings
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.progress import (
    MAX_FINAL_REPORT_BYTES,
    MAX_PROGRESS_EVENTS,
    MAX_PROGRESS_PAYLOAD_BYTES,
    MAX_VALIDATION_COMMAND,
    CompletionReportV1,
    DispatchProgressEventV1,
    DispatchProgressHeartbeatV1,
    ExplicitProgressCheckpointV1,
    OperatorInputRequestV1,
    ProgressPhase,
    ProgressService,
    ProgressValidationV1,
    sanitize_completion_report,
    sanitize_operator_input,
    sanitize_text,
)
from pa.modules.fleet import ingest_dispatch_progress

AUTHORITY = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"
TARGET = "02dbcd47-8f40-44eb-8403-5eb57545afc8"
DISPATCH = "33333333-3333-4333-8333-333333333333"
SESSION = "44444444-4444-4444-8444-444444444444"
CARD = "55555555-5555-4555-8555-555555555555"
VERSION = "2026-07-26T12:00:00+00:00"


def record() -> DispatchRecord:
    return DispatchRecord(
        dispatch_id=DISPATCH,
        mutation_id="66666666-6666-4666-8666-666666666666",
        card_id=CARD,
        card_version=VERSION,
        authority_instance_id=AUTHORITY,
        authority_url="https://authority.example",
        target_instance_id=TARGET,
        session_id=SESSION,
        state="running",
        progress_protocol_version=1,
    )


def checkpoint(
    sequence: int,
    *,
    key: str | None = None,
    summary: str = "Implementing progress",
    phase: ProgressPhase = ProgressPhase.IMPLEMENTING,
    dispatch_id: str = DISPATCH,
    session_id: str = SESSION,
) -> DispatchProgressEventV1:
    return DispatchProgressEventV1(
        card_id=CARD,
        dispatch_id=dispatch_id,
        acp_session_id=session_id,
        originating_instance_id=TARGET,
        authority_instance_id=AUTHORITY,
        authority_version=VERSION,
        sequence=sequence,
        idempotency_key=key or f"checkpoint-{sequence}",
        phase=phase,
        summary=summary,
    )


class ProgressStoreTests(unittest.TestCase):
    def test_malformed_historical_progress_does_not_break_ledger_loading(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dispatch_mutations.json"
            good = record().model_copy(update={"dispatch_id": "good"})
            malformed = record().model_dump(mode="json")
            malformed["progress_events"] = [{"schema_version": 999}]
            path.write_text(
                json.dumps({"good": good.model_dump(mode="json"), DISPATCH: malformed})
            )

            store = DispatchStore(Path(tmp))

            self.assertIsNotNone(store.get("good"))
            recovered = store.get(DISPATCH)
            self.assertIsNotNone(recovered)
            self.assertEqual(recovered.progress_events, [])

    def test_idempotent_ordering_conflicts_and_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())

            third = checkpoint(3)
            later = store.ingest_progress(third)
            late = store.ingest_progress(
                checkpoint(
                    1,
                    summary="Investigating architecture",
                    phase=ProgressPhase.INVESTIGATING,
                )
            )
            duplicate = store.ingest_progress(third)
            conflict = store.ingest_progress(
                checkpoint(3, key="conflicting-three", summary="Different payload")
            )

            self.assertEqual(later.status, "accepted")
            self.assertEqual(late.status, "late")
            self.assertEqual(duplicate.status, "duplicate")
            self.assertEqual(duplicate.replay_of_status, later.status)
            self.assertEqual(duplicate.sequence, later.sequence)
            self.assertEqual(conflict.status, "conflict")
            self.assertFalse(conflict.accepted)

            reloaded = DispatchStore(Path(tmp)).get(DISPATCH)
            assert reloaded is not None
            self.assertEqual(
                [event.sequence for event in reloaded.progress_events], [1, 3]
            )
            self.assertEqual(reloaded.latest_progress.sequence, 3)
            self.assertEqual(reloaded.progress_conflicts, 1)
            self.assertTrue(reloaded.public_dict()["progress"]["sequence_gap"])

    def test_heartbeat_is_replaceable_and_updates_freshness_not_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            original = record()
            store.put(original)
            before = original.updated_at
            heartbeat = DispatchProgressHeartbeatV1(
                card_id=CARD,
                dispatch_id=DISPATCH,
                acp_session_id=SESSION,
                originating_instance_id=TARGET,
                authority_instance_id=AUTHORITY,
                authority_version=VERSION,
                sequence=1,
                idempotency_key="heartbeat-1",
                phase=ProgressPhase.INVESTIGATING,
                summary="Agent active",
            )
            store.ingest_heartbeat(heartbeat)
            store.ingest_heartbeat(
                heartbeat.model_copy(
                    update={
                        "sequence": 2,
                        "idempotency_key": "heartbeat-2",
                        "phase": ProgressPhase.TESTING,
                        "summary": "Tests active",
                    }
                )
            )

            persisted = store.get(DISPATCH)
            assert persisted is not None
            self.assertEqual(persisted.progress_events, [])
            self.assertEqual(persisted.progress_heartbeat.sequence, 2)
            self.assertGreaterEqual(persisted.updated_at, before)
            public = persisted.public_dict()["progress"]
            self.assertEqual(public["freshness"]["state"], "live")
            self.assertEqual(public["heartbeat"]["summary"], "Tests active")
            self.assertFalse(public["sequence_gap"])
            self.assertEqual(public["accepted_ranges"], [[1, 2]])
            self.assertEqual(public["compacted_ranges"], [[1, 1]])

    def test_true_gap_reports_exact_range_without_confusing_compaction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())
            store.ingest_progress(checkpoint(1))
            store.ingest_heartbeat(
                DispatchProgressHeartbeatV1(
                    card_id=CARD,
                    dispatch_id=DISPATCH,
                    acp_session_id=SESSION,
                    originating_instance_id=TARGET,
                    authority_instance_id=AUTHORITY,
                    authority_version=VERSION,
                    sequence=3,
                    idempotency_key="heartbeat-3",
                    phase=ProgressPhase.IMPLEMENTING,
                    summary="Agent active",
                )
            )
            diagnostic = store.get(DISPATCH).public_dict()["progress"]
            self.assertTrue(diagnostic["sequence_gap"])
            self.assertEqual(diagnostic["missing_ranges"], [[2, 2]])
            self.assertEqual(diagnostic["highest_accepted_sequence"], 3)

    def test_history_and_payloads_are_bounded_and_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())
            for index in range(1, MAX_PROGRESS_EVENTS + 25):
                store.ingest_progress(
                    checkpoint(
                        index,
                        summary=f"step {index} token=super-secret-{index}",
                    )
                )
            persisted = store.get(DISPATCH)
            assert persisted is not None
            self.assertEqual(len(persisted.progress_events), MAX_PROGRESS_EVENTS)
            self.assertNotIn(
                "super-secret",
                " ".join(event.summary for event in persisted.progress_events),
            )
            self.assertIn("[REDACTED]", persisted.progress_events[-1].summary)
            self.assertLessEqual(len(persisted.progress_seen_keys), 512)

    def test_concurrent_sessions_cannot_overwrite_each_other(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            first = record()
            second = record().model_copy(
                update={
                    "dispatch_id": "77777777-7777-4777-8777-777777777777",
                    "mutation_id": "88888888-8888-4888-8888-888888888888",
                    "session_id": "99999999-9999-4999-8999-999999999999",
                }
            )
            store.put(first)
            store.put(second)

            def write(dispatch: DispatchRecord, index: int) -> None:
                sequence = store.allocate_progress_sequence(dispatch.dispatch_id)
                store.ingest_progress(
                    checkpoint(
                        sequence,
                        key=f"{dispatch.dispatch_id}-{index}",
                        summary=f"session {dispatch.session_id} step {index}",
                        dispatch_id=dispatch.dispatch_id,
                        session_id=dispatch.session_id or "",
                    )
                )

            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = [
                    pool.submit(write, first if index % 2 else second, index)
                    for index in range(40)
                ]
                for future in futures:
                    future.result()

            first_events = store.get(first.dispatch_id).progress_events
            second_events = store.get(second.dispatch_id).progress_events
            self.assertEqual(len(first_events), 20)
            self.assertEqual(len(second_events), 20)
            self.assertTrue(
                all(event.acp_session_id == first.session_id for event in first_events)
            )
            self.assertTrue(
                all(
                    event.acp_session_id == second.session_id for event in second_events
                )
            )

    def test_legacy_dispatch_renders_lifecycle_only(self) -> None:
        legacy = record().model_copy(update={"progress_protocol_version": None})
        public = legacy.public_dict()
        self.assertEqual(public["progress"]["reporting"], "lifecycle_only")
        self.assertEqual(public["progress"]["freshness"]["state"], "disconnected")

    def test_startup_failure_does_not_populate_completion_outbox_error(self) -> None:
        failed = record().model_copy(
            update={
                "state": "failed",
                "last_error": "blocking operation 'sqlite.card_write' exceeded 30.000s",
            }
        )
        public = failed.public_dict()
        self.assertIsNone(public["completion_outbox"]["last_error"])
        self.assertEqual(public["last_error"], failed.last_error)

    def test_authority_transfer_preserves_and_continues_stream_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())
            store.ingest_progress(checkpoint(1))
            new_authority = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            store.transfer_progress_authority(
                DISPATCH,
                authority_instance_id=new_authority,
                authority_url="https://new-authority.example",
                authority_version="2026-07-26T13:00:00+00:00",
            )
            # A delayed old-authority event remains valid and keeps old provenance.
            old = store.ingest_progress(
                checkpoint(2, summary="Delayed before transfer")
            )
            current = checkpoint(
                3,
                summary="Continued after transfer",
            ).model_copy(
                update={
                    "authority_instance_id": new_authority,
                    "authority_version": "2026-07-26T13:00:00+00:00",
                }
            )
            store.ingest_progress(current)

            persisted = store.get(DISPATCH)
            assert persisted is not None
            self.assertTrue(old.accepted)
            self.assertEqual(persisted.authority_instance_id, new_authority)
            self.assertEqual(
                [event.authority_instance_id for event in persisted.progress_events],
                [AUTHORITY, AUTHORITY, new_authority],
            )
            self.assertEqual(
                persisted.progress_authority_history[0]["last_sequence"], 1
            )


class ProgressDerivationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fifteen_minute_tool_trace_stays_concise_phase_stable_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())
            service = ProgressService(store, instance_id=TARGET, token="")
            await service.explicit(
                DISPATCH,
                ExplicitProgressCheckpointV1(
                    phase=ProgressPhase.IMPLEMENTING,
                    summary="Implementing compaction-aware diagnostics",
                    idempotency_key="milestone-implementing",
                ),
            )
            # One update per simulated second for fifteen minutes. Edit/inspection
            # lifecycle chatter is replaceable and cannot regress the milestone.
            for index in range(900):
                await service.observe(
                    SESSION,
                    {
                        "type": "tool_call_update",
                        "tool_call_id": f"edit-{index % 12}",
                        "title": "Edit dispatch ledger",
                        "kind": "apply_patch",
                        "status": "completed" if index % 2 else "running",
                    },
                )

            persisted = store.get(DISPATCH)
            assert persisted is not None
            public = persisted.public_dict()["progress"]
            self.assertEqual(len(persisted.progress_events), 1)
            self.assertEqual(persisted.latest_progress.phase, ProgressPhase.IMPLEMENTING)
            self.assertEqual(persisted.progress_heartbeat.phase, ProgressPhase.IMPLEMENTING)
            self.assertLessEqual(len(persisted.progress_events), MAX_PROGRESS_EVENTS)
            self.assertFalse(public["sequence_gap"])
            self.assertGreater(public["highest_accepted_sequence"], 1)

    async def test_repeated_tool_updates_coalesce_before_durable_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())
            service = ProgressService(store, instance_id=TARGET, token="")
            update = {
                "type": "tool_call_update",
                "tool_call_id": "tool-1",
                "title": "Run focused tests",
                "kind": "execute",
                "status": "running",
            }

            for _ in range(100):
                await service.observe(SESSION, update)

            persisted = store.get(DISPATCH)
            assert persisted is not None
            self.assertEqual(len(persisted.progress_events), 0)
            self.assertEqual(
                persisted.progress_heartbeat.summary, "Run focused tests · running"
            )
            self.assertLessEqual(persisted.progress_next_sequence, 3)
            self.assertEqual(
                service.snapshot()["coalesced_observations_by_session"][SESSION],
                99,
            )

    async def test_tool_status_transition_remains_a_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())
            service = ProgressService(store, instance_id=TARGET, token="")
            base = {
                "type": "tool_call_update",
                "tool_call_id": "tool-1",
                "title": "Run focused tests",
                "kind": "execute",
            }

            await service.observe(SESSION, {**base, "status": "running"})
            await service.observe(SESSION, {**base, "status": "running"})
            await service.observe(SESSION, {**base, "status": "completed"})

            persisted = store.get(DISPATCH)
            assert persisted is not None
            self.assertEqual(
                [event.tool_details[0].status for event in persisted.progress_events],
                ["completed"],
            )

    async def test_repeated_malformed_updates_log_once_and_do_not_escape(self) -> None:
        service = ProgressService(MagicMock(), instance_id=TARGET, token="")
        with (
            patch.object(
                service,
                "_observe",
                AsyncMock(side_effect=ValueError("oversized progress")),
            ),
            patch("pa.execution.progress.logger.warning") as warning,
        ):
            await service.observe(SESSION, {"type": "tool_call"})
            await service.observe(SESSION, {"type": "tool_call_update"})

        warning.assert_called_once()

    async def test_oversized_tool_fields_are_truncated_before_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())
            service = ProgressService(store, instance_id=TARGET, token="")
            long_command = "pytest " + ("very-long-argument " * 40)

            await service.observe(
                SESSION,
                {
                    "type": "tool_call",
                    "title": long_command,
                    "kind": "execute",
                    "status": "running",
                },
            )
            await service.observe(
                SESSION,
                {
                    "type": "agent_message_chunk",
                    "message_id": "after-oversized-tool",
                    "text": "Unrelated progress is still reported after the long command.",
                    "phase": "commentary",
                    "final": True,
                },
            )

            persisted = store.get(DISPATCH)
            assert persisted is not None
            self.assertEqual(len(persisted.progress_events), 1)
            self.assertLessEqual(len(persisted.progress_heartbeat.summary), 500)
            self.assertIn(
                "Unrelated progress",
                persisted.progress_events[0].summary,
            )

    async def test_visible_commentary_and_allowlisted_tool_metadata_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())
            service = ProgressService(
                store,
                instance_id=TARGET,
                token="",
                heartbeat_seconds=0,
            )
            await service.observe(
                SESSION,
                {
                    "type": "agent_thought_chunk",
                    "text": "hidden password=do-not-record",
                },
            )
            await service.observe(
                SESSION,
                {
                    "type": "tool_call",
                    "title": "Run pytest token=do-not-record",
                    "kind": "execute",
                    "status": "running",
                    "raw_input": {"authorization": "Bearer secret"},
                    "raw_output": "unrestricted output",
                },
            )
            await service.observe(
                SESSION,
                {
                    "type": "agent_message_chunk",
                    "message_id": "visible-1",
                    "text": (
                        "I’m implementing the durable progress store now. "
                        "api_key=do-not-record"
                    ),
                    "phase": "commentary",
                    "final": True,
                },
            )
            persisted = store.get(DISPATCH)
            assert persisted is not None
            serialized = str(
                [event.model_dump(mode="json") for event in persisted.progress_events]
            )
            self.assertNotIn("hidden password", serialized)
            self.assertNotIn("unrestricted output", serialized)
            self.assertNotIn("do-not-record", serialized)
            self.assertEqual(len(persisted.progress_events), 1)
            self.assertEqual(
                persisted.progress_events[0].phase, ProgressPhase.IMPLEMENTING
            )

    async def test_explicit_checkpoint_builds_structured_final_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            store.put(record())
            service = ProgressService(store, instance_id=TARGET, token="")
            await service.explicit(
                DISPATCH,
                ExplicitProgressCheckpointV1(
                    phase=ProgressPhase.TESTING,
                    summary="Validation complete",
                    branch="agent/progress",
                    commit_sha="a" * 40,
                    pr_url="https://github.com/petersky/pa/pull/999",
                    pr_number=999,
                    changed_file_count=12,
                    validations=[
                        ProgressValidationV1(
                            command="pytest tests/test_dispatch_progress.py",
                            status="passed",
                            summary="12 passed",
                        )
                    ],
                    idempotency_key="explicit-testing",
                ),
            )
            await service.observe(
                SESSION,
                {
                    "type": "turn_completed",
                    "result": {
                        "card_disposition": {
                            "contract": "pa.card-disposition/v1",
                            "lane": "waiting",
                            "outcome": "Ready for CI",
                            "evidence": {
                                "integration_required": True,
                                "pr_watch_id": "watch-1",
                                "watched_head_sha": "a" * 40,
                                "merge_commit_sha": None,
                                "references": [],
                            },
                        }
                    },
                },
            )
            persisted = store.get(DISPATCH)
            assert persisted is not None
            report = persisted.final_report
            assert report is not None
            self.assertEqual(report.outcome, "Ready for CI")
            self.assertEqual(report.branch, "agent/progress")
            self.assertEqual(report.pr_number, 999)
            self.assertEqual(report.validations[0].status, "passed")
            self.assertEqual(report.resulting_lane, "waiting")

    async def test_transient_delivery_retries_without_duplicate_authority_entry(
        self,
    ) -> None:
        with (
            tempfile.TemporaryDirectory() as target_tmp,
            tempfile.TemporaryDirectory() as authority_tmp,
        ):
            target = DispatchStore(Path(target_tmp))
            authority = DispatchStore(Path(authority_tmp))
            target.put(record())
            authority.put(record())
            calls = 0

            def handler(request: httpx.Request) -> httpx.Response:
                nonlocal calls
                calls += 1
                if calls == 1:
                    return httpx.Response(503, text="temporary")
                payload = DispatchProgressEventV1.model_validate(
                    __import__("json").loads(request.content)
                )
                result = authority.ingest_progress(payload, delivered=True)
                return httpx.Response(
                    200,
                    json=result.model_dump(mode="json"),
                )

            service = ProgressService(target, instance_id=TARGET, token="sync")
            service._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            await service.explicit(
                DISPATCH,
                ExplicitProgressCheckpointV1(
                    phase=ProgressPhase.IMPLEMENTING,
                    summary="Writing retry-safe progress",
                    idempotency_key="retry-safe",
                ),
            )
            pending = target.pending_progress(TARGET)
            self.assertEqual(len(pending), 1)
            target_record, payload = pending[0]
            await service._send(target_record, payload)
            self.assertEqual(
                target.get(DISPATCH).progress_events[0].delivery_attempts, 1
            )
            self.assertEqual(
                target.get(DISPATCH).public_dict()["progress"]["freshness"]["state"],
                "disconnected",
            )
            await service._send(target_record, payload)
            self.assertIsNotNone(target.get(DISPATCH).progress_events[0].delivered_at)
            self.assertEqual(
                target.get(DISPATCH).public_dict()["progress"]["freshness"]["state"],
                "live",
            )
            self.assertEqual(len(authority.get(DISPATCH).progress_events), 1)
            await service._send(target_record, payload)
            self.assertEqual(len(authority.get(DISPATCH).progress_events), 1)
            await service.close()


class ProgressApiAndUiTests(unittest.TestCase):
    def test_validation_command_character_boundaries_and_unicode(self) -> None:
        for size in (MAX_VALIDATION_COMMAND - 1, MAX_VALIDATION_COMMAND):
            value = ProgressValidationV1(command="é" * size, status="passed")
            self.assertEqual(len(value.command), size)
            self.assertEqual(len(value.command.encode()), size * 2)
        value = ProgressValidationV1(
            command="é" * (MAX_VALIDATION_COMMAND + 1), status="passed"
        )
        self.assertEqual(len(value.command), MAX_VALIDATION_COMMAND)

    def test_checkpoint_encoded_byte_boundaries_with_multibyte_unicode(self) -> None:
        base = checkpoint(1).model_copy(update={"operator_input": ""})
        base_size = len(base.model_dump_json().encode())

        below = checkpoint(1).model_copy(
            update={
                "operator_input": "x" * (MAX_PROGRESS_PAYLOAD_BYTES - base_size - 1)
            }
        )
        exact = checkpoint(1).model_copy(
            update={"operator_input": "x" * (MAX_PROGRESS_PAYLOAD_BYTES - base_size)}
        )
        self.assertEqual(
            len(below.model_dump_json().encode()), MAX_PROGRESS_PAYLOAD_BYTES - 1
        )
        self.assertEqual(
            len(exact.model_dump_json().encode()), MAX_PROGRESS_PAYLOAD_BYTES
        )
        DispatchProgressEventV1.model_validate(exact.model_dump(mode="json"))
        with self.assertRaises(ValidationError):
            DispatchProgressEventV1.model_validate(
                {
                    **exact.model_dump(mode="json"),
                    "operator_input": exact.operator_input + "x",
                }
            )

        unicode_exact = checkpoint(1).model_copy(
            update={
                "operator_input": "x" * (MAX_PROGRESS_PAYLOAD_BYTES - base_size - 4)
                + "😀"
            }
        )
        self.assertEqual(
            len(unicode_exact.model_dump_json().encode()), MAX_PROGRESS_PAYLOAD_BYTES
        )
        DispatchProgressEventV1.model_validate(unicode_exact.model_dump(mode="json"))
        with self.assertRaises(ValidationError):
            DispatchProgressEventV1.model_validate(
                {
                    **unicode_exact.model_dump(mode="json"),
                    "operator_input": unicode_exact.operator_input + "é",
                }
            )

    def test_completion_report_limit_remains_intentionally_at_least_64kb(self) -> None:
        self.assertGreaterEqual(MAX_FINAL_REPORT_BYTES, 64_000)

    def test_operator_input_preserves_legacy_strings_and_structured_contracts(
        self,
    ) -> None:
        self.assertEqual(
            sanitize_operator_input("Run gh auth login token=secret"),
            "Run gh auth login token=[REDACTED]",
        )
        structured = sanitize_operator_input(
            OperatorInputRequestV1(
                request_id="auth-choice",
                prompt="Choose the target",
                choices=[{"id": "local", "label": "Local", "value": "local"}],
                allow_freeform=False,
            )
        )
        self.assertIsInstance(structured, OperatorInputRequestV1)
        self.assertEqual(structured.request_id, "auth-choice")
        event = checkpoint(1).model_copy(update={"operator_input": structured})
        self.assertEqual(
            event.transport_dict()["operator_input"]["choices"][0]["id"], "local"
        )

    def test_authority_ingestion_requires_exact_origin_and_idempotency(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id=AUTHORITY)
            store = DispatchStore(Path(tmp))
            store.put(record())
            ctx = MagicMock(settings=settings)
            ctx.services = {"dispatch_store": store}
            request = MagicMock()
            request.app.state.ctx = ctx
            request.state.instance_authenticated = True
            request.headers = {
                "X-PA-Origin-Instance-ID": TARGET,
                "idempotency-key": "checkpoint-1",
            }
            response = ingest_dispatch_progress(request, DISPATCH, checkpoint(1))
            self.assertEqual(response.status_code, 200)
            self.assertEqual(len(store.get(DISPATCH).progress_events), 1)

    def test_templates_expose_card_fleet_freshness_and_sanitized_details(self) -> None:
        root = Path(__file__).parents[1]
        card = (root / "src/pa/server/templates/partials/card-detail.html").read_text()
        activity = (
            root / "src/pa/server/templates/partials/card-detail-activity.html"
        ).read_text()
        progress = (
            root / "src/pa/server/templates/partials/card-progress.html"
        ).read_text()
        work = (root / "src/pa/server/templates/pages/work.html").read_text()
        fleet = (root / "src/pa/server/templates/pages/fleet.html").read_text()
        script = (root / "src/pa/server/static/js/fleet.js").read_text()
        self.assertIn('include "partials/card-progress.html"', card)
        self.assertIn("data-card-live-progress", progress)
        self.assertIn("Open exact session", progress)
        self.assertIn("every 15s", progress)
        self.assertIn("every 15s", work)
        self.assertIn("Progress &amp; validation", activity)
        self.assertIn("Sanitized details", activity)
        self.assertIn("current_dispatch.freshness", fleet)
        self.assertIn("Lifecycle-only reporting from an older peer", script)

    def test_secret_patterns_are_redacted(self) -> None:
        value = sanitize_text(
            "Authorization: Bearer abcdef password=hunter2 "
            "https://user:pass@example.com ghp_abcdefghijklmnop"
        )
        self.assertNotIn("hunter2", value)
        self.assertNotIn("abcdef", value)
        self.assertNotIn("user:pass", value)
        self.assertNotIn("ghp_", value)

    def test_payload_bounds_and_completion_evidence_redaction(self) -> None:
        with self.assertRaises(ValidationError):
            ExplicitProgressCheckpointV1(
                phase=ProgressPhase.IMPLEMENTING,
                summary="Bounded input",
                blockers=["x" * 2000] * (MAX_PROGRESS_PAYLOAD_BYTES // 2000 + 1),
            )
        report = sanitize_completion_report(
            CompletionReportV1(
                outcome="Merged token=completion-secret",
                ci_evidence=["pytest password=hunter2"],
                card_disposition={
                    "contract": "pa.card-disposition/v1",
                    "outcome": "Bearer sensitive-token",
                },
            )
        )
        serialized = report.model_dump_json()
        self.assertNotIn("completion-secret", serialized)
        self.assertNotIn("hunter2", serialized)
        self.assertNotIn("sensitive-token", serialized)
