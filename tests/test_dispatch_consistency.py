from __future__ import annotations

import asyncio
import tempfile
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi import HTTPException

from pa.config import Settings
from pa.domain.models import (
    AgentSession,
    Card,
    CardEvent,
    CardLane,
    EventType,
    FleetInstance,
)
from pa.execution.dispatch import (
    CompletionOutbox,
    DispatchCompareConflict,
    DispatchEvent,
    DispatchRecord,
    DispatchStore,
    DispatchWorker,
    GoalDispatchProvenance,
)
from pa.execution.progress import CompletionReportV1
from pa.execution.reconciliation import CompletionReconciler
from pa.goals.materialization import (
    GoalMaterializationEnvelopeV1,
    GoalMaterializationReceiptV1,
    GoalMaterializationResourceClaimV1,
    canonical_materialization_digest,
)
from pa.instance.agent_session import AgentSessionManager, AgentSessionRecoveryError
from pa.modules.fleet import (
    DispatchCompletionBody,
    DispatchControlBody,
    DispatchFollowupBody,
    DispatchMaterializeBody,
    DispatchTerminalRepairBody,
    DispatchTerminalRepairCommitRequest,
    DispatchTerminalRepairCommitV1,
    DispatchTerminalRepairEvidenceRequest,
    DispatchTerminalRepairEvidenceV1,
    RemoteAgentStartBody,
    _assert_dispatch_sync_health,
    _expected_goal_dispatch_execution_identity,
    _goal_materialization_stage_provenance,
    _merge_dispatch_followup_operation,
    _peer_terminal_repair_evidence,
    _process_remote_dispatch,
    _release_terminal_repair_fence_if_uncommitted,
    _terminal_repair_commit_digest,
    _terminal_repair_evidence_digest,
    _terminal_repair_reservation_id,
    _wait_for_dispatch_sync_health,
    cancel_dispatch,
    complete_dispatch,
    materialize_dispatch,
    prompt_dispatch_session,
    repair_terminal_dispatch,
    retry_dispatch,
    start_remote_agent_work,
    target_terminal_repair_commit,
    target_terminal_repair_evidence,
)
from pa.pr_supervisor.models import PRWatch
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore

AUTHORITY_ID = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"
TARGET_ID = "2d22a9e1-a1a0-4900-8a8e-8284627aa6bf"
DISPATCH_ONE = "33333333-3333-4333-8333-333333333333"
MUTATION_ONE = "44444444-4444-4444-8444-444444444444"
CARD_ONE = "45cd58e9-1dd7-44b9-9e07-2ae58d12e685"


def request_for(settings: Settings, store: MagicMock, services: dict | None = None):
    ctx = MagicMock(settings=settings, store=store)
    ctx.services = dict(services or {})
    # These direct route tests model a running server, whose lifespan owns the
    # data-dir writer lock before dispatch mutations are accepted.
    ctx.services.setdefault("writer_lock", MagicMock())
    ctx.require_service.side_effect = lambda name: ctx.services[name]
    ctx.register_service.side_effect = lambda name, value: ctx.services.__setitem__(
        name, value
    )
    request = MagicMock()
    request.app.state.ctx = ctx
    request.headers = {}
    return request


def terminal_repair_evidence(
    record: DispatchRecord,
    idempotency_key: str,
    *,
    session_status: str = "closed",
    runtime_live: bool = False,
    observed_at: datetime | None = None,
) -> DispatchTerminalRepairEvidenceV1:
    observed = observed_at or datetime.now(UTC)
    evidence = DispatchTerminalRepairEvidenceV1(
        dispatch_id=record.dispatch_id,
        mutation_id=record.mutation_id,
        authority_instance_id=record.authority_instance_id,
        target_instance_id=record.target_instance_id,
        session_id=record.session_id or "",
        idempotency_key=idempotency_key,
        reservation_id=_terminal_repair_reservation_id(record, idempotency_key),
        dispatch_state=record.state,
        dispatch_recoverable=record.recoverable,
        dispatch_updated_at=record.updated_at,
        session_status=session_status,
        completion_acknowledged=False,
        completion_evidence_present=False,
        session_updated_at=observed,
        runtime_live=runtime_live,
        observed_at=observed,
        evidence_digest="0" * 64,
    )
    evidence.evidence_digest = _terminal_repair_evidence_digest(evidence)
    return evidence


def terminal_repair_commit(
    record: DispatchRecord,
    evidence: DispatchTerminalRepairEvidenceV1,
) -> DispatchTerminalRepairCommitV1:
    receipt = DispatchTerminalRepairCommitV1(
        dispatch_id=record.dispatch_id,
        mutation_id=record.mutation_id,
        authority_instance_id=record.authority_instance_id,
        target_instance_id=record.target_instance_id,
        session_id=record.session_id or "",
        idempotency_key=evidence.idempotency_key,
        reservation_id=evidence.reservation_id,
        evidence_digest=evidence.evidence_digest,
        target_state="cancelled",
        session_status="closed",
        committed_at=datetime.now(UTC),
        receipt_digest="0" * 64,
    )
    receipt.receipt_digest = _terminal_repair_commit_digest(receipt)
    return receipt


def remote_terminal_repair_context(
    data_dir: str,
    suffix: str,
) -> tuple[
    DispatchStore,
    DispatchRecord,
    MagicMock,
    DispatchTerminalRepairBody,
]:
    settings = Settings(data_dir=Path(data_dir), instance_id="authority")
    ledger = DispatchStore(Path(data_dir))
    record = DispatchRecord(
        dispatch_id=f"dispatch-remote-{suffix}",
        mutation_id=f"mutation-remote-{suffix}",
        card_id="card-done",
        authority_instance_id="authority",
        authority_url="http://authority",
        target_instance_id="target",
        session_id=f"session-remote-{suffix}",
        state="running",
        recoverable=False,
    )
    ledger.put(record)
    domain = MagicMock()
    domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
    domain.get_session.return_value = SimpleNamespace(status="closed")
    manager = MagicMock()
    manager.get.return_value = None
    request = request_for(
        settings,
        domain,
        {"dispatch_store": ledger, "instance_agent": manager},
    )
    body = DispatchTerminalRepairBody(
        idempotency_key=f"repair-remote-{suffix}",
        mode="abandoned_without_acknowledgement",
        expected_state="running",
        reason="Verified target-local terminal evidence.",
        confirm_no_outcome_inference=True,
    )
    return ledger, record, request, body


def target_terminal_repair_context(data_dir: str, suffix: str) -> SimpleNamespace:
    settings = Settings(data_dir=Path(data_dir), instance_id="target")
    ledger = DispatchStore(Path(data_dir))
    record = DispatchRecord(
        dispatch_id=f"dispatch-target-{suffix}",
        mutation_id=f"mutation-target-{suffix}",
        authority_instance_id="authority",
        authority_url="http://authority",
        target_instance_id="target",
        session_id=f"session-target-{suffix}",
        state="running",
        recoverable=False,
    )
    ledger.put(record)
    session = AgentSession(
        id=record.session_id or "",
        agent_name="codex",
        origin_instance_id="target",
        authority_instance_id="authority",
        dispatch_id=record.dispatch_id,
        status="closed",
    )
    domain = MagicMock()
    domain.get_session.return_value = session
    manager = AgentSessionManager(settings, domain, dispatch_store=ledger)
    request = request_for(
        settings, domain, {"dispatch_store": ledger, "instance_agent": manager}
    )
    request.state.instance_authenticated = True
    key = f"repair-target-{suffix}"
    request.headers = {
        "X-PA-Origin-Instance-ID": "authority",
        "Idempotency-Key": key,
    }
    proof_body = DispatchTerminalRepairEvidenceRequest(
        mutation_id=record.mutation_id,
        authority_instance_id="authority",
        target_instance_id="target",
        session_id=session.id,
        idempotency_key=key,
        expected_state="running",
    )
    return SimpleNamespace(
        settings=settings,
        ledger=ledger,
        record=record,
        session=session,
        domain=domain,
        manager=manager,
        request=request,
        proof_body=proof_body,
    )


class PeerLocalAuthorityTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_routes_to_explicit_peer_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="macbook",
                instance_url="http://macbook:8080",
            )
            request = request_for(settings, MagicMock(), {})
            request.state.instance_authenticated = False
            forwarded = {"accepted": True, "dispatch_id": "dispatch-1"}
            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_authority_json",
                    AsyncMock(return_value=forwarded),
                ) as proxy,
            ):
                result = await start_remote_agent_work(
                    request,
                    "target",
                    RemoteAgentStartBody(
                        authority_instance_id="monica",
                        card_id="card-1",
                        message="work",
                        idempotency_key="start-1",
                    ),
                )
            self.assertEqual(result, forwarded)
            self.assertEqual(proxy.await_args.args[1], "monica")
            self.assertEqual(
                proxy.await_args.kwargs["body"]["authority_instance_id"], "monica"
            )

    async def test_linked_followup_is_idempotent_at_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="monica")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                authority_instance_id="monica",
                authority_url="http://monica:8080",
                target_instance_id="target",
                session_id="session-1",
                state="running",
            )
            ledger.put(record)
            request = request_for(settings, MagicMock(), {"dispatch_store": ledger})
            request.state.instance_authenticated = True
            acknowledged = {
                "accepted": True,
                "session_id": "session-1",
                "prompt_id": "prompt-1",
                "duplicate": False,
            }
            with patch(
                "pa.modules.fleet._peer_agent_json",
                AsyncMock(return_value=acknowledged),
            ) as peer:
                first = await prompt_dispatch_session(
                    request,
                    "dispatch-1",
                    DispatchFollowupBody(
                        message="continue", idempotency_key="followup-1"
                    ),
                )
                repeated = await prompt_dispatch_session(
                    request,
                    "dispatch-1",
                    DispatchFollowupBody(
                        message="continue", idempotency_key="followup-1"
                    ),
                )
            self.assertTrue(first["accepted"])
            self.assertTrue(repeated["duplicate"])
            self.assertEqual(peer.await_count, 1)
            persisted = DispatchStore(Path(tmp)).get("dispatch-1")
            self.assertNotIn("continue", str(persisted.followup_operations))

    def test_concurrent_same_key_receipts_append_one_authority_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-concurrent-followup",
                    mutation_id="mutation-concurrent-followup",
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-concurrent-followup",
                    state="running",
                )
            )
            first = ledger.get("dispatch-concurrent-followup")
            second = ledger.get("dispatch-concurrent-followup")
            assert first is not None and second is not None
            receipt = {
                "accepted": True,
                "accepted_event": "queue_enqueued",
                "dispatch_id": first.dispatch_id,
                "prompt_id": "prompt-concurrent-followup",
                "queued": True,
                "session_id": first.session_id,
                "started": False,
                "stop_reason": "queued",
                "duplicate": False,
            }
            for snapshot, duplicate in ((first, False), (second, True)):
                snapshot.followup_operations["same-key"] = {
                    "fingerprint": "same-fingerprint",
                    "response": {**receipt, "duplicate": duplicate},
                    "state": "accepted",
                }

            _merge_dispatch_followup_operation(
                ledger,
                first,
                "same-key",
                event_message="Linked session follow-up durably acknowledged.",
            )
            _merge_dispatch_followup_operation(
                ledger,
                second,
                "same-key",
                event_message="Linked session follow-up durably acknowledged.",
            )

            persisted = ledger.get(first.dispatch_id)
            assert persisted is not None
            self.assertEqual(
                [
                    event.message
                    for event in persisted.events
                    if event.message == "Linked session follow-up durably acknowledged."
                ],
                ["Linked session follow-up durably acknowledged."],
            )
            self.assertFalse(
                persisted.followup_operations["same-key"]["response"]["duplicate"]
            )

    async def test_governed_ambiguous_followup_keeps_receipt_recovery_open(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-ambiguous-followup",
                mutation_id="mutation-ambiguous-followup",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-ambiguous-followup",
                state="running",
            )
            ledger.put(record)
            request = request_for(settings, MagicMock(), {"dispatch_store": ledger})
            request.state.instance_authenticated = True
            provenance = GoalDispatchProvenance(
                goal_id="goal-ambiguous-followup",
                goal_version=1,
                policy_revision=1,
                authority_instance_id="authority",
                fencing_token=1,
                action_reservation_id="reservation-ambiguous-followup",
                actor_principal="service:goal-supervisor:authority",
            )

            def reserve(_ctx, _ledger, current, *, idempotency_key, fingerprint):
                current.followup_operations[idempotency_key] = {
                    "fingerprint": fingerprint,
                    "state": "reservation_applied",
                    "goal_provenance": provenance.model_dump(mode="json"),
                }
                return provenance

            with (
                patch(
                    "pa.modules.fleet._reserve_goal_dispatch_followup",
                    side_effect=reserve,
                ),
                patch(
                    "pa.modules.fleet._validate_goal_dispatch_provenance",
                    return_value=provenance,
                ),
                patch(
                    "pa.modules.fleet._peer_agent_json",
                    AsyncMock(side_effect=TimeoutError("response lost")),
                ),
                patch("pa.modules.fleet._release_goal_dispatch_followup") as release,
                self.assertRaises(TimeoutError),
            ):
                await prompt_dispatch_session(
                    request,
                    record.dispatch_id,
                    DispatchFollowupBody(
                        message="continue",
                        idempotency_key="ambiguous-key",
                    ),
                )

            release.assert_not_called()
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            operation = persisted.followup_operations["ambiguous-key"]
            self.assertEqual(operation["state"], "delivery_ambiguous")
            self.assertTrue(operation["error"]["recoverable"])
            self.assertIsNone(operation["goal_provenance"].get("released_at"))

    async def test_followup_on_completed_dispatch_retains_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="monica")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                authority_instance_id="monica",
                authority_url="http://monica:8080",
                target_instance_id="target",
                session_id="session-1",
                state="completed",
                acknowledged_at=datetime.now(UTC),
                completion_delivery_class="acknowledged",
            )
            ledger.put(record)
            request = request_for(settings, MagicMock(), {"dispatch_store": ledger})
            request.state.instance_authenticated = True
            acknowledged = {
                "accepted": True,
                "session_id": "session-1",
                "prompt_id": "prompt-2",
                "duplicate": False,
            }
            with patch(
                "pa.modules.fleet._peer_agent_json",
                AsyncMock(return_value=acknowledged),
            ):
                result = await prompt_dispatch_session(
                    request,
                    "dispatch-1",
                    DispatchFollowupBody(
                        message="continue safely",
                        idempotency_key="followup-terminal",
                    ),
                )

            self.assertTrue(result["accepted"])
            persisted = DispatchStore(Path(tmp)).get("dispatch-1")
            self.assertEqual(persisted.state, "completed")
            self.assertTrue(persisted.public_dict()["dispatch_completion"]["completed"])

    async def test_acknowledged_legacy_running_record_repairs_idempotently(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-legacy",
                    mutation_id="mutation-legacy",
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="running",
                    acknowledged_at=datetime.now(UTC),
                    completion_delivery_class="acknowledged",
                )
            )
            request = request_for(settings, MagicMock(), {"dispatch_store": ledger})
            request.headers = {"idempotency-key": "repair-1"}
            body = DispatchControlBody(idempotency_key="repair-1")
            with patch("pa.modules.fleet.require_user"):
                first = await repair_terminal_dispatch(request, "dispatch-legacy", body)
                second = await repair_terminal_dispatch(
                    request, "dispatch-legacy", body
                )

            self.assertEqual(first["state"], "completed")
            self.assertEqual(second["state"], "completed")
            repaired = ledger.get("dispatch-legacy")
            self.assertEqual(
                repaired.lifecycle_inconsistencies[-1]["kind"],
                "legacy_terminal_record_repaired",
            )

    async def test_acknowledged_repair_rejects_stale_put(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            ledger = DispatchStore(Path(tmp))
            acknowledged_at = datetime.now(UTC)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-acknowledged-stale-put",
                    mutation_id="mutation-acknowledged-stale-put",
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-acknowledged-stale-put",
                    state="running",
                    acknowledged_at=acknowledged_at,
                    completion_payload={"outcome": "durably received"},
                    completion_envelope={
                        "completion_id": "completion-acknowledged-stale-put",
                        "payload_digest": "a" * 64,
                    },
                    completion_received_at=acknowledged_at,
                    completion_delivery_class="acknowledged",
                    capacity_reserved_at=acknowledged_at,
                )
            )
            stale = ledger.get("dispatch-acknowledged-stale-put")
            assert stale is not None
            request = request_for(settings, MagicMock(), {"dispatch_store": ledger})
            with patch("pa.modules.fleet.require_user"):
                repaired = await repair_terminal_dispatch(
                    request,
                    stale.dispatch_id,
                    DispatchControlBody(idempotency_key="repair-acknowledged-stale"),
                )

            self.assertEqual(repaired["state"], "completed")
            with self.assertRaises(DispatchCompareConflict):
                ledger.put(stale)
            persisted = ledger.get(stale.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "completed")
            self.assertEqual(
                persisted.control_operations["repair-acknowledged-stale"],
                "repair_terminal",
            )
            self.assertEqual(
                persisted.lifecycle_inconsistencies[-1]["kind"],
                "legacy_terminal_record_repaired",
            )
            self.assertEqual(persisted.completion_payload, stale.completion_payload)

    async def test_acknowledged_repair_releases_queued_capacity_atomically(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            reserved_at = datetime.now(UTC)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-queued-acknowledged",
                    mutation_id="mutation-queued-acknowledged",
                    authority_instance_id="target",
                    authority_url="http://target",
                    target_instance_id="target",
                    state="queued",
                    acknowledged_at=reserved_at,
                    completion_delivery_class="acknowledged",
                    capacity_reserved_at=reserved_at,
                )
            )
            request = request_for(
                settings, MagicMock(), {"dispatch_store": ledger}
            )
            body = DispatchControlBody(idempotency_key="repair-queued-acknowledged")

            with patch("pa.modules.fleet.require_user"):
                result = await repair_terminal_dispatch(
                    request, "dispatch-queued-acknowledged", body
                )

            self.assertEqual(result["state"], "completed")
            repaired = ledger.get("dispatch-queued-acknowledged")
            assert repaired is not None
            self.assertEqual(repaired.state, "completed")
            self.assertIsNotNone(repaired.capacity_released_at)
            self.assertEqual(repaired.capacity_release_reason, "completed")
            self.assertEqual(
                ledger.capacity_snapshot("target")["dispatch_reservations"], 0
            )

    async def test_abandoned_running_repair_is_terminal_audited_and_idempotent(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-abandoned",
                    mutation_id="mutation-abandoned",
                    card_id="card-done",
                    authority_instance_id="target",
                    authority_url="http://target",
                    target_instance_id="target",
                    session_id="session-gone",
                    state="running",
                    recoverable=False,
                )
            )
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            domain.get_session.return_value = SimpleNamespace(status="closed")
            request = request_for(
                settings, domain, {"dispatch_store": ledger}
            )
            request.headers = {"idempotency-key": "repair-abandoned-1"}
            body = DispatchTerminalRepairBody(
                idempotency_key="repair-abandoned-1",
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Canonical card is Done and the linked session is gone.",
                confirm_no_outcome_inference=True,
            )

            with patch("pa.modules.fleet.require_user"):
                first = await repair_terminal_dispatch(
                    request, "dispatch-abandoned", body
                )
                second = await repair_terminal_dispatch(
                    request, "dispatch-abandoned", body
                )

            self.assertEqual(first["state"], "cancelled")
            self.assertEqual(second["state"], "cancelled")
            self.assertFalse(first["dispatch_completion"]["completed"])
            self.assertIsNone(first["dispatch_completion"]["acknowledged_at"])
            self.assertFalse(first["agent_turn"]["ended"])
            repaired = ledger.get("dispatch-abandoned")
            self.assertFalse(repaired.recoverable)
            self.assertIsNone(repaired.acknowledged_at)
            self.assertIsNone(repaired.completion_payload)
            self.assertIsNone(repaired.completion_delivery_class)
            diagnostics = [
                item
                for item in repaired.lifecycle_inconsistencies
                if item["kind"] == "legacy_abandoned_dispatch_retired"
            ]
            self.assertEqual(len(diagnostics), 1)
            self.assertFalse(diagnostics[0]["outcome_inferred"])
            self.assertEqual(
                diagnostics[0]["evidence"]["session_status"], "closed"
            )
            self.assertEqual(
                ledger.current_card_ids(realm_id="default", limit=10), []
            )
            self.assertEqual(
                ledger.capacity_snapshot("target")["dispatch_reservations"], 0
            )

    async def test_abandoned_running_repair_requires_terminal_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-live",
                    mutation_id="mutation-live",
                    card_id="card-done",
                    authority_instance_id="target",
                    authority_url="http://target",
                    target_instance_id="target",
                    session_id="session-live",
                    state="running",
                    recoverable=False,
                )
            )
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            domain.get_session.return_value = SimpleNamespace(status="idle")
            request = request_for(
                settings, domain, {"dispatch_store": ledger}
            )
            body = DispatchTerminalRepairBody(
                idempotency_key="repair-live-1",
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Attempted stale-row repair.",
                confirm_no_outcome_inference=True,
            )

            with (
                patch("pa.modules.fleet.require_user"),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(request, "dispatch-live", body)

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"], "linked_session_not_terminal"
            )
            self.assertEqual(ledger.get("dispatch-live").state, "running")

    async def test_abandoned_repair_fails_closed_on_concurrent_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-race",
                    mutation_id="mutation-race",
                    card_id="card-done",
                    authority_instance_id="target",
                    authority_url="http://target",
                    target_instance_id="target",
                    session_id="session-race",
                    state="running",
                    recoverable=False,
                    events=[
                        DispatchEvent(
                            seq=1,
                            state="running",
                            message="Legacy dispatch running.",
                        )
                    ],
                )
            )
            session = AgentSession(
                id="session-race",
                agent_name="codex",
                origin_instance_id="target",
                authority_instance_id="target",
                dispatch_id="dispatch-race",
                status="closed",
            )
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            session_reads = 0
            acknowledged_at = datetime.now(UTC)
            completion_payload = {
                "stop_reason": "end_turn",
                "outcome": "Concurrent completion won.",
            }
            completion_envelope = {
                "completion_id": "completion-race",
                "payload_digest": "a" * 64,
            }

            def get_session(_session_id):
                nonlocal session_reads
                session_reads += 1
                if session_reads == 2:
                    concurrent = ledger.get("dispatch-race")
                    assert concurrent is not None
                    concurrent.acknowledged_at = acknowledged_at
                    concurrent.completion_payload = completion_payload
                    concurrent.completion_envelope = completion_envelope
                    concurrent.completion_received_at = acknowledged_at
                    concurrent.completion_delivery_class = "acknowledged"
                    ledger.transition(
                        concurrent,
                        "completed",
                        "Concurrent completion durably acknowledged.",
                    )
                return session

            domain.get_session.side_effect = get_session
            manager = AgentSessionManager(settings, domain, dispatch_store=ledger)
            request = request_for(
                settings,
                domain,
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            body = DispatchTerminalRepairBody(
                idempotency_key="repair-race-1",
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Attempt to retire a stale row.",
                confirm_no_outcome_inference=True,
            )

            with (
                patch("pa.modules.fleet.require_user"),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(request, "dispatch-race", body)

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"],
                "terminal_repair_concurrent_change",
            )
            self.assertEqual(session_reads, 2)
            self.assertIn("acknowledged_at", raised.exception.detail["changed_fields"])
            self.assertIn("events", raised.exception.detail["changed_fields"])
            repaired = ledger.get("dispatch-race")
            assert repaired is not None
            self.assertEqual(repaired.state, "completed")
            self.assertEqual(repaired.acknowledged_at, acknowledged_at)
            self.assertEqual(repaired.completion_payload, completion_payload)
            self.assertEqual(repaired.completion_envelope, completion_envelope)
            self.assertEqual(repaired.completion_received_at, acknowledged_at)
            self.assertEqual(repaired.completion_delivery_class, "acknowledged")
            self.assertEqual(
                repaired.events[-1].message,
                "Concurrent completion durably acknowledged.",
            )
            self.assertNotIn("repair-race-1", repaired.control_operations)
            self.assertFalse(
                any(
                    item["kind"] == "legacy_abandoned_dispatch_retired"
                    for item in repaired.lifecycle_inconsistencies
                )
            )
            self.assertIsNone(manager.terminal_repair_fence_id(session.id))
            queued = manager.enqueue_prompt(
                "completion won, so admission is no longer repair-fenced",
                session_id=session.id,
            )
            self.assertEqual(queued.session_id, session.id)
            with self.assertRaisesRegex(
                AgentSessionRecoveryError, "closed and has no resumable"
            ):
                await manager.recover_session(session.id)

    async def test_governed_repairs_require_recorded_authority_after_release(
        self,
    ) -> None:
        released = GoalDispatchProvenance(
            goal_id="goal-repair",
            goal_version=1,
            policy_revision=1,
            authority_instance_id="authority",
            fencing_token=7,
            action_reservation_id="reservation-repair",
            actor_principal="service:goal-supervisor:authority",
            released_at=datetime.now(UTC),
            release_reason="prior terminal reconciliation",
        )
        cases = (
            (
                "abandoned",
                DispatchTerminalRepairBody(
                    idempotency_key="repair-governed-abandoned",
                    mode="abandoned_without_acknowledgement",
                    expected_state="running",
                    reason="Wrong authority must not retire this row.",
                    confirm_no_outcome_inference=True,
                ),
                None,
            ),
            (
                "acknowledged",
                DispatchTerminalRepairBody(
                    idempotency_key="repair-governed-acknowledged",
                    mode="acknowledged_completion",
                ),
                datetime.now(UTC),
            ),
        )
        for name, body, acknowledged_at in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                settings = Settings(data_dir=Path(tmp), instance_id="target")
                ledger = DispatchStore(Path(tmp))
                ledger.put(
                    DispatchRecord(
                        dispatch_id=f"dispatch-governed-{name}",
                        mutation_id=f"mutation-governed-{name}",
                        card_id="card-done",
                        authority_instance_id="authority",
                        authority_url="http://authority",
                        target_instance_id="target",
                        session_id="session-closed",
                        state="running",
                        acknowledged_at=acknowledged_at,
                        completion_delivery_class=(
                            "acknowledged" if acknowledged_at else None
                        ),
                        goal_provenance=released,
                    )
                )
                domain = MagicMock()
                domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
                domain.get_session.return_value = SimpleNamespace(status="closed")
                request = request_for(settings, domain, {"dispatch_store": ledger})

                with (
                    patch("pa.modules.fleet.require_user"),
                    self.assertRaises(HTTPException) as raised,
                ):
                    await repair_terminal_dispatch(
                        request, f"dispatch-governed-{name}", body
                    )

                self.assertEqual(raised.exception.status_code, 409)
                self.assertEqual(
                    raised.exception.detail["code"],
                    "terminal_repair_wrong_authority",
                )
                persisted = ledger.get(f"dispatch-governed-{name}")
                assert persisted is not None
                self.assertEqual(persisted.state, "running")
                self.assertEqual(persisted.goal_provenance, released)
                self.assertNotIn(body.idempotency_key, persisted.control_operations)

    async def test_abandoned_repair_rejects_live_runtime_for_closed_session(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-live-runtime",
                    mutation_id="mutation-live-runtime",
                    card_id="card-done",
                    authority_instance_id="target",
                    authority_url="http://target",
                    target_instance_id="target",
                    session_id="session-live-runtime",
                    state="running",
                    recoverable=False,
                )
            )
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            domain.get_session.return_value = SimpleNamespace(status="closed")
            manager = MagicMock()
            manager.get.return_value = SimpleNamespace(_closed=False)
            request = request_for(
                settings,
                domain,
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            body = DispatchTerminalRepairBody(
                idempotency_key="repair-live-runtime-1",
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Attempted repair while runtime is live.",
                confirm_no_outcome_inference=True,
            )

            with (
                patch("pa.modules.fleet.require_user"),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(request, "dispatch-live-runtime", body)

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"], "linked_session_not_terminal"
            )
            self.assertTrue(raised.exception.detail["runtime_live"])
            self.assertEqual(ledger.get("dispatch-live-runtime").state, "running")

    async def test_local_repair_runtime_install_race_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-local-runtime-race",
                mutation_id="mutation-local-runtime-race",
                card_id="card-done",
                authority_instance_id="target",
                authority_url="http://target",
                target_instance_id="target",
                session_id="session-local-runtime-race",
                state="running",
                recoverable=False,
            )
            ledger.put(record)
            session = AgentSession(
                id=record.session_id or "",
                agent_name="codex",
                origin_instance_id="target",
                authority_instance_id="target",
                dispatch_id=record.dispatch_id,
                status="closed",
            )
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            domain.get_session.return_value = session
            manager = AgentSessionManager(settings, domain)
            live_runtime = SimpleNamespace(_closed=False)
            original_get = manager.get
            first_read = True

            def racing_get(session_id: str):
                nonlocal first_read
                if first_read:
                    first_read = False
                    manager._runtimes[session_id] = live_runtime
                    return None
                return original_get(session_id)

            manager.get = MagicMock(side_effect=racing_get)
            request = request_for(
                settings,
                domain,
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            body = DispatchTerminalRepairBody(
                idempotency_key="repair-local-runtime-race",
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Exercise runtime publication during local repair.",
                confirm_no_outcome_inference=True,
            )

            with (
                patch("pa.modules.fleet.require_user"),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(request, record.dispatch_id, body)

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"], "linked_session_not_terminal"
            )
            self.assertTrue(raised.exception.detail["runtime_live"])
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "running")
            self.assertNotIn(body.idempotency_key, persisted.control_operations)
            self.assertIsNone(manager.terminal_repair_fence_id(session.id))

    async def test_abandoned_repair_rejects_recoverable_and_quiesced_sessions(
        self,
    ) -> None:
        cases = (
            ("recoverable-quiesced", True, "quiesced", "dispatch_still_recoverable"),
            (
                "nonrecoverable-quiesced",
                False,
                "quiesced",
                "linked_session_not_terminal",
            ),
        )
        for suffix, recoverable, session_status, expected_code in cases:
            with self.subTest(case=suffix), tempfile.TemporaryDirectory() as tmp:
                settings = Settings(data_dir=Path(tmp), instance_id="target")
                ledger = DispatchStore(Path(tmp))
                dispatch_id = f"dispatch-{suffix}"
                ledger.put(
                    DispatchRecord(
                        dispatch_id=dispatch_id,
                        mutation_id=f"mutation-{suffix}",
                        card_id="card-done",
                        authority_instance_id="target",
                        authority_url="http://target",
                        target_instance_id="target",
                        session_id=f"session-{suffix}",
                        state="running",
                        recoverable=recoverable,
                    )
                )
                domain = MagicMock()
                domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
                domain.get_session.return_value = SimpleNamespace(status=session_status)
                request = request_for(settings, domain, {"dispatch_store": ledger})
                body = DispatchTerminalRepairBody(
                    idempotency_key=f"repair-{suffix}",
                    mode="abandoned_without_acknowledgement",
                    expected_state="running",
                    reason="Retryable or retained work must not be retired.",
                    confirm_no_outcome_inference=True,
                )

                with (
                    patch("pa.modules.fleet.require_user"),
                    self.assertRaises(HTTPException) as raised,
                ):
                    await repair_terminal_dispatch(request, dispatch_id, body)

                self.assertEqual(raised.exception.status_code, 409)
                self.assertEqual(raised.exception.detail["code"], expected_code)
                persisted = ledger.get(dispatch_id)
                assert persisted is not None
                self.assertEqual(persisted.state, "running")
                self.assertEqual(persisted.recoverable, recoverable)
                self.assertNotIn(body.idempotency_key, persisted.control_operations)

    def test_target_terminal_evidence_acquires_durable_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-target-proof",
                mutation_id="mutation-target-proof",
                card_id="card-done",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-target-proof",
                state="running",
                recoverable=False,
            )
            ledger.put(record)
            stored_before = ledger.get(record.dispatch_id)
            assert stored_before is not None
            session = AgentSession(
                id=record.session_id or "",
                agent_name="codex",
                origin_instance_id="target",
                authority_instance_id="authority",
                dispatch_id=record.dispatch_id,
                status="closed",
            )
            domain = MagicMock()
            domain.get_session.return_value = session
            manager = MagicMock()
            manager.get.return_value = None
            request = request_for(
                settings,
                domain,
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            request.state.instance_authenticated = True
            request.headers = {
                "X-PA-Origin-Instance-ID": "authority",
                "Idempotency-Key": "repair-target-proof",
            }
            body = DispatchTerminalRepairEvidenceRequest(
                mutation_id=record.mutation_id,
                authority_instance_id="authority",
                target_instance_id="target",
                session_id=session.id,
                idempotency_key="repair-target-proof",
                expected_state="running",
            )

            evidence = target_terminal_repair_evidence(
                request, record.dispatch_id, body
            )

            self.assertEqual(evidence.target_instance_id, "target")
            self.assertEqual(evidence.session_status, "closed")
            self.assertFalse(evidence.runtime_live)
            self.assertFalse(evidence.dispatch_recoverable)
            self.assertFalse(evidence.completion_acknowledged)
            self.assertFalse(evidence.completion_evidence_present)
            self.assertEqual(
                evidence.evidence_digest,
                _terminal_repair_evidence_digest(evidence),
            )
            stored_after = ledger.get(record.dispatch_id)
            assert stored_after is not None
            self.assertEqual(stored_after.state, stored_before.state)
            reservation = stored_after.terminal_repair_reservation
            assert reservation is not None
            self.assertEqual(reservation["state"], "prepared")
            self.assertEqual(reservation["reservation_id"], evidence.reservation_id)
            self.assertEqual(reservation["evidence_digest"], evidence.evidence_digest)

    def test_prepared_terminal_reservation_can_refresh_after_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ctx = target_terminal_repair_context(tmp, "refresh")
            first = target_terminal_repair_evidence(
                ctx.request, ctx.record.dispatch_id, ctx.proof_body
            )
            stale = ctx.ledger.get(ctx.record.dispatch_id)
            assert stale is not None
            reservation = dict(stale.terminal_repair_reservation or {})
            evidence_payload = dict(reservation["evidence"])
            evidence_payload["observed_at"] = (
                datetime.now(UTC) - timedelta(seconds=30)
            ).isoformat()
            reservation["evidence"] = evidence_payload
            reservation["prepared_at"] = evidence_payload["observed_at"]
            stale.terminal_repair_reservation = reservation
            ctx.ledger.put(stale)

            refreshed = target_terminal_repair_evidence(
                ctx.request, ctx.record.dispatch_id, ctx.proof_body
            )

            self.assertEqual(refreshed.reservation_id, first.reservation_id)
            self.assertGreater(refreshed.observed_at, first.observed_at)
            self.assertEqual(
                refreshed.evidence_digest,
                _terminal_repair_evidence_digest(refreshed),
            )
            persisted = ctx.ledger.get(ctx.record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.terminal_repair_reservation["state"], "prepared")
            self.assertEqual(
                persisted.terminal_repair_reservation["evidence_digest"],
                refreshed.evidence_digest,
            )

    def test_target_commit_revalidates_current_session_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ctx = target_terminal_repair_context(tmp, "session-binding")
            prepared = target_terminal_repair_evidence(
                ctx.request, ctx.record.dispatch_id, ctx.proof_body
            )
            ctx.domain.get_session.return_value = ctx.session.model_copy(
                update={"dispatch_id": "different-dispatch"}
            )
            commit_body = DispatchTerminalRepairCommitRequest(
                mutation_id=ctx.record.mutation_id,
                authority_instance_id="authority",
                target_instance_id="target",
                session_id=ctx.session.id,
                idempotency_key=ctx.proof_body.idempotency_key,
                reservation_id=prepared.reservation_id,
                evidence_digest=prepared.evidence_digest,
                expected_state="running",
            )

            with self.assertRaises(HTTPException) as raised:
                target_terminal_repair_commit(
                    ctx.request, ctx.record.dispatch_id, commit_body
                )

            self.assertEqual(
                raised.exception.detail["code"],
                "linked_session_provenance_mismatch",
            )
            self.assertIn("dispatch_id", raised.exception.detail["mismatched_fields"])
            persisted = ctx.ledger.get(ctx.record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "running")
            self.assertEqual(persisted.terminal_repair_reservation["state"], "prepared")

    async def test_durable_terminal_repair_fence_survives_manager_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ctx = target_terminal_repair_context(tmp, "restart-fence")
            prepared = target_terminal_repair_evidence(
                ctx.request, ctx.record.dispatch_id, ctx.proof_body
            )
            target_terminal_repair_commit(
                ctx.request,
                ctx.record.dispatch_id,
                DispatchTerminalRepairCommitRequest(
                    mutation_id=ctx.record.mutation_id,
                    authority_instance_id="authority",
                    target_instance_id="target",
                    session_id=ctx.session.id,
                    idempotency_key=ctx.proof_body.idempotency_key,
                    reservation_id=prepared.reservation_id,
                    evidence_digest=prepared.evidence_digest,
                    expected_state="running",
                ),
            )
            restarted = AgentSessionManager(
                ctx.settings, ctx.domain, dispatch_store=ctx.ledger
            )

            self.assertEqual(
                restarted.terminal_repair_fence_id(ctx.session.id),
                prepared.reservation_id,
            )
            with self.assertRaises(AgentSessionRecoveryError):
                restarted.enqueue_prompt("must remain fenced", session_id=ctx.session.id)
            with self.assertRaises(AgentSessionRecoveryError):
                await restarted.recover_session(ctx.session.id)
            runtime = MagicMock(session_id=ctx.session.id)
            runtime.close = AsyncMock(return_value=True)
            with self.assertRaisesRegex(RuntimeError, "terminal dispatch repair"):
                await restarted._publish_runtime(runtime)
            runtime.close.assert_awaited_once()

    def test_target_terminal_commit_replays_after_lost_response(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-target-commit-replay",
                mutation_id="mutation-target-commit-replay",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-target-commit-replay",
                state="running",
                recoverable=False,
            )
            ledger.put(record)
            session = AgentSession(
                id=record.session_id or "",
                agent_name="codex",
                origin_instance_id="target",
                authority_instance_id="authority",
                dispatch_id=record.dispatch_id,
                status="closed",
            )
            domain = MagicMock()
            domain.get_session.return_value = session
            manager = MagicMock()
            manager.get.return_value = None
            request = request_for(
                settings,
                domain,
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            request.state.instance_authenticated = True
            request.headers = {
                "X-PA-Origin-Instance-ID": "authority",
                "Idempotency-Key": "repair-target-commit-replay",
            }
            proof_body = DispatchTerminalRepairEvidenceRequest(
                mutation_id=record.mutation_id,
                authority_instance_id="authority",
                target_instance_id="target",
                session_id=session.id,
                idempotency_key="repair-target-commit-replay",
                expected_state="running",
            )
            prepared = target_terminal_repair_evidence(
                request, record.dispatch_id, proof_body
            )
            first = target_terminal_repair_commit(
                request,
                record.dispatch_id,
                DispatchTerminalRepairCommitRequest(
                    mutation_id=record.mutation_id,
                    authority_instance_id="authority",
                    target_instance_id="target",
                    session_id=session.id,
                    idempotency_key=proof_body.idempotency_key,
                    reservation_id=prepared.reservation_id,
                    evidence_digest=prepared.evidence_digest,
                    expected_state="running",
                ),
            )

            replayed_proof = target_terminal_repair_evidence(
                request, record.dispatch_id, proof_body
            )
            replayed = target_terminal_repair_commit(
                request,
                record.dispatch_id,
                DispatchTerminalRepairCommitRequest(
                    mutation_id=record.mutation_id,
                    authority_instance_id="authority",
                    target_instance_id="target",
                    session_id=session.id,
                    idempotency_key=proof_body.idempotency_key,
                    reservation_id=replayed_proof.reservation_id,
                    evidence_digest=replayed_proof.evidence_digest,
                    expected_state="running",
                ),
            )

            self.assertEqual(replayed_proof.reservation_state, "committed")
            self.assertEqual(replayed.reservation_id, first.reservation_id)
            self.assertEqual(replayed.evidence_digest, replayed_proof.evidence_digest)
            self.assertEqual(
                replayed.receipt_digest, _terminal_repair_commit_digest(replayed)
            )
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "cancelled")
            self.assertEqual(
                persisted.terminal_repair_reservation["state"], "committed"
            )
            self.assertEqual(len(persisted.events), 1)

    def test_target_terminal_evidence_fails_closed_on_concurrent_completion(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-target-proof-race",
                mutation_id="mutation-target-proof-race",
                card_id="card-done",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-target-proof-race",
                state="running",
                recoverable=False,
                events=[
                    DispatchEvent(
                        seq=1,
                        state="running",
                        message="Legacy dispatch running.",
                    )
                ],
            )
            ledger.put(record)
            acknowledged_at = datetime.now(UTC)
            completion_payload = {"outcome": "Concurrent target completion won."}
            completion_envelope = {
                "completion_id": "completion-target-proof-race",
                "payload_digest": "b" * 64,
            }
            session = AgentSession(
                id=record.session_id or "",
                agent_name="codex",
                origin_instance_id="target",
                authority_instance_id="authority",
                dispatch_id=record.dispatch_id,
                status="closed",
            )

            def get_session(_session_id):
                concurrent = ledger.get(record.dispatch_id)
                assert concurrent is not None
                concurrent.acknowledged_at = acknowledged_at
                concurrent.completion_payload = completion_payload
                concurrent.completion_envelope = completion_envelope
                concurrent.completion_received_at = acknowledged_at
                concurrent.completion_delivery_class = "acknowledged"
                ledger.transition(
                    concurrent,
                    "completed",
                    "Concurrent target completion durably acknowledged.",
                )
                return session

            domain = MagicMock()
            domain.get_session.side_effect = get_session
            manager = MagicMock()
            manager.get.return_value = None
            request = request_for(
                settings,
                domain,
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            request.state.instance_authenticated = True
            request.headers = {
                "X-PA-Origin-Instance-ID": "authority",
                "Idempotency-Key": "repair-target-proof-race",
            }
            body = DispatchTerminalRepairEvidenceRequest(
                mutation_id=record.mutation_id,
                authority_instance_id="authority",
                target_instance_id="target",
                session_id=session.id,
                idempotency_key="repair-target-proof-race",
                expected_state="running",
            )

            with self.assertRaises(HTTPException) as raised:
                target_terminal_repair_evidence(request, record.dispatch_id, body)

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"], "target_terminal_evidence_changed"
            )
            self.assertIn("acknowledged_at", raised.exception.detail["changed_fields"])
            self.assertIn("events", raised.exception.detail["changed_fields"])
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "completed")
            self.assertEqual(persisted.acknowledged_at, acknowledged_at)
            self.assertEqual(persisted.completion_payload, completion_payload)
            self.assertEqual(persisted.completion_envelope, completion_envelope)
            self.assertEqual(persisted.completion_delivery_class, "acknowledged")

    def test_target_terminal_evidence_fences_runtime_publication_race(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-target-runtime-race",
                mutation_id="mutation-target-runtime-race",
                card_id="card-done",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-target-runtime-race",
                state="running",
                recoverable=False,
            )
            ledger.put(record)
            session = AgentSession(
                id=record.session_id or "",
                agent_name="codex",
                origin_instance_id="target",
                authority_instance_id="authority",
                dispatch_id=record.dispatch_id,
                status="closed",
            )
            domain = MagicMock()
            domain.get_session.return_value = session
            manager = AgentSessionManager(settings, domain)
            live_runtime = SimpleNamespace(_closed=False)
            original_get = manager.get
            first_read = True

            def racing_get(session_id: str):
                nonlocal first_read
                if first_read:
                    first_read = False
                    # Model provider startup publishing immediately after the
                    # first empty observation. The atomic acquisition must
                    # observe it before a proof can be returned.
                    manager._runtimes[session_id] = live_runtime
                    return None
                return original_get(session_id)

            manager.get = MagicMock(side_effect=racing_get)
            request = request_for(
                settings,
                domain,
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            request.state.instance_authenticated = True
            request.headers = {
                "X-PA-Origin-Instance-ID": "authority",
                "Idempotency-Key": "repair-target-runtime-race",
            }
            body = DispatchTerminalRepairEvidenceRequest(
                mutation_id=record.mutation_id,
                authority_instance_id="authority",
                target_instance_id="target",
                session_id=session.id,
                idempotency_key="repair-target-runtime-race",
                expected_state="running",
            )

            with self.assertRaises(HTTPException) as raised:
                target_terminal_repair_evidence(request, record.dispatch_id, body)

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"], "linked_session_not_terminal"
            )
            self.assertTrue(raised.exception.detail["runtime_live"])
            self.assertIsNone(manager.terminal_repair_fence_id(session.id))

    async def test_runtime_started_before_terminal_fence_cannot_publish_after_it(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), instance_id="target"), MagicMock()
            )
            self.assertIsNone(
                manager.acquire_terminal_repair_fence(
                    "session-fenced", fence_id="dispatch:repair"
                )
            )
            runtime = MagicMock()
            runtime.session_id = "session-fenced"
            runtime.close = AsyncMock(return_value=True)

            with self.assertRaisesRegex(RuntimeError, "terminal dispatch repair"):
                await manager._publish_runtime(runtime)

            runtime.close.assert_awaited_once_with(
                reason="terminal_dispatch_repair_fenced",
                reconcile_workspace=False,
            )
            self.assertIsNone(manager.get("session-fenced"))

    async def test_terminal_repair_fence_blocks_recovery_and_prompt_admission(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), instance_id="target"), MagicMock()
            )
            session_id = "session-repair-admission-fence"
            self.assertIsNone(
                manager.acquire_terminal_repair_fence(
                    session_id, fence_id="dispatch:repair"
                )
            )

            with self.assertRaises(AgentSessionRecoveryError):
                manager.enqueue_prompt("must not queue", session_id=session_id)
            with self.assertRaises(AgentSessionRecoveryError):
                await manager.recover_session(session_id)
            with self.assertRaises(AgentSessionRecoveryError):
                await manager.prompt("must not run", session_id=session_id)

    def test_lost_release_cannot_report_newer_fence_as_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            session_id = "session-newer-repair-fence"
            fence_id = "dispatch:newer-repair-fence"
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-newer-repair-fence",
                    mutation_id="mutation-newer-repair-fence",
                    authority_instance_id="target",
                    authority_url="http://target",
                    target_instance_id="target",
                    session_id=session_id,
                    state="completed",
                    acknowledged_at=datetime.now(UTC),
                )
            )
            manager = AgentSessionManager(
                settings, MagicMock(), dispatch_store=ledger
            )
            manager.acquire_terminal_repair_fence(
                session_id,
                fence_id=fence_id,
                acquisition_id="old-acquisition",
            )
            original_release = manager.release_terminal_repair_fence

            def replace_before_release(
                current_session_id,
                *,
                fence_id,
                acquisition_id=None,
            ):
                manager.acquire_terminal_repair_fence(
                    current_session_id,
                    fence_id=fence_id,
                    acquisition_id="new-acquisition",
                )
                return original_release(
                    current_session_id,
                    fence_id=fence_id,
                    acquisition_id=acquisition_id,
                )

            with patch.object(
                manager,
                "release_terminal_repair_fence",
                side_effect=replace_before_release,
            ):
                with self.assertRaises(AgentSessionRecoveryError):
                    manager._require_not_terminal_repair_fenced(session_id)

            self.assertEqual(
                manager._terminal_repair_fence_acquisitions[session_id],
                "new-acquisition",
            )

    async def test_local_compare_conflict_releases_exact_fence_acquisition(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-local-fence-conflict",
                mutation_id="mutation-local-fence-conflict",
                card_id="card-done",
                authority_instance_id="target",
                authority_url="http://target",
                target_instance_id="target",
                session_id="session-local-fence-conflict",
                state="running",
                recoverable=False,
            )
            ledger.put(record)
            session = AgentSession(
                id=record.session_id or "",
                agent_name="codex",
                origin_instance_id="target",
                authority_instance_id="target",
                dispatch_id=record.dispatch_id,
                status="closed",
            )
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            reads = 0

            def mutate_on_revalidation(_session_id):
                nonlocal reads
                reads += 1
                if reads == 2:
                    concurrent = ledger.get(record.dispatch_id)
                    assert concurrent is not None
                    ledger.transition(
                        concurrent,
                        "running",
                        "Concurrent same-state lifecycle evidence.",
                    )
                return session

            domain.get_session.side_effect = mutate_on_revalidation
            manager = AgentSessionManager(settings, domain, dispatch_store=ledger)
            request = request_for(
                settings,
                domain,
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            body = DispatchTerminalRepairBody(
                idempotency_key="repair-local-fence-conflict",
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Exercise the exact local fence generation cleanup.",
                confirm_no_outcome_inference=True,
            )

            with (
                patch("pa.modules.fleet.require_user"),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(request, record.dispatch_id, body)

            self.assertEqual(
                raised.exception.detail["code"],
                "terminal_repair_concurrent_change",
            )
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "running")
            self.assertIsNone(persisted.terminal_repair_reservation)
            self.assertIsNone(manager.terminal_repair_fence_id(session.id))

    async def test_losing_same_key_repair_cannot_release_newer_fence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            session_id = "session-same-key-retry"
            dispatch_id = "dispatch-same-key-retry"
            fence_id = "terminal-repair:same-key"
            record = DispatchRecord(
                dispatch_id=dispatch_id,
                mutation_id="mutation-same-key-retry",
                authority_instance_id="target",
                authority_url="http://target",
                target_instance_id="target",
                session_id=session_id,
                state="running",
                recoverable=False,
            )
            ledger.put(record)
            manager = AgentSessionManager(
                settings, MagicMock(), dispatch_store=ledger
            )
            request = request_for(
                settings,
                MagicMock(),
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            self.assertIsNone(
                manager.acquire_terminal_repair_fence(
                    session_id,
                    fence_id=fence_id,
                    acquisition_id="old-acquisition",
                )
            )
            original_release = AgentSessionManager.release_terminal_repair_fence

            def prepare_retry_before_old_release(
                current_manager,
                current_session_id,
                *,
                fence_id,
                acquisition_id=None,
            ):
                self.assertIsNone(
                    current_manager.acquire_terminal_repair_fence(
                        current_session_id,
                        fence_id=fence_id,
                        acquisition_id="new-acquisition",
                    )
                )
                current = ledger.get(dispatch_id)
                assert current is not None
                current.terminal_repair_reservation = {
                    "state": "committed",
                    "reservation_id": fence_id,
                }
                ledger.put(current)
                return original_release(
                    current_manager,
                    current_session_id,
                    fence_id=fence_id,
                    acquisition_id=acquisition_id,
                )

            with patch.object(
                AgentSessionManager,
                "release_terminal_repair_fence",
                autospec=True,
                side_effect=prepare_retry_before_old_release,
            ):
                _release_terminal_repair_fence_if_uncommitted(
                    request,
                    record,
                    fence_id=fence_id,
                    fence_acquisition_id="old-acquisition",
                )

            self.assertEqual(
                manager.terminal_repair_fence_id(session_id), fence_id
            )
            self.assertEqual(
                manager._terminal_repair_fence_acquisitions[session_id],
                "new-acquisition",
            )
            runtime = MagicMock()
            runtime.session_id = session_id
            runtime.close = AsyncMock(return_value=True)
            with self.assertRaisesRegex(RuntimeError, "terminal dispatch repair"):
                await manager._publish_runtime(runtime)
            runtime.close.assert_awaited_once_with(
                reason="terminal_dispatch_repair_fenced",
                reconcile_workspace=False,
            )

    async def test_followup_failure_cannot_replay_state_over_terminal_repair(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            dispatch_id = "dispatch-followup-repair-race"
            session_id = "session-followup-repair-race"
            repair_key = "repair-followup-race"
            record = DispatchRecord(
                dispatch_id=dispatch_id,
                mutation_id="mutation-followup-repair-race",
                card_id="card-done",
                authority_instance_id="target",
                authority_url="http://target",
                target_instance_id="target",
                session_id=session_id,
                state="running",
                recoverable=False,
            )
            ledger.put(record)
            stale = ledger.get(dispatch_id)
            assert stale is not None
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            domain.get_session.return_value = SimpleNamespace(status="closed")
            manager = AgentSessionManager(
                settings, domain, dispatch_store=ledger
            )
            request = request_for(
                settings,
                domain,
                {"dispatch_store": ledger, "instance_agent": manager},
            )
            request.state.instance_authenticated = True
            body = DispatchTerminalRepairBody(
                idempotency_key=repair_key,
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Fence a stale follow-up failure writer.",
                confirm_no_outcome_inference=True,
            )
            prompt_started = asyncio.Event()
            resume_prompt = asyncio.Event()

            async def rejected_after_repair(*_args, **_kwargs):
                prompt_started.set()
                await resume_prompt.wait()
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "terminal_dispatch_repair_fenced",
                        "message": "Repair fenced the linked session.",
                    },
                )

            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_agent_json",
                    AsyncMock(side_effect=rejected_after_repair),
                ),
            ):
                prompt = asyncio.create_task(
                    prompt_dispatch_session(
                        request,
                        dispatch_id,
                        DispatchFollowupBody(
                            message="continue",
                            idempotency_key="followup-race",
                        ),
                    )
                )
                await asyncio.wait_for(prompt_started.wait(), timeout=5)
                repaired = await repair_terminal_dispatch(
                    request, dispatch_id, body
                )
                self.assertEqual(repaired["state"], "cancelled")
                with self.assertRaises(DispatchCompareConflict):
                    ledger.put(stale)
                resume_prompt.set()
                with self.assertRaises(HTTPException) as raised:
                    await asyncio.wait_for(prompt, timeout=5)

            self.assertEqual(raised.exception.status_code, 409)
            persisted = ledger.get(dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "cancelled")
            self.assertFalse(persisted.recoverable)
            self.assertEqual(
                persisted.control_operations[repair_key],
                "repair_terminal:abandoned_without_acknowledgement",
            )
            reservation = persisted.terminal_repair_reservation
            assert reservation is not None
            self.assertEqual(reservation["state"], "committed")
            self.assertIsNotNone(persisted.capacity_released_at)
            self.assertEqual(
                persisted.followup_operations["followup-race"]["state"],
                "failed",
            )
            self.assertEqual(
                persisted.followup_operations["followup-race"]["error"]["code"],
                "terminal_dispatch_repair_fenced",
            )
            self.assertIn(
                "legacy_abandoned_dispatch_retired",
                {
                    item["kind"]
                    for item in persisted.lifecycle_inconsistencies
                },
            )

    async def test_remote_repair_collects_target_proof_after_slow_snapshot(
        self,
    ) -> None:
        with (
            tempfile.TemporaryDirectory() as authority_tmp,
            tempfile.TemporaryDirectory() as target_tmp,
        ):
            authority_ledger, record, authority_request, body = (
                remote_terminal_repair_context(authority_tmp, "late-completion")
            )
            target_ledger = DispatchStore(Path(target_tmp))
            target_ledger.put(record.model_copy(deep=True))
            target_session = AgentSession(
                id=record.session_id or "",
                agent_name="codex",
                origin_instance_id="target",
                authority_instance_id="authority",
                dispatch_id=record.dispatch_id,
                status="closed",
            )
            target_domain = MagicMock()
            target_domain.get_session.return_value = target_session
            target_manager = MagicMock()
            target_manager.get.return_value = None
            target_request = request_for(
                Settings(data_dir=Path(target_tmp), instance_id="target"),
                target_domain,
                {
                    "dispatch_store": target_ledger,
                    "instance_agent": target_manager,
                },
            )
            target_request.state.instance_authenticated = True
            target_request.headers = {
                "X-PA-Origin-Instance-ID": "authority",
                "Idempotency-Key": body.idempotency_key,
            }

            def slow_card_snapshot(*_args, **_kwargs):
                completed = target_ledger.get(record.dispatch_id)
                assert completed is not None
                if completed.state != "completed":
                    now = datetime.now(UTC)
                    completed.acknowledged_at = now
                    completed.completion_received_at = now
                    completed.completion_delivery_class = "acknowledged"
                    completed.completion_payload = {
                        "outcome": "Completion won during the slow card snapshot."
                    }
                    target_ledger.transition(
                        completed,
                        "completed",
                        "Target completion was acknowledged before final proof.",
                    )
                return SimpleNamespace(lane=CardLane.DONE)

            authority_request.app.state.ctx.store.get_card.side_effect = (
                slow_card_snapshot
            )

            async def final_target_proof(_request, _target, dispatch_id, proof_body):
                return target_terminal_repair_evidence(
                    target_request, dispatch_id, proof_body
                )

            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_terminal_repair_evidence",
                    AsyncMock(side_effect=final_target_proof),
                ) as peer,
                self.assertRaises(HTTPException),
            ):
                await repair_terminal_dispatch(
                    authority_request, record.dispatch_id, body
                )

            peer.assert_awaited_once()
            authority = authority_ledger.get(record.dispatch_id)
            target = target_ledger.get(record.dispatch_id)
            assert authority is not None and target is not None
            self.assertEqual(authority.state, "running")
            self.assertEqual(target.state, "completed")
            self.assertEqual(target.completion_delivery_class, "acknowledged")

    async def test_remote_completion_after_prepare_prevents_authority_commit(
        self,
    ) -> None:
        with (
            tempfile.TemporaryDirectory() as authority_tmp,
            tempfile.TemporaryDirectory() as target_tmp,
        ):
            authority_ledger, record, authority_request, body = (
                remote_terminal_repair_context(
                    authority_tmp, "completion-after-prepare"
                )
            )
            target_ledger = DispatchStore(Path(target_tmp))
            target_ledger.put(record.model_copy(deep=True))
            session = AgentSession(
                id=record.session_id or "",
                agent_name="codex",
                origin_instance_id="target",
                authority_instance_id="authority",
                dispatch_id=record.dispatch_id,
                status="closed",
            )
            target_domain = MagicMock()
            target_domain.get_session.return_value = session
            target_settings = Settings(data_dir=Path(target_tmp), instance_id="target")
            target_manager = AgentSessionManager(
                target_settings, target_domain, dispatch_store=target_ledger
            )
            target_request = request_for(
                target_settings,
                target_domain,
                {
                    "dispatch_store": target_ledger,
                    "instance_agent": target_manager,
                },
            )
            target_request.state.instance_authenticated = True
            target_request.headers = {
                "X-PA-Origin-Instance-ID": "authority",
                "Idempotency-Key": body.idempotency_key,
            }
            payload = {
                "stop_reason": "end_turn",
                "outcome": "Completion won after reservation preparation.",
            }

            async def prepare(_request, _target, dispatch_id, proof_body):
                return target_terminal_repair_evidence(
                    target_request, dispatch_id, proof_body
                )

            async def complete_then_consume(
                _request, _target, dispatch_id, commit_body
            ):
                self.assertTrue(
                    target_ledger.queue_completion_payload(session.id, payload)
                )
                return target_terminal_repair_commit(
                    target_request, dispatch_id, commit_body
                )

            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_terminal_repair_evidence",
                    AsyncMock(side_effect=prepare),
                ),
                patch(
                    "pa.modules.fleet._peer_terminal_repair_commit",
                    AsyncMock(side_effect=complete_then_consume),
                ),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(
                    authority_request, record.dispatch_id, body
                )

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"],
                "target_terminal_reservation_not_consumable",
            )
            authority = authority_ledger.get(record.dispatch_id)
            target = target_ledger.get(record.dispatch_id)
            assert authority is not None and target is not None
            self.assertEqual(authority.state, "running")
            self.assertNotIn(body.idempotency_key, authority.control_operations)
            self.assertEqual(target.state, "completion_pending")
            self.assertEqual(target.completion_payload, payload)
            reservation = target.terminal_repair_reservation
            assert reservation is not None
            self.assertEqual(reservation["state"], "superseded_by_completion")
            self.assertIsNone(target_manager.terminal_repair_fence_id(session.id))
            queued = target_manager.enqueue_prompt(
                "completion superseded the target repair reservation",
                session_id=session.id,
            )
            self.assertEqual(queued.session_id, session.id)
            with self.assertRaisesRegex(
                AgentSessionRecoveryError, "closed and has no resumable"
            ):
                await target_manager.recover_session(session.id)

    async def test_completion_callback_supersedes_local_terminal_repair(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            session_id = "session-late-after-repair"
            repair_key = "repair-local-late-completion"
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-local-late-completion",
                    mutation_id="mutation-local-late-completion",
                    card_id="card-done",
                    authority_instance_id="target",
                    authority_url="http://target",
                    target_instance_id="target",
                    session_id=session_id,
                    state="cancelled",
                    recoverable=False,
                    error_code="legacy_abandoned_dispatch_retired",
                    control_operations={
                        repair_key: (
                            "repair_terminal:abandoned_without_acknowledgement"
                        )
                    },
                )
            )
            outbox = CompletionOutbox(ledger, "")
            agent = MagicMock()
            agent.async_runtime = None
            reconciler = CompletionReconciler(
                ledger,
                agent,
                outbox,
                MagicMock(),
                lambda: None,
            )
            payload = {
                "stop_reason": "end_turn",
                "outcome": "Late immutable completion must win.",
            }

            accepted = await reconciler.handle_completion(session_id, payload)

            self.assertTrue(accepted)
            persisted = ledger.by_session(session_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "completion_pending")
            self.assertEqual(persisted.completion_payload, payload)
            self.assertEqual(persisted.completion_delivery_class, "pending")
            self.assertIsNone(persisted.error_code)
            self.assertEqual(
                persisted.lifecycle_inconsistencies[-1]["kind"],
                "terminal_repair_superseded_by_completion",
            )

    async def test_completion_callback_started_before_repair_preserves_repair_audit(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            settings = Settings(data_dir=data_dir, instance_id="target")
            ledger = DispatchStore(data_dir)
            session_id = "session-stale-completion"
            dispatch_id = "dispatch-stale-completion"
            repair_key = "repair-stale-completion"
            payload = {
                "stop_reason": "end_turn",
                "outcome": "Completion callback began before repair committed.",
            }
            ledger.put(
                DispatchRecord(
                    dispatch_id=dispatch_id,
                    mutation_id="mutation-stale-completion",
                    card_id="card-done",
                    authority_instance_id="target",
                    authority_url="http://target",
                    target_instance_id="target",
                    session_id=session_id,
                    state="running",
                    recoverable=False,
                )
            )
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            domain.get_session.return_value = SimpleNamespace(status="closed")
            request = request_for(
                settings, domain, {"dispatch_store": ledger}
            )
            body = DispatchTerminalRepairBody(
                idempotency_key=repair_key,
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Exercise the callback/repair writer race.",
                confirm_no_outcome_inference=True,
            )
            outbox = CompletionOutbox(ledger, "")
            callback_started = threading.Event()
            resume_callback = threading.Event()
            original_queue = ledger.queue_completion_payload
            accepted: list[bool] = []

            def delayed_atomic_queue(
                current_session_id: str, current_payload: dict
            ) -> bool:
                callback_started.set()
                if not resume_callback.wait(5):
                    raise RuntimeError("completion callback race barrier timed out")
                return original_queue(current_session_id, current_payload)

            ledger.queue_completion_payload = delayed_atomic_queue
            worker = threading.Thread(
                target=lambda: accepted.append(outbox.queue(session_id, payload)),
                name="completion-callback-before-repair",
            )
            worker.start()
            try:
                self.assertTrue(callback_started.wait(5))
                with patch("pa.modules.fleet.require_user"):
                    repaired = await repair_terminal_dispatch(
                        request, dispatch_id, body
                    )
                self.assertEqual(repaired["state"], "cancelled")
            finally:
                resume_callback.set()
                worker.join(5)
                ledger.queue_completion_payload = original_queue
            self.assertFalse(worker.is_alive())
            self.assertEqual(accepted, [True])

            ledger.close()
            reopened = DispatchStore(data_dir)
            persisted = reopened.get(dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "completion_pending")
            self.assertEqual(persisted.completion_payload, payload)
            self.assertEqual(persisted.completion_delivery_class, "pending")
            self.assertEqual(
                persisted.control_operations[repair_key],
                "repair_terminal:abandoned_without_acknowledgement",
            )
            repair_audits = [
                item
                for item in persisted.lifecycle_inconsistencies
                if item["kind"] == "legacy_abandoned_dispatch_retired"
            ]
            supersession_audits = [
                item
                for item in persisted.lifecycle_inconsistencies
                if item["kind"] == "terminal_repair_superseded_by_completion"
            ]
            self.assertEqual(len(repair_audits), 1)
            self.assertEqual(len(supersession_audits), 1)
            self.assertEqual(
                [event.state for event in persisted.events],
                ["cancelled", "completion_pending"],
            )

            replay_request = request_for(
                settings, domain, {"dispatch_store": reopened}
            )
            with patch("pa.modules.fleet.require_user"):
                replay = await repair_terminal_dispatch(
                    replay_request, dispatch_id, body
                )
            self.assertEqual(replay["state"], "completion_pending")
            replayed = reopened.get(dispatch_id)
            assert replayed is not None
            self.assertEqual(replayed.control_operations, persisted.control_operations)
            self.assertEqual(
                replayed.lifecycle_inconsistencies,
                persisted.lifecycle_inconsistencies,
            )
            self.assertEqual(replayed.events, persisted.events)
            reopened.close()

    async def test_reconciliation_save_cannot_erase_concurrent_repair(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-reconciliation-race",
                mutation_id="mutation-reconciliation-race",
                card_id="card-done",
                authority_instance_id="target",
                authority_url="http://target",
                target_instance_id="target",
                session_id="session-reconciliation-race",
                state="running",
                recoverable=False,
            )
            ledger.put(record)
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            domain.get_session.return_value = SimpleNamespace(status="closed")
            request = request_for(settings, domain, {"dispatch_store": ledger})
            body = DispatchTerminalRepairBody(
                idempotency_key="repair-reconciliation-race",
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Fence a stale reconciliation metadata save.",
                confirm_no_outcome_inference=True,
            )
            agent = MagicMock()
            agent.store = domain
            agent.get.return_value = None
            agent.async_runtime = None
            reconciler = CompletionReconciler(
                ledger, agent, CompletionOutbox(ledger, ""), domain, lambda: None
            )
            save_started = asyncio.Event()
            resume_save = asyncio.Event()
            original_save = reconciler._save

            async def delayed_save(stale: DispatchRecord) -> None:
                save_started.set()
                await resume_save.wait()
                await original_save(stale)

            reconciler._save = delayed_save
            payload = {
                "card_disposition": {
                    "contract": "pa.card-disposition/v1",
                    "lane": "active",
                    "outcome": "Completion won the reconciliation race.",
                    "evidence": {"integration_required": False},
                }
            }
            completion = asyncio.create_task(
                reconciler.handle_completion(record.session_id or "", payload)
            )
            await asyncio.wait_for(save_started.wait(), timeout=5)
            try:
                with patch("pa.modules.fleet.require_user"):
                    repaired = await repair_terminal_dispatch(
                        request, record.dispatch_id, body
                    )
                self.assertEqual(repaired["state"], "cancelled")
            finally:
                resume_save.set()
            self.assertTrue(await asyncio.wait_for(completion, timeout=5))
            await original_save(record)

            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "completion_pending")
            self.assertEqual(persisted.completion_payload, payload)
            self.assertEqual(
                persisted.control_operations[body.idempotency_key],
                "repair_terminal:abandoned_without_acknowledgement",
            )
            reservation = persisted.terminal_repair_reservation
            assert reservation is not None
            self.assertEqual(reservation["state"], "committed")
            restarted = AgentSessionManager(
                settings, domain, dispatch_store=ledger
            )
            self.assertEqual(
                restarted.terminal_repair_fence_id(record.session_id or ""),
                reservation["reservation_id"],
            )
            with self.assertRaises(AgentSessionRecoveryError):
                restarted.enqueue_prompt(
                    "must remain fenced", session_id=record.session_id
                )
            with self.assertRaises(AgentSessionRecoveryError):
                await restarted.recover_session(record.session_id or "")
            kinds = {item["kind"] for item in persisted.lifecycle_inconsistencies}
            self.assertIn("legacy_abandoned_dispatch_retired", kinds)
            self.assertIn("terminal_repair_superseded_by_completion", kinds)

    async def test_missing_disposition_save_queues_after_concurrent_repair(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-missing-disposition-race",
                mutation_id="mutation-missing-disposition-race",
                card_id="card-done",
                authority_instance_id="target",
                authority_url="http://target",
                target_instance_id="target",
                session_id="session-missing-disposition-race",
                state="running",
                recoverable=False,
            )
            ledger.put(record)
            domain = MagicMock()
            domain.get_card.return_value = SimpleNamespace(lane=CardLane.DONE)
            domain.get_session.return_value = SimpleNamespace(status="closed")
            request = request_for(settings, domain, {"dispatch_store": ledger})
            body = DispatchTerminalRepairBody(
                idempotency_key="repair-missing-disposition-race",
                mode="abandoned_without_acknowledgement",
                expected_state="running",
                reason="Fence a missing-disposition completion metadata save.",
                confirm_no_outcome_inference=True,
            )
            agent = MagicMock()
            agent.store = domain
            agent.get.return_value = None
            agent.async_runtime = None
            reconciler = CompletionReconciler(
                ledger, agent, CompletionOutbox(ledger, ""), domain, lambda: None
            )
            save_started = asyncio.Event()
            resume_save = asyncio.Event()
            original_save = reconciler._save

            async def delayed_save(stale: DispatchRecord):
                save_started.set()
                await resume_save.wait()
                return await original_save(stale)

            reconciler._save = delayed_save
            payload = {
                "stop_reason": "end_turn",
                "outcome": "Missing disposition completion won the repair race.",
            }
            completion = asyncio.create_task(
                reconciler.handle_completion(record.session_id or "", payload)
            )
            await asyncio.wait_for(save_started.wait(), timeout=5)
            try:
                with patch("pa.modules.fleet.require_user"):
                    repaired = await repair_terminal_dispatch(
                        request, record.dispatch_id, body
                    )
                self.assertEqual(repaired["state"], "cancelled")
            finally:
                resume_save.set()

            self.assertTrue(await asyncio.wait_for(completion, timeout=5))
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "completion_pending")
            self.assertEqual(persisted.completion_payload, payload)
            self.assertEqual(persisted.completion_delivery_class, "pending")
            self.assertEqual(persisted.reconciliation_state, "pending")
            self.assertEqual(
                persisted.control_operations[body.idempotency_key],
                "repair_terminal:abandoned_without_acknowledgement",
            )
            reservation = persisted.terminal_repair_reservation
            assert reservation is not None
            self.assertEqual(reservation["state"], "committed")
            kinds = {item["kind"] for item in persisted.lifecycle_inconsistencies}
            self.assertIn("legacy_abandoned_dispatch_retired", kinds)
            self.assertIn("terminal_repair_superseded_by_completion", kinds)

    async def test_remote_target_closed_proof_repairs_without_local_replica_use(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger, record, request, body = remote_terminal_repair_context(
                tmp, "closed"
            )
            proof = terminal_repair_evidence(record, body.idempotency_key)
            commit = terminal_repair_commit(record, proof)
            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_terminal_repair_evidence",
                    AsyncMock(return_value=proof),
                ) as peer,
                patch(
                    "pa.modules.fleet._peer_terminal_repair_commit",
                    AsyncMock(return_value=commit),
                ) as peer_commit,
            ):
                result = await repair_terminal_dispatch(
                    request, record.dispatch_id, body
                )

            self.assertEqual(result["state"], "cancelled")
            peer.assert_awaited_once()
            peer_commit.assert_awaited_once()
            request.app.state.ctx.store.get_session.assert_not_called()
            request.app.state.ctx.services["instance_agent"].get.assert_not_called()
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            diagnostic = persisted.lifecycle_inconsistencies[-1]
            self.assertEqual(
                diagnostic["evidence"]["source"], "authenticated_remote_target"
            )
            self.assertEqual(
                diagnostic["evidence"]["target_evidence_digest"],
                proof.evidence_digest,
            )

    async def test_remote_target_live_proof_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger, record, request, body = remote_terminal_repair_context(tmp, "live")
            proof = terminal_repair_evidence(
                record, body.idempotency_key, runtime_live=True
            )
            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_terminal_repair_evidence",
                    AsyncMock(return_value=proof),
                ),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(request, record.dispatch_id, body)

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"], "linked_session_not_terminal"
            )
            self.assertTrue(raised.exception.detail["runtime_live"])
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "running")
            self.assertNotIn(body.idempotency_key, persisted.control_operations)

    async def test_remote_target_stale_closed_proof_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger, record, request, body = remote_terminal_repair_context(tmp, "stale")
            proof = terminal_repair_evidence(
                record,
                body.idempotency_key,
                observed_at=datetime.now(UTC) - timedelta(seconds=30),
            )
            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_terminal_repair_evidence",
                    AsyncMock(return_value=proof),
                ),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(request, record.dispatch_id, body)

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"], "target_terminal_evidence_stale"
            )
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "running")
            self.assertNotIn(body.idempotency_key, persisted.control_operations)

    async def test_remote_target_unreachable_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger, record, request, body = remote_terminal_repair_context(
                tmp, "unreachable"
            )
            unavailable = HTTPException(
                status_code=502,
                detail={
                    "code": "target_terminal_evidence_unavailable",
                    "target_instance_id": "target",
                    "recoverable": True,
                },
            )
            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_terminal_repair_evidence",
                    AsyncMock(side_effect=unavailable),
                ),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(request, record.dispatch_id, body)

            self.assertEqual(raised.exception.status_code, 502)
            self.assertEqual(
                raised.exception.detail["code"],
                "target_terminal_evidence_unavailable",
            )
            request.app.state.ctx.store.get_session.assert_not_called()
            request.app.state.ctx.services["instance_agent"].get.assert_not_called()
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "running")
            self.assertNotIn(body.idempotency_key, persisted.control_operations)

    async def test_remote_target_evidence_timeout_is_normalized(self) -> None:
        settings = Settings(
            instance_id="authority",
            instance_url="http://authority",
            sync_token="secret",
        )
        fleet = MagicMock()
        fleet.list_instances.return_value = [
            FleetInstance(
                instance_id="target",
                name="target",
                url="http://target",
            )
        ]
        client = MagicMock()
        request = request_for(
            settings,
            MagicMock(),
            {
                "fleet_registry": fleet,
                "fleet_http_client": client,
            },
        )
        body = DispatchTerminalRepairEvidenceRequest(
            mutation_id="mutation-timeout",
            authority_instance_id="authority",
            target_instance_id="target",
            session_id="session-timeout",
            idempotency_key="repair-timeout",
            expected_state="running",
        )

        with (
            patch(
                "pa.modules.fleet._fleet_http",
                AsyncMock(side_effect=TimeoutError),
            ),
            self.assertRaises(HTTPException) as raised,
        ):
            await _peer_terminal_repair_evidence(
                request, "target", "dispatch-timeout", body
            )

        self.assertEqual(raised.exception.status_code, 502)
        self.assertEqual(
            raised.exception.detail["code"], "target_terminal_evidence_unavailable"
        )
        self.assertEqual(raised.exception.detail["target_instance_id"], "target")
        self.assertTrue(raised.exception.detail["recoverable"])

    async def test_mixed_version_target_without_proof_route_fails_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger, record, request, body = remote_terminal_repair_context(
                tmp, "mixed-version"
            )
            unsupported = HTTPException(
                status_code=404,
                detail={
                    "code": "target_terminal_evidence_unsupported",
                    "target_instance_id": "target",
                    "upgrade_required": True,
                    "recoverable": True,
                },
            )
            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._peer_terminal_repair_evidence",
                    AsyncMock(side_effect=unsupported),
                ),
                self.assertRaises(HTTPException) as raised,
            ):
                await repair_terminal_dispatch(request, record.dispatch_id, body)

            self.assertEqual(raised.exception.status_code, 404)
            self.assertEqual(
                raised.exception.detail["code"],
                "target_terminal_evidence_unsupported",
            )
            self.assertTrue(raised.exception.detail["upgrade_required"])
            persisted = ledger.get(record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "running")
            self.assertNotIn(body.idempotency_key, persisted.control_operations)


class MaterializationTests(unittest.TestCase):
    def test_target_identity_upgrade_is_monotonic_idempotent_and_session_exact(
        self,
    ) -> None:
        session_id = "99999999-9999-4999-8999-999999999999"
        forged_session_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        plan = {
            "contract_version": 1,
            "profile": "research",
            "profile_source": "dispatch_override",
            "requirements": {
                "repository_required": False,
                "repositories": [],
                "attachments": False,
                "browser": False,
                "external_tools": [],
                "required_capabilities": [],
                "writable_artifact_workspace": True,
                "network_policy": "provider-default",
                "expected_deliverables": [],
            },
            "target_instance_id": TARGET_ID,
            "repositories": [],
            "workspace": {"kind": "artifact"},
            "missing_dependencies": [],
            "stale_dependencies": [],
            "confirmation_required": False,
            "summary": "Canonical target identity transition.",
        }
        envelope = GoalMaterializationEnvelopeV1(
            work_package_id="work-package-a",
            service_role="executor",
            resource_claims=(
                GoalMaterializationResourceClaimV1(key=f"fleet-dispatch:{TARGET_ID}"),
            ),
            execution_contract_digest=canonical_materialization_digest(None),
        )
        receipt = GoalMaterializationReceiptV1(
            envelope_digest=str(envelope.digest),
            target_instance_id=TARGET_ID,
            provider_id="codex",
            materialization_plan_digest=canonical_materialization_digest(plan),
        )
        stage = GoalDispatchProvenance(
            goal_id="goal-a",
            goal_version=1,
            policy_revision=1,
            authority_instance_id=AUTHORITY_ID,
            fencing_token=1,
            action_reservation_id="reservation-a",
            operation_key="dispatch-operation-a",
            requested_placement_target=TARGET_ID,
            placement_input_digest="a" * 64,
            resolved_target_instance_id=TARGET_ID,
            placement_decision_digest="b" * 64,
            materialization_envelope=envelope,
            materialization_receipt=receipt,
            actor_principal="service:goal-supervisor:authority",
            provider_id="codex",
            max_reservation_attempts=2,
        )
        identity = _expected_goal_dispatch_execution_identity(stage, session_id)
        bound = stage.model_copy(update={"execution_identity": identity})

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id=TARGET_ID)
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id=DISPATCH_ONE,
                    mutation_id=MUTATION_ONE,
                    realm_id="default",
                    authority_instance_id=AUTHORITY_ID,
                    authority_url="http://authority",
                    target_instance_id=TARGET_ID,
                    session_id=session_id,
                    state="starting_session",
                    request_payload={
                        "provider": "codex",
                        "model_id": None,
                        "mode_id": None,
                        "execution_contract": None,
                    },
                    materialization_plan=plan,
                    goal_provenance=stage,
                )
            )
            request = request_for(
                settings,
                MagicMock(),
                {"dispatch_store": ledger},
            )
            request.state.instance_authenticated = True
            request.headers = {"X-PA-Origin-Instance-ID": AUTHORITY_ID}

            def body(provenance, session: str | None = session_id):
                return DispatchMaterializeBody(
                    dispatch_id=DISPATCH_ONE,
                    mutation_id=MUTATION_ONE,
                    realm_id="default",
                    authority_instance_id=AUTHORITY_ID,
                    authority_url="http://authority",
                    target_instance_id=TARGET_ID,
                    provider="codex",
                    execution_contract=None,
                    session_id=session,
                    materialization_plan=plan,
                    goal_provenance=provenance,
                )

            crash_replay = materialize_dispatch(request, body(stage, None))
            self.assertIsNone(crash_replay["execution_identity_digest"])
            upgraded = materialize_dispatch(request, body(bound))
            self.assertEqual(upgraded["execution_identity_digest"], identity.digest)
            persisted = ledger.get(DISPATCH_ONE)
            assert persisted is not None and persisted.goal_provenance is not None
            self.assertEqual(persisted.goal_provenance.execution_identity, identity)

            # A retry replays the pre-session stage first, then the same full
            # binding. Neither request may downgrade or rewrite the target copy.
            retry_stage = stage.model_copy(
                update={
                    "goal_version": stage.goal_version + 1,
                    "action_reservation_id": "reservation-b",
                    "reservation_attempt": stage.reservation_attempt + 1,
                    "retry_idempotency_key": "retry-existing-session",
                }
            )
            retry_bound = retry_stage.model_copy(
                update={"execution_identity": identity}
            )
            stage_replay = materialize_dispatch(
                request,
                body(_goal_materialization_stage_provenance(retry_bound)),
            )
            self.assertEqual(stage_replay["execution_identity_digest"], identity.digest)
            replayed = materialize_dispatch(request, body(retry_bound))
            self.assertEqual(replayed["execution_identity_digest"], identity.digest)
            persisted = ledger.get(DISPATCH_ONE)
            assert persisted is not None and persisted.goal_provenance is not None
            self.assertEqual(persisted.goal_provenance.execution_identity, identity)
            self.assertEqual(
                persisted.goal_provenance.action_reservation_id,
                "reservation-b",
            )

            forged_identity = _expected_goal_dispatch_execution_identity(
                retry_stage,
                forged_session_id,
            )
            forged = retry_stage.model_copy(
                update={"execution_identity": forged_identity}
            )
            with self.assertRaises(HTTPException) as mismatch:
                materialize_dispatch(
                    request,
                    body(forged, forged_session_id),
                )
            self.assertEqual(
                mismatch.exception.detail["code"],
                "goal_execution_identity_mismatch",
            )
            persisted = ledger.get(DISPATCH_ONE)
            assert persisted is not None and persisted.goal_provenance is not None
            self.assertEqual(persisted.goal_provenance.execution_identity, identity)

    def test_missing_target_card_waits_for_projection_instead_of_side_loading(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id=TARGET_ID)
            card = Card(id=CARD_ONE, title="Fleet convergence")
            store = MagicMock()
            store.get_card.return_value = None
            log = MagicMock()
            request = request_for(settings, store, {"event_log": log})
            request.state.instance_authenticated = True
            request.headers = {"X-PA-Origin-Instance-ID": AUTHORITY_ID}
            body = DispatchMaterializeBody(
                dispatch_id=DISPATCH_ONE,
                mutation_id=MUTATION_ONE,
                card=card.model_dump(mode="json"),
                card_version=card.updated_at.isoformat(),
                realm_id="default",
                authority_instance_id=AUTHORITY_ID,
                authority_url="http://authority:8080",
                target_instance_id=TARGET_ID,
                progress_versions=[99, 1],
            )

            with self.assertRaises(HTTPException) as raised:
                materialize_dispatch(request, body)

            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(raised.exception.detail["code"], "target_card_not_found")
            self.assertTrue(raised.exception.detail["retry_after_convergence"])
            log.append_event.assert_not_called()
            store.apply_event.assert_not_called()
            self.assertIsNone(DispatchStore(settings.data_dir).get(DISPATCH_ONE))

    def test_stale_target_returns_actionable_409(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id=TARGET_ID)
            target = Card(id=CARD_ONE, title="stale")
            authority = target.model_copy(
                update={"title": "new", "updated_at": datetime.now(UTC)}
            )
            store = MagicMock()
            store.get_card.return_value = target
            request = request_for(settings, store, {"event_log": MagicMock()})
            request.state.instance_authenticated = True
            request.headers = {"X-PA-Origin-Instance-ID": AUTHORITY_ID}
            with self.assertRaises(HTTPException) as raised:
                materialize_dispatch(
                    request,
                    DispatchMaterializeBody(
                        dispatch_id=DISPATCH_ONE,
                        mutation_id=MUTATION_ONE,
                        card=authority.model_dump(mode="json"),
                        card_version=authority.updated_at.isoformat(),
                        realm_id="default",
                        authority_instance_id=AUTHORITY_ID,
                        authority_url="http://authority",
                        target_instance_id=TARGET_ID,
                    ),
                )
            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(raised.exception.detail["code"], "target_sync_conflict")

    def test_versioned_materialization_binds_full_ids_to_authenticated_authority(
        self,
    ) -> None:
        authority_id = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"
        target_id = "2d22a9e1-a1a0-4900-8a8e-8284627aa6bf"
        dispatch_id = "33333333-3333-4333-8333-333333333333"
        mutation_id = "44444444-4444-4444-8444-444444444444"
        card_id = "45cd58e9-1dd7-44b9-9e07-2ae58d12e685"
        project_id = "55555555-5555-4555-8555-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id=target_id)
            card = Card(id=card_id, title="Canonical", project_id=project_id)
            store = MagicMock()
            store.get_card.return_value = card
            store.get_project.return_value = MagicMock()
            request = request_for(settings, store, {"event_log": MagicMock()})
            request.state.instance_authenticated = True

            request.headers = {"X-PA-Origin-Instance-ID": authority_id}
            body = DispatchMaterializeBody(
                dispatch_id=dispatch_id,
                mutation_id=mutation_id,
                card=card.model_dump(mode="json"),
                card_version=card.updated_at.isoformat(),
                realm_id="default",
                project_id=project_id,
                principal_id="user:operator",
                provenance_version=1,
                authority_instance_id=authority_id,
                authority_url="http://authority",
                target_instance_id=target_id,
            )

            legacy_version = body.model_copy(
                update={
                    "dispatch_id": "88888888-8888-4888-8888-888888888888",
                    "provenance_version": 0,
                }
            )
            with self.assertRaises(HTTPException) as unsupported:
                materialize_dispatch(request, legacy_version)
            self.assertEqual(
                unsupported.exception.detail["code"],
                "unsupported_provenance_version",
            )

            result = materialize_dispatch(request, body)

            self.assertTrue(result["resolvable"])
            durable = DispatchStore(settings.data_dir).get(dispatch_id)
            self.assertEqual(durable.dispatch_id, dispatch_id)
            self.assertEqual(durable.card_id, card_id)
            self.assertEqual(durable.project_id, project_id)
            self.assertEqual(durable.authority_instance_id, authority_id)
            self.assertEqual(durable.target_instance_id, target_id)
            self.assertEqual(durable.principal_id, "user:operator")
            self.assertEqual(durable.request_payload["provenance_version"], 1)

            forged = body.model_copy(
                update={"dispatch_id": "66666666-6666-4666-8666-666666666666"}
            )
            request.headers = {
                "X-PA-Origin-Instance-ID": "77777777-7777-4777-8777-777777777777"
            }
            with self.assertRaises(HTTPException) as mismatch:
                materialize_dispatch(request, forged)
            self.assertEqual(
                mismatch.exception.detail["code"], "dispatch_authority_mismatch"
            )

            request.headers = {"X-PA-Origin-Instance-ID": authority_id}
            shortened = body.model_copy(
                update={"dispatch_id": "33333333-3333-43-33333333"}
            )
            with self.assertRaises(HTTPException) as malformed:
                materialize_dispatch(request, shortened)
            self.assertEqual(
                malformed.exception.detail["code"], "malformed_provenance_id"
            )


class CompletionTests(unittest.TestCase):
    def test_completion_report_is_enriched_from_linked_pr_watch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            card = Card(id="card-1", title="reported remotely")
            store = MagicMock()
            store.get_card.return_value = card
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="running",
                )
            )
            watch = PRWatch(
                id="watch-1",
                card_id=card.id,
                repository="petersky/pa",
                pr_number=999,
                pr_url="https://github.com/petersky/pa/pull/999",
                head_sha="b" * 40,
                state={
                    "head_sha": "b" * 40,
                    "checks": [
                        {
                            "name": "test",
                            "status": "completed",
                            "conclusion": "success",
                        }
                    ],
                    "review_threads": [{"path": "src/pa/example.py", "resolved": True}],
                    "merge_commit_sha": "c" * 40,
                },
            )
            supervisor = MagicMock()
            supervisor.list_watches.return_value = [watch]
            request = request_for(
                settings,
                store,
                {
                    "dispatch_store": ledger,
                    "pr_supervisor_store": supervisor,
                },
            )
            request.headers = {"idempotency-key": "mutation-1"}

            complete_dispatch(
                request,
                "dispatch-1",
                DispatchCompletionBody(
                    mutation_id="mutation-1",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    source_instance_id="target",
                    session_id="session-1",
                    final_report=CompletionReportV1(
                        outcome="Ready for integration",
                        commit_sha="a" * 40,
                    ),
                ),
            )

            report = ledger.get("dispatch-1").final_report
            self.assertEqual(report.pr_number, 999)
            self.assertEqual(report.commit_sha, "b" * 40)
            self.assertEqual(report.merge_commit_sha, "c" * 40)
            self.assertEqual(report.ci_evidence, ["test: success"])
            self.assertEqual(report.review_evidence, ["src/pa/example.py: resolved"])
            self.assertEqual(
                ledger.get("dispatch-1").post_turn_evaluations[-1].decision.value,
                "outcome_achieved",
            )

    def test_acknowledged_completion_preserves_concurrent_repair_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ctx = target_terminal_repair_context(tmp, "ack-race")
            card = Card(id="card-ack-race", title="Completion race")
            current = ctx.ledger.get(ctx.record.dispatch_id)
            assert current is not None
            current.card_id = card.id
            current.card_version = card.updated_at.isoformat()
            current.card_snapshot = card.model_dump(mode="json")
            ctx.ledger.put(current)
            domain = MagicMock()
            domain.get_card.return_value = card
            complete_request = request_for(
                ctx.settings, domain, {"dispatch_store": ctx.ledger}
            )
            complete_request.headers = {"idempotency-key": ctx.record.mutation_id}
            body = DispatchCompletionBody(
                mutation_id=ctx.record.mutation_id,
                card_id=card.id,
                card_version=card.updated_at.isoformat(),
                realm_id="default",
                source_instance_id="target",
                session_id=ctx.session.id,
                result={"outcome": "Concurrent immutable completion won."},
            )
            original_mutate = ctx.ledger.mutate_current

            def commit_then_ack(dispatch_id: str, *, mutate):
                prepared = target_terminal_repair_evidence(
                    ctx.request, ctx.record.dispatch_id, ctx.proof_body
                )
                target_terminal_repair_commit(
                    ctx.request,
                    ctx.record.dispatch_id,
                    DispatchTerminalRepairCommitRequest(
                        mutation_id=ctx.record.mutation_id,
                        authority_instance_id="authority",
                        target_instance_id="target",
                        session_id=ctx.session.id,
                        idempotency_key=ctx.proof_body.idempotency_key,
                        reservation_id=prepared.reservation_id,
                        evidence_digest=prepared.evidence_digest,
                        expected_state="running",
                    ),
                )
                return original_mutate(dispatch_id, mutate=mutate)

            ctx.ledger.mutate_current = MagicMock(side_effect=commit_then_ack)
            result = complete_dispatch(complete_request, ctx.record.dispatch_id, body)

            self.assertTrue(result["acknowledged"])
            persisted = ctx.ledger.get(ctx.record.dispatch_id)
            assert persisted is not None
            self.assertEqual(persisted.state, "completed")
            self.assertIsNotNone(persisted.acknowledged_at)
            self.assertEqual(
                persisted.completion_payload,
                {"outcome": "Concurrent immutable completion won."},
            )
            self.assertEqual(
                persisted.terminal_repair_reservation["state"], "committed"
            )
            self.assertEqual(
                persisted.control_operations[ctx.proof_body.idempotency_key],
                "repair_terminal:abandoned_without_acknowledgement",
            )
            kinds = {
                item["kind"] for item in persisted.lifecycle_inconsistencies
            }
            self.assertIn("target_terminal_repair_reservation_committed", kinds)
            self.assertIn("terminal_repair_superseded_by_completion", kinds)

    def test_duplicate_completion_updates_card_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            card = Card(id="card-1", title="done remotely")
            store = MagicMock()
            store.get_card.return_value = card
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="dispatched",
                )
            )
            request = request_for(settings, store, {"dispatch_store": ledger})
            request.headers = {"idempotency-key": "mutation-1"}
            body = DispatchCompletionBody(
                mutation_id="mutation-1",
                card_id=card.id,
                realm_id="default",
                card_version=card.updated_at.isoformat(),
                source_instance_id="target",
                session_id="session-1",
                disposition={
                    "contract": "pa.card-disposition/v1",
                    "lane": "waiting",
                    "outcome": "The agent turn ended with follow-up work remaining.",
                    "evidence": {"integration_required": False},
                },
            )

            first = complete_dispatch(request, "dispatch-1", body)
            second = complete_dispatch(request, "dispatch-1", body)

            self.assertFalse(first["duplicate"])
            self.assertTrue(second["duplicate"])
            store.update_card.assert_called_once()
            self.assertEqual(store.update_card.call_args.args[1].lane, CardLane.WAITING)

    def test_end_turn_accepts_dispatch_transition_without_marking_card_done(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            original = Card(id="card-1", title="remote")
            active = original.model_copy(
                update={"lane": CardLane.ACTIVE, "preferred_instance": "target"}
            )
            store = MagicMock()
            store.get_card.return_value = active
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=original.id,
                    realm_id="default",
                    card_version=original.updated_at.isoformat(),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="dispatched",
                )
            )
            request = request_for(settings, store, {"dispatch_store": ledger})
            request.headers = {"idempotency-key": "mutation-1"}
            result = complete_dispatch(
                request,
                "dispatch-1",
                DispatchCompletionBody(
                    mutation_id="mutation-1",
                    card_id=original.id,
                    realm_id="default",
                    card_version=original.updated_at.isoformat(),
                    source_instance_id="target",
                    session_id="session-1",
                    result={"stop_reason": "end_turn"},
                ),
            )
            self.assertTrue(result["acknowledged"])
            self.assertEqual(result["card_disposition"]["status"], "absent")
            self.assertEqual(result["card_disposition"]["lane_after"], "active")
            store.update_card.assert_not_called()
            persisted = ledger.get("dispatch-1")
            self.assertEqual(len(persisted.turn_end_snapshots), 1)
            self.assertEqual(
                persisted.turn_end_snapshots[0].final_outcome_text,
                "Agent turn ended.",
            )
            self.assertEqual(
                persisted.post_turn_evaluations[0].decision.value,
                "unable_to_determine",
            )
            reloaded = DispatchStore(settings.data_dir).get("dispatch-1")
            self.assertEqual(
                reloaded.turn_end_snapshots[0].contract,
                "pa.turn-end-snapshot/v1",
            )
            self.assertEqual(
                reloaded.post_turn_evaluations[0].contract,
                "pa.post-turn-evaluation/v1",
            )

    def test_disposition_extraction_error_survives_authority_audit_and_public_ui(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            card = Card(id="card-1", title="remote")
            store = MagicMock()
            store.get_card.return_value = card
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="running",
                )
            )
            request = request_for(settings, store, {"dispatch_store": ledger})
            request.headers = {"idempotency-key": "mutation-1"}
            exact_error = (
                "The final response was not exactly one JSON object: "
                "Expecting value: line 1 column 1 (char 0)"
            )

            complete_dispatch(
                request,
                "dispatch-1",
                DispatchCompletionBody(
                    mutation_id="mutation-1",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    source_instance_id="target",
                    session_id="session-1",
                    result={"card_disposition_error": exact_error},
                ),
            )

            persisted = ledger.get("dispatch-1")
            self.assertEqual(persisted.card_disposition_error, exact_error)
            self.assertEqual(
                persisted.turn_end_snapshots[0].disposition_parse_error,
                exact_error,
            )
            self.assertEqual(
                persisted.public_dict()["card_reconciliation"][
                    "disposition_error"
                ],
                exact_error,
            )

    def test_unrelated_card_edit_does_not_block_completion_or_lane(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            original = Card(id="card-1", title="original")
            changed = original.model_copy(
                update={"title": "operator edit", "updated_at": datetime.now(UTC)}
            )
            store = MagicMock()
            store.get_card.return_value = changed
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=original.id,
                    realm_id="default",
                    card_version=original.updated_at.isoformat(),
                    card_snapshot=original.model_dump(mode="json"),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="running",
                )
            )
            request = request_for(settings, store, {"dispatch_store": ledger})
            request.headers = {"idempotency-key": "mutation-1"}

            result = complete_dispatch(
                request,
                "dispatch-1",
                DispatchCompletionBody(
                    mutation_id="mutation-1",
                    card_id=original.id,
                    realm_id="default",
                    card_version=original.updated_at.isoformat(),
                    source_instance_id="target",
                    session_id="session-1",
                    result={"stop_reason": "end_turn"},
                    disposition={
                        "contract": "pa.card-disposition/v1",
                        "lane": "waiting",
                        "outcome": "Ready for review",
                        "evidence": {"integration_required": False},
                    },
                ),
            )

            self.assertTrue(result["acknowledged"])
            self.assertEqual(result["reconciliation"]["state"], "applied")
            self.assertEqual(ledger.get("dispatch-1").state, "completed")
            self.assertEqual(store.update_card.call_args.args[1].lane, CardLane.WAITING)

    def test_conflicting_operator_lane_is_preserved_after_ack(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            original = Card(id="card-1", title="original", lane=CardLane.ACTIVE)
            changed = original.model_copy(
                update={"lane": CardLane.WAITING, "updated_at": datetime.now(UTC)}
            )
            store = MagicMock()
            store.get_card.return_value = changed
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=original.id,
                    realm_id="default",
                    card_version=original.updated_at.isoformat(),
                    card_snapshot=original.model_dump(mode="json"),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="running",
                )
            )
            request = request_for(settings, store, {"dispatch_store": ledger})
            request.headers = {"idempotency-key": "mutation-1"}
            result = complete_dispatch(
                request,
                "dispatch-1",
                DispatchCompletionBody(
                    mutation_id="mutation-1",
                    card_id=original.id,
                    realm_id="default",
                    card_version=original.updated_at.isoformat(),
                    source_instance_id="target",
                    session_id="session-1",
                    disposition={
                        "contract": "pa.card-disposition/v1",
                        "lane": "done",
                        "outcome": "done",
                        "evidence": {"integration_required": False},
                    },
                ),
            )
            self.assertTrue(result["acknowledged"])
            self.assertEqual(
                result["reconciliation"]["state"], "conflict_requires_resolution"
            )
            self.assertEqual(ledger.get("dispatch-1").state, "completed")
            store.update_card.assert_not_called()

    def test_completion_from_wrong_target_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            card = Card(id="card-1", title="target separation")
            store = MagicMock()
            store.get_card.return_value = card
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target-a",
                    session_id="session-1",
                    state="running",
                )
            )
            request = request_for(settings, store, {"dispatch_store": ledger})
            request.headers = {"idempotency-key": "mutation-1"}

            with self.assertRaises(HTTPException) as raised:
                complete_dispatch(
                    request,
                    "dispatch-1",
                    DispatchCompletionBody(
                        mutation_id="mutation-1",
                        card_id=card.id,
                        realm_id="default",
                        card_version=card.updated_at.isoformat(),
                        source_instance_id="target-b",
                        session_id="session-1",
                    ),
                )

            self.assertEqual(
                raised.exception.detail["code"], "completion_dispatch_mismatch"
            )
            store.update_card.assert_not_called()

    def test_end_turn_done_with_open_pr_is_downgraded_to_waiting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            card = Card(id="card-1", title="open PR", lane=CardLane.ACTIVE)
            store = MagicMock()
            store.get_card.return_value = card
            watch = PRWatch(
                id="watch-1",
                card_id=card.id,
                repository="owner/repo",
                pr_number=17,
                pr_url="https://github.com/owner/repo/pull/17",
                head_sha="a" * 40,
                state={"state": "open", "mergeable_state": "clean"},
            )
            supervisor = MagicMock()
            supervisor.list_watches.return_value = [watch]
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="running",
                )
            )
            request = request_for(
                settings,
                store,
                {"dispatch_store": ledger, "pr_supervisor_store": supervisor},
            )
            request.headers = {"idempotency-key": "mutation-1"}

            result = complete_dispatch(
                request,
                "dispatch-1",
                DispatchCompletionBody(
                    mutation_id="mutation-1",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    source_instance_id="target",
                    session_id="session-1",
                    result={"stop_reason": "end_turn"},
                    disposition={
                        "contract": "pa.card-disposition/v1",
                        "lane": "done",
                        "outcome": "The turn ended.",
                        "evidence": {
                            "integration_required": True,
                            "pr_watch_id": watch.id,
                            "watched_head_sha": watch.head_sha,
                            "merge_commit_sha": "b" * 40,
                        },
                    },
                ),
            )

            self.assertEqual(result["card_disposition"]["status"], "downgraded")
            self.assertEqual(result["card_disposition"]["lane_after"], "waiting")
            self.assertEqual(store.update_card.call_args.args[1].lane, CardLane.WAITING)
            self.assertIn(
                "not merged", ledger.get("dispatch-1").card_disposition_reason
            )


class RetryAndConflictTests(unittest.IsolatedAsyncioTestCase):
    async def test_authority_unavailable_keeps_completion_pending_for_restart(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
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
            )
            outbox = CompletionOutbox(ledger, "secret", retry_seconds=0.01)
            outbox.queue("session-1", {"stop_reason": "end_turn"})
            with patch("pa.execution.dispatch.httpx.AsyncClient") as client:
                client.return_value.post = AsyncMock(
                    side_effect=httpx.ConnectError("offline")
                )
                await outbox._send(ledger.get("dispatch-1"))

            reloaded = DispatchStore(Path(tmp)).get("dispatch-1")
            self.assertEqual(reloaded.state, "completion_pending")
            self.assertEqual(reloaded.attempts, 1)
            self.assertIn("offline", reloaded.last_error)

    async def test_stable_semantic_conflict_stops_transport_retries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
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
            )
            outbox = CompletionOutbox(ledger, "secret", retry_seconds=0.01)
            outbox.queue("session-1", {"stop_reason": "end_turn"})
            response = MagicMock(status_code=409, text="authority_version_conflict")
            response.json.return_value = {
                "detail": {"code": "authority_version_conflict"}
            }
            with patch("pa.execution.dispatch.httpx.AsyncClient") as client:
                client.return_value.post = AsyncMock(return_value=response)
                await outbox._send(ledger.get("dispatch-1"))
                self.assertEqual(client.return_value.post.await_count, 1)

            reloaded = DispatchStore(Path(tmp)).get("dispatch-1")
            self.assertEqual(reloaded.state, "completed")
            self.assertEqual(reloaded.attempts, 1)
            self.assertEqual(reloaded.completion_delivery_class, "semantic_conflict")
            self.assertEqual(
                reloaded.reconciliation_state, "conflict_requires_resolution"
            )
            self.assertIsNone(reloaded.completion_next_retry_at)

    async def test_transport_retry_is_bounded_and_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="running",
                )
            )
            outbox = CompletionOutbox(ledger, "", retry_seconds=2, max_attempts=1)
            outbox.queue("session-1", {})
            with patch("pa.execution.dispatch.httpx.AsyncClient") as client:
                client.return_value.post = AsyncMock(
                    side_effect=httpx.ConnectError("offline")
                )
                await outbox._send(ledger.get("dispatch-1"))
            reloaded = DispatchStore(Path(tmp)).get("dispatch-1")
            self.assertEqual(reloaded.completion_delivery_class, "transport_exhausted")
            self.assertEqual(reloaded.attempts, 1)
            self.assertIsNone(reloaded.completion_next_retry_at)

    async def test_completion_uses_latest_running_dispatch_for_resumed_session(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            old = DispatchRecord(
                dispatch_id="old",
                mutation_id="old-mutation",
                card_id="old-card",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-1",
                state="completed",
            )
            ledger.put(old)
            current = DispatchRecord(
                dispatch_id="current",
                mutation_id="current-mutation",
                card_id="current-card",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-1",
                state="running",
            )
            ledger.put(current)
            outbox = CompletionOutbox(ledger, "", retry_seconds=60)

            self.assertTrue(outbox.queue("session-1", {"stop_reason": "end_turn"}))
            self.assertEqual(
                ledger.get(current.dispatch_id).state, "completion_pending"
            )
            self.assertEqual(old.state, "completed")

    async def test_unacknowledged_session_cannot_enqueue_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-1",
                state="starting_session",
            )
            ledger.put(record)
            outbox = CompletionOutbox(ledger, "", retry_seconds=60)
            self.assertFalse(outbox.queue("session-1", {}))
            self.assertEqual(record.state, "starting_session")

    async def test_dispatch_is_blocked_when_two_peer_heads_diverge(self) -> None:
        settings = Settings(
            instance_id="authority",
            peers=["http://peer-a", "http://peer-b"],
            sync_token="secret",
        )
        log = MagicMock()
        log.get_head.return_value = "head-local"
        request = request_for(settings, MagicMock(), {"event_log": log})
        responses = []
        for head in ("head-a", "head-b"):
            response = MagicMock()
            response.json.return_value = [{"realm_id": "default", "head_hash": head}]
            responses.append(response)
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=responses
            )
            with self.assertRaises(HTTPException) as raised:
                await _assert_dispatch_sync_health(request, "default")
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.detail["code"], "sync_conflict")

    async def test_unreachable_peer_blocks_dispatch_instead_of_hiding_divergence(
        self,
    ) -> None:
        settings = Settings(
            instance_id="authority",
            peers=["http://peer-a"],
            sync_token="secret",
        )
        log = MagicMock()
        log.get_head.return_value = "head-local"
        request = request_for(settings, MagicMock(), {"event_log": log})
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=httpx.ConnectError("offline")
            )
            with self.assertRaises(HTTPException) as raised:
                await _assert_dispatch_sync_health(request, "default")
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.detail["code"], "sync_unavailable")


class ScopedDispatchHealthTests(unittest.IsolatedAsyncioTestCase):
    def _request(self, *, projection_head="shared-head"):
        settings = Settings(
            instance_id=AUTHORITY_ID,
            peers=["http://target:8080", "http://observer:8080"],
            sync_token="secret",
        )
        store = MagicMock()
        store.get_projection_head.return_value = projection_head
        log = MagicMock()
        log.get_head.return_value = "shared-head"
        fleet = MagicMock()
        fleet.list_instances.return_value = [
            FleetInstance(
                instance_id=TARGET_ID, name="target", url="http://target:8080"
            )
        ]
        return request_for(settings, store, {"event_log": log, "fleet_registry": fleet})

    async def test_unrelated_offline_peer_is_recorded_without_blocking_target(
        self,
    ) -> None:
        request = self._request()
        target = MagicMock()
        target.json.return_value = [{"realm_id": "default", "head_hash": "shared-head"}]
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=[target, httpx.ConnectError("observer offline")]
            )
            evidence = await _assert_dispatch_sync_health(request, "default", TARGET_ID)
        self.assertEqual(evidence["code"], "unrelated_peers_degraded")
        self.assertTrue(evidence["safe_scoped_dispatch"])
        self.assertEqual(evidence["target_head"], "shared-head")
        self.assertEqual(evidence["degraded_peers"][0]["status"], "unavailable")

    async def test_local_target_uses_authenticated_local_heads_without_self_probe(
        self,
    ) -> None:
        request = self._request()
        request.app.state.ctx.settings.instance_id = TARGET_ID
        observer = MagicMock()
        observer.json.return_value = [
            {
                "realm_id": "default",
                "head_hash": "shared-head",
                "projection_head": "shared-head",
            }
        ]
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            get = client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=observer
            )
            evidence = await _assert_dispatch_sync_health(
                request, "default", TARGET_ID
            )

        self.assertEqual(evidence["code"], "scoped_sync_healthy")
        self.assertEqual(evidence["target_head"], "shared-head")
        self.assertEqual(evidence["target_projection_head"], "shared-head")
        self.assertEqual(get.await_count, 1)
        self.assertIn("http://observer:8080/api/sync/refs", get.await_args.args[0])

    async def test_selected_target_offline_blocks_recoverably(self) -> None:
        request = self._request()
        observer = MagicMock()
        observer.json.return_value = [
            {"realm_id": "default", "head_hash": "shared-head"}
        ]
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=[httpx.ConnectError("target offline"), observer]
            )
            with self.assertRaises(HTTPException) as raised:
                await _assert_dispatch_sync_health(request, "default", TARGET_ID)
        self.assertEqual(raised.exception.detail["code"], "target_unavailable")
        self.assertTrue(raised.exception.detail["recoverable"])

    async def test_stale_authority_projection_blocks_before_peer_probe(self) -> None:
        request = self._request(projection_head="stale-head")
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            with self.assertRaises(HTTPException) as raised:
                await _assert_dispatch_sync_health(request, "default", TARGET_ID)
        self.assertEqual(raised.exception.detail["code"], "authority_projection_stale")
        client.assert_not_called()

    async def test_divergent_selected_target_blocks_as_sync_conflict(self) -> None:
        request = self._request()
        target = MagicMock()
        target.json.return_value = [{"realm_id": "default", "head_hash": "target-head"}]
        observer = MagicMock()
        observer.json.return_value = [
            {"realm_id": "default", "head_hash": "shared-head"}
        ]
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=[target, observer]
            )
            with self.assertRaises(HTTPException) as raised:
                await _assert_dispatch_sync_health(request, "default", TARGET_ID)
        self.assertEqual(
            raised.exception.detail["code"], "target_projection_not_ready"
        )
        self.assertEqual(raised.exception.status_code, 503)

    async def test_current_durable_head_with_stale_projection_is_not_ready(self) -> None:
        request = self._request()
        target = MagicMock()
        target.json.return_value = [
            {
                "realm_id": "default",
                "head_hash": "shared-head",
                "projection_head": "previous-head",
            }
        ]
        observer = MagicMock()
        observer.json.return_value = [
            {"realm_id": "default", "head_hash": "shared-head"}
        ]
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=[target, observer]
            )
            with self.assertRaises(HTTPException) as raised:
                await _assert_dispatch_sync_health(request, "default", TARGET_ID)
        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(
            raised.exception.detail["code"], "target_projection_not_ready"
        )
        self.assertEqual(raised.exception.headers["Retry-After"], "1")

    async def test_dispatch_waits_for_recoverable_projection_lag(self) -> None:
        lag = HTTPException(
            status_code=503,
            detail={
                "code": "target_projection_not_ready",
                "recoverable": True,
            },
            headers={"Retry-After": "1"},
        )
        evidence = {"code": "scoped_sync_healthy"}
        with (
            patch(
                "pa.modules.fleet._assert_dispatch_sync_health",
                AsyncMock(side_effect=[lag, lag, evidence]),
            ) as check,
            patch("pa.modules.fleet.asyncio.sleep", AsyncMock()) as sleep,
        ):
            result = await _wait_for_dispatch_sync_health(
                self._request(), "default", TARGET_ID
            )
        self.assertEqual(result, evidence)
        self.assertEqual(check.await_count, 3)
        self.assertEqual(sleep.await_count, 2)

    async def test_dispatch_does_not_wait_for_non_sync_failure(self) -> None:
        unavailable = HTTPException(
            status_code=409,
            detail={"code": "target_unavailable", "recoverable": True},
        )
        with (
            patch(
                "pa.modules.fleet._assert_dispatch_sync_health",
                AsyncMock(side_effect=unavailable),
            ) as check,
            patch("pa.modules.fleet.asyncio.sleep", AsyncMock()) as sleep,
            self.assertRaises(HTTPException),
        ):
            await _wait_for_dispatch_sync_health(self._request(), "default", TARGET_ID)
        self.assertEqual(check.await_count, 1)
        sleep.assert_not_awaited()


class DurableDispatchJobTests(unittest.IsolatedAsyncioTestCase):
    def _job_app(self, root: Path):
        settings = Settings(
            data_dir=root,
            instance_id="authority",
            instance_name="authority",
            instance_url="http://authority:8080",
            sync_token="secret",
        )
        ledger = DispatchStore(root)
        fleet = MagicMock()
        fleet.list_instances.return_value = [
            MagicMock(instance_id="target", name="target", url="http://target:8080")
        ]
        domain = MagicMock()
        ctx = MagicMock(settings=settings, store=domain)
        ctx.services = {"dispatch_store": ledger, "fleet_registry": fleet}
        ctx.require_service.side_effect = lambda name: ctx.services[name]
        app = MagicMock()
        app.state.ctx = ctx
        return app, ledger, domain

    def _record(self, **updates) -> DispatchRecord:
        values = {
            "dispatch_id": "dispatch-1",
            "mutation_id": "mutation-1",
            "idempotency_key": "browser-1",
            "request_fingerprint": "fingerprint-1",
            "request_payload": {"message": "Do the work"},
            "authority_instance_id": "authority",
            "authority_url": "http://authority:8080",
            "target_instance_id": "target",
        }
        values.update(updates)
        return DispatchRecord(**values)

    async def test_worker_records_every_stage_and_requires_delivery_ack(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, domain = self._job_app(Path(tmp))
            record = self._record()
            ledger.transition(record, "queued", "admitted")
            ack = {
                "accepted": True,
                "accepted_event": "queue_enqueued",
                "session_id": "session-new",
                "dispatch_id": record.dispatch_id,
                "prompt_id": "prompt-1",
            }
            peer_agent = AsyncMock(
                side_effect=[{"session": {"id": "session-new"}}, ack]
            )
            with (
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(return_value={"resolvable": True}),
                ),
                patch("pa.modules.fleet._peer_agent_json", peer_agent),
            ):
                await _process_remote_dispatch(app, record)

            self.assertEqual(record.state, "running")
            self.assertEqual(record.session_id, "session-new")
            self.assertIsNotNone(record.prompt_acknowledged_at)
            states = [event.state for event in record.events]
            self.assertEqual(
                states,
                [
                    "queued",
                    "checking_sync",
                    "materializing",
                    "provisioning",
                    "starting_session",
                    "delivering_prompt",
                    "running",
                ],
            )
            create_body = peer_agent.await_args_list[0].kwargs["body"]
            self.assertEqual(create_body["label"], f"dispatch:{record.dispatch_id}")
            self.assertNotEqual(create_body["label"], "card:card-1")
            domain.add_knowledge.assert_called_once()

    async def test_remote_proxy_rejects_unconfirmed_config_before_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, domain = self._job_app(Path(tmp))
            record = self._record(
                request_payload={
                    "message": "Do the work",
                    "model_id": "gpt-5.6-sol[high]",
                }
            )
            ledger.transition(record, "queued", "admitted")
            peer_agent = AsyncMock(
                return_value={
                    "session": {"id": "session-new"},
                    "configuration": {
                        "state": "ready",
                        "effective": {
                            "model_id": "provider-default",
                            "reasoning": "medium",
                        },
                    },
                }
            )
            with (
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(return_value={"resolvable": True}),
                ),
                patch("pa.modules.fleet._peer_agent_json", peer_agent),
            ):
                with self.assertRaises(HTTPException) as raised:
                    await _process_remote_dispatch(app, record)

            self.assertEqual(raised.exception.status_code, 502)
            self.assertEqual(
                raised.exception.detail["code"],
                "remote_configuration_unconfirmed",
            )
            self.assertEqual(peer_agent.await_count, 1)
            domain.add_knowledge.assert_not_called()

    async def test_missing_prompt_ack_is_retryable_and_keeps_exact_session(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record()
            ledger.transition(record, "queued", "admitted")
            peer_agent = AsyncMock(
                side_effect=[
                    {"session": {"id": "session-new"}},
                    {"started": True, "session_id": "session-new"},
                ]
            )
            worker = DispatchWorker(
                ledger, lambda item: _process_remote_dispatch(app, item)
            )
            with (
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(return_value={"resolvable": True}),
                ),
                patch("pa.modules.fleet._peer_agent_json", peer_agent),
            ):
                await worker._execute(record)

            self.assertEqual(record.state, "failed")
            self.assertEqual(record.error_code, "prompt_ack_missing")
            self.assertEqual(record.session_id, "session-new")
            self.assertTrue(record.recoverable)

    async def test_materialization_409_is_audited_without_starting_session(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record(card_id="card-1", card_version="v1")
            ledger.transition(record, "queued", "admitted")
            app.state.ctx.store.get_card.return_value = MagicMock(
                id="card-1",
                title="Card",
                project_id=None,
                updated_at=MagicMock(isoformat=MagicMock(return_value="v1")),
                model_dump=MagicMock(return_value={"id": "card-1"}),
            )
            conflict = HTTPException(
                status_code=409,
                detail={
                    "code": "stale_target_card",
                    "message": "Target has a different card version.",
                    "recoverable": True,
                },
            )
            worker = DispatchWorker(
                ledger, lambda item: _process_remote_dispatch(app, item)
            )
            with (
                patch("pa.modules.fleet._assert_dispatch_sync_health", AsyncMock()),
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(side_effect=conflict),
                ),
                patch("pa.modules.fleet._peer_agent_json", AsyncMock()) as peer,
            ):
                await worker._execute(record)

            self.assertEqual(record.state, "failed")
            self.assertEqual(record.error_code, "stale_target_card")
            peer.assert_not_awaited()

    async def test_provider_timeout_is_background_failure_not_admission_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record()
            ledger.transition(record, "queued", "admitted")
            worker = DispatchWorker(
                ledger, lambda item: _process_remote_dispatch(app, item)
            )
            timeout = HTTPException(
                status_code=502,
                detail={
                    "code": "provider_timeout",
                    "message": "Provider startup timed out.",
                    "recoverable": True,
                },
            )
            with (
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(return_value={"resolvable": True}),
                ),
                patch(
                    "pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=timeout)
                ),
            ):
                await worker._execute(record)
            self.assertEqual(record.state, "failed")
            self.assertEqual(record.error_code, "provider_timeout")

    async def test_retry_and_cancel_preserve_dispatch_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record(state="failed", last_error="offline")
            ledger.put(record)
            request = MagicMock()
            request.app = app
            with patch("pa.modules.fleet.require_user"):
                retried = retry_dispatch(request, record.dispatch_id)
                self.assertEqual(retried["state"], "queued")
                cancelled = cancel_dispatch(request, record.dispatch_id)
            self.assertEqual(cancelled["state"], "cancelled")
            self.assertEqual(cancelled["dispatch_id"], "dispatch-1")

    async def test_retry_and_cancel_are_idempotent_by_control_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record(state="failed", last_error="offline")
            ledger.put(record)
            request = MagicMock()
            request.app = app
            request.headers = {}
            with patch("pa.modules.fleet.require_user"):
                first_retry = retry_dispatch(
                    request,
                    record.dispatch_id,
                    DispatchControlBody(idempotency_key="retry-1"),
                )
                repeated_retry = retry_dispatch(
                    request,
                    record.dispatch_id,
                    DispatchControlBody(idempotency_key="retry-1"),
                )
                first_cancel = cancel_dispatch(
                    request,
                    record.dispatch_id,
                    DispatchControlBody(idempotency_key="cancel-1"),
                )
                repeated_cancel = cancel_dispatch(
                    request,
                    record.dispatch_id,
                    DispatchControlBody(idempotency_key="cancel-1"),
                )

            self.assertEqual(first_retry["state"], "queued")
            self.assertEqual(repeated_retry["state"], "queued")
            self.assertEqual(first_cancel["state"], "cancelled")
            self.assertEqual(repeated_cancel["state"], "cancelled")
            persisted = DispatchStore(Path(tmp)).get(record.dispatch_id)
            self.assertEqual(
                persisted.control_operations,
                {"retry-1": "retry", "cancel-1": "cancel"},
            )

    async def test_control_key_cannot_be_reused_for_another_operation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record(state="failed", last_error="offline")
            ledger.put(record)
            request = MagicMock()
            request.app = app
            request.headers = {}
            with patch("pa.modules.fleet.require_user"):
                retry_dispatch(
                    request,
                    record.dispatch_id,
                    DispatchControlBody(idempotency_key="operation-1"),
                )
                with self.assertRaises(HTTPException) as raised:
                    cancel_dispatch(
                        request,
                        record.dispatch_id,
                        DispatchControlBody(idempotency_key="operation-1"),
                    )
            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(raised.exception.detail["code"], "idempotency_conflict")


class DispatchRestartTests(unittest.TestCase):
    def test_restart_requeues_interrupted_job_with_same_session_and_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = DispatchStore(root)
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                idempotency_key="browser-1",
                request_payload={"message": "work"},
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-1",
                state="delivering_prompt",
            )
            ledger.put(record)

            reloaded = DispatchStore(root)
            reloaded.reconcile_interrupted()
            recovered = reloaded.get("dispatch-1")

            self.assertEqual(recovered.state, "queued")
            self.assertEqual(recovered.session_id, "session-1")
            self.assertEqual(recovered.mutation_id, "mutation-1")
            self.assertEqual(
                recovered.events[-1].detail["previous_state"], "delivering_prompt"
            )

    def test_legacy_orphan_expires_with_actionable_retry_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = DispatchStore(root)
            ledger.put(
                DispatchRecord(
                    dispatch_id="orphan-1",
                    mutation_id="mutation-1",
                    card_id="card-1",
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    state="dispatching",
                )
            )
            ledger.reconcile_interrupted()
            orphan = ledger.get("orphan-1")
            self.assertEqual(orphan.state, "failed")
            self.assertEqual(orphan.error_code, "orphaned_legacy_dispatch")
            self.assertIn("retry", orphan.last_error.lower())


class EventLogMergeTests(unittest.TestCase):
    def test_compatible_heads_produce_same_deterministic_merge_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            objects = ObjectStore(Path(tmp) / "objects")
            log = EventLog(objects, Path(tmp), "node-a")
            other = EventLog(objects, Path(tmp) / "other", "node-b")
            # Parent hashes are sufficient for proving deterministic merge encoding.
            first = log.merge_heads("default", "b" * 64, "a" * 64, "ignored")
            second = other.merge_heads("default", "a" * 64, "b" * 64, "other")
            self.assertEqual(first.hash, second.hash)

    def test_metadata_resolution_is_deterministic_for_reversed_heads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            left = EventLog(objects, root / "left", "left")
            right = EventLog(objects, root / "right", "right")
            first_merger = EventLog(objects, root / "first", "first")
            second_merger = EventLog(objects, root / "second", "second")
            _, base = left.append_event(
                CardEvent(
                    type=EventType.CARD_CREATED,
                    realm_id="default",
                    card_id="card-1",
                    author_principal="test",
                    author_instance="left",
                    payload=Card(id="card-1", title="base").model_dump(mode="json"),
                )
            )
            right.advance_ref("default", base.hash)
            _, left_head = left.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="card-1",
                    author_principal="test",
                    author_instance="left",
                    payload={
                        "title": "left",
                        "updated_at": "2026-07-24T12:00:00+00:00",
                    },
                )
            )
            _, right_head = right.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="card-1",
                    author_principal="test",
                    author_instance="right",
                    payload={
                        "body": "right",
                        "updated_at": "2026-07-24T13:00:00Z",
                    },
                )
            )
            compatible, first_health = first_merger.compatible_histories(
                left_head.hash, right_head.hash
            )
            reverse_compatible, second_health = second_merger.compatible_histories(
                right_head.hash, left_head.hash
            )
            self.assertTrue(compatible, first_health)
            self.assertTrue(reverse_compatible, second_health)
            self.assertEqual(
                first_health["automatic_resolutions"][0]["value"],
                "2026-07-24T13:00:00Z",
            )
            first = first_merger.merge_heads(
                "default",
                left_head.hash,
                right_head.hash,
                "sync:auto",
                automatic_resolutions=first_health["automatic_resolutions"],
            )
            second = second_merger.merge_heads(
                "default",
                right_head.hash,
                left_head.hash,
                "sync:auto",
                automatic_resolutions=second_health["automatic_resolutions"],
            )
            self.assertEqual(first.hash, second.hash)

    def test_three_instance_disjoint_histories_converge_without_operator_conflict(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            for name in ("left", "right", "observer"):
                (root / name).mkdir()
            left = EventLog(objects, root / "left", "left")
            right = EventLog(objects, root / "right", "right")
            observer = EventLog(objects, root / "observer", "observer")
            base_event = CardEvent(
                type=EventType.CARD_CREATED,
                realm_id="default",
                card_id="base",
                author_principal="test",
                author_instance="left",
                payload=Card(id="base", title="base").model_dump(mode="json"),
            )
            _, base = left.append_event(base_event)
            right.advance_ref("default", base.hash)
            _, left_head = left.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="left-card",
                    author_principal="test",
                    author_instance="left",
                    payload={"title": "left"},
                )
            )
            _, right_head = right.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="right-card",
                    author_principal="test",
                    author_instance="right",
                    payload={"title": "right"},
                )
            )
            compatible, health = observer.compatible_histories(
                left_head.hash, right_head.hash
            )
            self.assertTrue(compatible, health)
            merged = observer.merge_heads(
                "default", left_head.hash, right_head.hash, "sync:auto"
            )
            seen: list[str] = []
            observer.apply_commit_chain(
                merged.hash, lambda event: seen.append(event.card_id or "merge")
            )
            self.assertIn("left-card", seen)
            self.assertIn("right-card", seen)

    def test_delete_and_concurrent_edit_require_operator_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            for name in ("left", "right"):
                (root / name).mkdir()
            left = EventLog(objects, root / "left", "left")
            right = EventLog(objects, root / "right", "right")
            _, base = left.append_event(
                CardEvent(
                    type=EventType.CARD_CREATED,
                    realm_id="default",
                    card_id="card-1",
                    author_principal="test",
                    author_instance="left",
                    payload=Card(id="card-1", title="base").model_dump(mode="json"),
                )
            )
            right.advance_ref("default", base.hash)
            _, deleted = left.append_event(
                CardEvent(
                    type=EventType.CARD_DELETED,
                    realm_id="default",
                    card_id="card-1",
                    author_principal="test",
                    author_instance="left",
                )
            )
            _, edited = right.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="card-1",
                    author_principal="test",
                    author_instance="right",
                    payload={"title": "edited"},
                )
            )
            compatible, health = left.compatible_histories(deleted.hash, edited.hash)
            self.assertFalse(compatible)
            self.assertEqual(health["conflicts"][0]["field"], "__terminal__")


class BoundedDrainTests(unittest.IsolatedAsyncioTestCase):
    async def test_outbox_shutdown_is_bounded_with_pending_stream_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = CompletionOutbox(DispatchStore(Path(tmp)), "", retry_seconds=60)
            outbox.start()
            await asyncio.wait_for(outbox.close(timeout=0.01), timeout=1.5)
