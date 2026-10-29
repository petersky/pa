"""Compaction changes only expired progress, with receipt and commit parity."""
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.post_turn import (
    FollowupActionName, FollowupActionV1, PostTurnDecision,
    PostTurnEvaluationV1, SafetyClassification,
)
from pa.execution.progress import (
    CompletionReportV1, DispatchProgressEventV1, ProgressPhase,
)


def completed(store, *, progress=False):
    record = DispatchRecord(
        dispatch_id="dispatch", mutation_id="mutation", idempotency_key="admit",
        card_id="card", session_id="session", authority_instance_id="authority",
        authority_url="https://example.invalid", target_instance_id="target",
        state="running", progress_protocol_version=1,
        completion_payload={"proof": "x" * 65536},
    )
    store.put(record)
    if progress:
        event = DispatchProgressEventV1(
            card_id="card", dispatch_id="dispatch", acp_session_id="session",
            originating_instance_id="target", authority_instance_id="authority",
            sequence=1, idempotency_key="progress", phase=ProgressPhase.IMPLEMENTING,
            summary="Retained progress",
        )
        store.ingest_progress(event)
        store.mark_progress_delivered("dispatch", "progress")
        record = store.get("dispatch")
    record.state = "completed"
    record.acknowledged_at = datetime.now(UTC)
    record.final_report = CompletionReportV1(outcome="Completed")
    record.post_turn_evaluations = [PostTurnEvaluationV1(
        snapshot_id="snapshot", context_digest="a" * 64,
        decision=PostTurnDecision.OUTCOME_ACHIEVED, rationale="Completed fixture",
        confidence=1.0, operator_status_text="Completed",
        recommended_actions=[FollowupActionV1(
            name=FollowupActionName.NO_ACTION, parameters={"reason": "Completed"},
            idempotency_key_inputs=["dispatch"], safety=SafetyClassification.RECORD_ONLY,
            human_approval_required=False,
        )],
    )]
    store.put(record)
    return record


def test_unchanged_compaction_keeps_record_and_durable_payload(tmp_path):
    store = DispatchStore(tmp_path)
    completed(store, progress=True)
    original = store._records["dispatch"]
    payload = store._conn.execute("SELECT payload_json FROM dispatches").fetchone()[0]
    changes = store._conn.total_changes
    with patch.object(store, "_snapshot", side_effect=AssertionError("copied unchanged record")), patch.object(
        store, "_persist_record_conn", side_effect=AssertionError("rewrote unchanged record")
    ):
        assert store.compact() == {"events": 0, "receipts": 0}
    assert store._records["dispatch"] is original
    assert store._conn.total_changes == changes
    assert store._conn.execute("SELECT payload_json FROM dispatches").fetchone()[0] == payload


@pytest.mark.parametrize("boundary,committed", [("commit_before", False), ("commit_after", True)])
def test_expiring_progress_publication_tracks_actual_commit(tmp_path, boundary, committed):
    store = DispatchStore(tmp_path)
    completed(store, progress=True)
    original = store._records["dispatch"]

    def fail(observed):
        if observed == boundary:
            raise RuntimeError("private commit-boundary failure")

    store._fault_injector = fail
    with pytest.raises(RuntimeError, match="private commit-boundary failure"):
        store.compact(now=datetime.now(UTC) + timedelta(days=60))
    store._fault_injector = None
    current = store.get("dispatch")
    assert len(current.progress_events) == int(not committed)
    assert current.progress_compacted_ranges == ([[1, 1]] if committed else [])
    assert len(original.progress_events) == 1
    assert original.progress_compacted_ranges == []
    store.close()
    reopened = DispatchStore(tmp_path)
    durable = reopened.get("dispatch")
    assert durable.progress_events == current.progress_events
    assert durable.progress_compacted_ranges == current.progress_compacted_ranges


def test_receipt_only_expiry_does_not_copy_or_rewrite_record(tmp_path):
    store = DispatchStore(tmp_path)
    record = completed(store)
    # A control receipt has no retained progress payload to protect its key.
    with store._transaction() as conn:
        store._persist_control_receipt_conn(
            conn, record, idempotency_key="old-control", operation="fixture", payload={},
        )
    original = store._records["dispatch"]
    with patch.object(store, "_snapshot", side_effect=AssertionError("copied receipt-only record")), patch.object(
        store, "_persist_record_conn", side_effect=AssertionError("rewrote receipt-only record")
    ):
        assert store.compact(now=datetime.now(UTC) + timedelta(days=60)) == {"events": 0, "receipts": 1}
    assert store._records["dispatch"] is original
    assert store._conn.execute("SELECT COUNT(*) FROM dispatch_receipts").fetchone()[0] == 0
    assert store.get("dispatch").completion_payload == record.completion_payload
