"""Expired progress preserves sparse ranges, protected evidence and replay keys."""
from datetime import UTC, datetime, timedelta

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.progress import DispatchProgressEventV1, ProgressPhase
from tests.test_dispatch_compaction_changes import completed


@pytest.mark.parametrize("boundary", [None, "commit_before", "commit_after"])
def test_mixed_expired_progress_keeps_order_ranges_and_commit_parity(tmp_path, boundary):
    store = DispatchStore(tmp_path)
    now = datetime.now(UTC) + timedelta(days=60)
    cutoff = now - timedelta(days=store.RECEIPT_REPLAY_DAYS)
    store.put(DispatchRecord(
        dispatch_id="dispatch", mutation_id="mutation", idempotency_key="admit",
        card_id="card", session_id="session", authority_instance_id="authority",
        authority_url="https://example.invalid", target_instance_id="target",
        state="running", progress_protocol_version=1,
    ))
    expired = []
    retained = []
    for i in range(1, 49):
        # Sparse sequence numbers and interleaved protected/recent checkpoints
        # prevent a contiguous-prefix shortcut from satisfying this contract.
        sequence = i * 2
        protected = i % 5 == 0
        recent = i % 7 == 0
        event = DispatchProgressEventV1(
            card_id="card", dispatch_id="dispatch", acp_session_id="session",
            originating_instance_id="target", authority_instance_id="authority",
            sequence=sequence, idempotency_key=f"progress-{i}",
            phase=ProgressPhase.BLOCKED if protected else ProgressPhase.IMPLEMENTING,
            summary=f"Private mixed expiry fixture {i}",
            occurred_at=cutoff if recent else datetime.now(UTC),
        )
        assert store.ingest_progress(event).accepted
        store.mark_progress_delivered("dispatch", event.idempotency_key)
        (retained if protected or recent else expired).append(event)
    template_store = DispatchStore(tmp_path / "terminal-template")
    terminal = completed(template_store)
    template_store.close()
    record = store.get("dispatch")
    record.state = terminal.state
    record.acknowledged_at = terminal.acknowledged_at
    record.final_report = terminal.final_report
    record.post_turn_evaluations = terminal.post_turn_evaluations
    store.put(record)
    original = store.get("dispatch")
    assert len(original.progress_events) == 48
    assert original.progress_compacted_ranges == []

    def fail(observed):
        if boundary and observed == boundary:
            raise RuntimeError("private compaction boundary")

    store._fault_injector = fail
    if boundary:
        with pytest.raises(RuntimeError, match="private compaction boundary"):
            store.compact(now=now)
    else:
        result = store.compact(now=now)
        assert result["events"] == len(expired)
    store._fault_injector = None
    committed = boundary != "commit_before"
    actual = store.get("dispatch")
    expected_keys = [event.idempotency_key for event in retained] if committed else [
        event.idempotency_key for event in original.progress_events
    ]
    assert [event.idempotency_key for event in actual.progress_events] == expected_keys
    assert actual.progress_compacted_ranges == (
        [[event.sequence, event.sequence] for event in expired] if committed else []
    )
    assert actual.progress_seen_keys == original.progress_seen_keys
    assert len(original.progress_events) == 48
    assert original.progress_compacted_ranges == []
    assert actual.completion_payload == original.completion_payload
    assert actual.updated_at == original.updated_at
    sql_keys = [row[0] for row in store._conn.execute(
        "SELECT idempotency_key FROM dispatch_progress_events ORDER BY sequence"
    )]
    assert sql_keys == expected_keys
    store.close()
    reopened = DispatchStore(tmp_path)
    durable = reopened.get("dispatch")
    assert durable.progress_events == actual.progress_events
    assert durable.progress_compacted_ranges == actual.progress_compacted_ranges
    assert durable.progress_accepted_ranges == actual.progress_accepted_ranges
    # Reopen reconstructs replay keys from durable receipts. Expired receipts
    # are absent; retained payload keys remain protected across restart.
    assert durable.progress_seen_keys == expected_keys
    assert durable.completion_payload == actual.completion_payload
    reopened.close()
