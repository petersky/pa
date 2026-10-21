"""Progress ingestion must not mutate a published snapshot before commit."""
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import json
from unittest.mock import patch

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore, DispatchReceiptConflict
from pa.execution.progress import DispatchProgressEventV1, ProgressPhase, ProgressToolDetailV1


def event(sequence, key=None, **updates):
    return DispatchProgressEventV1(
        dispatch_id="dispatch", card_id="card", acp_session_id="session",
        originating_instance_id="origin", authority_instance_id="authority",
        sequence=sequence, idempotency_key=key or f"event-{sequence}",
        phase=ProgressPhase.IMPLEMENTING, summary=f"checkpoint {sequence}",
        **updates,
    )


def seed(path):
    store = DispatchStore(path)
    store.put(DispatchRecord(
        dispatch_id="dispatch", mutation_id="fixture", card_id="card", session_id="session",
        authority_instance_id="authority", authority_url="https://fixture.invalid",
        target_instance_id="origin", materialization_plan={"steps": [{"proof": ["retain"]}]},
    ))
    store.ingest_progress(event(10, tool_details=[ProgressToolDetailV1(title="retained")]))
    return store


def incoming_for(store, branch):
    if branch == "conflict":
        return event(10, "conflicting-sequence")
    if branch == "coalesced":
        prior = store.get("dispatch").latest_progress
        return prior.model_copy(update={"sequence": 11, "idempotency_key": "coalesced"})
    return event(5 if branch == "late" else 11)


@pytest.mark.parametrize("branch", ["accepted", "late", "conflict", "coalesced"])
@pytest.mark.parametrize("boundary,committed", [("commit_before", False), ("commit_after", True)])
def test_progress_commit_boundaries_keep_prior_state_exact_receipts_and_reload(tmp_path, branch, boundary, committed):
    store = seed(tmp_path)
    original = store._records["dispatch"]
    before = original.model_dump()
    incoming = incoming_for(store, branch)
    error = RuntimeError("disposable progress boundary")
    def fail(observed):
        if observed == boundary:
            assert store.get("dispatch").model_dump() == before
            assert original.model_dump() == before
            raise error
    try:
        store._fault_injector = fail
        with pytest.raises(RuntimeError) as raised:
            store.ingest_progress(incoming, delivered=True)
        assert raised.value is error
        assert bool(getattr(error, "pa_transaction_committed", False)) is committed
        assert original.model_dump() == before
        visible = store.get("dispatch")
        assert (incoming.idempotency_key in visible.progress_seen_keys) is committed
        assert (visible.model_dump() != before) is committed
        store._fault_injector = None
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
        replay = store.ingest_progress(incoming, delivered=True)
        assert replay.status == ("duplicate" if committed else branch)
        assert (replay.replay_of_status if committed else replay.status) == branch
        with pytest.raises(DispatchReceiptConflict):
            store.ingest_progress(incoming.model_copy(update={"summary": "changed payload"}))
    finally:
        store._fault_injector = None
        store.close()


@pytest.mark.parametrize("branch", ["accepted", "late", "conflict", "coalesced"])
def test_progress_publication_deeply_detaches_prior_and_incoming_state(tmp_path, branch):
    store = seed(tmp_path)
    try:
        original = store._records["dispatch"]
        before = original.model_dump()
        incoming = incoming_for(store, branch)
        with patch.object(store, "_snapshot", wraps=store._snapshot) as snapshot:
            assert store.ingest_progress(incoming).status == branch
            assert snapshot.call_count == 1  # Full committed publication remains.
        visible = store.get("dispatch")
        assert original.model_dump() == before
        original.progress_events[0].tool_details[0].title = "stale payload"
        original.progress_events.clear()
        original.progress_seen_keys.append("stale receipt")
        original.progress_accepted_ranges[0][1] = 999
        original.materialization_plan["steps"][0]["proof"].append("stale plan")
        incoming.summary = "caller mutation"
        assert store.get("dispatch") == visible
        assert store.by_session("session") == visible
        assert store.latest_by_card({"card"})["card"] == visible
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
    finally:
        store.close()


@pytest.mark.parametrize("retention", ["age", "count", "bytes"])
@pytest.mark.parametrize("boundary,committed", [("commit_before", False), ("commit_after", True)])
def test_progress_retention_does_not_remove_prior_published_payloads(tmp_path, retention, boundary, committed):
    store = seed(tmp_path)
    record = store.get("dispatch")
    protected = event(1)
    protected.phase = ProgressPhase.TURN_ENDED
    protected.occurred_at = datetime.now(UTC) - timedelta(days=30)
    count = 64 if retention == "count" else 2
    record.progress_events = [protected] + [event(i + 2) for i in range(count)]
    if retention == "age":
        for item in record.progress_events[1:]:
            item.occurred_at = protected.occurred_at
    # Lifecycle put deliberately preserves normalized progress already stored.
    # Seed the retained history through its supported legacy migration instead.
    store.close()
    root = tmp_path / "retained"
    root.mkdir()
    (root / "dispatch_mutations.json").write_text(
        json.dumps({"dispatch": record.model_dump(mode="json")})
    )
    store = DispatchStore(root)
    if retention == "bytes":
        # A disposable boundary fixture, not a production limit change.
        store.MAX_PROGRESS_BYTES_PER_DISPATCH = 1
    original = store._records["dispatch"]
    before = original.model_dump()
    incoming = event(100)
    def fail(observed):
        if observed == boundary:
            assert original.model_dump() == before
            raise RuntimeError("disposable retention boundary")
    try:
        store._fault_injector = fail
        with pytest.raises(RuntimeError, match="disposable retention boundary"):
            store.ingest_progress(incoming)
        assert original.model_dump() == before
        visible = store.get("dispatch")
        if committed:
            assert visible.progress_events[0] == protected
            assert visible.progress_compacted_ranges
            assert len(visible.progress_events) == {"age": 2, "count": 64, "bytes": 1}[retention]
        else:
            assert visible == original
        store._fault_injector = None
        store.close()
        store = DispatchStore(root)
        assert store.get("dispatch") == visible
    finally:
        store._fault_injector = None
        store.close()


def test_progress_provenance_and_duplicate_are_no_copy_no_transaction(tmp_path):
    store = seed(tmp_path)
    try:
        prior = store.get("dispatch").latest_progress
        before = store.get("dispatch")
        with patch.object(store, "_snapshot", side_effect=AssertionError("No copy before validation")), \
             patch.object(store, "_transaction", side_effect=AssertionError("No invalid persistence")):
            assert store.ingest_progress(prior).status == "duplicate"
            with pytest.raises(ValueError):
                store.ingest_progress(event(11).model_copy(update={"originating_instance_id": "wrong"}))
        assert store.get("dispatch") == before
    finally:
        store.close()


def test_concurrent_progress_preserves_order_all_receipts_and_reload(tmp_path):
    store = seed(tmp_path)
    try:
        inputs = [event(i) for i in range(11, 31)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(store.ingest_progress, reversed(inputs)))
        assert all(result.accepted for result in results)
        visible = store.get("dispatch")
        assert [item.sequence for item in visible.progress_events] == list(range(10, 31))
        assert visible.progress_accepted_ranges == [[10, 30]]
        assert visible.progress_next_sequence == 31
        assert len(visible.progress_seen_keys) == 21
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
        for incoming in inputs:
            assert store.ingest_progress(incoming).status == "duplicate"
    finally:
        store.close()
