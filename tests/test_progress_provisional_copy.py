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


def retained_store(path, protected_count, ordinary_count):
    record = DispatchRecord(
        dispatch_id="dispatch", mutation_id="fixture", card_id="card", session_id="session",
        authority_instance_id="authority", authority_url="https://fixture.invalid",
        target_instance_id="origin",
    )
    record.progress_events = [event(i + 1) for i in range(protected_count + ordinary_count)]
    for item in record.progress_events[:protected_count]:
        item.phase = ProgressPhase.TURN_ENDED
    (path / "dispatch_mutations.json").write_text(
        json.dumps({"dispatch": record.model_dump(mode="json")})
    )
    return DispatchStore(path)


@pytest.mark.parametrize("count", [1, 64, 128, 1000])
def test_protected_only_history_is_not_serialized_for_inapplicable_byte_pruning(tmp_path, count):
    store = retained_store(tmp_path, count, 0)
    incoming = event(count + 1)
    incoming.phase = ProgressPhase.TURN_ENDED
    dumped = []
    original_dump = DispatchProgressEventV1.model_dump_json
    def count_dump(value, *args, **kwargs):
        dumped.append(value.idempotency_key)
        return original_dump(value, *args, **kwargs)
    try:
        with patch.object(DispatchProgressEventV1, "model_dump_json", count_dump):
            assert store.ingest_progress(incoming).accepted
        # The incoming payload still has its durable serialization; none of the
        # protected history can be removed by either existing retention bound.
        assert dumped == [incoming.idempotency_key]
        visible = store.get("dispatch")
        assert [item.sequence for item in visible.progress_events] == list(range(1, count + 2))
        assert not visible.progress_compacted_ranges
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
    finally:
        store.close()


@pytest.mark.parametrize("protected_count", [0, 63, 64, 128])
@pytest.mark.parametrize("byte_overflow", [False, True])
def test_count_then_byte_retention_preserves_first_eligible_order_and_receipts(tmp_path, protected_count, byte_overflow):
    store = retained_store(tmp_path, protected_count, 4)
    incoming = event(protected_count + 5)
    if byte_overflow:
        store.MAX_PROGRESS_BYTES_PER_DISPATCH = 1  # Disposable boundary only.
    try:
        assert store.ingest_progress(incoming).accepted
        visible = store.get("dispatch")
        protected = list(range(1, protected_count + 1))
        ordinary = list(range(protected_count + 1, protected_count + 6))
        keep = 0 if byte_overflow else min(5, max(0, 64 - protected_count))
        assert [item.sequence for item in visible.progress_events] == protected + (ordinary[-keep:] if keep else [])
        removed = ordinary[:len(ordinary) - keep]
        assert visible.progress_compacted_ranges == ([[removed[0], removed[-1]]] if removed else [])
        assert store.ingest_progress(incoming).status == "duplicate"
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
        assert store.ingest_progress(incoming).status == "duplicate"
    finally:
        store.close()


@pytest.mark.parametrize("offset", [0, -1])
def test_retention_byte_limit_equality_and_one_byte_overflow(tmp_path, offset):
    store = retained_store(tmp_path, 2, 2)
    incoming = event(5)
    before = store.get("dispatch")
    store.MAX_PROGRESS_BYTES_PER_DISPATCH = sum(
        len(item.model_dump_json().encode()) for item in [*before.progress_events, incoming]
    ) + offset
    try:
        assert store.ingest_progress(incoming).accepted
        visible = store.get("dispatch")
        assert [item.sequence for item in visible.progress_events] == ([1, 2, 4, 5] if offset else [1, 2, 3, 4, 5])
        assert visible.progress_compacted_ranges == ([[3, 3]] if offset else [])
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
    finally:
        store.close()


@pytest.mark.parametrize("shape", ["defaults", "legacy", "unicode", "delivery"])
def test_retention_counts_current_canonical_model_bytes(shape):
    payload = event(1)
    if shape == "legacy":
        # Omitted defaults are hydrated before retention; persisted JSON length
        # alone does not describe the current canonical model encoding.
        data = payload.model_dump(mode="json", exclude_unset=True)
        payload = DispatchProgressEventV1.model_validate(data)
    elif shape == "unicode":
        payload.summary = '界🙂 e\u0301 "quoted" \\ escaped\nline'
    elif shape == "delivery":
        payload.delivered_at = datetime.now(UTC)
        payload.delivery_attempts = 123
        payload.delivery_error = "界" * 240
    assert DispatchStore._progress_event_byte_size(payload) == len(
        payload.model_dump_json().encode()
    )


def test_retention_honors_an_overridden_json_encoding():
    class CustomEncoding(DispatchProgressEventV1):
        def model_dump_json(self, **kwargs):
            return super().model_dump_json(**kwargs) + " " * 7

    payload = CustomEncoding(**event(1).model_dump())
    assert DispatchStore._progress_event_byte_size(payload) == len(
        payload.model_dump_json().encode()
    )


def test_retention_does_not_materialize_retained_json_text(tmp_path):
    store = retained_store(tmp_path, 2, 2)
    incoming = event(5)
    dumped = []
    original = DispatchProgressEventV1.model_dump_json
    def count_dump(value, *args, **kwargs):
        dumped.append(value.idempotency_key)
        return original(value, *args, **kwargs)
    try:
        with patch.object(DispatchProgressEventV1, "model_dump_json", count_dump):
            assert store.ingest_progress(incoming).accepted
        assert dumped == [incoming.idempotency_key]
        assert [item.sequence for item in store.get("dispatch").progress_events] == list(range(1, 6))
    finally:
        store.close()


@pytest.mark.parametrize("delivery", ["failed", "delivered"])
def test_retention_reloads_normalized_bytes_after_delivery_and_stale_metadata(tmp_path, delivery):
    store = retained_store(tmp_path, 2, 2)
    try:
        if delivery == "failed":
            store.mark_progress_delivery_failed("dispatch", "event-3", "界" * 240)
        else:
            store.mark_progress_delivered("dispatch", "event-3")
        # Corrupt only the disposable fixture's size metadata. The retained
        # payload remains valid; neither branch may trust cached/persisted size.
        store._conn.execute("UPDATE dispatch_progress_events SET byte_size=1")
        store._conn.commit()
        store.close()
        store = DispatchStore(tmp_path)
        incoming = event(5)
        prior = store.get("dispatch")
        store.MAX_PROGRESS_BYTES_PER_DISPATCH = sum(
            len(item.model_dump_json().encode()) for item in [*prior.progress_events, incoming]
        ) - 1
        assert store.ingest_progress(incoming).accepted
        visible = store.get("dispatch")
        assert [item.sequence for item in visible.progress_events] == [1, 2, 4, 5]
        assert visible.progress_compacted_ranges == [[3, 3]]
        assert store.ingest_progress(incoming).status == "duplicate"
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
    finally:
        store.close()
