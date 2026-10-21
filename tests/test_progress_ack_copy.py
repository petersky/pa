"""Delivery acknowledgments mutate detached provisional state before commit."""
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import json
from unittest.mock import patch

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore, DispatchStoreReadOnlyError
from pa.execution.progress import (
    DispatchProgressEventV1, DispatchProgressHeartbeatV1, ProgressPhase,
    ProgressToolDetailV1, sanitize_text,
)


def seed(path):
    stamp = datetime.now(UTC)
    record = DispatchRecord(
        dispatch_id="dispatch", mutation_id="fixture", card_id="card", session_id="session",
        authority_instance_id="authority", authority_url="https://fixture.invalid",
        target_instance_id="origin", materialization_plan={"steps": [{"proof": ["retain"]}]},
    )
    fields = dict(card_id="card", dispatch_id="dispatch", acp_session_id="session",
                  originating_instance_id="origin", authority_instance_id="authority",
                  occurred_at=stamp, phase=ProgressPhase.TURN_ENDED)
    record.progress_events = [DispatchProgressEventV1(
        **fields, sequence=i, idempotency_key=f"event-{i}", summary=f"event {i}",
        tool_details=[ProgressToolDetailV1(title="retained tool")],
        delivery_attempts=2, delivery_error="prior error",
    ) for i in range(1, 5)]
    record.progress_heartbeat = DispatchProgressHeartbeatV1(
        **fields, sequence=5, idempotency_key="heartbeat", summary="heartbeat",
        delivery_attempts=3, delivery_error="prior heartbeat error",
    )
    (path / "dispatch_mutations.json").write_text(json.dumps({"dispatch": record.model_dump(mode="json")}))
    return DispatchStore(path)


def payload(record, kind):
    return record.progress_heartbeat if kind == "heartbeat" else record.progress_events[1]


def mutate(store, action, key):
    if action == "delivered":
        store.mark_progress_delivered("dispatch", key)
    else:
        store.mark_progress_delivery_failed("dispatch", key, "fixture " * 100)


@pytest.mark.parametrize("kind", ["event", "heartbeat"])
@pytest.mark.parametrize("action", ["delivered", "failed"])
@pytest.mark.parametrize("boundary,committed", [("commit_before", False), ("commit_after", True)])
def test_ack_fault_preserves_prior_publication_exact_exception_and_reload(tmp_path, kind, action, boundary, committed):
    store = seed(tmp_path)
    original = store._records["dispatch"]
    before = original.model_dump()
    key = payload(original, kind).idempotency_key
    error = RuntimeError("disposable ack boundary")
    def fail(observed):
        if observed == boundary:
            assert original.model_dump() == before
            assert store.get("dispatch").model_dump() == before
            raise error
    try:
        store._fault_injector = fail
        with pytest.raises(RuntimeError) as raised:
            mutate(store, action, key)
        assert raised.value is error
        assert bool(getattr(error, "pa_transaction_committed", False)) is committed
        assert original.model_dump() == before
        visible = store.get("dispatch")
        target = payload(visible, kind)
        previous = payload(original, kind)
        assert target.delivery_attempts == previous.delivery_attempts + int(committed and action == "failed")
        assert bool(target.delivered_at) is (committed and action == "delivered")
        assert target.delivery_error == (
            None if committed and action == "delivered"
            else sanitize_text("fixture " * 100, limit=240) if committed
            else previous.delivery_error
        )
        assert visible.materialization_plan == original.materialization_plan
        store._fault_injector = None
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
    finally:
        store._fault_injector = None
        store.close()


@pytest.mark.parametrize("kind", ["event", "heartbeat"])
@pytest.mark.parametrize("action", ["delivered", "failed"])
def test_ack_keeps_deep_committed_publication_and_one_full_copy(tmp_path, kind, action):
    store = seed(tmp_path)
    try:
        original = store._records["dispatch"]
        before = original.model_dump()
        key = payload(original, kind).idempotency_key
        with patch.object(store, "_snapshot", wraps=store._snapshot) as snapshot:
            mutate(store, action, key)
            assert snapshot.call_count == 1  # Complete committed publication only.
        assert original.model_dump() == before
        visible = store.get("dispatch")
        assert [e.idempotency_key for e in visible.progress_events] == [f"event-{i}" for i in range(1, 5)]
        assert store.by_session("session") == visible
        assert store.latest_by_card({"card"})["card"] == visible
        original.progress_events[0].tool_details[0].title = "stale writer"
        original.progress_events[1].tool_details[0].title = "stale selected payload"
        original.progress_heartbeat.summary = "stale heartbeat"
        original.materialization_plan["steps"][0]["proof"].append("stale plan")
        original.progress_seen_keys.append("stale receipt")
        assert store.get("dispatch") == visible
        assert store._write_rows == 2
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
    finally:
        store.close()


@pytest.mark.parametrize("action", ["delivered", "failed"])
def test_absent_ack_payload_is_no_copy_no_transaction_no_publication(tmp_path, action):
    store = seed(tmp_path)
    try:
        before = store.get("dispatch")
        changes = store._conn.total_changes
        with patch.object(store, "_snapshot", side_effect=AssertionError("No payload to copy")), \
             patch.object(store, "_transaction", side_effect=AssertionError("No payload to persist")):
            mutate(store, action, "absent")
        assert store.get("dispatch") == before
        assert store._conn.total_changes == changes
        assert store._write_rows == 0
    finally:
        store.close()


@pytest.mark.parametrize("action", ["delivered", "failed"])
def test_ack_writer_fence_remains_before_absent_key_lookup(tmp_path, action):
    store = DispatchStore(tmp_path, read_only=True, deferred_read_only=True)
    try:
        with pytest.raises(DispatchStoreReadOnlyError):
            mutate(store, action, "absent")
        assert store._conn is None and not store.db_path.exists()
    finally:
        store.close()


def test_concurrent_ack_updates_keep_attempts_delivery_order_and_durable_state(tmp_path):
    store = seed(tmp_path)
    try:
        before = store.get("dispatch")
        keys = [e.idempotency_key for e in before.progress_events] + ["heartbeat"]
        operations = [(action, key) for key in keys for action in ("failed", "failed", "failed", "delivered")]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda pair: mutate(store, *pair), operations))
        visible = store.get("dispatch")
        assert [e.idempotency_key for e in visible.progress_events] == keys[:-1]
        assert visible.materialization_plan == before.materialization_plan
        for target, previous in zip(visible.progress_events + [visible.progress_heartbeat],
                                    before.progress_events + [before.progress_heartbeat], strict=True):
            assert target.delivered_at is not None
            assert target.delivery_attempts == previous.delivery_attempts + 3
            assert target.delivery_error in (None, sanitize_text("fixture " * 100, limit=240))
        assert store.pending_progress_deliveries("origin") == []
        assert store._write_rows == 40
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
    finally:
        store.close()
