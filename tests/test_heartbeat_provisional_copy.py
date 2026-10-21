"""Heartbeat updates preserve committed state and durable receipt semantics."""
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore, DispatchReceiptConflict
from pa.execution.progress import DispatchProgressHeartbeatV1, ProgressPhase


def heartbeat(dispatch_id, sequence, key):
    return DispatchProgressHeartbeatV1(
        dispatch_id=dispatch_id, card_id="card", acp_session_id="session",
        originating_instance_id="origin", authority_instance_id="authority",
        sequence=sequence, idempotency_key=key,
        phase=ProgressPhase.INVESTIGATING, summary="fixture",
    )


def setup_store(path):
    store = DispatchStore(path)
    record = DispatchRecord(
        mutation_id="fixture", authority_instance_id="authority",
        authority_url="https://fixture.invalid", target_instance_id="origin",
        card_id="card", session_id="session",
        materialization_plan={"steps": [{"proof": ["retain"]}]},
    )
    store.put(record)
    store.ingest_heartbeat(heartbeat(record.dispatch_id, 10, "prior"))
    return store, record.dispatch_id


@pytest.mark.parametrize("sequence", [5, 11])
@pytest.mark.parametrize("boundary,committed", [("commit_before", False), ("commit_after", True)])
def test_heartbeat_commit_boundaries_preserve_prior_snapshot_and_replay(tmp_path, sequence, boundary, committed):
    store, key = setup_store(tmp_path)
    original = store._records[key]
    before = original.model_dump()
    incoming = heartbeat(key, sequence, "incoming")
    def fail(observed):
        if observed == boundary:
            assert store.get(key).model_dump() == before
            assert original.model_dump() == before
            raise RuntimeError("disposable heartbeat boundary")
    try:
        store._fault_injector = fail
        with pytest.raises(RuntimeError, match="disposable heartbeat boundary"):
            store.ingest_heartbeat(incoming)
        assert original.model_dump() == before
        visible = store.get(key)
        assert ("incoming" in visible.progress_seen_keys) is committed
        assert visible.progress_heartbeat.sequence == (sequence if committed and sequence > 10 else 10)
        assert visible.progress_late_arrivals == int(committed and sequence < 10)
        store._fault_injector = None
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get(key) == visible
        result = store.ingest_heartbeat(incoming)
        assert result.status == ("duplicate" if committed else "late" if sequence < 10 else "accepted")
        assert store.get(key).progress_seen_keys.count("incoming") == 1
    finally:
        store._fault_injector = None
        store.close()


def test_heartbeat_publication_detaches_seen_ranges_plan_and_input(tmp_path):
    store, key = setup_store(tmp_path)
    try:
        original = store._records[key]
        before = original.model_dump()
        incoming = heartbeat(key, 11, "next")
        with patch.object(store, "_snapshot", wraps=store._snapshot) as snapshot:
            store.ingest_heartbeat(incoming, delivered=True)
            # Only the committed publication needs a complete detached history.
            assert snapshot.call_count == 1
        published = store._records[key]
        assert original.model_dump() == before
        assert published.progress_seen_keys is not original.progress_seen_keys
        assert published.materialization_plan is not original.materialization_plan
        assert published.progress_accepted_ranges is not original.progress_accepted_ranges
        assert published.progress_heartbeat is not incoming
        original.progress_seen_keys.append("old only")
        original.materialization_plan["steps"][0]["proof"].append("old only")
        original.progress_accepted_ranges[0][1] = 999
        incoming.summary = "caller changed"
        visible = store.get(key)
        assert visible.progress_seen_keys == ["prior", "next"]
        assert visible.materialization_plan["steps"][0]["proof"] == ["retain"]
        assert visible.progress_accepted_ranges == [[10, 11]]
        assert visible.progress_heartbeat.summary == "fixture"
        assert visible.progress_heartbeat.delivered_at is not None
        assert store.by_session("session") == visible
        assert store.latest_by_card({"card"})["card"] == visible
    finally:
        store.close()


def test_concurrent_heartbeat_receipts_remain_complete_and_durable(tmp_path):
    store, key = setup_store(tmp_path)
    try:
        inputs = [heartbeat(key, seq, f"concurrent-{seq}") for seq in range(11, 27)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(store.ingest_heartbeat, inputs))
        assert all(r.accepted for r in results)
        visible = store.get(key)
        assert visible.progress_heartbeat.sequence == 26
        assert visible.progress_accepted_ranges == [[10, 26]]
        assert len(visible.progress_seen_keys) == 17
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get(key) == visible
        for incoming in inputs:
            assert store.ingest_heartbeat(incoming).status == "duplicate"
        with pytest.raises(DispatchReceiptConflict):
            store.ingest_heartbeat(inputs[0].model_copy(update={"summary": "different payload"}))
        assert store.get(key) == visible
    finally:
        store.close()
