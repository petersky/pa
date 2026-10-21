"""Lifecycle updates retain normalized progress without copying stale inputs."""
from concurrent.futures import ThreadPoolExecutor
import json
from unittest.mock import patch

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore, DispatchStoreReadOnlyError
from pa.execution.progress import DispatchProgressEventV1, DispatchProgressHeartbeatV1, ProgressPhase


def seed(path):
    fields = dict(dispatch_id="dispatch", card_id="card", acp_session_id="session",
                  originating_instance_id="origin", authority_instance_id="authority",
                  phase=ProgressPhase.TURN_ENDED, summary="retained")
    record = DispatchRecord(
        dispatch_id="dispatch", mutation_id="fixture", card_id="card", session_id="session",
        authority_instance_id="authority", authority_url="https://fixture.invalid",
        target_instance_id="origin", materialization_plan={"proof": ["retain"]},
        progress_events=[DispatchProgressEventV1(**fields, sequence=1, idempotency_key="event")],
        progress_heartbeat=DispatchProgressHeartbeatV1(**fields, sequence=2, idempotency_key="heartbeat"),
        progress_next_sequence=3, progress_conflicts=2, progress_protocol_version=1,
        card_version="current-version", progress_authority_history=[{"version": "current"}],
    )
    (path / "dispatch_mutations.json").write_text(json.dumps({"dispatch": record.model_dump(mode="json")}))
    return DispatchStore(path)


def stale_update(store):
    stale = store.get("dispatch")
    stale.progress_events[0].summary = "stale caller history"
    stale.progress_heartbeat.summary = "stale caller heartbeat"
    stale.progress_seen_keys = ["stale-key"]
    stale.progress_next_sequence = 1
    stale.progress_conflicts = 0
    stale.progress_protocol_version = None
    stale.last_error = "lifecycle update"
    return stale


@pytest.mark.parametrize("boundary,committed", [("commit_before", False), ("commit_after", True)])
def test_lifecycle_put_faults_preserve_prior_publication_current_progress_and_reload(tmp_path, boundary, committed):
    store = seed(tmp_path)
    original = store._records["dispatch"]
    before = original.model_dump()
    stale = stale_update(store)
    input_before = stale.model_dump()
    error = RuntimeError("disposable lifecycle boundary")
    def fail(observed):
        if observed == boundary:
            assert original.model_dump() == before
            assert store.get("dispatch").model_dump() == before
            raise error
    try:
        store._fault_injector = fail
        with pytest.raises(RuntimeError) as raised:
            store.put(stale)
        assert raised.value is error
        assert bool(getattr(error, "pa_transaction_committed", False)) is committed
        assert original.model_dump() == before and stale.model_dump() == input_before
        visible = store.get("dispatch")
        assert visible.last_error == ("lifecycle update" if committed else original.last_error)
        for field in ("progress_events", "progress_heartbeat", "progress_seen_keys", "progress_next_sequence", "progress_conflicts", "progress_protocol_version"):
            assert getattr(visible, field) == getattr(original, field)
        store._fault_injector = None
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
    finally:
        store._fault_injector = None
        store.close()


def test_lifecycle_put_skips_discarded_payloads_but_detaches_input_publication_and_return(tmp_path):
    store = seed(tmp_path)
    try:
        original = store._records["dispatch"]
        before = original.model_dump()
        stale = stale_update(store)
        discarded = (stale.progress_events[0], stale.progress_heartbeat)
        def observe(cls):
            original_copy = cls.__deepcopy__
            def checked(value, *args, **kwargs):
                assert all(value is not item for item in discarded)
                return original_copy(value, *args, **kwargs)
            return checked
        with patch.object(DispatchProgressEventV1, "__deepcopy__", observe(DispatchProgressEventV1)), \
             patch.object(DispatchProgressHeartbeatV1, "__deepcopy__", observe(DispatchProgressHeartbeatV1)):
            result = store.put(stale)
        assert original.model_dump() == before
        visible = store.get("dispatch")
        assert result == visible
        assert store.by_session("session") == visible
        assert store.latest_by_card({"card"})["card"] == visible
        original.progress_events[0].summary = "old publication changed"
        original.progress_heartbeat.summary = "old heartbeat changed"
        original.materialization_plan["proof"].append("old plan")
        stale.materialization_plan["proof"].append("caller changed")
        stale.progress_seen_keys.append("caller changed")
        result.progress_events[0].summary = "returned copy changed"
        result.progress_heartbeat.summary = "returned heartbeat changed"
        result.materialization_plan["proof"].append("returned plan changed")
        assert store.get("dispatch") == visible
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == visible
    finally:
        store.close()


def test_lifecycle_put_preserves_authority_fence_and_sequence_maxima(tmp_path):
    store = seed(tmp_path)
    try:
        before = store.get("dispatch")
        stale = stale_update(store)
        stale.progress_authority_history = []
        stale.authority_instance_id = "stale-authority"
        stale.authority_url = "https://stale.invalid"
        stale.card_version = "stale-version"
        result = store.put(stale)
        for field in ("authority_instance_id", "authority_url", "card_version", "progress_authority_history", "progress_next_sequence", "progress_conflicts", "progress_protocol_version"):
            assert getattr(result, field) == getattr(before, field)
        stale.progress_authority_history.append({"caller": "changed"})
        assert store.get("dispatch") == result
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == result
    finally:
        store.close()


def test_read_only_lifecycle_put_still_fences_before_copy(tmp_path):
    store = DispatchStore(tmp_path, read_only=True, deferred_read_only=True)
    try:
        with patch.object(store, "_snapshot", side_effect=AssertionError("No copy without writer")):
            with pytest.raises(DispatchStoreReadOnlyError):
                store.put(DispatchRecord(mutation_id="fixture", authority_instance_id="authority", authority_url="https://fixture.invalid", target_instance_id="origin"))
        assert store._conn is None and not store.db_path.exists()
    finally:
        store.close()


def test_lifecycle_put_after_concurrent_delivery_never_restores_stale_attempts(tmp_path):
    store = seed(tmp_path)
    try:
        stale = stale_update(store)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: store.mark_progress_delivery_failed("dispatch", "event", "retry"), range(12)))
        store.mark_progress_delivered("dispatch", "heartbeat")
        current = store.get("dispatch")
        result = store.put(stale)
        assert result.progress_events == current.progress_events
        assert result.progress_events[0].delivery_attempts == 12
        assert result.progress_heartbeat == current.progress_heartbeat
        assert result.progress_heartbeat.delivered_at is not None
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get("dispatch") == result
    finally:
        store.close()


def test_lifecycle_put_mutation_conflict_fences_before_copy(tmp_path):
    store = seed(tmp_path)
    try:
        before = store.get("dispatch")
        stale = stale_update(store)
        stale.mutation_id = "different-mutation"
        with patch.object(store, "_snapshot", side_effect=AssertionError("No copy before identity fence")):
            with pytest.raises(ValueError, match="another mutation"):
                store.put(stale)
        assert store.get("dispatch") == before
    finally:
        store.close()


def test_first_lifecycle_put_detaches_nested_input_and_return(tmp_path):
    store = DispatchStore(tmp_path)
    try:
        incoming = DispatchRecord(mutation_id="fixture", authority_instance_id="authority",
            authority_url="https://fixture.invalid", target_instance_id="origin",
            materialization_plan={"proof": ["input"]})
        result = store.put(incoming)
        visible = store.get(incoming.dispatch_id)
        incoming.materialization_plan["proof"].append("caller changed")
        result.materialization_plan["proof"].append("return changed")
        assert store.get(incoming.dispatch_id) == visible
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get(incoming.dispatch_id) == visible
    finally:
        store.close()
