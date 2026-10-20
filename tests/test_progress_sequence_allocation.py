"""Scalar sequence updates must preserve the committed dispatch snapshot."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore


def make_store(path):
    store = DispatchStore(path)
    record = DispatchRecord(
        mutation_id="sequence-fixture",
        authority_instance_id="fixture",
        authority_url="https://fixture.invalid",
        target_instance_id="fixture",
        card_id="fixture-card",
        session_id="fixture-session",
        materialization_plan={"steps": [{"evidence": ["keep"]}]},
        progress_compacted_ranges=[[1, 3]],
        progress_accepted_ranges=[[1, 3]],
        progress_highest_accepted_sequence=3,
        progress_next_sequence=4,
    )
    store.put(record)
    return store, record.dispatch_id


@pytest.mark.parametrize("boundary,committed", [("commit_before", False), ("commit_after", True)])
def test_sequence_commit_boundaries_keep_old_snapshot_and_reload(tmp_path, boundary, committed):
    store, dispatch_id = make_store(tmp_path)
    original = store._records[dispatch_id]
    prior = original.model_dump()

    def observe(actual):
        if actual == boundary:
            # Readers must not see uncommitted scalar changes or nested mutation.
            assert store.get(dispatch_id).model_dump() == prior
            raise RuntimeError("disposable sequence boundary")

    store._fault_injector = observe
    with pytest.raises(RuntimeError, match="disposable sequence boundary"):
        store.allocate_progress_sequence(dispatch_id)
    assert original.model_dump() == prior
    current = store.get(dispatch_id)
    assert current.progress_next_sequence == 4 + int(committed)
    assert current.materialization_plan == original.materialization_plan
    assert current.progress_compacted_ranges == [[1, 3]]
    store._fault_injector = None
    store.close()
    reopened = DispatchStore(tmp_path)
    try:
        assert reopened.get(dispatch_id) == current
        assert reopened.allocate_progress_sequence(dispatch_id) == 4 + int(committed)
    finally:
        reopened.close()


def test_sequence_publication_detaches_nested_fields_and_preserves_indexes(tmp_path):
    store, dispatch_id = make_store(tmp_path)
    try:
        original = store._records[dispatch_id]
        assert store.allocate_progress_sequence(dispatch_id) == 4
        published = store._records[dispatch_id]
        assert published is not original
        assert published.materialization_plan is not original.materialization_plan
        assert published.progress_compacted_ranges is not original.progress_compacted_ranges
        original.materialization_plan["steps"][0]["evidence"].append("old snapshot only")
        original.progress_compacted_ranges[0][1] = 99
        detached = store.get(dispatch_id)
        assert detached.materialization_plan["steps"][0]["evidence"] == ["keep"]
        assert detached.progress_compacted_ranges == [[1, 3]]
        assert store.latest_by_card({"fixture-card"})["fixture-card"] == detached
        assert store.by_session("fixture-session") == detached
    finally:
        store.close()


def test_concurrent_scalar_allocations_are_unique_and_durable(tmp_path):
    store, dispatch_id = make_store(tmp_path)
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            sequences = list(pool.map(lambda _: store.allocate_progress_sequence(dispatch_id), range(32)))
        assert sorted(sequences) == list(range(4, 36))
        assert store.get(dispatch_id).progress_next_sequence == 36
    finally:
        store.close()
    reopened = DispatchStore(tmp_path)
    try:
        assert reopened.allocate_progress_sequence(dispatch_id) == 36
    finally:
        reopened.close()


def test_missing_dispatch_does_not_allocate(tmp_path):
    store = DispatchStore(tmp_path)
    try:
        with pytest.raises(ValueError, match="dispatch not found"):
            store.allocate_progress_sequence("missing")
    finally:
        store.close()
