"""Summary reads must not expand discarded durable turn evidence."""
from unittest.mock import patch

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore, summarize_dispatch
from pa.execution.post_turn import TurnEndSnapshotV1


def retained_record(state="completed"):
    snapshot = TurnEndSnapshotV1(
        turn_id="turn", turn_sequence=1, dispatch_id="dispatch", card_id="card",
        authority_instance_id="fixture", originating_instance_id="fixture",
        dispatch_state=state,
        deliverables={"proof": [{"retained": "x" * 1024} for _ in range(64)]},
    )
    return DispatchRecord(
        dispatch_id="dispatch", mutation_id="mutation",
        authority_instance_id="fixture", authority_url="http://fixture",
        target_instance_id="fixture", state=state,
        turn_end_snapshots=[snapshot],
        progress_accepted_ranges=[[i, i] for i in range(1, 40, 2)],
        progress_highest_accepted_sequence=40,
        progress_compacted_ranges=[[i, i] for i in range(1, 40, 2)],
    )


@pytest.mark.parametrize("state", ["running", "completion_pending", "completed", "failed"])
def test_summary_matches_existing_full_projection_without_expanding_snapshot(state):
    record = retained_record(state)
    full = record.public_dict()
    expected = summarize_dispatch(full)
    with patch.object(TurnEndSnapshotV1, "model_dump", side_effect=AssertionError(
        "Summary expanded discarded turn evidence"
    )):
        assert record.public_dict(summary=True) == expected
    assert "turn_end" not in expected
    assert record.public_dict() == full
    assert full["turn_end"] == record.turn_end_snapshots[-1].model_dump(mode="json")
    # Complete protocol gaps/compaction are correctness evidence, including
    # ranges beyond the ordinary five-item presentation bound.
    assert expected["progress"]["missing_ranges"] == [[i, i] for i in range(2, 41, 2)]
    assert expected["progress"]["compacted_ranges"] == [[i, i] for i in range(1, 40, 2)]


def test_status_reads_skip_expansion_and_durable_detail_survives_reload(tmp_path):
    store = DispatchStore(tmp_path)
    record = retained_record()
    store.put(record)
    expected = store.get(record.dispatch_id).public_dict()
    before = store.get(record.dispatch_id).model_dump(mode="json")
    with patch.object(TurnEndSnapshotV1, "model_dump", side_effect=AssertionError(
        "Status expanded discarded turn evidence"
    )):
        statuses = store.public_statuses()
    assert len(statuses) == 1 and "turn_end" not in statuses[0]
    reloaded = DispatchStore(tmp_path).get(record.dispatch_id)
    assert reloaded.public_dict() == expected
    assert reloaded.model_dump(mode="json") == before


def test_empty_history_keeps_full_null_and_absent_summary_field():
    record = retained_record()
    record.turn_end_snapshots.clear()
    assert record.public_dict()["turn_end"] is None
    assert "turn_end" not in record.public_dict(summary=True)
