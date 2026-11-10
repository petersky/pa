"""Bound recovery copying without changing the authority's oldest-first batch."""
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore, GoalDispatchProvenance


def record(index, **updates):
    values = dict(
        dispatch_id=f"d-{index}", mutation_id=f"m-{index}",
        authority_instance_id="local", authority_url="http://fixture",
        target_instance_id="remote", state="running",
        updated_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index // 2),
        goal_provenance=GoalDispatchProvenance(
            goal_id="goal", goal_version=1, policy_revision=1,
            authority_instance_id="local", fencing_token=1,
            action_reservation_id=f"reservation-{index}", actor_principal="user:fixture",
        ),
        request_payload={"proof": [{"values": [index]}]},
    )
    values.update(updates)
    return DispatchRecord(**values)


@pytest.fixture
def ledger(tmp_path):
    store = DispatchStore(tmp_path)
    try:
        yield store
    finally:
        store.close()


def test_copies_only_returned_oldest_records_and_keeps_stable_ties(ledger):
    # Deliberately insert in descending order; ties retain insertion order.
    # Publish fixed timestamps directly in this private index fixture; put()
    # correctly replaces caller timestamps with the durable mutation time.
    with ledger._lock:
        ledger._publish_records_locked([record(index) for index in reversed(range(30))])
    expected = sorted(ledger.list(limit=100), key=lambda row: row.updated_at)[:3]
    original = ledger._snapshot
    with patch.object(ledger, "_snapshot", wraps=original) as copies:
        actual = ledger.pending_goal_lifecycle("local", limit=3)
    assert actual == expected
    assert [row.dispatch_id for row in actual] == ["d-1", "d-0", "d-3"]
    assert copies.call_count == 3


def test_authority_and_provenance_filters_precede_selection(ledger):
    ledger.put(record(0, authority_instance_id="other"))
    other = record(1)
    other.goal_provenance.authority_instance_id = "other"
    ledger.put(other)
    ledger.put(record(2, goal_provenance=None,
                      followup_operations={"op": {"state": "governance_pending"}}))
    ledger.put(record(3))
    assert [r.dispatch_id for r in ledger.pending_goal_lifecycle("local", limit=1)] == ["d-3"]
    assert ledger.pending_goal_lifecycle("missing", limit=1) == []


@pytest.mark.parametrize("limit,expected", [(-1, 1), (0, 1), (1, 1), (2, 2), (5000, 4)])
def test_existing_limit_clamps(ledger, limit, expected):
    for index in range(4):
        ledger.put(record(index))
    assert len(ledger.pending_goal_lifecycle("local", limit=limit)) == expected


def test_existing_upper_limit_and_selected_snapshot_boundary(ledger):
    with ledger._lock:
        ledger._publish_records_locked([record(index) for index in range(1001)])
    original = ledger._snapshot
    with patch.object(ledger, "_snapshot", wraps=original) as copies:
        result = ledger.pending_goal_lifecycle("local", limit=5000)
    assert len(result) == copies.call_count == 1000
    assert result[-1].dispatch_id == "d-999"


def test_returned_nested_fields_are_detached_and_refresh_after_publication(ledger):
    ledger.put(record(0))
    first = ledger.pending_goal_lifecycle("local")[0]
    first.request_payload["proof"][0]["values"].append("mutated")
    first.goal_provenance.released_at = datetime.now(UTC)
    assert ledger.pending_goal_lifecycle("local")[0].request_payload == {"proof": [{"values": [0]}]}
    assert ledger.pending_goal_lifecycle("local")[0].goal_provenance.released_at is None
    current = ledger.get("d-0")
    current.goal_provenance.released_at = datetime.now(UTC)
    ledger.put(current)
    assert ledger.pending_goal_lifecycle("local") == []


def test_terminal_release_and_followup_index_survive_reload(tmp_path):
    store = DispatchStore(tmp_path)
    try:
        store.put(record(0, state="failed"))  # Crash window: still needs release.
        released = record(1, state="completed")
        released.goal_provenance.released_at = datetime.now(UTC)
        store.put(released)
        followup = record(2, state="completed")
        followup.goal_provenance.released_at = datetime.now(UTC)
        followup.followup_operations = {"op": {"state": "governance_pending"}}
        store.put(followup)
    finally:
        store.close()
    reloaded = DispatchStore(tmp_path)
    try:
        assert [r.dispatch_id for r in reloaded.pending_goal_lifecycle("local")] == ["d-0", "d-2"]
        row = reloaded.get("d-2")
        row.followup_operations["op"]["state"] = "completed"
        reloaded.put(row)
        assert [r.dispatch_id for r in reloaded.pending_goal_lifecycle("local")] == ["d-0"]
    finally:
        reloaded.close()
