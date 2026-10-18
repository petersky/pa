from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.reconciliation import CompletionReconciler


def record(index, **changes):
    return DispatchRecord(
        dispatch_id=f"dispatch-{index}", mutation_id=f"mutation-{index}",
        authority_instance_id="fixture", authority_url="http://fixture.invalid",
        target_instance_id="fixture", card_id="fixture-card", state="running",
        completion_payload={}, updated_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index),
        **changes,
    )


def test_startup_recovery_matches_existing_newest_window_and_preserves_full_records(tmp_path):
    ledger = DispatchStore(tmp_path)
    values = [record(i) for i in range(8)]
    values[0].completion_payload = {"old": "eligible but outside newest window"}
    values[2].card_id = None
    values[3].state = "completed"
    values[4].completion_payload = None
    values[5].state = "completion_pending"
    values[6].card_id = ""
    values[7].materialization_plan = {"nested": [{"proof": "retained full candidate"}]}
    for value in values:
        ledger.put(value)
    baseline = [r for r in ledger.list(limit=6) if r.card_id
                and r.state in {"running", "completion_pending"}
                and r.completion_payload is not None]
    with patch.object(ledger, "_snapshot", wraps=ledger._snapshot) as copies:
        actual = ledger.startup_recovery_records(limit=6)
    assert [r.dispatch_id for r in actual] == ["dispatch-7", "dispatch-5"]
    assert [r.model_dump() for r in actual] == [r.model_dump() for r in baseline]
    assert copies.call_count == 2
    actual[0].materialization_plan["nested"][0]["proof"] = "changed detached snapshot"
    actual[1].completion_payload["new"] = "detached"
    assert ledger.get("dispatch-7").materialization_plan["nested"][0]["proof"] == "retained full candidate"
    assert ledger.get("dispatch-5").completion_payload == {}
    assert len(ledger.list(limit=100)) == 8


def test_newest_window_is_not_refilled_from_older_eligible_records(tmp_path):
    ledger = DispatchStore(tmp_path)
    older = record(0)
    newer = record(1)
    newer.state = "failed"
    ledger.put(older)
    ledger.put(newer)
    assert ledger.startup_recovery_records(limit=1) == []
    assert ledger.startup_recovery_records(limit=0) == []
    assert [r.dispatch_id for r in ledger.startup_recovery_records(limit=2)] == [older.dispatch_id]


def test_dynamic_legacy_adapter_uses_existing_list_contract():
    ledger = MagicMock()
    ledger.list.return_value = ["legacy recovery obligation"]
    reconciler = object.__new__(CompletionReconciler)
    reconciler.dispatch_store = ledger
    assert reconciler._startup_recovery_records() == ["legacy recovery obligation"]
    ledger.list.assert_called_once_with(limit=1000)
    assert "startup_recovery_records" not in ledger._mock_children


def test_declared_adapter_reader_is_bound_and_used():
    reader = Mock(return_value=["candidate"])
    old = Mock(side_effect=AssertionError("Should not copy the full inventory"))
    reconciler = object.__new__(CompletionReconciler)
    reconciler.dispatch_store = SimpleNamespace(startup_recovery_records=reader, list=old)
    assert reconciler._startup_recovery_records() == ["candidate"]
    reader.assert_called_once_with(limit=1000)
