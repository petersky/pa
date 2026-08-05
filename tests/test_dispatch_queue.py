from __future__ import annotations

import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from pa.execution.dispatch import (
    CapacityAdmission,
    DispatchQueueFull,
    DispatchRecord,
    DispatchStore,
)


def record(index: int, *, principal: str = "user:a", priority: int = 0) -> DispatchRecord:
    return DispatchRecord(
        mutation_id=f"mutation-{index}",
        idempotency_key=f"key-{index}",
        request_fingerprint=f"fingerprint-{index}",
        card_id=f"card-{index}",
        principal_id=principal,
        project_id="project",
        authority_instance_id="authority",
        authority_url="http://authority.test",
        target_instance_id="target",
        capacity_provider="codex",
        requested_priority=priority,
    )


def capacity(*, execution: int = 4, queue: int = 100) -> CapacityAdmission:
    return CapacityAdmission(
        limit=execution,
        source="configured",
        provider="codex",
        provider_specific=True,
        queue_limit=queue,
        queue_source="configured",
        queue_provider_specific=True,
        global_limit=execution,
        provider_limit=execution,
        global_queue_limit=queue,
        provider_queue_limit=queue,
    )


def test_history_counts_use_maintained_index_without_scanning_ledger() -> None:
    class NoScanRecords(dict):
        def values(self):
            raise AssertionError("history_counts must not scan dispatch history")

    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        first = record(1)
        first.card_id = "shared-card"
        second = record(2)
        second.card_id = "shared-card"
        second.allow_concurrent = True
        store.admit(first)
        store.admit(second)
        store._records = NoScanRecords(store._records)

        assert store.history_counts({"shared-card", "missing"}, realm_id="default") == {
            "shared-card": 2,
            "missing": 0,
        }


def test_latest_by_session_uses_maintained_index_without_scanning_ledger() -> None:
    class NoScanRecords(dict):
        def values(self):
            raise AssertionError("latest_by_session must not scan dispatch history")

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        store = DispatchStore(path)
        active = record(1)
        active.session_id = "shared-session"
        store.admit(active)

        completed = record(2)
        completed.session_id = "shared-session"
        store.admit(completed)
        completed.state = "completed"
        store.put(completed)

        assigned_later = record(3)
        store.admit(assigned_later)
        assigned_later.session_id = "shared-session"
        assigned_later.state = "running"
        store.put(assigned_later)

        restarted = DispatchStore(path)
        restarted._records = NoScanRecords(restarted._records)

        selected = restarted.latest_by_session(
            {"shared-session", "missing"}, realm_id="default"
        )

        assert selected == {"shared-session": restarted.get(assigned_later.dispatch_id)}


def test_six_dispatches_use_four_slots_and_two_durable_queue_entries() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        admitted = [store.admit(record(index), capacity=capacity())[0] for index in range(6)]

        assert [item.state for item in admitted] == [
            "queued",
            "queued",
            "queued",
            "queued",
            "waiting_capacity",
            "waiting_capacity",
        ]
        assert store.queue_snapshot()["total"] == 2
        assert [item.queue_position for item in store.waiting()] == [1, 2]


def test_queue_full_boundary_and_duplicate_retry_do_not_consume_slots() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        store.admit(record(0), capacity=capacity(execution=1))
        waiting = [
            store.admit(record(index), capacity=capacity(execution=1))[0]
            for index in range(1, 101)
        ]
        duplicate, repeated = store.admit(record(100), capacity=capacity(execution=1))
        assert repeated
        assert duplicate.dispatch_id == waiting[-1].dispatch_id
        assert store.queue_snapshot()["total"] == 100

        with pytest.raises(DispatchQueueFull) as raised:
            store.admit(record(101), capacity=capacity(execution=1))
        assert raised.value.detail["code"] == "dispatch_queue_full"
        assert raised.value.detail["current_count"] == 100
        assert raised.value.detail["maximum_count"] == 100


def test_zero_queue_capacity_rejects_first_waiter() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        store.admit(record(0), capacity=capacity(execution=1, queue=0))

        with pytest.raises(DispatchQueueFull) as raised:
            store.admit(record(1), capacity=capacity(execution=1, queue=0))

        assert raised.value.detail["current_count"] == 0
        assert raised.value.detail["maximum_count"] == 0
        assert store.queue_snapshot()["total"] == 0


def test_terminal_slot_release_promotes_oldest_exactly_once_after_restart() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        store = DispatchStore(path)
        active, _ = store.admit(record(0), capacity=capacity(execution=1))
        waiting, _ = store.admit(record(1), capacity=capacity(execution=1))
        store.transition(active, "running", "started")

        restarted = DispatchStore(path)
        assert restarted.runnable() == []
        active = restarted.get(active.dispatch_id)
        assert active is not None
        restarted.transition(active, "completed", "done")
        runnable = restarted.runnable()
        assert [item.dispatch_id for item in runnable] == [waiting.dispatch_id]
        assert restarted.runnable()[0].dispatch_id == waiting.dispatch_id
        assert restarted.get(waiting.dispatch_id).queue_launched_at is not None


def test_fair_waves_prevent_one_principal_bulk_from_starving_another() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        store.admit(record(0, principal="user:a"), capacity=capacity(execution=1))
        a_second, _ = store.admit(
            record(1, principal="user:a"), capacity=capacity(execution=1)
        )
        a_third, _ = store.admit(
            record(2, principal="user:a"), capacity=capacity(execution=1)
        )
        b_first, _ = store.admit(
            record(3, principal="user:b"), capacity=capacity(execution=1)
        )
        assert [item.dispatch_id for item in store.waiting()] == [
            b_first.dispatch_id,
            a_second.dispatch_id,
            a_third.dispatch_id,
        ]


def test_priority_change_is_idempotent_and_audited() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        store.admit(record(0), capacity=capacity(execution=1))
        low, _ = store.admit(record(1), capacity=capacity(execution=1))
        high, _ = store.admit(record(2), capacity=capacity(execution=1))
        changed = store.reprioritize(
            high,
            priority=5,
            principal_id="admin:one",
            idempotency_key="priority-1",
        )
        repeated = store.reprioritize(
            high,
            priority=5,
            principal_id="admin:one",
            idempotency_key="priority-1",
        )
        assert repeated.dispatch_id == changed.dispatch_id
        assert [item.dispatch_id for item in store.waiting()] == [
            high.dispatch_id,
            low.dispatch_id,
        ]
        assert changed.queue_audit[-1]["action"] == "priority_changed"


def test_simultaneous_slot_release_promotes_only_one_waiter() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        active, _ = store.admit(record(0), capacity=capacity(execution=1))
        first, _ = store.admit(record(1), capacity=capacity(execution=1))
        second, _ = store.admit(record(2), capacity=capacity(execution=1))
        store.transition(active, "completed", "done")

        with ThreadPoolExecutor(max_workers=2) as pool:
            promoted = list(
                pool.map(
                    lambda item: store.promote_waiting(
                        item, capacity(execution=1)
                    ),
                    [first, second],
                )
            )
        assert sum(promoted) == 1
        states = {store.get(first.dispatch_id).state, store.get(second.dispatch_id).state}
        assert states == {"queued", "waiting_capacity"}


def test_blocked_target_is_retained_until_an_explicit_readiness_recheck_succeeds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        store.admit(record(0), capacity=capacity(execution=1))
        waiting, _ = store.admit(record(1), capacity=capacity(execution=1))
        blocked = store.block_waiting(
            waiting,
            code="queued_target_unavailable",
            reason="target disconnected",
        )
        assert blocked.state == "blocked"
        assert store.runnable()[0].card_id == "card-0"
        assert store.get(waiting.dispatch_id).state == "blocked"

        assert not store.promote_waiting(waiting, capacity(execution=1))
        restored = store.get(waiting.dispatch_id)
        assert restored.state == "waiting_capacity"
        assert restored.queue_blocked_code is None


def test_global_and_provider_limits_both_apply_across_provider_scopes() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        codex_capacity = CapacityAdmission(
            limit=1,
            global_limit=2,
            provider_limit=1,
            source="configured_provider",
            provider="codex",
            provider_specific=True,
            queue_limit=100,
            global_queue_limit=100,
        )
        cursor_capacity = codex_capacity.model_copy(
            update={"provider": "cursor"}
        )
        codex_one, _ = store.admit(record(10), capacity=codex_capacity)
        codex_two, _ = store.admit(record(11), capacity=codex_capacity)
        cursor_record = record(12)
        cursor_record.capacity_provider = "cursor"
        cursor_one, _ = store.admit(cursor_record, capacity=cursor_capacity)
        cursor_two_record = record(13)
        cursor_two_record.capacity_provider = "cursor"
        cursor_two, _ = store.admit(cursor_two_record, capacity=cursor_capacity)

        assert codex_one.state == "queued"
        assert codex_two.state == "waiting_capacity"
        assert cursor_one.state == "queued"
        assert cursor_two.state == "waiting_capacity"

        snapshot = store.capacity_snapshot("target")
        assert snapshot["dispatch_reservations"] == 2
        assert snapshot["dispatch_waiting"] == 2
        assert snapshot["provider_concurrency"]["codex"] == {
            "dispatch_reservations": 1,
            "dispatch_waiting": 1,
        }
