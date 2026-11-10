"""Worker ownership, lost-cancel and stale-write regressions for the dispatch ledger."""
from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from pa.execution.dispatch import (
    DispatchCancelRequested, DispatchRecord, DispatchStore, DispatchSuperseded, DispatchWorker,
)

LOCAL, PEER = "instance-local", "instance-peer"


def make(dispatch_id: str, *, authority: str, target: str, state: str = "queued", **extra) -> DispatchRecord:
    return DispatchRecord(
        dispatch_id=dispatch_id, mutation_id=f"mutation-{dispatch_id}", authority_instance_id=authority,
        authority_url="http://authority.invalid", target_instance_id=target, state=state,
        request_payload={"message": "work"}, **extra,
    )


@pytest.fixture
def ledger(tmp_path):
    store = DispatchStore(tmp_path)
    yield store
    store.close()


def cancel_like_the_endpoint(ledger: DispatchStore, dispatch_id: str) -> None:
    """Mirror cancel_dispatch(): flag + transition from a fresh read."""
    record = ledger.get(dispatch_id)
    record.cancel_requested = True
    record.control_operations["cancel-key"] = "cancel"
    ledger.transition(record, record.state, "Cancellation requested.")


# --- finding 2: a target never adopts a peer's dispatch -------------------------------------

def test_restart_does_not_requeue_the_mirror_of_a_peer_dispatch(ledger):
    mirror = ledger.put(make("mirror", authority=PEER, target=LOCAL, state="materializing"))
    own = ledger.put(make("own", authority=LOCAL, target=PEER, state="starting_session"))

    recovered = ledger.reconcile_interrupted(LOCAL)

    assert [r.dispatch_id for r in recovered] == ["own"]
    assert ledger.get("mirror").state == "materializing"
    assert ledger.get("own").state == "queued"
    assert mirror.state == "materializing" and own.state == "starting_session"


def test_runnable_skips_mirror_records_already_flipped_to_queued(ledger):
    ledger.put(make("mirror", authority=PEER, target=LOCAL))
    ledger.put(make("own", authority=LOCAL, target=PEER))
    assert [r.dispatch_id for r in ledger.runnable(LOCAL)] == ["own"]


@pytest.mark.asyncio
async def test_worker_only_executes_records_it_is_the_authority_for(ledger):
    ledger.put(make("mirror", authority=PEER, target=LOCAL, state="starting_session"))
    ledger.put(make("own", authority=LOCAL, target=PEER))
    seen: list[str] = []

    async def handler(record):
        seen.append(record.dispatch_id)

    worker = DispatchWorker(ledger, handler, instance_id=LOCAL, retry_seconds=0.01, rng=random.Random(0))
    worker.start()
    for _ in range(100):
        if seen:
            break
        await __import__("asyncio").sleep(0.02)
    await worker.close()
    assert seen == ["own"]
    assert ledger.get("mirror").state == "starting_session"


# --- finding 3: lost cancel / stale whole-record writes -------------------------------------

def test_stale_worker_put_does_not_erase_a_committed_cancel(ledger):
    ledger.put(make("d", authority=LOCAL, target=PEER, state="checking_sync"))
    stale = ledger.get("d")                   # the worker's in-memory snapshot
    cancel_like_the_endpoint(ledger, "d")
    stale.sync_evidence = {"healthy": True}   # the worker's own progress
    ledger.put(stale)
    durable = ledger.get("d")
    assert durable.cancel_requested is True
    assert durable.control_operations.get("cancel-key") == "cancel"
    assert durable.sync_evidence == {"healthy": True}


def test_worker_commit_stops_at_a_committed_cancel_without_clearing_it(ledger):
    ledger.put(make("d", authority=LOCAL, target=PEER, state="checking_sync"))
    stale = ledger.get("d")
    cancel_like_the_endpoint(ledger, "d")
    with pytest.raises(DispatchCancelRequested):
        ledger.commit_worker_stage(stale, "materializing", "Materializing.")
    durable = ledger.get("d")
    assert durable.cancel_requested is True
    assert durable.state == "checking_sync"


def test_retry_may_still_clear_a_cancel(ledger):
    """The sticky cancel must not break the legacy retry write that clears it."""
    record = ledger.put(make("d", authority=LOCAL, target=PEER, state="checking_sync"))
    cancel_like_the_endpoint(ledger, "d")
    current = ledger.get("d")
    ledger.transition(current, "cancelled", "Dispatch cancelled.")
    current = ledger.get("d")
    current.cancel_requested = False
    current.control_operations["retry-key"] = "retry"
    ledger.transition(current, "queued", "Operator queued a safe retry.")
    assert ledger.get("d").cancel_requested is False
    assert record.dispatch_id == "d"


@pytest.mark.asyncio
async def test_queued_cancel_is_not_reverted_by_the_worker_claiming_it(ledger):
    ledger.put(make("d", authority=LOCAL, target=PEER))
    snapshot = ledger.runnable(LOCAL)[0]      # what the worker polled
    queued = ledger.get("d")
    ledger.transition(queued, "cancelled", "Operator cancelled queued dispatch.")
    handler = AsyncMock()
    worker = DispatchWorker(ledger, handler, instance_id=LOCAL)
    await worker._execute(snapshot)
    handler.assert_not_awaited()
    assert ledger.get("d").state == "cancelled"


def test_acknowledged_completion_is_not_regressed_by_a_stale_stage_write(ledger):
    ledger.put(make("d", authority=LOCAL, target=PEER, state="delivering_prompt"))
    stale = ledger.get("d")
    now = datetime.now(UTC)

    def acknowledge(current):
        current.acknowledged_at = now
        current.completion_received_at = now
        current.completion_payload = {"outcome": "done"}
        current.completion_delivery_class = "acknowledged"
        ledger.apply_transition(current, "completed", "Agent turn ended and was acknowledged.")

    ledger.mutate_current("d", mutate=acknowledge)

    ledger.transition(stale, "running", "Prompt accepted by the intended remote session.")
    durable = ledger.get("d")
    assert durable.state == "completed"
    assert durable.acknowledged_at is not None
    assert durable.completion_payload == {"outcome": "done"}
    stale_event = durable.events[-1]
    assert stale_event.detail["requested_state"] == "running"
    assert stale_event.detail["acknowledged_completion_wins"] is True


def test_worker_commit_after_acknowledgement_keeps_the_completion(ledger):
    ledger.put(make("d", authority=LOCAL, target=PEER, state="delivering_prompt"))
    stale = ledger.get("d")

    def acknowledge(current):
        current.acknowledged_at = datetime.now(UTC)
        current.completion_payload = {"outcome": "done"}
        ledger.apply_transition(current, "completed", "acknowledged")

    ledger.mutate_current("d", mutate=acknowledge)
    stale.knowledge_recorded_at = datetime.now(UTC)
    committed = ledger.commit_worker_stage(stale, "running", "Prompt accepted.")
    assert committed.state == "completed"
    assert committed.completion_payload == {"outcome": "done"}
    assert committed.knowledge_recorded_at is not None


def test_worker_commit_refuses_to_continue_a_cancelled_dispatch(ledger):
    ledger.put(make("d", authority=LOCAL, target=PEER, state="queued"))
    stale = ledger.get("d")
    ledger.transition(ledger.get("d"), "cancelled", "Operator cancelled queued dispatch.")
    with pytest.raises(DispatchSuperseded):
        ledger.commit_worker_stage(stale, "checking_sync", "Checking realm convergence.")
    assert ledger.get("d").state == "cancelled"


# --- finding 8: FIFO order and bounded re-execution -----------------------------------------

def test_runnable_is_priority_then_fifo(ledger):
    base = datetime.now(UTC)
    for name, age, priority in (("newest", 0, 0), ("oldest", 30, 0), ("middle", 10, 0), ("urgent", 1, 5)):
        ledger.put(make(name, authority=LOCAL, target=PEER, created_at=base - timedelta(minutes=age),
                        requested_priority=priority))
    assert [r.dispatch_id for r in ledger.runnable(LOCAL)] == ["urgent", "oldest", "middle", "newest"]


@pytest.mark.asyncio
async def test_a_record_that_keeps_crashing_the_worker_fails_after_the_attempt_cap(ledger):
    ledger.put(make("d", authority=LOCAL, target=PEER, stage_attempts=2))
    handler, notifier = AsyncMock(), MagicMock()
    worker = DispatchWorker(ledger, handler, instance_id=LOCAL, max_stage_attempts=2, stall_notifier=notifier)
    await worker._execute(ledger.runnable(LOCAL)[0])
    handler.assert_not_awaited()
    failed = ledger.get("d")
    assert failed.state == "failed" and failed.recoverable is True
    assert failed.error_code == "dispatch_stage_attempts_exhausted"
    notifier.assert_called_once()


@pytest.mark.asyncio
async def test_zero_cap_keeps_the_old_unlimited_behaviour(ledger):
    ledger.put(make("d", authority=LOCAL, target=PEER, stage_attempts=50))
    handler = AsyncMock()
    worker = DispatchWorker(ledger, handler, instance_id=LOCAL, max_stage_attempts=0)
    await worker._execute(ledger.runnable(LOCAL)[0])
    handler.assert_awaited_once()
