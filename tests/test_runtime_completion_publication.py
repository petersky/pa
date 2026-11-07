"""Completed worker results publish accounting before returning to a caller."""
import asyncio
import sqlite3
import threading
from unittest.mock import patch

import pytest

from pa.core.async_runtime import AsyncRuntime, BlockingOperationTimeout, BlockingQueueFull
from pa.core.sqlite_metrics import ObservedConnection


@pytest.mark.asyncio
@pytest.mark.parametrize("budgeted", [False, True])
@pytest.mark.parametrize("fails", [False, True])
async def test_already_completed_worker_publishes_once_before_return(budgeted, fails):
    operation = "fixture.completed_worker"
    runtime = AsyncRuntime(max_workers=1, max_queue=0,
        operation_budgets={operation: {}} if budgeted else None)
    loop = asyncio.get_running_loop()
    failure = RuntimeError("fixture payload excluded from diagnostics")
    committed = []

    def worker():
        with sqlite3.connect(":memory:", factory=ObservedConnection) as connection:
            assert connection.execute("SELECT 1").fetchone()[0] == 1
        if fails:
            raise failure
        return 17

    def already_completed(executor, call):
        # Exercise the supported already-done Future boundary deterministically.
        # The callable still runs on the real worker, with its copied context.
        result = loop.create_future()
        try:
            result.set_result(executor.submit(call).result(timeout=5))
        except BaseException as error:
            result.set_exception(error)
        return result

    try:
        with patch.object(loop, "run_in_executor", already_completed):
            if fails:
                with pytest.raises(RuntimeError) as raised:
                    await runtime.run_blocking(operation, worker, on_commit=committed.append)
                assert raised.value is failure
            else:
                assert await runtime.run_blocking(operation, worker, on_commit=committed.append) == 17
            assert committed == ([] if fails else [17])
            immediate = runtime.snapshot()
            measured = immediate["operations"][operation]
            assert measured["sqlite_calls"]["max"] > 0
            assert measured["sqlite_rows"]["max"] == 1
            assert measured["active"] == 0
            assert measured["completed"] == int(not fails)
            assert measured["failed"] == int(fails)
            assert immediate["executor"]["active"] == 0
            # A sequential operation must not observe a stale occupied slot.
            assert await runtime.run_blocking("fixture.next_worker", lambda: 19) == 19
        await asyncio.sleep(0)  # Deliver the previously registered callbacks.
        final = runtime.snapshot()
        assert final["operations"][operation]["completed"] == int(not fails)
        assert final["operations"][operation]["failed"] == int(fails)
        assert final["operations"][operation]["sqlite_calls"]["samples"] == 1
        assert final["operations"]["fixture.next_worker"]["completed"] == 1
        assert final["executor"]["active"] == 0
        assert committed == ([] if fails else [17])
        assert "fixture payload excluded" not in repr(final)
    finally:
        await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_late_commit_stays_owned_after_waiter_leaves(cancelled):
    runtime = AsyncRuntime(max_workers=1, max_queue=0)
    started, release = threading.Event(), threading.Event()
    committed, delivered = [], asyncio.Event()

    def worker():
        started.set()
        assert release.wait(5)
        return 17

    def on_commit(value):
        committed.append(value)
        delivered.set()

    task = asyncio.create_task(runtime.run_blocking("fixture.late_worker", worker,
        timeout=None if cancelled else 0.02, on_commit=on_commit))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        if cancelled:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancelled else BlockingOperationTimeout):
            await task
        assert committed == []
        assert runtime.snapshot()["executor"]["active"] == 1
        with pytest.raises(BlockingQueueFull):
            await runtime.run_blocking("fixture.overflow", lambda: None)
        release.set()
        await asyncio.wait_for(delivered.wait(), timeout=5)
        await asyncio.sleep(0)
        measured = runtime.snapshot()["operations"]["fixture.late_worker"]
        assert committed == [17]
        assert measured["completed"] == 1 and measured["active"] == 0
        assert measured["cancelled"] == int(cancelled)
        assert measured["timed_out"] == int(not cancelled)
    finally:
        release.set()
        await runtime.close()
