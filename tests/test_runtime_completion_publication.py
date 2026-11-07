"""Completed worker results publish accounting before returning to a caller."""
import asyncio
import sqlite3
from unittest.mock import patch

import pytest

from pa.core.async_runtime import AsyncRuntime
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
