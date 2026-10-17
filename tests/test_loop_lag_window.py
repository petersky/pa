"""A reset scopes existing timer lateness, never hides a subsequent stall."""
import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.core.async_runtime import AsyncRuntime


@pytest.mark.asyncio
@pytest.mark.parametrize('reset_at,finished,expected,pre_window', [
    (0.100, 0.100, 0, 95),       # Only the setup/pause predates the window.
    (0.100, 0.140, 40, 95),      # The entire subsequent on-loop stall remains.
    (0.002, 0.045, 40, 0),       # A future pending deadline is not moved later.
    (None, 0.100, 95, 0),        # No explicit reset retains the original sample.
])
async def test_pending_timer_respects_exact_reset_boundary(reset_at, finished, expected, pre_window):
    runtime = AsyncRuntime(lag_interval_seconds=0.005)
    clock = SimpleNamespace(now=0.0)
    loop = SimpleNamespace(time=lambda: clock.now)
    calls = 0
    async def sleep(interval):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise asyncio.CancelledError
        if reset_at is not None:
            clock.now = reset_at
            runtime.reset_lag_samples()
        clock.now = finished
    runtime._lag_task = SimpleNamespace(get_loop=lambda: loop, done=lambda: False)
    try:
        with patch('pa.core.async_runtime.asyncio.get_running_loop', return_value=loop), \
             patch('pa.core.async_runtime.asyncio.sleep', sleep):
            with pytest.raises(asyncio.CancelledError):
                await runtime._monitor_loop_lag()
        snapshot = runtime.snapshot()['event_loop']
        assert snapshot['samples'] == 1
        assert snapshot['max_lag_ms'] == pytest.approx(expected)
        assert (snapshot['pre_window_lag_ms']['max'] or 0) == pytest.approx(pre_window)
    finally:
        runtime._lag_task = None
        await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('debug', [False, True])
async def test_real_stall_stays_visible_in_both_loop_debug_profiles(debug):
    import time
    loop = asyncio.get_running_loop()
    previous = loop.get_debug()
    loop.set_debug(debug)
    runtime = AsyncRuntime(lag_interval_seconds=0.005)
    try:
        await runtime.start()
        await asyncio.sleep(0.01)
        time.sleep(0.04)
        deadline = loop.time() + 0.2
        while runtime.snapshot()['event_loop']['max_lag_ms'] < 30 and loop.time() < deadline:
            await asyncio.sleep(0.005)
        assert runtime.snapshot()['event_loop']['max_lag_ms'] >= 30
    finally:
        await runtime.close()
        loop.set_debug(previous)
