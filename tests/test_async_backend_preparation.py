"""Framework warm-up remains observable, single-owned and closed-admission safe."""
import asyncio
import logging

import pytest

from pa.core.async_runtime import AsyncRuntime, AsyncRuntimeClosed


@pytest.mark.asyncio
async def test_concurrent_start_prepares_once_before_lag_monitor(tmp_path, monkeypatch):
    runtime = AsyncRuntime(max_workers=2, max_queue=3)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0
    async def prepare():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
    monkeypatch.setattr('pa.core.async_runtime._prepare_async_backend', prepare)
    first = asyncio.create_task(runtime.start())
    try:
        await entered.wait()
        second = asyncio.create_task(runtime.start())
        await asyncio.sleep(0)
        assert runtime._lag_task is None and not first.done() and not second.done()
        assert runtime.snapshot()['async_backend_initialization']['state'] == 'preparing'
        release.set()
        await asyncio.gather(first, second)
        snapshot = runtime.snapshot()
        assert calls == 1
        assert snapshot['operations']['startup.async_backend_prepare']['completed'] == 1
        assert snapshot['executor']['max_workers'] == 2 and snapshot['executor']['max_queue'] == 3
        assert snapshot['executor']['active'] == snapshot['executor']['queued'] == 0
    finally:
        release.set()
        await runtime.close()


@pytest.mark.asyncio
async def test_warmup_duration_is_retained_and_warned_at_existing_threshold(monkeypatch, caplog):
    async def prepare():
        await asyncio.sleep(0.02)
    monkeypatch.setattr('pa.core.async_runtime._prepare_async_backend', prepare)
    runtime = AsyncRuntime(slow_call_seconds=0.005)
    try:
        with caplog.at_level(logging.WARNING):
            await runtime.start()
        snapshot = runtime.snapshot()
        assert snapshot['async_backend_initialization']['elapsed_ms'] >= 10
        metrics = snapshot['operations']['startup.async_backend_prepare']
        assert metrics['completed'] == metrics['slow_calls'] == 1
        assert metrics['runtime_ms']['max'] >= 10
        assert 'Slow async initialization' in caplog.text
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_close_during_preparation_cannot_start_late_monitor(monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    async def prepare():
        entered.set()
        await release.wait()
    monkeypatch.setattr('pa.core.async_runtime._prepare_async_backend', prepare)
    runtime = AsyncRuntime()
    task = asyncio.create_task(runtime.start())
    try:
        await entered.wait()
        await runtime.close()
        release.set()
        with pytest.raises(AsyncRuntimeClosed):
            await task
        assert runtime._lag_task is None
    finally:
        release.set()
        await runtime.close()


@pytest.mark.asyncio
async def test_failed_backend_preparation_has_no_monitor_and_can_recover(monkeypatch):
    async def failed():
        raise RuntimeError('fixture private failure')
    async def ready():
        await asyncio.sleep(0)
    monkeypatch.setattr('pa.core.async_runtime._prepare_async_backend', failed)
    runtime = AsyncRuntime()
    try:
        with pytest.raises(RuntimeError):
            await runtime.start()
        assert runtime._lag_task is None
        assert runtime.snapshot()['operations']['startup.async_backend_prepare']['failed'] == 1
        assert 'fixture private' not in str(runtime.snapshot()['async_backend_initialization'])
        monkeypatch.setattr('pa.core.async_runtime._prepare_async_backend', ready)
        await runtime.start()
        assert runtime.snapshot()['async_backend_initialization']['state'] == 'ready'
    finally:
        await runtime.close()
