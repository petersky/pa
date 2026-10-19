"""Bounded leaf diagnostics preserve the canonical worker's ownership."""
import ast
import asyncio
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from pa.core.async_runtime import AsyncRuntime, BlockingOperationTimeout, BlockingQueueFull
from pa.core.sqlite_metrics import ObservedConnection
from pa.health_journal.service import CANONICAL_PHASES, HealthService


def service(runtime):
    # No Journal, canonical Store, workspace, kernel or instance bootstrap.
    health = object.__new__(HealthService)
    health.ctx = SimpleNamespace(services={'async_runtime': runtime} if runtime else {})
    return health


@pytest.mark.asyncio
async def test_exact_results_sql_counts_and_existing_aggregate(caplog):
    runtime = AsyncRuntime(max_workers=1, slow_call_seconds=0)
    health = service(runtime)
    private = 'fixture-private-body-and-key'

    def read(value, *, key):
        assert value == private and key == private
        with sqlite3.connect(':memory:', factory=ObservedConnection) as connection:
            assert connection.execute('SELECT 1 UNION ALL SELECT 2').fetchall() == [(1,), (2,)]
        return sentinel

    sentinel = object()
    try:
        for phase in CANONICAL_PHASES:
            assert await health.canonical_call(read, private, key=private, diagnostic_phase=phase) is sentinel
        snapshot = runtime.snapshot()
        aggregate = snapshot['operations']['health.canonical_obligation']
        assert aggregate['submitted'] == aggregate['completed'] == aggregate['slow_calls'] == len(CANONICAL_PHASES)
        assert aggregate['failed'] == aggregate['timed_out'] == 0
        metrics = runtime.work_metrics.snapshot()['metrics']
        assert len(metrics) == len(CANONICAL_PHASES) * 6
        for phase in CANONICAL_PHASES:
            prefix = f'health.canonical.{phase}.'
            assert metrics[prefix + 'sqlite_rows']['total'] == 2
            assert metrics[prefix + 'sqlite_calls']['total'] == 2
            assert metrics[prefix + 'worker_failed']['total'] == 0
            assert metrics[prefix + 'worker_ms']['count'] == 1
            assert metrics[prefix + 'thread_cpu_ms']['count'] == 1
        assert private not in repr(snapshot) + repr(metrics) + caplog.text
        assert len([r for r in caplog.records if 'Slow off-loop operation' in r.message]) == len(CANONICAL_PHASES)
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_worker_error_propagates_exactly_and_is_counted():
    runtime = AsyncRuntime(max_workers=1)
    failure = ValueError('private-error-text')
    def fail():
        raise failure
    try:
        with pytest.raises(ValueError) as caught:
            await service(runtime).canonical_call(fail, diagnostic_phase='card_get')
        assert caught.value is failure
        # An already-completed failing future can propagate before its runtime
        # completion callback is delivered. Inspect aggregate counters after
        # that callback, not at an assumed scheduling boundary.
        async with asyncio.timeout(2):
            while runtime.snapshot()['operations']['health.canonical_obligation']['active']:
                await asyncio.sleep(0)
        metrics = runtime.work_metrics.snapshot()['metrics']
        assert metrics['health.canonical.card_get.worker_failed']['total'] == 1
        assert runtime.snapshot()['operations']['health.canonical_obligation']['failed'] == 1
        assert str(failure) not in repr(metrics)
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_cancelled_wait_keeps_worker_slot_and_records_late_completion():
    runtime = AsyncRuntime(max_workers=1, max_queue=0)
    entered, release = threading.Event(), threading.Event()
    def blocked():
        entered.set()
        assert release.wait(2)
        return 'late-result'
    task = asyncio.create_task(service(runtime).canonical_call(blocked, diagnostic_phase='session_get'))
    try:
        async with asyncio.timeout(2):
            while not entered.is_set():
                await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert runtime.work_metrics.snapshot()['metrics'] == {}
        with pytest.raises(BlockingQueueFull):
            await service(runtime).canonical_call(lambda: None, diagnostic_phase='watch_get')
        release.set()
        async with asyncio.timeout(2):
            while runtime.snapshot()['operations']['health.canonical_obligation']['active']:
                await asyncio.sleep(0)
        metrics = runtime.work_metrics.snapshot()['metrics']
        assert metrics['health.canonical.session_get.worker_failed']['total'] == 0
        assert metrics['health.canonical.session_get.worker_ms']['count'] == 1
        assert not any('watch_get' in name for name in metrics)
    finally:
        release.set()
        await runtime.close()


@pytest.mark.asyncio
async def test_deadline_retains_five_second_contract_and_late_worker_result():
    class ShortFixtureRuntime(AsyncRuntime):
        async def run_blocking(self, operation, call, /, *args, timeout=None, **kwargs):
            assert operation == 'health.canonical_obligation' and timeout == 5
            # Shorten only this disposable contract probe, never the source's
            # caller deadline or slow-warning threshold.
            return await super().run_blocking(operation, call, *args, timeout=0.01, **kwargs)
    runtime = ShortFixtureRuntime(max_workers=1, max_queue=0)
    release = threading.Event()
    def blocked():
        assert release.wait(2)
        return 'late-result'
    try:
        with pytest.raises(BlockingOperationTimeout):
            await service(runtime).canonical_call(blocked, diagnostic_phase='dispatch_get')
        assert runtime.work_metrics.snapshot()['metrics'] == {}
        assert runtime.snapshot()['operations']['health.canonical_obligation']['active'] == 1
        release.set()
        async with asyncio.timeout(2):
            while runtime.snapshot()['operations']['health.canonical_obligation']['active']:
                await asyncio.sleep(0)
        metrics = runtime.work_metrics.snapshot()['metrics']
        assert metrics['health.canonical.dispatch_get.worker_failed']['total'] == 0
        aggregate = runtime.snapshot()['operations']['health.canonical_obligation']
        assert aggregate['timed_out'] == aggregate['completed'] == 1 and aggregate['failed'] == 0
    finally:
        release.set()
        await runtime.close()


@pytest.mark.asyncio
async def test_finite_names_bounded_samples_and_no_callable_introspection():
    runtime = AsyncRuntime(max_workers=1)
    health = service(runtime)
    class Call:
        @property
        def __name__(self):
            raise AssertionError('diagnostics must not inspect a callable')
        def __call__(self):
            return 7
    try:
        with pytest.raises(ValueError, match='unknown canonical diagnostic phase'):
            await health.canonical_call(Call(), diagnostic_phase='user-controlled-key')
        assert runtime.snapshot()['operations'] == {}
        for _ in range(260):
            assert await health.canonical_call(Call()) == 7
        metrics = runtime.work_metrics.snapshot()['metrics']
        assert all(name.startswith('health.canonical.unclassified.') for name in metrics)
        assert all(value['samples'] == 256 and value['count'] == 260 for value in metrics.values())
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_runtime_free_fallback_preserves_arguments_and_result():
    sentinel = object()
    assert await service(None).canonical_call(lambda *, value: value, value=sentinel,
                                             diagnostic_phase='card_get') is sentinel


def test_health_direct_call_sites_use_only_fixed_labels():
    for path in ('src/pa/health_journal/service.py', 'src/pa/modules/health_journal.py'):
        calls = [node for node in ast.walk(ast.parse(Path(path).read_text()))
                 if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                 and node.func.attr == 'canonical_call']
        assert calls
        for call in calls:
            phase = next(kw.value for kw in call.keywords if kw.arg == 'diagnostic_phase')
            assert isinstance(phase, ast.Constant) and phase.value in CANONICAL_PHASES - {'unclassified'}
