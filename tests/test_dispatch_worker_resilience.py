from __future__ import annotations

import asyncio
import random
import threading
import unittest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

from pa.core.async_runtime import AsyncRuntime, BlockingQueueFull
from pa.execution.dispatch import DispatchRecord, DispatchWorker


async def eventually(predicate, timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


class DispatchWorkerResilienceTests(unittest.IsolatedAsyncioTestCase):
    def record(self) -> DispatchRecord:
        return DispatchRecord(
            dispatch_id="dispatch-1",
            mutation_id="mutation-1",
            idempotency_key="key-1",
            request_fingerprint="fingerprint",
            target_instance_id="target",
            authority_instance_id="authority",
            authority_instance_name="Authority",
            authority_url="http://authority",
            request_payload={"message": "work"},
            created_at=datetime.now(UTC),
        )

    def worker(self, store, handler, **kwargs) -> DispatchWorker:
        return DispatchWorker(
            store,
            handler,
            retry_seconds=0.01,
            retry_max_seconds=0.02,
            rng=random.Random(0),
            **kwargs,
        )

    async def test_queue_full_once_and_repeatedly_recovers(self) -> None:
        record = self.record()
        store = MagicMock()
        store.reconcile_interrupted.return_value = []
        store.runnable.side_effect = [
            BlockingQueueFull("full once"),
            BlockingQueueFull("full twice"),
            [record],
            [],
        ]
        handled = asyncio.Event()
        worker = self.worker(store, AsyncMock(side_effect=lambda _: handled.set()))
        worker.start()
        await asyncio.wait_for(handled.wait(), 1)
        self.assertGreaterEqual(worker.poll_failures, 2)
        self.assertIsNotNone(worker.last_successful_poll_at)
        self.assertEqual(worker.state, "running")
        await worker.close()

    async def test_unexpected_generation_failure_is_supervised(self) -> None:
        store = MagicMock()
        store.reconcile_interrupted.side_effect = [ValueError("corrupt poll"), []]
        store.runnable.return_value = []
        worker = self.worker(store, AsyncMock())
        worker.start()
        await eventually(lambda: worker.generation >= 2)
        self.assertEqual(worker.restart_count, 1)
        self.assertEqual(worker.last_failure_type, "ValueError")
        self.assertTrue(worker.snapshot()["live"])
        await worker.close()

    async def test_admission_wake_recreates_completed_runner(self) -> None:
        store = MagicMock()
        store.reconcile_interrupted.return_value = []
        store.runnable.return_value = []
        worker = self.worker(store, AsyncMock())
        worker._runner = asyncio.create_task(asyncio.sleep(0))
        await worker._runner
        worker.wake()
        await eventually(lambda: worker.generation == 1)
        self.assertTrue(worker.snapshot()["live"])
        await worker.close()

    async def test_generation_restart_does_not_duplicate_active_dispatch(self) -> None:
        record = self.record()
        store = MagicMock()
        store.reconcile_interrupted.return_value = []
        store.runnable.side_effect = [[record], ValueError("boom"), [record], []]
        release = asyncio.Event()
        calls = 0

        async def handler(_record):
            nonlocal calls
            calls += 1
            await release.wait()

        worker = self.worker(store, handler)
        worker.start()
        await eventually(lambda: worker.generation >= 2)
        await asyncio.sleep(0.05)
        self.assertEqual(calls, 1)
        self.assertEqual(record.stage_attempts, 1)
        release.set()
        await worker.close()

    async def test_dedicated_lane_runs_while_shared_executor_is_full(self) -> None:
        shared = AsyncRuntime(max_workers=1, max_queue=0, default_timeout=None)
        entered, release = threading.Event(), threading.Event()

        def block():
            entered.set()
            release.wait(2)

        blocking = asyncio.create_task(shared.run_blocking("progress.flood", block))
        await asyncio.to_thread(entered.wait, 1)
        record = self.record()
        store = MagicMock()
        store.reconcile_interrupted.return_value = []
        store.runnable.side_effect = [[record], []]
        handled = asyncio.Event()
        worker = self.worker(
            store,
            AsyncMock(side_effect=lambda _: handled.set()),
            async_runtime=shared,
        )
        worker.start()
        await asyncio.wait_for(handled.wait(), 1)
        self.assertEqual(record.stage_attempts, 1)
        release.set()
        await blocking
        await worker.close()
        await shared.close()

    async def test_shutdown_during_backoff_is_prompt(self) -> None:
        store = MagicMock()
        store.reconcile_interrupted.return_value = []
        store.runnable.side_effect = BlockingQueueFull("saturated")
        worker = DispatchWorker(
            store, AsyncMock(), retry_seconds=10, retry_max_seconds=10
        )
        worker.start()
        await eventually(lambda: worker.state == "backing_off")
        await asyncio.wait_for(worker.close(), 0.5)
        self.assertEqual(worker.state, "stopped")
