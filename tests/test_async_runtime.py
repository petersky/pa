from __future__ import annotations

import asyncio
import threading
import time
import unittest

from pa.core.async_runtime import (
    AsyncRuntime,
    BlockingOperationTimeout,
    BlockingQueueFull,
)
from pa.core.hooks import HookBus


class AsyncRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.runtime = AsyncRuntime(
            max_workers=1,
            max_queue=1,
            default_timeout=1.0,
            lag_interval_seconds=0.005,
            slow_call_seconds=1.0,
        )
        await self.runtime.start()

    async def asyncTearDown(self) -> None:
        await self.runtime.close(drain_timeout=1.0)

    async def test_blocked_worker_does_not_delay_unrelated_coroutines(self) -> None:
        release = threading.Event()
        started = threading.Event()

        def blocked_disk() -> str:
            started.set()
            release.wait(1)
            return "done"

        task = asyncio.create_task(
            self.runtime.run_blocking("disk.read", blocked_disk)
        )
        await asyncio.to_thread(started.wait, 1)
        before = time.perf_counter()
        await asyncio.sleep(0)
        unrelated_ms = (time.perf_counter() - before) * 1000
        release.set()
        self.assertEqual(await task, "done")
        self.assertLess(unrelated_ms, 25)

    async def test_queue_is_bounded(self) -> None:
        release = threading.Event()
        started = threading.Event()

        def blocked() -> None:
            started.set()
            release.wait(1)

        first = asyncio.create_task(self.runtime.run_blocking("git", blocked))
        await asyncio.to_thread(started.wait, 1)
        second = asyncio.create_task(self.runtime.run_blocking("git", lambda: None))
        await asyncio.sleep(0)
        with self.assertRaises(BlockingQueueFull):
            await self.runtime.run_blocking("git", lambda: None)
        release.set()
        await asyncio.gather(first, second)
        metrics = self.runtime.snapshot()["operations"]["git"]
        self.assertEqual(metrics["rejected"], 1)
        self.assertEqual(metrics["max_active"], 1)
        self.assertEqual(metrics["max_queued"], 1)

    async def test_timeout_keeps_worker_slot_charged_until_thread_finishes(self) -> None:
        release = threading.Event()
        started = threading.Event()

        def blocked_sqlite() -> None:
            started.set()
            release.wait(1)

        with self.assertRaises(BlockingOperationTimeout):
            await self.runtime.run_blocking(
                "sqlite.transaction", blocked_sqlite, timeout=0.01
            )
        self.assertTrue(started.is_set())
        # One queue slot remains available, but no worker capacity is falsely
        # released while the timed-out native call is still running.
        queued = asyncio.create_task(
            self.runtime.run_blocking("sqlite.read", lambda: "ok")
        )
        await asyncio.sleep(0)
        with self.assertRaises(BlockingQueueFull):
            await self.runtime.run_blocking("sqlite.read", lambda: "overflow")
        release.set()
        self.assertEqual(await queued, "ok")
        metrics = self.runtime.snapshot()["operations"]["sqlite.transaction"]
        self.assertEqual(metrics["timed_out"], 1)
        self.assertEqual(metrics["completed"], 1)

    async def test_timeout_includes_time_waiting_in_queue(self) -> None:
        release = threading.Event()
        started = threading.Event()

        def blocked() -> None:
            started.set()
            release.wait(1)

        active = asyncio.create_task(self.runtime.run_blocking("disk", blocked))
        await asyncio.to_thread(started.wait, 1)
        queued_started = time.perf_counter()
        with self.assertRaises(BlockingOperationTimeout):
            await self.runtime.run_blocking(
                "disk.queued", lambda: None, timeout=0.02
            )
        self.assertLess(time.perf_counter() - queued_started, 0.2)
        self.assertEqual(self.runtime.snapshot()["executor"]["queued"], 0)
        release.set()
        await active

    async def test_cancellation_is_prompt_and_work_remains_accounted(self) -> None:
        release = threading.Event()
        started = threading.Event()

        def blocked_provider() -> None:
            started.set()
            release.wait(1)

        task = asyncio.create_task(
            self.runtime.run_blocking("provider.legacy", blocked_provider)
        )
        await asyncio.to_thread(started.wait, 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.runtime.snapshot()["executor"]["active"], 1)
        release.set()
        for _ in range(20):
            if self.runtime.snapshot()["executor"]["active"] == 0:
                break
            await asyncio.sleep(0.005)
        self.assertEqual(self.runtime.snapshot()["executor"]["active"], 0)
        self.assertEqual(
            self.runtime.snapshot()["operations"]["provider.legacy"]["cancelled"],
            1,
        )

    async def test_loop_lag_probe_attributes_direct_blocking(self) -> None:
        await asyncio.sleep(0.01)
        time.sleep(0.03)
        await asyncio.sleep(0.01)
        snapshot = self.runtime.snapshot()["event_loop"]
        self.assertGreater(snapshot["max_lag_ms"], 10)

    async def test_native_async_deadline_and_cancellation_metrics(self) -> None:
        with self.assertRaises(TimeoutError):
            await self.runtime.observe(
                "peer.http", asyncio.sleep(1), timeout=0.01
            )
        metrics = self.runtime.snapshot()["operations"]["peer.http"]
        self.assertEqual(metrics["timed_out"], 1)
        self.assertEqual(metrics["active"], 0)

    async def test_synchronous_plugin_hook_runs_on_bounded_executor(self) -> None:
        hooks = HookBus()
        hooks.set_async_runtime(self.runtime)
        loop_thread = threading.get_ident()
        hook_thread = None

        def handler() -> str:
            nonlocal hook_thread
            hook_thread = threading.get_ident()
            time.sleep(0.02)
            return "ok"

        hooks.on("plugin.refresh", handler)
        event = await hooks.emit("plugin.refresh")
        self.assertEqual(event.results, ["ok"])
        self.assertNotEqual(hook_thread, loop_thread)
        self.assertEqual(
            self.runtime.snapshot()["operations"]["hook.plugin.refresh"][
                "completed"
            ],
            1,
        )


if __name__ == "__main__":
    unittest.main()
