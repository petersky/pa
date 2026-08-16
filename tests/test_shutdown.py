from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import patch

from pa.server.shutdown import (
    ShutdownAwareServer,
    is_shutting_down,
    reset_shutdown_event,
    signal_shutdown,
    wait_for_shutdown_or,
)


class ShutdownCoordinationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        reset_shutdown_event()

    async def asyncTearDown(self) -> None:
        reset_shutdown_event()

    async def test_shutdown_cancels_a_long_lived_stream_wait(self) -> None:
        operation_cancelled = asyncio.Event()

        async def operation() -> str:
            try:
                await asyncio.Future()
            finally:
                operation_cancelled.set()

        waiter = asyncio.create_task(wait_for_shutdown_or(operation()))
        await asyncio.sleep(0)
        signal_shutdown()

        stopping, value = await asyncio.wait_for(waiter, timeout=1.0)
        self.assertTrue(stopping)
        self.assertIsNone(value)
        self.assertTrue(operation_cancelled.is_set())

    async def test_server_signal_notifies_streams_before_uvicorn_drain(self) -> None:
        server = object.__new__(ShutdownAwareServer)
        with patch("uvicorn.Server.handle_exit") as parent:
            server.handle_exit(15, None)

        stopping, _ = await wait_for_shutdown_or(asyncio.sleep(60))
        self.assertTrue(stopping)
        parent.assert_called_once_with(15, None)

    async def test_signal_from_another_thread_wakes_waiters(self) -> None:
        started = asyncio.Event()

        async def waiter() -> tuple[bool, object | None]:
            started.set()
            return await wait_for_shutdown_or(asyncio.sleep(60))

        task = asyncio.create_task(waiter())
        await started.wait()
        await asyncio.sleep(0)
        thread = threading.Thread(target=signal_shutdown)
        thread.start()
        thread.join()
        stopping, value = await asyncio.wait_for(task, timeout=1.0)
        self.assertTrue(stopping)
        self.assertIsNone(value)
        self.assertTrue(is_shutting_down())
