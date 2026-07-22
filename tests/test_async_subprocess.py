from __future__ import annotations

import asyncio
import sys
import time
import unittest

from pa.core.subprocesses import ProcessOutputLimitExceeded, run_process


class AsyncSubprocessTests(unittest.IsolatedAsyncioTestCase):
    async def test_captures_bounded_output(self) -> None:
        result = await run_process(
            [sys.executable, "-c", "print('ready')"], timeout=2
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "ready")

    async def test_output_limit_terminates_process(self) -> None:
        with self.assertRaises(ProcessOutputLimitExceeded):
            await run_process(
                [sys.executable, "-c", "print('x' * 100000)"],
                timeout=2,
                output_limit=1024,
            )

    async def test_timeout_cleans_up_process_group_promptly(self) -> None:
        started = time.perf_counter()
        with self.assertRaises(TimeoutError):
            await run_process(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                timeout=0.05,
            )
        self.assertLess(time.perf_counter() - started, 1)

    async def test_cancellation_cleans_up_process_group_promptly(self) -> None:
        task = asyncio.create_task(
            run_process(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                timeout=30,
            )
        )
        await asyncio.sleep(0.05)
        started = time.perf_counter()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertLess(time.perf_counter() - started, 1)


if __name__ == "__main__":
    unittest.main()
