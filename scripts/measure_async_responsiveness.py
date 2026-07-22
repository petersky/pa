#!/usr/bin/env python3
"""Repeatable synthetic latency benchmark for PA async boundaries.

The delay injection is intentional: each label corresponds to a proven call
path in docs/ASYNC_BLOCKING_AUDIT.md.  The benchmark compares the historical
event-loop behavior with PA's bounded worker/native-async boundary and reports
the latency of an unrelated request-sized coroutine.
"""

from __future__ import annotations

import asyncio
import json
import time

from pa.core.async_runtime import AsyncRuntime


BLOCKING_SCENARIOS = (
    "git",
    "disk",
    "sqlite",
    "provider",
    "sync",
    "pr_supervisor",
    "subprocess",
)
NATIVE_ASYNC_SCENARIOS = ("peer_http", "browser_startup")
DELAY_SECONDS = 0.1


async def unrelated_latency(action) -> float:
    started = time.perf_counter()

    async def unrelated() -> float:
        await asyncio.sleep(0.005)
        return (time.perf_counter() - started) * 1000

    probe = asyncio.create_task(unrelated())
    await asyncio.sleep(0)
    await action()
    return await probe


async def main() -> None:
    runtime = AsyncRuntime(
        max_workers=4,
        max_queue=16,
        default_timeout=1,
        lag_interval_seconds=0.005,
        slow_call_seconds=1,
    )
    await runtime.start()
    rows = []
    try:
        for name in BLOCKING_SCENARIOS:
            before = await unrelated_latency(
                lambda: _direct_block(DELAY_SECONDS)
            )
            after = await unrelated_latency(
                lambda: runtime.run_blocking(
                    f"benchmark.{name}", time.sleep, DELAY_SECONDS
                )
            )
            rows.append(
                {
                    "scenario": name,
                    "baseline_unrelated_ms": round(before, 3),
                    "bounded_unrelated_ms": round(after, 3),
                }
            )
        for name in NATIVE_ASYNC_SCENARIOS:
            latency = await unrelated_latency(
                lambda: runtime.observe(
                    f"benchmark.{name}", asyncio.sleep(DELAY_SECONDS), timeout=1
                )
            )
            rows.append(
                {
                    "scenario": name,
                    "baseline_unrelated_ms": None,
                    "bounded_unrelated_ms": round(latency, 3),
                }
            )
        print(
            json.dumps(
                {
                    "delay_ms": DELAY_SECONDS * 1000,
                    "scenarios": rows,
                    "runtime": runtime.snapshot(),
                },
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        await runtime.close()


async def _direct_block(delay: float) -> None:
    time.sleep(delay)


if __name__ == "__main__":
    asyncio.run(main())
