"""Process-wide graceful-shutdown coordination for long-lived responses."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

import uvicorn

T = TypeVar("T")

_shutdown_event: asyncio.Event | None = None
_shutdown_loop: asyncio.AbstractEventLoop | None = None


def reset_shutdown_event() -> asyncio.Event:
    """Create the event used by the next server run."""
    global _shutdown_event, _shutdown_loop
    _shutdown_event = asyncio.Event()
    try:
        _shutdown_loop = asyncio.get_running_loop()
    except RuntimeError:
        _shutdown_loop = None
    return _shutdown_event


def shutdown_event() -> asyncio.Event:
    global _shutdown_event, _shutdown_loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    # Test clients and embedded uses may run multiple event loops in one process.
    # A production Uvicorn run has one loop, but never carry a bound Event into a
    # later loop where wait() would fail or look like an early shutdown.
    if _shutdown_event is None or (
        loop is not None and _shutdown_loop is not None and loop is not _shutdown_loop
    ):
        _shutdown_event = asyncio.Event()
        _shutdown_loop = loop
    elif loop is not None and _shutdown_loop is None:
        _shutdown_loop = loop
    return _shutdown_event


def signal_shutdown() -> None:
    shutdown_event().set()


def is_shutting_down() -> bool:
    return shutdown_event().is_set()


async def wait_for_shutdown(timeout: float | None = None) -> bool:
    """Return True when shutdown begins, or False when *timeout* elapses."""
    try:
        await asyncio.wait_for(shutdown_event().wait(), timeout=timeout)
    except TimeoutError:
        return False
    return True


async def wait_for_shutdown_or(
    operation: Awaitable[T], *, timeout: float | None = None
) -> tuple[bool, T | None]:
    """Race an operation against shutdown and cancel the losing waiter."""
    operation_task = asyncio.ensure_future(operation)
    shutdown_task = asyncio.create_task(shutdown_event().wait())
    try:
        done, _ = await asyncio.wait(
            {operation_task, shutdown_task},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            raise TimeoutError
        if shutdown_task in done:
            return True, None
        return False, await operation_task
    finally:
        for task in (operation_task, shutdown_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(operation_task, shutdown_task, return_exceptions=True)


class ShutdownAwareServer(uvicorn.Server):
    """Notify response streams as soon as Uvicorn receives TERM/INT."""

    def handle_exit(self, sig: int, frame) -> None:
        signal_shutdown()
        super().handle_exit(sig, frame)
