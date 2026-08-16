"""Process-wide graceful-shutdown coordination for long-lived responses."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

import uvicorn

T = TypeVar("T")

_shutdown_event: asyncio.Event | None = None
_shutdown_loop: asyncio.AbstractEventLoop | None = None
_shutdown_flag = False


def reset_shutdown_event() -> asyncio.Event:
    """Create the event used by the next server run."""
    global _shutdown_event, _shutdown_loop, _shutdown_flag
    _shutdown_flag = False
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
        if _shutdown_flag:
            _shutdown_event.set()
    elif loop is not None and _shutdown_loop is None:
        _shutdown_loop = loop
    return _shutdown_event


def signal_shutdown() -> None:
    """Mark shutdown from any thread, including Uvicorn's signal handler."""
    global _shutdown_flag
    _shutdown_flag = True
    event = _shutdown_event
    loop = _shutdown_loop
    if event is None:
        event = shutdown_event()
        loop = _shutdown_loop
    if loop is not None:
        try:
            if loop.is_running():
                loop.call_soon_threadsafe(event.set)
                return
        except RuntimeError:
            pass
    event.set()


def is_shutting_down() -> bool:
    # Do not call shutdown_event() here: a logging filter or signal-handler
    # context can see a different loop and would otherwise replace a set event
    # with a fresh unset one.
    return _shutdown_flag or (
        _shutdown_event is not None and _shutdown_event.is_set()
    )


async def wait_for_shutdown(timeout: float | None = None) -> bool:
    """Return True when shutdown begins, or False when *timeout* elapses."""
    if is_shutting_down():
        return True
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
    if is_shutting_down():
        operation_task.cancel()
        await asyncio.gather(operation_task, return_exceptions=True)
        return True, None
    shutdown_task = asyncio.create_task(shutdown_event().wait())
    try:
        done, _ = await asyncio.wait(
            {operation_task, shutdown_task},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            raise TimeoutError
        if shutdown_task in done or is_shutting_down():
            return True, None
        return False, await operation_task
    finally:
        for task in (operation_task, shutdown_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(operation_task, shutdown_task, return_exceptions=True)


class ShutdownAwareServer(uvicorn.Server):
    """Notify response streams as soon as Uvicorn receives TERM/INT."""

    async def startup(self, sockets: list | None = None) -> None:
        shutdown_event()
        await super().startup(sockets=sockets)

    def handle_exit(self, sig: int, frame) -> None:
        signal_shutdown()
        super().handle_exit(sig, frame)
