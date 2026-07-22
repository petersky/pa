from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from pa.core.async_runtime import AsyncRuntime

logger = logging.getLogger(__name__)

HookHandler = Callable[..., Any | Awaitable[Any]]


@dataclass
class HookEvent:
    name: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    results: list[Any] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class HookBus:
    """Internal event bus for cross-module coordination and debugging."""

    def __init__(self, *, history_size: int = 200) -> None:
        self._handlers: dict[str, list[tuple[int, HookHandler]]] = {}
        self._history: deque[HookEvent] = deque(maxlen=history_size)
        self._record_history = False
        self._async_runtime: AsyncRuntime | None = None

    def set_async_runtime(self, runtime: AsyncRuntime) -> None:
        self._async_runtime = runtime

    def enable_history(self, enabled: bool = True) -> None:
        self._record_history = enabled

    def on(
        self,
        name: str,
        handler: HookHandler,
        *,
        priority: int = 0,
    ) -> Callable[[], None]:
        self._handlers.setdefault(name, []).append((priority, handler))
        self._handlers[name].sort(key=lambda item: item[0], reverse=True)

        def unsubscribe() -> None:
            self._handlers[name] = [
                item for item in self._handlers[name] if item[1] is not handler
            ]

        return unsubscribe

    async def emit(self, name: str, **payload: Any) -> HookEvent:
        event = HookEvent(name=name, payload=payload)
        handlers = [handler for _, handler in self._handlers.get(name, [])]

        for handler in handlers:
            try:
                if inspect.iscoroutinefunction(handler):
                    result = handler(**payload) if payload else handler()
                    result = await result
                elif self._async_runtime:
                    result = await self._async_runtime.run_blocking(
                        f"hook.{name}",
                        handler,
                        **payload,
                    )
                    # A sync plugin may return an awaitable. Resume it on the
                    # event loop without sacrificing ordered hook delivery.
                    if asyncio.iscoroutine(result):
                        result = await result
                else:
                    result = handler(**payload) if payload else handler()
                    if asyncio.iscoroutine(result):
                        result = await result
                event.results.append(result)
            except Exception as exc:
                msg = f"{handler!r}: {exc}"
                event.errors.append(msg)
                logger.exception("Hook %s failed: %s", name, handler)

        if self._record_history:
            self._history.append(event)

        return event

    def list_hooks(self) -> dict[str, int]:
        return {name: len(handlers) for name, handlers in self._handlers.items()}

    def history(self, *, limit: int = 50, name: str | None = None) -> list[HookEvent]:
        events = list(self._history)
        if name:
            events = [event for event in events if event.name == name]
        return events[-limit:]
