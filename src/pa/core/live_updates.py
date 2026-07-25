from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any


class LiveUpdateBroker:
    """Thread-safe fan-out for lightweight browser invalidation events."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscribers: dict[str, set[asyncio.Queue[dict[str, Any]]]] = defaultdict(
            set
        )

    def start(self) -> None:
        self._loop = asyncio.get_running_loop()

    def subscribe(self, realm_id: str) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=8)
        self._subscribers[realm_id].add(queue)
        return queue

    def unsubscribe(self, realm_id: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        subscribers = self._subscribers.get(realm_id)
        if not subscribers:
            return
        subscribers.discard(queue)
        if not subscribers:
            self._subscribers.pop(realm_id, None)

    def publish(self, realm_id: str, event: dict[str, Any]) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(self._publish_on_loop, realm_id, event)

    def _publish_on_loop(self, realm_id: str, event: dict[str, Any]) -> None:
        for queue in tuple(self._subscribers.get(realm_id, ())):
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(event)
