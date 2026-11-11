"""Shared, single-flight Workshop snapshot producer.

Every connected Workshop client used to trigger its own full rebuild every
couple of seconds. The feed builds one canonical snapshot at a time, lets every
subscriber and request join the build already in flight, and serves the latest
snapshot to subscribers for a short TTL. It also derives each view's change
digest and SSE event once per snapshot instead of once per client.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any

from pa.fleet.workshop import workshop_semantic_snapshot, workshop_view

Produce = Callable[[], Awaitable[dict[str, Any]]]
Offload = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class WorkshopFrame:
    """One view of one built snapshot, ready to write to any number of clients."""

    view: str
    generation: int
    sequence: int
    digest: str
    # Projection of the snapshot this frame was derived from (always current for
    # ``generation``); ``event`` is the SSE text of the last *meaningful* change.
    snapshot: dict[str, Any]
    event: str
    built_at: float


@dataclass(frozen=True)
class _Built:
    generation: int
    built_at: float
    snapshot: dict[str, Any]


def snapshot_payload(snapshot: dict[str, Any]) -> str:
    return json.dumps(snapshot, default=str, separators=(",", ":"))


def make_snapshot_event(sequence: int, payload: str) -> str:
    return f"id: {sequence}\nevent: snapshot\ndata: {payload}\n\n"


def semantic_digest(snapshot: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(workshop_semantic_snapshot(snapshot), sort_keys=True, default=str).encode()
    ).hexdigest()


def _make_frame(
    view: str, built: _Built, previous: WorkshopFrame | None
) -> WorkshopFrame:
    projected = workshop_view(built.snapshot, view)
    digest = semantic_digest(projected)
    if previous is not None and previous.digest == digest:
        # Nothing but clocks moved: keep the sequence and event so subscribers
        # that already saw it stay quiet.
        return replace(
            previous,
            generation=built.generation,
            snapshot=projected,
            built_at=built.built_at,
        )
    sequence = (previous.sequence if previous else 0) + 1
    payload = snapshot_payload(projected)
    return WorkshopFrame(
        view=view,
        generation=built.generation,
        sequence=sequence,
        digest=digest,
        snapshot=projected,
        event=make_snapshot_event(sequence, payload),
        built_at=built.built_at,
    )


class WorkshopFeed:
    def __init__(self) -> None:
        self._built: _Built | None = None
        self._frames: dict[str, WorkshopFrame] = {}
        self._flights: dict[Any, tuple[asyncio.AbstractEventLoop, asyncio.Task[Any]]] = {}
        self._generation = 0
        self.builds = 0

    async def _flight(self, key: Any, factory: Callable[[], Awaitable[Any]]) -> Any:
        loop = asyncio.get_running_loop()
        entry = self._flights.get(key)
        if entry is None or entry[0] is not loop or entry[1].done():
            task = loop.create_task(factory())  # type: ignore[arg-type]
            self._flights[key] = (loop, task)

            def finished(done: asyncio.Task[Any], key: Any = key) -> None:
                current = self._flights.get(key)
                if current is not None and current[1] is done:
                    self._flights.pop(key, None)
                if not done.cancelled():
                    done.exception()  # mark retrieved when every waiter left

            task.add_done_callback(finished)
        else:
            task = entry[1]
        # Shield so one departing subscriber never cancels the shared build.
        return await asyncio.shield(task)

    async def _latest(self, produce: Produce, max_age: float) -> _Built:
        built = self._built
        if built is not None and max_age > 0 and time.monotonic() - built.built_at < max_age:
            return built

        async def build() -> _Built:
            snapshot = await produce()
            self._generation += 1
            self.builds += 1
            self._built = _Built(self._generation, time.monotonic(), snapshot)
            return self._built

        return await self._flight("build", build)

    async def frame(
        self, view: str, produce: Produce, offload: Offload, *, max_age: float
    ) -> WorkshopFrame:
        """Return the latest frame for ``view``, building at most one snapshot at a time.

        ``max_age`` is how stale a previously built snapshot may be; ``0`` always
        joins or starts a build.
        """
        built = await self._latest(produce, max_age)
        current = self._frames.get(view)
        if current is not None and current.generation == built.generation:
            return current

        async def derive() -> WorkshopFrame:
            frame = await offload(
                "fleet.workshop_frame", _make_frame, view, built, self._frames.get(view)
            )
            held = self._frames.get(view)
            if held is None or held.generation <= frame.generation:
                self._frames[view] = frame
            return frame

        return await self._flight(("frame", view, built.generation), derive)
