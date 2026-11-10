"""Workshop live stream: shared builds, off-loop rebuilds and the slim floor view."""

from __future__ import annotations

import asyncio
import json
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.fleet.workshop_feed import WorkshopFeed
from pa.instance.agent_session import reset_instance_agent
from pa.modules.fleet import fleet_workshop, fleet_workshop_events


def _ctx(tmp_path: Path, ttl: float = 1.5):
    ctx = MagicMock()
    ctx.settings = SimpleNamespace(
        data_dir=str(tmp_path), workshop_snapshot_ttl_seconds=ttl
    )
    ctx.services = {}
    fleet = MagicMock()
    fleet.list_instances.return_value = []
    peer_table = MagicMock()
    peer_table.all_routes.return_value = []
    ctx.require_service.side_effect = lambda name: {
        "fleet_registry": fleet,
        "peer_table": peer_table,
    }[name]
    return ctx


def _request(ctx, disconnected=None):
    request = MagicMock()
    request.app.state.ctx = ctx
    request.is_disconnected = AsyncMock(
        side_effect=disconnected or (lambda: False), return_value=False
    )
    return request


class _Builder:
    """Stands in for _build_workshop; records where and how often it ran."""

    def __init__(self, delay: float = 0.0):
        self.calls = 0
        self.threads: list[int] = []
        self.delay = delay
        self.age = 0
        self.title = "Build it"

    def __call__(self, ctx, instances, routes, observations=None):
        import time

        self.calls += 1
        self.threads.append(threading.get_ident())
        if self.delay:
            time.sleep(self.delay)
        self.age += 1
        return {
            "schema": "pa.workshop/v2",
            "generated_at": f"2026-11-10T00:00:{self.calls:02d}+00:00",
            "bays": [],
            "work_orders": [
                {"id": "c", "title": self.title, "progress_age_seconds": self.age}
            ],
        }


@pytest.fixture
def no_wait():
    """Skip the 2 s pause between stream iterations."""
    with patch("pa.server.shutdown.wait_for_shutdown", new=AsyncMock(return_value=False)):
        yield


def _patches(builder):
    return (
        patch("pa.modules.fleet.require_user"),
        patch(
            "pa.modules.fleet._refresh_workshop_dimensions",
            new=AsyncMock(return_value={}),
        ),
        patch("pa.modules.fleet._build_workshop", new=builder),
    )


@pytest.mark.asyncio
async def test_snapshot_rebuild_runs_off_the_event_loop(tmp_path):
    builder = _Builder()
    loop_thread = threading.get_ident()
    p1, p2, p3 = _patches(builder)
    with p1, p2, p3:
        response = await fleet_workshop(_request(_ctx(tmp_path)))
    assert response.status_code == 200
    assert json.loads(response.body)["work_orders"][0]["id"] == "c"
    assert builder.calls == 1
    assert builder.threads[0] != loop_thread


@pytest.mark.asyncio
async def test_stream_rebuild_runs_off_the_event_loop(tmp_path, no_wait):
    builder = _Builder()
    loop_thread = threading.get_ident()
    p1, p2, p3 = _patches(builder)
    with p1, p2, p3:
        response = await fleet_workshop_events(_request(_ctx(tmp_path)))
        iterator = response.body_iterator
        await anext(iterator)
        await iterator.aclose()
    assert builder.threads and builder.threads[0] != loop_thread


@pytest.mark.asyncio
async def test_three_subscribers_share_one_snapshot_build(tmp_path, no_wait):
    builder = _Builder(delay=0.05)
    ctx = _ctx(tmp_path)
    p1, p2, p3 = _patches(builder)
    with p1, p2, p3:
        responses = [
            await fleet_workshop_events(_request(ctx)) for _ in range(3)
        ]
        iterators = [response.body_iterator for response in responses]
        first = await asyncio.gather(*(anext(it) for it in iterators))
        second = await asyncio.gather(*(anext(it) for it in iterators))
        for iterator in iterators:
            await iterator.aclose()
    assert builder.calls == 1
    assert all("event: snapshot" in chunk for chunk in first)
    assert len({chunk for chunk in first}) == 1
    # Within the TTL nothing new was built, so every client is told "no change".
    assert second == [": workshop heartbeat\n\n"] * 3


@pytest.mark.asyncio
async def test_stream_heartbeats_when_only_ages_change_between_builds(tmp_path, no_wait):
    builder = _Builder()
    ctx = _ctx(tmp_path, ttl=0)
    p1, p2, p3 = _patches(builder)
    with p1, p2, p3:
        iterator = (await fleet_workshop_events(_request(ctx))).body_iterator
        first = await anext(iterator)
        second = await anext(iterator)  # rebuilt: only the age ticked
        builder.title = "Renamed"
        third = await anext(iterator)
        await iterator.aclose()
    assert builder.calls == 3
    assert "event: snapshot" in first
    assert second == ": workshop heartbeat\n\n"
    assert "event: snapshot" in third and "Renamed" in third


@pytest.mark.asyncio
async def test_departing_subscriber_does_not_cancel_the_shared_build(tmp_path, no_wait):
    builder = _Builder(delay=0.2)
    ctx = _ctx(tmp_path)
    p1, p2, p3 = _patches(builder)
    with p1, p2, p3:
        stayer = (await fleet_workshop_events(_request(ctx))).body_iterator
        leaver = (await fleet_workshop_events(_request(ctx))).body_iterator
        staying = asyncio.create_task(anext(stayer))
        leaving = asyncio.create_task(anext(leaver))
        await asyncio.sleep(0.05)
        leaving.cancel()
        with pytest.raises(asyncio.CancelledError):
            await leaving
        chunk = await asyncio.wait_for(staying, 2)
        await stayer.aclose()
    assert builder.calls == 1
    assert "event: snapshot" in chunk


@pytest.mark.asyncio
async def test_disconnected_client_stops_the_stream(tmp_path, no_wait):
    builder = _Builder()
    ctx = _ctx(tmp_path)
    states = iter([False, True])
    p1, p2, p3 = _patches(builder)
    with p1, p2, p3:
        request = _request(ctx)
        request.is_disconnected = AsyncMock(side_effect=lambda: next(states))
        iterator = (await fleet_workshop_events(request)).body_iterator
        await anext(iterator)
        with pytest.raises(StopAsyncIteration):
            await anext(iterator)


@pytest.mark.asyncio
async def test_stream_view_floor_ships_the_slim_projection(tmp_path, no_wait):
    class Heavy(_Builder):
        def __call__(self, *args, **kwargs):
            snapshot = super().__call__(*args, **kwargs)
            snapshot["areas"] = {"active": [{"id": "c", "summary": "x" * 5000}]}
            snapshot["counts"] = {"total": 1}
            return snapshot

    p1, p2, p3 = _patches(Heavy())
    with p1, p2, p3:
        full_it = (
            await fleet_workshop_events(_request(_ctx(tmp_path)))
        ).body_iterator
        floor_it = (
            await fleet_workshop_events(_request(_ctx(tmp_path)), view="floor")
        ).body_iterator
        full, floor = await anext(full_it), await anext(floor_it)
        await full_it.aclose()
        await floor_it.aclose()
    full_event = json.loads(full.split("data: ", 1)[1])
    floor_event = json.loads(floor.split("data: ", 1)[1])
    assert "areas" in full_event and "view" not in full_event
    assert floor_event["view"] == "floor" and "areas" not in floor_event
    assert len(floor) * 5 < len(full)


@pytest.mark.asyncio
async def test_unknown_view_is_rejected(tmp_path):
    from fastapi import HTTPException

    p1, p2, p3 = _patches(_Builder())
    with p1, p2, p3, pytest.raises(HTTPException) as caught:
        await fleet_workshop(_request(_ctx(tmp_path)), view="nope")
    assert caught.value.status_code == 422


@pytest.mark.asyncio
async def test_feed_joins_the_in_flight_build_and_rebuilds_after_ttl():
    feed = WorkshopFeed()
    started = 0
    release = asyncio.Event()

    async def produce():
        nonlocal started
        started += 1
        await release.wait()
        return {"work_orders": [], "generated_at": str(started)}

    async def offload(_operation, call, *args):
        return call(*args)

    waiting = [
        asyncio.create_task(feed.frame("full", produce, offload, max_age=0))
        for _ in range(4)
    ]
    await asyncio.sleep(0.01)
    release.set()
    frames = await asyncio.gather(*waiting)
    assert started == 1 and len({frame.generation for frame in frames}) == 1
    # Fresh enough: reused. Zero max_age: rebuilt.
    again = await feed.frame("full", produce, offload, max_age=60)
    assert started == 1 and again.generation == frames[0].generation
    rebuilt = await feed.frame("full", produce, offload, max_age=0)
    assert started == 2 and rebuilt.generation == frames[0].generation + 1
    # Content did not change, so subscribers keep the earlier sequence.
    assert rebuilt.sequence == frames[0].sequence


def _client_settings(tmp: str) -> Settings:
    return Settings(
        data_dir=Path(tmp),
        instance_id="local",
        instance_name="Workshop host",
        instance_url="http://workshop.test",
        agent_enabled=False,
        peers=[],
    )


def test_floor_view_endpoint_and_page_are_slim_and_full_stays_available():
    reset_store()
    reset_instance_agent()
    with tempfile.TemporaryDirectory() as tmp:
        try:
            app = Kernel.boot(settings=_client_settings(tmp)).build_app()
            with TestClient(app) as client:
                full = client.get("/api/fleet/workshop")
                floor = client.get("/api/fleet/workshop?view=floor")
                bad = client.get("/api/fleet/workshop?view=nope")
                assert full.status_code == floor.status_code == 200
                assert bad.status_code == 422
                assert full.headers["content-type"].startswith("application/json")
                assert "areas" in full.json() and "counts" in full.json()
                body = floor.json()
                assert body["view"] == "floor" and "areas" not in body
                assert body["bays"][0]["id"] == "local"
                assert "orphan_sessions" in body
                page = client.get("/workshop")
                embedded = page.text.split('id="pa-workshop-data">', 1)[1]
                assert '"view": "floor"' in embedded.split("</script>", 1)[0]
        finally:
            reset_instance_agent()
            reset_store()
