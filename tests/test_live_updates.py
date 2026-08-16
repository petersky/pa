from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pa.core.live_updates import LiveUpdateBroker
from pa.modules.fleet import fleet_workshop_events
from pa.modules.items import card_events


class LiveUpdateBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def test_threaded_publish_is_realm_scoped(self) -> None:
        broker = LiveUpdateBroker()
        broker.start()
        default = broker.subscribe("default")
        other = broker.subscribe("other")

        thread = threading.Thread(
            target=broker.publish,
            args=("default", {"type": "cards_changed", "head": "abc"}),
        )
        thread.start()
        thread.join()

        self.assertEqual(
            await asyncio.wait_for(default.get(), timeout=1.0),
            {"type": "cards_changed", "head": "abc"},
        )
        self.assertTrue(other.empty())

    async def test_slow_subscriber_keeps_the_newest_updates(self) -> None:
        broker = LiveUpdateBroker()
        broker.start()
        queue = broker.subscribe("default")

        for seq in range(10):
            broker.publish("default", {"seq": seq})
        await asyncio.sleep(0)

        updates = []
        while not queue.empty():
            updates.append(queue.get_nowait())
        self.assertEqual(len(updates), 8)
        self.assertEqual(updates[0]["seq"], 2)
        self.assertEqual(updates[-1]["seq"], 9)

    async def test_card_event_stream_emits_named_sse_invalidation(self) -> None:
        broker = LiveUpdateBroker()
        broker.start()
        ctx = MagicMock()
        ctx.require_service.return_value = broker
        request = MagicMock()
        request.app.state.ctx = ctx
        request.is_disconnected = AsyncMock(return_value=False)

        response = await card_events(request, realm="default")
        iterator = response.body_iterator
        self.assertEqual(await anext(iterator), ": connected\n\n")

        broker.publish(
            "default",
            {
                "type": "cards_changed",
                "realm_id": "default",
                "head": "abc",
                "source": "sync",
            },
        )
        event = await asyncio.wait_for(anext(iterator), timeout=1.0)

        self.assertIn("event: cards-changed\n", event)
        self.assertIn('"realm_id":"default"', event)
        self.assertEqual(response.media_type, "text/event-stream")
        await iterator.aclose()

    async def test_card_event_stream_exits_when_server_shutdown_begins(self) -> None:
        from pa.server.shutdown import reset_shutdown_event, signal_shutdown

        broker = LiveUpdateBroker()
        broker.start()
        ctx = MagicMock()
        ctx.require_service.return_value = broker
        request = MagicMock()
        request.app.state.ctx = ctx
        request.is_disconnected = AsyncMock(return_value=False)

        reset_shutdown_event()
        try:
            response = await card_events(request, realm="default")
            iterator = response.body_iterator
            self.assertEqual(await anext(iterator), ": connected\n\n")
            next_chunk = asyncio.create_task(anext(iterator))
            await asyncio.sleep(0)
            signal_shutdown()
            with self.assertRaises(StopAsyncIteration):
                await asyncio.wait_for(next_chunk, timeout=1.0)
            await iterator.aclose()
        finally:
            reset_shutdown_event()

    async def test_workshop_event_stream_exits_when_server_shutdown_begins(self) -> None:
        from pa.server.shutdown import reset_shutdown_event, signal_shutdown

        ctx = MagicMock()
        ctx.settings.data_dir = "/tmp"
        fleet = MagicMock()
        fleet.list_instances.return_value = []
        peer_table = MagicMock()
        peer_table.all_routes.return_value = []
        ctx.require_service.side_effect = lambda name: {
            "fleet_registry": fleet,
            "peer_table": peer_table,
        }[name]
        request = MagicMock()
        request.app.state.ctx = ctx
        request.is_disconnected = AsyncMock(return_value=False)

        reset_shutdown_event()
        try:
            with (
                patch("pa.modules.fleet.require_user"),
                patch(
                    "pa.modules.fleet._refresh_workshop_dimensions",
                    new=AsyncMock(return_value={}),
                ),
                patch("pa.modules.fleet._build_workshop", return_value={"generation": 1}),
            ):
                response = await fleet_workshop_events(request)
                iterator = response.body_iterator
                await anext(iterator)
                next_chunk = asyncio.create_task(anext(iterator))
                await asyncio.sleep(0)
                signal_shutdown()
                with self.assertRaises(StopAsyncIteration):
                    await asyncio.wait_for(next_chunk, timeout=1.0)
                await iterator.aclose()
        finally:
            reset_shutdown_event()
