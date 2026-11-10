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


class WorkshopFloorPublicationTests(unittest.IsolatedAsyncioTestCase):
    """Dispatch stage moves and PR gate changes reach the floor as small ids."""

    async def asyncSetUp(self) -> None:
        import tempfile
        from pathlib import Path

        from pa.execution.dispatch import DispatchRecord, DispatchStore
        from pa.fleet.workshop_transitions import WorkshopTransitionPublisher, floor_topic

        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.broker = LiveUpdateBroker()
        self.broker.start()
        self.floor = self.broker.subscribe(floor_topic("default"))
        self.cards = self.broker.subscribe("default")
        self.publisher = WorkshopTransitionPublisher(lambda: self.broker)
        self.store = DispatchStore(Path(self._tmp.name))
        self.store.transition_observer = self.publisher.dispatch_changed
        self.record = DispatchRecord(
            mutation_id="m1",
            idempotency_key="k1",
            request_fingerprint="k1",
            placement_request_fingerprint="k1",
            card_id="card-1",
            authority_instance_id="authority",
            authority_url="http://authority.test",
            target_instance_id="local",
            placement_policy="best_match",
            placement_decision={"chosen_instance_id": "local"},
        )

    async def _drain(self, queue) -> list[dict]:
        await asyncio.sleep(0.01)
        items = []
        while not queue.empty():
            items.append(queue.get_nowait())
        return items

    async def test_dispatch_stage_changes_publish_ids_and_new_state_only(self) -> None:
        record, _ = self.store.admit(self.record)
        self.store.transition(record, "materializing", "preparing")
        self.store.transition(record, "starting_session", "starting")
        events = await self._drain(self.floor)

        self.assertEqual(
            [(e["state"], e["previous_state"]) for e in events if e["kind"] == "dispatch"][-2:],
            [("materializing", "queued"), ("starting_session", "materializing")],
        )
        latest = events[-1]
        self.assertEqual(latest["type"], "workshop_floor")
        self.assertEqual(latest["realm_id"], "default")
        self.assertEqual(latest["dispatch_id"], record.dispatch_id)
        self.assertEqual(latest["card_id"], "card-1")
        self.assertEqual(latest["target_instance_id"], "local")
        # ids and the new state, never a snapshot
        self.assertLess(len(str(latest)), 400)
        self.assertEqual([e["seq"] for e in events], list(range(1, len(events) + 1)))
        # Floor moves never reach card-board consumers of the realm topic.
        self.assertEqual(await self._drain(self.cards), [])

    async def test_unchanged_state_writes_do_not_publish(self) -> None:
        record, _ = self.store.admit(self.record)
        await self._drain(self.floor)
        record.last_error = "noise"
        self.store.put(record)
        self.assertEqual(await self._drain(self.floor), [])

    async def test_pr_gate_changes_publish_only_when_the_gate_moves(self) -> None:
        from pa.pr_supervisor.models import PRWatch
        from pa.pr_supervisor.store import PRSupervisorStore
        from pathlib import Path

        store = PRSupervisorStore(Path(self._tmp.name) / "supervisor.db")
        store.watch_observer = self.publisher.watch_changed
        failing = [{"name": "t", "status": "completed", "conclusion": "failure"}]
        watch = PRWatch(
            id="w1", realm_id="default", card_id="card-1", repository="o/r",
            pr_number=4, pr_url="https://github.com/o/r/pull/4", head_sha="a",
            state={"state": "open", "draft": False, "checks": failing},
        )
        store.upsert_watch(watch, preserve_lease=False)
        store.upsert_watch(watch, preserve_lease=False)  # identical gate
        passing = [{"name": "t", "status": "completed", "conclusion": "success"}]
        store.upsert_watch(
            watch.model_copy(update={"state": {"state": "open", "checks": passing}}),
            preserve_lease=False,
        )
        events = await self._drain(self.floor)
        self.assertEqual(
            [(e["kind"], e["active"], e["reasons"]) for e in events],
            [("gate", True, ["ci_failing"]), ("gate", True, [])],
        )
        self.assertEqual(events[0]["card_id"], "card-1")
        self.assertEqual(events[0]["pr_number"], 4)
        self.assertNotIn("state", events[0])

    async def test_merged_watch_leaves_the_gantry(self) -> None:
        from pa.pr_supervisor.models import PRWatch, PRWatchStatus
        from pa.pr_supervisor.store import PRSupervisorStore
        from pathlib import Path

        store = PRSupervisorStore(Path(self._tmp.name) / "supervisor.db")
        store.watch_observer = self.publisher.watch_changed
        store.upsert_watch(
            PRWatch(id="w2", realm_id="default", card_id="c", repository="o/r",
                    pr_number=5, pr_url="https://github.com/o/r/pull/5", head_sha="a",
                    state={"state": "open"}),
            preserve_lease=False,
        )
        store.set_terminal("w2", PRWatchStatus.MERGED)
        events = await self._drain(self.floor)
        self.assertEqual([(e["status"], e["active"]) for e in events],
                         [("active", True), ("merged", False)])

    async def test_card_event_stream_names_floor_moves_only_when_asked(self) -> None:
        ctx = MagicMock()
        ctx.require_service.return_value = self.broker
        request = MagicMock()
        request.app.state.ctx = ctx
        request.is_disconnected = AsyncMock(return_value=False)

        plain = (await card_events(request, realm="default")).body_iterator
        joined = (
            await card_events(request, realm="default", include="workshop")
        ).body_iterator
        self.assertEqual(await anext(plain), ": connected\n\n")
        self.assertEqual(await anext(joined), ": connected\n\n")

        self.publisher.dispatch_changed(None, self.record.model_copy(
            update={"state": "queued", "realm_id": "default"}))
        self.broker.publish("default", {"type": "cards_changed", "realm_id": "default"})

        first = await asyncio.wait_for(anext(joined), timeout=1.0)
        second = await asyncio.wait_for(anext(joined), timeout=1.0)
        self.assertEqual(
            sorted(chunk.split("\n", 1)[0] for chunk in (first, second)),
            ["event: cards-changed", "event: workshop-floor"],
        )
        # The plain stream only ever carries the card invalidation.
        only = await asyncio.wait_for(anext(plain), timeout=1.0)
        self.assertTrue(only.startswith("event: cards-changed\n"))
        await plain.aclose()
        await joined.aclose()
        # Both streams released their queues; only the fixture's own remains.
        self.assertEqual(self.broker._subscribers["default"], {self.cards})
        self.assertEqual(self.broker._subscribers.get("workshop-floor:default"), {self.floor})


class WorkshopFloorWiringTests(unittest.TestCase):
    def test_booted_stores_publish_to_the_shared_live_update_broker(self) -> None:
        import tempfile
        from pathlib import Path

        from pa.config import Settings
        from pa.core.kernel import Kernel
        from pa.domain.store import reset_store
        from pa.instance.agent_session import reset_instance_agent

        reset_store()
        reset_instance_agent()
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp), instance_id="local", agent_enabled=False, peers=[],
            )
            try:
                kernel = Kernel.boot(settings=settings)
                services = kernel.ctx.services
                publisher = services["workshop_transitions"]
                self.assertEqual(
                    services["dispatch_store"].transition_observer, publisher.dispatch_changed
                )
                self.assertEqual(
                    services["pr_supervisor_store"].watch_observer, publisher.watch_changed
                )
            finally:
                reset_instance_agent()
                reset_store()
