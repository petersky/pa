"""Forced viewers join live work before an asynchronous cache read can race it."""
import asyncio
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pa.config import Settings
from pa.domain.models import FleetInstance
from pa.fleet import overview


class FleetProbeEntrySingleFlightTests(unittest.IsolatedAsyncioTestCase):
    async def test_forced_caller_joins_before_cache_read_can_outlive_probe(self):
        await self._assert_entry_case("reachability")

    async def test_cold_mcp_bootstrap_joins_the_task_captured_before_cache_reads(self):
        await self._assert_entry_case("mcp_bootstrap")

    async def _assert_entry_case(self, dimension):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = SimpleNamespace(settings=Settings(data_dir=Path(tmp), instance_id="local"), services={})
            inst = FleetInstance(instance_id="remote", name="remote", url="http://remote:8080")
            older_started = asyncio.Event()
            release_older = asyncio.Event()
            newer_entered = asyncio.Event()
            release_newer_cache = asyncio.Event()
            calls = 0
            newer_task = None
            native_offload = overview._offload

            async def probe(*_args):
                nonlocal calls
                calls += 1
                if calls == 1:
                    older_started.set()
                    await release_older.wait()
                return overview.field("timeout", None, duration_ms=2500, error="fixture deadline")

            async def offload(ctx, operation, call, *args, **kwargs):
                if asyncio.current_task() is newer_task and call is overview.cache_for:
                    await release_newer_cache.wait()
                return await native_offload(ctx, operation, call, *args, **kwargs)

            async def newer():
                nonlocal newer_task
                newer_task = asyncio.current_task()
                newer_entered.set()
                return await overview.probe_dimension(ctx, inst, dimension, force=True)

            with patch.object(overview, "_probe", side_effect=probe), patch.object(overview, "_offload", side_effect=offload):
                older = asyncio.create_task(overview.probe_dimension(ctx, inst, dimension, force=True))
                await older_started.wait()
                second = asyncio.create_task(newer())
                await newer_entered.wait()
                release_older.set()
                older_result = await older
                # The older task has completed and left the flight registry.
                # A cache read performed before joining would now start a new probe.
                release_newer_cache.set()
                newer_result = await second

            self.assertEqual(calls, 1)
            self.assertEqual(older_result, newer_result)
            self.assertEqual(newer_result["last_attempt_state"], "timeout")

    async def test_cancelled_forced_viewer_does_not_cancel_shared_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = SimpleNamespace(settings=Settings(data_dir=Path(tmp), instance_id="local"), services={})
            inst = FleetInstance(instance_id="remote", name="remote", url="http://remote:8080")
            entered, joined, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
            native_shield = asyncio.shield
            viewer_task = None
            calls = 0

            async def probe(*_args):
                nonlocal calls
                calls += 1
                entered.set()
                await release.wait()
                return overview.field("fresh", {"health": "up"})

            def shield(awaitable):
                result = native_shield(awaitable)
                if asyncio.current_task() is viewer_task:
                    joined.set()
                return result

            async def viewer():
                nonlocal viewer_task
                viewer_task = asyncio.current_task()
                return await overview.probe_dimension(ctx, inst, "reachability", force=True)

            with patch.object(overview, "_probe", side_effect=probe), patch.object(overview.asyncio, "shield", side_effect=shield):
                older = asyncio.create_task(overview.probe_dimension(ctx, inst, "reachability", force=True))
                await entered.wait()
                second = asyncio.create_task(viewer())
                await joined.wait()
                second.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await second
                self.assertFalse(older.done())
                release.set()
                result = await older
            self.assertEqual(calls, 1)
            self.assertEqual(result["value"], {"health": "up"})

    async def test_connected_mcp_cache_still_returns_while_a_probe_is_active(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = SimpleNamespace(settings=Settings(data_dir=Path(tmp), instance_id="local"), services={})
            inst = FleetInstance(instance_id="remote", name="remote", url="http://remote:8080")
            entered, release = asyncio.Event(), asyncio.Event()
            calls = 0

            async def probe(*_args):
                nonlocal calls
                calls += 1
                entered.set()
                await release.wait()
                return overview.field("timeout", None, error="fixture deadline")

            with patch.object(overview, "_probe", side_effect=probe):
                older = asyncio.create_task(overview.probe_dimension(ctx, inst, "mcp_bootstrap", force=True))
                await entered.wait()
                cache = overview.cache_for(Path(tmp))
                cache.put("remote", "mcp_bootstrap", overview.field(
                    "fresh", {"state": "connected"}, observed_at=datetime.now(UTC).isoformat(),
                ))
                try:
                    result = await asyncio.wait_for(overview.probe_dimension(ctx, inst, "mcp_bootstrap", force=True), 5)
                    self.assertFalse(older.done())
                    self.assertTrue(result["cache_hit"])
                    self.assertEqual(result["value"], {"state": "connected"})
                finally:
                    release.set()
                    await older
            self.assertEqual(calls, 1)
