"""Required-cache renewal, bounded retry and independent background ownership."""

import asyncio
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pa.domain.models import FleetInstance
from pa.fleet.overview import (
    _cache_is_fresh,
    field,
    refresh_required_dimension_loop,
    required_refresh_delay,
)


class RequiredRefreshDelayTests(unittest.TestCase):
    now = datetime(2026, 10, 24, tzinfo=UTC)

    def success(self, *, age=0, duration=20):
        return field("fresh", {}, observed_at=(self.now - timedelta(seconds=age)).isoformat(),
                     duration_ms=duration)

    def test_success_renews_before_unchanged_expiry(self):
        for dimension in ("reachability", "status", "sync"):
            value = self.success()
            self.assertAlmostEqual(required_refresh_delay(value, dimension, now=self.now), 2.7)
            self.assertTrue(_cache_is_fresh(value, dimension, now=self.now + timedelta(seconds=2.9)))
            self.assertFalse(_cache_is_fresh(value, dimension, now=self.now + timedelta(seconds=3)))

    def test_probe_duration_headroom_is_bounded(self):
        for duration, delay in ((1000, 2), (2500, 1.5), (-500, 2.7), (float("nan"), 2.7)):
            value = self.success()
            value["last_attempt_duration_ms"] = duration
            self.assertAlmostEqual(required_refresh_delay(value, "status", now=self.now), delay)
        self.assertEqual(required_refresh_delay(self.success(age=2.8), "status", now=self.now), 0)

    def test_failed_attempt_uses_last_attempt_not_old_good_time(self):
        value = self.success(age=100)
        value.update(last_attempt_state="timeout", last_attempted_at=(self.now - timedelta(seconds=4)).isoformat(), state="stale")
        self.assertEqual(required_refresh_delay(value, "sync", now=self.now), 11)
        for state in ("error", "unavailable", "stale"):
            value["last_attempt_state"] = state
            self.assertEqual(required_refresh_delay(value, "sync", now=self.now), 11)
        value["last_attempted_at"] = (self.now - timedelta(seconds=15)).isoformat()
        self.assertEqual(required_refresh_delay(value, "sync", now=self.now), 0)

    def test_missing_invalid_naive_future_success_is_due(self):
        for value in (None, {}, {"state": "fresh"}, self.success(age=-10)):
            self.assertEqual(required_refresh_delay(value, "status", now=self.now), 0)
        for timestamp in ("invalid", "2026-10-24T00:00:00"):
            value = self.success()
            value["observed_at"] = timestamp
            self.assertEqual(required_refresh_delay(value, "status", now=self.now), 0)


class RequiredRefreshWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def run_clock(self, *, cached=None, publish=True, fail=False):
        clock = [0.0]
        cache_value = [cached]
        starts, waits, forces = [], [], []
        origin = datetime(2026, 10, 24, tzinfo=UTC)

        class ClockDatetime:
            fromisoformat = staticmethod(datetime.fromisoformat)

            @staticmethod
            def now(_tz):
                return origin + timedelta(seconds=clock[0])

        async def offload(_ctx, _operation, call, *args, **kwargs):
            return call(*args, **kwargs)

        async def wait(delay):
            waits.append(delay)
            clock[0] += delay
            return len(waits) >= 3

        async def probe(_ctx, inst, dimension, *, force):
            starts.append(clock[0])
            forces.append(force)
            if fail:
                raise OSError("fixture-only publication failure")
            if publish:
                cache_value[0] = field("fresh", {}, observed_at=ClockDatetime.now(UTC).isoformat())

        ctx = SimpleNamespace(settings=SimpleNamespace(data_dir=Path("unused-fixture-only")))
        instances = {"peer": FleetInstance(instance_id="peer", name="peer", url="http://peer")}
        cache = SimpleNamespace(get=lambda *_args: cache_value[0])
        with (
            patch("pa.fleet.overview.datetime", ClockDatetime),
            patch("pa.fleet.overview.time.monotonic", side_effect=lambda: clock[0]),
            patch("pa.fleet.overview.cache_for", return_value=cache),
            patch("pa.fleet.overview._offload", side_effect=offload),
            patch("pa.fleet.overview.probe_dimension", side_effect=probe),
            patch("pa.server.shutdown.wait_for_shutdown", side_effect=wait),
            patch("pa.server.shutdown.is_shutting_down", return_value=False),
        ):
            await refresh_required_dimension_loop(ctx, instances, "peer", "status")
        return starts, waits, forces

    async def test_missing_first_then_bounded_early_renewals(self):
        starts, waits, forces = await self.run_clock()
        self.assertEqual(starts, [0, 2.7, 5.4])
        self.assertEqual(waits, [2.7, 2.7, 2.7])
        self.assertEqual(forces, [True] * 3)

    async def test_recent_failure_keeps_fifteen_second_backoff(self):
        cached = field("timeout", attempted_at="2026-10-24T00:00:00+00:00")
        starts, waits, _ = await self.run_clock(cached=cached)
        self.assertEqual(starts, [15, 17.7])
        self.assertEqual(waits[0], 15)

    async def test_unpublished_result_cannot_busy_loop(self):
        starts, waits, _ = await self.run_clock(publish=False)
        self.assertEqual(starts, [0, 1.5, 3])
        self.assertEqual(waits, [1.5] * 3)

    async def test_unexpected_failure_backs_off(self):
        with self.assertLogs("pa.fleet.overview", level="ERROR"):
            starts, waits, _ = await self.run_clock(fail=True)
        self.assertEqual(starts, [0, 15, 30])
        self.assertEqual(waits, [15] * 3)

    async def test_nonrequired_and_removed_instances_do_not_probe(self):
        with patch("pa.fleet.overview.probe_dimension") as probe:
            with self.assertRaises(ValueError):
                await refresh_required_dimension_loop(None, {}, "peer", "providers")
            with patch("pa.server.shutdown.is_shutting_down", return_value=False):
                await refresh_required_dimension_loop(None, {}, "peer", "status")
            probe.assert_not_called()

    async def test_workers_join_foreground_probe_and_keep_last_good_failure(self):
        from pa.fleet import overview
        from pa.fleet.overview import cache_for, probe_dimension

        with tempfile.TemporaryDirectory() as tmp:
            ctx = SimpleNamespace(settings=SimpleNamespace(data_dir=Path(tmp)), services={})
            inst = FleetInstance(instance_id="peer", name="peer", url="http://peer")
            cache = cache_for(Path(tmp))
            cache.put("peer", "status", field("fresh", {"version": "retained"}, observed_at="2026-10-24T00:00:00+00:00"))
            entered, joined, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
            calls = []
            original_shield = asyncio.shield

            def shield(awaitable):
                result = original_shield(awaitable)
                if asyncio.current_task().get_name() == "fixture-foreground":
                    joined.set()
                return result

            async def probe(*_args):
                calls.append(True)
                entered.set()
                await release.wait()
                return field("timeout", error="fixture deadline")

            async def stop(_delay):
                return True

            with (
                patch("pa.fleet.overview._probe", side_effect=probe),
                patch("pa.fleet.overview.asyncio.shield", side_effect=shield),
                patch("pa.server.shutdown.is_shutting_down", return_value=False),
                patch("pa.server.shutdown.wait_for_shutdown", side_effect=stop),
            ):
                worker = asyncio.create_task(refresh_required_dimension_loop(ctx, {"peer": inst}, "peer", "status"))
                await asyncio.wait_for(entered.wait(), 5)
                foreground = asyncio.create_task(probe_dimension(ctx, inst, "status", force=True), name="fixture-foreground")
                # Both callers join the existing shielded flight, not a second probe.
                await asyncio.wait_for(joined.wait(), 5)
                release.set()
                await asyncio.wait_for(asyncio.gather(worker, foreground), 5)
            self.assertEqual(len(calls), 1)
            result = cache.get("peer", "status")
            self.assertEqual(result["value"], {"version": "retained"})
            self.assertEqual(result["state"], "stale")
            self.assertEqual(result["last_attempt_state"], "timeout")


class RequiredRefreshOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_dimension_does_not_hold_healthy_and_retirement_reaps_workers(self):
        from pa.modules.fleet import _overview_refresh_loop

        first = FleetInstance(instance_id="first", name="first", url="http://first")
        second = FleetInstance(instance_id="second", name="second", url="http://second")
        instances = [first, second]
        starts, retired = set(), set()
        all_started, first_retired = asyncio.Event(), asyncio.Event()
        waits = []

        async def worker(_ctx, mapping, instance_id, dimension):
            key = instance_id, dimension
            starts.add(key)
            if len(starts) == 6:
                all_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                retired.add(key)
                if len([key for key in retired if key[0] == "first"]) == 3:
                    first_retired.set()

        async def wait(delay):
            waits.append(delay)
            if len(waits) == 1:
                return False
            if len(waits) == 2:
                await asyncio.wait_for(all_started.wait(), 5)
                instances[:] = [second]
                return False
            await asyncio.wait_for(first_retired.wait(), 5)
            return True

        ctx = SimpleNamespace(services={"fleet_registry": SimpleNamespace(list_instances=lambda: instances)})
        with (
            patch("pa.modules.fleet.refresh_required_dimension_loop", side_effect=worker),
            patch("pa.server.shutdown.wait_for_shutdown", side_effect=wait),
            patch("pa.server.shutdown.is_shutting_down", return_value=False),
        ):
            await asyncio.wait_for(_overview_refresh_loop(ctx), 5)
        self.assertEqual(len(starts), 6)
        self.assertEqual(retired, starts)
        self.assertEqual(waits, [1, 15, 15])
