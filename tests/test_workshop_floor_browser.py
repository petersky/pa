"""Real-browser checks for the Workshop floor: stations, walking, snapping, a11y."""

from __future__ import annotations

import asyncio
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pa.browser.manager import BrowserManager, _browser_executable
from tests import test_workshop_view as view


def _worker(worker_id: str, **changes) -> dict:
    worker = {
        "id": worker_id,
        "title": worker_id.title(),
        "purpose": "chat",
        "state": "quiet-active",
        "station": "bench",
        "card_id": None,
        "provider": "codex",
        "relationship_kind": "session",
        "href": f"/agent?session={worker_id}",
    }
    worker.update(changes)
    return worker


def floor_snapshot(agent_count: int = 3) -> dict:
    snapshot = copy.deepcopy(view._snapshot())
    base = snapshot["bays"][0]
    workers = [
        _worker(
            "ada",
            purpose="card",
            card_id="active",
            state="working",
            station="workbench",
            pose="edit",
        ),
        _worker("bo"),
        _worker("cy"),
    ]
    workers += [_worker(f"agent-{index:02d}") for index in range(3, agent_count)]
    base["workers"] = workers[:agent_count]
    base["active"] = 1
    snapshot["bays"].append(
        {
            **base,
            "id": "peer",
            "name": "Peer bay",
            "local": False,
            "connectivity": "disconnected",
            "connectivity_label": "Disconnected",
            "unreachable_since": "2026-08-03T09:00:00Z",
            "workers": [_worker("far", station="offline", title="Far agent")],
        }
    )
    snapshot["work_orders"] = [
        order
        for order in snapshot["work_orders"]
        if not order["id"].startswith("bulk-")
    ]
    return snapshot


def _page_for(snapshot: dict) -> bytes:
    with patch.object(view, "_snapshot", lambda: snapshot):
        return view._page().encode()


@unittest.skipUnless(_browser_executable(), "managed Chromium is not installed")
class WorkshopFloorBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def _open(self, snapshot: dict, *, reduced_motion: bool = False):
        body = _page_for(snapshot)

        async def serve(reader, writer) -> None:
            try:
                try:
                    await reader.readuntil(b"\r\n\r\n")
                except asyncio.IncompleteReadError:
                    return
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                    + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                    + body
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        self.addAsyncCleanup(self._close_server, server)
        port = server.sockets[0].getsockname()[1]
        temp_dir = tempfile.TemporaryDirectory()
        browser = BrowserManager(Path(temp_dir.name))
        self.addAsyncCleanup(self._release, browser, temp_dir)
        attachment = await browser.attach(
            "workshop-floor-test",
            url=f"http://127.0.0.1:{port}/workshop",
            width=1440,
            height=1000,
        )
        page = attachment.page
        if reduced_motion:
            await page.command(
                "Emulation.setEmulatedMedia",
                {"features": [{"name": "prefers-reduced-motion", "value": "reduce"}]},
            )
            await page.command("Page.reload", {})
        await page.wait_until_usable()
        await asyncio.sleep(0.15)
        return page

    async def _release(self, browser, temp_dir) -> None:
        await browser.close()
        temp_dir.cleanup()

    async def _close_server(self, server) -> None:
        server.close()
        await server.wait_closed()

    # A snippet that feeds one live multiplexed session event to the floor.
    EMIT = """
      window.__emit = function (type, sessionId, seq, payload) {
        var source = window.__eventSources.find(function (candidate) {
          return !candidate.closed && candidate.url.indexOf("/session-events?") !== -1;
        });
        source.listeners[type]({ data: JSON.stringify({
          type: type, session_id: sessionId, seq: seq, payload: payload || {} }) });
      };
      window.__stationOf = function (id) {
        var el = document.querySelector('[data-floor-unit="' + id + '"]');
        var station = el && el.closest("[data-station]");
        return station ? station.dataset.station : null;
      };
    """

    async def test_stations_are_headed_lists_and_agents_stand_where_state_says(self) -> None:
        page = await self._open(floor_snapshot())
        result = await page.evaluate(
            self.EMIT
            + """(() => {
              var stations = Array.from(document.querySelectorAll(".floor-station"));
              return {
                headed: stations.every(function (s) {
                  var h = s.querySelector("h3, h4");
                  return h && s.getAttribute("aria-labelledby") === h.id && s.querySelector("ul[role=list]");
                }),
                names: stations.map(function (s) { return s.dataset.station; }).slice(0, 8),
                ada: window.__stationOf("ada"), bo: window.__stationOf("bo"),
                far: window.__stationOf("far"),
                intake: document.querySelector('[data-station-key="intake"] [data-floor-count]').textContent,
                bayState: document.querySelector('[data-bay="peer"]').dataset.state,
                offline: document.querySelector('[data-bay="peer"] [data-bay-offline]').textContent,
                carried: !!document.querySelector('[data-floor-unit="ada"] .workshop-operation-card'),
                density: document.querySelector("[data-floor]").dataset.density
              };
            })()"""
        )
        self.assertTrue(result["headed"])
        self.assertEqual(result["ada"], "workbench")
        self.assertEqual(result["bo"], "bench")
        self.assertEqual(result["far"], "offline")
        self.assertEqual(result["bayState"], "disconnected")
        self.assertIn("Unreachable since", result["offline"])
        self.assertTrue(result["carried"], "1-6 agents show the full work order beside the figure")
        self.assertEqual(result["density"], "full")
        self.assertEqual(result["intake"], "1")

    async def test_real_transitions_walk_and_pose_changes_do_not(self) -> None:
        page = await self._open(floor_snapshot())
        walked = await page.evaluate(
            self.EMIT
            + """(async () => {
              var el = document.querySelector('[data-floor-unit="bo"]');
              window.__emit("permission_request", "bo", 1, {});
              await new Promise(requestAnimationFrame);
              var started = {
                station: window.__stationOf("bo"),
                walking: el.classList.contains("walking"),
                transition: el.style.transition,
                moving: getComputedStyle(el).transform !== "none",
                sameNode: el === document.querySelector('[data-floor-unit="bo"]'),
                bell: document.querySelector('[data-station-key="bell"] [data-floor-count]').textContent,
                bench: document.querySelector('[data-bay="local"] [data-station="bench"] [data-floor-count]').textContent,
                log: document.querySelector("[data-floor-log]").textContent
              };
              await new Promise(function (r) { setTimeout(r, 900); });
              started.settled = !el.classList.contains("walking") && el.style.transform === "";
              // A tool-kind change is a pose change only: same station, never walking.
              window.__emit("tool_call", "ada", 1, { kind: "execute" });
              await new Promise(requestAnimationFrame);
              var ada = document.querySelector('[data-floor-unit="ada"]');
              started.pose = ada.dataset.pose;
              started.adaWalking = ada.classList.contains("walking");
              started.adaStation = window.__stationOf("ada");
              return started;
            })()"""
        )
        self.assertEqual(walked["station"], "bell")
        self.assertTrue(walked["walking"])
        self.assertIn("600ms", walked["transition"])
        self.assertTrue(walked["moving"], "a transform is in flight from the old spot")
        self.assertTrue(walked["sameNode"], "the same element walks; it is not re-created")
        self.assertEqual(walked["bell"], "1")
        self.assertEqual(walked["bench"], "1")
        self.assertIn("Bo needs you: permission requested.", walked["log"])
        self.assertTrue(walked["settled"])
        self.assertEqual(walked["pose"], "test")
        self.assertFalse(walked["adaWalking"])
        self.assertEqual(walked["adaStation"], "workbench")

    async def test_reduced_motion_snaps_but_counts_and_log_still_update(self) -> None:
        page = await self._open(floor_snapshot(), reduced_motion=True)
        result = await page.evaluate(
            self.EMIT
            + """(async () => {
              var el = document.querySelector('[data-floor-unit="bo"]');
              window.__emit("user_message", "bo", 1, {});
              await new Promise(requestAnimationFrame);
              return {
                station: window.__stationOf("bo"),
                walking: el.classList.contains("walking"),
                transform: el.style.transform,
                workbench: document.querySelector('[data-bay="local"] [data-station="workbench"] [data-floor-count]').textContent,
                log: document.querySelector("[data-floor-log]").textContent,
                live: document.querySelector("[data-floor-log]").getAttribute("aria-live")
              };
            })()"""
        )
        self.assertEqual(result["station"], "workbench")
        self.assertFalse(result["walking"])
        self.assertEqual(result["transform"], "")
        self.assertEqual(result["workbench"], "2")
        self.assertIn("Bo started working at the workbench.", result["log"])
        self.assertEqual(result["live"], "polite")

    async def test_user_motion_toggle_and_instance_setting_stop_walking(self) -> None:
        page = await self._open(floor_snapshot())
        result = await page.evaluate(
            self.EMIT
            + """(async () => {
              document.querySelector("[data-workshop-motion]").click();
              var pressed = document.querySelector("[data-workshop-motion]").getAttribute("aria-pressed");
              var stored = localStorage.getItem("pa.workshop.motion.v1");
              window.__emit("connection_lost", "cy", 1, {});
              await new Promise(requestAnimationFrame);
              return { pressed: pressed, stored: stored, station: window.__stationOf("cy"),
                walking: document.querySelector('[data-floor-unit="cy"]').classList.contains("walking") };
            })()"""
        )
        self.assertEqual(result["pressed"], "true")
        self.assertEqual(result["stored"], "reduce")
        self.assertEqual(result["station"], "infirmary")
        self.assertFalse(result["walking"])

    async def test_at_most_four_walk_at_once_and_crowded_floors_use_slots(self) -> None:
        page = await self._open(floor_snapshot(agent_count=14))
        result = await page.evaluate(
            self.EMIT
            + """(async () => {
              var shell = document.querySelector("[data-floor]");
              var before = {
                density: shell.dataset.density,
                benchShown: document.querySelectorAll('[data-bay="local"] [data-station="bench"] [data-floor-unit]').length,
                overflow: document.querySelector('[data-bay="local"] [data-station="bench"] [data-floor-overflow]')
              };
              var info = { density: before.density, benchShown: before.benchShown,
                overflowText: before.overflow && before.overflow.textContent,
                overflowHidden: before.overflow && before.overflow.hidden };
              before.overflow.click();
              info.expandedShown = document.querySelectorAll('[data-bay="local"] [data-station="bench"] [data-floor-unit]').length;
              info.expandedLabel = document.querySelector('[data-bay="local"] [data-station="bench"] [data-floor-overflow]').textContent;
              for (var i = 3; i < 11; i++) window.__emit("user_message", "agent-" + String(i).padStart(2, "0"), 1, {});
              await new Promise(requestAnimationFrame);
              info.walking = document.querySelectorAll(".floor-unit.walking").length;
              info.workbench = document.querySelector('[data-bay="local"] [data-station="workbench"] [data-floor-count]').textContent;
              return info;
            })()"""
        )
        self.assertEqual(result["density"], "slots")
        self.assertEqual(result["benchShown"], 4)
        self.assertIn("more", result["overflowText"])
        self.assertEqual(result["expandedLabel"], "Show fewer")
        self.assertGreater(result["expandedShown"], 4)
        self.assertLessEqual(result["walking"], 4)
        self.assertEqual(result["workbench"], "9", "everyone still arrives; the extra moves just snap")

    async def test_follow_and_keyboard_navigation(self) -> None:
        page = await self._open(floor_snapshot())
        result = await page.evaluate(
            self.EMIT
            + """(() => {
              var agent = document.querySelector('[data-floor-unit="bo"] [data-workshop-kind="agent"]');
              agent.focus();
              agent.dispatchEvent(new KeyboardEvent("keydown", { key: "f", bubbles: true }));
              var following = document.querySelector('[data-floor-unit="bo"]').classList.contains("following");
              var pressed = document.querySelector('[data-floor-unit="bo"] [data-floor-follow]').getAttribute("aria-pressed");
              agent = document.querySelector('[data-floor-unit="bo"] [data-workshop-kind="agent"]');
              agent.focus();
              agent.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true, cancelable: true }));
              var moved = document.activeElement !== agent;
              document.querySelector('[data-floor-unit="bo"] [data-workshop-kind="agent"]').click();
              return { following: following, pressed: pressed, moved: moved,
                inspector: document.querySelector("[data-workshop-inspector] h3").textContent,
                station: document.querySelector("[data-workshop-inspector]").textContent };
            })()"""
        )
        self.assertTrue(result["following"])
        self.assertEqual(result["pressed"], "true")
        self.assertTrue(result["moved"])
        self.assertEqual(result["inspector"], "Bo")
        self.assertIn("Idle bench", result["station"])

    async def test_compact_list_is_still_available_and_releases_the_floor_stream(self) -> None:
        page = await self._open(floor_snapshot())
        result = await page.evaluate(
            """(async () => {
              var open = function () {
                return window.__eventSources.filter(function (s) {
                  return !s.closed && s.url.indexOf("/session-events?") !== -1; }).length;
              };
              var onFloor = open();
              document.querySelector('[data-workshop-view="compact"]').click();
              await new Promise(requestAnimationFrame);
              return { onFloor: onFloor, inCompact: open(),
                compactRows: document.querySelectorAll('[data-workshop-compact-row="work-order"]').length > 0,
                pager: document.querySelector("[data-workshop-pagination]").hidden };
            })()"""
        )
        self.assertEqual(result["onFloor"], 1)
        self.assertEqual(result["inCompact"], 0)
        self.assertTrue(result["compactRows"])
        self.assertFalse(result["pager"])


if __name__ == "__main__":
    unittest.main()
