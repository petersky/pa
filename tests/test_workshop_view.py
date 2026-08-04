"""Real-browser regressions for the persistent Workshop density control."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import tempfile
import unittest

from pa.browser.manager import BrowserManager, _browser_executable


ROOT = Path(__file__).parents[1]
WORKSHOP_SCRIPT = ROOT / "src/pa/server/static/js/workshop.js"
STYLE = ROOT / "src/pa/server/static/style.css"


def _card(card_id: str, title: str, lane: str) -> dict:
    return {
        "id": card_id,
        "title": title,
        "lane": lane,
        "dispatch_id": "dispatch" if lane == "active" else None,
        "dispatch_state": "running" if lane == "active" else lane,
        "session_id": "session" if lane == "active" else None,
        "target_instance_id": "local" if lane == "active" else None,
        "project": {"id": "project", "title": "PA Core"},
        "blockers": [],
        "branch": None,
        "pull_requests": [],
        "href": f"/cards/{card_id}",
    }


def _snapshot() -> dict:
    active = _card("active", "Build compact Workshop", "active")
    return {
        "generated_at": "2026-08-03T10:00:00Z",
        "authority": {
            "instance_id": "local",
            "current_instance_id": "local",
            "mode": "canonical",
        },
        "sync": {"state": "fresh", "nodes": []},
        "bays": [
            {
                "id": "local",
                "name": "Local instance",
                "zone": "default",
                "connectivity": "connected",
                "freshness": "fresh",
                "activity_freshness": "fresh",
                "activity_age_seconds": 1,
                "health": "healthy",
                "observed_at": "2026-08-03T10:00:00Z",
                "capacity": {"consumed": 1, "limit": 4},
                "providers": [],
                "workers": [
                    {
                        "id": "session",
                        "title": "Compact view session",
                        "state": "working",
                        "live": True,
                        "tool_category": "testing",
                        "card": active,
                        "provider": "codex",
                        "connected": True,
                        "elapsed_from": "started now",
                        "latest_progress": "Running browser coverage",
                        "dispatch_id": "dispatch",
                        "href": "/agent/session",
                    }
                ],
            }
        ],
        "areas": {
            "inbox": [_card("inbox", "Incoming card", "inbox")],
            "active": [active],
            "waiting": [_card("waiting", "Waiting card", "waiting")],
            "done": [_card("done", "Completed card", "done")],
        },
    }


def _workshop_markup() -> str:
    payload = json.dumps(_snapshot()).replace("</", "<\\/")
    return f"""
<section class="page-boundary" data-pa-live-history-boundary="workshop">
  <aside class="workshop-intro">
    <div class="workshop-controls" role="group" aria-label="Workshop layout">
      <button type="button" class="ghost small active" data-workshop-view="floor" aria-pressed="true">Floor view</button>
      <button type="button" class="ghost small" data-workshop-view="compact" aria-pressed="false">Compact view</button>
      <span class="workshop-view-status small" data-workshop-view-status aria-live="polite">Current layout: Floor view</span>
    </div>
  </aside>
  <main class="fixture-main">
    <div id="pa-workshop-root" class="workshop" data-realm="default">
      <script type="application/json" id="pa-workshop-data">{payload}</script>
      <header class="workshop-header">
        <div>
          <h1 data-workshop-view-heading>Floor view</h1>
          <p data-workshop-view-description>Current work, where it is running, and what needs attention.</p>
        </div>
        <div class="workshop-live"><span data-workshop-live>Loading</span></div>
      </header>
      <div class="notice workshop-alert" data-workshop-alert hidden></div>
      <section class="workshop-sync" data-workshop-sync></section>
      <div class="workshop-layout">
        <div class="workshop-scene" data-workshop-scene></div>
        <div class="workshop-compact" data-workshop-compact hidden></div>
        <aside class="panel workshop-inspector" data-workshop-inspector>
          <h2>Inspector</h2><p>Select an item.</p>
        </aside>
      </div>
    </div>
  </main>
</section>"""


def _page() -> str:
    markup = _workshop_markup()
    prelude = r"""
window.PA_TEST = true;
window.__eventSources = [];
window.__createdSources = 0;
window.__activeSources = 0;
window.__snapshot = JSON.parse(document.getElementById("pa-workshop-data").textContent);
window.addEventListener("error", function (event) {
  localStorage.setItem("pa.workshop.test.error", event.message || "script error");
});
window.addEventListener("unhandledrejection", function (event) {
  localStorage.setItem("pa.workshop.test.error", String(event.reason || "rejection"));
});
window.fetch = async function () {
  return { ok: true, json: async function () {
    return JSON.parse(JSON.stringify(window.__snapshot));
  }};
};
window.EventSource = function (url) {
  this.url = url;
  this.listeners = {};
  this.closed = false;
  window.__eventSources.push(this);
  window.__createdSources += 1;
  window.__activeSources += 1;
  var source = this;
  queueMicrotask(function () { if (!source.closed && source.onopen) source.onopen(); });
};
window.EventSource.prototype.addEventListener = function (name, listener) {
  this.listeners[name] = listener;
};
window.EventSource.prototype.close = function () {
  if (this.closed) return;
  this.closed = true;
  window.__activeSources -= 1;
};
window.__emitWorkshopSnapshot = function (title) {
  var update = JSON.parse(JSON.stringify(window.__snapshot));
  update.generated_at = "2026-08-03T10:00:01Z";
  update.bays[0].workers[0].title = title;
  window.__snapshot = update;
  var source = window.__eventSources.find(function (candidate) {
    return !candidate.closed && candidate.url.indexOf("workshop/events") !== -1;
  });
  source.listeners.snapshot({ data: JSON.stringify(update) });
};
"""
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<style>:root{--pa-surface:#fff;--pa-bg:#f5f7fa;--pa-text:#172033;"
        "--pa-text-muted:#536079;--pa-border:#73809a;--pa-accent:#315fc7;"
        "--pa-success:#167447;--pa-danger:#b42318;--radius:6px}"
        "body{margin:0;color:var(--pa-text);background:var(--pa-bg)}"
        ".page-boundary{display:grid;grid-template-columns:15rem minmax(0,1fr);gap:1rem;padding:1rem}"
        ".fixture-main{min-width:0}.fixture-tail{height:1200px}"
        "@media(max-width:720px){.page-boundary{grid-template-columns:minmax(0,1fr);padding:.5rem}}"
        + STYLE.read_text()
        + "</style></head><body><div id='fixture'>"
        + markup
        + "</div><div class='fixture-tail'></div><script>"
        + prelude
        + "\nwindow.__workshopMarkup = function () { return "
        + json.dumps(markup).replace("</", "<\\/")
        + "; };</script><script>"
        + WORKSHOP_SCRIPT.read_text()
        + "</script></body></html>"
    )


@unittest.skipUnless(_browser_executable(), "managed Chromium is not installed")
class WorkshopCompactViewBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_toggle_persistence_live_refresh_htmx_and_responsive_layout(self) -> None:
        page_body = _page().encode()

        async def serve(reader, writer) -> None:
            try:
                try:
                    request = await reader.readuntil(b"\r\n\r\n")
                except asyncio.IncompleteReadError:
                    return
                path = request.decode("latin-1").splitlines()[0].split(" ")[1]
                if path.split("?", 1)[0] == "/workshop":
                    body = page_body
                else:
                    body = b"<!doctype html><title>Elsewhere</title><p>Elsewhere</p>"
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
        port = server.sockets[0].getsockname()[1]
        temp_dir = tempfile.TemporaryDirectory()
        browser = BrowserManager(Path(temp_dir.name))
        try:
            attachment = await browser.attach(
                "workshop-density-test",
                url=f"http://127.0.0.1:{port}/workshop",
                width=1280,
                height=800,
            )
            page = attachment.page
            await page.wait_until_usable()
            await asyncio.sleep(0.1)

            initial = await page.evaluate(
                """({
                  view: document.querySelector("#pa-workshop-root").dataset.workshopLayout,
                  floorVisible: !document.querySelector("[data-workshop-scene]").hidden,
                  compactHidden: document.querySelector("[data-workshop-compact]").hidden,
                  sources: window.__activeSources,
                  workerHeight: document.querySelector(".workshop-worker-wrap").getBoundingClientRect().height
                })"""
            )
            self.assertEqual(initial["view"], "floor")
            self.assertTrue(initial["floorVisible"])
            self.assertTrue(initial["compactHidden"])
            self.assertEqual(initial["sources"], 2)

            toggled = await page.evaluate(
                """(async () => {
                  document.querySelector('[data-workshop-kind="worker"]').dispatchEvent(
                    new MouseEvent("click", {bubbles:true}));
                  var clickSelected = document.querySelector('[data-workshop-kind="worker"]').classList.contains("selected");
                  window.scrollTo(0, 160);
                  var beforeScroll = window.scrollY;
                  document.querySelector('[data-workshop-view="compact"]').click();
                  await new Promise(requestAnimationFrame);
                  var compact = document.querySelector("[data-workshop-compact]");
                  return {
                    view: document.querySelector("#pa-workshop-root").dataset.workshopLayout,
                    floorHidden: document.querySelector("[data-workshop-scene]").hidden,
                    compactVisible: !compact.hidden,
                    compactHeight: compact.querySelector("tr").getBoundingClientRect().height,
                    pressed: document.querySelector('[data-workshop-view="compact"]').getAttribute("aria-pressed"),
                    floorPressed: document.querySelector('[data-workshop-view="floor"]').getAttribute("aria-pressed"),
                    status: document.querySelector("[data-workshop-view-status]").textContent,
                    heading: document.querySelector("[data-workshop-view-heading]").textContent,
                    stored: localStorage.getItem("pa.workshop.view.v1"),
                    selected: compact.querySelector('[data-workshop-kind="worker"]').classList.contains("selected"),
                    inspector: document.querySelector("[data-workshop-inspector] h3") &&
                      document.querySelector("[data-workshop-inspector] h3").textContent,
                    scrollDelta: Math.abs(window.scrollY - beforeScroll),
                    error: localStorage.getItem("pa.workshop.test.error"),
                    clickSelected: clickSelected
                  };
                })()"""
            )
            self.assertEqual(toggled["view"], "compact")
            self.assertTrue(toggled["floorHidden"])
            self.assertTrue(toggled["compactVisible"])
            self.assertLess(toggled["compactHeight"], initial["workerHeight"])
            self.assertEqual(toggled["pressed"], "true")
            self.assertEqual(toggled["floorPressed"], "false")
            self.assertEqual(toggled["status"], "Current layout: Compact view")
            self.assertEqual(toggled["heading"], "Compact view")
            self.assertEqual(toggled["stored"], "compact")
            self.assertTrue(toggled["clickSelected"], toggled)
            self.assertTrue(toggled["selected"], toggled)
            self.assertEqual(toggled["inspector"], "Compact view session", toggled)
            self.assertLessEqual(toggled["scrollDelta"], 1)

            restored = await page.evaluate(
                """(() => {
                  document.querySelector('[data-workshop-view="floor"]').click();
                  var floor = !document.querySelector("[data-workshop-scene]").hidden &&
                    document.querySelector("[data-workshop-compact]").hidden;
                  document.querySelector('[data-workshop-view="compact"]').click();
                  return floor;
                })()"""
            )
            self.assertTrue(restored)

            live = await page.evaluate(
                """(() => {
                  window.__emitWorkshopSnapshot("Session updated live");
                  return {
                    compact: !document.querySelector("[data-workshop-compact]").hidden,
                    pressed: document.querySelector('[data-workshop-view="compact"]').getAttribute("aria-pressed"),
                    text: document.querySelector("[data-workshop-compact]").textContent
                  };
                })()"""
            )
            self.assertTrue(live["compact"])
            self.assertEqual(live["pressed"], "true")
            self.assertIn("Session updated live", live["text"])

            streams = await page.evaluate(
                """(() => {
                  var child = document.querySelector(".workshop-layout");
                  document.dispatchEvent(new CustomEvent("htmx:afterSwap", {detail:{target:child}}));
                  var afterChild = [window.__createdSources, window.__activeSources];
                  var boundary = document.querySelector("[data-pa-live-history-boundary='workshop']");
                  document.dispatchEvent(new CustomEvent("htmx:beforeSwap", {detail:{target:boundary}}));
                  document.querySelector("#fixture").innerHTML = window.__workshopMarkup();
                  var replacement = document.querySelector("[data-pa-live-history-boundary='workshop']");
                  document.dispatchEvent(new CustomEvent("htmx:afterSwap", {detail:{target:replacement}}));
                  return {
                    afterChild: afterChild,
                    afterReplacement: [window.__createdSources, window.__activeSources],
                    view: document.querySelector("#pa-workshop-root").dataset.workshopLayout,
                    compact: !document.querySelector("[data-workshop-compact]").hidden
                  };
                })()"""
            )
            self.assertEqual(streams["afterChild"], [2, 2])
            self.assertEqual(streams["afterReplacement"], [4, 2])
            self.assertEqual(streams["view"], "compact")
            self.assertTrue(streams["compact"])

            await attachment.resize(390, 700)
            await asyncio.sleep(0.05)
            narrow = await page.evaluate(
                """({
                  noPageOverflow: document.documentElement.scrollWidth <= window.innerWidth,
                  compactScrollable: document.querySelector("[data-workshop-compact]").scrollWidth >
                    document.querySelector("[data-workshop-compact]").clientWidth,
                  compactVisible: !document.querySelector("[data-workshop-compact]").hidden,
                  pressed: document.querySelector('[data-workshop-view="compact"]').getAttribute("aria-pressed")
                })"""
            )
            self.assertTrue(narrow["noPageOverflow"])
            self.assertTrue(narrow["compactScrollable"])
            self.assertTrue(narrow["compactVisible"])
            self.assertEqual(narrow["pressed"], "true")

            await page.navigate_and_wait(f"http://127.0.0.1:{port}/elsewhere")
            await page.evaluate("history.back()")
            for _ in range(50):
                await asyncio.sleep(0.05)
                back = await page.evaluate(
                    """({path:location.pathname, ready:document.readyState,
                    view:document.querySelector("#pa-workshop-root") &&
                      document.querySelector("#pa-workshop-root").dataset.workshopLayout})"""
                )
                if back["path"] == "/workshop" and back["ready"] == "complete":
                    break
            else:
                self.fail(f"Workshop back navigation did not finish: {back}")
            self.assertEqual(back["view"], "compact")

            await page.command("Page.reload")
            await page.wait_until_usable()
            await asyncio.sleep(0.1)
            reloaded = await page.evaluate(
                """({
                  view:document.querySelector("#pa-workshop-root").dataset.workshopLayout,
                  compact:!document.querySelector("[data-workshop-compact]").hidden,
                  pressed:document.querySelector('[data-workshop-view="compact"]').getAttribute("aria-pressed"),
                  error:localStorage.getItem("pa.workshop.test.error")
                })"""
            )
            self.assertEqual(reloaded["view"], "compact")
            self.assertTrue(reloaded["compact"])
            self.assertEqual(reloaded["pressed"], "true")
            self.assertIsNone(reloaded["error"])
        finally:
            await browser.close()
            server.close()
            await server.wait_closed()
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
