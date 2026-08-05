"""Real-browser regressions for the persistent Workshop density control."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

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
        "dispatch_label": "Running" if lane == "active" else "Not dispatched",
        "dispatch_current": lane == "active",
        "dispatch_exclusive": lane == "active",
        "can_dispatch": lane not in {"active", "done"},
        "dispatch_unavailable_reason": (
            "A current exclusive dispatch already owns this work order."
            if lane == "active"
            else "Done cards are not dispatchable."
            if lane == "done"
            else None
        ),
        "evaluated_outcome": None,
        "outcome_label": "No outcome yet",
        "href": f"/cards/{card_id}",
    }


def _work_order(
    card: dict, *, attention: bool = False, session: dict | None = None
) -> dict:
    active = card["lane"] == "active"
    return {
        "id": card["id"],
        "title": card["title"],
        "card": card,
        "lane": card["lane"],
        "lane_label": card["lane"].title(),
        "dispatch_state": card["dispatch_state"] if active else None,
        "dispatch_label": card["dispatch_label"],
        "dispatch_current": card["dispatch_current"],
        "activity_state": "working" if active else None,
        "activity_label": "Working" if active else "No current session",
        "freshness": "fresh" if active else None,
        "freshness_label": "Current" if active else "No live signal",
        "progress_freshness": "live" if active else None,
        "progress_freshness_label": "Current" if active else "No progress signal",
        "progress_last_activity_at": ("2026-08-03T10:00:00Z" if active else None),
        "progress_age_seconds": 0 if active else None,
        "evaluated_outcome": None,
        "outcome_label": "No outcome yet",
        "session": session,
        "reservation": None,
        "location": (
            {"id": "local", "name": "Local instance", "href": "/fleet?instance=local"}
            if active
            else None
        ),
        "live": active,
        "attention": attention,
        "attention_reasons": ["Card is waiting"] if attention else [],
        "attention_details": (
            [{"axis": "card", "code": "lane_waiting", "summary": "Card is waiting"}]
            if attention
            else []
        ),
        "updated_at": "2026-08-03T10:00:00Z",
    }


def _snapshot() -> dict:
    active = _card("active", "Build compact Workshop", "active")
    session = {
        "id": "session",
        "title": "Compact view session",
        "relationship_label": "Session: Compact view session",
        "href": "/agent/session",
        "provider": "codex",
        "connected": True,
        "latest_progress": "Running browser coverage",
        "tool_category": "testing",
    }
    inventory = [
        _work_order(active, session=session),
        _work_order(_card("waiting", "Waiting card", "waiting"), attention=True),
        _work_order(_card("inbox", "Incoming card", "inbox")),
        _work_order(_card("done", "Completed card", "done")),
    ]
    inventory.extend(
        _work_order(_card(f"bulk-{index}", f"Inventory card {index}", "inbox"))
        for index in range(125)
    )
    return {
        "schema": "pa.workshop/v2",
        "generated_at": "2026-08-03T10:00:00Z",
        "authority": {
            "instance_id": "local",
            "current_instance_id": "local",
            "mode": "canonical",
        },
        "sync": {
            "state": "degraded",
            "state_label": "Needs attention",
            "nodes": [
                {
                    "instance_id": "local",
                    "state": "fresh",
                    "durable_head": "a",
                    "projection_head": "a",
                    "conflicts": [],
                    "offline_peers": [],
                },
                {
                    "instance_id": "remote-current",
                    "state": "fresh",
                    "durable_head": "a",
                    "projection_head": "b",
                    "conflicts": [],
                    "offline_peers": [],
                },
                {
                    "instance_id": "remote-history",
                    "state": "stale",
                    "durable_head": "a",
                    "projection_head": "b",
                    "conflicts": [],
                    "offline_peers": [],
                },
            ],
            "issues": [
                {
                    "peer_name": "Remote current",
                    "instance_id": "remote-current",
                    "condition_label": "Current condition",
                    "observed_at": "2026-08-03T09:59:45Z",
                    "age_seconds": 15,
                    "summary": "Local view is catching up",
                    "recovery_label": "Retrying",
                    "recovery_attempt": 2,
                    "href": "/fleet?section=sync&instance=remote-current",
                },
                {
                    "peer_name": "Remote history",
                    "instance_id": "remote-history",
                    "condition_label": "Historical observation",
                    "observed_at": "2026-08-03T09:55:00Z",
                    "age_seconds": 300,
                    "summary": "Last report was behind",
                    "recovery_label": "Recovery status unavailable",
                    "recovery_attempt": None,
                    "href": "/fleet?section=sync&instance=remote-history",
                },
            ],
        },
        "bays": [
            {
                "id": "local",
                "name": "Local instance",
                "zone": "default",
                "connectivity": "connected",
                "connectivity_label": "Connected",
                "freshness": "fresh",
                "freshness_label": "Current",
                "activity_freshness": "fresh",
                "activity_freshness_label": "Current",
                "activity_age_seconds": 1,
                "activity_observed_at": "2026-08-03T09:59:59Z",
                "health": "healthy",
                "observed_at": "2026-08-03T10:00:00Z",
                "capacity": {"consumed": 1, "limit": 4},
                "active": 1,
                "queued": 0,
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
        "work_orders": inventory,
        "inventory": {
            "loaded": len(inventory),
            "total": len(inventory),
            "omitted": 0,
            "overflow_href": "/cards",
        },
        "counts": {
            "total": len(inventory),
            "live": 1,
            "attention": 1,
            "lanes": {"inbox": 126, "active": 1, "waiting": 1, "done": 1},
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
        <div class="workshop-live-controls">
          <div class="workshop-live" role="status" aria-live="polite"><span data-workshop-live>Loading</span></div>
          <button type="button" data-workshop-refresh>Refresh</button>
        </div>
      </header>
      <div class="notice workshop-alert" data-workshop-alert hidden></div>
      <section class="workshop-sync" data-workshop-sync></section>
      <form class="panel workshop-query" role="search">
        <label>Find work<input type="search" data-workshop-search></label>
        <label>Show<select data-workshop-filter>
          <option value="operational">Live + needs attention</option>
          <option value="live">Live only</option><option value="attention">Needs attention only</option>
          <option value="all">Loaded inventory</option>
        </select></label>
        <label>Group<select data-workshop-group><option value="attention">Attention first</option>
          <option value="location">Group by location</option><option value="lane">Group by card lane</option></select></label>
        <p data-workshop-results aria-live="polite"></p>
        <a data-workshop-overflow href="/cards">Open full Cards inventory</a>
      </form>
      <div class="workshop-layout">
        <div class="workshop-scene" data-workshop-scene></div>
        <div class="workshop-compact" data-workshop-compact hidden></div>
        <aside class="panel workshop-inspector" data-workshop-inspector tabindex="-1">
          <h2>Inspector</h2><p>Select an item.</p>
        </aside>
      </div>
      <p class="sr-only" data-workshop-announcer aria-live="polite"></p>
      <nav data-workshop-pagination></nav>
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
  update.work_orders[0].session.title = title;
  update.work_orders[0].session.relationship_label = "Session: " + title;
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
    async def test_toggle_persistence_live_refresh_htmx_and_responsive_layout(
        self,
    ) -> None:
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
                width=1440,
                height=900,
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
                  operationHeight: document.querySelector(".workshop-operation-card").getBoundingClientRect().height,
                  renderedRows: document.querySelectorAll('[data-workshop-compact-row="work-order"]').length,
                  results: document.querySelector("[data-workshop-results]").textContent,
                  refreshInsideStatus: !!document.querySelector('[role="status"] [data-workshop-refresh]'),
                  inspectorLive: document.querySelector("[data-workshop-inspector]").hasAttribute("aria-live"),
                  firstViewport: document.elementFromPoint(195, 690) !== null
                })"""
            )
            self.assertEqual(initial["view"], "floor")
            self.assertTrue(initial["floorVisible"])
            self.assertTrue(initial["compactHidden"])
            self.assertEqual(initial["sources"], 2)
            self.assertEqual(initial["renderedRows"], 2)
            self.assertIn("127 loaded work orders omitted", initial["results"])
            self.assertFalse(initial["refreshInsideStatus"])
            self.assertFalse(initial["inspectorLive"])

            performance_budget = await page.evaluate(
                """(() => {
                  var update = JSON.parse(JSON.stringify(window.__snapshot));
                  update.generated_at = "2026-08-03T10:00:00Z~";
                  var started = performance.now();
                  window.PAWorkshopTest.acceptSnapshot(document.querySelector("#pa-workshop-root"), update);
                  var reconcileMs = performance.now() - started;
                  started = performance.now();
                  window.PAWorkshopTest.setView(document.querySelector("#pa-workshop-root"), "compact",
                    {persist:false, preserveScroll:false});
                  window.PAWorkshopTest.setView(document.querySelector("#pa-workshop-root"), "floor",
                    {persist:false, preserveScroll:false});
                  return {reconcileMs:reconcileMs, switchMs:performance.now() - started};
                })()"""
            )
            self.assertLess(performance_budget["reconcileMs"], 1000)
            self.assertLess(performance_budget["switchMs"], 250)

            await page.evaluate(
                'document.querySelector("[data-workshop-filter]").focus()'
            )
            await page.command(
                "Input.dispatchKeyEvent",
                {
                    "type": "keyDown",
                    "key": "Tab",
                    "code": "Tab",
                    "windowsVirtualKeyCode": 9,
                },
            )
            await page.command(
                "Input.dispatchKeyEvent",
                {
                    "type": "keyUp",
                    "key": "Tab",
                    "code": "Tab",
                    "windowsVirtualKeyCode": 9,
                },
            )
            keyboard = await page.evaluate(
                """({groupFocused:document.activeElement.hasAttribute("data-workshop-group"),
                searchRole:document.querySelector(".workshop-query").getAttribute("role"),
                layoutButtons:Array.from(document.querySelectorAll("[data-workshop-view]"))
                  .every(function (button) { return button.tagName === "BUTTON" && button.hasAttribute("aria-pressed"); })})"""
            )
            self.assertTrue(keyboard["groupFocused"])
            self.assertEqual(keyboard["searchRole"], "search")
            self.assertTrue(keyboard["layoutButtons"])

            sync_details = await page.evaluate(
                """(() => {
                  document.querySelector('[data-workshop-kind="sync"]').click();
                  var panel = document.querySelector("[data-workshop-inspector]");
                  return {text:panel.textContent,
                    links:Array.from(panel.querySelectorAll(".workshop-sync-issues a"))
                      .map(function (link) { return {href:link.getAttribute("href"), text:link.textContent}; })};
                })()"""
            )
            self.assertIn("Remote current", sync_details["text"])
            self.assertIn("Current condition", sync_details["text"])
            self.assertIn("Remote history", sync_details["text"])
            self.assertIn("Historical observation", sync_details["text"])
            self.assertIn("Retrying", sync_details["text"])
            self.assertEqual(len(sync_details["links"]), 2)
            self.assertEqual(
                [link["text"] for link in sync_details["links"]],
                [
                    "Open sync details for Remote current",
                    "Open sync details for Remote history",
                ],
            )

            accessible_names = await page.evaluate(
                """(() => ({
                  sync:document.querySelector('[data-workshop-kind="sync"]').getAttribute("aria-label"),
                  bay:document.querySelector('[data-workshop-kind="bay"]').getAttribute("aria-label"),
                  order:document.querySelector('[data-workshop-kind="card"][data-workshop-id="active"]')
                    .getAttribute("aria-label")
                }))()"""
            )
            self.assertIn("Needs attention", accessible_names["sync"])
            self.assertIn("3 peers", accessible_names["sync"])
            self.assertIn("2 needing attention", accessible_names["sync"])
            self.assertIn("Connected", accessible_names["bay"])
            self.assertIn("1 of 4 slots used", accessible_names["bay"])
            for state in ("Active", "Running", "Working", "Current"):
                self.assertIn(state, accessible_names["order"])

            exact_attention = await page.evaluate(
                """(() => {
                  var root = document.querySelector("#pa-workshop-root");
                  var update = JSON.parse(JSON.stringify(window.__snapshot));
                  update.generated_at = "2026-08-03T10:00:00Z~~";
                  var active = update.work_orders.find(function (order) { return order.id === "active"; });
                  active.attention = true;
                  active.progress_freshness = "stale";
                  active.progress_freshness_label = "Stale";
                  active.progress_last_activity_at = "2026-08-03T09:30:00Z";
                  active.progress_age_seconds = 1800;
                  active.attention_reasons = ["Exact reconciliation dependency error",
                    "Exact disposition extraction error", "Structured progress is stale"];
                  active.attention_details = [
                    {axis:"card_reconciliation", code:"last_dependency_error",
                      summary:"Exact reconciliation dependency error"},
                    {axis:"card_disposition", code:"extraction_error",
                      summary:"Exact disposition extraction error"},
                    {axis:"progress", code:"freshness_stale", summary:"Structured progress is stale"}
                  ];
                  window.PAWorkshopTest.acceptSnapshot(root, update);
                  window.__snapshot = update;
                  document.querySelector('[data-workshop-kind="card"][data-workshop-id="active"]').click();
                  var panel = document.querySelector("[data-workshop-inspector]");
                  var cardButton = document.querySelector('[data-workshop-kind="card"][data-workshop-id="active"]');
                  return {text:panel.textContent, cardText:cardButton.textContent,
                    accessibleName:cardButton.getAttribute("aria-label")};
                })()"""
            )
            self.assertIn("Session signal freshnessCurrent", exact_attention["text"])
            self.assertIn("Dispatch progress freshnessStale", exact_attention["text"])
            self.assertIn(
                "Exact reconciliation dependency error", exact_attention["text"]
            )
            self.assertIn("Exact disposition extraction error", exact_attention["text"])
            self.assertIn("Structured progress is stale", exact_attention["text"])
            self.assertIn("ProgressStale", exact_attention["cardText"])
            self.assertIn("Stale", exact_attention["accessibleName"])

            lightweight_age = await page.evaluate(
                """(() => {
                  var root = document.querySelector("#pa-workshop-root");
                  var age = document.querySelector("[data-workshop-observed-at]");
                  age.dataset.workshopObservedAt = new Date(Date.now() - 65000).toISOString();
                  window.PAWorkshopTest.updateRelativeAges(root);
                  var first = age.textContent;
                  age.dataset.workshopObservedAt = new Date(Date.now() - 125000).toISOString();
                  window.PAWorkshopTest.updateRelativeAges(root);
                  return {sameNode:age === document.querySelector("[data-workshop-observed-at]"),
                    first:first, second:age.textContent};
                })()"""
            )
            self.assertTrue(lightweight_age["sameNode"])
            self.assertIn("1 minutes ago", lightweight_age["first"])
            self.assertIn("2 minutes ago", lightweight_age["second"])

            toggled = await page.evaluate(
                """(async () => {
                  document.querySelector('[data-workshop-kind="card"][data-workshop-id="active"]').dispatchEvent(
                    new MouseEvent("click", {bubbles:true}));
                  var clickSelected = document.querySelector('[data-workshop-kind="card"][data-workshop-id="active"]').classList.contains("selected");
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
                    selected: compact.querySelector('[data-workshop-kind="card"][data-workshop-id="active"]').classList.contains("selected"),
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
            self.assertLess(toggled["compactHeight"], initial["operationHeight"])
            self.assertEqual(toggled["pressed"], "true")
            self.assertEqual(toggled["floorPressed"], "false")
            self.assertEqual(toggled["status"], "Current layout: Compact view")
            self.assertEqual(toggled["heading"], "Compact view")
            self.assertEqual(toggled["stored"], "compact")
            self.assertTrue(toggled["clickSelected"], toggled)
            self.assertTrue(toggled["selected"], toggled)
            self.assertEqual(toggled["inspector"], "Build compact Workshop", toggled)
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

            bounded = await page.evaluate(
                """(() => {
                  var filter = document.querySelector("[data-workshop-filter]");
                  filter.value = "all";
                  filter.dispatchEvent(new Event("change", {bubbles:true}));
                  var search = document.querySelector("[data-workshop-search]");
                  search.value = "Inventory card 124";
                  search.dispatchEvent(new Event("input", {bubbles:true}));
                  var searchRows = document.querySelectorAll('[data-workshop-compact-row="work-order"]').length;
                  search = document.querySelector("[data-workshop-search]");
                  search.value = "";
                  search.dispatchEvent(new Event("input", {bubbles:true}));
                  var firstPage = document.querySelectorAll('[data-workshop-compact-row="work-order"]').length;
                  var groups = Array.from(document.querySelectorAll(".workshop-group-row"))
                    .map(function (row) { return row.textContent.trim(); });
                  document.querySelector('[data-workshop-page="next"]').click();
                  var secondPage = document.querySelectorAll('[data-workshop-compact-row="work-order"]').length;
                  var status = document.querySelector("[data-workshop-results]").textContent;
                  filter = document.querySelector("[data-workshop-filter]");
                  filter.value = "operational";
                  filter.dispatchEvent(new Event("change", {bubbles:true}));
                  return {searchRows:searchRows, firstPage:firstPage, secondPage:secondPage,
                    groups:groups, status:status,
                    domItems:document.querySelectorAll("[data-workshop-kind]").length};
                })()"""
            )
            self.assertEqual(bounded["searchRows"], 1)
            self.assertEqual(bounded["firstPage"], 20)
            self.assertEqual(bounded["secondPage"], 20)
            self.assertIn("Needs attention", bounded["groups"])
            self.assertIn("129 loaded matches", bounded["status"])
            self.assertLess(bounded["domItems"], 50)

            live = await page.evaluate(
                """(() => {
                  document.querySelector('[data-workshop-compact] [data-workshop-kind="card"][data-workshop-id="active"]').focus();
                  window.__emitWorkshopSnapshot("Session updated live");
                  return {
                    compact: !document.querySelector("[data-workshop-compact]").hidden,
                    pressed: document.querySelector('[data-workshop-view="compact"]').getAttribute("aria-pressed"),
                    text: document.querySelector("[data-workshop-compact]").textContent,
                    selected: document.querySelector('[data-workshop-compact] [data-workshop-id="active"]').classList.contains("selected"),
                    focusId: document.activeElement.dataset.workshopId,
                    inspector: document.querySelector("[data-workshop-inspector] h3").textContent
                  };
                })()"""
            )
            self.assertTrue(live["compact"])
            self.assertEqual(live["pressed"], "true")
            self.assertIn("Session updated live", live["text"])
            self.assertTrue(live["selected"])
            self.assertEqual(live["focusId"], "active")
            self.assertEqual(live["inspector"], "Build compact Workshop")

            reservation_transition = await page.evaluate(
                """(() => {
                  var root = document.querySelector("#pa-workshop-root");
                  var reserved = JSON.parse(JSON.stringify(window.__snapshot));
                  reserved.generated_at = "2026-08-03T10:00:02Z";
                  var waiting = reserved.work_orders.find(function (order) { return order.id === "waiting"; });
                  waiting.dispatch_current = true;
                  waiting.dispatch_state = "waiting_capacity";
                  waiting.dispatch_label = "Waiting for capacity";
                  waiting.live = false;
                  waiting.attention = true;
                  waiting.session = null;
                  waiting.reservation = {id:"dispatch:waiting", dispatch_id:"waiting",
                    relationship_kind:"reservation", label:"Dispatch reservation", state:"queued",
                    state_label:"Queued", reason:"All workers are occupied", queue_position:2};
                  window.PAWorkshopTest.acceptSnapshot(root, reserved);
                  document.querySelector('[data-workshop-compact] [data-workshop-id="waiting"]').click();
                  var reservationPanel = document.querySelector("[data-workshop-inspector]");
                  var before = {pressed:document.querySelector('[data-workshop-compact] [data-workshop-id="waiting"]')
                    .getAttribute("aria-pressed"), text:reservationPanel.textContent,
                    sessionLink:!!reservationPanel.querySelector('[data-workshop-focus-key="session-detail"]')};
                  var started = JSON.parse(JSON.stringify(reserved));
                  started.generated_at = "2026-08-03T10:00:03Z";
                  waiting = started.work_orders.find(function (order) { return order.id === "waiting"; });
                  waiting.reservation = null;
                  waiting.session = {id:"waiting-session", title:"Waiting follow-up",
                    relationship_label:"Session: Waiting follow-up", href:"/agent/waiting-session",
                    latest_progress:"Started"};
                  waiting.activity_state = "working";
                  waiting.activity_label = "Working";
                  waiting.live = true;
                  window.PAWorkshopTest.acceptSnapshot(root, started);
                  window.__snapshot = started;
                  var afterPanel = document.querySelector("[data-workshop-inspector]");
                  return {before:before,
                    afterPressed:document.querySelector('[data-workshop-compact] [data-workshop-id="waiting"]')
                      .getAttribute("aria-pressed"), afterText:afterPanel.textContent,
                    sessionLink:!!afterPanel.querySelector('[data-workshop-focus-key="session-detail"]')};
                })()"""
            )
            self.assertEqual(reservation_transition["before"]["pressed"], "true")
            self.assertIn(
                "Dispatch reservation", reservation_transition["before"]["text"]
            )
            self.assertFalse(reservation_transition["before"]["sessionLink"])
            self.assertEqual(reservation_transition["afterPressed"], "true")
            self.assertIn("Waiting follow-up", reservation_transition["afterText"])
            self.assertTrue(reservation_transition["sessionLink"])

            filter_safe_refresh = await page.evaluate(
                """(() => {
                  var root = document.querySelector("#pa-workshop-root");
                  document.querySelector('[data-workshop-compact] [data-workshop-id="active"]').click();
                  var search = document.querySelector("[data-workshop-search]");
                  search.value = "waiting card";
                  search.dispatchEvent(new Event("input", {bubbles:true}));
                  var before = document.querySelectorAll('[data-workshop-compact-row="work-order"]').length;
                  var update = JSON.parse(JSON.stringify(window.__snapshot));
                  update.generated_at = "2026-08-03T10:00:04Z";
                  window.PAWorkshopTest.acceptSnapshot(root, update);
                  window.__snapshot = update;
                  var after = document.querySelectorAll('[data-workshop-compact-row="work-order"]').length;
                  var context = document.querySelector("[data-workshop-selection-context]");
                  search = document.querySelector("[data-workshop-search]");
                  var searchValue = search.value;
                  var status = document.querySelector("[data-workshop-results]").textContent;
                  search.value = "";
                  search.dispatchEvent(new Event("input", {bubbles:true}));
                  return {before:before, after:after, context:context && context.textContent,
                    searchValue:searchValue, status:status};
                })()"""
            )
            self.assertEqual(filter_safe_refresh["before"], 1)
            self.assertEqual(filter_safe_refresh["after"], 1)
            self.assertEqual(filter_safe_refresh["searchValue"], "waiting card")
            self.assertIn("of 1 loaded matches", filter_safe_refresh["status"])
            self.assertIn("outside", filter_safe_refresh["context"])

            focus_and_quiet_summary = await page.evaluate(
                """(() => {
                  var root = document.querySelector("#pa-workshop-root");
                  document.querySelector('[data-workshop-compact] [data-workshop-id="active"]').click();
                  var link = document.querySelector('[data-workshop-focus-key="card-detail"]');
                  link.focus();
                  var status = document.querySelector("[data-workshop-results]");
                  var observer = new MutationObserver(function () {});
                  observer.observe(status, {subtree:true, childList:true, characterData:true});
                  var update = JSON.parse(JSON.stringify(window.__snapshot));
                  update.generated_at = "2026-08-03T10:00:05Z";
                  update.work_orders[0].session.latest_progress = "Focus-safe update";
                  window.PAWorkshopTest.acceptSnapshot(root, update);
                  window.__snapshot = update;
                  var mutations = observer.takeRecords().length;
                  observer.disconnect();
                  return {focusKey:document.activeElement.dataset.workshopFocusKey,
                    mutations:mutations,
                    progress:document.querySelector("[data-workshop-inspector]").textContent};
                })()"""
            )
            self.assertEqual(focus_and_quiet_summary["focusKey"], "card-detail")
            self.assertEqual(focus_and_quiet_summary["mutations"], 0)
            self.assertIn("Focus-safe update", focus_and_quiet_summary["progress"])

            disappearing_inspector_link = await page.evaluate(
                """(() => {
                  var root = document.querySelector("#pa-workshop-root");
                  document.querySelector('[data-workshop-compact] [data-workshop-id="active"]').click();
                  var link = document.querySelector('[data-workshop-focus-key="session-detail"]');
                  link.focus();
                  var update = JSON.parse(JSON.stringify(window.__snapshot));
                  update.generated_at = "2026-08-03T10:00:06Z";
                  var active = update.work_orders.find(function (order) { return order.id === "active"; });
                  active.session = null;
                  window.PAWorkshopTest.acceptSnapshot(root, update);
                  window.__snapshot = update;
                  var selectedControl = document.querySelector(
                    '[data-workshop-compact] [data-workshop-id="active"]');
                  return {
                    selected:selectedControl.getAttribute("aria-pressed"),
                    inspector:document.querySelector("[data-workshop-inspector] h3").textContent,
                    focusKey:document.activeElement.dataset.workshopFocusKey || null,
                    focusIsInspector:document.activeElement ===
                      document.querySelector("[data-workshop-inspector]"),
                    focusIsBody:document.activeElement === document.body
                  };
                })()"""
            )
            self.assertEqual(disappearing_inspector_link["selected"], "true")
            self.assertEqual(
                disappearing_inspector_link["inspector"], "Build compact Workshop"
            )
            self.assertTrue(
                disappearing_inspector_link["focusKey"] == "card-detail"
                or disappearing_inspector_link["focusIsInspector"]
            )
            self.assertFalse(disappearing_inspector_link["focusIsBody"])

            disappeared = await page.evaluate(
                """(() => {
                  var update = JSON.parse(JSON.stringify(window.__snapshot));
                  update.generated_at = "2026-08-03T10:00:07Z";
                  update.work_orders = update.work_orders.filter(function (order) { return order.id !== "active"; });
                  window.PAWorkshopTest.acceptSnapshot(document.querySelector("#pa-workshop-root"), update);
                  return {
                    selected:document.querySelectorAll('[data-workshop-kind][aria-pressed="true"]').length,
                    inspector:document.querySelector("[data-workshop-inspector]").textContent,
                    announcement:document.querySelector("[data-workshop-announcer]").textContent,
                    focusIsSearch:document.activeElement.hasAttribute("data-workshop-search")
                  };
                })()"""
            )
            self.assertEqual(disappeared["selected"], 0)
            self.assertNotIn("Build compact Workshop", disappeared["inspector"])
            self.assertIn("selection cleared", disappeared["announcement"].lower())
            self.assertTrue(disappeared["focusIsSearch"])

            await page.evaluate(
                """(() => {
                  var restored = JSON.parse(JSON.stringify(window.__snapshot));
                  restored.generated_at = "2026-08-03T10:00:08Z";
                  window.PAWorkshopTest.acceptSnapshot(document.querySelector("#pa-workshop-root"), restored);
                })()"""
            )

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

            paging_focus = await page.evaluate(
                """(() => {
                  var filter = document.querySelector("[data-workshop-filter]");
                  filter.value = "all";
                  filter.dispatchEvent(new Event("change", {bubbles:true}));
                  var guard = 0;
                  while (guard++ < 20) {
                    var next = document.querySelector('[data-workshop-page="next"]');
                    if (!next || next.disabled) break;
                    next.focus();
                    next.click();
                  }
                  var result = {direction:document.activeElement.dataset.workshopPage,
                    page:document.querySelector("[data-workshop-pagination]").textContent};
                  filter = document.querySelector("[data-workshop-filter]");
                  filter.value = "operational";
                  filter.dispatchEvent(new Event("change", {bubbles:true}));
                  result.announcement = document.querySelector("[data-workshop-announcer]").textContent;
                  return result;
                })()"""
            )
            self.assertEqual(paging_focus["direction"], "previous")
            self.assertIn("Page 7 of 7", paging_focus["page"])
            self.assertIn("matching work orders", paging_focus["announcement"])

            await attachment.resize(800, 600)
            await asyncio.sleep(0.05)
            medium_inspector = await page.evaluate(
                """(() => {
                  window.scrollTo(0, 0);
                  var control = document.querySelector('[data-workshop-compact] [data-workshop-id="active"]');
                  control.click();
                  var panel = document.querySelector("[data-workshop-inspector]");
                  var rect = panel.getBoundingClientRect();
                  return {focused:document.activeElement === panel,
                    visible:rect.top < window.innerHeight && rect.bottom > 0,
                    width:window.innerWidth, height:window.innerHeight};
                })()"""
            )
            self.assertEqual(medium_inspector["width"], 800)
            self.assertEqual(medium_inspector["height"], 600)
            self.assertTrue(medium_inspector["focused"])
            self.assertTrue(medium_inspector["visible"])

            await attachment.resize(390, 844)
            await asyncio.sleep(0.05)
            narrow = await page.evaluate(
                """({
                  noPageOverflow: document.documentElement.scrollWidth <= window.innerWidth,
                  compactScrollable: document.querySelector("[data-workshop-compact]").scrollWidth >
                    document.querySelector("[data-workshop-compact]").clientWidth,
                  compactVisible: !document.querySelector("[data-workshop-compact]").hidden,
                  pressed: document.querySelector('[data-workshop-view="compact"]').getAttribute("aria-pressed"),
                  stateLabels: Array.from(document.querySelectorAll("[data-workshop-compact] td"))
                    .every(function (cell) { return cell.dataset.label || cell.colSpan; })
                })"""
            )
            self.assertTrue(narrow["noPageOverflow"])
            self.assertFalse(narrow["compactScrollable"])
            self.assertTrue(narrow["compactVisible"])
            self.assertEqual(narrow["pressed"], "true")
            self.assertTrue(narrow["stateLabels"])

            inspector_reachability = await page.evaluate(
                """(() => {
                  var control = document.querySelector('[data-workshop-compact] [data-workshop-id="active"]');
                  control.click();
                  var panel = document.querySelector("[data-workshop-inspector]");
                  var rect = panel.getBoundingClientRect();
                  return {
                    focused:document.activeElement === panel,
                    selected:control.getAttribute("aria-pressed"),
                    visible:rect.top < window.innerHeight && rect.bottom > 0,
                    panelAfterControl:control.compareDocumentPosition(panel) & Node.DOCUMENT_POSITION_FOLLOWING,
                    firstLink:panel.querySelector("a") && panel.querySelector("a").textContent
                  };
                })()"""
            )
            self.assertTrue(inspector_reachability["focused"])
            self.assertEqual(inspector_reachability["selected"], "true")
            self.assertTrue(inspector_reachability["visible"])
            self.assertTrue(inspector_reachability["panelAfterControl"])
            self.assertEqual(inspector_reachability["firstLink"], "Open card detail")

            await page.command(
                "Input.dispatchKeyEvent",
                {
                    "type": "keyDown",
                    "key": "Tab",
                    "code": "Tab",
                    "windowsVirtualKeyCode": 9,
                },
            )
            await page.command(
                "Input.dispatchKeyEvent",
                {
                    "type": "keyUp",
                    "key": "Tab",
                    "code": "Tab",
                    "windowsVirtualKeyCode": 9,
                },
            )
            tab_order = await page.evaluate("document.activeElement.textContent")
            self.assertEqual(tab_order, "Open card detail")

            narrow_floor = await page.evaluate(
                """(() => {
                  document.querySelector('[data-workshop-view="floor"]').click();
                  var scene = document.querySelector("[data-workshop-scene]");
                  var bay = scene.querySelector(".workshop-bays");
                  var unassigned = scene.querySelector(".workshop-unassigned");
                  var result = {
                    floorVisible: !scene.hidden,
                    noPageOverflow: document.documentElement.scrollWidth <= window.innerWidth,
                    allStates: Array.from(scene.querySelectorAll(".workshop-operation-card"))
                      .every(function (card) { return card.querySelectorAll("dt").length === 6; }),
                    baysBeforeInventory: !unassigned || bay.compareDocumentPosition(unassigned) & Node.DOCUMENT_POSITION_FOLLOWING
                  };
                  document.querySelector('[data-workshop-view="compact"]').click();
                  return result;
                })()"""
            )
            self.assertTrue(narrow_floor["floorVisible"])
            self.assertTrue(narrow_floor["noPageOverflow"])
            self.assertTrue(narrow_floor["allStates"])
            self.assertTrue(narrow_floor["baysBeforeInventory"])

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
