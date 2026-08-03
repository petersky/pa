from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager


ROOT = Path(__file__).parents[1]
STATIC = ROOT / "src" / "pa" / "server" / "static"
TEMPLATES = ROOT / "src" / "pa" / "server" / "templates"
POLICY = STATIC / "js" / "history-policy.js"


class HistoryPolicyContractTests(unittest.TestCase):
    def test_shell_and_live_pages_define_bounded_private_history(self) -> None:
        shell = (TEMPLATES / "shell.html").read_text()
        agent = (TEMPLATES / "pages" / "agent.html").read_text()
        fleet = (TEMPLATES / "pages" / "fleet.html").read_text()
        workshop = (TEMPLATES / "pages" / "workshop.html").read_text()

        self.assertIn('"historyCacheSize":3', shell)
        self.assertIn('"refreshOnHistoryMiss":false', shell)
        self.assertIn('id="app-view" class="app-view" hx-history-elt="true"', shell)
        self.assertLess(
            shell.index("js/history-policy.js"),
            shell.index("js/agent-chat.js"),
        )
        self.assertIn('hx-history="false"', agent)
        self.assertIn("data-pa-history-private", agent)
        self.assertIn('hx-history="false"', fleet)
        self.assertIn('data-pa-live-history-boundary="fleet"', fleet)
        self.assertIn('hx-history="false"', workshop)
        self.assertIn('data-pa-live-history-boundary="workshop"', workshop)

    def test_popstate_does_not_duplicate_htmx_restoration(self) -> None:
        spa = (STATIC / "js" / "spa.js").read_text()
        popstate = spa[spa.index('window.addEventListener("popstate"') :]
        self.assertLess(
            popstate.index("if (event.state && event.state.htmx) return;"),
            popstate.index("window.PANavigation.navigate"),
        )

    def test_every_live_controller_has_reload_teardown(self) -> None:
        for filename in ("spa.js", "agent-chat.js", "fleet.js", "workshop.js"):
            source = (STATIC / "js" / filename).read_text()
            self.assertIn("pa:historyWillReload", source, filename)


@unittest.skipUnless(shutil.which("node"), "node is required for history policy tests")
class HistoryPolicyNodeTests(unittest.TestCase):
    def run_scenario(self, scenario: str) -> dict:
        harness = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");

class FakeEvent {
  constructor(type, options) {
    this.type = type;
    this.detail = options && options.detail || {};
    this.defaultPrevented = false;
  }
  preventDefault() { this.defaultPrevented = true; }
}
class FakeTarget {
  constructor() { this.listeners = {}; }
  addEventListener(type, callback) {
    (this.listeners[type] ||= []).push(callback);
  }
  dispatchEvent(event) {
    event.target = this;
    (this.listeners[event.type] || []).slice().forEach(fn => fn(event));
    return !event.defaultPrevented;
  }
}
class FakeStorage {
  constructor() { this.data = new Map(); this.mode = ""; }
  get length() { return this.data.size; }
  key(index) { return Array.from(this.data.keys())[index] || null; }
  getItem(key) {
    if (this.mode === "denied") throw domError("SecurityError", 18);
    return this.data.has(key) ? this.data.get(key) : null;
  }
  setItem(key, value) {
    if (this.mode === "denied") throw domError("SecurityError", 18);
    if (this.mode === "quota" && key === "htmx-history-cache") {
      throw domError("QuotaExceededError", 22);
    }
    this.data.set(key, String(value));
  }
  removeItem(key) {
    if (this.mode === "denied") throw domError("SecurityError", 18);
    this.data.delete(key);
  }
}
function domError(name, code) {
  const error = new Error(name);
  error.name = name;
  error.code = code;
  return error;
}

const document = new FakeTarget();
document.body = { id: "body" };
document.querySelector = () => null;
document.getElementById = () => null;
const storage = new FakeStorage();
const localStorage = new FakeStorage();
const errors = [];
const location = {
  reloads: 0,
  assignments: [],
  reload() { this.reloads += 1; },
  assign(value) { this.assignments.push(value); },
};
const window = {
  TextEncoder,
  sessionStorage: storage,
  localStorage,
  location,
  htmx: { config: { historyCacheSize: 3, refreshOnHistoryMiss: true } },
  console: { error(...args) { errors.push(args); } },
  setTimeout(callback) { callback(); },
};
const context = {
  window, document, sessionStorage: storage, localStorage, location,
  htmx: window.htmx, console: window.console, TextEncoder,
  CustomEvent: FakeEvent, encodeURIComponent, unescape, setTimeout: window.setTimeout,
};
vm.createContext(context);
vm.runInContext(fs.readFileSync(process.argv[1], "utf8"), context);

function event(type, detail) {
  const value = new FakeEvent(type, { detail });
  document.dispatchEvent(value);
  return value;
}
function boot() { event("DOMContentLoaded", {}); }
function vendorSave(content) {
  const raw = storage.getItem("htmx-history-cache");
  const cache = raw ? JSON.parse(raw) : [];
  const item = { url: "/page-" + Date.now() + "-" + Math.random(), content, title: "PA", scroll: 0 };
  event("htmx:historyItemCreated", { item, cache });
  cache.push(item);
  while (cache.length > window.htmx.config.historyCacheSize) cache.shift();
  if (cache.length) storage.setItem("htmx-history-cache", JSON.stringify(cache));
  return item;
}

let result;
if (process.argv[2] === "privacy") {
  storage.setItem("htmx-history-cache", JSON.stringify([{
    url: "/legacy-agent",
    content: "<div data-agent-chat>LEGACY_TRANSCRIPT_SECRET</div>",
    title: "PA",
    scroll: 0
  }]));
  boot();
  localStorage.setItem("pa.agent-chat-draft.v1:instance:user:session", "draft-secret");
  const privateItem = vendorSave('<section data-agent-chat data-pa-history-private>transcript-secret tool-secret</section>');
  const oversizedItem = vendorSave("<main>" + "x".repeat(140 * 1024) + "</main>");
  for (let index = 0; index < 5; index += 1) vendorSave("<p>safe-" + index + "</p>");
  const raw = storage.getItem("htmx-history-cache") || "";
  const cache = JSON.parse(raw);
  const snapshot = window.PAHistoryPolicy.snapshot();
  result = {
    privateMarker: privateItem.content.includes("data-pa-history-reload"),
    oversizedMarker: oversizedItem.content.includes("data-pa-history-reload"),
    noTranscript: !raw.includes("transcript-secret") && !raw.includes("tool-secret"),
    noLegacyTranscript: !raw.includes("LEGACY_TRANSCRIPT_SECRET"),
    draft: localStorage.getItem("pa.agent-chat-draft.v1:instance:user:session"),
    entries: cache.length,
    bytes: new TextEncoder().encode(raw).length,
    snapshot,
    errors: errors.length,
  };
} else if (process.argv[2] === "quota") {
  boot();
  storage.mode = "quota";
  let htmxErrors = 0;
  document.addEventListener("htmx:historyCacheError", () => { htmxErrors += 1; });
  const cache = [];
  const item = { url: "/agent", content: "<p>safe</p>", title: "PA", scroll: 0 };
  event("htmx:historyItemCreated", { item, cache });
  // This is the remainder of HTMX's save loop. The policy must make it empty.
  cache.push(item);
  while (cache.length > window.htmx.config.historyCacheSize) cache.shift();
  while (cache.length) {
    try { storage.setItem("htmx-history-cache", JSON.stringify(cache)); break; }
    catch (cause) { event("htmx:historyCacheError", { cause, cache }); cache.shift(); }
  }
  let prepared = 0;
  document.addEventListener("pa:historyWillReload", () => { prepared += 1; });
  const swap = event("htmx:beforeSwap", {
    target: { id: "app-view" },
    shouldSwap: true,
    pathInfo: { finalRequestPath: "/workshop" },
  });
  result = {
    disabled: window.PAHistoryPolicy.snapshot().disabled,
    classification: window.PAHistoryPolicy.snapshot().last_diagnostic.classification,
    historyCacheErrors: htmxErrors,
    prevented: swap.defaultPrevented,
    destination: location.assignments[0],
    prepared,
    errors: errors.length,
  };
} else if (process.argv[2] === "denied") {
  storage.mode = "denied";
  boot();
  let prepared = 0;
  document.addEventListener("pa:historyWillReload", () => { prepared += 1; });
  event("htmx:historyCacheMiss", {});
  result = {
    disabled: window.PAHistoryPolicy.snapshot().disabled,
    classification: window.PAHistoryPolicy.snapshot().last_diagnostic.classification,
    denied: window.PAHistoryPolicy.snapshot().counters.deniedFailures,
    misses: window.PAHistoryPolicy.snapshot().counters.cacheMissReloads,
    prepared,
    errors: errors.length,
  };
} else if (process.argv[2] === "unexpected") {
  boot();
  const cache = [{}, {}, {}];
  event("htmx:historyCacheError", { cause: new TypeError("serialization failed"), cache });
  result = {
    cacheLength: cache.length,
    disabled: window.PAHistoryPolicy.snapshot().disabled,
    classification: window.PAHistoryPolicy.snapshot().last_diagnostic.classification,
    errorSignals: window.PAHistoryPolicy.snapshot().counters.historyCacheErrors,
    errors: errors.length,
    diagnosticKeys: Object.keys(errors[0][1]).sort(),
  };
}
process.stdout.write(JSON.stringify(result));
"""
        completed = subprocess.run(
            ["node", "-e", harness, str(POLICY), scenario],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    def test_private_and_oversized_dom_are_replaced_and_cache_is_bounded(self) -> None:
        result = self.run_scenario("privacy")
        self.assertTrue(result["privateMarker"])
        self.assertTrue(result["oversizedMarker"])
        self.assertTrue(result["noTranscript"])
        self.assertTrue(result["noLegacyTranscript"])
        self.assertEqual(result["draft"], "draft-secret")
        self.assertLessEqual(result["entries"], 3)
        self.assertLessEqual(result["bytes"], 256 * 1024)
        self.assertEqual(result["errors"], 0)
        self.assertEqual(result["snapshot"]["counters"]["privateSnapshotsBlocked"], 1)
        self.assertEqual(result["snapshot"]["counters"]["oversizedSnapshots"], 1)
        self.assertEqual(result["snapshot"]["counters"]["privateEntriesPurged"], 1)

    def test_quota_failure_disables_current_cache_without_htmx_error_loop(self) -> None:
        result = self.run_scenario("quota")
        self.assertTrue(result["disabled"])
        self.assertEqual(result["classification"], "quota")
        self.assertEqual(result["historyCacheErrors"], 0)
        self.assertTrue(result["prevented"])
        self.assertEqual(result["destination"], "/workshop")
        self.assertEqual(result["prepared"], 1)
        self.assertEqual(result["errors"], 0)

    def test_storage_denial_is_quiet_and_cache_miss_prepares_clean_reload(self) -> None:
        result = self.run_scenario("denied")
        self.assertTrue(result["disabled"])
        self.assertEqual(result["classification"], "miss")
        self.assertEqual(result["denied"], 1)
        self.assertEqual(result["misses"], 1)
        self.assertEqual(result["prepared"], 1)
        self.assertEqual(result["errors"], 0)

    def test_unexpected_htmx_failure_is_single_and_content_free(self) -> None:
        result = self.run_scenario("unexpected")
        self.assertEqual(result["cacheLength"], 0)
        self.assertTrue(result["disabled"])
        self.assertEqual(result["classification"], "unexpected")
        self.assertEqual(result["errorSignals"], 1)
        self.assertEqual(result["errors"], 1)
        self.assertEqual(
            result["diagnosticKeys"],
            [
                "cache_bytes",
                "cache_entries",
                "classification",
                "expected",
                "phase",
                "schema",
                "snapshot_bytes",
                "surface",
            ],
        )


@unittest.skipUnless(_browser_executable(), "managed Chromium is not installed")
class HistoryPolicyManagedBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_navigation_is_private_and_back_restores_by_clean_reload(
        self,
    ) -> None:
        policy_source = POLICY.read_text()
        htmx_source = (
            STATIC / "vendor" / "htmx" / "htmx-2.0.10.min.js"
        ).read_text()
        full_requests: list[str] = []

        def surface(path: str) -> str:
            route = path.split("?", 1)[0]
            if route == "/workshop":
                return (
                    '<section id="pa-workshop-root" hx-history="false" '
                    'data-live-root="workshop">Workshop</section>'
                )
            if route == "/fleet":
                return (
                    '<section id="pa-fleet-root" hx-history="false" '
                    'data-live-root="fleet">Fleet</section>'
                )
            if route == "/safe":
                return '<section id="safe" data-live-root="safe">Safe shell</section>'
            return """
<section class="page-agent" hx-history="false" data-pa-history-private
         data-live-root="agent">
  <div data-agent-chat>
    <p id="transcript">TRANSCRIPT_SECRET TOOL_OUTPUT_SECRET</p>
    <textarea id="draft"></textarea>
  </div>
</section>"""

        def shell(path: str) -> str:
            return f"""<!doctype html>
<meta name="htmx-config" content='{{"historyCacheSize":3,"refreshOnHistoryMiss":false}}'>
<nav>
  <a id="to-agent" href="/agent" hx-get="/agent" hx-target="#app-view"
     hx-push-url="true">Agent</a>
  <a id="to-workshop" href="/workshop" hx-get="/workshop" hx-target="#app-view"
     hx-push-url="true">Workshop</a>
  <a id="to-fleet" href="/fleet" hx-get="/fleet" hx-target="#app-view"
     hx-push-url="true">Fleet</a>
</nav>
<main id="app-view" hx-history-elt="true">{surface(path)}</main>
<script>
window.__activeControllers = 0;
window.__controllerRoot = null;
function teardownController() {{
  window.__activeControllers = 0;
  window.__controllerRoot = null;
}}
function initializeController() {{
  const root = document.querySelector("[data-live-root]");
  if (root && root === window.__controllerRoot) return;
  teardownController();
  if (root) {{
    window.__controllerRoot = root;
    window.__activeControllers = 1;
  }}
}}
document.addEventListener("DOMContentLoaded", initializeController);
document.addEventListener("htmx:afterSwap", initializeController);
document.addEventListener("htmx:beforeSwap", teardownController);
document.addEventListener("pa:historyWillReload", function () {{
  teardownController();
  localStorage.setItem(
    "pa.history.test.reload-prep",
    String(Number(localStorage.getItem("pa.history.test.reload-prep") || 0) + 1)
  );
}});
document.addEventListener("htmx:historyCacheError", function () {{
  localStorage.setItem(
    "pa.history.test.htmx-errors",
    String(Number(localStorage.getItem("pa.history.test.htmx-errors") || 0) + 1)
  );
}});
document.addEventListener("pa:historyDiagnostic", function (event) {{
  localStorage.setItem(
    "pa.history.test.diagnostic",
    JSON.stringify(event.detail)
  );
}});
</script>
<script>{policy_source}</script>
<script>{htmx_source}</script>"""

        async def serve(reader, writer) -> None:
            try:
                request = await reader.readuntil(b"\r\n\r\n")
                header = request.decode("latin-1")
                first = header.splitlines()[0]
                path = first.split(" ")[1]
                is_htmx = "\r\nHX-Request: true\r\n" in header
                if is_htmx:
                    body = surface(path).encode()
                else:
                    full_requests.append(path)
                    body = shell(path).encode()
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
        manager = BrowserSessionManager(
            browser, instance_id="history-contract", idle_ttl_seconds=60
        )
        scope = BrowserScope("user:history", "session-history", "history-contract")
        try:
            await manager.attach(
                scope, url=f"http://127.0.0.1:{port}/agent", width=900, height=700
            )
            page = manager.resolve(scope).page
            await page.evaluate(
                """(() => {
                  localStorage.removeItem("pa.history.test.reload-prep");
                  localStorage.removeItem("pa.history.test.htmx-errors");
                  localStorage.setItem("pa.agent-chat-draft.v1:test", "DRAFT_SECRET");
                  document.querySelector("#to-workshop").click();
                })()"""
            )
            await asyncio.sleep(0.1)
            await page.evaluate('document.querySelector("#to-fleet").click()')
            await asyncio.sleep(0.1)
            fleet = await page.evaluate(
                """({
                  path: location.pathname,
                  root: document.querySelector("[data-live-root]").dataset.liveRoot,
                  active: window.__activeControllers,
                  cache: sessionStorage.getItem("htmx-history-cache"),
                  errors: Number(localStorage.getItem("pa.history.test.htmx-errors") || 0)
                })"""
            )
            self.assertEqual(fleet["path"], "/fleet")
            self.assertEqual(fleet["root"], "fleet")
            self.assertEqual(fleet["active"], 1)
            self.assertFalse(fleet["cache"])
            self.assertEqual(fleet["errors"], 0)

            await page.evaluate("history.back()")
            for _ in range(50):
                await asyncio.sleep(0.05)
                restored = await page.evaluate(
                    """({
                      path: location.pathname,
                      ready: document.readyState,
                      root: document.querySelector("[data-live-root]") &&
                            document.querySelector("[data-live-root]").dataset.liveRoot,
                      active: window.__activeControllers,
                      draft: localStorage.getItem("pa.agent-chat-draft.v1:test"),
                      prep: Number(localStorage.getItem("pa.history.test.reload-prep") || 0),
                      errors: Number(localStorage.getItem("pa.history.test.htmx-errors") || 0),
                      cache: sessionStorage.getItem("htmx-history-cache")
                    })"""
                )
                if (
                    restored["path"] == "/workshop"
                    and restored["ready"] == "complete"
                    and restored["root"] == "workshop"
                ):
                    break
            else:
                self.fail(f"Workshop history restoration did not finish: {restored}")

            self.assertEqual(restored["active"], 1)
            self.assertEqual(restored["draft"], "DRAFT_SECRET")
            self.assertEqual(restored["prep"], 1)
            self.assertEqual(restored["errors"], 0)
            self.assertFalse(restored["cache"])
            self.assertGreaterEqual(full_requests.count("/workshop"), 1)

            await page.evaluate("history.forward()")
            for _ in range(50):
                await asyncio.sleep(0.05)
                forward = await page.evaluate(
                    """({
                      path: location.pathname,
                      ready: document.readyState,
                      root: document.querySelector("[data-live-root]") &&
                            document.querySelector("[data-live-root]").dataset.liveRoot,
                      active: window.__activeControllers,
                      errors: Number(localStorage.getItem("pa.history.test.htmx-errors") || 0)
                    })"""
                )
                if (
                    forward["path"] == "/fleet"
                    and forward["ready"] == "complete"
                    and forward["root"] == "fleet"
                ):
                    break
            else:
                self.fail(f"Fleet forward restoration did not finish: {forward}")
            self.assertEqual(forward["active"], 1)
            self.assertEqual(forward["errors"], 0)

            # A real Storage QuotaExceededError is preflighted by PA. HTMX
            # emits no historyCacheError loop, the fragment swap is cancelled,
            # and the requested destination is loaded as a full document.
            await page.evaluate("location.assign('/safe')")
            for _ in range(50):
                await asyncio.sleep(0.05)
                safe = await page.evaluate(
                    """({
                      path: location.pathname,
                      ready: document.readyState,
                      root: document.querySelector("[data-live-root]") &&
                            document.querySelector("[data-live-root]").dataset.liveRoot
                    })"""
                )
                if safe["path"] == "/safe" and safe["ready"] == "complete":
                    break
            else:
                self.fail(f"Safe history harness did not load: {safe}")
            await page.evaluate(
                """(() => {
                  localStorage.removeItem("pa.history.test.diagnostic");
                  localStorage.setItem("pa.history.test.htmx-errors", "0");
                  const nativeSetItem = Storage.prototype.setItem;
                  Storage.prototype.setItem = function(key, value) {
                    if (this === sessionStorage && key === "htmx-history-cache") {
                      throw new DOMException("quota", "QuotaExceededError");
                    }
                    return nativeSetItem.call(this, key, value);
                  };
                  document.querySelector("#to-workshop").click();
                })()"""
            )
            for _ in range(50):
                await asyncio.sleep(0.05)
                quota = await page.evaluate(
                    """({
                      path: location.pathname,
                      ready: document.readyState,
                      root: document.querySelector("[data-live-root]") &&
                            document.querySelector("[data-live-root]").dataset.liveRoot,
                      diagnostic: JSON.parse(
                        localStorage.getItem("pa.history.test.diagnostic") || "null"
                      ),
                      errors: Number(
                        localStorage.getItem("pa.history.test.htmx-errors") || 0
                      ),
                      draft: localStorage.getItem("pa.agent-chat-draft.v1:test"),
                      active: window.__activeControllers
                    })"""
                )
                if (
                    quota["path"] == "/workshop"
                    and quota["ready"] == "complete"
                    and quota["root"] == "workshop"
                ):
                    break
            else:
                self.fail(f"Quota fallback did not finish: {quota}")
            self.assertEqual(quota["diagnostic"]["classification"], "quota")
            self.assertEqual(quota["diagnostic"]["phase"], "preflight")
            self.assertEqual(quota["errors"], 0)
            self.assertEqual(quota["draft"], "DRAFT_SECRET")
            self.assertEqual(quota["active"], 1)
        finally:
            await manager.close()
            await browser.close()
            server.close()
            await server.wait_closed()
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
