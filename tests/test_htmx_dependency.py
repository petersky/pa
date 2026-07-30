from __future__ import annotations

import asyncio
import hashlib
import re
import tempfile
import unittest
from pathlib import Path

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager


ROOT = Path(__file__).parents[1]
STATIC = ROOT / "src" / "pa" / "server" / "static"
TEMPLATES = ROOT / "src" / "pa" / "server" / "templates"
HTMX = STATIC / "vendor" / "htmx" / "htmx-2.0.10.min.js"
LICENSE = STATIC / "vendor" / "htmx" / "LICENSE-2.0.10.txt"


class HtmxDependencyContractTests(unittest.TestCase):
    def test_reviewed_asset_is_exact_local_version_with_license(self) -> None:
        source = HTMX.read_bytes()
        self.assertEqual(
            hashlib.sha256(source).hexdigest(),
            "71ea67185bfa8c98c39d31717c6fce5d852370fcdfd129db4543774d3145c0de",
        )
        self.assertIn('version:"2.0.10"', source.decode())
        self.assertEqual(
            hashlib.sha256(LICENSE.read_bytes()).hexdigest(),
            "d3d2456f76414f2456104660ebd65aff1c04cd7966b942bdabd63f3cdb316a38",
        )

    def test_shell_uses_cache_busted_local_asset_and_explicit_status_policy(self) -> None:
        shell = (TEMPLATES / "shell.html").read_text()
        self.assertIn(
            "{{ static_url('vendor/htmx/htmx-2.0.10.min.js') }}", shell
        )
        self.assertNotIn("cdn.jsdelivr.net/npm/htmx", shell)
        self.assertIn('{"code":"204","swap":false}', shell)
        self.assertIn('{"code":"304","swap":false}', shell)
        self.assertIn('{"code":"[23]..","swap":true}', shell)
        self.assertIn('{"code":"[45]..","swap":false,"error":true}', shell)

    def test_templates_only_use_reviewed_declarative_surface(self) -> None:
        allowed = {
            "hx-confirm",
            "hx-delete",
            "hx-get",
            "hx-history",
            "hx-history-elt",
            "hx-post",
            "hx-preserve",
            "hx-push-url",
            "hx-swap",
            "hx-target",
            "hx-trigger",
        }
        found: set[str] = set()
        for path in TEMPLATES.rglob("*.html"):
            found.update(re.findall(r"\b(hx-[a-z-]+)(?::[a-z-]+)?=", path.read_text()))
        self.assertEqual(found, allowed)

    def test_javascript_uses_only_htmx_2_event_contracts(self) -> None:
        sources = "\n".join(path.read_text() for path in (STATIC / "js").glob("*.js"))
        required = {
            "htmx:afterSwap",
            "htmx:beforeSwap",
            "htmx:configRequest",
            "htmx:historyRestore",
            "htmx:pushedIntoHistory",
            "htmx:replacedInHistory",
            "htmx:responseError",
        }
        for event in required:
            self.assertIn(f'"{event}"', sources)
        for removed in (
            "htmx:after:swap",
            "htmx:before:swap",
            "htmx:config:request",
            "htmx:after:history:update",
            "htmx:response:error",
        ):
            self.assertNotIn(removed, sources)

    def test_programmatic_surface_and_fleet_cancellation_are_explicit(self) -> None:
        spa = (STATIC / "js" / "spa.js").read_text()
        fleet = (STATIC / "js" / "fleet.js").read_text()
        self.assertIn("htmx.process(panel)", spa)
        self.assertIn("htmx.process(content)", spa)
        self.assertIn('htmx.ajax("GET"', spa)
        self.assertIn("new AbortController()", fleet)
        self.assertIn("controller.abort()", fleet)
        self.assertIn("htmx.swap(target, html", fleet)
        self.assertIn('"HX-Request": "true"', fleet)
        self.assertIn("isExpectedHtmxAbort(error)", fleet)
        self.assertIn(".catch(function (error)", fleet)


@unittest.skipUnless(_browser_executable(), "managed Chromium is not installed")
class HtmxManagedBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_vendored_runtime_processes_fragment_and_swaps_cleanly(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        browser = BrowserManager(Path(temp_dir.name))
        manager = BrowserSessionManager(
            browser, instance_id="htmx-contract", idle_ttl_seconds=60
        )
        scope = BrowserScope("user:htmx", "session-htmx", "htmx-contract")
        htmx_source = HTMX.read_text()
        html = f"""<!doctype html>
<meta name="htmx-config" content='{{"responseHandling":[{{"code":"204","swap":false}},{{"code":"304","swap":false}},{{"code":"[23]..","swap":true}},{{"code":"[45]..","swap":false,"error":true}}]}}'>
<div id="target">initial</div>
<script>
window.__errors = [];
addEventListener("error", event => __errors.push(String(event.error || event.message)));
addEventListener("unhandledrejection", event => __errors.push(String(event.reason)));
class FakeXHR {{
  constructor() {{
    this.upload = {{ addEventListener() {{}} }};
    this.status = 0;
    this.response = "";
    this.responseText = "";
    this.statusText = "";
    this.responseURL = location.href;
  }}
  addEventListener() {{}}
  open(method, url) {{ this.method = method; this.url = url; }}
  overrideMimeType() {{}}
  setRequestHeader(name, value) {{
    (this.headers ||= {{}})[name] = value;
  }}
  getAllResponseHeaders() {{ return ""; }}
  getResponseHeader() {{ return null; }}
  send() {{
    this.status = 200;
    this.response = this.responseText = '<p id="result">loaded</p>';
    setTimeout(() => this.onload(), 0);
  }}
  abort() {{ if (this.onabort) this.onabort(); }}
}}
window.XMLHttpRequest = FakeXHR;
</script>
<script>{htmx_source}</script>
<script>
document.addEventListener("DOMContentLoaded", () => {{
  const button = document.createElement("button");
  button.id = "load";
  button.setAttribute("hx-get", "/fragment");
  button.setAttribute("hx-target", "#target");
  document.body.appendChild(button);
  htmx.process(button);
  button.click();
}});
</script>"""
        async def serve_harness(reader, writer) -> None:
            await reader.readuntil(b"\r\n\r\n")
            body = html.encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(serve_harness, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            try:
                await manager.attach(
                    scope, url=f"http://127.0.0.1:{port}/", width=900, height=700
                )
            except (PermissionError, RuntimeError) as exc:
                if isinstance(exc, PermissionError) or "did not expose a usable page" in str(exc):
                    self.skipTest(str(exc))
                raise
            await asyncio.sleep(0.15)
            state = await manager.resolve(scope).page.evaluate(
                """({
                  version: htmx.version,
                  result: document.querySelector('#target').textContent,
                  errors: window.__errors,
                  config: htmx.config.responseHandling
                })"""
            )
            self.assertEqual(state["version"], "2.0.10")
            self.assertEqual(state["result"], "loaded")
            self.assertEqual(state["errors"], [])
            self.assertFalse(state["config"][0]["swap"])
            self.assertFalse(state["config"][1]["swap"])
        finally:
            await manager.close()
            await browser.close()
            server.close()
            await server.wait_closed()
            temp_dir.cleanup()
