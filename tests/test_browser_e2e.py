import asyncio
import tempfile
import unittest
from pathlib import Path
from urllib.parse import quote

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionError, BrowserSessionManager


@unittest.skipUnless(_browser_executable(), "managed Chromium is not installed")
class ManagedBrowserInteractionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.browser = BrowserManager(Path(self.temp_dir.name))
        self.manager = BrowserSessionManager(
            self.browser, instance_id="e2e-instance", idle_ttl_seconds=60
        )
        self.scope = BrowserScope("user:e2e", "session-e2e", "e2e-instance")
        html = """
        <!doctype html>
        <style>
          #scroll { width: 200px; height: 80px; overflow: auto }
          #spacer { height: 500px }
          #source, #target { width: 80px; height: 40px; margin: 12px }
        </style>
        <button id="button">Button</button>
        <form id="form"><input id="input"><button>Submit</button></form>
        <div id="scroll"><div id="spacer"></div></div>
        <div id="source">Source</div><div id="target">Target</div>
        <script>
          const button = document.querySelector('#button');
          button.addEventListener('mouseover', () => document.body.dataset.hover = 'yes');
          button.addEventListener('contextmenu', event => {
            event.preventDefault(); document.body.dataset.context = 'yes';
          });
          button.addEventListener('dblclick', () => document.body.dataset.double = 'yes');
          document.addEventListener('keydown', event => {
            if (event.key === 'k')
              document.body.dataset.key = `${event.ctrlKey}:${event.shiftKey}:${event.key}`;
          });
          document.querySelector('#form').addEventListener('submit', event => {
            event.preventDefault(); document.body.dataset.submit = 'yes';
          });
          let held = false;
          document.querySelector('#source').addEventListener('pointerdown', () => held = true);
          document.querySelector('#target').addEventListener('pointerup', () => {
            if (held) document.body.dataset.drag = 'yes';
          });
        </script>
        """
        await self.manager.attach(
            self.scope, url=f"data:text/html,{quote(html)}", width=900, height=700
        )

    async def asyncTearDown(self):
        await self.manager.close()
        await self.browser.close()
        self.temp_dir.cleanup()

    async def test_semantic_and_compound_interactions_in_managed_chromium(self):
        await self.manager.hover(self.scope, selector="#button")
        await self.manager.click(self.scope, selector="#button", button="right")
        await self.manager.click(self.scope, selector="#button", click_count=2)
        await self.manager.press(self.scope, key="k", modifiers=["Control", "Shift"])
        await self.manager.scroll(self.scope, selector="#scroll", delta_y=120)
        await self.manager.drag(
            self.scope, source_selector="#source", target_selector="#target", steps=4
        )
        await self.manager.type_text(
            self.scope, selector="#input", text="hello", submit=True, delay_ms=1
        )
        await self.manager.actions(
            self.scope,
            actions=[
                {"type": "pointer_move", "x": 10, "y": 10},
                {"type": "pointer_down", "button": "middle"},
                {"type": "key_down", "key": "Alt"},
                {"type": "pause", "duration_ms": 10},
                {"type": "key_up", "key": "Alt"},
                {"type": "pointer_up", "button": "middle"},
            ],
        )
        await asyncio.sleep(0.05)

        session = self.manager.resolve(self.scope)
        state = await session.page.evaluate(
            """({
              hover: document.body.dataset.hover,
              context: document.body.dataset.context,
              double: document.body.dataset.double,
              key: document.body.dataset.key,
              submit: document.body.dataset.submit,
              drag: document.body.dataset.drag,
              scroll: document.querySelector('#scroll').scrollTop,
              value: document.querySelector('#input').value
            })"""
        )
        self.assertEqual(state["hover"], "yes")
        self.assertEqual(state["context"], "yes")
        self.assertEqual(state["double"], "yes")
        self.assertEqual(state["key"], "true:true:k")
        self.assertEqual(state["submit"], "yes")
        self.assertEqual(state["drag"], "yes")
        self.assertGreater(state["scroll"], 0)
        self.assertEqual(state["value"], "hello")
        self.assertFalse(session.held_buttons)
        self.assertFalse(session.held_keys)

        with self.assertRaises(BrowserSessionError) as invalid_selector:
            await self.manager.click(self.scope, selector="[")
        self.assertEqual(invalid_selector.exception.code, "invalid_selector")

        snapshot = await self.manager.snapshot(self.scope)
        ref = snapshot["elements"][0]["ref"]
        await session.page.evaluate(
            "document.body.setAttribute('data-revision', String(Date.now()))"
        )
        await asyncio.sleep(0)
        # Live-region and telemetry mutations must not invalidate a semantic ref
        # when its target and document identity are unchanged.
        clicked = await self.manager.click(self.scope, ref=ref)
        self.assertTrue(clicked["ok"])

    async def test_telemetry_report_bounds_dom_and_pages_accessible_values(self):
        session = self.manager.resolve(self.scope)
        html = """
        <!doctype html>
        <form data-telemetry-filters>
          <select name="view"><option value="fleet">Fleet</option></select>
          <select name="instance_id" multiple><option value="">All</option></select>
          <select name="provider_id"><option value="">All</option></select>
          <select name="card_id"><option value="">All</option></select>
          <select name="scope_id"><option value="">All</option></select>
          <select name="range"><option value="1h">1h</option></select>
          <div data-custom-range hidden></div>
        </form>
        <div data-telemetry-report data-default-range="1h">
          <div data-telemetry-legend></div>
          <div data-telemetry-gaps hidden></div>
          <div data-report-diagnostics></div>
          <p class="sr-only" data-chart-live-output role="status" aria-live="polite" aria-atomic="true"></p>
          <section data-chart-group="count" data-unit="count" data-metrics="metric.one">
            <span data-chart-unit></span>
            <div class="telemetry-chart-stage" tabindex="0" role="img" aria-label="Count chart">
              <svg aria-hidden="true"><g data-chart-grid></g><g data-chart-axes></g><g data-chart-lines></g>
                <g data-chart-points></g><line data-chart-cursor hidden></line></svg>
              <div data-chart-tooltip hidden></div>
              <p data-chart-empty></p>
            </div>
            <div data-chart-status></div>
          </section>
          <p data-telemetry-summary></p>
          <button type="button" data-telemetry-table-prev disabled>Previous values</button>
          <span data-telemetry-table-page role="status" aria-live="polite"></span>
          <button type="button" data-telemetry-table-next disabled>Next values</button>
          <table><tbody data-telemetry-table-body></tbody></table>
        </div>
        """
        await session.page.navigate_and_wait(f"data:text/html,{quote(html)}")
        await session.page.evaluate(
            """(() => {
              const start = Date.parse("2026-01-01T00:00:00Z");
              const series = [0, 1].map(instance => ({
                instance_name: `Instance ${instance}`,
                scope_id: `instance-${instance}`,
                metric: "metric.one",
                unit: "count",
                gaps: [],
                points: Array.from({length: 400}, (_, bucket) => ({
                  timestamp: new Date(start + bucket * 60000).toISOString(),
                  avg: bucket + instance,
                  value_count: 1,
                  sample_count: 1,
                  quality: "measured"
                }))
              }));
              const data = {
                start: "2026-01-01T00:00:00Z",
                end: "2026-01-01T06:40:00Z",
                bucket_seconds: 60,
                series,
                failures: []
              };
              window.fetch = () => Promise.resolve({
                ok: true, json: () => Promise.resolve(data)
              });
              window.setInterval = () => 0;
            })()"""
        )
        script = Path(__file__).parents[1] / "src/pa/server/static/js/telemetry.js"
        await session.page.evaluate(script.read_text())
        await session.page.evaluate(
            "document.dispatchEvent(new Event('DOMContentLoaded'))"
        )
        for _ in range(100):
            row_count = await session.page.evaluate(
                "document.querySelectorAll('[data-telemetry-table-body] tr').length"
            )
            if row_count == 100:
                break
            await asyncio.sleep(0.05)
        self.assertEqual(row_count, 100)

        initial = await session.page.evaluate(
            """({
              rows: document.querySelectorAll("[data-telemetry-table-body] tr").length,
              circles: document.querySelectorAll("[data-chart-points] circle").length,
              page: document.querySelector("[data-telemetry-table-page]").textContent,
              first: document.querySelector("[data-telemetry-table-body] tr").textContent,
              nextDisabled: document.querySelector("[data-telemetry-table-next]").disabled
            })"""
        )
        self.assertEqual(initial["rows"], 100)
        self.assertLessEqual(initial["circles"], 160)
        self.assertEqual(initial["page"], "Page 1 of 8.")
        self.assertFalse(initial["nextDisabled"])

        await self.manager.click(self.scope, selector="[data-telemetry-table-next]")
        second = await session.page.evaluate(
            """({
              rows: document.querySelectorAll("[data-telemetry-table-body] tr").length,
              page: document.querySelector("[data-telemetry-table-page]").textContent,
              first: document.querySelector("[data-telemetry-table-body] tr").textContent
            })"""
        )
        self.assertEqual(second["rows"], 100)
        self.assertEqual(second["page"], "Page 2 of 8.")
        self.assertNotEqual(second["first"], initial["first"])

        await self.manager.click(self.scope, selector=".telemetry-chart-stage")
        await self.manager.press(self.scope, key="ArrowLeft")
        chart_label = await session.page.evaluate(
            "document.querySelector('.telemetry-chart-stage').getAttribute('aria-label')"
        )
        self.assertIn("Instance 0", chart_label)
        self.assertIn("metric.one", chart_label)
        accessibility = await session.page.evaluate(
            """({
              stageLabel: document.querySelector(".telemetry-chart-stage").getAttribute("aria-label"),
              svgHidden: document.querySelector(".telemetry-chart-stage svg").getAttribute("aria-hidden"),
              svgLabel: document.querySelector(".telemetry-chart-stage svg").getAttribute("aria-label"),
              liveRole: document.querySelector("[data-chart-live-output]").getAttribute("role"),
              livePoliteness: document.querySelector("[data-chart-live-output]").getAttribute("aria-live"),
              liveAtomic: document.querySelector("[data-chart-live-output]").getAttribute("aria-atomic"),
              liveText: document.querySelector("[data-chart-live-output]").textContent,
              tooltipRole: document.querySelector("[data-chart-tooltip]").getAttribute("role")
            })"""
        )
        self.assertEqual(accessibility["svgHidden"], "true")
        self.assertIsNone(accessibility["svgLabel"])
        self.assertEqual(accessibility["liveRole"], "status")
        self.assertEqual(accessibility["livePoliteness"], "polite")
        self.assertEqual(accessibility["liveAtomic"], "true")
        self.assertEqual(accessibility["liveText"], accessibility["stageLabel"])
        self.assertIsNone(accessibility["tooltipRole"])
        cursor_matches_path = await session.page.evaluate(
            """(() => {
              const cursorX = Number(document.querySelector("[data-chart-cursor]").getAttribute("x1"));
              const pathXs = Array.from(document.querySelectorAll("[data-chart-lines] path"))
                .flatMap(path => Array.from(path.getAttribute("d").matchAll(/[ML]([0-9.]+),/g)))
                .map(match => Number(match[1]));
              return pathXs.some(x => Math.abs(x - cursorX) < 0.11);
            })()"""
        )
        self.assertTrue(cursor_matches_path)

        resize_stable = await session.page.evaluate(
            """(async () => {
              const body = document.querySelector("[data-telemetry-table-body]");
              const first = body.firstElementChild;
              window.dispatchEvent(new Event("resize"));
              await new Promise(resolve => setTimeout(resolve, 25));
              return body.firstElementChild === first;
            })()"""
        )
        self.assertTrue(resize_stable)
