"""Bounded Home attention queue route and managed-browser regressions."""

from __future__ import annotations

import asyncio
import shutil
import tempfile
import threading
import unittest
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent

ROOT = Path(__file__).parents[1]


def _order(index: int, group: str) -> dict:
    states = {
        "attention": ("Input needed", "input_required", "Respond", "respond"),
        "motion": ("Working", "working", "Open agent", "open_agent"),
        "outcome": ("Completed", "completed", "Open card", "open_card"),
    }
    label, state, action_label, action_kind = states[group]
    if group == "attention" and index == 1:
        label, state, action_label, action_kind = (
            "Retry decision needed",
            "retry_required",
            "Retry",
            "retry",
        )
    title = f"{group.title()} {index} — 超長い Unicode 🧭 " + ("word" * 20)
    href = (
        f"/agent?session=session-{index}"
        if action_kind in {"respond", "open_agent"}
        else f"/?card=card-{group}-{index}"
    )
    presentation = {
        "group": group,
        "state": state,
        "state_label": label,
        "tone": "blocked"
        if group == "attention"
        else "success"
        if group == "outcome"
        else "active",
        "summary": "Choose the bounded next action without losing authored text."
        if group == "attention"
        else "Canonical current state from cached evidence.",
        "reason": "This reason explains why the state matters and who owns the next step.",
        "freshness": "fresh",
        "freshness_label": "Current",
        "occurred_at": f"2026-08-06T17:{index:02d}:00+00:00",
        "relative_time": f"{index + 1}m ago",
        "absolute_time": f"2026-08-06 17:{index:02d} UTC",
        "target_instance_name": "Monica — west-coast-worker-with-long-name",
        "priority": 120 - index,
        "accessible_label": f"{title}; {label}; {action_label}",
        "action": {
            "kind": action_kind,
            "label": action_label,
            "href": href,
            **({"dispatch_id": f"dispatch-{index}"} if action_kind == "retry" else {}),
        },
        "action_explanation": None
        if group == "attention"
        else "No operator action needed; autonomous work is progressing.",
    }
    return {
        "id": f"card-{group}-{index}",
        "title": title,
        "updated_at": presentation["occurred_at"],
        "card": {"realm_id": "default"},
        "presentation": presentation,
    }


def _snapshot() -> dict:
    orders = (
        [_order(index, "attention") for index in range(9)]
        + [_order(index, "motion") for index in range(11)]
        + [_order(index, "outcome") for index in range(9)]
    )
    return {
        "work_orders": list(reversed(orders)),
        "counts": {"lanes": {"done": 17}},
        "inventory": {"loaded": 29, "total": 123, "omitted": 94},
    }


class HomeAttentionQueueRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_settings()
        reset_store()
        reset_instance_agent()
        self.tmp = tempfile.TemporaryDirectory()
        settings = Settings(
            data_dir=Path(self.tmp.name),
            instance_id="home-test",
            instance_name="Home test",
            agent_enabled=False,
        )
        self.app = Kernel.boot(settings=settings).build_app()

    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()
        self.tmp.cleanup()

    def test_sections_are_sorted_bounded_actionable_and_link_to_filtered_work(
        self,
    ) -> None:
        with (
            patch(
                "pa.fleet.workshop.build_workshop_snapshot", return_value=_snapshot()
            ),
            TestClient(self.app) as client,
        ):
            response = client.get("/?q=no-match&blocked=blocked&kind=concern")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text.count('data-attention-group="attention"'), 6)
        self.assertEqual(response.text.count('data-attention-group="motion"'), 8)
        self.assertEqual(response.text.count('data-attention-group="outcome"'), 6)
        self.assertIn("Showing 6 of 9 actionable cards", response.text)
        self.assertIn("Showing 8 of 11 cards in motion", response.text)
        self.assertIn("Showing 6 of 17 completed cards", response.text)
        self.assertIn("94 older cards are intentionally omitted", response.text)
        self.assertIn("attention=actionable", response.text)
        self.assertIn("attention=motion", response.text)
        self.assertIn("attention=outcome", response.text)
        self.assertIn("Respond for Attention 0", response.text)
        self.assertIn("Retry Attention 1", response.text)
        self.assertLess(
            response.text.index("Attention 0"), response.text.index("Attention 5")
        )
        self.assertNotIn("Attention 8", response.text)


@unittest.skipUnless(_browser_executable(), "managed Chromium is not installed")
class HomeAttentionQueueManagedBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        reset_settings()
        reset_store()
        reset_instance_agent()
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        settings = Settings(
            data_dir=root / "data",
            instance_id="home-browser",
            instance_name="Home browser",
            agent_enabled=False,
        )
        app = Kernel.boot(settings=settings).build_app()
        with (
            patch(
                "pa.fleet.workshop.build_workshop_snapshot", return_value=_snapshot()
            ),
            TestClient(app) as client,
        ):
            html = client.get("/").text
        (root / "index.html").write_text(html)
        shutil.copytree(ROOT / "src/pa/server/static", root / "static")
        for group in ("attention", "motion", "outcome"):
            for index in range(9 if group != "motion" else 11):
                detail = (
                    root / "partials" / "cards" / f"card-{group}-{index}" / "detail"
                )
                detail.parent.mkdir(parents=True, exist_ok=True)
                detail.write_text(
                    '<article data-card-detail><h2 id="card-detail-title" tabindex="-1">'
                    f'{group.title()} {index}</h2><button type="button" '
                    "data-card-dialog-close>Close details</button></article>"
                )
        handler = partial(SimpleHTTPRequestHandler, directory=self.tmp.name)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.browser = BrowserManager(root / "browser")
        self.manager = BrowserSessionManager(
            self.browser, instance_id="home-browser", idle_ttl_seconds=60
        )
        self.scope = BrowserScope("user:home", "session-home", "home-browser")
        await self.manager.attach(
            self.scope,
            url=f"http://127.0.0.1:{self.httpd.server_port}/index.html",
            width=1440,
            height=1000,
        )
        await asyncio.sleep(0.25)

    async def asyncTearDown(self) -> None:
        await self.manager.close()
        await self.browser.close()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)
        reset_instance_agent()
        reset_store()
        reset_settings()
        self.tmp.cleanup()

    async def test_desktop_and_narrow_zoom_layout_names_order_and_focus_return(
        self,
    ) -> None:
        session = self.manager.resolve(self.scope)
        desktop = await session.page.evaluate(
            """(() => {
              const sections = Array.from(document.querySelectorAll('.command-section h2'))
                .map(node => node.textContent.trim());
              const cards = Array.from(document.querySelectorAll('[data-attention-card]'));
              const controls = Array.from(document.querySelectorAll(
                '[data-attention-card] a[href],[data-attention-card] button'
              ));
              return {
                sections,
                groups: cards.map(card => card.dataset.attentionGroup),
                counts: {
                  attention: document.querySelectorAll('[data-attention-group="attention"]').length,
                  motion: document.querySelectorAll('[data-attention-group="motion"]').length,
                  outcome: document.querySelectorAll('[data-attention-group="outcome"]').length
                },
                unnamedCards: cards.filter(card => !card.getAttribute('aria-label')).length,
                unnamedControls: controls.filter(control => !(control.getAttribute('aria-label') || control.textContent.trim())).length,
                noOverflow: document.documentElement.scrollWidth <= innerWidth
              };
            })()"""
        )
        self.assertEqual(
            desktop["sections"], ["Needs attention", "In motion", "Recent outcomes"]
        )
        self.assertEqual(desktop["counts"], {"attention": 6, "motion": 8, "outcome": 6})
        self.assertEqual(desktop["groups"][:6], ["attention"] * 6)
        self.assertEqual(desktop["unnamedCards"], 0)
        self.assertEqual(desktop["unnamedControls"], 0)
        self.assertTrue(desktop["noOverflow"])

        retry = await session.page.evaluate(
            """(() => {
              const originalFetch = window.fetch.bind(window);
              window.__homeRetryCalls = [];
              window.fetch = (url, options) => {
                if (String(url).includes('/api/fleet/dispatch-jobs/')) {
                  window.__homeRetryCalls.push({url:String(url), options});
                  return Promise.resolve(new Response(
                    JSON.stringify({dispatch_id:'dispatch-1', state:'queued'}),
                    {status:200, headers:{'Content-Type':'application/json'}}
                  ));
                }
                return originalFetch(url, options);
              };
              const button = document.querySelector(
                '[data-card-dispatch-retry="dispatch-1"]'
              );
              button.click();
              return new Promise(resolve => setTimeout(() => resolve({
                calls: window.__homeRetryCalls.length,
                url: window.__homeRetryCalls[0] && window.__homeRetryCalls[0].url,
                method: window.__homeRetryCalls[0] && window.__homeRetryCalls[0].options.method,
                disabled: button.disabled
              }), 50));
            })()"""
        )
        self.assertEqual(retry["calls"], 1)
        self.assertTrue(retry["url"].endswith("/dispatch-1/retry"))
        self.assertEqual(retry["method"], "POST")
        self.assertTrue(retry["disabled"])

        opener = '[data-attention-group="attention"] [data-card-detail-link]'
        await session.page.evaluate(f"document.querySelector('{opener}').focus()")
        await self.manager.press(self.scope, key="Enter")
        await asyncio.sleep(0.2)
        dialog = await session.page.evaluate(
            """({
              open: document.querySelector('#card-detail-dialog').open,
              headingFocused: document.activeElement.id === 'card-detail-title'
            })"""
        )
        self.assertEqual(dialog, {"open": True, "headingFocused": True})
        await session.page.evaluate(
            "document.querySelector('[data-card-dialog-close]').click()"
        )
        await asyncio.sleep(0.1)
        returned = await session.page.evaluate(
            f"document.activeElement === document.querySelector('{opener}')"
        )
        self.assertTrue(returned)

        await self.manager.resize(self.scope, width=390, height=844)
        narrow = await session.page.evaluate(
            """(() => {
              const cards = Array.from(document.querySelectorAll('[data-attention-card]'));
              return {
                width: innerWidth,
                page: document.documentElement.scrollWidth <= innerWidth,
                cards: cards.every(card => card.scrollWidth <= card.clientWidth)
              };
            })()"""
        )
        self.assertEqual(narrow["width"], 390)
        self.assertTrue(narrow["page"])
        self.assertTrue(narrow["cards"])

        # A 195 CSS-pixel content box is the reflow-equivalent width of the
        # 390px card surface at 200% browser zoom. Global chrome is covered by
        # its separate responsive contract.
        zoomed = await session.page.evaluate(
            """(() => {
              const center = document.querySelector('.command-center');
              center.style.width = '195px';
              center.style.maxWidth = '195px';
              const cards = Array.from(document.querySelectorAll('[data-attention-card]'));
              return {
                cards: cards.every(card => card.scrollWidth <= card.clientWidth),
                preserved: document.body.textContent.includes('超長い Unicode 🧭')
              };
            })()"""
        )
        self.assertTrue(zoomed["cards"], zoomed)
        self.assertTrue(zoomed["preserved"])
