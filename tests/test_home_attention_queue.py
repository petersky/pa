"""Bounded Home attention queue route and managed-browser regressions."""

from __future__ import annotations

import asyncio
import re
import shutil
import tempfile
import threading
import unittest
from datetime import UTC, datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from fastapi import Request
from fastapi.testclient import TestClient

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.core.ui.work_presentation import present_work_item
from pa.domain.models import CardCreate, CardLane
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.modules.items import _bounded_attention_cards_context
from pa.pr_supervisor.models import PRWatch

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

    def test_shell_defers_sections_and_partial_is_sorted_bounded_actionable(
        self,
    ) -> None:
        with (
            patch(
                "pa.fleet.workshop.build_workshop_snapshot", return_value=_snapshot()
            ),
            TestClient(self.app) as client,
        ):
            shell = client.get("/?q=no-match&blocked=blocked&kind=concern")
            response = client.get("/partials/home/sections")

        self.assertEqual(shell.status_code, 200)
        self.assertIn("data-home-attention-queue", shell.text)
        self.assertIn('id="home-command-grid"', shell.text)
        self.assertIn('hx-get="/partials/home/sections?realm=', shell.text)
        self.assertIn('hx-trigger="load, homeRefresh from:body"', shell.text)
        self.assertIn("data-home-refresh-status", shell.text)
        self.assertIn("Loading actionable work…", shell.text)
        self.assertIn("Loading work in motion…", shell.text)
        self.assertIn("Loading recent outcomes…", shell.text)
        self.assertIn("Loading fleet health…", shell.text)
        self.assertNotIn("data-attention-card", shell.text)
        self.assertNotIn("data-contextual-work-action", shell.text)

        self.assertEqual(response.status_code, 200)
        self.assertIn('data-contextual-work-action="respond"', response.text)
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
        self.assertIn("Retry for Attention 1", response.text)
        self.assertLess(
            response.text.index("Attention 0"), response.text.index("Attention 5")
        )
        self.assertNotIn("Attention 8", response.text)

    def test_home_totals_use_pre_limit_presentation_projection(self) -> None:
        snapshot = _snapshot()
        snapshot["inventory"].update(total=250, omitted=221)
        snapshot["counts"]["presentations"] = {
            "attention": 109,
            "motion": 121,
            "outcome": 9,
            "quiet": 11,
        }
        with (
            patch(
                "pa.fleet.workshop.build_workshop_snapshot", return_value=snapshot
            ),
            TestClient(self.app) as client,
        ):
            response = client.get("/partials/home/sections")

        self.assertIn("Showing 6 of 109 actionable cards", response.text)
        self.assertIn("Showing 8 of 121 cards in motion", response.text)

    def test_home_totals_count_all_durable_actionable_watch_cards(self) -> None:
        with TestClient(self.app) as client:
            store = self.app.state.ctx.store
            supervisor = self.app.state.ctx.require_service("pr_supervisor_store")
            for index in range(250):
                card = store.create_card(
                    CardCreate(
                        title=f"Canonical attention {index:03d}",
                        lane=CardLane.WAITING,
                    )
                )
                supervisor.upsert_watch(
                    PRWatch(
                        card_id=card.id,
                        repository="petersky/pa",
                        pr_number=index + 1,
                        pr_url=(
                            "https://github.com/petersky/pa/pull/"
                            f"{index + 1}"
                        ),
                        status="blocked",
                        state={
                            "gate": {
                                "actionable": True,
                                "reasons": ["Review required"],
                            }
                        },
                    ),
                    preserve_lease=False,
                )

            response = client.get("/partials/home/sections")

        self.assertEqual(store.count_cards(realm_id="default"), 250)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.text.count('data-attention-group="attention"'), 6
        )
        self.assertIn("Showing 6 of 250 actionable cards", response.text)

    def test_home_promotes_actionable_watch_past_non_actionable_saturation(
        self,
    ) -> None:
        with TestClient(self.app) as client:
            store = self.app.state.ctx.store
            supervisor = self.app.state.ctx.require_service("pr_supervisor_store")
            actionable = store.create_card(
                CardCreate(
                    title="Older blocked review still needs action",
                    lane=CardLane.WAITING,
                )
            )
            supervisor.upsert_watch(
                PRWatch(
                    card_id=actionable.id,
                    repository="petersky/pa",
                    pr_number=1,
                    pr_url="https://github.com/petersky/pa/pull/1",
                    status="blocked",
                    state={
                        "gate": {
                            "actionable": True,
                            "reasons": ["Independent review is required"],
                        }
                    },
                    next_poll_at=datetime(2100, 1, 1, tzinfo=UTC),
                ),
                preserve_lease=False,
            )
            for index in range(120):
                card = store.create_card(
                    CardCreate(title=f"Newer quiet card {index:03d}")
                )
                if index < 81:
                    supervisor.upsert_watch(
                        PRWatch(
                            card_id=card.id,
                            repository="petersky/pa",
                            pr_number=index + 2,
                            pr_url=(
                                "https://github.com/petersky/pa/pull/"
                                f"{index + 2}"
                            ),
                            status="active",
                            state={"gate": {"actionable": False}},
                            next_poll_at=datetime(2100, 1, 1, tzinfo=UTC),
                        ),
                        preserve_lease=False,
                    )

            promoted = supervisor.list_actionable_card_ids(
                realm_id="default", limit=80
            )
            response = client.get("/partials/home/sections")

        self.assertEqual(promoted, [actionable.id])
        self.assertEqual(response.status_code, 200)
        self.assertIn("Older blocked review still needs action", response.text)
        self.assertIn("Independent review is required", response.text)
        self.assertIn('data-contextual-work-action="review"', response.text)
        self.assertIn(
            'aria-label="Showing 1 of 1 actionable cards"', response.text
        )
        self.assertNotIn("No operator action is waiting", response.text)

    def test_actionable_and_motion_filters_page_all_body_free_results(self) -> None:
        with TestClient(self.app) as client:
            store = self.app.state.ctx.store
            for index in range(25):
                store.create_card(
                    CardCreate(
                        title=f"Filtered row {index:02d}",
                        body=f"Full authored body {index:02d} must stay off Work.",
                        lane=CardLane.WAITING,
                    )
                )

            def fake_presentations(request, cards):
                attention = request.query_params.get("attention")
                group = "attention" if attention == "actionable" else "motion"
                presentations = {}
                for candidate in cards:
                    presentation = present_work_item(candidate)
                    presentation.update(
                        {
                            "group": group,
                            "attention": group == "attention",
                            "state": (
                                "review_required"
                                if group == "attention"
                                else "working"
                            ),
                            "state_label": (
                                "Review needed"
                                if group == "attention"
                                else "Working"
                            ),
                            "summary": "Canonical bounded row.",
                            "reason": "Current lifecycle evidence.",
                            "action": {
                                "kind": (
                                    "review"
                                    if group == "attention"
                                    else "open_card"
                                ),
                                "label": (
                                    "Review"
                                    if group == "attention"
                                    else "Open card"
                                ),
                                "href": f"/?card={candidate.id}",
                            },
                        }
                    )
                    presentations[candidate.id] = presentation
                return {}, {}, presentations, {}

            for attention in ("actionable", "motion"):
                pages = []
                with patch(
                    "pa.modules.items._presentation_context_for_cards",
                    side_effect=fake_presentations,
                ):
                    for offset in (0, 10, 20):
                        pages.append(
                            client.get(
                                "/partials/cards?lane=waiting"
                                f"&attention={attention}&offset={offset}"
                            )
                        )

                expected_sizes = (10, 10, 5)
                page_ids = []
                for page, expected_size in zip(pages, expected_sizes, strict=True):
                    self.assertEqual(page.status_code, 200)
                    ids = re.findall(
                        r'<article class="compact-card[^>]+data-card-id="([^"]+)"',
                        page.text,
                    )
                    self.assertEqual(len(ids), expected_size)
                    page_ids.append(set(ids))
                    self.assertNotIn("Full authored body", page.text)

                self.assertEqual(len(set().union(*page_ids)), 25)
                self.assertTrue(page_ids[0].isdisjoint(page_ids[1]))
                self.assertTrue(page_ids[1].isdisjoint(page_ids[2]))
                self.assertIn("Showing 10 of 25", pages[0].text)
                self.assertIn("Showing 20 of 25", pages[1].text)
                self.assertIn("Showing 25 of 25", pages[2].text)
                self.assertIn(f"attention={attention}", pages[0].text)
                self.assertIn("offset=10", pages[0].text)
                self.assertIn("offset=20", pages[1].text)
                self.assertIn("data-filter-show-more", pages[0].text)
                self.assertIn("data-filter-show-more", pages[1].text)
                self.assertNotIn("data-filter-show-more", pages[2].text)

    def test_attention_filter_pages_body_free_rows_before_hydration(self) -> None:
        with TestClient(self.app):
            pass
        store = self.app.state.ctx.store
        for index in range(205):
            store.create_card(
                CardCreate(
                    title=f"Scale card {index:03d}",
                    body=f"Full authored body {index:03d}",
                    lane=CardLane.ACTIVE,
                )
            )

        observed: list[list[str]] = []

        def fake_presentations(request, cards):
            observed.append([card.body for card in cards])
            presentations = {}
            for card in cards:
                presentation = present_work_item(card)
                presentation["group"] = "attention"
                presentation["attention"] = True
                presentations[card.id] = presentation
            return {}, {}, presentations, {}

        request = Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": "/partials/cards",
                "raw_path": b"/partials/cards",
                "query_string": b"attention=actionable",
                "headers": [],
                "client": ("testclient", 50000),
                "server": ("testserver", 80),
                "app": self.app,
            }
        )
        with patch(
            "pa.modules.items._presentation_context_for_cards",
            side_effect=fake_presentations,
        ):
            context = _bounded_attention_cards_context(
                request,
                kind=None,
                lane=CardLane.ACTIVE,
                result_limit=10,
            )

        self.assertEqual(context["total_cards"], 205)
        self.assertEqual(len(context["cards"]), 10)
        self.assertEqual([len(batch) for batch in observed], [100, 100, 5, 10])
        self.assertTrue(all(not body for batch in observed[:3] for body in batch))
        self.assertTrue(all(body for body in observed[-1]))

    def test_work_uses_canonical_action_and_preserves_attention_filter(self) -> None:
        with TestClient(self.app) as client:
            self.app.state.ctx.store.create_card(
                CardCreate(title="Watch-only review gate")
            )

            def fake_presentations(_request, cards):
                presentations = {}
                for candidate in cards:
                    presentation = present_work_item(candidate)
                    presentation.update(
                        {
                            "group": "attention",
                            "attention": True,
                            "state": "review_required",
                            "state_label": "Review needed",
                            "summary": "Review gate is the current operator-owned step.",
                            "reason": "The supervised pull request requires review.",
                            "action": {
                                "kind": "review",
                                "label": "Review",
                                "href": "https://github.com/petersky/pa/pull/257",
                                "external": True,
                            },
                        }
                    )
                    presentations[candidate.id] = presentation
                return {}, {}, presentations, {}

            with patch(
                "pa.modules.items._presentation_context_for_cards",
                side_effect=fake_presentations,
            ):
                cards = client.get(
                    "/partials/cards?lane=inbox&attention=actionable"
                )

            work = client.get("/work?attention=actionable&q=review")

        self.assertEqual(cards.status_code, 200)
        self.assertIn("Review needed", cards.text)
        self.assertIn('data-contextual-work-action="review"', cards.text)
        self.assertIn(">Review</a>", cards.text)
        self.assertNotIn("data-card-dispatch-open", cards.text)
        self.assertNotIn(">Dispatch</button>", cards.text)
        self.assertEqual(work.status_code, 200)
        self.assertRegex(
            work.text,
            r'<input type="hidden" name="attention" value="actionable">',
        )
        self.assertIn("attention=actionable", work.text)


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
            sections = client.get("/partials/home/sections").text
        (root / "index.html").write_text(html)
        sections_path = root / "partials" / "home" / "sections"
        sections_path.parent.mkdir(parents=True, exist_ok=True)
        sections_path.write_text(sections)
        shutil.copytree(ROOT / "src/pa/server/static", root / "static")
        (root / "pagination-next.html").write_text(
            '<div class="compact-card-list">'
            '<article class="compact-card" data-card-id="page-two">'
            '<h3>Second bounded row — 超長い Unicode 🧭</h3></article></div>'
            '<div class="done-list-actions card-page-continuation" '
            'data-filtered-card-continuation role="status" aria-live="polite">'
            '<span class="muted">Showing 20 of 20</span>'
            '<span id="filtered-continuation-waiting" class="sr-only" '
            'tabindex="-1">All filtered cards are shown.</span></div>'
        )
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
        session = self.manager.resolve(self.scope)
        for _ in range(20):
            loaded = await session.page.evaluate(
                "Boolean(document.querySelector('[data-attention-card]'))"
            )
            if loaded:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("Home section partial did not load in the browser")

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
              window.__homeRefreshCalls = 0;
              window.__boardRefreshCalls = 0;
              document.body.addEventListener('homeRefresh', event => {
                window.__homeRefreshCalls += 1;
                event.stopImmediatePropagation();
              }, {capture:true});
              document.body.addEventListener('boardRefresh', () => {
                window.__boardRefreshCalls += 1;
              }, {capture:true});
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
              button.focus();
              button.click();
              return new Promise(resolve => setTimeout(() => {
                const pending = {
                  disabled: button.disabled,
                  busy: button.getAttribute('aria-busy')
                };
                document.body.dispatchEvent(new CustomEvent('htmx:afterSwap', {
                  detail: {target: document.getElementById('app-view')}
                }));
                setTimeout(() => resolve({
                  calls: window.__homeRetryCalls.length,
                  url: window.__homeRetryCalls[0] && window.__homeRetryCalls[0].url,
                  method: window.__homeRetryCalls[0] && window.__homeRetryCalls[0].options.method,
                  homeRefreshCalls: window.__homeRefreshCalls,
                  boardRefreshCalls: window.__boardRefreshCalls,
                  pending,
                  disabled: button.disabled,
                  busy: button.hasAttribute('aria-busy'),
                  focused: document.activeElement === button
                }), 20);
              }, 50));
            })()"""
        )
        self.assertEqual(retry["calls"], 1)
        self.assertTrue(retry["url"].endswith("/dispatch-1/retry"))
        self.assertEqual(retry["method"], "POST")
        self.assertEqual(retry["homeRefreshCalls"], 1)
        self.assertEqual(retry["boardRefreshCalls"], 0)
        self.assertEqual(retry["pending"], {"disabled": True, "busy": "true"})
        self.assertFalse(retry["disabled"])
        self.assertFalse(retry["busy"])
        self.assertTrue(retry["focused"])

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

    async def test_filtered_continuation_appends_and_restores_focus(self) -> None:
        session = self.manager.resolve(self.scope)
        await session.page.evaluate(
            """(() => {
              const view = document.getElementById('app-view');
              view.innerHTML = `
                <section class="board-column-body">
                  <div class="compact-card-list">
                    <article class="compact-card" data-card-id="page-one">
                      <h3>First bounded row</h3>
                    </article>
                  </div>
                  <div class="done-list-actions card-page-continuation"
                       data-filtered-card-continuation role="status"
                       aria-live="polite">
                    <span class="muted">Showing 10 of 20</span>
                    <button type="button" id="filtered-continuation-waiting"
                            data-filter-show-more
                            hx-get="/pagination-next.html"
                            hx-target="closest .card-page-continuation"
                            hx-swap="outerHTML"
                            aria-label="Show 10 more actionable cards in waiting">
                      Show 10 more
                    </button>
                  </div>
                </section>`;
              htmx.process(view);
              document.getElementById('filtered-continuation-waiting').focus();
            })()"""
        )
        await session.page.evaluate(
            "document.getElementById('filtered-continuation-waiting').click()"
        )
        await asyncio.sleep(0.3)
        state = await session.page.evaluate(
            """(() => ({
              ids: Array.from(document.querySelectorAll('[data-card-id]'))
                .map(node => node.dataset.cardId),
              showing: document.querySelector('[data-filtered-card-continuation]')
                .textContent.includes('Showing 20 of 20'),
              focused: document.activeElement.id === 'filtered-continuation-waiting',
              finalStatus: document.activeElement.textContent.trim(),
              noOverflow: document.documentElement.scrollWidth <= innerWidth
            }))()"""
        )
        self.assertEqual(state["ids"], ["page-one", "page-two"])
        self.assertTrue(state["showing"])
        self.assertTrue(state["focused"])
        self.assertEqual(state["finalStatus"], "All filtered cards are shown.")
        self.assertTrue(state["noOverflow"])
