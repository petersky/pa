"""Projects disclosure, health, and managed-browser regressions."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession, CardCreate, CardLane, ProjectCreate
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.pr_supervisor.models import PRWatch


ROOT = Path(__file__).parents[1]


class ProjectsHealthRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_settings()
        reset_store()
        reset_instance_agent()
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(
            data_dir=Path(self.tmp.name),
            instance_id="projects-test",
            instance_name="Projects test",
            agent_enabled=False,
        )
        self.app = Kernel.boot(settings=self.settings).build_app()

    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()
        self.tmp.cleanup()

    def test_metrics_reconcile_to_canonical_lanes_and_session_outcomes(self) -> None:
        with TestClient(self.app) as client:
            store = self.app.state.ctx.store
            project = store.create_project(
                ProjectCreate(title="Canonical project", description="Authored context.")
            )
            cards = {}
            for lane in CardLane:
                cards[lane] = store.create_card(
                    CardCreate(
                        title=f"{lane.value} card",
                        project_id=project.id,
                        lane=lane,
                    )
                )
            store.save_session(
                AgentSession(
                    agent_name="codex",
                    project_id=project.id,
                    card_id=cards[CardLane.ACTIVE].id,
                    title="Live execution",
                    status="working",
                )
            )
            store.save_session(
                AgentSession(
                    agent_name="codex",
                    project_id=project.id,
                    title="Historical execution",
                    status="closed",
                )
            )

            response = client.get(f"/projects?realm=default&project={project.id}")

        self.assertEqual(response.status_code, 200)
        self.assertIn("25%", response.text)
        self.assertIn("1 of 4 complete", response.text)
        self.assertIn("Blocked (Waiting)", response.text)
        self.assertIn("Completion means cards in Done", response.text)
        self.assertIn("Live agent sessions (1)", response.text)
        self.assertIn("Historical agent sessions (1)", response.text)
        self.assertIn("Live execution", response.text)
        self.assertIn("Historical execution", response.text)
        self.assertIn("Open agent", response.text)
        self.assertNotIn('aria-label="Dispatch active card"', response.text)
        self.assertIn("Refreshed", response.text)
        for lane in CardLane:
            self.assertIn(
                f"project={project.id}&amp;lane={lane.value}#lane-{lane.value}",
                response.text,
            )

    def test_working_session_wins_over_newer_quiesced_history(self) -> None:
        with TestClient(self.app) as client:
            store = self.app.state.ctx.store
            project = store.create_project(ProjectCreate(title="Session precedence"))
            card = store.create_card(
                CardCreate(
                    title="Still working",
                    project_id=project.id,
                    lane=CardLane.ACTIVE,
                )
            )
            working = AgentSession(
                id="working-session",
                agent_name="codex",
                project_id=project.id,
                card_id=card.id,
                title="Live work",
                status="working",
            )
            quiesced = AgentSession(
                id="quiesced-session",
                agent_name="codex",
                project_id=project.id,
                card_id=card.id,
                title="Newer historical session",
                status="quiesced",
                updated_at=working.updated_at + timedelta(seconds=1),
            )
            store.save_session(working)
            store.save_session(quiesced)

            preferred = store.list_preferred_sessions_for_project_cards(
                project.id, realm_id="default"
            )
            response = client.get(f"/projects?project={project.id}")

        self.assertEqual([session.id for session in preferred], [working.id])
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            'aria-label="Open live agent session for Still working"', response.text
        )
        self.assertNotIn('aria-label="Dispatch Still working"', response.text)
        self.assertIn("Live agent sessions (1)", response.text)
        self.assertIn("Historical agent sessions (1)", response.text)

    def test_create_partial_selects_the_project_and_pushes_the_same_url(self) -> None:
        with TestClient(self.app) as client:
            client.get("/projects")
            csrf = client.cookies.get("pa_csrf")
            assert csrf is not None
            response = client.post(
                "/projects",
                data={
                    "realm": "default",
                    "title": "Created with keyboard",
                    "description": "Keep this authored description.",
                },
                headers={"HX-Request": "true", "X-CSRF-Token": csrf},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("Created with keyboard", response.text)
        self.assertIn("Keep this authored description.", response.text)
        pushed = response.headers.get("HX-Push-Url", "")
        self.assertIn("view=projects", pushed)
        self.assertIn("project=", pushed)

    def test_session_projection_failure_keeps_card_health_understandable(self) -> None:
        with TestClient(self.app) as client:
            store = self.app.state.ctx.store
            project = store.create_project(ProjectCreate(title="Partial project"))
            store.create_card(
                CardCreate(
                    title="Still canonical",
                    project_id=project.id,
                    lane=CardLane.ACTIVE,
                )
            )
            with patch.object(
                store,
                "count_project_sessions",
                side_effect=RuntimeError("projection unavailable"),
            ):
                response = client.get(f"/projects?project={project.id}")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Still canonical", response.text)
        self.assertIn("session counts", response.text)
        self.assertIn("Card counts are current", response.text)
        self.assertNotIn("projection unavailable", response.text)

    def test_history_queries_and_rendering_are_bounded_and_paginated(self) -> None:
        with TestClient(self.app) as client:
            store = self.app.state.ctx.store
            project = store.create_project(ProjectCreate(title="Bounded history"))
            for index in range(25):
                store.save_session(
                    AgentSession(
                        agent_name="codex",
                        project_id=project.id,
                        title=f"Historical session {index:02d}",
                        status="closed",
                    )
                )
            supervisor_store = self.app.state.ctx.require_service(
                "pr_supervisor_store"
            )
            for index in range(23):
                supervisor_store.upsert_watch(
                    PRWatch(
                        project_id=project.id,
                        repository="petersky/pa",
                        pr_number=1000 + index,
                        pr_url=f"https://github.com/petersky/pa/pull/{1000 + index}",
                    )
                )

            self.assertEqual(
                store.count_project_sessions(
                    project.id, realm_id="default", historical=True
                ),
                25,
            )
            self.assertEqual(
                len(
                    store.list_project_sessions(
                        project.id,
                        realm_id="default",
                        historical=True,
                        limit=7,
                    )
                ),
                7,
            )
            self.assertEqual(
                supervisor_store.count_project_watches(
                    project.id, realm_id="default"
                ),
                23,
            )
            self.assertEqual(
                len(
                    supervisor_store.list_project_watches(
                        project.id, realm_id="default", limit=6
                    )
                ),
                6,
            )

            with (
                patch.object(
                    store,
                    "list_sessions",
                    side_effect=AssertionError("unbounded session projection"),
                ),
                patch.object(
                    supervisor_store,
                    "list_watches",
                    side_effect=AssertionError("unbounded watch projection"),
                ),
            ):
                first = client.get(f"/projects?project={project.id}")
                second = client.get(
                    f"/projects?project={project.id}"
                    "&session_history_page=2&pr_history_page=2"
                )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertIn("Historical agent sessions (25)", first.text)
        self.assertIn("Showing 1–10 of 25", first.text)
        self.assertEqual(first.text.count("/agent?session="), 10)
        self.assertIn("Historical session 24", first.text)
        self.assertNotIn("Historical session 14", first.text)
        self.assertIn("session_history_page=2", first.text)
        self.assertIn("Supervised pull request history (23)", first.text)
        self.assertIn("petersky/pa #1022", first.text)
        self.assertNotIn("petersky/pa #1012", first.text)
        self.assertEqual(first.text.count("/pull-requests?watch="), 10)

        self.assertIn("Showing 11–20 of 25", second.text)
        self.assertIn("Historical session 14", second.text)
        self.assertNotIn("Historical session 24", second.text)
        self.assertIn("petersky/pa #1012", second.text)
        self.assertNotIn("petersky/pa #1022", second.text)
        self.assertIn("Page 2 of 3", second.text)
        self.assertIn(
            f"project={project.id}&amp;pr_history_page=2&amp;session_history_page=1",
            second.text,
        )
        self.assertIn(
            f"project={project.id}&amp;session_history_page=2&amp;pr_history_page=1",
            second.text,
        )

    def test_work_lane_query_selects_the_exact_metric_destination(self) -> None:
        with TestClient(self.app) as client:
            response = client.get("/work?realm=default&lane=waiting")

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            'data-board-lane="waiting" aria-pressed="true"', response.text
        )
        self.assertIn(
            'id="lane-waiting" data-lane="waiting" data-mobile-active="true"',
            response.text,
        )

    def test_new_project_uses_an_accessible_modal_instead_of_a_sidebar_disclosure(
        self,
    ) -> None:
        with TestClient(self.app) as client:
            response = client.get("/projects?realm=default&view=projects")

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            'data-project-create-open="new-project-dialog" aria-haspopup="dialog"',
            response.text,
        )
        self.assertIn(
            '<dialog id="new-project-dialog" class="project-create-dialog" '
            'aria-labelledby="new-project-title">',
            response.text,
        )
        self.assertIn('data-project-create-close', response.text)
        self.assertNotIn('<details class="projects-create">', response.text)

        layout = (ROOT / "src/pa/server/static/js/layout.js").read_text()
        self.assertIn('dialog.showModal()', layout)
        self.assertIn('dialog.addEventListener("cancel"', layout)
        self.assertIn('button.focus({ preventScroll: true })', layout)


@unittest.skipUnless(_browser_executable(), "managed Chromium is not installed")
class ProjectsManagedChromiumTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.browser = BrowserManager(Path(self.tmp.name) / "browser")
        self.manager = BrowserSessionManager(
            self.browser,
            instance_id="projects-browser",
            idle_ttl_seconds=60,
        )
        self.scope = BrowserScope(
            "user:projects",
            "session-projects",
            "projects-browser",
        )
        layout = (
            ROOT / "src/pa/server/static/js/layout.js"
        ).read_text().replace("</script>", "<\\/script>")
        stylesheet = (ROOT / "src/pa/server/static/style.css").read_text()
        fixture = f"""<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{stylesheet}
:root {{--pa-bg:#fff;--pa-surface:#f7f8fa;--pa-text:#172033;--pa-text-muted:#536079;--pa-border:#73809a;--pa-accent:#315fc7;--pa-focus:#7047eb}}</style>
</head><body><main id="app-view">
<div class="projects-workspace" data-projects-disclosure-scope="default:projects:project-1">
<aside class="projects-sidebar">
  <details class="projects-create"><summary class="button">+ New Project</summary>
    <form><label>Title <input name="title" required></label>
      <label>Description <textarea name="description"></textarea></label>
      <button type="submit">Create project</button></form>
  </details>
  <label class="projects-search"><span>Search projects</span><input type="search" name="search"></label>
</aside>
<main class="projects-editor"><div class="panel">
  <details class="panel-inset"><summary>Edit project settings</summary>
    <form><label>Project title <input name="project_title" value="Authored"></label>
      <label>Goal and description <textarea name="goal">Keep me</textarea></label>
      <button type="submit">Save project</button></form>
  </details>
  <details><summary>Agent prompt</summary><pre>Preserve authored prompt.</pre></details>
  <details class="panel-inset"><summary>Fleet checkouts (3)</summary>
    <a href="#checkout">Inspect checkout</a></details>
  <details class="panel-inset"><summary>Link repository</summary>
    <form><label>Repository <select name="repository"><option>PA</option></select></label>
      <label>Branch <input name="branch"></label><button type="submit">Link repository</button></form>
  </details>
  <details class="panel-inset"><summary>Worker group defaults</summary>
    <form><label>Repository work <select name="group"><option>Automatic</option></select></label>
      <button type="submit">Set repository default</button></form>
  </details>
  <details class="panel-inset"><summary>Pull request policy</summary>
    <form><label><input type="checkbox" name="ready"> Open ready for review</label>
      <button type="submit">Save PR policy</button></form>
  </details>
  <a href="#project-historical-sessions" data-project-disclosure-jump="Historical agent sessions">2 historical sessions</a>
  <details class="panel-inset" id="project-historical-sessions">
    <summary>Historical agent sessions (25)</summary><a href="#one">Session one</a>
    <nav class="project-history-pagination" aria-label="Historical agent session pages">
      <a class="ghost small" href="?session_history_page=1">Previous</a>
      <span>Page 2 of 3</span>
      <a class="ghost small" href="?session_history_page=3">Next</a>
    </nav>
  </details>
  <details class="panel-inset"><summary>Supervised pull request history (23)</summary>
    <nav class="project-history-pagination" aria-label="Supervised pull request history pages">
      <a class="ghost small" href="?pr_history_page=2">Next</a>
    </nav>
  </details>
</div></main></div>
</main>
<script>window.fixtureWorkspace = document.querySelector(".projects-workspace").outerHTML;</script>
<script>{layout}</script>
</body></html>"""
        self.page_path = Path(self.tmp.name) / "projects.html"
        self.page_path.write_text(fixture)
        handler = partial(SimpleHTTPRequestHandler, directory=self.tmp.name)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.http_thread = threading.Thread(
            target=self.httpd.serve_forever, daemon=True
        )
        self.http_thread.start()
        await self.manager.attach(
            self.scope,
            url=f"http://127.0.0.1:{self.httpd.server_port}/projects.html",
            width=1440,
            height=1000,
        )
        await asyncio.sleep(0.1)

    async def asyncTearDown(self) -> None:
        await self.manager.close()
        await self.browser.close()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.http_thread.join(timeout=2)
        self.tmp.cleanup()

    async def test_keyboard_names_hidden_focus_and_swap_restoration(self) -> None:
        session = self.manager.resolve(self.scope)
        initial = await session.page.evaluate(
            """(() => {
              const controls = Array.from(document.querySelectorAll(
                'button:not([disabled]),a[href],input:not([disabled]),textarea:not([disabled]),select:not([disabled])'
              )).filter(el => el.offsetParent !== null);
              function name(el) {
                if (el.getAttribute('aria-label')) return el.getAttribute('aria-label').trim();
                if (el.labels && el.labels.length) return Array.from(el.labels)
                  .map(label => label.textContent.trim()).join(' ');
                return el.textContent.trim() || el.title || '';
              }
              const hiddenPanels = Array.from(
                document.querySelectorAll('.project-disclosure-panel[hidden]')
              );
              return {
                details: document.querySelectorAll('.projects-workspace details').length,
                toggleNames: Array.from(document.querySelectorAll(
                  '.project-disclosure-toggle'
                )).map(el => el.textContent.trim()),
                unnamed: controls.filter(el => !name(el)).length,
                collapsed: Array.from(document.querySelectorAll(
                  '.project-disclosure-toggle'
                )).every(el => el.getAttribute('aria-expanded') === 'false'),
                hiddenFocusable: hiddenPanels.some(panel => {
                  const target = panel.querySelector('input,textarea,select,button,a[href]');
                  if (!target) return false;
                  target.focus();
                  return document.activeElement === target;
                }),
                noOverflow: document.documentElement.scrollWidth <= innerWidth
              };
            })()"""
        )
        self.assertEqual(initial["details"], 0)
        self.assertIn("+ New Project", initial["toggleNames"])
        self.assertIn("Edit project settings", initial["toggleNames"])
        self.assertIn("Pull request policy", initial["toggleNames"])
        self.assertEqual(initial["unnamed"], 0)
        self.assertTrue(initial["collapsed"])
        self.assertFalse(initial["hiddenFocusable"])
        self.assertTrue(initial["noOverflow"])

        await session.page.evaluate(
            """document.querySelector(
              '[data-project-disclosure-key="new-project"]'
            ).focus()"""
        )
        await self.manager.press(self.scope, key="Space")
        await self.manager.press(self.scope, key="Tab")
        opened = await session.page.evaluate(
            """({
              expanded: document.querySelector(
                '[data-project-disclosure-key="new-project"]'
              ).getAttribute('aria-expanded'),
              active: document.activeElement.name
            })"""
        )
        self.assertEqual(opened, {"expanded": "true", "active": "title"})

        await self.manager.type_text(
            self.scope,
            selector='input[name="title"]',
            text="Unsaved title",
        )
        await session.page.evaluate(
            """document.querySelector(
              '[data-project-disclosure-panel="new-project"] [data-project-disclosure-close]'
            ).focus()"""
        )
        await self.manager.press(self.scope, key="Space")
        cancelled = await session.page.evaluate(
            """({
              expanded: document.querySelector(
                '[data-project-disclosure-key="new-project"]'
              ).getAttribute('aria-expanded'),
              active: document.activeElement.textContent.trim(),
              value: document.querySelector('input[name="title"]').value
            })"""
        )
        self.assertEqual(cancelled["expanded"], "false")
        self.assertEqual(cancelled["active"], "+ New Project")
        self.assertEqual(cancelled["value"], "")

        await session.page.evaluate(
            """document.querySelector(
              '[data-project-disclosure-key="agent-prompt"]'
            ).click()"""
        )
        await session.page.evaluate(
            """document.querySelector(
              '[data-project-disclosure-key="edit-project-settings"]'
            ).click()"""
        )
        await session.page.evaluate(
            """document.querySelector('textarea[name="goal"]').focus()"""
        )
        await self.manager.press(self.scope, key="Escape")
        escaped = await session.page.evaluate(
            """({
              expanded: document.querySelector(
                '[data-project-disclosure-key="edit-project-settings"]'
              ).getAttribute('aria-expanded'),
              active: document.activeElement.textContent.trim()
            })"""
        )
        self.assertEqual(escaped["expanded"], "false")
        self.assertEqual(escaped["active"], "Edit project settings")

        await session.page.evaluate(
            """(() => {
              const toggle = document.querySelector(
                '[data-project-disclosure-key="edit-project-settings"]'
              );
              toggle.click();
              const form = document.querySelector(
                '[data-project-disclosure-panel="edit-project-settings"] form'
              );
              form.addEventListener('submit', event => event.preventDefault(), {once:true});
              form.dispatchEvent(new Event('submit', {bubbles:true,cancelable:true}));
              const app = document.querySelector('#app-view');
              app.innerHTML = window.fixtureWorkspace;
              document.body.dispatchEvent(new CustomEvent('htmx:afterSwap', {
                bubbles:true, detail:{target:app}
              }));
            })()"""
        )
        restored = await session.page.evaluate(
            """({
              active: document.activeElement.textContent.trim(),
              expanded: document.activeElement.getAttribute('aria-expanded'),
              pending: sessionStorage.getItem('pa:projects:return-focus'),
              focusTag: document.activeElement.tagName,
              toggleCount: document.querySelectorAll('[data-project-disclosure-key]').length,
              promptExpanded: document.querySelector(
                '[data-project-disclosure-key="agent-prompt"]'
              ).getAttribute('aria-expanded')
            })"""
        )
        self.assertEqual(restored["active"], "Edit project settings")
        self.assertEqual(restored["focusTag"], "BUTTON")
        self.assertIsNone(restored["pending"])
        self.assertGreaterEqual(restored["toggleCount"], 7)
        self.assertEqual(restored["expanded"], "false")
        self.assertEqual(restored["promptExpanded"], "true")

        await session.page.evaluate(
            """document.querySelector(
              '[data-project-disclosure-jump="Historical agent sessions"]'
            ).click()"""
        )
        history_state = await session.page.evaluate(
            """({
              expanded: document.querySelector(
                '[data-project-disclosure-key="historical-agent-sessions"]'
              ).getAttribute('aria-expanded'),
              paginationLabel: document.querySelector(
                '[aria-label="Historical agent session pages"]'
              ).getAttribute('aria-label'),
              paginationLinks: Array.from(document.querySelectorAll(
                '[aria-label="Historical agent session pages"] a'
              )).map(link => link.textContent.trim())
            })"""
        )
        self.assertEqual(history_state["expanded"], "true")
        self.assertEqual(
            history_state["paginationLabel"], "Historical agent session pages"
        )
        self.assertEqual(history_state["paginationLinks"], ["Previous", "Next"])

        await self.manager.resize(self.scope, width=390, height=844)
        mobile = await session.page.evaluate(
            """({
              width: innerWidth,
              noPageOverflow: document.documentElement.scrollWidth <= innerWidth,
              noWorkspaceOverflow: document.querySelector(
                '.projects-workspace'
              ).scrollWidth <= document.querySelector('.projects-workspace').clientWidth
            })"""
        )
        self.assertEqual(mobile["width"], 390)
        self.assertTrue(mobile["noPageOverflow"])
        self.assertTrue(mobile["noWorkspaceOverflow"])
