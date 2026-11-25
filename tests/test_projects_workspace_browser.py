"""Project count tabs, settings and sticky layout through the real HTMX shell."""
from __future__ import annotations

import asyncio
import time

import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import AgentSession, CardCreate, ProjectCreate, RepositoryCreate
from pa.pr_supervisor.models import PRWatch
from tests.test_card_status_browser import operator_server
from tests.test_local_operator_acceptance import operator_app


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_project_tabs_modal_saves_sticky_and_responsive_layout(operator_server, tmp_path):
    app, base = operator_server
    store = app.state.ctx.store
    project = store.create_project(ProjectCreate(title="PA Core", description="Project context"))
    for index in range(25):
        store.create_card(CardCreate(title=f"Active card {index:02d}", project_id=project.id, lane="active", auto_enrich=False))
    store.create_card(CardCreate(title="Inbox card", project_id=project.id))
    store.save_session(AgentSession(agent_name="codex", project_id=project.id, title="Live execution", status="working"))
    store.save_session(AgentSession(agent_name="codex", project_id=project.id, title="Past execution", status="closed"))
    repository = store.create_repository(RepositoryCreate(url="https://github.com/example/pa", name="PA repository"))
    store.link_project_repository(project.id, repository.id)
    app.state.ctx.require_service("pr_supervisor_store").upsert_watch(PRWatch(
        project_id=project.id, repository="example/pa", pr_number=42,
        pr_url="https://github.com/example/pa/pull/42",
    ))
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="projects-test")
    scope = BrowserScope("user:test", "projects-workspace", "projects-test")
    try:
        await manager.attach(scope, url=f"{base}/projects?project={project.id}", width=1600, height=1000)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    # HTMX initializes incoming controls after its settle delay.
                    await asyncio.sleep(.05)
                    return
                await asyncio.sleep(.05)
            (tmp_path / "project-failure.png").write_bytes(await page.screenshot())
            state = await page.evaluate("({path:location.pathname, tab:new URL(location.href).searchParams.get('tab'), active:document.activeElement.id, heading:document.querySelector('.project-title-row h1')?.textContent, modal:document.querySelector('#project-settings-dialog')?.open})")
            pytest.fail("Browser condition did not become true: " + expression + " " + repr(state))

        await wait_for("document.querySelector('.project-work-tabs')?.dataset.tabsReady === '1'")
        assert await page.evaluate("document.querySelectorAll('[role=tab][data-project-work-tab]').length") == 8
        assert await page.evaluate("document.querySelectorAll('#project-work-panel article[data-card-id]').length") == 20
        assert await page.evaluate("document.querySelector('.project-title-row').textContent.includes('active')")
        assert await page.evaluate("(() => {const b=[...document.querySelectorAll('.project-heading-actions button')];return b[0].offsetHeight===b[1].offsetHeight && getComputedStyle(b[0]).fontSize===getComputedStyle(b[1]).fontSize})()")
        assert await page.evaluate("getComputedStyle(document.querySelector('.projects-tab.is-active')).backgroundColor !== getComputedStyle(document.querySelector('.projects-tab:not(.is-active)')).backgroundColor")
        assert await page.evaluate("document.querySelector('.projects-workspace').getBoundingClientRect().top === document.querySelector('#app-view').getBoundingClientRect().top")

        (tmp_path / "projects-desktop.png").write_bytes(await page.screenshot())

        # Scroll the actual editor; both title and count tabs stay at its top edge.
        before = await page.evaluate("document.querySelector('.project-workspace-header').getBoundingClientRect().top")
        await page.evaluate("document.querySelector('.projects-editor').scrollTop=600")
        after = await page.evaluate("document.querySelector('.project-workspace-header').getBoundingClientRect().top")
        assert abs(before - after) < 1
        assert await page.evaluate("document.querySelector('.projects-editor').scrollTop") > 0

        await manager.click(scope, selector='[data-project-work-tab="active"]')
        await wait_for("document.querySelector('#project-work-panel h2')?.textContent === 'Active cards (25)'")
        assert await page.evaluate("document.activeElement.id") == "project-tab-active"
        assert await page.evaluate("new URL(location.href).searchParams.get('tab')") == "active"
        await manager.click(scope, selector='[aria-label="Project card pages"] a')
        await wait_for("document.querySelectorAll('#project-work-panel article[data-card-id]').length === 5")
        assert await page.evaluate("new URL(location.href).searchParams.get('tab')") == "active"

        await page.evaluate("document.querySelector('[data-project-work-tab=active]').focus()")
        await manager.press(scope, key="End")
        await wait_for("document.querySelector('#project-pr-history')?.textContent.includes('example/pa #42')")
        assert await page.evaluate("document.activeElement.id") == "project-tab-prs"
        assert not await page.evaluate("!!document.querySelector('#project-work-panel article[data-card-id]')")
        await manager.press(scope, key="ArrowLeft")
        await wait_for("document.querySelector('#project-historical-sessions')?.textContent.includes('Past execution')")
        await manager.press(scope, key="ArrowLeft")
        await wait_for("document.querySelector('#project-live-sessions')?.textContent.includes('Live execution')")
        await manager.press(scope, key="Home")
        await wait_for("document.querySelector('#project-work-panel h2')?.textContent === 'All cards (26)'")

        # Opening configuration leaves the layout untouched and makes the rest
        # of the page inert. Escape restores focus and discards unsaved edits.
        panel_top = await page.evaluate("document.querySelector('#project-work-panel').getBoundingClientRect().top")
        await manager.click(scope, selector="[data-project-settings-open]")
        await wait_for("document.querySelector('#project-settings-dialog').open")
        assert await page.evaluate("document.querySelector('#project-settings-dialog').matches(':modal')")
        assert await page.evaluate("document.querySelector('#project-work-panel').getBoundingClientRect().top") == panel_top
        assert await page.evaluate("document.querySelector('#project-settings-dialog').textContent.includes('PA repository')")
        await manager.type_text(scope, selector='#project-settings-dialog input[name=title]', text="Unsaved title")
        await manager.press(scope, key="Escape")
        await wait_for("!document.querySelector('#project-settings-dialog').open")
        assert await page.evaluate("document.activeElement.hasAttribute('data-project-settings-open')")
        await manager.click(scope, selector="[data-project-settings-open]")
        assert await page.evaluate("document.querySelector('#project-settings-dialog input[name=title]').value") == "PA Core"
        assert await page.evaluate("document.querySelector('#project-settings-dialog').offsetWidth") > 700
        (tmp_path / "projects-settings.png").write_bytes(await page.screenshot())

        await page.evaluate("document.querySelector('#project-settings-dialog input[name=title]').select()")
        await page.command("Input.insertText", {"text": "Updated project"})
        await manager.click(scope, selector='#project-settings-dialog form button[type=submit]')
        await wait_for("document.querySelector('.project-title-row h1')?.textContent === 'Updated project' && document.querySelector('#project-settings-dialog').open")
        assert store.get_project(project.id).title == "Updated project"
        # Saving policy from a nested disclosure returns to this project and tab.
        await page.evaluate("[...document.querySelectorAll('#project-settings-dialog details')].find(d=>d.querySelector('summary').textContent==='Pull request policy').open=true")
        await manager.click(scope, selector='#project-settings-dialog form[hx-post*="pr-policy"] button[type=submit]')
        await wait_for("document.querySelector('#project-settings-dialog')?.open && !!document.querySelector('#project-settings-dialog h4')")
        assert await page.evaluate("document.querySelector('#project-work-panel h2').textContent") == "All cards (26)"
        assert await page.evaluate("document.querySelector('form[hx-post*=pr-policy]').closest('details').open")
        assert store.get_project(project.id).tool_config['pr_policy']['agent_merge_on_green']
        await manager.click(scope, selector="[data-project-settings-close]")

        for width in (320, 390, 720, 1024, 1600):
            await manager.resize(scope, width=width, height=900)
            assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth && document.querySelector('.projects-workspace').scrollWidth <= document.querySelector('.projects-workspace').clientWidth"), width
            assert await page.evaluate("document.querySelector('.projects-editor').clientHeight > 250"), width
            if width == 390:
                (tmp_path / "projects-mobile.png").write_bytes(await page.screenshot())
            await manager.click(scope, selector="[data-project-settings-open]")
            assert await page.evaluate("document.querySelector('#project-settings-dialog').scrollWidth <= document.querySelector('#project-settings-dialog').clientWidth"), width
            await manager.press(scope, key="Escape")
        # Existing repository selection still has a clear active state.
        await manager.click(scope, selector='.projects-tabs a[href*="view=repos"]')
        await wait_for("document.querySelector('.projects-tab[aria-current=page]')?.textContent === 'Repos'")
    finally:
        await manager.close()
        await browser.close()
