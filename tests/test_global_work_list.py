"""Global list uses the same bounded, realm-scoped card projection as boards."""
from html import unescape
from urllib.parse import parse_qs, urlsplit
import re

from fastapi.testclient import TestClient
from pa.domain.models import CardCreate, ProjectCreate
from tests.test_local_operator_acceptance import operator_app


def test_list_pagination_no_project_scope_and_saved_view(operator_app):
    app = operator_app
    with TestClient(app) as client:
        store = app.state.ctx.store
        project = store.create_project(ProjectCreate(title="List project"))
        for i in range(13):
            store.create_card(CardCreate(title=f"No project fixture {i}", tags=["list"], lane="inbox"))
        store.create_card(CardCreate(title="Assigned fixture", project_id=project.id, tags=["list"], lane="inbox"))
        store.create_card(CardCreate(title="Other realm fixture", realm_id="private", tags=["list"], lane="inbox"))
        shell = client.get('/work?view=list&project=none&tag=list')
        assert shell.status_code == 200
        assert 'aria-label="Global work list"' in shell.text
        assert 'value="none" selected>No project' in shell.text
        assert 'name="view" value="list"' in shell.text
        first = client.get('/partials/cards?view=list&project=none&tag=list')
        assert first.status_code == 200
        assert 'data-total="13"' in first.text
        assert first.text.count('class="work-list-entry"') == 10
        assert 'Assigned fixture' not in first.text and 'Other realm fixture' not in first.text
        more = unescape(re.search(r'hx-get="([^"]+)"[^>]*\s+hx-target="closest .board-column-body"', first.text).group(1))
        assert parse_qs(urlsplit(more).query)['project'] == ['none']
        expanded = client.get(more)
        assert expanded.text.count('class="work-list-entry"') == 13
        all_cards = client.get('/partials/cards?view=list&tag=list&limit=20')
        assert 'data-total="14"' in all_cards.text and 'Assigned fixture' in all_cards.text
        assert 'Other realm fixture' not in all_cards.text
        scoped = client.get(f'/partials/cards?view=list&project={project.id}&tag=list')
        assert 'data-total="1"' in scoped.text and 'Assigned fixture' in scoped.text
        assert 'No project fixture' not in scoped.text
        no_project_board = client.get('/partials/cards?lane=inbox&project=none&tag=list&limit=20')
        assert 'data-total="13"' in no_project_board.text and 'Assigned fixture' not in no_project_board.text
        client.get('/work')
        headers = {'X-CSRF-Token':client.cookies['pa_csrf'], 'Idempotency-Key':'global-list-saved-view'}
        saved = client.post('/api/cards/saved-views', headers=headers, json={'name':'Unassigned list', 'query':'view=list&project=none&lane=inbox&tag=list'})
        assert saved.status_code == 200
        query = parse_qs(saved.json()['query'])
        assert query['view'] == ['list'] and query['project'] == ['none'] and query['lane'] == ['inbox']


import asyncio
import pytest
from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.test_card_status_browser import operator_server
from tests.browser_conditions import wait_for_read_condition


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_global_list_filters_pagination_refresh_and_history(operator_server, tmp_path):
    app, base = operator_server
    store = app.state.ctx.store
    project = store.create_project(ProjectCreate(title="Global list project"))
    for i in range(12):
        store.create_card(CardCreate(title=f"Unassigned list item {i}", lane="inbox"))
    store.create_card(CardCreate(title="Assigned list item", project_id=project.id, lane="inbox"))
    browser = BrowserManager(tmp_path / 'browser')
    manager = BrowserSessionManager(browser, instance_id='global-list-test')
    scope = BrowserScope('user:test', 'global-list', 'global-list-test')
    try:
        await manager.attach(scope, url=base+'/work?view=list', width=1440, height=1000)
        page = manager.resolve(scope).page
        async def wait(expression):
            await wait_for_read_condition(page, expression)
            await asyncio.sleep(.1)
        await wait("document.querySelectorAll('.work-list .compact-card').length === 10")
        assert await page.evaluate("document.querySelector('[data-work-results]').textContent") == '13 cards across all lanes · 10 loaded'
        await manager.click(scope, selector='[data-filter-show-more]')
        await wait("document.querySelectorAll('.work-list .compact-card').length === 13")
        await page.evaluate("document.body.dispatchEvent(new CustomEvent('boardRefresh'))")
        await asyncio.sleep(.5)
        assert await page.evaluate("document.querySelectorAll('.work-list .compact-card').length") == 13
        await page.evaluate("var s=document.querySelector('select[name=project]');s.value='none'")
        await manager.click(scope, selector='.work-filter-primary button[type=submit]')
        await wait("!!document.querySelector('[data-applied-filter=project]') && document.querySelector('[data-work-results]').textContent.includes('12 cards')")
        assert await page.evaluate("new URL(location.href).searchParams.get('view')") == 'list'
        assert await page.evaluate("document.querySelector('.work-list').textContent.includes('Assigned list item')") is False
        # Card dialog and browser history preserve the global list context.
        await manager.click(scope, selector='.work-list [data-card-detail-link]')
        await wait("document.querySelector('#card-detail-dialog').open")
        await manager.press(scope, key='Escape')
        await wait("document.querySelector('#card-detail-dialog')?.open === false")
        for width in (1440,720,390,320):
            await manager.resize(scope, width=width, height=1000)
            assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth && document.querySelector('.page-main').scrollWidth <= document.querySelector('.page-main').clientWidth"), width
            (tmp_path / f'global-list-{width}.png').write_bytes(await page.screenshot())
        await manager.resize(scope, width=1440, height=1000)
        await manager.click(scope, selector='[data-applied-filter=project]')
        await wait("!document.querySelector('[data-applied-filter=project]')")
        await page.evaluate('history.back()')
        await wait("!!document.querySelector('[data-applied-filter=project]')")
        assert await page.evaluate("!!document.querySelector('.work-list')")
        # Existing board display remains reachable from the filtered list.
        await manager.click(scope, selector='.work-group-toggle a:last-child')
        await wait("document.querySelector('[data-board-group=lane]') !== null")
        assert await page.evaluate("new URL(location.href).searchParams.get('project')") == 'none'
    finally:
        await browser.close()
