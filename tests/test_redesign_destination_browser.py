"""All registered destinations remain reachable in the real redesigned shell."""
import asyncio
from pathlib import Path

import pytest
import httpx
from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import CardCreate, ProjectCreate
from tests.browser_conditions import wait_for_read_condition
from tests.test_card_status_browser import operator_server
from tests.visualization_chat_fixture import build_visualization_app


@pytest.fixture
def operator_app(tmp_path):
    return build_visualization_app(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_all_destinations_themes_and_zoom_keep_real_controls(operator_server, tmp_path):
    app, base = operator_server
    store = app.state.ctx.store
    project = store.create_project(ProjectCreate(title='A project with a long readable title ' * 3))
    store.create_card(CardCreate(title='Long title with spaces and Unicode 🧭 ' * 10, project_id=project.id, lane='done'))
    pages = app.state.ctx.require_service('pages').nav_pages()
    paths = list(dict.fromkeys([p.path for p in pages]+['/settings', '/agent?session=viz-session']))
    async with httpx.AsyncClient(base_url=base) as client:
        user = app.state.ctx.require_service('users').get('local')
        response = await client.post('/api/auth/users/local/password', headers={'Authorization':'Bearer '+user.cli_token}, json={'password':'redesign-fixture-password'})
        assert response.status_code == 200
    browser = BrowserManager(tmp_path/'browser')
    manager = BrowserSessionManager(browser, instance_id='redesign-destinations')
    scope = BrowserScope('user:test', 'redesign-destinations', 'redesign-destinations')
    try:
        await manager.attach(scope, url=base+'/login?next=/fixture', width=1440, height=1000)
        page = manager.resolve(scope).page
        async def wait(expression):
            await wait_for_read_condition(page, expression)
            await asyncio.sleep(.1)
        await manager.type_text(scope,selector='input[name="username"]',text='local')
        await manager.type_text(scope,selector='input[name="password"]',text='redesign-fixture-password')
        await manager.click(scope,selector='button[type="submit"]')
        await wait("!!document.querySelector('[data-agent-chat]')?._acw")
        for width in (1440,390):
            await manager.resize(scope, width=width, height=1000)
            for path in paths:
                print("Checking destination", path, width, flush=True)
                async with httpx.AsyncClient(base_url=base,timeout=5,headers={'Authorization':'Bearer '+user.cli_token}) as probe:
                    response = await probe.get(path)
                    print('HTTP destination',path,response.status_code,flush=True)
                    assert response.status_code == 200
                if width == 390:
                    await manager.click(scope,selector='[data-workspace-menu]')
                nav_path = path.split('?')[0]
                selector = '.workspace-rail a[href="'+ ('/work?view=list' if nav_path == '/work' else nav_path) +'"]'
                await manager.click(scope,selector=selector)
                await wait('location.pathname === '+repr(nav_path))
                await wait("!!document.querySelector('#app-view .page-layout') && !!window.htmx")
                assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth"), (path,width)
                fits = await page.evaluate("document.querySelector('.page-main').scrollWidth <= document.querySelector('.page-main').clientWidth")
                if not fits:
                    (tmp_path/'overflow.png').write_bytes(await page.screenshot())
                    print(await page.evaluate("[...document.querySelectorAll('.page-main *')].filter(e=>e.scrollWidth>e.clientWidth+2).slice(0,12).map(e=>({tag:e.tagName,class:e.className,client:e.clientWidth,scroll:e.scrollWidth}))"),flush=True)
                assert fits, (path,width)
                if path == '/':
                    assert await page.evaluate("document.querySelector('.command-center').getBoundingClientRect().left - (innerWidth > 720 ? document.querySelector('.workspace-rail').getBoundingClientRect().right : 0) >= 12")
                assert await page.evaluate("[...document.querySelectorAll('.workspace-rail a')].some(a=>new URL(a.href).pathname === location.pathname)"), path
                if width == 390:
                    await manager.click(scope, selector='[data-workspace-menu]')
                    assert await page.evaluate("document.querySelector('.workspace-rail').contains(document.activeElement)")
                    assert await page.evaluate("[...document.querySelectorAll('.workspace-rail .nav-btn')].every(a=>a.getBoundingClientRect().height >= 44)")
                    await manager.press(scope,key='Escape')
                (tmp_path/f"destination-{path.split('?')[0].strip('/') or 'home'}-{width}.png").write_bytes(await page.screenshot())
        # All existing theme variants, with real page content and user controls.
        await page.evaluate("PANavigation.navigate('/settings?section=appearance')")
        await wait("!!document.querySelector('[data-density-preference]')")
        theme_ids = [p.parent.name for p in Path('src/pa/server/static/themes').glob('*/manifest.json')]
        for theme in theme_ids:
            for appearance in ('light','dark'):
                await page.evaluate("PATheme.setThemeId("+repr(theme)+"); PATheme.setAppearance("+repr(appearance)+")")
                await wait("document.querySelector('#pa-theme-variant').sheet !== null && document.documentElement.dataset.appearance === "+repr(appearance))
                assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth"), (theme,appearance)
        await page.evaluate("PATheme.setThemeId('pa');PATheme.setAppearance('light')")
        # Browser page scaling and the corresponding half-width reflow are distinct.
        await manager.resize(scope,width=1440,height=1000)
        await page.command('Emulation.setPageScaleFactor',{'pageScaleFactor':2})
        assert await page.evaluate('visualViewport.scale') == 2
        (tmp_path/'appearance-page-scale-200.png').write_bytes(await page.screenshot())
        await page.command('Emulation.setPageScaleFactor',{'pageScaleFactor':1})
        await manager.resize(scope,width=720,height=500)
        assert await page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        # Repeated full-document visits must also work, without retaining old streams.
        for path in paths:
            print('Direct destination after lifecycle cleanup',path,flush=True)
            await manager.open(scope,url=base+path)
            await wait("!!document.querySelector('#app-view .page-layout')")
        # Persistent shell theme switching does not remount the current conversation.
        await page.evaluate("PANavigation.navigate('/agent?session=viz-session')")
        await wait("!!document.querySelector('[data-agent-chat]')?._acw")
        await page.evaluate("window.retainedChat=document.querySelector('[data-agent-chat]')._acw;PATheme.setAppearance('dark')")
        assert await page.evaluate("window.retainedChat === document.querySelector('[data-agent-chat]')._acw")
        assert await page.evaluate("['[data-acw-input]','[data-acw-tool-toggle]','[data-acw-terminal-toggle]','[data-acw-settings-form]','[data-thread-navigation]'].every(s=>document.querySelector(s))")
    finally:
        await browser.close()
