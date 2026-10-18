"""Design-first journey checks against real PA HTML, HTMX, CSS and browser input."""
import asyncio
import json
import os
from pathlib import Path
import time

import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.core.preferences import get_preferences_store
from tests.test_card_status_browser import operator_server
from tests.ux_guidance_fixture import build_ux_app


@pytest.fixture
def operator_app(tmp_path):
    return build_ux_app(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason='managed Chromium is not installed')
@pytest.mark.asyncio
@pytest.mark.parametrize('width', [1440, 390])
async def test_actions_results_evidence_and_permission_labels_without_help(operator_server, tmp_path, width):
    app, base = operator_server
    cards = app.state.ux_cards
    browser = BrowserManager(tmp_path / 'browser')
    manager = BrowserSessionManager(browser, instance_id='ux-test')
    scope = BrowserScope('user:local', 'ux-test', 'ux-test')
    evidence = Path(os.environ.get('BROWSER_TEST_EVIDENCE_DIR', tmp_path / 'evidence'))
    evidence.mkdir(parents=True, exist_ok=True)
    observations = []
    try:
        await manager.attach(scope, url=base + '/work', width=width, height=1000)
        page = manager.resolve(scope).page

        async def wait(expression):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail('Browser condition timed out: ' + expression)

        async def inspect(name, selector):
            result = await page.evaluate(f'''(() => {{
              const root=document.querySelector({json.dumps(selector)});
              return {{text:root.innerText, width:innerWidth,
                visible:!!root.getClientRects().length,
                pageFits:document.documentElement.scrollWidth<=innerWidth+2,
                overflow:root.scrollWidth>root.clientWidth+2,
                clipped:[root,...root.querySelectorAll('dt, dd, a, button, p')]
                  .filter(e=>e.getClientRects().length && !e.classList.contains('sr-only') && getComputedStyle(e).overflowY==='hidden' && e.scrollHeight>e.clientHeight+2)
                  .map(e=>e.tagName),
                helpOpen:[...root.querySelectorAll('details')].some(e=>e.open)}};
            }})()''')
            observations.append({'task': name, **result})
            assert result['visible'] and result['pageFits'] and not result['overflow'] and not result['clipped'], result
            (evidence / f'ux-{name}-{width}.png').write_bytes(await page.screenshot())
            return result

        async def assert_reachable(expression):
            bounds = await page.evaluate(f'''(() => {{
              const target={expression};
              target.scrollIntoView({{block:'center',behavior:'instant'}});
              const r=target.getBoundingClientRect();
              return {{left:r.left,right:r.right,top:r.top,bottom:r.bottom,
                width:innerWidth,height:innerHeight}};
            }})()''')
            assert bounds['left'] >= -1 and bounds['right'] <= bounds['width'] + 1, bounds
            assert bounds['top'] >= -1 and bounds['bottom'] <= bounds['height'] + 1, bounds

        await wait("document.querySelectorAll('.compact-card').length===3")
        for name, lane, state, action in [('Not started', 'inbox', 'Not started', 'Start work'),
                                         ('Turn ended', 'active', 'Work unfinished', 'Open card'),
                                         ('Delivered', 'done', 'Delivered', 'Open card')]:
            if width == 390:
                await manager.click(scope, selector=f'[data-board-lane="{lane}"]')
            selector = f'.compact-card[data-card-id="{cards[name].id}"]'
            result = await inspect(name.lower().replace(' ', '-'), selector)
            assert state.lower() in result['text'].lower(), result
            assert action in result['text'] and not result['helpOpen'], result

        # Delivered evidence must be an actual named link, with its revision.
        await page.navigate_and_wait(f'{base}/work?card={cards["Delivered"].id}')
        await wait("!!document.querySelector('.card-detail-body a')")
        result = await inspect('delivered-evidence', '[data-card-detail]')
        assert 'Subject revision: build-a' in result['text']
        assert await page.evaluate("document.querySelector('.card-detail-body a').getAttribute('href')") == 'https://example.test/report/build-a'
        assert await page.evaluate("document.querySelector('.card-detail-body a').textContent") == 'Regression report'

        await page.navigate_and_wait(f'{base}/work?card={cards["Turn ended"].id}')
        await wait("!!document.querySelector('[data-card-tab=summary]')")
        await manager.click(scope, selector='[data-card-tab=summary]')
        await manager.press(scope, key='ArrowRight')
        await wait("!!document.querySelector('[data-execution-id]')")
        assert await page.evaluate("document.activeElement.getAttribute('data-card-tab')") == 'agent'
        assert await page.evaluate("document.querySelector('[data-card-tab=agent]').getAttribute('aria-selected')") == 'true'
        assert 'agent tab loaded' in await page.evaluate("document.querySelector('[data-card-tab-announcer]').textContent.toLowerCase()")
        assert await page.evaluate("document.querySelector('[data-card-tab-announcer]').getAttribute('aria-live')") == 'polite'
        result = await inspect('worker-settings', '[data-card-detail]')
        assert 'Execution / permission mode' in result['text']
        assert 'codex' in result['text'] and 'model-a' in result['text'] and 'workspace-write' in result['text']
        assert not result['helpOpen']
        # Inspect browser accessibility semantics, without claiming audible SR output.
        tree = await page.command('Accessibility.getFullAXTree')
        names = [node.get('name', {}).get('value') for node in tree['nodes'] if not node.get('ignored')]
        assert 'Execution / permission mode' in names and 'workspace-write' in names

        if width == 1440:
            # 1440 physical pixels / 2 = 720 CSS pixels, as with browser zoom.
            # CSS zoom leaves viewport media queries unchanged and is unsuitable.
            await manager.resize(scope, width=720, height=500, device_scale_factor=2)
            await assert_reachable("[...document.querySelectorAll('[data-execution-id] dt')].find(e=>e.textContent==='Execution / permission mode').parentElement")
            await inspect('worker-settings-zoom200', '[data-card-detail]')
            await manager.resize(scope, width=width, height=1000)
        await manager.press(scope, key='Escape')
        await wait("!document.querySelector('dialog[open]')")

        # The visible personal scope must lead to a real, isolated save on both
        # layouts. Capability responses are fixtures; preference writes are real.
        settings = app.state.ctx.settings
        instance = get_preferences_store(settings.data_dir)
        personal = get_preferences_store(settings.data_dir, user_id='local')
        original_instance, original_personal = instance.load(), personal.load()
        await page.navigate_and_wait(base + '/settings?section=agent')
        await wait("document.querySelector('#pa-user-model-effective')?.textContent.includes('From Instance defaults')")
        assert await page.evaluate("document.querySelector('#pa-defaults-scope').value") == 'user'
        assert 'Affects only my new sessions' in await page.evaluate("document.querySelector('#pa-defaults-impact').textContent")
        await page.evaluate("""const model=document.querySelector('#pa-user-model');
          model.value='model-b';model.dispatchEvent(new Event('change',{bubbles:true}));""")
        await wait("document.querySelector('#pa-user-model-effective').textContent.includes('model-b · From My defaults') && !document.querySelector('#pa-agent-prefs-form [type=submit]').disabled")
        await manager.click(scope, selector='#pa-agent-prefs-form [type=submit]')
        await wait("document.querySelector('#pa-agent-prefs-status').textContent==='My defaults saved.'")
        assert personal.load().agent_surfaces['chat.default'].model_id == 'model-b'
        assert instance.load() == original_instance
        assert personal.load().agent_auto_approve_permissions == original_personal.agent_auto_approve_permissions
        assert app.state.settings_writes[-1]['scope'] == 'user'
        assert 'agent_auto_approve_permissions' not in app.state.settings_writes[-1]
        await inspect('personal-default-saved', '#pa-agent-prefs-form')
        if width == 1440:
            await manager.resize(scope, width=720, height=500, device_scale_factor=2)
            await assert_reachable("document.querySelector('#pa-agent-prefs-form [type=submit]')")
            await inspect('personal-default-zoom200', '#pa-agent-prefs-form')
    finally:
        (evidence / f'ux-observations-{width}.json').write_text(json.dumps(observations, indent=2))
        await manager.close()
        await browser.close()
