"""Scope, inheritance and save isolation against real PA routes in Chromium."""
import asyncio
import json
import os
from pathlib import Path
import time

import pytest
from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.core.preferences import get_preferences_store
from tests.settings_scope_fixture import build_settings_app
from tests.test_card_status_browser import operator_server


@pytest.fixture
def operator_app(tmp_path, request):
    return build_settings_app(tmp_path, personal=getattr(request, "param", True))


@pytest.mark.skipif(not _browser_executable(), reason='managed Chromium is not installed')
@pytest.mark.asyncio
async def test_settings_scope_sources_resets_isolated_saves_and_errors(operator_server, tmp_path):
    app, base = operator_server
    browser = BrowserManager(tmp_path / 'browser')
    manager = BrowserSessionManager(browser, instance_id='settings-test')
    scope = BrowserScope('user:local', 'settings-test', 'settings-test')
    settings = app.state.ctx.settings
    personal = get_preferences_store(settings.data_dir, user_id='local')
    instance = get_preferences_store(settings.data_dir)
    original_user, original_instance = personal.load(), instance.load()
    try:
        await manager.attach(scope, url=base+'/settings?section=agent', width=1280, height=1100)
        page = manager.resolve(scope).page
        async def wait(expr):
            deadline = time.monotonic()+15
            while time.monotonic()<deadline:
                if await page.evaluate(expr): return
                await asyncio.sleep(.05)
            pytest.fail(expr)
        async def choose(selector, value):
            await page.evaluate(f'''(()=>{{const e=document.querySelector({json.dumps(selector)});e.value={json.dumps(value)};e.dispatchEvent(new Event('change',{{bubbles:true}}));}})()''')
            await wait("![...document.querySelectorAll('[id$=\"-effective\"]')].some(e=>e.textContent.includes('Checking effective value')) && [...document.querySelectorAll('[data-settings-defaults-scope]')].every(e=>e.dataset.optionsLoading==='false')")
        async def text(selector):
            await wait("![...document.querySelectorAll('[id$=\"-effective\"]')].some(e=>e.textContent.includes('Checking effective value'))")
            return await page.evaluate(f'document.querySelector({json.dumps(selector)}).textContent')
        async def capture(name):
            directory=os.environ.get('BROWSER_TEST_EVIDENCE_DIR')
            if directory:
                target=Path(directory); target.mkdir(parents=True, exist_ok=True)
                (target/(name+'.png')).write_bytes(await manager.screenshot(scope))
                (target/(name+'.json')).write_text(json.dumps(await page.evaluate("({scope:document.querySelector('#pa-defaults-scope').value, impact:document.querySelector('#pa-defaults-impact').textContent, changes:document.querySelector('#pa-agent-changes').textContent, effective:[...document.querySelectorAll('[data-agent-scope]:not([hidden]) [id$=\"-effective\"]')].map(e=>e.textContent)})"),indent=2))
        async def submit():
            await wait("!document.querySelector('#pa-agent-prefs-form [type=submit]').disabled")
            await page.evaluate("document.querySelector('#pa-agent-prefs-form').requestSubmit()")
        await wait("[...document.querySelectorAll('[data-settings-defaults-scope]')].every(e=>e.dataset.optionsLoading==='false')")
        assert 'model-a · From Instance defaults' in await text('#pa-user-model-effective')
        assert 'openinterpreter (preferred) · From My defaults' in await text('#pa-user-provider-effective')
        assert 'workspace-write · From Instance defaults' in await text('#pa-user-mode-effective')
        assert 'high · From Instance defaults' in await text('#pa-user-effort-effective')
        assert await page.evaluate("document.querySelector('[data-agent-scope=global]').hidden")
        assert app.state.settings_writes == []
        assert 'No changes' in await text('#pa-agent-changes')
        await capture('personal-inherited')
        # Different scopes retain drafts, but personal preview uses saved instance values.
        await choose('#pa-defaults-scope','global')
        await choose('#pa-global-model','model-b')
        await choose('#pa-defaults-scope','user')
        assert 'model-a · From Instance defaults' in await text('#pa-user-model-effective')
        await choose('#pa-user-model','model-b')
        await wait("document.querySelector('#pa-user-model-effective').textContent.includes('model-b')")
        assert 'model-b · From My defaults' in await text('#pa-user-model-effective')
        assert 'Model: Inherited → model-b' in await text('#pa-agent-changes')
        await submit()
        await wait("document.querySelector('#pa-agent-prefs-status').textContent==='My defaults saved.'")
        assert len(app.state.settings_writes)==1
        body=app.state.settings_writes[-1]
        assert body['scope']=='user' and 'agent_auto_approve_permissions' not in body
        assert body['agent_surfaces']['chat.default']['config']=={'private-option':'keep'}
        assert personal.load().agent_surfaces['chat.default'].execution_preferences == original_user.agent_surfaces['chat.default'].execution_preferences
        assert personal.load().agent_auto_approve_permissions is False
        assert instance.load()==original_instance
        # Instance save does not write personal state or permissions, including retained saved effort.
        await choose('#pa-defaults-scope','global')
        assert 'model-b' in await text('#pa-agent-changes')
        await capture('instance-change-review')
        await submit()
        await wait("document.querySelector('#pa-agent-prefs-status').textContent==='Instance defaults saved.'")
        assert app.state.settings_writes[-1]['scope']=='global'
        assert instance.load().agent_surfaces['chat.default'].effort=='high'
        assert instance.load().agent_auto_approve_permissions is True
        assert personal.load().agent_surfaces['chat.default'].model_id=='model-b'
        # Reset is explicit and changes only the personal native field.
        await choose('#pa-defaults-scope','user')
        await page.evaluate("document.querySelector('#pa-user-model-effective').nextElementSibling.click()")
        await wait("document.querySelector('#pa-user-model-effective').textContent.includes('From Instance defaults')")
        assert 'model-b · From Instance defaults' in await text('#pa-user-model-effective')
        await submit()
        await wait("document.querySelector('#pa-agent-prefs-status').textContent==='My defaults saved.'")
        assert personal.load().agent_surfaces['chat.default'].model_id is None
        # A rejected save retains edits and cannot claim success.
        app.state.reject_settings=True
        await choose('#pa-user-model','model-a')
        await submit()
        await wait("document.querySelector('#pa-agent-prefs-status').textContent.includes('Save rejected (403)')")
        assert personal.load().agent_surfaces['chat.default'].model_id is None
        assert 'model-a · From My defaults' in await text('#pa-user-model-effective')
        app.state.reject_settings=False
        # Per-surface provider edits keep hidden model/config defaults on that surface.
        await page.evaluate("document.querySelector('[name=surface_chat_card]').closest('details').open=true")
        await choose('[name=surface_chat_card]','codex')
        await submit()
        await wait("document.querySelector('#pa-agent-prefs-status').textContent==='My defaults saved.'")
        assert personal.load().agent_surfaces['chat.card'].model_id=='card-model'
        assert personal.load().agent_surfaces['chat.card'].config=={'card-option':'keep'}
        # A failed read-only preview blocks saving and does not claim an effective value.
        app.state.reject_preview=True
        count=len(app.state.settings_writes)
        await choose('#pa-user-model','model-b')
        assert 'Preview unavailable' in await text('#pa-user-model-effective')
        assert await page.evaluate("document.querySelector('#pa-agent-prefs-form [type=submit]').disabled")
        assert len(app.state.settings_writes)==count
        app.state.reject_preview=False
        # Leaving the section retains the personal model draft and its unsaved summary.
        await choose('#pa-user-model','model-b')
        await page.evaluate("document.querySelector('[data-section-link=appearance]').click()")
        await wait("document.querySelector('[data-settings-loaded-section]')?.dataset.settingsLoadedSection==='appearance'")
        await page.evaluate("document.querySelector('[data-section-link=agent]').click()")
        await wait("document.querySelector('#pa-user-model-effective')?.textContent.includes('model-b') && !document.querySelector('#pa-user-model-effective').textContent.includes('Checking')")
        assert await page.evaluate("document.querySelector('#pa-user-model').value")=='model-b'
        assert 'Changes to my defaults' in await text('#pa-agent-changes')
        await page.evaluate("sessionStorage.removeItem('pa:settings:agent-draft')")
        app.state.catalog_empty=True
        # Reload and narrow viewport show the saved scope and source, with associated descriptions.
        await page.navigate_and_wait(base+'/settings?section=agent')
        await wait("document.querySelector('#pa-user-model-effective')?.textContent.includes('model-a')")
        assert await page.evaluate("document.querySelector('#pa-user-model').getAttribute('aria-describedby')")=='pa-user-model-effective'
        # Missing capability evidence must not reset saved effort on an unrelated instance save.
        await choose('#pa-defaults-scope','global')
        assert await page.evaluate("document.querySelector('#pa-global-effort').value")=='high'
        assert 'No changes' in await text('#pa-agent-changes')
        await choose('[name=global_provider]','openinterpreter')
        await submit()
        await wait("document.querySelector('#pa-agent-prefs-status').textContent==='Instance defaults saved.'")
        assert 'agent_surfaces' not in app.state.settings_writes[-1]
        assert instance.load().agent_surfaces['chat.default'].effort=='high'
        assert instance.load().agent_auto_approve_permissions is True
        assert personal.load().agent_auto_approve_permissions is False
        await choose('#pa-defaults-scope','user')
        await manager.resize(scope,width=390,height=844)
        assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        await capture('personal-mobile')
    finally:
        await manager.detach(scope)
        await browser.close()


@pytest.mark.skipif(not _browser_executable(), reason='managed Chromium is not installed')
@pytest.mark.parametrize('operator_app', [False], indirect=True)
@pytest.mark.asyncio
async def test_first_personal_save_tracks_updated_instance_fallback(operator_server, tmp_path):
    app, base = operator_server
    browser = BrowserManager(tmp_path/'browser')
    manager = BrowserSessionManager(browser, instance_id='settings-first-save')
    scope = BrowserScope('user:local','settings-first-save','settings-first-save')
    user = get_preferences_store(app.state.ctx.settings.data_dir, user_id='local')
    try:
        await manager.attach(scope, url=base+'/settings?section=agent', width=1280, height=1000)
        page = manager.resolve(scope).page
        async def wait(expr):
            deadline=time.monotonic()+15
            while time.monotonic()<deadline:
                if await page.evaluate(expr): return
                await asyncio.sleep(.05)
            pytest.fail(expr)
        async def choose(selector, value):
            await page.evaluate(f"(()=>{{const e=document.querySelector({json.dumps(selector)});e.value={json.dumps(value)};e.dispatchEvent(new Event('change',{{bubbles:true}}));}})()")
            await wait("![...document.querySelectorAll('[id$=\"-effective\"]')].some(e=>e.textContent.includes('Checking')) && [...document.querySelectorAll('[data-settings-defaults-scope]')].every(e=>e.dataset.optionsLoading==='false')")
        async def save(expected):
            await wait("!document.querySelector('#pa-agent-prefs-form [type=submit]').disabled")
            await page.evaluate("document.querySelector('#pa-agent-prefs-form').requestSubmit()")
            await wait(f"document.querySelector('#pa-agent-prefs-status').textContent==={json.dumps(expected)}")
        await wait("document.querySelector('#pa-user-model').value==='model-a'")
        assert not user.path.exists()
        await choose('#pa-defaults-scope','global')
        await choose('#pa-global-model','model-b')
        await save('Instance defaults saved.')
        assert not user.path.exists()
        await choose('#pa-defaults-scope','user')
        assert await page.evaluate("document.querySelector('#pa-user-model').value")=='model-b'
        assert 'No changes' in await page.evaluate("document.querySelector('#pa-agent-changes').textContent")
        assert 'Instance defaults (account inherits)' in await page.evaluate("document.querySelector('#pa-user-model-effective').textContent")
        await choose('[name=telemetry_session_header]','expanded')
        await save('My defaults saved.')
        assert user.path.exists()
        assert user.load().agent_surfaces['chat.default'].model_id=='model-b'
        assert user.load().agent_auto_approve_permissions is True
    finally:
        await manager.detach(scope)
        await browser.close()
