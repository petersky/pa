"""Rendered native settings follow model evidence and exact-session acknowledgments."""
import asyncio
import json
import os
from pathlib import Path
import time

import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.agent_effort_fixture import build_effort_fixture
from tests.test_card_status_browser import operator_server


@pytest.fixture
def operator_app(tmp_path):
    return build_effort_fixture(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason='managed Chromium is not installed')
@pytest.mark.asyncio
async def test_model_effort_drafts_acknowledgment_and_deferred_apply(operator_server, tmp_path):
    app, base = operator_server
    browser = BrowserManager(tmp_path / 'browser')
    manager = BrowserSessionManager(browser, instance_id='effort-test')
    scope = BrowserScope('user:test', 'effort-test', 'effort-test')
    try:
        await manager.attach(scope,url=base,width=1280,height=1000)
        page = manager.resolve(scope).page
        async def wait(expr):
            deadline=time.monotonic()+12
            while time.monotonic()<deadline:
                if await page.evaluate(expr): return
                await asyncio.sleep(.05)
            pytest.fail(expr)
        w="document.querySelector('[data-agent-chat]')._acw"
        model='[data-acw-config-id=model]'
        effort='[data-acw-config-id=reasoning_effort]'
        async def choose(selector,value):
            await page.evaluate(f'''(()=>{{const e=document.querySelector({json.dumps(selector)}); e.value={json.dumps(value)};e.dispatchEvent(new Event('change',{{bubbles:true}}));}})()''')
        async def values():
            return await page.evaluate(f"[...document.querySelector('{effort}').options].map(o=>o.value)")
        await wait(f"!!{w}.effortCatalog && {w}.effortCatalog.length>0")
        assert 'ultra' in await values()
        for high_model in ('gpt-6.1-sol','gpt-6-astra'):
            await choose(model,high_model)
            await choose(effort,'ultra')
            await choose(model,'gpt-6-luna')
            assert 'ultra' not in await values()
            assert await page.evaluate(f"document.querySelector('{effort}').value") == ''
            assert 'Reset to Automatic' in await page.evaluate("document.querySelector('[data-acw-settings-status]').textContent")
            assert app.state.calls == []  # Draft preview never mutates the provider.
            await choose(model,high_model)
            assert 'ultra' in await values()
            assert await page.evaluate(f"document.querySelector('{effort}').value") == ''
        await choose(effort,'high')
        await choose(model,'gpt-6-luna')
        assert await page.evaluate(f"document.querySelector('{effort}').value") == 'high'
        if os.environ.get('PA_TEST_EVIDENCE_DIR'):
            evidence_dir=Path(os.environ['PA_TEST_EVIDENCE_DIR']); evidence_dir.mkdir(parents=True,exist_ok=True)
            await page.evaluate("document.querySelector('.acw-settings-menu').open=true")
            (evidence_dir/'effort-luna.png').write_bytes(await manager.screenshot(scope))
            (evidence_dir/'effort-luna.json').write_text(json.dumps({'model':'gpt-6-luna','advertised_efforts':await values(),'selected':'high','provider_mutations':len(app.state.calls)}))
        # Model acknowledgment must precede effort, and both target the source session.
        await page.evaluate(f'{w}.applySettings()')
        assert [(c[0],c[2]['config_id']) for c in app.state.calls] == [('live-fixture','model'),('live-fixture','reasoning_effort')]
        assert app.state.current_effort == 'high'
        app.state.calls.clear()
        await choose(effort,'')
        await page.evaluate(f'{w}.applySettings()')
        assert app.state.calls == [] and app.state.current_effort == 'high'
        assert 'no reset was applied' in await page.evaluate("document.querySelector('[data-acw-settings-status]').textContent")
        await page.evaluate(f'{w}.resetSettingsDraft()')
        app.state.model_ack=False
        await choose(model,'gpt-6-astra'); await choose(effort,'ultra')
        await page.evaluate(f'{w}.applySettings()')
        assert len(app.state.calls)==1
        assert 'not confirmed' in await page.evaluate("document.querySelector('[data-acw-settings-status]').textContent")
        # Failed acknowledgments cannot claim success or send effort to the old model.
        app.state.model_ack=True
        await page.evaluate(f'{w}.resetSettingsDraft()')
        for unknown in ('unknown-model','no-effort'):
            await choose(model,unknown)
            assert await values() == ['']
            assert await page.evaluate(f"document.querySelector('{effort}').disabled")
        # A stale stored value is removed on restoration, never injected as an option.
        app.state.current_model='gpt-6-luna'; app.state.current_effort='ultra'
        await page.navigate_and_wait(base)
        await wait(f"!!{w}.effortCatalog && {w}.effortCatalog.length>0")
        assert 'ultra' not in await values()
        assert await page.evaluate(f"document.querySelector('{effort}').value") == ''
        # Reject stale cross-session snapshots instead of replacing current options.
        await page.evaluate(f"{w}.applyOptionSnapshot({{session:{{id:'other-fixture',model_id:'gpt-6.1-sol'}}}})")
        assert 'ultra' not in await values()
        # Combined native request defers as one model+effort tuple; no legacy PUTs.
        app.state.use_receipt=True; app.state.native_pending=True; app.state.calls.clear()
        await page.navigate_and_wait(base)
        await wait(f"!!{w}.effortCatalog && {w}.effortCatalog.length>0")
        await choose(model,'gpt-6-astra'); await choose(effort,'ultra')
        await page.evaluate(f'{w}.applySettings()')
        assert len(app.state.calls)==1 and app.state.calls[0][1]=='native'
        prefs=app.state.calls[0][2]['execution_preferences']
        assert prefs['model']['value']=='gpt-6-astra' and prefs['reasoning']['value']=='ultra'
        assert app.state.calls[0][2]['defer'] is True
        assert 'pending' in await page.evaluate("document.querySelector('[data-acw-settings-status]').textContent")
        assert app.state.current_model=='gpt-6-luna'
        # A late response from a previous catalog generation cannot restore ultra.
        app.state.catalog_delay=.4
        await page.evaluate(f"{w}.effortCatalogKey=null; {w}.refreshEffortCatalog({w}.lastSnapshot)")
        await choose(model,'gpt-6-luna')
        await asyncio.sleep(.55)
        assert 'ultra' not in await values()
        # Switch conversations while a legacy model acknowledgment is pending.
        app.state.use_receipt=False; app.state.model_delay=.5; app.state.catalog_delay=0
        await page.navigate_and_wait(base)
        await wait(f"!!{w}.effortCatalog && {w}.effortCatalog.length>0")
        app.state.calls.clear()
        await choose(model,'gpt-6-astra'); await choose(effort,'ultra')
        await page.evaluate(f'void {w}.applySettings()')
        await wait(f'{w}.settingsPending===true')
        await page.evaluate(f"window.confirm=()=>true; {w}.switchSession('other-fixture',true,'live-fixture')")
        await wait(f"{w}.sessionId==='other-fixture'")
        await asyncio.sleep(.7)
        assert len(app.state.calls)==1 and app.state.calls[0][0]=='live-fixture'
        assert app.state.calls[0][2]['config_id']=='model'
        assert not await page.evaluate(f'{w}.settingsPending')
    finally:
        await manager.detach(scope)
        await browser.close()


@pytest.mark.skipif(not _browser_executable(), reason='managed Chromium is not installed')
@pytest.mark.asyncio
async def test_new_chat_defaults_native_receipt_and_scope_switches(operator_server,tmp_path):
    app,base=operator_server
    browser=BrowserManager(tmp_path/'browser')
    manager=BrowserSessionManager(browser,instance_id='effort-new')
    scope=BrowserScope('user:test','effort-new','effort-new')
    try:
        await manager.attach(scope,url=base+'/new',width=1280,height=900)
        page=manager.resolve(scope).page
        async def wait(expr):
            deadline=time.monotonic()+12
            while time.monotonic()<deadline:
                if await page.evaluate(expr): return
                await asyncio.sleep(.05)
            pytest.fail(expr)
        async def choose(selector,value):
            await page.evaluate(f'''(()=>{{const e=document.querySelector({json.dumps(selector)});e.value={json.dumps(value)};e.dispatchEvent(new Event('change',{{bubbles:true}}));}})()''')
        async def vals(selector):
            return await page.evaluate(f'[...document.querySelector({json.dumps(selector)}).options].map(o=>o.value)')
        effort='select[aria-label="Native reasoning value"]'
        await wait("document.querySelector('[data-execution-preferences]')._selection?.catalogLoaded && document.querySelector('[data-execution-preferences]')._selection?.defaultsLoaded")
        assert '"ultra"' in await vals(effort)
        await choose('select[aria-label=Model]','"gpt-6-luna"')
        assert '"ultra"' not in await vals(effort)
        prefs=await page.evaluate("JSON.parse(document.querySelector('input[name=execution_preferences]').value)")
        assert prefs['reasoning']=={'intent':'automatic'}
        await choose('select[aria-label=Model]','"gpt-6-astra"')
        assert '"ultra"' in await vals(effort)
        # Changing agents clears model-scoped choices; unknown support never imports Codex levels.
        await choose('select[aria-label=Agent]','"cursor"')
        assert '"ultra"' not in await vals(effort)
        assert await page.evaluate(f'document.querySelector({json.dumps(effort)}).disabled')
        await choose('select[aria-label=Agent]','"codex"')
        await choose('select[aria-label=Model]','"gpt-6.1-sol"')
        assert '"ultra"' in await vals(effort)
        assert app.state.calls==[]
        app.state.defaults_model='gpt-6-luna'
        await page.navigate_and_wait(base+'/new')
        await wait("document.querySelector('[data-execution-preferences]')._selection?.catalogLoaded && document.querySelector('[data-execution-preferences]')._selection?.defaultsLoaded")
        assert '"ultra"' not in await vals(effort)
        assert await page.evaluate("document.querySelector('[data-execution-preferences]')._selection.prefs.reasoning.intent") == 'automatic'
        # Defaults use the same scoped evidence and do not reinsert stale saved effort.
        await page.navigate_and_wait(base+'/defaults')
        default_effort='[data-settings-default-effort]'
        await wait("document.querySelector('[data-settings-default-effort]').options.length>1")
        assert 'ultra' in await vals(default_effort)
        await choose('[data-settings-default-model]','gpt-6-luna[medium]')
        assert 'ultra' not in await vals(default_effort)
        assert await page.evaluate("document.querySelector('[data-settings-default-effort]').value") == ''
        assert 'Reset to provider default' in await page.evaluate("document.querySelector('[data-effort-reset]').textContent")
        await choose('[data-settings-default-model]','gpt-6-astra[medium]')
        assert 'ultra' in await vals(default_effort)
        await choose('[data-settings-default-provider]','cursor')
        await wait("document.querySelector('[data-settings-default-model]').value==='' && document.querySelector('[data-settings-default-effort]').disabled")
        assert await vals(default_effort)==['']
        app.state.option_delays['codex']=.5
        await choose('[data-settings-default-provider]','codex')
        await choose('[data-settings-default-provider]','cursor')
        await asyncio.sleep(.65)
        assert await vals(default_effort)==['']
        assert 'gpt-6.1-sol[high]' not in await vals('[data-settings-default-model]')
        # Native receipt uses its latest model selection, without clearing a supported explicit effort.
        app.state.use_receipt=True
        await page.navigate_and_wait(base)
        native='select[aria-label="Session Native reasoning"]'
        await wait("document.querySelector('select[aria-label=\"Session Model\"]')?.options.length>1")
        await choose(native,'"high"')
        await choose('select[aria-label="Session Model"]','"gpt-6-luna"')
        assert '"ultra"' not in await vals(native)
        assert await page.evaluate(f'document.querySelector({json.dumps(native)}).value')=='"high"'
        await choose('select[aria-label="Session Model"]','"gpt-6.1-sol"')
        await choose(native,'"ultra"')
        await choose('select[aria-label="Session Model"]','"gpt-6-luna"')
        assert await page.evaluate(f'document.querySelector({json.dumps(native)}).value')==''
        assert app.state.calls==[]
    finally:
        await manager.detach(scope)
        await browser.close()
