"""Real shell navigation, schedule controls and session alarm dialog."""
import asyncio
import time
from uuid import uuid4

import httpx
import pytest
from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.test_card_status_browser import operator_server  # noqa: F401
from tests.visualization_chat_fixture import build_visualization_app


@pytest.fixture
def operator_app(tmp_path):
    return build_visualization_app(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason='managed Chromium is not installed')
@pytest.mark.asyncio
@pytest.mark.parametrize('width',[1280,390])
async def test_loops_and_session_alarm(operator_server,tmp_path,width):
    app,base=operator_server
    browser=BrowserManager(tmp_path/'browser')
    manager=BrowserSessionManager(browser,instance_id='loops-test')
    scope=BrowserScope('user:test','loops','loops-test')
    async with httpx.AsyncClient(base_url=base,follow_redirects=True) as client:
        await client.get('/fixture')
        response=await client.get('/loops')
        assert response.status_code==200 and '>Loops<' in response.text
        headers={'X-CSRF-Token':client.cookies.get('pa_csrf')}
        async def command(action,**args):
            key=str(uuid4())
            result=await client.post('/api/schedules',headers={**headers,'Idempotency-Key':key},json={'action':action,'idempotency_key':key,**args})
            assert result.status_code==200,result.text
            return result.json()
        try:
            await manager.attach(scope,url=base+'/agent?session=viz-session',width=width,height=900)
            page=manager.resolve(scope).page
            async def wait(expression):
                deadline=time.monotonic()+20
                while time.monotonic()<deadline:
                    if await page.evaluate(expression): return
                    await asyncio.sleep(.05)
                pytest.fail('Browser timeout: '+expression)
            await wait("!!document.querySelector('[data-acw-reminders]')")
            assert await page.evaluate("document.querySelector('[data-acw-reminders]').hidden")
            records=[]
            for kind,trigger in [('once',{'at':'2030-01-01T00:00:00Z'}),('interval',{'seconds':600}),('event',{'event_type':'card.updated'})]:
                records.append(await command('create',schedule={'name':kind+' reminder','trigger':{'kind':kind,**trigger},'destination':{'kind':'inbox','target':'user:local'},'additional_destinations':[{'kind':'agent_session','target':'viz-session'}], 'payload':'fixture reminder','context_refs':{'session':'viz-session'}}))
            await page.evaluate("document.dispatchEvent(new Event('pa:loops-changed'))")
            await wait("!document.querySelector('[data-acw-reminders]').hidden")
            await page.evaluate("document.querySelector('[data-acw-reminders]').click()")
            await wait("document.querySelector('[data-acw-reminders-dialog]').open && document.querySelectorAll('[data-acw-reminders-list] article').length===3")
            text=await page.evaluate("document.querySelector('[data-acw-reminders-dialog]').textContent")
            assert all(value in text for value in ['once reminder','interval reminder','event reminder','user:local','viz-session','unsupported'])
            for record in records: await command('cancel',schedule_id=record['id'],version=record['version'])
            await page.evaluate("document.dispatchEvent(new Event('pa:loops-changed'))")
            await wait("document.querySelector('[data-acw-reminders-status]').textContent.startsWith('No pending reminders.')")
            assert await page.evaluate("document.querySelector('[data-acw-reminders]').hidden")
            await page.evaluate("document.querySelector('[data-acw-reminders-close]').click()")
            assert not await page.evaluate("document.querySelector('[data-acw-reminders-dialog]').open")
            # Native shell navigation runs the editor script after an HTMX swap.
            await page.evaluate("document.querySelector('a[href=\"/loops\"]').click()")
            await wait("!!document.querySelector('[data-loops-page]') && document.querySelectorAll('#records article').length===3")
            assert await page.evaluate("document.querySelector('#records').textContent.includes('cancelled')")
            assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            assert await page.evaluate("document.querySelectorAll('#records article button').length") == 6  # Delete and History only.
            # Discovery and list failures remain explicit and recover on refresh.
            await page.evaluate("""window.loopsFetch=window.fetch;window.fetch=(path,...args)=>String(path).includes('/api/backups/status')||String(path)==='/api/schedules'?Promise.reject(Error('Fixture network unavailable')):window.loopsFetch(path,...args);document.getElementById('refresh').click()""")
            await wait("document.getElementById('feedback').textContent.includes('Fixture network unavailable') && document.getElementById('managed-loops').textContent.includes('Discovery unavailable')")
            await page.evaluate("window.fetch=window.loopsFetch;document.getElementById('refresh').click()")
            await wait("document.querySelectorAll('#records article').length===3 && !document.getElementById('managed-loops').textContent.includes('Loading')")
            await page.evaluate("document.getElementById('create-loop').click()")
            assert await page.evaluate("document.getElementById('editor-panel').open && document.activeElement.id==='name'")
            await page.evaluate("""document.getElementById('name').value='UI loop';
                document.getElementById('payload').value='UI reminder';
                document.getElementById('source-session').value='viz-session';
                document.getElementById('additional-destinations').value=JSON.stringify([{kind:'agent_session',target:'viz-session'}]);
                document.getElementById('editor').requestSubmit();""")
            await wait("[...document.querySelectorAll('#records article')].some(a=>a.querySelector('h2').textContent==='UI loop')")
            action = """(([name,label])=>{const article=[...document.querySelectorAll('#records article')].find(a=>a.querySelector('h2').textContent===name);[...article.querySelectorAll('button')].find(b=>b.textContent===label).click();})"""
            await page.evaluate("document.getElementById('loop-search').value='UI loop';document.getElementById('loop-search').dispatchEvent(new Event('input'))")
            assert await page.evaluate("document.querySelectorAll('#records article:not([hidden])').length") == 1
            await page.evaluate("document.getElementById('loop-search').value='missing';document.getElementById('loop-search').dispatchEvent(new Event('input'))")
            assert await page.evaluate("!!document.getElementById('no-matches')")
            await page.evaluate("document.getElementById('loop-search').value='';document.getElementById('loop-search').dispatchEvent(new Event('input'))")
            for appearance in ['light','dark']:
                await page.evaluate("window.PATheme.setAppearance('"+appearance+"');document.getElementById('editor-panel').open=false;document.activeElement.blur();document.querySelectorAll('*').forEach(e=>{if(e.scrollTop)e.scrollTop=0});window.scrollTo(0,0)")
                await wait("getComputedStyle(document.documentElement).getPropertyValue('--pa-bg').trim()==='"+("#f4f5f8" if appearance=="light" else "#0f1117")+"'")
                assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                (tmp_path/f'loops-{width}-{appearance}.png').write_bytes(await page.screenshot())
            await page.evaluate(action + "(['UI loop','Edit'])")
            await page.evaluate("document.getElementById('name').value='Edited UI loop';document.getElementById('editor').requestSubmit()")
            await wait("[...document.querySelectorAll('#records h2')].some(a=>a.textContent==='Edited UI loop')")
            for label,state in [('Pause','paused'),('Resume','active'),('Cancel','cancelled')]:
                await page.evaluate(action + "(['Edited UI loop','"+label+"'])")
                await wait("[...document.querySelectorAll('#records article')].some(a=>a.querySelector('h2').textContent==='Edited UI loop'&&a.querySelector('pre').textContent.includes('State: "+state+"'))")
            await page.evaluate("document.getElementById('loop-state').value='cancelled';document.getElementById('loop-state').dispatchEvent(new Event('change'))")
            assert await page.evaluate("[...document.querySelectorAll('#records article:not([hidden])')].every(a=>a.dataset.state==='cancelled')")
            await page.evaluate(action + "(['Edited UI loop','Delete'])")
            await wait("![...document.querySelectorAll('#records h2')].some(a=>a.textContent==='Edited UI loop')")
        finally:
            await manager.close()
            await browser.close()
