"""Full PA chat regression: portable artifacts, opaque scripts and native UI."""
import asyncio
import base64
import json
import time

import pytest
import websockets

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.test_card_status_browser import operator_server
from tests.visualization_chat_fixture import build_visualization_app


@pytest.fixture
def operator_app(tmp_path):
    return build_visualization_app(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_real_chat_visualization_controls_state_reload_and_isolation(operator_server, tmp_path):
    app, base = operator_server
    browser = BrowserManager(tmp_path/"browser")
    manager = BrowserSessionManager(browser,instance_id="visualization-test")
    scope = BrowserScope("user:test","visualization-test","visualization-test")
    try:
        await manager.attach(scope,url=base+"/fixture",width=1280,height=1000)
        page = manager.resolve(scope).page

        async def wait_for(expression, timeout=15):
            deadline = time.monotonic()+timeout
            while time.monotonic()<deadline:
                if await page.evaluate(expression): return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition timed out: "+expression+"\n"+str(await page.evaluate("document.body.innerText.slice(-3000)")))

        frames_ready = (
            "document.querySelectorAll('.acw-visualization iframe').length===3 && "
            "[...document.querySelectorAll('.acw-visualization iframe')].every("
            "f=>f.parentElement.querySelector('[role=status]').textContent==='Interactive preview loaded')"
        )

        async def frame(expression, index=0):
            # Debugger-only inspection; the application cannot traverse the opaque iframe.
            tree = await page.command("DOM.getDocument",{"depth":-1,"pierce":True})
            frames = []
            def visit(node):
                if node.get("nodeName") == "IFRAME":
                    frames.append(node)
                for child in node.get("children",[]): visit(child)
            visit(tree["root"])
            assert len(frames)>index, "Preview iframe unavailable"
            if doc := frames[index].get("contentDocument"):
                obj=await page.command("DOM.resolveNode",{"backendNodeId":doc["backendNodeId"]})
                result=await page.command("Runtime.callFunctionOn",{
                    "objectId":obj["object"]["objectId"],
                    "functionDeclaration":"function(){const document=this,window=this.defaultView;return ("+expression+");}",
                    "returnByValue":True,"awaitPromise":True,"userGesture":True})
                assert "exceptionDetails" not in result,result
                return result.get("result",{}).get("value")
            target = frames[index]["frameId"]
            # Opaque srcdoc frames can be out-of-process. Attach via CDP rather
            # than weakening the application's sandbox for test inspection.
            from pa.browser.cdp import page_target
            endpoint = await page_target(page.endpoint,page.target_id)
            async with asyncio.timeout(10), websockets.connect(endpoint["webSocketDebuggerUrl"]) as ws:
                await ws.send(json.dumps({"id":1,"method":"Target.attachToTarget","params":{"targetId":target,"flatten":True}}))
                while True:
                    message=json.loads(await ws.recv())
                    if message.get("id")==1: break
                assert "error" not in message,message
                sid=message["result"]["sessionId"]
                await ws.send(json.dumps({"id":2,"sessionId":sid,"method":"Runtime.evaluate","params":{
                    "expression":expression,"returnByValue":True,"awaitPromise":True,"userGesture":True}}))
                while True:
                    message=json.loads(await ws.recv())
                    if message.get("id")==2: break
                assert "error" not in message,message
                result=message["result"]
            assert "exceptionDetails" not in result,result
            return result.get("result",{}).get("value")

        await wait_for("document.querySelectorAll('.acw-visualization iframe').length===3")
        await wait_for("[...document.querySelectorAll('.acw-visualization')].some(e=>e.textContent.includes('not supported'))")
        assert await page.evaluate("document.querySelectorAll('.acw-visualization').length") == 7
        assert await page.evaluate("document.querySelector('.acw-visualization.is-wide > strong').textContent") == "PA Home concepts"
        assert await page.evaluate("[...document.querySelectorAll('.acw-visualization')].map(e=>e.textContent).join(' ').includes('\\ue200visualize')") is False
        assert await page.evaluate("[...document.querySelectorAll('.acw-visualization')].some(e=>e.textContent.includes('reference is invalid'))")
        await wait_for("[...document.querySelectorAll('.acw-visualization')].some(e=>e.textContent.includes('artifact is unavailable'))")
        await wait_for("[...document.querySelectorAll('.acw-visualization')].some(e=>e.textContent.includes('do not have access'))")
        await wait_for(frames_ready)
        assert await page.evaluate("[...document.querySelectorAll('.acw-visualization iframe')].every(f=>f.getAttribute('sandbox')==='allow-scripts')")
        assert await frame("document.querySelector('[data-variant]:not([hidden])').dataset.variant") == "Action rows"
        await page.evaluate("document.querySelector('.acw-visualization').scrollIntoView({block:'start'})")
        screenshot=await page.command("Page.captureScreenshot",{"format":"png"})
        (tmp_path/"pa-chat-home-wide.png").write_bytes(base64.b64decode(screenshot["data"]))
        # Named variant select and native keyboard operation.
        await frame("document.querySelector('select[aria-label=\"Design variant\"]').focus()")
        await page.command("Input.dispatchKeyEvent",{"type":"keyDown","key":"ArrowDown","code":"ArrowDown","windowsVirtualKeyCode":40})
        await page.command("Input.dispatchKeyEvent",{"type":"keyUp","key":"ArrowDown","code":"ArrowDown","windowsVirtualKeyCode":40})
        await page.command("Input.dispatchKeyEvent",{"type":"keyDown","key":"Enter","code":"Enter","windowsVirtualKeyCode":13})
        await page.command("Input.dispatchKeyEvent",{"type":"keyUp","key":"Enter","code":"Enter","windowsVirtualKeyCode":13})
        # Native select behavior differs by platform; its next-design button is also keyboard accessible.
        if await frame("document.querySelector('[data-variant]:not([hidden])').dataset.variant") != "Attention first":
            await frame("document.querySelector('[aria-label=\"Next design\"]').click()")
        assert await frame("document.querySelector('[data-variant]:not([hidden])').dataset.variant") == "Attention first"
        for variant in ("Attention first","Action rows"):
            await frame("(()=>{const s=document.querySelector('[aria-label=\"Design variant\"]');s.value="+json.dumps("1" if variant=="Attention first" else "0")+";s.dispatchEvent(new Event('change'));return true})()")
            for name, expected in (("start","Availability"),("evidence","Permission defaults"),("pr","#464")):
                await frame("document.querySelector('[data-variant]:not([hidden]) [data-open="+name+"]').click()")
                assert expected in await frame("document.querySelector('[data-variant]:not([hidden]) [data-detail]').textContent")
                assert await frame("document.activeElement.hasAttribute('data-back')")
                if name == "start":
                    await frame("document.querySelector('[data-variant]:not([hidden]) details summary').click()")
                    assert await frame("document.querySelector('[data-variant]:not([hidden]) details').open")
                await frame("document.querySelector('[data-variant]:not([hidden]) [data-back]').click()")
                assert await frame("document.activeElement.dataset.open") == name
        await frame("document.querySelector('[data-variant]:not([hidden]) [data-open=pr]').focus()")
        await page.command("Input.dispatchKeyEvent",{"type":"keyDown","key":"Tab","code":"Tab","windowsVirtualKeyCode":9})
        await page.command("Input.dispatchKeyEvent",{"type":"keyUp","key":"Tab","code":"Tab","windowsVirtualKeyCode":9})
        assert await page.evaluate("document.activeElement.tagName!=='IFRAME'")
        # Tweak controls affect both variants and persist via the narrow bridge.
        await frame("document.querySelector('.pa-viz-tweak').open=true")
        await frame("document.querySelector('.pa-viz-tweak input').click()")
        await frame("(()=>{const s=document.querySelector('.pa-viz-tweak select');s.value='Compact';s.dispatchEvent(new Event('change'));return true})()")
        assert await frame("[...document.querySelectorAll('.pa-machine')].every(e=>e.hidden)")
        assert await frame("getComputedStyle(document.querySelector('.pa-row')).paddingTop") == "10px"
        await asyncio.sleep(.4)
        assert await frame("window.openai.widgetState.privateContent.density") == "Compact"
        # The supplied standalone wrapper uses the same PA runtime and bridge.
        assert await frame("document.querySelector('[data-variant]:not([hidden])').dataset.variant",2)=="Action rows"
        await frame("document.querySelector('[aria-label=\"Next design\"]').click()",2)
        assert await frame("document.querySelector('[data-variant]:not([hidden])').dataset.variant",2)=="Attention first"
        for name, expected in (("start","Availability"),("evidence","Permission defaults"),("pr","#464")):
            await frame("document.querySelector('[data-variant]:not([hidden]) [data-open="+name+"]').click()",2)
            assert expected in await frame("document.querySelector('[data-variant]:not([hidden]) [data-detail]').textContent",2)
            await frame("document.querySelector('[data-variant]:not([hidden]) [data-back]').click()",2)
        await frame("document.querySelector('.pa-viz-tweak').open=true",2)
        await frame("document.querySelector('.pa-viz-tweak input').click()",2)
        await frame("(()=>{const s=document.querySelector('.pa-viz-tweak select');s.value='Compact';s.dispatchEvent(new Event('change'));return true})()",2)
        assert await frame("[...document.querySelectorAll('.pa-machine')].every(e=>e.hidden)",2)
        assert await frame("getComputedStyle(document.querySelector('.pa-row')).paddingTop",2)=="10px"
        # The bridge grants no credential, DOM, storage, fetch, or application API access.
        assert await frame("(()=>{try{return parent.document.cookie}catch(e){return e.name}})()") == "SecurityError"
        assert await frame("(()=>{try{return document.cookie}catch(e){return e.name}})()") == "SecurityError"
        assert await frame("fetch('/api/agent/sessions').then(()=>false,()=>true)")
        assert await frame("typeof window.openai.callTool") == "undefined"
        # Spoofed window messages cannot resize or mutate preview state.
        before = await page.evaluate("document.querySelector('.acw-visualization iframe').style.height")
        await page.evaluate("window.postMessage({bridge:'pa.visualization/v1',type:'resize',height:3,token:'wrong'},'*')")
        assert await page.evaluate("document.querySelector('.acw-visualization iframe').style.height") == before
        # Parent route/conversation has not changed while the mockups navigate.
        assert await page.evaluate("new URL(location.href).searchParams.get('session')") == "viz-session"
        assert len(app.state.ctx.store.list_transcript_events("viz-session")) == 11
        await frame("window.retainedContext='kept'")
        await page.evaluate("(()=>{const w=document.querySelector('[data-agent-chat]')._acw;w.renderTranscript(w.transcriptEvents.slice());return true})()")
        assert await frame("window.retainedContext") == "kept"
        for appearance in ("light","dark"):
            await page.evaluate("document.documentElement.style.colorScheme="+json.dumps(appearance))
            await asyncio.sleep(.1)
            assert await frame("getComputedStyle(document.documentElement).colorScheme") == appearance
            for width in (390,820,1280):
                await manager.resize(scope,width=width,height=1000)
                await asyncio.sleep(.1)
                assert await page.evaluate("document.documentElement.scrollWidth<=innerWidth")
                assert await frame("document.documentElement.scrollWidth<=innerWidth")
        await page.evaluate("document.querySelector('.acw-visualization-actions button').click()")
        await wait_for("!!document.querySelector('dialog[open] iframe')")
        await page.command("Input.dispatchKeyEvent",{"type":"keyDown","key":"Escape","code":"Escape","windowsVirtualKeyCode":27})
        await page.command("Input.dispatchKeyEvent",{"type":"keyUp","key":"Escape","code":"Escape","windowsVirtualKeyCode":27})
        await wait_for("!document.querySelector('.acw-visualization-dialog')")
        assert await page.evaluate("document.activeElement.textContent") == "Open full size"
        # Reload with no originating file/process, then a second isolated viewer profile.
        await page.navigate_and_wait(base+"/agent?session=viz-session")
        # Element presence precedes the srcdoc document/bridge becoming ready.
        await wait_for(frames_ready)
        assert await frame("window.openai.widgetState.privateContent.density") == "Compact"
        assert await frame("[...document.querySelectorAll('.pa-machine')].every(e=>e.hidden)")
        assert await frame("window.openai.widgetState.privateContent.density",2)=="Compact"
        second = BrowserScope("user:test","second-viewer","visualization-test")
        await manager.attach(second,url=base+"/agent?session=viz-session",width=820,height=1000)
        second_page = manager.resolve(second).page
        deadline=time.monotonic()+10
        while time.monotonic()<deadline and not await second_page.evaluate("document.querySelector('.acw-visualization iframe')!==null"):
            await asyncio.sleep(.05)
        assert await second_page.evaluate("document.querySelector('.acw-visualization iframe')!==null")
    finally:
        await browser.close()
