"""Actual card details upload/link/edit/replace/remove and response-loss recovery."""
import asyncio
import json
import os
from pathlib import Path
import time

import httpx
import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import CardCreate
from tests.test_card_status_browser import operator_server
from tests.test_local_operator_acceptance import operator_app


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_attachment_details_full_lifecycle(operator_server, tmp_path):
    app, base = operator_server
    card = app.state.ctx.store.create_card(CardCreate(title="Attachment browser acceptance", body="Full supporting specification"))
    user = app.state.ctx.require_service("users").get("local")
    with httpx.Client(base_url=base, headers={"Authorization": "Bearer " + user.cli_token}) as client:
        initial = []
        for filename, content, media in [("assessment.html", b'<h1>Ribbon assessment</h1><script>parent.unsafe=1</script>', "text/html"), ("audit.json", b'{"audit":"verified"}', "application/json")]:
            result = client.post(f"/api/cards/{card.id}/attachments", files={"file": (filename, content, media)})
            assert result.status_code == 201, result.text
            initial.append(result.json())
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="attachment-test")
    scope = BrowserScope("user:local", "attachment-browser", "attachment-test")
    evidence_dir = Path(os.environ.get("PA_ATTACHMENT_BROWSER_EVIDENCE", str(tmp_path)))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    try:
        await manager.attach(scope, url=base + f"/work?card={card.id}", width=1280, height=1000)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail(expression + "\n" + str(await page.evaluate("document.querySelector('[data-card-attachments]')?.innerText")))

        async def fill(selector, value):
            await page.evaluate(f"document.querySelector({json.dumps(selector)}).value={json.dumps(value)}")

        async def submit(selector):
            await page.evaluate(f"document.querySelector({json.dumps(selector)}).requestSubmit()")

        async def file_input(selector, path):
            tree = await page.command("DOM.getDocument", {})
            node = await page.command("DOM.querySelector", {"nodeId": tree["root"]["nodeId"], "selector": selector})
            await page.command("DOM.setFileInputFiles", {"nodeId": node["nodeId"], "files": [str(path)]})

        await wait_for("document.querySelectorAll('[data-attachment-id]').length===2")
        assert "assessment.html" in await page.evaluate("document.querySelector('[data-card-attachments]').innerText")
        assert "Edit content" in await page.evaluate("document.querySelector('[data-card-attachments]').innerText")
        upload = tmp_path / "notes.txt"
        upload.write_text("User uploaded content")
        await file_input('[data-attachment-form="upload"] input[type=file]', upload)
        # Deliver the mutation, then lose its acknowledgement. The actual UI
        # retries the same request/key and must not create a second attachment.
        await page.evaluate("""window.realAttachmentFetch=window.fetch;window.lostAttachmentResponse=false;
          window.fetch=async function(url,options){const response=await realAttachmentFetch(url,options);
            if(!lostAttachmentResponse && options?.method==='POST' && String(url).includes('/attachments?')){
              lostAttachmentResponse=true;throw new TypeError('simulated response loss after server commit');}return response;};""")
        await submit('[data-attachment-form="upload"]')
        await wait_for("document.querySelector('[data-attachment-status]').textContent.includes('Outcome is unconfirmed')")
        # A different action must not replace the section and lose the original
        # request/key while its outcome is still unknown.
        await fill('[data-attachment-form="link"] [name=name]', "Wait for earlier save")
        await fill('[data-attachment-form="link"] [name=resource_url]', "https://example.org/pending")
        await submit('[data-attachment-form="link"]')
        await wait_for("document.querySelector('[data-attachment-status]').textContent.includes('Finish retrying')")
        await submit('[data-attachment-form="upload"]')
        await wait_for("document.querySelectorAll('[data-attachment-id]').length===3")
        await page.evaluate("window.fetch=realAttachmentFetch")
        link = '[data-attachment-form="link"]'
        await fill(link + ' [name=name]', "Resource specification")
        await fill(link + ' [name=resource_url]', "https://example.org/spec")
        await fill(link + ' [name=description]', "User supporting link")
        # The save succeeds but refreshing card details fails. Retry must replay
        # that same durable operation rather than create a duplicate link.
        await page.evaluate("""window.failedAttachmentRefresh=false;window.fetch=async function(url,options){
          if(!failedAttachmentRefresh && String(url).includes('/partials/cards/') && String(url).includes('/detail')){
            failedAttachmentRefresh=true;return new Response('unavailable',{status:503});}
          return realAttachmentFetch(url,options);};""")
        await submit(link)
        await wait_for("document.querySelector('[data-attachment-status]').textContent.includes('Retry this same action to refresh')")
        await submit(link)
        await wait_for("document.querySelectorAll('[data-attachment-id]').length===4")
        await page.evaluate("window.fetch=realAttachmentFetch")
        link_id = await page.evaluate("[...document.querySelectorAll('[data-attachment-id]')].find(e=>e.innerText.includes('Resource specification')).dataset.attachmentId")
        link_item = f'[data-attachment-id="{link_id}"]'
        await fill(link_item + ' [name=resource_url]', "https://example.org/revised")
        await fill(link_item + ' [name=name]', "Updated resource")
        await submit(link_item + ' [data-attachment-form="metadata"]')
        await wait_for(f"document.querySelector({json.dumps(link_item + ' a')}).href==='https://example.org/revised'")
        uploaded_id = await page.evaluate("[...document.querySelectorAll('[data-attachment-id]')].find(e=>e.querySelector('h4').textContent==='notes.txt').dataset.attachmentId")
        uploaded_item = f'[data-attachment-id="{uploaded_id}"]'
        await fill(uploaded_item + ' [name=name]', "Edited notes")
        await fill(uploaded_item + ' [name=description]', "Edited file metadata")
        await submit(uploaded_item + ' [data-attachment-form="metadata"]')
        await wait_for(f"document.querySelector({json.dumps(uploaded_item)}).dataset.revision==='2'")
        replacement = tmp_path / "replacement.txt"
        replacement.write_text("Replacement content")
        await file_input(uploaded_item + ' [data-attachment-form="replace"] input[type=file]', replacement)
        await submit(uploaded_item + ' [data-attachment-form="replace"]')
        await wait_for(f"document.querySelector({json.dumps(uploaded_item)}).dataset.revision==='3'")
        await page.evaluate(f"document.querySelector({json.dumps(uploaded_item + ' [data-attachment-edit-content]')}).click()")
        await wait_for(f"!document.querySelector({json.dumps(uploaded_item + ' [data-attachment-form=content]')}).hidden")
        await fill(uploaded_item + ' [name=content]', "Browser edited source")
        await submit(uploaded_item + ' [data-attachment-form="content"]')
        await wait_for(f"document.querySelector({json.dumps(uploaded_item)}).dataset.revision==='4'")
        await manager.open(scope, url=base + f"/work?card={card.id}")
        await wait_for("document.querySelectorAll('[data-attachment-id]').length===4")
        with httpx.Client(base_url=base) as client:
            current = client.get(f"/api/cards/{card.id}/materials").json()
            assert len(current["attachments"]) == 4
            assert client.get(f"/api/cards/{card.id}/attachments/{uploaded_id}").text == "Browser edited source"
            assert client.get(f"/api/cards/{card.id}/attachments/{uploaded_id}?revision=1").text == "User uploaded content"
        await page.evaluate("document.querySelector('[data-card-attachments]').scrollIntoView()")
        (evidence_dir / "card-attachments-desktop.png").write_bytes(await manager.screenshot(scope))
        await manager.resize(scope, width=390, height=844)
        assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        (evidence_dir / "card-attachments-mobile.png").write_bytes(await manager.screenshot(scope))
        # Follow the actual HTML browser preview, with scripts unable to run.
        html_url = base + f"/api/cards/{card.id}/attachments/{initial[0]['attachment_id']}/preview"
        await manager.open(scope, url=html_url)
        await wait_for("!!document.querySelector('iframe')")
        assert await page.evaluate("document.querySelector('iframe').getAttribute('sandbox')") == ""
        assert not await page.evaluate("!!window.unsafe")
        (evidence_dir / "card-attachment-html-preview.png").write_bytes(await manager.screenshot(scope))
        await manager.open(scope, url=base + f"/work?card={card.id}")
        await wait_for("document.querySelectorAll('[data-attachment-id]').length===4")
        for selector, remaining in [(link_item, 3), (uploaded_item, 2)]:
            await submit(selector + ' [data-attachment-form="remove"]')
            await wait_for(f"document.querySelectorAll('[data-attachment-id]').length==={remaining}")
        await manager.open(scope, url=base + f"/work?card={card.id}")
        await wait_for("document.querySelectorAll('[data-attachment-id]').length===2")
        (evidence_dir / "card-attachments-verification.json").write_text(json.dumps({"lifecycle": "passed", "reload": "passed", "lost_response_retry": "passed", "mobile": "passed", "html_sandbox": "passed", "remaining_originals": 2}, indent=2))
    finally:
        await manager.close()
        await browser.close()
