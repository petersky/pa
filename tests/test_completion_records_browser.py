"""Real card/result navigation, retained evidence and administrative endings."""
import asyncio
import time

import httpx
import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import CardCreate, CardUpdate, TranscriptEvent
from tests.test_card_status_browser import operator_server
from tests.test_local_operator_acceptance import operator_app
from tests.test_completion_records import envelope, bound_headers


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_actual_card_lists_revisions_after_worker_closes(operator_server, tmp_path):
    app, base = operator_server
    store = app.state.ctx.store
    card = store.create_card(CardCreate(title="Durable assessment", realm_id="research", lane="active"))
    user = app.state.ctx.require_service("users").get("local")
    with httpx.Client(base_url=base, headers={"Authorization": "Bearer " + user.cli_token}) as client:
        evidence = client.post(f"/api/cards/{card.id}/attachments?realm=research", headers={"Idempotency-Key": "browser-evidence"},
                               files={"file": ("report.txt", b"durable evidence after session closure", "text/plain")}).json()
        headers = bound_headers(app, card, "result-worker", "result-dispatch")
        # The helper defaults the dispatch realm; use the same actual card realm.
        ledger = app.state.ctx.require_service("dispatch_store")
        record = ledger.get("result-dispatch")
        record.realm_id = "research"
        ledger.put(record)
        first = client.post(f"/api/cards/{card.id}/completion-records?realm=research", json=envelope(references=[{
            "kind": "attachment", "label": "Read evidence", "attachment_id": evidence["attachment_id"], "sha256": evidence["sha256"]}]),
            headers={**headers, "Idempotency-Key": "browser-audit"}).json()
        assert first.get("record_id"), first
        second = client.post(f"/api/cards/{card.id}/completion-records?realm=research", json=envelope(title="Design handoff",
            supersedes_record_id=first["record_id"], sections=[{"heading": "Requirements and next steps", "content": "Review **before** implementation."}]),
            headers={**headers, "Idempotency-Key": "browser-design"}).json()
        assert second.get("record_id"), second
        assert client.delete(f"/api/cards/{card.id}/attachments/{evidence['attachment_id']}?realm=research").status_code == 200

    # A later administrative update must never replace these result entry points.
    store.update_card(card.id, CardUpdate(body="Administrative reconciliation completed."), realm_id="research")
    store.append_transcript_events([TranscriptEvent(session_id="result-worker", seq=1, event_type="card_disposition",
        payload={"contract": {"contract": "pa.card-disposition/v1", "lane": "waiting", "outcome": "Administrative reconciliation completed.",
                              "evidence": {"integration_required": False}}, "status": "applied", "authority_acknowledged": True})])
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="results-test")
    scope = BrowserScope("user:local", "results-test", "results-test")
    try:
        await manager.attach(scope, url=f"{base}/work?card={card.id}&realm=research", width=1280, height=1000)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail(expression + "\n" + str(await page.evaluate("document.body.innerText")))

        await wait_for("!!document.querySelector('[data-card-results]')")
        assert "Results (2)" in await page.evaluate("document.querySelector('[data-card-results]').innerText")
        await page.evaluate("document.querySelector('[data-card-results] h3 a').focus()")
        assert await page.evaluate("document.activeElement.matches('[data-card-results] h3 a')")
        await manager.press(scope, key="Enter")
        await wait_for("document.querySelectorAll('[data-result-id]').length === 2")
        assert await page.evaluate("[...document.querySelectorAll('[data-result-id]')].map(e=>e.dataset.resultId)") == [second["record_id"], first["record_id"]]
        await manager.click(scope, selector=f"[data-result-id='{first['record_id']}'] h2 a")
        await wait_for("!!document.querySelector('.result-record strong')")
        body = await page.evaluate("document.body.innerText")
        assert "result-worker" in body and "result-dispatch" in body
        assert "Validation and limits" in body and "Superseded by" in body
        assert "original evidence retained" in body and "No independently recorded acceptance receipt" in body
        # Follow the actual rendered evidence URL after the originating worker closed.
        url = await page.evaluate("[...document.querySelectorAll('a')].find(a=>a.textContent==='Read evidence').href")
        with httpx.Client() as client:
            assert client.get(url).content == b"durable evidence after session closure"
        await manager.open(scope, url=base + f"/results?card_id={card.id}&record={first['record_id']}")
        await wait_for("!!document.querySelector('.result-record strong')")
        await manager.click(scope, selector="aside[aria-label='Later revisions'] a")
        await wait_for("document.querySelector('[data-result-id]')?.dataset.resultId === '" + second["record_id"] + "'")
        assert "Requirements and next steps" in await page.evaluate("document.body.innerText")
        # Stable direct URLs and mobile layout both retain the complete report.
        await manager.resize(scope, width=390, height=844)
        assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        (tmp_path / "completion-results.png").write_bytes(await manager.screenshot(scope))
        empty = store.create_card(CardCreate(title="Legacy output"))
        await manager.open(scope, url=base + f"/results?card_id={empty.id}")
        await wait_for("document.body.innerText.includes('No structured results recorded')")
        assert "sessions and outputs" in await page.evaluate("document.body.innerText")
    finally:
        await manager.close()
        await browser.close()
