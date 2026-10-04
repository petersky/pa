"""Real HTMX status controls against an isolated running PA and Chromium."""
from __future__ import annotations

import asyncio
import json
import socket
import threading
import time

import httpx
import pytest
import uvicorn

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.test_local_operator_acceptance import operator_app, make_card


@pytest.fixture
def operator_server(operator_app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(operator_app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 20
    while not server.started:
        assert thread.is_alive() and time.monotonic() < deadline
        time.sleep(.02)
    try:
        yield operator_app, f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=20)
        sock.close()
        assert not thread.is_alive()


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_real_status_form_errors_restore_and_authenticated_done_reloads(operator_server, tmp_path):
    app, base = operator_server
    card = make_card(app)
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="operator-test")
    scope = BrowserScope("user:test", "operator-browser", "operator-test")
    lane = '[data-card-status-form] select[name="lane"]'
    error = '[data-card-status-error]'
    try:
        await manager.attach(scope, url=f"{base}/?card={card.id}", width=1280, height=900)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition did not become true: " + expression)

        async def choose(value):
            await page.evaluate(f'''(() => {{const s=document.querySelector({json.dumps(lane)});
              s.value={json.dumps(value)};s.dispatchEvent(new Event('change',{{bubbles:true}}));}})()''')

        await wait_for(f"!!document.querySelector({json.dumps(lane)})")
        await choose("done")
        await wait_for(f"document.querySelector({json.dumps(error)}).textContent.includes('Current status: active')")
        assert await page.evaluate(f"document.querySelector({json.dumps(lane)}).value") == "active"
        assert not await page.evaluate(f"document.querySelector({json.dumps(lane)}).disabled")
        message = await page.evaluate(f"document.querySelector({json.dumps(error)}).textContent")
        assert "pa user set-password local" in message and "[object Object]" not in message

        # A real failed XHR to an unreachable port; the authoritative GET still works.
        await page.evaluate('''window.originalOpen=XMLHttpRequest.prototype.open;
          XMLHttpRequest.prototype.open=function(method,url,...rest) {
            return originalOpen.call(this,method,method==='POST' && url.includes('/partials/cards/') ? 'http://127.0.0.1:1/unreachable' : url,...rest);
          };''')
        await choose("waiting")
        await wait_for(f"document.querySelector({json.dumps(error)}).textContent.includes('Connection failed') && !document.querySelector({json.dumps(lane)}).disabled")
        assert await page.evaluate(f"document.querySelector({json.dumps(lane)}).value") == "active"
        await page.evaluate("XMLHttpRequest.prototype.open=originalOpen")

        # The displayed snapshot is stale; a normal API update changes the authority.
        with httpx.Client(base_url=base) as client:
            client.get("/api/health")
            response = client.patch(f"/api/cards/{card.id}", json={"lane": "waiting", "expected_version": card.updated_at.isoformat()},
                headers={"X-CSRF-Token": client.cookies.get("pa_csrf"), "Idempotency-Key": "browser-stale"})
            assert response.status_code == 200, response.text
        await choose("done")
        await wait_for(f"document.querySelector({json.dumps(error)}).textContent.includes('Current status: waiting')")
        assert await page.evaluate(f"document.querySelector({json.dumps(lane)}).value") == "waiting"
        assert await page.evaluate(f"document.querySelector({json.dumps(lane)}).disabled")
        assert "card changed" in await page.evaluate(f"document.querySelector({json.dumps(error)}).textContent")
        await page.evaluate(f"document.querySelector({json.dumps(error + ' button')}).click()")
        await wait_for(f"!!document.querySelector({json.dumps(lane)}) && !document.querySelector({json.dumps(lane)}).disabled")

        # Both write and subsequent read fail: retain the last confirmed lane and
        # prevent a second mutation until the operator can reload.
        await page.evaluate('''window.originalFetch=window.fetch;
          window.fetch=function(url,...args) {
            if (String(url).includes('/api/cards/')) return Promise.reject(new TypeError('offline'));
            return originalFetch.call(this,url,...args);
          };
          XMLHttpRequest.prototype.open=function(method,url,...rest) {
            return originalOpen.call(this,method,method==='POST' && url.includes('/partials/cards/') ? 'http://127.0.0.1:1/unreachable' : url,...rest);
          };''')
        await choose("done")
        await wait_for(f"document.querySelector({json.dumps(error)}).textContent.includes('could not be verified')")
        assert await page.evaluate(f"document.querySelector({json.dumps(lane)}).value") == "waiting"
        assert await page.evaluate(f"document.querySelector({json.dumps(lane)}).disabled")
        await page.evaluate("XMLHttpRequest.prototype.open=originalOpen;window.fetch=originalFetch")

        # Configure through the same server API as `pa user set-password`, then
        # submit the real login form. No synthesized authenticated cookies.
        user = app.state.ctx.require_service("users").get("local")
        with httpx.Client(base_url=base) as client:
            response = client.post("/api/auth/users/local/password", json={"password": "browser-operator-password"},
                                   headers={"Authorization": "Bearer " + user.cli_token})
            assert response.status_code == 200
        await page.navigate_and_wait(base + "/login")
        await manager.type_text(scope, selector='input[name="username"]', text="local")
        await manager.type_text(scope, selector='input[name="password"]', text="browser-operator-password")
        await manager.click(scope, selector='button[type="submit"]')
        await wait_for("location.pathname === '/' && !!window.htmx")
        await page.navigate_and_wait(f"{base}/?card={card.id}")
        await wait_for(f"!!document.querySelector({json.dumps(lane)})")
        await choose("done")
        await wait_for("document.querySelector('[data-card-detail]')?.dataset.cardLane === 'done'")
        await page.navigate_and_wait(f"{base}/?card={card.id}")
        await wait_for(f"document.querySelector({json.dumps(lane)})?.value === 'done'")
        result = app.state.ctx.store.get_card(card.id)
        human = [r for r in result.completion_evidence if r.outcome == "human_override"]
        assert len(human) == 1 and human[0].actor_kind == "human" and human[0].actor == "user:local"
        assert result.completion_requirement == card.completion_requirement
        assert result.completion_evidence[:-1] == card.completion_evidence
    finally:
        await manager.close()
        await browser.close()
