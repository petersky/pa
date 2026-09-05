"""Managed-browser coverage for the actionable notification panel."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
import threading
import unittest
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager

ROOT = Path(__file__).parents[1]


def _interaction(state: str = "outstanding", **updates) -> dict:
    value = {
        "request_id": f"request-{state}",
        "kind": "acp_elicitation",
        "state": state,
        "prompt": "Choose the next step",
        "response_schema": None,
        "choices": [],
        "allow_freeform": False,
        "allow_cancel": True,
        "sensitive": False,
        "continuation_mode": "protocol",
    }
    value.update(updates)
    return value


def _notice(identifier: str, interaction: dict, **updates) -> dict:
    value = {
        "id": identifier,
        "realm_id": "default",
        "type": "interaction",
        "priority": "high",
        "title": "Response required: Choose the next step",
        "summary": "Readable summary.",
        "body": "## Full request\n\n- first\n- second\n\n`code` and [link](https://example.com)",
        "updated_at": "2026-09-05T19:00:00Z",
        "read_at": None,
        "acknowledged_at": None,
        "resolved_at": None,
        "interaction": interaction,
        "routing": {"destination": "/agent?session=session-exact", "response_mode": "local"},
        "context": {
            "project": {"id": "project-exact", "label": "PA Core"},
            "card": {"id": "card-exact", "label": "Readable notifications"},
            "session": {"id": "session-exact", "label": "Notification worker"},
            "dispatch": {"id": "dispatch-exact", "label": "Agent dispatch"},
            "owner": {"id": "owner-exact", "label": "MacBook"},
        },
        "presentation": {
            "category": "request",
            "status": "Response required",
            "required_action": "Choose one of the available responses",
            "next_effect": "Submitting sends the response to the waiting agent request.",
        },
    }
    value.update(updates)
    return value


def _payload() -> dict:
    long_body = (
        "# Long Markdown\n\n<script>window.xss = true</script>\n\n"
        + "- readable list item\n" * 250
        + "END-OF-FULL-REQUEST"
    )
    return {
        "outstanding_count": 5,
        "next_offset": None,
        "items": [
            _notice(
                "choice",
                _interaction(
                    choices=[
                        {"id": "approve", "label": "Approve", "description": "Resume work"},
                        {"id": "decline", "label": "Decline", "description": "Do not proceed"},
                    ]
                ),
                body=long_body,
                title='Response required: choose safely "><img src=x onerror="window.xss=true">',
            ),
            _notice(
                "fields",
                _interaction(
                    response_schema={
                        "type": "object",
                        "required": ["environment"],
                        "properties": {
                            "environment": {
                                "type": "string",
                                "title": "Environment",
                                "description": "Deployment target",
                                "minLength": 3,
                            }
                        },
                    },
                    sensitive=True,
                ),
            ),
            _notice("freeform", _interaction(allow_freeform=True, choices=[])),
            _notice("failed", _interaction("failed")),
            _notice("expired", _interaction("expired"), resolved_at="2026-09-05T19:01:00Z"),
            _notice("superseded", _interaction("superseded"), resolved_at="2026-09-05T19:02:00Z"),
            _notice(
                "remote",
                _interaction(allow_freeform=True),
                routing={"destination": "https://owner.example/agent?session=exact", "response_mode": "remote"},
            ),
        ],
    }


@unittest.skipIf(
    os.environ.get("CI"),
    "Managed-Chromium layout coverage is local-only; CI Firecracker may hang Chrome",
)
@unittest.skipUnless(_browser_executable(), "Chrome/Chromium is required")
class NotificationPanelManagedBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        shutil.copytree(ROOT / "src/pa/server/static", root / "static")
        payload = json.dumps(_payload()).replace("</", "<\\/")
        (root / "index.html").write_text(
            """<!doctype html><html><head><meta charset="utf-8">
<meta name="csrf-token" content="test"><link rel="stylesheet" href="/static/style.css">
<style>body{min-height:100vh;padding:1rem;background:var(--pa-bg)}.chrome-actions{justify-content:flex-end}</style>
<script>
window.__notificationPayload = __PAYLOAD__;
window.fetch = function () { return Promise.resolve(new Response(JSON.stringify(window.__notificationPayload), {status: 200, headers: {'Content-Type':'application/json'}})); };
window.PAAgentChat = {renderMarkdownAsync: function (text) { return Promise.resolve('<p>' + text.replace(/\\n/g, '<br>') + '</p>'); }};
</script><script src="/static/js/notifications.js" defer></script></head><body>
<div class="chrome-actions"><div class="notification-chrome" data-notification-chrome>
<button type="button" id="pa-notification-bell" class="icon-btn notification-bell" aria-expanded="false"><span class="notification-badge" data-notification-count hidden></span>Notifications</button>
<section id="pa-notification-panel" class="notification-panel" tabindex="-1" hidden>
<header><strong>Notifications</strong><span data-notification-status></span><button data-notification-close>Close</button></header>
<div class="notification-filters"><button data-notification-filter="outstanding">Outstanding</button><button data-notification-filter="unread">Unread</button></div>
<div class="notification-list" data-notification-list></div><button data-notification-more hidden>More</button>
</section></div></div></body></html>""".replace("__PAYLOAD__", payload),
            encoding="utf-8",
        )
        handler = partial(SimpleHTTPRequestHandler, directory=self.tmp.name)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.browser = BrowserManager(root / "browser")
        self.manager = BrowserSessionManager(
            self.browser, instance_id="notification-browser", idle_ttl_seconds=60
        )
        self.scope = BrowserScope(
            "user:notification", "session-notification", "notification-browser"
        )
        await self.manager.attach(
            self.scope,
            url=f"http://127.0.0.1:{self.httpd.server_port}/index.html",
            width=1440,
            height=1000,
        )
        session = self.manager.resolve(self.scope)
        for _ in range(30):
            if await session.page.evaluate(
                "document.querySelectorAll('[data-notification-id]').length === 7"
            ):
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("Notifications did not render")

    async def asyncTearDown(self) -> None:
        await self.manager.close()
        await self.browser.close()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)
        self.tmp.cleanup()

    async def test_contract_states_full_content_and_responsive_layout(self) -> None:
        session = self.manager.resolve(self.scope)
        await session.page.evaluate(
            """(() => {
              document.querySelector('#pa-notification-panel').hidden = false;
              document.querySelector('[data-notification-id=choice] .notification-full').open = true;
            })()"""
        )
        desktop = await session.page.evaluate(
            """(() => ({
              choices: document.querySelectorAll('[data-notification-id=choice] [data-notification-choice]').length,
              fields: document.querySelectorAll('[data-notification-id=fields] [data-notification-field]').length,
              freeform: document.querySelectorAll('[data-notification-id=freeform] [data-notification-send]').length,
              retry: document.querySelectorAll('[data-notification-id=failed] [data-notification-retry]').length,
              expiredActions: document.querySelectorAll('[data-notification-id=expired] .notification-actions').length,
              supersededActions: document.querySelectorAll('[data-notification-id=superseded] .notification-actions').length,
              remoteWarning: document.querySelector('[data-notification-id=remote] .notification-warning').textContent,
              longTail: document.querySelector('[data-notification-id=choice]').textContent.includes('END-OF-FULL-REQUEST'),
              rawScriptNodes: document.querySelectorAll('[data-notification-id=choice] script').length,
              injectedImages: document.querySelectorAll('[data-notification-id=choice] img').length,
              xssExecuted: window.xss === true,
              exactId: document.querySelector('[data-notification-id=choice] .notification-identifiers').textContent.includes('session-exact'),
              noOverflow: document.documentElement.scrollWidth <= innerWidth
            }))()"""
        )
        self.assertEqual(desktop["choices"], 2)
        self.assertEqual(desktop["fields"], 1)
        self.assertEqual(desktop["freeform"], 1)
        self.assertEqual(desktop["retry"], 1)
        self.assertEqual(desktop["expiredActions"], 0)
        self.assertEqual(desktop["supersededActions"], 0)
        self.assertIn("owning instance", desktop["remoteWarning"])
        self.assertTrue(desktop["longTail"])
        self.assertEqual(desktop["rawScriptNodes"], 0)
        self.assertEqual(desktop["injectedImages"], 0)
        self.assertFalse(desktop["xssExecuted"])
        self.assertTrue(desktop["exactId"])
        self.assertTrue(desktop["noOverflow"])
        screenshot_path = os.environ.get("PA_NOTIFICATION_BROWSER_SCREENSHOT")
        if screenshot_path:
            Path(screenshot_path).write_bytes(await self.manager.screenshot(self.scope))

        await self.manager.resize(self.scope, width=390, height=844)
        narrow = await session.page.evaluate(
            """({
              width: innerWidth,
              page: document.documentElement.scrollWidth <= innerWidth,
              items: Array.from(document.querySelectorAll('[data-notification-id]')).every(item => item.scrollWidth <= item.clientWidth)
            })"""
        )
        self.assertEqual(narrow["width"], 390)
        self.assertTrue(narrow["page"])
        self.assertTrue(narrow["items"])
