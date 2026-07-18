"""Image attachment validation and ACP prompt transport tests."""

from __future__ import annotations

import asyncio
import base64
import shutil
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from jinja2 import Environment, FileSystemLoader
from pydantic import ValidationError

from pa.acp.client import AgentConnection
from pa.instance.quiesce import ImageAttachment, QueuedPrompt
from pa.modules.agent_chat import PromptBody, session_prompt


def _image(name: str = "pixel.png") -> ImageAttachment:
    return ImageAttachment(
        name=name,
        mime_type="image/png",
        data=base64.b64encode(b"png bytes").decode(),
    )


class ImageAttachmentTests(unittest.TestCase):
    def test_rejects_unsupported_or_invalid_images(self) -> None:
        with self.assertRaises(ValidationError):
            ImageAttachment(name="vector.svg", mime_type="image/svg+xml", data="YWJj")
        with self.assertRaises(ValidationError):
            ImageAttachment(name="broken.png", mime_type="image/png", data="not base64")

    def test_prompt_can_contain_only_images(self) -> None:
        body = PromptBody(images=[_image()])
        queued = QueuedPrompt(message="", images=body.images)

        self.assertEqual(body.message, "")
        self.assertEqual(queued.public_dict()["images"], [
            {"name": "pixel.png", "mime_type": "image/png"}
        ])
        self.assertNotIn("data", queued.public_dict()["images"][0])


class AcpImagePromptTests(unittest.TestCase):
    def test_sends_text_and_image_content_blocks(self) -> None:
        store = MagicMock()
        connection = AgentConnection(MagicMock(), store)
        connection.session = MagicMock(
            id="pa-session",
            external_session_id="acp-session",
            status="idle",
            metrics_json={},
        )
        connection._conn = MagicMock()
        connection._conn.prompt = AsyncMock(
            return_value=SimpleNamespace(stop_reason="end_turn", usage=None)
        )

        async def run() -> None:
            with patch("pa.acp.client.capture_from_updates"):
                await connection.prompt("What is shown?", images=[_image()])

        asyncio.run(run())
        prompt = connection._conn.prompt.await_args.kwargs["prompt"]
        self.assertEqual([block.type for block in prompt], ["text", "image"])
        self.assertEqual(prompt[1].mime_type, "image/png")
        self.assertEqual(prompt[1].data, _image().data)


class ChatPromptEndpointTests(unittest.TestCase):
    def test_forwards_image_only_prompt_to_runtime(self) -> None:
        runtime = MagicMock()
        runtime.prompt = AsyncMock(return_value="started")
        runtime._queue = []

        async def run() -> dict:
            with (
                patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime),
                patch("pa.modules.agent_chat.get_principal_id", return_value="user:test"),
            ):
                return await session_prompt(
                    MagicMock(),
                    "session-1",
                    PromptBody(images=[_image()]),
                )

        result = asyncio.run(run())
        self.assertTrue(result["started"])
        runtime.prompt.assert_awaited_once()
        self.assertEqual(runtime.prompt.await_args.kwargs["images"][0].name, "pixel.png")


class ChatWidgetTemplateTests(unittest.TestCase):
    def test_shared_widget_exposes_drop_target_and_attach_control(self) -> None:
        template_root = Path(__file__).parents[1] / "src" / "pa" / "server" / "templates"
        env = Environment(loader=FileSystemLoader(template_root), autoescape=True)
        html = env.get_template("partials/agent/chat-widget.html").render()

        self.assertIn("data-acw-input", html)
        self.assertIn("drop images here", html)
        self.assertIn("data-acw-file-input", html)
        self.assertIn("data-acw-attach", html)
        self.assertIn("multiple hidden", html)
        self.assertIn("Agent settings…", html)
        self.assertIn("data-acw-settings-form", html)
        self.assertIn("data-acw-settings-apply disabled", html)
        self.assertIn("data-acw-settings-reset disabled", html)
        self.assertIn('role="status" aria-live="polite"', html)
        self.assertIn("data-acw-load-older-status", html)
        self.assertIn("Session…", html)
        self.assertIn("data-acw-toggle-system", html)
        self.assertIn("data-acw-toggle-raw", html)
        self.assertIn("data-acw-restart", html)
        self.assertIn("data-acw-end", html)
        self.assertIn("data-acw-stop", html)
        self.assertIn("disabled>Stop", html)
        self.assertNotIn("data-acw-provider", html)
        self.assertIn("data-acw-tool-toggle", html)
        self.assertIn("data-acw-tool-flyout", html)
        self.assertIn("data-acw-plan-toggle", html)
        self.assertIn("data-acw-plan-flyout", html)
        self.assertIn('data-api-base="/api/agent"', html)
        self.assertIn('data-auto-start="1"', html)

    @unittest.skipUnless(shutil.which("node"), "node is required for chat UI behavior tests")
    def test_transcript_dedup_scroll_follow_and_prepend_anchor(self) -> None:
        script_path = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "static"
            / "js"
            / "agent-chat.js"
        )
        program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
global.window = {};
global.document = {
  addEventListener: function () {},
  querySelector: function () { return null; },
  querySelectorAll: function () { return []; },
  body: null,
};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));

const Widget = window.PAAgentChat.AgentChatWidget;
const widget = Object.create(Widget.prototype);
widget.seenEvents = {};
widget.transcriptEvents = [];
widget.lastSeq = 0;
let bubbles = 0;
let scrolls = 0;
widget.addBubble = function () { bubbles += 1; };
widget.scrollToBottom = function () { scrolls += 1; };
widget.isNearBottom = function () { return false; };

widget.handleEvent({ seq: 7, type: "error", payload: { message: "once" } }, false);
widget.handleEvent({ seq: 7, type: "error", payload: { message: "duplicate" } }, false);
assert.strictEqual(bubbles, 1);
assert.strictEqual(widget.transcriptEvents.length, 1);
assert.strictEqual(scrolls, 0);

widget.isNearBottom = function () { return true; };
widget.handleEvent({ seq: 8, type: "error", payload: { message: "follow" } }, false);
assert.strictEqual(scrolls, 1);
assert.strictEqual(window.PAAgentChat.anchoredScrollTop(75, 400, 650), 325);
"""

        subprocess.run(
            [shutil.which("node"), "-e", program, str(script_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    @unittest.skipUnless(shutil.which("node"), "node is required for chat UI behavior tests")
    def test_user_markdown_is_sanitized_and_preserves_supported_formatting(self) -> None:
        script_path = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "static"
            / "js"
            / "agent-chat.js"
        )
        program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
global.window = {
  marked: { parse: function (raw, options) {
    assert.strictEqual(options.breaks, true);
    assert.ok(raw.includes("**bold**"));
    return '<p>line<br>two <strong>bold</strong> <em>em</em> <code>x</code></p>' +
      '<ul><li>item</li></ul><pre><code>block</code></pre>' +
      '<a href="javascript:alert(1)" onclick="alert(1)">bad</a><script>alert(1)</script>';
  } },
  DOMPurify: { sanitize: function (html, config) {
    assert.ok(config.FORBID_TAGS.includes("form"));
    assert.ok(config.FORBID_ATTR.includes("style"));
    return html.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
      .replace(/\s+onclick="[^"]*"/gi, "")
      .replace(/javascript:[^"]*/gi, "");
  } }
};
global.document = {
  addEventListener: function () {}, querySelector: function () { return null; },
  querySelectorAll: function () { return []; }, body: null
};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const html = window.PAAgentChat.renderMarkdown("line\ntwo **bold** *em* `x`\n\n- item\n\n```\nblock\n```");
assert.ok(html.includes("<br>"));
assert.ok(html.includes("<strong>bold</strong>"));
assert.ok(html.includes("<em>em</em>"));
assert.ok(html.includes("<ul>"));
assert.ok(html.includes("<pre><code>"));
assert.ok(!html.includes("<script"));
assert.ok(!html.includes("onclick"));
assert.ok(!html.includes("javascript:"));
"""
        subprocess.run(
            [shutil.which("node"), "-e", program, str(script_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    @unittest.skipUnless(shutil.which("node"), "node is required for chat UI behavior tests")
    def test_older_paging_retries_exhausts_and_keeps_concurrent_live_events(self) -> None:
        script_path = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "static"
            / "js"
            / "agent-chat.js"
        )
        program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
global.window = {};
global.document = {
  addEventListener: function () {}, querySelector: function () { return null; },
  querySelectorAll: function () { return []; }, body: null
};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const Widget = window.PAAgentChat.AgentChatWidget;
const widget = Object.create(Widget.prototype);
widget.sessionId = "session-1";
widget.hasOlder = true;
widget.olderCursor = 30;
widget.olderError = "";
widget.loadingOlder = false;
widget.prompting = false;
widget.turnStartedAt = null;
widget.transcriptEvents = [{ seq: 30 }, { seq: 31 }];
widget.els = {
  messages: { scrollHeight: 100, scrollTop: 25 },
  loadOlder: { hidden: false, disabled: false, textContent: "", setAttribute: function () {} },
  loadOlderStatus: { hidden: true, textContent: "" },
  status: { dataset: { state: "online" } }
};
widget.setTurnActive = function () {};
widget.setStatus = function () {};
let renderCalls = 0;
widget.renderTranscript = function (events) {
  renderCalls += 1;
  this.transcriptEvents = events;
  this.els.messages.scrollHeight = 140;
};
let rejectRequest;
widget.api = function (path) {
  assert.ok(path.includes("before_seq=30"));
  return new Promise(function (_, reject) { rejectRequest = reject; });
};
widget.loadOlderTranscript();
assert.strictEqual(widget.loadingOlder, true);
rejectRequest(new Error("offline"));
setImmediate(function () {
  assert.strictEqual(widget.loadingOlder, false);
  assert.ok(widget.olderError.includes("offline"));
  assert.strictEqual(widget.els.loadOlder.textContent, "Retry loading older messages");
  widget.api = function () {
    return Promise.resolve({ events: [{ seq: 10 }, { seq: 20 }], page: {
      has_older: false, oldest_seq: 10, next_before_seq: null
    } });
  };
  widget.transcriptEvents.push({ seq: 32 }); // concurrent SSE arrival
  widget.loadOlderTranscript();
  setImmediate(function () {
    assert.strictEqual(renderCalls, 1);
    assert.deepStrictEqual(widget.transcriptEvents.map(function (e) { return e.seq; }), [10, 20, 30, 31, 32]);
    assert.strictEqual(widget.hasOlder, false);
    assert.strictEqual(widget.els.loadOlder.hidden, true);
    assert.strictEqual(widget.els.messages.scrollTop, 65);
  });
});
"""
        subprocess.run(
            [shutil.which("node"), "-e", program, str(script_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    def test_agent_page_starts_new_sessions_from_a_configuration_dialog(self) -> None:
        template_root = Path(__file__).parents[1] / "src" / "pa" / "server" / "templates"
        source = (template_root / "pages" / "agent.html").read_text()

        self.assertIn("data-agent-new-dialog", source)
        self.assertIn('name="provider"', source)
        self.assertIn('name="model_id"', source)
        self.assertIn('name="mode_id"', source)
        self.assertIn('name="effort"', source)
        self.assertIn("data-agent-new-model", source)
        self.assertIn("data-agent-new-mode", source)
        self.assertIn("data-agent-new-effort", source)
        self.assertIn("data-agent-new-related", source)
        self.assertNotIn("<datalist", source)
        self.assertIn('name="cwd"', source)

        script = (template_root.parent / "static" / "js" / "agent-chat.js").read_text()
        self.assertIn('role === "user" || role === "agent"', script)
        self.assertIn('!child.hasAttribute("data-acw-load-older-status")', script)
        self.assertIn("newSessionSnapshotForProvider", script)
        self.assertIn('provider.addEventListener("change"', script)
        self.assertIn("populateSelect", script)
        self.assertIn("markSettingsDirty", script)
        self.assertIn("applySettings", script)
        self.assertIn('const modelId = this.els.model.value;', script)
        self.assertIn('const modeId = this.els.mode.value;', script)
        self.assertIn('errors.push(error)', script)
        self.assertIn('this.settingsPending = pending;', script)
        self.assertIn('if (this.settingsPending)', script)
        self.assertIn('No changes to apply.', script)
        self.assertIn("Discard unsaved Agent settings changes", script)
        self.assertIn("sessionConfigSummary", script)
        self.assertGreaterEqual(script.count("self.applyOptionSnapshot(snap);"), 2)

    def test_fleet_page_exposes_remote_operations_console(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server"
        template = (root / "templates" / "pages" / "fleet.html").read_text()
        fleet_script = (root / "static" / "js" / "fleet.js").read_text()
        chat_script = (root / "static" / "js" / "agent-chat.js").read_text()

        self.assertIn("Remote operations", template)
        self.assertIn("pa-remote-start-form", template)
        self.assertIn("pa-remote-session-list", template)
        self.assertIn("pa-remote-history-list", template)
        self.assertIn("auto_start=false", template)
        self.assertIn("watchRemoteSessions", fleet_script)
        self.assertIn("new Notification", fleet_script)
        self.assertIn("var selectedProvider = select.value;", fleet_script)
        self.assertIn("select.value = selectedProvider;", fleet_script)
        self.assertIn("function remoteNotificationsActive()", fleet_script)
        self.assertIn("function handleRemoteOperationsHidden()", fleet_script)
        self.assertIn("function refreshRemoteWatchers(instanceId)", fleet_script)
        self.assertIn("var generation = ++remoteLoadGeneration;", fleet_script)
        self.assertIn("instanceId !== remoteInstanceId", fleet_script)
        self.assertIn("var dispatchInstanceId = remoteInstanceId;", fleet_script)
        self.assertIn("loadOlderRemoteAudit", fleet_script)
        self.assertIn("data-remote-audit-older", fleet_script)
        self.assertNotIn("setTimeout(loadRemoteOperations", fleet_script)
        self.assertIn("setApiBase", chat_script)


if __name__ == "__main__":
    unittest.main()
