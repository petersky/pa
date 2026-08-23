"""Image attachment validation and ACP prompt transport tests."""

from __future__ import annotations

import asyncio
import base64
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
from jinja2 import Environment, FileSystemLoader
from pydantic import ValidationError

from pa.acp.client import AgentConnection
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.execution.dispatch import DispatchRecord, DispatchStore
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
        self.assertEqual(
            queued.public_dict()["images"],
            [{"name": "pixel.png", "mime_type": "image/png"}],
        )
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
                patch(
                    "pa.modules.agent_chat.get_principal_id", return_value="user:test"
                ),
            ):
                return await session_prompt(
                    MagicMock(),
                    "session-1",
                    PromptBody(images=[_image()]),
                )

        result = asyncio.run(run())
        self.assertTrue(result["started"])
        runtime.prompt.assert_awaited_once()
        self.assertEqual(
            runtime.prompt.await_args.kwargs["images"][0].name, "pixel.png"
        )

    def test_duplicate_dispatch_delivery_returns_original_ack_without_requeue(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    request_payload={"message": "work"},
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="running",
                    prompt_ack={
                        "event_type": "queue_enqueued",
                        "event_id": "event-1",
                        "event_seq": 7,
                        "prompt_id": "prompt-1",
                    },
                )
            )
            runtime = MagicMock()
            runtime.prompt = AsyncMock()
            runtime._queue = []
            request = MagicMock()
            request.app.state.ctx.services = {"dispatch_store": ledger}

            async def run() -> dict:
                with patch(
                    "pa.modules.agent_chat._runtime_or_404", return_value=runtime
                ):
                    return await session_prompt(
                        request,
                        "session-1",
                        PromptBody(message="work", dispatch_id="dispatch-1"),
                    )

            result = asyncio.run(run())
            self.assertTrue(result["accepted"])
            self.assertTrue(result["duplicate"])
            self.assertEqual(result["prompt_id"], "prompt-1")
            runtime.prompt.assert_not_awaited()


class ChatWidgetTemplateTests(unittest.TestCase):
    def test_activity_rail_icons_remain_visible_across_interaction_states(
        self,
    ) -> None:
        style_path = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "static"
            / "style.css"
        )
        style = style_path.read_text()
        interaction_states = (
            ":hover:not(:disabled), :focus, :focus-visible, "
            '[aria-expanded="true"]'
        )

        self.assertIn(
            f".acw-rail-button:is({interaction_states}) {{\n"
            "  color: var(--pa-bg);\n"
            "  background: var(--pa-accent);\n"
            "}",
            style,
        )
        self.assertIn(
            ".acw-rail-button.is-active .acw-gears {\n"
            "  color: var(--pa-accent);\n"
            "}",
            style,
        )
        self.assertIn(
            f".acw-rail-button.is-active:is({interaction_states}) "
            ".acw-gears {\n"
            "  color: var(--pa-bg);\n"
            "}",
            style,
        )
        self.assertIn(
            ".acw-rail-button.is-active .acw-gear-a,\n"
            ".acw-rail-button.is-active .acw-gear-c {\n"
            "  animation: acw-gear-clockwise 1.2s linear infinite;",
            style,
        )
        self.assertIn("@media (prefers-reduced-motion: reduce)", style)

    def test_shared_widget_exposes_drop_target_and_attach_control(self) -> None:
        template_root = (
            Path(__file__).parents[1] / "src" / "pa" / "server" / "templates"
        )
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
        self.assertIn("data-acw-recover", html)
        self.assertIn("Restart session", html)
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

    @unittest.skipUnless(
        shutil.which("node"), "node is required for chat UI behavior tests"
    )
    def test_tool_ids_with_newlines_do_not_become_css_selectors(self) -> None:
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
global.document = { addEventListener: function () {}, querySelector: function () { return null; }, querySelectorAll: function () { return []; }, body: null };
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const Widget = window.PAAgentChat.AgentChatWidget;
const widget = Object.create(Widget.prototype);
const id = "23\nfc_opaque";
const existing = { dataset: { toolId: id }, querySelector: function () { return null; } };
widget.els = {
  toolActivity: {
    querySelectorAll: function (selector) {
      assert.strictEqual(selector, "[data-tool-id]");
      return [existing];
    },
    querySelector: function () { throw new Error("raw tool id used as a selector"); }
  }
};
widget.activeToolIds = {};
widget.toolTimers = {};
widget.clearPlaceholder = function () {};
widget.upsertTool({ tool_call_id: id, status: "completed" });
"""
        subprocess.run(
            [shutil.which("node"), "-e", program, str(script_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    @unittest.skipUnless(
        shutil.which("node"), "node is required for chat UI behavior tests"
    )
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

    @unittest.skipUnless(
        shutil.which("node"), "node is required for chat UI behavior tests"
    )
    def test_user_markdown_is_sanitized_and_preserves_supported_formatting(
        self,
    ) -> None:
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
let lastSanitizeConfig = null;
global.window = {
  marked: { parse: function (raw, options) {
    assert.strictEqual(options.breaks, true);
    assert.ok(raw.includes("**bold**"));
    return '<p>line<br>two <strong>bold</strong> <em>em</em> <code>x</code></p>' +
      '<ul><li>item</li></ul><pre><code>block</code></pre>' +
      '<a href="javascript:alert(1)" onclick="alert(1)">bad</a><script>alert(1)</script>';
  } },
  DOMPurify: { sanitize: function (html, config) {
    lastSanitizeConfig = config;
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
window.PAAgentChat.renderMarkdown("**bold** embedded", { allowEmbeddedMedia: false });
["audio", "embed", "iframe", "object", "picture", "video"].forEach(function (tag) {
  assert.ok(lastSanitizeConfig.FORBID_TAGS.includes(tag));
});
assert.deepStrictEqual(lastSanitizeConfig.ADD_TAGS, []);
"""
        subprocess.run(
            [shutil.which("node"), "-e", program, str(script_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    @unittest.skipUnless(
        shutil.which("node"), "node is required for chat UI behavior tests"
    )
    def test_optimistic_user_bubble_dedupes_multiline_and_keeps_images(self) -> None:
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
  marked: { parse: function (raw) {
    return "<p>" + String(raw).replace(/\n/g, "<br>") + "</p>";
  } },
  DOMPurify: { sanitize: function (html) { return html; } }
};
global.document = {
  addEventListener: function () {},
  querySelector: function () { return null; },
  querySelectorAll: function () { return []; },
  body: null,
  createElement: function (tag) {
    const el = {
      tagName: String(tag).toUpperCase(),
      className: "",
      hidden: false,
      dataset: {},
      childNodes: [],
      children: [],
      style: {},
      textContent: "",
      innerHTML: "",
      src: "",
      alt: "",
      dateTime: "",
      appendChild: function (child) {
        this.childNodes.push(child);
        this.children.push(child);
        child.parentNode = this;
        return child;
      },
      insertBefore: function (child, _ref) {
        this.childNodes.unshift(child);
        this.children.unshift(child);
        child.parentNode = this;
        return child;
      },
      querySelector: function (sel) {
        if (sel === ".acw-message-images") {
          return this.children.find(function (c) { return c.className === "acw-message-images"; }) || null;
        }
        return null;
      },
      querySelectorAll: function () { return []; },
      remove: function () {
        if (!this.parentNode) return;
        const parent = this.parentNode;
        parent.childNodes = parent.childNodes.filter(function (c) { return c !== el; });
        parent.children = parent.children.filter(function (c) { return c !== el; });
        this.parentNode = null;
      },
      setAttribute: function () {},
    };
    Object.defineProperty(el, "textContent", {
      get: function () { return this._text || ""; },
      set: function (v) { this._text = String(v); this.innerHTML = ""; this.childNodes = []; this.children = []; },
      configurable: true,
    });
    Object.defineProperty(el, "innerHTML", {
      get: function () { return this._html || ""; },
      set: function (v) {
        this._html = String(v);
        // Simulate markdown replacing children (gallery would be wiped without preserve logic).
        this.childNodes = [];
        this.children = [];
        this._text = String(v).replace(/<[^>]+>/g, "");
      },
      configurable: true,
    });
    return el;
  },
};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const Widget = window.PAAgentChat.AgentChatWidget;
const widget = Object.create(Widget.prototype);
widget.rawText = false;
widget.showSystem = false;
widget.seenEvents = {};
widget.transcriptEvents = [];
widget.lastSeq = 0;
widget.streaming = {};
widget.els = {
  messages: {
    children: [],
    appendChild: function (child) { this.children.push(child); return child; },
    querySelectorAll: function (sel) {
      if (sel === ".acw-msg-user .acw-bubble") {
        return this.children
          .filter(function (row) { return row.className.indexOf("acw-msg-user") >= 0; })
          .map(function (row) { return row.children[0]; });
      }
      return [];
    },
  },
  placeholder: { hidden: false },
};
widget.clearPlaceholder = function () { this.els.placeholder.hidden = true; };
widget.isNearBottom = function () { return true; };
widget.scrollToBottom = function () {};
widget.setTurnActive = function () {};

const prompt = "line one\nline two";
widget.addBubble("user", prompt, new Date().toISOString(), {
  images: [{ name: "shot.png", mime_type: "image/png", preview: "data:image/png;base64,xx" }],
});
assert.strictEqual(widget.els.messages.children.length, 1);
const bubble = widget.els.messages.children[0].children[0];
assert.strictEqual(bubble.dataset.markdown, prompt);
assert.ok(bubble.querySelector(".acw-message-images"), "gallery should survive markdown render");
assert.strictEqual(bubble.querySelector(".acw-message-images").children[0].src, "data:image/png;base64,xx");

widget.handleEvent({
  seq: 42,
  type: "user_message",
  payload: { message: prompt, images: [{ name: "shot.png", mime_type: "image/png" }] },
  created_at: new Date().toISOString(),
}, false);
assert.strictEqual(widget.els.messages.children.length, 1, "SSE should not duplicate multiline optimistic bubble");
"""
        subprocess.run(
            [shutil.which("node"), "-e", program, str(script_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    @unittest.skipUnless(
        shutil.which("node"), "node is required for chat UI behavior tests"
    )
    def test_older_paging_retries_exhausts_and_keeps_concurrent_live_events(
        self,
    ) -> None:
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
let prependCalls = 0;
widget.prependTranscript = function (events) {
  prependCalls += 1;
  this.transcriptEvents = events.concat(this.transcriptEvents);
  this.els.messages.scrollHeight = 140;
  return events.length;
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
    assert.strictEqual(prependCalls, 1);
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

    @unittest.skipUnless(
        shutil.which("node"), "node is required for chat UI behavior tests"
    )
    def test_older_paging_is_incremental_busy_deduplicated_and_stale_safe(
        self,
    ) -> None:
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
function node(name, attrs) {
  return { name, attrs: attrs || {}, hasAttribute: function (key) { return !!this.attrs[key]; } };
}
global.window = {};
global.document = {
  addEventListener: function () {}, querySelector: function () { return null; },
  querySelectorAll: function () { return []; }, body: null,
  createDocumentFragment: function () { return { children: [], appendChild: function (child) { this.children.push(child); } }; }
};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const Widget = window.PAAgentChat.AgentChatWidget;
const widget = Object.create(Widget.prototype);
const control = node("control", { "data-acw-load-older": true });
const status = node("status", { "data-acw-load-older-status": true });
const placeholder = node("placeholder", { "data-acw-placeholder": true });
const existing = node("existing");
const children = [control, status, placeholder, existing];
const messages = {
  children, scrollHeight: 100, scrollTop: 20,
  appendChild: function (value) {
    if (value.children) this.children.push.apply(this.children, value.children);
    else this.children.push(value);
  },
  insertBefore: function (value, anchor) {
    const at = this.children.indexOf(anchor);
    this.children.splice.apply(this.children, [at, 0].concat(value.children));
  }
};
widget.els = {
  messages,
  loadOlder: { hidden: false, disabled: false, textContent: "", setAttribute: function () {} },
  loadOlderLabel: { textContent: "" }, historySpinner: { hidden: true },
  loadOlderStatus: { hidden: true, textContent: "" },
  status: { dataset: { state: "online" } }
};
widget.sessionId = "session-1"; widget.apiBase = "/api/agent";
widget.hasOlder = true; widget.olderCursor = 10001; widget.olderError = "";
widget.loadingOlder = false; widget.prompting = false; widget.turnStartedAt = null;
widget.seenEvents = {}; widget.lastSeq = 10001;
widget.transcriptEvents = Array.from({ length: 10000 }, function (_, index) { return { seq: index + 251 }; });
const activeTimer = { interval: 123 };
widget.toolTimers = { active: activeTimer };
widget.clearPlaceholder = function () {};
widget.setTurnActive = function () {}; widget.setStatus = function () {};
widget.handleEvent = function (event) {
  this.seenEvents["seq:" + event.seq] = true;
  this.els.messages.appendChild(node("event-" + event.seq));
  this.els.messages.scrollHeight += 1;
};
let resolveRequest; let requests = 0;
widget.api = function (path, options) {
  requests += 1;
  assert.ok(path.includes("limit=250"));
  assert.ok(options.signal);
  return new Promise(function (resolve) { resolveRequest = resolve; });
};
widget.loadOlderTranscript();
widget.loadOlderTranscript();
assert.strictEqual(requests, 1, "rapid double clicks share the in-flight request");
assert.strictEqual(widget.els.historySpinner.hidden, false);
assert.strictEqual(widget.els.loadOlderStatus.hidden, false);
assert.strictEqual(widget.els.loadOlderLabel.textContent, "Loading older messages…");
const page = Array.from({ length: 250 }, function (_, index) { return { seq: index + 1, type: "tool_call" }; });
page.push({ seq: 251, type: "tool_call" }); // duplicate with retained transcript
resolveRequest({ events: page, page: { has_older: false, oldest_seq: 1 }, diagnostics: { payload_bytes: 1234 } });
setImmediate(function () {
  assert.strictEqual(widget.transcriptEvents.length, 2000, "durable paging honors the browser retention bound");
  assert.strictEqual(widget.transcriptEvents[0].seq, 1);
  assert.strictEqual(widget.transcriptEvents[249].seq, 250);
  assert.strictEqual(widget.els.messages.children[252].name, "event-250");
  assert.strictEqual(widget.els.messages.children[253], existing, "existing DOM node is preserved");
  assert.strictEqual(widget.toolTimers.active, activeTimer, "active tool timer is preserved");
  assert.strictEqual(widget.els.historySpinner.hidden, true);

  widget.hasOlder = true; widget.olderCursor = 1; widget.loadingOlder = false;
  let staleResolve;
  widget.api = function () { return new Promise(function (resolve) { staleResolve = resolve; }); };
  widget.loadOlderTranscript();
  const staleController = widget.olderAbortController;
  widget.sessionId = "session-2";
  staleController.abort();
  staleResolve({ events: [{ seq: 0 }], page: { has_older: false } });
  setImmediate(function () {
    assert.strictEqual(widget.sessionId, "session-2");
    assert.strictEqual(widget.transcriptEvents[0].seq, 1, "stale session response is ignored");
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
        template_root = (
            Path(__file__).parents[1] / "src" / "pa" / "server" / "templates"
        )
        source = (template_root / "pages" / "agent.html").read_text()

        self.assertIn("data-agent-new-dialog", source)
        self.assertIn('name="provider"', source)
        self.assertIn("{% for provider in agent_providers | default([]) %}", source)
        self.assertIn("data-agent-new-status", source)
        self.assertIn("data-agent-new-model-spinner", source)
        self.assertIn("acw-spinner", source)
        self.assertIn('name="model_id"', source)
        self.assertIn('name="mode_id"', source)
        self.assertIn('name="effort"', source)
        self.assertIn("data-agent-new-model-provider", source)
        self.assertIn("data-agent-new-model", source)
        self.assertIn("data-agent-new-mode", source)
        self.assertIn("data-agent-new-effort", source)
        self.assertIn("data-agent-new-related", source)
        self.assertIn("model_provider", source)
        self.assertNotIn("<datalist", source)
        self.assertIn('name="cwd"', source)

        script = (template_root.parent / "static" / "js" / "agent-chat.js").read_text()
        self.assertIn('role === "user" || role === "agent"', script)
        self.assertIn('!child.hasAttribute("data-acw-load-older-status")', script)
        self.assertIn("newSessionSnapshotForProvider", script)
        self.assertIn("loadProviderCatalog", script)
        self.assertIn("readProviderCatalog", script)
        self.assertIn("setNewSessionBusy", script)
        self.assertIn("data-agent-new-model-spinner", script)
        self.assertIn("spinner.hidden = !busy", script)
        self.assertIn('csrfFetch("/providers/catalog")', script)
        self.assertIn('csrfFetch("/preferences")', script)
        self.assertNotIn('csrfFetch("/providers")', script)
        self.assertNotIn("item.available === false", script)
        shell = (template_root / "shell.html").read_text()
        self.assertIn('id="pa-provider-catalog"', shell)
        self.assertIn('provider.addEventListener("change"', script)
        self.assertIn('"requested: "', script)
        self.assertIn('"effective: "', script)
        self.assertIn("configuration", source)
        self.assertIn("populateSelect", script)
        self.assertIn("markSettingsDirty", script)
        self.assertIn("applySettings", script)
        self.assertIn("const modelId = this.els.model.value;", script)
        self.assertIn("const modeId = this.els.mode.value;", script)
        self.assertIn("errors.push(error)", script)
        self.assertIn("this.settingsPending = pending;", script)
        self.assertIn("if (this.settingsPending)", script)
        self.assertIn("No changes to apply.", script)
        self.assertIn("Discard unsaved Agent settings changes", script)
        self.assertIn("sessionConfigSummary", script)
        self.assertIn("sessionRuntimeLabel", script)
        self.assertIn("formatSessionModelId", script)
        self.assertIn("patchSessionListFromSnapshot", script)
        self.assertIn("refreshSessionList(self.sessionId, true)", script)
        self.assertIn('"live: "', script)
        self.assertGreaterEqual(script.count("self.applyOptionSnapshot(snap);"), 2)

    def test_agent_page_html_includes_registered_provider_options(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="test",
                agent_enabled=True,
            )
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                page = client.get("/agent")
                catalog = client.get("/api/agent/providers/catalog")
        self.assertEqual(page.status_code, 200)
        self.assertIn('value="cursor"', page.text)
        self.assertIn('value="codex"', page.text)
        self.assertIn('value="openinterpreter"', page.text)
        self.assertIn('id="pa-provider-catalog"', page.text)
        self.assertEqual(catalog.status_code, 200)
        self.assertEqual(
            {item["id"] for item in catalog.json()},
            {"cursor", "codex", "openinterpreter"},
        )

    def test_agent_page_defaults_to_collapsed_sessions_and_mobile_safe_composer(
        self,
    ) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server"
        page = (root / "templates" / "pages" / "agent.html").read_text()
        widget = (
            root / "templates" / "partials" / "agent" / "chat-widget.html"
        ).read_text()
        shell = (root / "templates" / "shell.html").read_text()
        script = (root / "static" / "js" / "agent-chat.js").read_text()
        style = (root / "static" / "style.css").read_text()

        self.assertIn("page-agent is-sidebar-collapsed", page)
        self.assertIn('aria-expanded="false">Show sessions', widget)
        self.assertIn('saved === null ? true : saved === "1"', script)
        self.assertIn('classList.toggle("is-sidebar-collapsed", collapsed)', script)
        self.assertIn("viewport-fit=cover", shell)
        self.assertIn("height: 100dvh", style)
        self.assertIn("padding-bottom: env(safe-area-inset-bottom, 0)", style)
        self.assertIn(".page-agent .page-main", style)
        self.assertIn(".page-agent-main .acw-chat-stage", style)
        self.assertIn("grid-template-rows: minmax(0, 1fr)", style)
        self.assertIn("max-height: min(10rem, 25dvh)", style)
        # Composer stays pinned: the flex/grid chain must shrink so only
        # .acw-messages scrolls, not the whole page-main region.
        def css_block(selector: str) -> str:
            needle = "\n" + selector + " {"
            self.assertIn(needle, style)
            return style.split(needle, 1)[1].split("}", 1)[0]

        self.assertIn("overflow: hidden", css_block(".page-agent .page-main"))
        self.assertIn("min-height: 0", css_block(".page-agent-main"))
        widget = css_block(".agent-chat-widget")
        self.assertIn("min-height: 0", widget)
        self.assertIn("overflow: hidden", widget)
        self.assertIn("min-height: 0", css_block(".acw-chat-stage"))
        self.assertIn("min-height: 0", css_block(".acw-messages"))

        # Pages that wrap page_layout in a history-boundary div must keep the
        # wrapper in the flex chain, or the layout collapses to content height
        # and the composer scrolls off-screen behind .app-view's clipping.
        boundary = css_block(".page-boundary")
        self.assertIn("display: flex", boundary)
        self.assertIn("flex: 1", boundary)
        self.assertIn("min-height: 0", boundary)
        self.assertIn('class="page-boundary"', page)
        for name in ("workshop.html", "fleet.html"):
            wrapped = (root / "templates" / "pages" / name).read_text()
            self.assertIn('class="page-boundary"', wrapped)

    def test_settings_page_exposes_durable_new_chat_defaults(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server"
        source = (root / "templates" / "pages" / "settings.html").read_text()

        self.assertIn("Instance defaults for new chats", source)
        self.assertIn("My overrides for new chats", source)
        self.assertIn('data-settings-defaults-scope="global"', source)
        self.assertIn('data-settings-defaults-scope="user"', source)
        self.assertIn("data-settings-default-provider", source)
        self.assertIn("data-settings-default-model", source)
        self.assertIn("data-settings-default-mode", source)
        self.assertIn("data-settings-default-effort", source)
        self.assertIn('surfaces["chat.default"]', source)
        self.assertIn("globalSurfaces", source)
        self.assertIn("/api/agent/provider-options/", source)

    def test_chat_links_open_externally_or_use_the_file_browser(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server"
        spa = (root / "static" / "js" / "spa.js").read_text()
        agent = (root / "static" / "js" / "agent-chat.js").read_text()
        shell = (root / "templates" / "shell.html").read_text()

        self.assertIn('link.target = "_blank"', spa)
        self.assertIn('link.rel = "noopener noreferrer"', spa)
        self.assertIn('browserLink.href = "/browse?"', spa)
        self.assertIn('direct = "file://"', spa)
        self.assertIn("window.PALinks.decorate(bubble)", agent)
        self.assertIn("renderMarkdownAsync", agent)
        self.assertIn("js/file-browser.js", shell)

    def test_fleet_page_exposes_remote_operations_console(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server"
        template = (root / "templates" / "pages" / "fleet.html").read_text()
        fleet_script = (root / "static" / "js" / "fleet.js").read_text()
        chat_script = (root / "static" / "js" / "agent-chat.js").read_text()

        self.assertIn("Remote operations", template)
        self.assertIn("pa-remote-start-form", template)
        self.assertIn("pa-remote-session-list", template)
        self.assertIn("pa-remote-history-list", template)
        self.assertIn("pa-remote-dispatch-list", template)
        self.assertIn('name="resume_session_id"', template)
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
        self.assertIn('"Idempotency-Key": admission.key', fleet_script)
        self.assertIn("function loadRemoteDispatches(instanceId)", fleet_script)
        self.assertIn('completion_pending: "Completion pending"', fleet_script)
        self.assertIn("data-dispatch-retry", fleet_script)
        self.assertIn("data-dispatch-cancel", fleet_script)
        self.assertNotIn("remoteInstanceSelect.disabled = true", fleet_script)
        self.assertIn("loadOlderRemoteAudit", fleet_script)
        self.assertIn("data-remote-audit-older", fleet_script)
        self.assertNotIn("setTimeout(loadRemoteOperations", fleet_script)
        self.assertIn("setApiBase", chat_script)


if __name__ == "__main__":
    unittest.main()
