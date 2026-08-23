"""Bounded ACP prompt recovery, quiesce admission, and chat paint regressions."""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from pa.config import Settings
from pa.domain.models import AgentSession, TranscriptEvent
from pa.instance.agent_session import AgentSessionManager
from pa.modules.agent_chat import get_prompt_acceptance_status

ROOT = Path(__file__).parents[1]
SERVER = ROOT / "src" / "pa" / "server"


class QuiesceAdmissionRestoreTests(unittest.IsolatedAsyncioTestCase):
    def _runtime(self, *, prompting: bool = True) -> SimpleNamespace:
        return SimpleNamespace(
            prompting=prompting,
            connected=True,
            session_id="s1",
            _closed=False,
            _queue=[],
            session=SimpleNamespace(
                agent_name="codex",
                external_session_id="ext-1",
                status="idle",
                cwd="/tmp",
                label="test",
            ),
        )

    async def test_quiesce_timeout_restores_prior_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp))
            store = MagicMock()
            manager = AgentSessionManager(settings, store)
            manager._accepting = True
            manager._quiescing = False
            manager._runtimes = {"s1": self._runtime()}

            with patch(
                "pa.server.shutdown.is_shutting_down", return_value=False
            ), self.assertRaises(TimeoutError):
                await manager.quiesce(timeout=0.05)

            self.assertTrue(manager._accepting)
            self.assertFalse(manager._quiescing)

    async def test_quiesce_timeout_keeps_drain_when_shutdown_fence_active(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp))
            manager = AgentSessionManager(settings, MagicMock())
            manager._accepting = True
            manager._quiescing = False
            manager._runtimes = {"s1": self._runtime()}

            with patch(
                "pa.server.shutdown.is_shutting_down", return_value=True
            ), self.assertRaises(TimeoutError):
                await manager.quiesce(timeout=0.05)

            self.assertFalse(manager._accepting)
            self.assertTrue(manager._quiescing)


class PromptAcceptanceStatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_prompt_status_reports_accepted_queued_and_not_accepted(
        self,
    ) -> None:
        store = MagicMock()
        store.get_session.return_value = AgentSession(
            id="session-1",
            agent_name="codex",
            status="idle",
            principal_id="user:local",
        )
        store.get_prompt_acceptance.side_effect = [
            TranscriptEvent(
                session_id="session-1",
                seq=3,
                event_type="queue_enqueued",
                payload={"id": "browser-prompt-1", "action": "append"},
            ),
            TranscriptEvent(
                session_id="session-1",
                seq=4,
                event_type="user_message",
                payload={"id": "browser-prompt-2", "message": "hi"},
            ),
            None,
        ]
        manager = MagicMock()
        manager.store = store
        manager.get.return_value = None

        request = MagicMock()
        request.app.state.ctx.settings = SimpleNamespace(
            auth_required=False, instance_id="local"
        )
        request.state.user = None
        request.state.instance_authenticated = False

        async def offload(_mgr, _name, fn, *args, **kwargs):
            return fn(*args)

        with patch("pa.modules.agent_chat._manager", return_value=manager), patch(
            "pa.modules.agent_chat._offload", side_effect=offload
        ), patch("pa.modules.agent_chat.get_principal_id", return_value="user:local"):
            queued = await get_prompt_acceptance_status(
                request, "session-1", "browser-prompt-1"
            )
            accepted = await get_prompt_acceptance_status(
                request, "session-1", "browser-prompt-2"
            )
            missing = await get_prompt_acceptance_status(
                request, "session-1", "browser-prompt-3"
            )

        self.assertEqual(queued["status"], "queued")
        self.assertTrue(queued["accepted"])
        self.assertTrue(queued["duplicate_safe"])
        self.assertEqual(accepted["status"], "accepted")
        self.assertEqual(missing["status"], "not_accepted")
        self.assertFalse(missing["accepted"])
        self.assertTrue(missing["duplicate_safe"])

    async def test_invalid_prompt_id_is_rejected(self) -> None:
        request = MagicMock()
        with self.assertRaises(HTTPException) as raised:
            await get_prompt_acceptance_status(request, "session-1", "short")
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail["code"], "invalid_client_prompt_id")


class AgentChatPromptRecoveryJsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node is required for agent-chat JS regressions")

    def _run_node(self, program: str, *scripts: Path) -> None:
        cmd = [self.node, "-e", program, *[str(path) for path in scripts]]
        completed = subprocess.run(
            cmd, capture_output=True, text=True, cwd=str(ROOT), check=False
        )
        if completed.returncode != 0:
            self.fail(completed.stderr or completed.stdout or "node failed")

    def test_never_settling_prompt_fetch_reconciles_to_retryable(self) -> None:
        script = SERVER / "static" / "js" / "agent-chat.js"
        program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
const noop = function () {};
global.document = {
  body: { addEventListener: noop },
  addEventListener: noop,
  createElement: function (tag) {
    return {
      tagName: String(tag).toUpperCase(),
      className: "",
      textContent: "",
      hidden: false,
      dataset: {},
      style: {},
      children: [],
      appendChild: function (child) { this.children.push(child); return child; },
      setAttribute: noop,
      addEventListener: noop,
      querySelector: function () { return null; },
      querySelectorAll: function () { return []; },
    };
  },
  querySelector: function () { return null; },
  querySelectorAll: function () { return []; },
};
global.window = {
  addEventListener: noop,
  location: { href: "http://127.0.0.1:8080/agent" },
  performance: { now: function () { return Date.now(); } },
};
global.URL = URL;
global.performance = global.window.performance;
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));

const Widget = window.PAAgentChat.AgentChatWidget;
const widget = Object.create(Widget.prototype);
const send = { disabled: false, textContent: "Send" };
const form = { setAttribute: noop };
const input = { value: "hello" };
const statusCalls = [];
const bubbles = [];
let promptResolve;
let statusResolve;
const drafts = {
  submissionId: "browser-prompt-stable",
  restoringSubmission: false,
  beginSubmission: function () { return "browser-prompt-stable"; },
  setStatus: function (message) { statusCalls.push(message); },
  submissionAccepted: noop,
  submissionFailed: noop,
  observeAcceptance: noop,
};
Object.assign(widget, {
  sessionId: "session-1",
  sessionClosed: false,
  subscriptionGeneration: 1,
  destroyed: false,
  submissionPending: false,
  submissionState: "idle",
  submissionRetryVisible: false,
  submissionRetryReason: "",
  composerEnabled: true,
  prompting: false,
  pendingImages: [],
  drafts,
  providerId: "codex",
  preferredProvider: "codex",
  els: { input, send, form, messages: null, toolActivity: null },
  root: {
    dataset: {},
    isConnected: true,
    querySelector: function (sel) {
      if (sel === "[data-acw-draft-status]") return { textContent: "", parentNode: null };
      return null;
    },
    querySelectorAll: function () { return []; },
  },
  commandInvocation: function () { return null; },
  api: function (path, options) {
    if (path.indexOf("/prompt") !== -1 && options && options.method === "POST") {
      return new Promise(function (resolve) { promptResolve = resolve; });
    }
    if (path.indexOf("/prompts/") !== -1) {
      return new Promise(function (resolve) { statusResolve = resolve; });
    }
    return Promise.resolve({});
  },
  _isDuplicateUserBubble: function () { return false; },
  addBubble: function (role, text) { bubbles.push({ role, text }); },
  setTurnActive: noop,
  scrollToBottom: noop,
  refreshQueue: noop,
  resolveSessionNotLive: noop,
  clearPendingImages: noop,
});

(async function () {
  // Force the network-uncertain path without waiting for the real timeout.
  widget.api = function (path) {
    if (path.indexOf("/prompts/") !== -1) {
      return new Promise(function (resolve) { statusResolve = resolve; });
    }
    const err = new Error("network drop");
    err.name = "TypeError";
    return Promise.reject(err);
  };
  widget.send("append");
  await new Promise(setImmediate);
  assert.ok(
    widget.submissionState === "acknowledgement_uncertain" ||
    widget.submissionState === "checking",
    "expected uncertain/checking, got " + widget.submissionState
  );
  statusResolve({
    accepted: false,
    status: "not_accepted",
    message: "No durable acceptance for this prompt id yet.",
    duplicate_safe: true,
  });
  await new Promise(setImmediate);
  assert.strictEqual(widget.submissionState, "retryable");
  assert.strictEqual(widget.submissionPending, false);
  assert.strictEqual(send.textContent, "Retry");
  assert.ok(widget.submissionRetryVisible);
})().catch(function (error) {
  process.stderr.write(error.stack || String(error));
  process.exitCode = 1;
});
"""
        self._run_node(program, script)

    def test_codex_commentary_in_chat_and_thoughts_nest_tools(self) -> None:
        script = SERVER / "static" / "js" / "agent-chat.js"
        program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
const noop = function () {};
function el(tag) {
  const node = {
    tagName: String(tag).toUpperCase(),
    className: "",
    textContent: "",
    hidden: false,
    dataset: {},
    children: [],
    attributes: {},
    style: {},
    classList: {
      add: function (name) { node.className = (node.className + " " + name).trim(); },
      toggle: function (name, on) {
        if (on) this.add(name);
      },
    },
    appendChild: function (child) { this.children.push(child); child.parentNode = this; return child; },
    setAttribute: function (name, value) { this.attributes[name] = value; },
    querySelector: function (sel) {
      if (sel === ".acw-tool-title" || sel === ".acw-tool-timer" || sel === ".acw-tool-status") {
        return this._parts && this._parts[sel.slice(1)] || { textContent: "" };
      }
      return null;
    },
    querySelectorAll: function (sel) {
      const out = [];
      const visit = function (n) {
        if (!n || !n.children) return;
        n.children.forEach(function (child) {
          if (sel.indexOf(".acw-tool") !== -1 && (child.className || "").indexOf("acw-tool") !== -1) out.push(child);
          if (sel.indexOf(".acw-explanation") !== -1 && (child.className || "").indexOf("acw-explanation") !== -1) out.push(child);
          if (sel.indexOf("[data-tool-id]") !== -1 && child.dataset && child.dataset.toolId) out.push(child);
          visit(child);
        });
      };
      visit(this);
      return out;
    },
  };
  return node;
}
global.document = {
  body: { addEventListener: noop },
  addEventListener: noop,
  createElement: el,
  querySelector: function () { return null; },
  querySelectorAll: function () { return []; },
};
global.window = { addEventListener: noop, location: { href: "http://127.0.0.1/agent" } };
global.URL = URL;
const intervals = [];
global.setInterval = function (fn, ms) {
  const id = { fn: fn, ms: ms };
  intervals.push(id);
  return id;
};
global.clearInterval = function (id) {
  const idx = intervals.indexOf(id);
  if (idx >= 0) intervals.splice(idx, 1);
};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));

const Widget = window.PAAgentChat.AgentChatWidget;
function makeWidget(provider) {
  const messages = el("div");
  const toolActivity = el("div");
  const toolEmpty = el("div");
  const widget = Object.create(Widget.prototype);
  Object.assign(widget, {
    providerId: provider,
    preferredProvider: provider,
    showThinking: true,
    lastSeq: 0,
    transcriptEvents: [],
    seenEvents: {},
    streaming: {},
    activityStreams: {},
    activityCount: 0,
    activeToolIds: {},
    toolTimers: {},
    messageRowCount: 0,
    hasNewer: false,
    activeExplanation: null,
    els: {
      messages,
      toolActivity,
      toolEmpty,
      placeholder: el("div"),
      toolFlyout: null,
      toolToggle: null,
    },
    clearPlaceholder: noop,
    isNearBottom: function () { return true; },
    toolActivityIsNearBottom: function () { return true; },
    followToolActivity: noop,
    updateToolAnimation: noop,
    finalizeStreams: noop,
  });
  // Provide a minimal addBubble/appendStream using prototype methods where possible.
  widget.addBubble = Widget.prototype.addBubble;
  widget.appendStream = Widget.prototype.appendStream;
  widget.renderMarkdownBubble = noop;
  widget._pruneMessageRows = noop;
  widget._pruneActivityRows = Widget.prototype._pruneActivityRows;
  widget.bumpActivityCount = Widget.prototype.bumpActivityCount;
  widget.ensureActivity = Widget.prototype.ensureActivity;
  widget._toolParent = Widget.prototype._toolParent;
  widget._isCodexProvider = Widget.prototype._isCodexProvider;
  widget.appendExplanationHeading = Widget.prototype.appendExplanationHeading;
  widget.appendActivityProgress = Widget.prototype.appendActivityProgress;
  widget.upsertTool = Widget.prototype.upsertTool;
  widget.handleEvent = Widget.prototype.handleEvent;
  widget._normalizeEvent = Widget.prototype._normalizeEvent;
  widget._eventKey = Widget.prototype._eventKey;
  return widget;
}

const codex = makeWidget("codex");
codex.handleEvent({
  seq: 1, type: "agent_message_chunk",
  payload: { phase: "commentary", message_id: "c1", text: "Working on it" },
});
assert.ok(codex.els.messages.children.some(function (child) {
  return (child.className || "").indexOf("acw-msg") !== -1 || child.children;
}) || Object.keys(codex.streaming).length > 0);

codex.handleEvent({
  seq: 2, type: "agent_thought_chunk",
  payload: { message_id: "t1", text: "Inspect files" },
});
assert.ok(codex.activeExplanation);
assert.strictEqual(codex.activeExplanation.el.textContent, "Inspect files");
codex.handleEvent({
  seq: 3, type: "tool_call",
  payload: { tool_call_id: "tool-1", title: "Read", status: "in_progress" },
});
assert.strictEqual(codex.activeExplanation.tools.children.length, 1);
assert.strictEqual(codex.activeExplanation.tools.children[0].dataset.toolId, "tool-1");

const cursor = makeWidget("cursor");
cursor.handleEvent({
  seq: 1, type: "agent_message_chunk",
  payload: { phase: "commentary", message_id: "c1", text: "progress" },
});
assert.ok(cursor.activityStreams["progress:c1"]);
assert.strictEqual(Object.keys(cursor.streaming).length, 0);
cursor.handleEvent({
  seq: 2, type: "agent_thought_chunk",
  payload: { message_id: "t1", text: "thinking" },
});
assert.ok(cursor.streaming["thought:t1"] || Object.keys(cursor.streaming).some(function (k) {
  return k.indexOf("thought") !== -1;
}));
intervals.slice().forEach(function (id) { clearInterval(id); });
"""
        self._run_node(program, script)

    def test_recent_first_paint_and_navigation_cache(self) -> None:
        script = SERVER / "static" / "js" / "agent-chat.js"
        program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
const noop = function () {};
function el() {
  return {
    children: [], className: "", dataset: {}, hidden: false, scrollTop: 0, scrollHeight: 100, clientHeight: 40,
    appendChild: function (child) { this.children.push(child); return child; },
    querySelectorAll: function () { return this.children; },
    hasAttribute: function () { return false; },
    remove: function () {},
    cloneNode: function () { return Object.assign({}, this, { children: this.children.slice() }); },
  };
}
global.document = {
  body: { addEventListener: noop },
  addEventListener: noop,
  createElement: function () {
    return {
      innerHTML: "", children: [],
      appendChild: function (c) { this.children.push(c); return c; },
      get firstChild() { return this.children[0] || null; },
    };
  },
  querySelector: function () { return null; },
  querySelectorAll: function () { return []; },
};
global.window = { addEventListener: noop, location: { href: "http://127.0.0.1/agent" } };
global.URL = URL;
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
assert.ok(window.PAAgentChat.INITIAL_VISIBLE_EVENTS <= 120);

const Widget = window.PAAgentChat.AgentChatWidget;
const widget = Object.create(Widget.prototype);
const messages = el();
Object.assign(widget, {
  sessionId: "session-cache",
  apiBase: "/api/agent",
  subscriptionGeneration: 1,
  destroyed: false,
  lastSeq: 0,
  transcriptEvents: [],
  seenEvents: {},
  hasOlder: false,
  olderCursor: null,
  hasNewer: false,
  newerCursor: null,
  providerId: "codex",
  messageRowCount: 0,
  els: { messages, toolActivity: el() },
  isNearBottom: function () { return true; },
  scrollToBottom: noop,
  clearPlaceholder: noop,
  updateOlderControl: noop,
  updateNewerControl: noop,
  _rebuildSeenEvents: Widget.prototype._rebuildSeenEvents,
  _eventKey: Widget.prototype._eventKey,
  _normalizeEvent: Widget.prototype._normalizeEvent,
  _boundedEvents: Widget.prototype._boundedEvents,
  _cacheKey: Widget.prototype._cacheKey,
  _stashSessionDomCache: Widget.prototype._stashSessionDomCache,
  _restoreSessionDomCache: Widget.prototype._restoreSessionDomCache,
  _paintRecentHistory: Widget.prototype._paintRecentHistory,
  renderTranscript: function (events, options) {
    this.transcriptEvents = events.slice();
    this.lastSeq = events.reduce(function (max, event) {
      return event.seq > max ? event.seq : max;
    }, 0);
    events.forEach(function (event) {
      const row = { className: "acw-msg", dataset: { seq: String(event.seq) }, outerHTML: "<div class='acw-msg'></div>", hasAttribute: function () { return false; }, cloneNode: function () { return this; } };
      messages.children.push(row);
    });
    this.messageRowCount = messages.children.length;
  },
});

const events = [];
for (let i = 1; i <= 200; i += 1) {
  events.push({ seq: i, type: "user_message", payload: { message: "m" + i } });
}
widget._paintRecentHistory({
  events,
  page: { has_older: true, oldest_seq: 1, newest_seq: 200, next_before_seq: 81 },
  session: { agent_name: "codex" },
}, 1);
assert.ok(widget.transcriptEvents.length <= window.PAAgentChat.INITIAL_VISIBLE_EVENTS);
assert.strictEqual(widget.lastSeq, 200);
assert.strictEqual(widget.hasOlder, true);
widget._stashSessionDomCache();
const restored = Object.create(Widget.prototype);
Object.assign(restored, {
  sessionId: "session-cache",
  apiBase: "/api/agent",
  subscriptionGeneration: 1,
  destroyed: false,
  lastSeq: 0,
  transcriptEvents: [],
  seenEvents: {},
  hasOlder: false,
  olderCursor: null,
  hasNewer: false,
  newerCursor: null,
  providerId: "",
  messageRowCount: 0,
  els: { messages: el(), toolActivity: el() },
  isNearBottom: function () { return true; },
  scrollToBottom: noop,
  clearPlaceholder: noop,
  updateOlderControl: noop,
  updateNewerControl: noop,
  _rebuildSeenEvents: Widget.prototype._rebuildSeenEvents,
  _eventKey: Widget.prototype._eventKey,
  _cacheKey: Widget.prototype._cacheKey,
  _restoreSessionDomCache: Widget.prototype._restoreSessionDomCache,
});
assert.strictEqual(restored._restoreSessionDomCache("session-cache"), true);
assert.strictEqual(restored.lastSeq, 200);
assert.ok(restored.transcriptEvents.length > 0);
"""
        self._run_node(program, script)


if __name__ == "__main__":
    unittest.main()
