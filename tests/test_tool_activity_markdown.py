"""Tool Activity shares chat's markdown, fallback, and raw-text rendering."""

import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(not shutil.which("node"), reason="Node is required")
def test_streamed_activity_markdown_and_rerender():
    script = Path(__file__).parents[1] / "src/pa/server/static/js/agent-chat.js"
    program = r'''
const assert = require("assert");
const fs = require("fs");
const vm = require("vm");
const noop = () => {};
function element() {
  return {
    dataset: {}, children: [], innerHTML: "", textContent: "",
    setAttribute: noop, querySelector: () => null,
    appendChild(child) { this.children.push(child); },
    content: { querySelectorAll: () => [] },
  };
}
global.document = {
  addEventListener: noop, querySelectorAll: () => [],
  querySelector: () => null, createElement: element, body: null,
};
global.window = {};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const widget = Object.create(window.PAAgentChat.AgentChatWidget.prototype);
const activity = element();
Object.assign(widget, {
  activityStreams: {}, activityCount: 0, activeToolIds: {},
  els: { toolActivity: activity }, clearPlaceholder: noop,
  toolActivityIsNearBottom: () => true,
  followToolActivity: follow => assert.strictEqual(follow, true),
});
// Both provider paths keep accumulated source, and escape it before libraries load.
widget.appendActivityProgress("p", "**bold");
widget.appendActivityProgress("p", "** <script>bad()</script>");
widget.appendExplanationHeading("e", "**bold** <script>bad()</script>");
const progress = widget.activityStreams["progress:p"].el;
const explanation = widget.activityStreams["explanation:e"].el;
for (const el of [progress, explanation]) {
  assert.strictEqual(el.dataset.markdown, "**bold** <script>bad()</script>");
  assert.ok(el.innerHTML.includes("&lt;script&gt;"));
  assert.ok(!el.innerHTML.includes("<script>"));
}
assert.strictEqual(widget.activityCount, 2);
const group = widget.activeExplanation.group;
const tools = widget.activeExplanation.tools;
const tool = element();
tools.appendChild(tool);
// Late library readiness and raw-mode toggles use the same re-render entry point.
widget.root = { querySelectorAll(selector) {
  assert.ok(selector.includes(".acw-progress-update"));
  return [progress, explanation];
} };
let sanitized = 0;
window.marked = { parse(raw) {
  assert.strictEqual(raw, "**bold** <script>bad()</script>");
  return "<p><strong>bold</strong> <script>bad()</script></p>";
} };
window.DOMPurify = { sanitize(html, config) {
  assert.ok(config.FORBID_ATTR.includes("style"));
  sanitized++;
  return html.replace("<script>bad()</script>", "");
} };
let decorated = 0;
window.PALinks = { decorate: () => decorated++ };
widget.rerenderMarkdownBubbles();
assert.strictEqual(sanitized, 2);
assert.strictEqual(decorated, 2);
for (const el of [progress, explanation]) {
  assert.strictEqual(el.innerHTML, "<p><strong>bold</strong> </p>");
}
widget.rawText = true;
widget.rerenderMarkdownBubbles();
for (const el of [progress, explanation]) {
  assert.strictEqual(el.textContent, el.dataset.markdown);
  assert.strictEqual(el.dataset.rawText, "1");
}
widget.rawText = false;
// Finalized/replayed DOM still carries its source independently of stream state.
widget.finalizeActivity();
widget.rerenderMarkdownBubbles();
assert.strictEqual(progress.dataset.rawText, "0");
assert.strictEqual(sanitized, 4);
assert.strictEqual(group.children[1], tools);
assert.strictEqual(tools.children[0], tool);
'''
    subprocess.run(
        [shutil.which("node"), "-e", program, str(script)],
        check=True, capture_output=True, text=True,
    )
