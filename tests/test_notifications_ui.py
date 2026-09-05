import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]


def test_bell_panel_accessibility_live_updates_and_draft_preservation_contract() -> (
    None
):
    chrome = (ROOT / "src/pa/server/templates/partials/chrome-actions.html").read_text()
    script = (ROOT / "src/pa/server/static/js/notifications.js").read_text()
    styles = (ROOT / "src/pa/server/static/style.css").read_text()

    assert 'aria-label="Open notifications"' in chrome
    assert 'role="dialog"' in chrome
    assert 'aria-live="polite"' in chrome
    assert "data-notification-count hidden" in chrome
    assert 'data-notification-filter="outstanding"' in chrome
    assert "var drafts = new Map()" in script
    assert "window.sessionStorage" in script
    assert "data-notification-send-fields" in script
    assert "data-notification-retry" in script
    assert "Full request and details" in script
    assert "Technical details" in script
    assert "Cancel request without responding" in script
    assert "Dismiss notice" in script
    assert "data-notification-ack" not in script
    assert "renderMarkdownAsync" in script
    assert "allowEmbeddedMedia: false" in script
    assert 'new EventSource("/api/cards/events")' in script
    assert "setInterval" in script
    assert 'event.key === "Escape"' in script
    assert "if (flyout.hidden) return;" in script
    assert "@media (max-width: 640px)" in styles


@pytest.mark.skipif(not shutil.which("node"), reason="node is required for UI tests")
def test_notification_browser_renderer_contracts_and_xss_safety() -> None:
    script = ROOT / "src/pa/server/static/js/notifications.js"
    harness = r"""
const assert = require("assert");
const path = process.argv[1];
let storageReads = 0;
function escapeHtml(value) {
  return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;")
    .replace(/>/g, "&gt;").replace(/\"/g, "&quot;");
}
global.window = {
  sessionStorage: {
    length: 0,
    getItem: function () { storageReads += 1; return null; },
    setItem: function () {}, removeItem: function () {}, key: function () { return null; }
  },
  setTimeout: setTimeout,
  clearInterval: function () {}
};
global.document = {
  readyState: "loading",
  addEventListener: function () {},
  querySelector: function () { return null; },
  createElement: function () {
    return {
      _text: "",
      set textContent(value) { this._text = String(value); },
      get innerHTML() { return escapeHtml(this._text); }
    };
  }
};
global.EventSource = function () {};
require(path);
const ui = window.PANotificationsTest;
const tail = "END-OF-LONG-MARKDOWN";
const body = "# Heading\n\n- one\n- two\n\n<script>alert(1)</script>" + "x".repeat(12000) + tail;
const bodyHtml = ui.bodyMarkup({ id: "notice-1", summary: "Summary", body: body });
assert.ok(bodyHtml.includes("Full request and details"));
assert.ok(bodyHtml.includes(tail));
assert.ok(bodyHtml.includes("&lt;script&gt;"));
assert.ok(!bodyHtml.includes("<script>"));
assert.strictEqual(ui.safeMarkdownSource("<b>raw</b>"), "&lt;b&gt;raw&lt;/b&gt;");

const choices = ui.interactionControls({
  id: "notice-choice",
  interaction: {
    state: "outstanding", sensitive: false, allow_freeform: false,
    allow_cancel: true, response_schema: null,
    choices: [{ id: 'approve" autofocus onfocus="window.xss=1', label: "Approve", description: "Resume this exact run" }]
  }
});
assert.ok(choices.includes("Approve"));
assert.ok(choices.includes("Resume this exact run"));
assert.ok(choices.includes("&quot; autofocus onfocus=&quot;"));
assert.ok(!choices.includes('data-notification-choice="approve" autofocus'));
assert.ok(!choices.includes("Write your response"));
assert.ok(choices.includes("Cancel request without responding"));

const structured = ui.interactionControls({
  id: "notice-fields",
  interaction: {
    state: "outstanding", sensitive: false, allow_freeform: false,
    allow_cancel: false, choices: [],
    response_schema: {
      type: "object", required: ["environment"],
      properties: { environment: { type: "string", title: "Environment", minLength: 3, description: "Deployment target" } }
    }
  }
});
assert.ok(structured.includes("Environment *"));
assert.ok(structured.includes('minlength="3"'));
assert.ok(structured.includes("Deployment target"));

storageReads = 0;
ui.interactionControls({
  id: "notice-secret",
  interaction: {
    state: "outstanding", sensitive: true, allow_freeform: true,
    allow_cancel: false, choices: [], response_schema: null
  }
});
assert.strictEqual(storageReads, 0, "sensitive drafts must not be loaded from browser storage");
assert.ok(ui.interactionControls({ id: "failed", interaction: { state: "failed" } }).includes("Retry delivery"));
assert.strictEqual(ui.interactionControls({ id: "expired", interaction: { state: "expired" } }), "");
"""
    completed = subprocess.run(
        [shutil.which("node"), "-e", harness, str(script)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
