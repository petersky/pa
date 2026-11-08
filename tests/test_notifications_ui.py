import shutil
import subprocess
from pathlib import Path

import pytest

from tests.test_notifications import _reset_singletons  # noqa: F401

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
const blocker = ui.interactionControls({id: "blocked", interaction: {
  state: "failed", delivery_retryable: false,
  delivery_recovery_action: "Restore <original> workspace"
}});
assert.ok(blocker.includes("Response recorded"));
assert.ok(blocker.includes("Restore &lt;original&gt; workspace"));
assert.ok(!blocker.includes("data-notification-retry"));
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
for (const state of ["outstanding", "failed", "answered", "delivery_pending"]) {
  assert.strictEqual(ui.interactionControls({ id: "retired", resolved_at: "2026-09-12T00:00:00Z", interaction: { state } }), "");
}
assert.strictEqual(ui.interactionControls({ id: "expired", interaction: { state: "expired" } }), "");
"""
    completed = subprocess.run(
        [shutil.which("node"), "-e", harness, str(script)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


@pytest.mark.skipif(not shutil.which("node"), reason="node is required for UI tests")
def test_retired_failure_api_payload_renders_accurate_non_actionable_history(tmp_path) -> None:
    import asyncio
    import json

    from fastapi.testclient import TestClient
    from pa.domain.notifications import InteractionResponse
    from pa.notifications import NotificationConflict
    from tests.test_notifications import _create, _kernel

    kernel = _kernel(tmp_path)
    service = kernel.ctx.require_service("notifications")
    notice = _create(service)

    def fail(_response):
        raise RuntimeError("Original execution unavailable")

    service.register_delivery_handler(notice.id, fail)
    with pytest.raises(NotificationConflict):
        asyncio.run(service.respond(notice, InteractionResponse(idempotency_key="answer", value="recorded"), principal_id="user:local"))
    service.resolve(notice, principal_id="user:local", idempotency_key="retire")
    with TestClient(kernel.build_app()) as client:
        payload = client.get(f"/api/notifications/{notice.id}").json()
    harness = r'''
const assert = require("assert");
const item = JSON.parse(process.argv[2]);
const list = { innerHTML: "", querySelectorAll: () => [], querySelector: () => null };
const chrome = { querySelector: () => list };
global.window = {};
global.document = {
  readyState: "loading", addEventListener: () => {}, querySelector: () => chrome,
  createElement: () => ({
    set textContent(value) { this.text = String(value); },
    get innerHTML() { return this.text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }
  })
};
require(process.argv[1]);
for (const response_mode of ["local", "remote"]) {
  item.routing.response_mode = response_mode;
  window.PANotificationsTest.render([item], false);
  assert.ok(list.innerHTML.includes("Request retired · resolved"));
  assert.ok(list.innerHTML.includes("Historical delivery failure"));
  assert.ok(list.innerHTML.includes("Response recorded"));
  assert.ok(!list.innerHTML.includes("data-notification-retry"));
  assert.ok(!list.innerHTML.includes("Required action:"));
  assert.ok(!list.innerHTML.includes("Respond on the owning instance"));
  assert.ok(!list.innerHTML.includes("Delivered to request"));
}
'''
    completed = subprocess.run(
        [shutil.which("node"), "-e", harness, str(ROOT / "src/pa/server/static/js/notifications.js"), json.dumps(payload)],
        capture_output=True, text=True,
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.skipif(not shutil.which("node"), reason="node is required for UI tests")
def test_failed_delivery_card_explains_failure_offers_repair_and_can_be_dismissed() -> None:
    script = ROOT / "src/pa/server/static/js/notifications.js"
    harness = r"""
const assert = require("assert");
function escapeHtml(value) {
  return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;")
    .replace(/>/g, "&gt;").replace(/\"/g, "&quot;");
}
const list = { innerHTML: "", querySelectorAll: () => [], querySelector: () => null, insertAdjacentHTML() {} };
const chrome = { querySelector: (s) => s === "[data-notification-list]" ? list : null };
global.window = { sessionStorage: { length: 0, getItem: () => null, setItem() {}, removeItem() {}, key: () => null }, setTimeout, clearInterval() {} };
global.document = {
  readyState: "loading", addEventListener() {},
  querySelector: (s) => s === "[data-notification-chrome]" ? chrome : null,
  createElement: () => ({ set textContent(v) { this._t = String(v); }, get innerHTML() { return escapeHtml(this._t); } })
};
global.EventSource = function () {};
require(process.argv[1]);
const ui = window.PANotificationsTest;

// A source superseded by a linked run: why it failed, the one safe repair, and a way out.
const recovery = { kind: "transfer_to_linked_run", label: "Send the recorded response to the linked run",
  expected_session_id: "old-s", expected_dispatch_id: "old-d", successor_session_id: "new-s", successor_dispatch_id: "new-d" };
const superseded = ui.interactionControls({ id: "n1", presentation: { recovery,
    failure_reason: 'This source has a durable linked attempt <img src=x onerror="window.xss=1">' },
  interaction: { state: "failed", delivery_retryable: false, delivery_failure_code: "source_superseded",
    delivery_recovery_action: "Send the recorded response to the linked run, or dismiss this request." } });
assert.ok(superseded.includes("superseded by a linked run"));
assert.ok(superseded.includes("Why delivery failed:") && superseded.includes("durable linked attempt"));
assert.ok(superseded.includes("&lt;img src=x onerror=&quot;window.xss=1&quot;&gt;"), "failure reason is escaped");
assert.ok(!superseded.includes("<img"));
assert.ok(!superseded.includes("data-notification-retry"), "retrying a fenced source cannot work, so it is not offered");
assert.ok(superseded.includes("data-notification-transfer"));
for (const [name, value] of [["expected-session", "old-s"], ["expected-dispatch", "old-d"], ["successor-session", "new-s"], ["successor-dispatch", "new-d"]]) {
  assert.ok(superseded.includes("data-" + name + '="' + value + '"'), name);
}
assert.ok(superseded.includes("data-notification-dismiss-failed") && superseded.includes("Dismiss without delivering"));

// An older record still marked retryable, but with a verified linked run: the repair
// is offered and the retry that cannot succeed is not.
const legacy = ui.interactionControls({ id: "n1b", presentation: { recovery, required_action: "Send it on" },
  interaction: { state: "failed", delivery_retryable: true, delivery_error: "stale" } });
assert.ok(legacy.includes("data-notification-transfer") && !legacy.includes("data-notification-retry"));
assert.ok(legacy.includes("superseded by a linked run") && legacy.includes("Send it on"));

// The linked run has ended: nothing can take the response, so no retry and no transfer.
const ended = ui.interactionControls({ id: "n1c", presentation: { required_action: "The linked run that replaced this session has ended.",
    recovery: { kind: "linked_run_ended", successor_session_id: "new-s" }, failure_reason: "fenced" },
  interaction: { state: "failed", delivery_retryable: true, delivery_recovery_action: "Send the recorded response to the linked run, or dismiss this request." } });
assert.ok(!ended.includes("data-notification-retry") && !ended.includes("data-notification-transfer"));
assert.ok(ended.includes("data-notification-dismiss-failed"));
assert.ok(ended.includes("The linked run that replaced this session has ended."), "live state wins over the stored action");
assert.ok(!ended.includes("Send the recorded response to the linked run, or dismiss"));

// No verifiable successor: still explained and dismissible, but no transfer button.
const noRepair = ui.interactionControls({ id: "n2", presentation: { failure_reason: "gone" },
  interaction: { state: "failed", delivery_retryable: false, delivery_failure_code: "source_superseded" } });
assert.ok(!noRepair.includes("data-notification-transfer") && !noRepair.includes("data-notification-retry"));
assert.ok(noRepair.includes("data-notification-dismiss-failed"));

// A transient failure keeps Retry, now with its reason, and can also be dismissed.
const transient = ui.interactionControls({ id: "n3", presentation: { failure_reason: "No space left on device" },
  interaction: { state: "failed", delivery_retryable: true } });
assert.ok(transient.includes("data-notification-retry") && transient.includes("No space left on device"));
assert.ok(transient.includes("data-notification-dismiss-failed") && !transient.includes("data-notification-transfer"));

// Only a failed response is dismissible: answered / delivery-pending ones must settle first.
for (const state of ["answered", "delivery_pending"]) {
  assert.ok(!ui.interactionControls({ id: "n4", interaction: { state } }).includes("data-notification-dismiss-failed"), state);
}
// Retired notices stay non-actionable.
assert.strictEqual(ui.interactionControls({ id: "n5", resolved_at: "2026-01-01T00:00:00Z",
  presentation: { recovery }, interaction: { state: "failed", delivery_retryable: false } }), "");

// The rendered card carries the version the transfer must name.
ui.render([{ id: "n6", version: 7, priority: "high", title: "T", updated_at: "2026-01-01T00:00:00Z",
  presentation: { category: "request", status: "Delivery failed" },
  interaction: { state: "failed", delivery_retryable: true } }], false);
assert.ok(list.innerHTML.includes('data-notification-version="7"'));
"""
    completed = subprocess.run([shutil.which("node"), "-e", harness, str(script)],
                               check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr or completed.stdout
