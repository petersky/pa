"""Durable browser-local Agent composer draft regressions."""

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

from pa.domain.models import TranscriptEvent
from pa.domain.projection import CardProjection
from pa.modules.agent_chat import PromptBody, _submit_client_prompt, session_prompt

ROOT = Path(__file__).parents[1]
SERVER = ROOT / "src" / "pa" / "server"


class AgentChatDraftContractTests(unittest.TestCase):
    def test_template_exposes_accessible_draft_controls_and_scoped_identity(
        self,
    ) -> None:
        widget = (
            SERVER / "templates" / "partials" / "agent" / "chat-widget.html"
        ).read_text()
        shell = (SERVER / "templates" / "shell.html").read_text()
        context = (ROOT / "src" / "pa" / "modules" / "ui_shell.py").read_text()
        style = (SERVER / "static" / "style.css").read_text()
        docs = (ROOT / "docs" / "AGENT_CHAT_DRAFTS.md").read_text()

        self.assertIn("data-acw-clear-draft", widget)
        self.assertIn("data-acw-draft-status", widget)
        self.assertIn('role="status" aria-live="polite"', widget)
        self.assertIn("data-pa-instance-id", shell)
        self.assertIn("data-pa-principal-id", shell)
        self.assertLess(
            shell.index("js/agent-chat-drafts.js"),
            shell.index("js/agent-chat.js"),
        )
        self.assertIn('"principal_id": get_principal_id(request)', context)
        self.assertIn("@media (max-width: 480px)", style)
        self.assertIn("@media (max-width: 768px)", style)
        self.assertIn("    align-self: flex-end;\n  }\n}\n\n.acw-message-images", style)
        self.assertIn("65,536", docs)
        self.assertIn("30 days", docs)
        self.assertIn("plaintext", docs)
        self.assertIn("never persists attachment bytes", docs)

    def test_session_routing_scopes_draft_before_restoring_conversation(self) -> None:
        script = (SERVER / "static" / "js" / "agent-chat.js").read_text()
        open_session = script.split(
            "AgentChatWidget.prototype.openSession", 1
        )[1].split("AgentChatWidget.prototype.recoverSession", 1)[0]

        self.assertLess(
            open_session.index("this.drafts.setInstance(ownerInstanceId)"),
            open_session.index("this.drafts.switchSession(sessionId)"),
        )
        self.assertLess(
            open_session.index("this.drafts.switchSession(sessionId)"),
            open_session.index("this.sessionId = sessionId"),
        )
        self.assertIn("self.drafts.setInstance(self.ownerInstanceId)", open_session)

    def test_snapshot_retains_draft_hook_while_blocking_recovery_composer(self) -> None:
        script = (SERVER / "static" / "js" / "agent-chat.js").read_text()
        apply_snapshot = script.split(
            "AgentChatWidget.prototype.applySnapshot", 1
        )[1].split("AgentChatWidget.prototype.applyOptionSnapshot", 1)[0]

        self.assertIn("this.drafts.onSnapshot(session)", apply_snapshot)
        self.assertIn(
            "this.setComposerEnabled(!this.sessionClosed && !recoveryBlocked)",
            apply_snapshot,
        )
        self.assertLess(
            apply_snapshot.index("this.drafts.onSnapshot(session)"),
            apply_snapshot.index("this.setComposerEnabled"),
        )

    def test_client_prompt_id_validation(self) -> None:
        body = PromptBody(message="keep me", client_prompt_id="browser-prompt-1")
        self.assertEqual(body.client_prompt_id, "browser-prompt-1")
        with self.assertRaises(ValueError):
            PromptBody(message="bad", client_prompt_id="short")
        with self.assertRaises(ValueError):
            PromptBody(
                message="bad",
                client_prompt_id="browser-prompt-2",
                dispatch_id="dispatch-1",
            )

    def test_projection_finds_durable_prompt_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            event = TranscriptEvent(
                session_id="session-1",
                seq=1,
                event_type="queue_enqueued",
                payload={
                    "id": "browser-prompt-1",
                    "message": "draft",
                    "images": [],
                    "action": "run",
                },
            )
            store.append_transcript_events([event])

            found = store.get_prompt_acceptance("session-1", "browser-prompt-1")
            missing = store.get_prompt_acceptance("session-1", "browser-prompt-missing")

        self.assertIsNotNone(found)
        assert found is not None
        self.assertEqual(found.payload["message"], "draft")
        self.assertIsNone(missing)


class _AcceptanceStore:
    def __init__(self) -> None:
        self.events: dict[tuple[str, str], TranscriptEvent] = {}

    def get_prompt_acceptance(
        self, session_id: str, prompt_id: str
    ) -> TranscriptEvent | None:
        return self.events.get((session_id, prompt_id))


class ClientPromptAdmissionTests(unittest.TestCase):
    def _runtime(self) -> tuple[SimpleNamespace, _AcceptanceStore]:
        store = _AcceptanceStore()
        runtime = SimpleNamespace(
            _prompt_admission_lock=asyncio.Lock(),
            _queue=[],
            store=store,
            _flush_transcript=lambda: None,
        )

        async def prompt(message: str, **kwargs) -> str:
            await asyncio.sleep(0)
            prompt_id = kwargs["prompt_id"]
            store.events[("session-1", prompt_id)] = TranscriptEvent(
                session_id="session-1",
                seq=len(store.events) + 1,
                event_type="queue_enqueued",
                payload={
                    "id": prompt_id,
                    "message": message,
                    "images": [image.public_dict() for image in kwargs["images"]],
                    "action": "run",
                },
            )
            return "started"

        runtime.prompt = AsyncMock(side_effect=prompt)
        return runtime, store

    def test_concurrent_retry_returns_original_acceptance_without_requeue(self) -> None:
        runtime, _store = self._runtime()
        request = MagicMock()
        body = PromptBody(message="durable", client_prompt_id="browser-prompt-1")

        async def run() -> list[dict]:
            return await asyncio.gather(
                _submit_client_prompt(request, "session-1", body, runtime, "durable"),
                _submit_client_prompt(request, "session-1", body, runtime, "durable"),
            )

        first, second = asyncio.run(run())

        self.assertEqual(runtime.prompt.await_count, 1)
        self.assertEqual({first["duplicate"], second["duplicate"]}, {False, True})
        self.assertTrue(first["accepted"])
        self.assertTrue(second["accepted"])

    def test_reused_prompt_id_with_different_content_is_rejected(self) -> None:
        runtime, store = self._runtime()
        store.events[("session-1", "browser-prompt-1")] = TranscriptEvent(
            session_id="session-1",
            seq=1,
            event_type="queue_enqueued",
            payload={
                "id": "browser-prompt-1",
                "message": "original",
                "images": [],
                "action": "run",
            },
        )

        async def run() -> None:
            with self.assertRaises(HTTPException) as raised:
                await _submit_client_prompt(
                    MagicMock(),
                    "session-1",
                    PromptBody(
                        message="changed",
                        client_prompt_id="browser-prompt-1",
                    ),
                    runtime,
                    "changed",
                )
            self.assertEqual(raised.exception.status_code, 409)

        asyncio.run(run())
        runtime.prompt.assert_not_awaited()

    def test_client_prompt_id_does_not_bypass_session_authorization(self) -> None:
        request = MagicMock()
        request.app.state.ctx.settings = SimpleNamespace(auth_required=True)
        request.state.instance_authenticated = False
        request.state.user = SimpleNamespace(role="user")
        runtime = SimpleNamespace(session=SimpleNamespace(principal_id="owner"))
        body = PromptBody(message="private", client_prompt_id="browser-prompt-1")

        async def run() -> None:
            with (
                patch(
                    "pa.modules.agent_chat._runtime_or_404", return_value=runtime
                ),
                patch(
                    "pa.modules.agent_chat.get_principal_id",
                    return_value="intruder",
                ),
                patch(
                    "pa.modules.agent_chat._submit_client_prompt",
                    new_callable=AsyncMock,
                ) as submit,
            ):
                with self.assertRaises(HTTPException) as raised:
                    await session_prompt(request, "session-1", body)
                self.assertEqual(raised.exception.status_code, 403)
                self.assertEqual(
                    raised.exception.detail["code"], "insufficient_authorization"
                )
                submit.assert_not_awaited()

        asyncio.run(run())


@unittest.skipUnless(shutil.which("node"), "node is required for draft UI tests")
class AgentChatDraftNodeTests(unittest.TestCase):
    def _run_node(self, program: str, *scripts: Path) -> None:
        subprocess.run(
            [shutil.which("node"), "-e", program, *map(str, scripts)],
            check=True,
            capture_output=True,
            text=True,
        )

    def test_store_scoping_refresh_tabs_clear_expiry_and_failure_modes(self) -> None:
        script = SERVER / "static" / "js" / "agent-chat-drafts.js"
        program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
class MemoryStorage {
  constructor() { this.values = new Map(); }
  get length() { return this.values.size; }
  key(index) { return Array.from(this.values.keys())[index] || null; }
  getItem(key) { return this.values.has(key) ? this.values.get(key) : null; }
  setItem(key, value) { this.values.set(key, String(value)); }
  removeItem(key) { this.values.delete(key); }
}
const storage = new MemoryStorage();
global.window = { localStorage: storage };
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const DraftStore = window.PAAgentDrafts.DraftStore;
let now = 1000;
const options = { instanceId: "instance-a", principalId: "user:a", storage, now: () => now };
const first = new DraftStore(Object.assign({ writerId: "tab-a" }, options));
let saved = first.write("session-a", {
  text: "line one\n**markdown**",
  selection_start: 4,
  selection_end: 9,
  selection_direction: "forward",
  attachments: [{ name: "shot.png", mime_type: "image/png", size: 10, data: "forbidden" }],
});
assert.strictEqual(saved.persisted, true);
assert.strictEqual(saved.record.attachments[0].data, undefined);
assert.strictEqual(first.read("session-b"), null);
assert.strictEqual(new DraftStore(options).read("session-a").text, "line one\n**markdown**");
assert.strictEqual(new DraftStore(Object.assign({}, options, { instanceId: "instance-b" })).read("session-a"), null);
assert.strictEqual(new DraftStore(Object.assign({}, options, { principalId: "user:b" })).read("session-a"), null);

now = 1000;
const tabB = new DraftStore(Object.assign({ writerId: "tab-b" }, options));
const newer = tabB.write("session-a", { text: "newer tab", attachments: [] });
const update = first.fromStorageEvent({
  key: tabB.key("session-a"), newValue: JSON.stringify(newer.record), storageArea: storage,
});
assert.strictEqual(update.record.text, "newer tab");
assert.strictEqual(first.fromStorageEvent({
  key: first.key("session-a"), newValue: JSON.stringify(saved.record), storageArea: storage,
}), null);

const cleared = tabB.clear("session-a", newer.record);
assert.strictEqual(cleared.record.cleared, true);
assert.strictEqual(cleared.record.text, "");
assert.strictEqual(cleared.record.attachments.length, 0);

const expiringStorage = new MemoryStorage();
let oldNow = 1;
const oldStore = new DraftStore({ instanceId: "i", principalId: "p", storage: expiringStorage, now: () => oldNow });
oldStore.write("old", { text: "abandoned" });
oldNow += window.PAAgentDrafts.RETENTION_MS + 10;
oldStore.gc();
assert.strictEqual(expiringStorage.length, 0);

const quota = new MemoryStorage();
quota.setItem = function () { const error = new Error("full"); error.name = "QuotaExceededError"; throw error; };
const quotaResult = new DraftStore({ instanceId: "i", principalId: "p", storage: quota }).write("s", { text: "keep" });
assert.strictEqual(quotaResult.persisted, false);
assert.strictEqual(quotaResult.error, "quota");

const unavailable = new DraftStore({ instanceId: "i", principalId: "p", storage: null });
const memoryOnly = unavailable.write("s", { text: "tab only" });
assert.strictEqual(memoryOnly.error, "unavailable");
assert.strictEqual(unavailable.read("s").text, "tab only");

const tooLarge = first.write("large", { text: "x".repeat(window.PAAgentDrafts.MAX_TEXT_LENGTH + 1) });
assert.strictEqual(tooLarge.error, "too-large");
"""
        self._run_node(program, script)

    def test_widget_restores_sessions_retains_failures_and_clears_acceptance(
        self,
    ) -> None:
        store_script = SERVER / "static" / "js" / "agent-chat-drafts.js"
        widget_script = SERVER / "static" / "js" / "agent-chat-draft-widget.js"
        program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
class MemoryStorage {
  constructor() { this.values = new Map(); }
  get length() { return this.values.size; }
  key(index) { return Array.from(this.values.keys())[index] || null; }
  getItem(key) { return this.values.has(key) ? this.values.get(key) : null; }
  setItem(key, value) { this.values.set(key, String(value)); }
  removeItem(key) { this.values.delete(key); }
}
const storage = new MemoryStorage();
const windowListeners = {};
global.window = {
  localStorage: storage,
  addEventListener: (name, fn) => { windowListeners[name] = fn; },
};
global.document = {
  documentElement: { dataset: { paInstanceId: "instance-a", paPrincipalId: "user:a" } },
  visibilityState: "visible",
  addEventListener: function () {},
};
global.URL = { revokeObjectURL: function () {} };
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
vm.runInThisContext(fs.readFileSync(process.argv[2], "utf8"));

function element() {
  const listeners = {};
  return {
    value: "", selectionStart: 0, selectionEnd: 0, selectionDirection: "none",
    hidden: false, textContent: "",
    addEventListener: (name, fn) => { listeners[name] = fn; },
    dispatch: (name, event) => { if (listeners[name]) listeners[name](event || {}); },
    setSelectionRange: function (start, end, direction) {
      this.selectionStart = start; this.selectionEnd = end; this.selectionDirection = direction;
    },
  };
}
function makeWidget(sessionId) {
  const input = element();
  const status = element();
  const notice = element();
  const clear = element();
  const lookup = {
    "[data-acw-draft-status]": status,
    "[data-acw-draft-attachments]": notice,
    "[data-acw-clear-draft]": clear,
  };
  return {
    sessionId,
    cardId: "card-a",
    submissionPending: false,
    pendingImages: [],
    els: { input },
    root: {
      dataset: {},
      querySelector: (selector) => lookup[selector] || null,
      querySelectorAll: () => [],
      isConnected: true,
    },
    renderPendingImages: function () {},
    status, notice, clear,
  };
}

const firstWidget = makeWidget("session-a");
const first = window.PAAgentDrafts.installWidget(firstWidget);
firstWidget.els.input.value = "draft A\nsecond line";
firstWidget.els.input.selectionStart = 7;
firstWidget.els.input.selectionEnd = 7;
firstWidget.els.input.dispatch("input");
first.flush({ force: true });
first.switchSession("session-b");
firstWidget.els.input.value = "draft B";
first.changed();
first.flush({ force: true });
first.switchSession("session-a");
assert.strictEqual(firstWidget.els.input.value, "draft A\nsecond line");
assert.strictEqual(firstWidget.els.input.selectionStart, 7);

firstWidget.pendingImages = [{
  name: "shot.png", mime_type: "image/png", size: 20,
  data: "binary-must-not-persist", preview: "blob:preview",
}];
first.changed();
first.flush({ force: true });
const stored = first.store.read("session-a");
assert.strictEqual(stored.attachments[0].name, "shot.png");
assert.strictEqual(stored.attachments[0].data, undefined);

const refreshedWidget = makeWidget("session-a");
const refreshed = window.PAAgentDrafts.installWidget(refreshedWidget);
assert.strictEqual(refreshedWidget.els.input.value, "draft A\nsecond line");
assert.strictEqual(refreshedWidget.pendingImages.length, 0);
assert.ok(refreshedWidget.notice.textContent.includes("Reselect"));

const retryId = refreshed.beginSubmission();
refreshed.submissionFailed({ rawText: refreshedWidget.els.input.value, images: [] });
assert.strictEqual(refreshed.beginSubmission(), retryId);
refreshedWidget.els.input.value += " edited";
refreshedWidget.els.input.dispatch("input");
assert.notStrictEqual(refreshed.beginSubmission(), retryId);

const acceptedText = refreshedWidget.els.input.value;
refreshed.submissionAccepted({ rawText: acceptedText, images: [] });
assert.strictEqual(refreshedWidget.els.input.value, "");
assert.strictEqual(refreshed.store.read("session-a").cleared, true);

firstWidget.root.isConnected = false;
windowListeners.pagehide();
assert.strictEqual(refreshed.store.read("session-a").cleared, true);

refreshedWidget.els.input.value = "secret";
refreshed.changed();
refreshed.flush({ force: true });
refreshedWidget.clear.dispatch("click");
assert.strictEqual(refreshedWidget.els.input.value, "");
assert.strictEqual(refreshed.store.read("session-a").text, "");

refreshedWidget.els.input.value = "unfinished composition";
refreshedWidget.els.input.dispatch("compositionstart");
refreshedWidget.els.input.dispatch("input");
assert.strictEqual(refreshed.composing, true);
refreshedWidget.els.input.dispatch("compositionend");
refreshed.flush({ force: true });
assert.strictEqual(refreshed.store.read("session-a").text, "unfinished composition");
"""
        self._run_node(program, store_script, widget_script)


if __name__ == "__main__":
    unittest.main()
