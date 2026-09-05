from __future__ import annotations

import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1] / "src" / "pa" / "server" / "static" / "js"


@unittest.skipUnless(shutil.which("node"), "node is required for sidebar UI tests")
class AgentSidebarFreshnessTests(unittest.TestCase):
    def _run_node(self, body: str, *scripts: Path) -> None:
        program = textwrap.dedent(
            """
            const fs = require("fs");
            const vm = require("vm");
            const assert = require("assert");
            const noop = () => {};
            const document = {
              hidden: false,
              cookie: "",
              body: { addEventListener: noop },
              addEventListener: noop,
              querySelector: () => null,
              querySelectorAll: () => [],
              createElement: () => ({
                className: "", dataset: {}, textContent: "", hidden: false,
                setAttribute: noop, appendChild: noop, querySelector: () => null,
              }),
            };
            const window = { addEventListener: noop, console };
            const context = {
              window, document, console: { debug: noop }, URL, AbortController,
              DOMException, Promise, Math, Date, setTimeout, clearTimeout,
              fetch: () => Promise.reject(new Error("unexpected fetch")),
            };
            for (const path of process.argv.slice(1)) {
              vm.runInNewContext(fs.readFileSync(path, "utf8"), context);
            }
            """
        ) + textwrap.dedent(body)
        completed = subprocess.run(
            [shutil.which("node"), "-e", program, *map(str, scripts)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)

    def test_confirmed_config_model_drives_sidebar_and_details(self) -> None:
        self._run_node(
            """
            const record = {
              agent_name: "codex",
              model_id: "gpt-5.6-sol[high]",
              config_json: {
                values: { model: "gpt-6-astra", reasoning_effort: "high" },
                configuration: {
                  state: "ready",
                  requested: { model_id: "requested-but-not-confirmed" },
                  effective: {
                    model_id: "gpt-5.6-sol[high]",
                    config: { model: "gpt-6-astra", reasoning_effort: "high" },
                  },
                },
              },
            };
            const fields = window.PAAgentChat.confirmedSessionFields(record);
            assert.strictEqual(fields.modelId, "gpt-6-astra");
            assert.strictEqual(fields.reasoning, "high");
            assert.strictEqual(
              window.PAAgentChat.sessionRuntimeLabel(record),
              "codex · gpt-6-astra · effort high"
            );
            const summary = window.PAAgentChat.sessionConfigSummary(record);
            assert.ok(summary.includes("effective: model gpt-6-astra"));
            assert.ok(!summary.includes("effective: model gpt-5.6-sol"));
            assert.ok(summary.includes("requested: model requested-but-not-confirmed"));
            """,
            ROOT / "agent-chat.js",
        )

    def test_inactive_session_events_coalesce_into_forced_list_refresh(self) -> None:
        self._run_node(
            """
            (async () => {
              const listeners = {};
              class FakeEventSource {
                constructor(url) { this.url = url; FakeEventSource.current = this; }
                addEventListener(type, callback) { listeners[type] = callback; }
                close() { this.closed = true; }
              }
              context.EventSource = FakeEventSource;
              const list = {
                isConnected: true, innerHTML: "", scrollTop: 23,
                dataset: {}, setAttribute: noop, appendChild: noop,
                closest: () => ({ dataset: { agentSessionView: "chats" } }),
                querySelector: () => null, querySelectorAll: () => [],
              };
              const selected = { _acw: { sessionId: "selected-session" } };
              document.querySelector = (selector) => {
                if (selector === "[data-agent-session-list]") return list;
                if (selector === "[data-agent-chat]") return selected;
                return null;
              };
              let fetches = 0;
              context.fetch = (url) => {
                fetches += 1;
                assert.strictEqual(
                  url,
                  "/api/agent/sessions?view=chats&selected_session_id=selected-session"
                );
                return Promise.resolve({
                  ok: true, headers: { get: () => null },
                  json: () => Promise.resolve([]),
                });
              };

              window.PAAgentChat.startSessionListFreshness();
              assert.ok(FakeEventSource.current.url.includes("/session-events"));
              listeners.queue_dequeued({ data: JSON.stringify({
                session_id: "inactive-session", seq: 8,
              }) });
              listeners.config_options_update({ data: JSON.stringify({
                session_id: "inactive-session", seq: 9,
              }) });
              await new Promise((resolve) => setTimeout(resolve, 80));
              assert.strictEqual(fetches, 1);
              assert.strictEqual(list.scrollTop, 23);
              assert.ok(window.PAAgentChat.sessionListRefreshEvents.includes("prompt_failed"));
              assert.ok(window.PAAgentChat.sessionListRefreshEvents.includes("session_recovered"));
              assert.ok(window.PAAgentChat.sessionListRefreshEvents.includes("configuration_changed"));
              window.PAAgentChat.stopSessionListFreshness("test-complete");
            })().catch((error) => {
              process.stderr.write(error.stack || String(error));
              process.exitCode = 1;
            });
            """,
            ROOT / "session-recovery.js",
            ROOT / "agent-chat.js",
        )


if __name__ == "__main__":
    unittest.main()
