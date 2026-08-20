"""Browser-side regression coverage for agent-session SSE ownership."""

from __future__ import annotations

import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path


class AgentChatSseLifecycleTests(unittest.TestCase):
    def test_local_owner_history_bypasses_fleet_self_proxy(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is required for the browser-side lifecycle harness")

        script_path = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "static"
            / "js"
            / "agent-chat.js"
        )
        harness = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const noop = () => {{}};
            const document = {{
              body: {{ addEventListener: noop }},
              addEventListener: noop,
              querySelector: () => null,
              querySelectorAll: () => [],
            }};
            const window = {{
              addEventListener: noop,
              location: {{ href: "http://127.0.0.1:8080/agent" }},
              history: {{ replaceState: noop, pushState: noop }},
            }};
            const context = {{
              window, document, console: {{ debug: noop }}, URL,
              setTimeout, clearTimeout, AbortController,
            }};
            vm.runInNewContext(
              fs.readFileSync({str(script_path)!r}, "utf8"),
              context
            );
            const Widget = window.PAAgentChat.AgentChatWidget;
            const widget = Object.create(Widget.prototype);
            let historyBase = "";
            Object.assign(widget, {{
              destroyed: false,
              subscriptionGeneration: 0,
              sessionId: "session-local",
              ownerInstanceId: "macbook-id",
              currentInstanceId: "macbook-id",
              apiBase: "/api/agent",
              root: {{ dataset: {{}}, closest: () => null }},
              els: {{ promote: null }},
              drafts: null,
              _setRecoveryControl: noop,
              showRecoveryActions: noop,
              setPlaceholder: noop,
              setComposerEnabled: noop,
              setStatus: noop,
              clearSelectedSession: noop,
              _applyDurableHistory: (_sid, history) => history,
              api: () => {{
                historyBase = widget.apiBase;
                return Promise.resolve({{ session: {{ id: "session-local" }}, events: [] }});
              }},
              resolveSessionRoute: (_sessionId, ownerId) => Promise.resolve({{
                owner: {{ instance_id: ownerId }},
                api_base: ownerId === "macbook-id"
                  ? "/api/agent"
                  : "/api/fleet/instances/" + ownerId + "/agent",
                history_url: "/history/session-local",
                state: "live",
                live: true,
              }}),
              _loadLiveSnapshot: () => Promise.resolve(null),
            }});
            widget.openSession("session-local", "macbook-id", {{ replace: true }})
              .then(() => {{
                if (historyBase !== "/api/agent") {{
                  throw new Error("local history used fleet self-proxy: " + historyBase);
                }}
                historyBase = "";
                return widget.openSession("session-remote", "monica-id", {{ replace: true }});
              }})
              .then(() => {{
                if (historyBase !== "/api/fleet/instances/monica-id/agent") {{
                  throw new Error("remote history skipped fleet proxy: " + historyBase);
                }}
              }})
              .catch((error) => {{
                process.stderr.write(error.stack || String(error));
                process.exitCode = 1;
              }});
            """
        )
        completed = subprocess.run(
            [node, "-e", harness],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_slow_live_snapshot_preserves_history_and_schedules_retry(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is required for the browser-side lifecycle harness")

        script_path = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "static"
            / "js"
            / "agent-chat.js"
        )
        harness = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const noop = () => {{}};
            const document = {{
              body: {{ addEventListener: noop }},
              addEventListener: noop,
              querySelector: () => null,
              querySelectorAll: () => [],
            }};
            const window = {{
              addEventListener: noop,
              location: {{ href: "http://127.0.0.1:8080/agent" }},
            }};
            const context = {{
              window, document, console: {{ debug: noop }}, URL,
              setTimeout, clearTimeout, AbortController,
            }};
            vm.runInNewContext(
              fs.readFileSync({str(script_path)!r}, "utf8"),
              context
            );
            const Widget = window.PAAgentChat.AgentChatWidget;
            const widget = Object.create(Widget.prototype);
            let message = "";
            let retry = false;
            Object.assign(widget, {{
              destroyed: false,
              subscriptionGeneration: 7,
              transcriptEvents: [{{ seq: 41, type: "agent_message_chunk" }}],
              sessionRoute: {{ state: "live" }},
              apiWithTimeout: () => Promise.reject(new Error("slow")),
              setStatus: noop,
              setComposerEnabled: noop,
              setPlaceholder: (value) => {{ message = value; }},
              _setRecoveryControl: noop,
              _scheduleLiveStateRetry: () => {{ retry = true; }},
              applySnapshot: () => {{ throw new Error("slow snapshot should not replace history"); }},
            }});
            widget._loadLiveSnapshot("session-1", 7).then(() => {{
              if (!message.includes("Durable history is shown")) {{
                throw new Error("missing actionable degraded-state message");
              }}
              if (!retry) throw new Error("live-state retry was not scheduled");
              if (widget.transcriptEvents.length !== 1 || widget.transcriptEvents[0].seq !== 41) {{
                throw new Error("durable history was discarded");
              }}
            }}).catch((error) => {{
              process.stderr.write(error.stack || String(error));
              process.exitCode = 1;
            }});
            """
        )
        completed = subprocess.run(
            [node, "-e", harness],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_switching_more_than_six_sessions_keeps_one_stream(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is required for the browser-side lifecycle harness")

        script_path = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "static"
            / "js"
            / "agent-chat.js"
        )
        harness = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            let active = 0;
            class FakeEventSource {{
              static CLOSED = 2;
              constructor(url) {{
                this.url = url;
                this.readyState = 0;
                this.closed = false;
                active += 1;
              }}
              addEventListener() {{}}
              close() {{
                if (this.closed) return;
                this.closed = true;
                this.readyState = FakeEventSource.CLOSED;
                active -= 1;
              }}
            }}
            const noop = () => {{}};
            const body = {{ addEventListener: noop }};
            const document = {{
              body,
              addEventListener: noop,
              querySelector: () => null,
              querySelectorAll: () => [],
            }};
            const window = {{
              addEventListener: noop,
              location: {{ href: "http://127.0.0.1:8080/agent" }},
            }};
            const context = {{
              window, document, EventSource: FakeEventSource,
              console: {{ debug: noop }}, URL, setTimeout, clearTimeout,
              fetch: () => Promise.reject(new Error("unexpected fetch")),
            }};
            vm.runInNewContext(
              fs.readFileSync({str(script_path)!r}, "utf8"),
              context
            );
            const Widget = window.PAAgentChat.AgentChatWidget;
            const widget = Object.create(Widget.prototype);
            Object.assign(widget, {{
              apiBase: "/api/agent",
              es: null,
              esSessionId: "",
              esApiBase: "",
              sseReconnectCount: 0,
              subscriptionGeneration: 0,
              destroyed: false,
              lastSeq: 0,
              connectionNoticeShown: false,
              api: () => Promise.resolve({{}}),
              applySnapshot: noop,
              setStatus: noop,
              addBubble: noop,
            }});

            for (let index = 0; index < 7; index += 1) {{
              widget.sessionId = "session-" + index;
              widget.connectSSE();
              if (active !== 1) throw new Error("parallel streams after switch: " + active);
            }}
            const current = widget.es;
            widget.connectSSE();
            if (widget.es !== current || active !== 1) {{
              throw new Error("same-session stream was not reused");
            }}
            widget.closeSSE("test-teardown");
            if (active !== 0) throw new Error("stream survived teardown");
            widget.sessionId = "remote-session";
            widget.useExternalEventTransport(true);
            widget.connectSSE();
            if (active !== 0) throw new Error("external multiplex opened a session stream");
            """
        )
        completed = subprocess.run(
            [node, "-e", harness],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_spa_and_modal_teardown_hooks_are_present(self) -> None:
        static = Path(__file__).parents[1] / "src" / "pa" / "server" / "static" / "js"
        agent_chat = (static / "agent-chat.js").read_text()
        spa = (static / "spa.js").read_text()

        self.assertIn('destroyAll(target || document, "spa-swap")', agent_chat)
        self.assertIn('destroyAll(document, "pagehide")', agent_chat)
        self.assertIn('closeAll(document, "pagehide-persisted")', agent_chat)
        self.assertIn("root._acw.connectSSE()", agent_chat)
        self.assertIn('window.PAAgentChat.destroy(content, "card-closed")', spa)
        self.assertIn('window.PAAgentChat.destroy(content, "card-replaced")', spa)
