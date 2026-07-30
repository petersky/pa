"""Browser-side regression coverage for agent-session SSE ownership."""

from __future__ import annotations

import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path


class AgentChatSseLifecycleTests(unittest.TestCase):
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
        self.assertIn('root._acw.connectSSE()', agent_chat)
        self.assertIn('window.PAAgentChat.destroy(content, "card-closed")', spa)
        self.assertIn('window.PAAgentChat.destroy(content, "card-replaced")', spa)
