"""Browser-side retention regressions for long-running agent chats."""

from __future__ import annotations

import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path


class AgentChatMemoryBoundsTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "node is required for the browser harness")
    def test_fifty_thousand_events_keep_metadata_and_dom_bounded(self) -> None:
        script = Path(__file__).parents[1] / "src/pa/server/static/js/agent-chat.js"
        program = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const assert = require("assert");
            const noop = () => {{}};
            const rows = [];
            const messages = {{
              querySelectorAll: (selector) => selector === ".acw-msg" ? rows.slice() : [],
              scrollHeight: 0, scrollTop: 0, clientHeight: 100,
            }};
            const document = {{
              body: {{ addEventListener: noop }}, addEventListener: noop,
              querySelector: () => null, querySelectorAll: () => [],
            }};
            const window = {{ addEventListener: noop }};
            vm.runInNewContext(fs.readFileSync({str(script)!r}, "utf8"), {{
              window, document, console: {{ debug: noop }}, URL,
              setTimeout, clearTimeout, setInterval, clearInterval,
            }});
            const Widget = window.PAAgentChat.AgentChatWidget;
            const widget = Object.create(Widget.prototype);
            Object.assign(widget, {{
              els: {{ messages }}, transcriptEvents: [], seenEvents: {{}}, lastSeq: 0,
              streaming: {{}}, activityStreams: {{}}, toolTimers: {{}}, plans: [],
              messageRowCount: 0,
              hasOlder: false, isNearBottom: () => false, scrollToBottom: noop,
              upsertTool: noop,
              addBubble: () => {{
                const row = {{ remove: () => rows.splice(rows.indexOf(row), 1) }};
                rows.push(row);
                widget.messageRowCount += 1;
                return {{ row, bubble: {{}} }};
              }},
            }});
            let heapAtHalf = 0;
            for (let seq = 1; seq <= 50000; seq += 1) {{
              const toolEvent = seq % 3 !== 0;
              widget.handleEvent({{
                seq,
                type: toolEvent ? (seq % 2 ? "tool_call" : "tool_call_update") : "error",
                payload: toolEvent
                  ? {{ tool_call_id: "tool-" + Math.floor(seq / 2), status: "completed" }}
                  : {{ message: "representative payload " + seq }}
              }}, false);
              if (seq === 25000) {{ global.gc(); heapAtHalf = process.memoryUsage().heapUsed; }}
            }}
            global.gc();
            const heapAtEnd = process.memoryUsage().heapUsed;
            const stats = widget.memoryDiagnostics();
            assert.strictEqual(stats.retainedEvents, stats.bounds.retainedEvents);
            assert.strictEqual(stats.dedupeKeys, stats.bounds.retainedEvents);
            assert.ok(stats.messageRows <= stats.bounds.messageRows, JSON.stringify(stats));
            assert.strictEqual(widget.lastSeq, 50000);
            widget.streaming.active = {{ text: "still active", row: null }};
            const historicalOldest = widget.transcriptEvents[0].seq;
            widget.hasNewer = true;
            widget.handleEvent({{
              seq: 50001, type: "error", payload: {{ message: "durable live tail" }}
            }}, false);
            assert.strictEqual(widget.transcriptEvents[0].seq, historicalOldest);
            assert.strictEqual(widget.lastSeq, 50001);
            // Evicted sequence keys cannot make a reconnect duplicate visible.
            widget.handleEvent({{ seq: 1, type: "error", payload: {{ message: "duplicate" }} }}, false);
            assert.strictEqual(widget.lastSeq, 50001);
            assert.ok(widget.hasOlder);
            assert.strictEqual(widget.streaming.active.text, "still active");
            // Once the window is full, another 25k events must not add a
            // second transcript-sized heap segment. Allow allocator jitter.
            assert.ok(heapAtEnd - heapAtHalf < 8 * 1024 * 1024,
              JSON.stringify({{ heapAtHalf, heapAtEnd }}));
            """
        )
        completed = subprocess.run(
            [shutil.which("node"), "--expose-gc", "-e", program],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_template_exposes_bidirectional_history_controls(self) -> None:
        template = (
            Path(__file__).parents[1]
            / "src/pa/server/templates/partials/agent/chat-widget.html"
        ).read_text()
        self.assertIn("data-acw-load-older", template)
        self.assertIn("data-acw-load-newer", template)


if __name__ == "__main__":
    unittest.main()
