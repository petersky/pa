"""Browser-side contracts for durable session menu actions."""

from __future__ import annotations

import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path


@unittest.skipUnless(shutil.which("node"), "node is required for session action UI tests")
class AgentChatSessionActionTests(unittest.TestCase):
    def test_every_session_state_and_transition_renders_valid_accessible_actions(self) -> None:
        script = (
            Path(__file__).parents[1]
            / "src/pa/server/static/js/agent-chat.js"
        )
        program = textwrap.dedent(
            """
            const fs = require("fs");
            const vm = require("vm");
            const noop = () => {};
            const document = {
              body: { addEventListener: noop }, addEventListener: noop,
              querySelector: () => null, querySelectorAll: () => [],
            };
            const window = { addEventListener: noop };
            vm.runInNewContext(fs.readFileSync(process.argv[1], "utf8"), {
              window, document, console: { debug: noop }, URL, AbortController,
              setTimeout, clearTimeout, fetch: noop,
            });
            const Widget = window.PAAgentChat.AgentChatWidget;
            function control() {
              return {
                hidden: false, disabled: false, textContent: "", title: "",
                attrs: {}, setAttribute(name, value) { this.attrs[name] = value; },
              };
            }
            const widget = Object.create(Widget.prototype);
            widget.sessionId = "session-1";
            widget.sessionRoute = { state: "live", live: true };
            widget.sessionClosed = false;
            widget.sessionRecoverable = false;
            widget.durableHistoryAvailable = false;
            widget.recoveryControlVisible = false;
            widget.recoveryControlLabel = "Recover session";
            widget.closePending = false;
            widget.els = {
              end: control(), restart: control(), recover: control(),
              history: control(), sessionActionStatus: control(),
            };

            // Live snapshots expose only End session.
            widget.renderSessionActions();
            if (widget.els.end.hidden || widget.els.end.disabled) throw new Error("live end hidden");
            if (!widget.els.restart.hidden || !widget.els.recover.hidden) throw new Error("invalid live action");
            if (widget.els.end.attrs["aria-disabled"] !== "false") throw new Error("live aria state");

            // SSE/local/End-all terminal state removes stale End immediately.
            widget.sessionClosed = true;
            widget.sessionRecoverable = true;
            widget.durableHistoryAvailable = true;
            widget.renderSessionActions();
            if (!widget.els.end.hidden || widget.els.restart.hidden) throw new Error("recoverable terminal actions");
            if (widget.els.restart.attrs["aria-disabled"] !== "false") throw new Error("restart aria state");
            if (!widget.els.sessionActionStatus.textContent.includes("ended")) throw new Error("missing explanation");

            // Nonrecoverable durable history exposes neither End nor Restart.
            widget.sessionRecoverable = false;
            widget.renderSessionActions();
            if (!widget.els.end.hidden || !widget.els.restart.hidden || widget.els.history.hidden) {
              throw new Error("nonrecoverable terminal actions");
            }

            // Switching back to a live session restores End.
            widget.sessionClosed = false;
            widget.durableHistoryAvailable = false;
            widget.renderSessionActions();
            if (widget.els.end.hidden || !widget.els.restart.hidden) throw new Error("live switch actions");

            // Owner-unreachable state suppresses destructive controls and offers retry.
            widget.sessionRoute = { state: "owner_unreachable" };
            widget.recoveryControlVisible = true;
            widget.recoveryControlLabel = "Retry connection";
            widget.renderSessionActions();
            if (!widget.els.end.hidden || widget.els.recover.hidden) throw new Error("owner unreachable actions");
            if (!widget.els.sessionActionStatus.textContent.includes("owner")) throw new Error("owner explanation");
            """
        )
        completed = subprocess.run(
            [shutil.which("node"), "-e", program, str(script)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_close_is_single_flight_and_restart_recovers_existing_identity(self) -> None:
        script = (
            Path(__file__).parents[1]
            / "src/pa/server/static/js/agent-chat.js"
        )
        source = script.read_text()
        close_block = source.split("AgentChatWidget.prototype.closeSession", 1)[1].split(
            "AgentChatWidget.prototype.retrySession", 1
        )[0]
        restart_block = source.split("AgentChatWidget.prototype.restartSession", 1)[1].split(
            "AgentChatWidget.prototype.queueControl", 1
        )[0]

        self.assertIn("this.closePending", close_block)
        self.assertIn("this.sessionClosed", close_block)
        self.assertIn("this.recoverSession(this.sessionId)", restart_block)
        self.assertNotIn('"/close"', restart_block)
        self.assertNotIn("this.init()", restart_block)


def test_template_starts_with_only_the_live_action_exposed() -> None:
    template = (
        Path(__file__).parents[1]
        / "src/pa/server/templates/partials/agent/chat-widget.html"
    ).read_text()
    assert 'data-acw-restart hidden' in template
    assert 'data-acw-session-action-status role="status" aria-live="polite"' in template
