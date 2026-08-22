"""Browser-side regression coverage for agent-session SSE ownership."""

from __future__ import annotations

import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path


class AgentChatSseLifecycleTests(unittest.TestCase):
    def test_owner_lookup_deadline_does_not_depend_on_fetch_abort(self) -> None:
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
            const window = {{ addEventListener: noop }};
            const immediateTimeout = (callback) => {{ callback(); return 1; }};
            vm.runInNewContext(
              fs.readFileSync({str(script_path)!r}, "utf8"),
              {{
                window, document, console: {{ debug: noop }}, URL, AbortController,
                fetch: () => new Promise(() => {{}}),
                setTimeout: immediateTimeout, clearTimeout: noop,
              }}
            );
            const Widget = window.PAAgentChat.AgentChatWidget;
            const widget = Object.create(Widget.prototype);
            widget.routeAbortController = null;
            widget.resolveSessionRoute("session-1", "owner-1")
              .then(() => {{ throw new Error("owner lookup unexpectedly resolved"); }})
              .catch((error) => {{
                if (!String(error.message).includes("latency budget")) throw error;
              }});
            """
        )
        completed = subprocess.run(
            [node, "-e", harness], check=False, capture_output=True, text=True
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_known_owner_falls_back_to_live_snapshot_when_routing_fails(self) -> None:
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
              body: {{ addEventListener: noop }}, addEventListener: noop,
              querySelector: () => null, querySelectorAll: () => [],
            }};
            const window = {{
              addEventListener: noop,
              location: {{ href: "http://127.0.0.1:8080/agent" }},
              history: {{ replaceState: noop, pushState: noop }},
            }};
            vm.runInNewContext(
              fs.readFileSync({str(script_path)!r}, "utf8"),
              {{ window, document, console: {{ debug: noop }}, URL, AbortController,
                 setTimeout, clearTimeout }}
            );
            const Widget = window.PAAgentChat.AgentChatWidget;
            const widget = Object.create(Widget.prototype);
            let liveBase = "";
            Object.assign(widget, {{
              destroyed: false, subscriptionGeneration: 0,
              currentInstanceId: "local", apiBase: "/api/agent",
              root: {{ dataset: {{}}, closest: () => null }},
              els: {{ promote: null }}, drafts: null,
              _setRecoveryControl: noop, showRecoveryActions: noop,
              setPlaceholder: noop, setComposerEnabled: noop,
              retryAfterStartupRecovery: () => false,
              resolveSessionRoute: () => Promise.reject(new Error("route timeout")),
              _loadLiveSnapshot: () => {{
                liveBase = widget.apiBase;
                return Promise.resolve({{ connected: true }});
              }},
            }});
            widget.openSession("session-1", "remote-owner", {{ replace: true }})
              .then(() => {{
                if (liveBase !== "/api/fleet/instances/remote-owner/agent") {{
                  throw new Error("known-owner fallback used " + liveBase);
                }}
                if (widget.sessionRoute.state !== "live_degraded") {{
                  throw new Error("fallback did not record degraded routing");
                }}
              }})
              .catch((error) => {{
                process.stderr.write(error.stack || String(error));
                process.exitCode = 1;
              }});
            """
        )
        completed = subprocess.run(
            [node, "-e", harness], check=False, capture_output=True, text=True
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_switch_clears_rendered_transcript_before_owner_lookup(self) -> None:
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
            const window = {{ addEventListener: noop, confirm: () => true }};
            vm.runInNewContext(
              fs.readFileSync({str(script_path)!r}, "utf8"),
              {{ window, document, console: {{ debug: noop }}, URL, setTimeout, clearTimeout }}
            );
            const Widget = window.PAAgentChat.AgentChatWidget;
            const widget = Object.create(Widget.prototype);
            const calls = [];
            Object.assign(widget, {{
              sessionId: "session-old",
              ownerInstanceId: "local",
              settingsDirty: false,
              transcriptEvents: [{{ seq: 1 }}],
              seenEvents: {{ "seq:1": true }},
              hasOlder: true,
              olderCursor: 1,
              loadingOlder: false,
              olderError: "",
              streaming: {{ old: true }},
              lastSnapshot: {{ session: {{ id: "session-old" }} }},
              closeSSE: () => calls.push("close"),
              updateOlderControl: noop,
              renderTranscript: (events) => calls.push("render:" + events.length),
              setPlaceholder: (message) => calls.push("placeholder:" + message),
              openSession: () => {{
                calls.push("open");
                return Promise.resolve(null);
              }},
            }});
            widget.switchSession("session-new", true, "local");
            if (calls.join("|") !== "close|render:0|placeholder:Loading session…|open") {{
              throw new Error("session transition order was " + calls.join("|"));
            }}
            if (widget.lastSnapshot !== null) throw new Error("stale snapshot was retained");
            """
        )
        completed = subprocess.run(
            [node, "-e", harness], check=False, capture_output=True, text=True
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_live_session_route_does_not_wait_for_durable_history(self) -> None:
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
            const snapshotBases = [];
            let historyLoads = 0;
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
              apiWithTimeout: () => {{
                historyLoads += 1;
                return new Promise(() => {{}});
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
              _loadLiveSnapshot: () => {{
                snapshotBases.push(widget.apiBase);
                return Promise.resolve(null);
              }},
            }});
            widget.openSession("session-local", "macbook-id", {{ replace: true }})
              .then(() => {{
                if (snapshotBases[0] !== "/api/agent") {{
                  throw new Error("local snapshot used wrong API base: " + snapshotBases[0]);
                }}
                return widget.openSession("session-remote", "monica-id", {{ replace: true }});
              }})
              .then(() => {{
                if (snapshotBases[1] !== "/api/fleet/instances/monica-id/agent") {{
                  throw new Error("remote snapshot used wrong API base: " + snapshotBases[1]);
                }}
                if (historyLoads !== 0) {{
                  throw new Error("live routing started a blocking durable-history request");
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

    def test_failed_stale_recovery_cannot_overwrite_new_session(self) -> None:
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
            let rejectRecovery;
            let durableLoads = 0;
            let composerEnabled = true;
            Object.assign(widget, {{
              destroyed: false,
              subscriptionGeneration: 4,
              sessionId: "stale-session",
              sessionRoute: {{ state: "recoverable" }},
              root: {{ dataset: {{ sessionId: "stale-session" }} }},
              els: {{}},
              startupRetryId: null,
              _setRecoveryControl: noop,
              showRecoveryActions: noop,
              setPlaceholder: noop,
              setStatus: noop,
              setComposerEnabled: (enabled) => {{ composerEnabled = enabled; }},
              retryAfterStartupRecovery: () => false,
              api: () => new Promise((_resolve, reject) => {{ rejectRecovery = reject; }}),
              loadDurableSession: () => {{
                durableLoads += 1;
                return Promise.resolve(null);
              }},
              addBubble: noop,
            }});
            const notLive = new Error("session is not live");
            notLive.detail = {{ code: "session_not_live", recoverable: true }};
            const pending = widget.resolveSessionNotLive(
              notLive, "stale-session", 4
            );
            widget.sessionId = "good-session";
            widget.root.dataset.sessionId = "good-session";
            widget.subscriptionGeneration = 5;
            composerEnabled = true;
            rejectRecovery(new Error("stale provider is unavailable"));
            pending.then(() => {{
              if (widget.sessionId !== "good-session") {{
                throw new Error("stale recovery replaced the new selection");
              }}
              if (durableLoads !== 0) {{
                throw new Error("stale recovery loaded history into the new selection");
              }}
              if (!composerEnabled) {{
                throw new Error("stale recovery disabled the new session composer");
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

    def test_local_owner_uses_agent_api_even_when_current_instance_unknown(self) -> None:
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
            const window = {{ addEventListener: noop }};
            vm.runInNewContext(
              fs.readFileSync({str(script_path)!r}, "utf8"),
              {{ window, document, console: {{ debug: noop }}, URL, setTimeout, clearTimeout }}
            );
            const Widget = window.PAAgentChat.AgentChatWidget;
            const widget = Object.create(Widget.prototype);
            widget.currentInstanceId = "";
            widget.defaultApiBase = "/api/agent";
            widget.apiBase = "/api/agent";
            const local = widget._apiBaseForOwner("local-instance-id");
            const remote = Object.assign(Object.create(Widget.prototype), {{
              currentInstanceId: "local-instance-id",
              defaultApiBase: "/api/agent",
            }})._apiBaseForOwner("remote-instance-id");
            if (local !== "/api/agent") throw new Error("unknown current still fleet-proxied: " + local);
            if (remote !== "/api/fleet/instances/remote-instance-id/agent") {{
              throw new Error("remote owner did not use fleet proxy: " + remote);
            }}
            """
        )
        completed = subprocess.run(
            [node, "-e", harness], check=False, capture_output=True, text=True
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_missing_route_does_not_autorestart_in_a_tight_loop(self) -> None:
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
            let initCalls = 0;
            const timeouts = [];
            vm.runInNewContext(
              fs.readFileSync({str(script_path)!r}, "utf8"),
              {{
                window, document, console: {{ debug: noop }}, URL, AbortController,
                setTimeout: (callback) => {{ timeouts.push(callback); return timeouts.length; }},
                clearTimeout: noop,
              }}
            );
            const Widget = window.PAAgentChat.AgentChatWidget;
            const widget = Object.create(Widget.prototype);
            Object.assign(widget, {{
              destroyed: false,
              subscriptionGeneration: 0,
              autoStart: true,
              _missingRestartAttempted: false,
              sessionId: "gone",
              ownerInstanceId: "peer",
              currentInstanceId: "local",
              defaultApiBase: "/api/agent",
              apiBase: "/api/fleet/instances/peer/agent",
              root: {{
                dataset: {{ apiBase: "/api/fleet/instances/peer/agent" }},
                closest: () => ({{}})
              }},
              els: {{ promote: null }},
              drafts: null,
              liveStateRetryId: null,
              startupRetryId: null,
              routeAbortController: null,
              _setRecoveryControl: noop,
              showRecoveryActions: noop,
              setPlaceholder: noop,
              setComposerEnabled: noop,
              setStatus: noop,
              closeSSE: noop,
              setTurnActive: noop,
              resolveSessionRoute: () => Promise.resolve({{
                state: "missing",
                live: false,
                message: "This agent session was deleted or has expired.",
              }}),
              init: () => {{ initCalls += 1; }},
            }});
            widget.clearSelectedSession = Widget.prototype.clearSelectedSession;
            widget._apiBaseForOwner = Widget.prototype._apiBaseForOwner;
            widget._writeSessionUrl = Widget.prototype._writeSessionUrl;
            widget.openSession("gone", "peer", {{ replace: true }})
              .then(() => {{
                timeouts.splice(0).forEach((callback) => callback());
                if (initCalls !== 1) throw new Error("expected one auto-restart, got " + initCalls);
                if (widget.apiBase !== "/api/agent") {{
                  throw new Error("missing route left poisoned apiBase: " + widget.apiBase);
                }}
                return widget.openSession("gone", "peer", {{ replace: true }});
              }})
              .then(() => {{
                const before = initCalls;
                const pending = timeouts.splice(0);
                pending.forEach((callback) => callback());
                if (initCalls !== before || pending.length !== 0) {{
                  throw new Error(
                    "missing route restarted again after the guard (inits=" +
                    initCalls + " pending=" + pending.length + ")"
                  );
                }}
              }})
              .catch((error) => {{
                console.error(error && error.stack || error);
                process.exit(1);
              }});
            """
        )
        completed = subprocess.run(
            [node, "-e", harness], check=False, capture_output=True, text=True
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_spa_and_modal_teardown_hooks_are_present(self) -> None:
        static = Path(__file__).parents[1] / "src" / "pa" / "server" / "static" / "js"
        agent_chat = (static / "agent-chat.js").read_text()
        spa = (static / "spa.js").read_text()

        self.assertIn('destroyAll(target || document, "spa-swap")', agent_chat)
        self.assertIn('destroyAll(document, "pagehide")', agent_chat)
        self.assertIn('closeAll(document, "pagehide-persisted")', agent_chat)
        self.assertIn("const SESSION_ROUTE_TIMEOUT_MS = 4000", agent_chat)
        self.assertIn("signal: controller.signal", agent_chat)
        self.assertIn("this.routeAbortController.abort()", agent_chat)
        self.assertIn("root._acw.connectSSE()", agent_chat)
        self.assertIn('window.PAAgentChat.destroy(content, "card-closed")', spa)
        self.assertIn('window.PAAgentChat.destroy(content, "card-replaced")', spa)
