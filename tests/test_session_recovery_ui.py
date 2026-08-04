from __future__ import annotations

import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path


class SessionRecoveryUiTests(unittest.TestCase):
    def test_retry_controller_covers_success_cancellation_and_real_failures(
        self,
    ) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is required for the session recovery UI harness")
        script = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "static"
            / "js"
            / "session-recovery.js"
        )
        harness = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const assert = require("assert");
            const window = {{}};
            const context = {{
              window, Date, Math, Promise, AbortController, DOMException,
              setTimeout, clearTimeout,
            }};
            vm.runInNewContext(fs.readFileSync({str(script)!r}, "utf8"), context);
            const Recovery = window.PASessionRecovery;

            function failure(code, status, retryAfterMs) {{
              const error = new Error(code);
              error.detail = {{ code, retry_after_ms: retryAfterMs || 0 }};
              error.status = status;
              error.retryAfterMs = retryAfterMs || 0;
              return error;
            }}

            (async () => {{
              const response = {{
                headers: {{ get: (name) => name === "Retry-After" ? "2" : null }},
              }};
              assert.strictEqual(
                Recovery.responseRetryAfterMs(response, {{ retry_after_ms: 250 }}),
                2000
              );

              let transientCalls = 0;
              let recovered = 0;
              let ready = null;
              const transient = new Recovery.Controller({{
                minimumMs: 1,
                maximumMs: 5,
                jitterRatio: 0,
                operation: () => {{
                  transientCalls += 1;
                  if (transientCalls === 1) {{
                    return Promise.reject(
                      failure("agent_recovery_in_progress", 503, 1)
                    );
                  }}
                  return Promise.resolve([{{ id: "ready" }}]);
                }},
                onRecovery: () => {{ recovered += 1; }},
                onSuccess: (value) => {{ ready = value; }},
              }});
              const first = transient.start();
              const duplicate = transient.start();
              assert.strictEqual(first, duplicate);
              await first;
              await new Promise((resolve) => setTimeout(resolve, 15));
              assert.strictEqual(transientCalls, 2);
              assert.strictEqual(recovered, 1);
              assert.strictEqual(ready[0].id, "ready");

              let aborted = false;
              let cancelCallbacks = 0;
              const pending = new Recovery.Controller({{
                operation: (signal) => new Promise((_resolve, reject) => {{
                  signal.addEventListener("abort", () => {{
                    aborted = true;
                    reject(new DOMException("aborted", "AbortError"));
                  }});
                }}),
                onRecovery: () => {{ cancelCallbacks += 1; }},
                onError: () => {{ cancelCallbacks += 1; }},
              }});
              const pendingRequest = pending.start();
              await Promise.resolve();
              pending.cancel("navigation");
              await pendingRequest;
              assert.strictEqual(aborted, true);
              assert.strictEqual(cancelCallbacks, 0);
              assert.strictEqual(pending.timer, null);

              const surfaced = [];
              let permanentCalls = 0;
              const permanent = new Recovery.Controller({{
                minimumMs: 1,
                maximumMs: 2,
                operation: () => {{
                  permanentCalls += 1;
                  return Promise.reject(
                    failure("agent_recovery_failed", 503)
                  );
                }},
                onError: (error) => surfaced.push(error.detail.code),
              }});
              await permanent.start();
              await new Promise((resolve) => setTimeout(resolve, 5));
              assert.strictEqual(permanentCalls, 1);

              let unreachableCalls = 0;
              const unreachable = new Recovery.Controller({{
                minimumMs: 1,
                maximumMs: 2,
                operation: () => {{
                  unreachableCalls += 1;
                  return Promise.reject(failure("peer_unreachable", 502));
                }},
                onError: (error) => surfaced.push(error.detail.code),
              }});
              await unreachable.start();
              await new Promise((resolve) => setTimeout(resolve, 5));
              assert.strictEqual(unreachableCalls, 1);
              assert.deepStrictEqual(
                surfaced,
                ["agent_recovery_failed", "peer_unreachable"]
              );
            }})().catch((error) => {{
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


if __name__ == "__main__":
    unittest.main()
