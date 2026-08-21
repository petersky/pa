from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CSRF_SCRIPT = ROOT / "src" / "pa" / "server" / "static" / "js" / "csrf.js"


def test_shared_helper_loads_before_mutating_bundles_and_configures_htmx() -> None:
    shell = (ROOT / "src" / "pa" / "server" / "templates" / "shell.html").read_text()
    assert shell.index("js/csrf.js") < shell.index("js/agent-chat.js")
    assert shell.index("js/csrf.js") < shell.index("vendor/htmx")
    script = CSRF_SCRIPT.read_text()
    assert 'document.addEventListener("htmx:configRequest"' in script
    assert 'event.detail.headers[HEADER_NAME] = token' in script


def test_live_cookie_sync_and_bounded_idempotent_retry() -> None:
    program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
const listeners = {};
const meta = { content: "page-token" };
const hidden = { value: "page-token" };
global.document = {
  cookie: "pa_csrf=cookie-token",
  body: {},
  querySelector: function (selector) { return selector.indexOf("meta") >= 0 ? meta : null; },
  querySelectorAll: function (selector) { return selector.indexOf("_csrf") >= 0 ? [hidden] : []; },
  addEventListener: function (name, handler) { listeners[name] = handler; },
};
global.window = {
  addEventListener: function (name, handler) { listeners[name] = handler; },
};
global.Headers = Headers;
global.Response = Response;
let calls = [];
global.fetch = async function (path, options) {
  calls.push({ path, options });
  if (calls.length === 1) {
    document.cookie = "pa_csrf=rotated-token";
    return new Response(JSON.stringify({detail: {code: "csrf_mismatch"}}), {
      status: 403, headers: {"Content-Type": "application/json"}
    });
  }
  return new Response(JSON.stringify({accepted: true}), {
    status: 200, headers: {"Content-Type": "application/json"}
  });
};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));

(async function () {
  assert.strictEqual(meta.content, "cookie-token");
  assert.strictEqual(hidden.value, "cookie-token");
  const body = JSON.stringify({message: "hello", client_prompt_id: "prompt-stable"});
  const response = await window.PACSRF.fetch("/api/agent/sessions/session-1/prompt", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Idempotency-Key": "prompt-stable"},
    body: body,
  });
  assert.strictEqual(response.status, 200);
  assert.strictEqual(calls.length, 2);
  assert.strictEqual(calls[0].options.body, calls[1].options.body);
  assert.strictEqual(calls[0].options.headers["Idempotency-Key"], "prompt-stable");
  assert.strictEqual(calls[1].options.headers["Idempotency-Key"], "prompt-stable");
  assert.strictEqual(calls[1].options.headers["X-CSRF-Token"], "rotated-token");
  assert.strictEqual(meta.content, "rotated-token");
  assert.strictEqual(hidden.value, "rotated-token");
})().catch(function (error) { console.error(error); process.exitCode = 1; });
"""
    result = subprocess.run(
        ["node", "-e", program, str(CSRF_SCRIPT)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_non_idempotent_mutation_is_never_retried() -> None:
    program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
global.document = {
  cookie: "pa_csrf=current",
  body: {},
  querySelector: function () { return null; },
  querySelectorAll: function () { return []; },
  addEventListener: function () {},
};
global.window = { addEventListener: function () {} };
global.Headers = Headers;
global.Response = Response;
let calls = 0;
global.fetch = async function () {
  calls += 1;
  return new Response(JSON.stringify({detail: {code: "csrf_invalid"}}), {
    status: 403, headers: {"Content-Type": "application/json"}
  });
};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
(async function () {
  const response = await window.PACSRF.fetch("/api/settings", {method: "POST", body: "{}"});
  assert.strictEqual(response.status, 403);
  assert.strictEqual(calls, 1);
})().catch(function (error) { console.error(error); process.exitCode = 1; });
"""
    result = subprocess.run(
        ["node", "-e", program, str(CSRF_SCRIPT)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_failed_recovery_stops_after_one_retry() -> None:
    program = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
global.document = {
  cookie: "pa_csrf=current", body: {},
  querySelector: function () { return null; }, querySelectorAll: function () { return []; },
  addEventListener: function () {},
};
global.window = { addEventListener: function () {} };
global.Headers = Headers;
global.Response = Response;
let calls = 0;
global.fetch = async function () {
  calls += 1;
  return new Response(JSON.stringify({detail: {code: "csrf_invalid"}}), {
    status: 403, headers: {"Content-Type": "application/json"}
  });
};
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
(async function () {
  const response = await window.PACSRF.fetch("/api/agent/sessions/s/prompt", {
    method: "POST", headers: {"Idempotency-Key": "stable"}, body: "unchanged"
  });
  assert.strictEqual(calls, 2);
  assert.strictEqual(response.paCsrfRecoveryFailed, true);
})().catch(function (error) { console.error(error); process.exitCode = 1; });
"""
    result = subprocess.run(
        ["node", "-e", program, str(CSRF_SCRIPT)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
