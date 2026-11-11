"""Behaviour of the Goals page script, run under Node against a minimal DOM."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "src/pa/server/static/js/goals.js"

HARNESS = r"""
const fs = require("fs");
const calls = [];
let version = 1;
const listeners = {};
let reloads = 0;
class FakeFormData {
  constructor(form) { this.fields = form.fields; }
  getAll(name) { return this.fields.filter((f) => f[0] === name).map((f) => f[1]); }
  [Symbol.iterator]() { return this.fields[Symbol.iterator](); }
}
global.FormData = FakeFormData;
global.document = {
  readyState: "complete", body: null,
  addEventListener(type, fn) { listeners[type] = fn; },
  querySelectorAll() { return []; }, querySelector() { return null; },
  getElementById() { return null; },
};
global.window = {
  location: { hash: "", href: "http://pa.test/goals" },
  PACSRF: { fetch: async (url, options) => {
    calls.push({ url, body: JSON.parse(options.body) });
    version += 1;
    return { ok: true, json: async () => ({ version, policy: { revision: 1 } }) };
  } },
};
global.location = { reload() { reloads += 1; }, href: "http://pa.test/goals", hash: "" };
global.history = { state: null, replaceState() {} };
global.sessionStorage = { getItem() { return ""; }, setItem() {}, removeItem() {} };
global.crypto = { randomUUID: () => "key-" + Math.random() };
global.CSS = { escape: (value) => value };
eval(fs.readFileSync(process.env.GOALS_JS, "utf8"));

function detail() {
  return { dataset: { goalDetail: "g1", version: "1", policy: "1" }, querySelector() { return null; } };
}
function element(selector, dataset, owner) {
  return {
    dataset, disabled: false, setAttribute() {}, removeAttribute() {},
    closest(query) { return query === selector ? this : (query === "[data-goal-detail]" ? owner : null); },
  };
}
async function click(selector, dataset) {
  const owner = detail();
  const button = element(selector, dataset, owner);
  await listeners.click({ target: { closest: (query) => (query === selector ? button : null) } });
  return owner;
}
async function submit(selector, fields, owner, extra) {
  const form = Object.assign({
    fields, dataset: extra || {},
    matches(query) { return query.includes(selector); },
    closest(query) { return query === "[data-goal-detail]" ? owner || null : null; },
    querySelector() { return { disabled: false, setAttribute() {}, removeAttribute() {}, textContent: "" }; },
  });
  await listeners.submit({ target: form, preventDefault() {} });
}

(async () => {
  const out = {};
  await click("[data-goal-transition]", { goalTransition: "active", goalVia: "ready", goalReason: "Activated by operator" });
  out.activate = calls.splice(0);

  await click("[data-goal-transition]", { goalTransition: "paused", goalReason: "Paused by operator" });
  out.pause = calls.splice(0);

  await click("[data-goal-retry]", { goalRetry: "pkg-1" });
  out.retry = calls.splice(0);

  await submit("[data-goal-create-form]", [
    ["objective", "Ship it"], ["motivation", ""], ["criterion", "works"],
    ["verification", "tests"], ["requirement", "green run"],
    ["autonomy_level", "3"], ["permitted_action", "create_work_package"],
    ["permitted_action", "dispatch_work_package"],
  ], null);
  out.create = calls.splice(0);

  const owner = detail();
  await submit("[data-goal-policy-form]", [
    ["autonomy_level", "4"], ["permitted_action", "create_work_package"], ["reason", "more autonomy"],
  ], owner, { policy: JSON.stringify({ revision: 1, autonomy_level: 3, permitted_actions: [], prohibited_actions: ["x"] }) });
  out.policy = calls.splice(0);

  const audit = detail();
  await submit("[data-goal-audit-form]", [
    ["evidence_id", "e1"], ["criterion_id", "c1"], ["criterion_id", "c2"],
    ["explanation", "looks right"], ["verdict", "satisfied"],
  ], audit);
  out.audit = calls.splice(0);
  out.reloads = reloads;
  process.stdout.write(JSON.stringify(out));
})().catch((error) => { console.error(error); process.exit(1); });
"""


@pytest.fixture(scope="module")
def outcome() -> dict:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run(
        [node, "-e", HARNESS],
        env={"GOALS_JS": str(SCRIPT), "PATH": ""},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_activate_chains_ready_then_active_with_fresh_versions(outcome) -> None:
    first, second = outcome["activate"]
    assert "/transition?expected_version=1&policy_revision=1" in first["url"]
    assert first["body"]["state"] == "ready"
    # The second call uses the version returned by the first.
    assert "/transition?expected_version=2&policy_revision=1" in second["url"]
    assert second["body"]["state"] == "active"
    assert second["body"]["reason"] == "Activated by operator"


def test_pause_is_a_single_transition_with_its_own_reason(outcome) -> None:
    (call,) = outcome["pause"]
    assert call["body"] == {"state": "paused", "reason": "Paused by operator"}


def test_retry_posts_to_the_work_package_retry_endpoint(outcome) -> None:
    (call,) = outcome["retry"]
    assert "/api/goals/g1/work-packages/pkg-1/retry?" in call["url"]


def test_create_form_sends_the_chosen_policy(outcome) -> None:
    (call,) = outcome["create"]
    policy = call["body"]["policy"]
    assert policy["autonomy_level"] == 3
    assert policy["revision"] == 1
    assert policy["permitted_actions"] == [
        "request_operator",
        "revise_goal",
        "create_work_package",
        "dispatch_work_package",
    ]


def test_policy_form_revises_the_policy_keeping_unrelated_fields(outcome) -> None:
    (call,) = outcome["policy"]
    assert "/revisions?" in call["url"]
    policy = call["body"]["policy"]
    assert policy["revision"] == 2
    assert policy["autonomy_level"] == 4
    assert policy["prohibited_actions"] == ["x"]
    assert "request_operator" in policy["permitted_actions"]
    assert call["body"]["reason"] == "more autonomy"


def test_audit_form_gives_a_verdict_for_every_criterion(outcome) -> None:
    (call,) = outcome["audit"]
    assert call["body"]["criterion_verdicts"] == {
        "c1": "satisfied",
        "c2": "satisfied",
    }
    assert call["body"]["evidence_ids"] == ["e1"]
