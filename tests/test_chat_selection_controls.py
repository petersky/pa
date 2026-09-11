"""Run the actual selection UI against a small DOM event harness."""

import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(not shutil.which("node"), reason="node required")
def test_chat_selection_preserves_required_preferred_and_automatic_intents():
    script = (
        Path(__file__).parents[1] / "src/pa/server/static/js/execution-selection.js"
    )
    program = r"""
const fs = require('fs'), vm = require('vm'), assert = require('assert');
class Element {
  constructor(tag) { this.tag = tag; this.children = []; this.options = []; this.handlers = {}; this.attrs = {}; this.value = ''; }
  append(...children) { this.children.push(...children); }
  add(option, index) { this.options.splice(index == null ? this.options.length : index, 0, option); }
  replaceChildren() { this.children = []; }
  setAttribute(key, value) { this.attrs[key] = value; }
  addEventListener(name, fn) { (this.handlers[name] ||= []).push(fn); }
  removeEventListener(name, fn) { this.handlers[name] = (this.handlers[name] || []).filter(x => x !== fn); }
  dispatchEvent(event) { for (const fn of this.handlers[event.type] || []) fn(event); }
  focus() {}
  setCustomValidity() {}
}
const fields = new Element('div'), output = new Element('input'), notice = new Element('p');
const refresh = new Element('button');
const staleError = new Element('p'), effective = new Element('p');
let failPreview = false, rateLimited = false;
const form = new Element('form'); form.elements = {};
form.querySelector = () => staleError;
const root = {dataset: {selectionPurpose: 'new-session'}, closest: () => form,
  querySelector: selector => ({'[data-selection-fields]': fields, '[data-selection-notice]': notice,
    'input[name="execution_preferences"]': output, '[data-selection-refresh]': refresh, '[data-selection-effective]': effective}[selector] || null),
  querySelectorAll: () => []};
const document = {cookie: '', documentElement: {}, addEventListener() {},
  querySelectorAll: () => [root], createElement: tag => new Element(tag)};
const window = {};
vm.runInNewContext(fs.readFileSync(process.argv[1], 'utf8'), {window, document,
  URLSearchParams, Event, crypto: {randomUUID: () => 'test'},
  Option: function(text, value) {this.textContent = text; this.value = value;},
  MutationObserver: class {observe() {}},
  fetch: async path => ({ok: !(failPreview && path.includes('/preview')), json: async () => path.includes('/defaults') ? {layers: []} : {
    detail: {message: 'still rejected'}, selected: {harness: 'codex', model: 'deep'},
    refresh: {state: rateLimited ? 'rate_limited' : 'refreshed', retry_after_seconds: 3},
    candidates: [{harness: 'codex', connection: 'default', model: 'deep'}]}})});
window.PAExecutionSelection.scan();
function find(label) {
  function walk(node) { if (node.attrs && node.attrs['aria-label'] === label) return node;
    for (const child of node.children || []) {const found = walk(child); if (found) return found;} }
  const found = walk(fields); assert.ok(found, label); return found;
}
function select(label, value) { const el = find(label); el.value = value; el.dispatchEvent(new Event('change')); }
setImmediate(async () => {
  try {
    select('Model', JSON.stringify('deep'));
    assert.deepStrictEqual(JSON.parse(output.value).model, {intent: 'required', value: 'deep'});
    select('Model intent', 'preferred');
    assert.deepStrictEqual(JSON.parse(output.value).model, {intent: 'preferred', value: 'deep'});
    select('Model', 'automatic');
    assert.deepStrictEqual(JSON.parse(output.value).model, {intent: 'automatic'});
    select('Agent', JSON.stringify('codex'));
    select('Agent harness intent', 'preferred');
    assert.deepStrictEqual(JSON.parse(output.value).harness, {intent: 'preferred', value: 'codex'});
    select('Agent', 'inherit');
    assert.deepStrictEqual(JSON.parse(output.value).harness, {intent: 'inherit'});
    staleError.hidden = false; staleError.textContent = 'old failed Start chat';
    failPreview = true; await root._selection.preview();
    assert.strictEqual(staleError.hidden, false, 'failed preview retains failure');
    failPreview = false; await root._selection.preview();
    assert.strictEqual(staleError.hidden, true, 'successful preview clears stale submit failure');
    assert.strictEqual(staleError.textContent, '');
    staleError.hidden = false; staleError.textContent = 'old failed Start chat';
    output.dispatchEvent(new Event('change'));
    assert.strictEqual(staleError.hidden, true, 'preference edit clears obsolete error');
    staleError.hidden = false;
    const modeChange = new Event('change'); Object.defineProperty(modeChange, 'target', {value: {name: 'mode_id'}}); form.dispatchEvent(modeChange);
    assert.strictEqual(staleError.hidden, true, 'permission edit clears obsolete error');
    staleError.hidden = false; rateLimited = true;
    refresh.dispatchEvent(new Event('click'));
    await new Promise(setImmediate);
    assert.strictEqual(staleError.hidden, true, 'successful refresh preview clears error');
    assert.ok(notice.textContent.includes('Wait 3 seconds'), notice.textContent);
  } catch(error) { console.error(error); process.exitCode = 1; }
});
"""
    result = subprocess.run(
        ["node", "-e", program, str(script)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
