const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const noop = () => {};
const document = {
  body: {addEventListener: noop}, addEventListener: noop,
  querySelector: () => null, querySelectorAll: () => [],
};
const window = {addEventListener: noop};
vm.runInNewContext(fs.readFileSync(process.argv[2], 'utf8'), {
  window, document, console, URL, setTimeout, clearTimeout,
});
const Widget = window.PAAgentChat.AgentChatWidget;
const widget = Object.create(Widget.prototype);
const action = {textContent: ''};
Object.defineProperty(action, 'innerHTML', {
  set() { throw new Error('Recovery details must be rendered as plain text'); },
});
let placeholder = '';
Object.assign(widget, {
  sessionId: 'blocked-fixture', commandCatalogSession: 'blocked-fixture',
  transcriptEvents: [], els: {recovery: {}, recoveryAction: action, recoveryRetry: {}, input: {}},
  setComposerEnabled(enabled) { this.composerEnabled = enabled; },
  setPlaceholder(message) { placeholder = message; },
  clearPlaceholder() { placeholder = ''; },
  renderTranscript(events) { this.transcriptEvents = events; },
  renderSessionActions: noop, setTurnActive: noop, setStatus: noop, renderMcpHealth: noop,
  renderQueue: noop, renderModelsModes: noop, renderConfigOptions: noop, renderMetrics: noop,
  addBubble: noop,
  api() { throw new Error('Rendering recovery guidance must not mutate the session'); },
});
const fallback = 'Recovery could not complete. Retry this session or inspect its diagnostics for details.';
const contextRemedy = 'The original provider context is unavailable. Continue in a new linked chat to preserve saved history with an explicit context boundary.';
const projectRemedy = 'Sync the project and repository links to this instance, then retry this session.';
const cases = [
  {
    name: 'lost provider context in a projectless session with a ready workspace',
    session: {project_id: null, config_json: {provisioning: {state: 'ready'}},
      recovery_json: {context_lost: true, remedy: contextRemedy}},
    expected: contextRemedy,
  },
  {
    name: 'current authentication failure supersedes old project guidance',
    session: {config_json: {provisioning: {state: 'blocked', action: projectRemedy}},
      recovery_json: {code: 'provider_auth_required', remedy: 'Sign in to the provider, then retry.'}},
    expected: 'Sign in to the provider, then retry.',
  },
  {
    name: 'workspace binding recovery',
    session: {recovery_json: {code: 'workspace_binding_mismatch', remedy: 'Restore the original workspace binding, then retry.'}},
    expected: 'Restore the original workspace binding, then retry.',
  },
  {
    name: 'recovery guidance from a peer presentation',
    presentation: {recovery: {remedy: 'Repair the provider configuration on the session owner.'}},
    expected: 'Repair the provider configuration on the session owner.',
  },
  {
    name: 'legacy project provisioning guidance',
    session: {config_json: {provisioning: {state: 'blocked', action: projectRemedy}}},
    expected: projectRemedy,
  },
  {
    name: 'unknown future failure with only recorded error details',
    session: {recovery_json: {code: 'future_error', last_error: 'Provider rejected <session> & requires attention.'}},
    expected: 'Provider rejected <session> & requires attention.',
  },
  {
    name: 'provisioning error without an action',
    session: {config_json: {provisioning: {state: 'blocked', error: 'Checkout path is missing.'}}},
    expected: 'Checkout path is missing.',
  },
  {name: 'missing recovery metadata', expected: fallback},
];

(async () => {
  for (const fixture of cases) {
    for (const history of [false, true]) {
      const snapshot = {
        session: {status: 'recovery_blocked', ...fixture.session},
        presentation: fixture.presentation,
      };
      widget.transcriptEvents = [];
      widget.applySnapshot(history ? widget._historySnapshot(snapshot) : snapshot);
      const name = fixture.name + (history ? ' (saved history)' : ' (live snapshot)');
      assert.equal(widget.els.recovery.hidden, false, name);
      assert.equal(action.textContent, fixture.expected, name);
      assert.equal(placeholder, fixture.expected, name + ' empty chat guidance');
      assert.equal(widget.composerEnabled, false, name);
      assert.equal(widget.els.recoveryRetry.disabled, false, name);
    }
  }

  // A failed manual retry must replace the preceding failure's guidance, even
  // when a future error supplies only a message rather than a known remedy.
  for (const detail of [
    {action: 'Reconnect the provider account, then retry.', message: 'Credentials expired.'},
    {message: 'A new provider failure requires attention.'},
  ]) {
    let calls = 0;
    widget.api = async (path, options) => {
      calls += 1;
      assert.equal(path, '/sessions/blocked-fixture/retry');
      assert.equal(options.method, 'POST');
      throw Object.assign(new Error(detail.message), {detail});
    };
    widget.retrySession();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(calls, 1);
    assert.equal(action.textContent, detail.action || detail.message);
    assert.equal(widget.els.recoveryRetry.disabled, false);
  }

  widget.applySnapshot({session: {status: 'idle'}, connected: true});
  assert.equal(widget.els.recovery.hidden, true, 'successful recovery hides the banner');
  assert.equal(action.textContent, '', 'old failure details are cleared');
  assert.equal(widget.composerEnabled, true);
  console.log('PASS recovery guidance for live/history snapshots, future errors, retries, and recovery');
})().catch(error => { console.error(error); process.exitCode = 1; });
