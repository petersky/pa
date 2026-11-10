// Drives the real agent-chat.js with a tiny fake DOM to check transcript
// rendering: optimistic prompt bubbles, event registration, held-event queue
// refresh and direct-stream reconnect. Usage: node <harness> <agent-chat.js> <scenario>
const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const noop = () => {};

const camel = name => name.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
function matchesSimple(el, simple) {
  const re = /(\.[\w-]+)|(\[[\w-]+(?:="[^"]*")?\])|^([a-z]+)/g;
  let m;
  while ((m = re.exec(simple))) {
    if (m[1]) { if (!(el.className || '').split(/\s+/).includes(m[1].slice(1))) return false; }
    else if (m[2]) {
      const [, name, value] = /\[([\w-]+)(?:="([^"]*)")?\]/.exec(m[2]);
      const have = el.dataset[camel(name.replace(/^data-/, ''))];
      if (value === undefined ? have === undefined : have !== value) return false;
    } else if (m[3]) { if (el.tagName !== m[3]) return false; }
  }
  return true;
}
class Element {
  constructor(tag = 'div') {
    this.tagName = tag; this.children = []; this.dataset = {}; this.attributes = {}; this.className = '';
    this.classList = {add: c => { this.className = (this.className + ' ' + c).trim(); },
      toggle: noop, contains: c => this.className.split(/\s+/).includes(c), remove: noop};
    this.hidden = false; this.parentNode = null; this.listeners = {};
  }
  appendChild(child) { child.remove(); child.parentNode = this; this.children.push(child); return child; }
  insertBefore(child, ref) {
    if (child === ref) return child;
    child.remove(); child.parentNode = this;
    const index = ref ? this.children.indexOf(ref) : -1;
    if (index < 0) this.children.push(child); else this.children.splice(index, 0, child);
    return child;
  }
  get nextSibling() {
    if (!this.parentNode) return null;
    return this.parentNode.children[this.parentNode.children.indexOf(this) + 1] || null;
  }
  get firstChild() { return this.children[0] || null; }
  remove() { if (this.parentNode) this.parentNode.children = this.parentNode.children.filter(c => c !== this); this.parentNode = null; }
  hasAttribute(key) { return key in this.attributes; }
  setAttribute(key, value) { this.attributes[key] = value; }
  removeAttribute(key) { delete this.attributes[key]; }
  addEventListener(type, handler) { this.listeners[type] = handler; }
  get textContent() { return this._text || ''; }
  set textContent(value) { this._text = value; }
  set innerHTML(value) { this.children = []; }
  get innerHTML() { return ''; }
  descendants() { return this.children.flatMap(c => [c, ...c.descendants()]); }
  querySelectorAll(selector) {
    const parts = selector.trim().split(/\s+/);
    return this.descendants().filter(el => {
      if (!matchesSimple(el, parts[parts.length - 1])) return false;
      let node = el.parentNode, index = parts.length - 2;
      while (index >= 0 && node && node !== this.parentNode) {
        if (matchesSimple(node, parts[index])) index -= 1;
        node = node.parentNode;
      }
      return index < 0;
    });
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  closest(selector) { let n = this; while (n) { if (matchesSimple(n, selector)) return n; n = n.parentNode; } return null; }
}

const timers = new Map();
let timerId = 0;
const setTimeoutStub = (callback, delay) => { const id = ++timerId; timers.set(id, {callback, delay}); return id; };
const clearTimeoutStub = id => timers.delete(id);
class EventSource {
  static CLOSED = 2;
  static instances = [];
  constructor(url) { this.url = url; this.readyState = 0; this.listeners = {}; EventSource.instances.push(this); }
  close() { this.readyState = 2; }
  addEventListener(type, handler) { this.listeners[type] = handler; }
  deliver(event) {
    // A browser only dispatches a named SSE event to a registered listener.
    const handler = this.listeners[event.type];
    if (handler) handler({data: JSON.stringify(event)});
    return !!handler;
  }
}
const document = {
  body: {addEventListener: noop}, addEventListener: noop, hidden: false,
  querySelector: () => null, querySelectorAll: () => [],
  createElement: tag => new Element(tag), createTextNode: text => ({textContent: text, remove: noop}),
};
const window = {addEventListener: noop};
vm.runInNewContext(fs.readFileSync(process.argv[2], 'utf8'), {
  window, document, console: {debug: noop, log: console.log, error: console.error, warn: noop},
  URL, EventSource, AbortController, setTimeout: setTimeoutStub, clearTimeout: clearTimeoutStub,
  setInterval: () => 0, clearInterval: noop, performance,
});
const Widget = window.PAAgentChat.AgentChatWidget;

function make(extra = {}) {
  const widget = Object.create(Widget.prototype), messages = new Element();
  const calls = {refreshQueue: 0, repairLiveGap: 0, turn: []};
  Object.assign(widget, {
    els: {messages, input: {value: ''}}, root: new Element(), sessionId: 's', apiBase: '/api/agent',
    subscriptionGeneration: 1, transcriptEvents: [], seenEvents: {}, lastSeq: 0, streaming: {},
    activityStreams: {}, toolTimers: {}, plans: [], providerId: 'codex', messageRowCount: 0, rawText: true,
    resetArtifacts: noop, clearPlaceholder: noop, setPlaceholder: noop, isNearBottom: () => true,
    scrollToBottom: noop, finalizeActivity: noop, finalizeStreams: noop, upsertTool: noop,
    setStatus: noop, renderQueue: noop, renderMetrics: noop, decorateMentions: noop,
    setSubmissionState: noop, _pruneMessageRows: noop,
    setTurnActive(active) { this.turnActive = active; calls.turn.push(active); },
    refreshQueue() { calls.refreshQueue += 1; },
    _repairLiveGap() { calls.repairLiveGap += 1; },
    appendStream(role, key, text, ts) { this.addBubble('agent', text, ts); },
  }, extra);
  widget.calls = calls;
  return widget;
}
const userTexts = w => w.els.messages.querySelectorAll('.acw-msg-user .acw-bubble').map(b => b.dataset.markdown);
const rowKinds = w => w.els.messages.children.map(r => r.className.includes('acw-msg-user')
  ? 'user:' + r.children[0].dataset.markdown : 'agent:' + r.children[0].dataset.markdown);
const ev = (seq, type, payload = {}) => ({seq, session_id: 's', type, payload, created_at: '2026-01-01T00:00:00Z'});
const accepted = (over = {}) => Object.assign({accepted: true, queued: false, started: true, duplicate: false, stop_reason: 'started'}, over);
const fireAll = () => { for (const [id, t] of Array.from(timers)) { timers.delete(id); t.callback(); } };

const scenarios = {
  // Items 1: A is running, B queued behind it. Draining A must not repaint A.
  queued_prompts_keep_one_bubble_each() {
    const w = make();
    w.markSubmissionAccepted(accepted(), 'A', [], 'A', 'idA');
    w.markSubmissionAccepted(accepted({queued: true, started: false, stop_reason: 'queued'}), 'B', [], 'B', 'idB');
    assert.deepEqual(userTexts(w), ['A', 'B']);
    w.handleEvent(ev(1, 'prompt_rendered', {id: 'idA'}));
    w.handleEvent(ev(2, 'user_message', {id: 'idA', message: 'A'}));
    assert.deepEqual(userTexts(w), ['A', 'B'], 'A must not be painted twice while B is still optimistic');
    w.handleEvent(ev(3, 'agent_message_chunk', {message_id: 'm1', text: 'answer A', content_mode: 'delta'}));
    w.handleEvent(ev(4, 'turn_completed'));
    w.handleEvent(ev(5, 'user_message', {id: 'idB', message: 'B'}));
    assert.deepEqual(rowKinds(w), ['user:A', 'agent:answer A', 'user:B'],
      'B belongs after the answer to A, not where it was painted mid-turn');
    assert.equal(w.els.messages.querySelectorAll('.acw-msg[data-prompt-id="idB"]')[0].dataset.sourceSeq, '5');
  },
  // Item 1: identical text with a different prompt id is a different message.
  repeated_identical_prompts_are_all_shown() {
    const w = make();
    w.markSubmissionAccepted(accepted(), 'continue', [], 'continue', 'c1');
    w.handleEvent(ev(1, 'user_message', {id: 'c1', message: 'continue'}));
    w.handleEvent(ev(2, 'turn_completed'));
    w.markSubmissionAccepted(accepted(), 'continue', [], 'continue', 'c2');
    assert.deepEqual(userTexts(w), ['continue', 'continue'], 'second identical send is visible live');
    w.handleEvent(ev(3, 'user_message', {id: 'c2', message: 'continue'}));
    assert.deepEqual(userTexts(w), ['continue', 'continue'], 'server echo adopts the optimistic bubble');
    // The server event can also beat the POST response.
    const early = make();
    early.handleEvent(ev(1, 'user_message', {id: 'c9', message: 'hi'}));
    early.markSubmissionAccepted(accepted(), 'hi', [], 'hi', 'c9');
    assert.deepEqual(userTexts(early), ['hi']);
  },
  repeated_identical_prompts_survive_replay() {
    const w = make({
      els: {messages: new Element(), loadOlder: new Element('button'), loadNewer: new Element('button'),
        loadNewerStatus: new Element(), input: {value: ''}},
    });
    w.renderTranscript([
      ev(1, 'user_message', {id: 'c1', message: 'continue'}), ev(2, 'turn_completed'),
      ev(3, 'user_message', {id: 'c2', message: 'continue'}), ev(4, 'turn_completed'),
    ], {scrollBottom: true});
    assert.deepEqual(userTexts(w), ['continue', 'continue']);
  },
  // Item 2
  queue_removed_discards_optimistic_bubble() {
    const w = make();
    w.markSubmissionAccepted(accepted(), 'A', [], 'A', 'idA');
    w.markSubmissionAccepted(accepted({queued: true, started: false}), 'B', [], 'B', 'idB');
    w.handleEvent(ev(1, 'user_message', {id: 'idA', message: 'A'}));
    w.handleEvent(ev(2, 'queue_removed', {id: 'idB'}));
    assert.deepEqual(userTexts(w), ['A']);
    // A removal can never delete a prompt that already ran.
    w.handleEvent(ev(3, 'queue_removed', {id: 'idA'}));
    assert.deepEqual(userTexts(w), ['A']);
  },
  // Item 3
  every_server_event_type_is_registered(types) {
    const direct = make({sessionId: 's'});
    direct.connectSSE();
    const source = EventSource.instances.at(-1);
    const missing = types.filter(type => !source.listeners[type]);
    assert.deepEqual(missing, [], 'direct EventSource lacks listeners for ' + missing);
    const multiplexed = window.PAAgentChat.sessionMultiplexEvents;
    const absent = types.filter(type => !multiplexed.includes(type));
    assert.deepEqual(absent, [], 'multiplex stream lacks listeners for ' + absent);
    direct.closeSSE('done');
  },
  prompt_rendered_does_not_cause_gap_repair() {
    const w = make();
    w.connectSSE();
    const source = EventSource.instances.at(-1);
    [ev(1, 'prompt_rendered', {id: 'p'}), ev(2, 'user_message', {id: 'p', message: 'hello'}),
      ev(3, 'prompt_mentions', {prompt_id: 'p', mentions: []}), ev(4, 'prompt_timing', {}),
      ev(5, 'turn_completed')].forEach(event => source.deliver(event));
    assert.equal(w.calls.repairLiveGap, 0, 'no sequence hole, so no history repair');
    assert.equal(w.lastSeq, 5);
    assert.deepEqual(userTexts(w), ['hello']);
    // A registered type without a dedicated case is a no-op that still advances the cursor.
    const quiet = make();
    quiet.handleEvent(ev(1, 'context_boundary', {}));
    assert.equal(quiet.lastSeq, 1);
    assert.equal(quiet.calls.repairLiveGap, 0);
  },
  live_mentions_decorate_the_prompt_bubble() {
    const w = make();
    const decorated = [];
    w.decorateMentions = row => decorated.push(row.dataset.promptId);
    w.connectSSE();
    const source = EventSource.instances.at(-1);
    source.deliver(ev(1, 'prompt_rendered', {id: 'p'}));
    source.deliver(ev(2, 'user_message', {id: 'p', message: 'see @a.py'}));
    decorated.length = 0;
    assert.ok(source.deliver(ev(3, 'prompt_mentions', {prompt_id: 'p', mentions: [{token: '@a.py'}]})));
    assert.deepEqual(decorated, ['p']);
  },
  // Item 4
  queued_prompt_does_not_claim_the_agent_is_working() {
    const w = make();
    w.markSubmissionAccepted(accepted({queued: true, started: false, stop_reason: 'queued'}), 'later', [], 'later', 'q1');
    assert.deepEqual(w.calls.turn, [], 'queued behind a pause or a busy turn');
    w.markSubmissionAccepted(accepted({duplicate: true}), 'again', [], 'again', 'q2');
    assert.deepEqual(w.calls.turn, [], 'duplicate retry of an already settled prompt');
    w.markSubmissionAccepted(accepted(), 'now', [], 'now', 'q3');
    assert.deepEqual(w.calls.turn, [true], 'a prompt that really started');
    // The durable-receipt reconcile endpoint reports a lifecycle status instead.
    const r = make();
    r.markSubmissionAccepted({accepted: true, status: 'completed'}, 'old', [], 'old', 'r1');
    r.markSubmissionAccepted({accepted: true, status: 'queued', queued: true}, 'old', [], 'old', 'r2');
    assert.deepEqual(r.calls.turn, []);
    r.markSubmissionAccepted({accepted: true, status: 'executing'}, 'run', [], 'run', 'r3');
    assert.deepEqual(r.calls.turn, [true]);
  },
  // Item 5
  queue_events_refresh_while_history_is_held() {
    for (const flag of ['recentBootstrap', 'historyPreserveWindow']) {
      const w = make({[flag]: true});
      ['queue_dequeued', 'queue_removed', 'queue_reordered', 'queue_paused', 'queue_resumed'].forEach((type, i) => {
        const before = w.calls.refreshQueue;
        w.handleEvent(ev(i + 1, type, {id: 'x'}));
        assert.ok(w.calls.refreshQueue > before, `${type} must refresh the queue while ${flag}`);
      });
    }
  },
  // Item 7
  closed_direct_stream_is_reopened_with_backoff() {
    const w = make();
    w.lastSeq = 7;
    w.connectSSE();
    const first = EventSource.instances.at(-1);
    const count = EventSource.instances.length;
    first.readyState = 0; // CONNECTING: the browser retries by itself.
    first.onerror();
    assert.equal(timers.size, 0, 'the browser owns retries for a CONNECTING source');
    const delays = [];
    let current = first;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      current.readyState = 2; // a 404/503 closes the source for good
      current.onerror();
      assert.equal(timers.size, 1, 'one reconnect timer is pending');
      delays.push(Array.from(timers.values())[0].delay);
      fireAll();
      const next = EventSource.instances.at(-1);
      assert.notEqual(next, current, 'a fresh EventSource replaces the dead one');
      assert.ok(next.url.endsWith('/events?after=7'), next.url);
      current = next;
    }
    assert.ok(delays[0] > 0 && delays[1] > delays[0] && delays[2] > delays[1], 'backoff grows: ' + delays);
    assert.ok(delays.every(d => d <= 30000));
    // A source replaced or destroyed meanwhile must not be resurrected.
    current.readyState = 2; current.onerror();
    w.closeSSE('left');
    fireAll();
    assert.equal(w.es, null);
    assert.equal(EventSource.instances.length, count + 3);
  },
  // Item 6
  stale_mention_matches_cannot_be_selected() {
    const w = make();
    const chosen = [], rendered = [];
    w.commandMatches = [{id: 'stale', kind: 'file', insert: '@src/a.py'}];
    w.commandSelectedIndex = 0;
    w.els.commandMenu = {hidden: false};
    w.selectMenuItem = item => chosen.push(item);
    w.renderMenu = (trigger, items, text) => rendered.push({trigger, items, text});
    w.send = () => chosen.push('SEND');
    w.commandTrigger = {trigger: '@', query: 'src/ab'};
    w.startMentionSearch('src/ab');
    assert.equal(w.commandMatches.length, 0, 'matches for the old query are dropped immediately');
    assert.equal(rendered.at(-1).items.length, 0);
    let prevented = false;
    w.onComposerKeydown({key: 'Enter', shiftKey: false, isComposing: false, keyCode: 13, preventDefault() { prevented = true; }});
    assert.deepEqual(chosen, [], 'Enter must neither pick the stale item nor send half a mention');
    assert.ok(prevented);
  },
};

(async () => {
  const name = process.argv[3];
  assert.ok(scenarios[name], 'unknown scenario ' + name);
  const arg = process.argv[4] ? JSON.parse(fs.readFileSync(process.argv[4], 'utf8')) : undefined;
  await scenarios[name](arg);
  console.log('PASS ' + name);
})().catch(error => { console.error(error); process.exitCode = 1; });
