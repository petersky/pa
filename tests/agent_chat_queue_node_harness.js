const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const noop = () => {};
let selectedRoot = null, sessionList = null;
const document = {
  body: {addEventListener: noop}, addEventListener: noop, hidden: false,
  querySelector: selector => selector === '[data-agent-chat]' ? selectedRoot
    : selector === '[data-agent-session-list]' ? sessionList : null,
  querySelectorAll: () => [],
  createElement: () => ({dataset: {}, addEventListener: noop,
    querySelector: () => ({addEventListener: noop})}),
};
const window = {addEventListener: noop};
class EventSource {
  static CLOSED = 2;
  static instances = [];
  constructor(url) { this.url = url; this.readyState = 1; this.listeners = {}; EventSource.instances.push(this); }
  close() { this.readyState = 2; }
  addEventListener(type, handler) { this.listeners[type] = handler; }
  emit(type, data) {
    assert.ok(this.listeners[type], `${this.url} must subscribe to ${type}`);
    this.listeners[type]({data: JSON.stringify(data)});
  }
}
vm.runInNewContext(fs.readFileSync(process.argv[2], 'utf8'), {
  window, document, EventSource, console, URL, setTimeout: () => 1, clearTimeout: noop,
});
const Widget = window.PAAgentChat.AgentChatWidget;
const event = {seq: 1, session_id: 'paused-fixture', type: 'publication_fence_established',
  payload: {reason: 'operator_interrupt', queued_prompts_blocked: 1}};
const flush = async () => { await Promise.resolve(); await Promise.resolve(); };

async function scenario(multiplex) {
  const pause = {}, resume = {}, meta = {};
  const controls = {'[data-acw-queue-pause]': pause, '[data-acw-queue-resume]': resume,
    '[data-acw-queue-meta]': meta};
  const root = {dataset: {}, isConnected: true, querySelector: selector => controls[selector], closest: () => null};
  const queued = [{id: 'older-prompt', message: 'Earlier question'}];
  let paused = true;
  const calls = [];
  const widget = Object.create(Widget.prototype);
  Object.assign(widget, {
    sessionId: 'paused-fixture', apiBase: '/api/agent', root, showQueue: true,
    queuePaused: false, lastSeq: 0, seenEvents: {}, transcriptEvents: [], subscriptionGeneration: 0,
    els: {queue: {}, queueList: {appendChild: noop}},
    isNearBottom: () => false, _pruneMessageRows: noop, _compactEvents: events => events,
    updateEmptyChatStatus: noop,
    api: async (path, options = {}) => {
      calls.push({path, ...options});
      if (options.method === 'POST') {
        assert.equal(path, '/sessions/paused-fixture/queue/resume');
        assert.equal(options.body, '{}');
        paused = false;
        return {queue_paused: false};
      }
      assert.equal(path, '/sessions/paused-fixture');
      return {queue_paused: paused, queue: queued};
    },
  });
  root._acw = widget;
  selectedRoot = root;
  widget.renderQueue(queued);
  assert.equal(resume.disabled, true, 'the pre-interrupt queue is not paused');
  if (multiplex) {
    sessionList = {dataset: {}, isConnected: true};
    window.PAAgentChat.startSessionListFreshness();
  } else {
    widget.connectSSE();
  }
  const source = EventSource.instances.at(-1);
  source.emit(event.type, event);
  await flush();
  assert.equal(widget.queuePaused, true);
  assert.equal(resume.disabled, false, 'interrupt completion enables Resume without a reload');
  assert.equal(pause.disabled, true);
  assert.equal(meta.textContent, '(1) paused');
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, undefined, 'observing an interrupt must only read queue state');

  // A user click uses the existing action endpoint. This API is a fake; no PA
  // service is contacted and no real queue is resumed by this regression.
  widget.queueControl('resume');
  await flush();
  assert.equal(calls.filter(call => call.method === 'POST').length, 1);
  assert.equal(widget.queuePaused, false);
  assert.equal(resume.disabled, true);
  assert.equal(pause.disabled, false);
  const beforeReplay = calls.length;
  widget.seenEvents = {};
  widget.handleEvent({...event, seq: 2}, true, false);
  await flush();
  assert.equal(calls.length, beforeReplay, 'replaying an old fence must not refetch or change the queue');
  assert.equal(widget.queuePaused, false);
  if (multiplex) window.PAAgentChat.stopSessionListFreshness('fixture-complete');
  else widget.closeSSE('fixture-complete');
  selectedRoot = null;
  sessionList = null;
}

(async () => {
  assert.ok(window.PAAgentChat.sessionListRefreshEvents.includes(event.type));
  await scenario(false);
  await scenario(true);
  console.log('PASS interrupt pause, Resume action, and historical replay on direct and multiplexed streams');
})().catch(error => { console.error(error); process.exitCode = 1; });
