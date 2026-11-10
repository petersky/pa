// Stop holds the queue in the UI, and queue controls surface failures.
const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const noop = () => {};
// Every <li> row records the click handlers its buttons register.
const rows = [];
const document = {
  body: {addEventListener: noop}, addEventListener: noop, hidden: false,
  querySelector: () => null, querySelectorAll: () => [], activeElement: null,
  createElement: () => {
    const handlers = {};
    const li = {dataset: {}, addEventListener: noop, handlers,
      querySelector: selector => ({disabled: false, focus: noop,
        addEventListener: (type, fn) => { if (type === 'click') handlers[selector] = fn; }})};
    rows.push(li);
    return li;
  },
};
const window = {addEventListener: noop};
const timers = new Map();
let timerId = 0;
vm.runInNewContext(fs.readFileSync(process.argv[2], 'utf8'), {
  window, document, console, URL, EventSource: class { addEventListener() {} close() {} },
  setTimeout: (callback, delay) => {const id = ++timerId; if (delay === 250) timers.set(id, {callback}); return id;},
  clearTimeout: id => timers.delete(id),
});
const Widget = window.PAAgentChat.AgentChatWidget;
const flush = async () => {
  for (let i = 0; i < 4; i++) await Promise.resolve();
  for (const [id, timer] of Array.from(timers)) {timers.delete(id); timer.callback();}
  for (let i = 0; i < 4; i++) await Promise.resolve();
};

function build(apiImpl) {
  const parts = {pause: {}, resume: {}, meta: {}, status: {textContent: ''}};
  const controls = {'[data-acw-queue-pause]': parts.pause, '[data-acw-queue-resume]': parts.resume,
    '[data-acw-queue-meta]': parts.meta, '[data-acw-queue-status]': parts.status};
  const root = {dataset: {}, isConnected: true, querySelector: selector => controls[selector], closest: () => null};
  const calls = [];
  const queued = [{id: 'p1', message: 'First'}, {id: 'p2', message: 'Second'}];
  let serverPaused = false;
  const widget = Object.create(Widget.prototype);
  Object.assign(widget, {
    sessionId: 'stop-fixture', apiBase: '/api/agent', root, showQueue: true,
    queuePaused: false, queueNotice: '', lastSeq: 4, seenEvents: {}, transcriptEvents: [],
    subscriptionGeneration: 0, destroyed: false, lastSnapshot: {queue: queued},
    els: {queue: {hidden: true}, queueList: {appendChild: noop}},
    isNearBottom: () => false, _pruneMessageRows: noop, _compactEvents: events => events,
    updateEmptyChatStatus: noop, finalizeStreams: noop, finalizeActivity: noop, setTurnActive: noop,
    renderPresentation: noop,
    api: async (path, options = {}) => {
      calls.push({path, method: options.method || 'GET'});
      if (apiImpl) {
        const handled = await apiImpl(path, options, {setPaused: v => { serverPaused = v; }});
        if (handled !== undefined) return handled;
      }
      if (!options.method) return {queue_paused: serverPaused, queue: queued};
      return {};
    },
    setServerPaused: v => { serverPaused = v; },
  });
  root._acw = widget;
  return {widget, parts, calls, queued};
}

const cancelled = (payload) => ({seq: 5, session_id: 'stop-fixture', type: 'cancelled', payload});
const reads = calls => calls.filter(call => call.method === 'GET').length;

(async () => {
  // 1. A user Stop with queued prompts shows the queue paused with a working
  //    Resume at once and re-reads the authoritative queue.
  {
    const {widget, parts, calls} = build();
    widget.setServerPaused(true);
    widget.handleEvent(cancelled({pause_queue: true, queue_paused: true}), false, false);
    assert.equal(widget.queuePaused, true);
    assert.equal(parts.resume.disabled, false, 'Resume is enabled right after Stop');
    assert.equal(parts.pause.disabled, true);
    assert.equal(parts.meta.textContent, '(2) paused');
    await flush();
    assert.equal(reads(calls), 1, 'Stop must refresh the queue from the server');
  }
  // 2. Replayed history never changes state or refetches.
  {
    const {widget, calls} = build();
    widget.handleEvent(cancelled({pause_queue: true, queue_paused: true}), true, false);
    await flush();
    assert.equal(widget.queuePaused, false);
    assert.equal(calls.length, 0);
  }
  // 3. An internal interrupt must not clear a pause the user set earlier, and
  //    an older server payload (no queue_paused) keeps the same guarantee.
  {
    const {widget} = build();
    widget.queuePaused = true;
    widget.setServerPaused(true);
    widget.handleEvent(cancelled({pause_queue: false}), false, false);
    assert.equal(widget.queuePaused, true);
    widget.handleEvent({...cancelled({pause_queue: false, queue_paused: false}), seq: 6}, false, false);
    assert.equal(widget.queuePaused, false, 'the server payload is authoritative when present');
  }
  // 4. Failed queue mutations refresh and show the failure; a prompt that
  //    already started is not an error.
  {
    const failure = Object.assign(new Error('Queue is busy'), {status: 500, detail: 'Queue is busy'});
    const gone = Object.assign(new Error('The prompt already left the queue.'),
      {status: 409, detail: {code: 'already_started', message: 'The prompt already left the queue.'}});
    let mode = 'fail';
    const {widget, parts, calls} = build(async (path, options) => {
      if (!options.method) return undefined;
      if (path.endsWith('/queue/p1') && mode === 'gone') throw gone;
      throw failure;
    });
    widget.renderQueue(widget.lastSnapshot.queue);
    const row = rows.slice(-2);
    // reorder
    row[0].handlers['[data-queue-action="down"]']();
    await flush();
    assert.equal(reads(calls), 1, 'a failed reorder still refreshes the queue');
    assert.match(parts.status.textContent, /Queue update failed: Queue is busy/);
    // remove: already started is success-and-refresh
    mode = 'gone';
    row[0].handlers['[data-remove]']();
    await flush();
    assert.equal(reads(calls), 2);
    assert.equal(widget.queueNotice, '', 'a prompt that already started is not an error');
    // pause/resume
    mode = 'fail';
    widget.queueControl('resume');
    await flush();
    assert.equal(reads(calls), 3);
    assert.match(widget.queueNotice, /Queue update failed/);
    // a later successful action clears the notice
    const ok = build();
    ok.widget.queueNotice = 'Queue update failed: old.';
    ok.widget.queueControl('pause');
    await flush();
    assert.equal(ok.widget.queueNotice, '');
  }
  console.log('PASS Stop shows a paused queue with Resume; queue control failures are surfaced');
})().catch(error => { console.error(error); process.exitCode = 1; });
