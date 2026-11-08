// Tool-activity and Terminal flyouts: open at the last entry, follow live output,
// show only the most recent entries and reveal older ones on scroll-up.
// Runs the real widget in a vm with a DOM that has real scroll geometry:
// scrollTop clamps to the content, and hiding an element forgets its position.
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const noop = () => {};
const ROW = 20, VIEWPORT = 400;

class Element {
  constructor(tag = 'div') {
    this.tagName = tag; this.children = []; this.dataset = {}; this.attributes = {};
    this.className = ''; this.textContent = ''; this._hidden = false; this._st = 0;
    this.handlers = {}; this.heightOf = null; this.parentNode = null;
    this.classList = { add: noop, toggle: noop, contains: () => false };
  }
  get hidden() { return this._hidden; }
  set hidden(v) { this._hidden = !!v; if (this._hidden) this._st = 0; }   // display:none forgets scroll
  // display:none has no layout: no height, no viewport, nothing to scroll.
  get scrollHeight() { return this._hidden ? 0 : this.heightOf ? this.heightOf() : this.children.length * ROW; }
  get clientHeight() { return this._hidden ? 0 : VIEWPORT; }
  set clientHeight(_) {}
  get scrollTop() { return this._st; }
  set scrollTop(v) { this._st = Math.max(0, Math.min(v, Math.max(0, this.scrollHeight - this.clientHeight))); }
  set innerHTML(_) {
    this.children = [];
    ['acw-tool-title', 'acw-tool-timer muted', 'acw-tool-status muted'].forEach(cls => {
      const c = new Element('span'); c.className = cls; this.appendChild(c);
    });
  }
  appendChild(child) { if (child.parentNode) child.remove(); child.parentNode = this; this.children.push(child); return child; }
  insertBefore(child, ref) {
    if (child.parentNode) child.remove();
    child.parentNode = this;
    const at = ref ? this.children.indexOf(ref) : -1;
    if (at < 0) this.children.push(child); else this.children.splice(at, 0, child);
    return child;
  }
  replaceChildren() { this.children.forEach(c => { c.parentNode = null; }); this.children = []; }
  remove() { if (this.parentNode) { this.parentNode.children = this.parentNode.children.filter(c => c !== this); this.parentNode = null; } }
  hasAttribute(k) { return k in this.attributes; }
  setAttribute(k, v) { this.attributes[k] = v; }
  addEventListener(type, fn) { (this.handlers[type] = this.handlers[type] || []).push(fn); }
  fire(type, event = {}) { (this.handlers[type] || []).forEach(fn => fn(event)); }
  descendants() { return this.children.flatMap(c => [c, ...c.descendants()]); }
  matches(sel) {
    return sel.split(',').some(part => {
      part = part.trim();
      if (part === '*') return true;
      if (part.startsWith('.')) return (this.className || '').split(/\s+/).includes(part.slice(1));
      const attr = /^\[([\w-]+)\]$/.exec(part);
      if (attr) return attr[1] === 'data-tool-id' ? 'toolId' in this.dataset : (attr[1] in this.attributes);
      return false;
    });
  }
  querySelectorAll(sel) { return this.descendants().filter(c => c.matches(sel)); }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  closest(sel) { let n = this; while (n) { if (n.matches && n.matches(sel)) return n; n = n.parentNode; } return null; }
  cloneNode() { return this; }
  get outerHTML() { return ''; }
}

const window = { addEventListener: noop };
const document = {
  createElement: tag => new Element(tag), createTextNode: text => ({ textContent: text }),
  body: { addEventListener: noop }, addEventListener: noop, querySelector: () => null, querySelectorAll: () => [],
};
const ctx = { window, document, console, URL, AbortController, setTimeout, clearTimeout, setInterval, clearInterval, performance };
vm.runInNewContext(fs.readFileSync('src/pa/server/static/js/agent-terminal.js', 'utf8'), ctx);
vm.runInNewContext(fs.readFileSync('src/pa/server/static/js/agent-chat.js', 'utf8'), ctx);
const Widget = window.PAAgentChat.AgentChatWidget;
const atBottom = el => el.scrollTop === Math.max(0, el.scrollHeight - el.clientHeight);

function make(sessionId = 's') {
  const w = Object.create(Widget.prototype);
  const toolFlyout = new Element('aside'), toolActivity = new Element(), toolEmpty = new Element('p');
  toolEmpty.attributes['data-acw-tool-empty'] = ''; toolEmpty.hidden = true;
  toolActivity.appendChild(toolEmpty);
  toolFlyout.hidden = true;
  toolFlyout.heightOf = () => toolActivity.children.filter(c => !c.hidden).length * ROW;
  const terminalFlyout = new Element('aside'), terminalOutput = new Element();
  terminalFlyout.hidden = true;
  terminalFlyout.heightOf = () => terminalOutput.children.length * ROW;
  const toolEarlier = new Element('button'); toolEarlier.hidden = true;
  const toolToggle = new Element('button'), terminalToggle = new Element('button');
  Object.assign(w, {
    els: { messages: new Element(), toolFlyout, toolActivity, toolEmpty, toolEarlier, toolToggle,
      terminalFlyout, terminalOutput, terminalToggle, loadOlder: new Element('button'),
      loadNewer: new Element('button'), loadNewerStatus: new Element(), input: { value: '' } },
    root: Object.assign(new Element(), { dataset: {} }),
    sessionId, apiBase: '/api/agent', subscriptionGeneration: 1, transcriptEvents: [], seenEvents: {},
    lastSeq: 0, streaming: {}, activityStreams: {}, activeToolIds: {}, toolTimers: {}, plans: [],
    providerId: 'other', messageRowCount: 0, rawText: true, activityCount: 0,
    clearPlaceholder: noop, setPlaceholder: noop, isNearBottom: () => true, scrollToBottom: noop,
    finalizeActivity: noop, setTurnActive(a) { this.prompting = a; }, setStatus: noop,
    renderQueue: noop, renderMetrics: noop,
  });
  w._bindFlyoutScrolling();
  return w;
}
const tool = (w, i, status = 'completed') => w.upsertTool({ tool_call_id: 't' + i, title: 'cmd ' + i, status }, '2026-01-01T00:00:00Z');
const toolIds = w => w._activityUnits().map(u => u.dataset.toolId);
const call = (seq, status = 'completed') => ({ seq, type: 'tool_call', session_id: 's',
  payload: { tool_call_id: 'c' + seq, title: 'shell ' + seq, status, external_session_id: 'n', turn_id: 't' } });
const calls = (from, to) => { const out = []; for (let i = from; i <= to; i++) out.push(call(i)); return out; };
const callCount = w => w.els.terminalOutput.children.filter(c => c.className === 'acw-terminal-call').length;

// ---- Tool activity -------------------------------------------------------
{ // Only the most recent rows are in the DOM; older ones are kept, bounded, not rendered.
  const w = make();
  for (let i = 1; i <= 250; i++) tool(w, i);
  assert.strictEqual(w._activityUnits().length, 100, 'only the last N rows are rendered');
  assert.strictEqual(toolIds(w)[0], 't151'); assert.strictEqual(toolIds(w).at(-1), 't250');
  assert.strictEqual(w.activityArchive.length, 150);
  assert.ok(!w.els.toolEarlier.hidden, 'an earlier-activity control appears');
  // Opening a flyout that was hidden lands on the last entry, not the first.
  assert.strictEqual(w.els.toolFlyout.scrollTop, 0);
  w.toggleFlyout('tool');
  assert.ok(w.els.toolFlyout.scrollTop > 0 && atBottom(w.els.toolFlyout), 'opens at the last entry');
  // Following live output while pinned.
  tool(w, 251); tool(w, 252);
  assert.ok(atBottom(w.els.toolFlyout), 'follows new activity');
  // A reader who scrolled up is not yanked down.
  w.els.toolFlyout.scrollTop = 600;
  tool(w, 253);
  assert.strictEqual(w.els.toolFlyout.scrollTop, 600, 'does not steal scroll from a reader');
  // Scrolling up to the top reveals the next page and keeps the reader on the same row.
  w.els.toolFlyout.scrollTop = 0;
  const beforeIds = toolIds(w), archivedBefore = w.activityArchive.length;
  w.els.toolFlyout.fire('wheel', { deltaY: -120 });
  assert.strictEqual(w._activityUnits().length, beforeIds.length + 50, 'one page of older rows is revealed');
  assert.strictEqual(w.activityArchive.length, archivedBefore - 50);
  const after = toolIds(w);
  assert.deepStrictEqual(after.slice(50), beforeIds, 'older rows are inserted above, in order');
  assert.strictEqual(w.els.toolFlyout.scrollTop, 50 * ROW, 'the reader stays on the same row');
  assert.ok(Number(after[0].slice(1)) < Number(beforeIds[0].slice(1)));
}
{ // A scroll event alone (e.g. restoring a position) never loads more; only a user gesture does.
  const w = make();
  for (let i = 1; i <= 250; i++) tool(w, i);
  w.toggleFlyout('tool');
  w.els.toolFlyout._st = 0;
  const n = w._activityUnits().length;
  w.els.toolFlyout.fire('scroll');
  assert.strictEqual(w._activityUnits().length, n, 'programmatic scroll cannot cascade into loading history');
  w.els.toolFlyout.fire('pointerdown'); w.els.toolFlyout.fire('scroll');
  assert.strictEqual(w._activityUnits().length, n + 50, 'a user-driven scroll to the top does');
}
{ // Retention stays bounded and an active tool is never archived.
  const w = make();
  tool(w, 0, 'in_progress');
  for (let i = 1; i <= 1000; i++) tool(w, i);
  assert.ok(toolIds(w).includes('t0'), 'a running tool stays visible');
  assert.ok(w._activityUnits().length + w.activityArchive.length <= 300, 'visible + archived within MAX_ACTIVITY_ROWS');
  assert.ok(w.memoryDiagnostics().bounds.activityRows === 300);
}
{ // With nothing archived but older server history, scrolling up asks for the next page.
  const w = make(); let asked = 0;
  w.loadOlderTranscript = function () { asked += 1; this.loadingOlder = true; };
  w.hasOlder = true;
  for (let i = 1; i <= 30; i++) tool(w, i);
  w.updateOlderControl();   // runs whenever hasOlder changes
  w.toggleFlyout('tool');
  assert.ok(!w.els.toolEarlier.hidden, 'offers to load earlier history');
  w.els.toolEarlier.fire('click');
  assert.strictEqual(asked, 1); assert.strictEqual(w._olderFlyoutLoad, true);
  assert.strictEqual(w.activityLimit, 150, 'the window grows to include the page that will arrive');
}

// ---- Terminal -------------------------------------------------------------
{
  const w = make();
  w.transcriptEvents = calls(1, 120);
  assert.strictEqual(w.els.terminalOutput.children.length, 0);
  w.toggleFlyout('terminal');
  assert.strictEqual(callCount(w), 50, 'only the last N calls are rendered');
  assert.ok(w.els.terminalOutput.children[0].attributes['data-acw-terminal-earlier'] !== undefined, 'earlier-calls control');
  assert.ok(w.els.terminalFlyout.scrollTop > 0 && atBottom(w.els.terminalFlyout), 'opens at the last entry');
  assert.ok(w.els.terminalOutput.children.at(-1).children[0].textContent.includes('shell 120'));
  // Live output is followed while pinned.
  w.transcriptEvents = w.transcriptEvents.concat(calls(121, 125));
  w.renderTerminal();
  assert.ok(atBottom(w.els.terminalFlyout), 'follows live terminal output');
  assert.strictEqual(callCount(w), 50, 'window slides while following');
  // A reader higher up is left where they are.
  w.els.terminalFlyout.scrollTop = 300;
  w.transcriptEvents = w.transcriptEvents.concat(calls(126, 127));
  w.renderTerminal();
  assert.strictEqual(w.els.terminalFlyout.scrollTop, 300, 'does not steal scroll from a reader');
  // Scrolling to the top reveals older loaded calls and keeps the reader's place.
  w.els.terminalFlyout.scrollTop = 0;
  const first = w.els.terminalOutput.children[1].children[0].textContent;
  w.els.terminalFlyout.fire('wheel', { deltaY: -120 });
  assert.strictEqual(callCount(w), 100, 'one page of older calls is revealed');
  assert.strictEqual(w.els.terminalFlyout.scrollTop, 50 * ROW, 'the reader stays on the same call');
  assert.strictEqual(w.els.terminalOutput.children[51].children[0].textContent, first);
  // Reopening starts at the last entry again with the small window.
  w.toggleFlyout('terminal'); w.toggleFlyout('terminal');
  assert.strictEqual(callCount(w), 50); assert.ok(atBottom(w.els.terminalFlyout));
}
{ // Everything loaded is shown, so the control asks the server for older history.
  const w = make(); let asked = 0;
  w.loadOlderTranscript = function () { asked += 1; this.loadingOlder = true; };
  w.hasOlder = true; w.transcriptEvents = calls(1, 10);
  w.toggleFlyout('terminal');
  assert.strictEqual(callCount(w), 10);
  w.els.terminalOutput.fire('click', { target: w.els.terminalOutput.children[0] });
  assert.strictEqual(asked, 1, 'the earlier-history control loads the next durable page');
}

// ---- Rebuilds keep the reader's place ---------------------------------------
{
  const w = make();
  w.renderTranscript(calls(1, 300), { scrollBottom: true });
  w.toggleFlyout('tool');
  assert.ok(atBottom(w.els.toolFlyout), 'tool flyout opens at the last entry after a replay');
  assert.strictEqual(w._activityUnits().length, 100, 'a replay builds only the last N rows');
  // Pinned readers stay pinned across a history rebuild.
  w.renderTranscript(w.transcriptEvents.concat(calls(301, 305)), { scrollBottom: true });
  assert.ok(atBottom(w.els.toolFlyout), 'still at the last entry after a rebuild');
  // A reader in the middle stays put across a rebuild.
  w.els.toolFlyout.scrollTop = 500;
  w.renderTranscript(w.transcriptEvents.concat(calls(306, 307)), { scrollBottom: true });
  assert.strictEqual(w.els.toolFlyout.scrollTop, 500, 'a rebuild does not reset the scroll position');
  // The terminal likewise.
  w.toggleFlyout('terminal');
  assert.ok(atBottom(w.els.terminalFlyout));
  w.els.terminalFlyout.scrollTop = 200;
  w.renderTranscript(w.transcriptEvents.concat(calls(308, 309)), { scrollBottom: true });
  assert.strictEqual(w.els.terminalFlyout.scrollTop, 200, 'terminal keeps its position across a rebuild');
}
{ // Prepending an older durable page keeps the reader on the same row.
  const w = make();
  w.renderTranscript(calls(201, 400), { scrollBottom: true });
  w.toggleFlyout('terminal');
  w.els.terminalFlyout.scrollTop = 0;
  w.terminalLimit = 100; w._olderFlyoutLoad = true;     // set by _requestOlderForFlyouts
  const topCall = w.els.terminalOutput.children[1].children[0].textContent;
  const oldHeight = w.els.terminalFlyout.scrollHeight;
  w.prependTranscript(calls(101, 200));
  w._olderFlyoutLoad = false;
  const delta = w.els.terminalFlyout.scrollHeight - oldHeight;
  assert.ok(delta > 0);
  assert.strictEqual(w.els.terminalFlyout.scrollTop, delta, 'anchored: older rows land above the reader');
  assert.ok(w.els.terminalOutput.children.some(c => c.children && c.children[0] && c.children[0].textContent === topCall));
}
{ // A different session starts with the small window again.
  const forSession = (id, list) => list.map(e => ({ ...e, session_id: id }));
  const w = make('a');
  w.transcriptEvents = forSession('a', calls(1, 200)); w.toggleFlyout('terminal');
  w.els.terminalFlyout.scrollTop = 0; w.els.terminalFlyout.fire('wheel', { deltaY: -1 });
  assert.strictEqual(callCount(w), 100);
  w.sessionId = 'b'; w.transcriptEvents = forSession('b', calls(1, 200)); w.renderTerminal({ pin: true });
  assert.strictEqual(callCount(w), 50, 'window resets for a new session');
}
console.log('ok');
process.exit(0);   // the running-tool case leaves a live elapsed-time interval
