const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const memory = new Map();
const window = {location: {href: "http://fixture/"}};
function element(tag) {
  return {tag, children: [], events: {}, dataset: {}, attributes: {}, setAttribute(name, value) {this.attributes[name] = value;}, addEventListener(name, callback) {this.events[name] = callback;},
    append(...children) {this.children.push(...children);}, appendChild(child) {this.children.push(child);}, replaceChildren() {this.children = [];}};
}
vm.runInNewContext(fs.readFileSync("src/pa/server/static/js/conversation-issues.js", "utf8"), {
  window, console, URL, document: {createElement: element}, setTimeout: () => 1, clearTimeout() {},
  localStorage: {getItem: k => memory.get(k), setItem: (k,v) => memory.set(k,v), removeItem: k => memory.delete(k)},
});
const proto = window.PAConversationIssues.prototype;
function controller(api) {
  const c = Object.create(proto);
  Object.assign(c, {widget: {sessionId: "a", api}, state: {revision: 1, issues: [], requests: []}, generation: 0,
    busy: false, selected: new Set(), controls() {}, render() {}, message(s) {this.lastMessage = s;}});
  return c;
}
const tick = () => new Promise(resolve => setImmediate(resolve));
(async () => {
  // Counts load on ready while closed, refresh without opening, and never
  // accept an old owner/thread response or a read racing a successful save.
  let reply, reads = 0;
  const live = controller(() => {reads++; return new Promise(resolve => {reply = resolve;});});
  let countWrites = 0, countText;
  const count = {get textContent() {return countText;}, set textContent(value) {countWrites++; countText = value;}};
  Object.assign(live, {state: null, panel: {open: false, contains: () => false},
    list: element("ol"), receipts: element("div"), addText: {}, render() {this.count();}});
  live.widget.root = {querySelector: () => count};
  live.count(); assert.equal(count.textContent, "(loading…)");
  live.ready(); live.ready(); assert.equal(reads, 1, "initial reads coalesce");
  reply({revision: 1, issues: [{id: "one"}], requests: []}); await tick();
  assert.equal(count.textContent, "(1)"); assert.equal(live.panel.open, false);
  const firstWrites = countWrites;
  live.load(); reply({revision: 1, issues: [{id: "one"}], requests: []}); await tick();
  assert.equal(countWrites, firstWrites, "unchanged polls cannot repeat live announcements");
  live.load(); reply({revision: 2, issues: [{id: "one"}, {id: "two"}], requests: []}); await tick();
  assert.equal(count.textContent, "(2 · updated)");
  live.load(); live.widget.apiBase = "other-owner";
  reply({revision: 999, issues: [], requests: []}); await tick();
  assert.equal(live.state.revision, 2, "old owner's count cannot paint");
  live.load(); live.readRevision++; live.state = {revision: 4, issues: [{id: "saved"}], requests: []};
  reply({revision: 3, issues: [], requests: []}); await tick();
  assert.equal(live.state.revision, 4, "pre-save read cannot overwrite saved revision");
  live.load(); live.widget.sessionId = "b"; live.reset();
  assert.equal(count.textContent, "(loading…)", "switch clears previous count synchronously");
  reply({revision: 5, issues: [{id: "source"}], requests: []}); await tick();
  assert.equal(live.state, null, "previous thread cannot leak its count");
  live.widget.api = () => Promise.reject(new Error("offline"));
  live.ready(); await tick(); assert.equal(count.textContent, "(unavailable)");
  live.widget.api = () => Promise.resolve({revision: 0, issues: [], requests: []});
  live.load(); await tick(); assert.equal(count.textContent, "(0)", "only fetched empty state is zero");
  live.widget.destroyed = true; live.reset();
  let finish, submitted;
  const c = controller((path, options) => {submitted = {path, options}; return new Promise(resolve => {finish = resolve;});});
  const body = {request_id: "durable-key", revision: 1, issue_ids: ["one"]};
  c.start(body);
  assert.equal(c.busy, true);
  assert.equal(submitted.options.headers["Idempotency-Key"], "durable-key");
  assert.equal(JSON.parse(memory.get("pa:conversation-start:a")).request_id, "durable-key");
  // A second click cannot issue a competing request.
  c.start({request_id: "another-key"});
  assert.equal(submitted.options.headers["Idempotency-Key"], "durable-key");
  c.widget.sessionId = "b"; c.generation++; c.state = {revision: 99};
  finish({revision: 2, issues: [], requests: []}); await tick();
  assert.equal(c.state.revision, 99, "late receipt cannot paint another thread");
  assert.equal(memory.has("pa:conversation-start:a"), false);
  const failed = controller(() => Promise.reject(new Error("lost ack")));
  failed.start(body); await tick();
  assert.equal(JSON.parse(memory.get("pa:conversation-start:a")).request_id, "durable-key");
  assert.equal(failed.busy, false);
  assert.match(failed.lastMessage, /same request/);
  let saved;
  const edited = controller((_path, options) => {saved = JSON.parse(options.body); return Promise.resolve({revision: 2});});
  edited.save([{id: "one", title: "<script>text</script>", request_id: "internal-only"}]); await tick();
  assert.equal(saved.issues[0].request_id, undefined, "clients cannot assign work claims");
  assert.equal(edited.state.revision, 2);
  let unsafeSaves = 0;
  const overflow = controller(() => {unsafeSaves++; return Promise.resolve({});});
  overflow.save([{id: "large", detail: "x".repeat(16001)}]);
  assert.equal(unsafeSaves, 0);
  assert.equal(overflow.dirty, true);
  assert.match(overflow.lastMessage, /text is unchanged/);
  // Unsaved fields survive polls, errors and edits to other rows. A single
  // save includes all pending row drafts under the same list revision CAS.
  const draft = controller(() => {unsafeSaves++; return Promise.resolve({});});
  draft.dirty = true; draft.panel = {contains: () => false};
  draft.load(); draft.start(body);
  assert.equal(unsafeSaves, 0, "poll and start cannot replace unsaved material");
  assert.match(draft.lastMessage, /Save your issue edits/);
  let draftSubmission;
  const multi = controller((_path, options) => {draftSubmission = JSON.parse(options.body); return Promise.reject(new Error("revision conflict"));});
  multi.drafts = new Map([["a", {detail: "retained first draft"}], ["b", {detail: "retained second draft"}]]);
  multi.save([{id: "a", detail: "old"}, {id: "b", detail: "old"}]); await tick();
  assert.deepEqual(draftSubmission.issues.map(i => i.detail), ["retained first draft", "retained second draft"]);
  assert.equal(multi.drafts.size, 2);
  assert.equal(multi.dirty, true);
  assert.match(multi.lastMessage, /edits are retained/);
  const fields = controller(() => {throw new Error("render cannot start work");});
  Object.assign(fields, {list: element("ul"), receipts: element("div")});
  fields.widget.root = {querySelector: () => ({})};
  fields.state.issues = [{id: "safe", title: "Title", detail: "<script>text</script>", card_id: "card", card_execution_version: "reviewed"}];
  proto.render.call(fields);
  const details = fields.list.children[0].children.find(e => e.tag === "details");
  const detailField = details.children.find(e => e.tag === "textarea");
  assert.equal(detailField.value, "<script>text</script>");
  detailField.value = "changed draft"; detailField.events.input();
  assert.equal(fields.drafts.get("safe").detail, "changed draft");
  assert.equal(fields.dirty, true);
  // Rendering restored receipts uses Work, including blocked groups that
  // already created a card. Older peers without realm metadata still work.
  const restored = controller(() => {throw new Error("render must not start work");});
  Object.assign(restored, {list: element("ul"), receipts: element("div")});
  restored.widget.root = {querySelector: () => ({})};
  restored.state.requests = [
    {request_id: "started", state: "admitted", realm_id: "team & research", groups: [
      {title: "Started", state: "admitted", card_id: "card/with?reserved#&="},
      {title: "Pending", state: "pending"},
    ]},
    {request_id: "blocked", state: "blocked", realm_id: "default", groups: [
      {title: "Card created", state: "pending", card_id: "existing-card"},
    ]},
    {request_id: "legacy", state: "admitted", groups: [{title: "Legacy", state: "admitted", card_id: "legacy-card"}]},
  ];
  restored.retryButton = () => {};
  proto.render.call(restored);
  const lines = restored.receipts.children.flatMap(section => section.children.filter(e => e.tag === "p"));
  const links = lines.flatMap(line => line.children);
  assert.deepEqual(links.map(link => link.href), [
    "/work?card=card%2Fwith%3Freserved%23%26%3D&realm=team%20%26%20research",
    "/work?card=existing-card&realm=default",
    "/work?card=legacy-card",
  ]);
  assert.ok(links.every(link => link.textContent === "Open card"));
  const parent = element("div"), recovery = controller(() => {throw new Error("render cannot start work");});
  let resumed;
  recovery.start = value => {resumed = value;};
  recovery.retryButton(parent, body);
  assert.equal(parent.children.length, 1, "saved authorization needs no review gate");
  assert.equal(parent.children[0].textContent, "Retry saved request");
  parent.children[0].events.click();
  assert.equal(resumed, body, "recovery retains the original request and selection");
})().catch(error => {console.error(error); process.exitCode = 1;});
