const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const memory = new Map();
const window = {};
function element(tag) {
  return {tag, children: [], events: {}, addEventListener(name, callback) {this.events[name] = callback;},
    append(...children) {this.children.push(...children);}, appendChild(child) {this.children.push(child);}, replaceChildren() {this.children = [];}};
}
vm.runInNewContext(fs.readFileSync("src/pa/server/static/js/conversation-issues.js", "utf8"), {
  window, console, document: {createElement: element}, setTimeout: () => 1, clearTimeout() {},
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
  const parent = element("div"), recovery = controller(() => {throw new Error("review cannot start work");});
  let resumed;
  recovery.start = value => {resumed = value;};
  recovery.retryButton(parent, body);
  parent.children[0].events.click();
  assert.equal(resumed, undefined, "review is separate from resuming the request");
  assert.equal(parent.children.at(-1).textContent, "Resume saved request");
  parent.children.at(-1).events.click();
  assert.equal(resumed, body, "recovery retains the original request and selection");
})().catch(error => {console.error(error); process.exitCode = 1;});
