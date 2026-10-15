const assert = require("node:assert/strict");
const fs = require("node:fs"), vm = require("node:vm"), memory = new Map(), window = {};
vm.runInNewContext(fs.readFileSync("src/pa/server/static/js/conversation-threads.js", "utf8"), {
  window, crypto: {randomUUID: () => "stable-branch-id"},
  localStorage: {getItem: k => memory.get(k), setItem: (k,v) => memory.set(k,v), removeItem: k => memory.delete(k)},
  document: {createElement: () => ({addEventListener() {}})},
});
const tick = () => new Promise(resolve => setImmediate(resolve));
function controller(api) {
  const c = Object.create(window.PAConversationThreads.prototype);
  Object.assign(c, {widget: {sessionId: "source", api}, generation: 0, busy: false,
    status: {textContent: "", appendChild(link) {this.link = link;}}, native: {}, copy: {}, capabilities: {native_branch_available: true}});
  return c;
}
(async () => {
  let finish, calls = [];
  const c = controller((path, options) => {calls.push({path, options}); return new Promise(r => {finish = r;});});
  c.branch("copy"); c.branch("copy");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].options.headers["Idempotency-Key"], "stable-branch-id");
  c.widget.sessionId = "another"; c.generation++; c.status.textContent = "another conversation";
  finish({session: {id: "target", status: "idle"}}); await tick();
  assert.equal(c.status.textContent, "another conversation");
  assert.equal(c.status.link, undefined, "late branch receipt cannot route another thread");
  assert.equal(memory.get("pa:conversation-branch:source:copy"), "stable-branch-id");
  const retry = controller((path, options) => {calls.push({path, options}); return Promise.reject(new Error("lost acknowledgement"));});
  retry.branch("copy"); await tick(); retry.branch("copy"); await tick();
  assert.equal(calls[1].options.headers["Idempotency-Key"], calls[2].options.headers["Idempotency-Key"]);
  assert.match(retry.status.textContent, /no automatic fallback/);
})().catch(error => {console.error(error); process.exitCode = 1;});
