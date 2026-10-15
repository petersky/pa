const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const memory = new Map();
const window = {};
vm.runInNewContext(fs.readFileSync("src/pa/server/static/js/conversation-issues.js", "utf8"), {
  window, console, setTimeout: () => 1, clearTimeout() {},
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
})().catch(error => {console.error(error); process.exitCode = 1;});
