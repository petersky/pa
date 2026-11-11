// Node harness for the Workshop floor activity transport (lease, resume, fan-out).
// usage: node workshop_floor_stream_node_harness.js <workshop-floor-stream.js>
const assert = require("assert/strict");
const path = require("path");
const Stream = require(path.resolve(process.argv[2]));

let clock = 1000000;
const timers = [];
function timerFor(owner) {
  return (fn, ms) => { timers.push({ fn, at: clock + ms, owner }); return timers.length; };
}
function runTimers(advance) {
  clock += advance;
  let ran = true;
  while (ran) {
    ran = false;
    for (let i = 0; i < timers.length; i++) {
      if (timers[i] && timers[i].at <= clock) { const t = timers[i]; timers[i] = null; t.fn(); ran = true; }
    }
  }
}
const storage = (() => {
  const data = new Map();
  return { getItem: (k) => (data.has(k) ? data.get(k) : null), setItem: (k, v) => data.set(k, String(v)),
    removeItem: (k) => data.delete(k) };
})();
const channels = [];
class FakeChannel {
  constructor(name) { this.name = name; channels.push(this); }
  postMessage(message) {
    channels.filter((c) => c !== this && c.name === this.name && c.onmessage)
      .forEach((c) => c.onmessage({ data: JSON.parse(JSON.stringify(message)) }));
  }
  close() { channels.splice(channels.indexOf(this), 1); }
}
class FakeEventSource {
  static all = [];
  constructor(url) { this.url = url; this.listeners = {}; this.closed = false; FakeEventSource.all.push(this); }
  addEventListener(type, fn) { this.listeners[type] = fn; }
  close() { this.closed = true; }
  emit(type, data) { this.listeners[type]({ data: JSON.stringify(data) }); }
  get open() { return !this.closed; }
}
function tab(id, extra) {
  const events = [];
  const readies = [];
  const unsupported = [];
  const stream = Stream.createStream(Object.assign({
    deps: { EventSource: FakeEventSource, localStorage: storage, BroadcastChannel: FakeChannel,
      setTimeout: timerFor(id), clearTimeout: (t) => { timers[t - 1] = null; }, now: () => clock, tabId: id,
      fetchJson: () => Promise.resolve({ transport: "sse" }) },
    onEvent: (instance, data) => events.push([instance, data.session_id, data.seq, data.type]),
    onReady: (instance, info) => readies.push([instance, info.reconnected]),
    onUnsupported: (instance) => unsupported.push(instance)
  }, extra));
  return { stream, events, readies, unsupported };
}
const flush = () => new Promise((r) => setImmediate(r));
const openSources = () => FakeEventSource.all.filter((s) => !s.closed);

(async () => {
  // One stream per instance across two tabs: the owner opens it, the follower listens.
  const a = tab("tab-a");
  const b = tab("tab-b");
  const instances = [{ id: "local", base: "/api/agent", local: true },
    { id: "peer", base: "/api/fleet/instances/peer/agent" }];
  a.stream.setInstances(instances);
  runTimers(0); await flush();
  b.stream.setInstances(instances);
  runTimers(0); await flush();
  assert.equal(a.stream.role("local"), "owner");
  assert.equal(b.stream.role("local"), "follower");
  assert.equal(openSources().length, 2, "exactly one EventSource per instance across both tabs");
  const local = openSources().find((s) => s.url.startsWith("/api/agent/session-events?"));
  const peer = openSources().find((s) => s.url.startsWith("/api/fleet/instances/peer/agent/session-events?"));
  assert.ok(local && peer);

  local.emit("ready", {});
  local.emit("user_message", { session_id: "s1", seq: 1, type: "user_message" });
  local.emit("user_message", { session_id: "s1", seq: 1, type: "user_message" }); // duplicate
  local.emit("tool_call", { session_id: "s1", seq: 2, type: "tool_call" });
  assert.deepEqual(a.events.map((e) => e[2]), [1, 2], "owner dedupes by (session, seq)");
  assert.deepEqual(b.events.map((e) => e.join(":")), ["local:s1:1:user_message", "local:s1:2:tool_call"],
    "follower receives the owner's events over the BroadcastChannel");
  assert.deepEqual(a.readies, [], "the first ready follows the skeleton and needs no refresh");

  // Resume: an error reopens with the per-session cursors, and a later ready asks for a refresh.
  local.onerror();
  await flush();
  runTimers(Stream.TICK_MS); runTimers(2100); await flush();
  const reopened = openSources().filter((s) => s.url.startsWith("/api/agent/session-events?"));
  assert.equal(reopened.length, 1);
  assert.equal(JSON.parse(new URL("http://x" + reopened[0].url).searchParams.get("after")).s1, 2);
  reopened[0].emit("ready", {});
  assert.deepEqual(a.readies, [["local", true]]);

  // The owner closing hands the lease to the follower within a tick.
  a.stream.close();
  runTimers(0); runTimers(Stream.TICK_MS); await flush();
  assert.equal(b.stream.role("local"), "owner");
  assert.equal(openSources().filter((s) => s.url.startsWith("/api/agent/session-events?")).length, 1);

  // A tab that disappears without releasing: the lease expires and a follower takes over.
  const c = tab("tab-c");
  c.stream.setInstances([{ id: "orphan", base: "/api/fleet/instances/orphan/agent" }]);
  runTimers(0); await flush();
  assert.equal(c.stream.role("orphan"), "owner");
  const d = tab("tab-d");
  d.stream.setInstances([{ id: "orphan", base: "/api/fleet/instances/orphan/agent" }]);
  runTimers(0); await flush();
  assert.equal(d.stream.role("orphan"), "follower");
  // Simulate a crashed owner: its timers stop and it never releases.
  timers.forEach((t, i) => { if (t && t.owner === "tab-c") timers[i] = null; });
  const stored = JSON.parse(storage.getItem(Stream.LEASE_PREFIX + "orphan"));
  assert.equal(stored.tab_id, "tab-c");
  storage.setItem(Stream.LEASE_PREFIX + "orphan", JSON.stringify({ tab_id: "tab-c", expires_at: clock - 1 }));
  runTimers(Stream.TICK_MS + 10); await flush();
  assert.equal(d.stream.role("orphan"), "owner", "expired lease is taken over");

  // Old peers (404 on the capabilities probe) are marked unsupported and never open a stream.
  const old = tab("tab-old", { deps: { EventSource: FakeEventSource, localStorage: storage,
    BroadcastChannel: FakeChannel, setTimeout: timerFor("tab-old"), clearTimeout: () => {}, now: () => clock,
    tabId: "tab-old", fetchJson: () => Promise.reject({ status: 404 }) } });
  const before = FakeEventSource.all.length;
  old.stream.setInstances([{ id: "ancient", base: "/api/fleet/instances/ancient/agent" }]);
  runTimers(0); await flush(); await flush();
  assert.deepEqual(old.unsupported, ["ancient"]);
  assert.equal(FakeEventSource.all.length, before, "no per-session SSE fallback");

  // Removing a bay closes its stream and releases its lease.
  b.stream.setInstances([{ id: "local", base: "/api/agent", local: true }]);
  assert.equal(openSources().filter((s) => s.url.includes("/peer/")).length, 0);
  assert.equal(storage.getItem(Stream.LEASE_PREFIX + "peer"), null);

  // Storage-disabled contexts stay isolated: they own their own stream.
  const lonely = tab("tab-lonely", { deps: { EventSource: FakeEventSource, localStorage: null,
    setTimeout: timerFor("tab-lonely"), clearTimeout: () => {}, now: () => clock, tabId: "tab-lonely",
    fetchJson: () => Promise.resolve({ transport: "sse" }) } });
  lonely.stream.setInstances([{ id: "solo", base: "/api/agent", local: true }]);
  runTimers(0); await flush();
  assert.equal(lonely.stream.role("solo"), "owner");

  console.log("ok");
})().catch((error) => { console.error(error); process.exit(1); });
