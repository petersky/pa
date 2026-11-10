// Node harness for the Workshop floor client model.
// usage: node workshop_floor_model_node_harness.js <workshop-floor-model.js> <stations.json>
const assert = require("assert/strict");
const fs = require("fs");
const Model = require(require("path").resolve(process.argv[2]));
const fixture = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));

let timers = [];
function newFloor(extra) {
  timers = [];
  return Model.createFloor(Object.assign({
    setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; },
    clearTimeout: () => {},
    now: () => Date.parse("2026-11-09T12:00:00Z"),
    onAsync: (result) => { newFloor.async.push(result); }
  }, extra || {}));
}
newFloor.async = [];

function worker(id, extra) {
  return Object.assign({
    id, title: id, state: "quiet-active", station: "bench", card_id: null, purpose: "chat",
    relationship_kind: "session", provider: "codex", href: "/agent?session=" + id
  }, extra);
}
function order(id, extra) {
  return Object.assign({ id, title: "Card " + id, lane: "active", station: null,
    presentation: { state: "working" }, gantry: null }, extra);
}
function snapshot(workers, orders, bayExtra) {
  return {
    bays: [Object.assign({ id: "local", name: "Local", connectivity: "connected", workers }, bayExtra)],
    work_orders: orders || []
  };
}

// 1. Shared fixture parity with the server mapping.
for (const c of fixture.cases) {
  const got = c.kind === "worker" ? Model.workerStation(c.input)
    : Model.cardStation(c.input, Date.parse("2026-11-09T12:00:00Z"));
  assert.equal(got, c.station, c.name);
}

// 2. Transition -> station move (and announcement text).
{
  const floor = newFloor();
  floor.load(snapshot([worker("ada", { card_id: "c1", purpose: "card", title: "Ada" })],
    [order("c1", { title: "Fix login" })]));
  assert.equal(floor.unit("ada").station, "bench");

  let r = floor.applySessionEvent({ session_id: "ada", type: "user_message", seq: 1 });
  assert.equal(floor.unit("ada").station, "workbench");
  assert.deepEqual(r.moves.map((m) => [m.from, m.to, m.mode]), [["bench", "workbench", "walk"]]);
  assert.deepEqual(r.announcements, ["Ada started working at the workbench."]);

  // Tool kind change is a pose change only: no walk, no announcement.
  r = floor.applySessionEvent({ session_id: "ada", type: "tool_call", seq: 2, payload: { kind: "edit" } });
  assert.equal(floor.unit("ada").pose, "edit");
  assert.deepEqual(r.moves.map((m) => m.kind), ["pose"]);
  assert.equal(floor.unit("ada").station, "workbench");
  assert.deepEqual(r.announcements, []);
  r = floor.applySessionEvent({ session_id: "ada", type: "tool_call", seq: 3, payload: { kind: "execute" } });
  assert.equal(floor.unit("ada").pose, "test");
  r = floor.applySessionEvent({ session_id: "ada", type: "tool_call_update", seq: 4, payload: {} });
  assert.deepEqual(r.moves, []);

  // Permission walks to the bell and back.
  r = floor.applySessionEvent({ session_id: "ada", type: "permission_request", seq: 5 });
  assert.equal(floor.unit("ada").station, "bell");
  assert.deepEqual(r.announcements, ["Ada needs you: permission requested."]);
  r = floor.applySessionEvent({ session_id: "ada", type: "permission_resolved", seq: 6 });
  assert.equal(floor.unit("ada").station, "workbench");
  assert.equal(floor.unit("ada").interaction, null);

  // Turn completed with no PR watch: back to the idle bench.
  r = floor.applySessionEvent({ session_id: "ada", type: "turn_completed", seq: 7 });
  assert.equal(floor.unit("ada").station, "bench");
  assert.deepEqual(r.announcements, ["Ada finished and went to the idle bench."]);

  // Lost connection goes to the infirmary; recovery comes back.
  r = floor.applySessionEvent({ session_id: "ada", type: "connection_lost", seq: 8 });
  assert.equal(floor.unit("ada").station, "infirmary");
  assert.match(r.announcements[0], /infirmary: connection lost/);
  r = floor.applySessionEvent({ session_id: "ada", type: "session_recovered", seq: 9 });
  assert.equal(floor.unit("ada").station, "bench");
}

// 3. turn_completed with an active PR watch carries the card to the gantry;
//    merge -> Shipping, then back to the bench.
{
  const floor = newFloor();
  floor.load(snapshot([worker("bo", { card_id: "c2", purpose: "card", title: "Bo", state: "working", station: "workbench" })],
    [order("c2", { title: "Ship it", gantry: { reasons: ["ci_pending"], pr_number: 9, status: "active" } })]));
  assert.equal(floor.unit("bo").gate.active, true);
  let r = floor.applySessionEvent({ session_id: "bo", type: "turn_completed", seq: 1 });
  assert.equal(floor.unit("bo").station, "gantry");
  assert.equal(r.announcements[0], "Bo took “Ship it” to the review gantry (CI pending).");

  r = floor.applyGateMove({ kind: "gate", card_id: "c2", active: false, status: "merged", reasons: [] });
  assert.equal(floor.unit("bo").station, "shipping");
  assert.equal(r.announcements[0], "Bo shipped “Ship it”.");
  assert.equal(timers.length, 1, "returns to the bench on a timer, not by polling");
  newFloor.async = [];
  timers[0].fn();
  assert.equal(floor.unit("bo").station, "bench");
  assert.equal(newFloor.async.length, 1);
  assert.match(newFloor.async[0].announcements[0], /returned to the idle bench/);
}

// 4. PR gate changes move a free card and an idle agent without refetching.
{
  const floor = newFloor();
  floor.load(snapshot([worker("cy", { card_id: "c3", purpose: "card", title: "Cy" })],
    [order("c3"), order("c4", { lane: "inbox", station: "intake" })]));
  let r = floor.applyGateMove({ kind: "gate", card_id: "c3", active: true, reasons: ["ci_failing", "draft"], pr_number: 1 });
  assert.equal(floor.unit("cy").station, "gantry");
  assert.equal(r.needsRefresh, false);
  r = floor.applyGateMove({ kind: "gate", card_id: "c3", active: false, status: "closed" });
  assert.equal(floor.unit("cy").station, "bench");
  // A gate for a card nobody shows asks for one skeleton refresh.
  r = floor.applyGateMove({ kind: "gate", card_id: "nobody", active: true, reasons: [] });
  assert.equal(r.needsRefresh, true);
  // Free card: gate -> gantry, then closed removes it.
  r = floor.applyGateMove({ kind: "gate", card_id: "c4", active: true, reasons: ["ci_pending"] });
  assert.equal(floor.unit("card:c4").station, "gantry");
}

// 5. Dispatch stages move the reservation token through Queue and Tooling-up.
{
  const floor = newFloor();
  floor.load(snapshot([worker("dispatch:d1", { relationship_kind: "reservation", title: "Reserved worker",
    station: "queue", dispatch_id: "d1", dispatch_state: "queued", card_id: "c5", purpose: null })],
    [order("c5", { title: "Big job" })]));
  let r = floor.applyDispatchMove({ kind: "dispatch", dispatch_id: "d1", state: "materializing", previous_state: "queued" });
  assert.equal(floor.unit("dispatch:d1").station, "tooling");
  assert.deepEqual(r.moves.map((m) => [m.from, m.to]), [["queue", "tooling"]]);
  r = floor.applyDispatchMove({ kind: "dispatch", dispatch_id: "d1", state: "delivering_prompt" });
  assert.equal(r.moves.length, 0);
  // Hand-off to a live session asks for the skeleton, which then keeps identity.
  r = floor.applyDispatchMove({ kind: "dispatch", dispatch_id: "d1", state: "running", session_id: "s9" });
  assert.equal(r.needsRefresh, true);
  r = floor.load(snapshot([worker("s9", { dispatch_id: "d1", card_id: "c5", purpose: "card",
    state: "working", station: "workbench", title: "Reserved worker" })], [order("c5", { title: "Big job" })]));
  assert.equal(r.moves.length, 1);
  assert.equal(r.moves[0].fromId, "dispatch:d1");
  assert.deepEqual([r.moves[0].from, r.moves[0].to, r.moves[0].mode], ["tooling", "workbench", "walk"]);
  // Ended dispatch removes a token; unknown new dispatch asks for a refresh once.
  const f2 = newFloor();
  f2.load(snapshot([worker("dispatch:d2", { relationship_kind: "reservation", station: "queue", dispatch_id: "d2", dispatch_state: "queued" })]));
  f2.applyDispatchMove({ kind: "dispatch", dispatch_id: "d2", state: "cancelled" });
  assert.equal(f2.unit("dispatch:d2"), null);
  assert.equal(f2.applyDispatchMove({ kind: "dispatch", dispatch_id: "dx", state: "queued" }).needsRefresh, true);
  assert.equal(f2.applyDispatchMove({ kind: "dispatch", dispatch_id: "dx", state: "tooling" }).needsRefresh, false);
}

// 6. Overflow, density, expansion.
{
  const floor = newFloor();
  const workers = [];
  for (let i = 0; i < 12; i++) workers.push(worker("w" + i, { title: "Agent " + String(i).padStart(2, "0") }));
  floor.load(snapshot(workers));
  const view = floor.view();
  assert.equal(view.density, "slots");
  const bench = view.bays[0].stations.bench;
  assert.equal(bench.count, 12);
  assert.equal(bench.shown.length, 4);
  assert.equal(bench.overflow, 8);
  assert.equal(floor.toggleExpanded(bench.key), true);
  assert.equal(floor.view().bays[0].stations.bench.shown.length, 12);
  assert.equal(floor.view().bays[0].stations.bench.overflow, 0);
  floor.toggleExpanded(bench.key);
  // Six agents or fewer: full figures.
  const small = newFloor();
  small.load(snapshot(workers.slice(0, 6)));
  assert.equal(small.view().density, "full");
  assert.equal(small.view().capacity, 8);
  // Idle bays shrink to a header at 7+ agents, not before.
  const crowded = newFloor();
  const idleSnapshot = snapshot(workers.slice(0, 7));
  idleSnapshot.bays.push({ id: "quiet", name: "Quiet", connectivity: "connected", workers: [] });
  crowded.load(idleSnapshot);
  assert.deepEqual(crowded.view().bays.map((b) => b.collapsed), [false, true]);
  const few = newFloor();
  const fewSnapshot = snapshot(workers.slice(0, 2));
  fewSnapshot.bays.push({ id: "quiet", name: "Quiet", connectivity: "connected", workers: [] });
  few.load(fewSnapshot);
  assert.deepEqual(few.view().bays.map((b) => b.collapsed), [false, false]);
}

// 7. At most four simultaneous walks; extra transitions snap.
{
  const floor = newFloor();
  const workers = [];
  for (let i = 0; i < 7; i++) workers.push(worker("m" + i));
  floor.load(snapshot(workers));
  const modes = [];
  for (let i = 0; i < 7; i++) {
    const r = floor.applySessionEvent({ session_id: "m" + i, type: "user_message", seq: 1 });
    modes.push(r.moves[0].mode);
  }
  assert.deepEqual(modes, Array(7).fill("walk"), "separate events each walk");
  // One batch (e.g. several moves landing together) is capped.
  const batch = newFloor();
  const batchWorkers = [];
  for (let i = 0; i < 7; i++) batchWorkers.push(worker("b" + i));
  batch.load(snapshot(batchWorkers));
  const gate = batch.applyGateMove({ kind: "gate", card_id: "k", active: true, reasons: [] });
  assert.equal(gate.moves.length, 0);
  const many = newFloor();
  const manyWorkers = [];
  for (let i = 0; i < 7; i++) manyWorkers.push(worker("g" + i, { card_id: "shared", purpose: "card" }));
  many.load(snapshot(manyWorkers, [order("shared")]));
  const r = many.applyGateMove({ kind: "gate", card_id: "shared", active: true, reasons: ["ci_pending"] });
  assert.equal(r.moves.length, 7);
  assert.deepEqual(r.moves.map((m) => m.mode), ["walk", "walk", "walk", "walk", "snap", "snap", "snap"]);
}

// 8. Reduced motion snaps everything; counts still update; announcements still fire.
{
  const floor = newFloor({ reducedMotion: true });
  floor.load(snapshot([worker("rm")]));
  const r = floor.applySessionEvent({ session_id: "rm", type: "user_message", seq: 1 });
  assert.equal(r.moves[0].mode, "snap");
  assert.equal(floor.unit("rm").station, "workbench");
  assert.deepEqual(r.announcements, ["rm started working at the workbench."]);
  assert.equal(floor.view().bays[0].stations.workbench.count, 1);
  floor.setReducedMotion(false);
  const r2 = floor.applySessionEvent({ session_id: "rm", type: "turn_completed", seq: 2 });
  assert.equal(r2.moves[0].mode, "walk");
}

// 9. Unknown sessions ask for one refresh; ended sessions leave; recovery asks for one too.
{
  const floor = newFloor();
  floor.load(snapshot([worker("known")]));
  assert.equal(floor.applySessionEvent({ session_id: "new", type: "session_started", seq: 1 }).needsRefresh, true);
  assert.equal(floor.applySessionEvent({ session_id: "new", type: "user_message", seq: 2 }).needsRefresh, false);
  assert.equal(floor.applySessionEvent({ session_id: "known", type: "stream_recovery" }).needsRefresh, true);
  const r = floor.applySessionEvent({ session_id: "known", type: "session_closed", seq: 3 });
  assert.equal(floor.unit("known"), null);
  assert.deepEqual(r.announcements, ["known left the floor."]);
}

// 10. Snapshot corrections snap (never walk) and unreachable bays stay greyed.
{
  const floor = newFloor();
  floor.load(snapshot([worker("q"), worker("r")]));
  const r = floor.load(snapshot([worker("q", { station: "infirmary" }), worker("r")]));
  assert.deepEqual(r.moves.map((m) => [m.id, m.to, m.mode]), [["q", "infirmary", "snap"]]);
  const down = newFloor();
  down.load(snapshot([worker("far", { station: "offline" })], [], { connectivity: "disconnected", unreachable_since: "2026-11-09T09:00:00Z" }));
  assert.equal(down.bay("local").connected, false);
  assert.equal(down.bay("local").unreachableSince, "2026-11-09T09:00:00Z");
  assert.equal(down.view().bays[0].stations.offline.count, 1);
}

// 11. Chat agents can be switched off.
{
  const floor = newFloor({ chatAgents: false });
  floor.load(snapshot([worker("chat1"), worker("job1", { card_id: "c", purpose: "card" })], [order("c")]));
  assert.equal(floor.unit("chat1"), null);
  assert.ok(floor.unit("job1"));
}

// 12. Follow highlights one agent and clears when it leaves.
{
  const floor = newFloor();
  floor.load(snapshot([worker("f1"), worker("f2")]));
  assert.equal(floor.follow("f1"), "f1");
  assert.equal(floor.follow("f1"), null);
  floor.follow("f2");
  floor.applySessionEvent({ session_id: "f2", type: "session_closed", seq: 1 });
  assert.equal(floor.followed(), null);
}

console.log("ok");
