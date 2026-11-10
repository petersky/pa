/*
 * Workshop floor model: where every agent and work order stands, and how real
 * transitions move them. Pure logic (no DOM, no network, no timers of its own)
 * so it can be tested under node and driven by workshop.js in the browser.
 *
 * The skeleton comes from GET /api/fleet/workshop?view=floor (every worker and
 * work order already carries its server-computed `station`). Motion comes only
 * from real transitions: multiplexed session events, dispatch-stage and PR-gate
 * moves published on /api/cards/events. Nothing here polls.
 *
 * The state -> station tables mirror src/pa/fleet/workshop_stations.py and are
 * both checked against tests/fixtures/workshop_floor_stations.json.
 */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.PAWorkshopFloorModel = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  var STATIONS = ["intake", "queue", "tooling", "workbench", "bell", "gantry",
    "verification", "shipping", "bench", "infirmary", "offline"];
  var BAY_STATIONS = { workbench: true, bench: true, offline: true };
  var CARRY_STATIONS = { queue: true, tooling: true, workbench: true, bell: true,
    gantry: true, verification: true, infirmary: true, offline: true };
  var QUEUE_STATES = { admission_pending: true, waiting_capacity: true, blocked: true,
    queued: true, checking_sync: true };
  var TOOLING_STATES = { materializing: true, provisioning: true, starting_session: true,
    delivering_prompt: true };
  var DISPATCH_ENDED = { failed: true, cancelled: true, completed: true, acknowledged: true };
  var VERIFICATION_STATES = { verification_pending: true, acceptance_pending: true };
  var SHIPPING_RECENT_SECONDS = 30 * 60;
  var STATION_LABELS = {
    intake: "Intake dock", queue: "Queue", tooling: "Tooling-up", workbench: "Workbench",
    bell: "Waiting-on-you bell", gantry: "Review gantry", verification: "Verification desk",
    shipping: "Shipping", bench: "Idle bench", infirmary: "Infirmary", offline: "Unreachable bay"
  };
  var POSE_LABELS = { edit: "editing", test: "running a command", browse: "browsing",
    read: "reading" };
  var DENSITY_FULL_MAX = 6;
  var CAPACITY = { full: 8, slots: 4 };
  var GATE_LABELS = { ci_failing: "CI failing", ci_pending: "CI pending", draft: "draft",
    behind: "behind base", conflicts: "merge conflicts", review_required: "review required" };

  function workerStation(input) {
    if (!input.bay_connected) return "offline";
    if (input.relationship === "reservation") {
      if (QUEUE_STATES[input.dispatch_state]) return "queue";
      if (TOOLING_STATES[input.dispatch_state]) return "tooling";
      return null;
    }
    var state = input.state;
    if (state === "completed" || state === "unsupported") return null;
    if (input.interaction) return "bell";
    if (input.recovery || state === "recovering" || state === "failed" ||
        state === "stalled" || input.connection === "disconnected") return "infirmary";
    if (state === "working") return "workbench";
    if (input.verification) return "verification";
    if (input.gantry) return "gantry";
    return "bench";
  }

  function cardStation(input, nowMs) {
    if (input.lane === "done") {
      var age = input.updated_age_seconds;
      if (age == null && input.updated_at) {
        age = ((nowMs == null ? Date.now() : nowMs) - Date.parse(input.updated_at)) / 1000;
      }
      return age != null && age >= 0 && age <= SHIPPING_RECENT_SECONDS ? "shipping" : null;
    }
    if (VERIFICATION_STATES[input.presentation_state]) return "verification";
    if (input.gantry) return "gantry";
    if (input.lane === "inbox" && !input.has_dispatch && !input.has_session) return "intake";
    return null;
  }

  // ACP tool kinds -> workbench poses.
  function poseForToolKind(kind) {
    switch (String(kind || "").toLowerCase()) {
      case "edit": case "delete": case "move": return "edit";
      case "execute": return "test";
      case "fetch": return "browse";
      case "read": case "search": case "think": case "other": return "read";
      default: return null;
    }
  }

  function stationKey(unit) {
    return BAY_STATIONS[unit.station] ? "bay:" + unit.bayId + ":" + unit.station : unit.station;
  }

  function idleStation(unit) {
    if (!unit.bayConnected) return "offline";
    if (unit.verification) return "verification";
    if (unit.gate && unit.gate.active) return "gantry";
    return "bench";
  }

  function unitLabel(unit) {
    return unit.name || unit.title || unit.id;
  }

  function cardPhrase(unit) {
    return unit.cardTitle ? "“" + unit.cardTitle + "”" : "their work order";
  }

  function announcement(move, unit) {
    var name = unitLabel(unit);
    var gate = unit.gate && unit.gate.reasons && unit.gate.reasons.length ?
      " (" + unit.gate.reasons.map(function (r) { return GATE_LABELS[r] || r; }).join(", ") + ")" : "";
    switch (move.to) {
      case null: return name + " left the floor.";
      case "workbench": return name + " started working at the workbench.";
      case "bell":
        return name + " needs you: " + (unit.interaction && unit.interaction.kind === "input" ?
          "input requested" : "permission requested") + ".";
      case "gantry": return name + " took " + cardPhrase(unit) + " to the review gantry" + gate + ".";
      case "verification": return name + " took " + cardPhrase(unit) + " to the verification desk.";
      case "shipping": return name + " shipped " + cardPhrase(unit) + ".";
      case "intake": return name + " arrived at the intake dock.";
      case "queue": return name + " is waiting in the queue.";
      case "tooling": return name + " is tooling up.";
      case "infirmary":
        return name + " went to the infirmary: " + (move.reason === "connection_lost" ?
          "connection lost" : "needs recovery") + ".";
      case "offline": return name + "’s bay is unreachable.";
      case "bench":
        if (move.from === "bell") return name + " is back at the bench after your answer.";
        if (move.from === "workbench") return name + " finished and went to the idle bench.";
        if (move.from === "shipping") return name + " returned to the idle bench.";
        if (move.from === "infirmary") return name + " recovered and is at the idle bench.";
        return name + " is at the idle bench.";
      default: return name + " moved to " + (STATION_LABELS[move.to] || move.to) + ".";
    }
  }

  function createFloor(options) {
    options = options || {};
    var now = options.now || Date.now;
    var maxWalks = options.maxWalks == null ? 4 : options.maxWalks;
    var lingerMs = options.shipLingerMs == null ? 4000 : options.shipLingerMs;
    var setTimer = options.setTimeout || function (fn, ms) { return setTimeout(fn, ms); };
    var clearTimer = options.clearTimeout || function (id) { clearTimeout(id); };
    var reduced = !!options.reducedMotion;
    var chatAgents = options.chatAgents !== false;
    var onAsync = options.onAsync || function () {};
    var units = {};
    var bays = {};
    var bayOrder = [];
    var absent = {};
    var expanded = {};
    var followId = null;
    var timers = {};
    var announced = [];

    function agentsCount() {
      return Object.keys(units).filter(function (id) { return units[id].type === "agent"; }).length;
    }

    function baySnapshot(bay) {
      return {
        id: bay.id, name: bay.name || bay.id,
        connected: bay.connectivity === "connected",
        unreachableSince: bay.unreachable_since || null,
        zone: bay.zone, capacity: bay.capacity, connectivityLabel: bay.connectivity_label,
        activityLabel: bay.activity_freshness_label
      };
    }

    function makeAgent(worker, bay, orders) {
      var order = orders[worker.card_id] || null;
      var relationship = worker.relationship_kind === "reservation" ? "reservation" : "session";
      var chat = relationship === "session" && (worker.purpose === "chat" || !worker.card_id);
      return {
        id: worker.id, type: "agent", kind: relationship === "reservation" ? "reservation" :
          chat ? "chat" : "session",
        name: worker.title || worker.id, bayId: bay.id, bayName: bay.name || bay.id,
        bayConnected: bay.connectivity === "connected",
        station: worker.station || null, pose: worker.pose || null,
        cardId: worker.card_id || null, cardTitle: order ? order.title : null,
        dispatchId: worker.dispatch_id || null, dispatchState: worker.dispatch_state || null,
        workerState: worker.state, stateLabel: worker.state_label,
        interaction: worker.interaction || null,
        gate: order && order.gantry ? Object.assign({ active: true }, order.gantry) : null,
        verification: !!(order && order.lane !== "done" && order.presentation &&
          VERIFICATION_STATES[order.presentation.state]),
        prompting: worker.station === "workbench",
        restore: null, href: worker.href || null, provider: worker.provider || null,
        progress: worker.latest_progress || null
      };
    }

    function makeCard(order) {
      return {
        id: "card:" + order.id, type: "card", cardId: order.id, cardTitle: order.title,
        name: order.title, bayId: null, station: order.station, lane: order.lane,
        gate: order.gantry ? Object.assign({ active: true }, order.gantry) : null,
        bayConnected: true
      };
    }

    // Replace the skeleton from a floor snapshot. Differences from what events
    // already showed are corrections: they snap, they do not walk.
    function load(snapshot) {
      var previous = units;
      var next = {};
      var orders = {};
      (snapshot.work_orders || []).forEach(function (order) { orders[order.id] = order; });
      bays = {};
      bayOrder = [];
      (snapshot.bays || []).forEach(function (bay) {
        bays[bay.id] = baySnapshot(bay);
        bayOrder.push(bay.id);
        (bay.workers || []).forEach(function (worker) {
          var unit = makeAgent(worker, bay, orders);
          if (!unit.station) return;
          if (unit.kind === "chat" && !chatAgents) return;
          next[unit.id] = unit;
        });
      });
      (snapshot.work_orders || []).forEach(function (order) {
        if (!order.station) return;
        var carried = Object.keys(next).some(function (id) {
          return next[id].cardId === order.id && CARRY_STATIONS[next[id].station];
        });
        if (!carried) next["card:" + order.id] = makeCard(order);
      });
      var moves = [];
      Object.keys(next).forEach(function (id) {
        var unit = next[id];
        var before = previous[id];
        // A reservation token that became a session keeps its identity.
        if (!before && unit.dispatchId && previous["dispatch:" + unit.dispatchId]) {
          before = previous["dispatch:" + unit.dispatchId];
        }
        if (before) {
          unit.restore = before.restore;
          unit.followed = before.followed;
          if (before.id !== unit.id) {
            // The reservation token became a live session: it walks on.
            var handoff = makeMove(unit, before.station, unit.station, "dispatch_started", "walk");
            handoff.fromId = before.id;
            moves.push(handoff);
          } else if (before.station !== unit.station || before.bayId !== unit.bayId) {
            moves.push(makeMove(unit, before.station, unit.station, "snapshot", "snap"));
          }
        }
        delete absent[id];
      });
      Object.keys(previous).forEach(function (id) {
        var gone = !next[id] && !Object.keys(next).some(function (key) {
          return next[key].dispatchId && "dispatch:" + next[key].dispatchId === id;
        });
        if (gone && previous[id].type === "agent") {
          moves.push(makeMove(previous[id], previous[id].station, null, "snapshot", "snap"));
        }
      });
      units = next;
      if (followId && !units[followId]) followId = null;
      return finish({ moves: moves, needsRefresh: false });
    }

    function makeMove(unit, from, to, reason, mode) {
      return { id: unit.id, unit: unit, from: from, to: to, bayId: unit.bayId, reason: reason,
        mode: mode, text: "" };
    }

    function finish(result) {
      // Plan motion: reduced motion snaps everything; at most maxWalks walk.
      var walks = 0;
      result.moves.forEach(function (move) {
        if (move.mode === "walk") {
          if (reduced || walks >= maxWalks) move.mode = "snap";
          else walks += 1;
        }
        if (move.unit && move.kind !== "pose" && move.reason !== "snapshot") {
          move.text = announcement(move, move.unit);
        }
      });
      announced = announced.concat(result.moves.filter(function (m) { return m.text; })
        .map(function (m) { return m.text; })).slice(-50);
      result.announcements = result.moves.filter(function (m) { return m.text; })
        .map(function (m) { return m.text; });
      return result;
    }

    function move(unit, to, reason, out) {
      if (unit.station === to) return;
      var from = unit.station;
      unit.station = to;
      out.push(makeMove(unit, from, to, reason, "walk"));
    }

    function pose(unit, next, out) {
      if (!next || unit.pose === next) return;
      unit.pose = next;
      out.push({ id: unit.id, unit: unit, kind: "pose", pose: next, mode: "snap", text: "" });
    }

    function remove(unit, reason, out) {
      var from = unit.station;
      delete units[unit.id];
      if (followId === unit.id) followId = null;
      var m = makeMove(unit, from, null, reason, "snap");
      out.push(m);
      return m;
    }

    function ship(unit, out) {
      move(unit, "shipping", "shipped", out);
      if (unit.type !== "agent") return;
      cancelTimer(unit.id);
      timers[unit.id] = setTimer(function () {
        delete timers[unit.id];
        var live = units[unit.id];
        if (!live || live.station !== "shipping") return;
        live.cardId = null;
        live.gate = null;
        var moved = [];
        move(live, idleStation(live), "returned", moved);
        onAsync(finish({ moves: moved, needsRefresh: false }));
      }, lingerMs);
    }

    function cancelTimer(id) {
      if (timers[id]) { clearTimer(timers[id]); delete timers[id]; }
    }

    function unknown(id, result) {
      if (!absent[id]) {
        absent[id] = true;
        result.needsRefresh = true;
      }
      return result;
    }

    // One multiplexed session event ({session_id, seq, type, payload, presentation}).
    function applySessionEvent(event) {
      var result = { moves: [], needsRefresh: false };
      var type = event.type;
      if (type === "stream_recovery") { result.needsRefresh = true; return finish(result); }
      var unit = units[event.session_id];
      if (!unit) {
        if (type === "session_started" || type === "user_message" || type === "queue_dequeued" ||
            type === "tool_call" || type === "permission_request" || type === "session_recovered") {
          unknown(event.session_id, result);
        }
        return finish(result);
      }
      var payload = event.payload || {};
      var presentation = event.presentation || null;
      var out = result.moves;
      switch (type) {
        case "user_message": case "queue_dequeued":
          unit.prompting = true;
          if (unit.station !== "bell") move(unit, "workbench", type, out);
          break;
        case "tool_call": case "tool_call_update":
          if (type === "tool_call" || payload.kind) pose(unit, poseForToolKind(payload.kind), out);
          if (type === "tool_call" && unit.station !== "bell" && unit.station !== "workbench") {
            unit.prompting = true;
            move(unit, "workbench", "tool_call", out);
          }
          break;
        case "permission_request": case "elicitation_request": case "interaction_request":
          unit.interaction = { kind: type === "permission_request" ? "permission" : "input",
            count: ((unit.interaction && unit.interaction.count) || 0) + 1 };
          if (unit.station !== "bell") unit.restore = unit.station;
          move(unit, "bell", type, out);
          break;
        case "permission_resolved": case "elicitation_resolved": case "interaction_superseded":
          if (unit.interaction && unit.interaction.count > 1 &&
              !(presentation && !presentation.pending_interaction)) {
            unit.interaction = { kind: unit.interaction.kind, count: unit.interaction.count - 1 };
            break;
          }
          unit.interaction = null;
          move(unit, unit.restore && unit.restore !== "bell" ? unit.restore :
            (unit.prompting ? "workbench" : idleStation(unit)), type, out);
          unit.restore = null;
          break;
        case "turn_completed":
          unit.prompting = false;
          unit.pose = null;
          if (unit.interaction) break;
          move(unit, idleStation(unit), type, out);
          break;
        case "connection_lost":
          unit.prompting = false;
          move(unit, "infirmary", "connection_lost", out);
          break;
        case "session_recovered": case "session_started":
          unit.prompting = !!(presentation && presentation.turn && presentation.turn.state === "running");
          move(unit, unit.prompting ? "workbench" : idleStation(unit), type, out);
          break;
        case "session_closed":
          cancelTimer(unit.id);
          remove(unit, type, out);
          break;
        default: break;
      }
      return finish(result);
    }

    // {kind: "dispatch", dispatch_id, card_id, session_id, state, previous_state}
    function applyDispatchMove(message) {
      var result = { moves: [], needsRefresh: false };
      var id = "dispatch:" + message.dispatch_id;
      var unit = units[id];
      var state = message.state;
      if (!unit) {
        // A reservation we have not seen yet, or a session that just started.
        if (!DISPATCH_ENDED[state]) {
          var hasSession = Object.keys(units).some(function (key) {
            return units[key].dispatchId === message.dispatch_id;
          });
          if (!hasSession) unknown(id, result);
        }
        return finish(result);
      }
      if (DISPATCH_ENDED[state]) {
        remove(unit, "dispatch_" + state, result.moves);
        return finish(result);
      }
      unit.dispatchState = state;
      if (QUEUE_STATES[state]) move(unit, "queue", "dispatch_" + state, result.moves);
      else if (TOOLING_STATES[state]) move(unit, "tooling", "dispatch_" + state, result.moves);
      else if (message.session_id || state === "running") {
        // Hand-off to a live session: the skeleton knows its bay and name.
        result.needsRefresh = true;
      }
      return finish(result);
    }

    // {kind: "gate", card_id, active, status, reasons}
    function applyGateMove(message) {
      var result = { moves: [], needsRefresh: false };
      var out = result.moves;
      var owners = Object.keys(units).map(function (id) { return units[id]; })
        .filter(function (unit) { return unit.cardId === message.card_id; });
      if (!owners.length) {
        if (message.active) result.needsRefresh = true;
        return finish(result);
      }
      owners.forEach(function (unit) {
        if (message.active) {
          unit.gate = { active: true, reasons: message.reasons || [], pr_number: message.pr_number };
          var parked = unit.type === "card" ? unit.station !== "gantry" :
            !unit.prompting && !unit.interaction && unit.station !== "infirmary" &&
            unit.station !== "offline" && unit.station !== "gantry";
          if (parked) move(unit, "gantry", "gate_active", out);
          return;
        }
        unit.gate = null;
        if (message.status === "merged") {
          ship(unit, out);
        } else if (unit.station === "gantry") {
          if (unit.type === "card") remove(unit, "gate_closed", out);
          else move(unit, idleStation(unit), "gate_closed", out);
        }
      });
      return finish(result);
    }

    function viewModel() {
      var count = agentsCount();
      var density = count > DENSITY_FULL_MAX ? "slots" : "full";
      var capacity = CAPACITY[density];
      var byKey = {};
      Object.keys(units).forEach(function (id) {
        var key = stationKey(units[id]);
        (byKey[key] = byKey[key] || []).push(id);
      });
      Object.keys(byKey).forEach(function (key) {
        byKey[key].sort(function (a, b) {
          return (units[a].type === units[b].type ? 0 : units[a].type === "agent" ? -1 : 1) ||
            String(unitLabel(units[a])).localeCompare(String(unitLabel(units[b])));
        });
      });
      function station(key) {
        var ids = byKey[key] || [];
        var open = !!expanded[key];
        var shown = open ? ids : ids.slice(0, capacity);
        return { key: key, ids: ids, shown: shown, overflow: ids.length - shown.length,
          expanded: open, count: ids.length };
      }
      var result = { density: density, agents: count, capacity: capacity, stations: {}, bays: [] };
      ["intake", "queue", "tooling", "bell", "infirmary", "gantry", "verification",
        "shipping"].forEach(function (name) { result.stations[name] = station(name); });
      bayOrder.forEach(function (bayId) {
        var bay = bays[bayId];
        var stationsForBay = {};
        ["workbench", "bench", "offline"].forEach(function (name) {
          stationsForBay[name] = station("bay:" + bayId + ":" + name);
        });
        var occupied = Object.keys(stationsForBay).reduce(function (sum, name) {
          return sum + stationsForBay[name].count;
        }, 0);
        result.bays.push({ bay: bay, stations: stationsForBay, occupied: occupied,
          collapsed: density === "slots" && occupied === 0 && bay.connected });
      });
      return result;
    }

    return {
      load: load,
      applySessionEvent: applySessionEvent,
      applyDispatchMove: applyDispatchMove,
      applyGateMove: applyGateMove,
      view: viewModel,
      unit: function (id) { return units[id] || null; },
      units: function () { return units; },
      bay: function (id) { return bays[id] || null; },
      setReducedMotion: function (value) { reduced = !!value; },
      reducedMotion: function () { return reduced; },
      setChatAgents: function (value) { chatAgents = value !== false; },
      toggleExpanded: function (key) { if (expanded[key]) delete expanded[key]; else expanded[key] = true; return !!expanded[key]; },
      isExpanded: function (key) { return !!expanded[key]; },
      follow: function (id) { followId = followId === id ? null : (units[id] ? id : followId); return followId; },
      followed: function () { return followId; },
      announcements: function () { return announced.slice(); },
      stationKey: stationKey,
      dispose: function () { Object.keys(timers).forEach(cancelTimer); }
    };
  }

  return {
    STATIONS: STATIONS, STATION_LABELS: STATION_LABELS, POSE_LABELS: POSE_LABELS,
    GATE_LABELS: GATE_LABELS, BAY_STATIONS: BAY_STATIONS, CARRY_STATIONS: CARRY_STATIONS,
    DENSITY_FULL_MAX: DENSITY_FULL_MAX, CAPACITY: CAPACITY,
    workerStation: workerStation, cardStation: cardStation, poseForToolKind: poseForToolKind,
    createFloor: createFloor, announcement: announcement, stationKey: stationKey
  };
});
