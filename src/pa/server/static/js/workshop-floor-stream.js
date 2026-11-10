/*
 * Workshop floor activity transport.
 *
 * One multiplexed, resumable `session-events` EventSource per instance (this
 * server, plus each reachable remote peer through the Fleet proxy) shared by
 * every open Workshop tab: a five-second renewable localStorage lease picks the
 * owner, the owner opens the stream and re-broadcasts each event over a
 * BroadcastChannel, followers just listen. This is the same protocol Fleet's
 * Remote Operations uses (docs/FLEET_ACTIVITY_STREAMS.md) with its own lease
 * namespace, so a Workshop tab and a Fleet tab never fight over one lease.
 *
 * The only timer here renews the local lease and reconnects with backoff; it
 * makes no network request. Peers that predate the multiplexed endpoint answer
 * its capabilities probe with 404: the floor then shows their last snapshot and
 * never falls back to per-session SSE.
 */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.PAWorkshopFloorStream = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  var LEASE_MS = 5000;
  var TICK_MS = 1500;
  var LEASE_PREFIX = "pa-workshop-activity-owner-v1:";
  var CHANNEL = "pa-workshop-activity-v1";
  // Everything the floor reacts to; other transcript traffic is ignored.
  var EVENT_TYPES = [
    "user_message", "queue_dequeued", "tool_call", "tool_call_update", "permission_request",
    "permission_resolved", "elicitation_request", "elicitation_resolved", "interaction_request",
    "interaction_superseded", "turn_completed", "connection_lost", "session_started",
    "session_recovered", "session_closed", "prompt_failed", "stream_recovery"
  ];

  function createStream(options) {
    var deps = options.deps || {};
    var setTimer = deps.setTimeout || function (fn, ms) { return setTimeout(fn, ms); };
    var clearTimer = deps.clearTimeout || function (id) { clearTimeout(id); };
    var now = deps.now || Date.now;
    var storage = deps.localStorage || null;
    var tabId = deps.tabId || (Math.random().toString(36).slice(2) + now().toString(36));
    var EventSourceImpl = deps.EventSource || null;
    var fetchJson = deps.fetchJson || function () { return Promise.resolve({ transport: "sse" }); };
    var channel = null;
    try { if (deps.BroadcastChannel) channel = new deps.BroadcastChannel(CHANNEL); } catch (e) { channel = null; }
    var instances = {};
    var tickTimer = null;
    var closed = false;

    function leaseKey(id) { return LEASE_PREFIX + id; }
    function readLease(id) {
      try { return JSON.parse(storage.getItem(leaseKey(id)) || "null"); } catch (e) { return null; }
    }
    function owns(id) {
      if (!storage) return true; // storage-disabled contexts stay isolated: one stream here
      var lease = readLease(id);
      return !!lease && lease.tab_id === tabId && Number(lease.expires_at || 0) > now();
    }
    function acquire(id) {
      if (!storage) return true;
      var current = readLease(id);
      if (current && current.tab_id !== tabId && Number(current.expires_at || 0) > now()) return false;
      try {
        storage.setItem(leaseKey(id), JSON.stringify({ tab_id: tabId, expires_at: now() + LEASE_MS }));
      } catch (e) { return true; }
      return owns(id);
    }
    function release(id) {
      if (storage && owns(id)) {
        try { storage.removeItem(leaseKey(id)); } catch (e) { /* ignore */ }
        if (channel) channel.postMessage({ kind: "owner-released", instance_id: id, tab_id: tabId });
      }
    }

    function accept(id, data) {
      var state = instances[id];
      if (!state || !data) return;
      var session = data.session_id;
      var seq = Number(data.seq || 0);
      if (session && seq) {
        if (seq <= (state.cursors[session] || 0)) return;
        state.cursors[session] = seq;
      }
      options.onEvent(id, data);
    }

    function stopSource(state) {
      state.generation += 1;
      if (state.source) { try { state.source.close(); } catch (e) { /* ignore */ } }
      state.source = null;
      state.starting = false;
      if (state.reconnectTimer) clearTimer(state.reconnectTimer);
      state.reconnectTimer = null;
    }

    function scheduleReconnect(id) {
      var state = instances[id];
      if (!state || closed || state.reconnectTimer) return;
      state.reconnects += 1;
      var delay = Math.min(30000, 1000 * Math.pow(2, Math.min(5, state.reconnects)));
      state.reconnectTimer = setTimer(function () {
        state.reconnectTimer = null;
        if (owns(id)) open(id);
      }, delay);
    }

    function open(id) {
      var state = instances[id];
      if (!state || closed || state.source || state.starting || state.unsupported || !EventSourceImpl) return;
      state.starting = true;
      var generation = ++state.generation;
      var probe = state.local ? Promise.resolve({ transport: "sse" })
        : fetchJson(state.base + "/session-events/capabilities");
      probe.then(function (capability) {
        if (generation !== state.generation || closed) return;
        state.starting = false;
        if (!capability || capability.transport !== "sse") { markUnsupported(id); return; }
        var url = state.base + "/session-events?client_id=" + encodeURIComponent("workshop-" + tabId) +
          "&after=" + encodeURIComponent(JSON.stringify(state.cursors)) +
          "&reconnect_attempt=" + encodeURIComponent(state.reconnects);
        var source = new EventSourceImpl(url);
        state.source = source;
        var sawReady = false;
        source.addEventListener("ready", function () {
          // The first ready follows the skeleton the page just fetched; later
          // ones mean we reconnected and may have missed state.
          if (sawReady || state.everReady) options.onReady(id, { reconnected: true });
          sawReady = true;
          state.everReady = true;
          state.reconnects = 0;
        });
        EVENT_TYPES.forEach(function (type) {
          source.addEventListener(type, function (event) {
            var data;
            try { data = JSON.parse(event.data || "{}"); } catch (e) { return; }
            if (!data.type) data.type = type;
            accept(id, data);
            if (channel) channel.postMessage({ kind: "activity", instance_id: id, data: data });
          });
        });
        source.onerror = function () {
          if (state.source === source) state.source = null;
          try { source.close(); } catch (e) { /* ignore */ }
          if (closed || !owns(id)) return;
          fetchJson(state.base + "/session-events/capabilities").then(function () {
            scheduleReconnect(id);
          }, function (error) {
            if (error && (error.status === 404 || error.status === 410)) markUnsupported(id);
            else scheduleReconnect(id);
          });
        };
      }, function (error) {
        if (generation !== state.generation) return;
        state.starting = false;
        if (error && (error.status === 404 || error.status === 410)) markUnsupported(id);
        else scheduleReconnect(id);
      });
    }

    function markUnsupported(id) {
      var state = instances[id];
      if (!state) return;
      state.unsupported = true;
      stopSource(state);
      if (options.onUnsupported) options.onUnsupported(id);
    }

    function tick() {
      tickTimer = null;
      if (closed) return;
      Object.keys(instances).forEach(function (id) {
        var state = instances[id];
        if (state.unsupported) return;
        if (acquire(id)) {
          state.role = "owner";
          open(id);
        } else {
          if (state.role === "owner" || state.source) stopSource(state);
          state.role = "follower";
        }
      });
      tickTimer = setTimer(tick, TICK_MS);
    }

    if (channel) {
      channel.onmessage = function (event) {
        var message = event.data || {};
        var state = instances[message.instance_id];
        if (!state) return;
        if (message.kind === "activity" && state.role !== "owner") accept(message.instance_id, message.data);
        else if (message.kind === "owner-released") {
          if (tickTimer) clearTimer(tickTimer);
          tickTimer = setTimer(tick, 0);
        }
      };
    }

    return {
      // [{id, base, local}] for every bay whose stream we want. Streams for bays
      // that are gone or unreachable are released.
      setInstances: function (list) {
        var wanted = {};
        list.forEach(function (item) {
          wanted[item.id] = true;
          if (!instances[item.id]) {
            instances[item.id] = { id: item.id, base: item.base, local: !!item.local, source: null,
              cursors: {}, reconnects: 0, generation: 0, role: "idle", starting: false,
              reconnectTimer: null, unsupported: false, everReady: false };
          }
        });
        Object.keys(instances).forEach(function (id) {
          if (wanted[id]) return;
          stopSource(instances[id]);
          release(id);
          delete instances[id];
        });
        if (!tickTimer && !closed) tickTimer = setTimer(tick, 0);
      },
      suspend: function () {
        Object.keys(instances).forEach(function (id) { stopSource(instances[id]); release(id); instances[id].role = "idle"; });
        if (tickTimer) clearTimer(tickTimer);
        tickTimer = null;
      },
      resume: function () {
        if (!closed && !tickTimer) tickTimer = setTimer(tick, 0);
      },
      close: function () {
        closed = true;
        Object.keys(instances).forEach(function (id) { stopSource(instances[id]); release(id); });
        instances = {};
        if (tickTimer) clearTimer(tickTimer);
        tickTimer = null;
        if (channel) { try { channel.close(); } catch (e) { /* ignore */ } }
      },
      role: function (id) { return instances[id] ? instances[id].role : null; },
      cursors: function (id) { return instances[id] ? Object.assign({}, instances[id].cursors) : {}; },
      tabId: tabId
    };
  }

  return { createStream: createStream, EVENT_TYPES: EVENT_TYPES, LEASE_MS: LEASE_MS, TICK_MS: TICK_MS,
    LEASE_PREFIX: LEASE_PREFIX, CHANNEL: CHANNEL };
});
