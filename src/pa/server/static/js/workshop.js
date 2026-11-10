(function () {
  "use strict";

  var pollTimer = null;
  var ageTimer = null;
  var activitySource = null;
  var cardSource = null;
  var refreshGeneration = 0;
  var selected = null;
  var lastSnapshotAt = "";
  var activeRoot = null;
  var viewPreference = "floor";
  var VIEW_STORAGE_KEY = "pa.workshop.view.v1";
  var MOTION_STORAGE_KEY = "pa.workshop.motion.v1";
  var FLOOR_WALK_MS = 600;
  var FLOOR_MAX_WALKS = 4;
  var FLOOR_REFRESH_MS = 700;
  var FLOOR_EVENT_PROTECT_MS = 3000;
  var floorModel = null;
  var floorStream = null;
  var floorRefreshTimer = null;
  var floorFrame = null;
  var floorPending = [];
  var floorMotionPreference = "auto";
  var floorLastSeq = 0;
  var floorWalking = 0;
  var floorEventAt = {};
  var floorEls = {};
  var floorLog = [];
  var reducedMotionQuery = null;
  var PAGE_SIZE = 20;
  // The page renders from the slim "floor" projection; the full snapshot stays
  // available at the same URLs without the view parameter.
  var WORKSHOP_VIEW = "floor";
  var CARD_REFRESH_DEBOUNCE_MS = 500;
  var streamHealthy = false;
  var lastStreamSnapshotMs = 0;
  var cardRefresh = null;
  var query = { filter: "operational", search: "", group: "attention", page: 1 };

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  function readInitial(root) {
    var script = root.querySelector("#pa-workshop-data");
    try { return JSON.parse(script.textContent); }
    catch (error) { return null; }
  }

  function normalizeView(view) {
    return view === "compact" || view === "list" ? "compact" : "floor";
  }

  // A browser's own choice wins; otherwise the instance's workshop_default_view.
  function readViewPreference(root) {
    var fallback = normalizeView(root && root.dataset ? root.dataset.defaultView : "floor");
    try {
      var stored = window.localStorage.getItem(VIEW_STORAGE_KEY);
      return stored ? normalizeView(stored) : fallback;
    } catch (error) { return fallback; }
  }

  function readMotionPreference() {
    try { return window.localStorage.getItem(MOTION_STORAGE_KEY) === "reduce" ? "reduce" : "auto"; }
    catch (error) { return "auto"; }
  }

  // Walking is off when the instance says so, the browser asks for reduced
  // motion, or this browser's toggle is on. Station counts still update.
  function motionReduced(root) {
    if (root && root.dataset && root.dataset.floorMotion === "off") return true;
    if (floorMotionPreference === "reduce") return true;
    return !!(reducedMotionQuery && reducedMotionQuery.matches);
  }

  function storeViewPreference(view) {
    try { window.localStorage.setItem(VIEW_STORAGE_KEY, normalizeView(view)); }
    catch (error) { /* Storage can be unavailable without disabling Workshop. */ }
  }

  function workshopBoundary(root) {
    return root.closest("[data-pa-live-history-boundary='workshop']") || document;
  }

  function formatAge(seconds) {
    if (seconds == null) return "Age unknown";
    if (seconds < -5) {
      var future = Math.abs(seconds);
      if (future < 60) return "In " + future + "s";
      if (future < 3600) return "In " + Math.floor(future / 60) + "m";
      if (future < 86400) return "In " + Math.floor(future / 3600) + "h";
      return "In " + Math.floor(future / 86400) + "d";
    }
    seconds = Math.max(0, seconds);
    if (seconds < 10) return "Just now";
    if (seconds < 60) return seconds + "s ago";
    if (seconds < 3600) return Math.floor(seconds / 60) + "m ago";
    if (seconds < 86400) return Math.floor(seconds / 3600) + "h ago";
    if (seconds < 604800) return Math.floor(seconds / 86400) + "d ago";
    return "Earlier";
  }

  function observedAge(observedAt, fallbackSeconds) {
    var observed = observedAt ? Date.parse(observedAt) : NaN;
    if (!Number.isNaN(observed)) {
      return Math.floor((Date.now() - observed) / 1000);
    }
    return fallbackSeconds;
  }

  function ageMarkup(observedAt, fallbackSeconds) {
    var absolute = observedAt && !Number.isNaN(Date.parse(observedAt))
      ? new Date(observedAt).toISOString().replace("T", " ").replace(".000Z", " UTC")
      : "Time unavailable";
    return '<time datetime="' + escapeHtml(observedAt || "") + '" title="' + escapeHtml(absolute) +
      '" data-workshop-observed-at="' + escapeHtml(observedAt || "") +
      '" data-workshop-fallback-age="' + escapeHtml(fallbackSeconds == null ? "" : fallbackSeconds) +
      '">' + escapeHtml(formatAge(observedAge(observedAt, fallbackSeconds))) + "</time>";
  }

  function updateRelativeAges(root) {
    root.querySelectorAll("[data-workshop-observed-at]").forEach(function (item) {
      var fallback = item.dataset.workshopFallbackAge === "" ? null :
        Number(item.dataset.workshopFallbackAge);
      item.textContent = formatAge(observedAge(item.dataset.workshopObservedAt, fallback));
    });
  }

  function orders(data) {
    return Array.isArray(data.work_orders) ? data.work_orders : [];
  }

  function orderSearchText(order) {
    return [order.title, order.lane_label, order.dispatch_label, order.activity_label,
      order.freshness_label, order.progress_freshness_label, order.outcome_label,
      order.location && order.location.name,
      order.card && order.card.project && order.card.project.title,
      order.session && order.session.title,
      order.reservation && order.reservation.state_label,
      order.reservation && order.reservation.reason,
      (order.attention_reasons || []).join(" ")].filter(Boolean).join(" ").toLowerCase();
  }

  function matches(order) {
    var filterMatch = query.filter === "all" ||
      (query.filter === "live" && order.live) ||
      (query.filter === "attention" && order.attention) ||
      (query.filter === "operational" && (order.live || order.attention));
    var searchMatch = !query.search || orderSearchText(order).indexOf(query.search) !== -1;
    return filterMatch && searchMatch;
  }

  function compareOrders(left, right) {
    if (query.group === "lane" && left.lane !== right.lane) {
      return left.lane_label.localeCompare(right.lane_label);
    }
    if (query.group === "location") {
      var leftLocation = left.location ? left.location.name : "Unassigned";
      var rightLocation = right.location ? right.location.name : "Unassigned";
      if (leftLocation !== rightLocation) return leftLocation.localeCompare(rightLocation);
    }
    if (left.attention !== right.attention) return left.attention ? -1 : 1;
    if (left.live !== right.live) return left.live ? -1 : 1;
    return String(right.updated_at || "").localeCompare(String(left.updated_at || ""));
  }

  function groupLabel(order) {
    if (query.group === "lane") return order.lane_label;
    if (query.group === "location") return order.location ? order.location.name : "Not assigned";
    if (order.attention) return "Needs attention";
    if (order.live) return "Live work";
    return "Inventory";
  }

  function pageSlice(data) {
    var matching = orders(data).filter(matches).sort(compareOrders);
    var pages = Math.max(1, Math.ceil(matching.length / PAGE_SIZE));
    query.page = Math.min(Math.max(1, query.page), pages);
    var start = (query.page - 1) * PAGE_SIZE;
    return { all: matching, page: matching.slice(start, start + PAGE_SIZE), pages: pages };
  }

  function stateCell(label, state) {
    return '<span class="workshop-state" data-state="' + escapeHtml(state || "none") +
      '">' + escapeHtml(label) + "</span>";
  }

  function attentionSummary(order) {
    var axisLabels = {
      card: "Card", dispatch: "Dispatch", session: "Session", reservation: "Reservation",
      progress: "Progress", card_disposition: "Card disposition",
      card_reconciliation: "Card reconciliation", completion_delivery: "Completion delivery"
    };
    var details = Array.isArray(order.attention_details) ? order.attention_details : [];
    if (details.length) {
      return details.map(function (detail) {
        return (axisLabels[detail.axis] || detail.axis || "Attention") + ": " + detail.summary;
      }).join("; ");
    }
    return (order.attention_reasons || []).length ? order.attention_reasons.join("; ") : "None";
  }

  function orderButton(order, className) {
    var relationship = order.session ? '<span class="workshop-relationship">' +
      escapeHtml(order.session.relationship_label) + "</span>" :
      order.reservation ? '<span class="workshop-relationship">' +
      escapeHtml(order.reservation.label + ": " + order.reservation.state_label) + "</span>" :
      '<span class="workshop-relationship">No current session</span>';
    var attention = order.attention ? '<span class="status status-blocked">Needs attention</span>' : "";
    var accessibleState = [order.title, order.lane_label, order.dispatch_label,
      order.activity_label, order.freshness_label, order.progress_freshness_label, order.outcome_label,
      order.attention ? "Needs attention" : "No attention issue"].filter(Boolean).join(", ");
    return '<button type="button" class="' + className + '" data-workshop-kind="card" ' +
      'data-workshop-id="' + escapeHtml(order.id) + '" aria-pressed="false" ' +
      'aria-label="Inspect work order: ' +
      escapeHtml(accessibleState) + '"><span class="workshop-order-heading"><strong>' +
      escapeHtml(order.title) + "</strong>" + attention + "</span>" + relationship +
      '<dl class="workshop-order-states"><div><dt>Lane</dt><dd>' +
      stateCell(order.lane_label, order.lane) + "</dd></div><div><dt>Dispatch</dt><dd>" +
      stateCell(order.dispatch_label, order.dispatch_state) + "</dd></div><div><dt>Activity</dt><dd>" +
      stateCell(order.activity_label, order.activity_state) + "</dd></div><div><dt>Session signal</dt><dd>" +
      stateCell(order.freshness_label, order.freshness) + "</dd></div><div><dt>Progress</dt><dd>" +
      stateCell(order.progress_freshness_label || "No progress signal", order.progress_freshness) +
      "</dd></div><div><dt>Outcome</dt><dd>" +
      stateCell(order.outcome_label, order.evaluated_outcome) + "</dd></div></dl></button>";
  }

  function renderQuery(root, data, result, options) {
    options = options || {};
    var loaded = orders(data).length;
    var inventory = data.inventory || { total: loaded, omitted: 0, loaded: loaded };
    var from = result.page.length ? ((query.page - 1) * PAGE_SIZE) + 1 : 0;
    var to = ((query.page - 1) * PAGE_SIZE) + result.page.length;
    var status = root.querySelector("[data-workshop-results]");
    var filter = root.querySelector("[data-workshop-filter]");
    var search = root.querySelector("[data-workshop-search]");
    var group = root.querySelector("[data-workshop-group]");
    if (filter) filter.value = query.filter;
    if (search && search.value.toLowerCase().trim() !== query.search) search.value = query.search;
    if (group) group.value = query.group;
    if (status) {
      var omitted = loaded - result.all.length;
      var resultText = "Showing " + from + "–" + to + " of " + result.all.length +
        " loaded matches; " + omitted + " loaded work orders omitted by this view. " +
        inventory.omitted + " additional admitted cards are outside this bounded projection; " +
        inventory.total + " admitted in total.";
      if (status.textContent !== resultText) status.textContent = resultText;
    }
    var overflow = root.querySelector("[data-workshop-overflow]");
    if (overflow) {
      overflow.href = inventory.overflow_href || "/work";
      overflow.hidden = !inventory.omitted;
      overflow.textContent = "Open Work board";
    }
    var pager = root.querySelector("[data-workshop-pagination]");
    if (pager) {
      pager.innerHTML = '<button type="button" class="ghost small" data-workshop-page="previous"' +
        (query.page <= 1 ? " disabled" : "") + '>Previous</button><span>Page ' + query.page +
        " of " + result.pages + '</span><span class="sr-only" tabindex="-1" ' +
        'data-workshop-page-status>Page ' + query.page + " of " + result.pages +
        '</span><button type="button" class="ghost small" ' +
        'data-workshop-page="next"' + (query.page >= result.pages ? " disabled" : "") +
        ">Next</button>";
    }
    if (options.announceResults) {
      var announcer = root.querySelector("[data-workshop-announcer]");
      if (announcer) announcer.textContent = result.all.length + " matching work orders, page " +
        query.page + " of " + result.pages + ".";
    }
  }

  function queuedPromptLabel(capacity) {
    var count = Number((capacity || {}).queued_prompts || 0);
    return count + " prompt" + (count === 1 ? "" : "s") + " queued";
  }

  function capacityLabel(capacity) {
    if ((capacity || {}).limit == null) return "Capacity unavailable";
    return String(capacity.consumed == null ? "Unknown" : capacity.consumed) +
      "/" + capacity.limit + " slots used · " + queuedPromptLabel(capacity);
  }

  function bayCard(bay, workOrders) {
    var capacity = capacityLabel(bay.capacity);
    var bayAccessible = [bay.name, bay.connectivity_label,
      bay.activity_freshness_label, capacity, bay.active + " active",
      bay.queued + " queued"].join(", ");
    return '<section class="workshop-bay" data-state="' +
      escapeHtml(bay.connectivity === "connected" ? bay.activity_freshness : "disconnected") +
      '"><button type="button" class="workshop-bay-header" data-workshop-kind="bay" ' +
      'data-workshop-id="' + escapeHtml(bay.id) + '" aria-pressed="false" ' +
      'aria-label="Inspect work bay: ' + escapeHtml(bayAccessible) + '"><span><strong>' +
      escapeHtml(bay.name) +
      "</strong><small>" + escapeHtml(bay.zone || "No zone") + "</small></span><span>" +
      stateCell(bay.connectivity_label || "Disconnected", bay.connectivity) +
      "</span><span class=\"workshop-capacity\">" + escapeHtml(capacity) +
      " · " + escapeHtml(bay.activity_freshness_label || "Activity unavailable") +
      "</span></button><div class=\"workshop-bench\">" +
      (workOrders.length ? workOrders.map(function (order) {
        return orderButton(order, "workshop-operation-card");
      }).join("") : '<p class="workshop-empty">No matching work in this bay.</p>') +
      "</div></section>";
  }

  // ---------------------------------------------------------------------------
  // Workshop floor
  // ---------------------------------------------------------------------------

  var FLOOR_LANES = [
    { id: "attention", title: "Needs you", stations: ["bell", "infirmary"] },
    { id: "dispatch", title: "Intake and dispatch", stations: ["intake", "queue", "tooling"] },
    { id: "bays", title: "Instance bays", stations: [] },
    { id: "outbound", title: "Review and shipping", stations: ["gantry", "verification", "shipping"] }
  ];
  var STATION_HINTS = {
    intake: "New work waiting to be dispatched", queue: "Waiting for capacity or a sync check",
    tooling: "Preparing a workspace and session", bell: "Waiting on a permission or an answer",
    infirmary: "Recovering, disconnected or silent", gantry: "Pull request in review or CI",
    verification: "Waiting on verification or acceptance", shipping: "Recently done",
    workbench: "Working on a turn", bench: "Connected and idle", offline: "Peer unreachable"
  };

  function floorLabel(station) {
    return (window.PAWorkshopFloorModel.STATION_LABELS || {})[station] || station;
  }

  function orderIndex(data) {
    var index = {};
    orders(data).forEach(function (order) { index[order.id] = order; });
    return index;
  }

  function orderChip(order) {
    var accessibleState = [order.title, order.lane_label, order.dispatch_label,
      order.activity_label, order.freshness_label, order.progress_freshness_label, order.outcome_label,
      order.attention ? "Needs attention" : "No attention issue"].filter(Boolean).join(", ");
    return '<button type="button" class="workshop-card-chip" data-workshop-kind="card" ' +
      'data-workshop-id="' + escapeHtml(order.id) + '" aria-pressed="false" ' +
      'aria-label="Inspect work order: ' + escapeHtml(accessibleState) + '"><span>' +
      escapeHtml(order.title) + "</span>" +
      (order.attention ? '<span class="status status-blocked">Needs attention</span>' : "") + "</button>";
  }

  function agentStatusText(unit) {
    var parts = [floorLabel(unit.station)];
    var poses = window.PAWorkshopFloorModel.POSE_LABELS;
    if (unit.station === "workbench" && unit.pose && poses[unit.pose]) parts.push(poses[unit.pose]);
    if (unit.station === "bell" && unit.interaction) {
      parts.push(unit.interaction.kind === "input" ? "needs your answer" : "needs your permission");
    }
    if (unit.station === "gantry" && unit.gate && unit.gate.reasons && unit.gate.reasons.length) {
      parts.push(unit.gate.reasons.map(function (reason) {
        return window.PAWorkshopFloorModel.GATE_LABELS[reason] || reason;
      }).join(", "));
    }
    return parts.join(" · ");
  }

  function unitSignature(unit, order, density) {
    return JSON.stringify([unit.name, unit.station, unit.pose, unit.interaction, unit.gate,
      unit.stateLabel, unit.kind, density, order || null]);
  }

  function unitMarkup(unit, order, density) {
    if (unit.type === "card") return order ? orderChip(order) : "";
    var status = agentStatusText(unit);
    var accessible = [unit.name, status, unit.cardTitle ? "working on " + unit.cardTitle : null,
      unit.kind === "chat" ? "chat agent" : unit.kind === "reservation" ? "reserved worker" : null]
      .filter(Boolean).join(", ");
    var card = "";
    if (order && window.PAWorkshopFloorModel.CARRY_STATIONS[unit.station]) {
      card = density === "full" ? orderButton(order, "workshop-operation-card") : orderChip(order);
    }
    return '<div class="floor-agent-row"><button type="button" class="floor-agent-button" ' +
      'data-workshop-kind="agent" data-workshop-id="' + escapeHtml(unit.id) +
      '" aria-pressed="false" aria-label="Inspect agent: ' + escapeHtml(accessible) + '">' +
      '<span class="workshop-worker-figure" aria-hidden="true"><i></i></span>' +
      '<span class="floor-agent-text"><strong>' + escapeHtml(unit.name) + "</strong><small>" +
      escapeHtml(status) + "</small></span></button>" +
      '<button type="button" class="ghost small floor-follow" data-floor-follow="' +
      escapeHtml(unit.id) + '" aria-pressed="false" aria-label="Follow ' + escapeHtml(unit.name) +
      '">Follow</button></div>' + card;
  }

  function stationSection(key, station, label, level) {
    var heading = "h" + level;
    return '<section class="floor-station" data-station="' + escapeHtml(station) +
      '" data-station-key="' + escapeHtml(key) + '" aria-labelledby="floor-h-' +
      escapeHtml(key.replace(/[^a-z0-9]+/gi, "-")) + '"><' + heading + ' id="floor-h-' +
      escapeHtml(key.replace(/[^a-z0-9]+/gi, "-")) + '">' + escapeHtml(label) +
      ' <span class="floor-count" data-floor-count>0</span></' + heading + ">" +
      '<p class="floor-hint small muted">' + escapeHtml(STATION_HINTS[station] || "") + "</p>" +
      '<ul class="floor-slots" role="list"></ul>' +
      '<button type="button" class="ghost small floor-overflow" data-floor-overflow="' +
      escapeHtml(key) + '" aria-expanded="false" hidden></button></section>';
  }

  function ensureFloorShell(scene) {
    if (scene.querySelector("[data-floor]")) return;
    floorEls = {};
    scene.innerHTML = '<div class="floorplan" data-floor data-density="full">' +
      FLOOR_LANES.map(function (lane) {
        return '<section class="floor-lane" data-floor-lane="' + lane.id + '" aria-labelledby="floor-lane-' +
          lane.id + '"><h2 id="floor-lane-' + lane.id + '">' + escapeHtml(lane.title) + "</h2>" +
          '<div class="floor-lane-body">' + (lane.id === "bays" ? "" : lane.stations.map(function (name) {
            return stationSection(name, name, floorLabel(name), 3);
          }).join("")) + "</div></section>";
      }).join("") +
      '<section class="floor-log" aria-labelledby="floor-log-heading"><h2 id="floor-log-heading">Floor log</h2>' +
      '<ol role="log" aria-live="polite" aria-relevant="additions" data-floor-log></ol></section></div>';
  }

  function bayMarkup(entry) {
    var bay = entry.bay;
    var unreachable = !bay.connected;
    return '<section class="workshop-bay floor-bay" data-bay="' + escapeHtml(bay.id) +
      '" data-collapsed="' + String(!!entry.collapsed) + '" data-state="' +
      (unreachable ? "disconnected" : "connected") + '" aria-label="Bay ' + escapeHtml(bay.name) + '">' +
      '<div data-bay-header></div>' +
      '<p class="floor-bay-offline" data-bay-offline hidden></p>' +
      '<div class="floor-bay-stations">' +
      stationSection("bay:" + bay.id + ":workbench", "workbench", "Workbench", 3) +
      stationSection("bay:" + bay.id + ":bench", "bench", "Idle bench", 3) +
      stationSection("bay:" + bay.id + ":offline", "offline", "Unreachable", 3) + "</div></section>";
  }

  function ageText(iso) {
    var seconds = observedAge(iso, null);
    return seconds == null ? "an unknown time" : formatAge(seconds).toLowerCase();
  }

  function bayHeaderMarkup(bay, entry) {
    var capacity = capacityLabel(bay.capacity);
    var bayAccessible = [bay.name, bay.connectivity_label, bay.activity_freshness_label, capacity,
      bay.active + " active", bay.queued + " queued"].join(", ");
    return '<button type="button" class="workshop-bay-header" data-workshop-kind="bay" ' +
      'data-workshop-id="' + escapeHtml(bay.id) + '" aria-pressed="false" ' +
      'aria-label="Inspect work bay: ' + escapeHtml(bayAccessible) + '"><span><strong>' +
      escapeHtml(bay.name) + "</strong><small>" + escapeHtml(bay.zone || "No zone") + "</small></span><span>" +
      stateCell(bay.connectivity_label || "Disconnected", bay.connectivity) +
      "</span><span class=\"workshop-capacity\">" + escapeHtml(capacity) +
      " · " + escapeHtml(bay.activity_freshness_label || "Activity unavailable") + "</span></button>";
  }

  function floorVisible(unit, index) {
    var order = unit.cardId ? index[unit.cardId] : null;
    if (query.filter === "live" && !(order ? order.live : unit.station === "workbench")) return false;
    if (query.filter === "attention" && !(order ? order.attention : unit.station === "bell" ||
        unit.station === "infirmary")) return false;
    if (query.search) {
      var text = (order ? orderSearchText(order) : "") + " " + String(unit.name || "").toLowerCase() +
        " " + String(unit.bayName || "").toLowerCase();
      if (text.indexOf(query.search) === -1) return false;
    }
    return true;
  }

  function syncStationElement(section, station, index, density) {
    var list = section.querySelector(".floor-slots");
    var overflow = section.querySelector("[data-floor-overflow]");
    section.querySelector("[data-floor-count]").textContent = String(station.count);
    section.dataset.count = String(station.count);
    var desired = [];
    station.shown.forEach(function (id) {
      var unit = floorModel.unit(id);
      if (!unit) return;
      var order = unit.cardId ? index[unit.cardId] : null;
      var el = floorEls[id];
      if (!el) {
        el = document.createElement("li");
        el.className = "floor-unit";
        floorEls[id] = el;
      }
      el.dataset.floorUnit = id;
      el.dataset.type = unit.type;
      el.dataset.kind = unit.kind || "card";
      el.dataset.pose = unit.pose || "";
      el.dataset.state = unit.workerState || "";
      el.classList.toggle("floor-agent", unit.type === "agent");
      el.classList.toggle("floor-card", unit.type === "card");
      var signature = unitSignature(unit, order, density);
      if (el.__signature !== signature) {
        el.innerHTML = unitMarkup(unit, order, density);
        el.__signature = signature;
      }
      desired.push(el);
    });
    var current = Array.prototype.slice.call(list.children);
    var same = current.length === desired.length && current.every(function (el, i) { return el === desired[i]; });
    if (!same) {
      var active = document.activeElement;
      var refocus = active && list.contains(active) ? active : null;
      desired.forEach(function (el, i) {
        if (list.children[i] !== el) list.insertBefore(el, list.children[i] || null);
      });
      while (list.children.length > desired.length) list.removeChild(list.lastChild);
      if (refocus && document.activeElement !== refocus && refocus.isConnected) {
        refocus.focus({ preventScroll: true });
      }
    }
    if (overflow) {
      overflow.hidden = station.overflow <= 0 && !station.expanded;
      overflow.setAttribute("aria-expanded", String(station.expanded));
      overflow.textContent = station.expanded ? "Show fewer" : "+" + station.overflow + " more";
      overflow.setAttribute("aria-label", station.expanded ? "Show fewer at " + floorLabel(section.dataset.station) :
        station.overflow + " more at " + floorLabel(section.dataset.station) + ", expand");
    }
  }

  function renderFloor(root, data, options) {
    options = options || {};
    var scene = root.querySelector("[data-workshop-scene]");
    if (!scene) return;
    if (!floorModel) {
      scene.innerHTML = '<p class="notice">The floor view could not start. Use the Compact view.</p>';
      return;
    }
    ensureFloorShell(scene);
    var index = orderIndex(data);
    var view = floorModel.view(function (unit) { return floorVisible(unit, index); });
    var shell = scene.querySelector("[data-floor]");
    shell.dataset.density = view.density;
    // Reads first: where every walker stands before the DOM changes.
    var walkers = (options.moves || []).filter(function (move) {
      return move.mode === "walk" && move.to && floorEls[move.fromId || move.id];
    });
    var first = {};
    var visibleScene = !scene.hidden && !document.hidden;
    if (visibleScene) {
      walkers.forEach(function (move) {
        var el = floorEls[move.fromId || move.id];
        if (el && el.isConnected) first[move.id] = el.getBoundingClientRect();
      });
    }
    (options.moves || []).forEach(function (move) {
      if (move.fromId && floorEls[move.fromId] && !floorEls[move.id]) {
        floorEls[move.id] = floorEls[move.fromId];
        delete floorEls[move.fromId];
      }
    });
    // Writes: shared stations, then bays.
    FLOOR_LANES.forEach(function (lane) {
      lane.stations.forEach(function (name) {
        var section = shell.querySelector('[data-station-key="' + name + '"]');
        if (section) {
          syncStationElement(section, view.stations[name], index, view.density);
          section.classList.toggle("floor-alert", (name === "bell" || name === "infirmary") &&
            view.stations[name].count > 0);
        }
      });
    });
    var baysBody = shell.querySelector('[data-floor-lane="bays"] .floor-lane-body');
    var wanted = {};
    view.bays.forEach(function (entry) {
      wanted[entry.bay.id] = true;
      var section = baysBody.querySelector('[data-bay="' + CSS.escape(entry.bay.id) + '"]');
      if (!section) {
        baysBody.insertAdjacentHTML("beforeend", bayMarkup(entry));
        section = baysBody.querySelector('[data-bay="' + CSS.escape(entry.bay.id) + '"]');
      }
      var full = data.bays.find(function (bay) { return bay.id === entry.bay.id; });
      var headerHost = section.querySelector("[data-bay-header]");
      var headerSignature = JSON.stringify([full, entry.bay.connected]);
      if (headerHost.__signature !== headerSignature) {
        headerHost.innerHTML = bayHeaderMarkup(full, entry);
        headerHost.__signature = headerSignature;
      }
      section.dataset.collapsed = String(!!entry.collapsed);
      section.dataset.state = entry.bay.connected ? "connected" : "disconnected";
      var offline = section.querySelector("[data-bay-offline]");
      offline.hidden = entry.bay.connected;
      offline.textContent = entry.bay.connected ? "" : "Unreachable" +
        (entry.bay.unreachableSince ? " since " + ageText(entry.bay.unreachableSince) : "") +
        ". Showing last-known agents.";
      ["workbench", "bench", "offline"].forEach(function (name) {
        var stationSectionEl = section.querySelector('[data-station="' + name + '"]');
        syncStationElement(stationSectionEl, entry.stations[name], index, view.density);
        stationSectionEl.hidden = name === "offline" ? entry.bay.connected && !entry.stations.offline.count :
          !entry.bay.connected && name !== "offline";
      });
    });
    Array.prototype.slice.call(baysBody.children).forEach(function (child) {
      if (!wanted[child.dataset.bay]) baysBody.removeChild(child);
    });
    Object.keys(floorEls).forEach(function (id) {
      if (!floorModel.unit(id) || !floorEls[id].isConnected) {
        if (!floorModel.unit(id)) delete floorEls[id];
      }
    });
    var followed = floorModel.followed();
    scene.querySelectorAll("[data-floor-unit]").forEach(function (el) {
      var follow = !!followed && el.dataset.floorUnit === followed;
      el.classList.toggle("following", follow);
      var button = el.querySelector("[data-floor-follow]");
      if (button) button.setAttribute("aria-pressed", String(follow));
    });
    renderFloorLog(scene);
    playWalks(scene, walkers, first);
  }

  function playWalks(scene, walkers, first) {
    if (!walkers.length) return;
    var plans = [];
    walkers.forEach(function (move) {
      var el = floorEls[move.id];
      var before = first[move.id];
      if (!el || !before || !el.isConnected) return;
      var after = el.getBoundingClientRect();
      var dx = before.left - after.left;
      var dy = before.top - after.top;
      if (Math.abs(dx) < 1 && Math.abs(dy) < 1) return;
      if (floorWalking + plans.length >= FLOOR_MAX_WALKS) return;
      plans.push({ el: el, dx: dx, dy: dy });
    });
    if (!plans.length) return;
    plans.forEach(function (plan) {
      plan.el.classList.remove("walking");
      plan.el.style.transition = "none";
      plan.el.style.transform = "translate(" + plan.dx + "px," + plan.dy + "px)";
    });
    void scene.offsetWidth; // one reflow for the whole batch
    plans.forEach(function (plan) {
      floorWalking += 1;
      var done = false;
      function finish() {
        if (done) return;
        done = true;
        floorWalking = Math.max(0, floorWalking - 1);
        plan.el.classList.remove("walking");
        plan.el.style.transition = "";
        plan.el.style.transform = "";
      }
      plan.el.classList.add("walking");
      plan.el.style.transition = "transform " + FLOOR_WALK_MS + "ms ease-in-out";
      plan.el.style.transform = "";
      plan.el.addEventListener("transitionend", finish, { once: true });
      window.setTimeout(finish, FLOOR_WALK_MS + 150);
    });
  }

  function renderFloorLog(scene) {
    var log = scene.querySelector("[data-floor-log]");
    if (!log) return;
    var html = floorLog.slice(-6).map(function (entry) {
      return "<li>" + escapeHtml(entry) + "</li>";
    }).join("");
    if (log.__html !== html) {
      log.innerHTML = html;
      log.__html = html;
    }
  }

  function loadFloor(root, data) {
    if (!floorModel) return { moves: [], announcements: [] };
    var keep = {};
    var cutoff = Date.now() - FLOOR_EVENT_PROTECT_MS;
    Object.keys(floorEventAt).forEach(function (id) {
      if (floorEventAt[id] >= cutoff) keep[id] = true; else delete floorEventAt[id];
    });
    return floorModel.load(data, { keep: keep });
  }

  function noteFloorResult(result) {
    (result.moves || []).forEach(function (move) {
      if (move.reason !== "snapshot") floorEventAt[move.id] = Date.now();
    });
    (result.announcements || []).forEach(function (text) { floorLog.push(text); });
    if (floorLog.length > 50) floorLog = floorLog.slice(-50);
  }

  // Coalesce bursts of live moves into one DOM pass per frame.
  function queueFloorResult(root, result) {
    noteFloorResult(result);
    floorPending.push(result);
    if (result.needsRefresh) scheduleFloorRefresh(root);
    if (floorFrame !== null) return;
    var run = function () {
      floorFrame = null;
      var batch = floorPending;
      floorPending = [];
      if (!root.isConnected || !root.__workshopData) return;
      var moves = [];
      var text = [];
      batch.forEach(function (item) {
        moves = moves.concat(item.moves || []);
        text = text.concat(item.announcements || []);
      });
      renderFloor(root, root.__workshopData, { moves: moves });
      markSelection(root);
      announceFloor(root, text);
      var followed = floorModel && floorModel.followed();
      if (followed && moves.some(function (move) { return move.id === followed; })) {
        var el = floorEls[followed];
        if (el && el.scrollIntoView) el.scrollIntoView({ block: "nearest", behavior: "auto" });
      }
      refreshInspectorForFloor(root, moves);
    };
    floorFrame = window.requestAnimationFrame ? window.requestAnimationFrame(run) :
      window.setTimeout(run, 16);
  }

  function announceFloor(root, lines) {
    if (!lines.length) return;
    var announcer = root.querySelector("[data-workshop-announcer]");
    if (announcer) announcer.textContent = lines.slice(-3).join(" ");
  }

  function scheduleFloorRefresh(root) {
    if (floorRefreshTimer) return;
    floorRefreshTimer = window.setTimeout(function () {
      floorRefreshTimer = null;
      if (activeRoot === root) refresh(root, false);
    }, FLOOR_REFRESH_MS);
  }

  function markSelection(root) {
    if (!selected) return;
    root.querySelectorAll("[data-workshop-kind]").forEach(function (button) {
      var active = button.dataset.workshopKind === selected.kind && button.dataset.workshopId === selected.id;
      button.classList.toggle("selected", active);
      button.setAttribute("aria-pressed", String(active));
    });
  }

  function refreshInspectorForFloor(root, moves) {
    if (selected && selected.kind === "agent" && moves.some(function (move) { return move.id === selected.id; })) {
      inspect(root, root.__workshopData, "agent", selected.id, { announce: false });
    }
  }

  function agentItem(id) {
    var unit = floorModel && floorModel.unit(id);
    return unit && unit.type === "agent" ? unit : null;
  }

  function handleFloorMessage(root, name, data) {
    if (!floorModel) return;
    if (name === "workshop-floor") {
      var seq = Number(data.seq || 0);
      var gap = floorLastSeq && seq && seq !== floorLastSeq + 1;
      if (seq) floorLastSeq = seq;
      if (gap) { scheduleFloorRefresh(root); }
      queueFloorResult(root, data.kind === "gate" ? floorModel.applyGateMove(data) :
        data.kind === "dispatch" ? floorModel.applyDispatchMove(data) : { moves: [] });
    }
  }

  function floorTransportInstances(data) {
    return (data.bays || []).filter(function (bay) {
      return bay.local || bay.connectivity === "connected";
    }).map(function (bay) {
      return bay.local ? { id: bay.id, base: "/api/agent", local: true } :
        { id: bay.id, base: "/api/fleet/instances/" + encodeURIComponent(bay.id) + "/agent" };
    });
  }

  function apiJson(url) {
    return window.fetch(url, { credentials: "same-origin", headers: { Accept: "application/json" } })
      .then(function (response) {
        if (!response.ok) { var error = new Error("HTTP " + response.status); error.status = response.status; throw error; }
        return response.json();
      });
  }

  function floorTabId() {
    try {
      var id = window.sessionStorage.getItem("pa-workshop-tab-id");
      if (!id) {
        id = (window.crypto && window.crypto.randomUUID) ? window.crypto.randomUUID() :
          Date.now().toString(36) + Math.random().toString(36).slice(2);
        window.sessionStorage.setItem("pa-workshop-tab-id", id);
      }
      return id;
    } catch (error) { return Date.now().toString(36) + Math.random().toString(36).slice(2); }
  }

  function startFloorTransport(root) {
    if (floorStream || !floorModel || !window.PAWorkshopFloorStream || !window.EventSource) return;
    var storage = null;
    try { storage = window.localStorage; } catch (error) { storage = null; }
    floorStream = window.PAWorkshopFloorStream.createStream({
      deps: { EventSource: window.EventSource, localStorage: storage, fetchJson: apiJson,
        BroadcastChannel: window.PA_TEST ? null : window.BroadcastChannel, tabId: floorTabId() },
      onEvent: function (instanceId, event) { queueFloorResult(root, floorModel.applySessionEvent(event)); },
      onReady: function () { scheduleFloorRefresh(root); }
    });
    if (root.__workshopData) floorStream.setInstances(floorTransportInstances(root.__workshopData));
  }

  function stopFloorTransport() {
    if (floorStream) floorStream.close();
    floorStream = null;
  }

  function syncFloorTransport(root, data) {
    if (viewPreference !== "floor") return;
    if (!floorStream) startFloorTransport(root);
    else floorStream.setInstances(floorTransportInstances(data));
  }

  function renderCompact(root, result) {
    var compact = root.querySelector("[data-workshop-compact]");
    var currentGroup = null;
    var rows = [];
    result.page.forEach(function (order) {
      var group = groupLabel(order);
      if (group !== currentGroup) {
        currentGroup = group;
        rows.push('<tr class="workshop-group-row"><th colspan="7" scope="rowgroup">' +
          escapeHtml(group) + "</th></tr>");
      }
      var accessibleState = [order.title, order.lane_label, order.dispatch_label,
        order.activity_label, order.freshness_label, order.progress_freshness_label,
        order.outcome_label,
        order.attention ? "Needs attention" : "No attention issue"].filter(Boolean).join(", ");
      rows.push('<tr data-workshop-compact-row="work-order"><td data-label="Work order">' +
        '<button class="link-button" type="button" data-workshop-kind="card" data-workshop-id="' +
        escapeHtml(order.id) + '" aria-pressed="false" aria-label="Inspect work order: ' +
        escapeHtml(accessibleState) + '"><strong>' +
        escapeHtml(order.title) + "</strong></button>" +
        (order.session ? '<span class="workshop-relationship">' +
        escapeHtml(order.session.relationship_label) + "</span>" :
        order.reservation ? '<span class="workshop-relationship">' +
        escapeHtml(order.reservation.label + ": " + order.reservation.state_label) + "</span>" : "") +
        '</td><td data-label="Lane">' + stateCell(order.lane_label, order.lane) +
        '</td><td data-label="Dispatch">' + stateCell(order.dispatch_label, order.dispatch_state) +
        '</td><td data-label="Activity">' + stateCell(order.activity_label, order.activity_state) +
        '</td><td data-label="Session signal">' + stateCell(order.freshness_label, order.freshness) +
        '</td><td data-label="Progress freshness">' +
        stateCell(order.progress_freshness_label || "No progress signal", order.progress_freshness) +
        '</td><td data-label="Evaluated outcome">' + stateCell(order.outcome_label, order.evaluated_outcome) +
        "</td></tr>");
    });
    compact.innerHTML = '<table class="data-table"><caption>Bounded operational work orders</caption>' +
      '<thead><tr><th>Work order</th><th>Card lane</th><th>Dispatch</th><th>Session activity</th>' +
      "<th>Session signal</th><th>Progress freshness</th><th>Evaluated outcome</th></tr></thead><tbody>" +
      (rows.length ? rows.join("") : '<tr><td colspan="7">No work matches this view.</td></tr>') +
      "</tbody></table>";
  }

  function renderSync(root, data) {
    var sync = root.querySelector("[data-workshop-sync]");
    var nodes = data.sync.nodes || [];
    var issues = data.sync.issues || [];
    var accessibleState = ["Shared sync rail", data.sync.state_label || "Status unavailable",
      nodes.length + " peers", issues.length + " needing attention"].join(", ");
    sync.dataset.state = data.sync.state;
    sync.innerHTML = '<button type="button" data-workshop-kind="sync" data-workshop-id="rail" ' +
      'aria-pressed="false" aria-label="' + escapeHtml(accessibleState) +
      '"><span aria-hidden="true">⟷</span>' +
      '<strong>Shared sync rail</strong><span>' + escapeHtml(data.sync.state_label || "Status unavailable") +
      " · " + nodes.length + " peers · " + issues.length +
      " needing attention</span></button>";
  }

  function findItem(data, kind, id) {
    if (kind === "bay") return data.bays.find(function (bay) { return bay.id === id; });
    if (kind === "card") return orders(data).find(function (order) { return order.id === id; });
    if (kind === "agent") return agentItem(id);
    return kind === "sync" ? data.sync : null;
  }

  function detailRows(rows) {
    return "<dl>" + rows.filter(function (row) { return row[1] != null && row[1] !== ""; })
      .map(function (row) { return "<dt>" + escapeHtml(row[0]) + "</dt><dd>" +
        (row[2] ? row[1] : escapeHtml(row[1])) + "</dd>"; }).join("") + "</dl>";
  }

  function clearInspection(root, message) {
    selected = null;
    root.removeAttribute("data-workshop-has-selection");
    root.querySelectorAll("[data-workshop-kind]").forEach(function (button) {
      button.classList.remove("selected");
      button.setAttribute("aria-pressed", "false");
    });
    var panel = root.querySelector("[data-workshop-inspector]");
    panel.innerHTML = "<h2>Inspector</h2><p class=\"muted\">" +
      escapeHtml(message || "Select an item on the floor for operational detail and safe links.") + "</p>";
  }

  function inspect(root, data, kind, id, options) {
    options = options || {};
    var item = findItem(data, kind, id);
    if (!item) return false;
    selected = { kind: kind, id: id };
    root.dataset.workshopHasSelection = "true";
    root.querySelectorAll("[data-workshop-kind]").forEach(function (button) {
      var active = button.dataset.workshopKind === kind && button.dataset.workshopId === id;
      button.classList.toggle("selected", active);
      button.setAttribute("aria-pressed", String(active));
    });
    var panel = root.querySelector("[data-workshop-inspector]");
    if (kind === "bay") {
      panel.innerHTML = "<h2>Work bay</h2><h3>" + escapeHtml(item.name) + "</h3>" +
        detailRows([["Connectivity", item.connectivity_label], ["Fleet status", item.freshness_label],
          ["Activity status", item.activity_freshness_label],
          ["Last activity", ageMarkup(item.activity_observed_at, item.activity_age_seconds), true],
          ["Capacity", capacityLabel(item.capacity)]]) +
        '<p><a data-workshop-focus-key="bay-detail" href="/fleet?instance=' +
        escapeHtml(item.id) + '">Open Fleet details</a></p>';
    } else if (kind === "agent") {
      var carried = item.cardId ? orderIndex(data)[item.cardId] : null;
      var poseLabel = window.PAWorkshopFloorModel.POSE_LABELS[item.pose];
      panel.innerHTML = "<h2>" + (item.kind === "reservation" ? "Reserved worker" : "Agent") + "</h2><h3>" +
        escapeHtml(item.name) + "</h3>" +
        detailRows([["Station", floorLabel(item.station)], ["Bay", item.bayName],
          ["Current activity", item.station === "workbench" && poseLabel ? poseLabel : item.stateLabel],
          ["Waiting on you", item.interaction ?
            (item.interaction.kind === "input" ? "An answer is needed" : "A permission is needed") : null],
          ["Pull request", item.gate && item.gate.pr_number ? "#" + item.gate.pr_number +
            (item.gate.reasons && item.gate.reasons.length ? " · " + item.gate.reasons.map(function (reason) {
              return window.PAWorkshopFloorModel.GATE_LABELS[reason] || reason;
            }).join(", ") : "") : null],
          ["Provider", item.provider], ["Work order", carried ? carried.title : null],
          ["Latest progress", item.progress]]) +
        '<p class="workshop-inspector-links">' +
        (item.href ? '<a data-workshop-focus-key="session-detail" href="' + escapeHtml(item.href) +
          '">Open agent session</a>' : "") +
        (carried ? '<a data-workshop-focus-key="card-detail" href="' + escapeHtml(carried.card.href) +
          '">Open card detail</a>' : "") + "</p>";
    } else if (kind === "card") {
      var card = item.card;
      var reasons = attentionSummary(item);
      var relationship = item.session ? item.session.relationship_label :
        item.reservation ? item.reservation.label + ": " + item.reservation.state_label :
        "No current session";
      var action = "";
      if (card.can_dispatch) {
        action = '<p><button type="button" class="primary" data-workshop-focus-key="dispatch" data-workshop-dispatch="' +
          escapeHtml(card.id) + '">Dispatch work order</button></p>';
      } else if (card.dispatch_unavailable_reason) {
        action = '<p class="muted small">Dispatch unavailable: ' +
          escapeHtml(card.dispatch_unavailable_reason) + "</p>";
      }
      panel.innerHTML = "<h2>Work order</h2><h3>" + escapeHtml(item.title) + "</h3>" +
        (!matches(item) ? '<p class="notice small" data-workshop-selection-context>' +
          "Selected work is outside the current filtered results.</p>" : "") +
        detailRows([["Card lane", item.lane_label], ["Dispatch", item.dispatch_label],
          ["Session relationship", relationship], ["Session activity", item.activity_label],
          ["Reservation reason", item.reservation && item.reservation.reason],
          ["Queue position", item.reservation && item.reservation.queue_position],
          ["Agent turn", item.agent_turn && item.agent_turn.ended ? "Ended" : "In progress or not started"],
          ["Dispatch completion", item.dispatch_completion && item.dispatch_completion.completed ?
            "Completed" : "Not completed"],
          ["Card disposition", item.card_completion && item.card_completion.status],
          ["Reconciliation", item.card_reconciliation && item.card_reconciliation.state],
          ["Session signal freshness", item.freshness_label],
          ["Dispatch progress freshness", item.progress_freshness_label],
          ["Dispatch progress age", ageMarkup(item.progress_last_activity_at,
            item.progress_age_seconds), true],
          ["Evaluated outcome", item.outcome_label],
          ["Location", item.location ? item.location.name : "Not assigned"],
          ["Needs attention", reasons], ["Latest progress", item.session && item.session.latest_progress]]) +
        action + '<p class="workshop-inspector-links"><a data-workshop-focus-key="card-detail" href="' +
        escapeHtml(card.href) + '">Open card detail</a>' +
        (item.session ? '<a data-workshop-focus-key="session-detail" href="' +
          escapeHtml(item.session.href) + '">Open linked session</a>' : "") + "</p>";
    } else {
      var issues = item.issues || [];
      var issueMarkup = issues.length ? '<ul class="workshop-sync-issues">' +
        issues.map(function (issue) {
          return "<li><strong>" + escapeHtml(issue.peer_name) + "</strong><span>" +
            escapeHtml(issue.condition_label) + " · " +
            ageMarkup(issue.observed_at, issue.age_seconds) +
            "</span><span>" + escapeHtml(issue.summary) + "</span><span>Recovery: " +
            escapeHtml(issue.recovery_label) + (issue.recovery_attempt ?
            " · attempt " + escapeHtml(issue.recovery_attempt) : "") +
            '</span><a data-workshop-focus-key="sync-' + escapeHtml(issue.instance_id) +
            '" href="' + escapeHtml(issue.href) + '">Open sync details for ' +
            escapeHtml(issue.peer_name) + "</a></li>";
        }).join("") + "</ul>" : '<p class="muted">No peers currently need attention.</p>';
      var raw = (item.nodes || []).map(function (node) {
        return { instance_id: node.instance_id, state: node.state,
          durable_head: node.durable_head, projection_head: node.projection_head,
          conflicts: node.conflicts, offline_peers: node.offline_peers };
      });
      panel.innerHTML = "<h2>Shared sync rail</h2>" +
        detailRows([["Realm state", item.state_label], ["Peers reporting", item.nodes.length],
          ["Peers needing attention", issues.length]]) + issueMarkup +
        '<p><a data-workshop-focus-key="realm-sync" href="/fleet?section=sync">Open Realm sync</a></p>' +
        '<details><summary>Raw diagnostics</summary><pre>' +
        escapeHtml(JSON.stringify(raw, null, 2)) + "</pre></details>";
    }
    if (options.announce !== false) {
      var announcer = root.querySelector("[data-workshop-announcer]");
      if (announcer) announcer.textContent = "Inspector updated for " +
        (kind === "card" ? item.title : kind === "bay" || kind === "agent" ? item.name :
          "Shared sync rail") + ".";
    }
    updateRelativeAges(root);
    if (options.reveal) {
      var rect = panel.getBoundingClientRect();
      var stacked = window.matchMedia && window.matchMedia("(max-width: 1100px)").matches;
      var outsideViewport = rect.top < 0 || rect.bottom > window.innerHeight;
      if (stacked || outsideViewport) {
        panel.focus({ preventScroll: true });
        panel.scrollIntoView({ block: "nearest" });
      }
    }
    return true;
  }

  function captureFocus(root) {
    var active = document.activeElement;
    if (!active || !root.contains(active)) return null;
    var item = active.closest && active.closest("[data-workshop-kind]");
    if (item) return { type: "item", kind: item.dataset.workshopKind,
      id: item.dataset.workshopId };
    if (active.dataset && active.dataset.workshopFocusKey) {
      return { type: "inspector", key: active.dataset.workshopFocusKey };
    }
    if (active.dataset && active.dataset.workshopPage) {
      return { type: "page", direction: active.dataset.workshopPage };
    }
    return null;
  }

  function restoreFocus(root, focusKey) {
    if (!focusKey) return false;
    var candidate = null;
    if (focusKey.type === "item") {
      var candidates = Array.from(root.querySelectorAll("[data-workshop-kind]")).filter(function (button) {
        return button.dataset.workshopKind === focusKey.kind &&
          button.dataset.workshopId === focusKey.id;
      });
      candidate = candidates.find(function (button) { return !button.closest("[hidden]"); }) ||
        candidates[0];
    } else if (focusKey.type === "inspector") {
      candidate = root.querySelector('[data-workshop-focus-key="' +
        CSS.escape(focusKey.key) + '"]');
    } else if (focusKey.type === "page") {
      candidate = root.querySelector('[data-workshop-page="' + focusKey.direction + '"]');
      if (candidate && candidate.disabled) candidate = null;
    }
    if (!candidate) return false;
    candidate.focus({ preventScroll: true });
    return true;
  }

  function restoreFocusContext(root, focusKey) {
    if (!focusKey) return false;
    var candidate = null;
    if (focusKey.type === "inspector" && selected) {
      candidate = root.querySelector('[data-workshop-focus-key="card-detail"]') ||
        root.querySelector("[data-workshop-inspector]");
    }
    if (!candidate && selected) {
      candidate = Array.from(root.querySelectorAll("[data-workshop-kind]")).find(function (item) {
        return item.dataset.workshopKind === selected.kind &&
          item.dataset.workshopId === selected.id && !item.closest("[hidden]");
      });
    }
    if (!candidate) candidate = root.querySelector("[data-workshop-search]");
    if (!candidate) return false;
    candidate.focus({ preventScroll: true });
    return true;
  }

  function render(root, data, options) {
    options = options || {};
    var focusKey = captureFocus(root);
    root.__workshopData = data;
    var result = pageSlice(data);
    renderQuery(root, data, result, options);
    renderSync(root, data);
    renderFloor(root, data, { moves: options.moves || [] });
    renderCompact(root, result);
    var selectionRemoved = false;
    if (selected && !inspect(root, data, selected.kind, selected.id, { announce: false })) {
      selectionRemoved = true;
      clearInspection(root, "The selected item is no longer in the bounded Workshop view.");
      var announcer = root.querySelector("[data-workshop-announcer]");
      if (announcer) announcer.textContent = "Workshop selection cleared because the item is no longer available.";
    }
    if (selectionRemoved) {
      var nearby = root.querySelector("[data-workshop-search]");
      if (nearby) nearby.focus({ preventScroll: true });
    } else {
      if (!restoreFocus(root, focusKey)) restoreFocusContext(root, focusKey);
    }
    updateRelativeAges(root);
  }

  function acceptSnapshot(root, data) {
    var observed = String((data && data.generated_at) || "");
    if (!data || !observed || (lastSnapshotAt && observed <= lastSnapshotAt)) return false;
    lastSnapshotAt = observed;
    var floorResult = loadFloor(root, data);
    noteFloorResult(floorResult);
    render(root, data, { moves: floorResult.moves });
    announceFloor(root, floorResult.announcements || []);
    syncFloorTransport(root, data);
    return true;
  }

  async function refresh(root, announced) {
    var generation = ++refreshGeneration;
    var status = root.querySelector("[data-workshop-live]");
    if (announced) status.textContent = "Refreshing canonical state…";
    try {
      var response = await fetch("/api/fleet/workshop?view=" + WORKSHOP_VIEW + (announced ? "&refresh=true" : ""), {
        credentials: "same-origin", headers: { Accept: "application/json" }
      });
      if (!response.ok) throw new Error("Workshop refresh failed (" + response.status + ")");
      var data = await response.json();
      if (generation !== refreshGeneration || !document.body.contains(root)) return;
      acceptSnapshot(root, data);
      status.textContent = "Live · updated " + new Date(data.generated_at).toLocaleTimeString();
      root.querySelector("[data-workshop-alert]").hidden = true;
    } catch (error) {
      if (generation !== refreshGeneration) return;
      status.textContent = "Disconnected · showing last-known state";
      var alert = root.querySelector("[data-workshop-alert]");
      alert.hidden = false;
      alert.textContent = error.message + ". Reconnect will retry safely.";
    }
  }

  function setView(root, view, options) {
    options = options || {};
    var previousX = window.scrollX;
    var previousY = window.scrollY;
    var scene = root.querySelector("[data-workshop-scene]");
    var compactList = root.querySelector("[data-workshop-compact]");
    var boundary = workshopBoundary(root);
    viewPreference = normalizeView(view);
    var compact = viewPreference === "compact";
    scene.hidden = compact;
    compactList.hidden = !compact;
    var pager = root.querySelector("[data-workshop-pagination]");
    if (pager) pager.hidden = !compact;
    root.dataset.workshopLayout = viewPreference;
    boundary.querySelectorAll("[data-workshop-view]").forEach(function (button) {
      var active = normalizeView(button.dataset.workshopView) === viewPreference;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    var heading = root.querySelector("[data-workshop-view-heading]");
    var description = root.querySelector("[data-workshop-view-description]");
    var status = boundary.querySelector("[data-workshop-view-status]");
    if (heading) heading.textContent = compact ? "Compact view" : "Floor view";
    if (description) description.textContent = compact ?
      "A bounded operational list with every state available at every width." :
      "Agents at the stations that match their real state, moving only when something changes.";
    if (status) status.textContent = "Current layout: " + (compact ? "Compact view" : "Floor view");
    if (options.persist !== false) storeViewPreference(viewPreference);
    if (compact) {
      // No floor on screen: release its shared activity connection.
      stopFloorTransport();
    } else if (root.__workshopData && !options.skipFloorReload) {
      // Back on the floor: catch up from the latest snapshot, then listen again.
      var back = loadFloor(root, root.__workshopData);
      renderFloor(root, root.__workshopData, { moves: back.moves });
      markSelection(root);
      syncFloorTransport(root, root.__workshopData);
    }
    if (options.preserveScroll !== false && window.requestAnimationFrame) {
      window.requestAnimationFrame(function () {
        window.scrollTo({ left: previousX, top: previousY, behavior: "instant" });
      });
    }
  }

  // Coalesce bursts of card changes into one trailing refresh, and skip it when
  // the live snapshot stream already delivered a snapshot after the last change.
  function createRefreshScheduler(options) {
    var timer = null;
    var lastChangeAt = 0;
    return {
      notify: function () {
        lastChangeAt = options.now();
        if (timer !== null) options.clear(timer);
        timer = options.set(function () {
          timer = null;
          if (options.deliveredSince(lastChangeAt)) return;
          options.refresh();
        }, options.delay);
      },
      cancel: function () {
        if (timer !== null) options.clear(timer);
        timer = null;
      }
    };
  }

  function stop() {
    refreshGeneration += 1;
    if (cardRefresh) cardRefresh.cancel();
    cardRefresh = null;
    streamHealthy = false;
    lastStreamSnapshotMs = 0;
    stopFloorTransport();
    if (floorRefreshTimer) window.clearTimeout(floorRefreshTimer);
    floorRefreshTimer = null;
    floorPending = [];
    if (floorModel) floorModel.dispose();
    floorModel = null;
    floorEls = {};
    floorEventAt = {};
    floorLastSeq = 0;
    if (reducedMotionQuery && reducedMotionQuery.removeEventListener) {
      reducedMotionQuery.removeEventListener("change", handleMotionChange);
    }
    if (pollTimer) window.clearInterval(pollTimer);
    pollTimer = null;
    if (ageTimer) window.clearInterval(ageTimer);
    ageTimer = null;
    if (activitySource) activitySource.close();
    if (cardSource) cardSource.close();
    activitySource = null;
    cardSource = null;
    if (activeRoot) {
      activeRoot.removeEventListener("click", handleRootClick);
      activeRoot.removeEventListener("input", handleRootInput);
      activeRoot.removeEventListener("change", handleRootChange);
      activeRoot.removeEventListener("submit", handleRootSubmit);
      activeRoot.removeEventListener("keydown", handleRootKeydown);
    }
    activeRoot = null;
  }

  function startFallback(root) {
    if (pollTimer) return;
    pollTimer = window.setInterval(function () { refresh(root, false); }, 10000);
  }

  function stopFallback() {
    if (pollTimer) window.clearInterval(pollTimer);
    pollTimer = null;
  }

  function init() {
    var root = document.querySelector("#pa-workshop-root");
    if (!root) { if (activeRoot) stop(); return; }
    if (root === activeRoot) return;
    stop();
    activeRoot = root;
    root.addEventListener("click", handleRootClick);
    root.addEventListener("input", handleRootInput);
    root.addEventListener("change", handleRootChange);
    root.addEventListener("submit", handleRootSubmit);
    root.addEventListener("keydown", handleRootKeydown);
    selected = null;
    lastSnapshotAt = "";
    query = { filter: "operational", search: "", group: "attention", page: 1 };
    floorMotionPreference = readMotionPreference();
    reducedMotionQuery = window.matchMedia ? window.matchMedia("(prefers-reduced-motion: reduce)") : null;
    if (reducedMotionQuery && reducedMotionQuery.addEventListener) {
      reducedMotionQuery.addEventListener("change", handleMotionChange);
    }
    floorLog = [];
    if (window.PAWorkshopFloorModel) {
      floorModel = window.PAWorkshopFloorModel.createFloor({
        reducedMotion: motionReduced(root), maxWalks: FLOOR_MAX_WALKS,
        chatAgents: root.dataset.floorChatAgents !== "false",
        onAsync: function (result) { queueFloorResult(root, result); }
      });
    }
    // Pick the layout first so the first snapshot does not open a floor
    // connection for a browser that opens straight into the compact list.
    viewPreference = readViewPreference(root);
    root.dataset.workshopLayout = viewPreference;
    var initial = readInitial(root);
    if (initial) acceptSnapshot(root, initial);
    setView(root, viewPreference, { persist: false, preserveScroll: false, skipFloorReload: true });
    syncMotionControl(root);
    refresh(root, false);
    ageTimer = window.setInterval(function () { updateRelativeAges(root); }, 30000);
    if (window.EventSource) {
      cardRefresh = createRefreshScheduler({
        delay: CARD_REFRESH_DEBOUNCE_MS,
        now: Date.now,
        set: function (callback, delay) { return window.setTimeout(callback, delay); },
        clear: function (handle) { window.clearTimeout(handle); },
        deliveredSince: function (changedAt) {
          return streamHealthy && lastStreamSnapshotMs >= changedAt;
        },
        refresh: function () { refresh(root, false); }
      });
      activitySource = new EventSource("/api/fleet/workshop/events?view=" + WORKSHOP_VIEW);
      activitySource.onopen = function () {
        streamHealthy = true;
        stopFallback();
        root.querySelector("[data-workshop-live]").textContent = "Live activity connected";
      };
      activitySource.addEventListener("snapshot", function (event) {
        var data;
        try { data = JSON.parse(event.data || "{}"); } catch (error) { return; }
        lastStreamSnapshotMs = Date.now();
        if (!acceptSnapshot(root, data)) return;
        root.querySelector("[data-workshop-live]").textContent =
          "Live · updated " + new Date(data.generated_at).toLocaleTimeString();
      });
      activitySource.onerror = function () {
        streamHealthy = false;
        root.querySelector("[data-workshop-live]").textContent =
          "Activity reconnecting · showing last-known state";
        startFallback(root);
      };
      // include=workshop adds dispatch-stage and PR-gate moves as ``workshop-floor``
      // events; ``cards-changed`` keeps its usual meaning.
      cardSource = new EventSource("/api/cards/events?realm=" +
        encodeURIComponent(root.dataset.realm) + "&include=workshop");
      cardSource.addEventListener("cards-changed", function () { cardRefresh.notify(); });
      cardSource.addEventListener("workshop-floor", function (event) {
        var message;
        try { message = JSON.parse(event.data || "{}"); } catch (error) { return; }
        handleFloorMessage(root, "workshop-floor", message);
      });
    } else {
      startFallback(root);
    }
  }

  function handleMotionChange() {
    if (activeRoot && floorModel) floorModel.setReducedMotion(motionReduced(activeRoot));
  }

  function syncMotionControl(root) {
    var boundary = workshopBoundary(root);
    var forced = root.dataset.floorMotion === "off";
    boundary.querySelectorAll("[data-workshop-motion]").forEach(function (button) {
      var reduced = forced || floorMotionPreference === "reduce";
      button.setAttribute("aria-pressed", String(reduced));
      button.disabled = forced;
      button.textContent = forced ? "Motion off (set by this instance)" :
        reduced ? "Motion reduced" : "Reduce motion";
    });
    if (floorModel) floorModel.setReducedMotion(motionReduced(root));
  }

  function rerender(root, options) {
    if (root.__workshopData) render(root, root.__workshopData, options);
  }

  function handleRootInput(event) {
    if (!event.target.matches("[data-workshop-search]")) return;
    query.search = event.target.value.toLowerCase().trim();
    query.page = 1;
    rerender(event.currentTarget, { announceResults: true });
    var search = event.currentTarget.querySelector("[data-workshop-search]");
    if (search) {
      search.focus({ preventScroll: true });
      search.setSelectionRange(search.value.length, search.value.length);
    }
  }

  function handleRootChange(event) {
    if (event.target.matches("[data-workshop-filter]")) query.filter = event.target.value;
    else if (event.target.matches("[data-workshop-group]")) query.group = event.target.value;
    else return;
    query.page = 1;
    rerender(event.currentTarget, { announceResults: true });
    var replacement = event.currentTarget.querySelector("[" +
      (event.target.matches("[data-workshop-filter]") ? "data-workshop-filter" : "data-workshop-group") + "]");
    if (replacement) replacement.focus({ preventScroll: true });
  }

  // Arrow keys walk focus across the floor's controls in reading order; F
  // follows the focused agent; Escape collapses an expanded station.
  function handleRootKeydown(event) {
    var root = event.currentTarget;
    var scene = root.querySelector("[data-workshop-scene]");
    if (!scene || scene.hidden || !scene.contains(event.target)) return;
    var controls = Array.prototype.slice.call(scene.querySelectorAll(
      "[data-workshop-kind], [data-floor-overflow]:not([hidden])"));
    var at = controls.indexOf(event.target.closest("[data-workshop-kind], [data-floor-overflow]"));
    if (event.key === "f" || event.key === "F") {
      var agent = event.target.closest('[data-workshop-kind="agent"]');
      if (agent && !event.ctrlKey && !event.metaKey && !event.altKey) {
        var button = agent.closest("[data-floor-unit]").querySelector("[data-floor-follow]");
        if (button) { event.preventDefault(); button.click(); }
      }
      return;
    }
    var step = { ArrowRight: 1, ArrowDown: 1, ArrowLeft: -1, ArrowUp: -1 }[event.key];
    if (at < 0 || (!step && event.key !== "Home" && event.key !== "End")) return;
    var target = event.key === "Home" ? controls[0] : event.key === "End" ? controls[controls.length - 1] :
      controls[at + step];
    if (target) { event.preventDefault(); target.focus(); }
  }

  function handleRootSubmit(event) {
    if (event.target.matches(".workshop-query")) event.preventDefault();
  }

  function handleRootClick(event) {
    var root = event.currentTarget;
    if (event.target.closest("[data-workshop-refresh]")) { refresh(root, true); return; }
    var page = event.target.closest("[data-workshop-page]");
    if (page) {
      query.page += page.dataset.workshopPage === "next" ? 1 : -1;
      rerender(root, { announceResults: true });
      var replacement = root.querySelector('[data-workshop-page="' +
        page.dataset.workshopPage + '"]');
      if (replacement && !replacement.disabled) replacement.focus({ preventScroll: true });
      else {
        var counterpart = root.querySelector('[data-workshop-page="' +
          (page.dataset.workshopPage === "next" ? "previous" : "next") + '"]');
        var fallback = counterpart && !counterpart.disabled ? counterpart :
          root.querySelector("[data-workshop-page-status]");
        if (fallback) fallback.focus({ preventScroll: true });
      }
      return;
    }
    var follow = event.target.closest("[data-floor-follow]");
    if (follow && floorModel) {
      var now = floorModel.follow(follow.dataset.floorFollow);
      renderFloor(root, root.__workshopData, {});
      markSelection(root);
      announceFloor(root, [now ? "Following " + (agentItem(now) ? agentItem(now).name : "agent") + "." :
        "No longer following an agent."]);
      var again = root.querySelector('[data-floor-follow="' + CSS.escape(follow.dataset.floorFollow) + '"]');
      if (again) again.focus({ preventScroll: true });
      return;
    }
    var overflow = event.target.closest("[data-floor-overflow]");
    if (overflow && floorModel) {
      var key = overflow.dataset.floorOverflow;
      floorModel.toggleExpanded(key);
      renderFloor(root, root.__workshopData, {});
      markSelection(root);
      var next = root.querySelector('[data-floor-overflow="' + CSS.escape(key) + '"]');
      if (next) next.focus({ preventScroll: true });
      return;
    }
    var dispatch = event.target.closest("[data-workshop-dispatch]");
    if (dispatch && window.PACardDispatch) {
      window.PACardDispatch.open(dispatch.dataset.workshopDispatch, root.dataset.realm, dispatch);
      return;
    }
    var target = event.target.closest("[data-workshop-kind]");
    if (target) inspect(root, root.__workshopData, target.dataset.workshopKind,
      target.dataset.workshopId, { reveal: true });
  }

  document.addEventListener("click", function (event) {
    var motion = event.target.closest("[data-workshop-motion]");
    if (motion) {
      var motionRoot = document.querySelector("#pa-workshop-root");
      if (!motionRoot) return;
      floorMotionPreference = floorMotionPreference === "reduce" ? "auto" : "reduce";
      try { window.localStorage.setItem(MOTION_STORAGE_KEY, floorMotionPreference); } catch (error) { /* optional */ }
      syncMotionControl(motionRoot);
      announceFloor(motionRoot, [motionReduced(motionRoot) ?
        "Motion reduced: agents jump between stations." : "Agents will walk between stations."]);
      return;
    }
    var view = event.target.closest("[data-workshop-view]");
    if (!view) return;
    var root = document.querySelector("#pa-workshop-root");
    if (root) setView(root, view.dataset.workshopView);
  });
  document.addEventListener("DOMContentLoaded", init);
  document.addEventListener("htmx:afterSwap", init);
  document.addEventListener("htmx:beforeSwap", function (event) {
    var target = event.detail && event.detail.target;
    if (activeRoot && (!target || target === activeRoot || target.contains(activeRoot))) stop();
  });
  document.addEventListener("pa:historyWillReload", stop);
  if (window.PA_TEST) window.PAWorkshopTest = {
    acceptSnapshot: acceptSnapshot,
    shouldAcceptSnapshot: function (data) {
      var observed = String((data && data.generated_at) || "");
      return !!observed && (!lastSnapshotAt || observed > lastSnapshotAt);
    },
    markSnapshot: function (generatedAt) { lastSnapshotAt = generatedAt; },
    reset: function () { lastSnapshotAt = ""; },
    getView: function () { return viewPreference; },
    setView: setView,
    updateRelativeAges: updateRelativeAges,
    createRefreshScheduler: createRefreshScheduler,
    floorModel: function () { return floorModel; },
    handleFloorMessage: function (root, data) { handleFloorMessage(root, "workshop-floor", data); },
    applyFloorEvent: function (root, event) { queueFloorResult(root, floorModel.applySessionEvent(event)); },
    debounceMs: CARD_REFRESH_DEBOUNCE_MS,
    view: WORKSHOP_VIEW,
    storageKey: VIEW_STORAGE_KEY,
    pageSize: PAGE_SIZE
  };
})();
