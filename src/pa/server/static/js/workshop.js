(function () {
  "use strict";

  var pollTimer = null;
  var activitySource = null;
  var cardSource = null;
  var refreshGeneration = 0;
  var selected = null;
  var lastSnapshotAt = "";
  var activeRoot = null;
  var viewPreference = "floor";
  var VIEW_STORAGE_KEY = "pa.workshop.view.v1";

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

  function stateLabel(state) {
    return String(state || "unsupported").replace(/-/g, " ");
  }

  function normalizeView(view) {
    return view === "compact" ? "compact" : "floor";
  }

  function readViewPreference() {
    try { return normalizeView(window.localStorage.getItem(VIEW_STORAGE_KEY)); }
    catch (error) { return "floor"; }
  }

  function storeViewPreference(view) {
    try { window.localStorage.setItem(VIEW_STORAGE_KEY, normalizeView(view)); }
    catch (error) { /* Browser storage can be unavailable without disabling the view. */ }
  }

  function workshopBoundary(root) {
    return root.closest("[data-pa-live-history-boundary='workshop']") || document;
  }

  function cardButton(card, area) {
    var dispatch = card.dispatch_id ? "" : '<button type="button" class="ghost small workshop-dispatch" data-workshop-dispatch="' +
      escapeHtml(card.id) + '" aria-label="Dispatch ' + escapeHtml(card.title) + '">Dispatch</button>';
    return '<div class="workshop-order-wrap"><button type="button" class="workshop-order" data-workshop-kind="card" ' +
      'data-workshop-id="' + escapeHtml(card.id) + '" data-state="' +
      escapeHtml(card.dispatch_state || card.lane) + '" aria-label="' +
      escapeHtml(card.title + ", " + area) + '">' +
      '<span aria-hidden="true">▤</span><strong>' + escapeHtml(card.title) +
      '</strong><small>' + escapeHtml(card.project ? card.project.title : "No project") +
      "</small></button>" + dispatch + "</div>";
  }

  function workerButton(worker) {
    var card = worker.card ? cardButton(worker.card, "at worker") : "";
    var tool = worker.tool_category ?
      '<span class="badge">' + escapeHtml(worker.tool_category) + "</span>" : "";
    return '<div class="workshop-worker-wrap"><button type="button" ' +
      'class="workshop-worker" data-workshop-kind="worker" data-workshop-id="' +
      escapeHtml(worker.id) + '" data-state="' + escapeHtml(worker.state) +
      '" aria-label="' + escapeHtml(worker.title + ", " + stateLabel(worker.state)) +
      '"><span class="workshop-worker-figure" aria-hidden="true"><i></i></span>' +
      '<span><strong>' + escapeHtml(worker.title) + "</strong><small>" +
      escapeHtml(stateLabel(worker.state) +
        (worker.live === false ? " · last known" : "")) +
      "</small></span>" + tool +
      "</button>" + card + "</div>";
  }

  function bayButton(bay) {
    var capacity = bay.capacity.limit == null ? "Capacity unsupported" :
      String(bay.capacity.consumed == null ? "?" : bay.capacity.consumed) + "/" +
      bay.capacity.limit + " slots";
    var workers = bay.workers.length ?
      bay.workers.map(workerButton).join("") :
      '<p class="workshop-empty">No current workers</p>';
    var activityAge = bay.activity_age_seconds == null ? "age unknown" :
      (bay.activity_age_seconds < 2 ? "now" : bay.activity_age_seconds + "s ago");
    return '<section class="workshop-bay" data-state="' +
      escapeHtml(bay.connectivity === "connected" ? bay.activity_freshness : "disconnected") +
      '"><button type="button" class="workshop-bay-header" data-workshop-kind="bay" ' +
      'data-workshop-id="' + escapeHtml(bay.id) + '" aria-label="Inspect work bay ' +
      escapeHtml(bay.name) + '"><span><strong>' + escapeHtml(bay.name) +
      '</strong><small>' + escapeHtml(bay.zone || "No zone") + "</small></span>" +
      '<span class="status status-' +
      (bay.connectivity === "connected" ? "active" : "blocked") + '">' +
      escapeHtml(bay.connectivity) + " · activity " + escapeHtml(bay.activity_freshness) +
      " · " + escapeHtml(activityAge) +
      "</span><span class=\"workshop-capacity\">" + escapeHtml(capacity) +
      "</span></button><div class=\"workshop-bench\">" + workers + "</div></section>";
  }

  function area(name, title, cards, note) {
    return '<section class="workshop-area workshop-area-' + name +
      '" aria-labelledby="workshop-area-' + name + '"><header><h2 id="workshop-area-' +
      name + '">' + title + '</h2><span class="badge">' + cards.length +
      "</span></header><p class=\"muted small\">" + note + "</p><div>" +
      (cards.length ? cards.map(function (card) { return cardButton(card, title); }).join("") :
      '<p class="workshop-empty">Nothing here</p>') + "</div></section>";
  }

  function renderSync(root, data) {
    var sync = root.querySelector("[data-workshop-sync]");
    var nodes = data.sync.nodes || [];
    var problems = nodes.filter(function (node) {
      return node.state !== "fresh" || node.consistent === false ||
        node.conflicts.length || node.offline_peers.length;
    }).length;
    sync.dataset.state = data.sync.state;
    sync.innerHTML = '<button type="button" data-workshop-kind="sync" ' +
      'data-workshop-id="rail" aria-label="Inspect fleet synchronization">' +
      '<span aria-hidden="true">⟷</span><strong>Shared sync rail</strong><span>' +
      escapeHtml(data.sync.state) + " · " + nodes.length + " peers · " +
      problems + " needing attention</span></button><div class=\"workshop-rail-line\" aria-hidden=\"true\"></div>";
  }

  function renderScene(root, data) {
    var scene = root.querySelector("[data-workshop-scene]");
    scene.innerHTML =
      '<section class="workshop-foreman"><span aria-hidden="true">⌂</span><div><strong>Foreman desk</strong>' +
      "<small>Authority: " + escapeHtml(data.authority.instance_id || "unsupported · " + data.authority.mode) +
      (data.authority.current_instance_id === data.authority.instance_id ? " · this instance" : "") +
      "</small></div></section>" +
      area("inbox", "Intake dock", data.areas.inbox || [], "Durably admitted cards remain here until their lane changes.") +
      '<section class="workshop-bays"><header><h2>Work bays</h2><span class="badge">' +
      data.bays.length + "</span></header><div>" +
      (data.bays.length ? data.bays.map(bayButton).join("") :
      '<p class="workshop-empty">No canonical fleet instances are registered.</p>') +
      "</div></section>" +
      area("active", "Active floor", data.areas.active || [], "Canonical Active cards; linked work orders also appear beside their real worker.") +
      area("waiting", "Holding racks", data.areas.waiting || [], "Cards with a durable Waiting disposition.") +
      area("done", "Shipping", data.areas.done || [], "Recently completed work orders.");
  }

  function renderCompact(root, data) {
    var compact = root.querySelector("[data-workshop-compact]");
    var rows = [];
    var linkedCards = {};
    data.bays.forEach(function (bay) {
      if (!bay.workers.length) {
        rows.push('<tr data-workshop-compact-row="bay"><td>Bay</td><td>' +
          '<button class="link-button" type="button" data-workshop-kind="bay" data-workshop-id="' +
          escapeHtml(bay.id) + '">' + escapeHtml(bay.name) + "</button></td><td>—</td><td>" +
          escapeHtml(bay.connectivity + " · " + bay.freshness) + "</td></tr>");
      }
      bay.workers.forEach(function (worker) {
        if (worker.card) linkedCards[worker.card.id] = true;
        rows.push('<tr data-workshop-compact-row="worker"><td>Session</td><td><button class="link-button" type="button" data-workshop-kind="bay" data-workshop-id="' +
          escapeHtml(bay.id) + '">' + escapeHtml(bay.name) + "</button></td><td>" +
          '<button class="link-button" type="button" data-workshop-kind="worker" data-workshop-id="' +
          escapeHtml(worker.id) + '">' + escapeHtml(worker.title) + "</button>" +
          (worker.card ? '<button class="link-button" type="button" data-workshop-kind="card" data-workshop-id="' +
          escapeHtml(worker.card.id) + '">' + escapeHtml(worker.card.title) + "</button>" : "") +
          "</td><td>" + escapeHtml(stateLabel(worker.state)) + "</td></tr>");
      });
    });
    ["inbox", "active", "waiting", "done"].forEach(function (lane) {
      (data.areas[lane] || []).forEach(function (card) {
        if (linkedCards[card.id]) return;
        rows.push('<tr data-workshop-compact-row="card"><td>Card</td><td>' +
          escapeHtml(stateLabel(lane)) + '</td><td><button class="link-button" type="button" ' +
          'data-workshop-kind="card" data-workshop-id="' + escapeHtml(card.id) + '">' +
          escapeHtml(card.title) + "</button></td><td>" +
          escapeHtml(stateLabel(card.dispatch_state || card.lane)) + "</td></tr>");
      });
    });
    compact.innerHTML = '<table class="data-table"><caption>Dense canonical card and session list</caption>' +
      "<thead><tr><th>Type</th><th>Instance / lane</th><th>Session / work order</th><th>State</th></tr></thead><tbody>" +
      (rows.length ? rows.join("") : '<tr><td colspan="4">No fleet activity is available.</td></tr>') +
      "</tbody></table>";
  }

  function findItem(data, kind, id) {
    if (kind === "bay") return data.bays.find(function (bay) { return bay.id === id; });
    if (kind === "card") {
      var lanes = ["inbox", "active", "waiting", "done"];
      for (var i = 0; i < lanes.length; i += 1) {
        var card = (data.areas[lanes[i]] || []).find(function (item) { return item.id === id; });
        if (card) return card;
      }
    }
    if (kind === "worker") {
      for (var j = 0; j < data.bays.length; j += 1) {
        var worker = data.bays[j].workers.find(function (item) { return item.id === id; });
        if (worker) return worker;
      }
    }
    return kind === "sync" ? data.sync : null;
  }

  function detailRows(rows) {
    return "<dl>" + rows.filter(function (row) { return row[1] != null && row[1] !== ""; })
      .map(function (row) { return "<dt>" + escapeHtml(row[0]) + "</dt><dd>" +
        escapeHtml(row[1]) + "</dd>"; }).join("") + "</dl>";
  }

  function inspect(root, data, kind, id) {
    var item = findItem(data, kind, id);
    if (!item) return;
    selected = { kind: kind, id: id };
    root.querySelectorAll("[data-workshop-kind]").forEach(function (button) {
      button.classList.toggle("selected", button.dataset.workshopKind === kind &&
        button.dataset.workshopId === id);
    });
    var panel = root.querySelector("[data-workshop-inspector]");
    if (kind === "bay") {
      panel.innerHTML = "<h2>Work bay</h2><h3>" + escapeHtml(item.name) + "</h3>" +
        detailRows([["Health", item.health], ["Connectivity", item.connectivity],
          ["Freshness", item.freshness], ["Observed", item.observed_at],
          ["Capacity", (item.capacity.consumed == null ? "?" : item.capacity.consumed) +
            "/" + (item.capacity.limit == null ? "unsupported" : item.capacity.limit)],
          ["Providers", item.providers.map(function (p) { return p.name + ": " + p.auth_state; }).join(", ") || "unsupported"]]) +
        '<p><a href="/fleet">Open Fleet operations</a></p>';
    } else if (kind === "worker") {
      panel.innerHTML = "<h2>Worker/session</h2><h3>" + escapeHtml(item.title) + "</h3>" +
        detailRows([["State", stateLabel(item.state)], ["Provider", item.provider || "unsupported"],
          ["Connected", item.connected ? "yes" : "no"], ["Started/updated", item.elapsed_from],
          ["Latest progress", item.latest_progress || "No supported structured progress"],
          ["Active tool", item.tool_category || "unsupported"], ["Dispatch", item.dispatch_id]]) +
        '<p><a href="' + escapeHtml(item.href) + '">Open session</a></p>';
    } else if (kind === "card") {
      var blockers = item.blockers.length ? item.blockers.join("; ") : "None reported";
      var prs = item.pull_requests.map(function (pr) {
        return pr.repository + "#" + pr.pr_number + " · " + pr.status;
      }).join(", ");
      panel.innerHTML = "<h2>Work order</h2><h3>" + escapeHtml(item.title) + "</h3>" +
        detailRows([["Project", item.project ? item.project.title : "No project"], ["Lane", item.lane],
          ["Dispatch", item.dispatch_id], ["Session", item.session_id], ["Dispatch state", item.dispatch_state],
          ["Target instance", item.target_instance_id], ["Blockers", blockers],
          ["Branch", item.branch || "No branch evidence"], ["Pull requests", prs || "No PR evidence"]]) +
        '<p><a href="' + escapeHtml(item.href) + '">Open card detail</a></p>';
    } else {
      var conflicts = item.nodes.reduce(function (sum, node) { return sum + node.conflicts.length; }, 0);
      var offline = item.nodes.reduce(function (sum, node) { return sum + node.offline_peers.length; }, 0);
      panel.innerHTML = "<h2>Shared sync rail</h2>" +
        detailRows([["Realm state", item.state], ["Instances reporting", item.nodes.length],
          ["Conflicts", conflicts], ["Offline peers", offline]]) +
        '<p><a href="/fleet?section=sync">Open Realm sync</a></p>';
    }
  }

  function render(root, data) {
    var focused = document.activeElement && document.activeElement.closest &&
      document.activeElement.closest("[data-workshop-kind]");
    var focusKey = focused ? {
      kind: focused.dataset.workshopKind, id: focused.dataset.workshopId
    } : null;
    root.__workshopData = data;
    renderSync(root, data);
    renderScene(root, data);
    renderCompact(root, data);
    if (selected) inspect(root, data, selected.kind, selected.id);
    if (focusKey) {
      var candidates = root.querySelectorAll("[data-workshop-kind]");
      for (var i = 0; i < candidates.length; i += 1) {
        if (candidates[i].dataset.workshopKind === focusKey.kind &&
            candidates[i].dataset.workshopId === focusKey.id) {
          candidates[i].focus({ preventScroll: true });
          break;
        }
      }
    }
  }

  function acceptSnapshot(root, data) {
    var observed = String((data && data.generated_at) || "");
    if (!data || !observed || (lastSnapshotAt && observed <= lastSnapshotAt)) {
      return false;
    }
    lastSnapshotAt = observed;
    render(root, data);
    return true;
  }

  async function refresh(root, announced) {
    var generation = ++refreshGeneration;
    var status = root.querySelector("[data-workshop-live]");
    if (announced) status.textContent = "Refreshing canonical state…";
    try {
      var response = await fetch("/api/fleet/workshop" + (announced ? "?refresh=true" : ""), {
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
    var requested = normalizeView(view);
    var available = requested === "compact" ? !!compactList : !!scene;
    viewPreference = available ? requested : "floor";
    var compact = viewPreference === "compact";

    if (scene) scene.hidden = compact;
    if (compactList) compactList.hidden = !compact;
    root.dataset.workshopLayout = viewPreference;
    boundary.querySelectorAll("[data-workshop-view]").forEach(function (button) {
      var buttonView = normalizeView(button.dataset.workshopView);
      var buttonAvailable = buttonView === "compact" ? !!compactList : !!scene;
      var active = buttonView === viewPreference;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
      button.disabled = !buttonAvailable;
      button.title = buttonAvailable ? "" :
        (button.textContent.trim() + " is unavailable in the current Workshop state.");
    });
    var heading = root.querySelector("[data-workshop-view-heading]");
    var description = root.querySelector("[data-workshop-view-description]");
    var status = boundary.querySelector("[data-workshop-view-status]");
    if (heading) heading.textContent = compact ? "Compact view" : "Floor view";
    if (description) description.textContent = compact ?
      "Cards and sessions in a dense, scan-friendly operational table." :
      "Current work, where it is running, and what needs attention.";
    if (status) status.textContent = available ?
      "Current layout: " + (compact ? "Compact view" : "Floor view") :
      "Compact view is unavailable in the current Workshop state.";
    if (options.persist !== false) storeViewPreference(viewPreference);
    if (options.preserveScroll !== false && window.requestAnimationFrame) {
      window.requestAnimationFrame(function () {
        window.scrollTo({ left: previousX, top: previousY, behavior: "instant" });
      });
    }
  }

  function stop() {
    refreshGeneration += 1;
    if (pollTimer) window.clearInterval(pollTimer);
    pollTimer = null;
    if (activitySource) activitySource.close();
    if (cardSource) cardSource.close();
    activitySource = null;
    cardSource = null;
    if (activeRoot) activeRoot.removeEventListener("click", handleRootClick);
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
    selected = null;
    lastSnapshotAt = "";
    var initial = readInitial(root);
    if (initial) acceptSnapshot(root, initial);
    viewPreference = readViewPreference();
    setView(root, viewPreference, { persist: false, preserveScroll: false });
    refresh(root, false);
    if (window.EventSource) {
      activitySource = new EventSource("/api/fleet/workshop/events");
      activitySource.onopen = function () {
        stopFallback();
        root.querySelector("[data-workshop-live]").textContent =
          "Live activity transport connected";
      };
      activitySource.addEventListener("snapshot", function (event) {
        var data;
        try { data = JSON.parse(event.data || "{}"); } catch (error) { return; }
        if (!acceptSnapshot(root, data)) return;
        root.querySelector("[data-workshop-live]").textContent =
          "Live · updated " + new Date(data.generated_at).toLocaleTimeString();
      });
      activitySource.onerror = function () {
        root.querySelector("[data-workshop-live]").textContent =
          "Activity reconnecting · showing last-known state";
        startFallback(root);
      };
      cardSource = new EventSource("/api/cards/events?realm=" + encodeURIComponent(root.dataset.realm));
      cardSource.addEventListener("cards-changed", function () { refresh(root, false); });
    } else {
      startFallback(root);
    }
  }

  function handleRootClick(event) {
    var root = event.currentTarget;
    if (event.target.closest("[data-workshop-view]")) return;
    if (event.target.closest("[data-workshop-refresh]")) { refresh(root, true); return; }
    var dispatch = event.target.closest("[data-workshop-dispatch]");
    if (dispatch && window.PACardDispatch) {
      window.PACardDispatch.open(dispatch.dataset.workshopDispatch, root.dataset.realm, dispatch);
      return;
    }
    var target = event.target.closest("[data-workshop-kind]");
    if (target) inspect(root, root.__workshopData, target.dataset.workshopKind,
      target.dataset.workshopId);
  }

  document.addEventListener("click", function (event) {
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
    storageKey: VIEW_STORAGE_KEY
  };
})();
