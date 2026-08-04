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
  var PAGE_SIZE = 20;
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
    return view === "compact" ? "compact" : "floor";
  }

  function readViewPreference() {
    try { return normalizeView(window.localStorage.getItem(VIEW_STORAGE_KEY)); }
    catch (error) { return "floor"; }
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
    if (seconds < 60) return seconds + " seconds ago";
    if (seconds < 3600) return Math.floor(seconds / 60) + " minutes ago";
    if (seconds < 86400) return Math.floor(seconds / 3600) + " hours ago";
    return Math.floor(seconds / 86400) + " days ago";
  }

  function observedAge(observedAt, fallbackSeconds) {
    var observed = observedAt ? Date.parse(observedAt) : NaN;
    if (!Number.isNaN(observed)) {
      return Math.max(0, Math.floor((Date.now() - observed) / 1000));
    }
    return fallbackSeconds;
  }

  function ageMarkup(observedAt, fallbackSeconds) {
    return '<span data-workshop-observed-at="' + escapeHtml(observedAt || "") +
      '" data-workshop-fallback-age="' + escapeHtml(fallbackSeconds == null ? "" : fallbackSeconds) +
      '">' + escapeHtml(formatAge(observedAge(observedAt, fallbackSeconds))) + "</span>";
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
      overflow.href = inventory.overflow_href || "/";
      overflow.hidden = !inventory.omitted;
      overflow.textContent = "Open " + inventory.omitted + " additional cards in Cards";
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

  function bayCard(bay, workOrders) {
    var capacity = bay.capacity.limit == null ? "Capacity unavailable" :
      String(bay.capacity.consumed == null ? "Unknown" : bay.capacity.consumed) +
      " of " + bay.capacity.limit + " slots used · " + queuedPromptLabel(bay.capacity);
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

  function renderScene(root, data, result) {
    var scene = root.querySelector("[data-workshop-scene]");
    var byLocation = {};
    result.page.forEach(function (order) {
      var key = order.location ? order.location.id : "unassigned";
      (byLocation[key] = byLocation[key] || []).push(order);
    });
    var activeBays = data.bays.filter(function (bay) {
      return byLocation[bay.id] || bay.connectivity !== "connected";
    });
    var assigned = activeBays.map(function (bay) {
      return bayCard(bay, byLocation[bay.id] || []);
    }).join("");
    var unassigned = byLocation.unassigned || [];
    scene.innerHTML = '<section class="workshop-bays" aria-labelledby="workshop-bays-heading">' +
      '<header><div><h2 id="workshop-bays-heading">Active bays and attention</h2>' +
      '<p class="muted small">Current work is shown once at its operating location.</p></div>' +
      '<span class="badge">' + result.page.length + " shown</span></header><div>" +
      (assigned || '<p class="workshop-empty">No matching bay activity.</p>') + "</div></section>" +
      (unassigned.length ? '<section class="workshop-unassigned" aria-labelledby="workshop-unassigned-heading">' +
      '<header><h2 id="workshop-unassigned-heading">Not in a work bay</h2><span class="badge">' +
      unassigned.length + '</span></header><div class="workshop-card-grid">' +
      unassigned.map(function (order) { return orderButton(order, "workshop-operation-card"); }).join("") +
      "</div></section>" : "");
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
          ["Capacity", (item.capacity.consumed == null ? "Unknown" : item.capacity.consumed) +
            " of " + (item.capacity.limit == null ? "unknown" : item.capacity.limit) +
            " · " + queuedPromptLabel(item.capacity)]]) +
        '<p><a data-workshop-focus-key="bay-detail" href="/fleet?instance=' +
        escapeHtml(item.id) + '">Open Fleet details</a></p>';
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
        (kind === "card" ? item.title : kind === "bay" ? item.name : "Shared sync rail") + ".";
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
    renderScene(root, data, result);
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
    viewPreference = normalizeView(view);
    var compact = viewPreference === "compact";
    scene.hidden = compact;
    compactList.hidden = !compact;
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
      "Current work, where it is running, and what needs attention.";
    if (status) status.textContent = "Current layout: " + (compact ? "Compact view" : "Floor view");
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
    selected = null;
    lastSnapshotAt = "";
    query = { filter: "operational", search: "", group: "attention", page: 1 };
    var initial = readInitial(root);
    if (initial) acceptSnapshot(root, initial);
    viewPreference = readViewPreference();
    setView(root, viewPreference, { persist: false, preserveScroll: false });
    refresh(root, false);
    ageTimer = window.setInterval(function () { updateRelativeAges(root); }, 30000);
    if (window.EventSource) {
      activitySource = new EventSource("/api/fleet/workshop/events");
      activitySource.onopen = function () {
        stopFallback();
        root.querySelector("[data-workshop-live]").textContent = "Live activity connected";
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
      cardSource = new EventSource("/api/cards/events?realm=" +
        encodeURIComponent(root.dataset.realm));
      cardSource.addEventListener("cards-changed", function () { refresh(root, false); });
    } else {
      startFallback(root);
    }
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
    storageKey: VIEW_STORAGE_KEY,
    pageSize: PAGE_SIZE
  };
})();
