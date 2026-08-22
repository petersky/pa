/**
 * Fleet wizard UI — join tokens, SSH install, register, remove, realm invites.
 * Passwords are sent once over the authenticated session and never kept in JS storage.
 * Uses event delegation so HTMX page swaps do not stack duplicate handlers.
 */
(function () {
  if (window.__paFleetBound) return;
  window.__paFleetBound = true;

  function csrfHeaders() {
    var headers = { "Content-Type": "application/json", Accept: "application/json" };
    if (window.PACSRF) return window.PACSRF.headers(headers);
    var meta = document.querySelector('meta[name="csrf-token"]');
    if (meta && meta.content) headers["X-CSRF-Token"] = meta.content;
    return headers;
  }

  function $(sel, root) {
    return (root || document).querySelector(sel);
  }

  function $all(sel, root) {
    return Array.prototype.slice.call((root || document).querySelectorAll(sel));
  }

  async function api(path, opts) {
    var options = Object.assign(
      { credentials: "same-origin", headers: csrfHeaders() },
      opts || {}
    );
    if (options.body && typeof options.body === "object" && !(options.body instanceof FormData)) {
      options.body = JSON.stringify(options.body);
    }
    var resp = await fetch(path, options);
    var text = await resp.text();
    var data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch (e) {
      data = { detail: text };
    }
    if (!resp.ok) {
      var detail = (data && data.detail) || resp.statusText || "Request failed";
      var error = new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
      error.detail = detail;
      error.status = resp.status;
      if (window.PASessionRecovery) {
        error.retryAfterMs = window.PASessionRecovery.responseRetryAfterMs(
          resp, detail
        );
      }
      throw error;
    }
    return data;
  }

  function showPanel(name) {
    $all("[data-fleet-panel]").forEach(function (el) {
      var show = el.getAttribute("data-fleet-panel") === name;
      el.hidden = !show;
      el.classList.toggle("hidden", !show);
    });
    $all("[data-fleet-path]").forEach(function (btn) {
      btn.classList.toggle("active", btn.getAttribute("data-fleet-path") === name);
    });
  }

  function formToObject(form) {
    var fd = new FormData(form);
    var obj = {};
    fd.forEach(function (value, key) {
      obj[key] = typeof value === "string" ? value.trim() : value;
    });
    return obj;
  }

  function commaList(value) {
    return String(value || "").split(",").map(function (item) {
      return item.trim();
    }).filter(Boolean);
  }

  function clearSecrets(form) {
    ["password", "passphrase", "sudo_password"].forEach(function (name) {
      var el = form.elements[name];
      if (el) el.value = "";
    });
  }

  var fleetPageRefreshGeneration = 0;
  var fleetPageRefreshRequest = null;
  var fleetPageRefreshController = null;
  var fleetPageRefreshUrl = "";

  function isExpectedHtmxAbort(error) {
    var name = String(error && error.name || "");
    var message = String(error && error.message || error || "").toLowerCase();
    return name === "AbortError" ||
      message.indexOf("signal is aborted") !== -1 ||
      message.indexOf("request cancelled") !== -1 ||
      message.indexOf("request canceled") !== -1;
  }

  function abortFleetPageRefresh() {
    fleetPageRefreshGeneration += 1;
    var controller = fleetPageRefreshController;
    fleetPageRefreshRequest = null;
    fleetPageRefreshController = null;
    fleetPageRefreshUrl = "";
    if (controller) controller.abort();
  }

  function refreshFleetPage() {
    var section = "";
    try {
      section = new URL(window.location.href).searchParams.get("section") || "";
    } catch (e) {}
    var url = "/fleet" + (section ? "?section=" + encodeURIComponent(section) : "");
    if (window.PANavigation) {
      return window.PANavigation.navigate(url).catch(function () {});
    }
    if (!window.htmx) {
      location.href = url;
      return Promise.resolve();
    }
    if (fleetPageRefreshRequest && fleetPageRefreshUrl === url) {
      return fleetPageRefreshRequest;
    }
    if (fleetPageRefreshRequest) abortFleetPageRefresh();
    var generation = ++fleetPageRefreshGeneration;
    var target = document.querySelector("#app-view");
    var controller = new AbortController();
    var headers = {
      Accept: "text/html",
      "HX-Request": "true",
      "HX-Target": "app-view",
      "X-PA-Navigation-Generation": String(generation)
    };
    var csrf = document.querySelector('meta[name="csrf-token"]');
    if (csrf && csrf.content) headers["X-CSRF-Token"] = csrf.content;
    fleetPageRefreshUrl = url;
    fleetPageRefreshController = controller;
    var request = fetch(url, {
      credentials: "same-origin",
      headers: headers,
      signal: controller.signal
    }).then(function (response) {
      if (generation !== fleetPageRefreshGeneration) return null;
      if (response.status === 204 || response.status === 304) return null;
      if (!response.ok) {
        var failure = new Error(response.statusText || "Fleet page refresh failed");
        failure.status = response.status;
        throw failure;
      }
      return response.text();
    }).then(function (html) {
      if (html === null || generation !== fleetPageRefreshGeneration) return;
      if (!target) {
        location.href = url;
        return;
      }
      htmx.swap(target, html, { swapStyle: "innerHTML" });
      history.pushState({}, "", url);
      htmx.trigger(document.body, "htmx:pushedIntoHistory", { path: url });
    }).catch(function (error) {
      if (generation !== fleetPageRefreshGeneration || isExpectedHtmxAbort(error)) return;
      console.error("Fleet page refresh failed", {
        operation: "fleet-page-refresh", url: url, generation: generation,
        status: Number(error && error.status || 0) || "network"
      });
    }).finally(function () {
      if (generation !== fleetPageRefreshGeneration) return;
      fleetPageRefreshRequest = null;
      fleetPageRefreshController = null;
      fleetPageRefreshUrl = "";
    });
    fleetPageRefreshRequest = request;
    return request;
  }

  function escapeHtml(text) {
    return String(text == null ? "" : text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fleetIdentityFallback(instanceId) {
    var id = String(instanceId || "");
    var node = fleetOverview && (fleetOverview.nodes || []).find(function (item) {
      return String(item.id || "") === id;
    });
    return node && node.name
      ? node.name
      : "Unknown instance · " + id.slice(0, 8);
  }

  function identityHtml(instanceId, className) {
    if (window.PAInstanceIdentity) {
      return window.PAInstanceIdentity.html(instanceId, className);
    }
    var id = String(instanceId || "");
    return id
      ? '<span title="' + escapeHtml(id) + '">' +
        escapeHtml(fleetIdentityFallback(id)) + "</span>"
      : '<span class="muted">Unknown instance</span>';
  }

  function identityName(instanceId) {
    return window.PAInstanceIdentity
      ? window.PAInstanceIdentity.resolve(instanceId).displayName
      : fleetIdentityFallback(instanceId);
  }

  function endpointIdentityHtml(instanceId) {
    return instanceId ? identityHtml(instanceId) : '<span class="muted">external</span>';
  }

  function endpointIdentityName(instanceId) {
    return instanceId ? identityName(instanceId) : "external";
  }

  var remoteInstanceId = "";
  var remoteOperationsSectionActive = false;
  var remoteActivitySource = null;
  var remoteActivityInstanceId = "";
  var remoteActivityStartingInstanceId = "";
  var remoteActivityGeneration = 0;
  var remoteActivitySessions = {};
  var remoteActivityCursors = {};
  var remoteActivityReconnects = 0;
  var remoteActivityReconnectTimer = null;
  var remoteActivityPollTimer = null;
  var remoteActivityLeaseTimer = null;
  var remoteActivityCompatibility = {};
  var remoteActivityEventTypes = [
    "user_message", "agent_message_chunk", "agent_thought_chunk",
    "tool_call", "tool_call_update", "plan", "permission_request",
    "permission_resolved", "turn_completed", "queue_enqueued",
    "queue_dequeued", "queue_removed", "queue_reordered", "queue_paused",
    "queue_resumed", "cancelled", "session_started", "session_closed",
    "session_recovered", "browser_attachment_changed", "connection_lost",
    "usage_update", "model_changed", "mode_changed", "config_changed",
    "config_option_update", "current_mode_update", "card_disposition",
    "error", "message"
  ];
  var remoteNotificationEventTypes = [
    "turn_completed", "permission_request", "error", "connection_lost"
  ];
  var remoteTabId = (function () {
    var value = "";
    try {
      value = sessionStorage.getItem("pa-fleet-tab-id") || "";
      if (!value) {
        value = (crypto.randomUUID ? crypto.randomUUID() :
          Date.now().toString(36) + Math.random().toString(36).slice(2));
        sessionStorage.setItem("pa-fleet-tab-id", value);
      }
    } catch (e) {
      value = Date.now().toString(36) + Math.random().toString(36).slice(2);
    }
    return value;
  })();
  var remoteActivityChannel = null;
  try {
    if (!window.PA_TEST && typeof BroadcastChannel !== "undefined") {
      remoteActivityChannel = new BroadcastChannel("pa-fleet-activity-v1");
    }
  } catch (e) {}
  var remoteLoadGeneration = 0;
  var remoteSessionRecovery = null;
  var remoteSessionSnapshot = [];
  var remoteHistorySnapshot = [];
  var remoteAuditGeneration = 0;
  var remoteAuditSessionId = "";
  var remoteAuditEvents = [];
  var remoteDispatchTimer = null;
  var syncPollTimer = null;
  var syncAllConflicts = [];
  var syncCurrentConflicts = [];
  var syncSelectedRemoteHead = "";

  function syncRealm() {
    var root = $("#pa-fleet-root");
    if (!root) return "default";
    try {
      return new URL(window.location.href).searchParams.get("realm") ||
        root.dataset.primaryRealm || "default";
    } catch (e) {
      return root.dataset.primaryRealm || "default";
    }
  }

  function shortHead(head) {
    return head ? String(head).slice(0, 12) : "—";
  }

  function displayValue(value) {
    if (value === undefined) return "not set";
    if (value === null) return "null";
    if (typeof value === "string") return value;
    try { return JSON.stringify(value); } catch (e) { return String(value); }
  }

  function renderSyncState(state) {
    var progress = $("#pa-sync-progress");
    var tbody = $("#pa-sync-instances tbody");
    var realm = $("#pa-sync-realm");
    if (realm) realm.textContent = state.realm_id || syncRealm();
    var labels = {
      idle: "Waiting for the next automatic anti-entropy pass.",
      checking: "Checking instance heads…",
      exchanging: "Exchanging missing history objects…",
      propagating: "Propagating the merged head to reachable instances…",
      retrying: "A head changed during convergence; automatic retry is scheduled.",
      converged: "Converged. Every reachable instance reports the same realm head.",
      degraded: "Reachable instances are repaired; unavailable instances will retry automatically.",
      conflict: "Convergence needs an operator decision for incompatible field edits.",
    };
    if (progress) {
      progress.textContent = labels[state.phase] || (state.phase || "Checking…");
      if (state.phase === "converged" && $("[data-remote-dispatch-retry]")) {
        progress.innerHTML = escapeHtml(progress.textContent) +
          ' <button type="button" class="primary small" data-sync-retry-dispatch>' +
          "Return and retry dispatch</button>";
      }
    }
    var instances = state.instances || [];
    if (tbody) {
      tbody.innerHTML = instances.length ? instances.map(function (item) {
        var status = item.status || "unknown";
        var badge = status === "reachable" ? "active" :
          status === "conflict" ? "blocked" : "open";
        return "<tr><td>" + identityHtml(item.instance_id) +
          (item.url ? '<br><span class="muted small">' +
          escapeHtml(item.url) + "</span>" : "") + "</td><td><span class=\"status status-" +
          badge + "\">" + escapeHtml(status) + "</span></td><td><code title=\"" +
          escapeHtml(item.head || "") + "\">" + escapeHtml(shortHead(item.head)) +
          "</code></td></tr>";
      }).join("") : '<tr><td colspan="3" class="muted">No convergence pass has reported yet.</td></tr>';
    }
    renderSyncConflicts(state.conflicts || []);
  }

  function renderSyncConflicts(conflicts) {
    var panel = $("#pa-sync-conflicts");
    var fields = $("#pa-sync-resolution-fields");
    syncAllConflicts = conflicts || [];
    syncCurrentConflicts = [];
    if (!panel || !fields) return;
    panel.hidden = !syncAllConflicts.length;
    if (!syncAllConflicts.length) {
      fields.innerHTML = "";
      return;
    }
    var remoteHeads = [];
    syncAllConflicts.forEach(function (item) {
      if (remoteHeads.indexOf(item.remote_head) === -1) remoteHeads.push(item.remote_head);
    });
    var remoteHead = remoteHeads.indexOf(syncSelectedRemoteHead) >= 0
      ? syncSelectedRemoteHead : remoteHeads[0];
    syncSelectedRemoteHead = remoteHead;
    syncCurrentConflicts = syncAllConflicts.filter(function (item) {
      return item.remote_head === remoteHead;
    });
    var peer = syncCurrentConflicts[0].peer || {};
    var queue = '<p class="muted small">Resolving ' +
      identityHtml(peer.instance_id) +
      (remoteHeads.length > 1
        ? ". Other divergent peer heads remain queued after this merge."
        : ".") + "</p>";
    if (remoteHeads.length > 1) {
      queue += '<label>Peer history <select id="pa-sync-conflict-head">' +
        remoteHeads.map(function (head) {
          var item = syncAllConflicts.find(function (conflict) {
            return conflict.remote_head === head;
          }) || {};
          var itemPeer = item.peer || {};
          return '<option value="' + escapeHtml(head) + '"' +
            (head === remoteHead ? " selected" : "") + ">" +
            escapeHtml(identityName(itemPeer.instance_id)) +
            " · " + escapeHtml(shortHead(head)) + "</option>";
        }).join("") + "</select></label>";
    }
    fields.innerHTML = queue + syncCurrentConflicts.map(function (item, index) {
      var local = item.local || {};
      var remote = item.remote || {};
      var localLabel = identityHtml(local.instance_id) +
        ": " + escapeHtml(displayValue(local.value));
      var remoteLabel = identityHtml(remote.instance_id || (item.peer && item.peer.instance_id)) +
        ": " + escapeHtml(displayValue(remote.value));
      var title = item.entity + " " + item.id + " · " +
        (item.field === "__terminal__" ? "delete/archive vs edit" : item.field);
      return '<fieldset class="panel-inset" data-sync-conflict="' + index + '">' +
        "<legend><strong>" + escapeHtml(title) + "</strong></legend>" +
        '<label><input type="radio" name="sync-choice-' + index +
        '" value="local" checked> ' + localLabel + "</label>" +
        '<label><input type="radio" name="sync-choice-' + index +
        '" value="remote"> ' + remoteLabel + "</label>" +
        (item.field === "__terminal__" ? "" :
          '<label><input type="radio" name="sync-choice-' + index +
          '" value="custom"> Custom value <input data-sync-custom="' + index +
          '" value="' + escapeHtml(displayValue(local.value)) + '"></label>') +
        "</fieldset>";
    }).join("");
  }

  function renderSyncAudit(data) {
    var list = $("#pa-sync-audit");
    if (!list) return;
    var entries = (data && data.entries) || [];
    list.innerHTML = entries.length ? entries.map(function (entry) {
      return "<li><strong>" + escapeHtml(entry.mode || "automatic") +
        " merge</strong> by " + escapeHtml(entry.author_principal || "sync:auto") +
        ' <span class="muted">' + escapeHtml(entry.timestamp || "") +
        " · <code>" + escapeHtml(shortHead(entry.head)) + "</code> · parents " +
        (entry.parents || []).map(shortHead).map(escapeHtml).join(", ") +
        "</span></li>";
    }).join("") : '<li class="muted">No merge decisions recorded yet.</li>';
  }

  async function loadSyncStatus(startIfIdle) {
    if (!$("#pa-sync-instances")) return;
    var realm = syncRealm();
    var state = await api("/api/sync/convergence?realm=" + encodeURIComponent(realm));
    renderSyncState(state);
    var audit = await api("/api/sync/audit?realm=" + encodeURIComponent(realm));
    renderSyncAudit(audit);
    if (startIfIdle && (!state.instances || !state.instances.length)) {
      await startSyncConvergence();
    }
    return state;
  }

  async function startSyncConvergence() {
    var realm = syncRealm();
    var state = await api("/api/sync/converge", {
      method: "POST", body: { realm_id: realm }
    });
    renderSyncState(state);
    clearTimeout(syncPollTimer);
    syncPollTimer = setTimeout(pollSyncConvergence, 350);
  }

  async function pollSyncConvergence() {
    try {
      var state = await loadSyncStatus(false);
      if (state && state.running) {
        syncPollTimer = setTimeout(pollSyncConvergence, 600);
      }
    } catch (err) {
      var progress = $("#pa-sync-progress");
      if (progress) progress.textContent = err.message;
    }
  }

  function maybeLoadSyncStatus() {
    if ($("#pa-sync-instances")) {
      loadSyncStatus(true).catch(function (err) {
        var progress = $("#pa-sync-progress");
        if (progress) progress.textContent = err.message;
      });
    }
  }

  function remoteApiBase(instanceId) {
    return "/api/fleet/instances/" + encodeURIComponent(instanceId) + "/agent";
  }

  function remoteNotificationsEnabled() {
    try {
      return localStorage.getItem("pa-remote-notifications") === "1";
    } catch (e) {
      return false;
    }
  }

  function remoteNotificationsActive() {
    return remoteNotificationsEnabled() &&
      typeof Notification !== "undefined" &&
      Notification.permission === "granted";
  }

  function updateRemoteNotificationButton() {
    var button = $("#pa-remote-notifications");
    if (!button) return;
    button.textContent = remoteNotificationsActive()
      ? "Notifications enabled"
      : "Enable notifications";
    button.classList.toggle("active", remoteNotificationsActive());
  }

  async function enableRemoteNotifications() {
    if (typeof Notification === "undefined") throw new Error("Browser notifications are not supported.");
    var permission = Notification.permission;
    if (permission !== "granted") permission = await Notification.requestPermission();
    if (permission !== "granted") throw new Error("Notification permission was not granted.");
    try { localStorage.setItem("pa-remote-notifications", "1"); } catch (e) {}
    updateRemoteNotificationButton();
    remoteActivityTick();
  }

  function notifyRemoteSession(session, type, payload) {
    if (!remoteNotificationsActive()) return;
    var labels = {
      turn_completed: "Work completed",
      permission_request: "Permission needed",
      error: "Agent error",
      connection_lost: "Connection lost",
    };
    var title = labels[type] || "Remote agent update";
    var detail = payload && (payload.message || payload.title || payload.stop_reason);
    var body = (session.title || session.label || session.id) + (detail ? " · " + detail : "");
    try {
      new Notification("PA · " + title, {
        body: body,
        tag: "pa-remote-" + session.id + "-" + type,
      });
    } catch (e) {}
  }

  function remoteActivityLeaseKey(instanceId) {
    return "pa-fleet-activity-owner-v1:" + instanceId;
  }

  function readRemoteActivityLease(instanceId) {
    try {
      return JSON.parse(localStorage.getItem(remoteActivityLeaseKey(instanceId)) || "null");
    } catch (e) {
      return null;
    }
  }

  function ownsRemoteActivityLease(instanceId) {
    var lease = readRemoteActivityLease(instanceId);
    return !!lease && lease.tab_id === remoteTabId && Number(lease.expires_at || 0) > Date.now();
  }

  function acquireRemoteActivityLease(instanceId) {
    if (!instanceId) return false;
    var current = readRemoteActivityLease(instanceId);
    if (current && current.tab_id !== remoteTabId &&
        Number(current.expires_at || 0) > Date.now()) return false;
    try {
      localStorage.setItem(remoteActivityLeaseKey(instanceId), JSON.stringify({
        tab_id: remoteTabId, expires_at: Date.now() + 5000
      }));
    } catch (e) {
      // Storage-disabled contexts are already isolated; remain bounded to one
      // transport in this tab.
      return true;
    }
    return ownsRemoteActivityLease(instanceId);
  }

  function releaseRemoteActivityLease(instanceId) {
    if (!instanceId || !ownsRemoteActivityLease(instanceId)) return;
    try { localStorage.removeItem(remoteActivityLeaseKey(instanceId)); } catch (e) {}
    if (remoteActivityChannel) {
      remoteActivityChannel.postMessage({
        kind: "owner-released", instance_id: instanceId, tab_id: remoteTabId
      });
    }
  }

  function stopRemoteActivity(reason, releaseLease) {
    remoteActivityGeneration += 1;
    clearTimeout(remoteActivityReconnectTimer);
    clearTimeout(remoteActivityPollTimer);
    remoteActivityReconnectTimer = null;
    remoteActivityPollTimer = null;
    if (remoteActivitySource) {
      try { remoteActivitySource.close(); } catch (e) {}
    }
    remoteActivitySource = null;
    var priorInstance = remoteActivityInstanceId || remoteActivityStartingInstanceId ||
      remoteInstanceId;
    remoteActivityInstanceId = "";
    remoteActivityStartingInstanceId = "";
    remoteActivityReconnects = 0;
    if (releaseLease) releaseRemoteActivityLease(priorInstance);
    if (reason && window.console && console.debug) {
      console.debug("Fleet activity transport closed", {
        reason: reason, instance_id: priorInstance, tab_id: remoteTabId
      });
    }
  }

  function clearRemoteWatchers() {
    // Compatibility name retained for extensions; the implementation owns one
    // multiplexed transport instead of one watcher per session.
    stopRemoteActivity("reconcile", true);
    clearTimeout(remoteDispatchTimer);
    remoteDispatchTimer = null;
  }

  function cancelRemoteSessionLoad(reason) {
    if (remoteSessionRecovery) {
      remoteSessionRecovery.controller.cancel(
        reason || "remote-session-context-changed"
      );
    }
    remoteSessionRecovery = null;
    remoteSessionSnapshot = [];
    remoteHistorySnapshot = [];
  }

  function remoteActivityWanted() {
    return !!remoteInstanceId &&
      (remoteOperationsSectionActive || remoteNotificationsActive()) &&
      document.visibilityState !== "hidden";
  }

  function handleRemoteOperationsHidden() {
    remoteLoadGeneration += 1;
    remoteAuditGeneration += 1;
    cancelRemoteSessionLoad("remote-operations-hidden");
    if (remoteNotificationsActive()) {
      if (remoteInstanceId) remoteActivityTick();
      return;
    }
    remoteInstanceId = "";
    clearRemoteWatchers();
  }

  function scheduleRemoteSessionRefresh(instanceId) {
    setTimeout(function () {
      if (instanceId !== remoteInstanceId) return;
      if (remoteOperationsSectionActive) {
        loadRemoteOperations();
      } else if (remoteNotificationsActive()) {
        refreshRemoteWatchers(instanceId);
      } else {
        remoteInstanceId = "";
        clearRemoteWatchers();
      }
    }, 250);
  }

  async function refreshRemoteWatchers(instanceId) {
    var generation = ++remoteLoadGeneration;
    try {
      var sessions = await api(remoteApiBase(instanceId) + "/sessions");
      if (
        generation !== remoteLoadGeneration ||
        instanceId !== remoteInstanceId ||
        !remoteNotificationsActive() ||
        remoteOperationsSectionActive
      ) return;
      watchRemoteSessions(instanceId, sessions || []);
    } catch (err) {
      // Existing EventSources retain their own reconnect behavior. A failed
      // reconciliation should not silently disable opted-in notifications.
    }
  }

  function watchRemoteSessions(instanceId, sessions) {
    var desired = {};
    (sessions || []).forEach(function (session) {
      // A durable record is not a runtime. Only normalized, authoritative live
      // records may contribute cursors or notification state.
      if (!session || !session.id || session.live !== true || session.orphan === true) return;
      if (typeof session.last_seq !== "number") return;
      desired[session.id] = session;
      remoteActivityCursors[session.id] = Math.max(
        Number(remoteActivityCursors[session.id] || 0),
        Number(session.last_seq || 0)
      );
    });
    remoteActivitySessions = desired;
    remoteActivityTick();
  }

  function handleRemoteActivityEvent(instanceId, type, data, rebroadcast) {
    if (!data || !data.session_id) return;
    var sessionId = String(data.session_id);
    var seq = Number(data.seq || 0);
    if (seq && seq <= Number(remoteActivityCursors[sessionId] || 0)) return;
    if (seq) remoteActivityCursors[sessionId] = seq;
    var session = remoteActivitySessions[sessionId] || {
      id: sessionId, title: sessionId
    };
    var widgetRoot = $("#pa-remote-chat [data-agent-chat]");
    var widget = widgetRoot && widgetRoot._acw;
    if (widget && widget.sessionId === sessionId &&
        widget.apiBase === remoteApiBase(instanceId)) {
      widget.handleEvent(data, false);
    }
    if (remoteNotificationEventTypes.indexOf(type) >= 0) {
      notifyRemoteSession(session, type, data.payload || {});
    }
    if (rebroadcast && remoteActivityChannel) {
      remoteActivityChannel.postMessage({
        kind: "activity",
        instance_id: instanceId,
        event_type: type,
        data: data,
        owner_tab_id: remoteTabId
      });
    }
    if (["turn_completed", "session_closed", "session_recovered",
         "connection_lost", "error"].indexOf(type) >= 0) {
      scheduleRemoteSessionRefresh(instanceId);
    }
  }

  function scheduleRemoteActivityTick(delay) {
    clearTimeout(remoteActivityLeaseTimer);
    remoteActivityLeaseTimer = setTimeout(remoteActivityTick, delay || 1500);
  }

  function scheduleLegacyRemotePoll(instanceId, immediate) {
    clearTimeout(remoteActivityPollTimer);
    remoteActivityPollTimer = setTimeout(function () {
      if (!remoteActivityWanted() || instanceId !== remoteInstanceId ||
          !ownsRemoteActivityLease(instanceId)) return;
      var previous = remoteActivitySessions;
      api(remoteApiBase(instanceId) + "/sessions").then(function (sessions) {
        (sessions || []).forEach(function (session) {
          if (!session || !session.id || session.live !== true || session.orphan === true) return;
          var prior = previous[session.id];
          if (prior && prior.prompting && !session.prompting) {
            notifyRemoteSession(session, "turn_completed", {});
          }
        });
        watchRemoteSessions(instanceId, sessions || []);
        if (remoteOperationsSectionActive) {
          renderRemoteSessions(sessions || []);
        }
      }).catch(function () {}).finally(function () {
        if (remoteActivityCompatibility[instanceId] === "polling") {
          scheduleLegacyRemotePoll(instanceId, false);
        }
      });
    }, immediate ? 0 : 15000);
  }

  function scheduleRemoteActivityReconnect(instanceId) {
    clearTimeout(remoteActivityReconnectTimer);
    remoteActivityReconnects += 1;
    var base = Math.min(30000, 1000 * Math.pow(2, Math.min(5, remoteActivityReconnects)));
    var jitter = Math.floor(Math.random() * Math.max(250, base * 0.25));
    remoteActivityReconnectTimer = setTimeout(function () {
      if (instanceId === remoteInstanceId && ownsRemoteActivityLease(instanceId)) {
        startRemoteActivity(instanceId);
      }
    }, base + jitter);
  }

  function startRemoteActivity(instanceId) {
    if (!remoteActivityWanted() || instanceId !== remoteInstanceId ||
        !ownsRemoteActivityLease(instanceId)) return;
    if (remoteActivitySource && remoteActivityInstanceId === instanceId) return;
    if (remoteActivityStartingInstanceId === instanceId) return;
    if (remoteActivityCompatibility[instanceId] === "polling") {
      scheduleLegacyRemotePoll(instanceId, true);
      return;
    }
    var generation = ++remoteActivityGeneration;
    remoteActivityStartingInstanceId = instanceId;
    api(remoteApiBase(instanceId) + "/session-events/capabilities").then(function (capability) {
      if (generation !== remoteActivityGeneration || !remoteActivityWanted() ||
          instanceId !== remoteInstanceId || !ownsRemoteActivityLease(instanceId)) return;
      remoteActivityStartingInstanceId = "";
      if (!capability || capability.transport !== "sse") {
        remoteActivityCompatibility[instanceId] = "polling";
        scheduleLegacyRemotePoll(instanceId, true);
        return;
      }
      var cursors = {};
      Object.keys(remoteActivitySessions).forEach(function (sessionId) {
        cursors[sessionId] = Number(remoteActivityCursors[sessionId] || 0);
      });
      var url = remoteApiBase(instanceId) + "/session-events?client_id=" +
        encodeURIComponent(remoteTabId) + "&after=" +
        encodeURIComponent(JSON.stringify(cursors)) + "&reconnect_attempt=" +
        encodeURIComponent(remoteActivityReconnects);
      var source = new EventSource(url);
      remoteActivitySource = source;
      remoteActivityInstanceId = instanceId;
      source.addEventListener("open", function () { remoteActivityReconnects = 0; });
      remoteActivityEventTypes.forEach(function (type) {
        source.addEventListener(type, function (event) {
          var data = {};
          try { data = JSON.parse(event.data || "{}"); } catch (e) { return; }
          handleRemoteActivityEvent(instanceId, type, data, true);
        });
      });
      source.onerror = function () {
        if (remoteActivitySource === source) remoteActivitySource = null;
        try { source.close(); } catch (e) {}
        if (!remoteActivityWanted() || !ownsRemoteActivityLease(instanceId)) return;
        api(remoteApiBase(instanceId) + "/session-events/capabilities").then(function () {
          scheduleRemoteActivityReconnect(instanceId);
        }).catch(function (error) {
          if (error.status === 404 || error.status === 410) {
            // Terminal for this transport version. Older peers use bounded
            // reconciliation polling and never fall back to per-session SSE.
            remoteActivityCompatibility[instanceId] = "polling";
            scheduleLegacyRemotePoll(instanceId, true);
          } else {
            scheduleRemoteActivityReconnect(instanceId);
          }
        });
      };
    }).catch(function (error) {
      if (generation !== remoteActivityGeneration) return;
      remoteActivityStartingInstanceId = "";
      if (error.status === 404 || error.status === 410) {
        remoteActivityCompatibility[instanceId] = "polling";
        scheduleLegacyRemotePoll(instanceId, true);
      } else {
        scheduleRemoteActivityReconnect(instanceId);
      }
    });
  }

  function remoteActivityTick() {
    if (!remoteActivityWanted()) {
      stopRemoteActivity("not-wanted", true);
      return;
    }
    var instanceId = remoteInstanceId;
    if (remoteActivityInstanceId && remoteActivityInstanceId !== instanceId) {
      stopRemoteActivity("instance-changed", true);
    }
    if (acquireRemoteActivityLease(instanceId)) {
      startRemoteActivity(instanceId);
    } else if (remoteActivitySource || remoteActivityInstanceId ||
               remoteActivityStartingInstanceId) {
      stopRemoteActivity("follower-tab", false);
    }
    scheduleRemoteActivityTick(1500);
  }

  if (remoteActivityChannel) {
    remoteActivityChannel.onmessage = function (event) {
      var message = event.data || {};
      if (message.instance_id !== remoteInstanceId) return;
      if (message.kind === "activity") {
        handleRemoteActivityEvent(
          message.instance_id, message.event_type, message.data || {}, false
        );
      } else if (message.kind === "owner-released") {
        scheduleRemoteActivityTick(0);
      }
    };
  }

  function renderRemoteSessions(sessions) {
    var list = $("#pa-remote-session-list");
    if (!list) return;
    if (!sessions || !sessions.length) {
      list.innerHTML = '<li class="muted">No live sessions.</li>';
      return;
    }
    list.innerHTML = sessions.map(function (session) {
      var title = escapeHtml(session.title || session.label || session.id);
      var state = session.prompting ? "working" : (session.status || "idle");
      return '<li><button type="button" class="ghost pa-remote-session-button" data-remote-session="' +
        escapeHtml(session.id) + '"><span>' + title + '</span><span class="status status-' +
        (session.prompting ? "active" : "open") + '">' + escapeHtml(state) +
        '</span></button><a class="text-btn small" href="/agent?session=' +
        encodeURIComponent(session.id) + '&instance=' + encodeURIComponent(remoteInstanceId) +
        '">Open in Agent</a></li>';
    }).join("");
  }

  function renderRemoteSessionState(message, blocked) {
    var list = $("#pa-remote-session-list");
    if (!list) return;
    list.setAttribute("aria-busy", blocked ? "false" : "true");
    list.innerHTML = '<li class="' + (blocked ? "status status-blocked" : "muted") +
      '" role="' + (blocked ? "alert" : "status") + '">' +
      escapeHtml(message) + "</li>";
  }

  function remoteSessionFailureMessage(error) {
    var detail = error && error.detail && typeof error.detail === "object"
      ? error.detail : {};
    if (detail.code === "agent_recovery_failed") {
      return detail.message ||
        "Durable session recovery failed. Audit history remains available.";
    }
    if (error && (error.status === 401 || error.status === 403)) {
      return "Authentication failed while loading this peer's sessions.";
    }
    if (error && error.status === 502) {
      return error.message || "The selected peer is unreachable.";
    }
    return error && error.message || "Could not load sessions from this peer.";
  }

  function startRemoteSessionLoad(instanceId, force) {
    if (!instanceId || instanceId !== remoteInstanceId) return Promise.resolve(null);
    if (remoteSessionRecovery && remoteSessionRecovery.instanceId !== instanceId) {
      cancelRemoteSessionLoad("remote-instance-changed");
    }
    if (!remoteSessionRecovery) {
      var state = {
        instanceId: instanceId,
        controller: null,
      };
      state.controller = new window.PASessionRecovery.Controller({
        minimumMs: 250,
        maximumMs: 30000,
        operation: function (signal) {
          return api(remoteApiBase(state.instanceId) + "/sessions", {
            signal: signal,
          });
        },
        isActive: function () {
          var select = $("#pa-remote-instance");
          return state.instanceId === remoteInstanceId &&
            remoteOperationsSectionActive &&
            document.visibilityState !== "hidden" &&
            !!(select && select.isConnected);
        },
        onSuccess: function (sessions) {
          remoteSessionSnapshot = sessions || [];
          renderRemoteSessions(remoteSessionSnapshot);
          var list = $("#pa-remote-session-list");
          if (list) list.setAttribute("aria-busy", "false");
          renderRemoteHistory(remoteHistorySnapshot, remoteSessionSnapshot);
          watchRemoteSessions(state.instanceId, remoteSessionSnapshot);
          var status = $("#pa-remote-status");
          if (status) {
            status.textContent = remoteSessionSnapshot.length + " live session" +
              (remoteSessionSnapshot.length === 1 ? "" : "s") +
              " on the selected instance.";
          }
        },
        onRecovery: function (error) {
          stopRemoteActivity("agent-recovery", true);
          var detail = error.detail || {};
          renderRemoteSessionState(
            detail.message || "Restoring sessions…",
            false
          );
          var status = $("#pa-remote-status");
          if (status) {
            status.textContent =
              "Restoring sessions… Other Fleet status and controls remain available.";
          }
        },
        onError: function (error) {
          stopRemoteActivity("session-load-failed", true);
          var message = remoteSessionFailureMessage(error);
          renderRemoteSessionState(message, true);
          var status = $("#pa-remote-status");
          if (status) status.textContent = message;
        },
      });
      remoteSessionRecovery = state;
    }
    return remoteSessionRecovery.controller.start(!!force);
  }

  function loadRemoteStreamDiagnostics() {
    var summary = $("[data-fleet-stream-summary]");
    var details = $("[data-fleet-stream-details]");
    if (!summary || !details || !remoteOperationsSectionActive) return;
    api("/api/runtime").then(function (runtime) {
      var streams = runtime && runtime.sse_connections || {};
      var paired = streams.paired || {};
      summary.textContent = String(streams.active || 0) +
        " active SSE transport leg" + (streams.active === 1 ? "" : "s") +
        " on this controller; " + String(streams.over_age || 0) + " over age.";
      details.innerHTML =
        "<dt>Opened / closed</dt><dd>" + escapeHtml(streams.opened || 0) +
        " / " + escapeHtml(streams.closed || 0) + "</dd>" +
        "<dt>Cancelled / errored</dt><dd>" + escapeHtml(streams.cancelled || 0) +
        " / " + escapeHtml(streams.errored || 0) + "</dd>" +
        "<dt>Reconnects / leaked</dt><dd>" + escapeHtml(streams.reconnecting || 0) +
        " / " + escapeHtml(streams.leaked || 0) + "</dd>" +
        "<dt>Proxy pair</dt><dd>" + escapeHtml(paired.downstream || 0) +
        " downstream · " + escapeHtml(paired.upstream || 0) + " upstream · " +
        (paired.balanced === false ? "unbalanced" : "balanced") + "</dd>" +
        "<dt>Browser budget</dt><dd>1 stream per selected instance across tabs</dd>";
    }).catch(function (error) {
      summary.textContent = "Transport diagnostics unavailable: " + error.message;
    });
  }

  function renderRemoteHistory(history, liveSessions) {
    var list = $("#pa-remote-history-list");
    if (!list) return;
    var live = {};
    (liveSessions || []).forEach(function (session) { live[session.id] = true; });
    var rows = (history || []).filter(function (session) { return !live[session.id]; });
    if (!rows.length) {
      list.innerHTML = '<li class="muted">No closed session history.</li>';
      return;
    }
    list.innerHTML = rows.map(function (session) {
      var title = escapeHtml(session.title || session.label || session.id);
      return '<li><button type="button" class="ghost pa-remote-session-button" data-remote-audit="' +
        escapeHtml(session.id) + '"><span>' + title + '</span><span class="muted small">' +
        escapeHtml(session.status || "closed") + "</span></button></li>";
    }).join("");
  }

  function remoteDispatchStageLabel(state) {
    return {
      waiting_capacity: "Queued for capacity",
      blocked: "Queued · blocked",
      queued: "Queued",
      checking_sync: "Checking sync",
      materializing: "Materializing",
      starting_session: "Starting session",
      delivering_prompt: "Delivering prompt",
      running: "Running",
      failed: "Failed",
      completion_pending: "Completion pending",
      completed: "Completed",
      cancelled: "Cancelled",
    }[state] || state || "Unknown";
  }

  function renderRemoteDispatches(dispatches) {
    var list = $("#pa-remote-dispatch-list");
    if (!list) return;
    if (!dispatches || !dispatches.length) {
      list.innerHTML = '<li class="muted">No durable dispatches for this instance.</li>';
      return;
    }
    list.innerHTML = dispatches.map(function (dispatch) {
      var recordedState = dispatch.state || "queued";
      var state = dispatch.effective_state || recordedState;
      var terminal = state === "failed" || state === "completed" || state === "cancelled";
      var evaluatedOutcome = dispatch.evaluated_outcome || "needs_evaluation";
      var badge = evaluatedOutcome === "attempt_succeeded" ? "active" :
        (evaluatedOutcome === "attempt_blocked" || evaluatedOutcome === "attempt_failed") ?
          "blocked" : "open";
      var latest = dispatch.events && dispatch.events.length
        ? dispatch.events[dispatch.events.length - 1].message : "";
      var error = dispatch.last_error
        ? '<p class="status status-blocked small">' + escapeHtml(dispatch.last_error) + "</p>" : "";
      var queue = dispatch.queue || {};
      var queueText = queue.waiting
        ? '<p class="muted small">Queue position ' + escapeHtml(queue.position || "pending") +
          ' · priority ' + escapeHtml(queue.requested_priority || 0) +
          (queue.reason ? " · " + escapeHtml(queue.reason) : "") + "</p>"
        : "";
      var syncEvidence = dispatch.sync_evidence || {};
      var degradedPeers = syncEvidence.degraded_peers || [];
      var syncWarning = degradedPeers.length
        ? '<p class="status status-open small">Scoped dispatch remained safe; ' +
          escapeHtml(degradedPeers.length) +
          ' unrelated fleet peer' + (degradedPeers.length === 1 ? " was" : "s were") +
          ' degraded and remain queued for normal sync convergence.</p>'
        : "";
      var outbox = dispatch.completion_outbox || {};
      var outboxText = state === "completion_pending"
        ? '<p class="muted small">Completion outbox attempt ' + escapeHtml(outbox.attempts || 0) +
          (outbox.last_error ? " · " + escapeHtml(outbox.last_error) : "") + "</p>" : "";
      var turn = dispatch.agent_turn || {};
      var transport = dispatch.dispatch_completion || {};
      var card = dispatch.card_completion || {};
      var reconciliation = dispatch.card_reconciliation || {};
      var evaluation = dispatch.post_turn_evaluation || null;
      var followupState = dispatch.followup_state || {};
      var progress = dispatch.progress || {};
      var checkpoint = progress.latest || null;
      var freshness = progress.freshness || {};
      var progressText = "";
      if (checkpoint) {
        var compactDetail = function (value, limit) {
          value = String(value || "");
          return value.length > limit ? value.slice(0, limit) + "…" : value;
        };
        var toolRows = (checkpoint.tool_details || []).map(function (detail) {
          return "<li title=\"" + escapeHtml(detail.title || "Tool") + "\">" +
            escapeHtml(compactDetail(detail.title || "Tool", 240)) +
            (detail.status ? " · " + escapeHtml(detail.status) : "") + "</li>";
        }).join("");
        var validationRows = (checkpoint.validations || []).map(function (validation) {
          var command = validation.command || "validation";
          return "<li><details><summary><code>" +
            escapeHtml(compactDetail(command, 240)) + "</code> · " +
            escapeHtml(validation.status || "unknown") +
            "</summary><code>" + escapeHtml(command) + "</code></details></li>";
        }).join("");
        progressText = '<div class="dispatch-progress dispatch-progress-' +
          escapeHtml(freshness.state || "delayed") + '"><p><strong>' +
          escapeHtml((checkpoint.phase || "investigating").replace(/_/g, " ")) +
          "</strong> · " + escapeHtml(checkpoint.summary || "Agent active") +
          '</p><p class="muted small">Reporting ' +
          escapeHtml(freshness.state || "delayed") +
          (freshness.age_seconds == null ? "" : " · " + escapeHtml(freshness.age_seconds) + "s ago") +
          "</p>" + (toolRows || validationRows
            ? "<details><summary>Sanitized tool and validation details</summary><ul>" +
              validationRows + toolRows + "</ul></details>"
            : "") + "</div>";
      } else if (progress.reporting === "lifecycle_only") {
        progressText = '<p class="muted small">Lifecycle-only reporting from an older peer.</p>';
      }
      var lifecycle = '<p class="muted small">Agent turn: ' +
        escapeHtml((turn.ended || turn.completed) ? "ended" : "in progress") +
        (turn.stop_reason ? " (" + escapeHtml(turn.stop_reason) + ")" : "") +
        ' · Dispatch: ' + escapeHtml(transport.completed ? "completed" : "in progress") + "</p>";
      var evaluationText = evaluation
        ? '<p class="dispatch-evaluated-outcome"><strong>' +
          escapeHtml(evaluatedOutcome.replace(/_/g, " ")) + "</strong> · " +
          escapeHtml(evaluation.operator_status_text || evaluation.decision || "") +
          (followupState.scheduled ? " · follow-up scheduled" : "") + "</p>"
        : (terminal
          ? '<p class="status status-open small">needs evaluation · turn lifecycle alone does not prove card success</p>'
          : "");
      var diagnosticText = recordedState !== state
        ? '<p class="status status-open small">Lifecycle inconsistency: recorded ' +
          escapeHtml(recordedState) + " · effective " + escapeHtml(state) +
          " because acknowledged completion wins</p>"
        : "";
      var cardText = "";
      if (dispatch.card_id && card.status && card.status !== "not_requested") {
        cardText = '<p class="muted small">Card: ' +
          escapeHtml(card.lane_after || card.lane_before || "unchanged") +
          ' · Disposition: ' + escapeHtml(card.status) +
          (card.reason ? " · " + escapeHtml(card.reason) : "") + "</p>";
      }
      var reconciliationText = "";
      if (dispatch.card_id && reconciliation.state &&
          reconciliation.state !== "not_requested" &&
          reconciliation.state !== "not_required") {
        var reconciliationLabel = {
          pending: "Turn ended; card update pending",
          applied: "Turn ended; card update applied",
          already_satisfied: "Turn ended; card already satisfied",
          operator_state_preserved: "Turn ended; operator change preserved",
          conflict_requires_resolution: "Turn ended; reconciliation needs attention",
          not_applicable: "Turn ended; card update not applicable",
        }[reconciliation.state] || reconciliation.state.replace(/_/g, " ");
        reconciliationText = '<p class="muted small">' +
          escapeHtml(reconciliationLabel) +
          (reconciliation.reason ? " · " + escapeHtml(reconciliation.reason) : "") +
          (reconciliation.disposition_error &&
           (!reconciliation.reason ||
            reconciliation.reason.indexOf(reconciliation.disposition_error) < 0)
            ? " · " + escapeHtml(reconciliation.disposition_error) : "") +
          "</p>";
      }
      var actions = '<span class="form-actions">';
      var staleRecovery = dispatch.stale_session_recovery || {};
      if (dispatch.can_retry) actions += '<button type="button" class="ghost small" data-dispatch-retry="' +
        escapeHtml(dispatch.dispatch_id) + '">Retry</button>';
      if (dispatch.can_cancel) actions += '<button type="button" class="ghost small" data-dispatch-cancel="' +
        escapeHtml(dispatch.dispatch_id) + '">Cancel</button>';
      if (staleRecovery.eligible_candidate) actions += '<button type="button" class="ghost small" data-dispatch-recover-stale="' +
        escapeHtml(dispatch.dispatch_id) + '" data-dispatch-state="' +
        escapeHtml(recordedState) + '">Recover closed session</button>';
      if (dispatch.session_id) actions += '<a class="ghost small" href="/agent?session=' +
        encodeURIComponent(dispatch.session_id) + '&instance=' + encodeURIComponent(remoteInstanceId) +
        '">Open session</a>';
      actions += "</span>";
      return '<li data-dispatch-id="' + escapeHtml(dispatch.dispatch_id) + '"><div class="panel-header"><div>' +
        '<strong>' + escapeHtml(dispatch.card_id ? "Card dispatch" : "Remote session") + '</strong> ' +
        '<span class="status status-' + badge + '">' + escapeHtml(remoteDispatchStageLabel(state)) + "</span>" +
        '<p class="muted small"><code>' + escapeHtml(dispatch.dispatch_id) + "</code>" +
        (latest ? " · " + escapeHtml(latest) : "") + "</p></div>" + actions + "</div>" +
        error + queueText + syncWarning + diagnosticText + evaluationText + progressText + lifecycle + cardText + reconciliationText + outboxText +
        (terminal ? "" : '<progress></progress>') + "</li>";
    }).join("");
  }

  async function loadRemoteDispatches(instanceId) {
    if (!instanceId || instanceId !== remoteInstanceId) return;
    clearTimeout(remoteDispatchTimer);
    var localPath = "/api/fleet/dispatch-jobs?target_instance_id=" + encodeURIComponent(instanceId);
    var targetPath = "/api/fleet/instances/" + encodeURIComponent(instanceId) + "/dispatches";
    var local = await api(localPath);
    if (instanceId !== remoteInstanceId || !$("#pa-remote-dispatch-list")) return;
    var merged = {};
    (local || []).forEach(function (item) { merged[item.dispatch_id] = item; });
    renderRemoteDispatches(local || []);
    var targetRows = await api(targetPath).catch(function () { return []; });
    if (instanceId !== remoteInstanceId || !$("#pa-remote-dispatch-list")) return;
    (targetRows || []).forEach(function (target) {
      var authority = merged[target.dispatch_id];
      if (!authority) {
        merged[target.dispatch_id] = target;
      } else {
        if (target.card_reconciliation &&
            target.card_reconciliation.state !== "not_requested") {
          authority.card_reconciliation = target.card_reconciliation;
          authority.updated_at = target.updated_at;
        }
        var authorityActivity = (((authority.progress || {}).freshness || {}).last_activity_at || "");
        var targetActivity = (((target.progress || {}).freshness || {}).last_activity_at || "");
        if (targetActivity > authorityActivity) {
          authority.progress = target.progress;
          authority.progress_events = target.progress_events;
          authority.updated_at = target.updated_at;
        }
        if (target.state === "completion_pending" || target.state === "completed") {
          authority.state = target.state;
          authority.last_error = target.last_error;
          authority.completion_outbox = target.completion_outbox;
          authority.updated_at = target.updated_at;
        }
      }
    });
    var rows = Object.keys(merged).map(function (key) { return merged[key]; });
    rows.sort(function (a, b) { return String(b.updated_at).localeCompare(String(a.updated_at)); });
    renderRemoteDispatches(rows);
    var active = rows.some(function (item) {
      if (((item.dispatch_completion || {}).completed)) return false;
      return ["waiting_capacity", "blocked", "queued", "checking_sync", "materializing", "starting_session",
        "delivering_prompt", "running", "completion_pending"].indexOf(item.state) >= 0;
    });
    if (active) remoteDispatchTimer = setTimeout(function () {
      loadRemoteDispatches(instanceId).catch(function () {});
    }, 1000);
  }

  async function loadRemoteProviders(instanceId, generation) {
    var select = $("[data-remote-provider]");
    if (!select) return;
    var providers = await api(remoteApiBase(instanceId) + "/providers/catalog").catch(function () {
      return api(remoteApiBase(instanceId) + "/providers");
    });
    if (
      generation !== remoteLoadGeneration ||
      instanceId !== remoteInstanceId ||
      !select.isConnected
    ) return;

    // Read the selection after the request so a choice made while providers
    // were loading wins over the refresh that initiated the request.
    var selectedProvider = select.value;
    var options = document.createDocumentFragment();
    var defaultOption = document.createElement("option");
    defaultOption.value = "";
    defaultOption.textContent = "Instance default";
    options.appendChild(defaultOption);
    (providers || []).forEach(function (provider) {
      if (!provider || !provider.id) return;
      var option = document.createElement("option");
      option.value = provider.id;
      option.textContent = provider.display_name || provider.id;
      options.appendChild(option);
    });
    select.replaceChildren(options);
    if (selectedProvider && $all("option", select).some(function (option) {
      return option.value === selectedProvider;
    })) select.value = selectedProvider;
  }

  function loadRemoteHistory(instanceId, generation) {
    return api(remoteApiBase(instanceId) + "/history").then(function (history) {
      if (
        generation !== remoteLoadGeneration ||
        instanceId !== remoteInstanceId ||
        !$("#pa-remote-history-list")
      ) return;
      remoteHistorySnapshot = history || [];
      renderRemoteHistory(remoteHistorySnapshot, remoteSessionSnapshot);
    }).catch(function (error) {
      if (
        generation !== remoteLoadGeneration ||
        instanceId !== remoteInstanceId ||
        !$("#pa-remote-history-list")
      ) return;
      var list = $("#pa-remote-history-list");
      if (list) {
        list.innerHTML = '<li class="status status-blocked">' +
          escapeHtml(error.message || "Audit history is unavailable.") + "</li>";
      }
    });
  }

  async function loadRemoteOperations(forceSessions) {
    var instanceSelect = $("#pa-remote-instance");
    if (!instanceSelect) {
      handleRemoteOperationsHidden();
      return;
    }
    var status = $("#pa-remote-status");
    var instanceId = remoteInstanceId;
    var generation = ++remoteLoadGeneration;
    if (!instanceId) {
      if (status) status.textContent = "Choose an instance to load its sessions.";
      cancelRemoteSessionLoad("no-remote-instance");
      clearRemoteWatchers();
      loadRemoteStreamDiagnostics();
      return;
    }
    if (status && !remoteSessionRecovery) status.textContent = "Loading remote sessions…";
    loadRemoteDispatches(instanceId).catch(function () {});
    loadRemoteHistory(instanceId, generation);
    loadRemoteProviders(instanceId, generation).catch(function () {
      if (
        generation !== remoteLoadGeneration ||
        instanceId !== remoteInstanceId ||
        !instanceSelect.isConnected
      ) return;
      var select = $("[data-remote-provider]");
      if (select) select.title = "Provider discovery is unavailable.";
    });
    loadRemoteStreamDiagnostics();
    return startRemoteSessionLoad(instanceId, !!forceSessions);
  }

  function selectRemoteSession(sessionId) {
    if (!remoteInstanceId || !sessionId) return;
    var chat = $("#pa-remote-chat");
    var audit = $("#pa-remote-audit");
    var widgetRoot = $("#pa-remote-chat [data-agent-chat]");
    if (!chat || !widgetRoot || !window.PAAgentChat) return;
    widgetRoot.dataset.draftInstanceId = remoteInstanceId;
    window.PAAgentChat.mount(chat);
    if (!widgetRoot._acw) return;
    widgetRoot._acw.setApiBase(remoteApiBase(remoteInstanceId), remoteInstanceId);
    widgetRoot._acw.useExternalEventTransport(true);
    if (audit) audit.hidden = true;
    chat.hidden = false;
    widgetRoot._acw.switchSession(sessionId, true, remoteInstanceId);
  }

  function remoteAuditEventHtml(event) {
    var payload = "";
    try { payload = JSON.stringify(event.payload || {}, null, 2).slice(0, 4000); } catch (e) {}
    return '<details class="pa-remote-audit-event"><summary><strong>' +
      escapeHtml(event.event_type) + '</strong> <span class="muted small">#' +
      escapeHtml(event.seq) + " · " + escapeHtml(event.created_at || "") +
      "</span></summary><pre>" + escapeHtml(payload) + "</pre></details>";
  }

  function renderRemoteAuditEvents(container, events, hasOlder) {
    if (!container) return;
    container.innerHTML = (hasOlder
      ? '<button type="button" class="ghost small" data-remote-audit-older>Load older events</button>'
      : "") + events.map(remoteAuditEventHtml).join("");
    var count = $("[data-remote-audit-count]");
    if (count) count.textContent = events.length + " loaded transcript events";
  }

  async function showRemoteAudit(sessionId) {
    if (!remoteInstanceId || !sessionId) return;
    var instanceId = remoteInstanceId;
    var generation = ++remoteAuditGeneration;
    var chat = $("#pa-remote-chat");
    var audit = $("#pa-remote-audit");
    var body = $("#pa-remote-audit-body");
    if (chat) chat.hidden = true;
    if (audit) audit.hidden = false;
    if (body) body.innerHTML = '<p class="muted">Loading audit history…</p>';
    try {
      var data = await api(remoteApiBase(instanceId) + "/history/" + encodeURIComponent(sessionId));
      if (
        generation !== remoteAuditGeneration ||
        instanceId !== remoteInstanceId ||
        !body ||
        !body.isConnected
      ) return;
      var session = data.session || {};
      var events = data.events || [];
      remoteAuditSessionId = sessionId;
      remoteAuditEvents = events;
      if (body) {
        body.innerHTML = '<p><strong>' + escapeHtml(session.title || session.label || session.id) +
          '</strong> <span class="badge">' + escapeHtml(session.status || "unknown") + '</span></p>' +
          '<p class="muted small">' + identityHtml(instanceId) +
          ' · <span data-remote-audit-count></span></p>' +
          '<div class="pa-remote-audit-events"></div>';
        renderRemoteAuditEvents(
          $(".pa-remote-audit-events", body),
          events,
          !!(data.page && data.page.has_older)
        );
      }
    } catch (err) {
      if (
        generation !== remoteAuditGeneration ||
        instanceId !== remoteInstanceId ||
        !body ||
        !body.isConnected
      ) return;
      if (body) body.innerHTML = '<p class="status status-blocked">' + escapeHtml(err.message) + "</p>";
    }
  }

  async function loadOlderRemoteAudit(button) {
    if (!remoteInstanceId || !remoteAuditSessionId || !remoteAuditEvents.length) return;
    var instanceId = remoteInstanceId;
    var sessionId = remoteAuditSessionId;
    var generation = remoteAuditGeneration;
    var container = button && button.closest(".pa-remote-audit-events");
    if (!container) return;
    var oldest = remoteAuditEvents.reduce(function (result, event) {
      var seq = Number(event && event.seq || 0);
      return seq && (!result || seq < result) ? seq : result;
    }, 0);
    if (!oldest) return;
    button.disabled = true;
    button.textContent = "Loading…";
    try {
      var data = await api(
        remoteApiBase(instanceId) + "/history/" + encodeURIComponent(sessionId) +
        "?before_seq=" + encodeURIComponent(oldest) + "&limit=1000"
      );
      if (
        generation !== remoteAuditGeneration ||
        instanceId !== remoteInstanceId ||
        sessionId !== remoteAuditSessionId ||
        !container.isConnected
      ) return;
      var keys = {};
      remoteAuditEvents = (data.events || []).concat(remoteAuditEvents).filter(function (event) {
        var key = String(event && (event.seq || event.id) || "");
        if (key && keys[key]) return false;
        if (key) keys[key] = true;
        return true;
      });
      remoteAuditEvents.sort(function (a, b) { return Number(a.seq || 0) - Number(b.seq || 0); });
      var oldHeight = container.scrollHeight;
      var oldTop = container.scrollTop;
      renderRemoteAuditEvents(
        container,
        remoteAuditEvents,
        !!(data.page && data.page.has_older)
      );
      container.scrollTop = oldTop + Math.max(0, container.scrollHeight - oldHeight);
    } catch (err) {
      button.disabled = false;
      button.textContent = "Load older events";
    }
  }

  function maybeLoadRemoteOperations() {
    var select = $("#pa-remote-instance");
    if (!select || !remoteOperationsSectionActive) {
      handleRemoteOperationsHidden();
      return;
    }
    updateRemoteNotificationButton();
    var saved = "";
    try { saved = localStorage.getItem("pa-remote-instance") || ""; } catch (e) {}
    if (saved && $all("option", select).some(function (option) { return option.value === saved; })) {
      select.value = saved;
    } else if (select.options.length === 2) {
      select.selectedIndex = 1;
    }
    var nextInstanceId = select.value || "";
    if (nextInstanceId !== remoteInstanceId) {
      remoteAuditGeneration += 1;
      cancelRemoteSessionLoad("remote-instance-changed");
      clearRemoteWatchers();
      remoteActivitySessions = {};
      remoteActivityCursors = {};
    }
    remoteInstanceId = nextInstanceId;
    if (remoteInstanceId) loadRemoteOperations();
    else clearRemoteWatchers();
  }

  function providerAuthState(provider) {
    if (provider && provider.auth_state) return provider.auth_state;
    if (provider && provider.auth_configured) return "authenticated";
    if (!provider || provider.available === false) return "unavailable";
    if (provider.auth_method === "unknown") return "unknown";
    return "not_configured";
  }

  function providerMechanismLabel(method) {
    return ({
      chatgpt_oauth: "ChatGPT OAuth", api_key: "API key",
      access_token: "access token", cursor_account: "Cursor account",
      active_acp_session: "active ACP session",
      environment: "environment credential", none: "none", unknown: "unknown"
    })[method] || String(method || "none").replace(/_/g, " ");
  }

  function providerStateLabel(state) {
    return ({
      authenticated: "authenticated", not_configured: "not configured",
      signed_out: "signed out", unavailable: "unavailable",
      probe_failed: "probe failed", timed_out: "timed out", unknown: "unknown"
    })[state] || "unknown";
  }

  function providerBadgeClass(state) {
    if (state === "authenticated") return "badge-ok";
    if (state === "signed_out") return "badge-danger";
    if (["probe_failed", "timed_out", "unknown"].indexOf(state) >= 0) return "badge-warning";
    return "badge-neutral";
  }

  function providersHtml(providers, instanceId) {
    if (!providers || !providers.length) return '<span class="muted">—</span>';
    return providers.map(function (p) {
      var label = escapeHtml(p.display_name || p.id || "?");
      var state = providerAuthState(p);
      var mark = state === "authenticated" ? " ✓" : (state === "signed_out" ? " !" : " ·");
      var mechanism = providerMechanismLabel(p.auth_method);
      var auth = providerStateLabel(state);
      if (state === "authenticated" && mechanism !== "none") auth += " · " + mechanism;
      var install = p.installed ? "installed" : "not installed";
      var attempted = p.last_attempt && p.last_attempt.state
        ? " · latest probe " + providerStateLabel(p.last_attempt.state) : "";
      var detail = escapeHtml(install + " · " + auth + attempted +
        (p.auth_status ? " · " + p.auth_status : "") +
        (p.auth_error ? " · " + p.auth_error : ""));
      var login = "";
      if (p.id === "codex" && p.login_in_progress) {
        var activeJob = p.meta && p.meta.active_login_job_id;
        login = activeJob ?
          ' <button type="button" class="ghost small" data-codex-login-resume="' +
            escapeHtml(instanceId || "") + '" data-login-job="' + escapeHtml(activeJob) +
            '">Resume sign-in</button>' :
          ' <span class="muted small">login in progress</span>';
      } else if (p.id === "codex" && p.codex_cli_installed &&
                 (state === "signed_out" || state === "not_configured")) {
        login = ' <button type="button" class="ghost small" data-codex-login="' +
          escapeHtml(instanceId || "") + '">Sign in with ChatGPT</button>';
      } else if (p.id === "codex" && !p.codex_cli_installed && state === "unavailable") {
        login = ' <button type="button" class="ghost small" data-codex-cli-install="' +
          escapeHtml(instanceId || "") + '">Install Codex CLI</button>';
      }
      return '<span class="badge ' + providerBadgeClass(state) + '" title="' + detail +
        '" aria-label="' + escapeHtml((p.display_name || p.id || "Provider") + ": " + auth) +
        '">' + label + mark + " · " + escapeHtml(auth) + "</span>" + login;
    }).join(" ");
  }

  function healthHtml(state) {
    var terminal = ["up", "down", "partial", "error", "timeout", "unavailable", "stale"];
    state = terminal.indexOf(state) >= 0 ? state : "error";
    return '<span class="status ' + (state === "up" ? "status-active" : "status-blocked") +
      '">' + escapeHtml(state) + "</span>";
  }

  function setLiveBanner(text) {
    var el = $("#pa-fleet-live-status");
    if (el) el.textContent = text || "";
    var progress = $("#pa-fleet-refresh-progress");
    if (progress) progress.textContent = text || "";
  }

  var codexLoginInstance = "";
  var codexLoginJob = "";
  var codexLoginStartSequence = 0;

  function codexLoginBase(instanceId) {
    if (!instanceId) return "/api/agent/providers/codex/login-jobs";
    return "/api/fleet/instances/" + encodeURIComponent(instanceId) +
      "/agent-providers/codex/login-jobs";
  }

  async function watchCodexLogin(instanceId, jobId) {
    var instructions = $("#pa-codex-login-instructions");
    while (codexLoginJob === jobId) {
      var job = await api(codexLoginBase(instanceId) + "/" + encodeURIComponent(jobId));
      var parts = [];
      if (job.verification_url || job.user_code) {
        parts.push("Use any browser to finish signing in; credentials stay on the target instance.");
      }
      if (job.verification_url) {
        parts.push('<a href="' + escapeHtml(job.verification_url) +
          '" target="_blank" rel="noopener">Open verification page</a>');
      }
      if (job.user_code) parts.push("Code: <code>" + escapeHtml(job.user_code) + "</code>");
      parts.push("Status: " + escapeHtml(job.state || "unknown"));
      if (job.error) parts.push(escapeHtml(job.error));
      if (instructions) instructions.innerHTML = parts.join(" · ");
      if (["succeeded", "failed", "cancelled", "timed_out", "interrupted"].indexOf(job.state) >= 0) {
        codexLoginJob = "";
        loadLiveStatus(true, instanceId || undefined);
        if (job.state === "succeeded") {
          setTimeout(function () { loadLiveStatus(true, instanceId || undefined); }, 1000);
        }
        return;
      }
      await new Promise(function (resolve) { setTimeout(resolve, 1000); });
    }
  }

  var fleetOverview = null;
  var fleetOverviewRoot = null;
  var selectedFleetItem = null;
  var fleetRefresh = null;
  var fleetRenderedSnapshot = null;

  function readFleetOverview() {
    var source = $("#pa-fleet-overview-data");
    if (!source) return null;
    try {
      return JSON.parse(source.textContent || "{}");
    } catch (err) {
      setLiveBanner("Cached overview could not be decoded · Use Refresh to retry.");
      return { version: 1, dimensions: [], nodes: [], edges: [] };
    }
  }

  function fieldValue(node, name) {
    return node && node.dimensions && node.dimensions[name]
      ? node.dimensions[name]
      : { state: "unavailable", value: null, observed_at: null, error: null };
  }

  function observationAttempt(field) {
    return field && (field.last_attempt_state || field.state) || "unavailable";
  }

  function syncStatusLabel(sync) {
    if (!sync || !sync.value) return (sync && sync.state) || "unavailable";
    if (sync.value.consistent) return "heads aligned";
    return observationAttempt(sync) === "fresh" ? "head mismatch" : "last known heads";
  }

  function requiredReadiness(node) {
    var reach = fieldValue(node, "reachability");
    var health = reach.value && reach.value.health;
    if (health !== "up") return health === "unknown" ? observationAttempt(reach) : (health || observationAttempt(reach));
    var sync = fieldValue(node, "sync");
    if (observationAttempt(sync) === "fresh" && sync.value && sync.value.consistent === false) return "error";
    var order = { error: 5, timeout: 4, unavailable: 3, stale: 2, fresh: 1 };
    var worst = "fresh";
    ["reachability", "status", "sync"].forEach(function (name) {
      var item = fieldValue(node, name);
      var attempt = observationAttempt(item);
      if ((order[attempt] || 5) > (order[worst] || 1)) worst = attempt || "error";
    });
    return worst;
  }

  function worstFreshness(node) {
    var readiness = requiredReadiness(node);
    if (readiness !== "fresh") return readiness;
    var optionalIssue = ["providers", "update", "activity", "repositories", "supervisor"]
      .some(function (name) {
        var item = fieldValue(node, name);
        var attempt = item.last_attempt_state || item.state;
        return ["error", "timeout", "stale"].indexOf(attempt) >= 0;
      });
    return optionalIssue ? "partial" : "fresh";
  }

  function topologyStatusForNode(node) {
    return requiredReadiness(node);
  }

  function fleetNodeLabel(node, freshness) {
    var reach = fieldValue(node, "reachability");
    var status = fieldValue(node, "status");
    var sync = fieldValue(node, "sync");
    var activity = fieldValue(node, "activity");
    var providers = fieldValue(node, "providers");
    var update = fieldValue(node, "update");
    var health = reach.value && reach.value.health || reach.state;
    var version = status.value && status.value.version || status.state;
    var syncLabel = syncStatusLabel(sync);
    var providerValues = Array.isArray(providers.value) ? providers.value : [];
    var readyProviders = providerValues.filter(function (provider) {
      return provider.available !== false && providerAuthState(provider) === "authenticated";
    }).length;
    var providerLabel = providerValues.length
      ? readyProviders + "/" + providerValues.length + " providers ready"
      : "providers " + providers.state;
    var updateLabel = update.value && update.value.upgrade_available
      ? "update " + (update.value.available_version || update.value.latest || "available")
      : "update " + update.state;
    return node.name + ": " + health + ", " + activityLabel(activity.value) +
      ", version " + version + ", " + updateLabel + ", sync " + syncLabel +
      ", " + providerLabel + ", freshness " + freshness;
  }

  function createFleetSnapshot(overview, refresh, selection) {
    var snapshot = {
      overview: overview || { nodes: [], edges: [] },
      refresh: refresh ? Object.assign({}, refresh, {
        terminal: Object.assign({}, refresh.terminal || {})
      }) : null,
      selection: selection ? Object.assign({}, selection) : null,
      nodes: [],
      nodesById: Object.create(null),
      selectedNode: null,
      selectedEdge: null,
      selectedEdgeItem: null
    };
    (snapshot.overview.nodes || []).forEach(function (node) {
      var freshness = worstFreshness(node);
      var state = {
        node: node,
        freshness: freshness,
        refreshing: Object.keys(node.dimensions || {}).some(function (name) {
          return !!(node.dimensions[name] || {}).refreshing;
        }),
        topologyStatus: topologyStatusForNode(node),
        accessibleLabel: fleetNodeLabel(node, freshness)
      };
      snapshot.nodes.push(state);
      snapshot.nodesById[node.id] = state;
    });
    if (snapshot.selection && snapshot.selection.kind === "node") {
      snapshot.selectedNode = snapshot.nodesById[snapshot.selection.id] || null;
    } else if (snapshot.selection && snapshot.selection.kind === "edge") {
      snapshot.selectedEdge = (snapshot.overview.edges || []).find(function (edge) {
        return edge.id === snapshot.selection.id;
      }) || null;
    } else if (snapshot.selection && snapshot.selection.kind === "edge-item") {
      snapshot.selectedEdge = (snapshot.overview.edges || []).find(function (edge) {
        return edge.id === snapshot.selection.edgeId;
      }) || null;
      snapshot.selectedEdgeItem = edgeItemById(
        snapshot.selectedEdge,
        snapshot.selection.id
      ) || null;
    }
    if (
      snapshot.selection &&
      !snapshot.selectedNode &&
      !snapshot.selectedEdge &&
      !snapshot.selectedEdgeItem
    ) {
      snapshot.selection = null;
    }
    return snapshot;
  }

  function replaceFleetDimension(overview, nodeId, dimension, value) {
    return Object.assign({}, overview, {
      snapshot_version: value && value.snapshot_version != null
        ? value.snapshot_version : overview.snapshot_version,
      nodes: (overview.nodes || []).map(function (node) {
        if (node.id !== nodeId) return node;
        var dimensions = Object.assign({}, node.dimensions || {});
        dimensions[dimension] = Object.assign({}, value);
        return Object.assign({}, node, { dimensions: dimensions });
      })
    });
  }

  function beginFleetRefresh(overview, nodeIds, dimensions, generation, startedAt) {
    var selected = Object.create(null);
    (nodeIds || []).forEach(function (id) { selected[id] = true; });
    var next = Object.assign({}, overview, {
      nodes: (overview.nodes || []).map(function (node) {
        if (!selected[node.id]) return node;
        var fields = Object.assign({}, node.dimensions || {});
        (dimensions || []).forEach(function (dimension) {
          fields[dimension] = Object.assign(
            {},
            fieldValue(node, dimension),
            { refreshing: true }
          );
        });
        return Object.assign({}, node, { dimensions: fields });
      })
    });
    return {
      overview: next,
      refresh: {
        generation: generation,
        total: (nodeIds || []).length * (dimensions || []).length,
        completed: 0,
        terminal: Object.create(null),
        startedAt: startedAt,
        elapsedMs: 0
      }
    };
  }

  function mergeFieldAttemptFailure(previous, state, error) {
    var attemptedAt = new Date().toISOString();
    if (previous && previous.value != null) {
      return Object.assign({}, previous, {
        state: "stale", error: error || null,
        last_attempted_at: attemptedAt, last_attempt_state: state,
        failure: { code: state, message: error || null, retryable: true }
      });
    }
    return Object.assign({}, previous || {}, {
      state: state, value: null, error: error || null,
      last_attempted_at: attemptedAt, last_attempt_state: state,
      failure: { code: state, message: error || null, retryable: true }
    });
  }

  function applyFleetDimensionUpdate(overview, refresh, update) {
    if (!refresh || update.generation !== refresh.generation) {
      return { overview: overview, refresh: refresh, accepted: false, newlyCompleted: false };
    }
    var key = update.nodeId + "\n" + update.dimension;
    var terminal = Object.assign({}, refresh.terminal || {});
    var newlyCompleted = !terminal[key];
    terminal[key] = true;
    var nextRefresh = Object.assign({}, refresh, {
      completed: refresh.completed + (newlyCompleted ? 1 : 0),
      terminal: terminal,
      elapsedMs: update.elapsedMs == null ? refresh.elapsedMs : update.elapsedMs
    });
    var previousNode = (overview.nodes || []).find(function (node) {
      return node.id === update.nodeId;
    });
    var previousField = fieldValue(previousNode, update.dimension);
    var nextValue = update.value || {};
    if (["timeout", "error"].indexOf(nextValue.state) >= 0) {
      nextValue = mergeFieldAttemptFailure(previousField, nextValue.state, nextValue.error);
    }
    return {
      overview: replaceFleetDimension(
        overview,
        update.nodeId,
        update.dimension,
        Object.assign({}, nextValue, { refreshing: false })
      ),
      refresh: nextRefresh,
      accepted: true,
      newlyCompleted: newlyCompleted
    };
  }

  function mergeFleetOverviewMetadata(overview, incoming) {
    var existingById = Object.create(null);
    (overview.nodes || []).forEach(function (node) { existingById[node.id] = node; });
    var seen = Object.create(null);
    var nodes = (incoming.nodes || []).map(function (node) {
      seen[node.id] = true;
      var existing = existingById[node.id];
      if (!existing) return node;
      return Object.assign({}, node, { dimensions: existing.dimensions });
    });
    (overview.nodes || []).forEach(function (node) {
      if (!seen[node.id]) nodes.push(node);
    });
    return Object.assign({}, overview, incoming, {
      nodes: nodes,
      edges: incoming.edges || []
    });
  }

  function activityLabel(value) {
    value = value || {};
    if (value.current_dispatch && value.current_dispatch.phase) {
      return value.current_dispatch.phase.replace(/_/g, " ");
    }
    var count = (value.sessions || []).length + (value.dispatches || []).length;
    var state = value.state || "unavailable";
    return count > 1 ? state + " +" + (count - 1) : state;
  }

  function fleetCapacityPresentation(activityValue, configuredCapacity) {
    var value = activityValue || {};
    var capacity = value.capacity || {};
    var promptBacklog = Number(value.queued_prompts || 0);
    var limit = capacity.limit || configuredCapacity || "pending";
    var consumed = Number(capacity.consumed || 0);
    return {
      summary: consumed + "/" + limit + " slots used · " + promptBacklog +
        " prompt" + (promptBacklog === 1 ? "" : "s") + " queued",
      source: String(capacity.source ||
        (configuredCapacity ? "configured" : "compatibility pending"))
        .replace(/_/g, " ")
    };
  }

  function setFieldState(element, state) {
    if (!element) return;
    ["fresh", "stale", "timeout", "error", "unavailable"].forEach(function (name) {
      element.classList.toggle("fleet-field-" + name, state === name);
    });
  }

  function renderFleetRow(nodeState) {
    var node = nodeState.node;
    var tr = $('#pa-fleet-instances tr[data-fleet-instance="' + CSS.escape(node.id) + '"]');
    if (!tr) return;
    var reach = fieldValue(node, "reachability");
    var status = fieldValue(node, "status");
    var update = fieldValue(node, "update");
    var providers = fieldValue(node, "providers");
    var activity = fieldValue(node, "activity");
    var sync = fieldValue(node, "sync");
    var reachValue = reach.value || {};
    var health = reachValue.health || reach.state;
    var healthEl = $("[data-fleet-health]", tr);
    if (healthEl) {
      healthEl.innerHTML = healthHtml(health);
      setFieldState(healthEl, reach.state);
      healthEl.dataset.fleetTerminal = "1";
    }
    var syncEl = $("[data-fleet-sync]", tr);
    if (syncEl) {
      syncEl.textContent = sync.value
        ? (sync.value.consistent ? "in sync" : (observationAttempt(sync) === "fresh" ? "head mismatch" : "last known"))
        : sync.state;
      setFieldState(syncEl, sync.state);
    }
    var currentEl = $("[data-fleet-current-version]", tr);
    if (currentEl) {
      currentEl.textContent = status.value && status.value.version
        ? status.value.version
        : status.state;
      setFieldState(currentEl, status.state);
      currentEl.dataset.fleetTerminal = "1";
    }
    var availableEl = $("[data-fleet-available-version]", tr);
    if (availableEl) {
      var available = update.value && (update.value.available_version || update.value.latest);
      availableEl.textContent = available || update.state;
      availableEl.classList.toggle("status-active", !!(update.value && update.value.upgrade_available));
      setFieldState(availableEl, update.state);
      availableEl.dataset.fleetTerminal = "1";
    }
    var providersEl = $("[data-fleet-providers]", tr);
    if (providersEl) {
      providersEl.innerHTML = providers.value
        ? providersHtml(providers.value, node.id)
        : '<span class="muted">' + escapeHtml(providers.state) + "</span>";
      setFieldState(providersEl, providers.state);
      providersEl.dataset.fleetTerminal = "1";
    }
    var activityEl = $("[data-fleet-active-work]", tr);
    if (activityEl) {
      var activityValue = activity.value || {};
      var currentDispatch = activityValue.current_dispatch || null;
      var currentFreshness = currentDispatch && currentDispatch.freshness || {};
      activityEl.innerHTML = "<strong>" + escapeHtml(activityLabel(activityValue)) +
        '</strong><span class="muted small">' +
        escapeHtml(currentDispatch && currentDispatch.summary ||
          activityValue.summary || "No activity detail yet") + "</span>" +
        (currentDispatch
          ? '<span class="fleet-freshness fleet-field-' +
            escapeHtml(currentFreshness.state || "delayed") + '">' +
            escapeHtml(currentFreshness.state || "delayed") +
            (currentFreshness.age_seconds == null
              ? "" : " · " + escapeHtml(currentFreshness.age_seconds) + "s") +
            "</span>"
          : "");
      setFieldState(activityEl, activity.state);
    }
    var capacityEl = $("[data-fleet-capacity]", tr);
    if (capacityEl) {
      var observedActivity = activity.value;
      var utilization = observedActivity && observedActivity.capacity || {};
      var queueUtilization = observedActivity && observedActivity.queue_capacity || {};
      var configured = utilization.limit || node.dispatch_capacity;
      var hasUtilization = !!observedActivity &&
        utilization.consumed != null && observedActivity.queued_prompts != null;
      capacityEl.removeAttribute("aria-label");
      if (configured && hasUtilization) {
        var capacityPresentation = fleetCapacityPresentation(
          observedActivity, node.dispatch_capacity
        );
        capacityEl.innerHTML = '<strong aria-label="' +
          escapeHtml(capacityPresentation.summary) +
          '">' + escapeHtml(capacityPresentation.summary) +
          '</strong><span class="muted small">' +
          escapeHtml(capacityPresentation.source) + "</span>" +
          (queueUtilization.limit == null || queueUtilization.consumed == null
            ? "" : '<span class="muted small">' +
              escapeHtml(queueUtilization.consumed) + "/" +
              escapeHtml(queueUtilization.limit) + " waiting</span>");
      } else if (configured) {
        capacityEl.innerHTML = '<strong>' + escapeHtml(configured) +
          ' slots configured</strong><span class="muted small">capacity utilization ' +
          escapeHtml(activity.state || "pending") + "</span>";
      } else {
        capacityEl.innerHTML =
          '<strong>pending</strong><span class="muted small">capacity probe unavailable</span>';
      }
      setFieldState(capacityEl, activity.state);
    }
    var freshnessEl = $("[data-fleet-freshness]", tr);
    if (freshnessEl) {
      var observed = reach.observed_at || status.observed_at;
      var freshnessLabel = nodeState.refreshing
        ? nodeState.freshness + " · refreshing"
        : nodeState.freshness;
      freshnessEl.innerHTML = '<span class="fleet-freshness fleet-field-' +
        escapeHtml(nodeState.freshness) + '" aria-label="Freshness ' +
        escapeHtml(freshnessLabel) + '">' + escapeHtml(freshnessLabel) +
        "</span>" + (observed ? '<time class="muted small" datetime="' +
          escapeHtml(observed) + '">' + escapeHtml(new Date(observed).toLocaleString()) + "</time>" : "");
    }
    tr.setAttribute("aria-label", nodeState.accessibleLabel);
    var statusValue = status.value || {};
    var updateValue = update.value || {};
    tr.dataset.updateChannel = updateValue.channel || statusValue.release_track || "release";
    tr.dataset.currentVersion = statusValue.version || "";
    tr.dataset.availableVersion = updateValue.available_version || "";
  }

  function nodeById(id, snapshot) {
    var state = snapshot && snapshot.nodesById[id];
    return state ? state.node : null;
  }

  function edgeById(id, snapshot) {
    return (snapshot && snapshot.overview.edges || []).find(function (edge) {
      return edge.id === id;
    });
  }

  function edgeItemById(edge, id) {
    return (edge && edge.details && edge.details.items || []).find(function (item) {
      return item.id === id;
    });
  }

  function edgeStatusSummary(edge) {
    var counts = edge && edge.status_counts || {};
    return Object.keys(counts).sort().map(function (status) {
      return status + " " + counts[status];
    }).join(", ");
  }

  function edgeVisualStatus(edge, snapshot) {
    var targetState = edge && edge.target && snapshot && snapshot.nodesById[edge.target];
    var target = targetState && targetState.node;
    if (!target) return edge && edge.status || "unavailable";
    var reach = fieldValue(target, "reachability");
    var health = reach.value && reach.value.health;
    if (health && health !== "up") {
      return health === "unknown" ? "unavailable" : "degraded";
    }
    if (reach.state === "error" || reach.state === "timeout") return "degraded";
    if (reach.state === "unavailable") return "unavailable";
    if (targetState.freshness === "stale") return "stale";
    return edge.status || "healthy";
  }

  function renderFleetEdgeList(snapshot) {
    var list = $("#pa-fleet-edge-list");
    if (!list || !snapshot) return;
    var edges = snapshot.overview.edges || [];
    list.innerHTML = edges.length ? edges.map(function (edge) {
      var status = edgeVisualStatus(edge, snapshot);
      return '<li><button type="button" class="link-button" data-fleet-edge="' +
        escapeHtml(edge.id) + '">' + escapeHtml(edge.kind + ": " +
          endpointIdentityName(edge.source) + " → " + endpointIdentityName(edge.target) +
          " · " + (edge.label || edge.id) + " · " + status) + "</button></li>";
    }).join("") : '<li class="muted">No registered routes.</li>';
  }

  function renderFleetDetail(kind, id, edgeId, snapshot) {
    var panel = $("#pa-fleet-detail");
    if (!panel || !fleetOverview) return;
    selectedFleetItem = { kind: kind, id: id };
    if (kind === "edge-item") selectedFleetItem.edgeId = edgeId;
    var current = snapshot;
    if (
      !current ||
      !current.selection ||
      current.selection.kind !== kind ||
      current.selection.id !== id ||
      current.selection.edgeId !== selectedFleetItem.edgeId
    ) {
      current = createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem);
      fleetRenderedSnapshot = current;
    if (window.PAInstanceIdentity) {
      window.PAInstanceIdentity.setDirectory(current.nodes.map(function (state) {
        return state.node || state;
      }));
    }
    }
    if (kind === "edge-item") {
      var parent = edgeById(edgeId, current);
      var item = edgeItemById(parent, id);
      if (!parent || !item) {
        if (parent) renderFleetDetail("edge", parent.id, null, current);
        return;
      }
      var watchId = item.details && item.details.id || item.id;
      panel.innerHTML = '<button type="button" class="ghost small" data-fleet-edge="' +
        escapeHtml(parent.id) + '">← Back to group</button><h3>' +
        escapeHtml(parent.kind + " detail") + "</h3><p><strong>" +
        escapeHtml(item.label || item.id) + "</strong></p>" +
        '<dl class="fleet-detail-list"><dt>Stable ID</dt><dd><code>' +
        escapeHtml(watchId) + "</code></dd><dt>Status</dt><dd>" +
        escapeHtml(item.status || "unavailable") +
        "</dd></dl><details open><summary>Exact watch detail</summary><pre>" +
        escapeHtml(JSON.stringify(item.details || {}, null, 2)) + "</pre></details>";
      return;
    }
    if (kind === "edge") {
      var edge = edgeById(id, current);
      if (!edge) return;
      selectedFleetItem = { kind: kind, id: id };
      var items = edge.details && edge.details.items || [];
      var itemList = items.length ? '<h4>Underlying ' +
        escapeHtml(edge.kind === "supervisor" ? "watches" : "activities") +
        '</h4><ul class="fleet-edge-item-list">' + items.map(function (item) {
          var stableId = item.details && item.details.id || item.id;
          var accessibleLabel = "Open " + edge.kind + " " + stableId + ": " +
            (item.label || item.id) + ", " + (item.status || "unavailable");
          return '<li><button type="button" class="link-button" data-fleet-edge-item="' +
            escapeHtml(item.id) + '" data-fleet-edge-parent="' + escapeHtml(edge.id) +
            '" aria-label="' + escapeHtml(accessibleLabel) + '">' +
            escapeHtml((item.label || item.id) + " · " + stableId + " · " +
              (item.status || "unavailable")) + "</button></li>";
        }).join("") + "</ul>" : "";
      var distinct = edge.kind === "supervisor"
        ? '<dt>Pull requests</dt><dd>' + escapeHtml(edge.distinct_count || 0) + "</dd>"
        : "";
      var statusSummary = edgeStatusSummary(edge);
      panel.innerHTML = "<h3>" + escapeHtml(edge.kind + " route") + "</h3>" +
        "<p><strong>" + escapeHtml(edge.label || edge.id) + "</strong></p>" +
        '<dl class="fleet-detail-list"><dt>Direction</dt><dd>' +
        endpointIdentityHtml(edge.source) + " → " + endpointIdentityHtml(edge.target) +
        "</dd><dt>Status</dt><dd>" + escapeHtml(edgeVisualStatus(edge, current)) +
        "</dd><dt>Activities</dt><dd>" + escapeHtml(edge.count || 1) + "</dd>" +
        distinct + (statusSummary ? "<dt>Status counts</dt><dd>" +
          escapeHtml(statusSummary) + "</dd>" : "") + "</dl>" + itemList +
        "<details><summary>Exact grouped detail</summary><pre>" +
        escapeHtml(JSON.stringify(edge.details || {}, null, 2)) + "</pre></details>";
      return;
    }
    var nodeState = current.nodesById[id];
    var node = nodeState && nodeState.node;
    if (!node) return;
    selectedFleetItem = { kind: kind, id: id };
    var sections = Object.keys(node.dimensions || {}).map(function (name) {
      var item = node.dimensions[name] || {};
      var timingValue = item.last_attempt_duration_ms == null
        ? item.duration_ms : item.last_attempt_duration_ms;
      var timing = timingValue == null ? "" : " · " + timingValue + " ms";
      var error = item.error
        ? '<p class="fleet-diagnostic-error">Latest refresh ' +
          escapeHtml(item.last_attempt_state || item.state || "failed") + ". Retry " +
          escapeHtml(name) + " on this instance. " + escapeHtml(item.error) + "</p>"
        : "";
      var attempted = item.last_attempted_at
        ? "Last attempted " + new Date(item.last_attempted_at).toLocaleString()
        : "Never attempted";
      var successful = item.last_successful_at || item.observed_at;
      return '<details><summary><strong>' + escapeHtml(name) + "</strong> · " +
        escapeHtml(item.state || "unavailable") + escapeHtml(timing) +
        '</summary><p class="muted small">' + escapeHtml(attempted) + " · " +
        escapeHtml(successful ? "Last successful " + new Date(successful).toLocaleString() : "Never successful") +
        "</p>" + error + "<pre>" + escapeHtml(JSON.stringify(item.value, null, 2)) +
        "</pre></details>";
    }).join("");
    panel.innerHTML = "<h3>" + identityHtml(node.id) + "</h3><p>" +
      escapeHtml(activityLabel(fieldValue(node, "activity").value)) +
      ' · <span class="fleet-freshness fleet-field-' + escapeHtml(nodeState.freshness) +
      '">' + escapeHtml(nodeState.freshness) + "</span></p>" +
      '<dl class="fleet-detail-list"><dt>Endpoint</dt><dd>' + escapeHtml(node.url) +
      "</dd><dt>Zone</dt><dd>" + escapeHtml(node.zone || "default") +
      "</dd><dt>Capacity</dt><dd>" +
      escapeHtml((function () {
        var activityValue = fieldValue(node, "activity").value || {};
        var presentation = fleetCapacityPresentation(
          activityValue, node.dispatch_capacity
        );
        return presentation.summary + " · " + presentation.source;
      })()) + "</dd></dl>" + sections;
  }

  function fleetTopologyLayout(nodes, containerWidth) {
    var count = nodes.length;
    var positions = {};
    var width = Number(containerWidth) || 960;
    var mode = "radial";
    var viewWidth = 960;
    var viewHeight = 420;

    if (count === 1 && width <= 600) {
      mode = "single";
      viewWidth = 320;
      viewHeight = 220;
      positions[(nodes[0].node || nodes[0]).id] = { x: 160, y: 110 };
    } else if (count > 1 && width <= 480) {
      mode = "stacked";
      viewWidth = 320;
      viewHeight = 224 + Math.max(0, count - 1) * 150;
      nodes.forEach(function (nodeState, index) {
        var node = nodeState.node || nodeState;
        positions[node.id] = { x: 160, y: 112 + index * 150 };
      });
    } else if (count > 1 && width <= 760) {
      mode = "grid";
      viewWidth = 640;
      viewHeight = 224 + Math.max(0, Math.ceil(count / 2) - 1) * 160;
      nodes.forEach(function (nodeState, index) {
        var node = nodeState.node || nodeState;
        positions[node.id] = {
          x: index % 2 === 0 ? 160 : 480,
          y: 112 + Math.floor(index / 2) * 160
        };
      });
    } else {
      nodes.forEach(function (nodeState, index) {
        var node = nodeState.node || nodeState;
        if (count === 1) {
          positions[node.id] = { x: 480, y: 210 };
          return;
        }
        var angle = -Math.PI / 2 + (Math.PI * 2 * index / count);
        positions[node.id] = {
          x: 480 + Math.cos(angle) * 310,
          y: 210 + Math.sin(angle) * 130
        };
      });
    }

    return {
      mode: mode,
      positions: positions,
      viewBox: "0 0 " + viewWidth + " " + viewHeight,
      width: viewWidth,
      height: viewHeight
    };
  }

  var FLEET_TOPOLOGY_MIN_SCALE = 0.5;
  var FLEET_TOPOLOGY_MAX_SCALE = 3;
  var fleetTopologyController = null;
  var fleetTopologySerial = 0;

  function clampFleetTopologyScale(scale) {
    return Math.max(
      FLEET_TOPOLOGY_MIN_SCALE,
      Math.min(FLEET_TOPOLOGY_MAX_SCALE, Number(scale) || 1)
    );
  }

  function topologyViewportAfterZoom(viewport, scale, center) {
    var previous = viewport || { x: 0, y: 0, scale: 1 };
    var nextScale = clampFleetTopologyScale(scale);
    var ratio = nextScale / previous.scale;
    var point = center || { x: 0, y: 0 };
    return {
      x: point.x - (point.x - previous.x) * ratio,
      y: point.y - (point.y - previous.y) * ratio,
      scale: nextScale,
      userSet: true
    };
  }

  function topologyEventPoint(svg, event) {
    var rect = svg.getBoundingClientRect();
    var viewBox = svg.viewBox && svg.viewBox.baseVal;
    var width = viewBox && viewBox.width || 960;
    var height = viewBox && viewBox.height || 420;
    return {
      x: (event.clientX - rect.left) * width / (rect.width || 1) +
        (viewBox && viewBox.x || 0),
      y: (event.clientY - rect.top) * height / (rect.height || 1) +
        (viewBox && viewBox.y || 0)
    };
  }

  function topologyNodeBoundaryPoint(node, toward) {
    var dx = toward.x - node.x;
    var dy = toward.y - node.y;
    if (!dx && !dy) return { x: node.x, y: node.y };
    var scale = Math.min(94 / (Math.abs(dx) || Infinity), 58 / (Math.abs(dy) || Infinity));
    return { x: node.x + dx * scale, y: node.y + dy * scale };
  }

  function syncTopologyElement(current, incoming) {
    Array.prototype.slice.call(current.attributes || []).forEach(function (attribute) {
      if (!incoming.hasAttribute(attribute.name)) current.removeAttribute(attribute.name);
    });
    Array.prototype.slice.call(incoming.attributes || []).forEach(function (attribute) {
      if (current.getAttribute(attribute.name) !== attribute.value) {
        current.setAttribute(attribute.name, attribute.value);
      }
    });
    var currentChildren = Array.prototype.slice.call(current.childNodes || []);
    var incomingChildren = Array.prototype.slice.call(incoming.childNodes || []);
    incomingChildren.forEach(function (nextChild, index) {
      var existing = currentChildren[index];
      if (!existing) {
        current.appendChild(nextChild.cloneNode(true));
        return;
      }
      if (
        existing.nodeType !== nextChild.nodeType ||
        (existing.nodeType === 1 && existing.tagName !== nextChild.tagName)
      ) {
        current.replaceChild(nextChild.cloneNode(true), existing);
        return;
      }
      if (existing.nodeType === 1) syncTopologyElement(existing, nextChild);
      else if (existing.nodeValue !== nextChild.nodeValue) {
        existing.nodeValue = nextChild.nodeValue;
      }
    });
    while (current.childNodes.length > incomingChildren.length) {
      current.removeChild(current.lastChild);
    }
  }

  function FleetTopologyController(host) {
    this.host = host;
    this.svg = $("svg", host);
    this.panel = (host.closest && host.closest(".fleet-topology-panel")) ||
      host.parentElement;
    this.state = $("[data-fleet-topology-state]", host);
    this.snapshot = null;
    this.layout = null;
    this.layoutSignature = "";
    this.viewport = { x: 0, y: 0, scale: 1, userSet: false };
    this.pointerId = null;
    this.pointerOrigin = null;
    this.viewportOrigin = null;
    this.observedWidth = Math.round(host.getBoundingClientRect().width);
    this.resizeFrame = null;
    this.markerId = "fleet-arrow-" + (++fleetTopologySerial);
    this.handlers = {};
    this.bind();
  }

  FleetTopologyController.prototype.bind = function () {
    var controller = this;
    var svg = this.svg;
    if (!svg) return;
    this.handlers.wheel = function (event) {
      var factor = event.deltaY < 0 ? 1.12 : 1 / 1.12;
      var nextScale = clampFleetTopologyScale(controller.viewport.scale * factor);
      if (Math.abs(nextScale - controller.viewport.scale) < 0.001) return;
      event.preventDefault();
      controller.viewport = topologyViewportAfterZoom(
        controller.viewport,
        nextScale,
        topologyEventPoint(svg, event)
      );
      controller.updateTransform();
    };
    this.handlers.pointerdown = function (event) {
      if (!event.target.closest || !event.target.closest("[data-fleet-pan-surface]")) return;
      if (event.button != null && event.button !== 0) return;
      event.preventDefault();
      controller.pointerId = event.pointerId;
      controller.pointerOrigin = topologyEventPoint(svg, event);
      controller.viewportOrigin = Object.assign({}, controller.viewport);
      controller.host.classList.add("is-panning");
      if (svg.setPointerCapture) {
        try { svg.setPointerCapture(event.pointerId); } catch (ignore) {}
      }
    };
    this.handlers.pointermove = function (event) {
      if (controller.pointerId !== event.pointerId || !controller.pointerOrigin) return;
      var point = topologyEventPoint(svg, event);
      controller.viewport = {
        x: controller.viewportOrigin.x + point.x - controller.pointerOrigin.x,
        y: controller.viewportOrigin.y + point.y - controller.pointerOrigin.y,
        scale: controller.viewportOrigin.scale,
        userSet: true
      };
      controller.updateTransform();
    };
    this.handlers.pointerend = function (event) {
      if (controller.pointerId !== event.pointerId) return;
      controller.cancelPan();
    };
    this.handlers.pointerover = function (event) {
      controller.setTransientTarget(event.target);
    };
    this.handlers.pointerout = function (event) {
      if (event.relatedTarget && svg.contains(event.relatedTarget)) {
        controller.setTransientTarget(event.relatedTarget);
      } else {
        controller.clearTransientTarget();
      }
    };
    this.handlers.focusin = function (event) {
      controller.setTransientTarget(event.target);
    };
    this.handlers.focusout = function (event) {
      if (event.relatedTarget && svg.contains(event.relatedTarget)) {
        controller.setTransientTarget(event.relatedTarget);
      } else {
        controller.clearTransientTarget();
      }
    };
    this.handlers.control = function (event) {
      var button = event.target.closest &&
        event.target.closest("[data-fleet-topology-action]");
      if (!button || !controller.panel || !controller.panel.contains(button)) return;
      event.preventDefault();
      var action = button.dataset.fleetTopologyAction;
      if (action === "zoom-in") controller.zoomBy(1.25);
      if (action === "zoom-out") controller.zoomBy(0.8);
      if (action === "reset") controller.resetViewport(true);
      if (action === "fit") controller.fitViewport(true);
    };
    svg.addEventListener("wheel", this.handlers.wheel, { passive: false });
    svg.addEventListener("pointerdown", this.handlers.pointerdown);
    svg.addEventListener("pointermove", this.handlers.pointermove);
    svg.addEventListener("pointerup", this.handlers.pointerend);
    svg.addEventListener("pointercancel", this.handlers.pointerend);
    svg.addEventListener("lostpointercapture", this.handlers.pointerend);
    svg.addEventListener("pointerover", this.handlers.pointerover);
    svg.addEventListener("pointerout", this.handlers.pointerout);
    svg.addEventListener("focusin", this.handlers.focusin);
    svg.addEventListener("focusout", this.handlers.focusout);
    if (this.panel) this.panel.addEventListener("click", this.handlers.control);
    if (typeof ResizeObserver === "function") {
      this.observer = new ResizeObserver(function (entries) {
        var width = entries[0] && Math.round(entries[0].contentRect.width);
        if (!width || Math.abs(width - controller.observedWidth) < 2) return;
        controller.observedWidth = width;
        controller.scheduleRender();
      });
      this.observer.observe(this.host);
    }
  };

  FleetTopologyController.prototype.destroy = function () {
    this.cancelPan();
    if (this.observer) this.observer.disconnect();
    if (this.resizeFrame) cancelAnimationFrame(this.resizeFrame);
    if (this.svg) {
      this.svg.removeEventListener("wheel", this.handlers.wheel);
      this.svg.removeEventListener("pointerdown", this.handlers.pointerdown);
      this.svg.removeEventListener("pointermove", this.handlers.pointermove);
      this.svg.removeEventListener("pointerup", this.handlers.pointerend);
      this.svg.removeEventListener("pointercancel", this.handlers.pointerend);
      this.svg.removeEventListener("lostpointercapture", this.handlers.pointerend);
      this.svg.removeEventListener("pointerover", this.handlers.pointerover);
      this.svg.removeEventListener("pointerout", this.handlers.pointerout);
      this.svg.removeEventListener("focusin", this.handlers.focusin);
      this.svg.removeEventListener("focusout", this.handlers.focusout);
    }
    if (this.panel) this.panel.removeEventListener("click", this.handlers.control);
    this.host.classList.remove("is-panning", "has-transient-emphasis");
    this.snapshot = null;
  };

  FleetTopologyController.prototype.cancelPan = function () {
    var pointerId = this.pointerId;
    this.pointerId = null;
    this.pointerOrigin = null;
    this.viewportOrigin = null;
    this.host.classList.remove("is-panning");
    if (
      pointerId != null &&
      this.svg &&
      this.svg.hasPointerCapture &&
      this.svg.hasPointerCapture(pointerId)
    ) {
      try { this.svg.releasePointerCapture(pointerId); } catch (ignore) {}
    }
  };

  FleetTopologyController.prototype.scheduleRender = function () {
    var controller = this;
    if (this.resizeFrame) cancelAnimationFrame(this.resizeFrame);
    this.resizeFrame = requestAnimationFrame(function () {
      controller.resizeFrame = null;
      if (controller.snapshot) controller.render(controller.snapshot);
    });
  };

  FleetTopologyController.prototype.updateControls = function () {
    if (!this.panel) return;
    var scale = this.viewport.scale;
    var zoomIn = $('[data-fleet-topology-action="zoom-in"]', this.panel);
    var zoomOut = $('[data-fleet-topology-action="zoom-out"]', this.panel);
    var reset = $('[data-fleet-topology-action="reset"]', this.panel);
    var scaleLabel = $("[data-fleet-topology-scale]", this.panel);
    if (zoomIn) zoomIn.disabled = scale >= FLEET_TOPOLOGY_MAX_SCALE - 0.001;
    if (zoomOut) zoomOut.disabled = scale <= FLEET_TOPOLOGY_MIN_SCALE + 0.001;
    if (reset) {
      reset.disabled = Math.abs(scale - 1) < 0.001 &&
        Math.abs(this.viewport.x) < 0.5 && Math.abs(this.viewport.y) < 0.5;
    }
    if (scaleLabel) scaleLabel.textContent = Math.round(scale * 100) + "%";
  };

  FleetTopologyController.prototype.updateTransform = function () {
    var viewport = $("[data-fleet-topology-viewport]", this.svg);
    if (viewport) {
      viewport.setAttribute(
        "transform",
        "translate(" + this.viewport.x + " " + this.viewport.y + ") scale(" +
          this.viewport.scale + ")"
      );
    }
    this.host.dataset.fleetTopologyScale = String(this.viewport.scale);
    this.updateControls();
  };

  FleetTopologyController.prototype.resetViewport = function (userSet) {
    this.viewport = { x: 0, y: 0, scale: 1, userSet: !!userSet };
    this.updateTransform();
  };

  FleetTopologyController.prototype.zoomBy = function (factor) {
    if (!this.layout) return;
    this.viewport = topologyViewportAfterZoom(
      this.viewport,
      this.viewport.scale * factor,
      { x: this.layout.width / 2, y: this.layout.height / 2 }
    );
    this.updateTransform();
  };

  FleetTopologyController.prototype.fitViewport = function (userSet) {
    if (!this.layout) return;
    var content = $("[data-fleet-topology-content]", this.svg);
    var bounds = content && content.getBBox ? content.getBBox() : null;
    if (!bounds || !bounds.width || !bounds.height) {
      this.resetViewport(userSet);
      return;
    }
    var padding = 36;
    var scale = clampFleetTopologyScale(Math.min(
      (this.layout.width - padding * 2) / bounds.width,
      (this.layout.height - padding * 2) / bounds.height
    ));
    this.viewport = {
      x: this.layout.width / 2 - (bounds.x + bounds.width / 2) * scale,
      y: this.layout.height / 2 - (bounds.y + bounds.height / 2) * scale,
      scale: scale,
      userSet: !!userSet
    };
    this.updateTransform();
  };

  FleetTopologyController.prototype.setTransientTarget = function (target) {
    var item = target && target.closest &&
      target.closest("[data-fleet-node], [data-fleet-edge]");
    if (!item || !this.svg.contains(item)) {
      this.clearTransientTarget();
      return;
    }
    var nodeId = item.getAttribute("data-fleet-node");
    var edgeId = item.getAttribute("data-fleet-edge");
    var nodes = $all("[data-fleet-node]", this.svg);
    var edges = $all("[data-fleet-edge]", this.svg);
    nodes.concat(edges).forEach(function (candidate) {
      var related = candidate === item;
      if (nodeId && candidate.hasAttribute("data-fleet-edge")) {
        related = candidate.dataset.fleetSource === nodeId ||
          candidate.dataset.fleetTarget === nodeId;
      } else if (edgeId && candidate.hasAttribute("data-fleet-node")) {
        related = candidate.dataset.fleetNode === item.dataset.fleetSource ||
          candidate.dataset.fleetNode === item.dataset.fleetTarget;
      }
      candidate.classList.toggle("fleet-transient-dimmed", !related);
    });
    this.host.classList.add("has-transient-emphasis");
  };

  FleetTopologyController.prototype.clearTransientTarget = function () {
    $all(".fleet-transient-dimmed", this.svg).forEach(function (item) {
      item.classList.remove("fleet-transient-dimmed");
    });
    this.host.classList.remove("has-transient-emphasis");
  };

  FleetTopologyController.prototype.render = function (current) {
    var host = this.host;
    var svg = this.svg;
    if (!svg || !current) return;
    this.cancelPan();
    this.snapshot = current;
    var nodes = current.nodes || [];
    var edges = current.overview.edges || [];
    var layout = fleetTopologyLayout(nodes, host.getBoundingClientRect().width);
    var signature = layout.mode + "|" + layout.viewBox + "|" + nodes.map(function (item) {
      return item.node.id;
    }).sort().join("\u0000");
    var layoutChanged = !!this.layoutSignature && this.layoutSignature !== signature;
    this.layout = layout;
    this.layoutSignature = signature;
    var active = document.activeElement;
    var focusedKind = active && active.closest && active.closest("[data-fleet-node]")
      ? "node"
      : (active && active.closest && active.closest("[data-fleet-edge]") ? "edge" : "");
    var focusedId = focusedKind && active.getAttribute("data-fleet-" + focusedKind);
    svg.setAttribute("viewBox", layout.viewBox);
    svg.dataset.layout = layout.mode;
    svg.classList.toggle("fleet-topology-multi", nodes.length > 1);
    svg.classList.toggle("fleet-topology-compact", layout.mode !== "radial");
    var positions = layout.positions;
    var selectedNodeId = current.selection && current.selection.kind === "node"
      ? current.selection.id : "";
    var selectedEdgeId = current.selection && current.selection.kind === "edge"
      ? current.selection.id
      : (current.selection && current.selection.kind === "edge-item"
        ? current.selection.edgeId : "");
    var parts = [
      '<defs><marker id="' + this.markerId +
        '" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">' +
        '<path d="M0,0 L10,4 L0,8 z" fill="context-stroke"></path></marker></defs>',
      '<rect class="fleet-topology-pan-surface" data-fleet-pan-surface x="0" y="0" width="' +
        layout.width + '" height="' + layout.height + '"></rect>',
      '<g data-fleet-topology-viewport><g data-fleet-topology-content>'
    ];
    var edgeVisuals = [];
    var edgeLabels = [];
    var edgeInteractions = [];
    var nodeVisuals = [];
    var parallelEdges = {};
    edges.forEach(function (edge) {
      if (!positions[edge.source] || !positions[edge.target]) return;
      var pair = [String(edge.source), String(edge.target)].sort().join("\u0000");
      if (!parallelEdges[pair]) parallelEdges[pair] = [];
      parallelEdges[pair].push(edge);
    });
    Object.keys(parallelEdges).forEach(function (pair) {
      parallelEdges[pair].sort(function (left, right) {
        return String(left.id).localeCompare(String(right.id));
      });
    });
    edges.forEach(function (edge) {
      var from = positions[edge.source];
      var to = positions[edge.target];
      if (!from || !to) return;
      var visualStatus = edgeVisualStatus(edge, current);
      var countLabel = edge.count > 1 ? edge.count + " activities · " : "";
      var label = escapeHtml(edge.kind + " · " + countLabel +
        (edge.label || visualStatus) + " · " + visualStatus);
      var pairKey = [String(edge.source), String(edge.target)].sort().join("\u0000");
      var siblings = parallelEdges[pairKey] || [edge];
      var parallelIndex = siblings.indexOf(edge);
      var parallelOffset = (parallelIndex - (siblings.length - 1) / 2) * 200;
      var visualLabel = edge.kind + (edge.count > 1 ? " ×" + edge.count : "");
      var path;
      var labelX;
      var labelY;
      if (edge.source === edge.target) {
        var loopLift = 92 + Math.abs(parallelOffset);
        var loopSpread = 115 + parallelIndex * 16;
        path = "M " + (from.x + 58) + " " + (from.y - 58) + " C " +
          (from.x + loopSpread) + " " + (from.y - loopLift) + ", " +
          (from.x - loopSpread) + " " + (from.y - loopLift) + ", " +
          (from.x - 58) + " " + (from.y - 58);
        labelX = from.x;
        labelY = from.y - loopLift + 10;
      } else {
        var canonicalIds = [String(edge.source), String(edge.target)].sort();
        var canonicalFrom = positions[canonicalIds[0]];
        var canonicalTo = positions[canonicalIds[1]];
        var dx = canonicalTo.x - canonicalFrom.x;
        var dy = canonicalTo.y - canonicalFrom.y;
        var length = Math.sqrt(dx * dx + dy * dy) || 1;
        var controlX = (from.x + to.x) / 2 - dy / length * parallelOffset;
        var controlY = (from.y + to.y) / 2 + dx / length * parallelOffset;
        var pathFrom = topologyNodeBoundaryPoint(from, { x: controlX, y: controlY });
        var pathTo = topologyNodeBoundaryPoint(to, { x: controlX, y: controlY });
        labelX = (from.x + 2 * controlX + to.x) / 4;
        labelY = (from.y + 2 * controlY + to.y) / 4 - 8;
        path = "M " + pathFrom.x + " " + pathFrom.y + " Q " + controlX + " " +
          controlY + ", " + pathTo.x + " " + pathTo.y;
      }
      var edgeClass = 'fleet-edge fleet-edge-' + escapeHtml(visualStatus) +
        (selectedEdgeId === edge.id ? " fleet-selected" : "");
      var edgeAttributes = ' class="' + edgeClass + '" data-fleet-edge="' +
        escapeHtml(edge.id) + '" data-fleet-source="' + escapeHtml(edge.source) +
        '" data-fleet-target="' + escapeHtml(edge.target) + '"';
      edgeVisuals.push('<g' + edgeAttributes + ' aria-hidden="true"><path class="fleet-edge-halo" d="' +
        path + '"></path><path class="fleet-edge-visual" marker-end="url(#' +
        this.markerId + ')" d="' + path + '"></path></g>');
      edgeLabels.push('<text class="fleet-edge-label" x="' + labelX + '" y="' +
        labelY + '" text-anchor="middle">' + escapeHtml(visualLabel) + '</text>');
      edgeInteractions.push('<g' + edgeAttributes + ' tabindex="0" role="button" aria-label="' +
        label + '"><title>' + label + '</title><path class="fleet-edge-hit" d="' +
        path + '"></path></g>');
    }, this);
    nodes.forEach(function (nodeState) {
      var node = nodeState.node;
      var pos = positions[node.id];
      var reach = fieldValue(node, "reachability");
      var status = fieldValue(node, "status");
      var sync = fieldValue(node, "sync");
      var activity = fieldValue(node, "activity");
      var providers = fieldValue(node, "providers");
      var update = fieldValue(node, "update");
      var health = reach.value && reach.value.health || reach.state;
      var mark = health === "up" ? "✓" : (health === "unknown" ? "?" : "!");
      var version = status.value && status.value.version || status.state;
      var syncLabel = syncStatusLabel(sync);
      var providerValues = Array.isArray(providers.value) ? providers.value : [];
      var readyProviders = providerValues.filter(function (provider) {
        return provider.available !== false && providerAuthState(provider) === "authenticated";
      }).length;
      var visualProviderLabel = providerValues.length
        ? readyProviders + "/" + providerValues.length + " auth"
        : "auth " + providers.state;
      var visualUpdateLabel = update.value && update.value.upgrade_available
        ? "upgrade " + (update.value.available_version || update.value.latest || "available")
        : (update.state === "fresh" ? "current" : update.state);
      nodeVisuals.push('<g class="fleet-node fleet-node-' + escapeHtml(nodeState.topologyStatus) +
        (node.local ? " fleet-node-local" : "") +
        (selectedNodeId === node.id ? " fleet-selected" : "") +
        '" data-fleet-node="' + escapeHtml(node.id) +
        '" tabindex="0" role="button" aria-label="' + escapeHtml(nodeState.accessibleLabel) + '"><title>' +
        escapeHtml(nodeState.accessibleLabel) + '</title><rect class="fleet-node-halo" x="' +
        (pos.x - 99) + '" y="' + (pos.y - 63) + '" width="198" height="126" rx="18"></rect><rect x="' +
        (pos.x - 94) + '" y="' + (pos.y - 58) +
        '" width="188" height="116" rx="14"></rect><text class="fleet-node-name" x="' +
        pos.x + '" y="' + (pos.y - 34) + '" text-anchor="middle">' + escapeHtml(mark + " " + node.name) +
        '</text><text x="' + pos.x + '" y="' + (pos.y - 10) + '" text-anchor="middle">' +
        escapeHtml(activityLabel(activity.value)) + '</text><text x="' + pos.x + '" y="' +
        (pos.y + 10) + '" text-anchor="middle">' + escapeHtml("v" + version + " · " + syncLabel) +
        '</text><text class="fleet-node-readiness" x="' + pos.x + '" y="' +
        (pos.y + 30) + '" text-anchor="middle">' +
        escapeHtml(visualProviderLabel + " · " + visualUpdateLabel) +
        '</text><text class="fleet-node-freshness" x="' + pos.x + '" y="' + (pos.y + 49) +
        '" text-anchor="middle">' + escapeHtml(nodeState.freshness) + "</text></g>");
    });
    parts.push('<g class="fleet-topology-layer fleet-topology-layer-nodes" data-fleet-layer="nodes">' +
      nodeVisuals.join("") + '</g><g class="fleet-topology-layer fleet-topology-layer-edges" data-fleet-layer="edges">' +
      edgeVisuals.join("") + '</g><g class="fleet-topology-layer fleet-topology-layer-labels" data-fleet-layer="labels">' +
      edgeLabels.join("") + '</g><g class="fleet-topology-layer fleet-topology-layer-interactions" data-fleet-layer="interactions">' +
      edgeInteractions.join("") + '</g>');
    parts.push("</g></g>");
    var markup = parts.join("");
    var existingContent = $("[data-fleet-topology-content]", svg);
    if (!layoutChanged && existingContent && typeof document.createElementNS === "function") {
      var scratch = document.createElementNS("http://www.w3.org/2000/svg", "svg");
      scratch.innerHTML = markup;
      var incomingContent = $("[data-fleet-topology-content]", scratch);
      syncTopologyElement(existingContent, incomingContent);
    } else {
      svg.innerHTML = markup;
    }
    if (layoutChanged) this.resetViewport(false);
    else this.updateTransform();
    this.clearTransientTarget();
    if (this.state) {
      this.state.hidden = nodes.length > 0;
      this.state.textContent = current.refresh && current.refresh.warning
        ? "Topology refresh failed. Use Refresh all to retry."
        : (current.refresh && current.refresh.message
          ? current.refresh.message
          : "No cached fleet topology yet. Refreshing automatically…");
    }
    if (focusedId) {
      var selector = '[data-fleet-' + focusedKind + '="' +
        String(focusedId).replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"]';
      var focused = svg.querySelector(selector);
      if (focused) focused.focus({ preventScroll: true });
    }
    renderFleetEdgeList(current);
  };

  function ensureFleetTopologyController(host) {
    if (
      fleetTopologyController &&
      (fleetTopologyController.host !== host || !fleetTopologyController.host.isConnected)
    ) {
      fleetTopologyController.destroy();
      fleetTopologyController = null;
    }
    if (!fleetTopologyController && host) {
      fleetTopologyController = new FleetTopologyController(host);
    }
    return fleetTopologyController;
  }

  function destroyFleetTopologyController() {
    if (!fleetTopologyController) return;
    fleetTopologyController.destroy();
    fleetTopologyController = null;
  }

  function renderFleetTopology(snapshot) {
    var current = snapshot || fleetRenderedSnapshot;
    var host = $("#pa-fleet-topology");
    if (!host || !current) return;
    ensureFleetTopologyController(host).render(current);
  }

  if (window.PA_TEST) {
    window.__paFleetTopology = {
      layout: fleetTopologyLayout,
      render: renderFleetTopology,
      renderRow: renderFleetRow,
      clampScale: clampFleetTopologyScale,
      viewportAfterZoom: topologyViewportAfterZoom,
      controller: function () { return fleetTopologyController; },
      destroy: destroyFleetTopologyController
    };
  }

  function fleetRefreshLabel(refresh) {
    if (!refresh) return "";
    if (refresh.message) return refresh.message;
    var prefix = refresh.completed === 0 ? "Refreshing " : "Refreshed ";
    var label = prefix + refresh.completed + " of " + refresh.total + " fields" +
      (refresh.completed === 0 ? "…" : " · " + Math.round(refresh.elapsedMs || 0) + " ms");
    return refresh.warning ? label + " · " + refresh.warning : label;
  }

  function renderFleetOverview(snapshot) {
    if (!fleetOverview) return;
    var requestedSelection = snapshot && snapshot.selection || selectedFleetItem;
    var current = snapshot || createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem);
    if (requestedSelection && !current.selection) {
      selectedFleetItem = null;
      var detail = $("#pa-fleet-detail");
      if (detail) detail.innerHTML = "<h3>Inspect activity</h3><p class=\"muted\">The selected topology item is no longer available.</p>";
    } else {
      selectedFleetItem = current.selection;
    }
    fleetRenderedSnapshot = current;
    current.nodes.forEach(renderFleetRow);
    renderFleetTopology(current);
    if (current.selection) {
      renderFleetDetail(
        current.selection.kind,
        current.selection.id,
        current.selection.edgeId,
        current
      );
    }
    if (current.refresh) setLiveBanner(fleetRefreshLabel(current.refresh));
  }

  var fleetUpdateName = "";
  var fleetUpdateInstanceId = "";
  var fleetUpdateSource = null;
  var fleetUpdateGeneration = 0;

  function closeFleetUpdateWatcher() {
    fleetUpdateGeneration += 1;
    if (fleetUpdateSource) fleetUpdateSource.close();
    fleetUpdateSource = null;
  }

  function fleetUpdatePresentation(job) {
    var events = (job && job.events) || [];
    var terminalSeverity = job && job.phase === "failed"
      ? "error"
      : (job && job.phase === "succeeded" ? "success" : "info");
    var statusText = job && job.phase === "succeeded"
      ? "Verified PA " + (job.verified_version || "unknown") + " on " + job.instance_name + "."
      : (job && job.phase === "failed" ? (job.error || "Update failed") :
        (events.length ? events[events.length - 1].message : "Update pending…"));
    return {
      severity: terminalSeverity,
      statusText: statusText,
      logText: events.map(function (item) {
        var severity = item.severity || (item.phase === "failed" ? "error" : "info");
        return "[" + severity + "] [" + (item.phase || "update") + "] " +
          (item.message || "");
      }).join("\n")
    };
  }

  if (window.PA_TEST) window.__paFleetUpdatePresentation = fleetUpdatePresentation;

  function renderFleetUpdateJob(job) {
    if (!job || job.instance_id !== fleetUpdateInstanceId) return;
    var log = $("#pa-fleet-update-log");
    var status = $("#pa-fleet-update-status");
    var phase = $("#pa-fleet-update-phase");
    var percent = $("#pa-fleet-update-percent");
    var progress = $("#pa-fleet-update-progress");
    var progressWrap = $("#pa-fleet-update-progress-wrap");
    var submit = $("#pa-fleet-update-form button[type=submit]");
    var value = Math.max(0, Math.min(100, Number(job.progress_percent) || 0));
    var terminal = job.phase === "succeeded" || job.phase === "failed";
    var presentation = fleetUpdatePresentation(job);
    if (progressWrap) progressWrap.hidden = false;
    if (phase) phase.textContent = String(job.phase || "pending").replace(/_/g, " ");
    if (percent) percent.textContent = value + "%";
    if (progress) {
      progress.value = value;
      progress.textContent = value + "%";
    }
    if (submit) submit.disabled = !terminal;
    if (log) {
      log.textContent = presentation.logText;
      log.scrollTop = log.scrollHeight;
    }
    if (status) {
      status.textContent = presentation.statusText;
      status.dataset.severity = presentation.severity;
    }
  }

  async function restoreFleetUpdate(instanceId) {
    var generation = fleetUpdateGeneration;
    var jobs = await api(
      "/api/fleet/instances/" + encodeURIComponent(instanceId) + "/update"
    );
    if (generation !== fleetUpdateGeneration || instanceId !== fleetUpdateInstanceId) return;
    var latest = jobs && jobs[0];
    if (!latest) return;
    renderFleetUpdateJob(latest);
    if (latest.phase !== "succeeded" && latest.phase !== "failed") {
      watchFleetUpdate(instanceId, latest.job_id);
    }
  }

  async function refreshFleetUpdateCheck() {
    var form = $("#pa-fleet-update-form");
    var confirmText = $("#pa-fleet-update-confirm");
    if (!form || !form.elements.instance_id.value) return null;
    var channel = form.elements.channel.value;
    if (confirmText) confirmText.textContent = "Checking " + channel + " availability…";
    var data = await api(
      "/api/fleet/instances/" + encodeURIComponent(form.elements.instance_id.value) +
      "/update-check?channel=" + encodeURIComponent(channel)
    );
    if (confirmText) confirmText.textContent =
      "Update " + fleetUpdateName + " on " + data.channel + " from " +
      (data.current_version || "unknown") + " to " +
      (data.available_version || "unknown") + "? Active agent sessions will be drained and PA will restart.";
    return data;
  }

  function watchFleetUpdate(instanceId, jobId) {
    if (instanceId !== fleetUpdateInstanceId) return;
    if (fleetUpdateSource) fleetUpdateSource.close();
    var generation = fleetUpdateGeneration;
    var source = new EventSource(
      "/api/fleet/instances/" + encodeURIComponent(instanceId) +
      "/update/" + encodeURIComponent(jobId) + "/events"
    );
    fleetUpdateSource = source;
    source.addEventListener("status", function (event) {
      if (generation !== fleetUpdateGeneration || instanceId !== fleetUpdateInstanceId) return;
      renderFleetUpdateJob(JSON.parse(event.data || "{}"));
    });
    source.addEventListener("done", function (event) {
      var job = JSON.parse(event.data || "{}");
      source.close();
      if (fleetUpdateSource === source) fleetUpdateSource = null;
      if (generation !== fleetUpdateGeneration || instanceId !== fleetUpdateInstanceId) return;
      renderFleetUpdateJob(job);
      loadLiveStatus();
    });
    source.onerror = function () {
      var status = $("#pa-fleet-update-status");
      if (generation === fleetUpdateGeneration &&
          instanceId === fleetUpdateInstanceId &&
          source.readyState === EventSource.CLOSED && status) {
        status.textContent = "Update event stream closed; refresh to inspect the persisted result.";
      }
    };
  }

  var liveStatusSeq = 0;
  var liveStatusRequest = null;
  var liveStatusController = null;
  var liveStatusTimer = null;

  function terminalLiveFailure(message, state) {
    if (!$("#pa-fleet-root")) return;
    fleetOverview = Object.assign({}, fleetOverview, {
      nodes: (fleetOverview && fleetOverview.nodes || []).map(function (node) {
        var dimensions = Object.assign({}, node.dimensions || {});
        Object.keys(dimensions).forEach(function (name) {
          var previous = dimensions[name];
          if (previous.refreshing) dimensions[name] = Object.assign(
            {}, mergeFieldAttemptFailure(previous, state, message), { refreshing: false }
          );
        });
        return Object.assign({}, node, { dimensions: dimensions });
      })
    });
    fleetRefresh = Object.assign({}, fleetRefresh || {}, {
      message: (message || "Health check failed") + " · Use Refresh to retry."
    });
    renderFleetOverview(createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem));
  }

  function abortLiveStatus() {
    liveStatusSeq += 1;
    if (liveStatusController) liveStatusController.abort();
    clearTimeout(liveStatusTimer);
    liveStatusTimer = null;
    liveStatusRequest = null;
    liveStatusController = null;
    if (fleetOverview) {
      fleetOverview = Object.assign({}, fleetOverview, {
        nodes: (fleetOverview.nodes || []).map(function (node) {
          var dimensions = {};
          Object.keys(node.dimensions || {}).forEach(function (name) {
            dimensions[name] = Object.assign({}, node.dimensions[name], {
              refreshing: false
            });
          });
          return Object.assign({}, node, { dimensions: dimensions });
        })
      });
    }
  }

  function loadLiveStatus(force, onlyInstance) {
    var root = $("#pa-fleet-root");
    var table = $("#pa-fleet-instances");
    if (!root || !table) return Promise.resolve();
    if (liveStatusRequest && !force) return liveStatusRequest;
    if (liveStatusRequest) abortLiveStatus();
    if (!fleetOverview) fleetOverview = readFleetOverview();
    if (!fleetOverview) {
      terminalLiveFailure("Health check failed", "error");
      return Promise.resolve();
    }
    var seq = ++liveStatusSeq;
    var discoveredNodes = false;
    liveStatusController = typeof AbortController === "function" ? new AbortController() : null;
    var metadataRequest = api(
      "/api/fleet/overview",
      liveStatusController ? { signal: liveStatusController.signal } : {}
    ).then(function (snapshot) {
      if (seq !== liveStatusSeq || !snapshot) return;
      var existingIds = Object.create(null);
      (fleetOverview.nodes || []).forEach(function (node) { existingIds[node.id] = true; });
      discoveredNodes = (snapshot.nodes || []).some(function (node) {
        return !existingIds[node.id];
      });
      fleetOverview = mergeFleetOverviewMetadata(fleetOverview, snapshot);
      renderFleetOverview(createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem));
    }).catch(function (error) {
      if (error.name !== "AbortError" && seq === liveStatusSeq) {
        fleetRefresh = Object.assign({}, fleetRefresh || {}, {
          warning: "Relationship refresh failed"
        });
        renderFleetOverview(createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem));
      }
    });
    var nodes = (fleetOverview.nodes || []).filter(function (node) {
      return !onlyInstance || node.id === onlyInstance;
    });
    var dimensions = fleetOverview.dimensions || [
      "reachability", "status", "providers", "update", "activity", "sync", "repositories", "supervisor"
    ];
    var started = performance.now();
    var begun = beginFleetRefresh(
      fleetOverview,
      nodes.map(function (node) { return node.id; }),
      dimensions,
      seq,
      started
    );
    fleetOverview = begun.overview;
    fleetRefresh = begun.refresh;
    var work = [];
    nodes.forEach(function (node) {
      dimensions.forEach(function (dimension) {
        work.push({ nodeId: node.id, dimension: dimension });
      });
    });
    renderFleetOverview(createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem));

    function currentField(item) {
      var node = (fleetOverview.nodes || []).find(function (candidate) {
        return candidate.id === item.nodeId;
      });
      return fieldValue(node, item.dimension);
    }

    function commitDimension(item, value) {
      var result = applyFleetDimensionUpdate(fleetOverview, fleetRefresh, {
        generation: seq,
        nodeId: item.nodeId,
        dimension: item.dimension,
        value: value,
        elapsedMs: performance.now() - started
      });
      if (!result.accepted || seq !== liveStatusSeq) return false;
      fleetOverview = result.overview;
      fleetRefresh = result.refresh;
      renderFleetOverview(createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem));
      return true;
    }

    async function runOne(item) {
      var controller = typeof AbortController === "function" ? new AbortController() : null;
      var timeout = item.dimension === "reachability" ? 3500 : 5500;
      if (liveStatusController && controller) {
        liveStatusController.signal.addEventListener("abort", function () {
          controller.abort();
        }, { once: true });
      }
      var path = "/api/fleet/overview/dimension?instance_id=" +
        encodeURIComponent(item.nodeId) + "&dimension=" +
        encodeURIComponent(item.dimension) + "&generation=" + seq +
        (force ? "&retry=true" : "");
      var mark = "fleet:" + seq + ":" + item.nodeId + ":" + item.dimension;
      if (performance.mark) performance.mark(mark + ":start");
      var timer = null;
      var deadline = new Promise(function (resolve) {
        timer = setTimeout(function () {
          if (seq === liveStatusSeq) {
            commitDimension(item, mergeFieldAttemptFailure(
              currentField(item),
              "timeout",
              item.dimension + " browser deadline exceeded; awaiting server result"
            ));
          }
          resolve();
        }, timeout);
      });
      var request = api(path, controller ? { signal: controller.signal } : {}).then(function (patch) {
        if (seq !== liveStatusSeq || !patch || patch.generation !== seq) return;
        // A response that arrives after the browser deadline supersedes the
        // provisional timeout without incrementing completed a second time.
        commitDimension(item, patch);
      }).catch(function (err) {
        if (seq !== liveStatusSeq || err.name === "AbortError") return;
        commitDimension(item, mergeFieldAttemptFailure(
          currentField(item), "error", err.message
        ));
      }).finally(function () {
        clearTimeout(timer);
        if (performance.mark && performance.measure) {
          try {
            performance.mark(mark + ":end");
            performance.measure(mark, mark + ":start", mark + ":end");
          } catch (ignore) {}
        }
      });
      await Promise.race([request, deadline]);
    }

    var cursor = 0;
    async function worker() {
      while (cursor < work.length && seq === liveStatusSeq) {
        var item = work[cursor++];
        await runOne(item);
      }
    }
    var workers = [];
    var concurrency = Math.min(4, work.length);
    for (var index = 0; index < concurrency; index += 1) workers.push(worker());
    liveStatusRequest = Promise.all(workers.concat([metadataRequest])).finally(function () {
      if (seq !== liveStatusSeq) return;
      clearTimeout(liveStatusTimer);
      liveStatusTimer = null;
      liveStatusController = null;
      liveStatusRequest = null;
      if (discoveredNodes) {
        setTimeout(function () {
          if (seq === liveStatusSeq && fleetOverviewRoot === $("#pa-fleet-root")) {
            loadLiveStatus(false);
          }
        }, 0);
      }
    });
    return liveStatusRequest;
  }

  function teardownFleetOverview() {
    if (liveStatusRequest || liveStatusController) abortLiveStatus();
    destroyFleetTopologyController();
    fleetOverviewRoot = null;
    fleetOverview = null;
    fleetRefresh = null;
    fleetRenderedSnapshot = null;
    selectedFleetItem = null;
  }

  function maybeLoadLiveStatus() {
    var root = $("#pa-fleet-root");
    if (!root) return;
    if (fleetOverviewRoot === root) {
      if (!fleetTopologyController && fleetRenderedSnapshot) {
        renderFleetTopology(fleetRenderedSnapshot);
      }
      return;
    }
    if (fleetOverviewRoot && fleetOverviewRoot !== root) teardownFleetOverview();
    fleetOverviewRoot = root;
    selectedFleetItem = null;
    fleetOverview = readFleetOverview();
    fleetRefresh = null;
    if (fleetOverview) renderFleetOverview();
    loadLiveStatus(false);
  }

  function initializeFleetPage() {
    var root = $("#pa-fleet-root");
    var layout = root && root.closest ? root.closest(".page-layout") : null;
    remoteOperationsSectionActive = !!layout &&
      layout.dataset.activeSection === "operations";
    maybeLoadLiveStatus();
    maybeLoadRemoteOperations();
    maybeLoadSyncStatus();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initializeFleetPage);
  } else {
    setTimeout(initializeFleetPage, 0);
  }

  window.addEventListener("resize", function () {
    if (fleetTopologyController && !fleetTopologyController.observer) {
      fleetTopologyController.scheduleRender();
    }
  });

  function fleetSwapTarget(evt) {
    return (evt && evt.detail && evt.detail.ctx && evt.detail.ctx.target) ||
      (evt && evt.detail && evt.detail.target) ||
      (evt && evt.target);
  }

  function fleetSwapContainsRoot(evt) {
    var target = fleetSwapTarget(evt);
    return target &&
      (target.id === "app-view" ||
        target.id === "pa-fleet-root" ||
        (target.querySelector && target.querySelector("#pa-fleet-root")));
  }

  function afterFleetSwap(evt) {
    if (fleetSwapContainsRoot(evt)) initializeFleetPage();
  }

  function fleetSwapGeneration(evt) {
    var detail = evt && evt.detail || {};
    var config = detail.requestConfig || detail.request || {};
    var headers = config.headers || {};
    return Number(headers["X-PA-Navigation-Generation"] || 0);
  }

  function beforeFleetSwap(evt) {
    var generation = fleetSwapGeneration(evt);
    if (generation && generation !== fleetPageRefreshGeneration) {
      if (evt.detail) evt.detail.shouldSwap = false;
      if (evt.preventDefault) evt.preventDefault();
      return;
    }
    var target = fleetSwapTarget(evt);
    if (
      target &&
      (target.id === "app-view" ||
        target.id === "pa-fleet-root" ||
        (fleetOverviewRoot && target.contains && target.contains(fleetOverviewRoot)))
    ) {
      remoteOperationsSectionActive = false;
      cancelRemoteSessionLoad("fleet-swap");
      if (!remoteNotificationsActive()) clearRemoteWatchers();
      else remoteActivityTick();
      teardownFleetOverview();
      closeFleetUpdateWatcher();
    }
  }

  document.addEventListener("pa:section-will-change", function (event) {
    var detail = event.detail || {};
    if (detail.from !== "operations") return;
    var root = $("#pa-fleet-root");
    if (!root || !detail.layout || !detail.layout.contains(root)) return;
    remoteOperationsSectionActive = false;
    cancelRemoteSessionLoad("fleet-section-changed");
    if (!remoteNotificationsActive()) clearRemoteWatchers();
    else remoteActivityTick();
  });
  document.addEventListener("pa:section-changed", function (event) {
    var detail = event.detail || {};
    var root = $("#pa-fleet-root");
    if (!root || !detail.layout || !detail.layout.contains(root)) return;
    remoteOperationsSectionActive = detail.to === "operations";
    if (remoteOperationsSectionActive) maybeLoadRemoteOperations();
  });
  document.body.addEventListener("htmx:beforeRequest", function (event) {
    var detail = event.detail || {};
    var target = detail.target || (detail.requestConfig && detail.requestConfig.target);
    var element = detail.elt || event.target;
    var leavesFleet = target && (
      target.id === "app-view" || target === "#app-view"
    );
    if (!leavesFleet && element && element.closest) {
      var link = element.closest('a[hx-get], a[data-spa-link]');
      leavesFleet = !!link;
    }
    if (!leavesFleet) return;
    remoteOperationsSectionActive = false;
    cancelRemoteSessionLoad("fleet-navigation");
    if (!remoteNotificationsActive()) clearRemoteWatchers();
    else remoteActivityTick();
  });
  document.body.addEventListener("htmx:afterSwap", afterFleetSwap);
  document.body.addEventListener("htmx:beforeSwap", beforeFleetSwap);
  document.addEventListener("htmx:historyRestore", function () {
    remoteOperationsSectionActive = false;
    cancelRemoteSessionLoad("fleet-history-restore");
    if (!remoteNotificationsActive()) clearRemoteWatchers();
    setTimeout(initializeFleetPage, 0);
  });
  window.addEventListener("pageshow", function () {
    setTimeout(initializeFleetPage, 0);
  });
  window.addEventListener("popstate", function () {
    remoteOperationsSectionActive = false;
    cancelRemoteSessionLoad("fleet-popstate");
    if (!remoteNotificationsActive()) clearRemoteWatchers();
    setTimeout(initializeFleetPage, 0);
  });
  window.addEventListener("online", function () {
    maybeLoadLiveStatus();
    if ($("#pa-fleet-root") && !liveStatusRequest) loadLiveStatus(false);
  });
  window.addEventListener("pagehide", function () {
    cancelRemoteSessionLoad("pagehide");
    stopRemoteActivity("pagehide", true);
    abortFleetPageRefresh();
    teardownFleetOverview();
  });
  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "hidden") {
      stopRemoteActivity("page-suspended", true);
      return;
    }
    if (remoteSessionRecovery && remoteOperationsSectionActive) {
      remoteSessionRecovery.controller.start(false);
    }
    if (remoteActivityWanted()) remoteActivityTick();
  });
  document.addEventListener("pa:historyWillReload", function () {
    cancelRemoteSessionLoad("history-reload");
    stopRemoteActivity("history-reload", true);
    abortFleetPageRefresh();
    teardownFleetOverview();
    closeFleetUpdateWatcher();
  });

  document.addEventListener("close", function (event) {
    if (event.target && event.target.id === "pa-fleet-update-dialog") {
      closeFleetUpdateWatcher();
    }
  }, true);

  document.addEventListener("keydown", function (event) {
    if (event.key !== "Enter" && event.key !== " ") return;
    var node = event.target.closest && event.target.closest("[data-fleet-node]");
    var item = event.target.closest && event.target.closest("[data-fleet-edge-item]");
    var edge = event.target.closest && event.target.closest("[data-fleet-edge]");
    if (!node && !item && !edge) return;
    event.preventDefault();
    if (node) renderFleetDetail("node", node.getAttribute("data-fleet-node"));
    if (item) {
      renderFleetDetail(
        "edge-item",
        item.getAttribute("data-fleet-edge-item"),
        item.getAttribute("data-fleet-edge-parent")
      );
      return;
    }
    if (edge) renderFleetDetail("edge", edge.getAttribute("data-fleet-edge"));
  });

  document.addEventListener("change", function (e) {
    if (e.target && e.target.id === "pa-sync-conflict-head") {
      syncSelectedRemoteHead = e.target.value || "";
      renderSyncConflicts(syncAllConflicts);
      return;
    }
    if (!e.target || e.target.id !== "pa-remote-instance") return;
    cancelRemoteSessionLoad("remote-instance-changed");
    remoteInstanceId = e.target.value || "";
    remoteAuditGeneration += 1;
    try { localStorage.setItem("pa-remote-instance", remoteInstanceId); } catch (err) {}
    clearRemoteWatchers();
    remoteActivitySessions = {};
    remoteActivityCursors = {};
    var chat = $("#pa-remote-chat");
    var audit = $("#pa-remote-audit");
    if (chat) chat.hidden = true;
    if (audit) audit.hidden = true;
    loadRemoteOperations();
  });

  async function pollJob(jobId, logEl, statusEl) {
    while (true) {
      var job = await api("/api/fleet/install-remote/" + encodeURIComponent(jobId));
      if (logEl) {
        logEl.hidden = false;
        logEl.textContent = job.log || "";
        logEl.scrollTop = logEl.scrollHeight;
      }
      if (statusEl) statusEl.textContent = "Status: " + job.status;
      if (job.status === "succeeded" || job.status === "failed") {
        if (job.status === "failed" && statusEl) {
          statusEl.textContent = job.error || "Install failed";
        }
        if (job.status === "succeeded") {
          if (statusEl) statusEl.textContent = "Succeeded — refreshing…";
          setTimeout(refreshFleetPage, 800);
        }
        return job;
      }
      await new Promise(function (r) {
        setTimeout(r, 1000);
      });
    }
  }

  function renderBootstrapInput(job) {
    var panel = $("#pa-bootstrap-required-input");
    if (!panel) return;
    var required = job && job.required_input;
    panel.hidden = !required;
    if (!required) {
      panel.textContent = "";
      return;
    }
    panel.innerHTML = "<strong>Action required: " +
      escapeHtml(required.kind.replace(/_/g, " ")) + "</strong><p>" +
      escapeHtml(required.prompt) + "</p><pre>" +
      escapeHtml(JSON.stringify(required.details || {}, null, 2)) + "</pre>" +
      "<p class=\"muted small\">Complete this supported action, then resume the durable job. Secret values are submitted through the protected input endpoint and are never persisted.</p>";
  }

  async function pollBootstrapJob(jobId, logEl, statusEl) {
    while (true) {
      var job = await api("/api/fleet/bootstrap-jobs/" + encodeURIComponent(jobId));
      if (logEl) {
        logEl.hidden = false;
        logEl.textContent = job.log || "";
        logEl.scrollTop = logEl.scrollHeight;
      }
      if (statusEl) {
        statusEl.textContent = "Phase: " + String(job.current_phase || "pending").replace(/_/g, " ") +
          " · state: " + String(job.state || "pending").replace(/_/g, " ") +
          " · readiness: " + String(job.readiness || "pending").replace(/_/g, " ");
      }
      renderBootstrapInput(job);
      if (job.terminal || job.state === "waiting_input" || job.state === "retryable") {
        if (job.state === "ready") setTimeout(refreshFleetPage, 800);
        return job;
      }
      await new Promise(function (resolve) { setTimeout(resolve, 1000); });
    }
  }

  document.addEventListener("click", function (e) {
    var discoverButton = e.target.closest("[data-bootstrap-discover]");
    if (discoverButton) {
      e.preventDefault();
      var discoverForm = discoverButton.closest("form");
      var targetInput = discoverForm && discoverForm.elements.target;
      var discoverStatus = $("#pa-fleet-ssh-status");
      if (!targetInput || !targetInput.value.trim()) {
        if (discoverStatus) discoverStatus.textContent = "Enter an OpenSSH target first.";
        return;
      }
      discoverButton.disabled = true;
      if (discoverStatus) discoverStatus.textContent = "Resolving SSH configuration and host key…";
      api("/api/fleet/bootstrap/discover", {
        method: "POST", body: { target: targetInput.value.trim() }
      }).then(function (result) {
        var discovery = result.discovery || {};
        var panel = $("#pa-bootstrap-discovery");
        var details = $("#pa-bootstrap-discovery-details");
        var confirmation = $("#pa-bootstrap-host-key-confirm");
        var fingerprint = discoverForm.elements.host_key_fingerprint;
        if (panel) panel.hidden = false;
        if (details) {
          details.textContent = [
            "Host: " + (discovery.host || ""),
            "User: " + (discovery.user || ""),
            "Port: " + (discovery.port || ""),
            "Host key: " + (discovery.host_key_algorithm || "unavailable"),
            "Fingerprint: " + (discovery.host_key_fingerprint || "unavailable"),
            "Trust state: " + (discovery.host_key_state || "unavailable")
          ].join("\n");
        }
        if (fingerprint) fingerprint.value = discovery.host_key_fingerprint || "";
        if (confirmation) confirmation.hidden = !result.requires_host_key_confirmation;
        if (discoverStatus) {
          discoverStatus.textContent = result.requires_host_key_confirmation
            ? "Verify the exact fingerprint before creating the job."
            : "Target is present in known_hosts. Review the plan and continue.";
        }
      }).catch(function (err) {
        if (discoverStatus) discoverStatus.textContent = err.message;
      }).finally(function () { discoverButton.disabled = false; });
      return;
    }
    var copyButton = e.target.closest("[data-copy-value]");
    if (copyButton) {
      e.preventDefault();
      navigator.clipboard.writeText(copyButton.getAttribute("data-copy-value") || "")
        .then(function () { copyButton.textContent = "Copied"; })
        .catch(function () { copyButton.textContent = "Copy failed"; });
      return;
    }
    var migrateButton = e.target.closest("[data-participation-migrate]");
    if (migrateButton) {
      e.preventDefault();
      var migrationStatus = $("[data-participation-migration-status]");
      var applyMigration = migrateButton.getAttribute("data-apply") === "true";
      migrateButton.disabled = true;
      api("/api/fleet/participation-migration", {
        method: "POST", body: { apply: applyMigration }
      }).then(function (result) {
        if (migrationStatus) {
          migrationStatus.hidden = false;
          migrationStatus.textContent = result.message ||
            (result.changes.length + " migration changes");
        }
        if (applyMigration) return refreshFleetPage();
        migrateButton.setAttribute("data-apply", "true");
        migrateButton.textContent = "Apply migration";
      }).catch(function (err) {
        if (migrationStatus) {
          migrationStatus.hidden = false;
          migrationStatus.textContent = err.message;
        }
      }).finally(function () { migrateButton.disabled = false; });
      return;
    }
    var archiveGroup = e.target.closest("[data-instance-group-archive]");
    if (archiveGroup) {
      e.preventDefault();
      if (!confirm("Archive this worker group? Existing defaults will fail visibly until changed.")) return;
      archiveGroup.disabled = true;
      api("/api/fleet/instance-groups/" + encodeURIComponent(
        archiveGroup.getAttribute("data-instance-group-archive")
      ) + "/archive", { method: "POST", body: {} })
        .then(refreshFleetPage)
        .catch(function (err) { alert(err.message); })
        .finally(function () { archiveGroup.disabled = false; });
      return;
    }
    var topologyNode = e.target.closest("[data-fleet-node]");
    if (topologyNode) {
      renderFleetDetail("node", topologyNode.getAttribute("data-fleet-node"));
      renderFleetTopology(createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem));
      return;
    }
    var topologyEdgeItem = e.target.closest("[data-fleet-edge-item]");
    if (topologyEdgeItem) {
      renderFleetDetail(
        "edge-item",
        topologyEdgeItem.getAttribute("data-fleet-edge-item"),
        topologyEdgeItem.getAttribute("data-fleet-edge-parent")
      );
      renderFleetTopology(createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem));
      return;
    }
    var topologyEdge = e.target.closest("[data-fleet-edge]");
    if (topologyEdge) {
      renderFleetDetail("edge", topologyEdge.getAttribute("data-fleet-edge"));
      renderFleetTopology(createFleetSnapshot(fleetOverview, fleetRefresh, selectedFleetItem));
      return;
    }
    var instanceRetry = e.target.closest("[data-fleet-retry-instance]");
    if (instanceRetry) {
      e.preventDefault();
      loadLiveStatus(true, instanceRetry.getAttribute("data-fleet-retry-instance"));
      return;
    }
    var cliInstallButton = e.target.closest("[data-codex-cli-install]");
    if (cliInstallButton) {
      var cliInstance = cliInstallButton.getAttribute("data-codex-cli-install") || "";
      if (!window.confirm("Install the official @openai/codex CLI on instance " + identityName(cliInstance) + "?")) return;
      cliInstallButton.disabled = true;
      api(codexLoginBase(cliInstance).replace(/\/login-jobs$/, "/codex-cli/install"), {
        method: "POST"
      }).then(function (result) {
        if (!result.ok) throw new Error(result.message || "Codex CLI install failed");
        loadLiveStatus(true, cliInstance || undefined);
      }).catch(function (err) {
        window.alert(err.message);
      }).finally(function () { cliInstallButton.disabled = false; });
      return;
    }
    var resumeButton = e.target.closest("[data-codex-login-resume]");
    if (resumeButton) {
      codexLoginInstance = resumeButton.getAttribute("data-codex-login-resume") || "";
      codexLoginJob = resumeButton.getAttribute("data-login-job") || "";
      codexLoginStartSequence += 1;
      var resumePanel = $("#pa-codex-login-panel");
      var resumeInstance = $("#pa-codex-login-instance");
      var resumeInstructions = $("#pa-codex-login-instructions");
      if (resumePanel) resumePanel.hidden = false;
      if (resumeInstance) resumeInstance.innerHTML = "Target instance: " + identityHtml(codexLoginInstance);
      if (resumeInstructions) resumeInstructions.textContent = "Restoring device authentication…";
      watchCodexLogin(codexLoginInstance, codexLoginJob).catch(function (err) {
        if (resumeInstructions) resumeInstructions.textContent = err.message;
      });
      return;
    }
    var loginButton = e.target.closest("[data-codex-login]");
    if (loginButton) {
      var nextLoginInstance = loginButton.getAttribute("data-codex-login") || "";
      if (codexLoginInstance && codexLoginInstance !== nextLoginInstance) {
        codexLoginJob = "";
        codexLoginStartSequence += 1;
      }
      codexLoginInstance = nextLoginInstance;
      var panel = $("#pa-codex-login-panel");
      var instance = $("#pa-codex-login-instance");
      var instructions = $("#pa-codex-login-instructions");
      if (panel) panel.hidden = false;
      if (instance) instance.innerHTML = "Target instance: " + identityHtml(codexLoginInstance);
      if (instructions) instructions.textContent = "No login has started. Confirm to continue.";
      return;
    }
    if (e.target.closest("#pa-codex-login-confirm")) {
      var confirmButton = $("#pa-codex-login-confirm");
      var loginInstructions = $("#pa-codex-login-instructions");
      if (!codexLoginInstance || !confirmButton) return;
      var startInstance = codexLoginInstance;
      var startSequence = ++codexLoginStartSequence;
      confirmButton.disabled = true;
      if (loginInstructions) loginInstructions.textContent = "Starting device authentication…";
      api(codexLoginBase(startInstance), {
        method: "POST", body: { consent: true, timeout_seconds: 600 }
      }).then(function (job) {
        if (startSequence !== codexLoginStartSequence) {
          return api(codexLoginBase(startInstance) + "/" +
            encodeURIComponent(job.job_id) + "/cancel", { method: "POST" });
        }
        codexLoginJob = job.job_id;
        return watchCodexLogin(startInstance, job.job_id);
      }).catch(function (err) {
        if (err.detail && err.detail.job_id) {
          if (startSequence !== codexLoginStartSequence) return;
          codexLoginJob = err.detail.job_id;
          if (loginInstructions) loginInstructions.textContent =
            "An existing login is active; restoring it…";
          return watchCodexLogin(startInstance, codexLoginJob);
        }
        if (loginInstructions) loginInstructions.textContent = err.message;
      }).finally(function () { confirmButton.disabled = false; });
      return;
    }
    if (e.target.closest("#pa-codex-login-cancel")) {
      var loginPanel = $("#pa-codex-login-panel");
      codexLoginStartSequence += 1;
      if (codexLoginJob && codexLoginInstance) {
        api(codexLoginBase(codexLoginInstance) + "/" + encodeURIComponent(codexLoginJob) + "/cancel", {
          method: "POST"
        }).catch(function () {});
      }
      codexLoginJob = "";
      if (loginPanel) loginPanel.hidden = true;
      return;
    }
    if (e.target.closest("#pa-remote-refresh")) {
      e.preventDefault();
      loadRemoteOperations(true);
      return;
    }

    if (e.target.closest("#pa-sync-refresh")) {
      e.preventDefault();
      startSyncConvergence().catch(function (err) {
        var progress = $("#pa-sync-progress");
        if (progress) progress.textContent = err.message;
      });
      return;
    }

    if (e.target.closest("#pa-sync-converge")) {
      e.preventDefault();
      startSyncConvergence().catch(function (err) {
        var progress = $("#pa-sync-progress");
        if (progress) progress.textContent = err.message;
      });
      return;
    }

    var recoveryLink = e.target.closest("[data-sync-recovery-link]");
    if (recoveryLink) {
      e.preventDefault();
      var syncSectionLink = $('[data-section-link="sync"]');
      if (syncSectionLink) syncSectionLink.click();
      startSyncConvergence().catch(function (err) {
        var progress = $("#pa-sync-progress");
        if (progress) progress.textContent = err.message;
      });
      return;
    }

    if (e.target.closest("[data-sync-retry-dispatch]")) {
      e.preventDefault();
      var operationsLink = $('[data-section-link="operations"]');
      if (operationsLink) operationsLink.click();
      var repairedForm = $("#pa-remote-start-form");
      if (repairedForm) repairedForm.requestSubmit();
      return;
    }

    if (e.target.closest("[data-remote-dispatch-retry]")) {
      e.preventDefault();
      var retryForm = $("#pa-remote-start-form");
      if (retryForm) retryForm.requestSubmit();
      return;
    }

    if (e.target.closest("#pa-remote-notifications")) {
      e.preventDefault();
      enableRemoteNotifications().catch(function (err) {
        var status = $("#pa-remote-status");
        if (status) status.textContent = err.message;
      });
      return;
    }

    var dispatchRetry = e.target.closest("[data-dispatch-retry]");
    if (dispatchRetry) {
      e.preventDefault();
      var retryId = dispatchRetry.getAttribute("data-dispatch-retry");
      dispatchRetry.disabled = true;
      api("/api/fleet/dispatch-jobs/" + encodeURIComponent(retryId) + "/retry", {
        method: "POST", body: {}
      }).then(function () {
        return loadRemoteDispatches(remoteInstanceId);
      }).catch(function (err) {
        var status = $("#pa-remote-status");
        if (status) status.textContent = err.message;
      }).finally(function () {
        if (dispatchRetry.isConnected) dispatchRetry.disabled = false;
      });
      return;
    }

    var dispatchCancel = e.target.closest("[data-dispatch-cancel]");
    if (dispatchCancel) {
      e.preventDefault();
      var cancelId = dispatchCancel.getAttribute("data-dispatch-cancel");
      dispatchCancel.disabled = true;
      api("/api/fleet/dispatch-jobs/" + encodeURIComponent(cancelId) + "/cancel", {
        method: "POST", body: {}
      }).then(function () {
        return loadRemoteDispatches(remoteInstanceId);
      }).catch(function (err) {
        var status = $("#pa-remote-status");
        if (status) status.textContent = err.message;
      }).finally(function () {
        if (dispatchCancel.isConnected) dispatchCancel.disabled = false;
      });
      return;
    }

    var dispatchRecovery = e.target.closest("[data-dispatch-recover-stale]");
    if (dispatchRecovery) {
      e.preventDefault();
      var recoveryId = dispatchRecovery.getAttribute("data-dispatch-recover-stale");
      var expectedState = dispatchRecovery.getAttribute("data-dispatch-state");
      if (!window.confirm("Fail this dispatch only if PA proves its linked session is closed and cannot execute any work?")) return;
      dispatchRecovery.disabled = true;
      api("/api/fleet/dispatch-jobs/" + encodeURIComponent(recoveryId) + "/repair-terminal", {
        method: "POST",
        body: {
          mode: "closed_session_recovery",
          expected_state: expectedState,
          reason: "Operator requested fenced recovery of a stale closed-session dispatch.",
          confirm_no_outcome_inference: true,
        },
      }).then(function () {
        return loadRemoteDispatches(remoteInstanceId);
      }).catch(function (err) {
        var status = $("#pa-remote-status");
        if (status) status.textContent = err.message;
      }).finally(function () {
        if (dispatchRecovery.isConnected) dispatchRecovery.disabled = false;
      });
      return;
    }

    var remoteSession = e.target.closest("[data-remote-session]");
    if (remoteSession) {
      selectRemoteSession(remoteSession.getAttribute("data-remote-session"));
      return;
    }

    var remoteAudit = e.target.closest("[data-remote-audit]");
    if (remoteAudit) {
      showRemoteAudit(remoteAudit.getAttribute("data-remote-audit"));
      return;
    }

    var olderAudit = e.target.closest("[data-remote-audit-older]");
    if (olderAudit) {
      loadOlderRemoteAudit(olderAudit);
      return;
    }

    if (e.target.closest("[data-remote-audit-close]")) {
      var audit = $("#pa-remote-audit");
      if (audit) audit.hidden = true;
      return;
    }

    if (e.target.closest("#pa-fleet-refresh")) {
      e.preventDefault();
      if ($("#pa-fleet-instances")) {
        loadLiveStatus(true);
      } else {
        refreshFleetPage();
      }
      return;
    }

    var pathBtn = e.target.closest("[data-fleet-path]");
    if (pathBtn && $("#pa-fleet-root")) {
      showPanel(pathBtn.getAttribute("data-fleet-path"));
      return;
    }

    if (e.target.closest("[data-fleet-ensure-token]") || e.target.closest("#pa-fleet-ensure-token")) {
      var status = $("#pa-fleet-readiness-status");
      api("/api/fleet/ensure-sync-token", { method: "POST", body: {} })
        .then(function () {
          if (status) status.textContent = "Sync token ready.";
          setTimeout(refreshFleetPage, 400);
        })
        .catch(function (err) {
          if (status) status.textContent = err.message;
        });
      return;
    }

    if (e.target.closest("[data-fleet-fix-bind]")) {
      var bindStatus = $("#pa-fleet-readiness-status");
      if (bindStatus) bindStatus.textContent = "Binding 0.0.0.0 and restarting…";
      api("/api/fleet/readiness", { method: "POST", body: { bind_all: true } })
        .then(function (data) {
          if (bindStatus) {
            bindStatus.textContent = data.restart_started
              ? "Saved. Restarting service so peers can connect…"
              : "Saved bind host 0.0.0.0. Restart PA if peers still cannot connect.";
          }
          setTimeout(refreshFleetPage, data.restart_started ? 2500 : 600);
        })
        .catch(function (err) {
          if (bindStatus) bindStatus.textContent = err.message;
        });
      return;
    }

    if (e.target.closest("#pa-fleet-mint-token")) {
      var out = $("#pa-fleet-token-out");
      api("/api/fleet/join-token", { method: "POST", body: {} })
        .then(function (data) {
          if (out) {
            out.hidden = false;
            out.textContent =
              "Token: " +
              data.token +
              "\nExpires: " +
              data.expires_at +
              "\nOwner: " +
              data.owner_url +
              "\n\n" +
              data.join_command;
          }
        })
        .catch(function (err) {
          if (out) {
            out.hidden = false;
            out.textContent = err.message;
          }
        });
      return;
    }

    var removeBtn = e.target.closest("[data-fleet-remove]");
    if (removeBtn) {
      var id = removeBtn.getAttribute("data-fleet-remove");
      if (!id || !confirm("Remove this instance from the fleet?")) return;
      api("/api/fleet/instances/" + encodeURIComponent(id), { method: "DELETE" })
        .then(refreshFleetPage)
        .catch(function (err) {
          alert(err.message);
        });
      return;
    }

    var updateBtn = e.target.closest("[data-fleet-update]");
    if (updateBtn) {
      var dialog = $("#pa-fleet-update-dialog");
      var updateForm = $("#pa-fleet-update-form");
      if (!dialog || !updateForm) return;
      closeFleetUpdateWatcher();
      fleetUpdateInstanceId = updateBtn.getAttribute("data-fleet-update") || "";
      updateForm.elements.instance_id.value = fleetUpdateInstanceId;
      fleetUpdateName = updateBtn.getAttribute("data-instance-name") || "this instance";
      var row = updateBtn.closest("tr[data-fleet-instance]");
      updateForm.elements.channel.value = (row && row.dataset.updateChannel) || "release";
      var updateLog = $("#pa-fleet-update-log");
      var updateStatus = $("#pa-fleet-update-status");
      var progressWrap = $("#pa-fleet-update-progress-wrap");
      if (updateLog) updateLog.textContent = "";
      if (updateStatus) updateStatus.textContent = "Loading persisted update state…";
      if (progressWrap) progressWrap.hidden = true;
      var submit = updateForm.querySelector("button[type=submit]");
      if (submit) submit.disabled = false;
      if (typeof dialog.showModal === "function") dialog.showModal();
      else dialog.setAttribute("open", "");
      Promise.all([restoreFleetUpdate(fleetUpdateInstanceId), refreshFleetUpdateCheck()]).catch(function (err) {
        var confirmText = $("#pa-fleet-update-confirm");
        if (confirmText) confirmText.textContent = err.message;
      });
      return;
    }

    if (e.target.closest("[data-fleet-update-cancel]")) {
      closeFleetUpdateWatcher();
      var updateDialog = $("#pa-fleet-update-dialog");
      if (updateDialog && updateDialog.open) updateDialog.close();
      return;
    }

    var inviteBtn = e.target.closest("[data-fleet-invite]");
    if (inviteBtn) {
      var realmId = inviteBtn.getAttribute("data-fleet-invite");
      var inviteOut = $("#pa-fleet-invite-out");
      api("/api/realms/invite", {
        method: "POST",
        body: { realm_id: realmId, role: "editor" },
      })
        .then(function (data) {
          if (inviteOut) {
            inviteOut.hidden = false;
            inviteOut.textContent =
              "Realm invite for " +
              data.realm_id +
              " (" +
              data.role +
              ")\nToken: " +
              data.token +
              (data.expires_at ? "\nExpires: " + data.expires_at : "");
          }
        })
        .catch(function (err) {
          if (inviteOut) {
            inviteOut.hidden = false;
            inviteOut.textContent = err.message;
          }
        });
    }
  });

  document.addEventListener("change", function (e) {
    if (!e.target.matches("#pa-fleet-update-form [name=channel]")) return;
    refreshFleetUpdateCheck().catch(function (err) {
      var confirmText = $("#pa-fleet-update-confirm");
      if (confirmText) confirmText.textContent = err.message;
    });
  });

  document.addEventListener("submit", function (e) {
    var form = e.target;
    if (!form) return;
    // Allow forms identified by id or data-fleet-fix (inline readiness fixes).
    if (!form.id && !form.getAttribute("data-fleet-fix") &&
        !form.matches("[data-participation-policy-form], [data-instance-group-create], " +
          "[data-instance-group-edit], [data-placement-default-form]")) return;

    if (form.matches("[data-participation-policy-form]")) {
      e.preventDefault();
      var policyStatus = $("[data-participation-policy-status]", form);
      var policyBody = formToObject(form);
      policyBody.allowed_profiles = $all('[name="allowed_profiles"]:checked', form)
        .map(function (item) { return item.value; });
      policyBody.denied_profiles = $all('[name="denied_profiles"]:checked', form)
        .map(function (item) { return item.value; });
      [
        "allowed_project_ids", "denied_project_ids",
        "allowed_repository_ids", "denied_repository_ids",
        "allowed_provider_ids", "allowed_model_families"
      ].forEach(function (name) { policyBody[name] = commaList(policyBody[name]); });
      policyBody.confirm_enable = !!form.elements.confirm_enable.checked;
      if (!policyBody.confirmation_reason) delete policyBody.confirmation_reason;
      api("/api/fleet/instances/" + encodeURIComponent(
        form.getAttribute("data-instance-id")
      ) + "/participation-policy", { method: "PUT", body: policyBody })
        .then(function (result) {
          if (policyStatus) policyStatus.textContent =
            "Saved version " + result.version + " — " + result.summary;
          return refreshFleetPage();
        })
        .catch(function (err) {
          if (policyStatus) policyStatus.textContent = err.message;
        });
      return;
    }

    if (form.matches("[data-instance-group-create]")) {
      e.preventDefault();
      var createStatus = $("[data-instance-group-status]", form);
      var createBody = formToObject(form);
      createBody.included_instance_ids = commaList(createBody.included_instance_ids);
      createBody.excluded_instance_ids = commaList(createBody.excluded_instance_ids);
      api("/api/fleet/instance-groups", { method: "POST", body: createBody })
        .then(refreshFleetPage)
        .catch(function (err) {
          if (createStatus) createStatus.textContent = err.message;
        });
      return;
    }

    if (form.matches("[data-instance-group-edit]")) {
      e.preventDefault();
      var editBody = formToObject(form);
      editBody.included_instance_ids = commaList(editBody.included_instance_ids);
      editBody.excluded_instance_ids = commaList(editBody.excluded_instance_ids);
      editBody.expected_version = Number(form.getAttribute("data-group-version"));
      api("/api/fleet/instance-groups/" + encodeURIComponent(
        form.getAttribute("data-group-id")
      ), { method: "PATCH", body: editBody })
        .then(refreshFleetPage)
        .catch(function (err) { alert(err.message); });
      return;
    }

    if (form.matches("[data-placement-default-form]")) {
      e.preventDefault();
      var defaultStatus = $("[data-placement-default-status]");
      var defaultBody = formToObject(form);
      if (!defaultBody.project_id) delete defaultBody.project_id;
      if (!defaultBody.workload_profile) delete defaultBody.workload_profile;
      api("/api/fleet/placement-defaults", { method: "PUT", body: defaultBody })
        .then(function (result) {
          if (defaultStatus) defaultStatus.textContent =
            "Saved default version " + result.version + ".";
          return refreshFleetPage();
        })
        .catch(function (err) {
          if (defaultStatus) defaultStatus.textContent = err.message;
        });
      return;
    }

    if (form.id === "pa-fleet-update-form") {
      e.preventDefault();
      var updateBody = formToObject(form);
      var updateInstanceId = updateBody.instance_id;
      delete updateBody.instance_id;
      updateBody.quiesce_timeout = parseFloat(updateBody.quiesce_timeout || "300");
      updateBody.install_timeout = parseFloat(updateBody.install_timeout || "900");
      updateBody.force = !!form.elements.force.checked;
      if (!updateBody.target_version) delete updateBody.target_version;
      var updateStatus = $("#pa-fleet-update-status");
      var updateLog = $("#pa-fleet-update-log");
      if (updateStatus) updateStatus.textContent = "Rechecking selected channel…";
      if (updateLog) updateLog.textContent = "";
      refreshFleetUpdateCheck().then(function () {
        if (updateStatus) updateStatus.textContent = "Starting persistent update job…";
        return api("/api/fleet/instances/" + encodeURIComponent(updateInstanceId) + "/update", {
          method: "POST",
          body: updateBody,
        });
      }).then(function (job) {
        renderFleetUpdateJob(job);
        watchFleetUpdate(updateInstanceId, job.job_id);
      }).catch(function (err) {
        if (updateStatus) updateStatus.textContent = err.message;
      });
      return;
    }

    if (form.id === "pa-remote-start-form") {
      e.preventDefault();
      var remoteStatus = $("#pa-remote-status");
      if (!remoteInstanceId) {
        if (remoteStatus) remoteStatus.textContent = "Choose a remote instance first.";
        return;
      }
      var dispatchInstanceId = remoteInstanceId;
      var remoteBody = formToObject(form);
      var cardSelect = form.elements.card_id;
      if (cardSelect && cardSelect.selectedOptions.length) {
        var projectId = cardSelect.selectedOptions[0].getAttribute("data-project-id");
        if (projectId) remoteBody.project_id = projectId;
      }
      Object.keys(remoteBody).forEach(function (key) {
        if (remoteBody[key] === "") delete remoteBody[key];
      });
      var admissionSlot = "pa-remote-dispatch-admission:" + dispatchInstanceId + ":" +
        (remoteBody.card_id || "standalone") + ":" + (remoteBody.resume_session_id || "fresh");
      var serializedBody = JSON.stringify(remoteBody);
      var admission = null;
      try { admission = JSON.parse(localStorage.getItem(admissionSlot) || "null"); } catch (err) {}
      if (!admission || admission.body !== serializedBody || !admission.key) {
        admission = {
          key: window.crypto && window.crypto.randomUUID
            ? window.crypto.randomUUID()
            : String(Date.now()) + "-" + Math.random().toString(16).slice(2),
          body: serializedBody,
        };
        try { localStorage.setItem(admissionSlot, JSON.stringify(admission)); } catch (err) {}
      }
      var submit = form.querySelector("[data-remote-start]");
      if (submit) submit.disabled = true;
      if (remoteStatus) remoteStatus.textContent = "Queueing durable remote dispatch…";
      api(remoteApiBase(dispatchInstanceId) + "/start", {
        method: "POST",
        body: remoteBody,
        headers: Object.assign(csrfHeaders(), { "Idempotency-Key": admission.key }),
      }).then(function (result) {
        if (!result.dispatch_id) throw new Error("PA did not return a durable dispatch id.");
        try { localStorage.removeItem(admissionSlot); } catch (err) {}
        if (
          dispatchInstanceId !== remoteInstanceId ||
          !form.isConnected
        ) return;
        if (remoteStatus) {
          remoteStatus.textContent = (result.duplicate ? "Recovered" : "Queued") +
            " durable dispatch " + result.dispatch_id + ". Other Fleet controls remain available.";
        }
        loadRemoteDispatches(dispatchInstanceId).catch(function () {});
      }).catch(function (err) {
        if (
          dispatchInstanceId === remoteInstanceId &&
          form.isConnected &&
          remoteStatus
        ) {
          if (err.detail && err.detail.recovery_url) {
            remoteStatus.innerHTML = escapeHtml(err.detail.message || err.message) +
              ' <a href="' + escapeHtml(err.detail.recovery_url) +
              '" data-sync-recovery-link>Open realm sync recovery</a> ' +
              '<button type="button" class="ghost small" data-remote-dispatch-retry>' +
              "Retry dispatch</button>";
          } else {
            remoteStatus.textContent = err.message;
          }
        }
      }).finally(function () {
        if (submit && submit.isConnected) submit.disabled = false;
      });
      return;
    }

    if (form.id === "pa-sync-resolution-form") {
      e.preventDefault();
      if (!syncCurrentConflicts.length) return;
      var grouped = {};
      try {
        syncCurrentConflicts.forEach(function (item, index) {
          var checked = form.querySelector('input[name="sync-choice-' + index + '"]:checked');
          var choice = checked ? checked.value : "local";
          var source = choice === "remote" ? item.remote : item.local;
          var key = item.entity + ":" + item.id;
          var resolution = grouped[key] || {
            entity: item.entity, id: item.id, action: "update", fields: {}
          };
          if (item.field === "__terminal__") {
            if (source.value === "card_deleted") {
              resolution.action = "delete";
              resolution.fields = {};
            } else if (source.value === "project_archived") {
              resolution.action = "archive";
              resolution.fields = {};
            } else {
              if (!source.snapshot) throw new Error("The selected history has no restorable entity snapshot.");
              resolution.action = "upsert";
              resolution.fields = source.snapshot;
            }
          } else {
            var value = source.value;
            if (choice === "custom") {
              var custom = form.querySelector('[data-sync-custom="' + index + '"]');
              var raw = custom ? custom.value : "";
              try { value = JSON.parse(raw); } catch (parseError) { value = raw; }
            }
            resolution.fields[item.field] = value;
          }
          grouped[key] = resolution;
        });
      } catch (buildError) {
        var resolutionProgress = $("#pa-sync-progress");
        if (resolutionProgress) resolutionProgress.textContent = buildError.message;
        return;
      }
      var first = syncCurrentConflicts[0];
      var submitResolution = form.querySelector('button[type="submit"]');
      if (submitResolution) submitResolution.disabled = true;
      var syncProgress = $("#pa-sync-progress");
      if (syncProgress) syncProgress.textContent = "Recording an immutable merge decision…";
      api("/api/sync/conflicts/resolve", {
        method: "POST",
        body: {
          realm_id: syncRealm(),
          remote_head: first.remote_head,
          resolutions: Object.keys(grouped).map(function (key) { return grouped[key]; }),
        },
      }).then(function (result) {
        renderSyncState(result.convergence || {});
        return loadSyncStatus(false);
      }).catch(function (err) {
        if (syncProgress) syncProgress.textContent = err.message;
      }).finally(function () {
        if (submitResolution && submitResolution.isConnected) submitResolution.disabled = false;
      });
      return;
    }

    if (form.id === "pa-fleet-ssh-form") {
      e.preventDefault();
      var logEl = $("#pa-fleet-ssh-log");
      var statusEl = $("#pa-fleet-ssh-status");
      var body = formToObject(form);
      var providers = Array.prototype.map.call(
        form.querySelectorAll('input[name="providers"]:checked'),
        function (input) { return input.value; }
      );
      var repositories = String(body.repositories || "").split(/\r?\n/)
        .map(function (value) { return value.trim(); }).filter(Boolean);
      var requiresConfirmation = !$("#pa-bootstrap-host-key-confirm").hidden;
      if (requiresConfirmation && !form.elements.confirm_host_key.checked) {
        if (statusEl) statusEl.textContent = "Confirm the exact discovered host-key fingerprint first.";
        return;
      }
      var bootstrapRequest = {
        target: body.target,
        identity_file: body.identity_file || "",
        instance_name: body.instance_name,
        instance_url: body.instance_url,
        channel: body.channel,
        realm: body.realm,
        existing_install_action: body.existing_install_action,
        worker_profile: body.worker_profile,
        dispatch_capacity: parseInt(body.dispatch_capacity || "1", 10),
        automatic_placement: !!form.elements.automatic_placement.checked,
        providers: providers,
        repositories: repositories,
        browser: !!form.elements.browser.checked,
        repository_cache: !!form.elements.repository_cache.checked,
        github_transport: body.github_transport,
        smoke_dispatch: !!body.smoke_card_id,
        smoke_card_id: body.smoke_card_id || "",
        sudo_policy: body.sudo_policy,
        password: body.password || "",
        passphrase: body.passphrase || ""
      };
      if (requiresConfirmation) {
        bootstrapRequest.host_key_policy = "pinned";
        bootstrapRequest.host_key_fingerprint = body.host_key_fingerprint;
      }
      clearSecrets(form);
      if (statusEl) statusEl.textContent = "Creating durable plan…";
      api("/api/fleet/bootstrap-jobs", {
        method: "POST",
        body: {
          idempotency_key: "fleet-ui-" + (
            window.crypto && crypto.randomUUID
              ? crypto.randomUUID()
              : String(Date.now()) + "-" + Math.random().toString(16).slice(2)
          ),
          start: true,
          request: bootstrapRequest
        }
      })
        .then(function (job) {
          return pollBootstrapJob(job.job_id, logEl, statusEl);
        })
        .catch(function (err) {
          if (statusEl) statusEl.textContent = err.message;
        });
      return;
    }

    if (form.id === "pa-fleet-ssh-join-form") {
      e.preventDefault();
      var joinLog = $("#pa-fleet-ssh-join-log");
      var joinBody = formToObject(form);
      joinBody.port = parseInt(joinBody.port || "22", 10);
      joinBody.join_only = true;
      clearSecrets(form);
      api("/api/fleet/install-remote", { method: "POST", body: joinBody })
        .then(function (job) {
          return pollJob(job.job_id, joinLog, null);
        })
        .catch(function (err) {
          if (joinLog) {
            joinLog.hidden = false;
            joinLog.textContent = err.message;
          }
        });
      return;
    }

    if (form.id === "pa-fleet-register-form") {
      e.preventDefault();
      var regStatus = $("#pa-fleet-register-status");
      var regBody = formToObject(form);
      if (!regBody.instance_id) delete regBody.instance_id;
      api("/api/fleet/register-remote", { method: "POST", body: regBody })
        .then(function (data) {
          if (regStatus) regStatus.textContent = "Registered " + data.name;
          setTimeout(refreshFleetPage, 500);
        })
        .catch(function (err) {
          if (regStatus) regStatus.textContent = err.message;
        });
      return;
    }

    if (
      form.id === "pa-fleet-readiness-form" ||
      form.id === "pa-fleet-fix-instance-url" ||
      form.getAttribute("data-fleet-fix") === "instance_url"
    ) {
      e.preventDefault();
      var readyStatus = $("#pa-fleet-readiness-status");
      var readyBody = formToObject(form);
      if (
        form.id === "pa-fleet-fix-instance-url" ||
        form.getAttribute("data-fleet-fix") === "instance_url"
      ) {
        readyBody = { instance_url: readyBody.instance_url || "" };
      }
      if (readyStatus) readyStatus.textContent = "Saving…";
      api("/api/fleet/readiness", { method: "POST", body: readyBody })
        .then(function (data) {
          var msg = "Saved.";
          if (data.restart_started) {
            msg = "Saved. Restarting service so the new bind address takes effect…";
          } else if (data.restart_required) {
            msg = "Saved. Restart PA (pa restart) for the bind change to take effect.";
          }
          if (readyStatus) readyStatus.textContent = msg;
          setTimeout(refreshFleetPage, data.restart_started ? 2500 : 600);
        })
        .catch(function (err) {
          if (readyStatus) readyStatus.textContent = err.message;
        });
    }
  });

  window.PAFleetOverview = {
    createSnapshot: createFleetSnapshot,
    beginRefresh: beginFleetRefresh,
    applyDimensionUpdate: applyFleetDimensionUpdate,
    replaceDimension: replaceFleetDimension,
    mergeMetadata: mergeFleetOverviewMetadata,
    worstFreshness: worstFreshness,
    requiredReadiness: requiredReadiness,
    observationAttempt: observationAttempt,
    syncStatusLabel: syncStatusLabel,
    mergeFieldAttemptFailure: mergeFieldAttemptFailure,
    providerAuthState: providerAuthState,
    providerBadgeClass: providerBadgeClass,
    capacityPresentation: fleetCapacityPresentation
  };
})();
