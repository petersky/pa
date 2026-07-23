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

  function clearSecrets(form) {
    ["password", "passphrase"].forEach(function (name) {
      var el = form.elements[name];
      if (el) el.value = "";
    });
  }

  function refreshFleetPage() {
    var section = "";
    try {
      section = new URL(window.location.href).searchParams.get("section") || "";
    } catch (e) {}
    var url = "/fleet" + (section ? "?section=" + encodeURIComponent(section) : "");
    if (window.htmx) {
      htmx.ajax("GET", url, { target: "#app-view", swap: "innerHTML", pushUrl: true });
    } else {
      location.href = url;
    }
  }

  function escapeHtml(text) {
    return String(text == null ? "" : text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  var remoteInstanceId = "";
  var remoteWatchers = {};
  var remoteLoadGeneration = 0;
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
        return "<tr><td><strong>" + escapeHtml(item.name || item.instance_id) +
          "</strong>" + (item.url ? '<br><span class="muted small">' +
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
      escapeHtml(peer.name || peer.instance_id || "peer") +
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
            escapeHtml(itemPeer.name || itemPeer.instance_id || shortHead(head)) +
            " · " + escapeHtml(shortHead(head)) + "</option>";
        }).join("") + "</select></label>";
    }
    fields.innerHTML = queue + syncCurrentConflicts.map(function (item, index) {
      var local = item.local || {};
      var remote = item.remote || {};
      var localLabel = (local.instance_name || local.instance_id || "local") +
        ": " + displayValue(local.value);
      var remoteLabel = (remote.instance_name || remote.instance_id ||
        (item.peer && item.peer.name) || "peer") + ": " + displayValue(remote.value);
      var title = item.entity + " " + item.id + " · " +
        (item.field === "__terminal__" ? "delete/archive vs edit" : item.field);
      return '<fieldset class="panel-inset" data-sync-conflict="' + index + '">' +
        "<legend><strong>" + escapeHtml(title) + "</strong></legend>" +
        '<label><input type="radio" name="sync-choice-' + index +
        '" value="local" checked> ' + escapeHtml(localLabel) + "</label>" +
        '<label><input type="radio" name="sync-choice-' + index +
        '" value="remote"> ' + escapeHtml(remoteLabel) + "</label>" +
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

  function clearRemoteWatchers() {
    Object.keys(remoteWatchers).forEach(function (key) {
      try { remoteWatchers[key].close(); } catch (e) {}
      delete remoteWatchers[key];
    });
    clearTimeout(remoteDispatchTimer);
    remoteDispatchTimer = null;
  }

  function handleRemoteOperationsHidden() {
    remoteLoadGeneration += 1;
    remoteAuditGeneration += 1;
    // Opted-in notifications intentionally outlive the Fleet view. Without
    // that explicit permission, navigation owns and closes every watcher.
    if (remoteNotificationsActive()) {
      if (remoteInstanceId) refreshRemoteWatchers(remoteInstanceId);
      return;
    }
    remoteInstanceId = "";
    clearRemoteWatchers();
  }

  function scheduleRemoteSessionRefresh(instanceId) {
    setTimeout(function () {
      if (instanceId !== remoteInstanceId) return;
      if ($("#pa-remote-instance")) {
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
        $("#pa-remote-instance")
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
      if (!session || !session.id) return;
      // Sequence cursors prevent historical completion/error events from being
      // replayed as fresh notifications when opening a peer running older PA.
      if (typeof session.last_seq !== "number") return;
      var key = instanceId + ":" + session.id;
      desired[key] = true;
      if (remoteWatchers[key]) return;
      var url = remoteApiBase(instanceId) + "/sessions/" + encodeURIComponent(session.id) +
        "/events?after=" + encodeURIComponent(session.last_seq || 0);
      var source = new EventSource(url);
      ["turn_completed", "permission_request", "error", "connection_lost"].forEach(function (type) {
        source.addEventListener(type, function (event) {
          var data = {};
          try { data = JSON.parse(event.data || "{}"); } catch (e) {}
          notifyRemoteSession(session, type, data.payload || {});
          if (type !== "permission_request") scheduleRemoteSessionRefresh(instanceId);
        });
      });
      remoteWatchers[key] = source;
    });
    Object.keys(remoteWatchers).forEach(function (key) {
      if (desired[key]) return;
      try { remoteWatchers[key].close(); } catch (e) {}
      delete remoteWatchers[key];
    });
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
        (session.prompting ? "active" : "open") + '">' + escapeHtml(state) + "</span></button></li>";
    }).join("");
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
      var state = dispatch.state || "queued";
      var terminal = state === "failed" || state === "completed" || state === "cancelled";
      var badge = state === "failed" ? "blocked" : state === "completed" ? "active" : "open";
      var latest = dispatch.events && dispatch.events.length
        ? dispatch.events[dispatch.events.length - 1].message : "";
      var error = dispatch.last_error
        ? '<p class="status status-blocked small">' + escapeHtml(dispatch.last_error) + "</p>" : "";
      var outbox = dispatch.completion_outbox || {};
      var outboxText = state === "completion_pending"
        ? '<p class="muted small">Completion outbox attempt ' + escapeHtml(outbox.attempts || 0) +
          (outbox.last_error ? " · " + escapeHtml(outbox.last_error) : "") + "</p>" : "";
      var turn = dispatch.agent_turn || {};
      var transport = dispatch.dispatch_completion || {};
      var card = dispatch.card_completion || {};
      var lifecycle = '<p class="muted small">Agent turn: ' +
        escapeHtml(turn.completed ? "completed" : "in progress") +
        (turn.stop_reason ? " (" + escapeHtml(turn.stop_reason) + ")" : "") +
        ' · Dispatch: ' + escapeHtml(transport.completed ? "completed" : "in progress") + "</p>";
      var cardText = "";
      if (dispatch.card_id && card.status && card.status !== "not_requested") {
        cardText = '<p class="muted small">Card: ' +
          escapeHtml(card.lane_after || card.lane_before || "unchanged") +
          ' · Disposition: ' + escapeHtml(card.status) +
          (card.reason ? " · " + escapeHtml(card.reason) : "") + "</p>";
      }
      var actions = '<span class="form-actions">';
      if (dispatch.can_retry) actions += '<button type="button" class="ghost small" data-dispatch-retry="' +
        escapeHtml(dispatch.dispatch_id) + '">Retry</button>';
      if (dispatch.can_cancel) actions += '<button type="button" class="ghost small" data-dispatch-cancel="' +
        escapeHtml(dispatch.dispatch_id) + '">Cancel</button>';
      if (dispatch.session_id) actions += '<button type="button" class="ghost small" data-remote-session="' +
        escapeHtml(dispatch.session_id) + '">Open session</button>';
      actions += "</span>";
      return '<li data-dispatch-id="' + escapeHtml(dispatch.dispatch_id) + '"><div class="panel-header"><div>' +
        '<strong>' + escapeHtml(dispatch.card_id ? "Card dispatch" : "Remote session") + '</strong> ' +
        '<span class="status status-' + badge + '">' + escapeHtml(remoteDispatchStageLabel(state)) + "</span>" +
        '<p class="muted small"><code>' + escapeHtml(dispatch.dispatch_id) + "</code>" +
        (latest ? " · " + escapeHtml(latest) : "") + "</p></div>" + actions + "</div>" +
        error + lifecycle + cardText + outboxText +
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
      } else if (target.state === "completion_pending" || target.state === "completed") {
        authority.state = target.state;
        authority.last_error = target.last_error;
        authority.completion_outbox = target.completion_outbox;
        authority.updated_at = target.updated_at;
      }
    });
    var rows = Object.keys(merged).map(function (key) { return merged[key]; });
    rows.sort(function (a, b) { return String(b.updated_at).localeCompare(String(a.updated_at)); });
    renderRemoteDispatches(rows);
    var active = rows.some(function (item) {
      return ["queued", "checking_sync", "materializing", "starting_session",
        "delivering_prompt", "running", "completion_pending"].indexOf(item.state) >= 0;
    });
    if (active) remoteDispatchTimer = setTimeout(function () {
      loadRemoteDispatches(instanceId).catch(function () {});
    }, 1000);
  }

  async function loadRemoteProviders(instanceId, generation) {
    var select = $("[data-remote-provider]");
    if (!select) return;
    var providers = await api(remoteApiBase(instanceId) + "/providers");
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
      if (!provider || !provider.id || provider.available === false) return;
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

  async function loadRemoteOperations() {
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
      clearRemoteWatchers();
      return;
    }
    if (status) status.textContent = "Loading remote sessions…";
    loadRemoteDispatches(instanceId).catch(function () {});
    try {
      var base = remoteApiBase(instanceId);
      var warnings = [];
      var results = await Promise.all([
        api(base + "/sessions"),
        api(base + "/history").catch(function () {
          warnings.push("Audit history requires a newer PA on the peer.");
          return [];
        }),
        loadRemoteProviders(instanceId, generation).catch(function () {
          warnings.push("Provider discovery is unavailable.");
          return null;
        }),
      ]);
      if (
        generation !== remoteLoadGeneration ||
        instanceId !== remoteInstanceId ||
        !instanceSelect.isConnected
      ) return;
      var sessions = results[0] || [];
      renderRemoteSessions(sessions);
      renderRemoteHistory(results[1] || [], sessions);
      watchRemoteSessions(instanceId, sessions);
      if (status) {
        status.textContent = sessions.length + " live session" + (sessions.length === 1 ? "" : "s") +
          " on the selected instance." + (warnings.length ? " " + warnings.join(" ") : "");
      }
    } catch (err) {
      if (
        generation !== remoteLoadGeneration ||
        instanceId !== remoteInstanceId ||
        !instanceSelect.isConnected
      ) return;
      clearRemoteWatchers();
      if (status) status.textContent = err.message;
    }
  }

  function selectRemoteSession(sessionId) {
    if (!remoteInstanceId || !sessionId) return;
    var chat = $("#pa-remote-chat");
    var audit = $("#pa-remote-audit");
    var widgetRoot = $("#pa-remote-chat [data-agent-chat]");
    if (!chat || !widgetRoot || !window.PAAgentChat) return;
    window.PAAgentChat.mount(chat);
    if (!widgetRoot._acw) return;
    widgetRoot._acw.setApiBase(remoteApiBase(remoteInstanceId));
    if (audit) audit.hidden = true;
    chat.hidden = false;
    widgetRoot._acw.switchSession(sessionId);
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
          '<p class="muted small">' + escapeHtml((data.instance && data.instance.name) || instanceId) +
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
    if (!select) {
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
      clearRemoteWatchers();
    }
    remoteInstanceId = nextInstanceId;
    if (remoteInstanceId) loadRemoteOperations();
    else clearRemoteWatchers();
  }

  function providersHtml(providers, instanceId) {
    if (!providers || !providers.length) {
      return '<span class="muted">—</span>';
    }
    return providers
      .map(function (p) {
        var label = escapeHtml(p.display_name || p.id || "?");
        var mark = p.available ? " ✓" : " ·";
        var title = escapeHtml(p.id || "");
        var install = p.installed ? "installed" : "not installed";
        var auth = p.auth_configured ? (p.auth_method || "authenticated") :
          (p.auth_method === "unknown" ? "auth unknown" : "not signed in");
        var probe = p.last_probe && p.last_probe.ok ? "probe ready" : "not probed";
        var detail = escapeHtml(install + " · " + auth + " · " + probe +
          (p.auth_status ? " · " + p.auth_status : ""));
        var login = "";
        if (p.id === "codex" && p.login_in_progress) {
          var activeJob = p.meta && p.meta.active_login_job_id;
          login = activeJob ?
            ' <button type="button" class="ghost small" data-codex-login-resume="' +
              escapeHtml(instanceId || "") + '" data-login-job="' +
              escapeHtml(activeJob) + '">Resume sign-in</button>' :
            ' <span class="muted small">login in progress</span>';
        } else if (p.id === "codex" && p.codex_cli_installed && !p.auth_configured) {
          login = ' <button type="button" class="ghost small" data-codex-login="' +
            escapeHtml(instanceId || "") + '">Sign in with ChatGPT</button>';
        } else if (p.id === "codex" && !p.codex_cli_installed) {
          login = ' <button type="button" class="ghost small" data-codex-cli-install="' +
            escapeHtml(instanceId || "") + '">Install Codex CLI</button>';
        }
        return '<span class="badge" title="' + detail + ' (' + title + ')">' +
          label + mark + " · " + escapeHtml(auth) + "</span>" + login;
      })
      .join(" ");
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
        loadLiveStatus();
        if (job.state === "succeeded") setTimeout(loadLiveStatus, 1000);
        return;
      }
      await new Promise(function (resolve) { setTimeout(resolve, 1000); });
    }
  }

  var fleetOverview = null;
  var selectedFleetItem = null;

  function readFleetOverview() {
    var source = $("#pa-fleet-overview-data");
    if (!source) return null;
    try {
      return JSON.parse(source.textContent || "{}");
    } catch (err) {
      setLiveBanner("Cached overview could not be decoded · Use Refresh to retry.");
      return null;
    }
  }

  function fieldValue(node, name) {
    return node && node.dimensions && node.dimensions[name]
      ? node.dimensions[name]
      : { state: "unavailable", value: null, observed_at: null, error: null };
  }

  function worstFreshness(node) {
    var order = { error: 5, timeout: 4, unavailable: 3, stale: 2, fresh: 1 };
    var worst = "fresh";
    Object.keys(node.dimensions || {}).forEach(function (name) {
      var item = node.dimensions[name] || {};
      if ((order[item.state] || 5) > (order[worst] || 1)) worst = item.state || "error";
    });
    return worst;
  }

  function activityLabel(value) {
    value = value || {};
    var count = (value.sessions || []).length + (value.dispatches || []).length;
    var state = value.state || "unavailable";
    return count > 1 ? state + " +" + (count - 1) : state;
  }

  function setFieldState(element, state) {
    if (!element) return;
    ["fresh", "stale", "timeout", "error", "unavailable"].forEach(function (name) {
      element.classList.toggle("fleet-field-" + name, state === name);
    });
  }

  function renderFleetRow(node) {
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
        ? (sync.value.consistent ? "in sync" : "head mismatch")
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
      activityEl.innerHTML = "<strong>" + escapeHtml(activityLabel(activityValue)) +
        '</strong><span class="muted small">' +
        escapeHtml(activityValue.summary || "No activity detail yet") + "</span>";
      setFieldState(activityEl, activity.state);
    }
    var freshnessEl = $("[data-fleet-freshness]", tr);
    if (freshnessEl) {
      var freshness = worstFreshness(node);
      var refreshing = Object.keys(node.dimensions || {}).some(function (name) {
        return node.dimensions[name].refreshing;
      });
      var observed = reach.observed_at || status.observed_at;
      freshnessEl.innerHTML = '<span class="fleet-freshness fleet-field-' +
        escapeHtml(freshness) + '">' + escapeHtml(refreshing ? freshness + " · refreshing" : freshness) +
        "</span>" + (observed ? '<time class="muted small" datetime="' +
          escapeHtml(observed) + '">' + escapeHtml(new Date(observed).toLocaleString()) + "</time>" : "");
    }
    var statusValue = status.value || {};
    var updateValue = update.value || {};
    tr.dataset.updateChannel = updateValue.channel || statusValue.release_track || "release";
    tr.dataset.currentVersion = statusValue.version || "";
    tr.dataset.availableVersion = updateValue.available_version || "";
  }

  function nodeById(id) {
    return (fleetOverview && fleetOverview.nodes || []).find(function (node) {
      return node.id === id;
    });
  }

  function edgeById(id) {
    return (fleetOverview && fleetOverview.edges || []).find(function (edge) {
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

  function edgeVisualStatus(edge) {
    var target = edge && edge.target && nodeById(edge.target);
    if (!target) return edge && edge.status || "unavailable";
    var reach = fieldValue(target, "reachability");
    var health = reach.value && reach.value.health;
    if (health && health !== "up") {
      return health === "unknown" ? "unavailable" : "degraded";
    }
    if (reach.state === "error" || reach.state === "timeout") return "degraded";
    if (reach.state === "unavailable") return "unavailable";
    if (worstFreshness(target) === "stale") return "stale";
    return edge.status || "healthy";
  }

  function renderFleetEdgeList() {
    var list = $("#pa-fleet-edge-list");
    if (!list || !fleetOverview) return;
    var edges = fleetOverview.edges || [];
    list.innerHTML = edges.length ? edges.map(function (edge) {
      var status = edgeVisualStatus(edge);
      return '<li><button type="button" class="link-button" data-fleet-edge="' +
        escapeHtml(edge.id) + '">' + escapeHtml(edge.kind + ": " +
          (edge.source || "external") + " → " + (edge.target || "external") +
          " · " + (edge.label || edge.id) + " · " + status) + "</button></li>";
    }).join("") : '<li class="muted">No registered routes.</li>';
  }

  function renderFleetDetail(kind, id, edgeId) {
    var panel = $("#pa-fleet-detail");
    if (!panel || !fleetOverview) return;
    if (kind === "edge-item") {
      var parent = edgeById(edgeId);
      var item = edgeItemById(parent, id);
      if (!parent || !item) {
        if (parent) renderFleetDetail("edge", parent.id);
        return;
      }
      selectedFleetItem = { kind: kind, id: id, edgeId: edgeId };
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
      var edge = edgeById(id);
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
        escapeHtml((edge.source || "external") + " → " + (edge.target || "external")) +
        "</dd><dt>Status</dt><dd>" + escapeHtml(edgeVisualStatus(edge)) +
        "</dd><dt>Activities</dt><dd>" + escapeHtml(edge.count || 1) + "</dd>" +
        distinct + (statusSummary ? "<dt>Status counts</dt><dd>" +
          escapeHtml(statusSummary) + "</dd>" : "") + "</dl>" + itemList +
        "<details><summary>Exact grouped detail</summary><pre>" +
        escapeHtml(JSON.stringify(edge.details || {}, null, 2)) + "</pre></details>";
      return;
    }
    var node = nodeById(id);
    if (!node) return;
    selectedFleetItem = { kind: kind, id: id };
    var sections = Object.keys(node.dimensions || {}).map(function (name) {
      var item = node.dimensions[name] || {};
      var timing = item.duration_ms == null ? "" : " · " + item.duration_ms + " ms";
      var error = item.error
        ? '<p class="fleet-diagnostic-error">Action: retry ' + escapeHtml(name) +
          " after checking the peer route. " + escapeHtml(item.error) + "</p>"
        : "";
      return '<details><summary><strong>' + escapeHtml(name) + "</strong> · " +
        escapeHtml(item.state || "unavailable") + escapeHtml(timing) +
        '</summary><p class="muted small">' +
        escapeHtml(item.observed_at ? new Date(item.observed_at).toLocaleString() : "Never observed") +
        "</p>" + error + "<pre>" + escapeHtml(JSON.stringify(item.value, null, 2)) +
        "</pre></details>";
    }).join("");
    panel.innerHTML = "<h3>" + escapeHtml(node.name) + "</h3><p>" +
      escapeHtml(activityLabel(fieldValue(node, "activity").value)) +
      ' · <span class="fleet-freshness fleet-field-' + escapeHtml(worstFreshness(node)) +
      '">' + escapeHtml(worstFreshness(node)) + "</span></p>" +
      '<dl class="fleet-detail-list"><dt>Instance</dt><dd><code>' + escapeHtml(node.id) +
      "</code></dd><dt>Endpoint</dt><dd>" + escapeHtml(node.url) +
      "</dd><dt>Zone</dt><dd>" + escapeHtml(node.zone || "default") +
      "</dd></dl>" + sections;
  }

  function topologyStatus(node) {
    var reach = fieldValue(node, "reachability");
    var health = reach.value && reach.value.health;
    if (health !== "up") return health === "unknown" ? reach.state : (health || reach.state);
    return worstFreshness(node);
  }

  function renderFleetTopology() {
    var host = $("#pa-fleet-topology");
    var svg = host && $("svg", host);
    if (!svg || !fleetOverview) return;
    var nodes = fleetOverview.nodes || [];
    var edges = fleetOverview.edges || [];
    var compactSingle = nodes.length === 1 && window.innerWidth <= 600;
    svg.setAttribute("viewBox", compactSingle ? "300 100 360 220" : "0 0 960 420");
    svg.classList.toggle("fleet-topology-multi", nodes.length > 1);
    var positions = {};
    nodes.forEach(function (node, index) {
      if (nodes.length === 1) {
        positions[node.id] = { x: 480, y: 210 };
        return;
      }
      var angle = -Math.PI / 2 + (Math.PI * 2 * index / nodes.length);
      positions[node.id] = { x: 480 + Math.cos(angle) * 310, y: 210 + Math.sin(angle) * 130 };
    });
    var parts = [
      '<defs><marker id="fleet-arrow" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto"><path d="M0,0 L10,4 L0,8 z"></path></marker></defs>'
    ];
    var parallelEdges = {};
    edges.forEach(function (edge) {
      if (!positions[edge.source] || !positions[edge.target]) return;
      var pair = [String(edge.source), String(edge.target)].sort().join("\u0000");
      if (!parallelEdges[pair]) parallelEdges[pair] = [];
      parallelEdges[pair].push(edge);
    });
    edges.forEach(function (edge) {
      var from = positions[edge.source];
      var to = positions[edge.target];
      if (!from || !to) return;
      var visualStatus = edgeVisualStatus(edge);
      var countLabel = edge.count > 1 ? edge.count + " activities · " : "";
      var label = escapeHtml(edge.kind + " · " + countLabel +
        (edge.label || visualStatus) + " · " + visualStatus);
      var pairKey = [String(edge.source), String(edge.target)].sort().join("\u0000");
      var siblings = parallelEdges[pairKey] || [edge];
      var parallelIndex = siblings.indexOf(edge);
      var parallelOffset = (parallelIndex - (siblings.length - 1) / 2) * 200;
      var visualLabel = edge.kind + (edge.count > 1 ? " ×" + edge.count : "");
      if (edge.source === edge.target) {
        var loopLift = 92 + Math.abs(parallelOffset);
        var loopSpread = 115 + parallelIndex * 16;
        parts.push('<g class="fleet-edge fleet-edge-' + escapeHtml(visualStatus) +
          '" data-fleet-edge="' + escapeHtml(edge.id) + '" tabindex="0" role="button" aria-label="' +
          label + '"><title>' + label + '</title><path d="M ' + (from.x + 58) + " " +
          (from.y - 28) + " C " + (from.x + loopSpread) + " " + (from.y - loopLift) + ", " +
          (from.x - loopSpread) + " " + (from.y - loopLift) + ", " + (from.x - 58) + " " +
          (from.y - 28) + '"></path><text x="' + from.x + '" y="' +
          (from.y - loopLift + 10) + '" text-anchor="middle">' +
          escapeHtml(visualLabel) + "</text></g>");
      } else {
        var canonicalIds = [String(edge.source), String(edge.target)].sort();
        var canonicalFrom = positions[canonicalIds[0]];
        var canonicalTo = positions[canonicalIds[1]];
        var dx = canonicalTo.x - canonicalFrom.x;
        var dy = canonicalTo.y - canonicalFrom.y;
        var length = Math.sqrt(dx * dx + dy * dy) || 1;
        var controlX = (from.x + to.x) / 2 - dy / length * parallelOffset;
        var controlY = (from.y + to.y) / 2 + dx / length * parallelOffset;
        var midX = (from.x + 2 * controlX + to.x) / 4;
        var midY = (from.y + 2 * controlY + to.y) / 4;
        parts.push('<g class="fleet-edge fleet-edge-' + escapeHtml(visualStatus) +
          '" data-fleet-edge="' + escapeHtml(edge.id) + '" tabindex="0" role="button" aria-label="' +
          label + '"><title>' + label + '</title><path d="M ' + from.x + " " + from.y +
          " Q " + controlX + " " + controlY + ", " + to.x + " " + to.y +
          '"></path><text x="' + midX + '" y="' + (midY - 8) +
          '" text-anchor="middle">' + escapeHtml(visualLabel) + "</text></g>");
      }
    });
    nodes.forEach(function (node) {
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
      var syncLabel = sync.value ? (sync.value.consistent ? "heads aligned" : "head mismatch") : sync.state;
      var providerValues = Array.isArray(providers.value) ? providers.value : [];
      var readyProviders = providerValues.filter(function (provider) {
        var needsAuth = provider.auth_method && provider.auth_method !== "none";
        return provider.available !== false && (!needsAuth || provider.auth_configured !== false);
      }).length;
      var providerLabel = providerValues.length
        ? readyProviders + "/" + providerValues.length + " providers ready"
        : "providers " + providers.state;
      var updateLabel = update.value && update.value.upgrade_available
        ? "update " + (update.value.available_version || update.value.latest || "available")
        : "update " + update.state;
      var visualProviderLabel = providerValues.length
        ? readyProviders + "/" + providerValues.length + " auth"
        : "auth " + providers.state;
      var visualUpdateLabel = update.value && update.value.upgrade_available
        ? "upgrade " + (update.value.available_version || update.value.latest || "available")
        : (update.state === "fresh" ? "current" : update.state);
      var title = node.name + ": " + health + ", " + activityLabel(activity.value) +
        ", version " + version + ", " + updateLabel + ", sync " + syncLabel +
        ", " + providerLabel + ", freshness " + worstFreshness(node);
      parts.push('<g class="fleet-node fleet-node-' + escapeHtml(topologyStatus(node)) +
        (node.local ? " fleet-node-local" : "") + '" data-fleet-node="' + escapeHtml(node.id) +
        '" tabindex="0" role="button" aria-label="' + escapeHtml(title) + '"><title>' +
        escapeHtml(title) + '</title><rect x="' + (pos.x - 94) + '" y="' + (pos.y - 58) +
        '" width="188" height="116" rx="14"></rect><text class="fleet-node-name" x="' +
        pos.x + '" y="' + (pos.y - 34) + '" text-anchor="middle">' + escapeHtml(mark + " " + node.name) +
        '</text><text x="' + pos.x + '" y="' + (pos.y - 10) + '" text-anchor="middle">' +
        escapeHtml(activityLabel(activity.value)) + '</text><text x="' + pos.x + '" y="' +
        (pos.y + 10) + '" text-anchor="middle">' + escapeHtml("v" + version + " · " + syncLabel) +
        '</text><text class="fleet-node-readiness" x="' + pos.x + '" y="' +
        (pos.y + 30) + '" text-anchor="middle">' +
        escapeHtml(visualProviderLabel + " · " + visualUpdateLabel) +
        '</text><text class="fleet-node-freshness" x="' + pos.x + '" y="' + (pos.y + 49) +
        '" text-anchor="middle">' + escapeHtml(worstFreshness(node)) + "</text></g>");
    });
    svg.innerHTML = parts.join("");
    renderFleetEdgeList();
  }

  function renderSelectedFleetDetail() {
    if (!selectedFleetItem) return;
    renderFleetDetail(
      selectedFleetItem.kind,
      selectedFleetItem.id,
      selectedFleetItem.edgeId
    );
  }

  function renderFleetOverview() {
    if (!fleetOverview) return;
    (fleetOverview.nodes || []).forEach(renderFleetRow);
    renderFleetTopology();
    renderSelectedFleetDetail();
  }

  var fleetUpdateName = "";

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
    var log = $("#pa-fleet-update-log");
    var status = $("#pa-fleet-update-status");
    var source = new EventSource(
      "/api/fleet/instances/" + encodeURIComponent(instanceId) +
      "/update/" + encodeURIComponent(jobId) + "/events"
    );
    source.addEventListener("phase", function (event) {
      var item = JSON.parse(event.data || "{}");
      if (log) {
        log.textContent += (log.textContent ? "\n" : "") +
          "[" + (item.phase || "update") + "] " + (item.message || "");
        log.scrollTop = log.scrollHeight;
      }
      if (status) status.textContent = item.message || item.phase || "Updating…";
    });
    source.addEventListener("done", function (event) {
      var job = JSON.parse(event.data || "{}");
      source.close();
      if (status) status.textContent = job.phase === "succeeded"
        ? "Verified PA " + (job.verified_version || "unknown") + " on " + job.instance_name + "."
        : (job.error || "Update failed");
      loadLiveStatus();
    });
    source.onerror = function () {
      if (source.readyState === EventSource.CLOSED && status) {
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
    (fleetOverview && fleetOverview.nodes || []).forEach(function (node) {
      Object.keys(node.dimensions || {}).forEach(function (name) {
        var previous = node.dimensions[name];
        if (previous.refreshing) node.dimensions[name] = Object.assign({}, previous, {
          state: state, refreshing: false, error: message
        });
      });
    });
    renderFleetOverview();
    setLiveBanner((message || "Health check failed") + " · Use Refresh to retry.");
  }

  function abortLiveStatus() {
    liveStatusSeq += 1;
    if (liveStatusController) liveStatusController.abort();
    clearTimeout(liveStatusTimer);
    liveStatusTimer = null;
    liveStatusRequest = null;
    liveStatusController = null;
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
    liveStatusController = typeof AbortController === "function" ? new AbortController() : null;
    api(
      "/api/fleet/overview",
      liveStatusController ? { signal: liveStatusController.signal } : {}
    ).then(function (snapshot) {
      if (seq !== liveStatusSeq || !snapshot) return;
      fleetOverview.edges = snapshot.edges || [];
      (snapshot.nodes || []).forEach(function (freshNode) {
        var existing = nodeById(freshNode.id);
        if (!existing) {
          fleetOverview.nodes.push(freshNode);
          return;
        }
        existing.name = freshNode.name;
        existing.url = freshNode.url;
        existing.zone = freshNode.zone;
        existing.capabilities = freshNode.capabilities;
      });
      renderFleetTopology();
      renderSelectedFleetDetail();
    }).catch(function (error) {
      if (error.name !== "AbortError" && seq === liveStatusSeq) {
        setLiveBanner("Relationship refresh failed · status fields are still refreshing.");
      }
    });
    var nodes = (fleetOverview.nodes || []).filter(function (node) {
      return !onlyInstance || node.id === onlyInstance;
    });
    var dimensions = fleetOverview.dimensions || [
      "reachability", "status", "providers", "update", "activity", "sync", "repositories", "supervisor"
    ];
    var work = [];
    nodes.forEach(function (node) {
      dimensions.forEach(function (dimension) {
        var previous = fieldValue(node, dimension);
        node.dimensions[dimension] = Object.assign({}, previous, { refreshing: true });
        work.push({ node: node, dimension: dimension });
      });
    });
    renderFleetOverview();
    var completed = 0;
    var started = performance.now();
    setLiveBanner("Refreshing 0 of " + work.length + " fields…");

    async function runOne(item) {
      var controller = typeof AbortController === "function" ? new AbortController() : null;
      var timedOut = false;
      var timeout = item.dimension === "reachability" ? 3500 : 5500;
      var timer = setTimeout(function () {
        timedOut = true;
        if (controller) controller.abort();
      }, timeout);
      if (liveStatusController && controller) {
        liveStatusController.signal.addEventListener("abort", function () {
          controller.abort();
        }, { once: true });
      }
      var path = "/api/fleet/overview/dimension?instance_id=" +
        encodeURIComponent(item.node.id) + "&dimension=" +
        encodeURIComponent(item.dimension) + "&generation=" + seq +
        (force ? "&retry=true" : "");
      var mark = "fleet:" + seq + ":" + item.node.id + ":" + item.dimension;
      if (performance.mark) performance.mark(mark + ":start");
      try {
        var patch = await api(path, controller ? { signal: controller.signal } : {});
        if (seq !== liveStatusSeq || patch.generation !== seq) return;
        item.node.dimensions[item.dimension] = Object.assign({}, patch, { refreshing: false });
      } catch (err) {
        if (seq !== liveStatusSeq) return;
        var previous = fieldValue(item.node, item.dimension);
        item.node.dimensions[item.dimension] = Object.assign({}, previous, {
          state: timedOut ? "timeout" : "error",
          refreshing: false,
          error: timedOut ? item.dimension + " browser deadline exceeded" : err.message
        });
      } finally {
        clearTimeout(timer);
        if (performance.mark && performance.measure) {
          try {
            performance.mark(mark + ":end");
            performance.measure(mark, mark + ":start", mark + ":end");
          } catch (ignore) {}
        }
        if (seq === liveStatusSeq) {
          completed += 1;
          renderFleetRow(item.node);
          renderFleetTopology();
          if (selectedFleetItem) {
            if (
              selectedFleetItem.kind !== "node" ||
              selectedFleetItem.id === item.node.id
            ) {
              renderSelectedFleetDetail();
            }
          }
          setLiveBanner("Refreshed " + completed + " of " + work.length +
            " fields · " + Math.round(performance.now() - started) + " ms");
        }
      }
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
    liveStatusRequest = Promise.all(workers).finally(function () {
      if (seq !== liveStatusSeq) return;
      clearTimeout(liveStatusTimer);
      liveStatusTimer = null;
      liveStatusController = null;
      liveStatusRequest = null;
    });
    return liveStatusRequest;
  }

  function maybeLoadLiveStatus() {
    if (!$("#pa-fleet-root")) return;
    fleetOverview = readFleetOverview();
    renderFleetOverview();
    loadLiveStatus(false);
  }

  document.addEventListener("DOMContentLoaded", function () {
    maybeLoadLiveStatus();
    maybeLoadRemoteOperations();
    maybeLoadSyncStatus();
  });
  var fleetResizeTimer = null;
  window.addEventListener("resize", function () {
    clearTimeout(fleetResizeTimer);
    fleetResizeTimer = setTimeout(renderFleetTopology, 80);
  });
  document.body.addEventListener("htmx:afterSwap", function (evt) {
    var target = evt.target;
    if (
      target &&
      (target.id === "app-view" ||
        target.id === "pa-fleet-root" ||
        (target.querySelector && target.querySelector("#pa-fleet-root")))
    ) {
      maybeLoadLiveStatus();
      maybeLoadRemoteOperations();
      maybeLoadSyncStatus();
    }
  });
  document.body.addEventListener("htmx:beforeSwap", function (evt) {
    var target = evt.target;
    if (target && target.id === "app-view" && liveStatusRequest) {
      abortLiveStatus();
    }
  });

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
    remoteInstanceId = e.target.value || "";
    remoteAuditGeneration += 1;
    try { localStorage.setItem("pa-remote-instance", remoteInstanceId); } catch (err) {}
    clearRemoteWatchers();
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

  document.addEventListener("click", function (e) {
    var topologyNode = e.target.closest("[data-fleet-node]");
    if (topologyNode) {
      renderFleetDetail("node", topologyNode.getAttribute("data-fleet-node"));
      return;
    }
    var topologyEdgeItem = e.target.closest("[data-fleet-edge-item]");
    if (topologyEdgeItem) {
      renderFleetDetail(
        "edge-item",
        topologyEdgeItem.getAttribute("data-fleet-edge-item"),
        topologyEdgeItem.getAttribute("data-fleet-edge-parent")
      );
      return;
    }
    var topologyEdge = e.target.closest("[data-fleet-edge]");
    if (topologyEdge) {
      renderFleetDetail("edge", topologyEdge.getAttribute("data-fleet-edge"));
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
      if (!window.confirm("Install the official @openai/codex CLI on instance " + cliInstance + "?")) return;
      cliInstallButton.disabled = true;
      api(codexLoginBase(cliInstance).replace(/\/login-jobs$/, "/codex-cli/install"), {
        method: "POST"
      }).then(function (result) {
        if (!result.ok) throw new Error(result.message || "Codex CLI install failed");
        loadLiveStatus();
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
      if (resumeInstance) resumeInstance.textContent = "Target instance: " + codexLoginInstance;
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
      if (instance) instance.textContent = "Target instance: " + codexLoginInstance;
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
      loadRemoteOperations();
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
      var panel = $("#pa-fleet-update-panel");
      var updateForm = $("#pa-fleet-update-form");
      if (!panel || !updateForm) return;
      updateForm.elements.instance_id.value = updateBtn.getAttribute("data-fleet-update") || "";
      fleetUpdateName = updateBtn.getAttribute("data-instance-name") || "this instance";
      var row = updateBtn.closest("tr[data-fleet-instance]");
      updateForm.elements.channel.value = (row && row.dataset.updateChannel) || "release";
      panel.hidden = false;
      panel.scrollIntoView({ behavior: "smooth", block: "nearest" });
      refreshFleetUpdateCheck().catch(function (err) {
        var confirmText = $("#pa-fleet-update-confirm");
        if (confirmText) confirmText.textContent = err.message;
      });
      return;
    }

    if (e.target.closest("[data-fleet-update-cancel]")) {
      var updatePanel = $("#pa-fleet-update-panel");
      if (updatePanel) updatePanel.hidden = true;
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
    if (!form.id && !form.getAttribute("data-fleet-fix")) return;

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
      body.port = parseInt(body.port || "22", 10);
      clearSecrets(form);
      if (statusEl) statusEl.textContent = "Starting…";
      api("/api/fleet/install-remote", { method: "POST", body: body })
        .then(function (job) {
          return pollJob(job.job_id, logEl, statusEl);
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
})();
