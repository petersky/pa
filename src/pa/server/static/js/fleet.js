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

  function healthHtml(healthy) {
    if (healthy) {
      return '<span class="status status-active">up</span>';
    }
    return '<span class="status status-blocked">down</span>';
  }

  function setLiveBanner(text) {
    var el = $("#pa-fleet-live-status");
    if (el) el.textContent = text || "";
  }

  var codexLoginInstance = "";
  var codexLoginJob = "";
  var codexLoginStartSequence = 0;

  function codexLoginBase(instanceId) {
    return "/api/fleet/instances/" + encodeURIComponent(instanceId) +
      "/agent-providers/codex/login-jobs";
  }

  async function watchCodexLogin(instanceId, jobId) {
    var instructions = $("#pa-codex-login-instructions");
    while (codexLoginJob === jobId) {
      var job = await api(codexLoginBase(instanceId) + "/" + encodeURIComponent(jobId));
      var parts = [];
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
        return;
      }
      await new Promise(function (resolve) { setTimeout(resolve, 1000); });
    }
  }

  function resetLivePlaceholders() {
    $all("#pa-fleet-instances [data-fleet-health]").forEach(function (el) {
      el.innerHTML = '<span class="muted small">Checking…</span>';
    });
    $all("#pa-fleet-instances [data-fleet-providers]").forEach(function (el) {
      el.innerHTML = '<span class="muted small">Checking…</span>';
    });
    $all("#pa-fleet-instances [data-fleet-current-version], #pa-fleet-instances [data-fleet-available-version]").forEach(function (el) {
      el.innerHTML = '<span class="muted small">Checking…</span>';
    });
    setLiveBanner("Checking instance health…");
  }

  function applyLiveStatus(rows) {
    var byId = {};
    (rows || []).forEach(function (row) {
      if (row && row.instance_id) byId[row.instance_id] = row;
    });
    $all("#pa-fleet-instances tr[data-fleet-instance]").forEach(function (tr) {
      var row = byId[tr.getAttribute("data-fleet-instance")];
      if (!row) return;
      var healthEl = $("[data-fleet-health]", tr);
      var providersEl = $("[data-fleet-providers]", tr);
      var currentEl = $("[data-fleet-current-version]", tr);
      var availableEl = $("[data-fleet-available-version]", tr);
      if (healthEl) healthEl.innerHTML = healthHtml(!!row.healthy);
      if (providersEl) providersEl.innerHTML = providersHtml(row.providers || [], row.instance_id);
      if (currentEl) currentEl.textContent = row.current_version || "—";
      if (availableEl) {
        availableEl.textContent = row.available_version || "—";
        availableEl.classList.toggle("status-active", !!row.upgrade_available);
      }
      tr.dataset.updateChannel = row.update_channel || "release";
      tr.dataset.currentVersion = row.current_version || "";
      tr.dataset.availableVersion = row.available_version || "";
    });
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

  async function loadLiveStatus() {
    var root = $("#pa-fleet-root");
    var table = $("#pa-fleet-instances");
    if (!root || !table) return;

    var seq = ++liveStatusSeq;
    resetLivePlaceholders();
    try {
      var rows = await api("/api/fleet/health");
      if (seq !== liveStatusSeq) return;
      applyLiveStatus(rows);
      var up = (rows || []).filter(function (r) {
        return r.healthy;
      }).length;
      var total = (rows || []).length;
      setLiveBanner(
        total
          ? "Checked " +
              total +
              " instance" +
              (total === 1 ? "" : "s") +
              " · " +
              up +
              " up"
          : ""
      );
    } catch (err) {
      if (seq !== liveStatusSeq) return;
      $all("#pa-fleet-instances [data-fleet-health]").forEach(function (el) {
        el.innerHTML = '<span class="status status-blocked">?</span>';
      });
      $all("#pa-fleet-instances [data-fleet-providers]").forEach(function (el) {
        el.innerHTML = '<span class="muted">—</span>';
      });
      setLiveBanner(err.message || "Health check failed");
    }
  }

  function maybeLoadLiveStatus() {
    if ($("#pa-fleet-root")) loadLiveStatus();
  }

  document.addEventListener("DOMContentLoaded", function () {
    maybeLoadLiveStatus();
    maybeLoadRemoteOperations();
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
    }
  });

  document.addEventListener("change", function (e) {
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
      codexLoginJob = "";
      codexLoginInstance = loginButton.getAttribute("data-codex-login") || "";
      codexLoginStartSequence += 1;
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

    if (e.target.closest("#pa-remote-notifications")) {
      e.preventDefault();
      enableRemoteNotifications().catch(function (err) {
        var status = $("#pa-remote-status");
        if (status) status.textContent = err.message;
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
        loadLiveStatus();
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
      var remoteInstanceSelect = $("#pa-remote-instance");
      var remoteBody = formToObject(form);
      var cardSelect = form.elements.card_id;
      if (cardSelect && cardSelect.selectedOptions.length) {
        var projectId = cardSelect.selectedOptions[0].getAttribute("data-project-id");
        if (projectId) remoteBody.project_id = projectId;
      }
      Object.keys(remoteBody).forEach(function (key) {
        if (remoteBody[key] === "") delete remoteBody[key];
      });
      var submit = form.querySelector("[data-remote-start]");
      if (submit) submit.disabled = true;
      if (remoteInstanceSelect) remoteInstanceSelect.disabled = true;
      if (remoteStatus) remoteStatus.textContent = "Starting remote session…";
      api(remoteApiBase(dispatchInstanceId) + "/start", {
        method: "POST",
        body: remoteBody,
      }).then(function (result) {
        var snapshot = result.session || {};
        var session = snapshot.session || snapshot;
        if (!session.id) throw new Error("Remote instance did not return a session id.");
        if (
          dispatchInstanceId !== remoteInstanceId ||
          !form.isConnected
        ) return;
        if (remoteStatus) {
          remoteStatus.textContent = result.prompt_error
            ? "Remote session " + session.id + " started, but its initial prompt failed: " + result.prompt_error
            : "Remote work started in session " + session.id + ".";
        }
        return loadRemoteOperations().then(function () {
          if (dispatchInstanceId === remoteInstanceId && form.isConnected) {
            selectRemoteSession(session.id);
          }
        });
      }).catch(function (err) {
        if (
          dispatchInstanceId === remoteInstanceId &&
          form.isConnected &&
          remoteStatus
        ) remoteStatus.textContent = err.message;
      }).finally(function () {
        if (submit && submit.isConnected) submit.disabled = false;
        if (remoteInstanceSelect && remoteInstanceSelect.isConnected) {
          remoteInstanceSelect.disabled = false;
        }
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
