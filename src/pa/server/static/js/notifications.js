(function () {
  if (window.__paNotificationsBound) return;
  window.__paNotificationsBound = true;

  var state = { filter: "outstanding", offset: 0, next: null, loading: false };
  var drafts = new Map();
  var pollTimer = null;

  function root() { return document.querySelector("[data-notification-chrome]"); }
  function panel() { var el = root(); return el && el.querySelector("[data-notification-list]"); }
  function csrf() {
    var meta = document.querySelector('meta[name="csrf-token"]');
    return meta ? { "X-CSRF-Token": meta.content } : {};
  }
  function esc(value) {
    var div = document.createElement("div");
    div.textContent = String(value == null ? "" : value);
    return div.innerHTML;
  }
  function age(value) {
    var seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
    if (seconds < 60) return "now";
    if (seconds < 3600) return Math.floor(seconds / 60) + "m";
    if (seconds < 86400) return Math.floor(seconds / 3600) + "h";
    return Math.floor(seconds / 86400) + "d";
  }
  function saveDrafts() {
    var list = panel();
    if (!list) return;
    list.querySelectorAll("[data-notification-draft]").forEach(function (input) {
      if (input.type === "checkbox") drafts.set(input.dataset.notificationDraft, input.checked);
      else if (input.value) drafts.set(input.dataset.notificationDraft, input.value);
      else drafts.delete(input.dataset.notificationDraft);
    });
  }
  function clearDrafts(notificationId) {
    Array.from(drafts.keys()).forEach(function (draftKey) {
      if (draftKey === notificationId || draftKey.indexOf(notificationId + ":") === 0) drafts.delete(draftKey);
    });
  }
  function structuredControls(item, interaction) {
    var schema = interaction.response_schema || {};
    var properties = schema.properties || {};
    var required = new Set(schema.required || []);
    var fields = Object.keys(properties).map(function (name) {
      var spec = properties[name] || {};
      var draftKey = item.id + ":" + name;
      var saved = drafts.get(draftKey);
      var requiredAttr = required.has(name) ? ' required data-required="true"' : "";
      var common = ' data-notification-draft="' + esc(draftKey) + '" data-notification-field="' + esc(name) + '" data-field-type="' + esc(spec.type || "string") + '"' + requiredAttr;
      var control;
      if (Array.isArray(spec.enum)) {
        control = '<select' + common + '><option value="">Select…</option>' + spec.enum.map(function (value) { return '<option value="' + esc(value) + '"' + (saved === String(value) ? " selected" : "") + '>' + esc(value) + "</option>"; }).join("") + "</select>";
      } else if (spec.type === "boolean") {
        control = '<input type="checkbox"' + common + (saved === true ? " checked" : "") + ">";
      } else {
        var inputType = ["integer", "number"].indexOf(spec.type) >= 0 ? "number" : "text";
        control = '<input type="' + inputType + '" value="' + esc(saved == null ? (spec.default == null ? "" : spec.default) : saved) + '"' + common + ">";
      }
      return '<label><span>' + esc(spec.title || name) + (required.has(name) ? " *" : "") + '</span>' + control + (spec.description ? '<small>' + esc(spec.description) + "</small>" : "") + "</label>";
    }).join("");
    return '<fieldset class="notification-fields"><legend>Response details</legend>' + fields + '<button type="button" class="primary small" data-notification-send-fields>Send</button></fieldset>';
  }
  function routeContext(item) {
    var parts = [item.realm_id, item.type, item.priority];
    if (item.source_instance_name || item.source_instance_id) parts.push(item.source_instance_name || item.source_instance_id);
    if (item.project_id) parts.push("project " + item.project_id.slice(0, 8));
    if (item.card_id) parts.push("card " + item.card_id.slice(0, 8));
    if (item.session_id) parts.push("session " + item.session_id.slice(0, 8));
    if (item.dispatch_id) parts.push("dispatch " + item.dispatch_id.slice(0, 8));
    if (item.pr_number) parts.push("PR #" + item.pr_number);
    if (item.watch_id) parts.push("watch " + item.watch_id.slice(0, 8));
    return parts.join(" · ");
  }
  function interactionControls(item) {
    var interaction = item.interaction;
    if (!interaction || ["outstanding", "delivery_pending"].indexOf(interaction.state) < 0) return "";
    var choices = (interaction.choices || []).map(function (choice) {
      return '<button type="button" class="small" data-notification-choice="' + esc(choice.id) + '">' + esc(choice.label) + "</button>";
    }).join("");
    var input = "";
    if (interaction.response_schema && interaction.response_schema.properties) {
      input = structuredControls(item, interaction);
    } else if (interaction.allow_freeform) {
      var draft = drafts.get(item.id) || "";
      input = '<label class="notification-reply"><span class="sr-only">Response</span><textarea rows="2" data-notification-draft="' + esc(item.id) + '" placeholder="Type a response">' + esc(draft) + '</textarea><button type="button" class="primary small" data-notification-send>Send</button></label>';
    }
    var cancel = interaction.allow_cancel ? '<button type="button" class="ghost small" data-notification-cancel>Cancel request</button>' : "";
    return '<div class="notification-actions">' + choices + input + cancel + "</div>";
  }
  function render(items, append) {
    var list = panel();
    if (!list) return;
    saveDrafts();
    var html = items.map(function (item) {
      var destination = item.routing && item.routing.destination;
      var remote = item.routing && item.routing.response_mode === "remote";
      return '<article class="notification-item priority-' + esc(item.priority) + '" data-notification-id="' + esc(item.id) + '" tabindex="0"' + (destination ? ' data-notification-destination="' + esc(destination) + '"' : "") + '>' +
        '<div class="notification-item-heading"><strong>' + esc(item.title) + '</strong><time datetime="' + esc(item.updated_at) + '">' + age(item.updated_at) + '</time></div>' +
        '<div class="notification-context">' + esc(routeContext(item)) + '</div>' +
        '<p>' + esc(item.summary || item.body) + '</p>' +
        (remote ? '<p class="notification-warning">Complete this action on the owning instance.</p>' : "") +
        interactionControls(item) +
        '<div class="notification-secondary"><button type="button" class="ghost small" data-notification-ack>Acknowledge</button></div>' +
        '</article>';
    }).join("");
    if (append) list.insertAdjacentHTML("beforeend", html);
    else list.innerHTML = html || '<p class="notification-empty">No notifications match this filter.</p>';
  }
  function query() {
    var params = new URLSearchParams({ limit: "40", offset: String(state.offset) });
    if (state.filter === "outstanding") params.set("outstanding", "true");
    if (state.filter === "unread") params.set("unread", "true");
    return params.toString();
  }
  function load(append) {
    if (state.loading) return Promise.resolve();
    state.loading = true;
    return fetch("/api/notifications?" + query(), { credentials: "same-origin", cache: "no-store" })
      .then(function (response) { if (!response.ok) throw new Error("Notifications unavailable"); return response.json(); })
      .then(function (data) {
        var chrome = root();
        if (!chrome) return;
        var badge = chrome.querySelector("[data-notification-count]");
        badge.hidden = !(data.outstanding_count > 0);
        badge.textContent = data.outstanding_count > 99 ? "99+" : String(data.outstanding_count || "");
        state.next = data.next_offset;
        chrome.querySelector("[data-notification-more]").hidden = state.next == null;
        render(data.items || [], append);
        chrome.querySelector("[data-notification-status]").textContent = data.outstanding_count ? " · " + data.outstanding_count + " outstanding" : "";
      })
      .catch(function () {
        var chrome = root();
        if (chrome) chrome.querySelector("[data-notification-status]").textContent = " · unavailable";
      })
      .finally(function () { state.loading = false; });
  }
  function mutate(item, action, body) {
    return fetch("/api/notifications/" + encodeURIComponent(item.dataset.notificationId) + "/" + action, {
      method: "POST", credentials: "same-origin",
      headers: Object.assign({ "Content-Type": "application/json" }, csrf()),
      body: JSON.stringify(body)
    }).then(function (response) {
      if (!response.ok) return response.json().catch(function () { return {}; }).then(function (data) { throw new Error((data.detail && data.detail.message) || "Action could not be completed"); });
      clearDrafts(item.dataset.notificationId);
      state.offset = 0;
      return load(false);
    }).catch(function (error) {
      var warning = item.querySelector(".notification-warning") || document.createElement("p");
      warning.className = "notification-warning";
      warning.textContent = error.message;
      item.appendChild(warning);
    });
  }
  function key() { return "ui:" + Date.now() + ":" + Math.random().toString(36).slice(2); }
  function navigateFrom(item) {
    var destination = item.dataset.notificationDestination;
    if (!destination) return;
    fetch("/api/notifications/" + encodeURIComponent(item.dataset.notificationId) + "/read", {
      method: "POST", credentials: "same-origin", keepalive: true,
      headers: Object.assign({ "Content-Type": "application/json" }, csrf()),
      body: JSON.stringify({ idempotency_key: key() })
    }).catch(function () {});
    window.location.assign(destination);
  }
  function open() {
    var chrome = root(); if (!chrome) return;
    var flyout = chrome.querySelector("#pa-notification-panel");
    flyout.hidden = false;
    chrome.querySelector("#pa-notification-bell").setAttribute("aria-expanded", "true");
    state.offset = 0; load(false).then(function () { flyout.focus(); });
  }
  function close() {
    var chrome = root(); if (!chrome) return;
    chrome.querySelector("#pa-notification-panel").hidden = true;
    var bell = chrome.querySelector("#pa-notification-bell"); bell.setAttribute("aria-expanded", "false"); bell.focus();
  }
  document.addEventListener("click", function (event) {
    var chrome = root(); if (!chrome) return;
    if (event.target.closest("#pa-notification-bell")) { chrome.querySelector("#pa-notification-panel").hidden ? open() : close(); return; }
    if (event.target.closest("[data-notification-close]")) { close(); return; }
    var filter = event.target.closest("[data-notification-filter]");
    if (filter) { state.filter = filter.dataset.notificationFilter; state.offset = 0; chrome.querySelectorAll("[data-notification-filter]").forEach(function (b) { b.classList.toggle("active", b === filter); b.classList.toggle("ghost", b !== filter); }); load(false); return; }
    if (event.target.closest("[data-notification-more]")) { state.offset = state.next || 0; load(true); return; }
    var item = event.target.closest("[data-notification-id]"); if (!item) { if (!event.target.closest("[data-notification-chrome]")) close(); return; }
    if (event.target.closest("[data-notification-ack]")) return void mutate(item, "acknowledge", { idempotency_key: key() });
    var choice = event.target.closest("[data-notification-choice]"); if (choice) return void mutate(item, "respond", { idempotency_key: key(), choice_id: choice.dataset.notificationChoice });
    if (event.target.closest("[data-notification-send]")) { var input = item.querySelector("[data-notification-draft]"); if (input && input.value.trim()) mutate(item, "respond", { idempotency_key: key(), value: input.value }); return; }
    if (event.target.closest("[data-notification-send-fields]")) {
      var fields = {}; var invalid = null;
      item.querySelectorAll("[data-notification-field]").forEach(function (input) {
        var raw = input.type === "checkbox" ? input.checked : input.value;
        if (input.dataset.required === "true" && raw === "") invalid = input;
        if (input.dataset.fieldType === "integer" && raw !== "") fields[input.dataset.notificationField] = parseInt(raw, 10);
        else if (input.dataset.fieldType === "number" && raw !== "") fields[input.dataset.notificationField] = parseFloat(raw);
        else fields[input.dataset.notificationField] = raw;
      });
      if (invalid) { invalid.focus(); invalid.setAttribute("aria-invalid", "true"); return; }
      mutate(item, "respond", { idempotency_key: key(), fields: fields }); return;
    }
    if (event.target.closest("[data-notification-cancel]")) return void mutate(item, "respond", { idempotency_key: key(), cancel: true });
    if (!event.target.closest("button,textarea,input,select,a")) navigateFrom(item);
  });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") { var chrome = root(); if (chrome && !chrome.querySelector("#pa-notification-panel").hidden) close(); }
    var item = event.target.closest && event.target.closest("[data-notification-id]");
    if (item && (event.key === "Enter" || event.key === " ") && event.target === item && item.dataset.notificationDestination) { event.preventDefault(); navigateFrom(item); }
  });
  function boot() {
    load(false);
    window.clearInterval(pollTimer); pollTimer = window.setInterval(function () { state.offset = 0; load(false); }, 15000);
    try { var source = new EventSource("/api/cards/events"); source.addEventListener("cards-changed", function (event) { try { var data = JSON.parse(event.data); if (data.type === "notifications-changed") { state.offset = 0; load(false); } } catch (_error) {} }); } catch (_error) {}
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else boot();
})();
