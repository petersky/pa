(function () {
  "use strict";
  if (window.__paNotificationsBound) return;
  window.__paNotificationsBound = true;

  var state = { filter: "outstanding", offset: 0, next: null, loading: false, refreshPending: false };
  var drafts = new Map();
  var pollTimer = null;
  var DRAFT_PREFIX = "pa.notification.draft.";

  function root() { return document.querySelector("[data-notification-chrome]"); }
  function panel() { var el = root(); return el && el.querySelector("[data-notification-list]"); }
  function csrf() {
    if (window.PACSRF) return window.PACSRF.headers();
    var meta = document.querySelector('meta[name="csrf-token"]');
    return meta ? { "X-CSRF-Token": meta.content } : {};
  }
  function esc(value) {
    var div = document.createElement("div");
    div.textContent = String(value == null ? "" : value);
    return div.innerHTML.replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function age(value) {
    var seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
    if (seconds < 60) return "now";
    if (seconds < 3600) return Math.floor(seconds / 60) + "m";
    if (seconds < 86400) return Math.floor(seconds / 3600) + "h";
    return Math.floor(seconds / 86400) + "d";
  }
  function storageGet(key) {
    try { return window.sessionStorage.getItem(DRAFT_PREFIX + key); } catch (_error) { return null; }
  }
  function storageSet(key, value) {
    try {
      if (value == null || value === "") window.sessionStorage.removeItem(DRAFT_PREFIX + key);
      else window.sessionStorage.setItem(DRAFT_PREFIX + key, JSON.stringify(value));
    } catch (_error) { /* in-memory drafts still survive polling */ }
  }
  function draftValue(key, sensitive) {
    if (drafts.has(key)) return drafts.get(key);
    if (sensitive) return null;
    var stored = storageGet(key);
    if (stored == null) return null;
    try { return JSON.parse(stored); } catch (_error) { return stored; }
  }
  function saveDrafts() {
    var list = panel();
    if (!list) return;
    list.querySelectorAll("[data-notification-draft]").forEach(function (input) {
      var key = input.dataset.notificationDraft;
      var value = input.type === "checkbox" ? input.checked : input.value;
      if (value === "") drafts.delete(key); else drafts.set(key, value);
      if (input.dataset.sensitive !== "true") storageSet(key, value);
    });
  }
  function clearDrafts(notificationId) {
    Array.from(drafts.keys()).forEach(function (draftKey) {
      if (draftKey === notificationId || draftKey.indexOf(notificationId + ":") === 0) {
        drafts.delete(draftKey);
        storageSet(draftKey, null);
      }
    });
    try {
      for (var index = window.sessionStorage.length - 1; index >= 0; index -= 1) {
        var key = window.sessionStorage.key(index) || "";
        if (key.indexOf(DRAFT_PREFIX + notificationId) === 0) window.sessionStorage.removeItem(key);
      }
    } catch (_error) { /* storage may be disabled */ }
  }
  function constraintAttrs(spec) {
    var attrs = "";
    [["minLength", "minlength"], ["maxLength", "maxlength"], ["minimum", "min"], ["maximum", "max"], ["pattern", "pattern"]].forEach(function (pair) {
      if (spec[pair[0]] != null) attrs += " " + pair[1] + '="' + esc(spec[pair[0]]) + '"';
    });
    if (spec.type === "integer") attrs += ' step="1"';
    return attrs;
  }
  function structuredControls(item, interaction) {
    var schema = interaction.response_schema || {};
    var properties = schema.properties || {};
    var required = new Set(schema.required || []);
    var names = Object.keys(properties);
    if (!names.length) return '<p class="notification-warning">This response schema has no supported fields. Open the linked session for details.</p>';
    var fields = names.map(function (name) {
      var spec = properties[name] || {};
      var draftKey = item.id + ":" + name;
      var saved = draftValue(draftKey, interaction.sensitive);
      var requiredAttr = required.has(name) ? ' required data-required="true"' : "";
      var common = ' data-notification-draft="' + esc(draftKey) + '" data-notification-field="' + esc(name) + '" data-field-type="' + esc(spec.type || "string") + '" data-sensitive="' + (interaction.sensitive ? "true" : "false") + '"' + requiredAttr + constraintAttrs(spec);
      var control;
      if (Array.isArray(spec.enum)) {
        control = '<select' + common + '><option value="">Select…</option>' + spec.enum.map(function (value) {
          return '<option value="' + esc(value) + '"' + (saved === String(value) ? " selected" : "") + '>' + esc(value) + "</option>";
        }).join("") + "</select>";
      } else if (spec.type === "boolean") {
        control = '<input type="checkbox"' + common + (saved === true ? " checked" : "") + ">";
      } else if (["array", "object"].indexOf(spec.type) >= 0 || spec.format === "multiline") {
        control = '<textarea rows="3"' + common + ' placeholder="' + (["array", "object"].indexOf(spec.type) >= 0 ? "Valid JSON" : "") + '">' + esc(saved == null ? "" : saved) + "</textarea>";
      } else {
        var inputType = ["integer", "number"].indexOf(spec.type) >= 0 ? "number" : ({ email: "email", uri: "url", date: "date", "date-time": "datetime-local" }[spec.format] || (interaction.sensitive ? "password" : "text"));
        control = '<input type="' + inputType + '" value="' + esc(saved == null ? (spec.default == null ? "" : spec.default) : saved) + '"' + common + ">";
      }
      return '<label><span>' + esc(spec.title || name) + (required.has(name) ? " *" : "") + '</span>' + control + (spec.description ? '<small>' + esc(spec.description) + "</small>" : "") + "</label>";
    }).join("");
    return '<fieldset class="notification-fields"><legend>Required response details</legend>' + fields + '<p class="notification-field-error" data-notification-field-error role="alert"></p><button type="button" class="primary small" data-notification-send-fields>Submit response</button></fieldset>';
  }
  function routeContext(item) {
    var context = item.context || {};
    var parts = [];
    if (context.project) parts.push(context.project.label);
    if (context.card) parts.push(context.card.label);
    if (context.session) parts.push(context.session.label);
    if (context.owner) parts.push(context.owner.label);
    if (!parts.length && (item.source_instance_name || item.source_instance_id)) parts.push(item.source_instance_name || item.source_instance_id);
    if (item.pr_number) parts.push("PR #" + item.pr_number);
    return parts.join(" · ");
  }
  function identityDetails(item) {
    var context = item.context || {};
    var rows = [];
    ["project", "card", "session", "dispatch", "owner"].forEach(function (kind) {
      if (context[kind] && context[kind].id) rows.push("<dt>" + esc(kind) + "</dt><dd><code>" + esc(context[kind].id) + "</code></dd>");
    });
    if (item.interaction && item.interaction.request_id) rows.push("<dt>request</dt><dd><code>" + esc(item.interaction.request_id) + "</code></dd>");
    if (item.interaction && item.interaction.protocol_method) rows.push("<dt>protocol</dt><dd><code>" + esc(item.interaction.protocol_method) + "</code></dd>");
    return rows.length ? '<details class="notification-identifiers"><summary>Technical details</summary><dl>' + rows.join("") + "</dl></details>" : "";
  }
  function choiceControls(interaction) {
    return (interaction.choices || []).map(function (choice) {
      return '<span class="notification-choice"><button type="button" class="small" data-notification-choice="' + esc(choice.id) + '">' + esc(choice.label) + "</button>" + (choice.description ? "<small>" + esc(choice.description) + "</small>" : "") + "</span>";
    }).join("");
  }
  function interactionControls(item) {
    var interaction = item.interaction;
    if (!interaction) return "";
    if (interaction.state === "failed") {
      return '<div class="notification-actions"><button type="button" class="primary small" data-notification-retry>Retry delivery</button></div>';
    }
    if (interaction.state !== "outstanding") return "";
    var choices = choiceControls(interaction);
    var input = "";
    if (interaction.response_schema && interaction.response_schema.properties) {
      input = structuredControls(item, interaction);
    } else if (interaction.allow_freeform) {
      var draft = draftValue(item.id, interaction.sensitive) || "";
      input = '<label class="notification-reply"><span>Response</span><textarea rows="3" data-notification-draft="' + esc(item.id) + '" data-sensitive="' + (interaction.sensitive ? "true" : "false") + '" placeholder="Write your response">' + esc(draft) + '</textarea><button type="button" class="primary small" data-notification-send>Submit response</button></label>';
    }
    var cancel = interaction.allow_cancel ? '<button type="button" class="ghost small" data-notification-cancel>Cancel request without responding</button>' : "";
    return '<div class="notification-actions">' + choices + input + cancel + "</div>";
  }
  function bodyMarkup(item) {
    var body = item.body || item.summary || "";
    var summary = item.summary || "";
    var markdown = '<div class="notification-markdown card-markdown" data-notification-markdown-body>' + esc(body) + "</div>";
    if (summary && summary !== body) {
      return '<p class="notification-summary">' + esc(summary) + '</p><details class="notification-full"><summary>Full request and details</summary>' + markdown + "</details>";
    }
    return markdown;
  }
  function safeMarkdownSource(value) {
    // Notification Markdown supports prose, lists, code and links, never raw HTML.
    return String(value == null ? "" : value).replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }
  function secondaryControls(item) {
    if (item.resolved_at) return "";
    if (item.interaction) {
      return item.read_at ? '<span class="notification-read-state">Read</span>' : '<button type="button" class="ghost small" data-notification-read>Mark read</button>';
    }
    return '<button type="button" class="ghost small" data-notification-resolve>Dismiss notice</button>';
  }
  function renderMarkdownBodies(items) {
    var list = panel();
    if (!list) return;
    items.forEach(function (item) {
      var selector = '[data-notification-id="' + String(item.id).replace(/["\\]/g, "\\$&") + '"]';
      var article = list.querySelector(selector);
      var target = article && article.querySelector("[data-notification-markdown-body]");
      if (!target || !window.PAAgentChat || typeof window.PAAgentChat.renderMarkdownAsync !== "function") return;
      window.PAAgentChat.renderMarkdownAsync(safeMarkdownSource(item.body || item.summary || ""), { allowEmbeddedMedia: false }).then(function (html) {
        if (!target.isConnected) return;
        target.innerHTML = html;
        if (window.PALinks) window.PALinks.decorate(target);
      });
    });
  }
  function render(items, append) {
    var list = panel();
    if (!list) return;
    saveDrafts();
    var html = items.map(function (item) {
      var destination = item.routing && item.routing.destination;
      var remote = item.routing && item.routing.response_mode === "remote";
      var presentation = item.presentation || {};
      return '<article class="notification-item priority-' + esc(item.priority) + ' category-' + esc(presentation.category || "notice") + '" data-notification-id="' + esc(item.id) + '" tabindex="0"' + (destination ? ' data-notification-destination="' + esc(destination) + '"' : "") + '>' +
        '<div class="notification-item-heading"><strong>' + esc(item.title) + '</strong><time datetime="' + esc(item.updated_at) + '">' + age(item.updated_at) + "</time></div>" +
        '<div class="notification-context">' + esc(routeContext(item)) + "</div>" +
        '<p class="notification-status"><strong>' + esc(presentation.status || "Notice") + "</strong></p>" +
        (presentation.required_action ? '<p class="notification-required"><strong>Required action:</strong> ' + esc(presentation.required_action) + "</p>" : "") +
        bodyMarkup(item) +
        (presentation.next_effect ? '<p class="notification-effect">' + esc(presentation.next_effect) + "</p>" : "") +
        (remote ? '<p class="notification-warning">Respond on the owning instance. This copy will remain outstanding until the owner records the result.</p>' : "") +
        (!remote ? interactionControls(item) : "") +
        '<p class="notification-feedback" data-notification-feedback role="status" aria-live="polite"></p>' +
        identityDetails(item) +
        '<div class="notification-secondary">' + secondaryControls(item) + "</div>" +
        "</article>";
    }).join("");
    if (append) list.insertAdjacentHTML("beforeend", html);
    else list.innerHTML = html || '<p class="notification-empty">No notifications match this filter.</p>';
    renderMarkdownBodies(items);
  }
  function query() {
    var params = new URLSearchParams({ limit: "40", offset: String(state.offset) });
    if (state.filter === "outstanding") params.set("outstanding", "true");
    if (state.filter === "unread") params.set("unread", "true");
    return params.toString();
  }
  function hasFocusedControl() {
    var chrome = root();
    var flyout = chrome && chrome.querySelector("#pa-notification-panel");
    return Boolean(flyout && !flyout.hidden && flyout.contains(document.activeElement) && document.activeElement !== flyout);
  }
  function load(append, force) {
    if (!append && !force && hasFocusedControl()) { state.refreshPending = true; return Promise.resolve(); }
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
      .finally(function () { state.loading = false; state.refreshPending = false; });
  }
  function mutate(item, action, body, pendingText) {
    if (item.dataset.notificationBusy === "true") return Promise.resolve();
    item.dataset.notificationBusy = "true";
    item.setAttribute("aria-busy", "true");
    item.querySelectorAll("button,input,textarea,select").forEach(function (control) { control.disabled = true; });
    var feedback = item.querySelector("[data-notification-feedback]");
    if (feedback) feedback.textContent = pendingText || "Submitting…";
    return fetch("/api/notifications/" + encodeURIComponent(item.dataset.notificationId) + "/" + action, {
      method: "POST", credentials: "same-origin",
      headers: Object.assign({ "Content-Type": "application/json" }, csrf()),
      body: JSON.stringify(body)
    }).then(function (response) {
      if (!response.ok) return response.json().catch(function () { return {}; }).then(function (data) { throw new Error((data.detail && data.detail.message) || "Action could not be completed"); });
      clearDrafts(item.dataset.notificationId);
      state.offset = 0;
      return load(false, true);
    }).catch(function (error) {
      item.dataset.notificationBusy = "false";
      item.removeAttribute("aria-busy");
      item.querySelectorAll("button,input,textarea,select").forEach(function (control) { control.disabled = false; });
      if (feedback) feedback.textContent = error.message + " Your draft is preserved; try again or open the linked session.";
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
  function fieldValues(item) {
    var fields = {};
    var invalid = null;
    var message = "";
    item.querySelectorAll("[data-notification-field]").forEach(function (input) {
      if (invalid) return;
      if (!input.checkValidity()) { invalid = input; message = input.validationMessage; return; }
      var raw = input.type === "checkbox" ? input.checked : input.value;
      if (input.dataset.required === "true" && raw === "") { invalid = input; message = "This field is required."; return; }
      if (raw === "" && input.dataset.required !== "true") return;
      try {
        if (input.dataset.fieldType === "integer" && raw !== "") fields[input.dataset.notificationField] = parseInt(raw, 10);
        else if (input.dataset.fieldType === "number" && raw !== "") fields[input.dataset.notificationField] = parseFloat(raw);
        else if (["array", "object"].indexOf(input.dataset.fieldType) >= 0) fields[input.dataset.notificationField] = JSON.parse(raw);
        else fields[input.dataset.notificationField] = raw;
      } catch (_error) { invalid = input; message = "Enter valid JSON."; }
    });
    return { fields: fields, invalid: invalid, message: message };
  }
  function open() {
    var chrome = root(); if (!chrome) return;
    var flyout = chrome.querySelector("#pa-notification-panel");
    flyout.hidden = false;
    chrome.querySelector("#pa-notification-bell").setAttribute("aria-expanded", "true");
    state.offset = 0; load(false, true).then(function () { flyout.focus(); });
  }
  function close() {
    var chrome = root(); if (!chrome) return;
    var flyout = chrome.querySelector("#pa-notification-panel");
    if (flyout.hidden) return;
    flyout.hidden = true;
    var bell = chrome.querySelector("#pa-notification-bell"); bell.setAttribute("aria-expanded", "false"); bell.focus();
  }
  document.addEventListener("click", function (event) {
    var chrome = root(); if (!chrome) return;
    if (event.target.closest("#pa-notification-bell")) { chrome.querySelector("#pa-notification-panel").hidden ? open() : close(); return; }
    if (event.target.closest("[data-notification-close]")) { close(); return; }
    var filter = event.target.closest("[data-notification-filter]");
    if (filter) { state.filter = filter.dataset.notificationFilter; state.offset = 0; chrome.querySelectorAll("[data-notification-filter]").forEach(function (button) { var active = button === filter; button.classList.toggle("active", active); button.classList.toggle("ghost", !active); button.setAttribute("aria-pressed", active ? "true" : "false"); }); load(false, true); return; }
    if (event.target.closest("[data-notification-more]")) { state.offset = state.next || 0; load(true); return; }
    var item = event.target.closest("[data-notification-id]"); if (!item) { if (!event.target.closest("[data-notification-chrome]")) close(); return; }
    if (event.target.closest("[data-notification-read]")) return void mutate(item, "read", { idempotency_key: key() }, "Marking read…");
    if (event.target.closest("[data-notification-resolve]")) return void mutate(item, "resolve", { idempotency_key: key() }, "Dismissing notice…");
    if (event.target.closest("[data-notification-retry]")) return void mutate(item, "respond", { idempotency_key: key(), retry: true }, "Retrying the recorded response…");
    var choice = event.target.closest("[data-notification-choice]");
    if (choice) return void mutate(item, "respond", { idempotency_key: key(), choice_id: choice.dataset.notificationChoice }, "Submitting selected response…");
    if (event.target.closest("[data-notification-send]")) {
      var input = item.querySelector("[data-notification-draft]");
      if (input && input.value.trim()) mutate(item, "respond", { idempotency_key: key(), value: input.value }, "Submitting response…");
      else if (input) { input.setAttribute("aria-invalid", "true"); input.focus(); }
      return;
    }
    if (event.target.closest("[data-notification-send-fields]")) {
      var result = fieldValues(item);
      if (result.invalid) {
        result.invalid.setAttribute("aria-invalid", "true"); result.invalid.focus();
        var error = item.querySelector("[data-notification-field-error]"); if (error) error.textContent = result.message;
        return;
      }
      mutate(item, "respond", { idempotency_key: key(), fields: result.fields }, "Submitting response details…"); return;
    }
    if (event.target.closest("[data-notification-cancel]")) return void mutate(item, "respond", { idempotency_key: key(), cancel: true }, "Cancelling request…");
    if (!event.target.closest("button,textarea,input,select,a,summary,details")) navigateFrom(item);
  });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") { var chrome = root(); if (chrome && !chrome.querySelector("#pa-notification-panel").hidden) close(); }
    var item = event.target.closest && event.target.closest("[data-notification-id]");
    if (item && (event.key === "Enter" || event.key === " ") && event.target === item && item.dataset.notificationDestination) { event.preventDefault(); navigateFrom(item); }
  });
  document.addEventListener("focusout", function (event) {
    var chrome = root();
    var flyout = chrome && chrome.querySelector("#pa-notification-panel");
    if (!flyout || !flyout.contains(event.target)) return;
    window.setTimeout(function () { if (state.refreshPending && !hasFocusedControl()) { state.offset = 0; load(false, true); } }, 0);
  });
  function boot() {
    load(false);
    window.clearInterval(pollTimer); pollTimer = window.setInterval(function () { state.offset = 0; load(false); }, 15000);
    try { var source = new EventSource("/api/cards/events"); source.addEventListener("cards-changed", function (event) { try { var data = JSON.parse(event.data); if (data.type === "notifications-changed") { state.offset = 0; load(false); } } catch (_error) {} }); } catch (_error) {}
  }
  window.PANotificationsTest = { bodyMarkup: bodyMarkup, fieldValues: fieldValues, interactionControls: interactionControls, routeContext: routeContext, safeMarkdownSource: safeMarkdownSource };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else boot();
})();
