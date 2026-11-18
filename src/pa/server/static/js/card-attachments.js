(function () {
  "use strict";
  function base(section) { return "/api/cards/" + encodeURIComponent(section.dataset.cardId); }
  function query(section) { return "?realm=" + encodeURIComponent(section.dataset.realm); }
  function status(section, message) { section.querySelector("[data-attachment-status]").textContent = message; }
  function csrf() { return window.PACSRF ? window.PACSRF.headers() : {}; }
  async function responseBody(response) {
    var body;
    try { body = await response.json(); } catch (_) { body = {}; }
    if (!response.ok) {
      var detail = body.detail;
      var message = typeof detail === "string" ? detail : (detail && detail.message);
      var error = new Error(message || "Request failed (HTTP " + response.status + ").");
      error.uncertain = response.status >= 500 || (detail && detail.code === "operation_in_progress");
      throw error;
    }
    return body;
  }
  async function refresh(section, message) {
    var response = await fetch("/partials/cards/" + encodeURIComponent(section.dataset.cardId) + "/detail" + query(section), {credentials: "same-origin"});
    if (!response.ok) throw new Error("Saved, but refreshing details failed. Reload to see current materials.");
    var doc = new DOMParser().parseFromString(await response.text(), "text/html");
    var next = doc.querySelector("[data-card-attachments]");
    if (!next) throw new Error("Saved. Reload card details to see current materials.");
    section.replaceWith(next);
    status(next, message);
  }
  document.addEventListener("submit", async function (event) {
    var form = event.target.closest("[data-attachment-form]");
    if (!form) return;
    event.preventDefault();
    if (form.dataset.busy) return;
    var section = form.closest("[data-card-attachments]");
    if (section._attachmentPendingForm && section._attachmentPendingForm !== form) {
      status(section, "Finish retrying the previous attachment action before saving another change.");
      return;
    }
    var item = form.closest("[data-attachment-id]");
    var mode = form.dataset.attachmentForm;
    var data = new FormData(form);
    var url = base(section), method = "POST", body, headers = csrf();
    var revision = item && Number(item.dataset.revision);
    var version = section.dataset.version;
    var file = data.get("file");
    if (file && file.size > 25 * 1024 * 1024) { status(section, "File exceeds 25 MiB. Choose a smaller file; nothing was changed."); return; }
    if (item) url += "/attachments/" + encodeURIComponent(item.dataset.attachmentId);
    if (mode === "upload" || mode === "replace") {
      if (mode === "upload") url += "/attachments";
      else { url += "/replace"; method = "PUT"; }
      body = data;
    } else if (mode === "remove") { method = "DELETE"; }
    else {
      var fields = Object.fromEntries(data.entries());
      if (mode === "link") url += "/attachment-links";
      else { fields.expected_revision = revision; fields.expected_version = form.dataset.editorVersion || version; method = mode === "metadata" ? "PATCH" : "PUT"; }
      if (mode === "content") { url += "/content"; fields.expected_revision = Number(form.dataset.editorRevision); }
      body = JSON.stringify(fields);
      headers["Content-Type"] = "application/json";
    }
    url += query(section);
    if (mode === "replace") url += "&expected_revision=" + revision + "&expected_version=" + encodeURIComponent(version);
    if (mode === "remove") url += "&expected_version=" + encodeURIComponent(version);
    // Retain this exact request and key for uncertain response retries. Controls
    // stay locked until its outcome is known; a changed body must use a new key.
    var pending = form._attachmentPending;
    if (!pending) {
      pending = {url: url, method: method, body: body, headers: Object.assign({}, headers, {"Idempotency-Key": crypto.randomUUID()})};
      form._attachmentPending = pending;
    }
    section._attachmentPendingForm = form;
    form.dataset.busy = "true";
    form.querySelectorAll("button").forEach(function (b) { b.disabled = true; });
    status(section, "Saving…");
    var saved = false;
    try {
      var result = await responseBody(await fetch(pending.url, {method: pending.method, body: pending.body, headers: pending.headers, credentials: "same-origin"}));
      saved = true;
      await refresh(section, result.attachment_removal ? result.attachment_removal.message : (result.message || "Attachment saved."));
      delete form._attachmentPending;
      delete section._attachmentPendingForm;
    } catch (error) {
      var uncertain = !saved && (error.uncertain || error instanceof TypeError);
      if (!uncertain && !saved) {
        delete form._attachmentPending;
        delete section._attachmentPendingForm;
      }
      form.querySelectorAll("input,textarea").forEach(function (input) { input.disabled = !!uncertain || saved; });
      status(section, error.message + (uncertain ? " Outcome is unconfirmed. Retry this same action to recover its saved result." : saved ? " Retry this same action to refresh its saved result." : ""));
    } finally {
      delete form.dataset.busy;
      form.querySelectorAll("button").forEach(function (b) { b.disabled = false; });
    }
  });
  document.addEventListener("click", async function (event) {
    var edit = event.target.closest("[data-attachment-edit-content]");
    var cancel = event.target.closest("[data-attachment-cancel-content]");
    if (cancel) {
      var cancelledForm = cancel.closest("form");
      if (cancelledForm._attachmentPending) {
        status(cancelledForm.closest("[data-card-attachments]"), "Retry this attachment action to recover its saved result before closing the editor.");
        return;
      }
      cancelledForm.hidden = true;
      return;
    }
    if (!edit) return;
    var item = edit.closest("[data-attachment-id]"), section = edit.closest("[data-card-attachments]");
    if (section._attachmentPendingForm) {
      status(section, "Finish retrying the previous attachment action before opening an editor.");
      return;
    }
    edit.disabled = true;
    try {
      var result = await responseBody(await fetch(base(section) + "/attachments/" + encodeURIComponent(item.dataset.attachmentId) + "/content" + query(section), {credentials: "same-origin"}));
      var form = item.querySelector('[data-attachment-form="content"]');
      form.elements.content.value = result.content;
      form.dataset.editorVersion = result.expected_version;
      form.dataset.editorRevision = result.expected_revision;
      form.hidden = false;
      form.elements.content.focus();
    } catch (error) { status(section, error.message); }
    finally { edit.disabled = false; }
  });
})();
