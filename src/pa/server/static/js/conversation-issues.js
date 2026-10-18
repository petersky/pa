/* Discussion notes stay inert until the human explicitly starts a selection. */
(function (root) {
  "use strict";
  function element(tag, text) { const e = document.createElement(tag); if (text !== undefined) e.textContent = text; return e; }
  function Controller(widget) {
    this.widget = widget; this.panel = widget.root.querySelector("[data-conversation-issues]");
    this.state = null; this.selected = new Set(); this.generation = 0; this.busy = false; this.readRevision = 0;
    if (!this.panel) return;
    this.list = this.panel.querySelector("[data-issues-list]");
    this.status = this.panel.querySelector("[data-issues-status]");
    this.receipts = this.panel.querySelector("[data-issues-receipts]");
    this.addText = this.panel.querySelector("[data-issues-add-text]");
    const self = this;
    this.panel.addEventListener("toggle", function () {
      if (self.panel.open) {
        self.changed = false; self.count();
        if (!self.state) self.load();
      }
    });
    this.panel.querySelector("[data-issues-add]").addEventListener("click", function () {
      const lines = self.addText.value.split("\n").map(s => s.trim()).filter(Boolean);
      if (!lines.length || !self.state) return;
      if (self.state.issues.length + lines.length > 100) return self.message("Keep at most 100 issues per conversation.");
      if (lines.some(s => s.length > 300)) return self.message("Keep each issue title within 300 characters.");
      self.save(self.state.issues.concat(lines.map(title => ({id: crypto.randomUUID(), title, detail: "", group: "", deferred: false}))), function () { self.addText.value = ""; });
    });
    this.panel.querySelector("[data-issues-defer]").addEventListener("click", function () { self.editSelected(i => Object.assign(i, {deferred: true})); });
    this.panel.querySelector("[data-issues-restore]").addEventListener("click", function () { self.editSelected(i => Object.assign(i, {deferred: false})); });
    this.panel.querySelector("[data-issues-group]").addEventListener("click", function () {
      const group = self.panel.querySelector("[data-issues-group-name]").value.trim();
      self.editSelected(i => Object.assign(i, {group}));
    });
    this.panel.querySelector("[data-issues-start]").addEventListener("click", function () {
      if (!self.state || !self.selected.size) return self.message("Select the issues to start.");
      const ids = self.state.issues.filter(i => self.selected.has(i.id) && !i.request_id && !i.deferred).map(i => i.id);
      if (!ids.length) return self.message("Deferred issues must be restored before starting.");
      if (ids.length > 20) return self.message("Start at most 20 issues at once.");
      self.start({request_id: crypto.randomUUID(), revision: self.state.revision, issue_ids: ids});
    });
  }
  Controller.prototype.message = function (text) { if (this.status) this.status.textContent = text; };
  Controller.prototype.reset = function () {
    this.generation++; clearTimeout(this.timer); this.loading = null; this.busy = false; this.state = null; this.changed = false; this.stale = false; this.selected.clear();
    if (!this.panel) return;
    this.list.replaceChildren(); this.receipts.replaceChildren(); this.addText.value = "";
    this.count(); this.controls();
    this.message("Loading this conversation's list…");
  };
  Controller.prototype.ready = function () {
    if (this.panel && new URL(window.location.href).searchParams.get("issues") === "1") this.panel.open = true;
    if (this.panel && !this.state) this.load();
  };
  Controller.prototype.current = function (sid, generation) { return !this.widget.destroyed && sid === this.widget.sessionId && generation === this.generation; };
  Controller.prototype.controls = function () {
    if (!this.panel) return;
    this.panel.querySelectorAll("button,input,textarea").forEach(e => { e.disabled = this.busy || !this.state || e.dataset.started === "1"; });
  };
  Controller.prototype.count = function () {
    const count = this.widget.root.querySelector("[data-issues-count]");
    if (!count) return;
    const text = this.state ? "(" + this.state.issues.length + (this.stale ? " · stale" : this.changed ? " · updated" : "") + ")"
      : this.stale ? "(unavailable)" : "(loading…)";
    if (count.textContent !== text) count.textContent = text;
    // Status descendants are excluded from a tab's computed accessible name.
    const trigger = count.closest && count.closest("[role=tab], summary");
    const label = "Issues " + text;
    if (trigger && trigger.getAttribute("aria-label") !== label) trigger.setAttribute("aria-label", label);
  };
  Controller.prototype.schedule = function () {
    clearTimeout(this.timer);
    if (!this.widget.destroyed) this.timer = setTimeout(() => this.load(), 5000);
  };
  Controller.prototype.load = function () {
    const self = this, sid = this.widget.sessionId, generation = this.generation, apiBase = this.widget.apiBase;
    if (!sid || this.widget.destroyed || this.widget.ownerResolutionPending || this.loading) return;
    // Background reads must not replace a title being edited or race a mutation.
    if (this.busy || (this.panel && this.panel.contains(document.activeElement) &&
        /^(INPUT|TEXTAREA)$/.test(document.activeElement.tagName))) { this.schedule(); return; }
    const revision = this.readRevision = this.readRevision || 0;
    const entry = this.loading = {};
    const current = () => self.current(sid, generation) && apiBase === self.widget.apiBase;
    const path = "/sessions/" + encodeURIComponent(sid) + "/issues";
    const request = this.widget.apiWithTimeout ? this.widget.apiWithTimeout(path, 5000) : this.widget.api(path);
    entry.promise = request.then(function (state) {
      if (!current() || self.busy || revision !== self.readRevision) return;
      if (self.panel && self.panel.contains(document.activeElement) && /^(INPUT|TEXTAREA)$/.test(document.activeElement.tagName)) return;
      const previous = self.state;
      if (previous && state.revision < previous.revision) return;
      if (previous && !self.panel.open && state.issues.some(i => !previous.issues.some(old => old.id === i.id))) self.changed = true;
      self.state = state; self.stale = false; self.count();
      if (!previous || JSON.stringify([previous.issues, previous.requests]) !== JSON.stringify([state.issues, state.requests])) self.render();
      if (!previous) self.message("Discussion list · " + state.issues.length + " issues. Nothing starts until Start selected.");
    }).catch(function (error) {
      if (current() && revision === self.readRevision) { self.stale = true; self.count(); self.message(error.message); }
    }).finally(function () {
      if (self.loading === entry) self.loading = null;
      if (self.current(sid, generation)) self.schedule();
    });
    return entry.promise;
  };
  Controller.prototype.save = function (issues, after) {
    if (this.busy || !this.state) return;
    const self = this, sid = this.widget.sessionId, generation = this.generation;
    this.readRevision = (this.readRevision || 0) + 1; this.busy = true; this.controls();
    const clean = issues.map(i => { const copy = Object.assign({}, i); delete copy.request_id; return copy; });
    this.widget.api("/sessions/" + encodeURIComponent(sid) + "/issues", {method: "PUT", body: JSON.stringify({revision: this.state.revision, issues: clean})})
      .then(function (state) { if (!self.current(sid, generation)) return; self.state = state; if (after) after(); self.render(); self.message("Issue list saved. Discussion only."); })
      .catch(function (error) { if (self.current(sid, generation)) self.message(error.message + " The list will refresh automatically; your composer is unchanged."); })
      .finally(function () { if (self.current(sid, generation)) { self.busy = false; self.controls(); } });
  };
  Controller.prototype.editSelected = function (edit) {
    if (!this.state) return;
    this.save(this.state.issues.map(i => this.selected.has(i.id) && !i.request_id ? edit(Object.assign({}, i)) : i));
  };
  Controller.prototype.start = function (body) {
    if (this.busy) return;
    const self = this, sid = this.widget.sessionId, generation = this.generation, key = "pa:conversation-start:" + sid;
    try { localStorage.setItem(key, JSON.stringify(body)); } catch (_) { return this.message("Cannot retain a retry identity in this browser. Enable site storage before starting work."); }
    this.readRevision = (this.readRevision || 0) + 1; this.busy = true; this.controls(); this.message("Accepting selected work…");
    this.widget.api("/sessions/" + encodeURIComponent(sid) + "/issues/start", {method: "POST", headers: {"Idempotency-Key": body.request_id}, body: JSON.stringify(body)})
      .then(function (state) {
        try { localStorage.removeItem(key); } catch (_) {}
        if (!self.current(sid, generation)) return;
        self.state = state; self.selected.clear(); self.render(); self.message("Request saved. Follow the receipt below; keep chatting while work proceeds.");
        self.timer = setTimeout(() => self.load(), 1000);
      }).catch(function (error) { if (self.current(sid, generation)) { self.message(error.message + " The same request is available to retry below."); self.render(); } })
      .finally(function () { if (self.current(sid, generation)) { self.busy = false; self.controls(); } });
  };
  Controller.prototype.render = function () {
    const self = this;
    this.stale = false; this.count();
    this.list.replaceChildren(); this.receipts.replaceChildren();
    this.state.issues.forEach(function (issue) {
      const row = element("li"), check = element("input"), title = element("input");
      check.type = "checkbox"; check.checked = self.selected.has(issue.id); check.dataset.started = issue.request_id ? "1" : "0";
      check.setAttribute("aria-label", "Select " + issue.title);
      check.addEventListener("change", () => { if (check.checked) self.selected.add(issue.id); else self.selected.delete(issue.id); });
      title.value = issue.title; title.maxLength = 300; title.dataset.started = check.dataset.started;
      title.setAttribute("aria-label", "Issue title");
      title.addEventListener("change", () => self.save(self.state.issues.map(i => i.id === issue.id ? Object.assign({}, i, {title: title.value}) : i)));
      row.append(check, title, element("small", [issue.group, issue.deferred ? "Deferred" : "", issue.request_id ? "Work requested" : ""].filter(Boolean).join(" · ")));
      self.list.appendChild(row);
    });
    this.state.requests.forEach(function (receipt) {
      const section = element("div"), label = {accepted: "Accepted · PA preparing work", admitting: "Preparing work · PA is checking the saved request", admitted: "Work admitted · follow card progress", blocked: "Work could not start · see the reason below"}[receipt.state] || receipt.state;
      section.appendChild(element("strong", label));
      receipt.groups.forEach(function (group) {
        const line = element("p", group.title + " ");
        if (group.card_id) {
          const link = element("a", "Open card");
          link.href = "/work?card=" + encodeURIComponent(group.card_id);
          if (receipt.realm_id) link.href += "&realm=" + encodeURIComponent(receipt.realm_id);
          line.appendChild(link);
        }
        section.appendChild(line);
      });
      if (receipt.error) {
        const message = typeof receipt.error === "string" ? receipt.error : receipt.error.message;
        section.appendChild(element("p", message && !/\b[a-z]+_[a-z_]+\b/.test(message) ? message : "Start blocked. Review the saved card and current constraints."));
        const details = element("details"); details.append(element("summary", "Start diagnostics"), element("pre", JSON.stringify(receipt.error, null, 2))); section.appendChild(details);
      }
      if (receipt.state === "blocked") self.retryButton(section, receipt.submission);
      self.receipts.appendChild(section);
    });
    try {
      const pending = JSON.parse(localStorage.getItem("pa:conversation-start:" + this.widget.sessionId) || "null");
      if (pending && !this.state.requests.some(r => r.request_id === pending.request_id)) this.retryButton(this.receipts, pending);
    } catch (_) {}
    this.controls();
  };
  Controller.prototype.retryButton = function (parent, body) {
    const button = element("button", "Review start"); button.type = "button";
    button.addEventListener("click", () => {
      button.disabled = true;
      parent.appendChild(element("p", "Check the saved card's project and execution settings. PA will recheck permissions, resources and capacity using this same request."));
      const resume = element("button", "Resume saved request"); resume.type = "button";
      resume.addEventListener("click", () => this.start(body)); parent.appendChild(resume);
    }); parent.appendChild(button);
  };
  root.PAConversationIssues = Controller;
})(window);
