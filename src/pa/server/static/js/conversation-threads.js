/* Branch requests have stable identities and never take over another thread. */
(function (root) {
  "use strict";
  function Controller(widget) {
    this.widget = widget; this.panel = widget.root.querySelector("[data-conversation-threads]");
    this.generation = 0; this.busy = false;
    this.originPanel = widget.root.querySelector("[data-conversation-origin]");
    this.originBoundary = widget.root.querySelector("[data-origin-boundary]");
    this.navigation = widget.root.querySelector("[data-thread-navigation]");
    this.list = widget.root.querySelector("[data-thread-list]");
    this.listStatus = widget.root.querySelector("[data-thread-list-status]");
    this.empty = widget.root.querySelector("[data-thread-empty]");
    if (this.empty) this.empty.addEventListener("click", () => this.branch("empty"));
    if (root.ResizeObserver && this.list) {
      this.resizeObserver = new root.ResizeObserver(() => this.revealSelected());
      this.resizeObserver.observe(this.list);
    }
    if (!this.panel) return;
    this.status = this.panel.querySelector("[data-thread-status]");
    this.native = this.panel.querySelector("[data-thread-native]");
    this.copy = this.panel.querySelector("[data-thread-copy]");
    this.native.addEventListener("click", () => this.branch("native"));
    this.copy.addEventListener("click", () => this.branch("copy"));
    this.panel.addEventListener("toggle", () => { if (this.panel.open) this.load(); });
  }
  Controller.prototype.current = function (sid, generation) { return !this.widget.destroyed && sid === this.widget.sessionId && generation === this.generation; };
  Controller.prototype.reset = function () {
    clearTimeout(this.refreshTimer); this.refreshTimer = null; this.listLoading = false;
    this.generation++; this.busy = false; this.loaded = false; this.capabilities = null;
    this.isChat = false; this.branchError = false;
    this.originLoaded = false; this.originLoading = false; this.source = null;
    this.originSignature = null;
    if (this.originPanel) this.originPanel.hidden = true;
    if (this.originBoundary) this.originBoundary.hidden = true;
    if (this.navigation) this.navigation.hidden = true;
    if (this.list) this.list.replaceChildren();
    if (this.listStatus) this.listStatus.textContent = "";
    this.setEmptyDisabled(true);
    if (!this.panel) return;
    this.native.disabled = this.copy.disabled = true;
    this.status.textContent = "Open to check branch support for this conversation.";
  };
  Controller.prototype.setEmptyDisabled = function (disabled) {
    if (this.empty) this.empty.disabled = disabled;
    document.querySelectorAll("[data-agent-new-thread]").forEach(button => { button.disabled = disabled; });
  };
  Controller.prototype.destroy = function () {
    this.reset();
    if (this.resizeObserver) this.resizeObserver.disconnect();
  };
  Controller.prototype.revealSelected = function () {
    const active = this.list && this.list.querySelector('[aria-current="page"]');
    if (!active) return;
    const item = active.getBoundingClientRect(), list = this.list.getBoundingClientRect();
    if (item.width >= list.width || item.left < list.left) this.list.scrollLeft += item.left - list.left;
    else if (item.right > list.right) this.list.scrollLeft += item.right - list.right;
  };
  Controller.prototype.ready = function (snap) {
    if (snap && snap.session) this.isChat = snap.session.purpose === "chat";
    const origin = snap && snap.session && snap.session.conversation_origin;
    this.renderOrigin(origin);
    if (origin && origin.strategy !== "empty" && !this.originLoaded && !this.originLoading) this.loadOrigin(origin);
    if (this.navigation) this.navigation.hidden = !this.isChat;
    this.setEmptyDisabled(!this.isChat || this.busy || this.widget.ownerResolutionPending);
    if (this.isChat && !this.refreshTimer && !this.listLoading) this.loadThreads();
    if (this.panel && this.panel.open && !this.loaded) this.load();
  };
  Controller.prototype.renderOrigin = function (origin) {
    if (!this.originPanel) return;
    this.originPanel.hidden = !origin;
    if (this.originBoundary) this.originBoundary.hidden = !origin;
    if (!origin) { this.originSignature = null; return; }
    const source = Object.assign({}, origin, this.source || {});
    const signature = JSON.stringify([source, this.originLoaded === true]);
    // Live/history snapshots can repeat. Keep a focused source link in place.
    if (signature === this.originSignature) return;
    this.originSignature = signature;
    const label = this.originPanel.querySelector("[data-origin-source]");
    const explanation = this.originPanel.querySelector("[data-origin-explanation]");
    const boundary = this.originBoundary;
    this.originPanel.dataset.strategy = origin.strategy;
    label.replaceChildren();
    if (origin.strategy === "empty") {
      label.textContent = "Fresh chat";
      explanation.textContent = "No conversation history was inherited. This conversation continues independently.";
      if (boundary) boundary.textContent = "Messages in this conversation";
      return;
    }
    label.appendChild(document.createTextNode(origin.strategy === "native" ? "Branched from " : "Context copied from "));
    const title = source.source_title || "Source conversation";
    if (source.source_available) {
      const link = document.createElement("a");
      link.textContent = title; link.title = title;
      link.href = "/agent?session=" + encodeURIComponent(origin.source_session_id);
      if (source.source_instance_id) link.href += "&instance=" + encodeURIComponent(source.source_instance_id);
      const sid = this.widget.sessionId, generation = this.generation;
      link.addEventListener("click", event => {
        if (event.button || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        event.preventDefault();
        if (this.current(sid, generation)) this.widget.switchSession(origin.source_session_id, true, source.source_instance_id || "");
      });
      label.appendChild(link);
      if (source.source_archived) label.appendChild(document.createTextNode(" · Archived source"));
    } else {
      label.appendChild(document.createTextNode(title + (source.source_available === false ? " · Source unavailable" :
        this.originLoaded ? " · Source availability unknown" : " · Checking source…")));
    }
    if (origin.strategy === "native") {
      explanation.textContent = origin.native_state === "forked"
        ? "Latest provider history was inherited at fork time. It is held by the provider, not shown here as a copied transcript. This branch continues independently."
        : "Native fork requested using the latest provider history. History inheritance is not yet confirmed. This branch has its own messages and workspace.";
    } else {
      explanation.textContent = "This copy uses recent visible text as context: up to 8000 characters from the last 100 events, with no hidden provider history. This conversation continues independently.";
    }
    if (boundary) boundary.textContent = "New messages in this branch";
  };
  Controller.prototype.loadOrigin = function (origin) {
    const sid = this.widget.sessionId, generation = this.generation;
    this.originLoading = true;
    this.widget.api("/sessions/" + encodeURIComponent(sid) + "/conversation-origin").then(result => {
      if (!this.current(sid, generation)) return;
      const value = result.origin || {};
      this.source = {source_available: value.source_available, source_archived: value.source_archived,
        source_title: value.source_title || origin.source_title, source_instance_id: value.source_instance_id};
    }).catch(() => {
      // A failed lookup is unknown availability, never a dead or unauthorized link.
    }).finally(() => {
      if (!this.current(sid, generation)) return;
      this.originLoaded = true; this.originLoading = false;
      this.renderOrigin((this.widget.lastSnapshot && this.widget.lastSnapshot.session.conversation_origin) || origin);
    });
  };
  Controller.prototype.loadThreads = function () {
    const sid = this.widget.sessionId, generation = this.generation;
    if (!sid || !this.isChat || this.widget.ownerResolutionPending || this.listLoading) return;
    this.listLoading = true;
    this.widget.api("/sessions/" + encodeURIComponent(sid) + "/threads").then(result => {
      if (!this.current(sid, generation)) return;
      this.renderThreads(result.threads || []);
      if (this.listStatus && !this.busy && !this.branchError) this.listStatus.textContent = "";
    }).catch(error => {
      if (this.current(sid, generation) && this.listStatus) this.listStatus.textContent = error.message;
    }).finally(() => {
      if (!this.current(sid, generation)) return;
      this.listLoading = false;
      this.refreshTimer = setTimeout(() => { this.refreshTimer = null; this.loadThreads(); }, 5000);
    });
  };
  Controller.prototype.renderThreads = function (threads) {
    if (!this.list) return;
    const scrollLeft = this.list.scrollLeft;
    const hadSelection = this.list.querySelector('[aria-current="page"]');
    const focused = this.list.contains(document.activeElement) && document.activeElement.dataset.threadId;
    this.list.replaceChildren();
    threads.forEach(thread => {
      const link = document.createElement("a");
      link.className = "ghost small acw-thread-link";
      link.dataset.threadId = thread.id;
      link.href = "/agent?session=" + encodeURIComponent(thread.id);
      if (thread.origin_instance_id) link.href += "&instance=" + encodeURIComponent(thread.origin_instance_id);
      if (thread.id === this.widget.sessionId) link.setAttribute("aria-current", "page");
      const source = threads.find(item => item.id === thread.source_session_id);
      const details = [thread.conversation_origin && thread.conversation_origin.label, thread.context, source ? "From: " + source.title : "",
        thread.presentation && thread.presentation.display_status,
        thread.queue_length ? thread.queue_length + " queued" : "", thread.archived ? "Archived" : ""].filter(Boolean).join(" · ");
      const title = document.createElement("strong"); title.textContent = thread.title;
      const context = document.createElement("small"); context.textContent = details;
      link.appendChild(title); link.appendChild(context); link.title = thread.title + " · " + details;
      link.addEventListener("click", event => {
        const receiptKey = "pa:conversation-branch:" + thread.source_session_id + ":" + thread.strategy;
        try { if (thread.request_id && localStorage.getItem(receiptKey) === thread.request_id) localStorage.removeItem(receiptKey); } catch (_) {}
        if (event.button || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        event.preventDefault();
        this.widget.switchSession(thread.id, true, thread.origin_instance_id || "");
      });
      this.list.appendChild(link);
      if (focused === thread.id) link.focus({preventScroll: true});
    });
    this.list.scrollLeft = scrollLeft;
    if (!hadSelection) this.revealSelected();
  };
  Controller.prototype.load = function () {
    const sid = this.widget.sessionId, generation = this.generation;
    if (!sid || this.widget.ownerResolutionPending || this.busy) return;
    this.widget.api("/sessions/" + encodeURIComponent(sid) + "/conversation-capabilities").then(caps => {
      if (!this.current(sid, generation) || this.busy) return;
      this.loaded = true; this.capabilities = caps;
      this.native.disabled = !caps.native_branch_available; this.copy.disabled = !caps.copy_available;
      this.status.textContent = (caps.native_branch_available ? "Native fork uses the latest provider history. " : "Native fork is unavailable on this connection. ") +
        "Copy recent context includes up to 8000 text characters from the last 100 events. Each branch uses its own workspace. " + caps.steering_reason;
    }).catch(error => { if (this.current(sid, generation)) this.status.textContent = error.message; });
  };
  Controller.prototype.branch = function (strategy) {
    if (this.busy || !this.widget.sessionId) return;
    const sid = this.widget.sessionId, generation = this.generation;
    const key = "pa:conversation-branch:" + sid + ":" + strategy;
    let requestId;
    try { requestId = localStorage.getItem(key) || crypto.randomUUID(); localStorage.setItem(key, requestId); }
    catch (_) { this.status.textContent = "Enable site storage to retain a branch retry identity."; return; }
    this.busy = true; this.branchError = false; this.native.disabled = this.copy.disabled = true;
    this.setEmptyDisabled(true);
    if (this.panel) this.panel.open = true;
    if (this.listStatus) this.listStatus.textContent = "Preparing thread…";
    this.status.textContent = "Preparing an independent conversation. You can keep chatting here.";
    this.widget.api("/sessions/" + encodeURIComponent(sid) + "/branch", {
      method: "POST", headers: {"Idempotency-Key": requestId}, body: JSON.stringify({request_id: requestId, strategy: strategy})
    }).then(result => {
      // Keep the receipt identity until the human opens the branch. Reload/retry
      // joins this same result even if a network response was lost.
      if (!this.current(sid, generation)) return;
      const session = result.session;
      if (strategy === "empty") {
        // The acknowledged receipt can now be consumed. Session switching uses
        // the widget's existing draft, history and subscription isolation.
        localStorage.removeItem(key);
        this.widget.switchSession(session.id, true, session.origin_instance_id || "");
        return;
      }
      this.status.textContent = "Branch saved · " + session.status + ". ";
      const link = document.createElement("a"); link.textContent = "Open branch";
      link.href = "/agent?session=" + encodeURIComponent(session.id);
      if (session.origin_instance_id) link.href += "&instance=" + encodeURIComponent(session.origin_instance_id);
      link.addEventListener("click", () => { localStorage.removeItem(key); });
      this.status.appendChild(link);
      if (this.listStatus) this.listStatus.textContent = "Branch saved. Open it below or from the thread list.";
      clearTimeout(this.refreshTimer); this.refreshTimer = null;
      if (this.isChat) this.loadThreads();
    }).catch(error => {
      if (this.current(sid, generation)) {
        this.branchError = true;
        this.status.textContent = error.message + " Retry the same choice to recover its receipt; no automatic fallback is used.";
        if (this.listStatus) this.listStatus.textContent = this.status.textContent;
      }
    }).finally(() => {
      if (!this.current(sid, generation)) return;
      this.busy = false; this.copy.disabled = false;
      this.setEmptyDisabled(!this.isChat);
      this.native.disabled = !(this.capabilities && this.capabilities.native_branch_available);
    });
  };
  root.PAConversationThreads = Controller;
})(window);
