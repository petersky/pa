/* Branch requests have stable identities and never take over another thread. */
(function (root) {
  "use strict";
  function Controller(widget) {
    this.widget = widget; this.panel = widget.root.querySelector("[data-conversation-threads]");
    this.generation = 0; this.busy = false;
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
    this.generation++; this.busy = false; this.loaded = false; this.capabilities = null;
    if (!this.panel) return;
    this.native.disabled = this.copy.disabled = true;
    this.status.textContent = "Open to check branch support for this conversation.";
  };
  Controller.prototype.ready = function () { if (this.panel && this.panel.open && !this.loaded) this.load(); };
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
    this.busy = true; this.native.disabled = this.copy.disabled = true;
    this.status.textContent = "Preparing an independent conversation. You can keep chatting here.";
    this.widget.api("/sessions/" + encodeURIComponent(sid) + "/branch", {
      method: "POST", headers: {"Idempotency-Key": requestId}, body: JSON.stringify({request_id: requestId, strategy: strategy})
    }).then(result => {
      // Keep the receipt identity until the human opens the branch. Reload/retry
      // joins this same result even if a network response was lost.
      if (!this.current(sid, generation)) return;
      const session = result.session;
      this.status.textContent = "Branch saved · " + session.status + ". ";
      const link = document.createElement("a"); link.textContent = "Open branch";
      link.href = "/agent?session=" + encodeURIComponent(session.id);
      if (session.origin_instance_id) link.href += "&instance=" + encodeURIComponent(session.origin_instance_id);
      link.addEventListener("click", () => { localStorage.removeItem(key); });
      this.status.appendChild(link);
    }).catch(error => {
      if (this.current(sid, generation)) this.status.textContent = error.message + " Retry the same choice to recover its receipt; no automatic fallback is used.";
    }).finally(() => {
      if (!this.current(sid, generation)) return;
      this.busy = false; this.copy.disabled = false;
      this.native.disabled = !(this.capabilities && this.capabilities.native_branch_available);
    });
  };
  root.PAConversationThreads = Controller;
})(window);
