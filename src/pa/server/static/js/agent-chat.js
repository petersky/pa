/* Multi-session AgentChatWidget client (SSE + REST). */
(function () {
  "use strict";

  const MARKED_URL = "https://cdn.jsdelivr.net/npm/marked@15.0.7/marked.min.js";
  const PURIFY_URL = "https://cdn.jsdelivr.net/npm/dompurify@3.2.4/dist/purify.min.js";
  const IMAGE_TYPES = ["image/png", "image/jpeg", "image/gif", "image/webp"];
  const MAX_IMAGES = 4;
  const MAX_IMAGE_BYTES = 10 * 1024 * 1024;
  const MAX_TOTAL_IMAGE_BYTES = 20 * 1024 * 1024;
  const TRANSCRIPT_PAGE_LIMIT = 250;
  const TRANSCRIPT_LATENCY_BUDGET_MS = 2000;
  // Paint roughly 3–4 viewport pages of recent chat immediately; older pages
  // load upward on demand. SSE must not replay a 1,000-event history.
  const INITIAL_VISIBLE_EVENTS = 120;
  // Browser retention is deliberately smaller than durable history. These are
  // public through memoryDiagnostics() so layout/memory regressions can assert
  // the contract without sampling implementation-specific browser heaps.
  const MAX_RETAINED_EVENTS = 2000;
  const MAX_MESSAGE_ROWS = 600;
  const MAX_ACTIVITY_ROWS = 300;
  const MAX_PLANS = 50;
  const MAX_STREAM_CHARS = 250000;
  const SESSION_ROUTE_TIMEOUT_MS = 4000;
  const LIVE_SNAPSHOT_TIMEOUT_MS = 3000;
  const LIVE_STATE_RETRY_MS = 3000;
  const PROMPT_SUBMIT_TIMEOUT_MS = 2000;
  const PROMPT_RECONCILE_TIMEOUT_MS = 2000;
  const OWNER_ROUTE_MAX_RETRIES = 3;
  const SESSION_DOM_CACHE_LIMIT = 6;
  const SESSION_DOM_CACHE_TTL_MS = 30 * 60 * 1000;

  const sessionDomCache = new Map();
  let libsPromise = null;
  let detailTabsId = 0;

  function bindDetailTabs(root) {
    const group = root.querySelector("[data-acw-details]");
    if (!group || group.dataset.tabsReady) return;
    group.dataset.tabsReady = "1";
    const disclosures = Array.from(group.children);
    const panels = disclosures.map(function (disclosure) {
      const panel = document.createElement("div");
      disclosure.replaceWith(panel);
      panel.appendChild(disclosure);
      return panel;
    });
    const row = document.createElement("div");
    row.className = "acw-detail-tabs";
    row.setAttribute("role", "tablist");
    row.setAttribute("aria-label", "Conversation details");
    const prefix = "acw-details-" + (++detailTabsId);
    const tabs = panels.map(function (panel, index) {
      const summary = panel.querySelector("summary");
      const tab = document.createElement("button");
      tab.type = "button";
      tab.className = "ghost small";
      tab.id = prefix + "-tab-" + index;
      tab.setAttribute("role", "tab");
      tab.setAttribute("aria-selected", "false");
      tab.setAttribute("aria-expanded", "false");
      tab.tabIndex = index === 0 ? 0 : -1;
      // Move the live issue count with its label; panel controls remain intact.
      tab.append(...Array.from(summary.childNodes));
      summary.hidden = true;
      panel.id = prefix + "-panel-" + index;
      panel.setAttribute("role", "tabpanel");
      panel.setAttribute("aria-labelledby", tab.id);
      panel.tabIndex = 0;
      disclosures[index].open = false;
      panel.hidden = true;
      tab.setAttribute("aria-controls", panel.id);
      row.appendChild(tab);
      return tab;
    });
    function select(index) {
      const expand = !disclosures[index].open;
      panels.forEach(function (panel, i) {
        const selected = i === index && expand;
        disclosures[i].open = selected; // Preserve lazy loading through native toggle events.
        panel.hidden = !selected;
        tabs[i].setAttribute("aria-selected", String(selected));
        tabs[i].setAttribute("aria-expanded", String(selected));
        tabs[i].tabIndex = i === index ? 0 : -1;
      });
    }
    tabs.forEach(function (tab, index) {
      tab.addEventListener("click", function () { select(index); });
      tab.addEventListener("keydown", function (event) {
        let next;
        if (event.key === "ArrowRight") next = (index + 1) % tabs.length;
        else if (event.key === "ArrowLeft") next = (index + tabs.length - 1) % tabs.length;
        else if (event.key === "Home") next = 0;
        else if (event.key === "End") next = tabs.length - 1;
        else return; // Native button Enter/Space clicks activate or collapse.
        event.preventDefault();
        tabs.forEach(function (item, i) { item.tabIndex = i === next ? 0 : -1; });
        tabs[next].focus(); // Manual activation avoids fetching on arrow navigation.
      });
    });
    group.prepend(row);
  }

  function loadScript(src) {
    return new Promise(function (resolve, reject) {
      const existing = document.querySelector('script[src="' + src + '"]');
      if (existing) {
        if (existing.dataset.loaded === "1") return resolve();
        existing.addEventListener("load", function () { resolve(); });
        existing.addEventListener("error", reject);
        return;
      }
      const s = document.createElement("script");
      s.src = src;
      s.async = true;
      s.onload = function () {
        s.dataset.loaded = "1";
        resolve();
      };
      s.onerror = reject;
      document.head.appendChild(s);
    });
  }

  function ensureMarkdown() {
    if (!libsPromise) {
      libsPromise = Promise.all([loadScript(MARKED_URL), loadScript(PURIFY_URL)]).catch(function () {
        libsPromise = null;
      });
    }
    return libsPromise || Promise.resolve();
  }

  function renderMarkdownAsync(text, options) {
    return ensureMarkdown().then(function () { return renderMarkdown(text, options); });
  }

  function csrfHeaders() {
    const headers = { "Content-Type": "application/json", Accept: "application/json" };
    if (window.PACSRF) return window.PACSRF.headers(headers);
    const meta = document.querySelector('meta[name="csrf-token"]');
    if (meta && meta.content) headers["X-CSRF-Token"] = meta.content;
    return headers;
  }

  function escapeHtml(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function renderMarkdown(text, options) {
    const raw = String(text || "");
    const allowEmbeddedMedia = !options || options.allowEmbeddedMedia !== false;
    if (window.marked && window.DOMPurify) {
      try {
        const html = window.marked.parse(raw, { breaks: true });
        const forbiddenTags = ["style", "form", "input", "button", "textarea", "select", "option"];
        if (!allowEmbeddedMedia) {
          forbiddenTags.push("audio", "embed", "iframe", "object", "picture", "source", "track", "video");
        }
        const sanitizeConfig = {
          USE_PROFILES: { html: true },
          ADD_TAGS: ["audio", "iframe", "picture", "source", "track", "video"],
          ADD_ATTR: [
            "allow", "allowfullscreen", "controls", "loading", "poster", "preload",
            "referrerpolicy", "sandbox", "srcset"
          ],
          FORBID_TAGS: forbiddenTags,
          FORBID_ATTR: ["style"]
        };
        if (!allowEmbeddedMedia) sanitizeConfig.ADD_TAGS = [];
        const sanitized = window.DOMPurify.sanitize(html, sanitizeConfig);
        if (typeof document === "undefined" || typeof document.createElement !== "function") {
          return sanitized;
        }
        const media = document.createElement("template");
        media.innerHTML = sanitized;
        media.content.querySelectorAll("iframe").forEach(function (frame) {
          const sandbox = ["allow-forms", "allow-popups", "allow-presentation", "allow-scripts"];
          try {
            const source = new URL(frame.getAttribute("src") || "", window.location.href);
            if (source.origin !== window.location.origin) sandbox.push("allow-same-origin");
          } catch (_) {
            /* retain the stricter sandbox for malformed or relative sources */
          }
          frame.setAttribute("loading", "lazy");
          frame.setAttribute("referrerpolicy", "strict-origin-when-cross-origin");
          frame.setAttribute("sandbox", sandbox.join(" "));
        });
        return media.innerHTML;
      } catch (_) {
        /* fall through */
      }
    }
    return "<p>" + escapeHtml(raw).replace(/\n/g, "<br>") + "</p>";
  }

  /* Cursor often omits messageId and also drops the separator between
     successive thought/response segments (especially across tool calls).
     When a chunk abuts a sentence end with no whitespace, insert a break. */
  function streamChunkSeparator(prev, chunk) {
    if (!prev || !chunk) return "";
    const left = prev.charAt(prev.length - 1);
    const right = chunk.charAt(0);
    if (!left || !right) return "";
    if (/\s/.test(left) || /\s/.test(right)) return "";
    if (/[.!?]/.test(left) && /[A-Z"'“‘(\[]/.test(right)) return "\n\n";
    return "";
  }

  function formatElapsed(ms) {
    const s = Math.max(0, Math.floor(ms / 1000));
    const m = Math.floor(s / 60);
    const r = s % 60;
    return m > 0 ? m + "m " + r + "s" : r + "s";
  }

  function anchoredScrollTop(oldTop, oldHeight, newHeight) {
    return oldTop + Math.max(0, newHeight - oldHeight);
  }

  function apiErrorMessage(body, fallback) {
    const detail = body && body.detail;
    if (typeof detail === "string") return detail;
    if (detail && typeof detail === "object") {
      return detail.message || detail.detail || detail.code || fallback;
    }
    return fallback;
  }

  function apiErrorCode(error) {
    const detail = error && error.detail;
    return detail && typeof detail === "object" ? detail.code : "";
  }

  function AgentChatWidget(root) {
    this.root = root;
    this.sessionId = root.dataset.sessionId || "";
    this.ownerInstanceId = root.dataset.ownerInstanceId || "";
    this.currentInstanceId = root.dataset.currentInstanceId || "";
    this.sessionRoute = null;
    this.createLabel = root.dataset.createLabel || "default";
    this.cardId = root.dataset.cardId || "";
    this.apiBase = (root.dataset.apiBase || "/api/agent").replace(/\/$/, "");
    this.defaultApiBase = this.apiBase === "" ? "/api/agent" : this.apiBase;
    this.autoStart = root.dataset.autoStart !== "0";
    this._missingRestartAttempted = false;
    this.showThinking = root.dataset.showThinking !== "0";
    this.showSystem = root.dataset.showSystemPrompts === "1";
    this.showQueue = root.dataset.showQueue !== "0";
    this.showMetrics = root.dataset.showMetrics !== "0";
    this.showModel = root.dataset.showModel !== "0";
    this.showMode = root.dataset.showMode !== "0";
    this.preferredProvider = root.dataset.provider || "";

    this.els = {
      messages: root.querySelector("[data-acw-messages]"),
      loadOlder: root.querySelector("[data-acw-load-older]"),
      loadOlderLabel: root.querySelector("[data-acw-load-older-label]"),
      historySpinner: root.querySelector("[data-acw-history-spinner]"),
      loadOlderStatus: root.querySelector("[data-acw-load-older-status]"),
      loadNewer: root.querySelector("[data-acw-load-newer]"),
      loadNewerStatus: root.querySelector("[data-acw-load-newer-status]"),
      historyNotice: root.querySelector("[data-acw-history-notice]"),
      historyNoticeText: root.querySelector("[data-acw-history-notice-text]"),
      livePreview: root.querySelector("[data-acw-live-preview]"),
      latest: root.querySelector("[data-acw-latest]"),
      placeholder: root.querySelector("[data-acw-placeholder]"),
      form: root.querySelector("[data-acw-form]"),
      input: root.querySelector("[data-acw-input]"),
      commandMenu: root.querySelector("[data-acw-command-menu]"),
      commandState: root.querySelector("[data-acw-command-state]"),
      commandOptions: root.querySelector("[data-acw-command-options]"),
      attachments: root.querySelector("[data-acw-attachments]"),
      attach: root.querySelector("[data-acw-attach]"),
      fileInput: root.querySelector("[data-acw-file-input]"),
      send: root.querySelector("[data-acw-send]"),
      stop: root.querySelector("[data-acw-stop]"),
      systemToggle: root.querySelector("[data-acw-toggle-system]"),
      rawToggle: root.querySelector("[data-acw-toggle-raw]"),
      recover: root.querySelector("[data-acw-recover]"),
      restart: root.querySelector("[data-acw-restart]"),
      end: root.querySelector("[data-acw-end]"),
      sessionActionStatus: root.querySelector("[data-acw-session-action-status]"),
      history: root.querySelector("[data-acw-history]"),
      promote: root.querySelector("[data-acw-promote]"),
      working: root.querySelector("[data-acw-working]"),
      workingLabel: root.querySelector("[data-acw-working-label]"),
      turnTimer: root.querySelector("[data-acw-turn-timer]"),
      status: root.querySelector("[data-acw-status]"),
      title: root.querySelector("[data-acw-title]"),
      metrics: root.querySelector("[data-acw-metrics]"),
      recovery: root.querySelector("[data-acw-recovery]"),
      recoveryAction: root.querySelector("[data-acw-recovery-action]"),
      recoveryRetry: root.querySelector("[data-acw-retry]"),
      permissions: root.querySelector("[data-acw-permissions]"),
      queue: root.querySelector("[data-acw-queue]"),
      queueList: root.querySelector("[data-acw-queue-list]"),
      model: root.querySelector("[data-acw-model]"),
      mode: root.querySelector("[data-acw-mode]"),
      modelWrap: root.querySelector("[data-acw-model-wrap]"),
      modeWrap: root.querySelector("[data-acw-mode-wrap]"),
      config: root.querySelector("[data-acw-config]"),
      settingsForm: root.querySelector("[data-acw-settings-form]"),
      settingsApply: root.querySelector("[data-acw-settings-apply]"),
      settingsReset: root.querySelector("[data-acw-settings-reset]"),
      settingsStatus: root.querySelector("[data-acw-settings-status]"),
      terminalToggle: root.querySelector("[data-acw-terminal-toggle]"),
      terminalFlyout: root.querySelector("[data-acw-terminal-flyout]"),
      terminalOutput: root.querySelector("[data-acw-terminal-output]"),
      toolToggle: root.querySelector("[data-acw-tool-toggle]"),
      toolFlyout: root.querySelector("[data-acw-tool-flyout]"),
      toolActivity: root.querySelector("[data-acw-tool-activity]"),
      toolEmpty: root.querySelector("[data-acw-tool-empty]"),
      planToggle: root.querySelector("[data-acw-plan-toggle]"),
      planFlyout: root.querySelector("[data-acw-plan-flyout]"),
      planList: root.querySelector("[data-acw-plan-list]"),
      planDetail: root.querySelector("[data-acw-plan-detail]"),
      planCount: root.querySelector("[data-acw-plan-count]"),
      browserToggle: root.querySelector("[data-acw-browser-toggle]"),
      browser: root.querySelector("[data-acw-browser]"),
      browserUrl: root.querySelector("[data-acw-browser-url]"),
      browserGo: root.querySelector("[data-acw-browser-go]"),
      browserWidth: root.querySelector("[data-acw-browser-width]"),
      browserHeight: root.querySelector("[data-acw-browser-height]"),
      browserResize: root.querySelector("[data-acw-browser-resize]"),
      browserRefresh: root.querySelector("[data-acw-browser-refresh]"),
      browserDetach: root.querySelector("[data-acw-browser-detach]"),
      browserViewport: root.querySelector("[data-acw-browser-viewport]"),
      browserImage: root.querySelector("[data-acw-browser-image]"),
    };

    this.es = null;
    this.esSessionId = "";
    this.esApiBase = "";
    this.sseReconnectCount = 0;
    this.externalEventTransport = false;
    this.destroyed = false;
    // Selection/request lifetime is independent of EventSource lifetime.
    this.subscriptionGeneration = 0;
    this.routeAbortController = null;
    this.lastSeq = 0;
    this.liveGapTarget = 0;
    this.livePendingEvents = {};
    if (this.liveGapRetryTimer) clearTimeout(this.liveGapRetryTimer);
    this.liveGapRetryTimer = null;
    this.liveGapLoading = false;
    this.transcriptEvents = [];
    this.seenEvents = {};
    this.hasOlder = false;
    this.olderCursor = null;
    this.loadingOlder = false;
    this.olderAbortController = null;
    this.olderError = "";
    this.hasNewer = false;
    this.newerCursor = null;
    this.newerError = "";
    this.loadingNewer = false;
    this.streaming = {};
    this.messageRowCount = 0;
    this.toolTimers = {};
    this.currentActivity = null;
    this.activityStreams = {};
    this.activityCount = 0;
    this.activeToolIds = {};
    this.plans = [];
    this.lastSnapshot = null;
    this.turnStartedAt = null;
    this.turnTimerId = null;
    this.queuePaused = false;
    this.prompting = false;
    this.submissionPending = false;
    this.submissionState = "idle";
    this.submissionRetryVisible = false;
    this.submissionRetryReason = "";
    this.submissionReconcileId = null;
    this.providerId = root.dataset.provider || "";
    this.composerEnabled = true;
    this.turnActive = false;
    // Treat a preselected session as unavailable until its durable/live state
    // is known; this prevents server-rendered controls from briefly going stale.
    this.sessionClosed = !!this.sessionId;
    this.ownerResolutionPending = false;
    this.sessionRecoverable = false;
    this.durableHistoryAvailable = false;
    this.recoveryControlVisible = false;
    this.recoveryControlLabel = "Recover session";
    this.closePending = false;
    this.connectionNoticeShown = false;
    this.rawText = false;
    this.pendingImages = [];
    this.browserAttached = false;
    this.browserVisible = false;
    this.browserDeviceScaleFactor = 1;
    this.browserRefreshId = null;
    this.commandCatalog = null;
    this.commandCatalogSession = "";
    this.commandMatches = [];
    this.commandSelectedIndex = 0;
    this.commandLoading = false;
    this.commandError = "";
    this.activeExplanation = null;

    this.startupRetryId = null;
    this.startupRetryCount = 0;
    this.liveStateRetryId = null;
    this.liveStateRetryCount = 0;
    this._bind();
    bindDetailTabs(root);
    this.issues = window.PAConversationIssues ? new window.PAConversationIssues(this) : null;
    this.threads = window.PAConversationThreads ? new window.PAConversationThreads(this) : null;
    this.renderSessionActions();
    if (root.dataset.newChat === "1") {
      const draftUrl = new URL(window.location.href);
      let draftKey = draftUrl.searchParams.get("new");
      if (!draftKey || !/^[0-9a-f-]{36}$/i.test(draftKey)) {
        draftKey = crypto.randomUUID();
        draftUrl.searchParams.set("new", draftKey);
        window.history.replaceState({}, "", draftUrl);
      }
      root.dataset.draftPendingId = "pending:chat:" + draftKey;
      this.createLabel = "chat:" + draftKey;
      this.root.dataset.createLabel = this.createLabel;
      this.newChat = true;
    }
    this.drafts = window.PAAgentDrafts && window.PAAgentDrafts.installWidget
      ? window.PAAgentDrafts.installWidget(this) : null;
    const self = this;
    ensureMarkdown().then(function () { self.rerenderMarkdownBubbles(); });
    if (root.dataset.newChat === "1") {
      this.updateTitle("New chat");
      this.setPlaceholder("What would you like to discuss?");
      if (this.els.input) window.requestAnimationFrame(function () {
        self.els.input.focus();
        if (window.performance) {
          self.composerReadyMs = window.performance.now();
          self.renderTimings();
        }
      });
    }
    if (this.autoStart) this.init();
    else {
      this.setPlaceholder("Select or start a remote session.");
      this.setStatus("offline");
    }
  }

  AgentChatWidget.prototype._bind = function () {
    const self = this;
    if (this.els.loadOlder) {
      this.els.loadOlder.addEventListener("click", function () {
        self.loadOlderTranscript();
      });
    }
    if (this.els.loadNewer) {
      this.els.loadNewer.addEventListener("click", function () {
        self.loadNewerTranscript();
      });
    }
    if (this.els.latest) this.els.latest.addEventListener("click", function () {
      if (self.olderAbortController) self.olderAbortController.abort();
      self.olderAbortController = null;
      self.loadingOlder = false;
      self.historyNavigationGeneration = (self.historyNavigationGeneration || 0) + 1;
      self.loadingNewer = false;
      self.historyPreserveWindow = false;
      self.historyUserIntent = false;
      self.recentRetryCount = 0;
      self._loadRecentHistory(self.sessionId, self.subscriptionGeneration).catch(function () {});
    });
    if (this.els.messages) ["wheel", "touchstart", "keydown"].forEach(function (type) {
      self.els.messages.addEventListener(type, function () { self.historyUserIntent = true; }, {passive:true});
    });
    this._bindHistoryScrolling();
    if (this.els.form) {
      this.els.form.addEventListener("submit", function (e) {
        e.preventDefault();
        self.send("append");
      });
    }
    this.root.querySelectorAll("[data-acw-action]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        self.send(btn.getAttribute("data-acw-action") || "append");
        const details = btn.closest("details");
        if (details) details.open = false;
      });
    });
    if (this.els.stop) this.els.stop.addEventListener("click", function () { self.cancel(); });
    const end = this.els.end;
    if (end) end.addEventListener("click", function () { self.closeSession(); });
    const restart = this.els.restart;
    if (restart) restart.addEventListener("click", function () { self.restartSession(); });
    const cardLink = this.root.querySelector("[data-acw-card-link]");
    if (cardLink) cardLink.addEventListener("click", function () {
      openSessionCardDialog(self, cardLink);
    });
    if (this.els.recoveryRetry) {
      this.els.recoveryRetry.addEventListener("click", function () {
        self.retrySession();
      });
    }
    if (this.els.recover) this.els.recover.addEventListener("click", function () {
      self.recoverSession(self.sessionId).catch(function (err) {
        self.setPlaceholder("Session recovery is still unavailable: " + err.message);
      });
    });
    if (this.els.history) this.els.history.addEventListener("click", function () {
      self.loadDurableSession(self.sessionId).catch(function (err) {
        self.setPlaceholder("Failed to load durable history: " + err.message);
        self.setStatus("error");
      });
    });
    if (this.els.systemToggle) {
      this.els.systemToggle.checked = this.showSystem;
      this.root.classList.toggle("show-system", this.showSystem);
      this.els.systemToggle.addEventListener("change", function () {
        self.showSystem = self.els.systemToggle.checked;
        self.root.dataset.showSystemPrompts = self.showSystem ? "1" : "0";
        self.root.classList.toggle("show-system", self.showSystem);
      });
    }
    if (this.els.rawToggle) {
      this.els.rawToggle.addEventListener("change", function () {
        self.rawText = self.els.rawToggle.checked;
        self.rerenderMarkdownBubbles();
      });
    }
    const qp = this.root.querySelector("[data-acw-queue-pause]");
    if (qp) qp.addEventListener("click", function () { self.queueControl("pause"); });
    const qr = this.root.querySelector("[data-acw-queue-resume]");
    if (qr) qr.addEventListener("click", function () { self.queueControl("resume"); });
    if (this.els.model) {
      this.els.model.addEventListener("change", function () {
        self.markSettingsDirty();
        self.updateEffortChoices();
      });
    }
    if (this.els.mode) {
      this.els.mode.addEventListener("change", function () {
        self.markSettingsDirty();
      });
    }
    if (this.els.settingsForm) {
      this.els.settingsForm.addEventListener("submit", function (event) {
        event.preventDefault();
        self.applySettings();
      });
    }
    if (this.els.settingsReset) {
      this.els.settingsReset.addEventListener("click", function () { self.resetSettingsDraft(); });
    }
    const settingsMenu = this.root.querySelector(".acw-settings-menu");
    if (settingsMenu) {
      settingsMenu.addEventListener("toggle", function () {
        if (!settingsMenu.open && self.settingsDirty) {
          if (!window.confirm("Discard unsaved Agent settings changes?")) settingsMenu.open = true;
          else self.resetSettingsDraft();
        }
      });
    }
    const renameInput = this.root.querySelector("[data-acw-rename-title]");
    [["[data-acw-rename]", false], ["[data-acw-auto-title]", true]].forEach(function (entry) {
      const button = self.root.querySelector(entry[0]);
      if (button) button.addEventListener("click", function () {
        if (!self.sessionId) return;
        const sessionId = self.sessionId;
        self.api("/sessions/" + encodeURIComponent(sessionId) + "/title", {
          method: "PUT", body: JSON.stringify({title: renameInput && renameInput.value, automatic: entry[1]})
        }).then(function (snap) {
          if (self.sessionId === sessionId) self.updateTitle(snap.session.title);
          refreshSessionList(self.sessionId, true);
        }).catch(function (error) { self.addBubble("system", error.message, null, {forceVisible: true}); });
      });
    });
    if (this.els.terminalToggle) this.els.terminalToggle.addEventListener("click", function () { self.toggleFlyout("terminal"); });
    if (this.els.toolToggle) this.els.toolToggle.addEventListener("click", function () { self.toggleFlyout("tool"); });
    if (this.els.planToggle) this.els.planToggle.addEventListener("click", function () { self.toggleFlyout("plan"); });
    this.root.querySelectorAll("[data-acw-flyout-close]").forEach(function (button) {
      button.addEventListener("click", function () { self.closeFlyouts(); });
    });
    if (this.els.input) {
      this.els.input.addEventListener("keydown", function (e) {
        if (self.commandMenuOpen()) {
          if (e.key === "ArrowDown" || e.key === "ArrowUp") {
            e.preventDefault();
            self.moveCommandSelection(e.key === "ArrowDown" ? 1 : -1);
            return;
          }
          if (e.key === "Escape") {
            e.preventDefault();
            self.closeCommandMenu();
            return;
          }
          if (e.key === "Enter" && !e.shiftKey && !e.isComposing && e.keyCode !== 229) {
            e.preventDefault();
            self.selectCommand(self.commandMatches[self.commandSelectedIndex]);
            return;
          }
        }
        if (e.key === "Enter" && !e.shiftKey && !e.isComposing && e.keyCode !== 229) {
          e.preventDefault();
          self.send(self.prompting ? "append" : "append");
        }
      });
      this.els.input.addEventListener("input", function () {
        self.updateCommandMenu();
      });
      ["dragenter", "dragover"].forEach(function (name) {
        self.els.input.addEventListener(name, function (e) {
          if (!self._hasImageFiles(e.dataTransfer)) return;
          e.preventDefault();
          self.els.input.classList.add("is-image-drop-target");
        });
      });
      this.els.input.addEventListener("dragleave", function () {
        self.els.input.classList.remove("is-image-drop-target");
      });
      this.els.input.addEventListener("drop", function (e) {
        if (!self._hasImageFiles(e.dataTransfer)) return;
        e.preventDefault();
        self.els.input.classList.remove("is-image-drop-target");
        self.addImageFiles(e.dataTransfer.files);
      });
    }
    if (this.els.attach && this.els.fileInput) {
      this.els.attach.addEventListener("click", function () {
        self.els.fileInput.click();
      });
      this.els.fileInput.addEventListener("change", function () {
        self.addImageFiles(self.els.fileInput.files);
        self.els.fileInput.value = "";
      });
    }
    if (this.els.browserToggle) this.els.browserToggle.addEventListener("click", function () {
      if (self.browserAttached) self.setBrowserVisible(!self.browserVisible);
      else self.attachBrowser();
    });
    if (this.els.browserGo) this.els.browserGo.addEventListener("click", function () { self.navigateBrowser(); });
    if (this.els.browserRefresh) this.els.browserRefresh.addEventListener("click", function () { self.refreshBrowser(); });
    if (this.els.browserResize) this.els.browserResize.addEventListener("click", function () { self.resizeBrowser(); });
    if (this.els.browserDetach) this.els.browserDetach.addEventListener("click", function () { self.detachBrowser(); });
    if (this.els.browserUrl) this.els.browserUrl.addEventListener("keydown", function (e) { if (e.key === "Enter") { e.preventDefault(); self.navigateBrowser(); } });
    if (this.els.browserImage) this.els.browserImage.addEventListener("click", function (e) {
      if (!self.browserAttached) return;
      const rect = self.els.browserImage.getBoundingClientRect();
      const scale = self.browserDeviceScaleFactor || 1;
      const x = (e.clientX - rect.left) * (self.els.browserImage.naturalWidth / rect.width) / scale;
      const y = (e.clientY - rect.top) * (self.els.browserImage.naturalHeight / rect.height) / scale;
      self.browserApi("/click", { method: "POST", body: JSON.stringify({ x: x, y: y }) }).then(function () { setTimeout(function () { self.refreshBrowser(); }, 250); });
    });
  };

  AgentChatWidget.prototype.api = function (path, opts) {
    opts = opts || {};
    const requestOptions = Object.assign({
      headers: csrfHeaders(),
      credentials: "same-origin",
    }, opts);
    requestOptions.headers = Object.assign({}, csrfHeaders(), opts.headers || {});
    const request = window.PACSRF ? window.PACSRF.fetch : fetch;
    return request(this.apiBase + path, requestOptions).then(function (res) {
      if (!res.ok) {
        return res.json().catch(function () { return {}; }).then(function (body) {
          const error = new Error(apiErrorMessage(body, res.statusText || "Request failed"));
          error.status = res.status;
          error.detail = body.detail;
          error.csrfRecoveryFailed = Boolean(res.paCsrfRecoveryFailed);
          if (window.PASessionRecovery) {
            error.retryAfterMs = window.PASessionRecovery.responseRetryAfterMs(
              res, error.detail
            );
          }
          throw error;
        });
      }
      if (res.status === 204) return null;
      return res.json();
    });
  };

  AgentChatWidget.prototype.apiWithTimeout = function (path, timeoutMs, opts) {
    opts = opts || {};
    const controller = new AbortController();
    const deadline = new Promise(function (_resolve, reject) {
      controller._deadlineTimer = setTimeout(function () {
        controller.abort();
        const error = new Error("Request exceeded its " + timeoutMs + "ms latency budget.");
        error.code = "request_timeout";
        error.timeout = true;
        reject(error);
      }, timeoutMs);
    });
    const timeoutId = controller._deadlineTimer;
    if (timeoutId && typeof timeoutId.unref === "function") timeoutId.unref();
    if (opts.signal) {
      if (opts.signal.aborted) controller.abort();
      else opts.signal.addEventListener("abort", function () { controller.abort(); }, { once: true });
    }
    const requestOpts = Object.assign({}, opts, { signal: controller.signal });
    return Promise.race([this.api(path, requestOpts), deadline])
      .catch(function (error) {
        if (error && error.name === "AbortError") {
          const timeoutError = new Error(
            "Request exceeded its " + timeoutMs + "ms latency budget."
          );
          timeoutError.code = "request_timeout";
          timeoutError.timeout = true;
          throw timeoutError;
        }
        throw error;
      })
      .finally(function () { clearTimeout(timeoutId); });
  };
  AgentChatWidget.prototype.retryAfterStartupRecovery = function (error) {
    const code = apiErrorCode(error);
    if (code !== "agent_recovery_in_progress") return false;
    if (this.destroyed || !this.root.isConnected) return true;
    const detail = error.detail || {};
    const delay = window.PASessionRecovery
      ? window.PASessionRecovery.retryDelayMs(error, this.startupRetryCount++, {
          minimumMs: 250,
          maximumMs: 30000,
        })
      : Math.min(
          30000,
          Math.max(250, Number(detail.retry_after_ms || 250)) *
            Math.pow(2, Math.min(8, this.startupRetryCount++))
        );
    this.setPlaceholder("Restoring durable agent sessions…");
    this.setStatus("starting");
    if (this.startupRetryId) clearTimeout(this.startupRetryId);
    const self = this;
    this.startupRetryId = setTimeout(function () {
      self.startupRetryId = null;
      if (self.destroyed || !self.root.isConnected) return;
      self.init();
    }, delay);
    return true;
  };


  AgentChatWidget.prototype.showRecoveryActions = function (detail) {
    detail = detail || {};
    this.sessionRecoverable = detail.recoverable === true;
    this.durableHistoryAvailable = !!detail.history_url;
    this.recoveryControlVisible = false;
    this.renderSessionActions();
  };

  AgentChatWidget.prototype.renderSessionActions = function (state) {
    state = state || {};
    const terminal = Object.prototype.hasOwnProperty.call(state, "terminal")
      ? !!state.terminal : !!this.sessionClosed;
    const recoverable = Object.prototype.hasOwnProperty.call(state, "recoverable")
      ? !!state.recoverable : !!this.sessionRecoverable;
    const ownerUnreachable = !!(this.sessionRoute &&
      (this.sessionRoute.state === "owner_unreachable" || this.sessionRoute.state === "live_degraded"));
    const hasSession = !!this.sessionId;
    const presentation = (this.lastSnapshot && this.lastSnapshot.presentation) || {};
    const isChat = !presentation.purpose || presentation.purpose === "chat";
    const archived = !!(presentation.archive && presentation.archive.archived);
    const canEnd = hasSession && isChat && !archived && !terminal && !ownerUnreachable;
    const canRestart = hasSession && terminal && recoverable;
    if (this.els.end) {
      this.els.end.hidden = !canEnd;
      this.els.end.disabled = !canEnd || this.closePending;
      this.els.end.setAttribute("aria-disabled", this.els.end.disabled ? "true" : "false");
      this.els.end.textContent = "Archive conversation";
      this.els.end.title = this.closePending
        ? "Archive request is in progress."
        : "Archive this conversation without deleting its history.";
    }
    if (this.els.restart) {
      this.els.restart.hidden = !canRestart;
      this.els.restart.disabled = !canRestart;
      this.els.restart.setAttribute("aria-disabled", canRestart ? "false" : "true");
      this.els.restart.title = canRestart
        ? "Resume this ended session using its existing durable provider identity. This does not restart PA."
        : "Session resume is unavailable.";
    }
    if (this.els.recover) {
      const showRecover = hasSession && this.recoveryControlVisible && !canRestart;
      this.els.recover.hidden = !showRecover;
      this.els.recover.disabled = !showRecover;
      this.els.recover.setAttribute("aria-disabled", showRecover ? "false" : "true");
      this.els.recover.textContent = this.recoveryControlLabel;
    }
    if (this.els.history) this.els.history.hidden = !this.durableHistoryAvailable;
    if (this.els.sessionActionStatus) {
      this.els.sessionActionStatus.textContent = this.closePending
        ? "Archiving this conversation…"
        : canRestart
          ? "This session has ended and can be resumed from durable state."
          : terminal
            ? "This session has ended or is not currently connected."
            : ownerUnreachable
              ? "Live session controls are unavailable while the owner cannot be reached."
              : canEnd ? "This conversation is available." : "No session is selected.";
    }
  };

  AgentChatWidget.prototype._apiBaseForOwner = function (ownerInstanceId) {
    const owner = String(ownerInstanceId || "");
    if (!owner) return this.defaultApiBase || "/api/agent";
    // Local ownership must never go through the fleet self-proxy. An empty
    // currentInstanceId used to treat every owner as remote, which after a
    // network change left the chat stuck proxying via a stale instance URL.
    if (!this.currentInstanceId || owner === this.currentInstanceId) {
      return "/api/agent";
    }
    return "/api/fleet/instances/" + encodeURIComponent(owner) + "/agent";
  };

  AgentChatWidget.prototype.clearSelectedSession = function () {
    clearTimeout(this.queueRefreshTimer);
    if (this.issues) this.issues.reset();
    if (this.threads) this.threads.reset();
    this.submitAckMs = null;
    this.turnTiming = null; this.startupTiming = null; this.renderTimings();
    this.subscriptionGeneration += 1;
    this.closeSSE("session-cleared");
    if (this.liveStateRetryId) clearTimeout(this.liveStateRetryId);
    this.liveStateRetryId = null;
    if (this.startupRetryId) clearTimeout(this.startupRetryId);
    this.startupRetryId = null;
    if (this.routeAbortController) {
      this.routeAbortController.abort();
      this.routeAbortController = null;
    }
    this.sessionId = "";
    this.ownerInstanceId = "";
    this.sessionRoute = null;
    this.root.dataset.sessionId = "";
    this.root.dataset.ownerInstanceId = "";
    this.apiBase = this.defaultApiBase || "/api/agent";
    this.root.dataset.apiBase = this.apiBase;
    this.lastSeq = 0;
    this.liveGapTarget = 0;
    this.livePendingEvents = {};
    if (this.liveGapRetryTimer) clearTimeout(this.liveGapRetryTimer);
    this.liveGapRetryTimer = null;
    this.liveGapLoading = false;
    this.sessionClosed = true;
    this.sessionRecoverable = false;
    this.durableHistoryAvailable = false;
    this.recoveryControlVisible = false;
    this.setTurnActive(false);
    this.setStatus("offline");
    this.setComposerEnabled(false);
    this.showRecoveryActions({});
    this.renderSessionActions({ terminal: true, recoverable: false });
    if (this.root.closest(".page-agent")) {
      const url = new URL(window.location.href);
      url.searchParams.delete("session");
      url.searchParams.delete("instance");
      window.history.replaceState({}, "", url);
    }
  };

  AgentChatWidget.prototype._isCurrentSessionRequest = function (sessionId, generation) {
    return !this.destroyed && sessionId === this.sessionId &&
      (generation == null || generation === this.subscriptionGeneration);
  };

  AgentChatWidget.prototype.loadDurableSession = function (sessionId, generation) {
    if (!sessionId) return Promise.reject(new Error("No session selected"));
    const requestGeneration = generation == null
      ? this.subscriptionGeneration
      : generation;
    if (!this._isCurrentSessionRequest(sessionId, requestGeneration)) {
      return Promise.resolve(null);
    }
    const self = this;
    return this.apiWithTimeout("/history/" + encodeURIComponent(sessionId) +
      "?message_boundaries=true&limit=" + TRANSCRIPT_PAGE_LIMIT, LIVE_SNAPSHOT_TIMEOUT_MS).then(function (history) {
      if (!self._isCurrentSessionRequest(sessionId, requestGeneration)) return null;
      const snap = {
        session: history.session,
        execution_selection: history.execution_selection,
        selection_history_only: true,
        presentation: history.presentation || {},
        transcript: history.events || [],
        transcript_page: history.page || {},
        prompting: false,
        connected: false,
        queue: [],
        queue_paused: false,
        pending_permissions: [],
        metrics: history.session && history.session.metrics_json || {},
      };
      self.sessionId = sessionId;
      self.root.dataset.sessionId = sessionId;
      self.applySnapshot(snap);
      self.sessionClosed = true;
      self.setComposerEnabled(false);
      self.setStatus("offline");
      const recovery = history.recovery || {};
      self.showRecoveryActions({ recoverable: recovery.recoverable, history_url: "/api/agent/history/" + sessionId });
      self.renderSessionActions({ terminal: true, recoverable: recovery.recoverable === true });
      return history;
    });
  };

  AgentChatWidget.prototype.resolveSessionNotLive = function (error, sessionId, generation) {
    const detail = error && typeof error.detail === "object" ? error.detail : {};
    const code = apiErrorCode(error);
    if (code !== "session_not_live" && code !== "session_deleted") return Promise.reject(error);
    const requestGeneration = generation == null
      ? this.subscriptionGeneration
      : generation;
    if (!this._isCurrentSessionRequest(sessionId, requestGeneration)) {
      return Promise.resolve(null);
    }
    const self = this;
    if (code === "session_not_live" && detail.recoverable === true) {
      this.showRecoveryActions(detail);
      return this.recoverSession(sessionId, requestGeneration).catch(function (recoveryError) {
        if (!self._isCurrentSessionRequest(sessionId, requestGeneration)) return null;
        return self.loadDurableSession(sessionId, requestGeneration).then(function (history) {
          if (!history || !self._isCurrentSessionRequest(sessionId, requestGeneration)) return null;
          self.addBubble(
            "system",
            "Provider recovery is still unavailable: " + recoveryError.message,
            new Date().toISOString(),
            { system: true, forceVisible: true }
          );
          return null;
        });
      });
    }
    const durable = detail.durable_session || {};
    if (durable.exists && detail.history_url) {
      return this.loadDurableSession(sessionId, requestGeneration).catch(function () {
        if (!self._isCurrentSessionRequest(sessionId, requestGeneration)) return null;
        self.clearSelectedSession();
        return null;
      });
    }
    this.clearSelectedSession();
    refreshSessionList(null);
    if (code === "session_deleted" && this.autoStart && !this._missingRestartAttempted) {
      this._missingRestartAttempted = true;
      setTimeout(function () { self.init(); }, 0);
    }
    return Promise.resolve(null);
  };

  AgentChatWidget.prototype.init = function () {
    const self = this;
    if (this.destroyed || !this.root.isConnected) return;
    if (this.sessionId) {
      this.openSession(this.sessionId, this.ownerInstanceId, { replace: true }).catch(function () {});
      return;
    }
    const body = {
      attach_default: this.createLabel === "default" && !this.cardId,
      label: this.createLabel,
      card_id: this.cardId || null,
      title: this.cardId ? "Card agent" : null,
    };
    if (this.root.dataset.fresh === "1") body.fresh = true;
    if (this.preferredProvider) body.provider = this.preferredProvider;
    const boot = this.api("/sessions", {
      method: "POST",
      body: JSON.stringify(body),
    });

    const generation = this.subscriptionGeneration;
    boot
      .then(function (snap) {
        if (self.destroyed || generation !== self.subscriptionGeneration) return;
        self.startupRetryCount = 0;
        const sid = (snap.session && snap.session.id) || (snap.id) || self.sessionId;
        if (self.drafts) self.drafts.promoteSession(sid);
        self.sessionId = sid;
        self.root.dataset.sessionId = sid;
        if (self.newChat) {
          const url = new URL(window.location.href);
          url.searchParams.delete("new");
          url.searchParams.set("session", sid);
          window.history.replaceState({}, "", url);
          self.newChat = false;
        }
        return self.openSession(sid, "", { replace: true });
      })
      .catch(function (err) {
        if (self.destroyed) return;
        if (self.retryAfterStartupRecovery(err)) return;
        const code = apiErrorCode(err);
        if (code === "session_not_live" || code === "session_deleted") {
          return self.resolveSessionNotLive(err, self.sessionId);
        }
        self.setPlaceholder("Failed to start session: " + err.message);
        self.setStatus("error");
      });
  };

  AgentChatWidget.prototype.resolveSessionRoute = function (sessionId, ownerInstanceId) {
    let url = "/api/fleet/session-route/" + encodeURIComponent(sessionId);
    if (ownerInstanceId) {
      url += "?owner_instance_id=" + encodeURIComponent(ownerInstanceId);
    }
    if (this.routeAbortController) this.routeAbortController.abort();
    const self = this;
    const controller = new AbortController();
    let rejectDeadline;
    const deadline = new Promise(function (_resolve, reject) {
      rejectDeadline = reject;
    });
    const timeoutId = setTimeout(function () {
      controller.abort();
      rejectDeadline(new Error("Session owner lookup exceeded its latency budget."));
    }, SESSION_ROUTE_TIMEOUT_MS);
    this.routeAbortController = controller;
    const lookup = fetch(url, {
      headers: csrfHeaders(),
      credentials: "same-origin",
      signal: controller.signal,
    })
      .then(function (res) {
        if (!res.ok) {
          return res.json().catch(function () { return {}; }).then(function (body) {
            throw new Error(apiErrorMessage(body, res.statusText || "Could not resolve session owner"));
          });
        }
        return res.json();
      })
      .catch(function (error) {
        if (error && error.name === "AbortError") {
          throw new Error("Session owner lookup exceeded its latency budget.");
        }
        throw error;
      });
    // Some browser/network combinations do not settle a fetch promptly after
    // AbortController.abort(). Keep owner resolution independently bounded so
    // the composer cannot remain stuck in its locating state forever.
    return Promise.race([lookup, deadline])
      .finally(function () {
        clearTimeout(timeoutId);
        if (self.routeAbortController === controller) self.routeAbortController = null;
      });
  };

  AgentChatWidget.prototype._historySnapshot = function (history) {
    return {
      session: history.session,
      execution_selection: history.execution_selection,
      selection_history_only: true,
      presentation: history.presentation || {},
      transcript: history.events || [],
      transcript_page: history.page || {},
      prompting: false,
      connected: false,
      queue: [],
      queue_paused: false,
      pending_permissions: [],
      metrics: history.session && history.session.metrics_json || {},
    };
  };

  AgentChatWidget.prototype._applyDurableHistory = function (sessionId, history, generation) {
    if (this.destroyed || generation !== this.subscriptionGeneration) return null;
    this.applySnapshot(this._historySnapshot(history));
    this.sessionId = sessionId;
    this.root.dataset.sessionId = sessionId;
    this.setComposerEnabled(false);
    return history;
  };

  AgentChatWidget.prototype._scheduleLiveStateRetry = function (sessionId, generation, reroute) {
    if (this.liveStateRetryId) clearTimeout(this.liveStateRetryId);
    const self = this;
    const attempt = this.liveStateRetryCount || 0;
    const delay = Math.min(
      30000,
      LIVE_STATE_RETRY_MS * Math.pow(2, Math.min(4, attempt))
    );
    this.liveStateRetryCount = attempt + 1;
    this.liveStateRetryId = setTimeout(function () {
      self.liveStateRetryId = null;
      if (self.destroyed || generation !== self.subscriptionGeneration) return;
      if (reroute) {
        self.openSession(sessionId, self.ownerInstanceId, { replace: true }).catch(function () {});
      } else {
        self._loadLiveSnapshot(sessionId, generation);
      }
    }, delay);
  };

  AgentChatWidget.prototype._cacheKey = function (sessionId) {
    return String(sessionId || "") + "::" + String(this.apiBase || "");
  };

  AgentChatWidget.prototype._bindHistoryScrolling = function () {
    const self = this, messages = this.els.messages;
    if (!messages) return;
    let intent = null, touchY = null;
    const loadAtTop = function () {
      if (!intent || performance.now() > intent.until || intent.generation !== self.subscriptionGeneration) return;
      if (messages.scrollTop > 80) return;
      intent = null; // One page per gesture; prepend/restore scrolls cannot cascade.
      if (!self.destroyed && !self.recentBootstrap && !self.loadingOlder && !self.olderError && self.hasOlder) self.loadOlderTranscript();
    };
    const gesture = function (upward) {
      intent = upward && !self.recentBootstrap && !self.loadingOlder && !self.olderError
        ? {generation:self.subscriptionGeneration, until:performance.now()+750} : null;
      // Also handle an upward gesture when already at the top (no scroll event).
      if (intent) (window.requestAnimationFrame || function(fn){setTimeout(fn,0);})(loadAtTop);
    };
    messages.addEventListener("wheel", function(e) { if (e.deltaY) gesture(e.deltaY < 0); }, {passive:true});
    messages.addEventListener("keydown", function(e) {
      if (e.altKey || e.ctrlKey || e.metaKey) return;
      if (["ArrowUp", "PageUp", "Home"].includes(e.key) || (e.key === " " && e.shiftKey)) gesture(true);
      else if (["ArrowDown", "PageDown", "End", " "].includes(e.key)) gesture(false);
    });
    messages.addEventListener("touchstart", function(e) { touchY = e.touches.length === 1 ? e.touches[0].clientY : null; }, {passive:true});
    messages.addEventListener("touchmove", function(e) {
      const next = e.touches.length === 1 ? e.touches[0].clientY : null;
      if (touchY !== null && next !== null && next !== touchY) gesture(next > touchY);
      touchY = next;
    }, {passive:true});
    messages.addEventListener("scroll", function () {
      loadAtTop();
      self._resumeHistoryAtTail();
    }, {passive:true});
  };

  AgentChatWidget.prototype._resumeHistoryAtTail = function () {
    // hasNewer describes a paged/evicted window, not buffered live arrivals.
    // Its bottom is still older history; only a window containing the tail can
    // resume live delivery without replacing the reader's position.
    if (!this.historyPreserveWindow || this.hasNewer || this.recentBootstrap ||
        this.loadingOlder || this.loadingNewer || !this.isNearBottom()) return;
    this.historyPreserveWindow = false;
    this.historyUserIntent = false;
    this.updateNewerControl();
    this._renderHistoryNotice();
    this._repairLiveGap();
  };

  AgentChatWidget.prototype._captureHistoryAnchor = function () {
    const container = this.els && this.els.messages;
    if (!container || !container.getBoundingClientRect) return null;
    const top = container.getBoundingClientRect().top;
    const row = Array.from(container.querySelectorAll(".acw-msg[data-source-seq]")).find(function(e) {
      return e.getBoundingClientRect().bottom > top;
    });
    return row ? {seq:row.dataset.sourceSeq, offset:row.getBoundingClientRect().top-top} : null;
  };

  AgentChatWidget.prototype._restoreHistoryAnchor = function (anchor) {
    const container = this.els && this.els.messages;
    if (!anchor || !container || !container.getBoundingClientRect) return;
    const row = Array.from(container.querySelectorAll(".acw-msg[data-source-seq]")).find(function(e) { return e.dataset.sourceSeq === anchor.seq; });
    if (row) container.scrollTop += row.getBoundingClientRect().top - container.getBoundingClientRect().top - anchor.offset;
  };

  AgentChatWidget.prototype._stashSessionDomCache = function () {
    if (!this.sessionId || !this.els || !this.els.messages) return;
    const key = this._cacheKey(this.sessionId);
    const messages = [];
    Array.from(this.els.messages.children).forEach(function (child) {
      if (
        child.hasAttribute("data-acw-placeholder") ||
        child.hasAttribute("data-acw-load-older") ||
        child.hasAttribute("data-acw-load-older-status") ||
        child.hasAttribute("data-acw-load-newer") ||
        child.hasAttribute("data-acw-load-newer-status")
      ) return;
      messages.push(child.cloneNode(true));
    });
    if (!messages.length && !this.transcriptEvents.length) return;
    sessionDomCache.set(key, {
      capturedAt: Date.now(),
      lastSeq: this.lastSeq,
      transcriptEvents: this._compactEvents(this.transcriptEvents),
      olderError: this.olderError,
      newerError: this.newerError,
      hasOlder: this.hasOlder,
      olderCursor: this.olderCursor,
      hasNewer: this.hasNewer,
      newerCursor: this.newerCursor,
      providerId: this.providerId,
      unsequencedHtml: messages.filter(function (node) {
        return !node.dataset.sourceSeq;
      }).map(function (node) { return node.outerHTML; }).join(""),
      scrollTop: this.els.messages.scrollTop,
      anchor: this._captureHistoryAnchor(),
      nearBottom: this.isNearBottom(),
      recentPending: !!this.recentBootstrap,
      readingOlder: !!this.hasNewer || (!this.isNearBottom() && (!!this.historyPreserveWindow || !!this.historyUserIntent)),
    });
    while (sessionDomCache.size > SESSION_DOM_CACHE_LIMIT) {
      const oldest = sessionDomCache.keys().next().value;
      sessionDomCache.delete(oldest);
    }
  };

  AgentChatWidget.prototype._restoreSessionDomCache = function (sessionId) {
    const entry = sessionDomCache.get(this._cacheKey(sessionId));
    if (!entry) return false;
    if (Date.now() - entry.capturedAt > SESSION_DOM_CACHE_TTL_MS) {
      sessionDomCache.delete(this._cacheKey(sessionId));
      return false;
    }
    this.transcriptEvents = (entry.transcriptEvents || []).slice();
    this.lastSeq = entry.lastSeq || 0;
    this.hasOlder = !!entry.hasOlder;
    this.olderError = entry.olderError || "";
    this.newerError = entry.newerError || "";
    this.olderCursor = entry.olderCursor || null;
    this.hasNewer = !!entry.hasNewer;
    this.newerCursor = entry.newerCursor || null;
    this.providerId = entry.providerId || this.providerId;
    this.historyPreserveWindow = !entry.recentPending && !!entry.readingOlder && (!entry.nearBottom || !!entry.hasNewer);
    this.renderTranscript(this.transcriptEvents, { scrollBottom: !!entry.nearBottom });
    if (entry.unsequencedHtml && this.els.messages) {
      const holder = document.createElement("div");
      holder.innerHTML = entry.unsequencedHtml;
      while (holder.firstChild) {
        const row = holder.firstChild;
        const user = row.querySelector(".acw-bubble-user");
        if (user && this._isDuplicateUserBubble(user.dataset.markdown || "")) row.remove();
        else this.els.messages.appendChild(row);
      }
      this.messageRowCount = this.els.messages.querySelectorAll(".acw-msg").length;
    }
    if (!entry.nearBottom && this.els.messages) this.els.messages.scrollTop = entry.scrollTop || 0;
    this.updateOlderControl();
    this.updateNewerControl();
    if (this.historyPreserveWindow) this._restoreHistoryAnchor(entry.anchor);
    return this.transcriptEvents.length > 0 && !entry.recentPending;
  };

  AgentChatWidget.prototype._validateHistoryPage = function (history) {
    if (!history || !Array.isArray(history.events) || !history.page ||
        typeof history.page.has_older !== "boolean" || typeof history.page.has_newer !== "boolean") {
      throw new Error("History response is incomplete. Retry loading messages.");
    }
    return history;
  };

  AgentChatWidget.prototype._paintRecentHistory = function (history, generation) {
    if (this.destroyed) return null;
    if (
      generation != null &&
      this.subscriptionGeneration != null &&
      generation !== this.subscriptionGeneration
    ) {
      return null;
    }
    this._validateHistoryPage(history);
    const events = history.events;
    const page = history.page;
    if (history.session && history.session.id !== this.sessionId) throw new Error("History belongs to another session.");
    this.olderError = "";
    this.newerError = "";
    if (this.historyPreserveWindow && this.transcriptEvents.length) {
      const anchor = this._captureHistoryAnchor();
      this.hasNewer = !!page.has_newer || (page.newest_seq || 0) > (this.newerCursor || this.lastSeq || 0);
      this.updateOlderControl(); this.updateNewerControl();
      this._restoreHistoryAnchor(anchor);
      return history;
    }
    const paint = events.slice();
    this.hasOlder = !!page.has_older;
    this.olderError = "";
    this.olderCursor = page.next_before_seq || page.oldest_seq ||
      (paint.length ? paint[0].seq : null);
    this.hasNewer = !!page.has_newer;
    this.newerCursor = page.newest_seq ||
      (paint.length ? paint[paint.length - 1].seq : null);
    if (history && history.session) this.providerId = history.session.agent_name || this.providerId;
    // A delayed history response must include live arrivals already painted.
    const arrivals = this.transcriptEvents.concat(Object.values(this.livePendingEvents || {})).filter(function (event) {
      return event.seq > (page.newest_seq || 0);
    });
    this.renderTranscript(paint, { scrollBottom: true, recentFirst: true });
    if (paint.length) {
      this.lastSeq = paint.reduce(function (max, event) {
        return event.seq && event.seq > max ? event.seq : max;
      }, this.lastSeq || 0);
    }
    if (history && history.session) {
      this.providerId = history.session.agent_name || this.providerId;
    }
    this.recentBootstrap = false;
    arrivals.sort(function(a,b){return a.seq-b.seq;}).forEach(function(event) { this.handleEvent(event, false, true); }, this);
    this._clearResolvedLiveGap();
    return history;
  };

  AgentChatWidget.prototype._recentHistory = function (sessionId, generation, prefetch) {
    const cached = this._pendingRecentHistory;
    if (cached && cached.sessionId === sessionId && cached.generation === generation && cached.apiBase === this.apiBase) {
      this._pendingRecentHistory = null;
      return cached.promise;
    }
    const entry = { sessionId: sessionId, generation: generation, apiBase: this.apiBase };
    const self = this;
    entry.promise = this.apiWithTimeout(
      "/history/" + encodeURIComponent(sessionId) +
        "?message_boundaries=true&limit=" + TRANSCRIPT_PAGE_LIMIT,
      LIVE_SNAPSHOT_TIMEOUT_MS
    ).catch(function (error) {
      if (self._pendingRecentHistory === entry) self._pendingRecentHistory = null;
      throw error;
    });
    if (prefetch) this._pendingRecentHistory = entry;
    return entry.promise;
  };

  AgentChatWidget.prototype._loadRecentHistory = function (sessionId, generation) {
    const self = this;
    const apiBase = this.apiBase;
    const current = function () { return self._isCurrentSessionRequest(sessionId, generation) && self.apiBase === apiBase; };
    const previous = this.recentLoad;
    if (previous && previous.sessionId === sessionId && previous.generation === generation && previous.apiBase === apiBase) return previous.promise;
    if (this.recentRetryTimer) clearTimeout(this.recentRetryTimer);
    this.recentRetryTimer = null;
    this.recentBootstrap = true;
    const entry = {sessionId:sessionId,generation:generation,apiBase:apiBase};
    this.recentLoad = entry;
    this._renderHistoryNotice();
    entry.promise = this._recentHistory(sessionId, generation).then(function (history) {
      if (!current()) return null;
      const result = self._paintRecentHistory(history, generation);
      self.recentBootstrap = false;
      self.recentRetryCount = 0;
      self.updateOlderControl();
      self._renderHistoryNotice();
      return result;
    }).catch(function (error) {
      if (current()) {
        self.olderError = "Recent history is temporarily unavailable. Retry recent messages.";
        self.updateOlderControl();
        self.recentRetryCount = (self.recentRetryCount || 0) + 1;
        self._renderHistoryNotice();
        if (self.recentRetryCount <= 5) {
          const timer = setTimeout(function () {
            if (!current() || self.recentRetryTimer !== timer) return;
            self.recentRetryTimer = null;
            self._loadRecentHistory(sessionId, generation).catch(function () {});
          }, Math.min(10000, 1000 * Math.pow(2, self.recentRetryCount - 1)));
          if (timer && typeof timer.unref === "function") timer.unref();
          self.recentRetryTimer = timer;
        }
      }
      throw error;
    }).finally(function () {
      if (self.recentLoad === entry) self.recentLoad = null;
    });
    return entry.promise;
  };

  AgentChatWidget.prototype._renderHistoryNotice = function () {
    if (!this.els || !this.els.historyNotice) return;
    const pending = Object.values(this.livePendingEvents || {}).sort(function(a,b){return a.seq-b.seq;});
    const gap = !!this.recentBootstrap || !!this.newerError || (!this.historyPreserveWindow && this.liveGapTarget > this.lastSeq);
    this.els.historyNotice.hidden = !gap && !this.historyPreserveWindow && !this.hasNewer;
    this.els.historyNoticeText.textContent = gap
      ? (this.olderError || "Loading recent history…") + " Live preview may be partial until history recovers."
      : "Reading earlier messages. Recent messages are available separately.";
    const useful = [], streams = new Map();
    pending.forEach(function(e) {
      if (e.seq <= Math.max(this.lastSeq || 0, this.initialLiveCursor || 0)) return;
      const p = e.payload || {};
      if (e.type === "agent_message_chunk") {
        const key = JSON.stringify([p.message_id || "agent", p.phase || ""]);
        let stream = streams.get(key);
        if (!stream) { stream = {text:""}; streams.set(key, stream); useful.push(stream); }
        const next = String(p.text || "");
        if (["snapshot", "replace", "replacement", "accumulated"].includes(p.content_mode)) stream.text = next;
        else stream.text += (p.content_mode ? "" : streamChunkSeparator(stream.text, next)) + next;
        stream.text = stream.text.slice(-600);
      } else if (e.type === "tool_call") {
        // Tools end anonymous text streams, just as they do in the transcript.
        streams.delete(JSON.stringify(["agent", ""]));
        if (p.title) useful.push({text:String(p.title)});
      } else if (["turn_completed", "cancelled", "prompt_failed", "connection_lost"].includes(e.type)) streams.clear();
    }, this);
    const preview = useful.filter(function(item) { return item.text; }).slice(-8);
    this.els.livePreview.textContent = preview.map(function(item) { return item.text.slice(-600); }).join("\n\n");
    this.els.livePreview.hidden = !preview.length;
    if (!gap && preview.length) this.els.historyNoticeText.textContent += " Live preview may be partial.";
    this.els.latest.textContent = gap ? "Retry recent messages" : "Jump to latest";
  };

  AgentChatWidget.prototype._holdHistoryEvent = function (event) {
    this.liveGapTarget = Math.max(this.liveGapTarget || 0, event.seq || 0);
    this.livePendingEvents = this.livePendingEvents || {};
    this.livePendingEvents[event.seq] = event;
    const keys = Object.keys(this.livePendingEvents).map(Number).sort(function(a,b){return a-b;});
    if (keys.length > 2048) delete this.livePendingEvents[keys[0]];
    // Permission controls and submission receipts are independent of transcript
    // paging. Never wait for older text before presenting a current request.
    const p = event.payload || {};
    const controlKey = p.request_id ? "permission:" + p.request_id
      : p.id && ["user_message", "queue_enqueued"].includes(event.type) ? "prompt:" + p.id : "turn";
    this.heldControlSeq = this.heldControlSeq || {};
    if (event.seq > Math.max(this.liveControlSeq || 0, this.heldControlSeq[controlKey] || 0)) {
      if (event.type === "permission_request") this.showPermission(p);
      if (event.type === "permission_resolved") this.hidePermission(p.request_id);
      if (["turn_completed", "cancelled", "connection_lost", "prompt_failed"].includes(event.type)) this.setTurnActive(false);
      if (event.type === "user_message") this.setTurnActive(true, event.created_at, true);
      if ((event.type === "user_message" || event.type === "queue_enqueued") && this.drafts) this.drafts.observeAcceptance(p.id, event.type === "queue_enqueued");
      if (["permission_request", "permission_resolved", "turn_completed", "cancelled", "connection_lost", "prompt_failed", "user_message", "queue_enqueued"].includes(event.type)) this.heldControlSeq[controlKey] = event.seq;
    }
    if (this.historyPreserveWindow) this.updateNewerControl();
    this._renderHistoryNotice();
  };

  AgentChatWidget.prototype._loadLiveSnapshot = function (sessionId, generation) {
    const self = this;
    const restored = this._restoreSessionDomCache(sessionId);
    // Cache is an immediate paint, never evidence that we are still at the tail.
    this._loadRecentHistory(sessionId, generation).catch(function () {});
    return this.apiWithTimeout(
        "/sessions/" + encodeURIComponent(sessionId),
        LIVE_SNAPSHOT_TIMEOUT_MS
      ).then(function (snap) {
      if (self.destroyed || generation !== self.subscriptionGeneration) return null;
      if (self.liveStateRetryId) clearTimeout(self.liveStateRetryId);
      self.liveStateRetryId = null;
      self.liveStateRetryCount = 0;
      self.showRecoveryActions({});
      self.applySnapshot(snap);
      self.connectSSE();
      self.refreshBrowserState();
      self.reconcilePendingSubmission();
      return snap;
    }).catch(function () {
      if (self.destroyed || generation !== self.subscriptionGeneration) return null;
      self.sessionRoute = Object.assign({}, self.sessionRoute || {}, { state: "live_degraded" });
      self.setStatus("offline");
      self.setComposerEnabled(false);
      const historyFallback = restored
        ? Promise.resolve(null)
        : self._loadRecentHistory(sessionId, generation).catch(function () { return null; });
      return historyFallback.then(function () {
        if (!self._isCurrentSessionRequest(sessionId, generation)) return null;
        self.setPlaceholder(
          "Live controls are temporarily unavailable. Durable history is shown when available; PA will retry automatically."
        );
        self._setRecoveryControl(true, "Retry live state");
        self.reconcilePendingSubmission({
          unreachable: true,
          reason: "Live state timed out before prompt acceptance could be confirmed.",
        });
        if ((self.liveStateRetryCount || 0) >= OWNER_ROUTE_MAX_RETRIES) {
          self.setPlaceholder(
            "Live controls remain unavailable after automatic retries. Use Retry live state when ready."
          );
          return null;
        }
        self._scheduleLiveStateRetry(sessionId, generation, false);
        return null;
      });
    });
  };

  AgentChatWidget.prototype._setRecoveryControl = function (visible, label) {
    this.recoveryControlVisible = !!visible;
    if (label) this.recoveryControlLabel = label;
    this.renderSessionActions();
  };

  AgentChatWidget.prototype._writeSessionUrl = function (replace) {
    if (!this.root.closest(".page-agent") || !this.sessionId) return;
    const url = new URL(window.location.href);
    url.searchParams.set("session", this.sessionId);
    if (this.ownerInstanceId) url.searchParams.set("instance", this.ownerInstanceId);
    else url.searchParams.delete("instance");
    const state = { paAgentSession: this.sessionId, paAgentInstance: this.ownerInstanceId };
    window.history[replace ? "replaceState" : "pushState"](state, "", url);
  };

  AgentChatWidget.prototype.openSession = function (sessionId, ownerInstanceId, options) {
    if (this.recentRetryTimer) clearTimeout(this.recentRetryTimer);
    this.recentRetryTimer = null; this.recentRetryCount = 0;
    this.recentBootstrap = true; this.historyPreserveWindow = false; this.historyUserIntent = false; this.liveControlSeq = 0; this.heldControlSeq = {}; this.initialLiveCursor = null;
    clearTimeout(this.queueRefreshTimer);
    if (this.issues) this.issues.reset();
    if (this.threads) this.threads.reset();
    this.submitAckMs = null;
    this.turnTiming = null; this.startupTiming = null; this.renderTimings();
    const self = this;
    options = options || {};
    const generation = ++this.subscriptionGeneration;
    this.closeSSE("session-selected");
    if (this.liveStateRetryId) clearTimeout(this.liveStateRetryId);
    this.liveStateRetryId = null;
    this.liveStateRetryCount = 0;
    if (this.drafts && ownerInstanceId) this.drafts.setInstance(ownerInstanceId);
    if (this.drafts) this.drafts.switchSession(sessionId);
    this.sessionId = sessionId;
    this.closePending = false;
    this.ownerInstanceId = ownerInstanceId || "";
    this.root.dataset.sessionId = sessionId;
    this.root.dataset.ownerInstanceId = this.ownerInstanceId;
    if (this.els.promote) {
      this.els.promote.href = "/knowledge?session=" + encodeURIComponent(sessionId);
    }
    this._setRecoveryControl(false);
    this.showRecoveryActions({});
    this.sessionClosed = false;
    this.ownerResolutionPending = true;
    this.renderSessionActions({ terminal: true, recoverable: false });
    this.setComposerEnabled(false);
    this.setPlaceholder("Locating session owner…");
    let durableHistory = null;
    if (this.ownerInstanceId) {
      this.apiBase = this._apiBaseForOwner(this.ownerInstanceId);
      this.root.dataset.apiBase = this.apiBase;
      // Fetch from the supplied owner while route resolution runs. Painting
      // still follows canonical routing and validates owner + selection.
      this._recentHistory(sessionId, generation, true).catch(function () {});
    }
    const loadDurableHistory = function () {
      if (!durableHistory) {
        durableHistory = self._recentHistory(sessionId, generation).then(function (history) {
          return self._applyDurableHistory(sessionId, history, generation);
        });
      }
      return durableHistory;
    };
    return this.resolveSessionRoute(sessionId, this.ownerInstanceId)
      .then(function (route) {
        if (self.destroyed || generation !== self.subscriptionGeneration) return null;
        self.ownerResolutionPending = false;
        self.sessionRoute = route;
        self.ownerInstanceId = route.owner && route.owner.instance_id || self.ownerInstanceId;
        self.root.dataset.ownerInstanceId = self.ownerInstanceId;
        if (self.drafts && self.ownerInstanceId) {
          self.drafts.setInstance(self.ownerInstanceId);
          self.drafts.switchSession(sessionId);
        }
        if (route.api_base) {
          self.apiBase = String(route.api_base).replace(/\/$/, "");
          self.root.dataset.apiBase = self.apiBase;
        }
        self._writeSessionUrl(!!options.replace);
        if (route.state === "owner_unreachable") {
          return loadDurableHistory().catch(function () { return null; }).then(function () {
            if (!self._isCurrentSessionRequest(sessionId, generation)) return null;
            self.setStatus("offline");
            self.setComposerEnabled(false);
            self.setPlaceholder(
              route.message ||
              "The session owner is temporarily unreachable. Durable history is shown when available."
            );
            self._setRecoveryControl(true, "Retry connection");
            self._scheduleLiveStateRetry(sessionId, generation, true);
            return null;
          });
        }
        if (route.state === "missing") {
          self.clearSelectedSession();
          refreshSessionList(null);
          self.setStatus("error");
          self.setPlaceholder(route.message || "This agent session was deleted or has expired.");
          // Replace a missing auto-started session once. Without this guard,
          // a poisoned fleet apiBase (or remote 404) creates sessions that
          // immediately resolve as missing again and flicker forever.
          if (self.autoStart && !self._missingRestartAttempted) {
            self._missingRestartAttempted = true;
            setTimeout(function () { self.init(); }, 0);
          }
          return null;
        }
        self._missingRestartAttempted = false;
        if (route.live) {
          if (self.startupRetryId) clearTimeout(self.startupRetryId);
          self.startupRetryId = null;
          self.startupRetryCount = 0;
          return self._loadLiveSnapshot(sessionId, generation);
        }
        return loadDurableHistory().then(function (history) {
          if (!self._isCurrentSessionRequest(sessionId, generation)) return null;
          self.sessionClosed = true;
          self.setComposerEnabled(false);
          self.showRecoveryActions({
            recoverable: route.recoverable,
            history_url: "/api/agent/history/" + sessionId,
          });
          self.renderSessionActions({ terminal: true, recoverable: route.recoverable === true });
          const presentation = history && history.presentation || {};
          const actions = presentation.permitted_actions || [];
          const autoPrepare = presentation.purpose === "chat" &&
            !(presentation.archive && presentation.archive.archived) &&
            actions.indexOf("recover") !== -1;
          if (autoPrepare) {
            self.setPlaceholder("Preparing this conversation…");
            return self.recoverSession(sessionId, generation);
          }
          if (route.recoverable) {
            self._setRecoveryControl(true, "Recover session");
            self.addBubble("system", "PA restored this session's durable history. Reconnect it to continue.", new Date().toISOString(), { system: true, forceVisible: true });
          } else {
            self.markSessionEnded("Session ended. Its durable history is still available.");
          }
          return history;
        });
      })
      .catch(function (err) {
        if (self.destroyed || generation !== self.subscriptionGeneration) return null;
        if (self.retryAfterStartupRecovery(err)) return null;
        const code = apiErrorCode(err);
        if (code === "session_not_live" || code === "session_deleted") {
          return self.resolveSessionNotLive(err, sessionId, generation);
        }
        // A supplied owner is already canonical enough to address the session.
        // Try its live endpoint before falling back to history-only mode; this
        // keeps an actively streaming standalone session usable when the fleet
        // route lookup itself is slow or unavailable.
        if (self.ownerInstanceId) {
          self.ownerResolutionPending = false;
          self.sessionRoute = { state: "live_degraded", live: true };
          self._writeSessionUrl(!!options.replace);
          return self._loadLiveSnapshot(sessionId, generation);
        }
        return loadDurableHistory().catch(function () { return null; }).then(function () {
          if (!self._isCurrentSessionRequest(sessionId, generation)) return null;
          self.ownerResolutionPending = false;
          self.sessionRoute = { state: "owner_unreachable" };
          self.setPlaceholder(
            "Live state is temporarily unavailable. Durable history is shown when available; PA will retry automatically."
          );
          self.setStatus("offline");
          self.setComposerEnabled(false);
          self._setRecoveryControl(true, "Retry connection");
          self._scheduleLiveStateRetry(sessionId, generation, true);
          return null;
        });
      });
  };

  AgentChatWidget.prototype.recoverSession = function (sessionId, generation) {
    const self = this;
    const targetSessionId = sessionId || this.sessionId;
    if (!targetSessionId) return Promise.reject(new Error("No session selected"));
    const requestGeneration = generation == null
      ? this.subscriptionGeneration
      : generation;
    if (!this._isCurrentSessionRequest(targetSessionId, requestGeneration)) {
      return Promise.resolve(null);
    }
    this._setRecoveryControl(false);
    if (
      this.sessionRoute &&
      (this.sessionRoute.state === "owner_unreachable" ||
       this.sessionRoute.state === "live_degraded")
    ) {
      return this.openSession(targetSessionId, this.ownerInstanceId, { replace: true });
    }
    this.setComposerEnabled(false);
    this.setPlaceholder("Preparing this conversation…");
    return this.api("/sessions/" + encodeURIComponent(targetSessionId) + "/recover", {
      method: "POST",
      body: "{}",
    })
      .then(function (snap) {
        if (!self._isCurrentSessionRequest(targetSessionId, requestGeneration)) return null;
        if (self.startupRetryId) clearTimeout(self.startupRetryId);
        self.startupRetryId = null;
        self.sessionId = targetSessionId;
        self.root.dataset.sessionId = targetSessionId;
        self.sessionRoute = self.sessionRoute || {};
        self.sessionRoute.state = "live";
        self.sessionRoute.live = true;
        self.sessionRoute.recoverable = false;
        self.showRecoveryActions({});
        self.applySnapshot(snap);
        self.sessionClosed = false;
        self.renderSessionActions({ terminal: false, recoverable: false });
        self.setComposerEnabled(true);
        self.connectSSE();
        refreshSessionList(targetSessionId);
        return snap;
      })
      .catch(function (err) {
        if (!self._isCurrentSessionRequest(targetSessionId, requestGeneration)) return null;
        if (self.retryAfterStartupRecovery(err)) return null;
        const code = apiErrorCode(err);
        if (code === "session_not_live" || code === "session_deleted") {
          return self.loadDurableSession(targetSessionId, requestGeneration);
        }
        if (code === "session_recovery_blocked" || code === "session_closed") {
          return self.loadDurableSession(targetSessionId, requestGeneration);
        }
        self.setPlaceholder("Session recovery is still unavailable: " + err.message);
        self.setStatus("offline");
        self._setRecoveryControl(true, "Retry recovery");
        throw err;
      });
  };

  AgentChatWidget.prototype.browserApi = function (path, opts) {
    opts = opts || {};
    return fetch(this.apiBase + "/sessions/" + this.sessionId + "/browser" + path, Object.assign({
      headers: csrfHeaders(), credentials: "same-origin",
    }, opts)).then(function (res) {
      if (!res.ok) return res.json().catch(function () { return {}; }).then(function (body) { throw new Error(body.detail || "Browser request failed"); });
      return res.json();
    });
  };

  AgentChatWidget.prototype.applyBrowserState = function (state) {
    this.browserAttached = !!(state && state.attached);
    if (!this.browserAttached) this.browserVisible = false;
    if (this.els.browser) this.els.browser.hidden = !(this.browserAttached && this.browserVisible);
    if (this.els.browserToggle) {
      this.els.browserToggle.textContent = this.browserAttached
        ? (this.browserVisible ? "Hide Browser" : "Show Browser")
        : "Attach Browser";
      this.els.browserToggle.classList.toggle("active", this.browserAttached);
      this.els.browserToggle.disabled = this.prompting;
    }
    if (this.browserAttached && this.els.browserUrl && state.url) this.els.browserUrl.value = state.url;
    if (this.browserAttached && this.els.browserWidth && state.width) this.els.browserWidth.value = state.width;
    if (this.browserAttached && this.els.browserHeight && state.height) this.els.browserHeight.value = state.height;
    if (this.browserAttached && state.device_scale_factor) this.browserDeviceScaleFactor = state.device_scale_factor;
    if (this.browserAttached && this.browserVisible) this.startBrowserRefresh(); else this.stopBrowserRefresh();
  };

  AgentChatWidget.prototype.setBrowserVisible = function (visible) {
    this.browserVisible = !!visible;
    this.applyBrowserState({
      attached: this.browserAttached,
      url: this.els.browserUrl && this.els.browserUrl.value,
      width: this.els.browserWidth && this.els.browserWidth.value,
      height: this.els.browserHeight && this.els.browserHeight.value,
    });
    if (this.browserVisible) this.refreshBrowser();
  };

  AgentChatWidget.prototype.refreshBrowserState = function () {
    const self = this;
    if (!this.sessionId) return;
    this.browserApi("").then(function (state) { self.applyBrowserState(state); if (state.attached && self.browserVisible) self.refreshBrowser(); }).catch(function () {});
  };

  AgentChatWidget.prototype.attachBrowser = function () {
    const self = this;
    if (this.browserAttached) { this.refreshBrowser(); return; }
    const url = (this.els.browserUrl && this.els.browserUrl.value) || "about:blank";
    if (this.els.browserToggle) this.els.browserToggle.disabled = true;
    const width = parseInt((this.els.browserWidth && this.els.browserWidth.value) || "1440", 10);
    const height = parseInt((this.els.browserHeight && this.els.browserHeight.value) || "900", 10);
    this.browserApi("/attach", { method: "POST", body: JSON.stringify({ url: url, width: width, height: height }) })
      .then(function (state) { self.browserVisible = true; self.applyBrowserState(state); self.refreshBrowser(); self.addBubble("system", "Headless browser attached to this agent session.", new Date().toISOString(), { system: true, forceVisible: true }); })
      .catch(function (err) { self.addBubble("system", err.message, new Date().toISOString(), { system: true, forceVisible: true }); })
      .finally(function () { if (self.els.browserToggle) self.els.browserToggle.disabled = self.prompting; });
  };

  AgentChatWidget.prototype.detachBrowser = function () {
    const self = this;
    this.browserApi("/detach", { method: "POST", body: "{}" }).then(function (state) { self.applyBrowserState(state); });
  };

  AgentChatWidget.prototype.navigateBrowser = function () {
    const self = this;
    let url = (this.els.browserUrl && this.els.browserUrl.value.trim()) || "about:blank";
    if (url !== "about:blank" && !/^[a-z][a-z0-9+.-]*:/i.test(url)) url = "https://" + url;
    this.browserApi("/navigate", { method: "POST", body: JSON.stringify({ url: url }) }).then(function (state) { self.applyBrowserState(state); setTimeout(function () { self.refreshBrowser(); }, 500); });
  };

  AgentChatWidget.prototype.resizeBrowser = function () {
    const self = this;
    const width = parseInt((this.els.browserWidth && this.els.browserWidth.value) || "1440", 10);
    const height = parseInt((this.els.browserHeight && this.els.browserHeight.value) || "900", 10);
    this.browserApi("/resize", { method: "POST", body: JSON.stringify({ width: width, height: height }) })
      .then(function (state) { self.applyBrowserState(state); self.refreshBrowser(); })
      .catch(function (err) { self.addBubble("system", err.message, new Date().toISOString(), { system: true, forceVisible: true }); });
  };

  AgentChatWidget.prototype.refreshBrowser = function () {
    if (!this.browserAttached || !this.els.browserImage) return;
    this.els.browserImage.src = this.apiBase + "/sessions/" + this.sessionId + "/browser/screenshot?t=" + Date.now();
  };

  AgentChatWidget.prototype.startBrowserRefresh = function () {
    const self = this;
    if (this.browserRefreshId) return;
    this.browserRefreshId = setInterval(function () { if (!document.hidden) self.refreshBrowser(); }, 1500);
  };

  AgentChatWidget.prototype.stopBrowserRefresh = function () {
    if (this.browserRefreshId) clearInterval(this.browserRefreshId);
    this.browserRefreshId = null;
  };

  AgentChatWidget.prototype.renderTerminal = function () {
    if (!window.PAAgentTerminal || !this.els.terminalOutput || !this.els.terminalFlyout || this.els.terminalFlyout.hidden) return;
    const limit = Number(this.root.dataset.terminalOutputLimit) || 1000;
    window.PAAgentTerminal.render(this.els.terminalOutput, this.transcriptEvents, this.sessionId, limit);
  };

  AgentChatWidget.prototype.updateTitle = function (title, version) {
    if (this._titleSession !== this.sessionId) { this._titleSession = this.sessionId; this._titleVersion = 0; }
    if (version != null && version < (this._titleVersion || 0)) return;
    if (version != null) this._titleVersion = version;
    if (typeof title !== "string" || !title.trim()) return;
    if (this.els.title) this.els.title.textContent = title;
    if (this.lastSnapshot && this.lastSnapshot.session) this.lastSnapshot.session.title = title;
    const item = sessionListItem(this.sessionId);
    if (item) {
      const label = item.querySelector("[data-agent-session-title]");
      if (label) { label.textContent = title; label.dataset.fullTitle = title; }
      const tooltip = item.querySelector(".agent-session-title-tooltip");
      if (tooltip) tooltip.textContent = title;
      const opener = item.querySelector("[data-agent-session-open]");
      if (opener) opener.setAttribute("aria-label", "Open session " + title);
    }
  };

  AgentChatWidget.prototype.toggleFlyout = function (kind) {
    const target = this.els[kind + "Flyout"];
    const open = !!(target && target.hidden);
    this.closeFlyouts();
    if (target) target.hidden = !open;
    const toggle = this.els[kind + "Toggle"];
    if (toggle) toggle.setAttribute("aria-expanded", open ? "true" : "false");
    if (kind === "terminal" && open) this.renderTerminal();
  };

  AgentChatWidget.prototype.closeFlyouts = function () {
    const self = this;
    ["tool", "terminal", "plan"].forEach(function (kind) {
      if (self.els[kind + "Flyout"]) self.els[kind + "Flyout"].hidden = true;
      if (self.els[kind + "Toggle"]) self.els[kind + "Toggle"].setAttribute("aria-expanded", "false");
    });
  };

  AgentChatWidget.prototype.setPlaceholder = function (text) {
    if (this.els.placeholder) {
      if (this.els.placeholder.textContent !== text) {
        this.els.placeholder.textContent = text;
      }
      this.els.placeholder.hidden = false;
    }
  };

  AgentChatWidget.prototype.clearPlaceholder = function () {
    if (this.els.placeholder) this.els.placeholder.hidden = true;
  };

  AgentChatWidget.prototype._hasImageFiles = function (dataTransfer) {
    if (!dataTransfer || !dataTransfer.items) return false;
    return Array.from(dataTransfer.items).some(function (item) {
      return item.kind === "file" && IMAGE_TYPES.indexOf(item.type) !== -1;
    });
  };

  AgentChatWidget.prototype.addImageFiles = function (fileList) {
    const self = this;
    const files = Array.from(fileList || []);
    files.forEach(function (file) {
      if (IMAGE_TYPES.indexOf(file.type) === -1) {
        self.addBubble("system", file.name + " is not a supported image.", new Date().toISOString(), { system: true, forceVisible: true });
        return;
      }
      if (file.size > MAX_IMAGE_BYTES) {
        self.addBubble("system", file.name + " exceeds the 10 MB image limit.", new Date().toISOString(), { system: true, forceVisible: true });
        return;
      }
      if (self.pendingImages.length >= MAX_IMAGES) {
        self.addBubble("system", "You can attach up to 4 images.", new Date().toISOString(), { system: true, forceVisible: true });
        return;
      }
      const total = self.pendingImages.reduce(function (sum, image) { return sum + image.size; }, 0);
      if (total + file.size > MAX_TOTAL_IMAGE_BYTES) {
        self.addBubble("system", "Attached images cannot exceed 20 MB combined.", new Date().toISOString(), { system: true, forceVisible: true });
        return;
      }

      const image = {
        name: file.name,
        mime_type: file.type,
        size: file.size,
        data: null,
        preview: URL.createObjectURL(file),
      };
      self.pendingImages.push(image);
      self.renderPendingImages();
      if (self.drafts) self.drafts.changed();

      const reader = new FileReader();
      reader.onload = function () {
        const result = String(reader.result || "");
        image.data = result.slice(result.indexOf(",") + 1);
      };
      reader.onerror = function () {
        self.removePendingImage(self.pendingImages.indexOf(image));
        self.addBubble("system", "Could not read " + file.name + ".", new Date().toISOString(), { system: true, forceVisible: true });
      };
      reader.readAsDataURL(file);
    });
  };

  AgentChatWidget.prototype.removePendingImage = function (index) {
    if (index < 0 || index >= this.pendingImages.length) return;
    const removed = this.pendingImages.splice(index, 1)[0];
    if (removed.preview) URL.revokeObjectURL(removed.preview);
    this.renderPendingImages();
    if (this.drafts) this.drafts.changed();
  };

  AgentChatWidget.prototype.clearPendingImages = function () {
    this.pendingImages.forEach(function (image) {
      if (image.preview) URL.revokeObjectURL(image.preview);
    });
    this.pendingImages = [];
    this.renderPendingImages();
  };

  AgentChatWidget.prototype.renderPendingImages = function () {
    if (!this.els.attachments) return;
    const self = this;
    this.els.attachments.innerHTML = "";
    this.els.attachments.hidden = !this.pendingImages.length;
    this.pendingImages.forEach(function (image, index) {
      const item = document.createElement("div");
      item.className = "acw-attachment";
      const preview = document.createElement("img");
      preview.src = image.preview;
      preview.alt = image.name;
      const name = document.createElement("span");
      name.textContent = image.name;
      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "ghost small";
      remove.setAttribute("aria-label", "Remove " + image.name);
      remove.textContent = "×";
      remove.addEventListener("click", function () { self.removePendingImage(index); });
      item.appendChild(preview);
      item.appendChild(name);
      item.appendChild(remove);
      self.els.attachments.appendChild(item);
    });
  };

  function imageSummary(images) {
    if (!images || !images.length) return "";
    return images.length === 1 ? "Attached image: " + images[0].name : "Attached " + images.length + " images";
  }

  AgentChatWidget.prototype._isDuplicateUserBubble = function (text) {
    // Compare against dataset.markdown (raw prompt), not textContent — markdown
    // rendering drops newlines (`<br>` / `<p>`), which made every multi-line
    // optimistic bubble look unique and paint a second copy from SSE.
    if (typeof text !== "string" || !this.els.messages) return false;
    const rows = this.els.messages.querySelectorAll(".acw-msg-user .acw-bubble");
    if (!rows.length) return false;
    const last = rows[rows.length - 1];
    const lastText = Object.prototype.hasOwnProperty.call(last.dataset, "markdown")
      ? last.dataset.markdown
      : (last.textContent || "");
    return lastText === text;
  };

  AgentChatWidget.prototype.setStatus = function (state) {
    if (!this.els.status) return;
    this.els.status.dataset.state = state;
    this.els.status.className = "acw-status-dot is-" + state;
  };

  AgentChatWidget.prototype.renderMcpHealth = function (health) {
    if (!this.els.status || !this.els.status.parentNode) return;
    if (!this.els.mcpHealth) {
      this.els.mcpHealth = document.createElement("span");
      this.els.mcpHealth.setAttribute("role", "status");
      this.els.status.parentNode.appendChild(this.els.mcpHealth);
    }
    const state = health && health.state;
    const label = state === "checking" ? "PA tools starting" :
      state === "disconnected" ? "PA tools unavailable" : "";
    this.els.mcpHealth.textContent = label;
    this.els.mcpHealth.hidden = !label;
    this.els.mcpHealth.title = health && (health.detail || health.recovery) || label;
  };

  AgentChatWidget.prototype._hasTurnHistory = function () {
    const turnEvents = {
      user_message: true,
      agent_message: true,
      agent_message_chunk: true,
      agent_thought_chunk: true,
      tool_call: true,
      tool_call_update: true,
      turn_completed: true,
      turn_waiting: true,
      prompt_failed: true,
      cancelled: true,
    };
    return (this.transcriptEvents || []).some(function (event) {
      return !!turnEvents[event && (event.type || event.event_type)];
    });
  };

  function recoveryGuidance(snap) {
    const session = snap.session || {};
    const recovery = session.recovery_json || {};
    const presentation = snap.presentation || {};
    const presentedRecovery = presentation.recovery || {};
    const provisioning = session.config_json && session.config_json.provisioning || {};
    // Current recovery evidence takes precedence over an older workspace error.
    // Keep provisioning guidance for peers without a durable recovery record.
    const workspace = provisioning.state === "blocked" ? provisioning : {};
    return recovery.remedy || presentedRecovery.remedy ||
      recovery.last_error || presentedRecovery.last_error ||
      workspace.action || workspace.error || presentation.explanation ||
      "Recovery could not complete. Retry this session or inspect its diagnostics for details.";
  }

  AgentChatWidget.prototype.updateEmptyChatStatus = function (snap) {
    snap = snap || this.lastSnapshot || {};
    if (this._hasTurnHistory()) {
      this.clearPlaceholder();
      return;
    }
    if (this.ownerResolutionPending) {
      this.setPlaceholder("Locating session owner…");
      return;
    }
    if (this.recentBootstrap) {
      this.setPlaceholder("Recent history is loading or unavailable. Live updates appear in the history notice while PA retries.");
      return;
    }
    const session = snap.session || {};
    const provisioning = session.config_json && session.config_json.provisioning || {};
    const status = session.status || "";
    const queue = snap.queue || [];
    if (status === "closed") {
      this.setPlaceholder("Session ended. Its durable history is still available.");
    } else if (status === "provisioning") {
      this.setPlaceholder("Preparing the session workspace…");
    } else if (status === "connecting") {
      this.setPlaceholder("Connecting to the agent provider…");
    } else if (status === "configuring") {
      this.setPlaceholder("Configuring the agent provider…");
    } else if (status === "provisioning_failed" || status === "configuration_failed") {
      this.setPlaceholder(
        provisioning.error || provisioning.action ||
        (status === "provisioning_failed"
          ? "Session provisioning failed. Retry or end the session."
          : "Provider configuration failed. Retry or end the session.")
      );
    } else if (status === "recovery_blocked" || provisioning.state === "blocked") {
      this.setPlaceholder(recoveryGuidance(snap));
    } else if (snap.prompting) {
      this.setPlaceholder("The agent is working…");
    } else if (queue.length) {
      this.setPlaceholder("Prompt queued — waiting to start.");
    } else if (
      status === "recoverable_interrupted" || status === "disconnected" ||
      status === "quiesced" || !snap.connected
    ) {
      this.setPlaceholder("The provider connection is recovering…");
    } else if (
      this.sessionRoute && this.sessionRoute.live &&
      snap.connected && this.composerEnabled && !this.sessionClosed
    ) {
      this.setPlaceholder("Ready to chat — send your first prompt.");
    } else {
      this.setPlaceholder("Send a message to the agent.");
    }
  };

  AgentChatWidget.prototype.renderTimings = function (turn, startup) {
    const target = this.root && typeof this.root.querySelector === "function" && this.root.querySelector("[data-acw-timings]");
    if (!target) return;
    if (turn) this.turnTiming = turn;
    if (startup) this.startupTiming = startup;
    const parts = [], timing = this.turnTiming || {};
    const ms = value => Math.round(value) + " ms";
    if (Number.isFinite(this.composerReadyMs)) parts.push("Page navigation to composer focus: " + ms(this.composerReadyMs));
    if (Number.isFinite(this.submitAckMs)) parts.push("Browser submit to PA receipt: " + ms(this.submitAckMs));
    (this.startupTiming && this.startupTiming.phases || []).forEach(function (phase) {
      const labels = {preference_resolution: "Preferences", execution_selection: "Selection", workspace_preparation: "Workspace", provider_launch: "Provider launch", provider_initialize: "Provider initialize", session_creation: "Provider session", response_readiness: "Ready"};
      if (labels[phase.name]) parts.push(labels[phase.name] + ": " + ms(phase.duration_ms));
    });
    if (Number.isFinite(timing.accept_to_provider_ms)) parts.push("Queue and PA preparation: " + ms(timing.accept_to_provider_ms));
    if (Number.isFinite(timing.provider_generation_ms)) parts.push("Provider to first useful event: " + ms(timing.provider_generation_ms));
    target.textContent = parts.join(" · ") || "Timings appear when PA accepts a message and the provider responds.";
  };

  AgentChatWidget.prototype.recordSubmissionTiming = function (promptId) {
    const timing = this.pendingSubmitTiming;
    if (!timing || timing.promptId !== promptId || timing.sessionId !== this.sessionId ||
        timing.generation !== this.subscriptionGeneration || timing.acknowledged) return;
    timing.acknowledged = true;
    this.submitAckMs = Date.now() - timing.startedAt;
    this.renderTimings();
  };

  AgentChatWidget.prototype.renderPresentation = function (snap) {
    const outcome = this.root && typeof this.root.querySelector === "function" && this.root.querySelector("[data-conversation-outcome]");
    if (outcome) {
      const presentation = snap.presentation || {}, actor = presentation.next_actor || {};
      outcome.hidden = !presentation.display_status;
      outcome.textContent = [presentation.display_status, actor.label ? "Next: " + actor.label : "", presentation.explanation].filter(Boolean).join(" · ");
    }
  };

  AgentChatWidget.prototype.applySnapshot = function (snap) {
    if (this.issues) this.issues.ready();
    if (this.threads) this.threads.ready(snap);
    const self = this;
    this.lastSnapshot = Object.assign({}, this.lastSnapshot || {}, snap);
    this.liveControlSeq = Math.max(this.liveControlSeq || 0, snap.last_event_seq || 0);
    if (this.initialLiveCursor == null && typeof snap.last_event_seq === "number") this.initialLiveCursor = snap.last_event_seq;
    this.renderPresentation(snap);
    const session = snap.session || {};
    this.renderTimings(session.metrics_json && session.metrics_json.turn_timing, session.config_json && session.config_json.startup_trace);
    if (session.agent_name) this.providerId = session.agent_name;
    const provisioning = session.config_json && session.config_json.provisioning || {};
    const recoveryBlocked = session.status === "recovery_blocked" || provisioning.state === "blocked";
    this.sessionClosed = session.status === "closed";
    if (!this.sessionClosed) this.sessionRecoverable = false;
    if (this.drafts) this.drafts.onSnapshot(snap);
    this.setComposerEnabled(!this.sessionClosed && !recoveryBlocked,
      this.sessionClosed ? "This session has ended. Select another conversation." : undefined);
    this.renderSessionActions();
    const handoffs = snap.restart_handoffs || [];
    const handoff = handoffs.length ? handoffs[handoffs.length - 1] : null;
    if (handoff && this.els.sessionActionStatus) {
      const labels = {
        requested: "Restart saved; execution has not started yet.",
        waiting_for_turn_end: "Restart requested; waiting for this turn to end and flush.",
        quiescing: "Turn ended; PA is quiescing sessions.",
        restarting: "PA is restarting.",
        resuming: "PA restarted; resuming the exact session.",
        continuation_queued: "Continuation accepted; waiting for its turn to finish.",
        continuation_delivered: handoff.continuation_prompt
          ? "PA service restarted; the continuation turn completed."
          : "PA service restarted; no continuation prompt was sent.",
        restart_completed: "PA service restarted; no continuation prompt was sent.",
        failed: "Restart handoff failed: " + (handoff.error || "operator repair and retry required.")
      };
      const observation = handoff.observation || {};
      this.els.sessionActionStatus.dataset.operationPhase = observation.phase || handoff.status;
      const prerequisites = {operator_paused: "Continuation paused; resume when ready.", exact_session_closed: "The exact session has ended; inspect its preserved history.", session_recovery_blocked: "Continuation waiting for session recovery.", execution_binding_mismatch: "Continuation waiting for its exact workspace binding."};
      this.els.sessionActionStatus.textContent = prerequisites[observation.reason_code] || labels[observation.domain_stage || handoff.status] || handoff.status;
    }
    if (recoveryBlocked && this.els.input) {
      this.els.input.placeholder = "Recovery is blocked. Follow the action above, retry, or end the session.";
    }
    if (this.els.recovery) {
      this.els.recovery.hidden = !recoveryBlocked;
      if (this.els.recoveryAction) {
        this.els.recoveryAction.textContent = recoveryBlocked ? recoveryGuidance(snap) : "";
      }
      if (this.els.recoveryRetry) this.els.recoveryRetry.disabled = false;
    }
    this.updateTitle(session.title || (session.purpose === "chat" ? "New chat" : session.label) || "Agent",
      ((session.config_json || {}).naming || {}).version || 0);
    this.queuePaused = !!snap.queue_paused;
    if (Object.prototype.hasOwnProperty.call(snap, "transcript")) {
      this.recentBootstrap = false;
      this.hasOlder = !!(snap.transcript_page && snap.transcript_page.has_older);
      this.olderCursor = snap.transcript_page && (
        snap.transcript_page.next_before_seq || snap.transcript_page.oldest_seq
      );
      this.hasNewer = !!(snap.transcript_page && snap.transcript_page.has_newer);
      this.newerCursor = snap.transcript_page && snap.transcript_page.newest_seq;
      this.newerError = "";
      this.olderError = "";
      this.renderTranscript(snap.transcript || [], { scrollBottom: true });
      this._renderHistoryNotice();
    }
    // Transcript replay includes historical turn-completed events. Apply the live
    // snapshot state afterward so replay cannot reset an active turn's timer.
    this.setTurnActive(!!snap.prompting, snap.turn_started_at);
    this.setStatus(this.prompting ? "working" : snap.connected ? "online" : "offline");
    this.renderMcpHealth(snap.pa_mcp);
    this.renderQueue(snap.queue || [], snap);
    this.renderModelsModes(snap);
    this.renderConfigOptions(snap);
    this.renderMetrics(snap.metrics || session.metrics_json || {});
    if (window.PAExecutionSelection) window.PAExecutionSelection.renderSession(this, snap);
    if (this.commandCatalogSession !== this.sessionId) this.refreshCommandCatalog();
    if (this.els.permissions) {
      this.els.permissions.innerHTML = "";
      this.els.permissions.hidden = true;
    }
    (snap.pending_permissions || []).forEach(function (req) {
      if (req && typeof req === "object") self.showPermission(req);
    });
    this.renderTerminal();
    this.updateEmptyChatStatus(snap);
    refreshSessionList(this.sessionId);
  };

  AgentChatWidget.prototype.applyOptionSnapshot = function (snap) {
    if (snap.session && snap.session.id !== this.sessionId) return;
    const previous = this.lastSnapshot;
    if (previous && previous.session && snap.session && previous.session.id === snap.session.id &&
        (Number(previous.last_event_seq || 0) > Number(snap.last_event_seq || 0) ||
         Date.parse(previous.session.updated_at) > Date.parse(snap.session.updated_at))) return;
    this.lastSnapshot = snap;
    this.renderModelsModes(snap);
    this.renderConfigOptions(snap);
    if (window.PAExecutionSelection) window.PAExecutionSelection.renderSession(this, snap);
  };

  AgentChatWidget.prototype._eventKey = function (event) {
    const seq = Number(event && event.seq || 0);
    if (seq) return "seq:" + seq;
    if (event && event.id) return "id:" + event.id;
    return "";
  };

  AgentChatWidget.prototype._normalizeEvent = function (event) {
    return {
      seq: Number(event && event.seq || 0),
      type: event && (event.type || event.event_type),
      payload: event && event.payload || {},
      created_at: event && event.created_at,
      id: event && event.id,
    };
  };

  AgentChatWidget.prototype.updateOlderControl = function () {
    if (!this.els.loadOlder) return;
    const anchor = this._captureHistoryAnchor();
    this.els.loadOlder.hidden = !this.hasOlder && !this.olderError;
    this.els.loadOlder.disabled = this.loadingOlder;
    const label = this.loadingOlder ? "Loading older messages…"
      : this.recentBootstrap ? "Retry recent messages"
      : this.olderError ? "Retry loading older messages" : "Load older messages";
    if (this.els.loadOlderLabel) this.els.loadOlderLabel.textContent = label;
    else this.els.loadOlder.textContent = label;
    if (this.els.historySpinner) this.els.historySpinner.hidden = !this.loadingOlder;
    this.els.loadOlder.setAttribute("aria-busy", this.loadingOlder ? "true" : "false");
    if (this.els.loadOlderStatus) {
      const status = this.loadingOlder ? "Loading older messages." : this.olderError;
      this.els.loadOlderStatus.hidden = !status;
      this.els.loadOlderStatus.textContent = status;
    }
    this._restoreHistoryAnchor(anchor);
  };

  AgentChatWidget.prototype.updateNewerControl = function () {
    if (!this.els || !this.els.loadNewer) return;
    const pending = this.historyPreserveWindow && this.liveGapTarget > this.lastSeq;
    this.els.loadNewer.hidden = !this.hasNewer && !this.newerError && !pending;
    this.els.loadNewer.disabled = this.loadingNewer;
    this.els.loadNewer.textContent = this.loadingNewer
      ? "Loading…"
      : this.newerError ? "Retry loading newer messages" : "Load newer messages";
    this.els.loadNewer.setAttribute("aria-busy", this.loadingNewer ? "true" : "false");
    if (this.els.loadNewerStatus) {
      this.els.loadNewerStatus.hidden = !this.newerError;
      this.els.loadNewerStatus.textContent = this.newerError;
    }
  };

  AgentChatWidget.prototype._compactEvents = function (events) {
    const result = [];
    events.forEach(function (event) {
      const previous = result[result.length - 1];
      const payload = event.payload || {};
      if (previous && payload.content_mode &&
          (event.type === "agent_message_chunk" || event.type === "agent_thought_chunk") &&
          previous.type === event.type && previous.payload.content_mode &&
          previous.payload.message_id === payload.message_id && previous.payload.phase === payload.phase) {
        const replacement = ["snapshot", "replace", "replacement", "accumulated"].indexOf(payload.content_mode) !== -1;
        result[result.length - 1] = Object.assign({}, event, { payload: Object.assign({}, payload, {
          text: (replacement ? "" : previous.payload.text || "") + (payload.text || ""),
          content_mode: "snapshot",
          source_first_seq: previous.payload.source_first_seq || previous.seq,
        }) });
      } else result.push(event);
    });
    return result;
  };

  AgentChatWidget.prototype._boundedEvents = function (events, keepOldest) {
    events = this._compactEvents(events);
    if (events.length <= MAX_RETAINED_EVENTS) return events;
    if (keepOldest) {
      this.hasNewer = true;
      return events.slice(0, MAX_RETAINED_EVENTS);
    }
    this.hasOlder = true;
    return events.slice(events.length - MAX_RETAINED_EVENTS);
  };

  AgentChatWidget.prototype._rebuildSeenEvents = function () {
    const self = this;
    this.seenEvents = {};
    this.transcriptEvents.forEach(function (event) {
      const key = self._eventKey(event);
      if (key) self.seenEvents[key] = true;
    });
  };

  AgentChatWidget.prototype._pruneMessageRows = function (keepOldest) {
    if (
      !this.els || !this.els.messages ||
      typeof this.els.messages.querySelectorAll !== "function"
    ) return;
    if (this.messageRowCount != null && this.messageRowCount <= MAX_MESSAGE_ROWS) return;
    let rows = Array.from(this.els.messages.querySelectorAll(".acw-msg"));
    const oldHeight = this.els.messages.scrollHeight;
    const oldTop = this.els.messages.scrollTop;
    const activeRows = {};
    Object.keys(this.streaming).forEach(function (key) {
      const row = this.streaming[key] && this.streaming[key].row;
      if (row) activeRows[key] = row;
    }, this);
    let firstRemovedSeq = null;
    while (rows.length > MAX_MESSAGE_ROWS) {
      const candidates = keepOldest ? rows.slice().reverse() : rows;
      const row = candidates.find(function (candidate) {
        return Object.keys(activeRows).every(function (key) { return activeRows[key] !== candidate; });
      });
      if (!row) break;
      const sourceSeq = Number(row.dataset && row.dataset.sourceSeq);
      if (sourceSeq) firstRemovedSeq = Math.min(firstRemovedSeq || sourceSeq, sourceSeq);
      row.remove();
      rows = rows.filter(function (candidate) { return candidate !== row; });
    }
    if (this.messageRowCount > rows.length && rows.length) {
      if (keepOldest) {
        this.hasNewer = true;
        if (firstRemovedSeq) {
          this.transcriptEvents = this.transcriptEvents.filter(function (event) {
            return (event.payload.source_first_seq || event.seq) < firstRemovedSeq;
          });
          this.newerCursor = this.transcriptEvents.reduce(function (seq, event) {
            return Math.max(seq, event.seq || 0);
          }, 0);
        }
        this.updateNewerControl();
      } else {
        this.hasOlder = true;
        this.olderCursor = Number(rows[0].dataset && rows[0].dataset.sourceSeq) || this.olderCursor;
        const oldest = this.olderCursor;
        if (oldest) this.transcriptEvents = this.transcriptEvents.filter(function (event) {
          return (event.payload.source_first_seq || event.seq) >= oldest;
        });
        this.updateOlderControl();
      }
      this._rebuildSeenEvents();
    }
    this.messageRowCount = rows.length;
    if (!keepOldest) {
      this.els.messages.scrollTop = Math.max(
        0,
        oldTop - Math.max(0, oldHeight - this.els.messages.scrollHeight)
      );
    }
  };

  AgentChatWidget.prototype.memoryDiagnostics = function () {
    const messageRows = this.els && this.els.messages
      ? this.els.messages.querySelectorAll(".acw-msg").length : 0;
    const activityRows = this.els && this.els.toolActivity
      ? this.els.toolActivity.querySelectorAll(".acw-tool,.acw-progress-update").length : 0;
    return {
      retainedEvents: this.transcriptEvents.length,
      dedupeKeys: Object.keys(this.seenEvents).length,
      messageRows: messageRows,
      activityRows: activityRows,
      streams: Object.keys(this.streaming).length,
      activityStreams: Object.keys(this.activityStreams).length,
      toolTimers: Object.keys(this.toolTimers).length,
      plans: this.plans.length,
      bounds: {
        retainedEvents: MAX_RETAINED_EVENTS,
        messageRows: MAX_MESSAGE_ROWS,
        activityRows: MAX_ACTIVITY_ROWS,
        plans: MAX_PLANS,
        streamChars: MAX_STREAM_CHARS,
      },
    };
  };

  AgentChatWidget.prototype.prependTranscript = function (events) {
    const self = this;
    const page = [];
    const known = {};
    this.transcriptEvents.forEach(function (event) {
      const key = self._eventKey(event);
      if (key) known[key] = true;
    });
    (events || []).forEach(function (event) {
      const normalized = self._normalizeEvent(event);
      const key = self._eventKey(normalized);
      if (key && known[key]) return;
      if (key) known[key] = true;
      page.push(normalized);
    });
    page.sort(function (a, b) { return (a.seq || 0) - (b.seq || 0); });
    this.renderTranscript(page.concat(this.transcriptEvents), { keepOldest: true });
    return page.length;
  };

  AgentChatWidget.prototype.renderTranscript = function (events, options) {
    const self = this;
    if (!this.els.messages) return;
    options = options || {};
    const unique = [];
    const keys = {};
    (events || []).forEach(function (event) {
      const normalized = self._normalizeEvent(event);
      const key = self._eventKey(normalized);
      if (key && keys[key]) return;
      if (key) keys[key] = true;
      unique.push(normalized);
    });
    unique.sort(function (a, b) { return (a.seq || 0) - (b.seq || 0); });
    this.transcriptEvents = this._boundedEvents(unique, !!options.keepOldest);
    const retained = this.transcriptEvents;
    this.seenEvents = {};
    Object.keys(this.toolTimers).forEach(function (id) {
      const timer = self.toolTimers[id];
      if (timer && timer.interval) clearInterval(timer.interval);
    });
    this.toolTimers = {};
    this.resetArtifacts();
    // Keep placeholder node; clear the rest
    Array.from(this.els.messages.children).forEach(function (child) {
      if (
        !child.hasAttribute("data-acw-placeholder") &&
        !child.hasAttribute("data-acw-load-older") &&
        !child.hasAttribute("data-acw-load-older-status") &&
        !child.hasAttribute("data-acw-load-newer") &&
        !child.hasAttribute("data-acw-load-newer-status")
      ) child.remove();
    });
    this.messageRowCount = 0;
    this.streaming = {};
    this.updateOlderControl();
    this.updateNewerControl();
    if (!retained.length) {
      this.setPlaceholder("Send a message to the agent.");
      return;
    }
    this.clearPlaceholder();
    retained.forEach(function (event) {
      self.handleEvent(event, true, false);
    });
    this._rebuildSeenEvents();
    this.renderTerminal();
    this._pruneMessageRows(!!options.keepOldest);
    if (options.scrollBottom) this.scrollToBottom();
  };

  AgentChatWidget.prototype.loadOlderTranscript = function () {
    if (this.recentBootstrap) { this._loadRecentHistory(this.sessionId, this.subscriptionGeneration).catch(function(){}); return; }
    if (this.loadingOlder || (!this.hasOlder && !this.olderError) || !this.sessionId || !this.els.messages) return;
    const oldest = this.olderCursor || this.transcriptEvents.reduce(function (result, event) {
      return event.seq && (!result || event.seq < result) ? event.seq : result;
    }, 0);
    if (!oldest) {
      this._loadRecentHistory(this.sessionId, this.subscriptionGeneration).catch(function () {});
      return;
    }
    const self = this;
    this.historyPreserveWindow = true;
    this._renderHistoryNotice();
    const requestSessionId = this.sessionId;
    const requestGeneration = this.subscriptionGeneration;
    const requestedSession = this.sessionId;
    const requestedApiBase = this.apiBase;
    const controller = new AbortController();
    this.olderAbortController = controller;
    const requestStarted = performance.now();
    this.loadingOlder = true;
    this.olderError = "";
    this.updateOlderControl();
    this.apiWithTimeout(
      "/history/" + encodeURIComponent(this.sessionId) +
      "?message_boundaries=true&before_seq=" + oldest + "&limit=" + TRANSCRIPT_PAGE_LIMIT,
      LIVE_SNAPSHOT_TIMEOUT_MS, { signal: controller.signal }
    ).then(function (data) {
      if (
        self.destroyed || controller.signal.aborted ||
        self.sessionId !== requestSessionId || self.sessionId !== requestedSession ||
        self.subscriptionGeneration !== requestGeneration || self.apiBase !== requestedApiBase
      ) return;
      self._validateHistoryPage(data);
      const pageEvents = data.events;
      self.hasOlder = !!(data && data.page && data.page.has_older);
      self.olderCursor = data && data.page && (
        data.page.next_before_seq || data.page.oldest_seq
      );
      const oldHeight = self.els.messages.scrollHeight;
      const oldTop = self.els.messages.scrollTop;
      const anchor = self._captureHistoryAnchor();
      // Read transcriptEvents only after the request resolves. It may now include
      // SSE events that arrived while the durable page was in flight.
      const renderStarted = performance.now();
      const insertedCount = self.prependTranscript(pageEvents);
      const renderMs = performance.now() - renderStarted;
      self.newerCursor = self.transcriptEvents.reduce(function (result, event) {
        return event.seq && event.seq > result ? event.seq : result;
      }, 0) || null;
      self.els.messages.scrollTop = anchoredScrollTop(
        oldTop,
        oldHeight,
        self.els.messages.scrollHeight
      );
      self._restoreHistoryAnchor(anchor);
      const totalMs = performance.now() - requestStarted;
      self.lastHistoryTiming = {
        total_ms: totalMs,
        network_and_parse_ms: renderStarted - requestStarted,
        render_ms: renderMs,
        events: insertedCount,
        server: data && data.diagnostics || null
      };
      console.debug("[PA agent history] page loaded", self.lastHistoryTiming);
      if (totalMs > TRANSCRIPT_LATENCY_BUDGET_MS) {
        console.warn("[PA agent history] interactive latency budget exceeded", self.lastHistoryTiming);
      }
    }).catch(function (err) {
      if (err && err.name === "AbortError") return;
      if (
        self.destroyed || self.sessionId !== requestSessionId ||
        self.sessionId !== requestedSession || self.subscriptionGeneration !== requestGeneration ||
        self.apiBase !== requestedApiBase
      ) return;
      self.olderError = "Could not load older messages: " + err.message;
    }).finally(function () {
      if (self.olderAbortController !== controller) return;
      self.olderAbortController = null;
      if (
        self.destroyed || self.sessionId !== requestSessionId ||
        self.subscriptionGeneration !== requestGeneration
      ) return;
      self.loadingOlder = false;
      self.updateOlderControl();
      self._resumeHistoryAtTail();
    });
  };

  AgentChatWidget.prototype.loadNewerTranscript = function () {
    if (this.recentBootstrap) { this._loadRecentHistory(this.sessionId, this.subscriptionGeneration).catch(function(){}); return; }
    if (!this.historyPreserveWindow && this.liveGapTarget > this.lastSeq) {
      this.liveGapRetryCount = 0;
      this.newerError = "";
      this._repairLiveGap();
      return;
    }
    const pending = this.historyPreserveWindow && this.liveGapTarget > this.lastSeq;
    if (this.loadingNewer || (!this.hasNewer && !this.newerError && !pending) || !this.sessionId || !this.els.messages) return;
    const newest = this.newerCursor || this.transcriptEvents.reduce(function (result, event) {
      return event.seq && event.seq > result ? event.seq : result;
    }, 0);
    if (!newest) return;
    const self = this;
    const requestSessionId = this.sessionId;
    const requestGeneration = this.subscriptionGeneration;
    const requestNavigation = this.historyNavigationGeneration;
    const oldHeight = this.els.messages.scrollHeight;
    const oldTop = this.els.messages.scrollTop;
    this.loadingNewer = true;
    this.newerError = "";
    this.updateNewerControl();
    this.apiWithTimeout(
      "/history/" + encodeURIComponent(this.sessionId) +
      "?message_boundaries=true&after_seq=" + newest + "&limit=" + TRANSCRIPT_PAGE_LIMIT,
      LIVE_SNAPSHOT_TIMEOUT_MS
    ).then(function (data) {
      if (self.destroyed || self.sessionId !== requestSessionId || self.subscriptionGeneration !== requestGeneration ||
          self.historyNavigationGeneration !== requestNavigation) return;
      self._validateHistoryPage(data);
      const pageEvents = data.events;
      self.hasNewer = !!(data && data.page && data.page.has_newer);
      if (!self.hasNewer) self.historyPreserveWindow = false;
      self.renderTranscript(self.transcriptEvents.concat(pageEvents), { scrollBottom: false });
      self.newerCursor = self.transcriptEvents.reduce(function (result, event) {
        return event.seq && event.seq > result ? event.seq : result;
      }, 0) || null;
      self.olderCursor = self.transcriptEvents.reduce(function (result, event) {
        return event.seq && (!result || event.seq < result) ? event.seq : result;
      }, 0) || null;
      self.els.messages.scrollTop = Math.max(0, oldTop - Math.max(0, oldHeight - self.els.messages.scrollHeight));
      self._renderHistoryNotice();
      if (!self.hasNewer) self._repairLiveGap();
    }).catch(function (err) {
      if (self.destroyed || self.sessionId !== requestSessionId || self.subscriptionGeneration !== requestGeneration ||
          self.historyNavigationGeneration !== requestNavigation) return;
      self.newerError = "Could not load newer messages: " + err.message;
    }).finally(function () {
      if (self.destroyed || self.sessionId !== requestSessionId || self.subscriptionGeneration !== requestGeneration ||
          self.historyNavigationGeneration !== requestNavigation) return;
      self.loadingNewer = false;
      self.updateNewerControl();
    });
  };

  AgentChatWidget.prototype.connectSSE = function () {
    const self = this;
    if (this.destroyed || !this.sessionId) return;
    if (this.apiBase === "/api/agent" && sessionListFreshness && sessionListFreshness.source && localSessionWidget() === this) {
      this.externalEventTransport = true;
      this.externalTransportApiBase = this.apiBase;
    }
    if (this.externalEventTransport && this.externalTransportApiBase &&
        this.externalTransportApiBase !== this.apiBase) this.externalEventTransport = false;
    if (this.externalEventTransport) {
      this.closeSSE("external-multiplex");
      return;
    }
    if (
      this.es &&
      this.esSessionId === this.sessionId &&
      this.esApiBase === this.apiBase &&
      this.es.readyState !== EventSource.CLOSED
    ) return;
    this.closeSSE("replaced");
    const url = this.apiBase + "/sessions/" + this.sessionId + "/events?after=" + this.lastSeq;
    const es = new EventSource(url);
    const generation = this.subscriptionGeneration;
    this.es = es;
    this.esSessionId = this.sessionId;
    this.esApiBase = this.apiBase;
    this.sseReconnectCount = 0;
    console.debug("[PA agent SSE] create", {
      sessionId: this.esSessionId,
      apiBase: this.esApiBase,
      reconnectCount: this.sseReconnectCount,
      readyState: es.readyState,
    });
    es.onopen = function () {
      if (self.es !== es || self.destroyed || generation !== self.subscriptionGeneration) {
        es.close();
        return;
      }
      self.connectionNoticeShown = false;
      self.api("/sessions/" + self.esSessionId).then(function (snap) {
        if (self.es !== es || self.destroyed || generation !== self.subscriptionGeneration) return;
        self.applySnapshot(snap);
      }).catch(function (err) {
        if (self.es !== es || self.destroyed || generation !== self.subscriptionGeneration) return;
        const code = apiErrorCode(err);
        if (code === "session_not_live" || code === "session_deleted") {
          self.resolveSessionNotLive(err, self.esSessionId, generation);
        } else {
          self.setStatus("offline");
          self._setRecoveryControl(true, "Retry live state");
        }
      });
    };

    function onAny(ev) {
      if (self.es !== es || self.destroyed || generation !== self.subscriptionGeneration) return;
      try {
        const data = JSON.parse(ev.data);
        self.handleEvent(data, false);
      } catch (_) {
        /* ignore */
      }
    }

    [
      "user_message",
      "agent_message_chunk",
      "agent_thought_chunk",
      "tool_call",
      "tool_call_update",
      "session_info_update",
      "prompt_timing",
      "plan",
      "permission_request",
      "permission_resolved",
      "turn_completed",
      "queue_enqueued",
      "queue_dequeued",
      "queue_removed",
      "queue_reordered",
      "queue_paused",
      "queue_resumed",
      "publication_fence_established",
      "session_control_changed",
      "cancelled",
      "session_started",
      "session_closed",
      "browser_attachment_changed",
      "connection_lost",
      "usage_update",
      "turn_waiting",
      "prompt_failed",
      "model_changed",
      "mode_changed",
      "config_changed",
      "config_option_update",
      "current_mode_update",
      "error",
      "message",
    ].forEach(function (name) {
      es.addEventListener(name, onAny);
    });
    // Do not also set es.onmessage — that would double-dispatch default
    // "message" events (addEventListener("message") is already registered).
    es.onerror = function () {
      if (self.es !== es || self.destroyed || generation !== self.subscriptionGeneration) return;
      self.sseReconnectCount += 1;
      console.debug("[PA agent SSE] reconnect", {
        sessionId: self.esSessionId,
        apiBase: self.esApiBase,
        reconnectCount: self.sseReconnectCount,
        readyState: es.readyState,
      });
      self.setStatus("offline");
      if (self.submissionPending) {
        self.setSubmissionState("reconnecting", true);
        if (self.drafts) {
          self.drafts.setStatus("Reconnecting — the prompt is not confirmed yet.");
        }
      }
      if (!self.connectionNoticeShown) {
        self.connectionNoticeShown = true;
        self.addBubble(
          "system",
          "Connection interrupted. PA is reconnecting; prompts are not confirmed until the session returns online.",
          new Date().toISOString(),
          { system: true, forceVisible: true }
        );
      }
    };
  };

  AgentChatWidget.prototype.useExternalEventTransport = function (enabled) {
    this.externalEventTransport = !!enabled;
    this.externalTransportApiBase = enabled ? this.apiBase : "";
    if (this.externalEventTransport) this.closeSSE("external-multiplex");
    else if (this.sessionId) this.connectSSE();
  };

  AgentChatWidget.prototype.closeSSE = function (reason) {
    const es = this.es;
    if (!es) return;
    console.debug("[PA agent SSE] close", {
      sessionId: this.esSessionId,
      apiBase: this.esApiBase,
      reason: reason || "unspecified",
      reconnectCount: this.sseReconnectCount,
      readyState: es.readyState,
    });
    es.close();
    this.es = null;
    this.esSessionId = "";
    this.esApiBase = "";
  };

  AgentChatWidget.prototype.destroy = function (reason) {
    if (this.destroyed) return;
    clearTimeout(this.queueRefreshTimer);
    if (this.issues) this.issues.reset();
    if (this.threads) this.threads.destroy();
    this._stashSessionDomCache();
    if (this.recentRetryTimer) clearTimeout(this.recentRetryTimer);
    this.recentRetryTimer = null;
    if (this.drafts) this.drafts.destroy();
    this.destroyed = true;
    if (this.routeAbortController) this.routeAbortController.abort();
    this.routeAbortController = null;
    if (this.olderAbortController) this.olderAbortController.abort();
    this.olderAbortController = null;
    this.closeSSE(reason || "widget-destroyed");
    this.stopBrowserRefresh();
    this.setTurnActive(false);
    if (this.startupRetryId) clearTimeout(this.startupRetryId);
    this.startupRetryId = null;
    if (this.liveStateRetryId) clearTimeout(this.liveStateRetryId);
    this.liveStateRetryId = null;
    if (this.submissionReconcileId) clearTimeout(this.submissionReconcileId);
    this.submissionReconcileId = null;
    if (this.liveGapRetryTimer) clearTimeout(this.liveGapRetryTimer);
    this.liveGapRetryTimer = null;
    Object.keys(this.toolTimers).forEach(function (key) {
      const timer = this.toolTimers[key];
      if (timer && timer.interval) clearInterval(timer.interval);
    }, this);
    this.toolTimers = {};
    this.clearPendingImages();
    this.transcriptEvents = [];
    this.seenEvents = {};
    this.streaming = {};
    this.resetArtifacts();
    if (this.els.messages) {
      Array.from(this.els.messages.querySelectorAll(".acw-msg")).forEach(function (row) { row.remove(); });
    }
    if (this.root && this.root._acw === this) this.root._acw = null;
  };

  AgentChatWidget.prototype._clearResolvedLiveGap = function () {
    // Once retired, a late response must not clear an unrelated paging error.
    if (!this.liveGapTarget) return true;
    if ((this.lastSeq || 0) < (this.liveGapTarget || 0)) return false;
    if (this.liveGapRetryTimer) clearTimeout(this.liveGapRetryTimer);
    this.liveGapRetryTimer = null;
    this.liveGapTarget = 0;
    this.livePendingEvents = {};
    this.liveGapRetryCount = 0;
    this.newerError = "";
    this.updateNewerControl();
    this._renderHistoryNotice();
    return true;
  };

  AgentChatWidget.prototype._repairLiveGap = function () {
    if (this.destroyed || !this.sessionId) return;
    if (this.historyPreserveWindow) { this._renderHistoryNotice(); return; }
    if (this.recentBootstrap) {
      if ((this.recentRetryCount || 0) <= 5 && !this.recentRetryTimer && !this.recentLoad) this._loadRecentHistory(this.sessionId, this.subscriptionGeneration).catch(function(){});
      return;
    }
    // Live delivery can finish recovery before a retry timer or history read.
    if (this._clearResolvedLiveGap()) return;
    if (this.liveGapLoading && this.liveGapGeneration === this.subscriptionGeneration &&
        this.liveGapApiBase === this.apiBase) return;
    if (this.liveGapRetryTimer) clearTimeout(this.liveGapRetryTimer);
    this.liveGapRetryTimer = null;
    const self = this, sessionId = this.sessionId, generation = this.subscriptionGeneration;
    const apiBase = this.apiBase;
    const isCurrent = function () {
      return self._isCurrentSessionRequest(sessionId, generation) && self.apiBase === apiBase;
    };
    const after = this.lastSeq || 0;
    this.liveGapRetryCount = (this.liveGapRetryCount || 0) + 1;
    this.liveGapLoading = true;
    this.liveGapGeneration = generation;
    this.liveGapApiBase = apiBase;
    this.apiWithTimeout("/history/" + encodeURIComponent(sessionId) +
      "?message_boundaries=true&after_seq=" + after + "&limit=" + TRANSCRIPT_PAGE_LIMIT,
      LIVE_SNAPSHOT_TIMEOUT_MS).then(function (history) {
        if (!isCurrent() || self.historyPreserveWindow || self._clearResolvedLiveGap()) return;
        const events = history.events || [];
        events.forEach(function (event) { self.handleEvent(event, false, true, true); });
        const pending = self.livePendingEvents || {};
        Object.keys(pending).map(Number).sort(function(a,b){return a-b;}).forEach(function(seq) {
          if (seq <= self.lastSeq) delete pending[seq];
          else if (seq === self.lastSeq + 1) {
            self.handleEvent(pending[seq], false, true, true);
            delete pending[seq];
          }
        });
        if (self._clearResolvedLiveGap()) return;
        if (self.lastSeq <= after) throw new Error("Waiting for durable stream history.");
        self.newerError = "";
        self.liveGapRetryCount = 0;
        if (self.liveGapRetryTimer) clearTimeout(self.liveGapRetryTimer);
        self.liveGapRetryTimer = null;
        self.updateNewerControl();
      }).catch(function (error) {
        if (!isCurrent() || self.historyPreserveWindow || self._clearResolvedLiveGap()) return;
        self.newerError = "Live messages are waiting for history. Retrying…";
        self.updateNewerControl();
        self._renderHistoryNotice();
        if (self.liveGapRetryCount >= 30) {
          self.newerError = "Live recovery is still pending. Retry loading newer messages.";
          self.updateNewerControl();
        }
        if (self.liveGapRetryCount < 30 && !self.liveGapRetryTimer) {
          const timer = setTimeout(function () {
            if (!isCurrent() || self.liveGapRetryTimer !== timer) return;
            self.liveGapRetryTimer = null;
            self._repairLiveGap();
          }, 2000);
          self.liveGapRetryTimer = timer;
        }
      }).finally(function () {
        if (!isCurrent()) return;
        self.liveGapLoading = false;
        if (!self.newerError && self.lastSeq < self.liveGapTarget) self._repairLiveGap();
      });
  };

  AgentChatWidget.prototype.handleEvent = function (event, replay, record, ordered) {
    if (!event || (event.session_id && event.session_id !== this.sessionId)) return;
    event = this._normalizeEvent(event);
    const seq = event.seq || 0;
    // Retired dedupe keys are safe to discard because a live transport must
    // never apply a sequence at or behind the high-water mark again.
    if (!replay && seq && seq <= this.lastSeq) return;
    if (!replay && !ordered && seq && (this.recentBootstrap || this.historyPreserveWindow)) {
      this._holdHistoryEvent(event);
      return;
    }
    if (!replay && !ordered && seq > (this.lastSeq || 0) + 1 && this.sessionId) {
      this._holdHistoryEvent(event);
      this._repairLiveGap();
      return;
    }
    const eventKey = this._eventKey(event);
    if (eventKey && this.seenEvents[eventKey]) return;
    const shouldFollow = !replay && this.isNearBottom();
    const self = this;
    const type = event.type || event.event_type;
    const payload = event.payload || {};
    const created = event.created_at;
    this.renderingEvent = event;

    switch (type) {
      case "user_message":
        if (this.drafts && typeof this.drafts.observeAcceptance === "function") {
          this.drafts.observeAcceptance(payload.id, false);
        }
        if (!replay) this.refreshQueue();
        const userText = payload.message || imageSummary(payload.images);
        // Skip duplicate if we already painted an optimistic bubble for this text.
        if (!this._isDuplicateUserBubble(userText)) {
          this.addBubble("user", payload.message || "", created, {
            system: payload.source === "system",
            images: payload.images || [],
          });
        }
        // A live user_message is emitted when the runtime actually begins a
        // turn (including a prompt drained from the queue). Transcript replay
        // is reconciled against the authoritative snapshot in applySnapshot.
        if (!replay) this.setTurnActive(true, created, true);
        break;
      case "agent_message_chunk":
        if (this._isCodexProvider()) {
          // Codex commentary/narrative belongs in the main chat transcript.
          this.appendStream("agent", (payload.message_id ? payload.message_id + "::" + (payload.phase || "") : "agent"), payload.text || "", created, payload.content_mode);
        } else if (payload.phase === "commentary") {
          this.appendActivityProgress(payload.message_id || "progress", payload.text || "", created, payload.content_mode);
        } else {
          this.appendStream("agent", (payload.message_id ? payload.message_id + "::" + (payload.phase || "") : "agent"), payload.text || "", created, payload.content_mode);
        }
        break;
      case "card_disposition":
        this.renderCardDisposition(payload, created);
        break;
      case "agent_thought_chunk":
        if (this._isCodexProvider()) {
          // Codex thought/explanation headings belong in Tool activity and
          // become the nesting parent for subsequent tool rows.
          this.appendExplanationHeading(payload.message_id || "thought", payload.text || "", created, payload.content_mode);
        } else if (this.showThinking) {
          this.appendStream("thought", payload.message_id || "thought", payload.text || "", created, payload.content_mode);
        }
        break;
      case "session_info_update":
        if (!replay) this.updateTitle(payload.title, payload.title_version);
        break;
      case "prompt_timing":
        this.renderTimings(payload);
        break;
      case "tool_call":
        // Cursor reuses a null messageId for the whole turn, so without this
        // post-tool text is appended onto the pre-tool bubble ("needed.Monica").
        this.finalizeStreams(created, true);
        if (!replay && payload.pa_mcp) this.renderMcpHealth(payload.pa_mcp);
        this.upsertTool(payload, created);
        break;
      case "tool_call_update":
        if (!replay && payload.pa_mcp) this.renderMcpHealth(payload.pa_mcp);
        this.upsertTool(payload, created);
        break;
      case "plan":
        this.renderPlan(payload, created);
        break;
      case "permission_request":
        this.showPermission(payload);
        if (!replay) this.refreshQueue();
        break;
      case "permission_resolved":
        this.hidePermission(payload.request_id);
        if (!replay) this.refreshQueue();
        break;
      case "turn_completed":
        this.finalizeStreams(created);
        this.finalizeActivity();
        if (!replay) this.setTurnActive(false);
        if (!replay) this.refreshQueue();
        if (payload.usage) this.renderMetrics({ last_usage: payload.usage });
        break;
      case "queue_enqueued":
        if (this.drafts && typeof this.drafts.observeAcceptance === "function") {
          this.drafts.observeAcceptance(payload.id, true);
        }
        if (!replay) this.refreshQueue();
        break;
      case "queue_dequeued":
      case "queue_removed":
      case "queue_reordered":
      case "queue_paused":
      case "queue_resumed":
      case "publication_fence_established":
      case "session_control_changed":
        if (!replay) this.refreshQueue();
        break;
      case "cancelled":
        this.finalizeStreams(created);
        this.finalizeActivity();
        if (!replay) this.setTurnActive(false);
        if (!replay) this.queuePaused = !!payload.pause_queue;
        break;
      case "usage_update":
        if (payload.usage) this.renderMetrics({ usage: payload.usage });
        break;
      case "model_changed":
      case "mode_changed":
      case "current_mode_update":
      case "config_changed":
      case "config_option_update":
      case "config_options_update":
      case "configuration_changed":
        if (!replay) {
          const sid = this.sessionId, generation = this.subscriptionGeneration;
          const revision = this.optionSnapshotRevision = (this.optionSnapshotRevision || 0) + 1;
          this.api("/sessions/" + sid).then(function (snap) {
            if (self.sessionId !== sid || self.subscriptionGeneration !== generation || self.optionSnapshotRevision !== revision) return;
            self.applyOptionSnapshot(snap);
            patchSessionListFromSnapshot(self.sessionId, snap);
            refreshSessionList(self.sessionId, true);
          }).catch(function () { /* ignore */ });
        }
        break;
      case "available_commands_update":
        if (!replay) this.refreshCommandCatalog(true);
        break;
      case "command_result":
        this.addBubble("system", payload.reason || "Command finished.", created, { system: true, forceVisible: true });
        break;
      case "session_closed":
        // A past provider closure is history, not current conversation state.
        if (replay) break;
        this.markSessionEnded("Session ended. Start or select another session to send more prompts.");
        refreshSessionList(null);
        break;
      case "browser_attachment_changed":
        this.applyBrowserState(payload);
        if (payload.attached) this.refreshBrowser();
        break;
      case "connection_lost":
        this.finalizeStreams(created);
        this.finalizeActivity();
        if (!replay) this.setTurnActive(false);
        if (!replay) this.setStatus("offline");
        this.addBubble("system", payload.message || "Connection to the agent was lost. You may want to retry the prompt.", created, { forceVisible: true });
        break;
      case "turn_waiting":
        if (!replay && !this.turnActive) this.setTurnActive(true, created);
        this.addBubble("system", payload.message || "Waiting for the agent…", created, { system: true, forceVisible: true });
        break;
      case "prompt_failed":
        this.finalizeStreams(created);
        this.finalizeActivity();
        if (!replay) this.setTurnActive(false);
        this.addBubble("system", payload.error || payload.message || "The prompt failed.", created, { system: true, forceVisible: true });
        break;
      case "error":
        this.addBubble("system", payload.message || "Error", created, { system: true });
        break;
      default:
        break;
    }
    this.renderingEvent = null;
    if (eventKey) this.seenEvents[eventKey] = true;
    if (record !== false) {
      if (type === "agent_message_chunk" && payload.content_mode && payload.message_id) {
        const stream = this.streaming["agent:" + payload.message_id + "::" + (payload.phase || "")];
        if (stream) {
          let firstSeq = stream.firstSeq || seq;
          for (let index = this.transcriptEvents.length - 1; index >= 0; index -= 1) {
            const previous = this.transcriptEvents[index];
            if (previous.type === "turn_completed" || previous.type === "user_message") break;
            if (previous.type === type && previous.payload.message_id === payload.message_id && previous.payload.phase === payload.phase) {
              firstSeq = previous.payload.source_first_seq || previous.seq;
              this.transcriptEvents.splice(index, 1);
              delete this.seenEvents[this._eventKey(previous)];
              break;
            }
          }
          event = Object.assign({}, event, { payload: Object.assign({}, payload, {
            text: stream.text, content_mode: "snapshot", source_first_seq: firstSeq,
          }) });
        }
      }
      const previous = this.transcriptEvents[this.transcriptEvents.length - 1];
      const tail = this._compactEvents(previous ? [previous, event] : [event]);
      if (previous && tail.length === 1) {
        this.transcriptEvents.pop();
        const previousKey = this._eventKey(previous);
        if (previousKey) delete this.seenEvents[previousKey];
      }
      this.transcriptEvents.push(tail[tail.length - 1]);
      if (this.transcriptEvents.length > MAX_RETAINED_EVENTS) {
        // When the user is browsing an older durable page, retain that window
        // and let forward paging retrieve live arrivals from durable storage.
        // Active stream/tool UI state is maintained independently below.
        const evicted = this.hasNewer
          ? this.transcriptEvents.pop()
          : this.transcriptEvents.shift();
        const evictedKey = this._eventKey(evicted);
        if (evictedKey) delete this.seenEvents[evictedKey];
        this.hasOlder = true;
        this.olderCursor = this.transcriptEvents.length
          ? this.transcriptEvents[0].payload.source_first_seq || this.transcriptEvents[0].seq : null;
        this.updateOlderControl();
      }
    }
    if (!replay && (type === "tool_call" || type === "tool_call_update")) this.renderTerminal();
    if (seq) this.lastSeq = Math.max(this.lastSeq, seq);
    if (!replay && this.liveGapTarget > 0) this._clearResolvedLiveGap();
    if (!replay) this._pruneMessageRows(!!this.hasNewer);
    if (!replay && shouldFollow) this.scrollToBottom();
  };

  AgentChatWidget.prototype.addBubble = function (role, text, ts, opts) {
    opts = opts || {};
    this.clearPlaceholder();
    const row = document.createElement("div");
    row.className = "acw-msg acw-msg-" + role + (opts.system ? " is-system" : "");
    if (this.renderingEvent) row.dataset.sourceSeq = this.renderingEvent.payload.source_first_seq || this.renderingEvent.seq;
    if (opts.system && !this.showSystem) row.hidden = true;
    const bubble = document.createElement("div");
    bubble.className = "acw-bubble acw-bubble-" + role;
    const images = opts.images || [];
    if (images.length) {
      const gallery = document.createElement("div");
      gallery.className = "acw-message-images";
      images.forEach(function (image) {
        if (image.preview) {
          const preview = document.createElement("img");
          preview.src = image.preview;
          preview.alt = image.name || "Attached image";
          gallery.appendChild(preview);
        } else {
          const attachment = document.createElement("span");
          attachment.className = "acw-message-image-name";
          attachment.textContent = image.name || "Attached image";
          gallery.appendChild(attachment);
        }
      });
      bubble.appendChild(gallery);
    }
    const content = text || imageSummary(images);
    if (role === "user" || role === "agent" || role === "thought") {
      bubble.dataset.markdown = content;
      this.renderMarkdownBubble(bubble);
    } else {
      bubble.appendChild(document.createTextNode(content));
    }
    row.appendChild(bubble);
    if (ts) {
      const time = document.createElement("time");
      time.className = "acw-ts muted";
      time.dateTime = ts;
      time.textContent = new Date(ts).toLocaleTimeString();
      row.appendChild(time);
    }
    this.els.messages.appendChild(row);
    this.messageRowCount = (this.messageRowCount || 0) + 1;
    return { row: row, bubble: bubble };
  };

  AgentChatWidget.prototype.renderCardDisposition = function (payload, ts) {
    payload = payload || {};
    const contract = payload.contract || {};
    const raw = payload.raw || (contract.contract
      ? JSON.stringify(contract, null, 2) : "");
    if (raw) {
      this.els.messages.querySelectorAll(".acw-msg-agent .acw-bubble-agent").forEach(function (bubble) {
        if ((bubble.dataset.markdown || "").trim() === raw.trim()) {
          bubble.closest(".acw-msg").hidden = true;
        }
      });
    }

    const existing = this.els.messages.querySelector("[data-card-disposition-status]");
    if (existing) existing.closest(".acw-msg").remove();
    const authority = payload.authority_acknowledged === true;
    const reason = payload.reason || "";
    const status = payload.status || "invalid";
    let state = "pending";
    if (payload.persistence === "failed") state = "persistence-failed";
    else if (status === "invalid" || !contract.contract) state = "invalid";
    else if (/head/i.test(reason) && /(stale|match|confirm)/i.test(reason)) state = "stale-head";
    else if (status === "downgraded" || /(evidence|required|missing|not merged)/i.test(reason)) state = "incomplete-evidence";
    else if (authority && ["applied", "preserved_done"].indexOf(status) >= 0) state = "accepted";
    else if (authority) state = "rejected";

    const lane = payload.lane_after || contract.lane || "unknown";
    const outcome = contract.outcome || reason || "Card disposition could not be validated.";
    const created = this.addBubble("system", "", ts, { forceVisible: true });
    created.row.hidden = false;
    created.bubble.classList.add("acw-disposition");
    created.bubble.dataset.cardDispositionStatus = state;
    created.bubble.setAttribute("role", "status");
    created.bubble.setAttribute("aria-label", "Card disposition " + state);

    const heading = document.createElement("strong");
    heading.className = "acw-disposition-title";
    heading.textContent = outcome;
    created.bubble.appendChild(heading);
    const meta = document.createElement("p");
    meta.className = "acw-disposition-meta";
    meta.textContent = "Lane: " + lane + " · " + (
      authority ? "PA authority acknowledged: " + state : "Awaiting durable PA authority acknowledgement"
    );
    created.bubble.appendChild(meta);
    if (reason && reason !== outcome) {
      const explanation = document.createElement("p");
      explanation.className = "acw-disposition-reason";
      explanation.textContent = reason;
      created.bubble.appendChild(explanation);
    }

    const evidence = contract.evidence || {};
    const evidenceItems = [
      ["Integration", evidence.integration_required],
      ["PR watch", evidence.pr_watch_id],
      ["Head", evidence.watched_head_sha],
      ["Merge", evidence.merge_commit_sha],
    ].filter(function (item) { return item[1] !== null && item[1] !== undefined && item[1] !== ""; });
    if (evidenceItems.length) {
      const list = document.createElement("dl");
      list.className = "acw-disposition-evidence";
      evidenceItems.forEach(function (item) {
        const dt = document.createElement("dt");
        const dd = document.createElement("dd");
        dt.textContent = item[0];
        dd.textContent = String(item[1]);
        list.appendChild(dt);
        list.appendChild(dd);
      });
      created.bubble.appendChild(list);
    }

    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.textContent = "Diagnostics and raw payload";
    details.appendChild(summary);
    const pre = document.createElement("pre");
    pre.textContent = raw || JSON.stringify(payload, null, 2);
    details.appendChild(pre);
    const copy = document.createElement("button");
    copy.type = "button";
    copy.className = "ghost small";
    copy.textContent = "Copy raw payload";
    copy.addEventListener("click", function () {
      const value = pre.textContent || "";
      Promise.resolve(navigator.clipboard && navigator.clipboard.writeText
        ? navigator.clipboard.writeText(value)
        : Promise.reject(new Error("Clipboard unavailable")))
        .then(function () { copy.textContent = "Copied"; })
        .catch(function () { copy.textContent = "Copy unavailable"; });
    });
    details.appendChild(copy);
    created.bubble.appendChild(details);
  };

  AgentChatWidget.prototype.renderMarkdownBubble = function (bubble) {
    const content = bubble.dataset.markdown || "";
    bubble.dataset.rawText = this.rawText ? "1" : "0";
    // Preserve attachment gallery across innerHTML replacement.
    const gallery = bubble.querySelector(".acw-message-images");
    if (gallery) gallery.remove();
    if (this.rawText) {
      bubble.textContent = content;
    } else {
      bubble.innerHTML = renderMarkdown(content);
      if (window.PALinks) window.PALinks.decorate(bubble);
    }
    if (gallery) bubble.insertBefore(gallery, bubble.firstChild);
  };

  AgentChatWidget.prototype.rerenderMarkdownBubbles = function () {
    const self = this;
    this.root.querySelectorAll(".acw-bubble-user, .acw-bubble-agent, .acw-bubble-thought, .acw-progress-update").forEach(function (bubble) {
      self.renderMarkdownBubble(bubble);
    });
  };

  AgentChatWidget.prototype.appendStream = function (role, key, chunk, ts, contentMode) {
    this.clearPlaceholder();
    const id = role + ":" + key;
    let stream = this.streaming[id];
    if (!stream) {
      const created = this.addBubble(role === "thought" ? "thought" : "agent", "", ts);
      if (role === "thought") created.row.classList.add("acw-msg-thought");
      stream = { text: "", bubble: created.bubble, row: created.row,
        firstSeq: this.renderingEvent && (this.renderingEvent.payload.source_first_seq || this.renderingEvent.seq) };
      this.streaming[id] = stream;
    }
    const next = chunk || "";
    if (["snapshot", "replace", "replacement", "accumulated"].indexOf(contentMode) !== -1) {
      stream.text = next;
    } else if (contentMode) {
      stream.text += next;
    } else {
      stream.text += streamChunkSeparator(stream.text, next) + next;
    }
    if (stream.text.length > MAX_STREAM_CHARS) {
      stream.text = "[… earlier streamed text is available in durable history …]\n" +
        stream.text.slice(-MAX_STREAM_CHARS);
    }
    stream.bubble.dataset.markdown = stream.text;
    this.renderMarkdownBubble(stream.bubble);
  };

  AgentChatWidget.prototype.finalizeStreams = function (ts, anonymousOnly) {
    const self = this;
    Object.keys(this.streaming).forEach(function (id) {
      const stream = self.streaming[id];
      if (stream && stream.row && ts && !stream.row.querySelector("time")) {
        const time = document.createElement("time");
        time.className = "acw-ts muted";
        time.dateTime = ts;
        time.textContent = new Date(ts).toLocaleTimeString();
        stream.row.appendChild(time);
      }
    });
    if (anonymousOnly) {
      delete this.streaming["agent:agent"];
      delete this.streaming["thought:thought"];
    } else this.streaming = {};
  };

  AgentChatWidget.prototype._isCodexProvider = function () {
    const provider = String(this.providerId || this.preferredProvider || "").toLowerCase();
    return provider === "codex";
  };

  AgentChatWidget.prototype.appendExplanationHeading = function (key, chunk, ts, contentMode) {
    this.clearPlaceholder();
    const shouldFollow = this.toolActivityIsNearBottom();
    const activity = this.ensureActivity();
    const id = "explanation:" + key;
    let stream = this.activityStreams[id];
    if (!stream) {
      const group = document.createElement("div");
      group.className = "acw-explanation";
      group.setAttribute("role", "group");
      group.setAttribute("aria-label", "Agent explanation");
      const heading = document.createElement("div");
      heading.className = "acw-progress-update acw-explanation-heading";
      group.appendChild(heading);
      const tools = document.createElement("div");
      tools.className = "acw-explanation-tools";
      group.appendChild(tools);
      activity.appendChild(group);
      stream = { text: "", el: heading, group: group, tools: tools };
      this.activityStreams[id] = stream;
      this.activeExplanation = stream;
      this.bumpActivityCount(activity);
    } else {
      this.activeExplanation = stream;
    }
    const next = chunk || "";
    if (["snapshot", "replace", "replacement", "accumulated"].indexOf(contentMode) !== -1) stream.text = next;
    else stream.text += (contentMode ? "" : streamChunkSeparator(stream.text, next)) + next;
    if (stream.text.length > MAX_STREAM_CHARS) {
      stream.text = "[… earlier explanation is available in durable history …]\n" +
        stream.text.slice(-MAX_STREAM_CHARS);
    }
    stream.el.dataset.markdown = stream.text;
    this.renderMarkdownBubble(stream.el);
    this._pruneActivityRows();
    this.followToolActivity(shouldFollow);
  };

  AgentChatWidget.prototype.ensureActivity = function () {
    this.currentActivity = this.els.toolActivity;
    if (this.els.toolEmpty) this.els.toolEmpty.hidden = true;
    return this.currentActivity;
  };

  AgentChatWidget.prototype._toolParent = function () {
    if (this._isCodexProvider() && this.activeExplanation && this.activeExplanation.tools) {
      return this.activeExplanation.tools;
    }
    return this.ensureActivity();
  };

  AgentChatWidget.prototype.bumpActivityCount = function (activity) {
    this.activityCount += 1;
  };

  AgentChatWidget.prototype.toolActivityIsNearBottom = function () {
    const container = this.els.toolFlyout || this.els.toolActivity;
    if (!container) return false;
    return container.scrollHeight - container.scrollTop - container.clientHeight < 72;
  };

  AgentChatWidget.prototype.followToolActivity = function (shouldFollow) {
    const container = this.els.toolFlyout || this.els.toolActivity;
    if (shouldFollow && container) container.scrollTop = container.scrollHeight;
  };

  AgentChatWidget.prototype.appendActivityProgress = function (key, chunk, ts, contentMode) {
    this.clearPlaceholder();
    const shouldFollow = this.toolActivityIsNearBottom();
    const activity = this.ensureActivity();
    const id = "progress:" + key;
    let stream = this.activityStreams[id];
    if (!stream) {
      const el = document.createElement("div");
      el.className = "acw-progress-update";
      activity.appendChild(el);
      stream = { text: "", el: el };
      this.activityStreams[id] = stream;
      this.bumpActivityCount(activity);
    }
    const next = chunk || "";
    if (["snapshot", "replace", "replacement", "accumulated"].indexOf(contentMode) !== -1) stream.text = next;
    else stream.text += (contentMode ? "" : streamChunkSeparator(stream.text, next)) + next;
    if (stream.text.length > MAX_STREAM_CHARS) {
      stream.text = "[… earlier progress is available in durable history …]\n" +
        stream.text.slice(-MAX_STREAM_CHARS);
    }
    stream.el.dataset.markdown = stream.text;
    this.renderMarkdownBubble(stream.el);
    this._pruneActivityRows();
    this.followToolActivity(shouldFollow);
  };

  AgentChatWidget.prototype._pruneActivityRows = function () {
    if (!this.els.toolActivity || this.activityCount == null || this.activityCount <= MAX_ACTIVITY_ROWS) return;
    let rows = Array.from(this.els.toolActivity.querySelectorAll(
      ".acw-tool,.acw-progress-update,.acw-explanation"
    ));
    while (rows.length > MAX_ACTIVITY_ROWS) {
      const stale = rows.find(function (row) {
        return !row.dataset.toolId || !this.activeToolIds[row.dataset.toolId];
      }, this);
      if (!stale) break;
      const staleId = stale.dataset.toolId;
      if (staleId && this.toolTimers[staleId]) {
        if (this.toolTimers[staleId].interval) clearInterval(this.toolTimers[staleId].interval);
        delete this.toolTimers[staleId];
      }
      stale.remove();
      rows = rows.filter(function (candidate) { return candidate !== stale; });
    }
    this.activityCount = rows.length;
  };

  AgentChatWidget.prototype.finalizeActivity = function () {
    this.currentActivity = null;
    this.activeExplanation = null;
    this.activityStreams = {};
    this.activeToolIds = {};
    this.updateToolAnimation();
  };

  AgentChatWidget.prototype.upsertTool = function (payload, ts) {
    this.clearPlaceholder();
    const shouldFollow = this.toolActivityIsNearBottom();
    const id = payload.tool_call_id || "tool";
    let el = null;
    if (this.els.toolActivity) {
      el = Array.from(this.els.toolActivity.querySelectorAll("[data-tool-id]")).find(function (candidate) {
        return candidate.dataset.toolId === id;
      }) || null;
    }
    if (!el) {
      el = document.createElement("div");
      el.className = "acw-tool";
      el.dataset.toolId = id;
      el.innerHTML =
        '<div class="acw-tool-header">' +
        '<span class="acw-tool-title"></span>' +
        '<span class="acw-tool-timer muted"></span>' +
        '<span class="acw-tool-status muted"></span>' +
        "</div>";
      const parent = this._toolParent();
      parent.appendChild(el);
      this.bumpActivityCount(this.ensureActivity());
      const eventTime = ts ? new Date(ts).getTime() : Date.now();
      this.toolTimers[id] = { started: eventTime, interval: null };
      const timerEl = el.querySelector(".acw-tool-timer");
      const started = this.toolTimers[id].started;
      const tick = function () {
        if (timerEl) timerEl.textContent = formatElapsed(Date.now() - started);
      };
      tick();
      this.toolTimers[id].interval = setInterval(tick, 500);
    }
    this._pruneActivityRows();
    const title = el.querySelector(".acw-tool-title");
    const status = el.querySelector(".acw-tool-status");
    if (title) title.textContent = payload.title || payload.kind || "Tool";
    if (status) status.textContent = payload.status || "";
    if (!payload.status || payload.status === "in_progress" || payload.status === "pending") {
      this.activeToolIds[id] = true;
    } else {
      delete this.activeToolIds[id];
      const t = this.toolTimers[id];
      if (t && t.interval) {
        clearInterval(t.interval);
        t.interval = null;
        const timerEl = el.querySelector(".acw-tool-timer");
        const ended = ts ? new Date(ts).getTime() : Date.now();
        if (timerEl && t.started) timerEl.textContent = formatElapsed(ended - t.started);
      }
    }
    this.updateToolAnimation();
    this.followToolActivity(shouldFollow);
  };

  AgentChatWidget.prototype.updateToolAnimation = function () {
    if (!this.els.toolToggle) return;
    this.els.toolToggle.classList.toggle("is-active", Object.keys(this.activeToolIds).length > 0);
  };

  AgentChatWidget.prototype.resetArtifacts = function () {
    if (this.els.terminalOutput) this.els.terminalOutput.replaceChildren();
    this.currentActivity = null;
    this.activeExplanation = null;
    this.activityStreams = {};
    this.activityCount = 0;
    this.activeToolIds = {};
    this.plans = [];
    if (this.els.toolActivity) {
      Array.from(this.els.toolActivity.children).forEach(function (child) {
        if (!child.hasAttribute("data-acw-tool-empty")) child.remove();
      });
    }
    if (this.els.toolEmpty) this.els.toolEmpty.hidden = false;
    if (this.els.planCount) {
      this.els.planCount.hidden = true;
      this.els.planCount.textContent = "0";
    }
    if (this.els.planList) this.els.planList.innerHTML = "";
    if (this.els.planDetail) this.els.planDetail.innerHTML = '<p class="muted">No plans yet.</p>';
    this.updateToolAnimation();
  };

  AgentChatWidget.prototype.selectPlan = function (index) {
    const plan = this.plans[index];
    if (!plan || !this.els.planDetail) return;
    this.els.planDetail.innerHTML = plan.html;
    if (window.PALinks) window.PALinks.decorate(this.els.planDetail);
    if (this.els.planList) {
      this.els.planList.querySelectorAll("button").forEach(function (button, buttonIndex) {
        button.classList.toggle("active", buttonIndex === index);
      });
    }
  };

  AgentChatWidget.prototype.renderPlan = function (payload, created) {
    this.clearPlaceholder();
    const self = this;
    const entries = payload.entries || [];
    const md = entries
      .map(function (e) {
        const status = e.status || e.priority || "";
        const content = e.content || e.title || JSON.stringify(e);
        return "- [" + status + "] " + content;
      })
      .join("\n");
    const planKey = String(payload.plan_id || payload.id || "current");
    let index = this.plans.findIndex(function (plan) { return plan.key === planKey; });
    const isNew = index < 0;
    if (isNew) {
      index = this.plans.length;
      this.plans.push({ key: planKey });
      if (this.plans.length > MAX_PLANS) {
        this.plans.shift();
        index -= 1;
        if (this.els.planList && this.els.planList.firstElementChild) {
          this.els.planList.firstElementChild.remove();
        }
      }
    }
    this.plans[index] = {
      key: planKey,
      html: renderMarkdown(md || "_Empty plan_"),
      created: created,
      entries: entries,
      title: payload.title || (planKey === "current" ? "Current plan" : "Plan " + (index + 1)),
    };
    if (this.els.planList && isNew) {
      const item = document.createElement("li");
      const button = document.createElement("button");
      button.type = "button";
      button.className = "ghost";
      button.dataset.planKey = planKey;
      button.addEventListener("click", function () {
        self.selectPlan(self.plans.findIndex(function (plan) { return plan.key === planKey; }));
      });
      item.appendChild(button);
      this.els.planList.appendChild(item);
    }
    if (this.els.planList) {
      const button = Array.from(this.els.planList.querySelectorAll("button")).find(function (candidate) {
        return candidate.dataset.planKey === planKey;
      });
      if (button) {
        button.innerHTML = "<strong>" + escapeHtml(this.plans[index].title) + "</strong>" +
          (created ? '<span class="muted">' + escapeHtml(new Date(created).toLocaleTimeString()) + "</span>" : "");
      }
    }
    if (this.els.planCount) {
      const completed = entries.filter(function (entry) {
        return ["completed", "complete", "done"].indexOf(String(entry.status || "").toLowerCase()) >= 0;
      }).length;
      this.els.planCount.hidden = entries.length === 0;
      this.els.planCount.textContent = completed + " of " + entries.length;
    }
    this.selectPlan(index);
  };

  AgentChatWidget.prototype.showPermission = function (payload) {
    if (!this.els.permissions) return;
    const self = this;
    this.els.permissions.hidden = false;
    const reqId = payload.request_id;
    let card = this.els.permissions.querySelector('[data-req-id="' + reqId + '"]');
    if (!card) {
      card = document.createElement("div");
      card.className = "acw-permission-card";
      card.dataset.reqId = reqId;
      this.els.permissions.appendChild(card);
    }
    const tool = payload.tool_call || {};
    const options = payload.options || [];
    card.innerHTML =
      "<strong>Permission required</strong>" +
      '<p class="muted">' +
      escapeHtml(tool.title || tool.kind || "Tool call") +
      "</p>" +
      '<div class="acw-permission-actions"></div>';
    const actions = card.querySelector(".acw-permission-actions");
    options.forEach(function (opt) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = opt.kind && String(opt.kind).indexOf("allow") === 0 ? "primary small" : "ghost small";
      btn.textContent = opt.name || opt.optionId || opt.option_id;
      btn.addEventListener("click", function () {
        const allow = opt.kind && String(opt.kind).indexOf("allow") === 0;
        self.api("/sessions/" + self.sessionId + "/permissions/" + reqId, {
          method: "POST",
          body: JSON.stringify({
            allow: !!allow,
            option_id: opt.optionId || opt.option_id,
            remember: opt.kind === "allow_always",
            scope: "user",
          }),
        });
      });
      actions.appendChild(btn);
    });
    const always = document.createElement("button");
    always.type = "button";
    always.className = "ghost small";
    always.textContent = "Always allow";
    always.addEventListener("click", function () {
      const allowOpt = options.find(function (o) {
        return o.kind === "allow_always" || o.kind === "allow_once";
      });
      self.api("/sessions/" + self.sessionId + "/permissions/" + reqId, {
        method: "POST",
        body: JSON.stringify({
          allow: true,
          option_id: allowOpt ? allowOpt.optionId || allowOpt.option_id : null,
          remember: true,
          scope: "user",
        }),
      });
    });
    actions.appendChild(always);
  };

  AgentChatWidget.prototype.hidePermission = function (requestId) {
    if (!this.els.permissions) return;
    const card = this.els.permissions.querySelector('[data-req-id="' + requestId + '"]');
    if (card) card.remove();
    if (!this.els.permissions.children.length) this.els.permissions.hidden = true;
  };

  AgentChatWidget.prototype.renderQueue = function (queue, snapshot) {
    if (!this.showQueue || !this.els.queue || !this.els.queueList) return;
    const self = this;
    const snap = snapshot || this.lastSnapshot || {};
    const status = this.root.querySelector("[data-acw-queue-status]");
    if (status) {
      const waiting = this.queuePaused ? "Paused — queued messages wait for Resume." :
        snap.prompting ? "Queued messages wait for the current reply." :
        snap.connected === false ? "Queued messages wait for reconnection." :
        queue.some(function (q) { return q.display && q.display.is_next; }) ? "Next eligible message is queued." :
        queue.length ? "Queued messages wait for eligibility to be confirmed." : "Queue is empty.";
      if (status.textContent !== waiting) status.textContent = waiting;
    }
    this.els.queue.hidden = !queue.length && !this.queuePaused;
    const meta = this.root.querySelector("[data-acw-queue-meta]");
    if (meta) {
      const parts = [];
      if (queue.length) parts.push("(" + queue.length + ")");
      if (this.queuePaused) parts.push("paused");
      meta.textContent = parts.join(" ");
    }
    const pauseBtn = this.root.querySelector("[data-acw-queue-pause]");
    const resumeBtn = this.root.querySelector("[data-acw-queue-resume]");
    if (pauseBtn) pauseBtn.disabled = !!this.queuePaused;
    if (resumeBtn) resumeBtn.disabled = !this.queuePaused;
    const focused = document.activeElement;
    const focusedRow = focused && focused.closest && focused.closest("[data-acw-queue-list] li");
    const focusId = focusedRow && focusedRow.dataset.id;
    const focusAction = focused && focused.dataset && focused.dataset.queueAction;
    this.els.queueList.innerHTML = "";
    function reorder(from, to) {
      if (!Number.isInteger(from) || from < 0 || from >= queue.length || to < 0 || to >= queue.length || from === to) return;
      const ids = queue.map(function (q) { return q.id; });
      ids.splice(to, 0, ids.splice(from, 1)[0]);
      self.api("/sessions/" + self.sessionId + "/queue/reorder", {
        method: "POST", body: JSON.stringify({ prompt_ids: ids }),
      }).then(function () { self.refreshQueue(); });
    }
    queue.forEach(function (item, index) {
      const li = document.createElement("li");
      li.draggable = true;
      li.dataset.id = item.id;
      const queueText = item.message || imageSummary(item.images);
      const display = item.display || {};
      const position = index + 1; // Preserve the server's array order, including held items.
      const isNext = display.is_next === true && display.eligibility === "eligible";
      const source = ["Human", "Response", "Automated", "Other"].includes(display.source) ? display.source : "Other";
      const eligibility = {held: "Held during human control", checking: "Checking eligibility", admission_pending: "Awaiting admission"}[display.eligibility] || "";
      const description = source + " message at queue position " + position;
      li.dataset.next = String(isNext);
      li.innerHTML =
        '<span class="acw-queue-order" aria-label="' + (isNext ? "Next eligible, queue position " : "Queue position ") + position + '">' +
        (isNext ? "Next · " : "") + position + '</span>' +
        '<span class="acw-queue-content"><span class="acw-queue-source">' + source + '</span>' +
        '<span class="acw-queue-text">' + escapeHtml(queueText) + '</span>' +
        (eligibility ? '<span class="acw-queue-eligibility">' + eligibility + '</span>' : '') + '</span>' +
        '<span class="acw-queue-row-actions">' +
        '<button type="button" class="ghost small" data-queue-action="up" aria-label="Move ' + description + ' up"' + (index === 0 ? ' disabled' : '') + '>↑</button>' +
        '<button type="button" class="ghost small" data-queue-action="down" aria-label="Move ' + description + ' down"' + (index === queue.length - 1 ? ' disabled' : '') + '>↓</button>' +
        '<button type="button" class="ghost small" data-queue-action="remove" data-remove aria-label="Remove ' + description + '">✕</button></span>';
      li.querySelector('[data-queue-action="up"]').addEventListener("click", function () { reorder(index, index - 1); });
      li.querySelector('[data-queue-action="down"]').addEventListener("click", function () { reorder(index, index + 1); });
      li.querySelector("[data-remove]").addEventListener("click", function () {
        self.api("/sessions/" + self.sessionId + "/queue/" + item.id, { method: "DELETE" }).then(function () {
          self.refreshQueue();
        });
      });
      li.addEventListener("dragstart", function (e) {
        e.dataTransfer.setData("text/plain", String(index));
      });
      li.addEventListener("dragover", function (e) {
        e.preventDefault();
      });
      li.addEventListener("drop", function (e) {
        e.preventDefault();
        const from = Number(e.dataTransfer.getData("text/plain"));
        const to = index;
        reorder(from, to);
      });
      self.els.queueList.appendChild(li);
      if (focusId === item.id && focusAction) {
        const target = li.querySelector('[data-queue-action="' + focusAction + '"]');
        (target && !target.disabled ? target : li.querySelector("[data-remove]")).focus();
      }
    });
  };

  AgentChatWidget.prototype.refreshQueue = function () {
    const self = this, sid = this.sessionId, generation = this.subscriptionGeneration;
    if (!this.sessionId) return;
    const revision = this.queueRefreshRevision = (this.queueRefreshRevision || 0) + 1;
    clearTimeout(this.queueRefreshTimer);
    this.queueRefreshTimer = setTimeout(function () {
      self.api("/sessions/" + sid).then(function (snap) {
        if (self.destroyed || sid !== self.sessionId || generation !== self.subscriptionGeneration || revision !== self.queueRefreshRevision) return;
        self.lastSnapshot = Object.assign({}, self.lastSnapshot || {}, snap);
        self.queuePaused = !!snap.queue_paused;
        self.renderQueue(snap.queue || [], snap);
        self.renderPresentation(snap);
        self.updateEmptyChatStatus(snap);
      }).catch(function () { /* existing live/recovery UI owns connection errors */ });
    }, 250);
  };

  AgentChatWidget.prototype.renderModelsModes = function (snap) {
    if (this.settingsDirty || this.settingsPending) return;
    if (this.showModel && this.els.model && this.els.modelWrap) {
      const models = (snap.models && (snap.models.availableModels || snap.models.available_models)) || [];
      const current = confirmedSessionFields(sessionRecordFromSnapshot(snap)).modelId ||
        (snap.models && (snap.models.currentModelId || snap.models.current_model_id));
      if (models.length) {
        this.els.modelWrap.hidden = false;
        this.els.model.innerHTML = models
          .map(function (m) {
            const id = m.modelId || m.model_id || m.id;
            const name = m.name || id;
            return '<option value="' + escapeHtml(id) + '"' + (id === current ? " selected" : "") + ">" + escapeHtml(name) + "</option>";
          })
          .join("");
      }
    }
    if (this.showMode && this.els.mode && this.els.modeWrap) {
      const modes = (snap.modes && (snap.modes.availableModes || snap.modes.available_modes)) || [];
      const current = (snap.session && snap.session.mode_id) ||
        (snap.modes && (snap.modes.currentModeId || snap.modes.current_mode_id));
      if (modes.length) {
        this.els.modeWrap.hidden = false;
        this.els.mode.innerHTML = modes
          .map(function (m) {
            const id = m.id || m.modeId || m.mode_id;
            const name = m.name || id;
            return '<option value="' + escapeHtml(id) + '"' + (id === current ? " selected" : "") + ">" + escapeHtml(name) + "</option>";
          })
          .join("");
      }
    }
  };

  AgentChatWidget.prototype.markSettingsDirty = function () {
    this.settingsDirty = true;
    if (this.els.settingsApply) this.els.settingsApply.disabled = false;
    if (this.els.settingsReset) this.els.settingsReset.disabled = false;
    if (this.els.settingsStatus) {
      this.els.settingsStatus.classList.remove("is-error");
      this.els.settingsStatus.textContent = "Unsaved changes.";
    }
  };

  AgentChatWidget.prototype.setSettingsPending = function (pending) {
    this.settingsPending = pending;
    [this.els.model, this.els.mode].concat(Array.from(this.els.config ? this.els.config.querySelectorAll("select,input") : []))
      .filter(Boolean).forEach(function (control) { control.disabled = pending || control.dataset.effortUnavailable === "true"; });
    if (this.els.settingsApply) {
      this.els.settingsApply.disabled = pending || !this.settingsDirty;
      this.els.settingsApply.textContent = pending ? "Applying…" : "Apply";
    }
    if (this.els.settingsReset) this.els.settingsReset.disabled = pending || !this.settingsDirty;
  };

  AgentChatWidget.prototype.resetSettingsDraft = function () {
    this.settingsDirty = false;
    if (this.lastSnapshot) {
      this.renderModelsModes(this.lastSnapshot);
      this.renderConfigOptions(this.lastSnapshot);
    }
    if (this.els.settingsApply) this.els.settingsApply.disabled = true;
    if (this.els.settingsReset) this.els.settingsReset.disabled = true;
    if (this.els.settingsStatus) {
      this.els.settingsStatus.classList.remove("is-error");
      this.els.settingsStatus.textContent = "No unsaved changes.";
    }
  };

  AgentChatWidget.prototype.applySettings = async function () {
    if (!this.settingsDirty || !this.sessionId || this.settingsPending) return;
    const sid = this.sessionId, base = this.apiBase, generation = this.subscriptionGeneration;
    const current = () => !this.destroyed && this.sessionId === sid && this.apiBase === base && this.subscriptionGeneration === generation;
    const snap = this.lastSnapshot || {}, helper = window.PANativeEffort;
    const confirmed = confirmedSessionFields(sessionRecordFromSnapshot(snap));
    const controls = Array.from(this.els.config ? this.els.config.querySelectorAll("[data-acw-config-id]") : []);
    const modelControl = controls.find(c => c.dataset.nativeKind === "model") || this.els.model;
    const effortControl = controls.find(c => c.dataset.nativeKind === "reasoning");
    const model = modelControl && modelControl.value;
    const modelChanged = !!model && (helper ? helper.model(model) : model) !== confirmed.modelId;
    const effort = effortControl && effortControl.value;
    const effortChanged = !!effortControl && (effort !== effortControl.dataset.acwOriginal || modelChanged);
    const path = "/sessions/" + encodeURIComponent(sid);
    const others = controls.filter(c => !["model", "reasoning"].includes(c.dataset.nativeKind) &&
      String(c.type === "checkbox" ? c.checked : c.value) !== String(c.dataset.acwOriginal));
    let fresh, outcome = "Applied successfully.";
    this.setSettingsPending(true);
    try {
      if (effort && (!helper || !helper.evidence(this.effortCatalog || [], this.effortCatalogScope || {}, model || confirmed.modelId).includes(effort))) {
        throw new Error("Effort support is unconfirmed for the selected model. Refresh choices before applying.");
      }
      if (snap.execution_selection && (modelChanged || effortChanged)) {
        const prefs = {};
        if (modelChanged) prefs.model = {intent: "required", value: helper ? helper.model(model) : model};
        if (effortChanged) prefs.reasoning = effort ? {intent: "required", value: effort} : {intent: "automatic"};
        const fingerprint = JSON.stringify([base, sid, prefs, snap.session.updated_at]);
        if (!this.settingsRequestIdentity || this.settingsRequestIdentity.fingerprint !== fingerprint) {
          this.settingsRequestIdentity = {fingerprint: fingerprint, key: crypto.randomUUID()};
        }
        const result = await this.api(path + "/execution-settings", {method: "POST", body: JSON.stringify({
          execution_preferences: prefs, expected_version: snap.session.updated_at, idempotency_key: this.settingsRequestIdentity.key, defer: true})});
        if (!current()) return;
        if (!["applied", "pending"].includes(result.state)) throw new Error("Native settings request: " + (result.state || "unconfirmed"));
        if (result.state === "pending") {
          outcome = "Settings request: " + result.state + ". Waiting for native confirmation; other draft settings were not applied.";
          fresh = await this.api(path);
          return;
        }
      } else {
        if (effortChanged && !effort && !modelChanged && effortControl.dataset.acwOriginal) {
          throw new Error("This provider has no advertised effort-reset action. Choose a supported effort; no reset was applied.");
        }
        if (modelChanged) {
          const configId = modelControl.dataset.acwConfigId;
          await this.api(path + (configId ? "/config" : "/model"), {method: "PUT", body: JSON.stringify(
            configId ? {config_id: configId, value: model} : {model_id: model})});
          if (!current()) return;
          fresh = await this.api(path);
          if (!current()) return;
          if (confirmedSessionFields(sessionRecordFromSnapshot(fresh)).modelId !== (helper ? helper.model(model) : model)) {
            throw new Error("Model change is not confirmed. Effort has not been sent.");
          }
          if (effortChanged && !effort) outcome = "Model confirmed. No explicit effort was sent; provider-reported settings are shown.";
        }
        if (effortChanged && effort) {
          const catalog = await this.api(path + "/execution-catalog");
          if (!current()) return;
          if (!helper.evidence(catalog.candidates || [], this.effortCatalogScope || {}, model || confirmed.modelId).includes(effort)) {
            throw new Error("The provider no longer advertises that effort for this model.");
          }
          await this.api(path + "/config", {method: "PUT", body: JSON.stringify({config_id: effortControl.dataset.acwConfigId, value: effort})});
          if (!current()) return;
        }
      }
      if (!current()) return;
      if (this.els.mode && this.els.mode.value && this.els.mode.value !== confirmed.modeId) {
        await this.api(path + "/mode", {method: "PUT", body: JSON.stringify({mode_id: this.els.mode.value})});
        if (!current()) return;
      }
      for (const input of others) {
        await this.api(path + "/config", {method: "PUT", body: JSON.stringify({config_id: input.dataset.acwConfigId,
          value: input.type === "checkbox" ? input.checked : input.value})});
        if (!current()) return;
      }
      fresh = await this.api(path);
    } catch (error) {
      outcome = "Could not apply settings: " + error.message;
      if (current() && this.els.settingsStatus) this.els.settingsStatus.classList.add("is-error");
    } finally {
      if (current()) {
        this.settingsDirty = !fresh;
        this.setSettingsPending(false);
        if (fresh) { this.applyOptionSnapshot(fresh); patchSessionListFromSnapshot(sid, fresh); }
        if (this.els.settingsStatus) this.els.settingsStatus.textContent = outcome;
      }
    }
  };

  AgentChatWidget.prototype.renderConfigOptions = function (snap) {
    if (!this.els.config) return;
    if (this.settingsDirty || this.settingsPending) { this.refreshEffortCatalog(snap); return; }
    const self = this;
    const raw = snap.config_options ||
      (snap.session && snap.session.config_json && snap.session.config_json.options) ||
      [];
    const options = Array.isArray(raw) ? raw : [];
    const configValues = (snap.session && snap.session.config_json && snap.session.config_json.values) || {};
    if (!options.length) {
      this.els.config.hidden = true;
      this.els.config.innerHTML = "";
      return;
    }
    const optionKinds = options.flatMap(function (opt) {
      if (!opt) return [];
      return [opt.id || opt.configId || opt.config_id, opt.name]
        .filter(Boolean)
        .map(function (value) { return String(value).toLowerCase().replace(/[_-]/g, ""); });
    });
    if (this.els.modelWrap && optionKinds.some(function (id) { return id === "model" || id === "modelid"; })) {
      this.els.modelWrap.hidden = true;
    }
    if (this.els.modeWrap && optionKinds.some(function (id) { return id === "mode" || id === "modeid"; })) {
      this.els.modeWrap.hidden = true;
    }
    this.els.config.hidden = false;
    this.els.config.innerHTML = "";
    options.forEach(function (opt) {
      if (!opt || typeof opt !== "object") return;
      const id = opt.id || opt.configId || opt.config_id;
      if (!id) return;
      const type = opt.type || opt.kind || "select";
      const wrap = document.createElement("label");
      wrap.className = "acw-select-wrap acw-config-item";
      const name = opt.name || id;
      if (type === "boolean") {
        const input = document.createElement("input");
        input.type = "checkbox";
        const current = Object.prototype.hasOwnProperty.call(configValues, id)
          ? configValues[id]
          : (opt.currentValue != null ? opt.currentValue : opt.current_value);
        input.checked = !!current;
        input.dataset.acwConfigId = id;
        input.dataset.acwOriginal = String(!!current);
        input.addEventListener("change", function () { self.markSettingsDirty(); });
        wrap.appendChild(input);
        wrap.appendChild(document.createTextNode(" " + name));
      } else {
        const select = document.createElement("select");
        select.setAttribute("aria-label", name);
        const choices = opt.options || opt.choices || opt.values || [];
        const current = Object.prototype.hasOwnProperty.call(configValues, id)
          ? configValues[id]
          : (opt.currentValue != null ? opt.currentValue : opt.current_value);
        choices.forEach(function (choice) {
          const value = typeof choice === "object" ? (choice.value || choice.id) : choice;
          const label = typeof choice === "object" ? (choice.name || choice.label || value) : choice;
          const option = document.createElement("option");
          option.value = value;
          option.textContent = label;
          if (String(value) === String(current)) option.selected = true;
          select.appendChild(option);
        });
        if (!choices.length && current != null) {
          const option = document.createElement("option");
          option.value = current;
          option.textContent = String(current);
          option.selected = true;
          select.appendChild(option);
        }
        select.dataset.acwConfigId = id;
        if (window.PANativeEffort && window.PANativeEffort.kind(opt) === "model" && current != null) select.value = String(current);
        select.dataset.acwOriginal = current == null ? "" : String(current);
        select.dataset.nativeKind = window.PANativeEffort ? window.PANativeEffort.kind(opt) : "other";
        if (select.dataset.nativeKind === "reasoning") select._effortWanted = current == null ? "" : String(current);
        select.addEventListener("change", function () {
          self.markSettingsDirty();
          if (select.dataset.nativeKind === "reasoning") select._effortWanted = select.value;
          if (select.dataset.nativeKind === "model") self.updateEffortChoices();
        });
        wrap.appendChild(document.createTextNode(name + " "));
        wrap.appendChild(select);
      }
      self.els.config.appendChild(wrap);
    });
    this.refreshEffortCatalog(snap);
  };

  AgentChatWidget.prototype.refreshEffortCatalog = function (snap) {
    if (!window.PANativeEffort || !snap.session || !this.sessionId) return;
    const key = JSON.stringify([this.apiBase, this.sessionId, this.subscriptionGeneration,
      snap.session.agent_name, snap.session.config_json && snap.session.config_json.execution_selection,
      snap.models, snap.config_options]);
    if (this.effortCatalogKey === key) { this.updateEffortChoices(); return; }
    this.effortCatalogKey = key;
    const selected = snap.session.config_json && snap.session.config_json.execution_selection && snap.session.config_json.execution_selection.selected || {};
    this.effortCatalogScope = {harness: snap.session.agent_name, connection: selected.connection || "default"};
    if (Object.prototype.hasOwnProperty.call(selected, "model_provider")) this.effortCatalogScope.model_provider = selected.model_provider;
    const instance = this.ownerInstanceId || this.currentInstanceId;
    if (instance) this.effortCatalogScope.instance_id = instance;
    this.effortCatalog = null;
    this.updateEffortChoices();
    const self = this, sid = this.sessionId, generation = this.subscriptionGeneration;
    this.api("/sessions/" + encodeURIComponent(sid) + "/execution-catalog").then(function (data) {
      if (self.destroyed || self.sessionId !== sid || self.subscriptionGeneration !== generation || self.effortCatalogKey !== key) return;
      self.effortCatalog = data.candidates || [];
      self.updateEffortChoices();
    }).catch(function () {
      if (self.sessionId !== sid || self.subscriptionGeneration !== generation || self.effortCatalogKey !== key) return;
      self.effortCatalog = []; self.updateEffortChoices();
    });
  };

  AgentChatWidget.prototype.updateEffortChoices = function () {
    const helper = window.PANativeEffort;
    if (!helper || !this.els.config) return;
    const select = this.els.config.querySelector('[data-native-kind="reasoning"]');
    if (!select) return;
    const model = this.els.config.querySelector('[data-native-kind="model"]') || this.els.model;
    const snap = this.lastSnapshot || {};
    const chosen = model && model.value || confirmedSessionFields(sessionRecordFromSnapshot(snap)).modelId;
    const values = helper.evidence(this.effortCatalog || [], this.effortCatalogScope || {}, chosen);
    const wanted = select._effortWanted == null ? select.value : select._effortWanted;
    const reset = helper.fill(select, values, wanted, this.effortCatalog === null ? "Checking model support…" :
      values.length ? "Automatic / provider default" : "Provider default — effort support unknown or unavailable");
    if (this.effortCatalog !== null) {
      select._effortWanted = select.value;
      if (reset && this.els.settingsStatus) this.els.settingsStatus.textContent =
        "Previous effort is unavailable for this model. Reset to Automatic / provider default; nothing has been applied.";
    }
    if (this.settingsPending) select.disabled = true;
  };

  AgentChatWidget.prototype.switchSession = function (sessionId, live, ownerInstanceId, options) {
    if (!sessionId || (sessionId === this.sessionId && (!ownerInstanceId || ownerInstanceId === this.ownerInstanceId))) return;
    if (this.settingsDirty && !window.confirm("Discard unsaved Agent settings changes and switch sessions?")) return;
    if (this.settingsDirty) this.resetSettingsDraft();
    this.setSettingsPending(false);
    this.settingsDirty = false;
    this.effortCatalogKey = null;
    this.effortCatalog = null;
    this._stashSessionDomCache();
    this.closeSSE("session-switched");
    if (this.olderAbortController) this.olderAbortController.abort();
    this.olderAbortController = null;
    this.lastSeq = 0;
    this.liveGapTarget = 0;
    this.livePendingEvents = {};
    if (this.liveGapRetryTimer) clearTimeout(this.liveGapRetryTimer);
    this.liveGapRetryTimer = null;
    this.liveGapLoading = false;
    this.transcriptEvents = [];
    this.seenEvents = {};
    this.hasOlder = false;
    this.olderCursor = null;
    this.loadingOlder = false;
    this.olderError = "";
    this.hasNewer = false;
    this.newerCursor = null;
    this.loadingNewer = false;
    this.newerError = "";
    this.updateOlderControl();
    this.updateNewerControl();
    this.streaming = {};
    this.lastSnapshot = null;
    this.renderTranscript([], { scrollBottom: false });
    this.setPlaceholder("Loading session…");
    this.openSession(sessionId, ownerInstanceId || "", options || {}).catch(function () {});
  };

  AgentChatWidget.prototype.setApiBase = function (apiBase, instanceId) {
    const next = String(apiBase || "/api/agent").replace(/\/$/, "");
    if (next === this.apiBase) return;
    this.subscriptionGeneration += 1;
    this.closeSSE("api-base-changed");
    if (this.olderAbortController) this.olderAbortController.abort();
    this.olderAbortController = null;
    this.stopBrowserRefresh();
    this.apiBase = next;
    this.root.dataset.apiBase = next;
    // Remote operations call setApiBase before selecting a session. Preserve
    // that base so clearSelectedSession does not bounce back to /api/agent.
    if (instanceId && this.currentInstanceId && instanceId !== this.currentInstanceId) {
      this.defaultApiBase = next;
    }
    if (this.drafts) this.drafts.setInstance(instanceId);
    this.sessionId = "";
    this.root.dataset.sessionId = "";
    this.lastSeq = 0;
    this.liveGapTarget = 0;
    this.livePendingEvents = {};
    if (this.liveGapRetryTimer) clearTimeout(this.liveGapRetryTimer);
    this.liveGapRetryTimer = null;
    this.liveGapLoading = false;
    this.transcriptEvents = [];
    this.seenEvents = {};
    this.hasOlder = false;
    this.olderCursor = null;
    this.loadingOlder = false;
    this.olderError = "";
    this.hasNewer = false;
    this.newerCursor = null;
    this.loadingNewer = false;
    this.newerError = "";
    this.updateOlderControl();
    this.updateNewerControl();
    this.streaming = {};
    this.lastSnapshot = null;
    this.renderTranscript([], { scrollBottom: false });
    this.setTurnActive(false);
    this.setStatus("offline");
    this.setPlaceholder("Select or start a remote session.");
  };

  AgentChatWidget.prototype.renderMetrics = function (metrics) {
    if (!this.showMetrics || !this.els.metrics) return;
    const usage = metrics.last_usage || metrics.usage || {};
    const parts = [];
    if (usage.total_tokens != null || usage.totalTokens != null) {
      parts.push("tokens " + (usage.total_tokens || usage.totalTokens));
    }
    if (usage.input_tokens != null || usage.inputTokens != null) {
      parts.push("in " + (usage.input_tokens || usage.inputTokens));
    }
    if (usage.output_tokens != null || usage.outputTokens != null) {
      parts.push("out " + (usage.output_tokens || usage.outputTokens));
    }
    if (metrics.turns != null) parts.push("turns " + metrics.turns);
    if (parts.length) {
      this.els.metrics.hidden = false;
      this.els.metrics.textContent = parts.join(" · ");
    }
  };

  AgentChatWidget.prototype.setWorking = function (on, startedAt) {
    const self = this;
    if (this.els.working) this.els.working.hidden = !on;
    if (this.els.stop) this.els.stop.disabled = !on;
    if (this.els.send) {
      this.els.send.textContent = on ? "Queue for current work" : "Send";
      this.els.send.title = on ? "Saved input runs after the active turn. This is not native steering." : "Send to this conversation";
    }
    if (!on) {
      if (this.turnTimerId) clearInterval(this.turnTimerId);
      this.turnTimerId = null;
      this.turnStartedAt = null;
      if (this.els.turnTimer) this.els.turnTimer.hidden = true;
      return;
    }
    if (startedAt) {
      this.turnStartedAt = new Date(startedAt).getTime();
    } else if (!this.turnStartedAt) {
      this.turnStartedAt = Date.now();
    }
    if (this.els.turnTimer) this.els.turnTimer.hidden = false;
    if (this.turnTimerId) clearInterval(this.turnTimerId);
    const tick = function () {
      if (self.els.turnTimer && self.turnStartedAt) {
        self.els.turnTimer.textContent = formatElapsed(Date.now() - self.turnStartedAt);
      }
      if (self.els.workingLabel && self.turnStartedAt) {
        self.els.workingLabel.textContent = "Working… " + formatElapsed(Date.now() - self.turnStartedAt);
      }
    };
    tick();
    this.turnTimerId = setInterval(tick, 500);
  };

  AgentChatWidget.prototype.setTurnActive = function (on, startedAt, restartTimer) {
    if (on && restartTimer) {
      if (this.turnTimerId) clearInterval(this.turnTimerId);
      this.turnTimerId = null;
      this.turnStartedAt = null;
    }
    this.turnActive = !!on;
    this.prompting = this.turnActive;
    this.setWorking(this.turnActive, startedAt);
    this.setStatus(this.turnActive ? "working" : "online");
    if (this.els.browserToggle) this.els.browserToggle.disabled = this.turnActive;
  };

  AgentChatWidget.prototype.setComposerEnabled = function (enabled, disabledMessage) {
    this.composerEnabled = !!enabled;
    const controls = [
      this.els.input,
      this.els.attach,
      this.els.fileInput,
    ];
    controls.forEach(function (control) {
      if (control) control.disabled = !enabled;
    });
    this._syncSubmissionControls();
    if (this.els.input) {
      this.els.input.placeholder = enabled
        ? "Message the agent, type / for commands, or drop images here…"
        : disabledMessage || "Preparing conversation…";
    }
  };

  AgentChatWidget.prototype._syncSubmissionControls = function () {
    const checking = this.submissionPending && (
      this.submissionState === "reconnecting" ||
      this.submissionState === "checking" ||
      this.submissionState === "acknowledgement_uncertain"
    );
    const sending = this.submissionPending && this.submissionState === "sending";
    const disabled = !this.composerEnabled || this.submissionPending || this.sessionClosed;
    if (this.els.send) {
      this.els.send.disabled = disabled;
      this.els.send.textContent = checking
        ? "Checking…"
        : sending
          ? "Sending…"
          : this.submissionState === "retryable"
            ? "Retry"
            : this.turnActive ? "Queue for current work" : "Send";
    }
    this.root.querySelectorAll("[data-acw-action]").forEach(function (control) {
      control.disabled = disabled;
    });
    if (this.els.form && typeof this.els.form.setAttribute === "function") {
      this.els.form.setAttribute("aria-busy", this.submissionPending ? "true" : "false");
    }
    if (this.root && this.root.dataset) {
      this.root.dataset.submissionState = this.submissionState;
    }
    let retry = this.root.querySelector("[data-acw-submission-retry]");
    if (!retry && this.root && this.submissionRetryVisible) {
      const status = this.root.querySelector("[data-acw-draft-status]");
      const parent = (status && status.parentNode) ||
        (this.els.form && typeof this.els.form.appendChild === "function" ? this.els.form : null);
      if (parent && typeof document !== "undefined" && typeof document.createElement === "function") {
        retry = document.createElement("button");
        retry.type = "button";
        retry.className = "button ghost acw-submission-retry";
        retry.setAttribute("data-acw-submission-retry", "1");
        retry.textContent = "Retry same prompt";
        if (status && status.parentNode) parent.insertBefore(retry, status.nextSibling);
        else parent.appendChild(retry);
        const self = this;
        retry.addEventListener("click", function () {
          self.send("append");
        });
      }
    }
    if (retry) {
      retry.hidden = !this.submissionRetryVisible;
      retry.disabled = this.submissionPending || this.sessionClosed || !this.composerEnabled;
      if (this.submissionRetryReason) retry.title = this.submissionRetryReason;
    }
  };

  AgentChatWidget.prototype.setSubmissionState = function (state, pending, options) {
    options = options || {};
    this.submissionState = state || "idle";
    this.submissionPending = !!pending;
    if (Object.prototype.hasOwnProperty.call(options, "retryVisible")) {
      this.submissionRetryVisible = !!options.retryVisible;
    } else if (state === "retryable" || state === "failed") {
      this.submissionRetryVisible = true;
    } else if (state === "idle" || state === "accepted" || state === "queued") {
      this.submissionRetryVisible = false;
      this.submissionRetryReason = "";
    }
    if (options.reason) this.submissionRetryReason = options.reason;
    this._syncSubmissionControls();
  };

  AgentChatWidget.prototype.markSubmissionAccepted = function (result, displayText, submittedImages, draftRawText, promptId) {
    result = result || {};
    const rawText = draftRawText == null ? displayText : draftRawText;
    if (!this._isDuplicateUserBubble(displayText)) {
      this.addBubble("user", displayText, new Date().toISOString(), {
        images: (submittedImages || []).map(function (image) {
          return {
            name: image.name,
            mime_type: image.mime_type,
            data: image.data,
            preview: image.preview || ("data:" + image.mime_type + ";base64," + image.data),
          };
        }),
      });
    }
    if (result.status !== "completed") this.setTurnActive(true);
    this.scrollToBottom();
    if (this.drafts && promptId) {
      this.drafts.observeAcceptance(promptId, !!result.queued);
    } else if (this.drafts) {
      this.drafts.submissionAccepted({
        rawText: rawText,
        images: submittedImages || [],
        message: "Prompt accepted.",
      });
    } else if (this.els.input && this.els.input.value === rawText) {
      this.els.input.value = "";
      this.clearPendingImages();
    }
    this.setSubmissionState(result.status || (result.queued ? "queued" : "accepted"), false, { retryVisible: false });
    this.refreshQueue();
  };

  // Transport generations fence event/history streams. Durable receipts instead
  // follow the same owner/session across reconnects and fence by receipt object.
  AgentChatWidget.prototype._submissionScope = function (promptId) {
    if (!this.drafts) this.fallbackSubmissionId = promptId;
    return { generation: this.subscriptionGeneration, sessionId: this.sessionId, apiBase: this.apiBase,
      owner: this.ownerInstanceId, drafts: this.drafts,
      receipt: this.drafts && this.drafts.pendingReceipt(), promptId: promptId };
  };

  AgentChatWidget.prototype._isCurrentSubmission = function (scope) {
    return !this.destroyed && !this.sessionClosed && this.sessionId === scope.sessionId &&
      this.apiBase === scope.apiBase && this.ownerInstanceId === scope.owner &&
      this.drafts === scope.drafts && ((!this.drafts &&
        scope.generation === this.subscriptionGeneration &&
        scope.promptId === this.fallbackSubmissionId) ||
        (this.drafts && this.drafts.pendingReceipt() === scope.receipt && scope.receipt &&
         scope.receipt.id === scope.promptId &&
         scope.receipt.sessionId === this.drafts.sessionId &&
         scope.receipt.instanceId === this.drafts.instanceId &&
         scope.receipt.principalId === this.drafts.principalId));
  };

  AgentChatWidget.prototype._matchesSubmissionResponse = function (scope, response) {
    return !response || ((!response.session_id || response.session_id === scope.sessionId) &&
      (!response.prompt_id || response.prompt_id === scope.promptId));
  };

  AgentChatWidget.prototype.reconcilePendingSubmission = function (options) {
    options = options || {};
    const self = this;
    const receipt = this.drafts && this.drafts.pendingReceipt();
    const promptId = receipt && receipt.id;
    if (!promptId || !this.sessionId) {
      if (options.unreachable && this.drafts && this.drafts.restoringSubmission) {
        this.setSubmissionState("retryable", false, {
          retryVisible: true,
          reason: options.reason || "Prompt acceptance status is unavailable.",
        });
        this.drafts.restoringSubmission = false;
        this.drafts.setStatus(
          (options.reason || "Status unavailable.") +
          " Retry reuses the same submission ID; duplicate execution is prevented."
        );
      }
      return Promise.resolve(null);
    }
    if (this.submissionReconcileId) clearTimeout(this.submissionReconcileId);
    this.setSubmissionState("checking", true);
    if (this.drafts) {
      this.drafts.setStatus("Checking durable acceptance for the previous prompt…");
    }
    const targetSessionId = this.sessionId;
    const scope = this._submissionScope(promptId);
    const started = performance.now();
    return this.apiWithTimeout(
      "/sessions/" + encodeURIComponent(targetSessionId) +
        "/prompts/" + encodeURIComponent(promptId),
      PROMPT_RECONCILE_TIMEOUT_MS
    ).then(function (status) {
      if (!self._isCurrentSubmission(scope)) return null;
      if (!self._matchesSubmissionResponse(scope, status)) {
        throw new Error("Prompt acceptance response identity did not match the requested submission.");
      }
      const elapsed = performance.now() - started;
      if (status && status.accepted) {
        const lifecycleState = status.status || (status.queued ? "queued" : "accepted");
        if (self.drafts) {
          if (!self.drafts.observeAcceptance(promptId, !!status.queued)) return null;
          self.setSubmissionState(lifecycleState, false, { retryVisible: false });
        } else {
          const rawText = self.els.input && self.els.input.value || "";
          self.markSubmissionAccepted(status, rawText, []);
          self.setSubmissionState(lifecycleState, false, { retryVisible: false });
        }
        self.addBubble(
          "system",
          (status.message || "Prompt accepted.") + " (reconciled in " + Math.round(elapsed) + "ms)",
          new Date().toISOString(),
          { system: true, forceVisible: true }
        );
        return status;
      }
      const reason = (status && status.message) ||
        "No durable acceptance yet. Retry reuses the same ID and remains exactly once.";
      self.setSubmissionState("retryable", false, { retryVisible: true, reason: reason });
      if (self.drafts) {
        self.drafts.restoringSubmission = false;
        self.drafts.setStatus(reason);
      }
      return status;
    }).catch(function (err) {
      if (!self._isCurrentSubmission(scope)) return null;
      const reason = options.reason ||
        ("Prompt acceptance lookup failed: " + (err && err.message || "unreachable") +
          ". Retry reuses the same submission ID; duplicate execution is prevented.");
      self.setSubmissionState("retryable", false, { retryVisible: true, reason: reason });
      if (self.drafts) {
        self.drafts.restoringSubmission = false;
        self.drafts.setStatus(reason);
      }
      return null;
    });
  };

  AgentChatWidget.prototype.markSessionEnded = function (message) {
    this.subscriptionGeneration += 1;
    const endedMessage = message || "Session ended.";
    this.sessionClosed = true;
    this.closePending = false;
    this.setTurnActive(false);
    this.setStatus("offline");
    this.setComposerEnabled(false, endedMessage);
    this.closeSSE("session-ended");
    this.renderSessionActions();
    this.addBubble("system", endedMessage, new Date().toISOString(), {
      system: true,
      forceVisible: true,
    });
    if (!this._hasTurnHistory()) this.setPlaceholder(endedMessage);
  };

  AgentChatWidget.prototype.scrollToBottom = function () {
    if (this.els.messages) this.els.messages.scrollTop = this.els.messages.scrollHeight;
  };

  AgentChatWidget.prototype.isNearBottom = function () {
    if (!this.els.messages) return true;
    const distance = this.els.messages.scrollHeight - this.els.messages.scrollTop - this.els.messages.clientHeight;
    return distance <= 48;
  };

  AgentChatWidget.prototype.commandMenuOpen = function () {
    return !!(this.els.commandMenu && !this.els.commandMenu.hidden);
  };

  AgentChatWidget.prototype.closeCommandMenu = function () {
    if (!this.els.commandMenu) return;
    this.els.commandMenu.hidden = true;
    if (this.els.input) {
      this.els.input.setAttribute("aria-expanded", "false");
      this.els.input.removeAttribute("aria-activedescendant");
    }
  };

  AgentChatWidget.prototype.refreshCommandCatalog = function (force) {
    if (!this.sessionId || this.commandLoading) return Promise.resolve(null);
    if (!force && this.commandCatalogSession === this.sessionId && this.commandCatalog) {
      return Promise.resolve(this.commandCatalog);
    }
    const self = this;
    const expectedSession = this.sessionId;
    this.commandLoading = true;
    this.commandError = "";
    this.updateCommandMenu();
    return this.api("/sessions/" + encodeURIComponent(expectedSession) + "/commands")
      .then(function (catalog) {
        if (self.sessionId !== expectedSession) return null;
        self.commandCatalog = catalog;
        self.commandCatalogSession = expectedSession;
        self.commandError = "";
        self.updateCommandMenu();
        return catalog;
      })
      .catch(function (err) {
        if (self.sessionId === expectedSession) {
          self.commandError = err.message || "Commands are temporarily unavailable.";
          self.updateCommandMenu();
        }
        return null;
      })
      .finally(function () {
        if (self.sessionId === expectedSession) {
          self.commandLoading = false;
          self.updateCommandMenu();
        }
      });
  };

  AgentChatWidget.prototype.updateCommandMenu = function () {
    if (!this.els.input || !this.els.commandMenu || !this.els.commandOptions) return;
    const raw = this.els.input.value || "";
    if (raw.charAt(0) !== "/" || raw.indexOf("//") === 0 || /\s/.test(raw.slice(1))) {
      this.closeCommandMenu();
      return;
    }
    const query = raw.slice(1).toLowerCase();
    const commands = this.commandCatalog && this.commandCatalog.commands || [];
    this.commandMatches = commands
      .filter(function (command) { return command.name.toLowerCase().indexOf(query) !== -1; })
      .sort(function (a, b) {
        const ap = a.name.toLowerCase().indexOf(query) === 0 ? 0 : 1;
        const bp = b.name.toLowerCase().indexOf(query) === 0 ? 0 : 1;
        return ap - bp || a.name.localeCompare(b.name) || a.origin.localeCompare(b.origin);
      });
    if (this.commandSelectedIndex >= this.commandMatches.length) this.commandSelectedIndex = 0;
    this.els.commandOptions.innerHTML = "";
    const self = this;
    this.commandMatches.forEach(function (command, index) {
      const option = document.createElement("button");
      option.type = "button";
      option.id = "acw-command-option-" + index + "-" + command.name.replace(/[^a-z0-9_-]/gi, "-");
      option.className = "acw-command-option" + (index === self.commandSelectedIndex ? " is-selected" : "");
      option.setAttribute("role", "option");
      option.setAttribute("aria-selected", index === self.commandSelectedIndex ? "true" : "false");
      const disabled = command.availability && command.availability !== "available";
      option.disabled = disabled;
      const title = document.createElement("span");
      title.className = "acw-command-title";
      title.textContent = "/" + command.name;
      const origin = document.createElement("span");
      origin.className = "acw-command-origin";
      origin.textContent = command.origin === "pa" ? "PA" : (command.provider || "provider");
      const description = document.createElement("span");
      description.className = "acw-command-description";
      description.textContent = command.description || "No description provided.";
      const input = document.createElement("span");
      input.className = "acw-command-input";
      input.textContent = disabled
        ? (command.disabled_reason || "Unavailable")
        : command.input_required
          ? ("Input required" + (command.input_hint ? ": " + command.input_hint : ""))
          : "Runs immediately";
      option.appendChild(title);
      option.appendChild(origin);
      option.appendChild(description);
      option.appendChild(input);
      option.addEventListener("pointerdown", function (event) { event.preventDefault(); });
      option.addEventListener("click", function () { self.selectCommand(command); });
      self.els.commandOptions.appendChild(option);
    });
    this.els.commandMenu.hidden = false;
    this.els.input.setAttribute("aria-expanded", "true");
    if (this.els.commandState) {
      this.els.commandState.hidden = false;
      this.els.commandState.textContent = this.commandLoading
        ? "Loading commands…"
        : this.commandError
          ? this.commandError
          : this.commandMatches.length
            ? this.commandMatches.length + " command" + (this.commandMatches.length === 1 ? "" : "s")
            : "No matching commands.";
    }
    this.moveCommandSelection(0);
  };

  AgentChatWidget.prototype.moveCommandSelection = function (delta) {
    if (!this.commandMatches.length) {
      if (this.els.input) this.els.input.removeAttribute("aria-activedescendant");
      return;
    }
    const count = this.commandMatches.length;
    this.commandSelectedIndex = (this.commandSelectedIndex + delta + count) % count;
    const options = this.els.commandOptions.querySelectorAll("[role=option]");
    options.forEach(function (option, index) {
      option.classList.toggle("is-selected", index === this.commandSelectedIndex);
      option.setAttribute("aria-selected", index === this.commandSelectedIndex ? "true" : "false");
    }, this);
    const active = options[this.commandSelectedIndex];
    if (active) {
      this.els.input.setAttribute("aria-activedescendant", active.id);
      active.scrollIntoView({ block: "nearest" });
    }
  };

  AgentChatWidget.prototype.selectCommand = function (command) {
    if (!command || (command.availability && command.availability !== "available")) return;
    if (command.input_required) {
      this.els.input.value = "/" + command.name + " ";
      this.closeCommandMenu();
      this.els.input.focus();
      this.els.input.dispatchEvent(new Event("input", { bubbles: true }));
      return;
    }
    this.closeCommandMenu();
    this.executeCommand(command, "", this.els.input.value || ("/" + command.name));
  };

  AgentChatWidget.prototype.commandInvocation = function (rawText) {
    if (!rawText || rawText.charAt(0) !== "/" || rawText.indexOf("//") === 0) return null;
    const match = rawText.match(/^\/([^\s]+)(?:\s+([\s\S]*))?$/);
    if (!match) return null;
    const name = match[1];
    const commands = this.commandCatalog && this.commandCatalog.commands || [];
    const command = commands.find(function (item) { return item.name === name; });
    return command ? { command: command, arguments: match[2] || "" } : null;
  };

  AgentChatWidget.prototype.executeCommand = function (command, argumentsText, rawText) {
    if (!command || this.submissionPending || !this.sessionId) return;
    const self = this;
    const key = window.PAAgentDrafts ? window.PAAgentDrafts.randomId() : ("command-" + Date.now());
    this.submissionPending = true;
    if (this.els.send) this.els.send.disabled = true;
    this.addBubble("system", "Running /" + command.name + "…", new Date().toISOString(), { system: true, forceVisible: true });
    this.api("/sessions/" + encodeURIComponent(this.sessionId) + "/commands/execute", {
      method: "POST",
      body: JSON.stringify({
        name: command.name,
        arguments: argumentsText || null,
        catalog_generation: this.commandCatalog && this.commandCatalog.generation,
        dispatch_id: this.commandCatalog && this.commandCatalog.dispatch_id,
        card_id: this.commandCatalog && this.commandCatalog.card_id,
        authority_instance_id: this.commandCatalog && this.commandCatalog.authority_instance_id,
        authority_version: this.commandCatalog && this.commandCatalog.authority_version,
        idempotency_key: key,
      }),
    }).then(function (result) {
      self.addBubble(
        "system",
        "/" + command.name + ": " + (result.reason || result.status),
        new Date().toISOString(),
        { system: true, forceVisible: true }
      );
      if (self.els.input && self.els.input.value === rawText) {
        self.els.input.value = "";
        self.els.input.dispatchEvent(new Event("input", { bubbles: true }));
      }
      if (self.drafts) self.drafts.submissionAccepted({ rawText: rawText, images: [] });
      if (result.status === "stale") self.refreshCommandCatalog(true);
    }).catch(function (err) {
      self.addBubble("system", "Command failed: " + err.message, new Date().toISOString(), { system: true, forceVisible: true });
      if (self.drafts) self.drafts.submissionFailed({ rawText: rawText, images: [], conflict: err.status === 409 });
    }).finally(function () {
      self.submissionPending = false;
      if (self.els.send) self.els.send.disabled = false;
      if (self.els.input) self.els.input.focus();
    });
  };

  AgentChatWidget.prototype.send = function (action) {
    const self = this;
    if (this.submissionPending) return;
    if (this.sessionClosed) {
      this.addBubble(
        "system",
        "This prompt was not sent because the session has ended. Start or select another session first.",
        new Date().toISOString(),
        { system: true, forceVisible: true }
      );
      return;
    }
    let rawText = this.els.input && this.els.input.value || "";
    const draftRawText = rawText;
    const invocation = this.commandInvocation(rawText.trim());
    if (invocation) {
      this.executeCommand(invocation.command, invocation.arguments, rawText);
      return;
    }
    const literalLeadingSlash = rawText.indexOf("//") === 0;
    if (literalLeadingSlash) rawText = rawText.slice(1);
    const text = rawText.trim();
    if (!text && !this.pendingImages.length) return;
    if (!this.sessionId) {
      this.setPlaceholder("Preparing your conversation. Your draft is saved; send when ready.");
      return;
    }
    if (this.pendingImages.some(function (image) { return !image.data; })) {
      this.addBubble("system", "Please wait for the images to finish loading.", new Date().toISOString(), { system: true, forceVisible: true });
      return;
    }
    const act = action || "append";
    const targetSessionId = this.sessionId;
    const generation = this.subscriptionGeneration;
    const submittedImages = this.pendingImages.slice();
    const displayImages = submittedImages.map(function (image) {
      return {
        name: image.name,
        mime_type: image.mime_type,
        data: image.data,
        preview: "data:" + image.mime_type + ";base64," + image.data,
      };
    });
    const promptId = this.drafts
      ? this.drafts.beginSubmission()
      : (window.PAAgentDrafts ? window.PAAgentDrafts.randomId() : String(Date.now()));
    const scope = this._submissionScope(promptId);
    this.setSubmissionState("sending", true, { retryVisible: false });
    if (this.drafts) this.drafts.setStatus("Sending — waiting for durable acknowledgement…");
    const body = {
      message: text,
      images: displayImages.map(function (image) {
        return { name: image.name, mime_type: image.mime_type, data: image.data };
      }),
      action: act,
      client_prompt_id: promptId,
    };
    const submittedAt = Date.now();
    this.pendingSubmitTiming = {promptId: promptId, sessionId: targetSessionId, generation: generation, startedAt: submittedAt};
    this.apiWithTimeout(
      "/sessions/" + targetSessionId + "/prompt",
      PROMPT_SUBMIT_TIMEOUT_MS,
      {
        method: "POST",
        headers: { "Idempotency-Key": promptId },
        body: JSON.stringify(body),
      }
    )
      .then(function (res) {
        if (res && res.accepted && self._matchesSubmissionResponse(scope, res)) self.recordSubmissionTiming(promptId);
        if (!self._isCurrentSubmission(scope)) return;
        if (!res || !res.accepted || !self._matchesSubmissionResponse(scope, res)) {
          const error = new Error("PA could not confirm durable prompt acceptance.");
          error.acceptanceUnconfirmed = true;
          throw error;
        }
        self.markSubmissionAccepted(res, text, submittedImages, draftRawText, promptId);
      })
      .catch(function (err) {
        if (!self._isCurrentSubmission(scope)) return;
        const code = apiErrorCode(err);
        const timedOut = !!(err && (err.timeout || err.code === "request_timeout"));
        const networkish = timedOut || err.acceptanceUnconfirmed ||
          !err.status || err.status >= 500;
        if (networkish) {
          self.setSubmissionState("acknowledgement_uncertain", true, {
            reason: "Acknowledgement is uncertain; checking durable acceptance…",
          });
          if (self.drafts) {
            self.drafts.setStatus(
              "Acknowledgement uncertain — checking durable acceptance without clearing the draft."
            );
          }
          return self.reconcilePendingSubmission({
            reason: timedOut
              ? "Prompt submission exceeded its deadline before acknowledgement."
              : (err.message || "Prompt acknowledgement was interrupted."),
          });
        }
        if (self.drafts) {
          self.drafts.submissionFailed({
            rawText: draftRawText,
            images: submittedImages,
            conflict: code === "client_prompt_id_conflict",
          });
        }
        if (code === "session_not_live" || code === "session_deleted") {
          if (self.drafts && code === "session_not_live") {
            self.drafts.setStatus("Reconnecting — retry will reuse the same submission ID.");
          }
          self.addBubble("system", "Prompt not sent: " + err.message, new Date().toISOString(), { system: true, forceVisible: true });
          self.setSubmissionState("retryable", false, {
            retryVisible: true,
            reason: err.message,
          });
          self.resolveSessionNotLive(err, targetSessionId, generation);
          return;
        }
        const message = err.csrfRecoveryFailed
          ? "Security token changed and automatic recovery failed. Your draft is preserved; retry once or reload PA."
          : "Prompt not sent: " + err.message;
        self.addBubble("system", message, new Date().toISOString(), { system: true, forceVisible: true });
        self.setSubmissionState("failed", false, {
          retryVisible: true,
          reason: message,
        });
      });
  };


  AgentChatWidget.prototype.cancel = function () {
    if (!this.sessionId) return;
    this.api("/sessions/" + this.sessionId + "/cancel", { method: "POST", body: "{}" });
  };

  AgentChatWidget.prototype.closeSession = function () {
    const self = this;
    if (!this.sessionId || this.sessionClosed || this.closePending) return;
    const targetSessionId = this.sessionId;
    const generation = this.subscriptionGeneration;
    this.closePending = true;
    this.renderSessionActions();
    this.api("/sessions/" + targetSessionId + "/archive", { method: "POST", body: "{}" }).then(function (result) {
      if (!self._isCurrentSessionRequest(targetSessionId, generation)) return;
      self.markSessionEnded("Conversation archived. Its history remains available under Archived.");
      const recovery = result && result.recovery || {};
      self.showRecoveryActions({
        recoverable: recovery.recoverable,
        history_url: "/api/agent/history/" + targetSessionId,
      });
      self.renderSessionActions({ terminal: true, recoverable: recovery.recoverable === true });
      refreshSessionList(null);
    }).catch(function (err) {
      if (!self._isCurrentSessionRequest(targetSessionId, generation)) return;
      self.closePending = false;
      self.renderSessionActions();
      self.addBubble("system", "Could not archive conversation: " + err.message, new Date().toISOString(), { system: true, forceVisible: true });
    });
  };

  AgentChatWidget.prototype.retrySession = function () {
    const self = this;
    if (!this.sessionId || !this.els.recoveryRetry) return;
    this.els.recoveryRetry.disabled = true;
    this.api("/sessions/" + this.sessionId + "/retry", {
      method: "POST",
      body: "{}",
    }).then(function (snap) {
      self.applySnapshot(snap);
      self.connectSSE();
      refreshSessionList(self.sessionId);
    }).catch(function (err) {
      const action = err.detail && err.detail.action;
      if (self.els.recoveryAction) {
        self.els.recoveryAction.textContent = action || err.message || recoveryGuidance({});
      }
      self.addBubble("system", "Recovery retry failed: " + err.message, new Date().toISOString(), { system: true, forceVisible: true });
    }).finally(function () {
      if (self.els.recoveryRetry) self.els.recoveryRetry.disabled = false;
    });
  };

  AgentChatWidget.prototype.restartSession = function () {
    if (!this.sessionId || !this.sessionClosed || !this.sessionRecoverable) return;
    return this.recoverSession(this.sessionId);
  };

  AgentChatWidget.prototype.queueControl = function (action) {
    if (!this.sessionId) return;
    this.api("/sessions/" + this.sessionId + "/queue/" + action, { method: "POST", body: "{}" }).then(
      this.refreshQueue.bind(this)
    );
  };

  AgentChatWidget.prototype.putOption = function (kind, body) {
    const self = this;
    if (!this.sessionId) return Promise.reject(new Error("No active session"));
    return this.api("/sessions/" + this.sessionId + "/" + kind, {
      method: "PUT",
      body: JSON.stringify(body),
    }).catch(function (err) {
      self.addBubble("system", "Could not update agent setting: " + err.message, new Date().toISOString(), { system: true, forceVisible: true });
      return self.api("/sessions/" + self.sessionId).then(function (snap) {
        self.applyOptionSnapshot(snap);
        throw err;
      });
    });
  };

  function csrfFetch(path, opts) {
    opts = opts || {};
    return fetch("/api/agent" + path, Object.assign({
      headers: csrfHeaders(),
      credentials: "same-origin",
    }, opts)).then(function (res) {
      if (!res.ok) {
        return res.json().catch(function () { return {}; }).then(function (body) {
          const error = new Error(apiErrorMessage(body, res.statusText || "Request failed"));
          error.status = res.status;
          error.detail = body.detail;
          if (window.PASessionRecovery) {
            error.retryAfterMs = window.PASessionRecovery.responseRetryAfterMs(
              res, error.detail
            );
          }
          throw error;
        });
      }
      return res.json();
    });
  }

  function paFetch(path, opts) {
    opts = opts || {};
    const headers = Object.assign({}, csrfHeaders(), opts.headers || {});
    return fetch(path, Object.assign({
      headers: headers,
      credentials: "same-origin",
    }, opts, { headers: headers })).then(function (res) {
      if (!res.ok) {
        return res.json().catch(function () { return {}; }).then(function (body) {
          const error = new Error(apiErrorMessage(body, res.statusText || "Request failed"));
          error.status = res.status;
          error.detail = body.detail;
          throw error;
        });
      }
      return res.json();
    });
  }

  function openSessionCardDialog(widget, opener) {
    const dialog = document.querySelector("[data-agent-card-dialog]");
    if (!dialog || !widget || !widget.sessionId) return;
    dialog._agentWidget = widget;
    dialog._agentOpener = opener || document.activeElement;
    const form = dialog.querySelector("[data-agent-card-form]");
    const error = dialog.querySelector("[data-agent-card-error]");
    if (form) form.reset();
    if (error) error.hidden = true;
    if (typeof dialog.showModal === "function") dialog.showModal();
    else dialog.setAttribute("open", "");
    window.requestAnimationFrame(function () {
      const initial = dialog.querySelector("[data-agent-card-existing]");
      if (initial) initial.focus();
    });
  }

  let sessionListRecovery = null;
  let sessionListFreshness = null;
  const SESSION_LIST_REFRESH_EVENTS = [
    "session_info_update", "session_started", "session_closed", "session_recovered",
    "user_message", "turn_waiting", "turn_completed", "cancelled",
    "queue_enqueued", "queue_dequeued", "queue_removed", "queue_reordered",
    "queue_paused", "queue_resumed", "publication_fence_established", "session_control_changed", "prompt_failed", "connection_lost",
    "model_changed", "mode_changed", "current_mode_update", "config_changed",
    "configuration_changed",
    "config_option_update", "config_options_update", "permission_request",
    "permission_resolved", "elicitation_request", "elicitation_resolved", "error"
  ];
  const SESSION_MULTIPLEX_EVENTS = [
    "user_message", "agent_message", "agent_message_chunk", "agent_thought_chunk",
    "tool_call", "tool_call_update", "session_info_update", "prompt_timing", "plan", "usage_update", "card_disposition",
    "stream_recovery", "available_commands_update", "command_result", "browser_attachment_changed",
    "turn_waiting", "turn_completed", "prompt_failed", "cancelled",
    "queue_enqueued", "queue_dequeued", "queue_removed", "queue_reordered",
    "queue_paused", "queue_resumed", "publication_fence_established", "session_control_changed", "session_started", "session_closed",
    "session_recovered", "connection_lost", "model_changed", "mode_changed",
    "current_mode_update", "config_changed", "configuration_changed", "config_option_update",
    "config_options_update", "permission_request", "permission_resolved",
    "elicitation_request", "elicitation_resolved", "error", "message"
  ];
  const SESSION_LIST_REFRESH_EVENT_SET = SESSION_LIST_REFRESH_EVENTS.reduce(function (result, type) {
    result[type] = true;
    return result;
  }, {});

  function selectedSessionId() {
    const widget = document.querySelector("[data-agent-chat]");
    return widget && widget._acw ? widget._acw.sessionId : "";
  }

  function localSessionWidget() {
    const root = document.querySelector("[data-agent-chat]");
    const widget = root && root._acw;
    if (!widget || widget.destroyed) return null;
    return widget.apiBase === "/api/agent" ? widget : null;
  }

  function useSessionListTransportForChat(enabled) {
    const widget = localSessionWidget();
    if (!widget) return;
    widget.useExternalEventTransport(!!enabled);
    if (!enabled && !document.hidden && widget.sessionId) widget.connectSSE();
  }

  function stopSessionListFreshness(reason) {
    if (!sessionListFreshness) return;
    const state = sessionListFreshness;
    sessionListFreshness = null;
    state.generation += 1;
    if (state.refreshTimer) clearTimeout(state.refreshTimer);
    if (state.pollTimer) clearTimeout(state.pollTimer);
    if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
    if (state.watchdogTimer) clearTimeout(state.watchdogTimer);
    if (state.source) state.source.close();
    if (reason && window.console && console.debug) {
      console.debug("Agent session list freshness stopped", { reason: reason });
    }
  }

  function scheduleSessionListPoll(state) {
    if (!state || state !== sessionListFreshness) return;
    if (state.pollTimer) clearTimeout(state.pollTimer);
    state.pollTimer = setTimeout(function () {
      state.pollTimer = null;
      if (state !== sessionListFreshness || !state.list.isConnected) return;
      if (!document.hidden) refreshSessionList(selectedSessionId(), false);
      scheduleSessionListPoll(state);
    }, 15000);
  }

  function scheduleSessionListEventRefresh(state) {
    if (!state || state !== sessionListFreshness || state.refreshTimer) return;
    state.refreshTimer = setTimeout(function () {
      state.refreshTimer = null;
      if (state !== sessionListFreshness || !state.list.isConnected) return;
      refreshSessionList(selectedSessionId(), true);
    }, 50);
  }

  function connectSessionListFreshness(state) {
    if (!state || state !== sessionListFreshness || state.source || document.hidden) return;
    if (typeof EventSource !== "function") {
      scheduleSessionListPoll(state);
      return;
    }
    const generation = state.generation;
    const cursors = Object.assign({}, state.cursors);
    const selected = localSessionWidget();
    if (selected && selected.sessionId) cursors[selected.sessionId] = selected.lastSeq || 0;
    const source = new EventSource("/api/agent/session-events?client_id=agent-sidebar&after=" +
      encodeURIComponent(JSON.stringify(cursors)));

    state.source = source;
    function observedTraffic() {
      if (state !== sessionListFreshness || generation !== state.generation || state.source !== source) return;
      if (state.watchdogTimer) clearTimeout(state.watchdogTimer);
      state.watchdogTimer = setTimeout(function () {
        if (state === sessionListFreshness && state.source === source && source.onerror) source.onerror();
      }, 45000);
    }
    observedTraffic();
    source.addEventListener("heartbeat", observedTraffic);
    useSessionListTransportForChat(true);
    source.addEventListener("ready", function () {
      if (state !== sessionListFreshness || generation !== state.generation || state.source !== source) return;
      observedTraffic();
      state.reconnects = 0;
      scheduleSessionListEventRefresh(state);
    });
    SESSION_MULTIPLEX_EVENTS.forEach(function (type) {
      source.addEventListener(type, function (event) {
        if (state !== sessionListFreshness || generation !== state.generation || state.source !== source) return;
        observedTraffic();
        let data = null;
        try { data = JSON.parse(event.data || "{}"); } catch (_error) { data = null; }
        if (data && data.session_id && data.seq) state.cursors[data.session_id] = data.seq;
        const widget = localSessionWidget();
        if (data && widget && data.session_id === widget.sessionId) {
          if (type === "stream_recovery") {
            widget.liveGapTarget = Math.max(widget.liveGapTarget || 0,
              Number((data.payload || {}).target_seq) || 0);
            widget._repairLiveGap();
          }
          else widget.handleEvent(data, false);
        }
        if (SESSION_LIST_REFRESH_EVENT_SET[type]) scheduleSessionListEventRefresh(state);
      });
    });
    source.onerror = function () {
      if (state !== sessionListFreshness || generation !== state.generation || state.source !== source) return;
      if (state.source === source) state.source = null;
      if (state.watchdogTimer) clearTimeout(state.watchdogTimer);
      state.watchdogTimer = null;
      source.close();
      useSessionListTransportForChat(false);
      scheduleSessionListEventRefresh(state);
      const delay = Math.min(
        30000,
        1000 * Math.pow(2, Math.min(5, state.reconnects++))
      );
      state.reconnectTimer = setTimeout(function () {
        state.reconnectTimer = null;
        connectSessionListFreshness(state);
      }, delay);
    };
    scheduleSessionListPoll(state);
  }

  function startSessionListFreshness() {
    const list = document.querySelector("[data-agent-session-list]");
    if (!list || list.dataset.agentEnabled === "false") return;
    if (sessionListFreshness && sessionListFreshness.list === list) return;
    stopSessionListFreshness("list-replaced");
    sessionListFreshness = {
      list: list,
      source: null,
      refreshTimer: null,
      pollTimer: null,
      reconnectTimer: null,
      reconnects: 0,
      cursors: {},
      generation: 1,
    };
    connectSessionListFreshness(sessionListFreshness);
  }

  function renderSessionListState(list, message, blocked) {
    if (!list || !list.isConnected) return;
    list.innerHTML = "";
    const item = document.createElement("li");
    item.className = blocked ? "status status-blocked" : "muted";
    item.dataset.agentSessionState = blocked ? "error" : "restoring";
    item.setAttribute("role", blocked ? "alert" : "status");
    item.textContent = message;
    list.appendChild(item);
    list.setAttribute("aria-busy", blocked ? "false" : "true");
  }

  function renderSessionList(list, sessions, includeClosed, activeId) {
    if (!list || !list.isConnected) return;
    const previousScrollTop = list.scrollTop;
    const openDetails = {};
    list.querySelectorAll("[data-session-id]").forEach(function (item) {
      const details = item.querySelector(".agent-session-details");
      if (details && details.open) openDetails[item.dataset.sessionId] = true;
    });
    list.innerHTML = "";
    list.setAttribute("aria-busy", "false");
    if (!sessions || !sessions.length) {
      const empty = document.createElement("li");
      empty.className = "muted";
      empty.dataset.agentSessionEmpty = "1";
      empty.textContent = "No matching sessions.";
      list.appendChild(empty);
      return;
    }
    sessions.forEach(function (s) {
      const li = document.createElement("li");
      li.dataset.sessionId = s.id;
      li.dataset.sessionInstance = s.origin_instance_id || s.instance_id || "";
      li.dataset.sessionLive = s.live === false
        ? "false"
        : "true";
      li.dataset.sessionRecoverable = s.recovery && s.recovery.recoverable ? "true" : "false";
      if (activeId && s.id === activeId) li.classList.add("active");
      const sessionInstanceId = s.origin_instance_id || s.instance_id || "";
      const sessionIdentity = sessionInstanceId && window.PAInstanceIdentity
        ? window.PAInstanceIdentity.html(sessionInstanceId)
        : "";
      const title = s.title || (s.purpose === "chat" ? "New chat" : s.label) || "Agent";
      const state = sessionListState(s);
      const presentation = s.presentation || {};
      const stateLabel = state.label + (s.prompting && Number(s.queue_length || 0) > 0
        ? " · " + Number(s.queue_length) + " queued"
        : "");
      const cards = s.cards || [];
      const primaryCard = cards.find(function (card) { return card.primary; }) || cards[0];
      const project = s.project || null;
      const execution = sessionListExecution(s);
      const metrics = s.metrics_json || {};
      const usage = metrics.last_usage || metrics.usage || {};
      const totalTokens = usage.total_tokens != null
        ? usage.total_tokens
        : usage.totalTokens;
      const contextParts = [];
      if (primaryCard) {
        contextParts.push('<a href="/work?card=' + encodeURIComponent(primaryCard.id) +
          '" title="Current card">Card · ' + escapeHtml(primaryCard.title) + "</a>");
      } else if (project) {
        contextParts.push('<a href="/projects?project=' + encodeURIComponent(project.id) +
          '" title="Project">Project · ' + escapeHtml(project.title) + "</a>");
      } else {
        contextParts.push('<span class="muted">Standalone session</span>');
      }
      if (execution.repositoryName) {
        contextParts.push('<span title="' + escapeHtml(execution.repositoryUrl) + '">' +
          escapeHtml(execution.repositoryName + (execution.branch ? " · " + execution.branch : "")) +
          "</span>");
      }
      const usageParts = [];
      if (metrics.turns) usageParts.push(metrics.turns + " turn" + (metrics.turns === 1 ? "" : "s"));
      if (totalTokens) usageParts.push(compactSessionNumber(totalTokens) + " tokens");
      const related = cards.filter(function (card) {
        return !primaryCard || card.id !== primaryCard.id;
      }).map(function (card) {
        return '<a class="agent-session-chip" href="/work?card=' +
          encodeURIComponent(card.id) + '">Card · ' + escapeHtml(card.title) + "</a>";
      }).concat((s.pr_watches || []).map(function (watch) {
        return '<a class="agent-session-chip" href="/pull-requests?watch=' +
          encodeURIComponent(watch.id) + '" title="' +
          escapeHtml(watch.repository + "#" + watch.pr_number + " PR supervision") +
          '">PR #' + escapeHtml(watch.pr_number) + " · " + escapeHtml(watch.status) + "</a>";
      })).join("");
      li.innerHTML =
        (presentation.purpose === "automated_run" || presentation.purpose === "one_shot_job"
          ? '<div class="agent-activity-group">' +
              escapeHtml(s.activity_group || "Other activity") + "</div>"
          : "") +
        '<div class="agent-session-shell">' +
          '<span class="agent-session-provider-mark" aria-hidden="true">' +
            escapeHtml(String(s.agent_name || "PA").slice(0, 2).toUpperCase()) + "</span>" +
          '<div class="agent-session-summary">' +
            '<div class="agent-session-row-head">' +
              '<button type="button" class="agent-session-select" data-agent-session-open ' +
                'aria-label="Open session ' + escapeHtml(title) + '"' +
                (activeId && s.id === activeId ? ' aria-current="true"' : '') + '>' +
                '<strong class="agent-session-title" data-agent-session-title data-full-title="' +
                  escapeHtml(title) + '">' + escapeHtml(title) + "</strong></button>" +
              '<span class="agent-session-state agent-session-state-' + escapeHtml(state.key) + '">' +
                '<span aria-hidden="true">●</span> ' + escapeHtml(stateLabel) + "</span>" +
            "</div>" +
            '<span class="agent-session-title-tooltip" role="tooltip">' +
              escapeHtml(title) + "</span>" +
            '<div class="agent-session-context-line">' + contextParts.join("") + "</div>" +
            '<div class="agent-session-context-line"><span>' +
              escapeHtml(presentation.explanation || "Session details are limited on this peer.") +
              "</span></div>" +
            '<div class="agent-session-facts">' +
              '<span class="agent-session-instance" title="Execution instance">' +
                (sessionIdentity || escapeHtml(s.origin_instance_name || "Local instance")) + "</span>" +
              '<span class="agent-session-age" title="Elapsed time">' +
                escapeHtml(sessionElapsed(s.created_at)) + "</span>" +
              '<span class="agent-session-runtime" title="Live provider, model, and settings">' +
                escapeHtml(sessionRuntimeLabel(s)) +
                "</span>" +
              '<span class="agent-session-usage' + (usageParts.length ? "" : " muted") +
                '" title="Session usage">' +
                escapeHtml(usageParts.length ? usageParts.join(" · ") : "No usage yet") + "</span>" +
            "</div>" +
          "</div>" +
        "</div>" +
        '<div class="agent-session-related">' + related + "</div>" +
        '<details class="agent-session-details"><summary>Details</summary>' +
          '<div class="agent-session-details-body">' + sessionConfigSummary(s) +
            '<span class="muted small">Last activity <time datetime="' +
              escapeHtml(s.updated_at || "") + '">' + escapeHtml(sessionTimestamp(s.updated_at)) +
              "</time></span>" +
            '<span class="muted small"><b>Provider attempts</b> ' +
              escapeHtml(s.provider_attempts == null ? "See session diagnostics" : s.provider_attempts) +
              (s.closure_reason ? " · <b>Closure reason</b> " + escapeHtml(s.closure_reason) : "") +
              "</span>" +
            '<a class="text-btn small" href="/knowledge?session=' + encodeURIComponent(s.id) +
              '">Promote conclusion to memory</a>' +
            '<span class="agent-session-actions"></span>' +
          "</div></details>";
      const actions = li.querySelector(".agent-session-actions");
      const permitted = presentation.permitted_actions || [];
      if (presentation.purpose === "chat" &&
          (permitted.indexOf("pin") !== -1 || permitted.indexOf("unpin") !== -1)) {
        const pin = document.createElement("button");
        pin.type = "button";
        pin.className = "ghost small";
        pin.dataset.agentSessionPin = "1";
        pin.dataset.pinned = s.pinned_at ? "true" : "false";
        pin.textContent = s.pinned_at ? "Unpin" : "Pin";
        actions.appendChild(pin);
      }
      if (permitted.indexOf("archive") !== -1 || permitted.indexOf("unarchive") !== -1) {
        const archive = document.createElement("button");
        archive.type = "button";
        archive.className = "ghost small danger agent-session-close";
        archive.dataset.agentSessionArchive = "1";
        const isArchived = !!(presentation.archive && presentation.archive.archived);
        archive.dataset.archived = isArchived ? "true" : "false";
        archive.textContent = isArchived ? "Unarchive" : "Archive";
        archive.title = isArchived
          ? "Return this conversation to Chats"
          : "Archive this conversation without deleting its history";
        actions.appendChild(archive);
      }
      if (permitted.indexOf("take_over") !== -1 || permitted.indexOf("return_to_automation") !== -1) {
        const control = document.createElement("button");
        control.type = "button";
        control.className = "ghost small";
        control.dataset.agentSessionControl = "1";
        control.dataset.mode = permitted.indexOf("take_over") !== -1 ? "human" : "automation";
        control.textContent = control.dataset.mode === "human" ? "Take over" : "Return to automation";
        actions.appendChild(control);
      }
      if (permitted.indexOf("continue_in_new_chat") !== -1) {
        const continuation = document.createElement("button");
        continuation.type = "button";
        continuation.className = "ghost small";
        continuation.dataset.agentSessionContinue = "1";
        continuation.textContent = "Continue in new chat";
        continuation.title = "Start a linked conversation with an explicit context boundary";
        actions.appendChild(continuation);
      }
      list.appendChild(li);
      const details = li.querySelector(".agent-session-details");
      if (details && openDetails[s.id]) details.open = true;
    });
    list.scrollTop = previousScrollTop;
    updateSessionTitleTooltips(list);
    filterSessionList();
  }

  function cancelSessionListRecovery(scope, reason) {
    if (sessionListFreshness) {
      const freshnessList = sessionListFreshness.list;
      const containsFreshnessList = scope === document || scope === freshnessList ||
        (scope && typeof scope.contains === "function" && scope.contains(freshnessList));
      if (containsFreshnessList) stopSessionListFreshness(reason || "session-list-removed");
    }
    if (!sessionListRecovery) return;
    const list = sessionListRecovery.list;
    const containsList = scope === document || scope === list ||
      (scope && typeof scope.contains === "function" && scope.contains(list));
    if (!containsList) return;
    sessionListRecovery.controller.cancel(reason || "session-list-removed");
    sessionListRecovery = null;
  }

  function refreshSessionList(activeId, force) {
    const list = document.querySelector("[data-agent-session-list]");
    if (!list) return Promise.resolve(null);
    const panel = list.closest("[data-agent-session-view]");
    const view = panel && panel.dataset.agentSessionView || "chats";
    const archived = view === "archived";
    const effectiveView = archived ? "chats" : view;
    const path = "/sessions?view=" + encodeURIComponent(effectiveView) +
      (archived ? "&archived=true" : "") +
      (effectiveView === "activity" && panel && panel.dataset.agentActivityFilter
        ? "&activity_filter=" + encodeURIComponent(panel.dataset.agentActivityFilter)
        : "") +
      (activeId ? "&selected_session_id=" + encodeURIComponent(activeId) : "");
    const includeClosed = effectiveView === "all" || archived;
    if (sessionListRecovery &&
        (sessionListRecovery.list !== list || sessionListRecovery.path !== path)) {
      sessionListRecovery.controller.cancel("session-list-context-changed");
      sessionListRecovery = null;
    }
    if (!sessionListRecovery) {
      const state = {
        list: list,
        path: path,
        includeClosed: includeClosed,
        activeId: activeId || "",
        controller: null,
      };
      state.controller = new window.PASessionRecovery.Controller({
        minimumMs: 250,
        maximumMs: 30000,
        operation: function (signal) {
          return csrfFetch(state.path, { signal: signal });
        },
        isActive: function () {
          return state.list.isConnected;
        },
        onSuccess: function (sessions) {
          renderSessionList(
            state.list, sessions, state.includeClosed, state.activeId
          );
        },
        onRecovery: function (error) {
          const detail = error.detail || {};
          renderSessionListState(
            state.list,
            detail.message || "Restoring sessions…",
            false
          );
        },
        onError: function (error) {
          renderSessionListState(
            state.list,
            error.message || "Could not load agent sessions.",
            true
          );
        },
      });
      sessionListRecovery = state;
    } else {
      sessionListRecovery.activeId = activeId || "";
    }
    return sessionListRecovery.controller.start(!!force);
  }

  function compactSessionNumber(value) {
    const number = Number(value || 0);
    if (number >= 1000000) return (number / 1000000).toFixed(1).replace(/\.0$/, "") + "m";
    if (number >= 1000) return (number / 1000).toFixed(1).replace(/\.0$/, "") + "k";
    return String(number);
  }

  function sessionElapsed(createdAt) {
    const created = Date.parse(createdAt || "");
    if (!Number.isFinite(created)) return "—";
    const seconds = Math.max(0, Math.floor((Date.now() - created) / 1000));
    if (seconds >= 86400) return Math.floor(seconds / 86400) + "d";
    if (seconds >= 3600) return (seconds / 3600).toFixed(1).replace(/\.0$/, "") + "h";
    if (seconds >= 60) return Math.floor(seconds / 60) + "m";
    return seconds + "s";
  }

  function sessionTimestamp(value) {
    const timestamp = Date.parse(value || "");
    if (!Number.isFinite(timestamp)) return "unknown";
    return new Date(timestamp).toLocaleString();
  }

  function sessionListExecution(session) {
    const config = session.config_json || {};
    const execution = config.execution_context || {};
    const repository = (execution.repositories || [])[0] || {};
    const repositoryUrl = String(repository.repository_url || "");
    let repositoryName = repositoryUrl.replace(/\/$/, "").split("/").pop() || "";
    repositoryName = repositoryName.replace(/\.git$/, "");
    return {
      repositoryName: repositoryName,
      repositoryUrl: repositoryUrl,
      branch: repository.branch || "",
    };
  }

  function sessionListState(session) {
    const presentation = session.presentation || {};
    if (presentation.display_status) {
      return {
        key: String(presentation.status_code || presentation.display_status)
          .toLowerCase().replace(/[^a-z0-9_-]/g, "-"),
        label: presentation.display_status,
      };
    }
    if ((session.metrics_json || {}).pending_approval ||
        (session.config_json || {}).pending_approval) {
      return { key: "waiting", label: "Approval" };
    }
    if (session.prompting) return { key: "working", label: "Working" };
    if (session.queue_length) return { key: "queued", label: "Queued" };
    if (session.live && session.connected) return { key: "idle", label: "Idle" };
    const raw = String(session.status || "unknown");
    return {
      key: raw.toLowerCase().replace(/[^a-z0-9_-]/g, "-"),
      label: raw.replace(/_/g, " ").replace(/\b\w/g, function (letter) {
        return letter.toUpperCase();
      }),
    };
  }

  function updateSessionTitleTooltips(scope) {
    (scope || document).querySelectorAll("[data-agent-session-title]").forEach(function (title) {
      const item = title.closest("[data-session-id]");
      if (!item) return;
      const clamped = title.scrollHeight > title.clientHeight + 1;
      item.dataset.titleClamped = clamped ? "true" : "false";
      const open = item.querySelector("[data-agent-session-open]");
      if (open) open.setAttribute(
        "aria-label",
        "Open session " + (title.dataset.fullTitle || title.textContent || "Agent session")
      );
    });
  }

  function filterSessionList() {
    const list = document.querySelector("[data-agent-session-list]");
    const search = document.querySelector("[data-agent-session-search]");
    if (!list) return;
    const query = String(search && search.value || "").trim().toLowerCase();
    let visible = 0;
    list.querySelectorAll("[data-session-id]").forEach(function (item) {
      const match = !query || (item.textContent || "").toLowerCase().indexOf(query) !== -1;
      item.hidden = !match;
      if (match) visible += 1;
    });
    let empty = list.querySelector("[data-agent-session-filter-empty]");
    if (!visible && query) {
      if (!empty) {
        empty = document.createElement("li");
        empty.className = "muted";
        empty.dataset.agentSessionFilterEmpty = "1";
        list.appendChild(empty);
      }
      empty.textContent = "No sessions match “" + query + "”.";
      empty.hidden = false;
    } else if (empty) {
      empty.hidden = true;
    }
  }

  function formatSessionModelId(modelId) {
    const raw = String(modelId || "").trim();
    if (!raw) return "";
    if (raw.charAt(raw.length - 1) === "]") {
      const opened = raw.lastIndexOf("[");
      if (opened > 0) {
        const name = raw.slice(0, opened).trim();
        const inner = raw.slice(opened + 1, -1).trim();
        if (!inner) return name || "default";
      }
    }
    return raw;
  }

  function semanticSessionValue(values, aliases) {
    if (!values || typeof values !== "object") return null;
    const matches = Object.keys(values).filter(function (key) {
      const compact = String(key).toLowerCase().replace(/[^a-z0-9]+/g, "");
      return aliases.indexOf(compact) !== -1 && values[key] != null && values[key] !== "";
    }).map(function (key) { return values[key]; });
    if (!matches.length) return null;
    return matches.every(function (value) { return String(value) === String(matches[0]); })
      ? matches[0]
      : null;
  }

  function confirmedSessionFields(sessionOrConfig) {
    const record = sessionRecord(sessionOrConfig);
    const config = record.config_json || {};
    const values = Object.assign({}, config.values || {});
    (config.options || []).forEach(function (option) {
      if (!option || typeof option !== "object") return;
      const id = option.id || option.configId || option.config_id;
      const current = option.currentValue != null ? option.currentValue : option.current_value;
      if (id && current != null) values[id] = current;
    });
    const admission = config.configuration || {};
    const effective = admission.effective || {};
    const effectiveConfig = effective.config || {};
    const modelAliases = ["model", "models", "modelid", "languagemodel"];
    const modeAliases = ["mode", "sessionmode", "agentmode", "interactionmode"];
    const reasoningAliases = [
      "effort", "reasoning", "reasoningeffort", "reasoninglevel",
      "thought", "thoughteffort", "thoughtlevel", "thinking",
      "thinkingeffort", "thinkinglevel"
    ];
    let modelId = semanticSessionValue(values, modelAliases);
    if (modelId == null) modelId = semanticSessionValue(effectiveConfig, modelAliases);
    if (modelId == null) modelId = effective.model_id;
    if (modelId == null) modelId = record.model_id;
    let selectorReasoning = null;
    const rawModel = String(modelId || "").trim();
    if (rawModel.endsWith("]") && rawModel.lastIndexOf("[") > 0) {
      const opened = rawModel.lastIndexOf("[");
      const candidate = rawModel.slice(opened + 1, -1).trim();
      if (candidate) {
        modelId = rawModel.slice(0, opened).trim();
        selectorReasoning = candidate;
      }
    }
    let modeId = semanticSessionValue(values, modeAliases);
    if (modeId == null) modeId = semanticSessionValue(effectiveConfig, modeAliases);
    if (modeId == null) modeId = effective.mode_id;
    if (modeId == null) modeId = record.mode_id;
    let reasoning = semanticSessionValue(values, reasoningAliases);
    if (reasoning == null) reasoning = semanticSessionValue(effectiveConfig, reasoningAliases);
    if (reasoning == null) reasoning = effective.reasoning;
    if (reasoning == null) reasoning = selectorReasoning;
    return { modelId: modelId || null, modeId: modeId || null, reasoning: reasoning || null, values: values };
  }

  function sessionConfigOptionLabel(key) {
    const compact = String(key || "").toLowerCase().replace(/[_-]+/g, "");
    if ([
      "effort", "reasoning", "reasoningeffort", "reasoninglevel",
      "thought", "thoughteffort", "thoughtlevel"
    ].indexOf(compact) !== -1) {
      return "effort";
    }
    if (["thinking", "thinkingeffort", "thinkinglevel"].indexOf(compact) !== -1) {
      return "thinking";
    }
    return key;
  }

  function sessionConfigOptionParts(values) {
    if (!values || typeof values !== "object") return [];
    const seen = {};
    return Object.keys(values).reduce(function (parts, key) {
      const compact = String(key).toLowerCase().replace(/[_-]+/g, "");
      if ([
        "model", "models", "modelid", "languagemodel",
        "mode", "sessionmode", "agentmode", "interactionmode",
        "collaborationmode"
      ].indexOf(compact) !== -1) {
        return parts;
      }
      const value = values[key];
      if (value == null || value === "" || typeof value === "object") return parts;
      const label = sessionConfigOptionLabel(key);
      if (seen[label]) return parts;
      seen[label] = true;
      parts.push(label + " " + value);
      return parts;
    }, []);
  }

  function sessionRecord(sessionOrConfig) {
    if (sessionOrConfig && sessionOrConfig.config_json) return sessionOrConfig;
    return { config_json: sessionOrConfig || {} };
  }

  function sessionRuntimeLabel(session) {
    const record = sessionRecord(session);
    const confirmed = confirmedSessionFields(record);
    const model = formatSessionModelId(confirmed.modelId) || "default model";
    const parts = [record.agent_name || "Default provider", model];
    if (confirmed.modeId) parts.push(confirmed.modeId);
    return parts.concat(sessionConfigOptionParts(confirmed.values)).join(" · ");
  }

  function sessionConfigSummary(sessionOrConfig) {
    const record = sessionRecord(sessionOrConfig);
    const config = record.config_json || {};
    const confirmed = confirmedSessionFields(record);
    const values = confirmed.values;
    const liveParts = [];
    const model = formatSessionModelId(confirmed.modelId);
    if (model) liveParts.push("model " + model);
    if (confirmed.modeId) liveParts.push("mode " + confirmed.modeId);
    liveParts.push.apply(liveParts, sessionConfigOptionParts(values));
    const admission = config.configuration;
    const requested = (admission && admission.requested) || {};
    const effective = (admission && admission.effective) || {};
    const requestedParts = [
      requested.model_id && ("model " + requested.model_id),
      requested.mode_id && ("mode " + requested.mode_id),
      requested.reasoning && ("reasoning " + requested.reasoning)
    ].filter(Boolean);
    const effectiveParts = [
      confirmed.modelId && ("model " + confirmed.modelId),
      confirmed.modeId && ("mode " + confirmed.modeId),
      confirmed.reasoning && ("reasoning " + confirmed.reasoning)
    ].filter(Boolean);
    const summary = [
      liveParts.length && ("live: " + liveParts.join(", ")),
      requestedParts.length && ("requested: " + requestedParts.join(", ")),
      effectiveParts.length && ("effective: " + effectiveParts.join(", ")),
      admission && admission.state && ("settings status: " + admission.state)
    ].filter(Boolean).join("\n");
    if (!summary) return "";
    return '<span class="muted small agent-session-config">' +
      escapeHtml(summary) + "</span>";
  }

  function sessionRecordFromSnapshot(snap, fallback) {
    const nested = (snap && snap.session) || {};
    const base = fallback || {};
    const config = Object.assign({}, nested.config_json || base.config_json || {});
    if (snap && Array.isArray(snap.config_options)) config.options = snap.config_options;
    return {
      id: nested.id || base.id,
      title: nested.title || base.title,
      label: nested.label || base.label,
      agent_name: nested.agent_name || base.agent_name,
      model_id: nested.model_id != null ? nested.model_id : base.model_id,
      mode_id: nested.mode_id != null ? nested.mode_id : base.mode_id,
      config_json: config
    };
  }

  function sessionListItem(sessionId) {
    const list = document.querySelector("[data-agent-session-list]");
    if (!list || !sessionId) return null;
    const items = list.querySelectorAll("[data-session-id]");
    for (let i = 0; i < items.length; i += 1) {
      if (items[i].dataset.sessionId === sessionId) return items[i];
    }
    return null;
  }

  function patchSessionListFromSnapshot(sessionId, snap) {
    const item = sessionListItem(sessionId);
    if (!item) return;
    const runtime = item.querySelector(".agent-session-runtime");
    const fallbackName = runtime
      ? String(runtime.textContent || "").split(" · ")[0]
      : "";
    const record = sessionRecordFromSnapshot(snap, {
      id: sessionId,
      agent_name: fallbackName
    });
    if (runtime) {
      runtime.title = "Live provider, model, and settings";
      runtime.textContent = sessionRuntimeLabel(record);
    }
    const html = sessionConfigSummary(record);
    const details = item.querySelector(".agent-session-config");
    if (details) {
      if (html) details.outerHTML = html;
      else details.remove();
    } else if (html) {
      const body = item.querySelector(".agent-session-details-body");
      if (body) body.insertAdjacentHTML("afterbegin", html);
    }
  }

  function populateSelect(select, values, idKeys, defaultLabel) {
    if (!select) return;
    const selected = select.value;
    select.innerHTML = "";
    const inherited = document.createElement("option");
    inherited.value = "";
    inherited.textContent = defaultLabel || "Provider default";
    select.appendChild(inherited);
    (values || []).forEach(function (item) {
      const value = typeof item === "object"
        ? idKeys.map(function (key) { return item[key]; }).find(Boolean)
        : item;
      if (!value) return;
      const option = document.createElement("option");
      option.value = value;
      option.textContent = typeof item === "object" ? (item.name || item.label || value) : value;
      if (String(value) === selected) option.selected = true;
      select.appendChild(option);
    });
    if (selected && !Array.prototype.some.call(select.options, function (option) {
      return option.value === selected;
    })) {
      const option = document.createElement("option");
      option.value = selected;
      option.textContent = selected;
      option.selected = true;
      select.appendChild(option);
    }
  }

  function newSessionOptionSelects(dialog) {
    return dialog.querySelectorAll("[data-agent-new-model-provider], [data-agent-new-model], [data-agent-new-mode], [data-agent-new-effort]");
  }

  function setNewSessionBusy(dialog, busy, message) {
    const form = dialog.querySelector("[data-agent-new-form]");
    const submit = dialog.querySelector("[data-agent-new-submit]");
    const status = dialog.querySelector("[data-agent-new-status]");
    const spinner = dialog.querySelector("[data-agent-new-model-spinner]");
    const model = dialog.querySelector("[data-agent-new-model]");
    dialog.setAttribute("aria-busy", busy ? "true" : "false");
    dialog.dataset.agentNewOptionsLoaded = busy ? "false" : "true";
    if (form) form.setAttribute("aria-busy", busy ? "true" : "false");
    if (submit && submit.textContent !== "Starting…") submit.disabled = !!busy;
    if (status) {
      status.hidden = !busy;
      if (message) status.textContent = message;
    }
    if (spinner) spinner.hidden = !busy;
    if (model) model.setAttribute("aria-busy", busy ? "true" : "false");
    newSessionOptionSelects(dialog).forEach(function (select) {
      select.disabled = !!busy;
    });
  }

  function fillProviderSelect(select, providers) {
    if (!select) return;
    const selected = select.value;
    const seen = {};
    Array.prototype.forEach.call(select.options, function (option) {
      seen[option.value] = true;
    });
    (providers || []).forEach(function (item) {
      if (!item || !item.id || seen[item.id]) return;
      seen[item.id] = true;
      const option = document.createElement("option");
      option.value = item.id;
      option.textContent = item.display_name || item.id;
      select.appendChild(option);
    });
    if (selected) select.value = selected;
  }

  function readProviderCatalog() {
    const script = document.getElementById("pa-provider-catalog");
    if (!script) return [];
    try {
      const parsed = JSON.parse(script.textContent || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
      return [];
    }
  }

  function loadProviderCatalog(select) {
    if (!select) return Promise.resolve([]);
    fillProviderSelect(select, readProviderCatalog());
    return csrfFetch("/providers/catalog").then(function (providers) {
      fillProviderSelect(select, providers);
      return providers || [];
    }).catch(function () {
      return readProviderCatalog();
    });
  }

  function populateNewSessionOptions(dialog, snap) {
    const modelProviderWrap = dialog.querySelector("[data-agent-new-model-provider-wrap]");
    const modelProviderSelect = dialog.querySelector("[data-agent-new-model-provider]");
    const modelProviders = (snap && snap.model_providers) || [];
    const supportsModelProvider = !!(snap && (snap.supports_model_provider || modelProviders.length));
    if (modelProviderWrap) modelProviderWrap.hidden = !supportsModelProvider;
    if (modelProviderSelect) {
      const previous = modelProviderSelect.value;
      populateSelect(modelProviderSelect, modelProviders, ["id"], "Host default");
      const preferred = previous || (snap && snap.model_provider) || "";
      if (preferred) {
        modelProviderSelect.value = preferred;
        if (modelProviderSelect.value !== preferred) {
          const option = document.createElement("option");
          option.value = preferred;
          option.textContent = preferred;
          option.selected = true;
          modelProviderSelect.appendChild(option);
        }
      }
    }
    const models = snap && snap.models && (snap.models.availableModels || snap.models.available_models);
    const modes = snap && snap.modes && (snap.modes.availableModes || snap.modes.available_modes);
    populateSelect(dialog.querySelector("[data-agent-new-model]"), models, ["modelId", "model_id", "id"]);
    populateSelect(dialog.querySelector("[data-agent-new-mode]"), modes, ["id", "modeId", "mode_id"]);
    const raw = (snap && snap.config_options) || [];
    const effort = raw.find(function (option) {
      const id = String(option && (option.id || option.configId || option.config_id || option.name) || "").toLowerCase().replace(/[_ -]/g, "");
      return ["effort", "reasoningeffort", "reasoninglevel", "thinkinglevel"].includes(id);
    });
    updateNewSessionEffort(dialog);
    const related = dialog.querySelector("[data-agent-new-related]");
    if (!related) return;
    related.innerHTML = "";
    raw.filter(function (option) {
      if (!option || option === effort) return false;
      const id = String(option.id || option.configId || option.config_id || "").toLowerCase();
      return id !== "model";
    }).forEach(function (option) {
      const id = option.id || option.configId || option.config_id;
      const choices = option.options || option.choices || option.values || [];
      if (!id || !choices.length) return;
      const label = document.createElement("label");
      const caption = document.createElement("span");
      caption.textContent = option.name || id;
      const select = document.createElement("select");
      select.name = "config." + id;
      select.dataset.agentNewConfig = id;
      populateSelect(select, choices, ["value", "id"]);
      label.appendChild(caption);
      label.appendChild(select);
      related.appendChild(label);
    });
    related.hidden = !related.children.length;
  }

  function newSessionSnapshotForProvider(widget, providerId, modelProvider) {
    const snap = widget && widget._acw && widget._acw.lastSnapshot;
    const activeProvider = snap && snap.session && snap.session.agent_name;
    if (providerId && providerId === activeProvider && !modelProvider) return Promise.resolve(snap);
    if (!providerId) return Promise.resolve(null);
    let path = "/provider-options/" + encodeURIComponent(providerId);
    if (modelProvider) path += "?model_provider=" + encodeURIComponent(modelProvider);
    return csrfFetch(path);
  }

  function updateNewSessionEffort(dialog, wanted) {
    const helper = window.PANativeEffort, select = dialog.querySelector("[data-agent-new-effort]");
    if (!select || !helper) return;
    const model = dialog.querySelector("[data-agent-new-model]");
    const values = helper.evidence(dialog._effortCatalog || [], dialog._effortScope || {}, model && model.value);
    const reset = helper.fill(select, values, wanted, values.length ? "Automatic / provider default" : "Provider default — effort support unknown or unavailable");
    if (reset) {
      const status = dialog.querySelector("[data-agent-new-status]");
      if (status) {status.hidden = false; status.textContent = "Previous effort is unavailable for this model. Reset to provider default; review before starting.";}
    }
  }

  function refreshNewSessionOptions(dialog, widget) {
    const provider = dialog.querySelector("[data-agent-new-provider]");
    const modelProvider = dialog.querySelector("[data-agent-new-model-provider]");
    const providerId = provider ? provider.value : "";
    const modelProviderId = modelProvider && !modelProvider.hidden && modelProvider.value ? modelProvider.value : "";
    const requestId = dialog._acwOptionsRequest = Number(dialog._acwOptionsRequest || 0) + 1;
    dialog._effortCatalog = [];
    updateNewSessionEffort(dialog);
    setNewSessionBusy(dialog, true, "Loading session options…");
    return Promise.all([newSessionSnapshotForProvider(widget, providerId, modelProviderId),
      window.PANativeEffort ? window.PANativeEffort.catalog().catch(() => ({})) : Promise.resolve({})])
      .then(function (results) {
        if (dialog._acwOptionsRequest !== requestId) return;
        dialog._effortCatalog = results[1].candidates || [];
        dialog._effortScope = window.PANativeEffort && window.PANativeEffort.localScope(results[1], providerId, modelProviderId);
        populateNewSessionOptions(dialog, results[0]);
      })
      .catch(function () {
        if (dialog._acwOptionsRequest === requestId) populateNewSessionOptions(dialog, null);
      })
      .finally(function () {
        if (dialog._acwOptionsRequest !== requestId) return;
        setNewSessionBusy(dialog, false); updateNewSessionEffort(dialog);
      });
  }

  function applyNewSessionDefaults(dialog, defaults) {
    defaults = defaults || {};
    const values = {
      "[data-agent-new-model-provider]": defaults.model_provider || "",
      "[data-agent-new-model]": defaults.model_id || "",
      "[data-agent-new-mode]": defaults.mode_id || "",
      "[data-agent-new-effort]": defaults.effort || "",
    };
    Object.keys(values).forEach(function (selector) {
      const select = dialog.querySelector(selector);
      if (!select) return;
      select.value = values[selector];
      if (selector === "[data-agent-new-effort]") { updateNewSessionEffort(dialog, values[selector]); return; }
      if (values[selector] && select.value !== values[selector]) {
        const option = document.createElement("option");
        option.value = values[selector];
        option.textContent = values[selector];
        option.selected = true;
        select.appendChild(option);
      }
    });
    const config = defaults.config || {};
    dialog.querySelectorAll("[data-agent-new-config]").forEach(function (select) {
      if (Object.prototype.hasOwnProperty.call(config, select.dataset.agentNewConfig)) {
        select.value = String(config[select.dataset.agentNewConfig]);
      }
    });
  }

  function prepareNewSessionDialog(dialog, widget) {
    const form = dialog.querySelector("[data-agent-new-form]");
    const selectionControls = dialog.querySelector("[data-execution-preferences]");
    if (selectionControls) {
      if (form) form.reset();
      if (selectionControls._selection) selectionControls._selection.reset();
      const active = widget && widget._acw && widget._acw.lastSnapshot;
      dialog._executionBoundarySource = active && active.session;
      dialog._executionBoundaryKey = crypto.randomUUID();
      const linked = dialog.querySelector("[data-agent-new-linked]");
      if (linked) linked.disabled = !active || !active.session || !!active.session.dispatch_id;
      const modes = active && active.modes && (active.modes.availableModes || active.modes.available_modes);
      populateSelect(dialog.querySelector("[data-agent-new-mode]"), modes || [], ["id", "modeId", "mode_id"], "Chat default (Full access for Codex)");
      setNewSessionBusy(dialog, false);
      return Promise.resolve(); // Selection controls read cached catalogs, never probe per dropdown.
    }
    const provider = dialog.querySelector("[data-agent-new-provider]");
    const snap = widget && widget._acw && widget._acw.lastSnapshot;
    const activeProvider = snap && snap.session && snap.session.agent_name;
    dialog._acwProviderTouched = false;
    if (form) form.reset();
    fillProviderSelect(provider, readProviderCatalog());
    populateNewSessionOptions(dialog, null);
    setNewSessionBusy(dialog, true, "Loading session options…");
    return Promise.all([loadProviderCatalog(provider), csrfFetch("/preferences")])
      .then(function (results) {
        const prefs = results[1] || {};
        const userSurfaces = prefs.user && prefs.user.agent_surfaces || {};
        const globalSurfaces = prefs.global && prefs.global.agent_surfaces || {};
        const userDefaults = userSurfaces["chat.default"] || {};
        const globalDefaults = globalSurfaces["chat.default"] || {};
        const defaults = {
          provider: userDefaults.provider || globalDefaults.provider || prefs.agent_provider || activeProvider || "",
          model_provider: userDefaults.model_provider || globalDefaults.model_provider || "",
          model_id: userDefaults.model_id || globalDefaults.model_id || "",
          mode_id: userDefaults.mode_id || globalDefaults.mode_id || "",
          effort: userDefaults.effort || globalDefaults.effort || "",
          config: Object.assign({}, globalDefaults.config || {}, userDefaults.config || {}),
        };
        if (provider && !dialog._acwProviderTouched && defaults.provider) {
          provider.value = defaults.provider;
        }
        return refreshNewSessionOptions(dialog, widget).then(function () {
          if (!dialog._acwProviderTouched) applyNewSessionDefaults(dialog, defaults);
        });
      })
      .catch(function () {
        setNewSessionBusy(dialog, false);
      });
  }

  function bindSessionSidebar(scope) {
    const root = scope || document;
    root.querySelectorAll("[data-agent-new-thread]").forEach(function (button) {
      if (button._acwBound) return;
      button._acwBound = true;
      button.addEventListener("click", function () {
        const widget = document.querySelector("[data-agent-chat]");
        if (widget && widget._acw && widget._acw.threads) widget._acw.threads.branch("empty");
      });
    });
    const list = root.querySelector("[data-agent-session-list]");
    if (list && !list._acwBound) {
      list._acwBound = true;
      list.addEventListener("click", function (e) {
        const li = e.target.closest("[data-session-id]");
        if (!li) return;
        const archiveButton = e.target.closest(
          "[data-agent-session-archive], [data-agent-session-unarchive]"
        );
        if (archiveButton) {
          e.preventDefault();
          e.stopPropagation();
          const sessionId = li.dataset.sessionId;
          const unarchive = archiveButton.hasAttribute("data-agent-session-unarchive") ||
            archiveButton.dataset.archived === "true";
          const widget = document.querySelector("[data-agent-chat]");
          csrfFetch("/sessions/" + encodeURIComponent(sessionId) +
            (unarchive ? "/unarchive" : "/archive"), {
            method: "POST",
            body: "{}",
          }).then(function () {
            if (!unarchive && widget && widget._acw && widget._acw.sessionId === sessionId) {
              widget._acw.markSessionEnded(
                "Conversation archived. Its durable history remains available."
              );
            }
            refreshSessionList(null);
          }).catch(function (err) {
            window.alert("Could not " + (unarchive ? "unarchive" : "archive") +
              " conversation: " + err.message);
          });
          return;
        }
        const pinButton = e.target.closest("[data-agent-session-pin]");
        if (pinButton) {
          e.preventDefault();
          e.stopPropagation();
          const pinned = pinButton.dataset.pinned !== "true";
          csrfFetch("/sessions/" + encodeURIComponent(li.dataset.sessionId) + "/pin", {
            method: "PUT",
            body: JSON.stringify({ pinned: pinned }),
          }).then(function () {
            refreshSessionList(null, true);
          }).catch(function (err) {
            window.alert("Could not update pin: " + err.message);
          });
          return;
        }
        const controlButton = e.target.closest("[data-agent-session-control]");
        if (controlButton) {
          e.preventDefault();
          e.stopPropagation();
          const mode = controlButton.dataset.mode || controlButton.dataset.controlMode;
          csrfFetch("/sessions/" + encodeURIComponent(li.dataset.sessionId) + "/control", {
            method: "POST",
            body: JSON.stringify({ mode: mode }),
          }).then(function () {
            refreshSessionList(null, true);
          }).catch(function (err) {
            window.alert("Could not change session control: " + err.message);
          });
          return;
        }
        const continueButton = e.target.closest("[data-agent-session-continue]");
        if (continueButton) {
          e.preventDefault();
          e.stopPropagation();
          continueButton.disabled = true;
          csrfFetch("/sessions/" + encodeURIComponent(li.dataset.sessionId) + "/continue", {
            method: "POST",
            body: "{}",
          }).then(function (snapshot) {
            const widget = document.querySelector("[data-agent-chat]");
            const sessionId = snapshot && snapshot.session && snapshot.session.id;
            if (widget && widget._acw && sessionId) {
              widget._acw.switchSession(sessionId, true, "");
            }
            refreshSessionList(sessionId, true);
          }).catch(function (err) {
            continueButton.disabled = false;
            window.alert("Could not continue in a new chat: " + err.message);
          });
          return;
        }
        if (!e.target.closest("[data-agent-session-open]")) return;
        const widget = document.querySelector("[data-agent-chat]");
        if (widget && widget._acw) {
          widget._acw.switchSession(
            li.dataset.sessionId,
            li.dataset.sessionLive !== "false",
            li.dataset.sessionInstance || ""
          );
        }
      });
    }
    const historyToggle = root.querySelector("[data-agent-history-toggle]");
    const sessionSearch = root.querySelector("[data-agent-session-search]");
    if (historyToggle && !historyToggle._acwBound) {
      historyToggle._acwBound = true;
      historyToggle.addEventListener("click", function () {
        const enabled = historyToggle.getAttribute("aria-checked") !== "true";
        historyToggle.setAttribute("aria-checked", enabled ? "true" : "false");
        const state = historyToggle.querySelector("[data-agent-history-state]");
        if (state) state.textContent = enabled ? "On" : "Off";
        if (sessionSearch) {
          sessionSearch.hidden = !enabled;
          if (!enabled) sessionSearch.value = "";
        }
        const widget = document.querySelector("[data-agent-chat]");
        refreshSessionList(widget && widget._acw && widget._acw.sessionId);
      });
    }
    updateSessionTitleTooltips(root);
    if (!window._paAgentTitleResizeBound) {
      window._paAgentTitleResizeBound = true;
      window.addEventListener("resize", function () { updateSessionTitleTooltips(document); });
    }
    if (sessionSearch && !sessionSearch._acwBound) {
      sessionSearch._acwBound = true;
      sessionSearch.addEventListener("input", filterSessionList);
    }
    const endAll = root.querySelector("[data-agent-end-all]");
    if (endAll && !endAll._acwBound) {
      endAll._acwBound = true;
      endAll.addEventListener("click", function () {
        if (!window.confirm("End every live and recoverable agent session?")) return;
        endAll.disabled = true;
        csrfFetch("/sessions/close-all", { method: "POST", body: "{}" })
          .then(function () {
            const widget = document.querySelector("[data-agent-chat]");
            if (widget && widget._acw && widget._acw.sessionId) {
              widget._acw.markSessionEnded(
                "All sessions ended. Start a new session to continue."
              );
            }
            refreshSessionList(null);
          })
          .catch(function (err) {
            window.alert("Could not end all sessions: " + err.message);
          })
          .finally(function () {
            endAll.disabled = false;
          });
      });
    }
    const neu = root.querySelector("[data-agent-new-session]");
    if (neu && !neu._acwBound) {
      neu._acwBound = true;
      neu.addEventListener("click", function () {
        const widget = document.querySelector("[data-agent-chat]");
        const dialog = document.querySelector("[data-agent-new-dialog]");
        if (!dialog) return;
        const error = dialog.querySelector("[data-agent-new-error]");
        if (error) error.hidden = true;
        dialog._agentOpener = neu;
        prepareNewSessionDialog(dialog, widget);
        if (typeof dialog.showModal === "function") dialog.showModal();
        else dialog.setAttribute("open", "");
        window.requestAnimationFrame(function () {
          const initial = dialog.querySelector('input[name="title"]');
          if (initial) initial.focus();
        });
      });
    }
    const dialog = root.querySelector("[data-agent-new-dialog]");
    if (dialog && !dialog._acwBound) {
      dialog._acwBound = true;
      const provider = dialog.querySelector("[data-agent-new-provider]");
      if (provider) {
        provider.addEventListener("change", function () {
          dialog._acwProviderTouched = true;
          dialog.querySelectorAll("[data-agent-new-model-provider], [data-agent-new-model], [data-agent-new-mode], [data-agent-new-effort]").forEach(function (select) {
            select.value = "";
          });
          refreshNewSessionOptions(dialog, document.querySelector("[data-agent-chat]"));
        });
      }
      const draftModel = dialog.querySelector("[data-agent-new-model]");
      if (draftModel) draftModel.addEventListener("change", function () { updateNewSessionEffort(dialog); });
      const modelProvider = dialog.querySelector("[data-agent-new-model-provider]");
      if (modelProvider && !modelProvider._acwBound) {
        modelProvider._acwBound = true;
        modelProvider.addEventListener("change", function () {
          const model = dialog.querySelector("[data-agent-new-model]");
          if (model) model.value = "";
          refreshNewSessionOptions(dialog, document.querySelector("[data-agent-chat]"));
        });
      }
      dialog.querySelectorAll("[data-agent-new-cancel]").forEach(function (button) {
        button.addEventListener("click", function () { dialog.close(); });
      });
      dialog.addEventListener("close", function () {
        if (dialog._agentOpener && dialog._agentOpener.isConnected) dialog._agentOpener.focus();
        dialog._agentOpener = null;
      });
      const form = dialog.querySelector("[data-agent-new-form]");
      if (form) form.addEventListener("submit", function (event) {
        event.preventDefault();
        if (dialog.getAttribute("aria-busy") === "true") return;
        const data = new FormData(form);
        const body = {};
        if (data.get("linked_context") && dialog._executionBoundarySource) {
          const source = dialog._executionBoundarySource;
          body.context_source_session_id = source.id;
          body.card_id = source.card_id;
          body.project_id = source.project_id;
          body.label = "execution-boundary:" + source.id + ":" + dialog._executionBoundaryKey;
        }
        if (data.get("execution_preferences")) body.execution_preferences = JSON.parse(data.get("execution_preferences"));
        ["title", "provider", "model_provider", "model_id", "mode_id", "effort", "cwd"].forEach(function (key) {
          const value = String(data.get(key) || "").trim();
          if (value) body[key] = value;
        });
        body.config = {};
        dialog.querySelectorAll("[data-agent-new-config]").forEach(function (select) {
          if (select.value && !select.closest("fieldset[disabled]")) body.config[select.dataset.agentNewConfig] = select.value;
        });
        if (!Object.keys(body.config).length) delete body.config;
        const submit = dialog.querySelector("[data-agent-new-submit]");
        const error = dialog.querySelector("[data-agent-new-error]");
        if (submit) {
          submit.disabled = true;
          submit.textContent = "Starting…";
        }
        if (error) error.hidden = true;
        csrfFetch("/sessions", { method: "POST", body: JSON.stringify(body) })
          .then(function (snap) {
            const sid = (snap.session && snap.session.id) || snap.id;
            dialog.close();
            const list = document.querySelector("[data-agent-session-list]");
            if (list && sid) {
              const record = sessionRecordFromSnapshot(snap, { id: sid });
              record.presentation = snap.presentation || {};
              const holder = document.createElement("ul");
              // Render through the same escaping and action code as refreshed rows.
              list.appendChild(holder);
              renderSessionList(holder, [record], true, sid);
              const row = holder.firstElementChild;
              if (row) list.prepend(row);
              holder.remove();
              list.querySelectorAll("[data-agent-session-empty], [data-agent-session-state]").forEach(function (node) { node.remove(); });
              list.setAttribute("aria-busy", "false");
            }
            refreshSessionList(sid, true);
            const widget = document.querySelector("[data-agent-chat]");
            if (widget && widget._acw && sid) widget._acw.switchSession(sid);
          })
          .catch(function (err) {
            if (error) {
              error.textContent = err.message;
              error.hidden = false;
            }
          })
          .finally(function () {
            if (submit) {
              submit.disabled = false;
              submit.textContent = "Start chat";
            }
          });
      });
    }
    const cardDialog = root.querySelector("[data-agent-card-dialog]");
    if (cardDialog && !cardDialog._acwBound) {
      cardDialog._acwBound = true;
      cardDialog.querySelectorAll("[data-agent-card-cancel]").forEach(function (button) {
        button.addEventListener("click", function () { cardDialog.close(); });
      });
      cardDialog.addEventListener("close", function () {
        if (cardDialog._agentOpener && cardDialog._agentOpener.isConnected) {
          cardDialog._agentOpener.focus();
        }
        cardDialog._agentOpener = null;
      });
      const cardForm = cardDialog.querySelector("[data-agent-card-form]");
      if (cardForm) cardForm.addEventListener("submit", function (event) {
        event.preventDefault();
        const widget = cardDialog._agentWidget;
        if (!widget || !widget.sessionId) return;
        const data = new FormData(cardForm);
        const selectedCardId = String(data.get("card_id") || "").trim();
        const title = String(data.get("title") || "").trim();
        const projectId = String(data.get("project_id") || "").trim();
        const error = cardDialog.querySelector("[data-agent-card-error]");
        const submit = cardDialog.querySelector("[data-agent-card-submit]");
        if (!selectedCardId && !title) {
          if (error) {
            error.textContent = "Choose an existing card or enter a new card title.";
            error.hidden = false;
          }
          return;
        }
        if (error) error.hidden = true;
        if (submit) submit.disabled = true;
        let cardPromise = Promise.resolve({ id: selectedCardId, title: "the selected card" });
        if (!selectedCardId) {
          const key = "session-card:" + widget.sessionId + ":" +
            (window.crypto && window.crypto.randomUUID ? window.crypto.randomUUID() : Date.now());
          cardPromise = paFetch("/api/cards", {
            method: "POST",
            headers: { "Idempotency-Key": key },
            body: JSON.stringify({
              realm_id: cardDialog.dataset.agentCardRealm || "default",
              kind: "task",
              title: title,
              body: "Captured from agent session " + widget.sessionId + ".",
              lane: "active",
              project_id: projectId || null,
              tags: ["agent-session"],
              auto_enrich: false,
            }),
          });
        }
        cardPromise.then(function (card) {
          return widget.api(
            "/sessions/" + encodeURIComponent(widget.sessionId) +
              "/cards/" + encodeURIComponent(card.id),
            { method: "POST", body: JSON.stringify({ make_primary: true }) }
          ).then(function () { return card; });
        }).then(function (card) {
          widget.cardId = card.id;
          cardDialog.close();
          widget.addBubble(
            "system",
            "This session is now captured in card “" + card.title + "”.",
            new Date().toISOString(),
            { system: true, forceVisible: true }
          );
          refreshSessionList(widget.sessionId);
        }).catch(function (err) {
          if (error) {
            error.textContent = err.message;
            error.hidden = false;
          }
        }).finally(function () {
          if (submit) submit.disabled = false;
        });
      });
    }
    root.querySelectorAll("[data-agent-sidebar-toggle]").forEach(function (toggle) {
      if (toggle._acwBound) return;
      toggle._acwBound = true;
      toggle.addEventListener("click", function () {
        const layout = toggle.closest(".page-agent") || document.querySelector(".page-agent");
        if (!layout) return;
        const collapsed = layout.classList.toggle("is-sidebar-collapsed");
        toggle.textContent = collapsed ? "Show sessions" : "Hide sessions";
        toggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
        try { localStorage.setItem("pa-agent-sidebar-collapsed", collapsed ? "1" : "0"); } catch (_) {}
      });
      let collapsed = false;
      try {
        const saved = localStorage.getItem("pa-agent-sidebar-collapsed");
        collapsed = saved === "1";
      } catch (_) {}
      const layout = toggle.closest(".page-agent") || document.querySelector(".page-agent");
      if (layout) layout.classList.toggle("is-sidebar-collapsed", collapsed);
      toggle.textContent = collapsed ? "Show sessions" : "Hide sessions";
      toggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
    });
  }

  function mountAll(scope) {
    const root = scope || document;
    root.querySelectorAll("[data-agent-chat]").forEach(function (el) {
      if (el._acw) return;
      el._acw = new AgentChatWidget(el);
    });
    bindSessionSidebar(root);
    startSessionListFreshness();
    const dialog = root.querySelector("[data-agent-new-dialog]");
    if (dialog) {
      fillProviderSelect(
        dialog.querySelector("[data-agent-new-provider]"),
        readProviderCatalog()
      );
    }
  }

  function destroyAll(scope, reason) {
    const target = scope || document;
    cancelSessionListRecovery(target, reason || "subtree-removed");
    const roots = [];
    if (target.matches && target.matches("[data-agent-chat]")) roots.push(target);
    if (target.querySelectorAll) {
      target.querySelectorAll("[data-agent-chat]").forEach(function (root) {
        roots.push(root);
      });
    }
    roots.forEach(function (root) {
      if (root._acw && typeof root._acw.destroy === "function") {
        root._acw.destroy(reason || "subtree-removed");
      }
    });
  }

  function closeAll(scope, reason) {
    const target = scope || document;
    if (target.querySelectorAll) {
      target.querySelectorAll("[data-agent-chat]").forEach(function (root) {
        if (root._acw) root._acw.closeSSE(reason || "page-suspended");
      });
    }
  }

  window.PAAgentChat = {
    mount: mountAll,
    destroy: destroyAll,
    AgentChatWidget: AgentChatWidget,
    refreshSessionList: refreshSessionList,
    patchSessionListFromSnapshot: patchSessionListFromSnapshot,
    sessionRuntimeLabel: sessionRuntimeLabel,
    sessionConfigSummary: sessionConfigSummary,
    confirmedSessionFields: confirmedSessionFields,
    startSessionListFreshness: startSessionListFreshness,
    stopSessionListFreshness: stopSessionListFreshness,
    sessionListRefreshEvents: SESSION_LIST_REFRESH_EVENTS.slice(),
    anchoredScrollTop: anchoredScrollTop,
    renderMarkdown: renderMarkdown,
    renderMarkdownAsync: renderMarkdownAsync,
    INITIAL_VISIBLE_EVENTS: INITIAL_VISIBLE_EVENTS,
    PROMPT_SUBMIT_TIMEOUT_MS: PROMPT_SUBMIT_TIMEOUT_MS,
    sessionDomCache: sessionDomCache,
  };

  document.addEventListener("DOMContentLoaded", function () {
    mountAll(document);
  });
  document.addEventListener("visibilitychange", function () {
    if (document.hidden) {
      stopSessionListFreshness("document-hidden");
      return;
    }
    startSessionListFreshness();
    refreshSessionList(selectedSessionId(), true);
  });
  if (window.addEventListener) {
    window.addEventListener("pagehide", function (event) {
      if (event.persisted) closeAll(document, "pagehide-persisted");
      else destroyAll(document, "pagehide");
    });
    window.addEventListener("pageshow", function (event) {
      if (!event.persisted) return;
      document.querySelectorAll("[data-agent-chat]").forEach(function (root) {
        if (root._acw && root._acw.sessionId) root._acw.connectSSE();
      });
    });
    window.addEventListener("popstate", function () {
      const root = document.querySelector(".page-agent [data-agent-chat]");
      if (!root || !root._acw) return;
      const params = new URL(window.location.href).searchParams;
      const sessionId = params.get("session") || "";
      const instanceId = params.get("instance") || "";
      if (sessionId) {
        root._acw.switchSession(sessionId, true, instanceId, { replace: true });
      }
    });
  }
  document.body && document.body.addEventListener("htmx:afterSwap", function (e) {
    const target = (e.detail && e.detail.target) || e.target;
    mountAll(target || document);
  });
  document.body && document.body.addEventListener("htmx:beforeSwap", function (e) {
    let target = (e.detail && e.detail.target) || e.target;
    if (typeof target === "string") target = document.querySelector(target);
    destroyAll(target || document, "spa-swap");
  });
  document.addEventListener("pa:historyWillReload", function () {
    destroyAll(document, "history-reload");
  });
})();
