/* Multi-session AgentChatWidget client (SSE + REST). */
(function () {
  "use strict";

  const MARKED_URL = "https://cdn.jsdelivr.net/npm/marked@15.0.7/marked.min.js";
  const PURIFY_URL = "https://cdn.jsdelivr.net/npm/dompurify@3.2.4/dist/purify.min.js";
  const IMAGE_TYPES = ["image/png", "image/jpeg", "image/gif", "image/webp"];
  const MAX_IMAGES = 4;
  const MAX_IMAGE_BYTES = 10 * 1024 * 1024;
  const MAX_TOTAL_IMAGE_BYTES = 20 * 1024 * 1024;
  const TRANSCRIPT_PAGE_LIMIT = 1000;
  const SESSION_ROUTE_TIMEOUT_MS = 4000;
  const LIVE_SNAPSHOT_TIMEOUT_MS = 3000;
  const LIVE_STATE_RETRY_MS = 3000;

  let libsPromise = null;

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
    this.autoStart = root.dataset.autoStart !== "0";
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
      loadOlderStatus: root.querySelector("[data-acw-load-older-status]"),
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
    this.subscriptionGeneration = 0;
    this.routeAbortController = null;
    this.lastSeq = 0;
    this.transcriptEvents = [];
    this.seenEvents = {};
    this.hasOlder = false;
    this.olderCursor = null;
    this.loadingOlder = false;
    this.olderError = "";
    this.streaming = {};
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
    this.composerEnabled = true;
    this.turnActive = false;
    this.sessionClosed = false;
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

    this.startupRetryId = null;
    this.startupRetryCount = 0;
    this.liveStateRetryId = null;
    this._bind();
    this.drafts = window.PAAgentDrafts && window.PAAgentDrafts.installWidget
      ? window.PAAgentDrafts.installWidget(this) : null;
    const self = this;
    ensureMarkdown().then(function () { self.rerenderMarkdownBubbles(); });
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
    const end = this.root.querySelector("[data-acw-end]");
    if (end) end.addEventListener("click", function () { self.closeSession(); });
    const restart = this.root.querySelector("[data-acw-restart]");
    if (restart) restart.addEventListener("click", function () { self.restartSession(); });
    const cardLink = this.root.querySelector("[data-acw-card-link]");
    if (cardLink) cardLink.addEventListener("click", function () {
      openSessionCardDialog(self);
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

  AgentChatWidget.prototype.apiWithTimeout = function (path, timeoutMs) {
    const controller = new AbortController();
    const timeoutId = setTimeout(function () { controller.abort(); }, timeoutMs);
    return this.api(path, { signal: controller.signal })
      .catch(function (error) {
        if (error && error.name === "AbortError") {
          throw new Error("Live state request exceeded its latency budget.");
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
    const recoverable = detail.recoverable === true;
    if (this.els.recover) this.els.recover.hidden = !recoverable;
    if (this.els.history) this.els.history.hidden = !detail.history_url;
  };

  AgentChatWidget.prototype.clearSelectedSession = function () {
    this.closeSSE("session-cleared");
    this.sessionId = "";
    this.ownerInstanceId = "";
    this.sessionRoute = null;
    this.root.dataset.sessionId = "";
    this.root.dataset.ownerInstanceId = "";
    this.apiBase = String(this.root.dataset.apiBase || "/api/agent").replace(/\/$/, "");
    this.lastSeq = 0;
    this.sessionClosed = true;
    this.setTurnActive(false);
    this.setStatus("offline");
    this.setComposerEnabled(false);
    this.showRecoveryActions({});
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
    return this.api("/history/" + encodeURIComponent(sessionId)).then(function (history) {
      if (!self._isCurrentSessionRequest(sessionId, requestGeneration)) return null;
      const snap = {
        session: history.session,
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
    if (code === "session_deleted" && this.autoStart) {
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

    boot
      .then(function (snap) {
        self.startupRetryCount = 0;
        const sid = (snap.session && snap.session.id) || (snap.id) || self.sessionId;
        if (self.drafts) self.drafts.promoteSession(sid);
        self.sessionId = sid;
        self.root.dataset.sessionId = sid;
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
    this.liveStateRetryId = setTimeout(function () {
      self.liveStateRetryId = null;
      if (self.destroyed || generation !== self.subscriptionGeneration) return;
      if (reroute) {
        self.openSession(sessionId, self.ownerInstanceId, { replace: true }).catch(function () {});
      } else {
        self._loadLiveSnapshot(sessionId, generation);
      }
    }, LIVE_STATE_RETRY_MS);
  };

  AgentChatWidget.prototype._loadLiveSnapshot = function (sessionId, generation) {
    const self = this;
    return this.apiWithTimeout(
      "/sessions/" + encodeURIComponent(sessionId),
      LIVE_SNAPSHOT_TIMEOUT_MS
    ).then(function (snap) {
      if (self.destroyed || generation !== self.subscriptionGeneration) return null;
      if (self.liveStateRetryId) clearTimeout(self.liveStateRetryId);
      self.liveStateRetryId = null;
      self.showRecoveryActions({});
      self.applySnapshot(snap);
      self.connectSSE();
      self.refreshBrowserState();
      return snap;
    }).catch(function () {
      if (self.destroyed || generation !== self.subscriptionGeneration) return null;
      self.sessionRoute = Object.assign({}, self.sessionRoute || {}, { state: "live_degraded" });
      self.setStatus("offline");
      self.setComposerEnabled(false);
      self.setPlaceholder(
        "Live controls are temporarily unavailable. Durable history is shown; PA will retry automatically."
      );
      self._setRecoveryControl(true, "Retry live state");
      self._scheduleLiveStateRetry(sessionId, generation, false);
      return null;
    });
  };

  AgentChatWidget.prototype._setRecoveryControl = function (visible, label) {
    const button = this.root.querySelector("[data-acw-recover]");
    if (!button) return;
    button.hidden = !visible;
    if (label) button.textContent = label;
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
    const self = this;
    options = options || {};
    const generation = ++this.subscriptionGeneration;
    if (this.drafts && ownerInstanceId) this.drafts.setInstance(ownerInstanceId);
    if (this.drafts) this.drafts.switchSession(sessionId);
    this.sessionId = sessionId;
    this.ownerInstanceId = ownerInstanceId || "";
    this.root.dataset.sessionId = sessionId;
    this.root.dataset.ownerInstanceId = this.ownerInstanceId;
    if (this.els.promote) {
      this.els.promote.href = "/knowledge?session=" + encodeURIComponent(sessionId);
    }
    this._setRecoveryControl(false);
    this.showRecoveryActions({});
    this.sessionClosed = true;
    this.setComposerEnabled(false);
    this.setPlaceholder("Locating session owner…");
    let durableHistory = null;
    if (this.ownerInstanceId) {
      this.apiBase = this.ownerInstanceId === this.currentInstanceId
        ? "/api/agent"
        : "/api/fleet/instances/" +
          encodeURIComponent(this.ownerInstanceId) + "/agent";
      this.root.dataset.apiBase = this.apiBase;
    }
    const loadDurableHistory = function () {
      if (!durableHistory) {
        durableHistory = self.apiWithTimeout(
          "/history/" + encodeURIComponent(sessionId),
          LIVE_SNAPSHOT_TIMEOUT_MS
        ).then(function (history) {
          return self._applyDurableHistory(sessionId, history, generation);
        });
      }
      return durableHistory;
    };
    return this.resolveSessionRoute(sessionId, this.ownerInstanceId)
      .then(function (route) {
        if (self.destroyed || generation !== self.subscriptionGeneration) return null;
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
          if (self.autoStart) setTimeout(function () { self.init(); }, 0);
          return null;
        }
        if (route.live) {
          if (self.startupRetryId) clearTimeout(self.startupRetryId);
          self.startupRetryId = null;
          self.startupRetryCount = 0;
          return self._loadLiveSnapshot(sessionId, generation);
        }
        return loadDurableHistory().then(function (history) {
          self.setComposerEnabled(false);
          self.showRecoveryActions({
            recoverable: route.recoverable,
            history_url: "/api/agent/history/" + sessionId,
          });
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
          self.sessionRoute = { state: "live_degraded", live: true };
          self._writeSessionUrl(!!options.replace);
          return self._loadLiveSnapshot(sessionId, generation);
        }
        return loadDurableHistory().catch(function () { return null; }).then(function () {
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
    this.setPlaceholder("Recovering provider thread…");
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

  AgentChatWidget.prototype.toggleFlyout = function (kind) {
    const target = kind === "plan" ? this.els.planFlyout : this.els.toolFlyout;
    const other = kind === "plan" ? this.els.toolFlyout : this.els.planFlyout;
    const toggle = kind === "plan" ? this.els.planToggle : this.els.toolToggle;
    const otherToggle = kind === "plan" ? this.els.toolToggle : this.els.planToggle;
    const open = !!(target && target.hidden);
    if (target) target.hidden = !open;
    if (other) other.hidden = true;
    if (toggle) toggle.setAttribute("aria-expanded", open ? "true" : "false");
    if (otherToggle) otherToggle.setAttribute("aria-expanded", "false");
  };

  AgentChatWidget.prototype.closeFlyouts = function () {
    if (this.els.toolFlyout) this.els.toolFlyout.hidden = true;
    if (this.els.planFlyout) this.els.planFlyout.hidden = true;
    if (this.els.toolToggle) this.els.toolToggle.setAttribute("aria-expanded", "false");
    if (this.els.planToggle) this.els.planToggle.setAttribute("aria-expanded", "false");
  };

  AgentChatWidget.prototype.setPlaceholder = function (text) {
    if (this.els.placeholder) {
      this.els.placeholder.textContent = text;
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

  AgentChatWidget.prototype.applySnapshot = function (snap) {
    const self = this;
    this.lastSnapshot = Object.assign({}, this.lastSnapshot || {}, snap);
    const session = snap.session || {};
    const provisioning = session.config_json && session.config_json.provisioning || {};
    const recoveryBlocked = session.status === "recovery_blocked" || provisioning.state === "blocked";
    this.sessionClosed = session.status === "closed";
    if (this.drafts) this.drafts.onSnapshot(snap);
    this.setComposerEnabled(!this.sessionClosed && !recoveryBlocked);
    if (recoveryBlocked && this.els.input) {
      this.els.input.placeholder = "Recovery is blocked. Follow the action above, retry, or end the session.";
    }
    if (this.els.recovery) {
      this.els.recovery.hidden = !recoveryBlocked;
      if (this.els.recoveryAction) {
        this.els.recoveryAction.textContent = provisioning.action ||
          "Correct the project availability, retry this session, or end it from the Session menu.";
      }
      if (this.els.recoveryRetry) this.els.recoveryRetry.disabled = false;
    }
    if (this.els.title) {
      this.els.title.textContent = session.title || session.label || "Agent";
    }
    this.queuePaused = !!snap.queue_paused;
    if (Object.prototype.hasOwnProperty.call(snap, "transcript")) {
      this.hasOlder = !!(snap.transcript_page && snap.transcript_page.has_older);
      this.olderCursor = snap.transcript_page && (
        snap.transcript_page.next_before_seq || snap.transcript_page.oldest_seq
      );
      this.olderError = "";
      this.renderTranscript(snap.transcript || [], { scrollBottom: true });
    }
    // Transcript replay includes historical turn-completed events. Apply the live
    // snapshot state afterward so replay cannot reset an active turn's timer.
    this.setTurnActive(!!snap.prompting, snap.turn_started_at);
    this.setStatus(this.prompting ? "working" : snap.connected ? "online" : "offline");
    this.renderQueue(snap.queue || []);
    this.renderModelsModes(snap);
    this.renderConfigOptions(snap);
    this.renderMetrics(snap.metrics || session.metrics_json || {});
    if (this.commandCatalogSession !== this.sessionId) this.refreshCommandCatalog();
    if (this.els.permissions) {
      this.els.permissions.innerHTML = "";
      this.els.permissions.hidden = true;
    }
    (snap.pending_permissions || []).forEach(function (req) {
      if (req && typeof req === "object") self.showPermission(req);
    });
    refreshSessionList(this.sessionId);
  };

  AgentChatWidget.prototype.applyOptionSnapshot = function (snap) {
    this.lastSnapshot = snap;
    this.renderModelsModes(snap);
    this.renderConfigOptions(snap);
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
    this.els.loadOlder.hidden = !this.hasOlder && !this.olderError;
    this.els.loadOlder.disabled = this.loadingOlder;
    this.els.loadOlder.textContent = this.loadingOlder
      ? "Loading…"
      : this.olderError ? "Retry loading older messages" : "Load older messages";
    this.els.loadOlder.setAttribute("aria-busy", this.loadingOlder ? "true" : "false");
    if (this.els.loadOlderStatus) {
      this.els.loadOlderStatus.hidden = !this.olderError;
      this.els.loadOlderStatus.textContent = this.olderError;
    }
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
    this.transcriptEvents = unique;
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
        !child.hasAttribute("data-acw-load-older-status")
      ) child.remove();
    });
    this.streaming = {};
    this.updateOlderControl();
    if (!unique.length) {
      this.setPlaceholder("Send a message to the agent.");
      return;
    }
    this.clearPlaceholder();
    unique.forEach(function (event) {
      self.handleEvent(event, true, false);
    });
    if (options.scrollBottom) this.scrollToBottom();
  };

  AgentChatWidget.prototype.loadOlderTranscript = function () {
    if (this.loadingOlder || (!this.hasOlder && !this.olderError) || !this.sessionId || !this.els.messages) return;
    const oldest = this.olderCursor || this.transcriptEvents.reduce(function (result, event) {
      return event.seq && (!result || event.seq < result) ? event.seq : result;
    }, 0);
    if (!oldest) return;
    const self = this;
    const status = this.els.status && this.els.status.dataset.state;
    const wasPrompting = this.prompting;
    const startedAt = this.turnStartedAt && this.turnStartedAt.toISOString();
    this.loadingOlder = true;
    this.olderError = "";
    this.updateOlderControl();
    this.api(
      "/history/" + encodeURIComponent(this.sessionId) +
      "?before_seq=" + oldest + "&limit=" + TRANSCRIPT_PAGE_LIMIT
    ).then(function (data) {
      const pageEvents = data && data.events || [];
      self.hasOlder = !!(data && data.page && data.page.has_older);
      self.olderCursor = data && data.page && (
        data.page.next_before_seq || data.page.oldest_seq
      );
      const oldHeight = self.els.messages.scrollHeight;
      const oldTop = self.els.messages.scrollTop;
      // Read transcriptEvents only after the request resolves. It may now include
      // SSE events that arrived while the durable page was in flight.
      self.renderTranscript(pageEvents.concat(self.transcriptEvents), { scrollBottom: false });
      self.setTurnActive(wasPrompting, startedAt);
      if (status) self.setStatus(status);
      self.els.messages.scrollTop = anchoredScrollTop(
        oldTop,
        oldHeight,
        self.els.messages.scrollHeight
      );
    }).catch(function (err) {
      self.olderError = "Could not load older messages: " + err.message;
    }).finally(function () {
      self.loadingOlder = false;
      self.updateOlderControl();
    });
  };

  AgentChatWidget.prototype.connectSSE = function () {
    const self = this;
    if (this.destroyed || !this.sessionId) return;
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
      if (self.es !== es || self.destroyed) {
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
        } else if (err.status === 404) self.markSessionEnded("This session is no longer running.");
      });
    };

    function onAny(ev) {
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
      if (self.es !== es || self.destroyed) return;
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
    if (this.externalEventTransport) this.closeSSE("external-multiplex");
    else if (this.sessionId) this.connectSSE();
  };

  AgentChatWidget.prototype.closeSSE = function (reason) {
    this.subscriptionGeneration += 1;
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
    this.destroyed = true;
    if (this.routeAbortController) this.routeAbortController.abort();
    this.routeAbortController = null;
    this.closeSSE(reason || "widget-destroyed");
    this.stopBrowserRefresh();
    this.setTurnActive(false);
    if (this.startupRetryId) clearTimeout(this.startupRetryId);
    this.startupRetryId = null;
    if (this.liveStateRetryId) clearTimeout(this.liveStateRetryId);
    this.liveStateRetryId = null;
    Object.keys(this.toolTimers).forEach(function (key) {
      clearTimeout(this.toolTimers[key]);
    }, this);
    this.toolTimers = {};
    if (this.root && this.root._acw === this) this.root._acw = null;
  };

  AgentChatWidget.prototype.handleEvent = function (event, replay, record) {
    if (!event) return;
    event = this._normalizeEvent(event);
    const eventKey = this._eventKey(event);
    if (eventKey && this.seenEvents[eventKey]) return;
    if (eventKey) this.seenEvents[eventKey] = true;
    if (record !== false) this.transcriptEvents.push(event);
    const shouldFollow = !replay && this.isNearBottom();
    const self = this;
    const seq = event.seq || 0;
    if (seq) this.lastSeq = Math.max(this.lastSeq, seq);
    const type = event.type || event.event_type;
    const payload = event.payload || {};
    const created = event.created_at;

    switch (type) {
      case "user_message":
        if (this.drafts && typeof this.drafts.observeAcceptance === "function") {
          this.drafts.observeAcceptance(payload.id, false);
        }
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
        if (payload.phase === "commentary") {
          this.appendActivityProgress(payload.message_id || "progress", payload.text || "", created);
        } else {
          this.appendStream("agent", payload.message_id || "agent", payload.text || "", created);
        }
        break;
      case "card_disposition":
        this.renderCardDisposition(payload, created);
        break;
      case "agent_thought_chunk":
        if (this.showThinking) {
          this.appendStream("thought", payload.message_id || "thought", payload.text || "", created);
        }
        break;
      case "tool_call":
        // Cursor reuses a null messageId for the whole turn, so without this
        // post-tool text is appended onto the pre-tool bubble ("needed.Monica").
        this.finalizeStreams(created);
        this.upsertTool(payload, created);
        break;
      case "tool_call_update":
        this.upsertTool(payload, created);
        break;
      case "plan":
        this.renderPlan(payload, created);
        break;
      case "permission_request":
        this.showPermission(payload);
        break;
      case "permission_resolved":
        this.hidePermission(payload.request_id);
        break;
      case "turn_completed":
        this.finalizeStreams(created);
        this.finalizeActivity();
        this.setTurnActive(false);
        if (payload.usage) this.renderMetrics({ last_usage: payload.usage });
        break;
      case "queue_enqueued":
        if (this.drafts && typeof this.drafts.observeAcceptance === "function") {
          this.drafts.observeAcceptance(payload.id, true);
        }
        this.refreshQueue();
        break;
      case "queue_dequeued":
      case "queue_removed":
      case "queue_reordered":
      case "queue_paused":
      case "queue_resumed":
        this.refreshQueue();
        break;
      case "cancelled":
        this.finalizeStreams(created);
        this.finalizeActivity();
        this.setTurnActive(false);
        this.queuePaused = !!payload.pause_queue;
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
        if (!replay) {
          this.api("/sessions/" + this.sessionId).then(function (snap) {
            self.applyOptionSnapshot(snap);
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
        if (this.drafts) this.drafts.clear(true, "Draft cleared because this session ended.");
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
        this.setTurnActive(false);
        this.setStatus("offline");
        this.addBubble("system", payload.message || "Connection to the agent was lost. You may want to retry the prompt.", created, { forceVisible: true });
        break;
      case "turn_waiting":
        if (!this.turnActive) this.setTurnActive(true, created);
        this.addBubble("system", payload.message || "Waiting for the agent…", created, { system: true, forceVisible: true });
        break;
      case "prompt_failed":
        this.finalizeStreams(created);
        this.finalizeActivity();
        this.setTurnActive(false);
        this.addBubble("system", payload.error || payload.message || "The prompt failed.", created, { system: true, forceVisible: true });
        break;
      case "error":
        this.addBubble("system", payload.message || "Error", created, { system: true });
        break;
      default:
        break;
    }
    if (!replay && shouldFollow) this.scrollToBottom();
  };

  AgentChatWidget.prototype.addBubble = function (role, text, ts, opts) {
    opts = opts || {};
    this.clearPlaceholder();
    const row = document.createElement("div");
    row.className = "acw-msg acw-msg-" + role + (opts.system ? " is-system" : "");
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
    this.root.querySelectorAll(".acw-bubble-user, .acw-bubble-agent, .acw-bubble-thought").forEach(function (bubble) {
      self.renderMarkdownBubble(bubble);
    });
  };

  AgentChatWidget.prototype.appendStream = function (role, key, chunk, ts) {
    this.clearPlaceholder();
    const id = role + ":" + key;
    let stream = this.streaming[id];
    if (!stream) {
      const created = this.addBubble(role === "thought" ? "thought" : "agent", "", ts);
      if (role === "thought") created.row.classList.add("acw-msg-thought");
      stream = { text: "", bubble: created.bubble, row: created.row };
      this.streaming[id] = stream;
    }
    const next = chunk || "";
    stream.text += streamChunkSeparator(stream.text, next) + next;
    stream.bubble.dataset.markdown = stream.text;
    this.renderMarkdownBubble(stream.bubble);
  };

  AgentChatWidget.prototype.finalizeStreams = function (ts) {
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
    this.streaming = {};
  };

  AgentChatWidget.prototype.ensureActivity = function () {
    this.currentActivity = this.els.toolActivity;
    if (this.els.toolEmpty) this.els.toolEmpty.hidden = true;
    return this.currentActivity;
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

  AgentChatWidget.prototype.appendActivityProgress = function (key, chunk) {
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
    stream.text += streamChunkSeparator(stream.text, next) + next;
    stream.el.textContent = stream.text;
    this.followToolActivity(shouldFollow);
  };

  AgentChatWidget.prototype.finalizeActivity = function () {
    this.currentActivity = null;
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
      const activity = this.ensureActivity();
      activity.appendChild(el);
      this.bumpActivityCount(activity);
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
    this.currentActivity = null;
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
      button.addEventListener("click", function () { self.selectPlan(index); });
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

  AgentChatWidget.prototype.renderQueue = function (queue) {
    if (!this.showQueue || !this.els.queue || !this.els.queueList) return;
    const self = this;
    this.els.queue.hidden = !queue.length && !this.queuePaused;
    this.els.queueList.innerHTML = "";
    queue.forEach(function (item, index) {
      const li = document.createElement("li");
      li.draggable = true;
      li.dataset.id = item.id;
      const queueText = item.message || imageSummary(item.images);
      li.innerHTML =
        '<span class="acw-queue-text">' +
        escapeHtml(queueText) +
        '</span><button type="button" class="ghost small" data-remove>✕</button>';
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
        if (Number.isNaN(from) || from === to) return;
        const ids = queue.map(function (q) { return q.id; });
        const moved = ids.splice(from, 1)[0];
        ids.splice(to, 0, moved);
        self.api("/sessions/" + self.sessionId + "/queue/reorder", {
          method: "POST",
          body: JSON.stringify({ prompt_ids: ids }),
        }).then(function () { self.refreshQueue(); });
      });
      self.els.queueList.appendChild(li);
    });
  };

  AgentChatWidget.prototype.refreshQueue = function () {
    const self = this;
    if (!this.sessionId) return;
    this.api("/sessions/" + this.sessionId).then(function (snap) {
      self.queuePaused = !!snap.queue_paused;
      self.renderQueue(snap.queue || []);
    }).catch(function () { /* ignore */ });
  };

  AgentChatWidget.prototype.renderModelsModes = function (snap) {
    if (this.showModel && this.els.model && this.els.modelWrap) {
      const models = (snap.models && (snap.models.availableModels || snap.models.available_models)) || [];
      const current = (snap.session && snap.session.model_id) ||
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
      .filter(Boolean).forEach(function (control) { control.disabled = pending; });
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

  AgentChatWidget.prototype.applySettings = function () {
    const self = this;
    if (!this.settingsDirty || !this.sessionId) return Promise.resolve();
    const snap = this.lastSnapshot || {};
    const currentModel = (snap.session && snap.session.model_id) || (snap.models && (snap.models.currentModelId || snap.models.current_model_id));
    const currentMode = (snap.session && snap.session.mode_id) || (snap.modes && (snap.modes.currentModeId || snap.modes.current_mode_id));
    const requests = [];
    if (this.els.model && this.els.model.value && this.els.model.value !== currentModel) {
      const modelId = this.els.model.value;
      requests.push(function () { return self.putOption("model", { model_id: modelId }); });
    }
    if (this.els.mode && this.els.mode.value && this.els.mode.value !== currentMode) {
      const modeId = this.els.mode.value;
      requests.push(function () { return self.putOption("mode", { mode_id: modeId }); });
    }
    if (this.els.config) {
      this.els.config.querySelectorAll("[data-acw-config-id]").forEach(function (input) {
        const original = input.dataset.acwOriginal;
        const value = input.type === "checkbox" ? input.checked : input.value;
        if (String(value) !== String(original)) {
          const configId = input.dataset.acwConfigId;
          requests.push(function () { return self.putOption("config", { config_id: configId, value: value }); });
        }
      });
    }
    if (!requests.length) {
      this.resetSettingsDraft();
      if (this.els.settingsStatus) this.els.settingsStatus.textContent = "No changes to apply.";
      return Promise.resolve();
    }
    this.setSettingsPending(true);
    const errors = [];
    return requests.reduce(function (promise, request) {
      return promise.then(function () {
        return request().catch(function (error) { errors.push(error); });
      });
    }, Promise.resolve())
      .then(function () { return self.api("/sessions/" + self.sessionId); })
      .then(function (fresh) {
        self.settingsDirty = false;
        self.applyOptionSnapshot(fresh);
        refreshSessionList(self.sessionId);
        if (errors.length) throw new Error(errors.map(function (error) { return error.message; }).join("; "));
        if (self.els.settingsStatus) {
          self.els.settingsStatus.classList.remove("is-error");
          self.els.settingsStatus.textContent = "Applied successfully.";
        }
      })
      .catch(function (error) {
        if (self.els.settingsStatus) {
          self.els.settingsStatus.classList.add("is-error");
          self.els.settingsStatus.textContent = "Could not apply settings: " + error.message;
        }
      })
      .finally(function () { self.setSettingsPending(false); });
  };

  AgentChatWidget.prototype.renderConfigOptions = function (snap) {
    if (!this.els.config) return;
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
        select.dataset.acwOriginal = current == null ? "" : String(current);
        select.addEventListener("change", function () { self.markSettingsDirty(); });
        wrap.appendChild(document.createTextNode(name + " "));
        wrap.appendChild(select);
      }
      self.els.config.appendChild(wrap);
    });
    if (this.settingsPending) {
      this.els.config.querySelectorAll("select,input").forEach(function (control) {
        control.disabled = true;
      });
    }
  };

  AgentChatWidget.prototype.switchSession = function (sessionId, live, ownerInstanceId, options) {
    if (!sessionId || (sessionId === this.sessionId && (!ownerInstanceId || ownerInstanceId === this.ownerInstanceId))) return;
    if (this.settingsDirty && !window.confirm("Discard unsaved Agent settings changes and switch sessions?")) return;
    if (this.settingsDirty) this.resetSettingsDraft();
    this.closeSSE("session-switched");
    this.lastSeq = 0;
    this.transcriptEvents = [];
    this.seenEvents = {};
    this.hasOlder = false;
    this.olderCursor = null;
    this.loadingOlder = false;
    this.olderError = "";
    this.updateOlderControl();
    this.streaming = {};
    this.lastSnapshot = null;
    this.renderTranscript([], { scrollBottom: false });
    this.setPlaceholder("Loading session…");
    this.openSession(sessionId, ownerInstanceId || "", options || {}).catch(function () {});
  };

  AgentChatWidget.prototype.setApiBase = function (apiBase, instanceId) {
    const next = String(apiBase || "/api/agent").replace(/\/$/, "");
    if (next === this.apiBase) return;
    this.closeSSE("api-base-changed");
    this.stopBrowserRefresh();
    this.apiBase = next;
    if (this.drafts) this.drafts.setInstance(instanceId);
    this.sessionId = "";
    this.root.dataset.apiBase = next;
    this.root.dataset.sessionId = "";
    this.lastSeq = 0;
    this.transcriptEvents = [];
    this.seenEvents = {};
    this.hasOlder = false;
    this.olderCursor = null;
    this.loadingOlder = false;
    this.olderError = "";
    this.updateOlderControl();
    this.streaming = {};
    this.lastSnapshot = null;
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
    if (this.els.send) this.els.send.textContent = on ? "Queue" : "Send";
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

  AgentChatWidget.prototype.setComposerEnabled = function (enabled) {
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
        : "This session has ended. Start or select another session.";
    }
  };

  AgentChatWidget.prototype._syncSubmissionControls = function () {
    const disabled = !this.composerEnabled || this.submissionPending || this.sessionClosed;
    if (this.els.send) {
      this.els.send.disabled = disabled;
      this.els.send.textContent = this.submissionPending
        ? (this.submissionState === "reconnecting" ? "Checking…" : "Sending…")
        : "Send";
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
  };

  AgentChatWidget.prototype.setSubmissionState = function (state, pending) {
    this.submissionState = state || "idle";
    this.submissionPending = !!pending;
    this._syncSubmissionControls();
  };

  AgentChatWidget.prototype.markSessionEnded = function (message) {
    this.sessionClosed = true;
    this.setTurnActive(false);
    this.setStatus("offline");
    this.setComposerEnabled(false);
    this.closeSSE("session-ended");
    this.addBubble("system", message || "Session ended.", new Date().toISOString(), {
      system: true,
      forceVisible: true,
    });
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
    if ((!text && !this.pendingImages.length) || !this.sessionId) return;
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
    this.setSubmissionState("sending", true);
    if (this.drafts) this.drafts.setStatus("Sending — waiting for durable acknowledgement…");
    this.api("/sessions/" + targetSessionId + "/prompt", {
      method: "POST",
      headers: { "Idempotency-Key": promptId },
      body: JSON.stringify({
        message: text,
        images: displayImages.map(function (image) {
          return { name: image.name, mime_type: image.mime_type, data: image.data };
        }),
        action: act,
        client_prompt_id: promptId,
      }),
    })
      .then(function (res) {
        if (!self._isCurrentSessionRequest(targetSessionId, generation)) return;
        if (!res || !res.accepted) {
          const error = new Error("PA could not confirm durable prompt acceptance.");
          error.acceptanceUnconfirmed = true;
          throw error;
        }
        if (!self._isDuplicateUserBubble(text)) {
          self.addBubble("user", text, new Date().toISOString(), { images: displayImages });
        }
        self.setTurnActive(true);
        self.scrollToBottom();
        if (self.drafts) {
          self.drafts.submissionAccepted({
            rawText: draftRawText,
            images: submittedImages,
            message: res.queued ? "Prompt queued." : "Prompt accepted.",
          });
        } else {
          if (self.els.input && self.els.input.value === draftRawText) self.els.input.value = "";
          self.clearPendingImages();
        }
        if (res.queued) self.refreshQueue();
      })
      .catch(function (err) {
        if (!self._isCurrentSessionRequest(targetSessionId, generation)) return;
        const code = apiErrorCode(err);
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
          self.resolveSessionNotLive(err, targetSessionId, generation);
          return;
        }
        const message = err.csrfRecoveryFailed
          ? "Security token changed and automatic recovery failed. Your draft is preserved; retry once or reload PA."
          : "Prompt not sent: " + err.message;
        self.addBubble("system", message, new Date().toISOString(), { system: true, forceVisible: true });
      })
      .finally(function () {
        self.setSubmissionState("idle", false);
      });
  };


  AgentChatWidget.prototype.cancel = function () {
    if (!this.sessionId) return;
    this.api("/sessions/" + this.sessionId + "/cancel", { method: "POST", body: "{}" });
  };

  AgentChatWidget.prototype.closeSession = function () {
    const self = this;
    if (!this.sessionId) return;
    this.api("/sessions/" + this.sessionId + "/close", { method: "POST", body: "{}" }).then(function () {
      if (self.drafts) self.drafts.clear(true, "Draft cleared because this session ended.");
      self.markSessionEnded("Session ended. Start or select another session to send more prompts.");
      refreshSessionList(null);
    }).catch(function (err) {
      self.addBubble("system", "Could not end session: " + err.message, new Date().toISOString(), { system: true, forceVisible: true });
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
      if (self.els.recoveryAction && action) self.els.recoveryAction.textContent = action;
      self.addBubble("system", "Recovery retry failed: " + err.message, new Date().toISOString(), { system: true, forceVisible: true });
    }).finally(function () {
      if (self.els.recoveryRetry) self.els.recoveryRetry.disabled = false;
    });
  };

  AgentChatWidget.prototype.restartSession = function () {
    const self = this;
    if (!this.sessionId) return;
    this.api("/sessions/" + this.sessionId + "/close", { method: "POST", body: "{}" })
      .then(function () {
        self.closeSSE("session-restarted");
        if (self.drafts) self.drafts.clear(true, "Draft cleared because this session ended.");
        if (self.drafts) self.drafts.switchSession("");
        self.sessionId = "";
        self.root.dataset.sessionId = "";
        self.lastSeq = 0;
        self.streaming = {};
        Object.keys(self.toolTimers).forEach(function (id) {
          const timer = self.toolTimers[id];
          if (timer && timer.interval) clearInterval(timer.interval);
        });
        self.toolTimers = {};
        self.setTurnActive(false);
        self.setPlaceholder("Restarting session…");
        self.init();
      })
      .catch(function (err) {
        self.addBubble("system", err.message, new Date().toISOString(), { system: true, forceVisible: true });
      });
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

  function openSessionCardDialog(widget) {
    const dialog = document.querySelector("[data-agent-card-dialog]");
    if (!dialog || !widget || !widget.sessionId) return;
    dialog._agentWidget = widget;
    const form = dialog.querySelector("[data-agent-card-form]");
    const error = dialog.querySelector("[data-agent-card-error]");
    if (form) form.reset();
    if (error) error.hidden = true;
    if (typeof dialog.showModal === "function") dialog.showModal();
    else dialog.setAttribute("open", "");
  }

  let sessionListRecovery = null;

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
    list.innerHTML = "";
    list.setAttribute("aria-busy", "false");
    if (!sessions || !sessions.length) {
      const empty = document.createElement("li");
      empty.className = "muted";
      empty.dataset.agentSessionEmpty = "1";
      empty.textContent = includeClosed
        ? "No matching session history."
        : "No live agent sessions yet.";
      list.appendChild(empty);
      return;
    }
    sessions.forEach(function (s) {
      const li = document.createElement("li");
      li.dataset.sessionId = s.id;
      li.dataset.sessionInstance = s.origin_instance_id || s.instance_id || "";
      li.dataset.sessionLive = s.live === false || s.status === "closed"
        ? "false"
        : "true";
      li.dataset.sessionRecoverable = s.recovery && s.recovery.recoverable ? "true" : "false";
      li.setAttribute("role", "button");
      li.tabIndex = 0;
      if (activeId && s.id === activeId) li.classList.add("active");
      const sessionInstanceId = s.origin_instance_id || s.instance_id || "";
      const sessionIdentity = sessionInstanceId && window.PAInstanceIdentity
        ? window.PAInstanceIdentity.html(sessionInstanceId)
        : "";
      const title = s.title || s.label || "Agent";
      const state = sessionListState(s);
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
        '<div class="agent-session-shell">' +
          '<span class="agent-session-provider-mark" aria-hidden="true">' +
            escapeHtml(String(s.agent_name || "PA").slice(0, 2).toUpperCase()) + "</span>" +
          '<div class="agent-session-summary">' +
            '<div class="agent-session-row-head">' +
              '<strong class="agent-session-title" data-agent-session-title data-full-title="' +
                escapeHtml(title) + '">' + escapeHtml(title) + "</strong>" +
              '<span class="agent-session-state agent-session-state-' + escapeHtml(state.key) + '">' +
                '<span aria-hidden="true">●</span> ' + escapeHtml(state.label) + "</span>" +
            "</div>" +
            '<span class="agent-session-title-tooltip" role="tooltip">' +
              escapeHtml(title) + "</span>" +
            '<div class="agent-session-context-line">' + contextParts.join("") + "</div>" +
            '<div class="agent-session-facts">' +
              '<span class="agent-session-instance" title="Execution instance">' +
                (sessionIdentity || escapeHtml(s.origin_instance_name || "Local instance")) + "</span>" +
              '<span class="agent-session-age" title="Elapsed time">' +
                escapeHtml(sessionElapsed(s.created_at)) + "</span>" +
              '<span class="agent-session-runtime" title="Provider and model">' +
                escapeHtml((s.agent_name || "Default provider") + " · " +
                  (s.model_id || "default model") + (s.mode_id ? " · " + s.mode_id : "")) +
                "</span>" +
              '<span class="agent-session-usage' + (usageParts.length ? "" : " muted") +
                '" title="Session usage">' +
                escapeHtml(usageParts.length ? usageParts.join(" · ") : "No usage yet") + "</span>" +
            "</div>" +
          "</div>" +
        "</div>" +
        '<div class="agent-session-related">' + related + "</div>" +
        '<details class="agent-session-details"><summary>Details</summary>' +
          '<div class="agent-session-details-body">' + sessionConfigSummary(s.config_json) +
            '<span class="muted small">Last activity <time datetime="' +
              escapeHtml(s.updated_at || "") + '">' + escapeHtml(sessionTimestamp(s.updated_at)) +
              "</time></span>" +
            '<a class="text-btn small" href="/knowledge?session=' + encodeURIComponent(s.id) +
              '">Promote conclusion to memory</a>' +
            '<span class="agent-session-actions"></span>' +
          "</div></details>";
      if (s.status !== "closed") {
        const close = document.createElement("button");
        close.type = "button";
        close.className = "ghost small danger agent-session-close";
        close.dataset.agentSessionClose = "1";
        close.textContent = s.live === false ? "Forget" : "Close";
        close.title = s.live === false
          ? "Forget this orphan so it is not retried"
          : "Close the live session";
        const actions = li.querySelector(".agent-session-actions");
        actions.appendChild(close);
      }
      list.appendChild(li);
    });
    updateSessionTitleTooltips(list);
    filterSessionList();
  }

  function cancelSessionListRecovery(scope, reason) {
    if (!sessionListRecovery) return;
    const list = sessionListRecovery.list;
    const containsList = scope === document || scope === list ||
      (scope && typeof scope.contains === "function" && scope.contains(list));
    if (!containsList) return;
    sessionListRecovery.controller.cancel(reason || "session-list-removed");
    sessionListRecovery = null;
  }

  function refreshSessionList(activeId) {
    const list = document.querySelector("[data-agent-session-list]");
    if (!list) return Promise.resolve(null);
    const toggle = document.querySelector("[data-agent-history-toggle]");
    const includeClosed = !!(toggle && toggle.getAttribute("aria-checked") === "true");
    const path = includeClosed ? "/history?limit=500" : "/sessions";
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
    return sessionListRecovery.controller.start(false);
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
      if (clamped) {
        item.setAttribute("aria-label", title.dataset.fullTitle || title.textContent || "Agent session");
      } else {
        item.removeAttribute("aria-label");
      }
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

  function sessionConfigSummary(config) {
    const admission = config && config.configuration;
    if (admission && (admission.requested || admission.effective)) {
      const requested = admission.requested || {};
      const effective = admission.effective || {};
      const requestedParts = [
        requested.model_id && ("model " + requested.model_id),
        requested.mode_id && ("mode " + requested.mode_id),
        requested.reasoning && ("reasoning " + requested.reasoning)
      ].filter(Boolean);
      const effectiveParts = [
        effective.model_id && ("model " + effective.model_id),
        effective.mode_id && ("mode " + effective.mode_id),
        effective.reasoning && ("reasoning " + effective.reasoning)
      ].filter(Boolean);
      const summary = [
        requestedParts.length && ("requested: " + requestedParts.join(", ")),
        effectiveParts.length && ("effective: " + effectiveParts.join(", ")),
        admission.state && ("settings status: " + admission.state)
      ].filter(Boolean).join("\n");
      if (summary) {
        return '<span class="muted small agent-session-config">' +
          escapeHtml(summary) + "</span>";
      }
    }
    const values = config && config.values;
    if (!values || !Object.keys(values).length) return "";
    return '<span class="muted small agent-session-config">' + escapeHtml(
      Object.keys(values).map(function (key) { return key + ": " + values[key]; }).join("\n")
    ) + "</span>";
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
    dialog.setAttribute("aria-busy", busy ? "true" : "false");
    if (form) form.setAttribute("aria-busy", busy ? "true" : "false");
    if (submit && submit.textContent !== "Starting…") submit.disabled = !!busy;
    if (status) {
      status.hidden = !busy;
      if (message) status.textContent = message;
    }
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
    const effortChoices = effort && (effort.options || effort.choices || effort.values);
    populateSelect(dialog.querySelector("[data-agent-new-effort]"), effortChoices && effortChoices.length ? effortChoices : [], ["value", "id"], "Provider default");
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

  function refreshNewSessionOptions(dialog, widget) {
    const provider = dialog.querySelector("[data-agent-new-provider]");
    const modelProvider = dialog.querySelector("[data-agent-new-model-provider]");
    const providerId = provider ? provider.value : "";
    const modelProviderId = modelProvider && !modelProvider.hidden && modelProvider.value
      ? modelProvider.value
      : "";
    const requestId = Number(dialog._acwOptionsRequest || 0) + 1;
    dialog._acwOptionsRequest = requestId;
    setNewSessionBusy(dialog, true, "Loading session options…");
    return newSessionSnapshotForProvider(widget, providerId, modelProviderId)
      .then(function (snap) {
        if (dialog._acwOptionsRequest === requestId) populateNewSessionOptions(dialog, snap);
      })
      .catch(function () {
        if (dialog._acwOptionsRequest === requestId) populateNewSessionOptions(dialog, null);
      })
      .finally(function () {
        if (dialog._acwOptionsRequest !== requestId) return;
        setNewSessionBusy(dialog, false);
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
    const list = root.querySelector("[data-agent-session-list]");
    if (list && !list._acwBound) {
      list._acwBound = true;
      list.addEventListener("click", function (e) {
        const li = e.target.closest("[data-session-id]");
        if (!li) return;
        if (e.target.closest("[data-agent-session-close]")) {
          e.preventDefault();
          e.stopPropagation();
          const sessionId = li.dataset.sessionId;
          const widget = document.querySelector("[data-agent-chat]");
          csrfFetch("/sessions/" + encodeURIComponent(sessionId) + "/close", {
            method: "POST",
            body: "{}",
          }).then(function () {
            if (widget && widget._acw && widget._acw.sessionId === sessionId) {
              widget._acw.markSessionEnded(
                "Session ended. Start or select another session to send more prompts."
              );
            }
            refreshSessionList(null);
          }).catch(function (err) {
            window.alert("Could not close session: " + err.message);
          });
          return;
        }
        if (e.target.closest("a, summary, details, button, pa-instance-identity")) return;
        const widget = document.querySelector("[data-agent-chat]");
        if (widget && widget._acw) {
          widget._acw.switchSession(
            li.dataset.sessionId,
            li.dataset.sessionLive !== "false",
            li.dataset.sessionInstance || ""
          );
        }
      });
      list.addEventListener("keydown", function (e) {
        if (e.key !== "Enter" && e.key !== " ") return;
        if (e.target.closest("a, summary, details, button, pa-instance-identity")) return;
        const li = e.target.closest("[data-session-id]");
        if (!li) return;
        e.preventDefault();
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
        prepareNewSessionDialog(dialog, widget);
        if (typeof dialog.showModal === "function") dialog.showModal();
        else dialog.setAttribute("open", "");
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
      const form = dialog.querySelector("[data-agent-new-form]");
      if (form) form.addEventListener("submit", function (event) {
        event.preventDefault();
        if (dialog.getAttribute("aria-busy") === "true") return;
        const data = new FormData(form);
        const body = {};
        ["title", "provider", "model_provider", "model_id", "mode_id", "effort", "cwd"].forEach(function (key) {
          const value = String(data.get(key) || "").trim();
          if (value) body[key] = value;
        });
        body.config = {};
        dialog.querySelectorAll("[data-agent-new-config]").forEach(function (select) {
          if (select.value) body.config[select.dataset.agentNewConfig] = select.value;
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
            refreshSessionList(sid);
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
              submit.textContent = "Start session";
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
      let collapsed = true;
      try {
        const saved = localStorage.getItem("pa-agent-sidebar-collapsed");
        collapsed = saved === null ? true : saved === "1";
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
    anchoredScrollTop: anchoredScrollTop,
    renderMarkdown: renderMarkdown,
    renderMarkdownAsync: renderMarkdownAsync,
  };

  document.addEventListener("DOMContentLoaded", function () {
    mountAll(document);
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
