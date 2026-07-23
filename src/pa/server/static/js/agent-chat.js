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

  function renderMarkdownAsync(text) {
    return ensureMarkdown().then(function () { return renderMarkdown(text); });
  }

  function csrfHeaders() {
    const meta = document.querySelector('meta[name="csrf-token"]');
    const headers = { "Content-Type": "application/json", Accept: "application/json" };
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

  function renderMarkdown(text) {
    const raw = String(text || "");
    if (window.marked && window.DOMPurify) {
      try {
        const html = window.marked.parse(raw, { breaks: true });
        const sanitized = window.DOMPurify.sanitize(html, {
          USE_PROFILES: { html: true },
          ADD_TAGS: ["audio", "iframe", "picture", "source", "track", "video"],
          ADD_ATTR: [
            "allow", "allowfullscreen", "controls", "loading", "poster", "preload",
            "referrerpolicy", "sandbox", "srcset"
          ],
          FORBID_TAGS: ["style", "form", "input", "button", "textarea", "select", "option"],
          FORBID_ATTR: ["style"]
        });
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

  function AgentChatWidget(root) {
    this.root = root;
    this.sessionId = root.dataset.sessionId || "";
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
      attachments: root.querySelector("[data-acw-attachments]"),
      attach: root.querySelector("[data-acw-attach]"),
      fileInput: root.querySelector("[data-acw-file-input]"),
      send: root.querySelector("[data-acw-send]"),
      stop: root.querySelector("[data-acw-stop]"),
      systemToggle: root.querySelector("[data-acw-toggle-system]"),
      rawToggle: root.querySelector("[data-acw-toggle-raw]"),
      working: root.querySelector("[data-acw-working]"),
      workingLabel: root.querySelector("[data-acw-working-label]"),
      turnTimer: root.querySelector("[data-acw-turn-timer]"),
      status: root.querySelector("[data-acw-status]"),
      title: root.querySelector("[data-acw-title]"),
      metrics: root.querySelector("[data-acw-metrics]"),
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
    this.turnActive = false;
    this.rawText = false;
    this.pendingImages = [];
    this.browserAttached = false;
    this.browserVisible = false;
    this.browserDeviceScaleFactor = 1;
    this.browserRefreshId = null;

    this._bind();
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
        if (e.key === "Enter" && !e.shiftKey) {
          e.preventDefault();
          self.send(self.prompting ? "append" : "append");
        }
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
    return fetch(this.apiBase + path, Object.assign({
      headers: csrfHeaders(),
      credentials: "same-origin",
    }, opts)).then(function (res) {
      if (!res.ok) {
        return res.json().catch(function () { return {}; }).then(function (body) {
          throw new Error(body.detail || res.statusText || "Request failed");
        });
      }
      if (res.status === 204) return null;
      return res.json();
    });
  };

  AgentChatWidget.prototype.init = function () {
    const self = this;
    const body = {
      attach_default: this.createLabel === "default" && !this.cardId,
      label: this.createLabel,
      card_id: this.cardId || null,
      title: this.cardId ? "Card agent" : null,
    };
    if (this.preferredProvider) body.provider = this.preferredProvider;
    const boot = this.sessionId
      ? Promise.resolve({ session: { id: this.sessionId } })
      : this.api("/sessions", {
          method: "POST",
          body: JSON.stringify(body),
        });

    boot
      .then(function (snap) {
        const sid = (snap.session && snap.session.id) || (snap.id) || self.sessionId;
        self.sessionId = sid;
        self.root.dataset.sessionId = sid;
        return self.api("/sessions/" + sid);
      })
      .then(function (snap) {
        self.applySnapshot(snap);
        self.connectSSE();
        self.refreshBrowserState();
      })
      .catch(function (err) {
        self.setPlaceholder("Failed to start session: " + err.message);
        self.setStatus("error");
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
    this.lastSnapshot = snap;
    const session = snap.session || {};
    if (this.els.title) {
      this.els.title.textContent = session.title || session.label || "Agent";
    }
    this.queuePaused = !!snap.queue_paused;
    this.hasOlder = !!(snap.transcript_page && snap.transcript_page.has_older);
    this.olderCursor = snap.transcript_page && (
      snap.transcript_page.next_before_seq || snap.transcript_page.oldest_seq
    );
    this.olderError = "";
    this.renderTranscript(snap.transcript || [], { scrollBottom: true });
    // Transcript replay includes historical turn-completed events. Apply the live
    // snapshot state afterward so replay cannot reset an active turn's timer.
    this.setTurnActive(!!snap.prompting, snap.turn_started_at);
    this.setStatus(this.prompting ? "working" : snap.connected ? "online" : "offline");
    this.renderQueue(snap.queue || []);
    this.renderModelsModes(snap);
    this.renderConfigOptions(snap);
    this.renderMetrics(snap.metrics || session.metrics_json || {});
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
    if (this.es) {
      this.es.close();
      this.es = null;
    }
    if (!this.sessionId) return;
    const url = this.apiBase + "/sessions/" + this.sessionId + "/events?after=" + this.lastSeq;
    const es = new EventSource(url);
    this.es = es;

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
      self.setStatus("offline");
    };
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
        if (!replay) {
          this.api("/sessions/" + this.sessionId).then(function (snap) {
            self.applyOptionSnapshot(snap);
          }).catch(function () { /* ignore */ });
        }
        break;
      case "session_closed":
        this.setTurnActive(false);
        this.setStatus("offline");
        this.setPlaceholder("Session ended.");
        if (this.es) this.es.close();
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

  AgentChatWidget.prototype.switchSession = function (sessionId) {
    if (!sessionId || sessionId === this.sessionId) return;
    if (this.settingsDirty && !window.confirm("Discard unsaved Agent settings changes and switch sessions?")) return;
    if (this.settingsDirty) this.resetSettingsDraft();
    if (this.es) {
      this.es.close();
      this.es = null;
    }
    this.sessionId = sessionId;
    this.root.dataset.sessionId = sessionId;
    this.lastSeq = 0;
    this.transcriptEvents = [];
    this.seenEvents = {};
    this.hasOlder = false;
    this.olderCursor = null;
    this.loadingOlder = false;
    this.olderError = "";
    this.updateOlderControl();
    this.streaming = {};
    this.setPlaceholder("Loading session…");
    const self = this;
    this.api("/sessions/" + sessionId)
      .then(function (snap) {
        self.applySnapshot(snap);
        self.connectSSE();
      })
      .catch(function (err) {
        self.setPlaceholder("Failed to load session: " + err.message);
        self.setStatus("error");
      });
  };

  AgentChatWidget.prototype.setApiBase = function (apiBase) {
    const next = String(apiBase || "/api/agent").replace(/\/$/, "");
    if (next === this.apiBase) return;
    if (this.es) {
      this.es.close();
      this.es = null;
    }
    this.stopBrowserRefresh();
    this.apiBase = next;
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

  AgentChatWidget.prototype.scrollToBottom = function () {
    if (this.els.messages) this.els.messages.scrollTop = this.els.messages.scrollHeight;
  };

  AgentChatWidget.prototype.isNearBottom = function () {
    if (!this.els.messages) return true;
    const distance = this.els.messages.scrollHeight - this.els.messages.scrollTop - this.els.messages.clientHeight;
    return distance <= 48;
  };

  AgentChatWidget.prototype.send = function (action) {
    const self = this;
    const text = (this.els.input && this.els.input.value || "").trim();
    if ((!text && !this.pendingImages.length) || !this.sessionId) return;
    if (this.pendingImages.some(function (image) { return !image.data; })) {
      this.addBubble("system", "Please wait for the images to finish loading.", new Date().toISOString(), { system: true, forceVisible: true });
      return;
    }
    const act = action || "append";
    const images = this.pendingImages.map(function (image) {
      return {
        name: image.name,
        mime_type: image.mime_type,
        data: image.data,
        preview: "data:" + image.mime_type + ";base64," + image.data,
      };
    });
    this.els.input.value = "";
    this.clearPendingImages();
    // Optimistic user bubble; SSE user_message may also arrive — dedupe by text+recency.
    this.addBubble("user", text, new Date().toISOString(), { images: images });
    this.scrollToBottom();
    this.api("/sessions/" + this.sessionId + "/prompt", {
      method: "POST",
      body: JSON.stringify({
        message: text,
        images: images.map(function (image) {
          return { name: image.name, mime_type: image.mime_type, data: image.data };
        }),
        action: act,
      }),
    })
      .then(function (res) {
        if (res && res.queued) self.refreshQueue();
      })
      .catch(function (err) {
        self.addBubble("system", err.message, new Date().toISOString(), { system: true });
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
      if (self.es) self.es.close();
      self.setTurnActive(false);
      self.setStatus("offline");
      self.setPlaceholder("Session ended.");
      refreshSessionList(null);
    });
  };

  AgentChatWidget.prototype.restartSession = function () {
    const self = this;
    if (!this.sessionId) return;
    this.api("/sessions/" + this.sessionId + "/close", { method: "POST", body: "{}" })
      .then(function () {
        if (self.es) self.es.close();
        self.es = null;
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
          throw new Error(body.detail || res.statusText || "Request failed");
        });
      }
      return res.json();
    });
  }

  function refreshSessionList(activeId) {
    const list = document.querySelector("[data-agent-session-list]");
    if (!list) return;
    csrfFetch("/sessions")
      .then(function (sessions) {
        list.innerHTML = "";
        if (!sessions || !sessions.length) {
          const empty = document.createElement("li");
          empty.className = "muted";
          empty.dataset.agentSessionEmpty = "1";
          empty.textContent = "No live agent sessions yet.";
          list.appendChild(empty);
          return;
        }
        sessions.forEach(function (s) {
          const li = document.createElement("li");
          li.dataset.sessionId = s.id;
          li.setAttribute("role", "button");
          li.tabIndex = 0;
          if (activeId && s.id === activeId) li.classList.add("active");
          li.innerHTML =
            "<strong>" + escapeHtml(s.title || s.label || "Agent") + "</strong>" +
            '<span class="muted">' + escapeHtml(s.status || "") + "</span>" +
            '<span class="muted agent-session-runtime">' + escapeHtml(
              [s.agent_name, s.model_id, s.mode_id].filter(Boolean).join(" · ")
            ) + "</span>" +
            sessionConfigSummary(s.config_json);
          list.appendChild(li);
        });
      })
      .catch(function () { /* ignore */ });
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
        admission.state && ("state: " + admission.state)
      ].filter(Boolean).join(" · ");
      if (summary) {
        return '<span class="muted small agent-session-config">' +
          escapeHtml(summary) + "</span>";
      }
    }
    const values = config && config.values;
    if (!values || !Object.keys(values).length) return "";
    return '<span class="muted small agent-session-config">' + escapeHtml(
      Object.keys(values).map(function (key) { return key + ": " + values[key]; }).join(" · ")
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

  function populateNewSessionOptions(dialog, snap) {
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
    populateSelect(dialog.querySelector("[data-agent-new-effort]"), effortChoices && effortChoices.length ? effortChoices : ["low", "medium", "high", "xhigh"], ["value", "id"], "Provider default");
    const related = dialog.querySelector("[data-agent-new-related]");
    if (!related) return;
    related.innerHTML = "";
    raw.filter(function (option) { return option && option !== effort; }).forEach(function (option) {
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

  function newSessionSnapshotForProvider(widget, providerId) {
    const snap = widget && widget._acw && widget._acw.lastSnapshot;
    const activeProvider = snap && snap.session && snap.session.agent_name;
    if (providerId && providerId === activeProvider) return Promise.resolve(snap);
    if (!providerId) return Promise.resolve(null);
    return csrfFetch("/provider-options/" + encodeURIComponent(providerId));
  }

  function refreshNewSessionOptions(dialog, widget) {
    const provider = dialog.querySelector("[data-agent-new-provider]");
    const providerId = provider ? provider.value : "";
    const requestId = Number(dialog._acwOptionsRequest || 0) + 1;
    dialog._acwOptionsRequest = requestId;
    dialog.querySelectorAll("[data-agent-new-model], [data-agent-new-mode], [data-agent-new-effort]").forEach(function (select) {
      select.disabled = true;
    });
    return newSessionSnapshotForProvider(widget, providerId)
      .then(function (snap) {
        if (dialog._acwOptionsRequest === requestId) populateNewSessionOptions(dialog, snap);
      })
      .catch(function () {
        if (dialog._acwOptionsRequest === requestId) populateNewSessionOptions(dialog, null);
      })
      .finally(function () {
        if (dialog._acwOptionsRequest !== requestId) return;
        dialog.querySelectorAll("[data-agent-new-model], [data-agent-new-mode], [data-agent-new-effort]").forEach(function (select) {
          select.disabled = false;
        });
      });
  }

  function applyNewSessionDefaults(dialog, defaults) {
    defaults = defaults || {};
    const values = {
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
    const provider = dialog.querySelector("[data-agent-new-provider]");
    const snap = widget && widget._acw && widget._acw.lastSnapshot;
    const activeProvider = snap && snap.session && snap.session.agent_name;
    populateNewSessionOptions(dialog, snap);
    return Promise.all([csrfFetch("/providers"), csrfFetch("/preferences")])
      .then(function (results) {
        const providers = results[0];
        const prefs = results[1] || {};
        const userSurfaces = prefs.user && prefs.user.agent_surfaces || {};
        const globalSurfaces = prefs.global && prefs.global.agent_surfaces || {};
        const userDefaults = userSurfaces["chat.default"] || {};
        const globalDefaults = globalSurfaces["chat.default"] || {};
        const defaults = {
          provider: userDefaults.provider || globalDefaults.provider || prefs.agent_provider || activeProvider || "",
          model_id: userDefaults.model_id || globalDefaults.model_id || "",
          mode_id: userDefaults.mode_id || globalDefaults.mode_id || "",
          effort: userDefaults.effort || globalDefaults.effort || "",
          config: Object.assign({}, globalDefaults.config || {}, userDefaults.config || {}),
        };
        if (!provider) return;
        provider.innerHTML = '<option value="">Instance default</option>';
        (providers || []).forEach(function (item) {
          if (!item || !item.id || item.available === false) return;
          const option = document.createElement("option");
          option.value = item.id;
          option.textContent = item.display_name || item.id;
          if (item.id === defaults.provider) option.selected = true;
          provider.appendChild(option);
        });
        if (!provider.value && defaults.provider) provider.value = defaults.provider;
        return refreshNewSessionOptions(dialog, widget).then(function () {
          applyNewSessionDefaults(dialog, defaults);
        });
      })
      .catch(function () { /* the instance default remains available */ });
  }

  function bindSessionSidebar(scope) {
    const root = scope || document;
    const list = root.querySelector("[data-agent-session-list]");
    if (list && !list._acwBound) {
      list._acwBound = true;
      list.addEventListener("click", function (e) {
        const li = e.target.closest("[data-session-id]");
        if (!li) return;
        const widget = document.querySelector("[data-agent-chat]");
        if (widget && widget._acw) widget._acw.switchSession(li.dataset.sessionId);
      });
      list.addEventListener("keydown", function (e) {
        if (e.key !== "Enter" && e.key !== " ") return;
        const li = e.target.closest("[data-session-id]");
        if (!li) return;
        e.preventDefault();
        const widget = document.querySelector("[data-agent-chat]");
        if (widget && widget._acw) widget._acw.switchSession(li.dataset.sessionId);
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
          dialog.querySelectorAll("[data-agent-new-model], [data-agent-new-mode], [data-agent-new-effort]").forEach(function (select) {
            select.value = "";
          });
          refreshNewSessionOptions(dialog, document.querySelector("[data-agent-chat]"));
        });
      }
      dialog.querySelectorAll("[data-agent-new-cancel]").forEach(function (button) {
        button.addEventListener("click", function () { dialog.close(); });
      });
      const form = dialog.querySelector("[data-agent-new-form]");
      if (form) form.addEventListener("submit", function (event) {
        event.preventDefault();
        const data = new FormData(form);
        const body = {};
        ["title", "provider", "model_id", "mode_id", "effort", "cwd"].forEach(function (key) {
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
  }

  window.PAAgentChat = {
    mount: mountAll,
    AgentChatWidget: AgentChatWidget,
    refreshSessionList: refreshSessionList,
    anchoredScrollTop: anchoredScrollTop,
    renderMarkdown: renderMarkdown,
    renderMarkdownAsync: renderMarkdownAsync,
  };

  document.addEventListener("DOMContentLoaded", function () {
    mountAll(document);
  });
  // HTMX 4 uses colon-separated event names (htmx:after:swap).
  ["htmx:after:swap", "htmx:afterSwap"].forEach(function (evt) {
    document.body && document.body.addEventListener(evt, function (e) {
      const target = (e.detail && (e.detail.target || (e.detail.ctx && e.detail.ctx.target))) || e.target;
      mountAll(target || document);
    });
  });
})();
