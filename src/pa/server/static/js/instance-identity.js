(function () {
  if (window.PAInstanceIdentity) return;

  var identities = Object.create(null);
  var refreshPromise = null;
  var lastRefresh = 0;

  function shortId(value) {
    var id = String(value || "").trim();
    return id.length >= 32 ? id.slice(0, 8) : id.slice(0, 12);
  }

  function normalize(items) {
    var rows = (items || []).map(function (item) {
      return {
        id: String(item.id || item.instance_id || "").trim(),
        name: String(item.name || "").trim(),
      };
    }).filter(function (item) { return item.id; });
    var counts = Object.create(null);
    rows.forEach(function (item) {
      if (!item.name) return;
      var key = item.name.toLocaleLowerCase();
      counts[key] = (counts[key] || 0) + 1;
    });
    var next = Object.create(null);
    rows.forEach(function (item) {
      var duplicate = !!(item.name && counts[item.name.toLocaleLowerCase()] > 1);
      next[item.id] = {
        id: item.id,
        name: item.name || "Unknown instance",
        displayName: item.name
          ? item.name + (duplicate ? " · " + shortId(item.id) : "")
          : "Unknown instance · " + shortId(item.id),
        duplicate: duplicate,
        known: !!item.name,
      };
    });
    identities = next;
    updateElements();
    updateSelections(document);
    var localId = document.documentElement.dataset.paInstanceId || "";
    var brand = document.querySelector("[data-pa-instance-name]");
    if (brand && localId) brand.dataset.paInstanceName = resolve(localId).displayName;
  }

  function initialDirectory() {
    var node = document.getElementById && document.getElementById("pa-instance-identities");
    if (!node) return [];
    try { return JSON.parse(node.textContent || "[]"); } catch (_error) { return []; }
  }

  function resolve(instanceId) {
    var id = String(instanceId || "").trim();
    return identities[id] || {
      id: id,
      name: "Unknown instance",
      displayName: id ? "Unknown instance · " + shortId(id) : "Unknown instance",
      duplicate: false,
      known: false,
    };
  }

  function refresh(force) {
    if (typeof window.fetch !== "function") return Promise.resolve(identities);
    var now = Date.now();
    if (!force && now - lastRefresh < 1000) return Promise.resolve(identities);
    if (refreshPromise) return refreshPromise;
    refreshPromise = window.fetch("/api/fleet/instances", {
      credentials: "same-origin",
      cache: "no-store",
      headers: { Accept: "application/json" },
    }).then(function (response) {
      if (!response.ok) throw new Error("Could not refresh fleet instance names");
      return response.json();
    }).then(function (items) {
      normalize(rows);
      lastRefresh = Date.now();
      return identities;
    }).catch(function () {
      return identities;
    }).finally(function () {
      refreshPromise = null;
    });
    return refreshPromise;
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function html(instanceId, className) {
    var id = String(instanceId || "").trim();
    if (!id) return '<span class="muted">Unknown instance</span>';
    return "<pa-instance-identity instance-id=\"" + escapeHtml(id) + "\"" +
      (className ? " class=\"" + escapeHtml(className) + "\"" : "") +
      "></pa-instance-identity>";
  }

  function copyText(value) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(value);
    }
    return new Promise(function (accept, reject) {
      var input = document.createElement("textarea");
      input.value = value;
      input.setAttribute("readonly", "");
      input.className = "instance-identity-copy-source";
      document.body.appendChild(input);
      input.select();
      try {
        if (!document.execCommand || !document.execCommand("copy")) {
          throw new Error("Copy is unavailable");
        }
        accept();
      } catch (error) {
        reject(error);
      } finally {
        input.remove();
      }
    });
  }

  function updateElements() {
    if (!document.querySelectorAll) return;
    document.querySelectorAll("pa-instance-identity").forEach(function (element) {
      if (typeof element.render === "function") element.render();
    });
  }

  function updateSelections(root) {
    if (!root || !root.querySelectorAll) return;
    root.querySelectorAll("[data-instance-identity-select]").forEach(function (select) {
      var container = select.parentElement;
      var target = container && container.querySelector("[data-instance-identity-selection]");
      if (!target && container && container.parentElement) {
        target = container.parentElement.querySelector("[data-instance-identity-selection]");
      }
      if (!target) return;
      var render = function () {
        target.innerHTML = select.value
          ? html(select.value, "instance-identity-selection-value")
          : "";
        target.hidden = !select.value;
      };
      if (!select.dataset.instanceIdentityBound) {
        select.dataset.instanceIdentityBound = "true";
        select.addEventListener("change", render);
      }
      render();
    });
  }

  function InstanceIdentity() {
    return Reflect.construct(HTMLElement, [], InstanceIdentity);
  }

  if (typeof window.HTMLElement === "function" && window.customElements) {
    InstanceIdentity.prototype = Object.create(HTMLElement.prototype);
    InstanceIdentity.prototype.constructor = InstanceIdentity;
    Object.setPrototypeOf(InstanceIdentity, HTMLElement);
    Object.defineProperty(InstanceIdentity, "observedAttributes", {
      get: function () { return ["instance-id"]; },
    });
    InstanceIdentity.prototype.connectedCallback = function () {
      this.render();
      refresh(false);
    };
    InstanceIdentity.prototype.attributeChangedCallback = function () {
      if (this.isConnected) this.render();
    };
    InstanceIdentity.prototype.render = function () {
      var element = this;
      var instanceId = this.getAttribute("instance-id") || "";
      var identity = resolve(instanceId);
      var feedback = this.querySelector("[data-instance-copy-feedback]");
      var previousFeedback = feedback && feedback.textContent || "";
      this.title = instanceId;
      this.innerHTML =
        '<span class="instance-identity-name" title="' + escapeHtml(instanceId) + '">' +
        escapeHtml(identity.displayName) + "</span>" +
        '<button type="button" class="instance-identity-copy" aria-label="Copy instance ID" ' +
        'title="Copy full instance ID">' +
        '<svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">' +
        '<path d="M5 2.5h7.5v8H11V4H5V2.5Zm-2 3h6.5v8H3v-8Zm1.5 1.5v5h3.5V7H4.5Z"></path>' +
        "</svg></button>" +
        '<span class="instance-identity-feedback" data-instance-copy-feedback ' +
        'role="status" aria-live="polite">' + escapeHtml(previousFeedback) + "</span>";
      var button = this.querySelector("button");
      button.addEventListener("keydown", function (event) {
        event.stopPropagation();
      });
      button.addEventListener("click", function (event) {
        event.stopPropagation();
        event.preventDefault();
        copyText(instanceId).then(function () {
          element.setFeedback("Copied");
        }).catch(function () {
          element.setFeedback("Copy failed");
        });
      });
    };
    InstanceIdentity.prototype.setFeedback = function (message) {
      var feedback = this.querySelector("[data-instance-copy-feedback]");
      if (!feedback) return;
      feedback.textContent = message;
      window.clearTimeout(this._feedbackTimer);
      this._feedbackTimer = window.setTimeout(function () {
        if (feedback.isConnected) feedback.textContent = "";
      }, 1800);
    };
    if (!window.customElements.get("pa-instance-identity")) {
      window.customElements.define("pa-instance-identity", InstanceIdentity);
    }
  }

  window.PAInstanceIdentity = {
    copyText: copyText,
    html: html,
    resolve: resolve,
    refresh: refresh,
    setDirectory: normalize,
    shortId: shortId,
  };

  normalize(initialDirectory());
  if (document.addEventListener) {
    document.addEventListener("htmx:afterSwap", function (event) {
      updateSelections(event.detail && event.detail.target || document);
      refresh(false);
    });
    document.addEventListener("htmx:historyRestore", function () { refresh(true); });
    document.addEventListener("visibilitychange", function () {
      if (document.visibilityState === "visible") refresh(true);
    });
  }
  if (window.addEventListener) window.addEventListener("online", function () { refresh(true); });
  if (typeof window.fetch === "function") {
    window.setInterval(function () {
      if (!document.hidden && document.querySelector("pa-instance-identity")) refresh(true);
    }, 30000);
  }
})();
