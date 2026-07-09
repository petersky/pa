(function () {
  var VERSION_POLL_MS = 45000;

  function normalizePath(path) {
    if (!path) return "/";
    var clean = String(path).split("?")[0].split("#")[0];
    if (clean.length > 1 && clean.endsWith("/")) {
      clean = clean.slice(0, -1);
    }
    return clean || "/";
  }

  function setActiveNav(path) {
    var current = normalizePath(path || window.location.pathname);
    document.querySelectorAll(".nav-btn").forEach(function (btn) {
      btn.classList.toggle("active", normalizePath(btn.getAttribute("href")) === current);
    });
    document.querySelectorAll(".icon-btn[href]").forEach(function (btn) {
      var href = normalizePath(btn.getAttribute("href"));
      if (href === "/settings" || href === "/agent") {
        btn.classList.toggle("active", href === current);
      }
    });
  }

  function swapTarget(event) {
    return (event.detail && event.detail.ctx && event.detail.ctx.target) ||
      (event.detail && event.detail.target) ||
      null;
  }

  function updateTitle() {
    const active = document.querySelector(".nav-btn.active span:last-child");
    const instance = document.querySelector(".instance-indicator");
    if (active && instance) {
      const name = instance.getAttribute("title") || "PA";
      const label = name.replace(/^Instance:\s*/, "");
      document.title = active.textContent.trim() + " — " + label;
    }
  }

  function showToast(message, kind) {
    let toast = document.getElementById("pa-toast");
    if (!toast) {
      toast = document.createElement("div");
      toast.id = "pa-toast";
      toast.className = "pa-toast";
      document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.dataset.kind = kind || "error";
    toast.classList.add("visible");
    window.clearTimeout(showToast._timer);
    showToast._timer = window.setTimeout(function () {
      toast.classList.remove("visible");
    }, 4000);
  }

  function csrfHeader() {
    var meta = document.querySelector('meta[name="csrf-token"]');
    return meta && meta.content ? { "X-CSRF-Token": meta.content } : {};
  }

  function reloadWithCacheBust() {
    var url = new URL(window.location.href);
    url.searchParams.set("_cb", String(Date.now()));
    window.location.replace(url.toString());
  }

  function showUpdateBanner() {
    var banner = document.getElementById("pa-update-banner");
    if (!banner || !banner.classList.contains("hidden")) {
      return;
    }
    banner.classList.remove("hidden");
    var btn = document.getElementById("pa-update-refresh");
    if (btn && !btn.dataset.bound) {
      btn.dataset.bound = "1";
      btn.addEventListener("click", reloadWithCacheBust);
    }
  }

  function checkServerBuild() {
    var current = window.PA_BUILD;
    if (!current) return;
    fetch("/api/ui/assets", { cache: "no-store" })
      .then(function (resp) {
        if (!resp.ok) return null;
        return resp.json();
      })
      .then(function (data) {
        if (!data || !data.build_id || data.build_id === current) return;
        showUpdateBanner();
      })
      .catch(function () {});
  }

  function initBoardDragDrop(root) {
    var scope = root || document;
    scope.querySelectorAll(".board-column").forEach(function (col) {
      if (col.dataset.dndBound) return;
      col.dataset.dndBound = "1";
      var lane = col.dataset.lane;
      var body = col.querySelector(".board-column-body");
      if (!body || !lane) return;

      body.addEventListener("dragover", function (event) {
        event.preventDefault();
        if (event.dataTransfer) event.dataTransfer.dropEffect = "move";
        col.classList.add("drag-over");
      });
      body.addEventListener("dragleave", function (event) {
        if (!col.contains(event.relatedTarget)) {
          col.classList.remove("drag-over");
        }
      });
      body.addEventListener("drop", function (event) {
        event.preventDefault();
        col.classList.remove("drag-over");
        var cardId = event.dataTransfer && event.dataTransfer.getData("text/pa-card-id");
        var realm = event.dataTransfer && event.dataTransfer.getData("text/pa-realm");
        var fromLane = event.dataTransfer && event.dataTransfer.getData("text/pa-lane");
        if (!cardId || !realm || fromLane === lane) return;

        var bodyParams = new URLSearchParams({ lane: lane });
        fetch("/partials/cards/" + encodeURIComponent(cardId) + "/move?realm=" + encodeURIComponent(realm), {
          method: "POST",
          headers: Object.assign({ "Content-Type": "application/x-www-form-urlencoded" }, csrfHeader()),
          body: bodyParams.toString(),
        })
          .then(function (resp) {
            if (!resp.ok) throw new Error("Move failed");
            document.body.dispatchEvent(new CustomEvent("boardRefresh"));
          })
          .catch(function () {
            showToast("Could not move card", "error");
          });
      });
    });

    scope.querySelectorAll(".item-list .item[draggable]").forEach(function (item) {
      if (item.dataset.dndBound) return;
      item.dataset.dndBound = "1";
      item.addEventListener("dragstart", function (event) {
        var cardId = item.dataset.cardId;
        var realm = item.dataset.realm;
        var laneEl = item.closest(".board-column");
        var lane = laneEl && laneEl.dataset.lane;
        if (!cardId || !realm || !event.dataTransfer) return;
        event.dataTransfer.setData("text/pa-card-id", cardId);
        event.dataTransfer.setData("text/pa-realm", realm);
        event.dataTransfer.setData("text/pa-lane", lane || "");
        event.dataTransfer.effectAllowed = "move";
        item.classList.add("dragging");
      });
      item.addEventListener("dragend", function () {
        item.classList.remove("dragging");
        document.querySelectorAll(".board-column.drag-over").forEach(function (col) {
          col.classList.remove("drag-over");
        });
      });
    });
  }

  function initAgentReconnect() {
    document.querySelectorAll("#pa-agent-reconnect").forEach(function (btn) {
      if (btn.dataset.bound) return;
      btn.dataset.bound = "1";
      btn.addEventListener("click", function () {
        btn.disabled = true;
        fetch("/api/agent/reconnect", {
          method: "POST",
          headers: csrfHeader(),
        })
          .then(function (resp) {
            return resp.json().then(function (data) {
              if (!resp.ok) throw new Error(data.detail || "Reconnect failed");
              return data;
            });
          })
          .then(function (data) {
            if (data.connected) {
              reloadWithCacheBust();
              return;
            }
            showToast(data.error || "Agent still offline", "error");
          })
          .catch(function (err) {
            showToast(err.message || "Reconnect failed", "error");
          })
          .finally(function () {
            btn.disabled = false;
          });
      });
    });
  }

  document.body.addEventListener("htmx:config:request", function (event) {
    var headers = csrfHeader();
    var ctx = event.detail && event.detail.ctx;
    var target = (ctx && ctx.request && ctx.request.headers) ||
      (event.detail && event.detail.headers);
    if (!target) return;
    Object.keys(headers).forEach(function (key) {
      target[key] = headers[key];
    });
  });

  document.body.addEventListener("htmx:after:swap", function (event) {
    var target = swapTarget(event);
    if (target && target.id === "app-view") {
      setActiveNav(window.location.pathname);
      updateTitle();
      initBoardDragDrop(target);
      initAgentReconnect();
    }
    if (target && target.classList.contains("board-column-body")) {
      initBoardDragDrop(target.closest(".board-grid") || document);
    }
    if (target && target.id === "agent-messages") {
      const placeholder = document.querySelector(".chat-placeholder");
      if (placeholder) placeholder.remove();
    }
  });

  document.body.addEventListener("htmx:after:history:update", function () {
    setActiveNav(window.location.pathname);
    updateTitle();
  });

  document.body.addEventListener("htmx:response:error", function (event) {
    var ctx = event.detail && event.detail.ctx;
    var message = "Request failed";
    var text = ctx && ctx.text;
    var statusText = ctx && ctx.response && ctx.response.raw && ctx.response.raw.statusText;
    if (text) {
      try {
        const data = JSON.parse(text);
        message = data.detail || data.message || message;
        if (Array.isArray(message)) {
          message = message.map(function (item) {
            return item.msg || String(item);
          }).join("; ");
        }
      } catch (_err) {
        message = statusText || message;
      }
    } else if (statusText) {
      message = statusText;
    }
    showToast(message, "error");
  });

  window.addEventListener("popstate", function () {
    if (typeof htmx === "undefined") return;
    htmx.ajax("GET", window.location.pathname, {
      target: "#app-view",
      swap: "innerHTML",
    });
    setActiveNav(window.location.pathname);
    updateTitle();
  });

  document.addEventListener("DOMContentLoaded", function () {
    setActiveNav(window.location.pathname);
    updateTitle();
    initBoardDragDrop(document);
    initAgentReconnect();
    checkServerBuild();
    window.setInterval(checkServerBuild, VERSION_POLL_MS);
  });
})();
