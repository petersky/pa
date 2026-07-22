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

  function moveCard(card, lane) {
    if (!card || !lane || card.dataset.cardLane === lane) return Promise.resolve();
    var cardId = card.dataset.cardId;
    var realm = card.dataset.realm;
    if (!cardId || !realm) return Promise.reject(new Error("Card context is missing"));

    var originalParent = card.parentNode;
    var originalNext = card.nextSibling;
    var originalLane = card.dataset.cardLane || "";
    var targetColumn = document.querySelector('.board-column[data-lane="' + CSS.escape(lane) + '"]');
    var targetBody = targetColumn && targetColumn.querySelector(".board-column-body");
    if (targetBody) {
      var targetList = targetBody.querySelector(".compact-card-list");
      if (!targetList) {
        targetList = document.createElement("div");
        targetList.className = "compact-card-list";
        targetBody.replaceChildren(targetList);
      }
      targetList.appendChild(card);
      card.dataset.cardLane = lane;
      card.classList.add("is-moving");
    }

    var bodyParams = new URLSearchParams({ lane: lane });
    return fetch("/partials/cards/" + encodeURIComponent(cardId) + "/move?realm=" + encodeURIComponent(realm), {
      method: "POST",
      credentials: "same-origin",
      headers: Object.assign({ "Content-Type": "application/x-www-form-urlencoded" }, csrfHeader()),
      body: bodyParams.toString(),
    })
      .then(function (resp) {
        if (!resp.ok) throw new Error("Move failed");
        document.body.dispatchEvent(new CustomEvent("boardRefresh"));
      })
      .catch(function (error) {
        card.dataset.cardLane = originalLane;
        if (originalParent) originalParent.insertBefore(card, originalNext);
        showToast("Could not move card. Its original lane was restored.", "error");
        throw error;
      })
      .finally(function () {
        card.classList.remove("is-moving");
      });
  }

  function filesystemTarget(href) {
    var raw = String(href || "");
    var path = "";
    if (raw.indexOf("file:///") === 0) {
      try { path = decodeURIComponent(new URL(raw).pathname); } catch (_error) { return null; }
    } else if (/^\/(Users|home|tmp|private|workspace|mnt|opt|var)(\/|$)/.test(raw)) {
      try { path = decodeURIComponent(raw.split(/[?#]/, 1)[0]); } catch (_error) { path = raw; }
    } else {
      return null;
    }
    var line = null;
    var match = path.match(/:(\d+)$/);
    if (match) {
      line = Number(match[1]);
      path = path.slice(0, -match[0].length);
    }
    return { path: path, line: line };
  }

  function decorateLinks(scope) {
    (scope || document).querySelectorAll("a[href]").forEach(function (link) {
      if (link.dataset.paLinkDecorated === "1") return;
      link.dataset.paLinkDecorated = "1";
      var raw = link.getAttribute("href") || "";
      var file = filesystemTarget(raw);
      if (file) {
        var direct = "file://" + encodeURI(file.path).replace(/#/g, "%23").replace(/\?/g, "%3F");
        var params = new URLSearchParams({ path: file.path });
        if (file.line) params.set("line", String(file.line));
        link.href = direct;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.classList.add("pa-file-link");
        link.title = "Open file directly";
        var browserLink = document.createElement("a");
        browserLink.href = "/browse?" + params.toString();
        browserLink.className = "pa-file-browser-link";
        browserLink.setAttribute("aria-label", "View " + file.path + " in PA");
        browserLink.title = "View in PA";
        browserLink.textContent = "▣";
        browserLink.dataset.paLinkDecorated = "1";
        link.insertAdjacentElement("afterend", browserLink);
        return;
      }
      try {
        var url = new URL(raw, window.location.href);
        if (url.origin !== window.location.origin || !/^https?:$/.test(url.protocol)) {
          link.target = "_blank";
          link.rel = "noopener noreferrer";
        }
      } catch (_error) {}
    });
  }

  window.PALinks = {
    decorate: decorateLinks,
    filesystemTarget: filesystemTarget,
  };

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

        var card = document.querySelector('.compact-card[data-card-id="' + CSS.escape(cardId) + '"]');
        moveCard(card, lane).catch(function () {});
      });
    });

    scope.querySelectorAll(".compact-card[draggable]").forEach(function (item) {
      if (item.dataset.dndBound) return;
      item.dataset.dndBound = "1";
      item.addEventListener("dragstart", function (event) {
        var cardId = item.dataset.cardId;
        var realm = item.dataset.realm;
        var lane = item.dataset.cardLane || "";
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

    scope.querySelectorAll("[data-card-move-to]").forEach(function (button) {
      if (button.dataset.moveBound) return;
      button.dataset.moveBound = "1";
      button.addEventListener("click", function () {
        var card = button.closest(".compact-card");
        var details = button.closest("details");
        if (details) details.open = false;
        button.disabled = true;
        moveCard(card, button.dataset.cardMoveTo).catch(function () {}).finally(function () {
          button.disabled = false;
        });
      });
    });

    scope.querySelectorAll("[data-board-lane]").forEach(function (button) {
      if (button.dataset.laneBound) return;
      button.dataset.laneBound = "1";
      button.addEventListener("click", function () {
        var lane = button.dataset.boardLane;
        document.querySelectorAll("[data-board-lane]").forEach(function (candidate) {
          candidate.setAttribute("aria-pressed", candidate === button ? "true" : "false");
        });
        document.querySelectorAll(".board-column").forEach(function (column) {
          column.dataset.mobileActive = column.dataset.lane === lane ? "true" : "false";
        });
      });
    });
  }

  var cardDialogOpener = null;
  var cardDialogOwnsHistory = false;
  var cardDialogRequest = null;
  var cardDialogBackNavigation = false;

  function cardDialog() {
    return document.getElementById("card-detail-dialog");
  }

  function cardDialogContent() {
    return document.getElementById("card-detail-dialog-content");
  }

  function showCardDialog() {
    var dialog = cardDialog();
    if (!dialog || dialog.open) return;
    if (typeof dialog.showModal === "function") dialog.showModal();
    else dialog.setAttribute("open", "");
  }

  function cardDetailUrl(cardId, realm) {
    var params = new URLSearchParams({ realm: realm || "default" });
    return "/partials/cards/" + encodeURIComponent(cardId) + "/detail?" + params.toString();
  }

  function cardMarkdownSource(element) {
    if (Object.prototype.hasOwnProperty.call(element, "_paMarkdownSource")) {
      return element._paMarkdownSource;
    }
    var source = element.querySelector("[data-card-markdown-source]");
    if (!source) return "";
    try {
      element._paMarkdownSource = JSON.parse(source.textContent || '""');
    } catch (_error) {
      element._paMarkdownSource = source.textContent || "";
    }
    return element._paMarkdownSource;
  }

  function renderMarkdownInto(element, markdown) {
    if (!element) return Promise.resolve();
    if (!window.PAAgentChat || typeof window.PAAgentChat.renderMarkdownAsync !== "function") {
      element.textContent = markdown || "";
      return Promise.resolve();
    }
    element.setAttribute("aria-busy", "true");
    return window.PAAgentChat.renderMarkdownAsync(markdown).then(function (html) {
      element.innerHTML = html;
      element.removeAttribute("aria-busy");
      decorateLinks(element);
    });
  }

  function renderCardMarkdown(scope) {
    var root = scope || document;
    var elements = [];
    if (root.matches && root.matches("[data-card-markdown]")) elements.push(root);
    root.querySelectorAll("[data-card-markdown]").forEach(function (element) {
      elements.push(element);
    });
    elements.forEach(function (element) {
      var markdown = cardMarkdownSource(element);
      renderMarkdownInto(element, markdown);
    });
  }

  function setMarkdownEditorTab(editor, name) {
    if (!editor) return;
    editor.querySelectorAll("[data-markdown-tab]").forEach(function (tab) {
      var selected = tab.dataset.markdownTab === name;
      tab.setAttribute("aria-selected", selected ? "true" : "false");
      tab.classList.toggle("ghost", !selected);
    });
    editor.querySelectorAll("[data-markdown-panel]").forEach(function (panel) {
      panel.hidden = panel.dataset.markdownPanel !== name;
    });
    if (name === "preview") {
      var input = editor.querySelector("[data-markdown-input]");
      var preview = editor.querySelector("[data-markdown-preview]");
      renderMarkdownInto(preview, input ? input.value : "");
    }
  }

  function closeInlineEditor(field, restoreFocus) {
    if (!field) return;
    var form = field.querySelector("[data-inline-edit-form]");
    var trigger = field.querySelector("[data-inline-edit-open]");
    if (form) {
      form.reset();
      form.hidden = true;
      setMarkdownEditorTab(form.closest("[data-markdown-editor]"), "edit");
    }
    if (trigger) {
      trigger.hidden = false;
      if (restoreFocus) trigger.focus();
    }
    field.classList.remove("is-editing");
  }

  function openInlineEditor(field) {
    if (!field) return;
    field.closest("[data-card-detail]").querySelectorAll("[data-inline-edit-field].is-editing").forEach(function (openField) {
      if (openField !== field) closeInlineEditor(openField, false);
    });
    var form = field.querySelector("[data-inline-edit-form]");
    var trigger = field.querySelector("[data-inline-edit-open]");
    if (!form || !trigger) return;
    trigger.hidden = true;
    form.hidden = false;
    field.classList.add("is-editing");
    setMarkdownEditorTab(form.closest("[data-markdown-editor]"), "edit");
    var input = form.querySelector("[data-inline-edit-input]");
    if (input) {
      input.focus();
      if (typeof input.select === "function" && input.tagName === "INPUT") input.select();
    }
  }

  function renderCardDialogError(cardId, realm, message) {
    var content = cardDialogContent();
    if (!content) return;
    content.innerHTML =
      '<div class="card-dialog-state" role="alert"><h2>Card details unavailable</h2>' +
      '<p>' + String(message || "The card could not be loaded.").replace(/[&<>]/g, function (char) {
        return { "&": "&amp;", "<": "&lt;", ">": "&gt;" }[char];
      }) + '</p><div class="form-actions"><button type="button" data-card-detail-retry>Retry</button>' +
      '<button type="button" class="ghost" data-card-dialog-close>Close</button></div></div>';
    var retry = content.querySelector("[data-card-detail-retry]");
    if (retry) retry.addEventListener("click", function () {
      loadCardDetail(cardId, realm, false);
    });
  }

  function loadCardDetail(cardId, realm, pushHistory) {
    var content = cardDialogContent();
    if (!content || !cardId) return;
    if (cardDialogRequest) cardDialogRequest.abort();
    cardDialogRequest = new AbortController();
    content.innerHTML = '<div class="card-dialog-state" role="status" aria-live="polite"><p>Loading card details…</p><button type="button" class="ghost" data-card-dialog-close>Close</button></div>';
    showCardDialog();

    if (pushHistory) {
      var url = new URL(window.location.href);
      url.searchParams.set("card", cardId);
      if (realm) url.searchParams.set("realm", realm);
      history.pushState({ paCard: cardId }, "", url);
      cardDialogOwnsHistory = true;
    }

    fetch(cardDetailUrl(cardId, realm), {
      credentials: "same-origin",
      signal: cardDialogRequest.signal,
    })
      .then(function (response) {
        if (!response.ok) throw new Error(response.status === 404 ? "This card no longer exists." : "Request failed.");
        return response.text();
      })
      .then(function (html) {
        content.innerHTML = html;
        if (typeof htmx !== "undefined") htmx.process(content);
        decorateLinks(content);
        renderCardMarkdown(content);
        if (window.PAAgentChat && typeof window.PAAgentChat.mount === "function") {
          window.PAAgentChat.mount(content);
        }
        var heading = content.querySelector("#card-detail-title");
        if (heading) heading.focus({ preventScroll: true });
      })
      .catch(function (error) {
        if (error.name !== "AbortError") renderCardDialogError(cardId, realm, error.message);
      });
  }

  function closeCardDialog(updateHistory) {
    var dialog = cardDialog();
    if (cardDialogRequest) cardDialogRequest.abort();
    cardDialogRequest = null;
    if (dialog && dialog.open) dialog.close();
    var content = cardDialogContent();
    if (content) content.replaceChildren();
    if (updateHistory && new URL(window.location.href).searchParams.has("card")) {
      if (cardDialogOwnsHistory) {
        cardDialogBackNavigation = true;
        history.back();
      }
      else {
        var url = new URL(window.location.href);
        url.searchParams.delete("card");
        history.replaceState({}, "", url);
      }
    }
    cardDialogOwnsHistory = false;
    if (cardDialogOpener && document.contains(cardDialogOpener)) cardDialogOpener.focus();
    cardDialogOpener = null;
  }

  function openCardFromLocation() {
    var url = new URL(window.location.href);
    var cardId = url.searchParams.get("card");
    if (cardId) loadCardDetail(cardId, url.searchParams.get("realm") || "default", false);
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
    if (target) renderCardMarkdown(target);
    if (target && target.id === "app-view") {
      setActiveNav(window.location.pathname);
      updateTitle();
      initBoardDragDrop(target);
      initAgentReconnect();
      decorateLinks(target);
      if (window.PAAgentChat && typeof window.PAAgentChat.mount === "function") {
        window.PAAgentChat.mount(target);
      }
    }
    if (target && target.classList.contains("board-column-body")) {
      initBoardDragDrop(target.closest(".board-grid") || document);
    }
    if (target && target.id === "card-detail-dialog-content") {
      if (!target.querySelector("[data-card-detail]")) {
        closeCardDialog(true);
        document.body.dispatchEvent(new CustomEvent("boardRefresh"));
        return;
      }
      decorateLinks(target);
      if (window.PAAgentChat && typeof window.PAAgentChat.mount === "function") {
        window.PAAgentChat.mount(target);
      }
      document.body.dispatchEvent(new CustomEvent("boardRefresh"));
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

  window.addEventListener("popstate", function (event) {
    var url = new URL(window.location.href);
    var cardId = url.searchParams.get("card");
    if (cardId) {
      cardDialogBackNavigation = false;
      cardDialogOwnsHistory = !!(event.state && event.state.paCard);
      loadCardDetail(cardId, url.searchParams.get("realm") || "default", false);
      return;
    }
    if (cardDialogBackNavigation) {
      cardDialogBackNavigation = false;
      return;
    }
    if (cardDialog() && cardDialog().open) {
      closeCardDialog(false);
      return;
    }
    if (typeof htmx === "undefined") return;
    htmx.ajax("GET", window.location.pathname + window.location.search, {
      target: "#app-view",
      swap: "innerHTML",
    });
    setActiveNav(window.location.pathname);
    updateTitle();
  });

  document.body.addEventListener("click", function (event) {
    var detailLink = event.target.closest("[data-card-detail-link]");
    if (detailLink) {
      event.preventDefault();
      cardDialogOpener = detailLink;
      loadCardDetail(
        detailLink.dataset.cardId,
        detailLink.dataset.cardRealm || "default",
        true
      );
      return;
    }
    if (event.target.closest("[data-card-dialog-close]")) {
      closeCardDialog(true);
      return;
    }
    var detail = event.target.closest("[data-card-detail]");
    if (!detail) return;
    var editTrigger = event.target.closest("[data-inline-edit-open]");
    if (editTrigger) {
      var embeddedControl = event.target.closest("a, audio, video, iframe, input, select, textarea");
      if (embeddedControl && embeddedControl !== editTrigger) return;
      openInlineEditor(editTrigger.closest("[data-inline-edit-field]"));
      return;
    }
    var editCancel = event.target.closest("[data-inline-edit-cancel]");
    if (editCancel) {
      closeInlineEditor(editCancel.closest("[data-inline-edit-field]"), true);
      return;
    }
    var markdownTab = event.target.closest("[data-markdown-tab]");
    if (markdownTab) {
      setMarkdownEditorTab(
        markdownTab.closest("[data-markdown-editor]"),
        markdownTab.dataset.markdownTab
      );
      return;
    }
    var agentButton = event.target.closest("[data-card-agent-start]");
    if (agentButton) {
      var pane = detail.querySelector("[data-card-agent-pane]");
      if (!pane) return;
      pane.hidden = false;
      if (window.PAAgentChat && typeof window.PAAgentChat.mount === "function") {
        window.PAAgentChat.mount(pane);
      }
      var widget = pane.querySelector("[data-agent-chat]");
      if (widget && widget._acw && !widget.dataset.explicitlyStarted) {
        widget.dataset.explicitlyStarted = "1";
        agentButton.disabled = true;
        agentButton.textContent = widget.dataset.sessionId ? "Resuming…" : "Starting…";
        widget._acw.init();
        window.setTimeout(function () {
          agentButton.hidden = true;
        }, 250);
      }
    }
  });

  document.body.addEventListener("keydown", function (event) {
    var trigger = event.target.closest("[data-inline-edit-open]");
    if (trigger && event.target === trigger && (event.key === "Enter" || event.key === " ")) {
      event.preventDefault();
      openInlineEditor(trigger.closest("[data-inline-edit-field]"));
      return;
    }
    var form = event.target.closest("[data-inline-edit-form]");
    if (!form) return;
    if (event.key === "Escape") {
      event.preventDefault();
      closeInlineEditor(form.closest("[data-inline-edit-field]"), true);
      return;
    }
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      if (typeof form.requestSubmit === "function") form.requestSubmit();
    }
  });

  document.addEventListener("DOMContentLoaded", function () {
    setActiveNav(window.location.pathname);
    updateTitle();
    initBoardDragDrop(document);
    initAgentReconnect();
    decorateLinks(document);
    renderCardMarkdown(document);
    checkServerBuild();
    var dialog = cardDialog();
    if (dialog) {
      dialog.addEventListener("cancel", function (event) {
        event.preventDefault();
        closeCardDialog(true);
      });
    }
    openCardFromLocation();
    window.setInterval(checkServerBuild, VERSION_POLL_MS);
  });
})();
