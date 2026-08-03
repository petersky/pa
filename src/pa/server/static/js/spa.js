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
    var detail = event.detail || {};
    var candidate = detail.target || (detail.ctx && detail.ctx.target) || event.target;
    if (typeof candidate === "string") {
      candidate = document.querySelector(candidate);
    }
    return candidate && typeof candidate.querySelectorAll === "function"
      ? candidate
      : null;
  }

  function updateTitle() {
    const active = document.querySelector(".nav-btn.active span:last-child");
    const instance = document.querySelector("[data-pa-instance-name]");
    if (active && instance) {
      const label = instance.getAttribute("data-pa-instance-name") || "PA";
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

  var boardEventSource = null;
  var boardEventRealm = null;

  function stopBoardLiveUpdates() {
    if (boardEventSource) boardEventSource.close();
    boardEventSource = null;
    boardEventRealm = null;
    window.__paWorkResources = { eventSources: 0 };
  }

  function initBoardLiveUpdates() {
    var board = document.querySelector("[data-work-board]");
    var realm = board && board.dataset.realm;
    if (!realm) {
      stopBoardLiveUpdates();
      return;
    }
    if (boardEventSource && boardEventRealm === realm) return;
    if (boardEventSource) boardEventSource.close();
    boardEventRealm = realm;
    boardEventSource = new EventSource(
      "/api/cards/events?realm=" + encodeURIComponent(realm)
    );
    window.__paWorkResources = { eventSources: 1 };
    boardEventSource.addEventListener("cards-changed", function (event) {
      var current = document.querySelector("[data-work-board]");
      if (!current || current.dataset.realm !== realm) return;
      try {
        var update = JSON.parse(event.data);
        if (update.realm_id !== realm) return;
      } catch (_error) {
        return;
      }
      document.body.dispatchEvent(new CustomEvent("boardRefresh"));
    });
  }

  document.addEventListener("pa:historyWillReload", function () {
    stopBoardLiveUpdates();
  });

  document.body.addEventListener("htmx:beforeSwap", function (event) {
    var target = event.detail && event.detail.target;
    if (target && (target.matches("#app-view") || target.querySelector("[data-work-board]"))) {
      stopBoardLiveUpdates();
    }
  });

  var cardDialogOpener = null;
  var cardDialogHistoryDepth = 0;
  var cardDialogHasBaseEntry = false;
  var cardDialogRequest = null;
  var cardDialogBackNavigation = false;
  var cardTabs = ["summary", "agent", "activity"];
  var cardDispatchDialogOpener = null;
  var cardDispatchRequest = null;

  function cardDispatchDialog() {
    return document.getElementById("card-dispatch-dialog");
  }

  function cardDispatchDialogContent() {
    return document.getElementById("card-dispatch-dialog-content");
  }

  function closeCardDispatchDialog() {
    if (cardDispatchRequest) cardDispatchRequest.abort();
    cardDispatchRequest = null;
    var dialog = cardDispatchDialog();
    if (dialog && dialog.open) dialog.close();
    var content = cardDispatchDialogContent();
    if (content) content.replaceChildren();
    if (cardDispatchDialogOpener && document.contains(cardDispatchDialogOpener)) {
      cardDispatchDialogOpener.focus();
    }
    cardDispatchDialogOpener = null;
  }

  function openCardDispatchDialog(cardId, realm, opener) {
    var dialog = cardDispatchDialog();
    var content = cardDispatchDialogContent();
    if (!dialog || !content || !cardId) return;
    if (cardDispatchRequest) cardDispatchRequest.abort();
    cardDispatchDialogOpener = opener || document.activeElement;
    content.innerHTML = '<div class="card-dialog-state" role="status"><p>Loading dispatch configuration…</p></div>';
    if (!dialog.open) {
      if (typeof dialog.showModal === "function") dialog.showModal();
      else dialog.setAttribute("open", "");
    }
    cardDispatchRequest = new AbortController();
    fetch("/partials/cards/" + encodeURIComponent(cardId) + "/dispatch?realm=" + encodeURIComponent(realm || "default"), {
      credentials: "same-origin", signal: cardDispatchRequest.signal,
    }).then(function (response) {
      if (!response.ok) throw new Error("Dispatch configuration could not be loaded.");
      return response.text();
    }).then(function (html) {
      content.innerHTML = html;
      var context = content.querySelector("[data-card-dispatch-context]");
      initCardDispatchContext(context);
      var title = content.querySelector("#card-dispatch-dialog-title");
      if (title) title.focus();
    }).catch(function (error) {
      if (error.name === "AbortError") return;
      content.innerHTML = '<div class="card-dialog-state" role="alert"><h2>Dispatch unavailable</h2><p>' +
        error.message + '</p><button type="button" data-card-dispatch-close>Close</button></div>';
    });
  }

  window.PACardDispatch = { open: openCardDispatchDialog, close: closeCardDispatchDialog };

  var dispatchDialogElement = cardDispatchDialog();
  if (dispatchDialogElement) {
    dispatchDialogElement.addEventListener("cancel", function (event) {
      event.preventDefault();
      closeCardDispatchDialog();
    });
  }

  function hideCardDispatchActions(cardId) {
    document.querySelectorAll('[data-card-dispatch-open][data-card-id="' + CSS.escape(cardId) + '"]').forEach(function (button) {
      if (!button.closest("[data-card-detail]")) button.remove();
    });
  }

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

  function normalizedCardTab(name) {
    return cardTabs.indexOf(name) >= 0 ? name : "summary";
  }

  function currentCardTab() {
    return normalizedCardTab(new URL(window.location.href).searchParams.get("tab"));
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
    var options = element.classList.contains("memory-markdown")
      ? { allowEmbeddedMedia: false }
      : undefined;
    return window.PAAgentChat.renderMarkdownAsync(markdown, options).then(function (html) {
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

  function observeMarkdownMutations() {
    if (typeof MutationObserver !== "function" || !document.body) return;
    var observer = new MutationObserver(function (records) {
      records.forEach(function (record) {
        record.addedNodes.forEach(function (node) {
          if (node && node.nodeType === 1) renderCardMarkdown(node);
        });
      });
    });
    observer.observe(document.body, { childList: true, subtree: true });
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

  function initActivityPanel(panel) {
    if (!panel || panel.dataset.activityBound) return;
    panel.dataset.activityBound = "1";
    var filter = panel.querySelector("[data-card-activity-filter]");
    var more = panel.querySelector("[data-card-activity-more]");
    var entries = Array.prototype.slice.call(panel.querySelectorAll("[data-card-activity-entry]"));
    var empty = panel.querySelector("[data-card-activity-empty]");
    var expanded = false;

    function applyFilter() {
      var value = filter ? filter.value : "all";
      var visible = 0;
      entries.forEach(function (entry) {
        var matches = value === "all" || entry.dataset.activityKind === value;
        var withinLimit = expanded || visible < 25;
        entry.hidden = !matches || !withinLimit;
        if (matches) visible += 1;
      });
      var shown = entries.filter(function (entry) { return !entry.hidden; }).length;
      if (empty) empty.hidden = shown > 0;
      if (more) {
        more.hidden = expanded || visible <= 25;
        more.textContent = "Show older activity";
      }
    }

    if (filter) filter.addEventListener("change", applyFilter);
    if (more) more.addEventListener("click", function () {
      expanded = true;
      applyFilter();
    });
    applyFilter();
  }

  function renderCardTabError(panel, message) {
    panel.dataset.cardTabState = "error";
    panel.innerHTML =
      '<div class="card-tab-state" role="alert"><h3>Could not load this tab</h3><p>' +
      String(message || "Request failed.").replace(/[&<>]/g, function (char) {
        return { "&": "&amp;", "<": "&lt;", ">": "&gt;" }[char];
      }) +
      '</p><button type="button" data-card-tab-retry>Retry</button></div>';
  }

  function loadCardTab(detail, name) {
    var panel = detail && detail.querySelector('[data-card-tab-panel="' + name + '"]');
    if (!panel || !panel.dataset.cardTabSrc || panel.dataset.cardTabState === "loaded" ||
        panel.dataset.cardTabState === "loading") return;
    panel.dataset.cardTabState = "loading";
    panel.innerHTML =
      '<div class="card-tab-state" role="status" aria-live="polite"><span class="loading-spinner" aria-hidden="true"></span><p>Loading ' +
      name + "…</p></div>";
    fetch(panel.dataset.cardTabSrc, { credentials: "same-origin" })
      .then(function (response) {
        if (!response.ok) throw new Error(response.status === 404 ? "This card no longer exists." : "Request failed.");
        return response.text();
      })
      .then(function (html) {
        panel.innerHTML = html;
        panel.dataset.cardTabState = "loaded";
        if (typeof htmx !== "undefined") htmx.process(panel);
        decorateLinks(panel);
        renderCardMarkdown(panel);
        initActivityPanel(panel);
        if (window.PAAgentChat && typeof window.PAAgentChat.mount === "function") {
          window.PAAgentChat.mount(panel);
        }
        var announcer = detail.querySelector("[data-card-tab-announcer]");
        if (announcer) announcer.textContent = name + " tab loaded.";
      })
      .catch(function (error) {
        renderCardTabError(panel, error.message);
      });
  }

  function activateCardTab(detail, name, pushHistory, focusTab) {
    if (!detail) return;
    name = normalizedCardTab(name);
    var selected = detail.querySelector('[data-card-tab="' + name + '"]');
    if (!selected) return;
    var previous = detail.querySelector('[data-card-tab][aria-selected="true"]');

    detail.querySelectorAll("[data-card-tab]").forEach(function (tab) {
      var active = tab === selected;
      tab.setAttribute("aria-selected", active ? "true" : "false");
      tab.tabIndex = active ? 0 : -1;
    });
    detail.querySelectorAll("[data-card-tab-panel]").forEach(function (panel) {
      panel.hidden = panel.dataset.cardTabPanel !== name;
    });

    if (pushHistory && (!previous || previous.dataset.cardTab !== name)) {
      var url = new URL(window.location.href);
      url.searchParams.set("card", detail.dataset.cardId);
      url.searchParams.set("tab", name);
      if (detail.dataset.cardRealm) url.searchParams.set("realm", detail.dataset.cardRealm);
      if (cardDialogHistoryDepth === 0 && !(history.state && history.state.paCard)) {
        history.replaceState(
          {
            paCard: detail.dataset.cardId,
            paCardTab: previous ? previous.dataset.cardTab : "summary",
            paCardDepth: 0,
            paCardHasBase: cardDialogHasBaseEntry,
          },
          "",
          window.location.href
        );
      }
      cardDialogHistoryDepth += 1;
      history.pushState(
        {
          paCard: detail.dataset.cardId,
          paCardTab: name,
          paCardDepth: cardDialogHistoryDepth,
          paCardHasBase: cardDialogHasBaseEntry,
        },
        "",
        url
      );
    }
    if (focusTab) selected.focus();
    loadCardTab(detail, name);
  }


  function dispatchOperationKey(prefix) {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return prefix + ":" + window.crypto.randomUUID();
    }
    return prefix + ":" + Date.now() + ":" + Math.random().toString(16).slice(2);
  }

  function dispatchIsTerminal(state) {
    return ["failed", "completed", "cancelled", "acknowledged"].indexOf(state) !== -1;
  }

  function dispatchStateLabel(state) {
    if (["materializing", "starting_session", "delivering_prompt"].indexOf(state) !== -1) return "starting";
    return String(state || "queued").replace(/_/g, " ");
  }

  function renderCardDispatch(detail, dispatch) {
    var region = detail && detail.querySelector("[data-card-dispatch-status]");
    if (!region || !dispatch) return;
    region.dataset.dispatchId = dispatch.dispatch_id || "";
    region.replaceChildren();

    var heading = document.createElement("div");
    heading.className = "card-dispatch-status-heading";
    var badge = document.createElement("span");
    badge.className = "status status-" + String(dispatch.state || "queued").replace(/[^a-z_-]/g, "");
    badge.textContent = dispatchStateLabel(dispatch.state);
    var target = document.createElement("strong");
    target.textContent = dispatch.target_instance_name || dispatch.target_instance_id || "Resolving target";
    heading.append(badge, target);
    region.appendChild(heading);

    var events = Array.isArray(dispatch.events) ? dispatch.events : [];
    var message = document.createElement("p");
    message.textContent = events.length ? events[events.length - 1].message : "Dispatch admitted.";
    region.appendChild(message);

    if (dispatch.materialization_plan && dispatch.materialization_plan.summary) {
      var plan = document.createElement("p");
      plan.className = "muted";
      plan.textContent = dispatch.materialization_plan.summary;
      region.appendChild(plan);
    }

    if (dispatch.placement_decision && dispatch.placement_decision.tie_breaking_reason) {
      var explanation = document.createElement("p");
      explanation.className = "muted";
      explanation.textContent = dispatch.placement_decision.tie_breaking_reason;
      region.appendChild(explanation);
    }
    if (dispatch.last_error) {
      var error = document.createElement("p");
      error.className = "danger";
      error.textContent = dispatch.last_error;
      region.appendChild(error);
    }

    var links = document.createElement("div");
    links.className = "card-dispatch-links";
    if (dispatch.session_id) {
      var session = document.createElement("a");
      session.href = "/agent?session=" + encodeURIComponent(dispatch.session_id);
      session.textContent = "Open durable session";
      links.appendChild(session);
    }
    if (dispatch.can_retry) {
      var retry = document.createElement("button");
      retry.type = "button";
      retry.className = "ghost small";
      retry.dataset.cardDispatchRetry = dispatch.dispatch_id;
      retry.textContent = "Retry dispatch";
      links.appendChild(retry);
    }
    region.appendChild(links);

    var form = detail.querySelector("[data-card-dispatch-form]");
    if (form) {
      var submit = form.querySelector('button[type="submit"]');
      if (submit) {
        submit.disabled = !dispatchIsTerminal(dispatch.state);
        submit.textContent = dispatchIsTerminal(dispatch.state) ? "Dispatch card" : "Dispatch in progress…";
      }
      if (dispatchIsTerminal(dispatch.state)) form.dataset.idempotencyKey = "";
    }
  }

  function pollCardDispatch(detail, dispatchId, delay) {
    if (!detail || !dispatchId) return;
    window.clearTimeout(detail._cardDispatchPollTimer);
    detail._cardDispatchPollTimer = window.setTimeout(function () {
      if (!document.contains(detail)) return;
      fetch("/api/fleet/dispatch-jobs/" + encodeURIComponent(dispatchId), {
        credentials: "same-origin",
      })
        .then(function (response) {
          if (!response.ok) throw new Error("Dispatch status is temporarily unavailable.");
          return response.json();
        })
        .then(function (dispatch) {
          renderCardDispatch(detail, dispatch);
          if (!dispatchIsTerminal(dispatch.state)) pollCardDispatch(detail, dispatchId, 1200);
        })
        .catch(function () {
          pollCardDispatch(detail, dispatchId, 3000);
        });
    }, delay || 0);
  }

  function renderCardDispatchError(detail, payload) {
    var region = detail && detail.querySelector("[data-card-dispatch-status]");
    if (!region) return;
    var error = payload && payload.detail ? payload.detail : payload || {};
    region.replaceChildren();
    var heading = document.createElement("strong");
    heading.textContent = error.code === "no_eligible_instance" ? "No eligible target" : "Dispatch not admitted";
    var message = document.createElement("p");
    message.className = "danger";
    message.textContent = error.message || "The fleet dispatch request failed.";
    region.append(heading, message);
    if (error.plan && error.plan.summary) {
      var plan = document.createElement("p");
      plan.textContent = error.plan.summary;
      region.appendChild(plan);
    }
    var rejected = Array.isArray(error.rejected_candidates) ? error.rejected_candidates : [];
    if (rejected.length) {
      var list = document.createElement("ul");
      list.className = "card-dispatch-rejections";
      rejected.slice(0, 5).forEach(function (candidate) {
        var item = document.createElement("li");
        item.textContent = (candidate.name || candidate.instance_id || "Instance") + ": " +
          (Array.isArray(candidate.reasons) ? candidate.reasons.join("; ") : "not eligible");
        list.appendChild(item);
      });
      region.appendChild(list);
    }
    var consumers = Array.isArray(error.consumer_links) ? error.consumer_links : [];
    if (consumers.length) {
      var consumerList = document.createElement("ul");
      consumerList.className = "card-dispatch-rejections";
      consumers.slice(0, 8).forEach(function (consumer) {
        var item = document.createElement("li");
        var link = document.createElement("a");
        link.href = consumer.href || "/fleet?section=overview";
        link.textContent = (consumer.kind || "work") + " · " +
          (consumer.state || "active") + " · " + (consumer.slots || 1) + " slot" +
          ((consumer.slots || 1) === 1 ? "" : "s");
        item.appendChild(link);
        consumerList.appendChild(item);
      });
      region.appendChild(consumerList);
    }
    if (error.recovery_url) {
      var recovery = document.createElement("a");
      recovery.href = error.recovery_url;
      recovery.textContent = "Review Fleet readiness";
      region.appendChild(recovery);
    }
    if (error.dispatch_id) {
      pollCardDispatch(detail, error.dispatch_id, 0);
    }
  }

  function initCardDetail(detail, selectedTab) {
    if (!detail) return;
    activateCardTab(detail, selectedTab, false, false);
    initActivityPanel(detail.querySelector('[data-card-tab-panel="activity"]'));
    var dispatchStatus = detail.querySelector("[data-card-dispatch-status]");
    if (dispatchStatus && dispatchStatus.dataset.dispatchId) {
      pollCardDispatch(detail, dispatchStatus.dataset.dispatchId, 0);
    }
    var dispatchForm = detail.querySelector("[data-card-dispatch-form]");
    refreshCardDispatchSelectors(dispatchForm);
    updateCardDispatchUtilization(dispatchForm);
    previewCardPlacement(dispatchForm);
  }

  function initCardDispatchContext(context) {
    if (!context) return;
    var dispatchStatus = context.querySelector("[data-card-dispatch-status]");
    if (dispatchStatus && dispatchStatus.dataset.dispatchId) {
      pollCardDispatch(context, dispatchStatus.dataset.dispatchId, 0);
    }
    var dispatchForm = context.querySelector("[data-card-dispatch-form]");
    refreshCardDispatchSelectors(dispatchForm);
    updateCardDispatchUtilization(dispatchForm);
    previewCardPlacement(dispatchForm);
  }

  function dispatchInventory(form) {
    var script = form && form.querySelector("[data-card-dispatch-inventory]");
    if (!script) return {};
    try { return JSON.parse(script.textContent || "{}"); } catch (_error) { return {}; }
  }

  function refreshCardDispatchSelectors(form) {
    if (!form) return;
    var providerSelect = form.elements.provider;
    var modelSelect = form.elements.model_id;
    var help = form.querySelector("[data-dispatch-provider-help]");
    if (!providerSelect || !modelSelect) return;
    var previousProvider = providerSelect.value;
    var previousModel = modelSelect.value;
    var target = form.elements.dispatch_target.value;
    var inventory = dispatchInventory(form);
    var instanceIds = target.indexOf("instance:") === 0 ? [target.slice(9)] : Object.keys(inventory);
    var snapshots = instanceIds.map(function (id) { return inventory[id]; }).filter(Boolean);
    var fresh = snapshots.filter(function (snapshot) { return snapshot.state === "fresh"; });
    var byProvider = {};
    fresh.forEach(function (snapshot) {
      (Array.isArray(snapshot.providers) ? snapshot.providers : []).forEach(function (provider) {
        var id = String(provider.id || "").toLowerCase();
        if (!id) return;
        var entry = byProvider[id] || (byProvider[id] = {id: id, names: [], reasons: [], models: {}, ready: false});
        entry.names.push(snapshot.instance_name || "instance");
        var ready = !!provider.available && String(provider.auth_state || "unknown") === "authenticated";
        entry.ready = entry.ready || ready;
        if (!ready) entry.reasons.push((snapshot.instance_name || "instance") + ": " +
          (!provider.available ? (provider.error || provider.auth_status || "provider unavailable") :
            "authentication " + (provider.auth_state || "unknown")));
        (provider.models || (provider.meta || {}).models || []).forEach(function (model) {
          var modelId = typeof model === "string" ? model : String(model.id || model.model_id || "");
          if (modelId) entry.models[modelId] = true;
        });
      });
    });
    providerSelect.replaceChildren(new Option("Automatic — any eligible authenticated provider", ""));
    Object.keys(byProvider).sort().forEach(function (id) {
      var entry = byProvider[id];
      var suffix = entry.ready ? " · authenticated on " + entry.names.join(", ") : " · unavailable — " + (entry.reasons.join("; ") || "not ready");
      var option = new Option(id.charAt(0).toUpperCase() + id.slice(1) + suffix, id);
      option.disabled = !entry.ready;
      providerSelect.add(option);
    });
    providerSelect.value = Array.from(providerSelect.options).some(function (option) {
      return option.value === previousProvider && !option.disabled;
    }) ? previousProvider : "";
    var selected = byProvider[providerSelect.value];
    modelSelect.replaceChildren(new Option("Provider default / automatic", ""));
    Object.keys(selected ? selected.models : {}).sort().forEach(function (modelId) {
      modelSelect.add(new Option(modelId, modelId));
    });
    modelSelect.value = Array.from(modelSelect.options).some(function (option) {
      return option.value === previousModel;
    }) ? previousModel : "";
    if (help) {
      help.textContent = !snapshots.length || fresh.length !== snapshots.length ?
        "Provider inventory is stale or refreshing; unavailable choices cannot be submitted. Retry placement refresh to update it." :
        "Choices are validated against the selected target and revalidated during admission.";
    }
  }

  function updateCardDispatchUtilization(form) {
    if (!form) return;
    var select = form.elements.dispatch_target;
    var output = form.querySelector("[data-card-dispatch-utilization]");
    if (!select || !output) return;
    var option = select.options[select.selectedIndex];
    if (!option || !option.dataset.capacitySummary) {
      output.textContent = "This policy probes fresh utilization on every candidate before admission.";
      output.classList.remove("danger");
      return;
    }
    var eligible = option.dataset.capacityEligible === "true";
    output.textContent = option.dataset.capacitySummary +
      (eligible ? " · currently eligible" : " · currently ineligible; dispatch will recheck fresh data");
    output.classList.toggle("danger", !eligible);
  }

  function cardDispatchPayload(form, detail) {
    var target = form.elements.dispatch_target.value;
    var profile = form.elements.execution_profile.value;
    var payload = {
      card_id: detail.dataset.cardId,
      provider: form.elements.provider ? form.elements.provider.value.trim() || null : null,
      model_id: form.elements.model_id && form.elements.model_id.value !== "None" ? form.elements.model_id.value.trim() || null : null,
      execution_contract: {
        version: 1,
        profile: profile,
        confirmed: profile !== "automatic",
        requirements: {},
      },
    };
    if (target.indexOf("policy:") === 0) {
      payload.placement_policy = target.slice(7);
      if (form.elements.worker_group && form.elements.worker_group.value) {
        payload.group_id = form.elements.worker_group.value;
      }
    }
    if (target.indexOf("instance:") === 0) {
      payload.target_instance_id = target.slice(9);
      if (form.elements.participation_override &&
          form.elements.participation_override.checked) {
        payload.participation_override = true;
        payload.participation_override_reason =
          form.elements.participation_override_reason.value.trim();
      }
    }
    return payload;
  }

  function renderCardPlacementPreview(form, data) {
    var region = form && form.querySelector("[data-card-dispatch-preview]");
    if (!region || !data || !data.decision) return;
    var decision = data.decision;
    region.replaceChildren();
    var heading = document.createElement("strong");
    heading.textContent = (decision.resolved_group_name || "Named instance") +
      " · " + (decision.workload_profile || "work") +
      " · expected " + (decision.chosen_instance_name || decision.chosen_instance_id);
    region.appendChild(heading);
    var explanation = document.createElement("p");
    explanation.className = "muted";
    explanation.textContent = decision.tie_breaking_reason || "Placement preview resolved.";
    region.appendChild(explanation);
    var eligible = Array.isArray(decision.eligible_candidates) ? decision.eligible_candidates : [];
    var rejected = Array.isArray(decision.rejected_candidates) ? decision.rejected_candidates : [];
    var summary = document.createElement("p");
    summary.textContent = eligible.length + " eligible · " + rejected.length +
      " rejected · group version " + (decision.group_version || "system");
    region.appendChild(summary);
    if (eligible.length) {
      var eligibleList = document.createElement("ul");
      eligibleList.className = "card-dispatch-rejections";
      eligible.forEach(function (candidate) {
        var item = document.createElement("li");
        item.textContent = (candidate.name || candidate.instance_id) +
          " — eligible; " + candidate.consumed + "/" + candidate.capacity +
          " slots used; " + (candidate.policy_summary || "policy passed");
        eligibleList.appendChild(item);
      });
      region.appendChild(eligibleList);
    }
    if (rejected.length) {
      var rejectedList = document.createElement("ul");
      rejectedList.className = "card-dispatch-rejections";
      rejected.forEach(function (candidate) {
        var item = document.createElement("li");
        var codes = Array.isArray(candidate.rejection_codes) ?
          " [" + candidate.rejection_codes.join(", ") + "]" : "";
        item.textContent = (candidate.name || candidate.instance_id) +
          " — excluded" + codes + ": " +
          (Array.isArray(candidate.reasons) ? candidate.reasons.join("; ") : "not eligible");
        rejectedList.appendChild(item);
      });
      region.appendChild(rejectedList);
    }
  }

  function previewCardPlacement(form) {
    if (!form) return;
    var detail = form.closest("[data-card-dispatch-context]");
    var region = form.querySelector("[data-card-dispatch-preview]");
    if (!detail || !region) return;
    window.clearTimeout(form._placementPreviewTimer);
    form._placementPreviewTimer = window.setTimeout(function () {
      if (!document.contains(form)) return;
      if (form._placementPreviewAbort) form._placementPreviewAbort.abort();
      form._placementPreviewAbort = new AbortController();
      region.textContent = "Refreshing placement preview…";
      fetch("/api/fleet/placement/preview", {
        method: "POST",
        credentials: "same-origin",
        headers: Object.assign({"Content-Type": "application/json"}, csrfHeader()),
        signal: form._placementPreviewAbort.signal,
        body: JSON.stringify(cardDispatchPayload(form, detail)),
      })
        .then(function (response) {
          return response.json().catch(function () { return {}; }).then(function (data) {
            if (!response.ok) throw data;
            return data;
          });
        })
        .then(function (data) { renderCardPlacementPreview(form, data); })
        .catch(function (payload) {
          if (payload && payload.name === "AbortError") return;
          var error = payload && payload.detail ? payload.detail : payload || {};
          region.replaceChildren();
          var heading = document.createElement("strong");
          heading.textContent = error.code === "no_eligible_instance" ?
            "No eligible candidate" : "Preview unavailable";
          var message = document.createElement("p");
          message.className = "danger";
          message.textContent = error.message || "Placement preview could not be resolved.";
          region.append(heading, message);
          var rejected = Array.isArray(error.rejected_candidates) ?
            error.rejected_candidates : [];
          if (rejected.length) {
            var list = document.createElement("ul");
            list.className = "card-dispatch-rejections";
            rejected.forEach(function (candidate) {
              var item = document.createElement("li");
              item.textContent = (candidate.name || candidate.instance_id) + ": " +
                (candidate.reasons || []).join("; ");
              list.appendChild(item);
            });
            region.appendChild(list);
          }
        });
    }, 180);
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
      loadCardDetail(cardId, realm, false, currentCardTab());
    });
  }

  function loadCardDetail(cardId, realm, pushHistory, selectedTab) {
    var content = cardDialogContent();
    if (!content || !cardId) return;
    if (window.PAAgentChat && typeof window.PAAgentChat.destroy === "function") {
      window.PAAgentChat.destroy(content, "card-replaced");
    }
    selectedTab = normalizedCardTab(selectedTab);
    if (cardDialogRequest) cardDialogRequest.abort();
    cardDialogRequest = new AbortController();
    content.innerHTML = '<div class="card-dialog-state" role="status" aria-live="polite"><p>Loading card details…</p><button type="button" class="ghost" data-card-dialog-close>Close</button></div>';
    showCardDialog();

    if (pushHistory) {
      var url = new URL(window.location.href);
      url.searchParams.set("card", cardId);
      url.searchParams.set("tab", selectedTab);
      if (realm) url.searchParams.set("realm", realm);
      if (cardDialogHistoryDepth === 0) {
        cardDialogHasBaseEntry = !new URL(window.location.href).searchParams.has("card");
      }
      cardDialogHistoryDepth += 1;
      history.pushState(
        {
          paCard: cardId,
          paCardTab: selectedTab,
          paCardDepth: cardDialogHistoryDepth,
          paCardHasBase: cardDialogHasBaseEntry,
        },
        "",
        url
      );
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
        initCardDetail(content.querySelector("[data-card-detail]"), selectedTab);
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
    if (content) {
      if (window.PAAgentChat && typeof window.PAAgentChat.destroy === "function") {
        window.PAAgentChat.destroy(content, "card-closed");
      }
      content.replaceChildren();
    }
    if (updateHistory && new URL(window.location.href).searchParams.has("card")) {
      if (cardDialogHasBaseEntry && cardDialogHistoryDepth > 0) {
        cardDialogBackNavigation = true;
        history.go(-cardDialogHistoryDepth);
      }
      else {
        var url = new URL(window.location.href);
        url.searchParams.delete("card");
        url.searchParams.delete("tab");
        history.replaceState({}, "", url);
      }
    }
    cardDialogHistoryDepth = 0;
    cardDialogHasBaseEntry = false;
    if (cardDialogOpener && document.contains(cardDialogOpener)) cardDialogOpener.focus();
    cardDialogOpener = null;
  }

  function openCardFromLocation() {
    var url = new URL(window.location.href);
    var cardId = url.searchParams.get("card");
    if (cardId) {
      var state = history.state || {};
      cardDialogHistoryDepth = Number(state.paCardDepth || 0);
      cardDialogHasBaseEntry = !!state.paCardHasBase;
      loadCardDetail(
        cardId,
        url.searchParams.get("realm") || "default",
        false,
        normalizedCardTab(url.searchParams.get("tab"))
      );
    }
  }

  function restoreCardFromHistory() {
    window.setTimeout(function () {
      var url = new URL(window.location.href);
      var cardId = url.searchParams.get("card");
      if (!cardId) return;
      var selectedTab = normalizedCardTab(url.searchParams.get("tab"));
      var content = cardDialogContent();
      var detail = content && content.querySelector("[data-card-detail]");
      if (detail && detail.dataset.cardId === cardId && cardDialog() && cardDialog().open) {
        activateCardTab(detail, selectedTab, false, false);
      } else {
        loadCardDetail(
          cardId,
          url.searchParams.get("realm") || "default",
          false,
          selectedTab
        );
      }
    }, 0);
  }

  var newCardDialogOpener = null;
  var newCardRequest = null;
  var newCardFiles = [];
  var NEW_CARD_MAX_FILES = 10;
  var NEW_CARD_MAX_FILE_BYTES = 25 * 1024 * 1024;
  var NEW_CARD_IMAGE_TYPES = [
    "image/avif", "image/gif", "image/jpeg", "image/png", "image/webp"
  ];

  function newCardDialog() {
    return document.getElementById("new-card-dialog");
  }

  function newCardDialogContent() {
    return document.getElementById("new-card-dialog-content");
  }

  function newCardContextUrl() {
    var current = new URL(window.location.href);
    var params = new URLSearchParams();
    params.set("realm", current.searchParams.get("realm") || "default");
    var project = current.searchParams.get("project");
    if (project) params.set("project", project);
    return "/partials/cards/new?" + params.toString();
  }

  function showNewCardDialog() {
    var dialog = newCardDialog();
    if (!dialog || dialog.open) return;
    if (typeof dialog.showModal === "function") dialog.showModal();
    else dialog.setAttribute("open", "");
  }

  function clearNewCardFiles() {
    newCardFiles.forEach(function (record) {
      if (record.preview) URL.revokeObjectURL(record.preview);
    });
    newCardFiles = [];
  }

  function closeNewCardDialog() {
    if (newCardRequest) newCardRequest.abort();
    newCardRequest = null;
    clearNewCardFiles();
    var dialog = newCardDialog();
    if (dialog && dialog.open) dialog.close();
    var content = newCardDialogContent();
    if (content) content.replaceChildren();
    if (newCardDialogOpener && document.contains(newCardDialogOpener)) {
      newCardDialogOpener.focus();
    }
    newCardDialogOpener = null;
  }

  function newCardErrorMessage(data, fallback) {
    var detail = data && data.detail;
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) {
      return detail.map(function (item) { return item.msg || String(item); }).join("; ");
    }
    return fallback || "The card could not be created.";
  }

  function addNewCardLinkRow(form) {
    var template = form.querySelector("[data-new-card-link-template]");
    var list = form.querySelector("[data-new-card-link-list]");
    if (!template || !list) return;
    list.appendChild(template.content.cloneNode(true));
    var rows = list.querySelectorAll(".new-card-link-row");
    var input = rows.length && rows[rows.length - 1].querySelector('input[type="url"]');
    if (input && rows.length > 1) input.focus();
  }

  function newCardFileToken() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }
    return Date.now().toString(36) + Math.random().toString(36).slice(2);
  }

  function newCardFileMediaKind(file) {
    var type = String(file.type || "").toLowerCase();
    var name = String(file.name || "").toLowerCase();
    if (NEW_CARD_IMAGE_TYPES.indexOf(type) !== -1 ||
        /\.(avif|gif|jpe?g|png|webp)$/.test(name)) {
      return "image";
    }
    if (type.indexOf("video/") === 0 ||
        /\.(m4v|mov|mp4|ogv|webm)$/.test(name)) {
      return "video";
    }
    if (type.indexOf("audio/") === 0 ||
        /\.(aac|flac|m4a|mp3|oga|ogg|opus|wav|weba)$/.test(name)) {
      return "audio";
    }
    return "file";
  }

  function newCardFileMarkup(file, token) {
    var name = String(file.name || "attachment")
      .replace(/\\/g, "\\\\")
      .replace(/\[/g, "\\[")
      .replace(/\]/g, "\\]");
    var target = "attachment:" + token;
    var kind = newCardFileMediaKind(file);
    if (kind === "image") {
      return "![" + name + "](" + target + ")";
    }
    if (kind === "video") {
      return '<video controls preload="metadata" src="' + target + '"></video>';
    }
    if (kind === "audio") {
      return '<audio controls preload="metadata" src="' + target + '"></audio>';
    }
    return "[" + name + "](" + target + ")";
  }

  function insertNewCardDescriptionMarkup(textarea, markup) {
    var start = typeof textarea.selectionStart === "number"
      ? textarea.selectionStart : textarea.value.length;
    var end = typeof textarea.selectionEnd === "number" ? textarea.selectionEnd : start;
    var before = textarea.value.slice(0, start);
    var after = textarea.value.slice(end);
    var leading = before && !before.endsWith("\n") ? "\n\n" : "";
    var trailing = after && !after.startsWith("\n") ? "\n\n" : "";
    var inserted = leading + markup + trailing;
    textarea.value = before + inserted + after;
    var cursor = before.length + inserted.length;
    textarea.focus();
    textarea.setSelectionRange(cursor, cursor);
  }

  function formatNewCardFileSize(size) {
    if (size >= 1024 * 1024) return (size / (1024 * 1024)).toFixed(1) + " MB";
    if (size >= 1024) return Math.ceil(size / 1024) + " KB";
    return size + " B";
  }

  function renderNewCardFiles(form) {
    var list = form.querySelector("[data-new-card-file-list]");
    if (!list) return;
    list.replaceChildren();
    list.hidden = !newCardFiles.length;
    newCardFiles.forEach(function (record, index) {
      var item = document.createElement("div");
      item.className = "new-card-file-item";
      if (record.preview) {
        var image = document.createElement("img");
        image.src = record.preview;
        image.alt = "";
        item.appendChild(image);
      } else {
        var icon = document.createElement("span");
        icon.className = "new-card-file-icon";
        icon.textContent = "↥";
        icon.setAttribute("aria-hidden", "true");
        item.appendChild(icon);
      }
      var meta = document.createElement("span");
      meta.className = "new-card-file-meta";
      var name = document.createElement("strong");
      name.textContent = record.file.name;
      var size = document.createElement("small");
      size.textContent = formatNewCardFileSize(record.file.size);
      meta.appendChild(name);
      meta.appendChild(size);
      item.appendChild(meta);
      var remove = document.createElement("button");
      remove.type = "button";
      remove.className = "ghost small";
      remove.textContent = "×";
      remove.setAttribute("aria-label", "Remove " + record.file.name);
      remove.addEventListener("click", function () {
        var removed = newCardFiles.splice(index, 1)[0];
        if (removed.preview) URL.revokeObjectURL(removed.preview);
        if (removed.insertedMarkup) {
          var textarea = form.querySelector("[data-new-card-description]");
          textarea.value = textarea.value.replace(removed.insertedMarkup, "");
        }
        renderNewCardFiles(form);
      });
      item.appendChild(remove);
      list.appendChild(item);
    });
  }

  function addNewCardFiles(form, files, embedInDescription) {
    var error = form.querySelector("[data-new-card-error]");
    var textarea = form.querySelector("[data-new-card-description]");
    Array.from(files || []).forEach(function (file) {
      if (newCardFiles.length >= NEW_CARD_MAX_FILES) {
        if (error) {
          error.textContent = "A card can have at most " + NEW_CARD_MAX_FILES + " files.";
          error.hidden = false;
        }
        return;
      }
      if (file.size > NEW_CARD_MAX_FILE_BYTES) {
        if (error) {
          error.textContent = file.name + " exceeds the 25 MB file limit.";
          error.hidden = false;
        }
        return;
      }
      var duplicate = newCardFiles.some(function (record) {
        return record.file.name === file.name && record.file.size === file.size &&
          record.file.lastModified === file.lastModified;
      });
      if (duplicate) return;
      var token = newCardFileToken();
      var markup = newCardFileMarkup(file, token);
      var record = {
        file: file,
        token: token,
        preview: newCardFileMediaKind(file) === "image"
          ? URL.createObjectURL(file) : "",
        insertedMarkup: embedInDescription ? markup : "",
      };
      newCardFiles.push(record);
      if (embedInDescription && textarea) {
        insertNewCardDescriptionMarkup(textarea, markup);
      }
    });
    renderNewCardFiles(form);
  }

  function refreshCurrentPageAfterCardCreate() {
    var url = window.location.pathname + window.location.search;
    if (typeof htmx !== "undefined") {
      htmx.ajax("GET", url, { target: "#app-view", swap: "innerHTML" });
    } else {
      window.location.reload();
    }
  }

  function initNewCardForm(form) {
    addNewCardLinkRow(form);
    var fileInput = form.querySelector("[data-new-card-files]");
    var textarea = form.querySelector("[data-new-card-description]");
    var error = form.querySelector("[data-new-card-error]");
    if (fileInput) {
      fileInput.addEventListener("change", function () {
        addNewCardFiles(form, fileInput.files, false);
        fileInput.value = "";
      });
    }
    if (textarea) {
      textarea.addEventListener("dragover", function (event) {
        if (!event.dataTransfer ||
            Array.from(event.dataTransfer.types || []).indexOf("Files") === -1) {
          return;
        }
        event.preventDefault();
        event.dataTransfer.dropEffect = "copy";
        textarea.classList.add("is-file-drop-target");
      });
      textarea.addEventListener("dragleave", function (event) {
        if (!textarea.contains(event.relatedTarget)) {
          textarea.classList.remove("is-file-drop-target");
        }
      });
      textarea.addEventListener("drop", function (event) {
        if (!event.dataTransfer || !event.dataTransfer.files.length) return;
        event.preventDefault();
        textarea.classList.remove("is-file-drop-target");
        addNewCardFiles(form, event.dataTransfer.files, true);
      });
    }
    form.addEventListener("click", function (event) {
      if (event.target.closest("[data-new-card-add-link]")) {
        addNewCardLinkRow(form);
        return;
      }
      var remove = event.target.closest("[data-new-card-remove-link]");
      if (remove) remove.closest(".new-card-link-row").remove();
    });
    form.addEventListener("keydown", function (event) {
      if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        form.requestSubmit();
      }
    });
    form.addEventListener("submit", function (event) {
      event.preventDefault();
      if (!form.reportValidity()) return;
      var submit = form.querySelector("[data-new-card-submit]");
      var formData = new FormData(form);
      newCardFiles.forEach(function (record) {
        formData.append("files", record.file, record.file.name);
        formData.append("file_tokens", record.token);
      });
      if (error) error.hidden = true;
      if (submit) {
        submit.disabled = true;
        submit.textContent = "Creating…";
      }
      fetch(form.action, {
        method: "POST",
        credentials: "same-origin",
        headers: csrfHeader(),
        body: formData,
      })
        .then(function (response) {
          return response.json().catch(function () { return {}; }).then(function (data) {
            if (!response.ok) throw new Error(newCardErrorMessage(data));
            return data;
          });
        })
        .then(function () {
          showToast("Card created.", "success");
          closeNewCardDialog();
          refreshCurrentPageAfterCardCreate();
        })
        .catch(function (failure) {
          if (error) {
            error.textContent = failure.message || "The card could not be created.";
            error.hidden = false;
          }
        })
        .finally(function () {
          if (submit) {
            submit.disabled = false;
            submit.textContent = "Create card";
          }
        });
    });
  }

  function openNewCardDialog(opener) {
    var content = newCardDialogContent();
    if (!content) return;
    newCardDialogOpener = opener || document.activeElement;
    clearNewCardFiles();
    if (newCardRequest) newCardRequest.abort();
    newCardRequest = new AbortController();
    content.innerHTML = '<div class="card-dialog-state" role="status"><p>Loading new card…</p></div>';
    showNewCardDialog();
    fetch(newCardContextUrl(), {
      credentials: "same-origin",
      signal: newCardRequest.signal,
    })
      .then(function (response) {
        if (!response.ok) throw new Error("The new-card form could not be loaded.");
        return response.text();
      })
      .then(function (html) {
        content.innerHTML = html;
        var form = content.querySelector("[data-new-card-form]");
        if (form) initNewCardForm(form);
        var focus = content.querySelector("[autofocus]");
        if (focus) focus.focus({ preventScroll: true });
      })
      .catch(function (error) {
        if (error.name === "AbortError") return;
        content.innerHTML = '<div class="card-dialog-state" role="alert"><p>' +
          String(error.message).replace(/[&<>]/g, function (character) {
            return { "&": "&amp;", "<": "&lt;", ">": "&gt;" }[character];
          }) + '</p><button type="button" class="ghost" data-new-card-close>Close</button></div>';
      });
  }

  function initAgentReconnect() {
    document.querySelectorAll("#pa-agent-reconnect").forEach(function (btn) {
      if (btn.dataset.bound) return;
      btn.dataset.bound = "1";
      var retryTimer = null;
      var retryCount = 0;
      function reconnect() {
        retryTimer = null;
        if (!btn.isConnected) return;
        btn.disabled = true;
        btn.setAttribute("aria-busy", "true");
        fetch("/api/agent/reconnect", {
          method: "POST",
          headers: csrfHeader(),
        })
          .then(function (resp) {
            return resp.json().then(function (data) {
              if (!resp.ok) {
                var detail = data.detail || {};
                var error = new Error(detail.message || "Reconnect failed");
                error.detail = detail;
                throw error;
              }
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
            var detail = err.detail || {};
            if (detail.code === "agent_recovery_in_progress") {
              var label = btn.querySelector(".status-label");
              if (label) label.textContent = "Restoring sessions…";
              btn.title = detail.message || "Restoring durable agent sessions";
              var baseDelay = Math.max(250, Number(detail.retry_after_ms || 250));
              var delay = Math.min(5000, baseDelay * Math.pow(2, retryCount++));
              retryTimer = window.setTimeout(reconnect, delay);
              return;
            }
            showToast(err.message || "Reconnect failed", "error");
          })
          .finally(function () {
            if (!retryTimer) {
              btn.disabled = false;
              btn.removeAttribute("aria-busy");
            }
          });
      }
      btn.addEventListener("click", reconnect);
    });
  }

  document.body.addEventListener("htmx:configRequest", function (event) {
    var headers = csrfHeader();
    var target = event.detail && event.detail.headers;
    if (!target) return;
    Object.keys(headers).forEach(function (key) {
      target[key] = headers[key];
    });
  });

  function handleAfterSwap(event) {
    var target = swapTarget(event);
    // HTMX 4 variants disagree about whether the after-swap target is the
    // source element, selector text, or replaced node. Re-scan the document so
    // every swapped Markdown surface is idempotently rendered from its source.
    renderCardMarkdown(document);
    if (target && target.id === "app-view") {
      setActiveNav(window.location.pathname);
      updateTitle();
      initBoardDragDrop(target);
      initBoardLiveUpdates();
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
      initCardDetail(target.querySelector("[data-card-detail]"), currentCardTab());
      if (window.PAAgentChat && typeof window.PAAgentChat.mount === "function") {
        window.PAAgentChat.mount(target);
      }
      document.body.dispatchEvent(new CustomEvent("boardRefresh"));
    }
  }
  document.body.addEventListener("htmx:afterSwap", handleAfterSwap);

  function handleHistoryUpdate() {
    setActiveNav(window.location.pathname);
    updateTitle();
  }
  document.body.addEventListener("htmx:pushedIntoHistory", handleHistoryUpdate);
  document.body.addEventListener("htmx:replacedInHistory", handleHistoryUpdate);

  document.addEventListener("htmx:historyRestore", function () {
    restoreCardFromHistory();
  });

  document.body.addEventListener("htmx:responseError", function (event) {
    var source = (event.detail && event.detail.elt) ||
      event.target;
    var form = source && typeof source.matches === "function"
      ? (source.matches("form") ? source : source.closest("form"))
      : null;
    var operation = form && form.dataset.operation;
    var message = "Request failed";
    var xhr = event.detail && event.detail.xhr;
    var text = xhr && xhr.responseText;
    var statusText = xhr && xhr.statusText;
    if (text) {
      try {
        const data = JSON.parse(text);
        message = data.detail || data.message || message;
        if (Array.isArray(message)) {
          message = message.map(function (item) {
            var location = Array.isArray(item.loc) ? item.loc : [];
            var field = location.filter(function (part) {
              return part !== "body" && part !== "query" && part !== "path";
            }).pop();
            return (field ? field + ": " : "") + (item.msg || String(item));
          }).join("; ");
        }
      } catch (_err) {
        message = statusText || message;
      }
    } else if (statusText) {
      message = statusText;
    }
    if (operation) message = operation + " — " + message;
    showToast(message, "error");
  });

  document.body.addEventListener("pa:navigationError", function (event) {
    var detail = event.detail || {};
    var error = detail.error || {};
    showToast(error.message || "Navigation failed", "error");
    console.error("PA navigation failed", {
      operation: "spa-navigation", url: detail.url || "", status: detail.status || "network"
    });
  });

  window.addEventListener("popstate", function (event) {
    // HTMX owns entries carrying its state marker. Handling them here as well
    // starts a second restoration request and can duplicate live controllers.
    if (event.state && event.state.htmx) return;
    var url = new URL(window.location.href);
    var cardId = url.searchParams.get("card");
    if (cardId) {
      cardDialogBackNavigation = false;
      cardDialogHistoryDepth = Number(event.state && event.state.paCardDepth || 0);
      cardDialogHasBaseEntry = !!(event.state && event.state.paCardHasBase);
      restoreCardFromHistory();
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
    if (!window.PANavigation) return;
    window.PANavigation.navigate(window.location.href, { history: false }).catch(function () {});
    setActiveNav(window.location.pathname);
    updateTitle();
  });


  document.body.addEventListener("submit", function (event) {
    var form = event.target.closest("[data-card-dispatch-form]");
    if (!form) return;
    event.preventDefault();
    var detail = form.closest("[data-card-dispatch-context]");
    var key = form.dataset.idempotencyKey || dispatchOperationKey("card-dispatch:" + detail.dataset.cardId);
    form.dataset.idempotencyKey = key;
    var payload = cardDispatchPayload(form, detail);
    payload.idempotency_key = key;
    var submit = form.querySelector('button[type="submit"]');
    submit.disabled = true;
    submit.textContent = "Checking fleet…";
    fetch("/api/fleet/dispatch", {
      method: "POST",
      credentials: "same-origin",
      headers: Object.assign({"Content-Type": "application/json"}, csrfHeader()),
      body: JSON.stringify(payload),
    })
      .then(function (response) {
        return response.json().catch(function () { return {}; }).then(function (data) {
          if (!response.ok) throw data;
          return data;
        });
      })
      .then(function (result) {
        renderCardDispatch(detail, result.dispatch);
        pollCardDispatch(detail, result.dispatch_id, 500);
        hideCardDispatchActions(detail.dataset.cardId);
        document.body.dispatchEvent(new CustomEvent("boardRefresh"));
      })
      .catch(function (error) {
        renderCardDispatchError(detail, error);
        var dispatchError = error && error.detail ? error.detail : error || {};
        if (dispatchError.code === "card_dispatch_in_progress") {
          hideCardDispatchActions(detail.dataset.cardId);
        }
        submit.disabled = false;
        submit.textContent = "Retry dispatch";
      });
  });

  document.body.addEventListener("change", function (event) {
    if (event.target && ["dispatch_target", "worker_group", "execution_profile",
      "provider", "model_id", "participation_override",
      "participation_override_reason"].indexOf(event.target.name) !== -1) {
      var dispatchForm = event.target.closest("[data-card-dispatch-form]");
      if (event.target.name === "dispatch_target" || event.target.name === "provider") {
        refreshCardDispatchSelectors(dispatchForm);
      }
      updateCardDispatchUtilization(dispatchForm);
      previewCardPlacement(dispatchForm);
    }
  });

  document.body.addEventListener("click", function (event) {
    var dispatchOpen = event.target.closest("[data-card-dispatch-open]");
    if (dispatchOpen) {
      event.preventDefault();
      openCardDispatchDialog(dispatchOpen.dataset.cardId, dispatchOpen.dataset.cardRealm, dispatchOpen);
      return;
    }
    if (event.target.closest("[data-card-dispatch-close]")) {
      closeCardDispatchDialog();
      return;
    }
    var newCardOpen = event.target.closest("[data-new-card-open]");
    if (newCardOpen) {
      event.preventDefault();
      openNewCardDialog(newCardOpen);
      return;
    }
    if (event.target.closest("[data-new-card-close]")) {
      closeNewCardDialog();
      return;
    }
    var detailLink = event.target.closest("[data-card-detail-link]");
    if (detailLink) {
      event.preventDefault();
      cardDialogOpener = detailLink;
      loadCardDetail(
        detailLink.dataset.cardId,
        detailLink.dataset.cardRealm || "default",
        true,
        "summary"
      );
      return;
    }
    if (event.target.closest("[data-card-dialog-close]")) {
      closeCardDialog(true);
      return;
    }
    var detail = event.target.closest("[data-card-detail], [data-card-dispatch-context]");
    if (!detail) return;
    var cardTab = event.target.closest("[data-card-tab]");
    if (cardTab) {
      activateCardTab(detail, cardTab.dataset.cardTab, true, false);
      return;
    }
    var tabRetry = event.target.closest("[data-card-tab-retry]");
    if (tabRetry) {
      var retryPanel = tabRetry.closest("[data-card-tab-panel]");
      if (retryPanel) {
        retryPanel.dataset.cardTabState = "";
        loadCardTab(detail, retryPanel.dataset.cardTabPanel);
      }
      return;
    }
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
    var dispatchRetry = event.target.closest("[data-card-dispatch-retry]");
    if (dispatchRetry) {
      var retryId = dispatchRetry.dataset.cardDispatchRetry;
      dispatchRetry.disabled = true;
      fetch("/api/fleet/dispatch-jobs/" + encodeURIComponent(retryId) + "/retry", {
        method: "POST",
        credentials: "same-origin",
        headers: Object.assign({"Content-Type": "application/json"}, csrfHeader()),
        body: JSON.stringify({idempotency_key: dispatchOperationKey("dispatch-retry:" + retryId)}),
      })
        .then(function (response) {
          return response.json().then(function (data) {
            if (!response.ok) throw data;
            return data;
          });
        })
        .then(function (dispatch) {
          renderCardDispatch(detail, dispatch);
          pollCardDispatch(detail, retryId, 300);
        })
        .catch(function (error) {
          renderCardDispatchError(detail, error);
          dispatchRetry.disabled = false;
        });
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
    var cardTab = event.target.closest("[data-card-tab]");
    if (cardTab && event.target === cardTab) {
      var detail = cardTab.closest("[data-card-detail]");
      var tabs = Array.prototype.slice.call(detail.querySelectorAll("[data-card-tab]"));
      var index = tabs.indexOf(cardTab);
      var nextIndex = null;
      if (event.key === "ArrowRight" || event.key === "ArrowDown") nextIndex = (index + 1) % tabs.length;
      if (event.key === "ArrowLeft" || event.key === "ArrowUp") nextIndex = (index - 1 + tabs.length) % tabs.length;
      if (event.key === "Home") nextIndex = 0;
      if (event.key === "End") nextIndex = tabs.length - 1;
      if (nextIndex !== null) {
        event.preventDefault();
        activateCardTab(detail, tabs[nextIndex].dataset.cardTab, true, true);
      }
      return;
    }
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
    initBoardLiveUpdates();
    initAgentReconnect();
    decorateLinks(document);
    renderCardMarkdown(document);
    observeMarkdownMutations();
    checkServerBuild();
    var dialog = cardDialog();
    if (dialog) {
      dialog.addEventListener("cancel", function (event) {
        event.preventDefault();
        closeCardDialog(true);
      });
    }
    var createDialog = newCardDialog();
    if (createDialog) {
      createDialog.addEventListener("cancel", function (event) {
        event.preventDefault();
        closeNewCardDialog();
      });
    }
    openCardFromLocation();
    window.setInterval(checkServerBuild, VERSION_POLL_MS);
  });
})();
