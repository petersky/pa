/* Open a conversation in a shell-level dialog from any page.
 *
 * Any link to /agent?session=<id> outside the chat page itself opens here instead of
 * navigating away; modified clicks (new tab/window), target=_blank and `data-no-chat-modal`
 * keep the normal link. The dialog hosts one ordinary chat widget (see
 * partials/agent/chat-modal.html) and tears it down on close so no SSE stream is left open.
 */
(function () {
  "use strict";

  var LINK = /^\/agent\?(?:.*&)?session=/;
  var opener = null;
  var request = null;
  var openCount = 0;

  function dialog() {
    return document.getElementById("chat-modal-dialog");
  }

  function content() {
    return document.getElementById("chat-modal-dialog-content");
  }

  function widgetRoot() {
    var host = content();
    return host ? host.querySelector("[data-agent-chat]") : null;
  }

  function teardown() {
    if (request) request.abort();
    request = null;
    var host = content();
    if (!host) return;
    if (window.PAAgentChat && typeof window.PAAgentChat.destroy === "function") {
      window.PAAgentChat.destroy(host, "chat-modal-closed");
    }
    host.replaceChildren();
  }

  function close() {
    var dlg = dialog();
    if (dlg && dlg.open) dlg.close();
    finish();
  }

  function stateMarkup(title, message, retry) {
    var wrapper = document.createElement("div");
    wrapper.className = "card-dialog-state";
    wrapper.setAttribute("role", "alert");
    var heading = document.createElement("h2");
    heading.textContent = title;
    var text = document.createElement("p");
    text.textContent = message;
    var actions = document.createElement("div");
    actions.className = "form-actions";
    var closeButton = document.createElement("button");
    closeButton.type = "button";
    closeButton.setAttribute("data-chat-modal-close", "");
    closeButton.textContent = "Close";
    actions.appendChild(closeButton);
    if (retry) actions.appendChild(retry);
    wrapper.append(heading, text, actions);
    return wrapper;
  }

  function open(sessionId, options) {
    var dlg = dialog();
    var host = content();
    if (!dlg || !host || !sessionId) return false;
    options = options || {};
    if (request) request.abort();
    teardown();
    opener = options.opener || document.activeElement;
    host.innerHTML = '<div class="card-dialog-state" role="status"><p>Loading conversation…</p></div>';
    if (!dlg.open) {
      if (typeof dlg.showModal === "function") dlg.showModal();
      else dlg.setAttribute("open", "");
    }
    var token = ++openCount;
    var params = new URLSearchParams({ session: sessionId });
    if (options.instance) params.set("instance", options.instance);
    if (options.card) params.set("card", options.card);
    request = new AbortController();
    fetch("/partials/agent/chat-modal?" + params.toString(), {
      credentials: "same-origin",
      signal: request.signal,
    }).then(function (response) {
      if (!response.ok) {
        throw new Error(response.status === 404
          ? "This conversation is not available on this instance."
          : "The conversation could not be loaded.");
      }
      return response.text();
    }).then(function (html) {
      if (token !== openCount) return;
      host.innerHTML = html;
      if (window.PAAgentChat && typeof window.PAAgentChat.mount === "function") {
        window.PAAgentChat.mount(host);
      }
      var root = widgetRoot();
      if (root && root._acw) {
        root._acw.openSession(sessionId, options.instance || "", { replace: true }).catch(function () {});
      }
      var input = host.querySelector("[data-acw-input]");
      if (input) input.focus();
    }).catch(function (error) {
      if (error.name === "AbortError" || token !== openCount) return;
      host.replaceChildren(stateMarkup("Conversation unavailable", error.message));
    });
    return true;
  }

  function sessionLink(anchor) {
    if (!anchor || anchor.hasAttribute("data-no-chat-modal")) return null;
    if (anchor.target && anchor.target !== "_self") return null;
    if (anchor.closest(".page-agent, #chat-modal-dialog")) return null;
    var raw = anchor.getAttribute("href") || "";
    if (!LINK.test(raw)) return null;
    var url;
    try {
      url = new URL(raw, window.location.origin);
    } catch (_error) {
      return null;
    }
    var sessionId = url.searchParams.get("session");
    if (!sessionId) return null;
    return { session: sessionId, instance: url.searchParams.get("instance") || "" };
  }

  document.addEventListener("click", function (event) {
    if (event.defaultPrevented || event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    var closer = event.target.closest && event.target.closest("[data-chat-modal-close]");
    if (closer) {
      event.preventDefault();
      close();
      return;
    }
    var anchor = event.target.closest && event.target.closest("a[href]");
    var target = sessionLink(anchor);
    if (!target) return;
    if (!dialog()) return;
    event.preventDefault();
    open(target.session, { instance: target.instance, opener: anchor });
  });

  // Cleanup must not depend on the dialog's "close" event alone: it is queued asynchronously
  // and can arrive late (or not at all while the tab is starved), which would leave the
  // widget and its event stream running behind a closed dialog. "cancel" (Esc) and "close"
  // both finish the job, and finishing twice is harmless.
  function restoreFocus() {
    var dlg = dialog();
    if (dlg && dlg.open) return; // Still modal: the page behind it cannot take focus yet.
    if (opener && document.contains(opener) && typeof opener.focus === "function") opener.focus();
    opener = null;
  }

  function finish() {
    teardown();
    restoreFocus();
  }

  function finishAfterCancel() {
    teardown();
    // The dialog closes right after "cancel" returns; focus can only move once it has.
    setTimeout(restoreFocus, 0);
  }

  document.addEventListener("DOMContentLoaded", function () {
    var dlg = dialog();
    if (!dlg) return;
    dlg.addEventListener("cancel", finishAfterCancel);
    dlg.addEventListener("close", finish);
    // A click on the backdrop (the dialog element itself, outside its content box) closes.
    dlg.addEventListener("mousedown", function (event) {
      if (event.target === dlg) dlg.close();
    });
  });

  window.PAChatModal = { open: open, close: close };
})();
