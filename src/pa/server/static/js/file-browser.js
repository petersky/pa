(function () {
  "use strict";

  var HIGHLIGHT_URL = "https://cdn.jsdelivr.net/npm/highlight.js@11.11.1/lib/common.min.js";
  var highlightPromise = null;

  function loadHighlighting() {
    if (window.hljs) return Promise.resolve(window.hljs);
    if (highlightPromise) return highlightPromise;
    highlightPromise = new Promise(function (resolve, reject) {
      var script = document.createElement("script");
      script.src = HIGHLIGHT_URL;
      script.async = true;
      script.onload = function () { resolve(window.hljs); };
      script.onerror = reject;
      document.head.appendChild(script);
    }).catch(function () { return null; });
    return highlightPromise;
  }

  function showView(root, name) {
    var selectedTab = null;
    root.querySelectorAll('[role="tab"][data-file-view]').forEach(function (button) {
      var selected = button.dataset.fileView === name;
      button.classList.toggle("active", selected);
      button.setAttribute("aria-selected", selected ? "true" : "false");
      button.tabIndex = selected ? 0 : -1;
      if (selected) selectedTab = button;
    });
    if (!selectedTab) return null;
    root.querySelectorAll("[data-file-view-panel]").forEach(function (panel) {
      panel.hidden = panel.dataset.fileViewPanel !== name;
    });
    return selectedTab;
  }

  function handleTabKeydown(browser, button, event) {
    var tabs = Array.prototype.slice.call(
      browser.querySelectorAll('[role="tab"][data-file-view]')
    );
    var index = tabs.indexOf(button);
    var nextIndex = index;

    if (index < 0 || tabs.length < 1) return;
    if (event.key === "ArrowLeft") nextIndex = (index - 1 + tabs.length) % tabs.length;
    else if (event.key === "ArrowRight") nextIndex = (index + 1) % tabs.length;
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = tabs.length - 1;
    else return;

    event.preventDefault();
    var selectedTab = showView(browser, tabs[nextIndex].dataset.fileView);
    if (selectedTab) selectedTab.focus();
  }

  function mount(root) {
    var scope = root || document;
    scope.querySelectorAll("[data-file-browser]").forEach(function (browser) {
      if (browser.dataset.fileBrowserMounted === "1") return;
      browser.dataset.fileBrowserMounted = "1";
      var tabs = Array.prototype.slice.call(
        browser.querySelectorAll('[role="tab"][data-file-view]')
      );
      tabs.forEach(function (button) {
        button.addEventListener("click", function () { showView(browser, button.dataset.fileView); });
        button.addEventListener("keydown", function (event) {
          handleTabKeydown(browser, button, event);
        });
      });
      var selectedTab = tabs.find(function (button) {
        return button.getAttribute("aria-selected") === "true";
      }) || tabs.find(function (button) {
        return button.classList.contains("active");
      }) || tabs[0];
      if (selectedTab) showView(browser, selectedTab.dataset.fileView);
      var markdown = browser.querySelector("[data-file-markdown]");
      var source = browser.querySelector("[data-file-markdown-source]");
      if (markdown && source && window.PAAgentChat) {
        var render = window.PAAgentChat.renderMarkdownAsync || function (text) {
          return Promise.resolve(window.PAAgentChat.renderMarkdown(text));
        };
        render(source.value || "").then(function (html) {
          markdown.innerHTML = html;
          if (window.PALinks) window.PALinks.decorate(markdown);
        });
      }
      loadHighlighting().then(function (hljs) {
        if (!hljs) return;
        browser.querySelectorAll("[data-source-code]").forEach(function (code) {
          var raw = code.textContent || "";
          var language = code.dataset.language;
          var highlighted;
          try {
            highlighted = language && hljs.getLanguage(language)
              ? hljs.highlight(raw, { language: language }).value
              : hljs.highlightAuto(raw).value;
          } catch (_) {
            highlighted = raw.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
          }
          var focus = Number(code.dataset.focusLine || 0);
          code.innerHTML = highlighted.split("\n").map(function (line, index) {
            var number = index + 1;
            return '<span class="file-source-line' + (number === focus ? ' is-focused' : '') +
              '" data-line="' + number + '">' + line + "\n</span>";
          }).join("");
        });
        var focused = browser.querySelector(".file-source-line.is-focused");
        if (focused) focused.scrollIntoView({ block: "center" });
      });
    });
  }

  window.PAFileBrowser = { mount: mount };
  document.addEventListener("DOMContentLoaded", function () { mount(document); });
  document.body && document.body.addEventListener("htmx:afterSwap", function (event) {
    mount(event.detail && event.detail.target || event.target || document);
  });
})();
