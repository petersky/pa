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
    root.querySelectorAll("[data-file-view]").forEach(function (button) {
      button.classList.toggle("active", button.dataset.fileView === name);
    });
    root.querySelectorAll("[data-file-view-panel]").forEach(function (panel) {
      panel.hidden = panel.dataset.fileViewPanel !== name;
    });
  }

  function mount(root) {
    var scope = root || document;
    scope.querySelectorAll("[data-file-browser]").forEach(function (browser) {
      if (browser.dataset.fileBrowserMounted === "1") return;
      browser.dataset.fileBrowserMounted = "1";
      browser.querySelectorAll("[data-file-view]").forEach(function (button) {
        button.addEventListener("click", function () { showView(browser, button.dataset.fileView); });
      });
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
  ["htmx:after:swap", "htmx:afterSwap"].forEach(function (name) {
    document.body && document.body.addEventListener(name, function (event) {
      mount(event.detail && event.detail.target || event.target || document);
    });
  });
})();
