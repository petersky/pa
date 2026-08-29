(function () {
  if (window.PANavigation) return;

  var generation = 0;
  var active = null;

  function expectedCancellation(error, requestGeneration) {
    return requestGeneration !== generation ||
      (error && error.name === "AbortError");
  }

  function cancel(reason) {
    generation += 1;
    var request = active;
    active = null;
    if (request) request.controller.abort(reason || "pa-navigation-superseded");
  }

  function navigate(rawUrl, options) {
    options = options || {};
    var url = new URL(rawUrl, window.location.href);
    if (url.origin !== window.location.origin) {
      window.location.assign(url.href);
      return Promise.resolve(false);
    }
    cancel("pa-navigation-superseded");
    var requestGeneration = generation;
    var controller = new AbortController();
    var target = document.querySelector("#app-view");
    if (!target || !window.htmx) {
      window.location.assign(url.href);
      return Promise.resolve(false);
    }
    var headers = {
      Accept: "text/html",
      "HX-Request": "true",
      "HX-Target": "app-view",
      "X-PA-Navigation-Generation": String(requestGeneration)
    };
    if (window.PACSRF) headers = window.PACSRF.headers(headers);
    else {
      var csrf = document.querySelector('meta[name="csrf-token"]');
      if (csrf && csrf.content) headers["X-CSRF-Token"] = csrf.content;
    }
    var request = {
      controller: controller,
      generation: requestGeneration,
      url: url.href
    };
    active = request;
    return fetch(url.href, {
      credentials: "same-origin",
      headers: headers,
      signal: controller.signal
    }).then(function (response) {
      if (requestGeneration !== generation) return null;
      if (response.status === 204 || response.status === 304) return null;
      if (!response.ok) {
        var failure = new Error(response.statusText || "Navigation failed");
        failure.status = response.status;
        throw failure;
      }
      return response.text();
    }).then(function (html) {
      if (html === null || requestGeneration !== generation) return false;
      htmx.swap(target, html, { swapStyle: "innerHTML" });
      if (options.history === "replace") history.replaceState({ paNavigation: true }, "", url.href);
      else if (options.history !== false) history.pushState({ paNavigation: true }, "", url.href);
      if (options.history !== false) {
        htmx.trigger(document.body, options.history === "replace"
          ? "htmx:replacedInHistory" : "htmx:pushedIntoHistory", { path: url.pathname + url.search });
      }
      return true;
    }).catch(function (error) {
      if (expectedCancellation(error, requestGeneration)) return false;
      document.body.dispatchEvent(new CustomEvent("pa:navigationError", {
        detail: { error: error, status: Number(error && error.status || 0), url: url.pathname + url.search }
      }));
      throw error;
    }).finally(function () {
      if (active === request) active = null;
    });
  }

  function managedLink(event) {
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey ||
        event.shiftKey || event.altKey) return null;
    var link = event.target && event.target.closest && event.target.closest("a[href][hx-get]");
    if (!link || link.target || link.hasAttribute("download")) return null;
    if (link.getAttribute("hx-target") !== "#app-view") return null;
    var url = new URL(link.href, window.location.href);
    return url.origin === window.location.origin ? { link: link, url: url } : null;
  }

  document.addEventListener("click", function (event) {
    var match = managedLink(event);
    if (!match) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    navigate(match.url.href).catch(function () {});
  }, true);

  document.addEventListener("click", function (event) {
    document.querySelectorAll("[data-responsive-nav][open]").forEach(function (menu) {
      if (!menu.contains(event.target)) menu.removeAttribute("open");
    });
  });

  document.addEventListener("keydown", function (event) {
    if (event.key !== "Escape") return;
    document.querySelectorAll("[data-responsive-nav][open]").forEach(function (menu) {
      menu.removeAttribute("open");
      var summary = menu.querySelector("summary");
      if (summary) summary.focus();
    });
  });
  document.addEventListener("pa:historyWillReload", function () { cancel("pa-history-reload"); });
  window.addEventListener("pagehide", function () { cancel("pa-pagehide"); });

  window.PANavigation = { navigate: navigate, cancel: cancel };
})();
