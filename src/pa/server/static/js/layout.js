/**
 * Shared layout behaviors: resizable right sidebar + page section nav.
 */
(function () {
  if (window.__paLayoutBound) return;
  window.__paLayoutBound = true;

  var RIGHT_KEY = "pa.sidebar.rightWidth";
  var MIN_RIGHT = 180;
  var MAX_RIGHT = 560;

  function storageGet(key, fallback) {
    try {
      var v = localStorage.getItem(key);
      return v != null ? v : fallback;
    } catch (e) {
      return fallback;
    }
  }

  function storageSet(key, value) {
    try {
      localStorage.setItem(key, String(value));
    } catch (e) {}
  }

  function applyRightWidth(layout, px) {
    if (!layout) return;
    var width = Math.max(MIN_RIGHT, Math.min(MAX_RIGHT, Math.round(px)));
    layout.style.setProperty("--pa-sidebar-right-width", width + "px");
    var sidebar = layout.querySelector('[data-resizable-sidebar="right"]');
    if (sidebar) sidebar.style.width = width + "px";
    return width;
  }

  function initResize(root) {
    var scope = root || document;
    scope.querySelectorAll(".page-layout[data-has-right]").forEach(function (layout) {
      if (layout.dataset.resizeReady) return;
      layout.dataset.resizeReady = "1";
      var saved = parseInt(storageGet(RIGHT_KEY, "240"), 10);
      if (!isNaN(saved)) applyRightWidth(layout, saved);

      var handle = layout.querySelector('[data-resize-side="right"]');
      if (!handle) return;

      function startDrag(clientX) {
        var startX = clientX;
        var startWidth =
          parseInt(getComputedStyle(layout).getPropertyValue("--pa-sidebar-right-width"), 10) ||
          240;
        document.body.classList.add("is-resizing-sidebar");

        function onMove(ev) {
          var x = ev.touches ? ev.touches[0].clientX : ev.clientX;
          // Dragging left grows the right sidebar.
          var next = startWidth + (startX - x);
          applyRightWidth(layout, next);
        }

        function onUp() {
          document.body.classList.remove("is-resizing-sidebar");
          document.removeEventListener("mousemove", onMove);
          document.removeEventListener("mouseup", onUp);
          document.removeEventListener("touchmove", onMove);
          document.removeEventListener("touchend", onUp);
          var w =
            parseInt(getComputedStyle(layout).getPropertyValue("--pa-sidebar-right-width"), 10) ||
            240;
          storageSet(RIGHT_KEY, w);
        }

        document.addEventListener("mousemove", onMove);
        document.addEventListener("mouseup", onUp);
        document.addEventListener("touchmove", onMove, { passive: true });
        document.addEventListener("touchend", onUp);
      }

      handle.addEventListener("mousedown", function (e) {
        e.preventDefault();
        startDrag(e.clientX);
      });
      handle.addEventListener("touchstart", function (e) {
        if (!e.touches || !e.touches[0]) return;
        startDrag(e.touches[0].clientX);
      }, { passive: true });

      handle.addEventListener("keydown", function (e) {
        var cur =
          parseInt(getComputedStyle(layout).getPropertyValue("--pa-sidebar-right-width"), 10) ||
          240;
        if (e.key === "ArrowLeft") {
          e.preventDefault();
          storageSet(RIGHT_KEY, applyRightWidth(layout, cur + 16));
        } else if (e.key === "ArrowRight") {
          e.preventDefault();
          storageSet(RIGHT_KEY, applyRightWidth(layout, cur - 16));
        }
      });
    });
  }

  function showSection(root, sectionId) {
    if (!root || !sectionId) return;
    var previous = root.dataset.activeSection || "";
    if (previous !== sectionId) {
      root.dispatchEvent(new CustomEvent("pa:section-will-change", {
        bubbles: true,
        detail: { layout: root, from: previous, to: sectionId }
      }));
    }
    root.querySelectorAll("[data-section]").forEach(function (el) {
      var show = el.getAttribute("data-section") === sectionId;
      el.hidden = !show;
      el.classList.toggle("hidden", !show);
    });
    root.querySelectorAll("[data-section-link]").forEach(function (btn) {
      btn.classList.toggle("active", btn.getAttribute("data-section-link") === sectionId);
    });
    try {
      var url = new URL(window.location.href);
      url.searchParams.set("section", sectionId);
      history.replaceState(null, "", url.toString());
    } catch (e) {}
    root.dataset.activeSection = sectionId;
    root.dispatchEvent(new CustomEvent("pa:section-changed", {
      bubbles: true,
      detail: { layout: root, from: previous, to: sectionId }
    }));
  }

  function preserveSettingsDraft() {
    var form = document.getElementById("pa-agent-prefs-form");
    if (!form) return;
    var values = {};
    Array.from(form.elements).forEach(function (field) {
      if (!field.name || field.type === "password") return;
      values[field.name] = field.type === "checkbox" ? field.checked : field.value;
    });
    try { sessionStorage.setItem("pa:settings:agent-draft", JSON.stringify(values)); } catch (_error) {}
  }

  function restoreSettingsDraft() {
    var form = document.getElementById("pa-agent-prefs-form");
    if (!form) return;
    var values = null;
    try { values = JSON.parse(sessionStorage.getItem("pa:settings:agent-draft") || "null"); } catch (_error) {}
    if (!values) return;
    Object.keys(values).forEach(function (name) {
      var field = form.elements[name];
      if (!field) return;
      if (field.type === "checkbox") field.checked = !!values[name];
      else field.value = values[name];
    });
  }

  function initSections(root) {
    restoreSettingsDraft();
    var scope = root || document;
    scope.querySelectorAll(".page-layout").forEach(function (layout) {
      var links = layout.querySelectorAll("[data-section-link]");
      if (!links.length) return;
      if (layout.dataset.sectionsReady) return;
      layout.dataset.sectionsReady = "1";

      var initial = null;
      try {
        initial = new URL(window.location.href).searchParams.get("section");
      } catch (e) {}
      if (!initial || !layout.querySelector('[data-section="' + initial + '"]')) {
        var active = layout.querySelector("[data-section-link].active");
        initial = active
          ? active.getAttribute("data-section-link")
          : links[0].getAttribute("data-section-link");
      }
      showSection(layout, initial);
    });
  }

  function initProjectFilters(root) {
    var scope = root || document;
    scope.querySelectorAll("[data-projects-filter]").forEach(function (input) {
      if (input.dataset.filterReady) return;
      input.dataset.filterReady = "1";
      var listName = input.getAttribute("data-projects-filter");
      var list = scope.querySelector('[data-projects-list="' + listName + '"]');
      if (!list) return;

      input.addEventListener("input", function () {
        var query = input.value.trim().toLowerCase();
        var visible = 0;
        list.querySelectorAll("[data-projects-name]").forEach(function (item) {
          var matches = !query ||
            (item.getAttribute("data-projects-name") || "").indexOf(query) !== -1;
          item.hidden = !matches;
          if (matches) visible += 1;
        });
        var empty = list.querySelector(".projects-filter-empty");
        if (empty) empty.hidden = visible !== 0;
      });
    });
  }

  document.addEventListener("click", function (e) {
    var link = e.target.closest("[data-section-link]");
    if (!link) return;
    var layout = link.closest(".page-layout");
    if (!layout) return;
    var settingsMarker = document.querySelector("[data-settings-loaded-section]");
    var requestedSection = link.getAttribute("data-section-link");
    if (settingsMarker && settingsMarker.getAttribute("data-settings-loaded-section") !== requestedSection) {
      e.preventDefault();
      preserveSettingsDraft();
      var url = new URL(window.location.href);
      url.searchParams.set("section", requestedSection);
      link.textContent = "Loading…";
      layout.setAttribute("aria-busy", "true");
      window.location.assign(url.toString());
      return;
    }
    e.preventDefault();
    showSection(layout, requestedSection);
  });

  // The app shell keeps the document itself at overflow:hidden and scrolls
  // .page-main instead, so scroll keys pressed with body focus would
  // otherwise do nothing. Route them to the page's scroll region.
  function findScrollRegion() {
    var main = document.querySelector(".page-main");
    if (!main) return null;
    if (main.scrollHeight > main.clientHeight + 1) return main;
    var candidates = main.querySelectorAll("*");
    for (var i = 0; i < candidates.length; i++) {
      var el = candidates[i];
      if (el.scrollHeight <= el.clientHeight + 1) continue;
      var overflowY = getComputedStyle(el).overflowY;
      if (overflowY === "auto" || overflowY === "scroll") return el;
    }
    return null;
  }

  document.addEventListener("keydown", function (e) {
    if (e.defaultPrevented || e.altKey || e.ctrlKey || e.metaKey) return;
    if (e.target !== document.body && e.target !== document.documentElement) return;
    var region = findScrollRegion();
    if (!region) return;
    var page = Math.max(40, region.clientHeight - 40);
    var delta = null;
    if (e.key === "ArrowDown") delta = 60;
    else if (e.key === "ArrowUp") delta = -60;
    else if (e.key === "PageDown") delta = page;
    else if (e.key === "PageUp") delta = -page;
    else if (e.key === " ") delta = e.shiftKey ? -page : page;
    else if (e.key === "Home" && !e.shiftKey) delta = -region.scrollTop;
    else if (e.key === "End" && !e.shiftKey) delta = region.scrollHeight;
    if (delta === null) return;
    e.preventDefault();
    region.scrollBy({ top: delta, behavior: "auto" });
  });

  function boot(root) {
    initResize(root);
    initSections(root);
    initProjectFilters(root);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      boot(document);
    });
  } else {
    boot(document);
  }

  document.body.addEventListener("htmx:afterSwap", function (evt) {
    var target = (evt.detail && evt.detail.target) || null;
    if (target && target.id === "app-view") boot(target);
  });

  window.PALayout = { boot: boot, showSection: showSection };
})();
