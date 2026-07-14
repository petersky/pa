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
  }

  function initSections(root) {
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

  document.addEventListener("click", function (e) {
    var link = e.target.closest("[data-section-link]");
    if (!link) return;
    var layout = link.closest(".page-layout");
    if (!layout) return;
    e.preventDefault();
    showSection(layout, link.getAttribute("data-section-link"));
  });

  function boot(root) {
    initResize(root);
    initSections(root);
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
  document.body.addEventListener("htmx:after:swap", function (evt) {
    var target =
      (evt.detail && evt.detail.ctx && evt.detail.ctx.target) ||
      (evt.detail && evt.detail.target) ||
      null;
    if (target && target.id === "app-view") boot(target);
  });

  window.PALayout = { boot: boot, showSection: showSection };
})();
