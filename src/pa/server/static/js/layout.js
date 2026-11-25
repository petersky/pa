/**
 * Shared layout behaviors: resizable right sidebar + page section nav.
 */
(function () {
  if (window.__paLayoutBound) return;
  window.__paLayoutBound = true;

  // Transient menus only: inline disclosures and conversation reference panels
  // deliberately stay open while the user works elsewhere.
  var popupMenus = "details.acw-menu[open], details.acw-send-menu[open], " +
    "details[data-card-action-menu][open], details[data-responsive-nav][open], " +
    "details[data-account-menu][open]";
  document.addEventListener("click", function (event) {
    document.querySelectorAll(popupMenus).forEach(function (menu) {
      if (!menu.contains(event.target)) menu.open = false;
    });
    document.querySelectorAll("[data-agent-chat]").forEach(function (root) {
      var widget = root._acw;
      if (widget && widget.commandMenuOpen() &&
          !widget.els.commandMenu.contains(event.target) && event.target !== widget.els.input) {
        widget.closeCommandMenu();
      }
    });
  }, true);
  document.addEventListener("keydown", function (event) {
    if (event.key !== "Escape" || event.defaultPrevented) return;
    document.querySelectorAll(popupMenus).forEach(function (menu) {
      menu.open = false;
      var summary = menu.querySelector("summary");
      if (summary && menu.contains(document.activeElement)) summary.focus();
    });
  });

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
      var active = btn.getAttribute("data-section-link") === sectionId;
      btn.classList.toggle("active", active);
      if (active) btn.setAttribute("aria-current", "page");
      else btn.removeAttribute("aria-current");
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
      else {
        if (field.hasAttribute("data-saved-value")) {
          field.dataset.draftValue = values[name];
          if (values[name] && !Array.from(field.options).some(option => option.value === values[name])) {
            field.add(new Option(values[name], values[name]));
          }
        }
        field.value = values[name];
      }
    });
    form.dispatchEvent(new Event("pa:settings-draft-restored"));
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

  function initProjectCreateDialogs(root) {
    var scope = root || document;
    scope.querySelectorAll("[data-project-create-open]").forEach(function (button) {
      if (button.dataset.projectCreateReady) return;
      button.dataset.projectCreateReady = "1";
      var dialog = document.getElementById(button.dataset.projectCreateOpen);
      if (!dialog) return;

      function closeDialog() {
        var form = dialog.querySelector("form");
        if (form) form.reset();
        if (dialog.open && typeof dialog.close === "function") dialog.close();
        else dialog.removeAttribute("open");
        button.focus({ preventScroll: true });
      }

      button.addEventListener("click", function () {
        if (typeof dialog.showModal === "function") dialog.showModal();
        else dialog.setAttribute("open", "");
        var first = dialog.querySelector("input:not([type=hidden]), textarea, select");
        if (first) first.focus();
      });
      dialog.querySelectorAll("[data-project-create-close]").forEach(function (close) {
        close.addEventListener("click", closeDialog);
      });
      dialog.addEventListener("cancel", function (event) {
        event.preventDefault();
        closeDialog();
      });
      dialog.addEventListener("click", function (event) {
        if (event.target === dialog) closeDialog();
      });
    });
  }

  // Keep the configuration in the top layer so it never changes the workspace
  // height. Reopen the same project after an HTMX save replaces the app view.
  var projectSettingsReturn = null;
  var projectSettingsReady = new WeakSet();
  function initProjectSettings(root) {
    var scope = root || document;
    scope.querySelectorAll("[data-project-settings-dialog]").forEach(function (dialog) {
      if (projectSettingsReady.has(dialog)) return;
      projectSettingsReady.add(dialog);
      var workspace = dialog.closest(".projects-workspace");
      var button = workspace.querySelector("[data-project-settings-open]");
      var identity = workspace.dataset.projectsDisclosureScope;
      function openDialog() {
        // A settings save replaces the dialog during HTMX's settle phase.
        // Bind the replacement forms before making them interactive again.
        if (window.htmx) window.htmx.process(dialog);
        dialog.showModal();
        dialog.querySelector("#project-settings-title").focus();
      }
      function closeDialog() {
        projectSettingsReturn = null;
        dialog.querySelectorAll("form").forEach(function (form) { form.reset(); });
        dialog.close();
        button.focus({ preventScroll: true });
      }
      button.addEventListener("click", openDialog);
      dialog.querySelectorAll("[data-project-settings-close]").forEach(function (close) {
        close.addEventListener("click", closeDialog);
      });
      dialog.addEventListener("cancel", function (event) {
        event.preventDefault();
        closeDialog();
      });
      dialog.addEventListener("click", function (event) {
        if (event.target !== dialog) return;
        var rect = dialog.getBoundingClientRect();
        if (event.clientX < rect.left || event.clientX > rect.right ||
            event.clientY < rect.top || event.clientY > rect.bottom) closeDialog();
      });
      dialog.addEventListener("htmx:beforeRequest", function () {
        projectSettingsReturn = {
          identity: identity,
          scrollTop: dialog.scrollTop,
          openSections: Object.fromEntries(Array.from(dialog.querySelectorAll("details")).map(function (details) {
            return [projectDisclosureKey(details.querySelector("summary").textContent), details.open];
          }))
        };
      });
      if (projectSettingsReturn && projectSettingsReturn.identity === identity) {
        openDialog();
        dialog.querySelectorAll("details").forEach(function (details) {
          details.open = !!projectSettingsReturn.openSections[
            projectDisclosureKey(details.querySelector("summary").textContent)
          ];
        });
        dialog.scrollTop = projectSettingsReturn.scrollTop;
        projectSettingsReturn = null;
      }
    });
  }

  var projectTabReturn = false;
  var projectTabsReady = new WeakSet();
  function initProjectWorkTabs(root) {
    var scope = root || document;
    scope.querySelectorAll(".project-work-tabs").forEach(function (tablist) {
      if (projectTabsReady.has(tablist)) return;
      projectTabsReady.add(tablist);
      tablist.dataset.tabsReady = "1";
      var tabs = Array.from(tablist.querySelectorAll("[data-project-work-tab]"));
      tablist.addEventListener("pointerdown", function (event) {
        if (event.target.closest("[data-project-work-tab]") &&
            event.button === 0 &&
            !event.ctrlKey && !event.metaKey && !event.shiftKey && !event.altKey) {
          projectTabReturn = true;
        }
      });
      tablist.addEventListener("keydown", function (event) {
        var current = tabs.indexOf(event.target);
        if (current < 0) return;
        if (event.key === "Enter") { projectTabReturn = true; return; }
        var target;
        if (event.key === "ArrowRight") target = (current + 1) % tabs.length;
        else if (event.key === "ArrowLeft") target = (current + tabs.length - 1) % tabs.length;
        else if (event.key === "Home") target = 0;
        else if (event.key === "End") target = tabs.length - 1;
        else if (event.key === " ") target = current;
        else return;
        event.preventDefault();
        projectTabReturn = true;
        tabs[target].focus();
        tabs[target].click();
      });
      var selected = tablist.querySelector('[aria-selected="true"]');
      if (projectTabReturn) {
        selected.focus({ preventScroll: true });
        projectTabReturn = false;
      }
      selected.scrollIntoView({ block: "nearest", inline: "nearest" });
      updateProjectScrollPadding();
    });
  }

  function updateProjectScrollPadding() {
    var header = document.querySelector(".project-workspace-header");
    if (header) header.closest(".projects-editor").style.scrollPaddingTop = header.offsetHeight + "px";
  }
  window.addEventListener("resize", updateProjectScrollPadding);

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

  var PROJECT_DISCLOSURE_STATE = "pa:projects:disclosures:";
  var PROJECT_DISCLOSURE_RETURN = "pa:projects:return-focus";

  function projectDisclosureKey(label) {
    return (label || "")
      .replace(/\s*\(\d+\)\s*$/, "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "") || "section";
  }

  function projectDisclosureState(workspace) {
    var key = PROJECT_DISCLOSURE_STATE +
      (workspace.getAttribute("data-projects-disclosure-scope") || "projects");
    var state = {};
    try { state = JSON.parse(sessionStorage.getItem(key) || "{}"); } catch (_error) {}
    return { key: key, value: state && typeof state === "object" ? state : {} };
  }

  function storeProjectDisclosure(workspace, key, open) {
    var stored = projectDisclosureState(workspace);
    stored.value[key] = !!open;
    try { sessionStorage.setItem(stored.key, JSON.stringify(stored.value)); } catch (_error) {}
  }

  function setProjectDisclosure(toggle, panel, open, focusToggle) {
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    panel.hidden = !open;
    var workspace = toggle.closest(".projects-workspace");
    if (workspace) {
      storeProjectDisclosure(workspace, toggle.dataset.projectDisclosureKey, open);
    }
    if (focusToggle) toggle.focus({ preventScroll: true });
  }

  function transformProjectDisclosures(root) {
    var scope = root || document;
    scope.querySelectorAll(".projects-workspace").forEach(function (workspace) {
      var stored = projectDisclosureState(workspace).value;
      var used = {};
      workspace.querySelectorAll("details").forEach(function (details) {
        if (details.closest("[data-project-settings-dialog]")) return;
        if (details.dataset.projectDisclosureReady) return;
        var summary = details.querySelector(":scope > summary");
        if (!summary) return;
        var label = summary.textContent.trim();
        var base = projectDisclosureKey(label);
        var occurrence = used[base] || 0;
        used[base] = occurrence + 1;
        var key = occurrence ? base + "-" + occurrence : base;
        var originalId = details.id;
        var wrapper = document.createElement("div");
        wrapper.className = (details.className ? details.className + " " : "") +
          "project-disclosure";
        if (originalId) wrapper.id = originalId;

        var toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = (summary.className ? summary.className + " " : "") +
          "project-disclosure-toggle";
        toggle.textContent = label;
        toggle.dataset.projectDisclosureKey = key;

        var panel = document.createElement("div");
        panel.className = "project-disclosure-panel";
        panel.id = "project-disclosure-" + key + "-" + occurrence;
        panel.dataset.projectDisclosurePanel = key;
        toggle.setAttribute("aria-controls", panel.id);

        Array.from(details.childNodes).forEach(function (child) {
          if (child !== summary) panel.appendChild(child);
        });
        wrapper.appendChild(toggle);
        wrapper.appendChild(panel);
        details.replaceWith(wrapper);

        var initialOpen = Object.prototype.hasOwnProperty.call(stored, key)
          ? !!stored[key]
          : details.open;
        toggle.setAttribute("aria-expanded", initialOpen ? "true" : "false");
        panel.hidden = !initialOpen;

        toggle.addEventListener("click", function () {
          setProjectDisclosure(
            toggle,
            panel,
            toggle.getAttribute("aria-expanded") !== "true",
            false
          );
        });
        panel.addEventListener("keydown", function (event) {
          if (event.key !== "Escape") return;
          event.preventDefault();
          var form = event.target.closest("form");
          if (form) form.reset();
          setProjectDisclosure(toggle, panel, false, true);
        });

        panel.querySelectorAll("form").forEach(function (form) {
          form.dataset.projectDisclosureForm = key;
          if (form.querySelector("[data-project-disclosure-close]")) return;
          var cancel = document.createElement("button");
          cancel.type = "button";
          cancel.className = "ghost";
          cancel.textContent = "Cancel";
          cancel.dataset.projectDisclosureClose = "";
          cancel.addEventListener("click", function () {
            form.reset();
            setProjectDisclosure(toggle, panel, false, true);
          });
          form.appendChild(cancel);
        });
      });
    });
  }

  function rememberProjectDisclosureSubmit(event) {
    var form = event.target.closest &&
      event.target.closest("form[data-project-disclosure-form]");
    if (!form) return;
    try {
      sessionStorage.setItem(PROJECT_DISCLOSURE_RETURN, JSON.stringify({
        key: form.dataset.projectDisclosureForm
      }));
    } catch (_error) {}
  }

  function restoreProjectDisclosureFocus(root) {
    var pending = null;
    try {
      pending = JSON.parse(sessionStorage.getItem(PROJECT_DISCLOSURE_RETURN) || "null");
    } catch (_error) {}
    if (!pending || !pending.key) return;
    var scope = root || document;
    var toggle = scope.querySelector(
      '[data-project-disclosure-key="' + pending.key + '"]'
    );
    if (!toggle) return;
    var panel = document.getElementById(toggle.getAttribute("aria-controls"));
    if (panel) setProjectDisclosure(toggle, panel, false, false);
    toggle.focus({ preventScroll: true });
    try { sessionStorage.removeItem(PROJECT_DISCLOSURE_RETURN); } catch (_error) {}
  }

  function initProjectDisclosureJumps(root) {
    var scope = root || document;
    scope.querySelectorAll("[data-project-disclosure-jump]").forEach(function (link) {
      if (link.dataset.projectDisclosureJumpReady) return;
      link.dataset.projectDisclosureJumpReady = "1";
      link.addEventListener("click", function () {
        var wanted = projectDisclosureKey(
          link.getAttribute("data-project-disclosure-jump")
        );
        var toggle = document.querySelector(
          '[data-project-disclosure-key="' + wanted + '"]'
        );
        if (!toggle) return;
        var panel = document.getElementById(toggle.getAttribute("aria-controls"));
        if (panel) setProjectDisclosure(toggle, panel, true, false);
      });
    });
  }

  document.addEventListener("submit", rememberProjectDisclosureSubmit);
  document.body.addEventListener("htmx:responseError", function () {
    projectSettingsReturn = null;
    projectTabReturn = false;
    try { sessionStorage.removeItem(PROJECT_DISCLOSURE_RETURN); } catch (_error) {}
  });

  function boot(root) {
    initResize(root);
    initSections(root);
    initProjectFilters(root);
    initProjectCreateDialogs(root);
    initProjectSettings(root);
    initProjectWorkTabs(root);
    transformProjectDisclosures(root);
    initProjectDisclosureJumps(root);
    restoreProjectDisclosureFocus(root);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      boot(document);
    });
  } else {
    boot(document);
  }

  document.body.addEventListener("htmx:afterSwap", function () {
    // Some HTMX swaps report the source or the replaced node as their target.
    // Initialize the connected DOM so newly loaded project controls always bind.
    boot(document);
  });

  document.addEventListener("htmx:historyRestore", function () {
    projectSettingsReturn = null;
    projectTabReturn = false;
    initProjectSettings(document);
    initProjectWorkTabs(document);
  });

  document.body.addEventListener("htmx:sendError", function () {
    projectSettingsReturn = null;
    projectTabReturn = false;
  });

  window.PALayout = { boot: boot, showSection: showSection };
})();
