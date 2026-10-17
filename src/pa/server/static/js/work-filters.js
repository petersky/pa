(function () {
  "use strict";

  var readyWidgets = new WeakSet();
  var readySaveButtons = new WeakSet();
  var tagDropdown = null;

  document.addEventListener("click", function (event) {
    if (!tagDropdown) return;
    if (!tagDropdown.element.isConnected) { tagDropdown = null; return; }
    if (!tagDropdown.element.contains(event.target)) tagDropdown.close();
  });

  function updateResults() {
    var board = document.querySelector("[data-work-board]");
    var status = document.querySelector("[data-work-results]");
    if (!board || !status) return;
    var counts = board.querySelectorAll("[data-work-lane-results]");
    if (counts.length !== board.querySelectorAll(".board-column").length) return;
    counts.forEach(function (node) {
      var shown = node.closest(".board-column").querySelectorAll(".compact-card").length;
      var total = Number(node.dataset.total);
      node.textContent = shown < total ? "Showing " + shown + " of " + total + " cards" : total + (total === 1 ? " card" : " cards");
    });
    var total = Array.from(counts).reduce(function (sum, node) { return sum + Number(node.dataset.total); }, 0);
    var shown = board.querySelectorAll(".compact-card").length;
    var message = total + (total === 1 ? " card" : " cards") + " across all lanes" + (shown < total ? " · " + shown + " loaded" : "");
    if (status.textContent !== message) status.textContent = message;
  }

  function init(root) {
    var widget = (root || document).querySelector("[data-work-tag-combobox]");
    if (!widget || readyWidgets.has(widget)) return;
    readyWidgets.add(widget);
    if ((root || document).querySelector("[data-work-board]")) {
      var current = new URL(location.href);
      var stable = new URLSearchParams();
      Array.from(new Set(Array.from(current.searchParams.keys()))).sort().forEach(function (key) {
        current.searchParams.getAll(key).filter(Boolean).sort(function (a, b) { return a.localeCompare(b); })
          .forEach(function (value) { if (!Array.from(stable.getAll(key)).includes(value)) stable.append(key, value); });
      });
      var canonical = stable.toString();
      if (canonical !== current.searchParams.toString()) history.replaceState(history.state, "", current.pathname + (canonical ? "?" + canonical : "") + current.hash);
    }
    var input = widget.querySelector("[data-facet-input]");
    var list = widget.querySelector("[data-facet-options]");
    var chips = widget.querySelector("[data-facet-chips]");
    var status = widget.querySelector("[data-facet-status]");
    var fallback = widget.querySelector(".facet-no-js");
    var active = -1;
    var timer = 0;
    var controller = null;
    fallback.disabled = true; fallback.hidden = true;

    function values() {
      return Array.from(chips.querySelectorAll('input[name="tag"]')).map(function (node) { return node.value; });
    }
    function announce(message) { status.textContent = message; }
    function close() {
      list.hidden = true; input.setAttribute("aria-expanded", "false");
      input.removeAttribute("aria-activedescendant"); active = -1;
    }
    tagDropdown = {element: widget, close: close};
    function add(value) {
      if (!value || values().indexOf(value) !== -1) return;
      var chip = document.createElement("span"); chip.className = "facet-chip";
      chip.append(document.createTextNode(value));
      var button = document.createElement("button"); button.type = "button";
      button.dataset.removeTag = value; button.setAttribute("aria-label", "Remove tag " + value); button.textContent = "×";
      var hidden = document.createElement("input"); hidden.type = "hidden"; hidden.name = "tag"; hidden.value = value;
      chip.append(button, hidden); chips.append(chip);
      widget.querySelector("[data-clear-tags]").hidden = false;
      announce(value + " added. " + values().length + " tags selected."); input.value = ""; input.focus(); close();
    }
    function render(options) {
      list.replaceChildren(); active = -1;
      options.filter(function (option) { return values().indexOf(option.value) === -1; }).forEach(function (option, index) {
        var item = document.createElement("li"); item.id = "work-tag-option-" + index;
        item.setAttribute("role", "option"); item.dataset.value = option.value;
        item.textContent = option.value + " (" + option.count + ")";
        item.addEventListener("mousedown", function (event) { event.preventDefault(); });
        item.addEventListener("click", function () { add(option.value); });
        list.append(item);
      });
      list.hidden = false; input.setAttribute("aria-expanded", "true");
      announce(list.children.length ? list.children.length + " matching tags." : "No matching tags.");
    }
    function search() {
      if (controller) controller.abort(); controller = new AbortController();
      announce("Searching tags…");
      fetch(widget.dataset.facetUrl + "&q=" + encodeURIComponent(input.value), {signal: controller.signal, credentials: "same-origin"})
        .then(function (response) { if (!response.ok) throw new Error(); return response.json(); })
        .then(function (data) { render(data.options || []); })
        .catch(function (error) { if (error.name !== "AbortError") { close(); announce("Tags could not be loaded. Try again."); } });
    }
    input.addEventListener("input", function () { clearTimeout(timer); timer = setTimeout(search, 120); });
    input.addEventListener("focus", search);
    input.addEventListener("keydown", function (event) {
      var options = Array.from(list.children);
      if (event.key === "ArrowDown" || event.key === "ArrowUp") {
        event.preventDefault(); if (!options.length) return;
        active = (active + (event.key === "ArrowDown" ? 1 : -1) + options.length) % options.length;
        options.forEach(function (item, index) { item.setAttribute("aria-selected", index === active ? "true" : "false"); });
        input.setAttribute("aria-activedescendant", options[active].id); options[active].scrollIntoView({block: "nearest"});
      } else if (event.key === "Enter" && active >= 0) { event.preventDefault(); add(options[active].dataset.value); }
      else if (event.key === "Escape") { close(); }
    });
    widget.addEventListener("click", function (event) {
      var remove = event.target.closest("[data-remove-tag]");
      if (remove) { var value = remove.dataset.removeTag; remove.closest(".facet-chip").remove(); announce(value + " removed. " + values().length + " tags selected."); }
      if (event.target.closest("[data-clear-tags]")) { chips.replaceChildren(); event.target.closest("[data-clear-tags]").hidden = true; announce("All tags cleared."); input.focus(); }
    });

    var save = (root || document).querySelector("[data-save-work-view]");
    if (save && !readySaveButtons.has(save)) {
      readySaveButtons.add(save);
      save.addEventListener("click", function () {
        var name = window.prompt("Name this view"); if (!name) return;
        (window.PACSRF ? window.PACSRF.fetch : fetch)("/api/cards/saved-views?realm=" + encodeURIComponent(new URL(location.href).searchParams.get("realm") || "default"), {
          method: "POST", credentials: "same-origin", headers: {"Content-Type": "application/json", "Idempotency-Key": "work-view-" + Date.now()},
          body: JSON.stringify({name: name, query: location.search.slice(1)})
        }).then(function (response) { if (!response.ok) throw new Error(); location.reload(); })
          .catch(function () { announce("View could not be saved. Try again."); });
      });
    }
  }
  document.addEventListener("DOMContentLoaded", function () { init(document); updateResults(); });
  document.body.addEventListener("htmx:afterSwap", function (event) { init(event.detail.target); updateResults(); });
  document.addEventListener("htmx:historyRestore", function () { init(document); updateResults(); });
})();
