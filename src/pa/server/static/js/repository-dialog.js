(function () {
  "use strict";

  async function request(path, options) {
    var response = await window.PACSRF.fetch(path, Object.assign({ cache: "no-store" }, options));
    var body = await response.json().catch(function () { return {}; });
    if (!response.ok) {
      var error = new Error(typeof body.detail === "string" ? body.detail : "The request failed. Please retry.");
      error.status = response.status;
      throw error;
    }
    return body;
  }

  function init() {
    document.querySelectorAll("[data-repository-dialog]").forEach(function (root) {
      if (root.dataset.ready) return;
      root.dataset.ready = "1";
      var dialog = root.closest("dialog");
      var find = function (selector) { return root.querySelector(selector); };
      var identity = find("[data-github-identity]");
      var browse = find("[data-repository-browse]");
      var browser = find("#repository-browser");
      var name = find("[data-repository-name]");
      var check = find("[data-repository-check]");
      var create = find("[data-repository-create]");
      var status = find("[data-repository-availability]");
      var listStatus = find("[data-repository-list-status]");
      var more = find("[data-repository-more]");
      var retry = find("[data-repository-retry]");
      var login = null, confirmed = null, busy = false, key = null;
      var rows = [], nextPage = 1, generation = 0, checkGeneration = 0, accountGeneration = 0;

      function invalidate() {
        checkGeneration += 1;
        confirmed = null;
        create.disabled = true;
        create.textContent = "Create and add repository";
        status.textContent = "";
        status.classList.remove("repository-error");
      }
      function tab(mode, focus) {
        root.querySelectorAll("[data-repository-tab]").forEach(function (button) {
          var selected = button.dataset.repositoryTab === mode;
          button.setAttribute("aria-selected", String(selected));
          button.tabIndex = selected ? 0 : -1;
          find("#repository-" + button.dataset.repositoryTab).hidden = !selected;
          if (focus && selected) button.focus();
        });
      }
      root.querySelectorAll("[data-repository-tab]").forEach(function (button) {
        button.addEventListener("click", function () { if (!busy) tab(button.dataset.repositoryTab); });
        button.addEventListener("keydown", function (event) {
          if (["ArrowLeft", "ArrowRight", "Home", "End"].indexOf(event.key) === -1 || busy) return;
          event.preventDefault();
          tab(event.key === "Home" ? "existing" : event.key === "End" ? "new" : button.dataset.repositoryTab === "new" ? "existing" : "new", true);
        });
      });
      async function authenticate() {
        var current = ++accountGeneration;
        login = null;
        browse.disabled = check.disabled = true;
        identity.textContent = "Checking GitHub authentication…";
        identity.classList.remove("repository-error");
        try {
          var user = await request("/api/github/identity");
          if (current !== accountGeneration || !root.isConnected) return;
          login = user.login;
          identity.textContent = "GitHub: authenticated as @" + login;
          browse.disabled = check.disabled = false;
        } catch (error) {
          if (current !== accountGeneration) return;
          identity.textContent = "GitHub not authenticated or unavailable. " + error.message;
          identity.classList.add("repository-error");
        }
      }
      function reset() {
        if (busy) return;
        tab("existing");
        invalidate();
        key = null;
        find("[data-repository-new-form]").reset();
        browser.hidden = true;
        browse.setAttribute("aria-expanded", "false");
        generation += 1;
        rows = []; nextPage = 1;
        find("[data-repository-search]").value = "";
        authenticate();
      }
      document.querySelectorAll('[data-project-create-open="' + dialog.id + '"]').forEach(function (button) {
        button.addEventListener("click", reset);
      });
      function renderRows() {
        var query = find("[data-repository-search]").value.trim().toLowerCase();
        var results = find("[data-repository-results]");
        results.replaceChildren();
        var filtered = rows.filter(function (row) { return (row.full_name + " " + (row.description || "")).toLowerCase().includes(query); });
        filtered.forEach(function (row) {
          var button = document.createElement("button");
          button.type = "button";
          button.className = "ghost repository-result";
          button.textContent = row.full_name + (row.private ? " · Private" : " · Public") + (row.archived ? " · Archived" : "") + (row.fork ? " · Fork" : "");
          button.addEventListener("click", function () {
            var form = find("#repository-existing form");
            form.elements.url.value = row.clone_url;
            form.elements.name.value = row.name;
            form.elements.default_branch.value = row.default_branch || "";
            form.elements.provider.value = "github";
            form.elements.provider_repository_id.value = String(row.id);
            form.elements.provider_metadata.value = JSON.stringify({ full_name: row.full_name, private: row.private });
            browser.hidden = true;
            browse.setAttribute("aria-expanded", "false");
            form.elements.url.focus();
          });
          results.appendChild(button);
        });
        listStatus.textContent = (filtered.length ? filtered.length + " matching" : "No matching") + " repositories (" + rows.length + " loaded)." + (nextPage ? " Load more to search additional repositories." : "");
      }
      async function load(resetList) {
        var current = ++generation;
        if (resetList) { rows = []; nextPage = 1; find("[data-repository-results]").replaceChildren(); }
        var page = nextPage || 1;
        more.disabled = true; retry.hidden = true;
        listStatus.textContent = "Loading repositories…";
        listStatus.classList.remove("repository-error");
        try {
          var query = new URLSearchParams({ page: page, visibility: find("[data-repository-visibility]").value, sort: find("[data-repository-sort]").value });
          var result = await request("/api/github/repositories?" + query);
          if (current !== generation || !root.isConnected) return;
          rows = rows.concat(result.repositories);
          nextPage = result.next_page;
          more.hidden = !nextPage;
          more.disabled = false;
          renderRows();
        } catch (error) {
          if (current !== generation) return;
          listStatus.textContent = error.message;
          listStatus.classList.add("repository-error");
          retry.hidden = false;
          more.hidden = true;
        }
      }
      browse.addEventListener("click", function () {
        browser.hidden = !browser.hidden;
        browse.setAttribute("aria-expanded", String(!browser.hidden));
        if (!browser.hidden) { load(true); find("[data-repository-search]").focus(); }
      });
      find("[data-repository-search]").addEventListener("input", renderRows);
      ["[data-repository-visibility]", "[data-repository-sort]"].forEach(function (selector) { find(selector).addEventListener("change", function () { load(true); }); });
      more.addEventListener("click", function () { load(false); });
      retry.addEventListener("click", function () { load(false); });
      name.addEventListener("input", function () { invalidate(); key = null; });
      check.addEventListener("click", async function () {
        invalidate();
        if (!name.reportValidity()) return;
        var value = name.value.trim(), current = checkGeneration;
        check.disabled = true;
        status.textContent = "Checking availability…";
        try {
          var result = await request("/api/github/repository-availability?name=" + encodeURIComponent(value));
          if (current !== checkGeneration || !root.isConnected) return;
          if (result.login !== login) {
            login = result.login;
            identity.textContent = "GitHub: authenticated as @" + login;
          }
          status.textContent = result.available ? result.login + "/" + result.name + " is available. Ready to create a private repository." : "That name is already taken. Choose another name or use Existing.";
          status.classList.toggle("repository-error", !result.available);
          if (result.available) { confirmed = result; create.disabled = false; }
        } catch (error) {
          if (current !== checkGeneration) return;
          status.textContent = error.message;
          status.classList.add("repository-error");
        } finally { check.disabled = !login || busy; }
      });
      find("[data-repository-new-form]").addEventListener("submit", async function (event) {
        event.preventDefault();
        if (busy || !confirmed || confirmed.name !== name.value.trim()) return;
        busy = true;
        key = key || crypto.randomUUID();
        var controls = root.querySelectorAll("button, input, select");
        controls.forEach(function (control) { control.disabled = true; });
        status.textContent = "Creating repository on GitHub and adding it to PA…";
        try {
          var result = await request("/api/github/repositories", {
            method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": key },
            body: JSON.stringify({ name: confirmed.name, confirmed_login: confirmed.login, realm: root.dataset.realm })
          });
          window.location.assign("/projects?" + new URLSearchParams({ realm: root.dataset.realm, view: "repos", repository: result.id }));
        } catch (error) {
          status.textContent = error.message;
          status.classList.add("repository-error");
          if (error.status === 503) create.textContent = "Retry adding repository";
          else confirmed = null;
        } finally {
          busy = false;
          controls.forEach(function (control) { control.disabled = false; });
          browse.disabled = check.disabled = !login;
          create.disabled = !confirmed;
        }
      });
      // Keep the in-flight external operation visible until it has a receipt.
      dialog.addEventListener("cancel", function (event) { if (busy) { event.preventDefault(); event.stopImmediatePropagation(); } }, true);
      dialog.addEventListener("click", function (event) { if (busy && event.target === dialog) event.stopImmediatePropagation(); }, true);
    });
  }
  document.addEventListener("DOMContentLoaded", init);
  document.addEventListener("htmx:afterSwap", init);
  init();
})();
