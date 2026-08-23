(function () {
  "use strict";

  function init(root) {
    var scope = root || document;
    var form = scope.querySelector("#pa-configuration-form");
    if (!form || form.dataset.configurationReady) return;
    form.dataset.configurationReady = "1";

    var stateNode = scope.querySelector("#pa-configuration-state");
    var state = {};
    try { state = JSON.parse(stateNode && stateNode.textContent || "{}"); } catch (_) {}
    var staged = {};
    var clear = new Set();
    var rows = Array.from(form.querySelectorAll("[data-configuration-row]"));
    var search = scope.querySelector("#pa-configuration-search");
    var category = scope.querySelector("#pa-configuration-category");
    var applicability = scope.querySelector("#pa-configuration-applicability");
    var count = scope.querySelector("#pa-configuration-count");
    var stagedLabel = scope.querySelector("#pa-configuration-staged");
    var discard = scope.querySelector("#pa-configuration-discard");
    var reviewButton = scope.querySelector("#pa-configuration-review-button");
    var review = scope.querySelector("#pa-configuration-review");
    var diff = scope.querySelector("#pa-configuration-diff");
    var apply = scope.querySelector("#pa-configuration-apply");
    var cancel = scope.querySelector("#pa-configuration-cancel");
    var status = scope.querySelector("#pa-configuration-status");
    var validationTimer = null;
    var summaryTest = scope.querySelector("#pa-card-summary-test");
    var summaryProgress = scope.querySelector("#pa-card-summary-test-progress");
    var summaryResult = scope.querySelector("#pa-card-summary-test-result");
    var summaryKeys = new Set([
      "card_summary_provider", "card_summary_model", "card_summary_base_url",
      "card_summary_api_key", "card_summary_anthropic_api_key",
      "card_summary_minimax_api_key"
    ]);

    function headers() {
      var result = {"Content-Type": "application/json", Accept: "application/json"};
      if (window.PACSRF) return window.PACSRF.headers(result);
      var csrf = document.querySelector('meta[name="csrf-token"]');
      if (csrf && csrf.content) result["X-CSRF-Token"] = csrf.content;
      return result;
    }

    function errorMessage(value, fallback) {
      var detail = value && value.detail ? value.detail : value;
      if (detail && detail.message) return detail.message;
      if (Array.isArray(detail)) {
        return detail.map(function (item) { return item.msg; }).join("; ");
      }
      return fallback;
    }

    async function request(path, method, payload) {
      var response = await fetch(path, {
        method: method,
        credentials: "same-origin",
        headers: headers(),
        body: JSON.stringify(payload),
      });
      var body = await response.json().catch(function () { return {}; });
      if (!response.ok) throw body;
      return body;
    }

    function payload(extra) {
      return Object.assign({
        changes: staged,
        clear: Array.from(clear),
        target: "local",
      }, extra || {});
    }

    function stagedCount() {
      return Object.keys(staged).length + clear.size;
    }

    function summaryTestPayload() {
      var changes = {};
      Object.keys(staged).forEach(function (key) {
        if (summaryKeys.has(key)) changes[key] = staged[key];
      });
      return {
        changes: changes,
        clear: Array.from(clear).filter(function (key) { return summaryKeys.has(key); }),
        target: "local",
        idempotency_key: "web-summary-test:" + (
          window.crypto && window.crypto.randomUUID
            ? window.crypto.randomUUID()
            : Date.now() + ":" + Math.random()
        )
      };
    }

    if (summaryTest) summaryTest.addEventListener("click", async function () {
      if (summaryTest.disabled) return;
      summaryTest.disabled = true;
      summaryResult.hidden = true;
      summaryProgress.textContent = "Testing the card-summary provider with a minimal model invocation…";
      try {
        var result = await request(
          "/api/configuration/card-summary/test", "POST", summaryTestPayload()
        );
        summaryResult.querySelector("[data-summary-test-outcome]").textContent =
          result.ok ? "Connection succeeded" : "Connection failed (" + result.code + ")";
        summaryResult.querySelector("[data-summary-test-provider]").textContent = result.provider;
        summaryResult.querySelector("[data-summary-test-model]").textContent = result.model;
        summaryResult.querySelector("[data-summary-test-configuration]").textContent =
          result.configuration === "staged" ? "Staged values (not saved)" : "Saved effective values";
        summaryResult.querySelector("[data-summary-test-invocation]").textContent =
          result.invocation.ok ? "Succeeded" : "Failed (" + result.invocation.code + ")";
        summaryResult.querySelector("[data-summary-test-schema]").textContent =
          result.summary_schema.ok ? "Conformant" : "Not conformant (" + result.summary_schema.code + ")";
        summaryResult.querySelector("[data-summary-test-elapsed]").textContent =
          result.elapsed_ms + " ms";
        summaryResult.querySelector("[data-summary-test-message]").textContent = result.message;
        summaryResult.hidden = false;
        summaryProgress.textContent = result.ok
          ? "Card-summary connection test completed successfully."
          : "Card-summary connection test completed with an actionable failure.";
      } catch (error) {
        summaryProgress.textContent = errorMessage(error, "The connection test could not be completed.");
      } finally {
        summaryTest.disabled = false;
      }
    });

    function updateActions() {
      var total = stagedCount();
      stagedLabel.textContent = total
        ? total + (total === 1 ? " staged change" : " staged changes")
        : "No staged changes";
      discard.disabled = total === 0;
      reviewButton.disabled = total === 0;
      if (!total) review.hidden = true;
    }

    function rowError(row, message) {
      var node = row.querySelector("[data-configuration-error]");
      if (!node) return;
      node.hidden = !message;
      node.textContent = message || "";
    }

    function parse(row, input) {
      var kind = row.dataset.kind;
      if (kind === "bool") return input.checked;
      if (kind === "int" || kind === "optional_int") {
        if (!input.value.trim() && kind === "optional_int") return null;
        var integer = Number(input.value);
        if (!Number.isInteger(integer)) throw new Error("Expected an integer.");
        return integer;
      }
      if (kind === "float") {
        var number = Number(input.value);
        if (!Number.isFinite(number)) throw new Error("Expected a number.");
        return number;
      }
      if (kind === "list_str" || kind === "optional_list_str" || kind === "dict_int") {
        try { return JSON.parse(input.value); }
        catch (_) { throw new Error("Expected valid JSON."); }
      }
      return input.value;
    }

    function stage(row, value) {
      var key = row.dataset.key;
      staged[key] = value;
      clear.delete(key);
      rowError(row, "");
      updateActions();
      window.clearTimeout(validationTimer);
      validationTimer = window.setTimeout(validate, 250);
    }

    async function validate() {
      if (!stagedCount()) return;
      status.textContent = "Validating staged changes…";
      try {
        await request("/api/configuration/validate", "POST", payload());
        status.textContent = "Staged changes are valid.";
      } catch (error) {
        status.textContent = errorMessage(error, "Configuration is invalid.");
      }
    }

    rows.forEach(function (row) {
      var input = row.querySelector("[data-configuration-input]");
      if (input) {
        input.addEventListener("change", function () {
          try { stage(row, parse(row, input)); }
          catch (error) { rowError(row, error.message); }
        });
      }
      var clearButton = row.querySelector("[data-configuration-clear]");
      if (clearButton) {
        clearButton.addEventListener("click", function () {
          var key = row.dataset.key;
          delete staged[key];
          clear.add(key);
          rowError(row, "");
          updateActions();
          validate();
        });
      }
      var replaceButton = row.querySelector("[data-configuration-secret-replace]");
      if (replaceButton) {
        replaceButton.addEventListener("click", function () {
          var secret = row.querySelector("[data-configuration-secret]");
          if (!secret || !secret.value) {
            rowError(row, "Enter a non-empty replacement secret.");
            return;
          }
          stage(row, secret.value);
          secret.value = "";
          replaceButton.textContent = "Replacement staged";
        });
      }
    });

    function filter() {
      var query = (search.value || "").trim().toLowerCase();
      var wantedCategory = category.value || "";
      var wanted = applicability.value || "";
      var visible = 0;
      rows.forEach(function (row) {
        var applicable = !wanted ||
          (wanted === "supported" && row.dataset.supported === "true") ||
          (wanted === "unsupported" && row.dataset.supported === "false") ||
          (wanted === "deprecated" && row.dataset.deprecated === "true") ||
          (wanted === "readonly" && row.dataset.readonly === "true");
        var matches = (!query || row.dataset.search.indexOf(query) !== -1) &&
          (!wantedCategory || row.dataset.category === wantedCategory) && applicable;
        row.hidden = !matches;
        if (matches) visible += 1;
      });
      count.textContent = visible + (visible === 1 ? " setting" : " settings");
    }
    search.addEventListener("input", filter);
    category.addEventListener("change", filter);
    applicability.addEventListener("change", filter);

    discard.addEventListener("click", function () {
      staged = {};
      clear.clear();
      rows.forEach(function (row) {
        rowError(row, "");
        var secret = row.querySelector("[data-configuration-secret]");
        if (secret) secret.value = "";
      });
      status.textContent = "Staged changes discarded.";
      updateActions();
    });

    reviewButton.addEventListener("click", async function () {
      status.textContent = "Validating and building review…";
      try {
        var result = await request("/api/configuration/diff", "POST", payload());
        diff.textContent = "";
        if (!result.changes.length) {
          diff.textContent = "No persisted values would change.";
        } else {
          var list = document.createElement("dl");
          list.className = "settings-dl";
          result.changes.forEach(function (item) {
            var term = document.createElement("dt");
            term.textContent = item.key;
            var value = document.createElement("dd");
            value.textContent = JSON.stringify(item.before) + " → " +
              JSON.stringify(item.after) + " [" + item.apply + "]" +
              (item.overridden_by_environment ? " (environment still wins)" : "");
            list.append(term, value);
          });
          diff.appendChild(list);
        }
        review.hidden = false;
        review.scrollIntoView({behavior: "smooth", block: "nearest"});
        status.textContent = "Review the complete diff before applying.";
      } catch (error) {
        status.textContent = errorMessage(error, "Configuration is invalid.");
      }
    });

    cancel.addEventListener("click", function () {
      review.hidden = true;
      reviewButton.focus();
    });

    apply.addEventListener("click", async function () {
      apply.disabled = true;
      status.textContent = "Applying configuration atomically…";
      try {
        var result = await request("/api/configuration", "PATCH", payload({
          expected_revision: state.revision,
          idempotency_key: "web:" + (
            window.crypto && window.crypto.randomUUID
              ? window.crypto.randomUUID()
              : Date.now() + ":" + Math.random()
          ),
          interface: "web",
        }));
        state.revision = result.revision;
        staged = {};
        clear.clear();
        updateActions();
        review.hidden = true;
        var note = result.restart_required.length
          ? " Restart required for: " + result.restart_required.join(", ") + "."
          : result.reload_required.length
            ? " Reload required for: " + result.reload_required.join(", ") + "."
            : "";
        status.textContent = "Configuration applied and audited." + note;
        window.setTimeout(function () {
          window.location.href = "/settings?section=configuration";
        }, 700);
      } catch (error) {
        status.textContent = errorMessage(error, "Configuration was not applied.");
      } finally {
        apply.disabled = false;
      }
    });

    updateActions();
    if (window.location.hash.indexOf("#configuration-") === 0) {
      var target = document.querySelector(window.location.hash);
      if (target) target.scrollIntoView({block: "start"});
    }
  }

  function boot(event) {
    init(event && event.detail && event.detail.target || document);
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () { init(document); });
  } else {
    init(document);
  }
  document.body.addEventListener("htmx:afterSwap", boot);
})();
