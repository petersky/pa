/* Capability evidence and explicit intent, never provider-confirmed runtime state. */
(function () {
  "use strict";
  var fields = ["harness", "connection", "model_provider", "model", "reasoning"];
  var names = {harness: "Agent harness", connection: "Configured connection / account", model_provider: "Backend provider", model: "Model", reasoning: "Native reasoning"};
  var catalogPromise;
  function api(path, method, body) {
    var cookie = document.cookie.split("; ").find(function (c) {return c.indexOf("pa_csrf=") === 0;});
    var headers = {"Content-Type": "application/json", "Idempotency-Key": crypto.randomUUID()};
    if (cookie) headers["X-CSRF-Token"] = decodeURIComponent(cookie.slice(8));
    return fetch(path, {method: method || "GET", credentials: "same-origin", headers: headers,
      body: body ? JSON.stringify(body) : undefined}).then(async function (response) {
        var data = await response.json();
        if (!response.ok) throw new Error((data.detail || {}).message || JSON.stringify(data.detail || data));
        return data;
      });
  }
  function readScript(root, selector, fallback) {
    var node = root.querySelector(selector);
    try { return node ? JSON.parse(node.textContent) : fallback; } catch (_) { return fallback; }
  }
  function init(root) {
    if (root._selection) return;
    var state = root._selection = {prefs: readScript(root, "[data-selection-initial]", {}), candidates: [], inherited: {}, sources: {}};
    var output = root.querySelector('input[name="execution_preferences"]');
    var notice = root.querySelector("[data-selection-notice]");
    var form = root.closest("form");
    function scopedApi(path, method, body) {
      if (root.dataset.selectionRealm) path += (path.includes("?") ? "&" : "?") + "realm=" + encodeURIComponent(root.dataset.selectionRealm);
      return api(path, method, body);
    }
    function noticeText(message) { notice.textContent = message; }
    function effective(name) {
      var pref = name.indexOf("options.") === 0 ? (state.prefs.options || {})[name.slice(8)] : state.prefs[name];
      return pref && pref.intent !== "inherit" ? pref : state.inherited[name] || {intent: "automatic"};
    }
    function choices(name) {
      var target = form && form.elements.dispatch_target && form.elements.dispatch_target.value;
      var allowed = state.candidates.filter(function (c) {
        if (target && target.indexOf("instance:") === 0 && c.instance_id !== target.slice(9)) return false;
        return fields.slice(0, fields.indexOf(name) < 0 ? fields.length - 1 : fields.indexOf(name)).every(function (parent) {
          var pref = effective(parent);
          return !pref.value || c[parent] === pref.value;
        });
      });
      var values = [];
      allowed.forEach(function (c) {
        if (name === "reasoning" || name.indexOf("options.") === 0) {
          var opt = name === "reasoning" ? c.reasoning : (c.options || {})[name.slice(8)];
          if (opt && opt.support === "supported") values.push.apply(values, opt.values || []);
        } else if (c[name]) values.push(c[name]);
      });
      return Array.from(new Set(values)).sort(function (a, b) {return String(a).localeCompare(String(b));});
    }
    function sync() { output.value = JSON.stringify(state.prefs); }
    var constraintsBox = root.querySelector("[data-selection-constraints]");
    if (constraintsBox) [["instance_ids", "Allowed instances", "list"], ["harnesses", "Allowed harnesses", "list"],
      ["connection_ids", "Allowed connections", "list"], ["model_providers", "Allowed backends", "list"],
      ["required_tools", "Required tools", "list"], ["modalities", "Required modalities", "list"],
      ["min_context_tokens", "Minimum context tokens", "number"], ["max_cost_usd", "Maximum estimated cost (USD)", "number"]].forEach(function (item) {
      var label = document.createElement("label"); label.textContent = item[1];
      var input = document.createElement("input"); input.type = item[2] === "number" ? "number" : "text";
      input.dataset.selectionConstraint = item[0];
      input.setAttribute("aria-label", item[1]); input.placeholder = item[2] === "list" ? "Comma-separated; empty adds no restriction" : "No additional limit";
      if (item[2] === "number") {input.min = "0"; input.step = item[0] === "max_cost_usd" ? "any" : "1";}
      var current = (state.prefs.hard_constraints || {})[item[0]];
      input.value = Array.isArray(current) ? current.join(", ") : current == null ? "" : current;
      input.addEventListener("change", function () {
        state.prefs.hard_constraints = state.prefs.hard_constraints || {};
        if (!input.value.trim()) delete state.prefs.hard_constraints[item[0]];
        else state.prefs.hard_constraints[item[0]] = item[2] === "list" ? input.value.split(",").map(function (v) {return v.trim();}).filter(Boolean) : Number(input.value);
        sync(); output.dispatchEvent(new Event("change", {bubbles: true}));
      });
      label.append(input); constraintsBox.append(label);
    });
    var taskInput = root.querySelector("[data-selection-task]");
    if (taskInput) {
      taskInput.value = state.prefs.task ? JSON.stringify(state.prefs.task, null, 2) : "";
      taskInput.addEventListener("input", function () {
        try {state.prefs.task = taskInput.value.trim() ? JSON.parse(taskInput.value) : null; taskInput.setCustomValidity(""); sync();}
        catch (_) {taskInput.setCustomValidity("Enter a valid task assessment JSON object, or leave empty.");}
      });
      taskInput.addEventListener("change", function () {output.dispatchEvent(new Event("change", {bubbles: true}));});
    }
    function render() {
      var container = root.querySelector("[data-selection-fields]");
      container.replaceChildren();
      var optionIds = new Set(Object.keys(state.prefs.options || {}));
      Object.keys(state.inherited).filter(function (id) {return id.indexOf("options.") === 0;}).forEach(function (id) {optionIds.add(id.slice(8));});
      state.candidates.forEach(function (c) {Object.keys(c.options || {}).forEach(function (id) {optionIds.add(id);});});
      fields.concat(Array.from(optionIds).sort().map(function (id) {return "options." + id;})).forEach(function (name) {
        var pref = name.indexOf("options.") === 0 ? (state.prefs.options || {})[name.slice(8)] : state.prefs[name];
        pref = pref || {intent: "inherit"};
        var row = document.createElement("div"); row.className = "execution-selection-row";
        var label = document.createElement("label"); label.textContent = names[name] || "Native option: " + name.slice(8);
        var intent = document.createElement("select"); intent.setAttribute("aria-label", label.textContent + " intent");
        [["inherit", "Inherit"], ["automatic", "Automatic"], ["required", "Required"], ["preferred", "Preferred"]].forEach(function (p) {intent.add(new Option(p[1], p[0]));});
        intent.value = pref.intent;
        var value = document.createElement("select"); value.setAttribute("aria-label", label.textContent + " value");
        value.add(new Option("Choose an advertised value", ""));
        var vals = choices(name);
        vals.forEach(function (v) {value.add(new Option(String(v), JSON.stringify(v)));});
        if (pref.value !== undefined && pref.value !== null && !vals.includes(pref.value)) value.add(new Option(String(pref.value) + " — unavailable / unknown", JSON.stringify(pref.value)));
        value.value = pref.value === undefined || pref.value === null ? "" : JSON.stringify(pref.value);
        value.disabled = !["required", "preferred"].includes(pref.intent);
        value.required = !value.disabled;
        var source = document.createElement("small"); source.className = "muted";
        var inherited = state.inherited[name] || {};
        source.textContent = "Inherited: " + (state.sources[name] || "automatic policy") + (inherited.value !== undefined && inherited.value !== null ? " · " + String(inherited.value) : " · automatic") + (!vals.length ? " · capability unknown" : "");
        function change() {
          var next = {intent: intent.value};
          if (["required", "preferred"].includes(next.intent)) {
            if (!value.value) { value.disabled = false; value.required = true; value.focus(); return; }
            next.value = JSON.parse(value.value);
          }
          if (name.indexOf("options.") === 0) {
            state.prefs.options = state.prefs.options || {}; state.prefs.options[name.slice(8)] = next;
          } else state.prefs[name] = next;
          var cleared = [];
          if (["harness", "connection", "model_provider", "model"].includes(name)) {
            fields.slice(fields.indexOf(name) + 1).forEach(function (child) {
              var old = effective(child);
              if (old && old.value !== undefined && !choices(child).includes(old.value)) {
                state.prefs[child] = {intent: "automatic"}; cleared.push(names[child]);
              }
            });
            Array.from(optionIds).forEach(function (id) {
              var old = effective("options." + id);
              if (old.value != null && !choices("options." + id).includes(old.value)) {
                state.prefs.options = state.prefs.options || {};
                state.prefs.options[id] = {intent: "automatic"}; cleared.push(id);
              }
            });
          }
          sync(); render();
          noticeText(cleared.length ? "Cleared incompatible child choices: " + cleared.join(", ") + ". Review before saving or dispatching." : "Requested preferences changed. Nothing has been applied to a provider.");
          output.dispatchEvent(new Event("change", {bubbles: true}));
        }
        intent.addEventListener("change", change); value.addEventListener("change", change);
        label.append(intent); row.append(label, value, source); container.append(row);
      });
      sync();
    }
    function loadDefaults() {
      var query = new URLSearchParams();
      if (root.dataset.selectionCard) query.set("card_id", root.dataset.selectionCard);
      if (root.dataset.selectionSurface) query.set("surface", root.dataset.selectionSurface);
      var project = form && form.elements.project_id;
      if (project && project.value) query.set("project_id", project.value);
      return scopedApi("/api/execution/defaults?" + query).then(function (data) {
        var layers = data.layers || [];
        if (root.dataset.selectionPurpose !== "dispatch") layers = layers.filter(function (l) {return l.source !== "card";});
        state.inherited = {}; state.sources = {};
        layers.forEach(function (layer) { fields.concat(Object.keys(layer.preferences.options || {}).map(function (id) {return "options." + id;})).forEach(function (name) {
          var p = name.indexOf("options.") === 0 ? layer.preferences.options[name.slice(8)] : layer.preferences[name];
          if (!state.inherited[name] && p && p.intent !== "inherit") {state.inherited[name] = p; state.sources[name] = layer.source;}
        });});
        if (data.card_version) root.dataset.selectionVersion = data.card_version;
        render();
      }).catch(function (e) {noticeText(e.message);});
    }
    root.querySelector("[data-selection-refresh]").addEventListener("click", function () {
      noticeText("Refreshing local capability evidence…");
      api("/api/execution/catalog/refresh", "POST", {}).then(function (data) {
        var local = data.instance_id;
        state.candidates = state.candidates.filter(function (c) {return c.instance_id !== local;}).concat(data.candidates);
        catalogPromise = Promise.resolve(data); render(); noticeText("Local catalog refreshed. Remote catalog refresh is owned by fleet discovery; unknown values remain unknown.");
      }).catch(function (e) {noticeText(e.message);});
    });
    var save = root.querySelector("[data-selection-save]");
    if (save) save.addEventListener("click", async function () {
      if (Array.from(root.querySelectorAll("select, input, textarea")).some(function (s) {return !s.reportValidity();})) return;
      save.disabled = true;
      try {
        var prefs = state.prefs;
        if (root.dataset.selectionPurpose === "dispatch") {
          var card = await scopedApi("/api/cards/" + root.dataset.selectionCard);
          prefs = structuredClone(card.execution_preferences || {});
          Object.keys(state.prefs).forEach(function (key) {if (key === "options") prefs.options = Object.assign(prefs.options || {}, state.prefs.options);
            else if (fields.includes(key)) {if (state.prefs[key].intent !== "inherit") prefs[key] = state.prefs[key];}
            else if (state.prefs[key] != null) prefs[key] = state.prefs[key];});
        }
        var saved = await scopedApi("/api/cards/" + root.dataset.selectionCard, "PATCH", {execution_preferences: prefs,
          updated_at: root.dataset.selectionVersion, field_intent: ["execution_preferences"]});
        root.dataset.selectionVersion = saved.updated_at;
        await loadDefaults(); noticeText("Card defaults saved. Existing attempts and runtime settings were not changed.");
      } catch (e) {noticeText(e.message);} finally {save.disabled = false;}
    });
    if (form) form.addEventListener("change", function (event) {
      if (event.target.name === "project_id") loadDefaults();
      if (event.target.name === "dispatch_target") render();
    });
    var preview = root.querySelector("[data-selection-preview]");
    if (preview) preview.addEventListener("click", async function () {
      if (form && !form.reportValidity()) return;
      preview.disabled = true;
      try {
        var project = form && form.elements.project_id;
        var result = await scopedApi("/api/execution/preview", "POST", {card_id: root.dataset.selectionCard || null,
          project_id: project && project.value || null, surface: root.dataset.selectionSurface || "execution",
          replace_card_preferences: root.dataset.selectionPurpose === "edit", execution_preferences: state.prefs});
        var panel = root.querySelector("[data-selection-preview-output]"); panel.hidden = false; panel.open = true;
        var explanation = panel.querySelector("[data-selection-explanation]");
        if (explanation) explanation.textContent = [result.selected.instance_id, result.selected.harness, result.selected.connection,
          result.selected.model || "provider default (actual model unknown)", result.selected.reasoning].filter(Boolean).join(" / ") + ". " + result.explanation +
          " Sources: " + fields.map(function (f) {return names[f] + " = " + result.provenance[f];}).join("; ") +
          ". Known tradeoffs: " + ((result.tradeoffs || []).join(", ") || "none recorded") + ". Alternatives considered: " + result.alternatives.length + ". Provider confirmation is pending.";
        panel.querySelector("pre").textContent = JSON.stringify(result, null, 2);
        noticeText("Preview only. Provider confirmation is pending; admission revalidates current constraints and capacity.");
      } catch (e) {noticeText(e.message);} finally {preview.disabled = false;}
    });
    var embedded = readScript(root, "[data-selection-catalog]", []);
    if (!catalogPromise) catalogPromise = api("/api/execution/catalog");
    catalogPromise.then(function (data) {state.candidates = embedded.concat(data.candidates || []); render();
      noticeText(state.candidates.length ? "Cached evidence loaded; supported choices depend on the complete tuple. Provider confirmation happens after admission." : "No cached capability catalog. Automatic still uses admission-time discovery; refresh to choose explicit values.");
    }).catch(function (e) {noticeText(e.message); render();});
    state.reset = function () {
      state.prefs = {};
      root.querySelectorAll("[data-selection-constraint]").forEach(function (input) {input.value = "";});
      if (taskInput) {taskInput.value = ""; taskInput.setCustomValidity("");}
      render();
    };
    loadDefaults(); render();
  }
  function renderSession(widget, snap) {
    var view = snap.execution_selection;
    var panel = widget.root.querySelector("[data-session-execution-selection]");
    if (!view) {if (panel) panel.hidden = true; return;}
    if (panel && panel.dataset.sessionId !== snap.session.id) {panel.remove(); panel = null;}
    if (!panel) {
      panel = document.createElement("details"); panel.dataset.sessionExecutionSelection = "";
      panel.dataset.sessionId = snap.session.id; panel.className = "execution-selection-receipt";
      panel.innerHTML = '<summary>Execution selection · requested and confirmed</summary><p data-selection-summary></p><p class="muted small" data-native-settings-status role="status"></p><form data-native-settings><div data-native-fields></div><button type="submit" class="small">Request settings (defer if busy)</button> <button type="button" class="ghost small" data-native-refresh>Refresh native choices</button> <button type="button" class="ghost small" data-native-cancel hidden>Cancel pending change</button></form><details><summary>Why this selection? Receipt and alternatives</summary><pre></pre></details>';
      widget.root.querySelector(".acw-toolbar").after(panel);
      panel._rows = [];
      panel._prefs = {};
      panel._catalog = [];
      panel._status = panel.querySelector("[data-native-settings-status]");
      function draw() {
        var container = panel.querySelector("[data-native-fields]"); container.replaceChildren();
        var chosen = panel._prefs.model && panel._prefs.model.value || panel._view.requested.model;
        var candidate = panel._catalog.find(function (c) {return c.model === chosen;});
        var models = Array.from(new Set(panel._catalog.map(function (c) {return c.model;}).filter(Boolean)));
        var rows = [{id: "model", label: "Model", values: models}];
        if (candidate && candidate.reasoning.support === "supported") rows.push({id: "reasoning", label: "Native reasoning", values: candidate.reasoning.values});
        Object.keys(candidate && candidate.options || {}).forEach(function (id) {
          var option = candidate.options[id]; if (option.support === "supported") rows.push({id: "options." + id, label: "Native option: " + id, values: option.values});
        });
        rows.forEach(function (row) {
          var label = document.createElement("label"); label.textContent = row.label;
          var select = document.createElement("select"); select.setAttribute("aria-label", "Session " + row.label);
          select.add(new Option("Keep current / unspecified", ""));
          row.values.forEach(function (v) {select.add(new Option(String(v), JSON.stringify(v)));});
          var pref = row.id.indexOf("options.") === 0 ? (panel._prefs.options || {})[row.id.slice(8)] : panel._prefs[row.id];
          select.value = pref && pref.value !== undefined ? JSON.stringify(pref.value) : "";
          select.onchange = function () {
            var value = select.value ? {intent: "required", value: JSON.parse(select.value)} : {intent: "inherit"};
            if (row.id.indexOf("options.") === 0) {panel._prefs.options = panel._prefs.options || {}; panel._prefs.options[row.id.slice(8)] = value;}
            else panel._prefs[row.id] = value;
            if (row.id === "model") {
              panel._prefs.reasoning = {intent: "automatic"}; panel._prefs.options = {};
              Object.keys(panel._view.requested.options || {}).forEach(function (id) {panel._prefs.options[id] = {intent: "automatic"};});
              panel._status.textContent = "Model changed: reasoning and native options reset to Automatic. Choose supported values; nothing has been applied.";
              draw();
            }
          };
          label.append(select); container.append(label);
        });
      }
      function refresh() {
        return widget.api("/sessions/" + encodeURIComponent(panel.dataset.sessionId) + "/execution-catalog").then(function (data) {
          panel._catalog = data.candidates || []; draw();
        }).catch(function (e) {panel._status.textContent = e.message;});
      }
      panel.querySelector("[data-native-refresh]").onclick = refresh;
      panel.querySelector("form").onsubmit = async function (event) {
        event.preventDefault();
        var button = panel.querySelector('button[type="submit"]'); button.disabled = true;
        try {
          var result = await widget.api("/sessions/" + encodeURIComponent(panel.dataset.sessionId) + "/execution-settings", {method: "POST", body: JSON.stringify({
            execution_preferences: panel._prefs, expected_version: panel._snapshot.session.updated_at, idempotency_key: crypto.randomUUID(), defer: true})});
          panel._status.textContent = "Settings request: " + result.state + ". Card defaults, permission mode and collaboration mode were not changed.";
          panel._prefs = {}; draw();
          var updated = await widget.api("/sessions/" + encodeURIComponent(panel.dataset.sessionId)); widget.applyOptionSnapshot(updated);
        } catch (e) {panel._status.textContent = e.message;} finally {button.disabled = false;}
      };
      panel.querySelector("[data-native-cancel]").onclick = async function () {
        try {
          await widget.api("/sessions/" + encodeURIComponent(panel.dataset.sessionId) + "/execution-settings/cancel", {method: "POST", body: JSON.stringify({
            idempotency_key: panel._view.pending_changes.id, expected_version: panel._snapshot.session.updated_at})});
          panel._status.textContent = "Pending change cancelled. No rollback or provider change was performed.";
          var updated = await widget.api("/sessions/" + encodeURIComponent(panel.dataset.sessionId)); widget.applyOptionSnapshot(updated);
        } catch (e) {panel._status.textContent = e.message;}
      };
      panel._refresh = refresh;
    }
    panel.hidden = false; panel._view = view; panel._snapshot = snap;
    panel.querySelector("form").hidden = !!snap.selection_history_only;
    var confirmed = view.provider_confirmation || {};
    var native = confirmed.effective || {};
    panel.querySelector("[data-selection-summary]").textContent = "Requested: " + [view.requested.harness, view.requested.connection, view.requested.model || "provider default (actual unknown)", view.requested.reasoning].filter(Boolean).join(" / ") +
      ". Native confirmed (" + confirmed.state + "): " + [native.model_id || "model unknown", native.reasoning || "reasoning unknown / absent"].join(" / ") +
      ". Backend/account routing is configured; native confirmation remains unknown" +
      (snap.selection_history_only ? ". Historical receipt: these are the last confirmed values, not a live provider readback" : "") +
      (view.pending_changes ? ". Settings change: " + view.pending_changes.state : "") +
      (view.blocked ? ". Blocked: " + view.blocked.message : "") +
      ". Harness/account changes require a linked context boundary, not in-place replacement.";
    panel.querySelector("pre").textContent = JSON.stringify(view, null, 2);
    panel.querySelector("[data-native-cancel]").hidden = !view.pending_changes || view.pending_changes.state !== "pending";
    if (!panel._loaded && !snap.selection_history_only) {panel._loaded = true; panel._refresh();}
  }
  function scan() {document.querySelectorAll("[data-execution-preferences]").forEach(init);}
  window.PAExecutionSelection = {scan: scan, api: api, renderSession: renderSession};
  document.addEventListener("DOMContentLoaded", scan);
  document.addEventListener("htmx:afterSwap", scan);
  new MutationObserver(scan).observe(document.documentElement, {childList: true, subtree: true});
})();
