/* Read-only, model-scoped effort evidence shared by settings and chat. */
(function () {
  "use strict";
  function model(value) {
    return String(value || "").replace(/\s*\[[^\[\]]+\]$/, "").trim();
  }
  function kind(option) {
    const id = String(option && (option.category || option.id || option.configId || option.config_id) || "").toLowerCase().replace(/[^a-z]/g, "");
    if (["model", "modelid", "languagemodel"].includes(id)) return "model";
    if (["effort", "reasoning", "reasoningeffort", "reasoninglevel", "thinking", "thinkingeffort", "thinkinglevel", "thought", "thoughteffort", "thoughtlevel"].includes(id)) return "reasoning";
    return "other";
  }
  function evidence(candidates, scope, selectedModel) {
    const wanted = model(selectedModel);
    const matches = (candidates || []).filter(function (c) {
      return Object.keys(scope || {}).every(function (key) { return (c[key] || null) === (scope[key] || null); }) &&
        (wanted ? c.model === wanted : c.default === true);
    });
    const tuples = new Set(matches.map(c => JSON.stringify([c.instance_id, c.harness, c.connection, c.model_provider, c.model])));
    // Ambiguous routing/defaults or missing support must never become a union.
    if (!matches.length || tuples.size !== 1 || matches.some(c => c.freshness === "stale" || !c.reasoning || c.reasoning.support !== "supported")) return [];
    return Array.from(new Set(matches[0].reasoning.values || [])).filter(function (value) {
      return typeof value === "string" && matches.every(c => (c.reasoning.values || []).includes(value));
    });
  }
  function fill(select, values, wanted, defaultLabel) {
    if (!select) return false;
    wanted = wanted == null ? select.value : String(wanted);
    select.replaceChildren(new Option(defaultLabel || "Automatic / provider default", ""));
    values.forEach(value => select.add(new Option(value, value)));
    select.value = values.includes(wanted) ? wanted : "";
    select.disabled = !values.length;
    select.dataset.effortUnavailable = String(!values.length);
    return !!wanted && !values.includes(wanted);
  }
  let pending;
  function catalog() {
    if (!pending) pending = fetch("/api/execution/catalog", {credentials: "same-origin", headers: {Accept: "application/json"}})
      .then(r => {if (!r.ok) throw new Error("Effort choices unavailable"); return r.json();})
      .finally(() => {pending = null;});
    return pending;
  }
  function localScope(data, harness, provider) {
    const scope = {instance_id: data.instance_id, harness: harness, connection: "default"};
    if (provider) scope.model_provider = provider;
    return scope;
  }
  window.PANativeEffort = {model, kind, evidence, fill, catalog, localScope};
})();
