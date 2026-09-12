(function () {
  var form = document.getElementById("pa-github-scope-form");
  if (!form || form.dataset.bound) return;
  form.dataset.bound = "true";
  var input = document.getElementById("pa-github-scope-repositories");
  var status = document.getElementById("pa-github-scope-status");
  var review = document.getElementById("pa-github-scope-review");
  var apply = document.getElementById("pa-github-scope-apply");
  var revision = null, proposal = null, busy = false;
  async function request(path, method, body) {
    var headers = {"Content-Type": "application/json", Accept: "application/json"};
    if (window.PACSRF) headers = window.PACSRF.headers(headers);
    else {
      var csrf = document.querySelector('meta[name="csrf-token"]');
      if (csrf) headers["X-CSRF-Token"] = csrf.content;
    }
    var response = await fetch("/api/github/supervision-scope" + path, {
      method: method, credentials: "same-origin", headers: headers,
      body: body ? JSON.stringify(body) : undefined
    });
    var data = await response.json();
    if (!response.ok) throw new Error((data.detail && data.detail.message) || "Could not complete the scope request. Reload and review again.");
    return data;
  }
  function clear() { proposal = null; review.hidden = true; }
  async function load() {
    clear(); revision = null;
    status.textContent = "";
    try {
      var data = await request("", "GET");
      revision = data.revision;
      input.value = data.allowed_repositories.join("\n");
      var publication = data.published_capability || {};
      document.getElementById("pa-github-scope-current").textContent =
        "Current instance: " + data.instance_name + " (" + data.instance_id + "). Scope: " +
        data.scope_mode + "; " + (data.allowed_repositories.join(", ") || "no listed repositories") +
        ". Source: " + data.policy_source + ". Saved revision: " + data.revision +
        ". Published revision: " + (publication.policy_revision || "unknown") +
        ". Observation: " + (publication.observed_at || "unknown") +
        (publication.policy_revision !== data.revision || publication.state === "publication_pending" ?
          ". Capability publication pending." : ". Capability: " + publication.state + ".");
    } catch (error) {
      document.getElementById("pa-github-scope-current").textContent = "Current scope: unknown; configuration unavailable or invalid.";
      input.value = ""; status.textContent = error.message;
    }
    var comparison = document.getElementById("pa-github-scope-comparison");
    comparison.replaceChildren();
    try {
      var fleet = await request("/comparison", "GET");
      if (fleet.evaluation_state === "unavailable") {
        comparison.textContent = fleet.message; return;
      }
      if (!fleet.candidates.length) comparison.textContent = "No advertisements in the bounded inventory; remote configuration is unknown.";
      fleet.candidates.forEach(function (row) {
        var entry = document.createElement("p");
        entry.textContent = (row.instance_name || "Instance") + " (" + row.instance_id + ")" + (row.instance_id === fleet.current_instance_id ? " (current instance)" : "") +
          ": " + (row.scope_mode || "unknown") + "; " + (row.repositories === null ? "scope unknown" : row.repositories.join(", ")) +
          ". Source: " + row.policy_source + ". Revision: " + (row.policy_revision || "unavailable (older peer)") +
          ". Observed: " + row.observed_at + ". Authority received: " + (row.authority_received_at || "unknown") + ". " + row.freshness +
          ". Authentication: " + (row.authenticated ? "advertised authenticated" : "unavailable") +
          (row.reason_code ? ". " + row.reason_code + ": " + row.action : ". Available advertisement.");
        comparison.appendChild(entry);
      });
    } catch (error) { comparison.textContent = "Advertised scope unknown. " + error.message; }
  }
  input.addEventListener("input", clear);
  document.getElementById("pa-github-scope-reload").addEventListener("click", load);
  document.getElementById("pa-github-scope-keep").addEventListener("click", function () {
    clear(); status.textContent = "Scope unchanged.";
  });
  form.addEventListener("submit", async function (event) {
    event.preventDefault();
    if (busy) return;
    if (!revision) { status.textContent = "Restore a readable scope configuration and reload before reviewing a change."; return; }
    busy = true; clear(); status.textContent = "Validating repository access…";
    var candidateText = input.value;
    try {
      var data = await request("/preview", "POST", {
        allowed_repositories: candidateText.split(/\n/).map(function (v) { return v.trim(); }).filter(Boolean),
        expected_revision: revision
      });
      if (input.value !== candidateText) { status.textContent = "Repositories changed; validate again."; return; }
      proposal = Object.assign({}, data.operator_input.choices[0].value);
      delete proposal.instance_id;
      proposal.idempotency_key = crypto.randomUUID();
      document.getElementById("pa-github-scope-question").textContent = data.operator_input.prompt;
      document.getElementById("pa-github-scope-diff").textContent =
        "Add: " + (data.additions.join(", ") || "none") + "\nRemove: " + (data.removals.join(", ") || "none") +
        "\nPrivate repositories: " + (data.validated_repositories.filter(function (r) { return r.private; }).map(function (r) { return r.repository; }).join(", ") || "none");
      review.hidden = false; status.textContent = "Review the exact scope, then choose an action.";
    } catch (error) { status.textContent = error.message; }
    finally { busy = false; }
  });
  apply.addEventListener("click", async function () {
    if (!proposal || busy) return;
    busy = true; apply.disabled = true;
    try {
      var result = await request("", "PUT", proposal);
      await load();
      status.textContent = result.capability_refresh.state === "ready" ? "Scope saved and capability refreshed." : "Scope saved. Capability refresh needs attention; retrying the same request is safe.";
    } catch (error) { status.textContent = error.message; }
    finally { busy = false; apply.disabled = false; }
  });
  load();
})();
