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
    clear();
    try {
      var data = await request("", "GET");
      revision = data.revision;
      input.value = data.allowed_repositories.join("\n");
      document.getElementById("pa-github-scope-current").textContent = "Current scope: " +
        (data.scope_mode === "unrestricted" ? "unrestricted (legacy empty list)" : data.allowed_repositories.join(", "));
    } catch (error) { status.textContent = error.message; }
  }
  input.addEventListener("input", clear);
  document.getElementById("pa-github-scope-reload").addEventListener("click", load);
  document.getElementById("pa-github-scope-keep").addEventListener("click", function () {
    clear(); status.textContent = "Scope unchanged.";
  });
  form.addEventListener("submit", async function (event) {
    event.preventDefault();
    if (busy) return;
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
