(function () {
  "use strict";
  function key() { return crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + Math.random(); }
  async function mutate(path, body, detail) {
    var query = detail ? "?expected_version=" + detail.dataset.version + "&policy_revision=" + detail.dataset.policy : "";
    var response = await window.PACSRF.fetch(path + query, {method: "POST", headers: {"Content-Type": "application/json", "Idempotency-Key": key()}, body: JSON.stringify(body)});
    var payload = await response.json().catch(function () { return {}; });
    if (!response.ok) throw new Error(typeof payload.detail === "string" ? payload.detail : ((payload.detail || {}).message || "Goal mutation failed"));
    return payload;
  }
  function status(detail, message) { var node = detail.querySelector("[data-goal-status]"); if (node) node.textContent = message; }
  function selectGoal(goalId, focus) {
    if (!goalId) return;
    document.querySelectorAll("[data-goal-detail]").forEach(function (node) {
      node.hidden = node.dataset.goalDetail !== goalId;
    });
    document.querySelectorAll("[data-goal-select]").forEach(function (node) {
      var active = node.dataset.goalSelect === goalId;
      node.classList.toggle("is-selected", active);
      node.setAttribute("aria-current", String(active));
      if (active && focus) node.focus({ preventScroll: true });
    });
    try {
      var url = new URL(window.location.href);
      url.hash = "goal-" + goalId;
      history.replaceState(history.state, "", url.toString());
    } catch (_) {}
  }
  function rememberFocus(goalId) {
    try { sessionStorage.setItem("pa:goal-focus", goalId || ""); } catch (_) {}
  }
  function mount() {
    var selected = "";
    if (window.location.hash.indexOf("#goal-") === 0) selected = window.location.hash.slice(6);
    var remembered = "";
    try {
      remembered = sessionStorage.getItem("pa:goal-focus") || "";
      sessionStorage.removeItem("pa:goal-focus");
    } catch (_) {}
    var target = remembered || selected;
    if (target && document.querySelector('[data-goal-select="' + CSS.escape(target) + '"]')) {
      selectGoal(target, Boolean(remembered));
    }
  }
  document.addEventListener("click", async function (event) {
    var toggle = event.target.closest("[data-goal-create-toggle]"); if (toggle) { var panel = document.getElementById("goal-create"); panel.hidden = !panel.hidden; document.querySelectorAll('[data-goal-create-toggle]').forEach(function (button) { button.setAttribute("aria-expanded", String(!panel.hidden)); }); if (!panel.hidden) panel.querySelector("input").focus(); return; }
    var select = event.target.closest("[data-goal-select]"); if (select) { selectGoal(select.dataset.goalSelect, false); return; }
    var edit = event.target.closest("[data-goal-edit-toggle]"); if (edit) { var form = edit.closest("[data-goal-detail]").querySelector("[data-goal-edit-form]"); form.hidden = !form.hidden; edit.setAttribute("aria-expanded", String(!form.hidden)); if (!form.hidden) form.querySelector("input").focus(); return; }
    var transition = event.target.closest("[data-goal-transition]"); if (transition) { var detail = transition.closest("[data-goal-detail]"); transition.disabled = true; transition.setAttribute("aria-busy", "true"); try { await mutate("/api/goals/" + detail.dataset.goalDetail + "/transition", {state: transition.dataset.goalTransition, reason: transition.dataset.goalTransition === "paused" ? "Paused by operator" : "Activated by operator"}, detail); rememberFocus(detail.dataset.goalDetail); location.reload(); } catch (error) { status(detail, error.message); transition.disabled = false; transition.removeAttribute("aria-busy"); } }
    var dispatch = event.target.closest("[data-goal-dispatch]"); if (dispatch) { var target = dispatch.closest("[data-goal-detail]"); dispatch.disabled = true; dispatch.setAttribute("aria-busy", "true"); try { await mutate("/api/goals/" + target.dataset.goalDetail + "/proposals", {proposer_principal: "user:local", proposer_role: "coordinator", expected_goal_version: Number(target.dataset.version), policy_revision: Number(target.dataset.policy), rationale: "Operator requested governed dispatch", action: {kind: "dispatch_work_package", work_package_id: dispatch.dataset.goalDispatch, placement_policy: "best_match"}}, target); rememberFocus(target.dataset.goalDetail); location.reload(); } catch (error) { status(target, error.message); dispatch.disabled = false; dispatch.removeAttribute("aria-busy"); } }
  });
  document.addEventListener("submit", async function (event) {
    var form = event.target; if (!form.matches("[data-goal-create-form],[data-goal-edit-form],[data-goal-work-form],[data-goal-evidence-form],[data-goal-audit-form]")) return; event.preventDefault(); var values = Object.fromEntries(new FormData(form)); var detail = form.closest("[data-goal-detail]");
    var submit = form.querySelector('[type="submit"]'); if (submit) { submit.disabled = true; submit.setAttribute("aria-busy", "true"); }
    try {
      if (form.matches("[data-goal-create-form]")) await mutate("/api/goals", {objective: values.objective, motivation: values.motivation, criteria: [{description: values.criterion, verification_method: values.verification, evidence_requirement: values.requirement}]});
      else if (form.matches("[data-goal-edit-form]")) await mutate("/api/goals/" + detail.dataset.goalDetail + "/revisions", {objective: values.objective, motivation: values.motivation, reason: values.reason}, detail);
      else if (form.matches("[data-goal-work-form]")) await mutate("/api/goals/" + detail.dataset.goalDetail + "/proposals", {proposer_principal: "user:local", proposer_role: "coordinator", expected_goal_version: Number(detail.dataset.version), policy_revision: Number(detail.dataset.policy), rationale: "Operator proposed bounded work", action: {kind: "create_work_package", title: values.title, objective: values.objective, criterion_ids: [values.criterion_id]}}, detail);
      else if (form.matches("[data-goal-evidence-form]")) await mutate("/api/goals/" + detail.dataset.goalDetail + "/evidence", {evidence: {criterion_ids: [values.criterion_id], kind: "artifact", summary: values.summary, uri: values.uri}, criterion_verdicts: {}}, detail);
      else { var criterion = detail.querySelector("[data-goal-evidence-form] select").value; var evidenceIds = new FormData(form).getAll("evidence_id"); await mutate("/api/goals/" + detail.dataset.goalDetail + "/audit", {independent: true, criterion_verdicts: {[criterion]: values.verdict}, evidence_ids: evidenceIds, explanation: values.explanation}, detail); }
      if (detail) rememberFocus(detail.dataset.goalDetail);
      location.reload();
    } catch (error) { if (detail) status(detail, error.message); else form.querySelector("[role=alert]").textContent = error.message; if (submit) { submit.disabled = false; submit.removeAttribute("aria-busy"); } }
  });
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount); else mount();
  document.body && document.body.addEventListener("htmx:afterSwap", mount);
})();
