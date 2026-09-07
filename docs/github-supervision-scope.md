# GitHub supervision repository scope

Settings → GitHub supervision manages the current instance's explicit repository
allowlist. Enter the complete desired list, validate access, review additions,
removals and private repositories, then choose **Apply exact scope** or **Keep
current scope**. Existing credentials and unrelated integration metadata are
preserved. An empty list is rejected because legacy empty lists mean unrestricted
supervision; this API never enables that state.

The authenticated local API exposes:

* `GET /api/github/supervision-scope`: repositories, scope mode, revision and instance.
* `POST /api/github/supervision-scope/preview`: validate the complete candidate
  `allowed_repositories` with `expected_revision`, returning the exact diff and
  structured `operator_input` choices. Preview does not persist a scope change.
* `PUT /api/github/supervision-scope`: apply `allowed_repositories`,
  `expected_revision`, `idempotency_key`, `confirmed_additions` and
  `confirmation_id`. Unknown fields (including credentials) are rejected without
  reflecting their values. User authentication follows PA's configured login
  policy; a fleet bearer cannot authorize this setting. Browser writes require CSRF.
* `GET /api/github/supervision-scope/audit`: scope changes, actor, instance,
  revisions, idempotency key and confirmation correlation ID, without credentials.

MCP exposes `github_supervision_scope`, `preview_github_supervision_scope`,
`update_github_supervision_scope` and `github_supervision_scope_audit`. These tools
proxy through the running server, which remains the only PA data directory writer.
During a dispatch, pass preview's `operator_input` unchanged to
`report_dispatch_progress` for that dispatch. Await PA's correlated response;
only `apply_scope` authorizes the exact change. Preserve its `confirmation_id`
and use a stable idempotency key for retries. Never interpret a preview, default,
keep/cancel choice, or uncorrelated prose as approval.

For example, the narrow Eschaton repair is a candidate list of
`["petersky/eschaton", "petersky/pa"]`, with only `petersky/eschaton` added.
Read the live revision and obtain operator consent before applying it. Then
refresh the existing watch by ID; do not register a second watch or bypass its
merge gate.

Updates recheck the full credential document and effective credentials after
GitHub validation, rejecting concurrent configuration changes. Scope, audit and
idempotency receipt are saved together atomically with private file permissions.
The public revision contains no token-derived material. A stale update returns
409 and requires another read and review. An identical successful retry returns
the original receipt even if its original revision is now stale; it does not
reapply an old scope over a later change.

The server refreshes and advertises its local capability after saving. If that
step fails, the response explicitly reports `refresh_pending`; retry the same
request to refresh without duplicating the mutation. A successful scope write
does not authorize a merge or change supervision policy.
