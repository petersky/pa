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

Policy loading is shared by Settings, credentials and capability production.
New documents record `pa_supervision_scope.schema_version: 1` and an explicit
`mode`: `none` (no grants), `allowlist` (nonempty list), or `unrestricted` (no
list entries). This API writes only `allowlist` after the existing exact consent
and access check. Legacy nonempty lists keep their exact permissions and CAS
revision and are labeled `legacy_explicit`. An explicitly stored legacy empty
list retains its historical unrestricted meaning, labeled `legacy_unrestricted`;
this is compatibility evidence, not a new approval. No startup migration writes.

**Compatibility change:** an environment-token-only installation with no scope
file no longer has an implicit unrestricted grant. Use the normal Settings
preview and exact-list update to establish scope (the unconfigured revision is
`missing`). Missing scope fields, malformed JSON/types, unknown schemas and
unreadable files are ineligible even when authentication comes from the
environment. Restore a valid local document if it is damaged; PA will not
silently overwrite it. A current invalid policy immediately removes eligibility;
a previous valid grant is never reused for new effects. Environment-over-file
credential precedence is preserved.

Settings shows the current instance's saved revision separately from its
published capability. “Advertised scope by instance” is a read-only view of the
configured authority's received advertisements, bounded to 200 rows from the
last day. Older authorities may expose only fresh rows. The existing capabilities
endpoint remains fresh-only by default; the diagnostic reader opts into
`include_stale=true`. It is not a remote configuration audit. Eligibility still expires at
120 seconds; historical rows remain visible as stale. Missing information is
unknown, not a denial or an empty unrestricted list. Older valid advertisements
retain their existing scope semantics and show an unavailable revision.
Different instance lists are expected local policy, not evidence of a reset.

Authority failures, invalid responses, stale/incompatible capabilities, local
configuration failures, authentication, repository access and explicit scope
denials have separate diagnostic causes. Watch state/API and supervision/card
views retain their causal context. Authority reads are shared across watches for
15 seconds (30 seconds after failure), and blocked watches retain their bounded
poll schedule. Receiver ordering prevents older advertisements replacing newer
ones; future observations and expired lease capabilities cannot grant effects.
The optional `eligibility_journal_hook` receives safe typed reports; shared
journal infrastructure owns issue storage, deduplication and recovery correlation.

A failed GitHub `/user` verification is also causal evidence: HTTP 401 reports
`credentials_rejected`; transport errors, provider errors and verification
deadlines report `verification_unavailable`. Both deny new supervision effects
and use the existing bounded error-probe retry interval. A timeout does not prove
that credentials were rejected. Authority asyncio deadlines report
`authority_unreachable` through the same bounded inventory path; caller
cancellation remains cancellation rather than an eligibility diagnosis.
