# Conversation work intent

PA acts on user intent and carries forward standing authority, explicit
preferences and execution defaults. Ordinary investigation, implementation
decisions, root causes, necessary related corrections, verification and completion
proceed autonomously. Agents explain meaningful discoveries, coordinate existing
workers and use separate reviewable changes where useful. Card/issue boundaries
organize work; they are not permission boundaries.

Discussion-only and creation-only instructions apply in conversational context
alongside later direction and standing authority. Historical wording is not a
permanent veto. A mention alone does not start unrelated work. Agents ask only for
a real permission boundary, explicit user restriction, consequential irreversible
choice or essential missing decision, naming the concrete action and reason.
Selecting a deferred issue and starting it clears only that issue's deferral;
restoring the list entry first is not a separate authorization prerequisite.

`conversation_issues` owns the existing durable request identity, frozen selection
and admission receipts. Exact reload/retry uses the original request ID,
selection and revision. The dispatch message freezes compact source/request/time
and selected issue provenance beside the execution inputs. Retrying an ambiguous
legacy admission retains its original payload. Local worker prompts can recover
the original receipt through the card association; remote retries and successor
attempts retain the original dispatch instruction. These are intent records for
deduplication and recovery, not permission tokens or a prerequisite for exercising
standing authority. Current guidance interprets historical PA-authored scope
summaries in the user's conversational context; it does not treat them as new
user restrictions. Native tool/sandbox boundaries, execution defaults, workspace
fencing, PR supervision and completion acceptance still apply.

Same-session continuation includes root causes and necessary adjacent corrections
without routine reapproval. Existing action bindings and idempotency still fence
PA's issued effects; their templates do not limit implementation judgment.
Reaching the configured automatic continuation limit produces a concrete resource
limit error, not a new authorization requirement. The work request remains in
effect; the same session can be resumed or the limit adjusted.

Turn completion, task completion, acceptance and card lane are independent.
Both `pa.card-disposition/v1` and `v2` support `inbox`, `active`, `waiting` and
`done`. Inbox records captured work without asserting implementation completion
or scheduling continuation. Waiting represents an actual external dependency;
Done retains its existing integration and acceptance guards. Exhausting status
reconciliation preserves the current lane and original result, keeps the failure
diagnostic and notifies the operator when configured. It does not fabricate a
Waiting outcome or a business blocker.

Results lead with substantive outcome, remaining work and next actor. Internal
status contracts and integration bookkeeping are available in collapsed
diagnostics. Failed start receipts show their concrete reason and offer one
Retry saved request action, using the original receipt. Permission interactions
show only provider-supplied options; native rejection is delivered as the exact
selected provider option, and invalid/stale selections leave the request pending
with a correlated error. Permission defaults are unchanged.

This change does not implement completion-record envelopes or attachment
management owned by cards `73fe8fa7-793c-4f2d-b133-42e579580d22` and
`a69a703a-87a4-43df-8e10-66eb871e0ea8`.
