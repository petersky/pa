# Conversation work intent

Discussion and creation-only requests capture work without starting it. An
explicit fix/start request authorizes only its selected scope. Later starts
supersede historical create-only/deferred notes for that scope; unrelated Inbox
cards remain unstarted.
Selecting a deferred issue and starting it clears only that issue's deferral;
restoring the list entry first is not a separate authorization prerequisite.

`conversation_issues` owns the existing durable request identity, frozen selection
and admission receipts. Exact reload/retry uses the original request ID,
selection and revision. The dispatch message freezes compact source/request/time
and selected issue provenance beside the execution inputs. Retrying an ambiguous
legacy admission retains its original payload. Local worker prompts can recover
the original receipt through the card association; remote retries and successor
attempts retain the original dispatch instruction. These are intent records,
not new permissions. Native tool/sandbox boundaries, execution defaults, workspace
fencing, PR supervision and completion acceptance still apply.

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
