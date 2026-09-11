# Notification interactions

PA uses InteractionRequest and InteractionChoice for native ACP permission and
elicitation requests, MCP operator_input, and versioned post-turn
request_operator_input actions. Requests retain notification, request, session,
dispatch and owner identities; replies never authorize unrelated actions.

Use 2–4 meaningful options for bounded questions. Labels should be 1–5 words;
optional descriptions explain consequences and optional value holds JSON data.
IDs must be stable and unique within the request. Keep the actual question in
prompt and optional explanation in details. A MCP operator_input example:

```json
{"schema_version":1,"request_id":"test-target-v1","prompt":"Where should tests run?","choices":[{"id":"local","label":"Local","description":"Run in this worktree","value":"local"},{"id":"ci","label":"CI","description":"Push and run CI","value":"ci"}],"allow_freeform":false,"allow_cancel":true}
```

Post-turn pa.followup-action/v1 request_operator_input uses question instead of
prompt and requires keep_lane. It accepts choices, response_schema, details,
allow_freeform, allow_cancel, sensitive and deadline. Native permissions retain
provider-defined options and approval scope without inferred or invented choices.
No final-text or quoted-JSON authorization envelope is supported.

Single selection submits choice_id. For multiple selection use response_schema
with type array (items validates choice values; minItems/maxItems bound selection)
and submit choice_ids. PA stores exact IDs and corresponding values in order.
No choice is automatically submitted. Freeform is optional; cancellation and
retry are distinct response shapes. Invalid IDs, duplicate selections, schema
violations, and stale requests are rejected before delivery.

Response recording, delivery, and continuation are separate stages. Delivery
acknowledges the waiting protocol request or durable continuation queue, not
successful agent action. Follow the continuation/progress link to verify results.
Prompt continuations carry pa.interaction-response/v1 JSON with exact ownership
and response data. Delivery retries reuse the recorded response and stable prompt
ID; a recorded durable admission is never intentionally re-enqueued. Sensitive
response values are hidden from public notification diagnostics.

Progress version negotiation occurs on authenticated materialization, including
local/replayed admissions. Existing unnegotiated dispatches can still create a
typed operator interaction: the receipt explicitly reports progress_recorded=false
and includes the notification. This does not upgrade remote transport capability.

## Recover a pending MCP continuation

`POST /api/notifications/{notification_id}/transfer-continuation` (MCP:
`transfer_notification_continuation`) repairs **only the delivery destination** of
one existing MCP `report_dispatch_progress.operator_input` prompt continuation.
It does not answer, cancel, approve, recover a provider session, or create another
question. Invoke it on the notification's owning instance as the originating
principal, with the normal authenticated API/CSRF contract. Use an operator UI
session or the user bearer credential used by the local MCP bridge. A shared
fleet/sync bearer cannot transfer a continuation, even with an acting-principal
header and even when `auth_required` is false. An explicitly invalid bearer is
also rejected instead of falling back to the open-mode default user or a valid
UI cookie; requests without a bearer retain normal UI authentication behavior:

```json
{
  "idempotency_key": "recover-decision-1",
  "expected_version": 3,
  "expected_session_id": "original-session-id",
  "expected_dispatch_id": "original-dispatch-id",
  "successor_session_id": "successor-session-id",
  "successor_dispatch_id": "successor-dispatch-id",
  "reason": "Original provider context is lost; retain this pending decision"
}
```

Read the notification immediately before preparing the request. A changed version
or origin is a conflict (409); inspect the new state instead of guessing an answer
or blindly replacing the expected version. A successful repair increments the
version once. Exact retries replay the durable receipt even after a response;
changing the key, target, origin, expected version, or reason does not authorize a
second transfer. The receipt retains the before/after session and dispatch IDs,
expected version, actor, authority, timestamp and reason in the existing event
history; notification audit reports `continuation.transferred`. Do not put secrets
or private response values in the reason.

The original notification ID, request ID, session/dispatch provenance, question,
choice IDs/values (including confirmation IDs), response schema, and any recorded
response remain unchanged. The separate `continuation_transfer` receipt identifies
the explicit successor. Response delivery uses that successor and the original
stable `notification-response:{notification_id}:{request_id}` prompt ID. The
`pa.interaction-response/v1` envelope continues to identify the **original** request,
session and dispatch. The successor must apply only the real correlated answer's
scope, revalidate external state, and report the outcome.
Public `routing.destination` and the rendered continuation/progress link point to
the explicit successor. The stored `destination_url`, session and dispatch fields
continue to describe the original request.

On retry, PA validates routing identity before checking the successor's durable
admission receipt. An already admitted response can be acknowledged even if the
successor has since closed or lost recoverability; it is not enqueued or recovered
again. New admission still requires the full live/recoverable target checks.

This first version deliberately supports one local hop only. Both dispatches and
sessions must match the same local authority, execution instance, realm, card,
project and principal. The original dispatch must be terminal and its session
closed/recovery-blocked with no resumable identity or explicit blocked/lost-context
evidence, no live runtime and no in-progress admission. The successor must have a
matching immutable execution binding and a recoverable running/completed dispatch
and provider context. Missing legacy provenance is rejected; this API does not
backfill it or infer successors from mutable card links. Cross-instance transfers,
transfer chains, and ACP native permission/elicitation ownership are unsupported.
Only MCP operator-input requests with the expected prompt-continuation protocol
are eligible; provider delivery handlers are never moved.

Fresh workspace admission records `dispatch_id`, `realm_id`, and `principal_id`
alongside card/project/origin and lease facts in the immutable execution binding.
The existing `workspace_binding_initialized` compare-and-set transition audits
the complete binding before provider startup. Subsequent admission rejects a
conflicting dispatch, realm, or principal instead of overwriting that provenance.
Existing incomplete bindings (including those produced by PA 1.4.5) are not
backfilled by workspace recovery or this transfer API. Reprovisioning such a
session does not make it transfer-eligible. Use a freshly admitted successor after
activation, or a separately supported audited CAS repair that independently
verifies all durable provenance; do not copy current session fields into history.

An outstanding question remains outstanding. A previously recorded **failed**
response stays unchanged and is not sent by transfer: use `respond_notification`
with `retry=true` and a fresh stable retry key afterward. Already delivered,
cancelled, expired, superseded, answered or delivery-pending requests are rejected.
Evidence that the original session already admitted the response (transcript or
durable queue/in-flight state) also rejects transfer because its effects may be
ambiguous. Resolve that ambiguity through the owning operation; never re-report a
duplicate question as a repair. Concurrent response handling is serialized with
transfer. All local notification saves compare versions; metadata/coalescing
writers reload on contention rather than erasing a transfer or recorded response.
Response-stage saves merge concurrent read/acknowledgement/coalescing metadata
only, so those updates cannot strand a recorded answer between delivery stages.
Changes to the interaction, routing, resolution or delivery state remain conflicts.

Deploy this capability through the normal release process before using it. All
writers handling these notifications must understand the new routing receipt;
older binaries must not write transferred notifications. No live data rewrite,
service restart, or actual scope approval is part of implementing this API.
