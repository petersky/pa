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
