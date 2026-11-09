# Exact-session recovery audit

`POST /api/agent/sessions/{session_id}/restart-handoffs/{handoff_id}/recovery-audit`
records a fail-closed recovery assessment. It does not load a provider, reopen a
session, reconcile receipt status, restart PA, enqueue or deliver any prompt.
There is no production application merely because this endpoint exists.

The authenticated principal must own the saved session, and the server instance
must match both session authority and receipt owner. The body contains
`expected_version` (receipt phase version), `expected_session_version` (saved
session updated_at), `expected_binding_hash` (SHA256 of execution_binding encoded
as sorted compact JSON), and a stable `idempotency_key`. New observations reject
stale versions/bindings. An exact duplicate returns the original observation,
not a fresh health assertion. Reuse of the same key for different inputs fails.
Operation receipts survive transcript retention. A failed transcript mirror can
be repaired by retrying the same key; session and handoff are never changed.

The result records exact native history identity, a hash of saved provider/model/
mode/configuration, receipt/session/binding versions, completion event identity
when present, explicit blockers and an evidence query. It never records prompt
content or raw settings. Missing local completion remains UNKNOWN, including
when queue admission, launcher failure or provider load evidence exists.
Completion evidence does not settle intentional close/dismissal. Every result
has `state=blocked`, `restored=false` and `delivery_allowed=false`. Live lease and
worker revalidation remains required; matching the stored binding is not proof
of a live lease. This audit does not call a loader, so provider load failures and
concurrent workers cannot cause an attempted recovery or prompt delivery here.

## Why loading is deferred

At this revision, ACPConnection.connect in src/pa/acp/client.py sets a restored
saved session to idle. AgentSessionRuntime.start in
src/pa/instance/agent_session.py restores queued_prompts, checkpoints ready and
starts the drain unless _defer_drain is set. AgentSessionManager._resume_from_snapshot
passes the recovered queue to that method and publishes the runtime. Runtime
prompt and resume_queue are independent entry points; queue_paused and
_defer_drain do not persistently prohibit all user, automatic and continuation
admission. The manager deliberately skips durably closed snapshots.

Consequently, removing that closed-session check or using a paused queue would
not establish the requested all-source delivery prohibition. A later restore
implementation needs a durable admission fence shared by all prompt sources,
owner/lease/worker fencing, exact native restore and saved settings verification,
plus an explicit disposition for deliberate cancellation before admission. This
endpoint makes no assertion that those requirements have been implemented.

## Original recovery remains open

No live original audit/recovery operation was invoked by this source change.
For session 1eb4393e-26a0-471b-9b01-0b96763d2c01 and receipt
be010d09-11db-54e2-8415-262746e3cc84, the outcome auditor must query the exact
saved continuation_prompt_id associated with key
cleanup14-pr655-exact9c3542b9-ordinary-managed-activation-20261108-v1.
Obtain authoritative prompt-specific completion or non-delivery evidence from
the preserved native history, correlating provider request/turn identity and
terminal outcome with the receipt's single attempt. Audit the recorded close
and dismissal separately. Neither absence of local turn evidence nor a healthy
ACP launcher proves non-delivery. Preserve UNKNOWN if those links cannot be
established. Do not rearm the receipt or replace its prompt identity.
