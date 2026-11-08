# Scheduling and reliable delivery

Approved scope for card `4acf4af8-7938-4391-a4b9-3c85b738867a`, source
conversation `7e7ffd35-adfc-4e2f-afd0-bc794a06ae78`.

## Ownership

PA owns timing, trigger matching and reliable delivery. Recipient agents choose
actions, reporting and completion. PA never interprets task completion or response
adequacy. Delivery means durable destination acceptance, not action or task
completion. Reminders require no card, project, goal, plan, final report,
task-specific completion gate, or additional policy/approval layer.

## Required behavior

1. Support once (date/time), interval, standard five-field cron with timezone,
   named PA events with simple event type/card/session/state/repository field
   filters, authenticated inbound webhook, and manual fire. No scripting or rules
   engine.
2. Persist independent records with stable ID, name, owner, trigger, recipient,
   payload, context references, enabled/paused state, and optional expiry or
   maximum occurrences. Work without a browser or live agent.
3. Use a common delivery interface for agent sessions, PA conversations/inboxes,
   existing configured messaging integrations, and outbound webhooks. Return
   durable acceptance or useful failure. Agent delivery uses normal input/queue;
   busy agents queue normally, interrupt only when explicitly selected. Missing
   targets remain visibly pending/failed and correctable; never silently choose
   another thread.
4. Envelopes include schedule/occurrence IDs, due/event/actual delivery times,
   user payload/context references, retry and missed metadata. Supply sufficient
   context without copying the full transcript.
5. Persist before acknowledgment, recover after restart, prevent duplicate
   occurrence creation across instances, reuse stable IDs on retry, and honor
   destination acknowledgment to prevent resend. Promise at-least-once delivery
   with deduplication where available, never exactly-once agent actions.
6. Default to one catch-up with missed count, then normal cadence; support skip
   and bounded replay. Retry transient failures with backoff; expose exhaustion
   and manual retry. Optional coalescing of undelivered busy-recipient messages
   retains the latest notification and missed count. Document/test cron DST and
   timezone semantics and preview upcoming times.
7. Provide equivalent MCP/API/UI create/read/update/delete, pause/resume,
   fire-now, history and retry operations using existing access permissions,
   without per-occurrence approval. Creation returns ID, enabled state,
   destination and next fire. Define predictable update/pause/delete races.
8. Show last occurrence, next due, last successful delivery, pending count,
   error, next retry, missed/coalesced count and scheduler running/overdue health.
   Failures remain visible when the destination channel is unavailable. Bound
   history and use ordinary logs. Reuse server infrastructure; no permanent
   model process.
9. Example: every ten minutes deliver to a monitoring agent: “Check the cleanup's
   current state and report a concise update to this conversation”. Reference
   cleanup card `7938e21e`, existing workers and conversation `7e7ffd35` and
   coalesce misses. The agent/user pauses or deletes when appropriate. PA does
   not judge the response or cleanup completion.
10. Acceptance requires two actual ten-minute deliveries with browser closed;
    idle-agent and busy normal-queue delivery; restart/machine-outage recovery;
    lost-reply deduplication; catch-up; cron DST/timezone; event filters, webhook
    authentication and duplicate events; unavailable-target failure/retry; and
    pause/update/delete races. Use disposable fault fixtures only. Label
    compressed-time tests separately from actual runtime evidence.

## Integration and evidence

Commit and push this requirements document before implementation. Use scoped
commits, relevant tests, a ready-for-review PR and PA's durable PR supervisor.
Merge only after stable green supervision and independent exact-head validation
of CI, review and mergeability. Record instance/repository/branch/base/worktree,
commits and PR in dispatch progress. Coordinate shared-code deployment with
systemic owner session `1eb4393e`; preserve cleanup `0337` and control `9bb`
work. Activation uses managed handoff only: no direct PAStore writes, unknown
mutation replay, default changes or synchronous restart. Report document commit,
implementation PR/tests/runtime evidence, residual work and next actor.
