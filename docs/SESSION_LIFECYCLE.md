# Agent session lifecycle and retention

PA distinguishes runtime eviction from durable closure. Disconnecting or
quiescing a provider keeps a session recoverable. Closing changes the durable
status to `closed`, disconnects a live provider, appends a `session_closed`
transcript event containing the close reason and prior status, and makes the
workspace lease eligible for the existing safe reconciliation process. Closed
session transcripts and completion evidence remain queryable.

The automatic lifecycle sweep runs on each execution instance every 30 seconds
by default. Decisions are idempotent and are made from durable session,
dispatch, reconciliation, PR-watch, card, and instance-local workspace state.
The default idle follow-up window is 24 hours. Configure these with
`PA_AGENT_SESSION_SWEEP_SECONDS` and
`PA_AGENT_SESSION_IDLE_RETENTION_HOURS`.

Lifecycle filesystem and SQLite work uses a reserved single-worker lane rather
than the shared control-plane blocking pool. Concurrent sweeps coalesce. Durable
close attempts fence on a monotonically increasing attempt number and bound both
the projection mutation-lock wait and SQLite busy wait to one second. A
contended attempt finishes as deferred before another attempt begins; it cannot
continue invisibly after the sweep has reported its terminal result.

## Lifecycle matrix

| Session/workflow | Policy | Close reason or retention condition |
| --- | --- | --- |
| Local interactive chat | Bounded retention | Close after idle retention; retain during the follow-up window. |
| Completed dispatched card turn | Immediate after proof | Close only after completion acknowledgement, card disposition, and reconciliation are terminal. |
| Superseded dispatch/session | Immediate | The newest durable session is canonical; older duplicates close as `superseded_duplicate`. |
| Cancelled/unrecoverable dispatch | Immediate | Close as `dispatch_terminal`; recoverable failures remain retained. |
| PR executor/watch | Immediate when terminal | Active/blocked watches retain; merged, closed, or retired watches close as `pr_watch_terminal`. |
| Completed card | Immediate when unencumbered | Close as `card_completed` after dispatch/watch safety checks. |
| Deleted card | Immediate when unencumbered | Close as `card_deleted`. |
| Recovery/reprovision replacement | Immediate | Duplicate canonicalization closes the predecessor. |
| Advisor/coordinator/login/operational executor | Immediate after idle | Single-purpose labels close as `single_purpose_finished`. |
| Provisioning/recovery failure | Operator or workflow dependent | Recoverable and blocked states remain retained; an unrecoverable terminal dispatch closes. |
| Fleet authority migration/sync conflict | Deferred | Retain while completion delivery or reconciliation is unresolved; retrying either side is safe. |

## Safety gates

Automatic closure is refused while any of these are true:

- a live or durable prompt is queued or in flight;
- a permission decision is pending;
- transcript/completion delivery is pending;
- dispatch or card-disposition reconciliation is active;
- a recoverable failed/cancelled dispatch remains actionable;
- an active or blocked PR watch is linked to the session or card;
- workspace status cannot be verified, has tracked/untracked changes, is still
  provisioning, or has an unresolved cleanup obligation.

Workspace closure does not delete a worktree. It expires the lease and invokes
the existing reconciler/collector, which requires completion/merge evidence and
refuses to remove a dirty worktree. A retained or deferred decision is logged
through reason-keyed lifecycle metrics rather than silently destroying work.

## Metrics and operations

The lifecycle service records counters keyed by outcome and reason, including
`auto_closed`, `auto_closed:<reason>`, `retained:<reason>`,
`deferred:<reason>`, and `skipped:already_closed`. These distinguish sessions
closed by policy from sessions retained for follow-up, deferred for durable
obligations, or skipped. The durable transcript event is the per-session audit
record and remains visible in session history after closure.

`GET /agent/status` includes the most recent 100 redacted lifecycle attempts.
Each entry exposes only session ID, close reason, queue and lease state, mutation
lock wait, lifecycle worker owner, attempt fence, and terminal result. Transcript
content is never included. `coalesced:sweep_active` counts redundant sweep
wakeups, and `deferred:close_contention` counts bounded lock/SQLite failures.

Explicit operator closure remains available. It uses the same durable audit and
workspace reconciliation primitives but is intentionally not a substitute for
the automatic safety evaluator.
