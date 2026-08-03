# Fleet execution capacity

Fleet execution capacity is the number of concurrent execution slots an
instance admits for prompting and provisioning work. It is a global
instance limit with optional per-provider ceilings. When a provider ceiling is
configured, both limits apply and the lower value wins. PA does not currently
support per-model limits.

`dispatch_capacity` accepts integers from 1 through 256. The effective default
is 4. Four preserves PA's historical admission ceiling and is intentionally
conservative because PA cannot infer provider account quotas, operator policy,
or whether host CPU and memory are shared with other workloads. It is not a
host-resource calculation.

Configure capacity in Settings → Instance, with `pa config set
dispatch_capacity N`, through `PATCH /api/config/capacity`, or with the
`set_dispatch_capacity` MCP tool. Changes are validated, persisted to
`config.json`, advertised in canonical Fleet membership, invalidate the local
activity snapshot, and apply immediately to new admissions. Existing work is
not preempted when a limit is lowered.

## Compatibility and precedence

The effective global value is resolved in this order:

1. the typed `dispatch_capacity` instance/member setting;
2. the first valid legacy `capacity:N` capability when the typed field is
   absent; or
3. the documented default of 4.

Fleet/API/UI responses identify the source as `configured`,
`legacy_capability`, or `documented_default`. A provider ceiling reports
`configured_provider`. Invalid legacy tags are ignored. `capacity:N` is a
read-only mixed-version compatibility path; operators should migrate the value
to `dispatch_capacity` and then remove the tag.

## What consumes a slot

The activity probe exposes these counts separately:

- `connected_runtimes`: live ACP connections, including idle sessions;
- `idle_sessions`: connected runtimes with no turn in flight;
- `deferred_sessions`: durable nonterminal sessions without a live runtime;
- `prompting_turns` / `active_capacity_consumers`: turns currently executing;
- `queued_prompts`: prompts waiting behind a runtime;
- `dispatch_reservations`: launchable durable dispatches in queued, sync-check,
  materialization, session-start, or prompt-delivery stages;
- `completion_work`: completion-outbox and reconciliation work; and
- `provider_concurrency`: the same execution counts grouped by provider.

Admission consumption is `prompting_turns + queued_prompts +
dispatch_reservations`. A dispatch linked to an already-prompting session is not
also counted as a reservation. Idle and deferred sessions, completion and
reconciliation, PR-supervisor polling, advisor/control-plane work with no ACP
turn, provider login jobs, and other operational sessions consume no slot by
themselves. PR executors, advisors, and temporary operational agents consume a
slot whenever their ACP turn is prompting or queued, exactly like user-created
sessions.

Provider action gates (`max_active=2`, `max_queue=8` by default) bound calls
inside one provider adapter. They protect a different resource and do not
replace fleet admission. The fleet limit decides whether work may be placed on
the instance; the provider gate controls how admitted work enters that provider.

## Admission, freshness, and reservations

All placement policies and named dispatch use the same fresh activity and
capacity model. A placement decision records the limit, source, working/queued/
reserved counts, observation timestamp, and links to consumers. Stale or failed
probes make a candidate ineligible.

The authority persists a reservation in the dispatch ledger in the same lock
that performs idempotency and concurrent-card checks. That lock rechecks current
reservations so concurrent last-slot attempts at one authority cannot both be
admitted. Idempotent retries return the original target; explicit retry probes
that target again and renews its reservation atomically. Reservations survive
restart, release when work begins running or fails/cancels/completes, and fail
recoverably after a one-hour pre-start timeout.

## Durable waiting queue

Execution capacity is not an admission limit. When every execution slot is
occupied, PA promptly persists eligible work as `waiting_capacity`. Waiting and
visibly `blocked` records do not consume execution slots, and terminal history
consumes neither execution nor queue capacity. Provisioning does consume an
execution slot, so PA creates no worktree or provider process before promotion.

`dispatch_queue_capacity` defaults to 100 and accepts 0 through 10,000. Zero
disables waiting when execution is full. Optional
`dispatch_provider_queue_capacities` apply together with the instance limit;
the lower value wins. These settings use the same web, CLI, authenticated API,
MCP, validation, and configuration-audit surfaces as execution capacity.

Idempotency and concurrent-card checks run before queue counting. Identical
retries return the existing record without using another queue slot. Queue
ordering is priority first, then fair principal/project/provider/target waves,
then admission time. This preserves FIFO within equal classes while preventing
one bulk class from starving the first item in another class. Authorized
priority changes and cancellation are idempotent and audited.

Before promotion, PA rechecks readiness for the original target and contract.
Unavailable targets remain blocked and are retried without silent rerouting;
Done cards and deleted or archived projects do not launch. Atomic promotion
ensures a newly free execution slot starts one queued dispatch exactly once,
including after restart.

When the queue is full, PA returns `dispatch_queue_full` with current and maximum
counts, execution capacity, source/provider, retry guidance, remediation
options, and the Operations recovery URL. Mixed-version targets that do not
advertise durable queue support retain the legacy capacity rejection for safety.

An administrator can set `capacity_override=true` with a non-empty
`capacity_override_reason`. The dispatch record retains both fields. Overrides
skip only the capacity-full rejection; readiness, authorization, provider,
repository, and lifecycle checks still apply.

Capacity overrides intentionally exceed concurrent execution limits and are not
required for ordinary queue admission.
