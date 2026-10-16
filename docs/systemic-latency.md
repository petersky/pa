# Systemic latency and session startup

This extends the merged database/API work in PR #471 (canonical card
`89957d94-db93-4bf4-b534-3fbea8ba783c`). The implementation belongs to child
`7894e44b-eef9-4da8-9f7f-7d3ae82c0b54`. Backup archive generation, inventory,
verification and Settings changes remain on card
`0331d21a-f014-44c8-9176-bc1e955a9ad2`.

## Evidence and shared causes

The original transcript drain timeout and 30-second materialization failure are
user reports. A bounded read of the running macbook's numeric runtime diagnostics
independently found 25 materialization calls, five recorded timeouts, maximum
runtime 197,942 ms, Python lock-wait p99 197,935 ms, and lock-hold p99 21.298 ms.
Sync push lock-hold p99 was 73,949 ms. These are cumulative/windowed observations,
not a synchronized trace proving that each historical warning shared one cause.
The journal report is `657c98f5-72e1-4ae6-adf6-f06dbb8d1b9b`.

Source inspection and an isolated normal-flow reproduction establish a shared
cause: incremental DAG indexing decoded every old commit and event before
skipping already indexed rows. Projection catch-up also decoded projected
ancestry. These reads occurred under the primary mutation lock used by session
execution-binding writes. In the baseline mixed fixture, a sync push took
6,379.538 ms while a binding operation took 6,343.104 ms.

Ready DAG topology now supplies projected ancestry; incomplete indexes retain
the canonical fallback. Incremental indexing decodes only the new suffix. New
objects still undergo hash/schema/realm validation; parent-first traversal,
cycle/history bounds, both merge parents, atomic projection/head checkpoints,
operation receipt validation and ref CAS remain intact. Indexed ancestry queries
walk only relevant parent edges. Fast-forward index maintenance follows canonical
projection/ref publication outside the primary mutation lock. Concurrent canonical
writers retain their own indexed-parent checks and builder fencing. An index
failure remains a visible error; it never rolls back or pretends to undo an
already durable ref.

The session list compounded contention: each row copied dispatch plans/history,
loaded every PR watch and returned full selection proofs. The baseline fixture
returned 3,955,888 bytes per list. Sidebar card and PR links now use batched narrow
queries; lifecycle summaries share one read connection. The canonical session
presenter reads indexed dispatch evidence without copying history. List defaults
are summaries; `detail=full` and session detail endpoints retain full evidence.
JSON encoding runs in the bounded executor. Durable sessions, prompts, card
bodies, proofs, transcripts, receipts and watch observations are preserved.

A separate cold-session probe found 8,144 ms of an 8,596 ms first start in
execution selection. Required harnesses now have separate discovery locks,
generation/refresh throttles and catalog scopes, avoiding optional provider
probes. Normal preference precedence determines whether narrowing is allowed;
preferred and automatic selectors still use the full candidate set. Partial
catalogs never replace full-instance evidence. Freshness, connection revision,
hard constraints, authority revalidation and provider confirmation still apply.
The background refresh remains coalesced and closes with the service.

Transcript writers combine already queued batches before the next durable append,
without a timer. A normal 512-event burst uses eight writes of 64 events instead
of 64 writes of eight events. Immutable event IDs, order, retry ownership,
cancellation recovery, compatibility rows and queue join accounting remain intact.

## Bounded measurements

Off-loop diagnostics separate admission queue wait, executor handoff, worker
service, thread CPU and event-loop completion delivery. Existing whole-operation
timings and warning thresholds remain unchanged. Each distribution retains 256
samples and exposes p50/p95/p99/max; lifetime slow-call counters remain available.
Dispatch writer/index locks now participate in Python wait/hold measurements.
Session-list diagnostics expose returned rows, encoded bytes and serialization
time. Existing transcript and dispatch metrics expose SQLite writer acquisition,
transaction hold, row changes and payload preparation/publication.

Worker service minus thread CPU includes I/O, locks, GIL and OS scheduling; it is
not pure disk time. SQLite writer acquisition includes successful busy-handler
waiting and statement execution; Python sqlite3 does not expose a successful
busy-wait callback. Nested lock holds can overlap. Returned rows and SQL statement
counts are not engine rows scanned. The benchmark records OS output-block counts,
not portable physical bytes written. No SQL, arguments, prompts or transcript
bodies enter these numeric diagnostics.

## Reproducible acceptance inventory

`uv run python scripts/benchmark_systemic_latency.py --output /tmp/pa-mixed.json`
creates its own temporary PA data/workspaces and a normal server lifecycle. It
seeds 1,000 history commits, 101 dispatches with retained plans, 30 chat sessions
with selection proofs, and 30 terminal watches. It measures 30 iterations per
family and three concurrent transcript producers (90 drains, 2,880 events).
The default eight-worker executor, existing deadlines and warning thresholds
apply. It runs no provider, external peer or model account. Fixture seeding is
outside the measurement window; raw runtime lifetime counters include seeding,
so explicit scenario samples and counter deltas define acceptance.

| Operation family | Representative exercised path |
| --- | --- |
| Foreground | Health, session list, Agent page, Home partial, status, SSE diagnostics |
| Sync | Canonical object publication, normal fast-forward push, projection/ref/index publication, commit reads |
| Startup storage | New session row and audited execution-binding compare-and-set |
| Transcript | Three producers, ordered append, compatibility mirror and durable drain |
| Progress/dispatch | Durable sequence allocation/checkpoint receipt, runnable and reconciliation reads |
| Background filesystem | Fleet cache write/read, wire-log append, ordinary local lifecycle workers |

The same script with `PYTHONPATH=.dev/systemic-baseline/src` measures base
`4697ed583f6a5eceead72217d07ba2ebaf4ccc83`, which already contains PR #471.
Disposable baseline source can be extracted with `git archive`; no other active
session's worktree is used.

`scripts/benchmark_database_api.py` separately covers cold transcript objects,
concurrent card/session writes and response projections. The systemic fixture
primarily uses hot transcript chunks. Regression tests cover cold object
integrity, rollback, sparse replacement, cancellation, bounds, merge ancestry,
receipt transitions, policy precedence, cache scope isolation and a stalled
derived index concurrent with a successful audited binding write.

`uv run python scripts/benchmark_session_cold_start.py --output /tmp/pa-cold.json`
launches ten tool-free ACP fixture processes through real selection, admission,
workspace provisioning, configuration, binding, publication and transcript drain.
It registers synthetic advertisements for all harnesses, so no installed provider
or model account is probed. It measures PA orchestration; real provider/MCP/network
initialization is outside this fixture's latency claim.

Results and integration checks are recorded below after final validation.
