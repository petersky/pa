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
Primary session saves/disconnects and transcript compatibility transactions now
share the existing metadata mutation admission boundary. Only the short metadata
transaction is serialized; cold payload preparation/publication precedes it.
This removed the measured successful SQLite contention tails: before this change,
statement service reached 550 ms while commit service remained below 10 ms.

## Bounded measurements

Off-loop diagnostics separate admission queue wait, executor handoff, worker
service, thread CPU and event-loop completion delivery. Existing whole-operation
timings and warning thresholds remain unchanged. Each distribution retains 256
samples and exposes p50/p95/p99/max; lifetime slow-call counters remain available.
Dispatch writer/index locks now participate in Python wait/hold measurements.
Session-list diagnostics expose returned rows, encoded bytes and serialization
time. Existing transcript and dispatch metrics expose SQLite writer acquisition,
transaction hold, row changes and payload preparation/publication.
Primary projection connection calls also expose statement service, commit service,
call counts and BUSY/LOCKED failure counts per off-loop operation. These observe
connection execute/executemany/commit calls, not cursor fetch or schema-script
execution; an executemany batch counts as one Python API call.

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

## Acceptance results

Measured on this arm64 host with 10 logical CPUs and Python 3.14.2. The source
for the measured mixed scenario is `7e578095b990f35a7107a62082ca8d583339c846`.
The subsequent required-harness cache follow-up preserves warm full-catalog
evidence and has separate regression coverage.

| Path | Baseline p95 / max, ms | Final p50 / p95 / p99 / max, ms |
| --- | ---: | ---: |
| Sync push | 3,934.652 / 6,379.538 | 149.933 / 475.143 / 485.452 / 485.452 |
| Startup row + audited binding | 3,890.812 / 6,343.104 | 32.087 / 233.832 / 341.509 / 341.509 |
| Transcript drain | 447.306 / 739.386 | 95.089 / 242.715 / 373.651 / 373.651 |
| Session list | 802.010 / 858.263 | 33.418 / 82.336 / 97.507 / 97.507 |
| Agent page | 539.722 / 623.020 | 54.963 / 292.184 / 293.364 / 293.364 |
| Progress sequence | 29.727 / 980.444 | 5.464 / 33.302 / 44.175 / 44.175 |
| Progress checkpoint | 15.602 / 24.714 | 7.977 / 24.150 / 45.358 / 45.358 |
| Fleet cache read | 11.227 / 17.248 | 2.205 / 10.542 / 18.714 / 18.714 |

The final mixed scenario recorded **zero** off-loop warnings, end-to-end
threshold violations, startup timeouts, drain timeouts, failed/rejected workload
calls or missing transcript events. Both sources retained all 2,880 transcript
events and consistent canonical/projection heads. Intermediate fixed runs still
had 2–5 slow-operation warnings; they failed acceptance and motivated further
instrumentation and writer admission changes. Thresholds and deadlines were not
raised. Individual unaffected small operations can vary or increase under host
contention; this is not a universal speedup for every microbenchmark.
The baseline's initial samples overlapped the tail of local regression work;
the final mixed run had no competing validation process from this session.
Other host activity was uncontrolled. Compare structural reads/bytes directly;
latency ratios include that scheduling uncertainty.

Canonical object reads fell from 184,074 / 78,681,559 bytes to 330 / 128,530 bytes.
SQL SELECT calls fell from 35,868 to 2,324. Fixture BEGIN/COMMIT counts fell from
659 to 278; durable transcript event/mirror rows were preserved. Session lists
fell from 3,955,888 to 75,522 bytes; final serialization p95 is recorded in the
numeric artifact. Statement counts are affected by ordinary lifecycle workers
and run duration, so they are workload observations rather than per-event
amplification constants.

Ten real tool-free ACP starts completed with zero failed/timed-out/slow
off-loop calls. The first complete start and drain took 392.207 ms; subsequent
starts ranged up to 158.064 ms (median 145.010 ms). The separate cold-object
database fixture retained all 500 cold objects/references and compatibility rows,
reported no missing objects, and passed SQLite integrity. Its cold append p95
was 59.367 ms and maximum 116.979 ms.

Nearest-rank p99 over 30 observations equals the maximum; drain has 90 samples.
These are finite local scenarios on shared hardware, not production SLOs or
guarantees for arbitrary retained data, deep recovery/rebuild, automatic discovery
across optional integrations, slow disks, external provider/MCP/GitHub/network
latency, or unlimited concurrent agents. Fixture seeding can produce warnings
before the measurement window; the artifacts distinguish lifetime counters from
scenario deltas. No installed service was deployed or restarted for these tests.

Numeric evidence: [baseline](benchmarks/systemic-latency-baseline.json),
[final mixed workload](benchmarks/systemic-latency-after.json), and
[cold starts](benchmarks/systemic-latency-cold-start.json), plus the
[cold-object database fixture](benchmarks/systemic-latency-database.json).

Targeted suites passed: 63 sync/storage cases plus five subtests; 122 session and
transcript cases plus seven subtests; 167 presentation/supervision cases plus 12
subtests; 90 sync/selection cases plus five subtests; and 95 final metadata,
ordering, cancellation and CAS cases. One existing 40 ms maximum loop-lag check
failed at 129.389 ms while concurrent local work ran, then passed alone. Its
threshold was preserved. Isolated boot smoke and wheel/source builds passed.
The final full-suite and supervised integration results follow the PR record.
