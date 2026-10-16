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
Terminal turn and connection-loss checkpoints also await off-loop persistence.
Their former synchronous saves could make an event-loop callback wait behind
the metadata writer; a regression keeps the loop live while that writer is held
and verifies the terminal checkpoint is durable before completion returns.

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
for the measured mixed and cold-start scenarios is
`a2802e7b09af6a22297a1977830501e24c092de9`, including warm full-catalog reuse
and awaited terminal checkpoints. The later evidence commit changes benchmark
counter compatibility and documentation only.

| Path | Baseline p95 / max, ms | Final p50 / p95 / p99 / max, ms |
| --- | ---: | ---: |
| Sync push | 4,235.549 / 7,089.001 | 139.200 / 365.185 / 370.080 / 370.080 |
| Startup row + audited binding | 4,212.452 / 6,843.431 | 36.770 / 189.984 / 218.872 / 218.872 |
| Transcript drain | 235.916 / 446.274 | 87.109 / 183.825 / 324.981 / 324.981 |
| Session list | 1,025.040 / 2,667.950 | 32.164 / 74.037 / 89.826 / 89.826 |
| Agent page | 584.729 / 1,393.456 | 56.709 / 255.171 / 274.718 / 274.718 |
| Progress sequence | 16.906 / 207.288 | 5.190 / 33.693 / 53.155 / 53.155 |
| Progress checkpoint | 13.631 / 16.805 | 7.218 / 28.979 / 36.660 / 36.660 |
| Fleet cache read | 2.926 / 9.951 | 1.769 / 7.265 / 25.967 / 25.967 |

The final mixed scenario recorded **zero** off-loop warnings, end-to-end
threshold violations, startup timeouts, drain timeouts, failed/rejected workload
calls or missing transcript events. Both sources retained all 2,880 transcript
events and consistent canonical/projection heads. Intermediate fixed runs still
had 2–5 slow-operation warnings; they failed acceptance and motivated further
instrumentation and writer admission changes. Thresholds and deadlines were not
raised. Individual unaffected small operations can vary or increase under host
contention; this is not a universal speedup for every microbenchmark.
The committed comparison runs were sequential, with no competing validation
process from this session. Other host activity was uncontrolled. An earlier
diagnostic baseline overlapped local tests and was replaced for this comparison.
The controlled baseline recorded 38 off-loop warnings and 58 end-to-end threshold
violations across sync, binding, list, Agent and status calls; the final run
recorded zero of each. Baseline counters absent in its source are null, not zero.

Canonical object reads fell from 184,098 / 78,669,051 bytes to 330 / 128,530 bytes.
SQL SELECT calls fell from 35,906 to 2,322. Fixture BEGIN/COMMIT counts fell from
695 to 267; durable transcript event/mirror rows were preserved. Session lists
fell from 3,955,888 to 75,522 bytes; final serialization p95 is recorded in the
numeric artifact. Statement counts are affected by ordinary lifecycle workers
and run duration, so they are workload observations rather than per-event
amplification constants.

Ten real tool-free ACP starts completed with zero failed/timed-out/slow
off-loop calls. The first complete start and drain took 296.269 ms; subsequent
starts ranged up to 136.444 ms (median 114.860 ms). The separate cold-object
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
The broad Mac four-worker run on the earlier implementation passed 3,387 tests
and 887 subtests, with 14 failures and five setup errors. Final-source serial
reruns cleared 18 of those 19 cases (including dispatch identity, workspace,
MCP handshake, SQLite and browser cases). The remaining unchanged maximum
loop-lag check failed at 44.512 ms in the serial run and 52.752 ms alone against
its 40 ms limit. Callback tracing found several short callbacks and aggregate
cold scheduling lag, not a single blocking 40 ms callback. A declared five-sample
comparison passed all five on the final source (maxima 33.580, 22.976, 18.778,
21.243, 18.633 ms); unchanged base source failed two of five (40.174 and
48.209 ms). This is a retained host-sensitive validation limit, not an assertion
that every Mac run is green. No test threshold was relaxed. Final mixed-workload
loop lag was p95 24.923 ms / max 72.274 ms, versus baseline p95 14.731 ms /
max 2,064.922 ms; there is no claim that every loop percentile improved.

All four required CI shards and the smoke/build/packaged-MCP job passed on
`a2802e7`. The final evidence commit and supervised integration results follow
[PR #484](https://github.com/petersky/pa/pull/484).

## Broader retained-log inventory and follow-up

The card's expanded inventory remains Active. The declared fixture above does
not establish whole-PA or production/fleet acceptance. A separate actual-log
scan on 2026-10-16 read 16 active/retained files, including seven gzip archives
and five numbered structured rotations: 86,440,227 source bytes at open,
100,754,466 decoded bytes and 570,983 physical lines. Known timestamps span
2026-10-08T03:25:05.396667Z–2026-10-16T07:10:18.520347Z; 3,200 records have no
attributable timezone-aware timestamp. Active-file growth is recorded, so this
is not an atomic snapshot or proof of exhaustive retention/fleet coverage.

All retained files yielded 110 slow-operation names across 22 prefix families,
with 18,115 raw slow emissions. A separate supported CLI 24-hour request returned
238,343 records and 106 names (12,658 raw slow emissions); its one-million-line
cap was not reached. Cross-sink duplicates remain possible; these are not
distinct-incident counts. Log percentiles describe logged slow calls, while
runtime percentiles use rolling windows and process maxima span process life.

Reviewable HTML, complete JSON and per-operation CSV are durable attachments on
card `7894e44b-eef9-4da8-9f7f-7d3ae82c0b54`: HTML
`5ca38a7337ef44e6a4bf43300a41ca9f`, JSON
`861e8a6e7ea14651918f84a557851d5f` (SHA256
`d797858e312a10168f438b248e49680e4c3a66b6583c4e31e51a4d3f83bd5345`),
CSV `6b413b471c734f409a8247f2fab11bd6`. They include every leaf/family's
count/p50/p95/p99/max/time range, safe examples, file manifest, page/startup/error
tables, HTTP access counts separated from SSE-like paths, and the dated runtime
route/operation inventory. Individual access-log durations and per-route maxima
are unavailable and are not inferred from aggregate process maxima.

Further findings include reconcile max 305.8 s, card-write max 159.9 s,
transcript-append max 40.8 s, provider initialization max 39.7 s, and 492 raw
completion-reconciler no-progress/restart error emissions. Peer failure causes,
logging loss/identity gaps, progress backlog, stale transcript integrity
diagnostics, provider/workspace paths and remaining leaf causes are explicitly
unverified. Backup ownership and the canonical database/API card are preserved.
No deployed repair or production retest is inferred from these logs.

The follow-up fixes corroborated CLI provenance/severity defects: auxiliary
`pa logs` used a new process UUID rather than configured source identity, and
`WARN` replacement transformed `WARNING` into `WARNINGING`, impairing severity
filtering and cross-sink deduplication. Identity reads now preserve explicit
settings and never initialize missing configuration; unknown identity is labelled
unknown. Warning normalization preserves severity.

Dispatch-job lists also serialized full plans/proofs. The default list now uses
detached status projections, with explicit `view=full` retaining full evidence.
Encoding runs in the bounded executor; rows/bytes/serialization are measured.
Protocol gap/compaction ranges, recovery controls and degraded-peer counts remain
visible. No durable plans, receipts, progress or execution state are removed.
An isolated 50-record/20-request fixture measured response bytes
20,275,331 → 123,140 and p50/p95/p99/max latency
38.304/63.564/79.788/79.788 → 10.693/14.569/15.768/15.768 ms. Its loop probe
includes the same-process ASGI client's JSON decode and is not a production
server-only loop SLO. These fixes do not resolve the remaining inventory.
