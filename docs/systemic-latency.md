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

The full-Kernel incoming-sync regression also confirmed a callback contract
defect: the engine invoked a target-aware callback before ref publication, but
the application callback required the target to be the already-published ref.
A valid first import or fast-forward returned stale after three attempts.
Incoming publication now has an explicit application callback that checks the
expected ref and projects the explicit target under the canonical mutation
boundary, then publishes the ref. Recovery retains its strict published-tip
check. Derived indexing runs after releasing primary mutation admission.
First-import replay now uses the incoming target, rather than the previous ref.
Tests cover first import, fast-forward, projection rollback, stale CAS and an
independent session-binding write while derived indexing is held. This defect
does not establish the cause of every historical reconcile delay.

The expanded version-2 mixed fixture adds 30 incoming reconciliations and 30
100-row dispatch-list requests to the previous workload. Its first run retained
all 2,880 transcript events with no failures, rejections or timeouts, but produced
two slow calls: push maximum 643.293 ms and reconcile maximum 514.471 ms.
Those violations remain evidence, not a passing acceptance result. Their p95s
were 356.763/181.240 ms; executor dispatch maxima were 4.550/5.136 ms and
primary SQLite statement/commit totals per call were small. Disk-output blocks
reported zero on this Mac and cannot establish absence of disk activity.

A subsequent disposable database check confirmed both DAG index and object
catalog connections used WAL with `synchronous=FULL` (2), although initialization
requested NORMAL (1). SQLite's synchronous setting is connection-local; opening
each later connection restored its default. Apply the existing NORMAL policy
on each connection of these two explicitly disposable, rebuildable indexes,
and include their numeric SQLite calls/commits/BUSY errors in operation metrics.
Canonical object/ref publication and primary/transcript durability policies
remain unchanged. This policy defect is confirmed; its contribution to the
first run's two individual tails is not proven without comparative evidence.
The policy-only comparison still produced two warnings (push maximum 535.109 ms,
reconcile 1,091.566 ms); it does not establish tail acceptance or improvement.
Source inspection then found four DAG-index paths using SQLite's connection
context manager without closing the connection. That context manager commits or
rolls back but does not close. An explicit-lifetime regression retained strong
references to every opened connection and independently reproduced unclosed
handles after publication. Both writes and entity-snapshot reads now use the
existing closing transaction helper, including rollback. The benchmark also
records bounded numeric GC-pause evidence; no GC suppression is used.
After explicit closure, the same version-2 run retained all 2,880 events and had
zero startup/drain timeouts or failed/rejected operations, but one push still
violated 500 ms (526.823 ms). Push p50/p95/p99/max was
114.220/258.738/526.823/526.823 ms; reconcile was
101.305/286.721/444.309/444.309 ms. Binding maximum was 245.390 ms, drain
240.854 ms, Agent page 254.565 ms and dispatch list 98.280 ms. GC pauses reached
154.582 ms; their per-call causal overlap and external host/disk contention are
not established. This is a retained acceptance failure requiring further work,
not a zero-warning result. The three raw numeric runs are retained under
`docs/benchmarks/systemic-inventory-v2-*.json`; workload version 2 must not be
compared as an identical scenario to version 1.
Ten final-source tool-free ACP fixture starts and drains completed with no
failed, timed-out, rejected or slow operations. The first took 326.148 ms;
subsequent starts had median 144.994 ms and maximum 260.735 ms. Numeric evidence
is `docs/benchmarks/systemic-inventory-cold-start.json`; this does not measure
real provider, peer-network or installed-service startup.

Per-operation diagnostic timers then located substantial derived-index work:
one slow push spent 446.933 ms in indexing, including nested ancestry/connect/
close costs; its four explicit filesystem fsync calls totalled 0.453 ms. These
wall phases overlap, include scheduling/GC effects and add diagnostic overhead;
they do not prove that every historical delay was disk or SQLite locking. An
earlier global cProfile run mixed thread attribution and is excluded from causal
claims. The production bounded operation context remains thread-scoped.

A direct single-parent target proves ancestry without enumerating all retained
ancestors. Both projection catch-up and derived-index advance now stop at that
verified parent. General suffixes and merges retain complete ancestry exclusion,
canonical validation, CAS and receipt replay. A regression forbids ancestry
enumeration/old-history reads while checking matching checkpoints and history.
Index-only optimization still retained one 511.337 ms push violation. With both
paths bounded, the identical 30-iteration version-2 run reported zero warnings,
threshold violations, failures, rejections or startup/drain timeouts and retained
all 2,880 events. Push p50/p95/p99/max was
129.414/268.269/284.801/284.801 ms; reconcile was
77.550/189.265/478.238/478.238 ms. Numeric comparisons are in
`docs/benchmarks/systemic-inventory-v2-linear.json`. This finite scenario does
not close whole-PA/fleet acceptance or erase the failed preceding comparisons.
The stronger 100-iteration run retained all 9,600 transcript events without
failures/rejections/startup/drain timeouts, but failed the zero-warning target:
three transcript appends and two reconciles were slow. Reconcile p50/p95/p99/max
was 72.215/167.001/2,750.416/3,492.914 ms; drain maximum was 859.333 ms.
Canonical reads rose to 3,987 versus approximately 1,800 for normal deltas.
This result is retained in the same numeric comparison, not discarded in favor
of the passing 30-iteration run.

Imported forks not yet in the index still reopened their complete shared
canonical ancestry. They now walk novel commits to indexed boundaries and union
only the exact verified ancestors reachable from those boundaries. Unrelated
local descendants must not enter the answer. A regression checks exact fork
ancestry/common-base conflict assessment without reopening shared objects, then
resets the derived index and verifies the original canonical fallback agrees.

Version 3 makes ten ordinary compatible forks explicit per 100 iterations and
checks every requested sync head remains included, plus accepted progress
receipts. A prior partial run aborted on the fixture's exact-one-applied-commit
assumption; a compatible fork correctly applies an incoming commit and merge
marker. A real-Kernel regression verifies both parents/effects and the two-commit
result. First-import and fork API pushes now defer derived indexing until after
primary mutation admission is released, as fast-forward pushes already did.

The first complete version-3 run retained all 9,600 events with no failures,
rejections or startup/drain timeouts, but logged 19 slow calls: 12 reconciles,
six transcript appends and one execution binding. Reconcile maximum was
4,357.257 ms. It completed 12 compatible merges (ten explicit plus two incidental)
and 88 fast-forwards; canonical reads dropped to 1,975. Thus avoiding canonical
rereads alone did not close merge-path latency.

Numeric phase timers isolated the remaining ancestor-set cost. EXPLAIN confirmed
SQLite scanned all realm parent edges for every recursive frontier node. The
old query used 5,027,000 VM instructions at 1,000 edges and 20,054,000 at 2,000.
Adding a forward covering index alone still used 5,027,000 at 1,000: join order
also matters. A frontier-first CROSS JOIN plus the forward covering index uses
27,000/54,000 instructions respectively, with identical ancestor sets, UNION
deduplication and traversal limits. Tests assert a linear instruction bound,
independent of shared-host wall timing. This is confirmed in the source fixture;
it is not attributed to an unidentified installed binary or every historical
warning. The extra derived index maintains one entry per parent edge; its
one-time build cost on a large existing installation remains unmeasured.

Earlier SQLite timings measured execute/commit but omitted lazy row production,
so they could not rule out query service time. Numeric observations now cover
fetchone/fetchmany/fetchall/iteration and count returned rows, with cursor row
factories and transaction behavior preserved. SQL text, parameters and returned
values are never retained. Successful native busy-handler waiting still cannot
be isolated exactly from SQLite service using the standard-library interface.
The full runtime/cursor, history-scale, fork, CAS, receipt and rebuild regression
run passed 112 tests plus five subtests. The unchanged auth-navigation browser
case failed once in CI on head `686a8db` during read-only navigation polling and
passed in isolation; that failed CI run is retained, not treated as merge green.

The version-3 retest after the query repair still failed zero-warning acceptance:
eight slow calls (four reconciles, three transcript appends and one binding),
while retaining all 9,600 events with no failed/rejected operations or timeouts.
Reconcile p50/p95/p99/max was 86.356/443.855/1,125.391/3,497.975 ms; push maximum
was 392.384 ms, drain 547.657 ms and binding 519.978 ms. Both version-3 results,
their complete path distributions, row/byte metrics and counter deltas are in
`docs/benchmarks/systemic-inventory-v3.json`; query-plan work counts are in
`docs/benchmarks/systemic-ancestry-query-plan.json`. These remaining tails require
further investigation. SQL trace counts cover newly hooked connections and may
exclude preexisting persistent connections; physical disk write amplification
remains unmeasured on this Mac. No threshold, deadline, warning or durable-data
retention policy was relaxed.

Reconciliation scans now return immutable scheduling identities instead of
copying retained proofs for up to 1,000 dispatches. Each candidate still reloads
the current durable record before action. Worker progress is reported between
completed candidates; no timer heartbeat disguises a stalled candidate. The
retained restart emissions motivate this repair, but their historical individual
causes are not proven. Normal prompt enqueue still contains synchronous runtime
checkpoints and requires separate admission-safe investigation.

Bounded read-only peer observations at approximately 07:22 UTC are recorded in
card attachment `c846ed5c59d1486ba0a006abafc7189a`. The nonresponding configured
peer remained unidentified after one bounded identity/runtime read each. The
responding macmini (`02dbcd47-8f40-44eb-8403-5eb57545afc8`) reported 6,800
fleet-overview failed/timed-out calls out of 9,980 submissions, 638 PR-peer
failures out of 1,843, and 269 sync-peer failures out of 3,452. Failed and timed-out
counters overlap; these are process-lifetime counters, not new incidents.
Reconcile/push lifetime maxima remained 92.881/79.034 seconds. Peer build identity,
logs, individual failure causes and post-deployment acceptance remain unverified.
At 07:41 UTC the local status API reported running version 1.5.6, installed
1.5.8, null runtime VCS revision and release track `release`. Verified tag
`v1.5.8` points to `fe26edfab7899d1c4a64e8063d82093ffd550ef9`, preceding both
backup PR #483 and systemic PR #484. Neither a restart into that wheel nor a
source-branch merge would establish deployment of these repairs. No channel
switch, installed-service update or restart was performed.
